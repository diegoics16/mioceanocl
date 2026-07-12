"""
POAL full pipeline: legacy archive scraper + standardized dataset + comparison + centralization.

WHAT THIS DOES
  1. Crawls DIRECTEMAR's legacy per-location/per-matrix listing pages and downloads
     every yearly file it finds (Isla de Pascua, Quintero, Concon, Valparaiso, Playa Ancha).
  2. Parses each file defensively: tries real Excel (xlrd for .xls, openpyxl for .xlsx),
     falls back to HTML-table parsing (many gov "xls" files are actually HTML), and if
     both fail, logs it as unparseable instead of guessing. Nothing is silently dropped.
  3. Auto-detects the header row per file by keyword search (ESTACION, PARAMETRO, VALOR,
     FECHA, etc.) rather than assuming header=0, and normalizes column names into a
     canonical schema.
  4. Downloads + parses the standardized national ZIP (same logic as build_poal_dataset.py).
  5. Compares legacy vs standardized for the overlap (same location/matriz/year/estacion/
     parametro) and reports matches, value mismatches, and coverage gaps in each direction.
  6. Writes a centralized dataset with a `source` column (legacy / standardized / both)
     so nothing is silently deduplicated in a way you can't audit later.

WHAT THIS DOES NOT DO
  - It does not assume the legacy file structure is stable across 30 years of files.
    Different years almost certainly have different column layouts. That's expected —
    check parsing_manifest.csv after the run for anything flagged needs_review.
  - Pagination detection on the listing pages is best-effort (looks for common Spanish
    "next page" markers). If a location/matriz shows suspiciously few files, that's the
    first thing to check manually in a browser.

RUN
    python -m pip install requests beautifulsoup4 pandas xlrd openpyxl lxml
    python poal_full_pipeline.py

    SAMPLE_MODE (below, default True) parses only ~3 files per location/matriz
    (oldest/middle/newest, to catch format drift across eras) instead of all ~312.
    Use this while iterating on parsing bugs — full run takes 15+ min, sample
    mode takes ~2 min. Set SAMPLE_MODE = False once parsing looks solid and you
    want the real, complete dataset. Files are cached to ./poal_output/cache/
    either way, so switching from sample to full mode only downloads the files
    not already fetched — it won't re-download the sample.

OUTPUT (all in ./poal_output/)
    parsing_manifest.csv       — one row per file attempted, with outcome
    poal_legacy_long.csv       — everything successfully parsed from the legacy archive
    poal_standardized_long.csv — the standardized national dataset, filtered to these 5 locations
    poal_comparison_report.csv — row-level match/mismatch/gap report for the overlap
    poal_centralized.csv       — merged dataset with a `source` provenance column
"""

import hashlib
import io
import json
import os
import re
import time
import zipfile
import unicodedata
import threading
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) research-script/1.0"}

# ---------------------------------------------------------------------------
# Supabase — added so this pipeline's output actually lands somewhere a
# public dashboard can query, instead of staying as local-only CSVs.
# Same pattern as snifa_scraper.py: service key, upsert via on_conflict,
# local backup written BEFORE the network call so a credentials/network
# failure can't lose an hour of parsing.
# ---------------------------------------------------------------------------
SUPABASE_URL = (os.environ.get("SUPABASE_URL") or "").rstrip("/")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
SUPABASE_TABLE = "poal_readings"
SUPABASE_CHUNK_SIZE = 500  # rows per POST — centralized dataset can run into the
                            # thousands of rows; one giant payload risks a timeout


def check_credentials():
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("SUPABASE_URL / SUPABASE_SERVICE_KEY not set — will run the full parse "
              "and write local CSVs, but will SKIP the Supabase push at the end.")
        return False
    print(f"Testing Supabase connection against: {SUPABASE_URL}/rest/v1/sync_runs")
    print(f"(SUPABASE_URL as received, length {len(SUPABASE_URL)}: {SUPABASE_URL!r})")
    try:
        supabase_write("sync_runs", [{
            "source": "credentials_check", "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(), "status": "test", "rows_written": 0,
        }])
        print("Supabase credentials OK.\n")
        return True
    except Exception as e:
        print(f"SUPABASE_URL / SUPABASE_SERVICE_KEY set but test write failed: {e}")
        print("Will run the full parse and write local CSVs, but will SKIP the Supabase push.")
        return False


def _sanitize(obj):
    """Recursively convert a rows payload into plain JSON-safe values.

    Deliberately NOT just a json.dumps(default=...) fallback: default() is
    only called for types the encoder doesn't recognize at all. Two real
    values from this pipeline slip past that net silently instead of
    raising, which is worse than an error:
      - plain float('nan') IS "recognized" by json — it gets emitted as the
        bare token NaN, which isn't valid JSON and PostgREST will reject.
      - pd.NaT has an .isoformat() method that returns the *string* "NaT"
        instead of raising, so a naive isoformat-fallback would happily
        write the literal text "NaT" into a date column.
    Both are handled explicitly below, before any generic isoformat call.
    """
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize(v) for v in obj]
    if obj is None or obj is pd.NaT:
        return None
    if isinstance(obj, float) and obj != obj:  # NaN (self-inequality trick; catches np.float64 too)
        return None
    if isinstance(obj, np.floating):
        return None if np.isnan(obj) else float(obj)
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, np.ndarray):
        return _sanitize(obj.tolist())
    if isinstance(obj, datetime):  # covers pd.Timestamp — it subclasses datetime.datetime
        return obj.isoformat()
    if hasattr(obj, "isoformat"):  # datetime.date, datetime.time
        try:
            return obj.isoformat()
        except Exception:
            return str(obj)
    try:
        if pd.isna(obj):
            return None
    except (TypeError, ValueError):
        pass  # pd.isna() raises on some array-likes/objects — not what we're sanitizing here
    return obj


def supabase_write(table, rows, on_conflict=None):
    if not rows:
        return
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    if on_conflict:
        url += f"?on_conflict={on_conflict}"
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Content-Type": "application/json",
        "Prefer": ("resolution=merge-duplicates,return=minimal" if on_conflict else "return=minimal"),
    }
    # NOTE: deliberately NOT using requests' json=rows here — see _sanitize()
    # docstring for why that silently produced bad payloads (not just threw
    # exceptions) for exactly the kind of values real POAL data contains.
    # default=str is a last-resort net for anything _sanitize() didn't think of.
    payload = json.dumps(_sanitize(rows), default=str).encode("utf-8")
    resp = requests.post(url, headers=headers, data=payload, timeout=60)
    if resp.status_code >= 300:
        raise RuntimeError(f"Supabase write to {table} failed ({resp.status_code}): {resp.text}")


def supabase_write_chunked(table, rows, on_conflict=None, label=""):
    """Same as supabase_write but in batches, with progress printed — the
    centralized POAL dataset is long-format and can easily be 5,000+ rows
    once legacy + standardized are both included.

    Returns (written, failed, first_error) — NOT just len(rows). A previous
    version of this function returned len(rows) unconditionally, so a run
    where every single chunk failed still logged status="success" with the
    full attempted row count. That's the exact bug behind sync_runs showing
    rows_written=83814 while poal_readings was empty: the number logged was
    always "attempted," never "actually landed."
    """
    if not rows:
        return 0, 0, None
    n_chunks = (len(rows) + SUPABASE_CHUNK_SIZE - 1) // SUPABASE_CHUNK_SIZE
    written = 0
    failed = 0
    first_error = None
    for i in range(0, len(rows), SUPABASE_CHUNK_SIZE):
        chunk = rows[i:i + SUPABASE_CHUNK_SIZE]
        chunk_num = i // SUPABASE_CHUNK_SIZE + 1
        try:
            supabase_write(table, chunk, on_conflict=on_conflict)
            print(f"    [{label}] chunk {chunk_num}/{n_chunks} written ({len(chunk)} rows)")
            written += len(chunk)
        except Exception as e:
            print(f"    [{label}] chunk {chunk_num}/{n_chunks} FAILED ({len(chunk)} rows): {e}")
            failed += len(chunk)
            if first_error is None:
                first_error = str(e)
            # keep going — one bad chunk (e.g. a NaN that serializes wrong)
            # shouldn't block every other chunk from landing
    return written, failed, first_error


def log_sync_run(source, started_at, finished_at, status, rows_written, error=None):
    try:
        supabase_write("sync_runs", [{
            "source": source, "started_at": started_at.isoformat(), "finished_at": finished_at.isoformat(),
            "status": status, "rows_written": rows_written, "error": error,
        }])
    except Exception as e:
        print(f"WARNING: could not write sync_runs log entry: {e}")


def make_row_id(*parts):
    """Deterministic id for upsert on_conflict. There's no natural primary key
    across two independently-sourced datasets, so hash the identifying fields.
    Same inputs always produce the same id — re-running the pipeline updates
    existing rows instead of duplicating them."""
    key = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:24]


OUT_DIR = Path("./poal_output")
OUT_DIR.mkdir(exist_ok=True)
SLEEP_BETWEEN_REQUESTS = 1.0  # seconds per worker — be polite, this is a government site
MAX_WORKERS = 5               # concurrent downloads. ~5 req/s aggregate at 1s/worker sleep.

SAMPLE_MODE = False         # False = full archive, every file, nothing sampled away
SAMPLE_PER_LISTING = 3      # only used if you flip SAMPLE_MODE to True for a quick debug run

# Confirmed-live listing pages as of 2026-07-10. Each maps to a "matriz" (water/biota/sediment).
LOCATIONS = {
    "Isla de Pascua": {
        "Agua": "https://www.directemar.cl/directemar/site/tax/port/fid_adjunto/taxport_45_149_387_1.html",
        "Sedimento": "https://www.directemar.cl/directemar/site/tax/port/fid_adjunto/taxport_45_149_389_1.html",
    },
    "Quintero": {
        "Agua": "https://www.directemar.cl/directemar/site/tax/port/fid_adjunto/taxport_45_150_390_1.html",
        "Biota": "https://www.directemar.cl/directemar/site/tax/port/fid_adjunto/taxport_45_150_391_1.html",
        "Sedimento": "https://www.directemar.cl/directemar/site/tax/port/fid_adjunto/taxport_45_150_392_1.html",
    },
    "Concon": {
        "Agua": "https://www.directemar.cl/directemar/site/tax/port/fid_adjunto/taxport_45_151_393_1.html",
        "Biota": "https://www.directemar.cl/directemar/site/tax/port/fid_adjunto/taxport_45_151_394_1.html",
        "Sedimento": "https://www.directemar.cl/directemar/site/tax/port/fid_adjunto/taxport_45_151_395_1.html",
    },
    "Valparaiso": {
        "Agua": "https://www.directemar.cl/directemar/site/tax/port/fid_adjunto/taxport_45_152_396_1.html",
        "Biota": "https://www.directemar.cl/directemar/site/tax/port/fid_adjunto/taxport_45_152_397_1.html",
        "Sedimento": "https://www.directemar.cl/directemar/site/tax/port/fid_adjunto/taxport_45_152_398_1.html",
    },
    "Playa Ancha": {
        "Agua": "https://www.directemar.cl/directemar/site/tax/port/fid_adjunto/taxport_45_153_399_1.html",
        "Biota": "https://www.directemar.cl/directemar/site/tax/port/fid_adjunto/taxport_45_153_400_1.html",
        "Sedimento": "https://www.directemar.cl/directemar/site/tax/port/fid_adjunto/taxport_45_153_401_1.html",
    },
}

STANDARDIZED_ZIP_URL = "https://www.directemar.cl/directemar/site/docs/20260114/20260114164231/poal_estandarizado_2024.zip"

# The standardized file is NATIONAL — every water body DIRECTEMAR monitors, not just
# these five. Unlike the legacy archive (which only has these five locations because
# that's all we pointed the scraper at), the standardized file needs an explicit
# filter or it centralizes ~300k irrelevant rows. Keyword match (not exact match)
# because the actual column value spelling isn't confirmed yet — see the printed
# "Guessed location column(s)" output on the first real run, and adjust this list
# if it turns out DIRECTEMAR spells something differently than expected.
# Zapallar and Quintay are IN here even though they're not in LOCATIONS above —
# they're the clean-water comparison sites for the Ventanas-vs-Zapallar contrast,
# and only the standardized file might carry them (the legacy archive's listing
# pages were never scraped for them, so this is the one chance to catch them).
KEEP_LOCATION_KEYWORDS = [
    "QUINTERO", "PUCHUNCAVI", "PUCHUNCAVÍ", "VENTANAS", "CONCON", "CONCÓN",
    "VALPARAISO", "VALPARAÍSO", "PLAYA ANCHA", "ISLA DE PASCUA",
    "ZAPALLAR", "QUINTAY",
]

# Canonical column names we're trying to map every legacy file's messy headers onto.
HEADER_KEYWORDS = ["ESTACION", "ESTACIÓN", "PARAMETRO", "PARÁMETRO", "VALOR", "FECHA", "UNIDAD"]

COLUMN_MAP = {
    "fecha": ["FECHA", "FECHA MUESTREO", "FECHA DE MUESTREO", "FECHA CAMPAÑA"],
    "estacion": ["ESTACION", "ESTACIÓN", "NOMBRE ESTACION", "NOMBRE ESTACIÓN", "PUNTO", "PUNTO MUESTREO"],
    "parametro": ["PARAMETRO", "PARÁMETRO", "VARIABLE", "ANALITO"],
    "valor": ["VALOR", "VALOR CORREGIDA", "VALOR CORREGIDO", "RESULTADO", "CONCENTRACION", "CONCENTRACIÓN"],
    "unidad": ["UNIDAD", "UNIDADES", "UNIDAD MEDIDA"],
    "latitud": ["LATITUD", "LAT"],
    "longitud": ["LONGITUD", "LONG", "LON"],
}

manifest_rows = []
manifest_lock = threading.Lock()

CACHE_DIR = OUT_DIR / "cache"
CACHE_DIR.mkdir(exist_ok=True)


def dedupe_columns(columns):
    """Guarantee unique column labels. Merged/blank header cells in old government
    Excel exports commonly produce repeated or empty labels, which breaks pd.concat
    later with InvalidIndexError. Never silently drops a column — just renames dupes."""
    seen = {}
    result = []
    for col in columns:
        key = "BLANK" if pd.isna(col) or str(col).strip() == "" else str(col).strip()
        if key not in seen:
            seen[key] = 0
            result.append(key)
        else:
            seen[key] += 1
            result.append(f"{key}__dup{seen[key]}")
    return result


def strip_accents(s):
    return "".join(c for c in unicodedata.normalize("NFD", str(s)) if unicodedata.category(c) != "Mn")


def norm(s):
    return strip_accents(str(s)).strip().upper()


def get(url, binary=False):
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    time.sleep(SLEEP_BETWEEN_REQUESTS)
    return r.content if binary else r.text


def get_binary_cached(url):
    """Binary downloads are cached to disk. Once a file is fetched once, re-running
    the script to fix a parsing bug reads from disk instead of re-hitting the site."""
    cache_path = CACHE_DIR / (re.sub(r"[^A-Za-z0-9]+", "_", url)[-180:] + ".cache")
    if cache_path.exists():
        return cache_path.read_bytes()
    content = get(url, binary=True)
    cache_path.write_bytes(content)
    return content


# ---------------------------------------------------------------------------
# STEP 1: discover every yearly file per location/matriz
# ---------------------------------------------------------------------------

def discover_year_files(listing_url, max_pages=10):
    """Follow best-effort pagination on a taxport listing page, collecting .xls/.xlsx links."""
    files = []
    seen_pages = set()
    url = listing_url
    for _ in range(max_pages):
        if not url or url in seen_pages:
            break
        seen_pages.add(url)
        try:
            html = get(url)
        except Exception as e:
            print(f"  [WARN] could not load listing page {url}: {e}")
            break
        soup = BeautifulSoup(html, "html.parser")

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if re.search(r"\.xlsx?$", href, re.IGNORECASE):
                full = href if href.startswith("http") else "https://www.directemar.cl" + href
                if full not in files:
                    files.append(full)

        # best-effort "next page" detection — adjust if this misses real pagination
        next_href = None
        for a in soup.find_all("a", href=True):
            text = norm(a.get_text())
            if text in (">", "»", "SIGUIENTE", "NEXT") or "pagina" in a["href"].lower():
                next_href = a["href"]
                break
        url = (next_href if not next_href or next_href.startswith("http")
               else "https://www.directemar.cl" + next_href) if next_href else None

    return files


# ---------------------------------------------------------------------------
# STEP 2: defensive parsing of one legacy file
# ---------------------------------------------------------------------------

def find_header_row(df_raw, max_scan=25):
    """Scan the first rows for the one that looks most like a real header."""
    best_row, best_score = None, 0
    for i in range(min(max_scan, len(df_raw))):
        row_vals = [norm(v) for v in df_raw.iloc[i].tolist()]
        score = sum(1 for kw in HEADER_KEYWORDS if any(kw in v for v in row_vals))
        if score > best_score:
            best_score, best_row = score, i
    return best_row if best_score >= 2 else None


def map_columns(columns):
    """Map raw column labels to canonical names. Only the first raw column matching a
    given canonical name gets renamed — if a file has e.g. both 'VALOR' and 'VALOR
    CORREGIDO', collapsing them into one 'valor' column would silently discard one of
    two genuinely different measurements. Later matches keep their original (deduped)
    label so the data survives and shows up in columns_found for manual review."""
    mapped = {}
    used_canonical = set()
    for col in columns:
        col_n = norm(col)
        for canonical, variants in COLUMN_MAP.items():
            if canonical in used_canonical:
                continue
            if any(norm(v) in col_n or col_n in norm(v) for v in variants):
                mapped[col] = canonical
                used_canonical.add(canonical)
                break
    return mapped


def parse_excel_bytes(content, engine):
    xl = pd.ExcelFile(io.BytesIO(content), engine=engine)
    frames = []
    for sheet in xl.sheet_names:
        raw = xl.parse(sheet, header=None)
        hdr_idx = find_header_row(raw)
        if hdr_idx is None:
            continue
        df = raw.iloc[hdr_idx + 1:].copy()
        df.columns = dedupe_columns(raw.iloc[hdr_idx].tolist())
        frames.append(df)
    if not frames:
        raise ValueError("no sheet had a detectable header row")
    return pd.concat(frames, ignore_index=True)


def parse_html_table_bytes(content):
    tables = pd.read_html(io.BytesIO(content))
    frames = []
    for raw in tables:
        hdr_idx = find_header_row(raw)
        if hdr_idx is not None:
            df = raw.iloc[hdr_idx + 1:].copy()
            df.columns = dedupe_columns(raw.iloc[hdr_idx].tolist())
            frames.append(df)
    if not frames:
        # maybe the table already has a sane header from read_html itself
        frames = [t for t in tables if len(t.columns) >= 3]
    if not frames:
        raise ValueError("no HTML table had a detectable header row")
    return pd.concat(frames, ignore_index=True)


def log_manifest(entry):
    with manifest_lock:
        manifest_rows.append(entry)


def parse_legacy_file(url, location, matriz):
    entry = {"url": url, "location": location, "matriz": matriz, "status": None,
             "method": None, "rows": 0, "columns_found": None, "note": ""}
    try:
        content = get_binary_cached(url)
    except Exception as e:
        entry["status"] = "download_failed"
        entry["note"] = str(e)
        log_manifest(entry)
        return None

    engine = "xlrd" if url.lower().endswith(".xls") else "openpyxl"
    df = None
    for method, fn in [
        (engine, lambda: parse_excel_bytes(content, engine)),
        ("html_table", lambda: parse_html_table_bytes(content)),
    ]:
        try:
            df = fn()
            entry["method"] = method
            break
        except Exception as e:
            entry["note"] += f"{method} failed: {e}; "

    if df is None:
        entry["status"] = "unparseable"
        log_manifest(entry)
        return None

    col_map = map_columns(df.columns)
    df = df.rename(columns=col_map)
    entry["columns_found"] = list(df.columns)

    required = {"estacion", "parametro", "valor"}
    if not required.issubset(set(df.columns)):
        entry["status"] = "needs_review_missing_columns"
        entry["rows"] = len(df)
        log_manifest(entry)
        return None

    df.columns = dedupe_columns(df.columns.tolist())  # belt-and-suspenders after rename
    df["location"] = location
    df["matriz"] = matriz
    df["source_file"] = url
    entry["status"] = "ok"
    entry["rows"] = len(df)
    log_manifest(entry)
    return df


# ---------------------------------------------------------------------------
# STEP 3: standardized dataset (same approach as build_poal_dataset.py)
# ---------------------------------------------------------------------------

def process_standardized():
    """Download the national ZIP, map its columns onto the same canonical schema
    the legacy archive uses, then filter it down to the bay + comparison sites.
    The old version of this function just downloaded and returned the raw
    national file — every water body DIRECTEMAR monitors, unfiltered. That's
    not usable for centralization: it needs the same fecha/estacion/parametro/
    valor/matriz/location shape as the legacy dataframe before a join is possible."""
    print("Downloading standardized ZIP...")
    content = get_binary_cached(STANDARDIZED_ZIP_URL)
    z = zipfile.ZipFile(io.BytesIO(content))
    csv_names = [n for n in z.namelist() if n.lower().endswith(".csv")]
    if not csv_names:
        raise ValueError("no CSV found inside standardized ZIP")
    with z.open(csv_names[0]) as f:
        raw = pd.read_csv(f, sep=";", encoding="latin-1", low_memory=False)
    print(f"Standardized file: {len(raw)} total rows nationwide, columns: {list(raw.columns)}")

    col_map = map_columns(raw.columns)
    df = raw.rename(columns=col_map)

    loc_col_candidates = [c for c in raw.columns if "CUERPO" in norm(c) or "AGUA" in norm(c)]
    loc_col = loc_col_candidates[0] if loc_col_candidates else None
    matriz_col_candidates = [c for c in raw.columns if "MATRIZ" in norm(c)]
    matriz_col = matriz_col_candidates[0] if matriz_col_candidates else None
    print(f"Guessed location column: {loc_col!r} | Guessed matriz column: {matriz_col!r}")

    if loc_col is None:
        print("  [FLAG] no location column auto-detected on the standardized file — cannot safely "
              "filter a NATIONAL dataset down to this bay. Returning nothing rather than silently "
              "centralizing every water body in Chile. Check the column list printed above and add "
              "the real column name to the CUERPO/AGUA keyword match if it's spelled differently.")
        return pd.DataFrame()

    df["location"] = raw[loc_col]
    df["matriz"] = raw[matriz_col] if matriz_col else None
    if matriz_col is None:
        print("  [FLAG] no matriz column auto-detected — every standardized row will have a blank "
              "matriz (water/sediment/biota unknown). Check the column list above; the panel-shift "
              "story (coliforms stop ~2017, metals continue) depends on matriz being right.")

    mask = df["location"].astype(str).map(norm).apply(lambda v: any(kw in v for kw in KEEP_LOCATION_KEYWORDS))
    kept = df[mask].copy()
    kept["source_file"] = "standardized_national_zip"
    print(f"Filtered to bay + comparison sites: {len(kept)} / {len(df)} rows kept "
          f"(dropped {len(df) - len(kept)} rows — other water bodies nationwide)")
    if len(kept) == 0:
        print("  [FLAG] zero rows matched KEEP_LOCATION_KEYWORDS. The keyword list almost certainly "
              "doesn't match this file's real spelling — inspect df['location'].unique() manually "
              "before trusting that the bay genuinely has zero standardized-file rows.")
    return kept


# ---------------------------------------------------------------------------
# STEP 4: orchestration
# ---------------------------------------------------------------------------

YEAR_RE = re.compile(r"(19|20)\d{2}")
JOIN_KEYS = ["location_norm", "matriz_norm", "estacion_norm", "parametro_norm", "year"]


def extract_year(fecha_val):
    if fecha_val is None or (isinstance(fecha_val, float) and pd.isna(fecha_val)):
        return None
    m = YEAR_RE.search(str(fecha_val))
    return int(m.group(0)) if m else None


def canonical_location(raw_location):
    """Legacy uses bare names ('Quintero'); the standardized file uses fuller
    names ('BAHIA QUINTERO', possibly 'BAHIA DE QUINTERO' etc). Exact-string
    matching after normalization fails on this every time. Bucket both onto
    whichever KEEP_LOCATION_KEYWORDS entry appears as a substring instead —
    coarser, but it's what actually lets the two sources agree on "same place"."""
    if raw_location is None or (isinstance(raw_location, float) and pd.isna(raw_location)):
        return None
    v = norm(raw_location)
    for kw in KEEP_LOCATION_KEYWORDS:
        if kw in v:
            return kw
    return v  # fell outside the keyword list entirely — keep it visible, not silently dropped


# Order matters: checked top to bottom, first match wins. AGUA is last
# because 'SEDIMENTO'/'BIOTA' never contain 'AGUA' as a substring, but this
# keeps the list honest about that being why the order is safe either way.
MATRIZ_KEYWORDS = [
    ("SEDIMENT", "Sedimento"),
    ("BIOTA", "Biota"),
    ("AGUA", "Agua"),  # covers 'AGUA', 'AGUA DE MAR', 'AGUA SUPERFICIAL', etc.
]


def canonical_matriz(raw_matriz):
    """The legacy archive scraper hardcodes matriz as our own labels ('Agua',
    'Sedimento', 'Biota' — literally the LOCATIONS dict keys). The
    standardized national file instead carries DIRECTEMAR's own MATRIZ
    column verbatim, and their real vocabulary is more specific — e.g.
    'AGUA DE MAR' for seawater. That's a real value, not a typo, but left
    unmapped it becomes a second dropdown option instead of merging with
    'Agua'. Bucket by keyword the same way canonical_location() already
    does for locations."""
    if raw_matriz is None or (isinstance(raw_matriz, float) and pd.isna(raw_matriz)):
        return None
    v = norm(raw_matriz)
    for kw, canonical in MATRIZ_KEYWORDS:
        if kw in v:
            return canonical
    return raw_matriz  # unrecognized variant — keep visible, don't silently drop


def add_join_keys(df):
    """Normalize whatever's in location/matriz/estacion/parametro/fecha into join-safe
    keys. Station name and parameter spelling will NOT match character-for-character
    between a 1990s Excel export and a 2024 standardized CSV — this is the best
    reasonably achievable join without a hand-built station name crosswalk, which is
    a real limitation worth stating in the presentation, not hiding."""
    df = df.copy()
    for col in ("location", "matriz", "estacion", "parametro"):
        if col not in df.columns:
            df[col] = None
    df["matriz"] = df["matriz"].map(canonical_matriz)  # fix BEFORE join keys are derived
    df["location_norm"] = df["location"].map(canonical_location)
    df["matriz_norm"] = df["matriz"].map(lambda v: norm(v) if pd.notna(v) else None)
    df["estacion_norm"] = df["estacion"].map(lambda v: norm(v) if pd.notna(v) else None)
    df["parametro_norm"] = df["parametro"].map(lambda v: norm(v) if pd.notna(v) else None)
    df["year"] = df["fecha"].map(extract_year) if "fecha" in df.columns else None
    df["valor_numeric"] = pd.to_numeric(df.get("valor"), errors="coerce")
    return df


def build_comparison_report(legacy_kw, std_kw):
    """One row per (location, matriz, estacion, parametro, year) bucket, classified
    as matched/mismatched/only-in-one-source. This is the actual Phase 3 the
    docstring promised and the old version of this script never built."""
    if legacy_kw.empty and std_kw.empty:
        return pd.DataFrame()
    l = (legacy_kw.groupby(JOIN_KEYS, dropna=False)["valor_numeric"]
         .agg(legacy_mean="mean", legacy_count="count").reset_index())
    s = (std_kw.groupby(JOIN_KEYS, dropna=False)["valor_numeric"]
         .agg(standardized_mean="mean", standardized_count="count").reset_index())
    merged = l.merge(s, on=JOIN_KEYS, how="outer", indicator=True)

    def classify(row):
        if row["_merge"] == "left_only":
            return "only_in_legacy"
        if row["_merge"] == "right_only":
            return "only_in_standardized"
        if pd.isna(row["legacy_mean"]) or pd.isna(row["standardized_mean"]):
            return "both_present_no_numeric_value"
        denom = max(abs(row["standardized_mean"]), 1e-9)
        rel_diff = abs(row["legacy_mean"] - row["standardized_mean"]) / denom
        return "match" if rel_diff <= 0.01 else "value_mismatch"

    merged["match_status"] = merged.apply(classify, axis=1)
    return merged.drop(columns=["_merge"])


def build_centralized(legacy_kw, std_kw, comparison_df):
    """Every row from BOTH sources, kept — never silently deduplicated — each
    tagged with its own source and with the match_status of the bucket it
    belongs to, so a bucket marked value_mismatch can be traced back to the
    exact rows on each side that disagree."""
    status_lookup = {}
    if not comparison_df.empty:
        status_lookup = comparison_df.set_index(JOIN_KEYS)["match_status"].to_dict()

    def finalize(df, source_name):
        df = df.copy()
        df["source"] = source_name
        df["match_status"] = df[JOIN_KEYS].apply(lambda r: status_lookup.get(tuple(r), "unknown"), axis=1)
        # 'valor' (not just valor_numeric) is part of the id on purpose: two
        # rows with the same station/parameter/date but different depths or
        # replicates are DIFFERENT measurements, and previously hashed to the
        # same id — which is exactly what caused "ON CONFLICT DO UPDATE
        # cannot affect row a second time" when both landed in one chunk.
        # Genuinely identical rows (same value too) still collapse to one id,
        # which is correct — that's the same measurement, not two.
        df["id"] = df.apply(lambda r: make_row_id(
            source_name, r.get("location"), r.get("matriz"), r.get("estacion"),
            r.get("parametro"), r.get("fecha"), r.get("source_file"), r.get("valor")), axis=1)
        return df

    parts = []
    if not legacy_kw.empty:
        parts.append(finalize(legacy_kw, "legacy"))
    if not std_kw.empty:
        parts.append(finalize(std_kw, "standardized"))
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True, sort=False)


def _clean_int(v):
    """Coerce anything year-like (int, float, '2005', '2005.0', numpy scalar,
    NaN/None) into a plain Python int or None. Applied right before export
    rather than fixed further upstream: pandas silently upcasts an
    int-with-nulls column to float64 on concat (legacy_kw + std_kw merging in
    build_centralized), and there's more than one place that could happen —
    validating at the system boundary, right before the external write,
    catches all of them instead of chasing each one individually."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def push_centralized_to_supabase(centralized_df):
    # DIRECTEMAR (Armada de Chile) confirmed by email: only the standardized
    # national dataset ("DATA ESTANDARIZADA POAL 1993-2024") is authorized
    # for public display. The legacy per-location archive is still parsed
    # and kept in poal_comparison_report.csv / poal_centralized.csv locally
    # (useful for auditing source agreement) — it just never gets pushed to
    # the public-facing table from here on.
    before = len(centralized_df)
    centralized_df = centralized_df[centralized_df["source"] == "standardized"]
    dropped = before - len(centralized_df)
    if dropped:
        print(f"\n[{SUPABASE_TABLE}] Excluding {dropped} legacy-source rows from the public push "
              f"per DIRECTEMAR's authorization — standardized-source only.")

    keep_cols = ["id", "source", "location", "matriz", "estacion", "parametro", "valor",
                 "valor_numeric", "unidad", "fecha", "year", "latitud", "longitud",
                 "source_file", "match_status"]
    present_cols = [c for c in keep_cols if c in centralized_df.columns]
    export_df = centralized_df[present_cols].where(pd.notnull(centralized_df[present_cols]), None)
    if "year" in export_df.columns:
        # NOT export_df["year"].map(_clean_int) — pandas re-infers a numeric
        # dtype from the mapped output when the source column is float64,
        # silently turning the clean ints right back into floats (and None
        # back into NaN). Confirmed by testing; costly to get wrong silently
        # a second time. Explicit object dtype is what actually holds.
        export_df["year"] = pd.Series(
            [_clean_int(v) for v in export_df["year"]],
            index=export_df.index, dtype="object",
        )
    rows = export_df.to_dict("records")

    # Belt-and-suspenders: 'valor' in the id hash (see finalize()) should
    # make same-id rows genuinely identical content, so collapsing them is
    # correct rather than lossy — but making that visible beats a silent
    # overwrite if that assumption is ever wrong for some future data shape.
    seen = {}
    deduped = []
    for r in rows:
        rid = r.get("id")
        if rid in seen:
            seen[rid] += 1
            continue
        seen[rid] = 1
        deduped.append(r)
    dupe_count = sum(v - 1 for v in seen.values() if v > 1)
    if dupe_count:
        print(f"\n[{SUPABASE_TABLE}] {dupe_count} rows had an id matching an earlier row in this "
              f"push and were skipped (first occurrence kept). If this number is large, the "
              f"disambiguation in finalize()/make_row_id() needs another look — it should be near "
              f"zero, since it now means truly identical (station, matriz, parametro, fecha, source, "
              f"valor) rows, not just same-day/same-station replicates.")
    rows = deduped

    print(f"\nPushing {len(rows)} centralized rows to Supabase table '{SUPABASE_TABLE}'...")
    written, failed, first_error = supabase_write_chunked(SUPABASE_TABLE, rows, on_conflict="id", label=SUPABASE_TABLE)
    print(f"\n[{SUPABASE_TABLE}] {written} rows actually written, {failed} rows failed"
          + (f" — first error: {first_error}" if first_error else ""))
    return written, failed, first_error


def sample_files(files):
    """Oldest/middle/newest — enough to catch format drift across eras without
    downloading everything. Listing pages present files roughly chronologically."""
    if not SAMPLE_MODE or len(files) <= SAMPLE_PER_LISTING:
        return files
    idx = sorted(set([0, len(files) // 2, len(files) - 1]))
    return [files[i] for i in idx]


def main():
    t0 = time.time()
    pipeline_started_at = datetime.now(timezone.utc)
    has_supabase = check_credentials()
    if SAMPLE_MODE:
        print(f"*** SAMPLE_MODE is ON — parsing ~{SAMPLE_PER_LISTING} files per listing for a fast test run. ***")
        print("*** Set SAMPLE_MODE = False at the top of the script for the real full run. ***\n")

    print("=" * 80)
    print("PHASE 1a — discovering files (serial — cheap, ~15 listing pages)")
    print("=" * 80)
    tasks = []  # (url, location, matriz) for every file to download+parse
    for location, matrices in LOCATIONS.items():
        for matriz, listing_url in matrices.items():
            print(f"{location} / {matriz}", end="  ")
            files = discover_year_files(listing_url)
            print(f"-> {len(files)} candidate files")
            if len(files) < 5:
                print("  [FLAG] suspiciously few files — check this listing page manually in a browser")
            files_to_parse = sample_files(files)
            if SAMPLE_MODE and len(files_to_parse) < len(files):
                print(f"  sampling {len(files_to_parse)} of {len(files)}")
            tasks.extend((f, location, matriz) for f in files_to_parse)
    print(f"\n[timing] discovery done at {time.time() - t0:.0f}s — {len(tasks)} files queued for download+parse")

    print("\n" + "=" * 80)
    print(f"PHASE 1b — downloading + parsing {len(tasks)} files ({MAX_WORKERS} concurrent workers)")
    print("=" * 80)
    legacy_frames = []
    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(parse_legacy_file, url, loc, mat): url for url, loc, mat in tasks}
        for fut in as_completed(futures):
            done += 1
            if done % 25 == 0 or done == len(tasks):
                print(f"  {done}/{len(tasks)} files processed ({time.time() - t0:.0f}s elapsed)")
            try:
                df = fut.result()
                if df is not None:
                    legacy_frames.append(df)
            except Exception as e:
                print(f"  [WARN] worker error on {futures[fut]}: {e}")
    print(f"\n[timing] Phase 1 fully done at {time.time() - t0:.0f}s elapsed")

    legacy_df = pd.concat(legacy_frames, ignore_index=True) if legacy_frames else pd.DataFrame()
    manifest_df = pd.DataFrame(manifest_rows)
    manifest_df.to_csv(OUT_DIR / "parsing_manifest.csv", index=False)
    legacy_df.to_csv(OUT_DIR / "poal_legacy_long.csv", index=False)

    ok_count = (manifest_df["status"] == "ok").sum() if len(manifest_df) else 0
    print(f"\nLegacy parse summary: {ok_count} ok / {len(manifest_df)} attempted")
    print(f"Status breakdown:\n{manifest_df['status'].value_counts() if len(manifest_df) else 'none'}")

    print("\n" + "=" * 80)
    print("PHASE 2 — standardized dataset")
    print("=" * 80)
    try:
        std_df = process_standardized()
        std_df.to_csv(OUT_DIR / "poal_standardized_long.csv", index=False)
    except Exception as e:
        print(f"[FAILED] standardized download/parse: {e}")
        std_df = pd.DataFrame()

    print("\n" + "=" * 80)
    print("PHASE 3 — comparison + centralization")
    print("=" * 80)
    legacy_kw = add_join_keys(legacy_df) if not legacy_df.empty else legacy_df
    std_kw = add_join_keys(std_df) if not std_df.empty else std_df

    comparison_df = build_comparison_report(legacy_kw, std_kw)
    comparison_df.to_csv(OUT_DIR / "poal_comparison_report.csv", index=False)
    if not comparison_df.empty:
        print(f"Comparison buckets: {len(comparison_df)}")
        print(comparison_df["match_status"].value_counts().to_string())
    else:
        print("No comparison buckets — one or both of legacy/standardized came back empty this run.")

    centralized_df = build_centralized(legacy_kw, std_kw, comparison_df)
    centralized_df.to_csv(OUT_DIR / "poal_centralized.csv", index=False)
    print(f"Centralized dataset: {len(centralized_df)} rows "
          f"({(centralized_df['source'] == 'legacy').sum() if not centralized_df.empty else 0} legacy, "
          f"{(centralized_df['source'] == 'standardized').sum() if not centralized_df.empty else 0} standardized)")

    print("\n" + "=" * 80)
    print("PHASE 4 — push centralized dataset to Supabase")
    print("=" * 80)
    push_error = None
    if not has_supabase:
        print("Skipped — no working Supabase credentials this run. Local CSVs in ./poal_output/ "
              "are complete; run again with SUPABASE_URL / SUPABASE_SERVICE_KEY set to push them.")
        log_status, rows_written = "success_local_only", 0
    elif centralized_df.empty:
        print("Skipped — centralized dataset is empty, nothing to push.")
        log_status, rows_written = "success_empty", 0
    else:
        try:
            written, failed, first_error = push_centralized_to_supabase(centralized_df)
            rows_written = written
            if failed == 0:
                log_status = "success"
            elif written == 0:
                log_status = "failed"
                push_error = first_error
                print(f"\n[FAILED] ALL {failed} rows failed to write to '{SUPABASE_TABLE}'. "
                      f"First error: {first_error}")
                print("This almost always means either (a) the 'id' column in poal_readings "
                      "doesn't accept the string ids this script generates (check its column "
                      "type in the Supabase table editor — it needs to be text, not int8/uuid), "
                      "or (b) there's no unique constraint on 'id' for on_conflict to target, "
                      "or (c) an RLS policy is blocking the service key. Check the exact error "
                      "text above against those three first.")
            else:
                log_status = "partial_failure"
                push_error = f"{failed} of {written + failed} rows failed; first error: {first_error}"
                print(f"\n[PARTIAL FAILURE] {written} rows written, {failed} rows failed. "
                      f"First error: {first_error}")
        except Exception as e:
            print(f"[FAILED] Supabase push: {e}")
            log_status, rows_written, push_error = "failed", 0, str(e)

    log_sync_run("poal_pipeline", pipeline_started_at, datetime.now(timezone.utc), log_status, rows_written, error=push_error)

    print("\nDone. Check ./poal_output/parsing_manifest.csv first — that tells you how much of the")
    print("legacy archive actually parsed cleanly before trusting anything downstream of it.")
    print("Then check poal_comparison_report.csv for how much of the bay is corroborated by BOTH")
    print("sources versus resting on just one — that's a real finding, not just a QA step.")
    print(f"[timing] total elapsed: {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
