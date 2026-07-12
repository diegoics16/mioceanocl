"""
POAL pipeline: standardized national dataset only.

DIRECTEMAR (Armada de Chile) confirmed by email that only the standardized
dataset ("DATA ESTANDARIZADA POAL 1993-2024") is authorized for public
display. An earlier version of this script also crawled DIRECTEMAR's legacy
per-location archive (Isla de Pascua, Quintero, Concon, Valparaiso, Playa
Ancha — ~312 individual Excel/HTML files) to cross-check the standardized
file against it. That comparison is gone now, not just filtered out at the
end — there is no reason left to scrape pages DIRECTEMAR asked not to be
shown, even if the result was only used internally.

WHAT THIS DOES
  1. Downloads the standardized national ZIP (cached to disk — re-running
     to fix a bug downstream doesn't re-hit DIRECTEMAR's server).
  2. Maps its columns onto a canonical schema (fecha/estacion/parametro/
     valor/unidad/matriz/location).
  3. Filters the NATIONAL file down to this bay + the Zapallar/Quintay
     comparison sites (KEEP_LOCATION_KEYWORDS) — without this the dataset
     is ~300k rows for water bodies nationwide, not just this project's.
  4. Canonicalizes matriz ('AGUA DE MAR' -> 'Agua', etc.) so the same real
     category doesn't show up as two options in the frontend dropdown.
  5. Pushes to Supabase with a deterministic id (source+location+matriz+
     estacion+parametro+fecha+source_file+valor hashed) so re-running
     updates existing rows via upsert instead of duplicating them.

WHAT THIS DOES NOT DO ANYMORE
  - No legacy archive scraping, no Excel/HTML per-file parsing, no
    legacy-vs-standardized comparison report. If you need that history,
    it's in prior commits — this version doesn't touch DIRECTEMAR's
    legacy listing pages at all.

RUN
    python -m pip install requests pandas
    python poal_full_pipeline.py

OUTPUT (in ./poal_output/)
    poal_standardized_long.csv — the standardized dataset, filtered to this bay
    poal_centralized.csv       — same data, with id/source columns added (what gets pushed)
"""

import hashlib
import io
import json
import os
import re
import time
import zipfile
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

import requests
import pandas as pd
import numpy as np

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) research-script/1.0"}

# ---------------------------------------------------------------------------
# Supabase — service key, upsert via on_conflict, local backup written
# BEFORE the network call so a credentials/network failure can't lose a run.
# ---------------------------------------------------------------------------
SUPABASE_URL = (os.environ.get("SUPABASE_URL") or "").rstrip("/")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
SUPABASE_TABLE = "poal_readings"
SUPABASE_CHUNK_SIZE = 500  # rows per POST — thousands of rows total; one giant payload risks a timeout


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
    # Deliberately NOT using requests' json=rows here — see _sanitize()
    # docstring for why that silently produced bad payloads (not just threw
    # exceptions) for exactly the kind of values real POAL data contains.
    # default=str is a last-resort net for anything _sanitize() didn't think of.
    payload = json.dumps(_sanitize(rows), default=str).encode("utf-8")
    resp = requests.post(url, headers=headers, data=payload, timeout=60)
    if resp.status_code >= 300:
        raise RuntimeError(f"Supabase write to {table} failed ({resp.status_code}): {resp.text}")


def supabase_write_chunked(table, rows, on_conflict=None, label=""):
    """Same as supabase_write but in batches, with progress printed.

    Returns (written, failed, first_error) — NOT just len(rows). An earlier
    version of this function returned len(rows) unconditionally, so a run
    where every single chunk failed still logged status="success" with the
    full attempted row count. That's the exact bug behind sync_runs once
    showing rows_written=83814 while poal_readings was empty: the number
    logged was always "attempted," never "actually landed."
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
    """Deterministic id for upsert on_conflict. Same inputs always produce
    the same id — re-running the pipeline updates existing rows instead of
    duplicating them. 'valor' (the actual measured value, not just
    valor_numeric) is one of the hashed parts: two rows sharing the same
    station/parameter/date but different depths or replicates are DIFFERENT
    measurements, not duplicates, and need different ids."""
    key = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:24]


OUT_DIR = Path("./poal_output")
OUT_DIR.mkdir(exist_ok=True)
SLEEP_BETWEEN_REQUESTS = 1.0  # be polite, this is a government site
CACHE_DIR = OUT_DIR / "cache"
CACHE_DIR.mkdir(exist_ok=True)

STANDARDIZED_ZIP_URL = "https://www.directemar.cl/directemar/site/docs/20260114/20260114164231/poal_estandarizado_2024.zip"

# The standardized file is NATIONAL — every water body DIRECTEMAR monitors,
# not just this project's. Needs an explicit filter or it centralizes
# ~300k irrelevant rows. Keyword match (not exact match) because the actual
# column value spelling can vary — see the printed "Guessed location
# column(s)" output on a real run, and adjust this list if DIRECTEMAR
# spells something differently than expected.
# Zapallar and Quintay are the clean-water comparison sites for the
# Ventanas-vs-Zapallar contrast — kept in even though they're outside the
# bay itself.
KEEP_LOCATION_KEYWORDS = [
    "QUINTERO", "PUCHUNCAVI", "PUCHUNCAVÍ", "VENTANAS", "CONCON", "CONCÓN",
    "VALPARAISO", "VALPARAÍSO", "PLAYA ANCHA", "ISLA DE PASCUA",
    "ZAPALLAR", "QUINTAY",
]

COLUMN_MAP = {
    "fecha": ["FECHA", "FECHA MUESTREO", "FECHA DE MUESTREO", "FECHA CAMPAÑA"],
    "estacion": ["ESTACION", "ESTACIÓN", "NOMBRE ESTACION", "NOMBRE ESTACIÓN", "PUNTO", "PUNTO MUESTREO"],
    "parametro": ["PARAMETRO", "PARÁMETRO", "VARIABLE", "ANALITO"],
    "valor": ["VALOR", "VALOR CORREGIDA", "VALOR CORREGIDO", "RESULTADO", "CONCENTRACION", "CONCENTRACIÓN"],
    "unidad": ["UNIDAD", "UNIDADES", "UNIDAD MEDIDA"],
    "latitud": ["LATITUD", "LAT"],
    "longitud": ["LONGITUD", "LONG", "LON"],
}


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
    """Binary downloads are cached to disk. Once fetched once, re-running
    the script to fix a bug downstream reads from disk instead of
    re-hitting DIRECTEMAR's server."""
    cache_path = CACHE_DIR / (re.sub(r"[^A-Za-z0-9]+", "_", url)[-180:] + ".cache")
    if cache_path.exists():
        return cache_path.read_bytes()
    content = get(url, binary=True)
    cache_path.write_bytes(content)
    return content


def map_columns(columns):
    """Map raw column labels to canonical names. Only the first raw column
    matching a given canonical name gets renamed — if a file has e.g. both
    'VALOR' and 'VALOR CORREGIDO', collapsing them into one 'valor' column
    would silently discard one of two genuinely different measurements."""
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


def process_standardized():
    """Download the national ZIP, map its columns onto the canonical schema,
    then filter it down to the bay + comparison sites."""
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
        print("  [FLAG] no matriz column auto-detected — every row will have a blank matriz "
              "(water/sediment/biota unknown). Check the column list above.")

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


YEAR_RE = re.compile(r"(19|20)\d{2}")


def extract_year(fecha_val):
    if fecha_val is None or (isinstance(fecha_val, float) and pd.isna(fecha_val)):
        return None
    m = YEAR_RE.search(str(fecha_val))
    return int(m.group(0)) if m else None


def canonical_location(raw_location):
    """The standardized file uses fuller names ('BAHIA QUINTERO', possibly
    'BAHIA DE QUINTERO' etc) than a simple bay name. Bucket onto whichever
    KEEP_LOCATION_KEYWORDS entry appears as a substring instead of an exact
    match, which fails on this every time."""
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
    """DIRECTEMAR's standardized MATRIZ column uses fuller labels than a
    simple three-way split — e.g. 'AGUA DE MAR' for seawater. That's a real
    value, not a typo, but left unmapped it becomes a second dropdown
    option instead of merging with 'Agua'. Bucket by keyword the same way
    canonical_location() does for locations."""
    if raw_matriz is None or (isinstance(raw_matriz, float) and pd.isna(raw_matriz)):
        return None
    v = norm(raw_matriz)
    for kw, canonical in MATRIZ_KEYWORDS:
        if kw in v:
            return canonical
    return raw_matriz  # unrecognized variant — keep visible, don't silently drop


def add_join_keys(df):
    """Normalize location/matriz/estacion/parametro/fecha and derive year +
    valor_numeric. The name is a holdover from when this also built join
    keys for a legacy-vs-standardized comparison; kept because 'year' and
    'valor_numeric' are still needed downstream."""
    df = df.copy()
    for col in ("location", "matriz", "estacion", "parametro"):
        if col not in df.columns:
            df[col] = None
    df["matriz"] = df["matriz"].map(canonical_matriz)
    df["location_norm"] = df["location"].map(canonical_location)
    df["estacion_norm"] = df["estacion"].map(lambda v: norm(v) if pd.notna(v) else None)
    df["parametro_norm"] = df["parametro"].map(lambda v: norm(v) if pd.notna(v) else None)
    df["year"] = df["fecha"].map(extract_year) if "fecha" in df.columns else None
    df["valor_numeric"] = pd.to_numeric(df.get("valor"), errors="coerce")
    return df


def build_centralized(std_kw):
    """Tag every standardized row with its source and a deterministic id.
    Kept as its own function (rather than inlining into main()) since
    push_centralized_to_supabase() and main()'s logging both key off its
    output shape."""
    if std_kw.empty:
        return pd.DataFrame()
    df = std_kw.copy()
    df["source"] = "standardized"
    df["id"] = df.apply(lambda r: make_row_id(
        "standardized", r.get("location"), r.get("matriz"), r.get("estacion"),
        r.get("parametro"), r.get("fecha"), r.get("source_file"), r.get("valor")), axis=1)
    return df


def _clean_int(v):
    """Coerce anything year-like (int, float, '2005', '2005.0', numpy scalar,
    NaN/None) into a plain Python int or None. Applied right before export:
    pandas can silently upcast an int-with-nulls column to float64, and
    Postgres' integer parser rejects '2005.0' outright even though it's a
    whole number."""
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
    keep_cols = ["id", "source", "location", "matriz", "estacion", "parametro", "valor",
                 "valor_numeric", "unidad", "fecha", "year", "latitud", "longitud", "source_file"]
    present_cols = [c for c in keep_cols if c in centralized_df.columns]
    export_df = centralized_df[present_cols].where(pd.notnull(centralized_df[present_cols]), None)
    if "year" in export_df.columns:
        # NOT export_df["year"].map(_clean_int) — pandas re-infers a numeric
        # dtype from the mapped output when the source column is float64,
        # silently turning the clean ints right back into floats (and None
        # back into NaN). Confirmed by testing. Explicit object dtype is
        # what actually holds.
        export_df["year"] = pd.Series(
            [_clean_int(v) for v in export_df["year"]],
            index=export_df.index, dtype="object",
        )
    rows = export_df.to_dict("records")

    # Belt-and-suspenders: 'valor' in the id hash (see make_row_id call in
    # build_centralized) should make same-id rows genuinely identical
    # content, so collapsing them is correct rather than lossy — but making
    # that visible beats a silent overwrite if that assumption is ever
    # wrong for some future data shape.
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
              f"disambiguation in build_centralized()/make_row_id() needs another look.")
    rows = deduped

    print(f"\nPushing {len(rows)} rows to Supabase table '{SUPABASE_TABLE}'...")
    written, failed, first_error = supabase_write_chunked(SUPABASE_TABLE, rows, on_conflict="id", label=SUPABASE_TABLE)
    print(f"\n[{SUPABASE_TABLE}] {written} rows actually written, {failed} rows failed"
          + (f" — first error: {first_error}" if first_error else ""))
    return written, failed, first_error


def main():
    t0 = time.time()
    pipeline_started_at = datetime.now(timezone.utc)
    has_supabase = check_credentials()

    print("=" * 80)
    print("PHASE 1 — standardized dataset (the only source DIRECTEMAR authorized for public display)")
    print("=" * 80)
    try:
        std_df = process_standardized()
        std_df.to_csv(OUT_DIR / "poal_standardized_long.csv", index=False)
    except Exception as e:
        print(f"[FAILED] standardized download/parse: {e}")
        std_df = pd.DataFrame()

    print("\n" + "=" * 80)
    print("PHASE 2 — centralization (id assignment)")
    print("=" * 80)
    std_kw = add_join_keys(std_df) if not std_df.empty else std_df
    centralized_df = build_centralized(std_kw)
    centralized_df.to_csv(OUT_DIR / "poal_centralized.csv", index=False)
    print(f"Centralized dataset: {len(centralized_df)} rows (standardized-source only)")

    print("\n" + "=" * 80)
    print("PHASE 3 — push to Supabase")
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

    print(f"\nDone. [timing] total elapsed: {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
