"""
POAL pipeline — download DIRECTEMAR's standardized dataset, write it to the
poal_readings table. That's it.

No legacy archive scraping, no comparison logic, no fuzzy column guessing.
DIRECTEMAR's real column names are known (confirmed from a prior run) so
they're mapped directly, not searched for by keyword.

RUN
    python -m pip install requests pandas
    python poal_full_pipeline.py

OUTPUT (in ./poal_output/)
    poal_table.csv — exactly what gets pushed to Supabase
"""

import hashlib
import io
import json
import os
import re
import time
import unicodedata
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import requests

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) research-script/1.0"}

STANDARDIZED_ZIP_URL = "https://www.directemar.cl/directemar/site/docs/20260114/20260114164231/poal_estandarizado_2024.zip"

# DIRECTEMAR's real column names in the standardized file, confirmed from a
# prior run's printed column list. Mapped directly — if DIRECTEMAR ever
# renames one of these, build_table() below fails loudly with the missing
# name rather than silently guessing wrong.
RAW_COLUMNS = {
    "location": "Cuerpo.de.Agua",
    "matriz": "MATRIZ",
    "estacion": "Estación.POAL",
    "parametro": "Parámetro",
    "valor": "VALOR Corregida",
    "unidad": "UNIDAD Corregida",
    "fecha": "Fecha.de.Muestreo",
    "year": "AÑO",
}

# This is a NATIONAL file — every water body DIRECTEMAR monitors, not just
# this project's. Zapallar and Quintay are in here even though they're
# outside the bay itself — they're the clean-water comparison sites for
# the Ventanas-vs-Zapallar contrast.
KEEP_LOCATION_KEYWORDS = [
    "QUINTERO", "PUCHUNCAVI", "PUCHUNCAVÍ", "VENTANAS", "CONCON", "CONCÓN",
    "VALPARAISO", "VALPARAÍSO", "PLAYA ANCHA", "ISLA DE PASCUA",
    "ZAPALLAR", "QUINTAY",
]

# Order matters: checked top to bottom, first match wins. DIRECTEMAR's real
# MATRIZ values are fuller than a plain three-way split — e.g. 'AGUA DE MAR'
# for seawater — which would otherwise show up as a second, redundant
# option in the frontend's matrix dropdown next to 'Agua'.
MATRIZ_KEYWORDS = [
    ("SEDIMENT", "Sedimento"),
    ("BIOTA", "Biota"),
    ("AGUA", "Agua"),
]

OUT_DIR = Path("./poal_output")
OUT_DIR.mkdir(exist_ok=True)
CACHE_DIR = OUT_DIR / "cache"
CACHE_DIR.mkdir(exist_ok=True)

SUPABASE_URL = (os.environ.get("SUPABASE_URL") or "").rstrip("/")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
SUPABASE_TABLE = "poal_readings"
SUPABASE_CHUNK_SIZE = 500


# ---------------------------------------------------------------------------
# Small helpers — text normalization, Supabase I/O. Not "extra logic":
# every one of these exists because a real run failed without it.
# ---------------------------------------------------------------------------

def strip_accents(s):
    return "".join(c for c in unicodedata.normalize("NFD", str(s)) if unicodedata.category(c) != "Mn")


def norm(s):
    return strip_accents(str(s)).strip().upper()


def to_number(series):
    """DIRECTEMAR's CSV uses comma decimals ('0,021', not '0.021'). Plain
    pd.to_numeric() on that returns NaN for every value — a silent data
    hole, not an error. Convert the separator first."""
    return pd.to_numeric(series.astype(str).str.replace(",", ".", regex=False), errors="coerce")


def clean_int(v):
    """Coerce anything year-like into a plain Python int or None. Needed
    because pandas silently upcasts an int-with-nulls column to float64,
    and Postgres' integer column type rejects '2005.0' outright even
    though it's a whole number."""
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


def canonical_location(raw_location):
    if raw_location is None or (isinstance(raw_location, float) and pd.isna(raw_location)):
        return None
    v = norm(raw_location)
    for kw in KEEP_LOCATION_KEYWORDS:
        if kw in v:
            return kw
    return v


def canonical_matriz(raw_matriz):
    if raw_matriz is None or (isinstance(raw_matriz, float) and pd.isna(raw_matriz)):
        return None
    v = norm(raw_matriz)
    for kw, canonical in MATRIZ_KEYWORDS:
        if kw in v:
            return canonical
    return raw_matriz


def make_row_id(*parts):
    """Deterministic id so re-running this script updates existing rows
    (upsert) instead of duplicating them. Includes 'valor' on purpose: two
    rows sharing the same station/parameter/date but different depths or
    replicates are different measurements, not duplicates."""
    key = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:24]


def _sanitize(obj):
    """Recursively convert a rows payload into plain JSON-safe values.
    Real failure modes this has hit before, not hypothetical ones:
      - a date-formatted Excel/CSV cell reads back as a real
        datetime/Timestamp object, which json.dumps can't serialize at all.
      - plain float('nan') IS "recognized" by json — it gets emitted as the
        bare token NaN, which isn't valid JSON and PostgREST rejects it.
      - pd.NaT has an .isoformat() that returns the *string* "NaT" instead
        of raising, so a naive fallback would write the literal text "NaT"
        into a date column.
    """
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize(v) for v in obj]
    if obj is None or obj is pd.NaT:
        return None
    if isinstance(obj, float) and obj != obj:  # NaN
        return None
    if isinstance(obj, np.floating):
        return None if np.isnan(obj) else float(obj)
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if hasattr(obj, "isoformat"):
        try:
            return obj.isoformat()
        except Exception:
            return str(obj)
    try:
        if pd.isna(obj):
            return None
    except (TypeError, ValueError):
        pass
    return obj


def get_binary_cached(url):
    cache_path = CACHE_DIR / (re.sub(r"[^A-Za-z0-9]+", "_", url)[-180:] + ".cache")
    if cache_path.exists():
        return cache_path.read_bytes()
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    cache_path.write_bytes(r.content)
    return r.content


def check_credentials():
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("SUPABASE_URL / SUPABASE_SERVICE_KEY not set — will parse and write the local "
              "CSV, but will SKIP the Supabase push.")
        return False
    try:
        supabase_write("sync_runs", [{
            "source": "credentials_check", "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(), "status": "test", "rows_written": 0,
        }])
        print("Supabase credentials OK.\n")
        return True
    except Exception as e:
        print(f"Supabase credentials set but test write failed: {e}")
        return False


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
    payload = json.dumps(_sanitize(rows), default=str).encode("utf-8")
    resp = requests.post(url, headers=headers, data=payload, timeout=60)
    if resp.status_code >= 300:
        raise RuntimeError(f"Supabase write to {table} failed ({resp.status_code}): {resp.text}")


def supabase_write_chunked(table, rows, on_conflict=None):
    if not rows:
        return 0, 0, None
    written, failed, first_error = 0, 0, None
    n_chunks = (len(rows) + SUPABASE_CHUNK_SIZE - 1) // SUPABASE_CHUNK_SIZE
    for i in range(0, len(rows), SUPABASE_CHUNK_SIZE):
        chunk = rows[i:i + SUPABASE_CHUNK_SIZE]
        chunk_num = i // SUPABASE_CHUNK_SIZE + 1
        try:
            supabase_write(table, chunk, on_conflict=on_conflict)
            print(f"  chunk {chunk_num}/{n_chunks} written ({len(chunk)} rows)")
            written += len(chunk)
        except Exception as e:
            print(f"  chunk {chunk_num}/{n_chunks} FAILED ({len(chunk)} rows): {e}")
            failed += len(chunk)
            if first_error is None:
                first_error = str(e)
    return written, failed, first_error


def log_sync_run(status, rows_written, started_at, error=None):
    try:
        supabase_write("sync_runs", [{
            "source": "poal_pipeline", "started_at": started_at.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "status": status, "rows_written": rows_written, "error": error,
        }])
    except Exception as e:
        print(f"WARNING: could not write sync_runs log entry: {e}")


# ---------------------------------------------------------------------------
# The actual pipeline: download -> map columns -> filter to region -> push
# ---------------------------------------------------------------------------

def download_standardized():
    print("Downloading standardized ZIP...")
    content = get_binary_cached(STANDARDIZED_ZIP_URL)
    z = zipfile.ZipFile(io.BytesIO(content))
    csv_names = [n for n in z.namelist() if n.lower().endswith(".csv")]
    if not csv_names:
        raise ValueError("no CSV found inside the standardized ZIP")
    with z.open(csv_names[0]) as f:
        raw = pd.read_csv(f, sep=";", encoding="latin-1", low_memory=False)
    print(f"{len(raw)} rows nationwide, columns: {list(raw.columns)}")
    return raw


def build_table(raw):
    missing = [v for v in RAW_COLUMNS.values() if v not in raw.columns]
    if missing:
        raise ValueError(
            f"Standardized file is missing expected column(s): {missing}. "
            f"DIRECTEMAR may have renamed them — check the column list printed above "
            f"and update RAW_COLUMNS at the top of this script."
        )

    df = pd.DataFrame({canonical: raw[real] for canonical, real in RAW_COLUMNS.items()})

    mask = df["location"].astype(str).map(norm).apply(lambda v: any(kw in v for kw in KEEP_LOCATION_KEYWORDS))
    df = df[mask].copy()
    print(f"Filtered to this region: {len(df)} / {len(raw)} rows kept")
    if len(df) == 0:
        print("[FLAG] zero rows matched KEEP_LOCATION_KEYWORDS — inspect raw['Cuerpo.de.Agua'].unique() "
              "manually before trusting that the region genuinely has zero rows this run.")
        return df

    df["location"] = df["location"].map(canonical_location)
    df["matriz"] = df["matriz"].map(canonical_matriz)
    df["valor_numeric"] = to_number(df["valor"])
    df["year"] = pd.Series([clean_int(v) for v in df["year"]], index=df.index, dtype="object")
    df["source"] = "standardized"
    df["source_file"] = "standardized_national_zip"
    df["id"] = df.apply(lambda r: make_row_id(
        r["location"], r["matriz"], r["estacion"], r["parametro"], r["fecha"], r["valor"]), axis=1)

    return df


def push(df):
    keep_cols = ["id", "source", "location", "matriz", "estacion", "parametro",
                 "valor", "valor_numeric", "unidad", "fecha", "year", "source_file"]
    export_df = df[keep_cols].where(pd.notnull(df[keep_cols]), None)
    rows = export_df.to_dict("records")

    # Belt-and-suspenders: 'valor' is in the id hash, so a duplicate id
    # should mean genuinely identical content. Making that visible beats a
    # silent overwrite if that assumption is ever wrong.
    seen, deduped = set(), []
    for r in rows:
        if r["id"] in seen:
            continue
        seen.add(r["id"])
        deduped.append(r)
    if len(deduped) < len(rows):
        print(f"{len(rows) - len(deduped)} rows had a duplicate id within this push (kept first occurrence).")

    print(f"\nPushing {len(deduped)} rows to '{SUPABASE_TABLE}'...")
    return supabase_write_chunked(SUPABASE_TABLE, deduped, on_conflict="id")


def main():
    t0 = time.time()
    started_at = datetime.now(timezone.utc)
    has_supabase = check_credentials()

    raw = download_standardized()
    table_df = build_table(raw)
    table_df.to_csv(OUT_DIR / "poal_table.csv", index=False)

    if not has_supabase:
        print("\nNo Supabase credentials — local CSV written, nothing pushed.")
        return
    if table_df.empty:
        print("\nNothing to push — build_table() returned zero rows.")
        log_sync_run("success_empty", 0, started_at)
        return

    written, failed, first_error = push(table_df)
    if failed == 0:
        status = "success"
    elif written == 0:
        status = "failed"
        print(f"\n[FAILED] all {failed} rows failed. First error: {first_error}")
        print("Usually means the 'id' column in poal_readings isn't type text, or there's no "
              "unique constraint on it for on_conflict to target, or an RLS policy is blocking "
              "the service key.")
    else:
        status = "partial_failure"
        print(f"\n[PARTIAL] {written} written, {failed} failed. First error: {first_error}")

    log_sync_run(status, written, started_at, error=(first_error if failed else None))
    print(f"\nDone in {time.time() - t0:.0f}s.")


if __name__ == "__main__":
    main()
