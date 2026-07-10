"""
SNIFA production scraper - for the weekly scheduled job.

Combines every fix confirmed working this week:
  - 5 registries, region + category filtered
  - fiscalizaciones runs LAST, not first (heaviest endpoint, was always
    hit first and hardest)
  - verified category selection, verified page-size bump (both were
    silently failing before - this version actually checks)
  - real Chrome channel + hidden webdriver flag + realistic user-agent -
    a manual test in a real browser got instant results on a query that
    failed automated; this is the direct response to that
  - facility coordinates via tLat_/tLng_ hidden inputs, only for
    facilities not already in the database (not a full re-fetch weekly)
  - local JSON backup of every module BEFORE the Supabase write attempt,
    so a credentials or network failure can't lose a paced scrape again

OPEN QUESTION THIS SCRIPT DOESN'T RESOLVE: whether the anti-detection
measures are enough WITHOUT a real visible browser window. The only
confirmed-working test was a real, visible Chrome window (a human
manually browsing). Running unattended in GitHub Actions means no
display exists at all - headless is forced there, not a choice. This
script defaults to headful (matching what's confirmed) for local/
Task-Scheduler use, and the GitHub Actions workflow explicitly runs it
headless as a genuine test of whether that's sufficient - not a known
answer. Check the first scheduled run's results before trusting this
unattended long-term; if fiscalizaciones comes back empty in a headless
CI run but fine in a local headful run, that's the answer, and the
Task Scheduler fallback (see repo README) is the one guaranteed to
match the proven-working configuration.

USAGE
    python -m pip install playwright requests
    playwright install chrome
    python snifa_scraper_production.py
"""

import csv
import json
import os
import random
import re
import time
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright
import requests

VALPARAISO_REGION = "6"
LOCAL_BACKUP_DIR = "local_backup"

MIN_DELAY_BETWEEN_SEARCHES, MAX_DELAY_BETWEEN_SEARCHES = 4.0, 7.0
DELAY_BETWEEN_MODULES = 15.0
MIN_DELAY_BETWEEN_COORD_FETCHES, MAX_DELAY_BETWEEN_COORD_FETCHES = 4.0, 7.0
MAX_OUTER_ATTEMPTS = 2

OCEAN_CATEGORIES = {
    "1": "Instalación fabril", "2": "Infraestructura Hidráulica", "3": "Saneamiento Ambiental",
    "5": "Infraestructura Portuaria", "8": "Pesca y Acuicultura", "9": "Minería",
    "10": "Energía", "18": "Monitoreo de calidad Ambiental",
}

# fiscalizaciones LAST on purpose - see module docstring
REGISTRY_MODULES = {
    "procedimientos_sancionatorios": {"url": "https://snifa.sma.gob.cl/Sancionatorio", "categoria_field": "categoria", "detail_prefix": "/Sancionatorio/Ficha/"},
    "requerimientos_de_ingreso": {"url": "https://snifa.sma.gob.cl/RequerimientoIngreso", "categoria_field": "categoria", "detail_prefix": "/RequerimientoIngreso/Ficha/"},
    "medidas_provisionales": {"url": "https://snifa.sma.gob.cl/MedidaProvisional", "categoria_field": "sltCategoria", "detail_prefix": "/MedidaProvisional/Ficha/"},
    "registro_publico_sanciones": {"url": "https://snifa.sma.gob.cl/RegistroPublico", "categoria_field": "sltCategoria", "detail_prefix": "/RegistroPublico/Ficha/", "unconfirmed": True},
    "fiscalizaciones": {"url": "https://snifa.sma.gob.cl/Fiscalizacion", "categoria_field": "categoria", "detail_prefix": "/Fiscalizacion/Ficha/"},
}

SUPABASE_URL = (os.environ.get("SUPABASE_URL") or "").rstrip("/")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
HEADLESS = os.environ.get("SNIFA_HEADFUL", "1") != "1"  # headful by default; CI forces this via env

FACILITY_ID_RE = re.compile(r"/UnidadFiscalizable/Ficha/(\d+)")
YEAR_RE = re.compile(r"(19|20)\d{2}")


# ──────────────────────────────────────────────────────────
# Supabase
# ──────────────────────────────────────────────────────────
def check_credentials():
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("SUPABASE_URL / SUPABASE_SERVICE_KEY not set.")
        raise SystemExit(1)
    print(f"Testing Supabase connection against: {SUPABASE_URL}/rest/v1/sync_runs")
    print(f"(SUPABASE_URL as received, length {len(SUPABASE_URL)}: {SUPABASE_URL!r})")
    try:
        supabase_write("sync_runs", [{
            "source": "credentials_check", "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(), "status": "test", "rows_written": 0,
        }])
        print("Credentials OK.\n")
    except Exception as e:
        print(f"Credentials set but test write failed: {e}")
        raise SystemExit(1)


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
    resp = requests.post(url, headers=headers, json=rows, timeout=60)
    if resp.status_code >= 300:
        raise RuntimeError(f"Supabase write to {table} failed ({resp.status_code}): {resp.text}")


def supabase_read(table, query):
    url = f"{SUPABASE_URL}/rest/v1/{table}?{query}"
    resp = requests.get(url, headers={"apikey": SUPABASE_SERVICE_KEY}, timeout=60)
    if resp.status_code >= 300:
        raise RuntimeError(f"Supabase read from {table} failed ({resp.status_code}): {resp.text}")
    return resp.json()


def log_sync_run(source, started_at, finished_at, status, rows_written, error=None):
    try:
        supabase_write("sync_runs", [{
            "source": source, "started_at": started_at.isoformat(), "finished_at": finished_at.isoformat(),
            "status": status, "rows_written": rows_written, "error": error,
        }])
    except Exception as e:
        print(f"WARNING: could not write sync_runs log entry: {e}")


def throttle(min_s, max_s, label="next search"):
    delay = random.uniform(min_s, max_s)
    print(f"    (pausing {delay:.1f}s before {label})")
    time.sleep(delay)


def absolutize(link):
    if not link:
        return None
    return link if link.startswith("http") else f"https://snifa.sma.gob.cl{link}"


# ──────────────────────────────────────────────────────────
# Phase 1
# ──────────────────────────────────────────────────────────
def submit_search_verified(page, categoria_field, category_value):
    for attempt in range(3):
        page.select_option("select#ddlRegion", VALPARAISO_REGION)
        page.wait_for_timeout(500)
        page.select_option(f"select#{categoria_field}", category_value)
        page.wait_for_timeout(600)
        if page.eval_on_selector(f"select#{categoria_field}", "el => el.value") != category_value:
            continue
        called = page.evaluate("() => { if (typeof buscar === 'function') { buscar(); return true; } return false; }")
        if not called:
            try:
                page.click("button:has-text('Buscar')", timeout=5000)
            except Exception:
                page.wait_for_timeout(1000)
                continue
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(1500)
        return True
    return False


def bump_page_size(page):
    try:
        sel = page.query_selector("select[id$='_length']")
        if not sel:
            return
        sel_id = sel.get_attribute("id")
        page.select_option(f"#{sel_id}", "100")
        page.wait_for_timeout(1500)
        if page.eval_on_selector(f"#{sel_id}", "el => el.value") != "100":
            print(f"    WARNING: page-size bump did not take effect")
    except Exception as e:
        print(f"    (page-size bump failed: {e})")


def find_results_table(page):
    tables = page.query_selector_all("table")
    best, best_rows = None, 0
    for t in tables:
        rows = t.query_selector_all("tbody tr")
        if len(rows) > best_rows:
            best, best_rows = t, len(rows)
    return best


def extract_page_rows(table):
    headers = [th.inner_text().strip() for th in table.query_selector_all("th")]
    rows = []
    for tr in table.query_selector_all("tbody tr"):
        cells = [td.inner_text().strip() for td in tr.query_selector_all("td")]
        links = [a.get_attribute("href") or "" for a in tr.query_selector_all("a[href]")]
        if cells:
            row = dict(zip(headers, cells))
            row["_links"] = links
            rows.append(row)
    return rows


def paginate_and_collect(page, max_pages=500):
    all_rows, page_num = [], 1
    while page_num <= max_pages:
        table = find_results_table(page)
        if not table:
            break
        all_rows.extend(extract_page_rows(table))
        next_btn = page.query_selector("a.paginate_button.next")
        if not next_btn or "disabled" in (next_btn.get_attribute("class") or ""):
            break
        next_btn.click()
        page.wait_for_timeout(2500)
        page_num += 1
    return all_rows, page_num


def try_one_category(page, config, category_value):
    page.goto(config["url"])
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(1500)
    if not submit_search_verified(page, config["categoria_field"], category_value):
        return None
    bump_page_size(page)
    return paginate_and_collect(page)


def facility_row_from(row):
    facility_id = absolutize(next((l for l in row.get("_links", []) if FACILITY_ID_RE.search(l)), None))
    if not facility_id:
        return None
    return {
        "id": facility_id,
        "razon_social": row.get("Nombre razón social") or row.get("Nombre Razón Social") or row.get("Nombre Razon Social"),
        "comuna": row.get("Comuna"),
        "categoria": row.get("Categoría"),
        "detail_url": facility_id,
        "last_scraped_at": datetime.now(timezone.utc).isoformat(),
    }


def event_row_from(row, module_key, config):
    facility_id, record_id = None, None
    for link in row.get("_links", []):
        if FACILITY_ID_RE.search(link):
            facility_id = absolutize(link)
        elif config["detail_prefix"] in link:
            record_id = absolutize(link)
    if not facility_id:
        return None
    expediente = row.get("Expediente")
    year_match = YEAR_RE.search(expediente) if expediente else None
    return {
        "facility_id": facility_id, "section": module_key, "rol_or_expediente": expediente,
        "fecha": None, "anio": int(year_match.group(0)) if year_match else None,
        "estado": row.get("Estado"),
        "raw": {k: v for k, v in row.items() if k != "_links"} | {"detail_url": record_id},
    }


def run_registry_scrape(page):
    started_at = datetime.now(timezone.utc)
    total_written = 0
    os.makedirs(LOCAL_BACKUP_DIR, exist_ok=True)

    for module_idx, (module_key, config) in enumerate(REGISTRY_MODULES.items()):
        print(f"\n=== {module_key} {'(UNCONFIRMED structure)' if config.get('unconfirmed') else ''} ===")
        if module_idx > 0:
            throttle(DELAY_BETWEEN_MODULES, DELAY_BETWEEN_MODULES, "next module")

        module_facility_rows, module_event_rows = [], []
        for cat_value, cat_name in OCEAN_CATEGORIES.items():
            throttle(MIN_DELAY_BETWEEN_SEARCHES, MAX_DELAY_BETWEEN_SEARCHES)
            result, last_error = None, None
            for attempt in range(1, MAX_OUTER_ATTEMPTS + 1):
                try:
                    result = try_one_category(page, config, cat_value)
                    if result is not None:
                        break
                except Exception as e:
                    last_error = e
                if attempt < MAX_OUTER_ATTEMPTS:
                    time.sleep(10.0 * attempt)

            if result is None:
                print(f"    {cat_name}: FAILED - left for next scheduled run")
                continue

            rows, n_pages = result
            print(f"    {cat_name}: {len(rows)} rows across {n_pages} page(s)")
            for row in rows:
                frow = facility_row_from(row)
                if frow:
                    module_facility_rows.append(frow)
                erow = event_row_from(row, module_key, config)
                if erow:
                    module_event_rows.append(erow)

        dedup_facilities = list({f["id"]: f for f in module_facility_rows}.values())

        # Local backup FIRST, unconditionally - see module docstring
        backup_path = f"{LOCAL_BACKUP_DIR}/{module_key}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}.json"
        with open(backup_path, "w", encoding="utf-8") as f:
            json.dump({"facilities": dedup_facilities, "events": module_event_rows}, f, ensure_ascii=False, indent=2)
        print(f"  (backed up to {backup_path})")

        if dedup_facilities:
            supabase_write("snifa_facilities", dedup_facilities, on_conflict="id")
        if module_event_rows:
            supabase_write("snifa_events", module_event_rows)
        total_written += len(dedup_facilities) + len(module_event_rows)
        print(f"  -> wrote {len(dedup_facilities)} facilities, {len(module_event_rows)} events")

    log_sync_run("snifa_registries", started_at, datetime.now(timezone.utc), "success", total_written)
    print(f"\nPhase 1 done. {total_written} rows written.")


# ──────────────────────────────────────────────────────────
# Phase 2
# ──────────────────────────────────────────────────────────
def extract_coordinates(page):
    lat_el = page.query_selector("input[id^='tLat_']")
    lng_el = page.query_selector("input[id^='tLng_']")
    return (lat_el.get_attribute("value") if lat_el else None,
            lng_el.get_attribute("value") if lng_el else None)


def run_coordinate_backfill(page):
    started_at = datetime.now(timezone.utc)
    try:
        missing = supabase_read("snifa_facilities", "lat=is.null&select=id,razon_social")
    except Exception as e:
        log_sync_run("snifa_coordinates", started_at, datetime.now(timezone.utc), "failed", 0, error=str(e))
        return

    print(f"\n=== Phase 2: coordinates ({len(missing)} facilities missing one) ===")
    n_found = 0
    for i, fac in enumerate(missing, 1):
        throttle(MIN_DELAY_BETWEEN_COORD_FETCHES, MAX_DELAY_BETWEEN_COORD_FETCHES)
        fid = fac["id"]
        print(f"  [{i}/{len(missing)}] {fid} - {fac.get('razon_social', '?')}")
        try:
            page.goto(fid)
            page.wait_for_load_state("networkidle")
            page.wait_for_timeout(1000)
            lat, lng = extract_coordinates(page)
            if lat and lng:
                supabase_write("snifa_facilities", [{"id": fid, "lat": lat, "lon": lng}], on_conflict="id")
                n_found += 1
                print(f"      lat={lat}, lng={lng}")
            else:
                print(f"      no coordinate on this page")
        except Exception as e:
            print(f"      FAILED: {e}")

    log_sync_run("snifa_coordinates", started_at, datetime.now(timezone.utc), "success", n_found)
    print(f"\nPhase 2 done. {n_found}/{len(missing)} new coordinates.")


# ──────────────────────────────────────────────────────────
def main():
    check_credentials()
    with sync_playwright() as p:
        launch_kwargs = {"headless": HEADLESS}
        try:
            browser = p.chromium.launch(channel="chrome", **launch_kwargs)
        except Exception as e:
            print(f"Real Chrome channel unavailable ({e}), falling back to bundled Chromium.")
            browser = p.chromium.launch(**launch_kwargs)

        context = browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"),
            viewport={"width": 1280, "height": 800},
        )
        context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        page = context.new_page()
        try:
            run_registry_scrape(page)
            run_coordinate_backfill(page)
        finally:
            browser.close()


if __name__ == "__main__":
    main()
