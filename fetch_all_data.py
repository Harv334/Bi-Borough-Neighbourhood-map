"""
fetch_all_data.py - NW London Population Health Pipeline (single-file)
======================================================================

Produces the three JSON files consumed by index.html:
    ward_data.json   - ward-level indicators (188 wards, nested shape)
    lsoa_data.json   - LSOA-level IMD + census (33,755 LSOAs)
    pharmacies.json  - pharmacy point data (~540 rows)

Plus it re-splices the GPS and HOSP constants inside index.html.

------------------------------------------------------------------------------
MANUAL DOWNLOADS  (do this once, then rerun the script any time)
------------------------------------------------------------------------------
Drop the following files in the .cache/ folder. Everything else is fetched
from open APIs on the fly.

  .cache/onspd/ONSPD_<MONTH>_<YEAR>_UK.zip        (~250 MB, required)
      ONS Postcode Directory. Download the latest release from
      https://geoportal.statistics.gov.uk/  (search 'ONS Postcode Directory'
      and grab the 'full' zip). Re-download quarterly for freshness.

  .cache/imd2025/File_7_IoD2025_All_Ranks_...csv  (~10 MB, required)
      Index of Multiple Deprivation 2025 - File 7 (all domains).
      https://www.gov.uk/government/statistics/english-indices-of-deprivation-2025

  .cache/gp_practices/epraccur.zip                (~700 KB, required)
      NHS ODS GP practice register. Download from
      https://digital.nhs.uk/services/organisation-data-service/export-data-files/csv-downloads/gp-and-gp-practice-related-data
      (click the 'epraccur.zip' link)

  .cache/pharmacies/edispensary.csv               (~3.5 MB, required)
      NHS BSA monthly dispensing list. Download the latest month from
      https://www.nhsbsa.nhs.uk/  (the script will try to auto-discover the
      latest URL but the monthly slug changes - manual is more reliable).

  .cache/hospitals/Hospital.csv                   [optional]
      NHS.uk dataset. https://www.nhs.uk/about-us/nhs-website-datasets/
      If missing, hospitals simply won't render on the map.

No cache needed - the script hits these APIs directly (cached between runs):
  - OHID Fingertips      (health outcomes per LAD)
  - data.police.uk       (crime per borough polygon per month)
  - Nomis Census 2021    (topic-summary tables, ~150 MB first run, cached)
  - Nomis Census 2021    (topic-summary tables, ~150 MB first run, cached)

------------------------------------------------------------------------------
USAGE
------------------------------------------------------------------------------
  python fetch_all_data.py                      # run all sources + export
  python fetch_all_data.py --only imd gp        # run a subset, then export
  python fetch_all_data.py --skip crime         # skip slow sources
  python fetch_all_data.py --export-only        # skip fetches, just rebuild JSON

Dependencies:
  pip install pandas pyarrow requests pyproj shapely

------------------------------------------------------------------------------
WHY THE ATOMIC WRITES?
------------------------------------------------------------------------------
The Windows workspace mount has a disk-sync quirk where `open(...).write()`
can return before bytes reach disk, producing 2-byte truncated files.
All JSON + Parquet outputs are written via tempfile + fsync + os.replace
to defeat this. If you see empty outputs, that's the bug - never disable
the atomic wrappers.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

try:
    import pandas as pd
    import requests
except ImportError as e:
    print(f"ERROR: missing dependency ({e.name}). Run: pip install pandas pyarrow requests pyproj shapely")
    sys.exit(1)

REPO_ROOT = Path(__file__).resolve().parent
CACHE_DIR = REPO_ROOT / ".cache"
DATA_DIR  = REPO_ROOT / "data"

# ============================================================================
# SCOPE: 9 NW London boroughs (LAD25CD)
# ============================================================================
BOROUGHS = [
    ("Brent",                 "E09000005", ["HA", "NW", "W"]),
    ("Camden",                "E09000007", ["N", "NW", "WC", "W"]),
    ("Ealing",                "E09000009", ["W", "UB", "TW", "NW"]),
    ("Hammersmith & Fulham",  "E09000013", ["W", "SW"]),
    ("Harrow",                "E09000015", ["HA", "NW"]),
    ("Hillingdon",            "E09000017", ["UB", "HA", "TW"]),
    ("Hounslow",              "E09000018", ["TW", "W", "UB"]),
    ("Kensington & Chelsea",  "E09000020", ["W", "SW"]),
    ("City of Westminster",   "E09000033", ["W", "NW", "WC", "SW"]),
]
NW_LADS = {b[1] for b in BOROUGHS}
POSTCODE_AREAS = sorted({p for b in BOROUGHS for p in b[2]})

LAD_NAMES = {b[1]: b[0] for b in BOROUGHS}

# ============================================================================
# LOGGING (no rich dep — plain ANSI colours)
# ============================================================================
def _c(code: str, s: str) -> str:
    return f"\033[{code}m{s}\033[0m"

def info(msg: str) -> None:  print(_c("36", "[..]") + " " + msg)
def ok(msg: str)   -> None:  print(_c("32", "[OK]") + " " + msg)
def warn(msg: str) -> None:  print(_c("33", "[!!]") + " " + msg)
def err(msg: str)  -> None:  print(_c("31", "[ER]") + " " + msg)
def rule(msg: str) -> None:  print("\n" + _c("1;34", f"─── {msg} " + "─" * (60 - len(msg))))

# ============================================================================
# ATOMIC WRITE — defeats the workspace disk-sync bug
# ============================================================================
def write_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)

def _scrub_nan(obj):
    """Recursively replace NaN/Infinity with None so JSON is browser-parseable."""
    import math
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {k: _scrub_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_scrub_nan(v) for v in obj]
    return obj

def write_json_atomic(path: Path, data, pretty: bool = False) -> None:
    data = _scrub_nan(data)
    # allow_nan=False so we error loudly if any NaN snuck through (browser JSON.parse rejects NaN)
    if pretty:
        s = json.dumps(data, indent=2, ensure_ascii=False, sort_keys=True, allow_nan=False)
    else:
        s = json.dumps(data, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    write_atomic(path, s)

def write_parquet_atomic(path: Path, df: "pd.DataFrame") -> None:
    """Parquet + fsync + rename. pyarrow is required."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    df.to_parquet(tmp, engine="pyarrow", compression="snappy", index=False)
    # Force sync on the file before replace
    with open(tmp, "rb+") as f:
        os.fsync(f.fileno())
    os.replace(tmp, path)

# ============================================================================
# HTTP helpers — browser-like headers to bypass NHS/gov Cloudflare blocks
# ============================================================================
def browser_session(referer: str | None = None) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
                  "image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    })
    if referer:
        s.headers["Referer"] = referer
    return s

def normalise_postcode(pc: str) -> str:
    return (pc or "").replace(" ", "").upper().strip()

# ============================================================================
# ONSPD postcode lookup — built once from the zip, cached in-memory
# ============================================================================
@lru_cache(maxsize=1)
def get_postcode_lookup() -> dict:
    """Returns {postcode_no_spaces: (lat, lng, LSOA21CD, LAD25CD, WD25CD)}."""
    cache = CACHE_DIR / "onspd"
    zips = sorted(cache.glob("ONSPD_*_UK.zip"))
    if not zips:
        raise FileNotFoundError(
            f"No ONSPD zip found in {cache}.\n"
            "Download the latest ONS Postcode Directory from "
            "https://geoportal.statistics.gov.uk/ "
            "(search 'ONS Postcode Directory', grab the 'full' zip), "
            f"and drop it at {cache}."
        )
    path = zips[-1]
    info(f"ONSPD: loading from {path.name}")
    lk: dict = {}
    with zipfile.ZipFile(path) as z:
        for member in z.namelist():
            if not member.endswith(".csv") or "/multi_csv/" not in member:
                continue
            # Filename like ONSPD_FEB_2026_UK_NW.csv
            stem = Path(member).stem
            area = stem.split("_")[-1]
            if area not in POSTCODE_AREAS:
                continue
            with z.open(member) as raw:
                text = io.TextIOWrapper(raw, encoding="utf-8")
                r = csv.DictReader(text)
                for row in r:
                    if (row.get("doterm") or "").strip():
                        continue  # terminated postcode
                    try:
                        lat = float(row["lat"])
                        lng = float(row["long"])
                    except (ValueError, TypeError, KeyError):
                        continue
                    if lat == 99.999999:
                        continue  # ONSPD sentinel for 'no grid ref'
                    pcd = normalise_postcode(row.get("pcds", ""))
                    if not pcd:
                        continue
                    lk[pcd] = (
                        lat, lng,
                        row.get("lsoa21cd", ""),
                        row.get("lad25cd", ""),
                        row.get("wd25cd", ""),
                    )
    ok(f"ONSPD: loaded {len(lk):,} active postcodes "
       f"(areas: {', '.join(POSTCODE_AREAS)})")
    return lk

# ============================================================================
# Boundary / point-in-polygon helpers (lazy, only load when needed)
# ============================================================================
@lru_cache(maxsize=4)
def load_boundary_index(kind: str):
    """kind in {'lsoa', 'wards', 'boroughs'}. Returns a PolygonIndex."""
    from shapely.geometry import Point, shape
    from shapely.strtree import STRtree

    path = DATA_DIR / "boundaries" / f"{kind}.geojson"
    if not path.exists():
        raise FileNotFoundError(
            f"Boundary file not found: {path}\n"
            "Run download_boundaries() or place the GeoJSON manually."
        )
    with open(path, encoding="utf-8") as f:
        fc = json.load(f)
    feats = fc["features"]
    geoms = [shape(f["geometry"]) for f in feats]
    props = [f["properties"] for f in feats]
    tree  = STRtree(geoms)

    def find(lng: float, lat: float):
        pt = Point(lng, lat)
        for idx in tree.query(pt):
            if geoms[idx].contains(pt):
                return props[idx]
        return None

    def features_iter():
        from shapely.geometry import mapping
        return [
            {"type": "Feature", "geometry": mapping(g), "properties": p}
            for g, p in zip(geoms, props)
        ]

    # Return a lightweight object so callers have .find and .features
    class _Idx:
        pass
    idx_obj = _Idx()
    idx_obj.find = find
    idx_obj.features = features_iter()
    return idx_obj

def bng_to_wgs84(e: float, n: float) -> tuple[float, float]:
    """British National Grid easting/northing -> (lat, lng) WGS84."""
    from pyproj import Transformer
    global _BNG_TRANSFORMER
    try:
        t = _BNG_TRANSFORMER
    except NameError:
        t = Transformer.from_crs(27700, 4326, always_xy=True)
        _BNG_TRANSFORMER = t
    lng, lat = t.transform(e, n)
    return lat, lng

# ============================================================================
# SOURCE 1: GP practices  (NHS ODS EPRACCUR)
# ============================================================================
EPRACCUR_HEADER = [
    "OrganisationCode", "Name", "NationalGrouping", "HighLevelHealthGeography",
    "AddressLine1", "AddressLine2", "AddressLine3", "AddressLine4", "AddressLine5",
    "Postcode", "OpenDate", "CloseDate", "StatusCode", "OrganisationSubTypeCode",
    "Commissioner", "JoinProviderDate", "LeftProviderDate", "ContactTelephoneNumber",
    "_n1", "_n2", "_n3", "AmendedRecordIndicator", "_n4",
    "ProviderPurchaser", "_n5", "PrescribingSetting", "_n6",
]

def run_gp_practices() -> pd.DataFrame:
    rule("GP practices (NHS ODS EPRACCUR)")
    cache_dir = CACHE_DIR / "gp_practices"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache = cache_dir / "epraccur.zip"

    if not cache.exists():
        url = "https://files.digital.nhs.uk/assets/ods/current/epraccur.zip"
        info("No local cache — trying direct download (often 403s from Cloudflare)")
        sess = browser_session(referer=(
            "https://digital.nhs.uk/services/organisation-data-service/"
            "export-data-files/csv-downloads/gp-and-gp-practice-related-data"
        ))
        r = sess.get(url, timeout=60, allow_redirects=True)
        if r.status_code != 200:
            raise RuntimeError(
                f"EPRACCUR download blocked (HTTP {r.status_code}).\n"
                "Open https://digital.nhs.uk/services/organisation-data-service/"
                "export-data-files/csv-downloads/gp-and-gp-practice-related-data "
                f"in a browser, click 'epraccur.zip', and drop the file at {cache}"
            )
        cache.write_bytes(r.content)

    with zipfile.ZipFile(cache) as z:
        with z.open("epraccur.csv") as f:
            df = pd.read_csv(
                io.TextIOWrapper(f, encoding="latin-1"),
                header=None, names=EPRACCUR_HEADER,
                dtype=str, keep_default_na=False,
            )

    # Active only. Then filter to actual GP practices (not branches/clinics).
    # Handles three EPRACCUR shipping formats:
    #   Legacy:  StatusCode=A, PrescribingSetting=4 (numeric)
    #   Modern:  StatusCode=ACTIVE, SubType=B + Role codes (RO76 = GP practice)
    # B + RO76 is the canonical NHS ODS definition of a main GP practice.
    df = df[df["StatusCode"].isin(["A", "ACTIVE"])]
    setting = df["PrescribingSetting"].astype(str)
    if setting.str.fullmatch(r"\d+").any():
        df = df[df["PrescribingSetting"] == "4"]
    elif setting.str.contains("RO", na=False).any():
        df = df[
            (df["OrganisationSubTypeCode"] == "B")
            & (setting.str.contains("RO76", na=False))
        ]

    lookup = get_postcode_lookup()
    rows = []
    for _, r in df.iterrows():
        pc = normalise_postcode(r["Postcode"])
        hit = lookup.get(pc)
        if not hit:
            continue
        lat, lng, lsoa, lad, wd = hit
        if lad not in NW_LADS:
            continue
        rows.append({
            "code": r["OrganisationCode"],
            "name": (r["Name"] or "").title(),
            "addr": ", ".join(filter(None, [
                (r["AddressLine1"] or "").title(),
                (r["AddressLine2"] or "").title(),
                (r["AddressLine3"] or "").title(),
            ])),
            "postcode": r["Postcode"],
            "tel": r["ContactTelephoneNumber"],
            "lat": lat, "lng": lng,
            "LSOA21CD": lsoa, "WD25CD": wd, "LAD25CD": lad,
            "lad": LAD_NAMES.get(lad, ""),
        })
    out = pd.DataFrame(rows)
    out_path = DATA_DIR / "healthcare" / "gp_practices.parquet"
    write_parquet_atomic(out_path, out)
    ok(f"gp_practices: {len(out):,} rows -> {out_path.relative_to(REPO_ROOT)}")
    return out


# ============================================================================
# SOURCE 2: Pharmacies  (NHS BSA edispensary)
# ============================================================================
def run_pharmacies() -> pd.DataFrame:
    rule("Pharmacies (NHS BSA edispensary)")
    cache_dir = CACHE_DIR / "pharmacies"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache = cache_dir / "edispensary.csv"

    if not cache.exists():
        info("No local cache — walking back months trying to auto-discover the latest file")
        sess = browser_session(referer="https://www.nhsbsa.nhs.uk/")
        found = False
        now = datetime.utcnow()
        for delta in range(0, 6):
            year  = now.year if (now.month - delta) > 0 else now.year - 1
            month = (now.month - delta - 1) % 12 + 1
            ym = f"{year}-{month:02d}"
            url = f"https://www.nhsbsa.nhs.uk/sites/default/files/{ym}/edispensary.csv"
            r = sess.get(url, timeout=30)
            if r.status_code == 200 and len(r.content) > 1000:
                cache.write_bytes(r.content)
                found = True
                ok(f"edispensary: fetched {ym} slug")
                break
        if not found:
            raise RuntimeError(
                "Could not auto-discover an edispensary.csv. "
                "Download the latest monthly file manually from "
                "https://www.nhsbsa.nhs.uk/ and drop it at " + str(cache)
            )

    df = pd.read_csv(
        cache, header=None, names=EPRACCUR_HEADER,
        dtype=str, keep_default_na=False, encoding="latin-1",
    )
    df = df[df["StatusCode"].isin(["A", "ACTIVE"])]

    lookup = get_postcode_lookup()
    rows = []
    for _, r in df.iterrows():
        pc = normalise_postcode(r.get("Postcode", ""))
        hit = lookup.get(pc)
        if not hit:
            continue
        lat, lng, lsoa, lad, wd = hit
        if lad not in NW_LADS:
            continue
        rows.append({
            "code": r.get("OrganisationCode", ""),
            "name": (r.get("Name") or "").title(),
            "addr": ", ".join(filter(None, [
                (r.get("AddressLine1") or "").title(),
                (r.get("AddressLine2") or "").title(),
                (r.get("AddressLine4") or "").title(),
            ])),
            "postcode": r.get("Postcode", ""),
            "tel": r.get("ContactTelephoneNumber", ""),
            "lat": lat, "lng": lng,
            "LSOA21CD": lsoa, "WD25CD": wd, "LAD25CD": lad,
        })
    out = pd.DataFrame(rows)
    out_path = DATA_DIR / "healthcare" / "pharmacies.parquet"
    write_parquet_atomic(out_path, out)
    ok(f"pharmacies: {len(out):,} rows -> {out_path.relative_to(REPO_ROOT)}")
    return out

# ============================================================================
# SOURCE 3: IMD 2025  (MHCLG, LSOA-level, all 7 domains)
# ============================================================================
IMD_DEFAULT_URL = (
    "https://assets.publishing.service.gov.uk/media/"
    "691ded56d140bbbaa59a2a7d/"
    "File_7_IoD2025_All_Ranks_Scores_Deciles_Population_Denominators.csv"
)

def run_imd2025() -> pd.DataFrame:
    rule("IMD 2025 (MHCLG)")
    cache_dir = CACHE_DIR / "imd2025"
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Prefer local cache. If both a CSV (File 7, all domains) and XLSX (File 1,
    # ranks only) are present, pick the larger one.
    candidates = sorted(
        [*cache_dir.glob("*.csv"), *cache_dir.glob("*.xlsx")],
        key=lambda p: p.stat().st_size, reverse=True,
    )
    if candidates:
        src = candidates[0]
    else:
        info("No local cache — downloading File 7 CSV")
        url = os.environ.get("IMD_2025_URL", IMD_DEFAULT_URL)
        src = cache_dir / "imd2025.csv"
        r = requests.get(url, timeout=120, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        src.write_bytes(r.content)

    if src.suffix.lower() in (".xlsx", ".xls"):
        xl = pd.ExcelFile(src)
        name_match = [s for s in xl.sheet_names
                      if "imd" in s.lower() or "iod" in s.lower()]
        non_notes  = [s for s in xl.sheet_names if s.lower() not in ("notes",)]
        tab = (name_match + non_notes + xl.sheet_names)[0]
        df = pd.read_excel(src, sheet_name=tab)
    else:
        df = pd.read_csv(src)

    if df.empty:
        warn("imd2025: empty input")
        return df

    df = df.rename(columns={c: c.strip() for c in df.columns})

    def find_col(*kws):
        for c in df.columns:
            lc = c.lower()
            if all(k in lc for k in kws):
                return c
        return None

    code_col   = find_col("lsoa", "code")
    score_col  = find_col("multiple deprivation", "score")  or find_col("imd", "score")
    decile_col = find_col("multiple deprivation", "decile") or find_col("imd", "decile")
    rank_col   = find_col("multiple deprivation", "rank")   or find_col("imd", "rank")
    income_col = find_col("income",    "score")
    emp_col    = find_col("employment","score")
    edu_col    = find_col("education", "score")
    health_col = find_col("health",    "score")
    crime_col  = find_col("crime",     "score")
    barriers_col = find_col("barriers","score")
    env_col    = find_col("environment","score")

    if not code_col:
        raise RuntimeError(f"imd2025: no LSOA code column found in {src}")

    def num(col):
        if col is None:
            return pd.Series([pd.NA] * len(df))
        return pd.to_numeric(df[col], errors="coerce")

    out = pd.DataFrame({
        "LSOA21CD":       df[code_col].astype(str).str.strip(),
        "imd_score":      num(score_col),
        "imd_decile":     num(decile_col),
        "imd_rank":       num(rank_col),
        "income_score":   num(income_col),
        "employment_score": num(emp_col),
        "education_score":  num(edu_col),
        "health_score":   num(health_col),
        "crime_score":    num(crime_col),
        "barriers_score": num(barriers_col),
        "environment_score": num(env_col),
    }).dropna(subset=["LSOA21CD"])

    out_path = DATA_DIR / "demographics" / "imd2025.parquet"
    write_parquet_atomic(out_path, out)
    ok(f"imd2025: {len(out):,} LSOAs -> {out_path.relative_to(REPO_ROOT)}")
    return out


# ============================================================================
# SOURCE 3b: Census 2021  (Nomis bulk topic-summary tables, LSOA-level)
# ============================================================================
# We pull ~13 Topic Summary (TS) tables from the Nomis bulk endpoint, extract
# the LSOA-level CSV from each, and compute the per-LSOA metrics the map
# dropdowns expect (census_* keys). No manual download needed - Nomis is a
# public endpoint. First run downloads ~150 MB to .cache/census2021/.
# Column names inside each table vary, so we match by keyword substrings
# rather than exact names, which survives the periodic Nomis renames.
#
# Indicator -> table mapping:
#   census_population              TS001  (residents total)
#   census_under16_pct / over65_pct TS009 (age, 18 categories)
#   census_non_white_pct           TS021  (ethnic group)
#   census_born_outside_uk_pct     TS004  (country of birth)
#   census_good_health_pct / bad   TS037  (general health)
#   census_disability_any / lot    TS038  (disability, Equality Act)
#   census_provides_unpaid_care_pct TS039
#   census_housing_deprived_pct    TS044  (household deprivation, any dim.)
#   census_no_car_pct              TS045
#   census_owned_pct / social_rented / private_rented  TS054  (tenure)
#   census_higher_managerial_pct / routine_semi_routine_pct  TS062  (NS-SEC)
#   census_unemployed_pct          TS066
#   census_no_qual_pct / level4_qual_pct  TS067

CENSUS_TABLES = [
    "TS001", "TS004", "TS007A", "TS021", "TS037", "TS038", "TS039",
    "TS044", "TS045", "TS054", "TS062", "TS066", "TS067",
    # Phase-A expansion (2026-04): religion, language, travel, household,
    # accommodation, country-of-birth detail, year-of-arrival.
    "TS022", "TS024", "TS025", "TS041", "TS059", "TS061", "TS068",
    # Note: TS009 was previously requested but its Nomis bulk zip has no
    # LSOA-level sheet. TS007A ("Age by five-year age bands") is the
    # canonical LSOA-granularity age source.
]


def _nomis_urls(tab_id: str) -> list[str]:
    """Nomis has shipped the bulk zips under two URL patterns. Try both."""
    t = tab_id.lower()
    return [
        f"https://www.nomisweb.co.uk/output/census/2021/census2021-{t}.zip",
        f"https://www.nomisweb.co.uk/output/census/2021/{t}-2021-1.zip",
    ]


def _fetch_census_table(tab_id: str) -> pd.DataFrame | None:
    """Cache-first download + return the LSOA CSV as a DataFrame. None on failure."""
    cache_dir = CACHE_DIR / "census2021"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_zip = cache_dir / f"{tab_id.lower()}.zip"

    need_download = not cache_zip.exists() or cache_zip.stat().st_size < 2048
    if not need_download:
        try:
            with zipfile.ZipFile(cache_zip) as z:
                z.namelist()
        except zipfile.BadZipFile:
            need_download = True

    if need_download:
        sess = browser_session(referer="https://www.nomisweb.co.uk/")
        downloaded = False
        for url in _nomis_urls(tab_id):
            try:
                r = sess.get(url, timeout=180)
                if r.status_code == 200 and len(r.content) > 2048:
                    cache_zip.write_bytes(r.content)
                    info(f"  {tab_id}: downloaded {len(r.content)/1e6:.1f} MB")
                    downloaded = True
                    break
            except requests.RequestException:
                continue
        if not downloaded:
            warn(f"  {tab_id}: all URL patterns failed")
            return None

    try:
        with zipfile.ZipFile(cache_zip) as z:
            names = z.namelist()
            lsoa_name = next(
                (n for n in names if "lsoa" in n.lower() and n.endswith(".csv")),
                None,
            )
            if not lsoa_name:
                warn(f"  {tab_id}: no LSOA CSV inside zip ({names[:3]}...)")
                return None
            with z.open(lsoa_name) as f:
                return pd.read_csv(f, low_memory=False)
    except (zipfile.BadZipFile, pd.errors.EmptyDataError):
        warn(f"  {tab_id}: zip/CSV corrupt - delete .cache/census2021/{tab_id.lower()}.zip and rerun")
        return None


def _cen_code_col(df: pd.DataFrame) -> str | None:
    return next(
        (c for c in df.columns if c.strip().lower() in
         ("geography code", "lsoa code", "geographycode", "lsoa21cd", "2021 super output area - lower layer")),
        None,
    )


def _cen_find(df: pd.DataFrame, *kws, exclude=()) -> str | None:
    """First column whose lowercased name contains ALL kws and no excludes."""
    for c in df.columns:
        cl = c.lower()
        if all(k.lower() in cl for k in kws) and not any(e.lower() in cl for e in exclude):
            return c
    return None


def _cen_findall(df: pd.DataFrame, *kws, exclude=()) -> list[str]:
    """All columns whose lowercased name contains ALL kws and no excludes,
    with parent/child de-duplication.

    Nomis Census tables use ": " as a hierarchy separator, so the naive
    match for "owned" in TS054 hits BOTH the "Tenure of household: Owned"
    parent AND its "...: Owned: Owns outright" / "...: Owns with a mortgage"
    children. Summing all three double-counts the parent.

    Resolution: when a matched column is a strict descendant of another
    matched column (same prefix followed by ": "), drop the descendant.
    This keeps parent totals and returns leaves only when no parent
    matched the keywords.
    """
    raw = []
    for c in df.columns:
        cl = c.lower()
        if all(k.lower() in cl for k in kws) and not any(e.lower() in cl for e in exclude):
            raw.append(c)
    if len(raw) <= 1:
        return raw
    # Drop any column that is a strict descendant of another matched column.
    out = []
    for c in raw:
        is_descendant = False
        for other in raw:
            if other == c:
                continue
            if c.startswith(other + ": "):
                is_descendant = True
                break
        if not is_descendant:
            out.append(c)
    return out


def _cen_pct(df: pd.DataFrame, num_cols: list[str], den_col: str) -> "pd.Series":
    """Compute (sum(numerator) / denominator) * 100, as a float Series."""
    num = df[num_cols].apply(pd.to_numeric, errors="coerce").sum(axis=1)
    den = pd.to_numeric(df[den_col], errors="coerce").replace(0, pd.NA)
    return (num / den) * 100


def run_census2021() -> pd.DataFrame:
    rule("Census 2021 (Nomis bulk, LSOA)")
    tables: dict[str, pd.DataFrame] = {}
    for tab in CENSUS_TABLES:
        df = _fetch_census_table(tab)
        if df is not None:
            tables[tab] = df
            info(f"  {tab}: {len(df):,} rows x {len(df.columns)} cols")

    if "TS001" not in tables:
        warn("census2021: TS001 (population) missing - can't anchor, skipping")
        return pd.DataFrame()

    t001 = tables["TS001"]
    code_col = _cen_code_col(t001)
    if not code_col:
        warn("census2021: TS001 has no recognisable LSOA code column")
        return pd.DataFrame()

    pop_col = (_cen_find(t001, "observation")
               or _cen_find(t001, "residence type", "total")
               or _cen_find(t001, "total", "residents")
               or _cen_find(t001, "age: total")
               or _cen_find(t001, "age", "total")
               or _cen_find(t001, "total"))
    out = pd.DataFrame({
        "LSOA21CD": t001[code_col].astype(str).str.strip(),
    })
    if pop_col:
        out["census_population"] = pd.to_numeric(t001[pop_col], errors="coerce")

    # Helper: compute one metric from a table and merge onto `out` by LSOA
    def attach(key: str, tab: str, num_kw_list: list[tuple], den_kw=("total",),
               den_exclude=(), num_exclude=()):
        nonlocal out
        t = tables.get(tab)
        if t is None:
            return
        cc = _cen_code_col(t)
        if not cc:
            return
        num_cols: list[str] = []
        for kws in num_kw_list:
            for c in _cen_findall(t, *kws, exclude=num_exclude):
                if c not in num_cols:
                    num_cols.append(c)
        den_col = _cen_find(t, *den_kw, exclude=den_exclude)
        if not num_cols or not den_col:
            warn(f"  {tab}/{key}: num={len(num_cols)} den={den_col} - column match failed")
            return
        vals = _cen_pct(t, num_cols, den_col)
        df = pd.DataFrame({
            "LSOA21CD": t[cc].astype(str).str.strip(),
            key: vals.values,
        })
        out = out.merge(df, on="LSOA21CD", how="left")

    # TS007A age bands (LSOA-level) ---------------------------------------
    # Column names look like "Age (5 category broad age bands): Aged 0 to 15
    # years" etc. We try TS007A first; fall back to TS009 if the
    # downstream format ever reintroduces LSOA rows.
    _age_tab = "TS007A" if "TS007A" in tables else "TS009"
    attach("census_under16_pct", _age_tab, [
        ("aged 0 to 15",),
        ("aged 4 years and under",), ("aged 5 to 9",),
        ("aged 10 to 14",), ("aged 15 years",),
    ], num_exclude=("and over",))
    # Children under 5 (health-need indicator: child immunisation, HV caseload).
    # Only TS009's single-year bands give this directly; on TS007A the
    # youngest band is 0-15, so this stays blank there.
    attach("census_under5_pct", _age_tab, [
        ("aged 4 years and under",),
    ], num_exclude=("and over",))
    attach("census_over65_pct", _age_tab, [
        ("aged 65",), ("aged 70 to 74",), ("aged 75 to 79",),
        ("aged 80 to 84",), ("aged 85 years and over",),
    ])
    # Frail-elderly 85+ (falls, dementia, end-of-life care demand)
    attach("census_over85_pct", _age_tab, [
        ("aged 85 years and over",),
    ])
    # Derived working-age 16-64 band (computed once under16 + over65 are present).
    if "census_under16_pct" in out.columns and "census_over65_pct" in out.columns:
        wa = 100 - pd.to_numeric(out["census_under16_pct"], errors="coerce") \
                 - pd.to_numeric(out["census_over65_pct"], errors="coerce")
        out["census_working_age_pct"] = wa

    # TS004 country of birth ---------------------------------------------
    attach("census_born_outside_uk_pct", "TS004",
           [("not born in the uk",)])
    if "census_born_outside_uk_pct" not in out.columns:
        # Fallback: 1 - UK
        t = tables.get("TS004")
        if t is not None:
            cc = _cen_code_col(t)
            uk_col = _cen_find(t, "united kingdom", exclude=("not",))
            tot_col = _cen_find(t, "total")
            if cc and uk_col and tot_col:
                vals = (1 - pd.to_numeric(t[uk_col], errors="coerce")
                        / pd.to_numeric(t[tot_col], errors="coerce").replace(0, pd.NA)) * 100
                out = out.merge(pd.DataFrame({
                    "LSOA21CD": t[cc].astype(str).str.strip(),
                    "census_born_outside_uk_pct": vals.values,
                }), on="LSOA21CD", how="left")

    # TS021 ethnic group -> non-white = 1 - (white/total) -----------------
    # IMPORTANT: the "white" column must be the PARENT "Ethnic group: White"
    # total — NOT a sub-category like "Mixed ... White and Asian" (which is
    # what a naive substring match picks up first in Nomis column order).
    t = tables.get("TS021")
    if t is not None:
        cc = _cen_code_col(t)
        tot_col = _cen_find(t, "total")
        # TS021 has five top-level ethnic-group categories whose Nomis column
        # names start with "Ethnic group: <cat>" and have exactly one colon
        # (sub-categories like "Ethnic group: White: Irish" have two). We match
        # each parent by prefix and use column-count==1 to filter out leaves.
        # Parents of interest (as they appear in Nomis, verbatim):
        #   Ethnic group: White
        #   Ethnic group: Asian, Asian British or Asian Welsh
        #   Ethnic group: Black, Black British, Black Welsh, Caribbean or African
        #   Ethnic group: Mixed or Multiple ethnic groups
        #   Ethnic group: Other ethnic group
        def _parent_col(prefix_kw: str, must_include_all: tuple = ()) -> str | None:
            """Find the top-level TS021 column whose name starts with
            "Ethnic group: <prefix_kw...>" and has exactly one ":".
            must_include_all tightens the match when a prefix could be
            ambiguous (e.g. "white" vs "white and asian")."""
            pk = prefix_kw.lower()
            for c in t.columns:
                cl = c.lower()
                if not cl.startswith("ethnic group:"):
                    continue
                if c.count(":") != 1:
                    continue
                head = cl.split(":", 1)[1].strip()
                if not head.startswith(pk):
                    continue
                if must_include_all and not all(k in cl for k in must_include_all):
                    continue
                return c
            return None

        parents = {
            "census_white_pct": _parent_col("white"),
            "census_asian_pct": _parent_col("asian"),
            "census_black_pct": _parent_col("black"),
            "census_mixed_pct": _parent_col("mixed"),
            "census_other_ethnic_pct": _parent_col("other"),
        }
        if cc and tot_col:
            den = pd.to_numeric(t[tot_col], errors="coerce").replace(0, pd.NA)
            merge_df = pd.DataFrame({
                "LSOA21CD": t[cc].astype(str).str.strip(),
            })
            for key, col in parents.items():
                if not col:
                    warn(f"  TS021/{key}: parent column not found")
                    continue
                merge_df[key] = (pd.to_numeric(t[col], errors="coerce") / den) * 100
            # Derive non-white as 100 - white (kept for back-compat with the UI).
            if "census_white_pct" in merge_df.columns:
                merge_df["census_non_white_pct"] = 100 - merge_df["census_white_pct"]
            out = out.merge(merge_df, on="LSOA21CD", how="left")

    # TS037 general health -----------------------------------------------
    # "Very good health" + "Good health" for good; "Bad health" + "Very bad health" for bad.
    # The `exclude` guards stop "good health" from also catching "very good health" twice
    # (but our dedup in attach() already does that via findall + set-insert).
    attach("census_good_health_pct", "TS037",
           [("very good health",), ("good health",)],
           num_exclude=("fair",))
    attach("census_bad_health_pct", "TS037",
           [("bad health",), ("very bad health",)])

    # TS038 disability ---------------------------------------------------
    attach("census_disability_lot_pct", "TS038",
           [("limited a lot",)])
    attach("census_disability_any_pct", "TS038",
           [("limited a lot",), ("limited a little",)])

    # TS039 unpaid care --------------------------------------------------
    # Easier: pick "provides NO unpaid care" and do 1 - that/total
    t = tables.get("TS039")
    if t is not None:
        cc = _cen_code_col(t)
        none_col = _cen_find(t, "provides no unpaid care") or _cen_find(t, "no unpaid care")
        tot_col = _cen_find(t, "total")
        if cc and none_col and tot_col:
            vals = (1 - pd.to_numeric(t[none_col], errors="coerce")
                    / pd.to_numeric(t[tot_col], errors="coerce").replace(0, pd.NA)) * 100
            out = out.merge(pd.DataFrame({
                "LSOA21CD": t[cc].astype(str).str.strip(),
                "census_provides_unpaid_care_pct": vals.values,
            }), on="LSOA21CD", how="left")

    # TS044 household deprivation (any of 4 dimensions) ------------------
    t = tables.get("TS044")
    if t is not None:
        cc = _cen_code_col(t)
        none_col = _cen_find(t, "not deprived in any dimension") or _cen_find(t, "not deprived")
        tot_col = _cen_find(t, "total")
        if cc and none_col and tot_col:
            vals = (1 - pd.to_numeric(t[none_col], errors="coerce")
                    / pd.to_numeric(t[tot_col], errors="coerce").replace(0, pd.NA)) * 100
            out = out.merge(pd.DataFrame({
                "LSOA21CD": t[cc].astype(str).str.strip(),
                "census_housing_deprived_pct": vals.values,
            }), on="LSOA21CD", how="left")

    # TS045 car/van ------------------------------------------------------
    attach("census_no_car_pct", "TS045",
           [("no cars or vans",)])

    # TS054 tenure -------------------------------------------------------
    # Column names: "Tenure: Owned: Owns outright"/"Owns with a mortgage"/"Shared ownership"
    #               "Social rented: ..."/"Private rented: ..."/"Lives rent free"
    attach("census_owned_pct", "TS054",
           [("owned",)],
           num_exclude=("shared",))
    attach("census_social_rented_pct", "TS054",
           [("social rented",)])
    attach("census_private_rented_pct", "TS054",
           [("private rented",)])

    # TS062 NS-SEC -------------------------------------------------------
    # L1-L3 = higher managerial/professional; L7+L8 = semi-routine + routine
    attach("census_higher_managerial_pct", "TS062",
           [("l1, l2 and l3",), ("higher managerial",)])
    attach("census_routine_semi_routine_pct", "TS062",
           [("l7 ",), ("l8 ",), ("routine occupations",), ("semi-routine",)])

    # TS066 economic activity -------------------------------------------
    attach("census_unemployed_pct", "TS066",
           [("unemployed",)],
           den_kw=("economically active", "total"), den_exclude=())
    if "census_unemployed_pct" not in out.columns:
        # Fallback: use "all categories" total
        attach("census_unemployed_pct", "TS066",
               [("unemployed",)])

    # TS067 qualifications ----------------------------------------------
    attach("census_no_qual_pct", "TS067",
           [("no qualifications",)])
    attach("census_level4_qual_pct", "TS067",
           [("level 4 qualifications",)])

    # TS022 religion (one-line top-level categories) --------------------
    # Categories: Christian / Buddhist / Hindu / Jewish / Muslim / Sikh /
    # Other religion / No religion / Not answered. Parent rows are
    # "Religion: <label>"; attach the five that matter for outreach.
    attach("census_christian_pct",  "TS022", [("christian",)],
           num_exclude=("no",))
    attach("census_muslim_pct",     "TS022", [("muslim",)])
    attach("census_hindu_pct",      "TS022", [("hindu",)])
    attach("census_jewish_pct",     "TS022", [("jewish",)])
    attach("census_no_religion_pct","TS022", [("no religion",)])

    # TS024 main language ------------------------------------------------
    # Top-level: English / Main language is not English. Sub-levels give
    # South Asian / European / African / Arabic / Chinese / other groups
    # and specific languages. We pull the "not English" umbrella and
    # specific high-volume community languages for NW London outreach.
    attach("census_english_main_pct",  "TS024",
           [("main language is english",)],
           num_exclude=("not",))
    attach("census_non_english_main_pct","TS024",
           [("main language is not english",)])
    # Individual languages use the NOMIS hierarchy "Main language is not
    # English: ... : Arabic" etc. The _cen_findall parent/child dedup
    # picks the leaf if present; if not, match by terminal keyword.
    attach("census_arabic_main_pct",   "TS024", [("arabic",)])
    attach("census_bengali_main_pct",  "TS024", [("bengali",)])
    attach("census_polish_main_pct",   "TS024", [("polish",)])
    attach("census_portuguese_main_pct","TS024", [("portuguese",)])
    attach("census_somali_main_pct",   "TS024", [("somali",)])
    attach("census_urdu_main_pct",     "TS024", [("urdu",)])

    # TS025 proficiency in English --------------------------------------
    # Categories: Main language is English / Can speak English very well /
    # ...well / ...not well / Cannot speak English. Low-proficiency sum
    # is the most actionable outreach metric.
    attach("census_english_not_well_pct",  "TS025",
           [("cannot speak english",), ("does not speak english well",),
            ("cannot speak english well",)])
    attach("census_english_main_or_well_pct","TS025",
           [("main language is english",), ("can speak english well",)],
           num_exclude=("not",))

    # TS041 household composition ---------------------------------------
    # We want: one-person households, lone-parent households with
    # dependent children, and one-person aged 66+ (proxy for "older
    # people living alone").
    attach("census_one_person_hh_pct", "TS041",
           [("one person household",)])
    attach("census_one_person_66plus_pct", "TS041",
           [("one person household", "aged 66"), ("one person household: aged 66",)])
    attach("census_lone_parent_hh_pct", "TS041",
           [("lone parent",)])

    # TS059 accommodation type ------------------------------------------
    attach("census_flat_pct",         "TS059",
           [("flat",)])
    attach("census_whole_house_pct",  "TS059",
           [("whole house or bungalow",)])

    # TS061 method of travel to work ------------------------------------
    # Active travel = walking + bicycle. Car = driving car/van +
    # passenger in car/van.
    attach("census_active_travel_pct", "TS061",
           [("on foot",), ("bicycle",)])
    attach("census_car_to_work_pct",   "TS061",
           [("driving a car",), ("passenger in a car",)])
    attach("census_public_transport_pct","TS061",
           [("underground",), ("train",), ("bus",), ("taxi",)])

    # TS068 year of arrival in UK ---------------------------------------
    # "Arrived 2011 or later" is the best proxy for recent arrivals that
    # the MSDS / inclusion-health team cares about.
    attach("census_arrived_2011plus_pct", "TS068",
           [("arrived in the uk: 2011",), ("2011 to 2020",), ("2021 onwards",)])

    # Clean up + save ----------------------------------------------------
    out = out.dropna(subset=["LSOA21CD"]).drop_duplicates(subset=["LSOA21CD"])

    # Round percentages to 2dp for smaller output
    for col in out.columns:
        if col.endswith("_pct"):
            out[col] = pd.to_numeric(out[col], errors="coerce").round(2)
    if "census_population" in out.columns:
        out["census_population"] = pd.to_numeric(out["census_population"], errors="coerce").astype("Int64")

    out_path = DATA_DIR / "demographics" / "census2021.parquet"
    write_parquet_atomic(out_path, out)
    ok(f"census2021: {len(out):,} LSOAs x {len(out.columns)-1} indicators -> {out_path.relative_to(REPO_ROOT)}")
    return out


# ============================================================================
# SOURCE 3b: NOMIS claimant count (CLA01, NM_162) - monthly LSOA labour-market
# ============================================================================
# UC + legacy JSA combined count. Latest month + 12-month change. Pulled via
# NOMIS API (not bulk zip) because the file refreshes monthly. LSOA-level.
def run_claimant_count() -> pd.DataFrame:
    rule("NOMIS claimant count (CLA01 / NM_162, LSOA monthly)")
    cache_dir = CACHE_DIR / "claimant"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache = cache_dir / "claimant_nwl_latest.csv"

    # NW London LSOAs via geography TYPE 298 (LSOA 2021), filtered by the
    # 8-borough parent area. Simpler: ask for ALL London LSOAs (TYPE 297,
    # "2021 super output areas - lower layer for London") and filter our
    # NW London set downstream. Pull latest month (MEASURES=20100 is the
    # raw count; 20200 is the claimant rate per 100 residents aged 16-64).
    if not cache.exists() or cache.stat().st_size < 1024:
        # geography code 2013265921...TYPE298 returns *all* England LSOAs;
        # that's oversized but the LSOA join at merge time handles it.
        url = (
            "https://www.nomisweb.co.uk/api/v01/dataset/NM_162_1.data.csv"
            "?geography=TYPE298"
            "&date=latestMINUS0,latestMINUS12"
            "&gender=0&age=0"
            "&measures=20100,20200"
        )
        try:
            r = requests.get(url, timeout=180)
            r.raise_for_status()
            cache.write_bytes(r.content)
            info(f"  downloaded {len(r.content)/1e6:.1f} MB")
        except Exception as e:
            warn(f"claimant count: fetch failed ({e})")
            return pd.DataFrame()

    try:
        df = pd.read_csv(cache, dtype=str, low_memory=False)
    except pd.errors.EmptyDataError:
        warn("claimant count: cached CSV empty")
        return pd.DataFrame()

    # Column names we need (NOMIS API naming is stable):
    # GEOGRAPHY_CODE, DATE (e.g. "2026-03"), MEASURES_NAME, OBS_VALUE
    need = {"GEOGRAPHY_CODE", "DATE", "MEASURES_NAME", "OBS_VALUE"}
    if not need.issubset(df.columns):
        warn(f"claimant count: unexpected columns {list(df.columns)[:6]}")
        return pd.DataFrame()

    df["OBS_VALUE"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")
    dates = sorted(df["DATE"].dropna().unique())
    if len(dates) < 1:
        warn("claimant count: no dates in response")
        return pd.DataFrame()
    latest = dates[-1]
    prev   = dates[0] if len(dates) >= 2 else latest
    info(f"  latest={latest}  prev={prev}")

    def slice_month(date: str, measure_name_contains: str) -> pd.DataFrame:
        m = df[(df["DATE"] == date) &
               df["MEASURES_NAME"].str.contains(measure_name_contains,
                                                case=False, na=False)]
        return (m.groupby("GEOGRAPHY_CODE", as_index=False)["OBS_VALUE"]
                  .first())

    cnt_latest = slice_month(latest, "value").rename(
        columns={"GEOGRAPHY_CODE": "LSOA21CD", "OBS_VALUE": "claimant_count"})
    rate_latest = slice_month(latest, "rate").rename(
        columns={"GEOGRAPHY_CODE": "LSOA21CD", "OBS_VALUE": "claimant_rate_pct"})
    cnt_prev = slice_month(prev, "value").rename(
        columns={"GEOGRAPHY_CODE": "LSOA21CD", "OBS_VALUE": "claimant_count_yearAgo"})

    out = (cnt_latest.merge(rate_latest, on="LSOA21CD", how="outer")
                      .merge(cnt_prev,   on="LSOA21CD", how="left"))
    out["claimant_yoy_change"] = out["claimant_count"] - out["claimant_count_yearAgo"]
    out["claimant_yoy_pct"]    = (
        (out["claimant_count"] - out["claimant_count_yearAgo"]) /
        out["claimant_count_yearAgo"].replace(0, pd.NA) * 100
    ).round(1)
    out["claimant_month"] = latest
    out = out.drop(columns=["claimant_count_yearAgo"])

    # Keep only NW London LSOAs (prefix match via the LAD list used elsewhere).
    # If NWL_LSOAS is defined (injected by run_imd), filter to that; else keep all.
    nwl_codes = globals().get("_NWL_LSOA_SET")
    if isinstance(nwl_codes, set) and nwl_codes:
        out = out[out["LSOA21CD"].isin(nwl_codes)].copy()

    out_path = DATA_DIR / "economy" / "claimant_count.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(out_path, out)
    ok(f"claimant count: {len(out):,} LSOAs  month={latest}  "
       f"-> {out_path.relative_to(REPO_ROOT)}")
    return out


# ============================================================================
# SOURCE 4: OHID Fingertips  (public health outcomes per LAD)
# ============================================================================
FINGERTIPS_INDICATORS = [
    # id, short_name, description
    (90366, "life_expectancy_male",        "Life expectancy at birth (Male)"),
    (90367, "life_expectancy_female",      "Life expectancy at birth (Female)"),
    (92901, "healthy_life_expectancy_male","Healthy life expectancy at birth (Male)"),
    (92902, "healthy_life_expectancy_female","Healthy life expectancy at birth (Female)"),
    (  219, "smoking_prevalence_adults",   "Smoking prevalence in adults (18+)"),
    (90640, "obesity_adults",              "Adults overweight or obese"),
    (90323, "obesity_year6",               "Year 6: obesity (incl. severe)"),
    (92588, "physical_activity_adults",    "Physically active adults"),
    (  241, "hypertension_qof",            "Hypertension: QOF prevalence"),
    (  848, "depression_qof",              "Depression: QOF prevalence (18+)"),
    (41001, "suicide_rate",                "Suicide rate (age standardised)"),
    (90813, "severe_mental_illness_qof",   "Severe mental illness: QOF prevalence"),
    (30307, "child_poverty_low_income",    "Children in low-income families (under 16)"),
    (91142, "self_harm_admissions_10_24",  "Self-harm admissions (10-24 yrs)"),
    (30315, "a_e_attendance_under_5",      "A&E attendances (0-4 yrs)"),
    (30309, "mmr_2_doses_age5",            "MMR 2 doses at 5 yrs"),
    (91361, "flu_vaccination_65plus",      "Flu vaccination (65+)"),
    (22001, "cervical_screening_25_49",    "Cervical screening (25-49)"),
    (93701, "fuel_poverty_lihc",           "Fuel poverty (LIHC)"),
    (90282, "gp_patient_satisfaction",     "GP patient satisfaction"),
]

def run_fingertips() -> pd.DataFrame:
    rule("OHID Fingertips (public health outcomes)")
    cache_dir = CACHE_DIR / "fingertips"
    cache_dir.mkdir(parents=True, exist_ok=True)
    AREA_TYPE_LA = 502  # Upper-tier LAs (post Apr 2023)

    rows: list = []
    for ind_id, short, desc in FINGERTIPS_INDICATORS:
        cache = cache_dir / f"ind_{ind_id}.csv"
        if not cache.exists():
            url = (
                f"https://fingertips.phe.org.uk/api/all_data/csv/by_indicator_id"
                f"?indicator_ids={ind_id}"
                f"&child_area_type_id={AREA_TYPE_LA}"
                f"&parent_area_type_id=15"
            )
            try:
                r = requests.get(url, timeout=30)
                r.raise_for_status()
                cache.write_bytes(r.content)
                time.sleep(1.0)  # be polite
            except Exception as e:
                warn(f"fingertips {ind_id}: {e}")
                continue
        try:
            df = pd.read_csv(cache, dtype=str, low_memory=False)
        except pd.errors.EmptyDataError:
            continue
        if df.empty or "Area Code" not in df.columns:
            continue
        df = df[df["Area Code"].isin(NW_LADS)]
        if df.empty:
            continue
        df = df.sort_values("Time period Sortable").groupby("Area Code", as_index=False).tail(1)
        for _, row in df.iterrows():
            rows.append({
                "LAD25CD": row["Area Code"],
                "lad_name": row.get("Area Name", ""),
                "indicator_id": ind_id,
                "indicator_short": short,
                "indicator_name":  desc,
                "value":    _tofloat(row.get("Value")),
                "lower_ci": _tofloat(row.get("Lower CI 95.0 limit")),
                "upper_ci": _tofloat(row.get("Upper CI 95.0 limit")),
                "period":   row.get("Time period", ""),
                "sex":      row.get("Sex", ""),
                "age":      row.get("Age", ""),
            })

    out = pd.DataFrame(rows)
    out_path = DATA_DIR / "outcomes" / "fingertips.parquet"
    write_parquet_atomic(out_path, out)
    ok(f"fingertips: {len(out):,} rows -> {out_path.relative_to(REPO_ROOT)}")
    return out

def _tofloat(v):
    try: return float(v)
    except (TypeError, ValueError): return None


# ============================================================================
# SOURCE 4b: DESNZ sub-regional fuel poverty 2023 (LSOA-level, LILEE)
# ============================================================================
# DESNZ publishes "sub-regional fuel poverty" statistics annually — latest
# dataset (2023 data, published Feb 2025) gives % of households in fuel
# poverty by LSOA under the Low Income Low Energy Efficiency (LILEE)
# definition. That's the canonical small-area cold-homes / excess winter
# deaths proxy. Source page:
#   https://www.gov.uk/government/collections/fuel-poverty-sub-regional-statistics
#
# Point the fetcher at the XLSX of "Table 3 (LSOA)"; column we want is
# "Proportion of households fuel poor (%)".
# URLs rotate on each release, so default may 404 — drop the file manually
# in .cache/fuel_poverty/ and the fetcher will pick it up.
FUEL_POVERTY_DEFAULT_URL = os.environ.get(
    "FUEL_POVERTY_URL",
    # 2023 data (pub. Feb 2025). If 404, grab the latest XLSX from
    # https://www.gov.uk/government/collections/fuel-poverty-sub-regional-statistics
    # and save as .cache/fuel_poverty/fuel_poverty_lsoa.xlsx.
    "https://assets.publishing.service.gov.uk/media/"
    "67a5a52fd0346e3cb63419c7/"
    "sub-regional-fuel-poverty-2025-tables.xlsx",
)


def run_fuel_poverty() -> pd.DataFrame | None:
    rule("Fuel poverty (DESNZ sub-regional, LSOA)")
    cache_dir = CACHE_DIR / "fuel_poverty"
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Prefer any XLSX the user has dropped in; else try the default URL.
    candidates = sorted(cache_dir.glob("*.xlsx"),
                        key=lambda p: p.stat().st_size, reverse=True)
    if not candidates:
        url = FUEL_POVERTY_DEFAULT_URL
        src = cache_dir / "fuel_poverty_lsoa.xlsx"
        info(f"No local cache — downloading {url}")
        try:
            r = browser_session(referer="https://www.gov.uk/").get(url, timeout=120)
            r.raise_for_status()
            src.write_bytes(r.content)
            candidates = [src]
        except Exception as e:
            warn(f"fuel_poverty download failed: {e}. "
                 f"Drop the DESNZ sub-regional XLSX (LSOA tab) in {cache_dir}/ "
                 "and re-run `python fetch_all_data.py --only fuel_poverty`.")
            return None

    src = candidates[0]
    info(f"fuel_poverty: reading {src.name}")
    xl = pd.ExcelFile(src)

    # Find the LSOA sheet (header rows vary year to year — DESNZ tabs are
    # usually called "Table 3", "LSOA", or "Table 3 LSOA").
    lsoa_sheets = [s for s in xl.sheet_names
                   if "lsoa" in s.lower() or s.lower().startswith("table 3")]
    if not lsoa_sheets:
        warn(f"fuel_poverty: no LSOA-looking sheet in {xl.sheet_names}")
        return None

    # Try each header row until we find the LSOA21CD column.
    df = None
    for sheet in lsoa_sheets:
        for hdr in range(0, 5):
            try:
                tmp = pd.read_excel(src, sheet_name=sheet, header=hdr,
                                    dtype=str)
            except Exception:
                continue
            norm = {c.strip().lower(): c for c in tmp.columns if isinstance(c, str)}
            if any("lsoa" in k and "code" in k for k in norm):
                df = tmp
                break
        if df is not None:
            break
    if df is None:
        warn("fuel_poverty: could not locate LSOA code column")
        return None

    def find_col(*kws, exclude=()):
        for c in df.columns:
            if not isinstance(c, str): continue
            lc = c.lower()
            if (all(k in lc for k in kws)
                    and not any(e in lc for e in exclude)):
                return c
        return None

    code_col = find_col("lsoa", "code")
    # "Proportion of households fuel poor (%)" — or "% of households fuel poor"
    pct_col = (find_col("proportion", "fuel", "poor")
               or find_col("%", "fuel", "poor")
               or find_col("percentage", "fuel", "poor"))
    if not code_col or not pct_col:
        warn(f"fuel_poverty: columns not found (code={code_col!r}, "
             f"pct={pct_col!r}). Columns seen: {list(df.columns)[:10]}")
        return None

    out = pd.DataFrame({
        "LSOA21CD": df[code_col].astype(str).str.strip(),
        "fuel_poverty_pct": pd.to_numeric(df[pct_col], errors="coerce"),
    }).dropna(subset=["LSOA21CD"])
    # Many DESNZ releases use LSOA 2011 codes (E01xxxxxx) which still align
    # with most 2021 boundaries — keep as-is; mismatches will just be skipped
    # in build_lsoa_data.
    out_path = DATA_DIR / "demographics" / "fuel_poverty.parquet"
    write_parquet_atomic(out_path, out)
    ok(f"fuel_poverty: {len(out):,} LSOAs -> "
       f"{out_path.relative_to(REPO_ROOT)}")
    return out


# ============================================================================
# SOURCE 4c: GLA LSOA Atlas — PTAI score (LSOA-level)
# ============================================================================
# PTAL (Public Transport Accessibility Level) is TfL's 0-6b banded score of
# how well-connected a location is by public transport. The underlying
# continuous score (PTAI) is published at LSOA level in the GLA's
# "LSOA Atlas". Source:
#   https://data.london.gov.uk/dataset/lsoa-atlas
#
# The LSOA Atlas CSV contains a column "Average PTAI score" per LSOA. Bigger
# is better (6b ~= 25+, 0 ~= <0.01).
PTAL_DEFAULT_URL = os.environ.get(
    "PTAL_URL",
    # LSOA Atlas CSV on London Datastore. If this 404s, grab the CSV from
    # https://data.london.gov.uk/dataset/lsoa-atlas and drop in
    # .cache/ptal/lsoa_atlas.csv.
    "https://data.london.gov.uk/download/lsoa-atlas/"
    "00f1a8c6-9a8e-4d90-a48e-7b2d2b4ab15b/lsoa-data.csv",
)


def run_ptal() -> pd.DataFrame | None:
    rule("PTAL (GLA LSOA Atlas, average PTAI)")
    cache_dir = CACHE_DIR / "ptal"
    cache_dir.mkdir(parents=True, exist_ok=True)

    candidates = sorted(cache_dir.glob("*.csv"),
                        key=lambda p: p.stat().st_size, reverse=True)
    if not candidates:
        url = PTAL_DEFAULT_URL
        src = cache_dir / "lsoa_atlas.csv"
        info(f"No local cache — downloading {url}")
        try:
            r = browser_session(referer="https://data.london.gov.uk/").get(
                url, timeout=120)
            r.raise_for_status()
            src.write_bytes(r.content)
            candidates = [src]
        except Exception as e:
            warn(f"PTAL download failed: {e}. "
                 f"Download the LSOA Atlas CSV from "
                 "https://data.london.gov.uk/dataset/lsoa-atlas and save as "
                 f"{cache_dir}/lsoa_atlas.csv, then re-run `python "
                 "fetch_all_data.py --only ptal`.")
            return None

    src = candidates[0]
    info(f"ptal: reading {src.name}")
    # GLA atlas ships with two header rows (category, variable). Read with a
    # simple single-row header and pick the PTAI column by substring.
    df = None
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            df = pd.read_csv(src, dtype=str, encoding=enc, low_memory=False)
            break
        except UnicodeDecodeError:
            continue
    if df is None or df.empty:
        warn("ptal: empty or unreadable CSV")
        return None

    def find_col(*kws):
        for c in df.columns:
            if not isinstance(c, str): continue
            lc = c.lower()
            if all(k in lc for k in kws):
                return c
        return None

    code_col = (find_col("lower super output area")
                or find_col("lsoa", "code")
                or find_col("codes"))
    ptai_col = (find_col("average", "ptai")
                or find_col("ptai", "score")
                or find_col("ptai"))
    if not code_col or not ptai_col:
        warn(f"ptal: columns not found (code={code_col!r}, "
             f"ptai={ptai_col!r}). Columns seen: {list(df.columns)[:10]}")
        return None

    out = pd.DataFrame({
        "LSOA21CD": df[code_col].astype(str).str.strip(),
        "ptai_score": pd.to_numeric(df[ptai_col], errors="coerce"),
    }).dropna(subset=["LSOA21CD", "ptai_score"])
    # Filter to well-formed E01 LSOA codes.
    out = out[out["LSOA21CD"].str.startswith("E01")]
    out_path = DATA_DIR / "demographics" / "ptal.parquet"
    write_parquet_atomic(out_path, out)
    ok(f"ptal: {len(out):,} LSOAs -> {out_path.relative_to(REPO_ROOT)}")
    return out


# ============================================================================
# SOURCE 5: Police.uk street crime  (polygon queries per borough per month)
# ============================================================================
def run_police_crime(months_back: int = 12) -> pd.DataFrame:
    rule(f"Police.uk crime (last {months_back} months)")
    cache_dir = CACHE_DIR / "police_uk_crime"
    cache_dir.mkdir(parents=True, exist_ok=True)

    boroughs_idx = load_boundary_index("boroughs")

    # Build polygon strings (subsampled) per borough. MultiPolygons iterate
    # every sub-polygon so detached landmasses are kept (e.g. Hounslow's main
    # body is polygon index 7; coordinates[0][0] was a 4-point island).
    polys = []
    for feat in boroughs_idx.features:
        p = feat["properties"]
        lad = p.get("LAD25CD") or p.get("LAD24CD") or p.get("code") or ""
        name = p.get("LAD25NM") or p.get("name") or ""
        if lad not in NW_LADS:
            continue
        geom = feat["geometry"]
        if geom["type"] == "MultiPolygon":
            rings = [poly[0] for poly in geom["coordinates"]]
        else:
            rings = [geom["coordinates"][0]]
        # Boundaries are BNG — convert, subsample (URL len cap)
        for idx, ring in enumerate(rings):
            pts = [bng_to_wgs84(pt[0], pt[1]) for pt in ring[::5]]
            if len(pts) < 3:
                continue  # degenerate, skip
            poly_str = ":".join(f"{lat:.5f},{lng:.5f}" for lat, lng in pts)
            polys.append((name, lad, idx, len(rings), poly_str))

    # 12 months lagged by 2 (publication delay)
    today = pd.Timestamp.utcnow().normalize()
    months = [(today - pd.DateOffset(months=i)).strftime("%Y-%m")
              for i in range(2, 2 + months_back)]

    all_crimes: list = []
    for name, lad, idx, total_rings, poly in polys:
        for ym in months:
            # Keep existing single-polygon cache layout; MultiPolygons add
            # a __p{idx} suffix so each sub-polygon caches independently.
            if total_rings == 1:
                cache = cache_dir / f"{lad}__{ym}.json"
            else:
                cache = cache_dir / f"{lad}__p{idx}__{ym}.json"
            # Cached files <= 3 bytes are empty responses from a bad earlier fetch;
            # retry those.
            if not cache.exists() or cache.stat().st_size <= 3:
                try:
                    r = requests.post(
                        "https://data.police.uk/api/crimes-street/all-crime",
                        data={"poly": poly, "date": ym}, timeout=60,
                    )
                    if r.status_code != 200:
                        # Keep walking, don't crash a 12-month run on one bad hit
                        continue
                    cache.write_bytes(r.content)
                    time.sleep(0.3)
                except requests.RequestException:
                    continue
            try:
                data = json.loads(cache.read_text())
            except json.JSONDecodeError:
                continue
            for c in data:
                c["_borough_name"] = name
                c["_borough_code"] = lad
                c["_month"] = ym
            all_crimes.extend(data)

    # Join to ward / LSOA via point-in-polygon
    wards_idx = load_boundary_index("wards")
    lsoa_idx  = load_boundary_index("lsoa")

    rows = []
    for c in all_crimes:
        loc = c.get("location") or {}
        try:
            lat = float(loc.get("latitude"))
            lng = float(loc.get("longitude"))
        except (TypeError, ValueError):
            continue
        wp = wards_idx.find(lng, lat) or {}
        lp = lsoa_idx.find(lng, lat) or {}
        rows.append({
            "category": c.get("category", ""),
            "lat": lat, "lng": lng,
            "month": c.get("_month", ""),
            "street_name": (loc.get("street") or {}).get("name", ""),
            "LSOA21CD": lp.get("code") or lp.get("LSOA21CD") or "",
            "WD25CD":   wp.get("WD25CD") or wp.get("WD24CD") or "",
            "LAD25CD":  c.get("_borough_code", ""),
            "borough_name": c.get("_borough_name", ""),
        })
    out = pd.DataFrame(rows)
    out_path = DATA_DIR / "crime" / "police_uk_crime.parquet"
    # Safeguard: never clobber a good existing parquet with an empty one
    # (network blocked, API down, polygon regression, etc).
    if len(out) == 0 and out_path.exists():
        try:
            existing = pd.read_parquet(out_path)
            if len(existing) > 0:
                warn(f"police_uk_crime: fetched 0 rows but {out_path.name} "
                     f"already has {len(existing):,} — keeping existing file.")
                return existing
        except Exception:
            pass
    write_parquet_atomic(out_path, out)
    ok(f"police_uk_crime: {len(out):,} crimes -> {out_path.relative_to(REPO_ROOT)}")
    return out


# ============================================================================
# SOURCE 6: Hospitals  (NHS.uk dataset - optional)
# ============================================================================
def run_hospitals() -> pd.DataFrame | None:
    rule("Hospitals (NHS.uk, optional)")
    cache_dir = CACHE_DIR / "hospitals"
    cache_dir.mkdir(parents=True, exist_ok=True)
    csvs = list(cache_dir.glob("*.csv"))
    if not csvs:
        warn("No Hospital.csv in .cache/hospitals/ — skipping. "
             "Download from https://www.nhs.uk/about-us/nhs-website-datasets/ "
             "if you want hospital markers on the map.")
        return None

    src = csvs[0]
    df = pd.read_csv(src, dtype=str, keep_default_na=False, low_memory=False)

    # NHS.uk schema varies - find columns by keyword
    def col(*kws):
        for c in df.columns:
            lc = c.lower()
            if all(k in lc for k in kws):
                return c
        return None
    name_c = col("organisationname") or col("name")
    addr_c = col("address1") or col("address")
    pc_c   = col("postcode")
    lat_c  = col("lat")
    lng_c  = col("long") or col("lng")
    type_c = col("organisationtype") or col("sector") or col("type")

    lookup = get_postcode_lookup()
    rows = []
    for _, r in df.iterrows():
        pc = normalise_postcode(r.get(pc_c, "") if pc_c else "")
        # Prefer explicit lat/lng if present; else postcode lookup
        lat = _tofloat(r.get(lat_c, "")) if lat_c else None
        lng = _tofloat(r.get(lng_c, "")) if lng_c else None
        lsoa = lad = wd = ""
        if (lat is None or lng is None) and pc:
            hit = lookup.get(pc)
            if hit:
                lat, lng, lsoa, lad, wd = hit
        if pc:
            hit2 = lookup.get(pc)
            if hit2:
                _, _, lsoa, lad, wd = hit2
        if lat is None or lng is None:
            continue
        if lad and lad not in NW_LADS:
            continue
        rows.append({
            "name": r.get(name_c, "") if name_c else "",
            "addr": r.get(addr_c, "") if addr_c else "",
            "postcode": pc,
            "lat": lat, "lng": lng,
            "type": r.get(type_c, "") if type_c else "",
            "LSOA21CD": lsoa, "WD25CD": wd, "LAD25CD": lad,
        })
    out = pd.DataFrame(rows)
    out_path = DATA_DIR / "healthcare" / "hospitals.parquet"
    write_parquet_atomic(out_path, out)
    ok(f"hospitals: {len(out):,} rows -> {out_path.relative_to(REPO_ROOT)}")
    return out


# ============================================================================
# SOURCE 7: VCSE - Charity Commission for England & Wales (bulk extract)
# ============================================================================
CCEW_URLS = {
    "charity": [
        "https://ccewuksprdoneregsadata1.blob.core.windows.net/data/json/publicextract.charity.zip",
    ],
    "classification": [
        "https://ccewuksprdoneregsadata1.blob.core.windows.net/data/json/publicextract.charity_classification.zip",
    ],
    "area_of_operation": [
        "https://ccewuksprdoneregsadata1.blob.core.windows.net/data/json/publicextract.charity_area_of_operation.zip",
    ],
}

# Classification codes confirmed against the April 2026 bulk extract:
# 17 'What' codes (101-117), 10 'How' codes (301-310), 7 'Who' codes (201-207).
CCEW_WHAT_GROUPS = {
    101: "general_purposes",
    102: "education",
    103: "health",
    104: "disability",
    105: "poverty",
    106: "overseas_aid",
    107: "housing",
    108: "religion",
    109: "arts_culture",
    110: "amateur_sport",
    111: "animals",
    112: "environment",
    113: "community_economic",
    114: "armed_forces",
    115: "human_rights",
    116: "recreation",
    117: "other_charitable",
}
CCEW_HOW_GROUPS = {
    301: "grants_individuals",
    302: "grants_organisations",
    303: "other_finance",
    304: "human_resources",
    305: "buildings_facilities",
    306: "services",
    307: "advocacy_info",
    308: "research",
    309: "umbrella_body",
    310: "other_activities",
}
CCEW_WHO_GROUPS = {
    201: "children_youth",
    202: "older_people",
    203: "disability",
    204: "ethnic_racial_origin",
    205: "other_charities",
    206: "other_defined_groups",
    207: "general_public",
}


def _ccew_zip(kind):
    cache = CACHE_DIR / "ccew"
    cache.mkdir(parents=True, exist_ok=True)
    for z in cache.glob("publicextract.charity*.zip"):
        nm = z.name.lower()
        if kind == "charity" and "classif" not in nm and "area" not in nm:
            return z
        if kind == "classification" and "classif" in nm:
            return z
        if kind == "area_of_operation" and ("area_of_operation" in nm or "area-of-operation" in nm):
            return z
    for z in cache.glob("*.zip"):
        nm = z.name.lower()
        if kind == "charity" and "charity" in nm and "classif" not in nm and "area" not in nm:
            return z
        if kind == "classification" and "classif" in nm:
            return z
        if kind == "area_of_operation" and ("area_of_operation" in nm or "area-of-operation" in nm):
            return z
    for url in CCEW_URLS.get(kind, []):
        try:
            sess = browser_session(referer="https://register-of-charities.charitycommission.gov.uk/")
            info(f"CCEW: fetching {kind} <- {url}")
            r = sess.get(url, timeout=180, stream=True)
            r.raise_for_status()
            dst = cache / Path(url).name
            with open(dst, "wb") as f:
                for chunk in r.iter_content(65536):
                    if chunk:
                        f.write(chunk)
            if dst.stat().st_size > 100_000:
                ok(f"CCEW: saved {dst.name} ({dst.stat().st_size/1e6:.1f} MB)")
                return dst
        except Exception as e:
            warn(f"CCEW auto-download failed for {kind}: {type(e).__name__}: {e}")
    return None


def _ccew_read_json(zpath):
    """Read the single JSON file inside a CCEW bulk-extract zip.
    CCEW extracts have a UTF-8 BOM, so decode with utf-8-sig."""
    with zipfile.ZipFile(zpath) as z:
        members = [m for m in z.namelist() if m.lower().endswith(".json")]
        if not members:
            raise RuntimeError(f"no .json inside {zpath.name}")
        with z.open(members[0]) as raw:
            return json.load(io.TextIOWrapper(raw, encoding="utf-8-sig"))


def _ccew_income_band(inc):
    try:
        v = float(inc)
    except (TypeError, ValueError):
        return "unknown"
    if v <= 0:        return "zero"
    if v < 10_000:    return "micro"
    if v < 100_000:   return "small"
    if v < 1_000_000: return "medium"
    return "large"


# Map of lowercase AOO area names -> NWL LAD25CD. Keys cover the exact strings
# that appear in the Charity Commission area_of_operation table.
NWL_AOO_NAMES = {
    "brent":                  "E09000005",
    "camden":                 "E09000007",
    "ealing":                 "E09000009",
    "hammersmith and fulham": "E09000013",
    "hammersmith & fulham":   "E09000013",
    "harrow":                 "E09000015",
    "hillingdon":             "E09000017",
    "hounslow":                "E09000018",
    "kensington and chelsea": "E09000020",
    "kensington & chelsea":   "E09000020",
    "city of westminster":    "E09000033",
    "westminster":            "E09000033",
}
LONDON_WIDE_AOO = {"throughout london", "london", "greater london"}


def run_charities():
    """Place-based VCSE fetch.

    Filter rule: a charity is kept iff its declared area of operation covers
    one or more NWL boroughs, OR it declares London-wide operation (Greater
    London Region row). HQ postcode is used for map-pin placement only, not
    for gating. Charities HQ'd outside NWL are kept (no pin, but listed in
    the ward/LSOA panel as a service provider).
    """
    rule("VCSE (Charity Commission bulk extract)")
    main_zip  = _ccew_zip("charity")
    class_zip = _ccew_zip("classification")
    area_zip  = _ccew_zip("area_of_operation")
    if main_zip is None:
        warn("No CCEW charity extract found. Download the JSON bulk extract zips from "
             "https://register-of-charities.charitycommission.gov.uk/register/full-register-download "
             "and drop them into .cache/ccew/")
        return None

    # ---- Pass 1: read area-of-operation, compute `covers` per org number ----
    if area_zip is None:
        err("CCEW: missing area-of-operation extract — cannot filter by coverage")
        return None
    info(f"CCEW: reading area-of-operation extract {area_zip.name}")
    area_rows = _ccew_read_json(area_zip)
    info(f"CCEW: {len(area_rows):,} area-of-operation rows")

    ALL_NWL_LADS = sorted(set(NWL_AOO_NAMES.values()))
    covers_map: dict[int, set[str]] = {}
    areas_map:  dict[int, list[dict]] = {}
    scope_map:  dict[int, str] = {}   # "explicit" | "london_wide"
    for r in area_rows:
        try:
            num = int(r.get("organisation_number") or 0)
        except (TypeError, ValueError):
            continue
        if not num:
            continue
        desc  = (r.get("geographic_area_description") or "").strip()
        gtype = (r.get("geographic_area_type") or "").strip()
        if desc:
            areas_map.setdefault(num, []).append({"type": gtype, "area": desc})
        key = desc.lower()
        hit_local = NWL_AOO_NAMES.get(key)
        if hit_local:
            covers_map.setdefault(num, set()).add(hit_local)
            scope_map[num] = "explicit"
            continue
        if gtype.lower() == "region" and key in LONDON_WIDE_AOO:
            # London-wide declarations cover all 9 NWL boroughs
            covers_map.setdefault(num, set()).update(ALL_NWL_LADS)
            if scope_map.get(num) != "explicit":
                scope_map[num] = "london_wide"

    info(f"CCEW: {len(covers_map):,} charities cover at least one NWL borough "
         f"(explicit: {sum(1 for v in scope_map.values() if v == 'explicit'):,}, "
         f"london_wide only: {sum(1 for v in scope_map.values() if v == 'london_wide'):,})")

    # ---- Pass 2: read main extract, keep only charities in covers_map ----
    info(f"CCEW: reading main extract {main_zip.name}")
    main_rows = _ccew_read_json(main_zip)
    info(f"CCEW: {len(main_rows):,} charity rows in extract")

    lookup = get_postcode_lookup()
    charities: dict[int, dict] = {}
    skipped_removed = skipped_linked = skipped_no_coverage = 0
    no_geocode = hq_outside_nwl = 0
    for r in main_rows:
        status = (r.get("charity_registration_status") or "").lower()
        if status != "registered":
            skipped_removed += 1
            continue
        try:
            linked = int(r.get("linked_charity_number") or 0)
        except (TypeError, ValueError):
            linked = 0
        if linked > 0:
            skipped_linked += 1
            continue
        try:
            num = int(r.get("organisation_number") or r.get("registered_charity_number") or 0)
        except (TypeError, ValueError):
            continue
        if not num:
            continue
        if num not in covers_map:
            skipped_no_coverage += 1
            continue

        pc = normalise_postcode(r.get("charity_contact_postcode") or "")
        lat = lng = lsoa = lad = wd = None
        if pc:
            hit = lookup.get(pc)
            if hit:
                lat, lng, lsoa, lad, wd = hit
                if lad not in NW_LADS:
                    hq_outside_nwl += 1
            else:
                no_geocode += 1
        else:
            no_geocode += 1

        addr_parts = []
        for i in range(1, 6):
            v = (r.get(f"charity_contact_address{i}") or "").strip()
            if v:
                addr_parts.append(v)
        addr = ", ".join(addr_parts)
        charities[num] = {
            "num": num,
            "name": (r.get("charity_name") or "").strip(),
            "addr": addr, "postcode": pc or "",
            "lat": lat, "lng": lng,
            "LSOA21CD": lsoa, "WD25CD": wd, "LAD25CD": lad,
            "hq_in_nwl": bool(lad and lad in NW_LADS),
            "income": r.get("latest_income"),
            "income_band": _ccew_income_band(r.get("latest_income")),
            "website": (r.get("charity_contact_web") or "").strip(),
            "activities": (r.get("charity_activities") or "").strip()[:500],
            "registered": (r.get("date_of_registration") or "")[:10],
            "covers": sorted(covers_map[num]),
            "scope":  scope_map.get(num, "explicit"),
            "areas":  areas_map.get(num, []),
            "what_codes": [], "what_tags": [], "what_desc": [],
            "how_codes":  [], "how_tags":  [], "how_desc":  [],
            "who_codes":  [], "who_tags":  [], "who_desc":  [],
        }

    pinned = sum(1 for c in charities.values() if c["lat"] is not None and c["hq_in_nwl"])
    info(
        f"CCEW: kept {len(charities):,} NWL-serving charities "
        f"(skipped: {skipped_removed:,} removed, {skipped_linked:,} subsidiary, "
        f"{skipped_no_coverage:,} no NWL coverage) "
        f"— pinnable on map: {pinned:,}, HQ outside NWL: {hq_outside_nwl:,}, no geocode: {no_geocode:,}"
    )

    # ---- Pass 3: attach classification rows ----
    if class_zip is not None:
        info(f"CCEW: reading classification extract {class_zip.name}")
        cls_rows = _ccew_read_json(class_zip)
        attached = 0
        for r in cls_rows:
            try:
                num = int(r.get("organisation_number") or 0)
            except (TypeError, ValueError):
                continue
            ch = charities.get(num)
            if not ch:
                continue
            try:
                code = int(r.get("classification_code") or 0)
            except (TypeError, ValueError):
                code = 0
            ctype = (r.get("classification_type") or "").lower()
            desc  = (r.get("classification_description") or "").strip()
            if "what" in ctype:
                ch["what_codes"].append(code); ch["what_desc"].append(desc)
                tag = CCEW_WHAT_GROUPS.get(code)
                if tag and tag not in ch["what_tags"]:
                    ch["what_tags"].append(tag)
            elif "how" in ctype:
                ch["how_codes"].append(code); ch["how_desc"].append(desc)
                tag = CCEW_HOW_GROUPS.get(code)
                if tag and tag not in ch["how_tags"]:
                    ch["how_tags"].append(tag)
            elif "who" in ctype:
                ch["who_codes"].append(code); ch["who_desc"].append(desc)
                tag = CCEW_WHO_GROUPS.get(code)
                if tag and tag not in ch["who_tags"]:
                    ch["who_tags"].append(tag)
            attached += 1
        ok(f"CCEW: attached {attached:,} classification rows")

    rows = list(charities.values())
    for r in rows:
        for k in ("what_codes","what_tags","what_desc","how_codes","how_tags",
                  "how_desc","who_codes","who_tags","who_desc","areas","covers"):
            r[k] = json.dumps(r[k], ensure_ascii=False) if r.get(k) else "[]"
    out = pd.DataFrame(rows)
    out_path = DATA_DIR / "vcse" / "charities.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(out_path, out)
    ok(f"vcse: {len(out):,} rows -> {out_path.relative_to(REPO_ROOT)}")
    return out


# ============================================================================
# EXPORT: build the 3 JSON files the map consumes
# ============================================================================
def _read_parquet_opt(path: Path):
    if not path.exists():
        warn(f"missing (skipped): {path.relative_to(REPO_ROOT)}")
        return None
    return pd.read_parquet(path)

def build_ward_data() -> dict:
    """Wrapped shape expected by index.html loadData():
         {wards: {WD25CD: {name, lad, indicators:{...}}}, metadata: {...}}
    """
    wards: dict[str, dict] = {}
    sources: dict[str, str] = {}

    # Seed ward shells from the boundaries GeoJSON so every ward has name+lad
    # even if no indicator source covers it.
    wards_gj = DATA_DIR / "boundaries" / "wards.geojson"
    if wards_gj.exists():
        gj = json.loads(wards_gj.read_text(encoding="utf-8"))
        for feat in gj.get("features", []):
            p = feat.get("properties", {})
            code = p.get("WD25CD") or p.get("WD24CD")
            if not code:
                continue
            wards[code] = {
                "name": p.get("WD25NM") or p.get("WD24NM") or "",
                "lad":  p.get("LAD25NM") or p.get("LAD24NM") or "",
                "lad_code": p.get("LAD25CD") or p.get("LAD24CD") or "",
                "indicators": {},
            }

    def _get(code):
        wards.setdefault(code, {"name": "", "lad": "", "indicators": {}})
        return wards[code]

    gps = _read_parquet_opt(DATA_DIR / "healthcare" / "gp_practices.parquet")
    if gps is not None and "WD25CD" in gps.columns:
        for wd, n in gps.groupby("WD25CD").size().items():
            if wd:
                _get(wd)["indicators"]["gp_practice_count"] = int(n)
        # Named GP list per ward (for ward-profile download)
        for wd, grp in gps.groupby("WD25CD"):
            if not wd:
                continue
            _get(wd)["gp_list"] = [
                {"name": str(r.get("name", "") or ""),
                 "addr": str(r.get("addr", "") or ""),
                 "postcode": str(r.get("postcode", "") or ""),
                 "tel": str(r.get("tel", "") or "")}
                for r in grp.to_dict("records")
            ]
        sources["gp"] = "NHS Digital ODS"

    pharm = _read_parquet_opt(DATA_DIR / "healthcare" / "pharmacies.parquet")
    if pharm is not None and "WD25CD" in pharm.columns:
        for wd, n in pharm.groupby("WD25CD").size().items():
            if wd:
                _get(wd)["indicators"]["pharmacy_count"] = int(n)
        # Named pharmacy list per ward
        for wd, grp in pharm.groupby("WD25CD"):
            if not wd:
                continue
            _get(wd)["pharmacy_list"] = [
                {"name": str(r.get("name", "") or ""),
                 "addr": str(r.get("addr", "") or ""),
                 "postcode": str(r.get("postcode", "") or "")}
                for r in grp.to_dict("records")
            ]
        sources["pharmacy"] = "NHS BSA"

    crime = _read_parquet_opt(DATA_DIR / "crime" / "police_uk_crime.parquet")
    if crime is not None and "WD25CD" in crime.columns:
        for wd, n in crime.groupby("WD25CD").size().items():
            if wd:
                _get(wd)["indicators"]["crime_total"] = int(n)
        # Per-category crime breakdown (violence, theft, drugs, etc.)
        if "category" in crime.columns:
            for (wd, cat), n in crime.groupby(["WD25CD", "category"]).size().items():
                if not wd or not cat:
                    continue
                w = _get(wd)
                w.setdefault("crime_by_category", {})[str(cat)] = int(n)
        sources["crime"] = "police.uk"

    ft = _read_parquet_opt(DATA_DIR / "outcomes" / "fingertips.parquet")
    if ft is not None:
        sources["health"] = "OHID Fingertips"
        # Fingertips is per-LAD. Join on LAD25CD (stored on each ward from the
        # boundaries GeoJSON) - avoids brittle name matching.
        lad_ind: dict = {}
        for _, row in ft.iterrows():
            lad = row["LAD25CD"]
            lad_ind.setdefault(lad, {})[row["indicator_short"]] = row["value"]
        for w in wards.values():
            lc = w.get("lad_code", "")
            if lc and lc in lad_ind:
                for k, v in lad_ind[lc].items():
                    if v is not None:
                        w["indicators"][f"ft_{k}"] = v

    # --- Census 2021: per-LSOA -> per-ward via population-weighted mean ----
    cen = _read_parquet_opt(DATA_DIR / "demographics" / "census2021.parquet")
    if cen is not None and not cen.empty:
        sources["census"] = "ONS Census 2021 (Nomis)"
        # Build LSOA21CD -> WD25CD mapping using ONSPD postcodes. The modal
        # (most-common) ward across postcodes in that LSOA wins.
        lookup = get_postcode_lookup()
        from collections import Counter, defaultdict
        # get_postcode_lookup() returns {pc: (lat, lng, LSOA21CD, LAD25CD, WD25CD)}
        lsoa_to_ward_votes = defaultdict(Counter)
        for rec in lookup.values():
            if not isinstance(rec, (tuple, list)) or len(rec) < 5:
                continue
            lc = rec[2] or ""
            wd = rec[4] or ""
            if lc and wd:
                lsoa_to_ward_votes[lc][wd] += 1
        lsoa_to_ward = {lc: votes.most_common(1)[0][0]
                        for lc, votes in lsoa_to_ward_votes.items()}

        pop_by_lsoa = dict(zip(
            cen["LSOA21CD"].astype(str),
            pd.to_numeric(cen.get("census_population", pd.Series(dtype="float")),
                          errors="coerce").fillna(0),
        ))
        pct_cols = [c for c in cen.columns if c.endswith("_pct")]
        from collections import defaultdict as _dd
        # Weighted numerator/denominator: uses LSOA population when present,
        # otherwise a weight of 1.0 per LSOA (unweighted mean). This keeps the
        # fetcher producing ward-level %s even when TS001 pop is missing.
        ward_num = _dd(lambda: _dd(float))
        ward_den = _dd(lambda: _dd(float))
        ward_pop_sum = _dd(float)

        for _, row in cen.iterrows():
            lc = str(row["LSOA21CD"])
            wd = lsoa_to_ward.get(lc)
            if not wd:
                continue
            pop = pop_by_lsoa.get(lc, 0) or 0
            if pop > 0:
                ward_pop_sum[wd] += float(pop)
            weight = float(pop) if pop > 0 else 1.0
            for pc in pct_cols:
                v = row[pc]
                if pd.notna(v):
                    ward_num[wd][pc] += float(v) * weight
                    ward_den[wd][pc] += weight

        for wd, w in wards.items():
            if ward_pop_sum.get(wd, 0) > 0:
                w["indicators"]["census_population"] = int(round(ward_pop_sum[wd]))
            for pc in pct_cols:
                den = ward_den[wd].get(pc, 0)
                if den > 0:
                    w["indicators"][pc] = round(ward_num[wd][pc] / den, 2)

    # --- Fuel poverty + PTAL: per-LSOA -> per-ward (population weighted) ----
    # Reuses lsoa_to_ward + pop_by_lsoa built in the census block above. If
    # census was absent both dicts may be missing, so guard for that.
    if "lsoa_to_ward" not in locals():
        lookup = get_postcode_lookup()
        from collections import Counter as _Ctr3, defaultdict as _dd3
        _votes = _dd3(_Ctr3)
        for rec in lookup.values():
            if isinstance(rec, (tuple, list)) and len(rec) >= 5:
                lc, wd = rec[2] or "", rec[4] or ""
                if lc and wd:
                    _votes[lc][wd] += 1
        lsoa_to_ward = {lc: v.most_common(1)[0][0] for lc, v in _votes.items()}
    if "pop_by_lsoa" not in locals():
        pop_by_lsoa = {}

    def _agg_to_wards(df, value_col, ward_key):
        if df is None or df.empty or value_col not in df.columns:
            return
        num, den = {}, {}
        for _, row in df.iterrows():
            lc = str(row["LSOA21CD"])
            wd = lsoa_to_ward.get(lc)
            v = row.get(value_col)
            if not wd or pd.isna(v):
                continue
            pop = pop_by_lsoa.get(lc, 0) or 0
            weight = float(pop) if pop > 0 else 1.0
            num[wd] = num.get(wd, 0.0) + float(v) * weight
            den[wd] = den.get(wd, 0.0) + weight
        for wd, w in wards.items():
            if den.get(wd, 0) > 0:
                w["indicators"][ward_key] = round(num[wd] / den[wd], 2)

    fp = _read_parquet_opt(DATA_DIR / "demographics" / "fuel_poverty.parquet")
    _agg_to_wards(fp, "fuel_poverty_pct", "fuel_poverty_pct")
    if fp is not None:
        sources["fuel_poverty"] = "DESNZ sub-regional fuel poverty (LILEE)"

    pt = _read_parquet_opt(DATA_DIR / "demographics" / "ptal.parquet")
    _agg_to_wards(pt, "ptai_score", "ptai_score")
    if pt is not None:
        sources["ptal"] = "GLA LSOA Atlas (average PTAI score)"

    # --- Claimant count: counts SUM, rates pop-weighted MEAN ----------------
    cl = _read_parquet_opt(DATA_DIR / "economy" / "claimant_count.parquet")
    if cl is not None and not cl.empty:
        sources["claimant"] = f"NOMIS CLA01 (UC + JSA, {cl['claimant_month'].iloc[0]})"
        # raw count + YoY change → straight sum
        for raw_col, ward_key in [("claimant_count", "claimant_count"),
                                   ("claimant_yoy_change", "claimant_yoy_change")]:
            agg = {}
            for _, row in cl.iterrows():
                lc = str(row["LSOA21CD"])
                wd = lsoa_to_ward.get(lc)
                v = row.get(raw_col)
                if not wd or pd.isna(v):
                    continue
                agg[wd] = agg.get(wd, 0) + int(v)
            for wd, w in wards.items():
                if wd in agg:
                    w["indicators"][ward_key] = int(agg[wd])
        # rate / yoy pct → pop-weighted mean
        _agg_to_wards(cl, "claimant_rate_pct", "claimant_rate_pct")
        _agg_to_wards(cl, "claimant_yoy_pct",  "claimant_yoy_pct")
        # also push the month label through metadata
        try:
            wards_mo = str(cl["claimant_month"].dropna().iloc[0])
        except Exception:
            wards_mo = ""
        if wards_mo:
            sources["_claimant_month"] = wards_mo

    # --- Core20: ward is Core20 if any of its LSOAs is in IMD decile 1-2 -----
    # NHS Core20PLUS5 framework definition.
    imd = _read_parquet_opt(DATA_DIR / "demographics" / "imd2025.parquet")
    if imd is not None and "imd_decile" in imd.columns:
        # Reuse the LSOA->ward map built for census; rebuild if census was absent.
        if "lsoa_to_ward" not in locals():
            lookup = get_postcode_lookup()
            from collections import Counter as _Ctr, defaultdict as _dd2
            votes = _dd2(_Ctr)
            for rec in lookup.values():
                if isinstance(rec, (tuple, list)) and len(rec) >= 5:
                    lc, wd = rec[2] or "", rec[4] or ""
                    if lc and wd:
                        votes[lc][wd] += 1
            lsoa_to_ward = {lc: v.most_common(1)[0][0] for lc, v in votes.items()}
        core20_wards: set = set()
        n_core20_lsoas: dict = {}
        n_ward_lsoas: dict = {}
        for _, r in imd.iterrows():
            d = r.get("imd_decile")
            lc = str(r.get("LSOA21CD") or "")
            wd = lsoa_to_ward.get(lc)
            if not wd:
                continue
            n_ward_lsoas[wd] = n_ward_lsoas.get(wd, 0) + 1
            if pd.notna(d) and int(d) in (1, 2):
                core20_wards.add(wd)
                n_core20_lsoas[wd] = n_core20_lsoas.get(wd, 0) + 1
        for wd, w in wards.items():
            w["is_core20"] = wd in core20_wards
            if wd in n_ward_lsoas:
                w["indicators"]["core20_lsoa_count"] = n_core20_lsoas.get(wd, 0)
                w["indicators"]["total_lsoa_count"] = n_ward_lsoas[wd]
        sources["core20"] = "IMD2025 deciles 1-2 per LSOA"

    return {
        "wards": wards,
        "metadata": {
            "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "sources": sources,
            "claimant_period": "",
        },
    }

def build_lsoa_data() -> dict:
    out: dict[str, dict] = {}
    imd = _read_parquet_opt(DATA_DIR / "demographics" / "imd2025.parquet")
    if imd is not None:
        for _, row in imd.iterrows():
            code = row["LSOA21CD"]
            if not code:
                continue
            rec = {}
            for col in imd.columns:
                if col == "LSOA21CD":
                    continue
                v = row[col]
                rec[col] = (int(v) if col in ("imd_decile", "imd_rank") and pd.notna(v)
                            else (None if pd.isna(v) else v))
            out[code] = rec

    # Merge census 2021 LSOA-level fields onto the same dict. Only merge onto
    # LSOAs that already exist (scoped to NW London by the IMD step); don't
    # add E&W LSOAs outside our scope.
    cen = _read_parquet_opt(DATA_DIR / "demographics" / "census2021.parquet")
    if cen is not None and not cen.empty:
        for _, row in cen.iterrows():
            code = str(row["LSOA21CD"])
            if not code or code not in out:
                continue
            rec = out[code]
            for col in cen.columns:
                if col == "LSOA21CD":
                    continue
                v = row[col]
                if pd.isna(v):
                    continue
                if col == "census_population":
                    rec[col] = int(v)
                else:
                    rec[col] = float(v)

    # Fuel poverty (DESNZ LILEE) — one field per LSOA
    fp = _read_parquet_opt(DATA_DIR / "demographics" / "fuel_poverty.parquet")
    if fp is not None and not fp.empty:
        for _, row in fp.iterrows():
            code = str(row["LSOA21CD"])
            v = row.get("fuel_poverty_pct")
            if code in out and pd.notna(v):
                out[code]["fuel_poverty_pct"] = round(float(v), 2)

    # PTAL (GLA LSOA Atlas, average PTAI score)
    pt = _read_parquet_opt(DATA_DIR / "demographics" / "ptal.parquet")
    if pt is not None and not pt.empty:
        for _, row in pt.iterrows():
            code = str(row["LSOA21CD"])
            v = row.get("ptai_score")
            if code in out and pd.notna(v):
                out[code]["ptai_score"] = round(float(v), 2)
    return out

def build_vcse_json():
    df = _read_parquet_opt(DATA_DIR / "vcse" / "charities.parquet")
    if df is None:
        return []
    list_cols = ["what_codes","what_tags","what_desc","how_codes","how_tags",
                 "how_desc","who_codes","who_tags","who_desc","areas","covers"]
    for c in list_cols:
        if c in df.columns:
            df[c] = df[c].apply(lambda v: json.loads(v) if isinstance(v, str) and v else [])
    keep = ["num","name","addr","postcode","lat","lng","LAD25CD","LSOA21CD","WD25CD",
            "hq_in_nwl","covers","scope",
            "income","income_band","website","activities","registered",
            "what_tags","what_desc","how_tags","how_desc","who_tags","who_desc","areas"]
    cols = [c for c in keep if c in df.columns]
    out = df[cols].rename(columns={
        "name": "n", "addr": "a", "postcode": "pc",
        "LAD25CD": "lad", "LSOA21CD": "lsoa", "WD25CD": "ward",
        "hq_in_nwl": "hq", "covers": "cv", "scope": "sc",
        "income": "inc", "income_band": "ib",
        "website": "w", "activities": "act", "registered": "reg",
        "what_tags": "wt", "what_desc": "wd",
        "how_tags":  "ht", "how_desc":  "hd",
        "who_tags":  "ot", "who_desc":  "od",
        "areas": "ar",
    })
    return out.to_dict(orient="records")


def build_pharmacies_json() -> list:
    pharm = _read_parquet_opt(DATA_DIR / "healthcare" / "pharmacies.parquet")
    if pharm is None:
        return []
    keep = ["name", "addr", "postcode", "tel", "lat", "lng",
            "LAD25CD", "LSOA21CD", "WD25CD"]
    cols = [c for c in keep if c in pharm.columns]
    df = pharm[cols].rename(columns={
        "name": "n", "addr": "a", "postcode": "pc",
        "LAD25CD": "lad", "LSOA21CD": "lsoa", "WD25CD": "ward",
    })
    return df.to_dict(orient="records")

def splice_index_html() -> None:
    """Re-splice the GPS/HOSP constants in index.html from the Parquet stores."""
    index_path = REPO_ROOT / "index.html"
    if not index_path.exists():
        warn(f"index.html not found at {index_path} — skipping splice")
        return
    html = index_path.read_text(encoding="utf-8")

    gps = _read_parquet_opt(DATA_DIR / "healthcare" / "gp_practices.parquet")
    if gps is not None:
        cols = [c for c in ["name", "addr", "lat", "lng", "postcode", "code",
                            "ward", "lad", "tel"] if c in gps.columns]
        js = "const GPS = " + json.dumps(
            gps[cols].rename(columns={"name": "n", "addr": "a", "postcode": "pc"})
                     .to_dict(orient="records"),
            ensure_ascii=False,
        ) + ";"
        html = re.sub(r"const GPS = \[(?:\{[^\n]*\},?\s*)+\];", js, html, count=1)

    hosp = _read_parquet_opt(DATA_DIR / "healthcare" / "hospitals.parquet")
    if hosp is not None:
        cols = [c for c in ["name", "addr", "lat", "lng", "type"]
                if c in hosp.columns]
        js = "const HOSP = " + json.dumps(
            hosp[cols].rename(columns={"name": "n", "addr": "a", "type": "t"})
                      .to_dict(orient="records"),
            ensure_ascii=False,
        ) + ";"
        html = re.sub(r"const HOSP = \[(?:\{[^\n]*\},?\s*)+\];", js, html, count=1)

    write_atomic(index_path, html)
    ok("re-spliced index.html")


def export_all() -> None:
    rule("Export Leaflet JSON outputs")
    ward_data  = build_ward_data()
    lsoa_data  = build_lsoa_data()
    pharm_data = build_pharmacies_json()
    vcse_data  = build_vcse_json()

    write_json_atomic(REPO_ROOT / "ward_data.json",  ward_data)
    write_json_atomic(REPO_ROOT / "lsoa_data.json",  lsoa_data)
    write_json_atomic(REPO_ROOT / "pharmacies.json", pharm_data)
    write_json_atomic(REPO_ROOT / "vcse_data.json",  vcse_data)
    ok(f"ward_data.json:  {len(ward_data.get('wards', {})):,} wards")
    ok(f"lsoa_data.json:  {len(lsoa_data):,} LSOAs")
    ok(f"pharmacies.json: {len(pharm_data):,} rows")
    ok(f"vcse_data.json:  {len(vcse_data):,} charities")

    splice_index_html()

# ============================================================================
# MAIN
# ============================================================================
SOURCES = {
    "gp":          run_gp_practices,
    "pharmacies":  run_pharmacies,
    "imd":         run_imd2025,
    "census":      run_census2021,
    "fingertips":  run_fingertips,
    "fuel_poverty": run_fuel_poverty,
    "ptal":        run_ptal,
    "crime":       run_police_crime,
    "hospitals":   run_hospitals,
    "charities":   run_charities,
}

def main() -> int:
    p = argparse.ArgumentParser(
        description="Fetch + aggregate NW London population health data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Sources: " + ", ".join(SOURCES) + "\n"
            "Each source caches its raw files in .cache/<source>/ — safe to rerun."
        ),
    )
    p.add_argument("--only", nargs="+", choices=list(SOURCES),
                   help="Only run the named sources (default: all).")
    p.add_argument("--skip", nargs="+", choices=list(SOURCES), default=[],
                   help="Skip the named sources.")
    p.add_argument("--export-only", action="store_true",
                   help="Skip all fetches; just rebuild ward/lsoa/pharmacy JSON "
                        "from the existing parquets + re-splice index.html.")
    args = p.parse_args()

    if not args.export_only:
        to_run = list(SOURCES) if not args.only else args.only
        to_run = [s for s in to_run if s not in args.skip]
        info(f"Running sources: {', '.join(to_run)}")
        start = time.time()

        for s in to_run:
            try:
                SOURCES[s]()
            except Exception as e:
                err(f"{s} failed: {type(e).__name__}: {e}")
                # Keep going — we'd rather have partial outputs than zero.
        info(f"Fetch phase done in {time.time() - start:.1f}s")

    export_all()
    print()
    ok("All done. Refresh index.html in your browser.")
    return 0


if __name__ == "__main__":
    sys.exit(main())