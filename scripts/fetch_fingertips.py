#!/usr/bin/env python3
"""Fetch Fingertips MSOA data via fingertips_py and aggregate to NWL wards.

Cleaner rewrite of the earlier API fetcher. Uses ONLY the documented
fingertips_py API surface: get_all_profiles → get_metadata_for_profile_as_dataframe
→ get_data_by_indicator_ids per LAD. Reuses the local scripts/lsoa_to_msoa.csv
lookup we built from the ONS PCD_OA21 file (no network calls for the lookup).

Methodology — LSOA-bridge population-weighted mean:
    For each indicator:
        for each ward W:
            ward_value[W] = Σ(LSOA_pop × MSOA_value) / Σ(LSOA_pop)
                over LSOAs in W, where MSOA_value is the parent MSOA's value.

USAGE (with the specific Python that has fingertips_py installed):

    "C:\\Users\\harve\\AppData\\Local\\Programs\\Python\\Python312\\python.exe" `
        scripts\\fetch_fingertips.py --dry-run

    # OR if `py -m pip install fingertips_py pandas` was run:
    py scripts\\fetch_fingertips.py --dry-run

    # Real run (patches ward_data.json):
    py scripts\\fetch_fingertips.py

    # Limit to N indicators for fast smoke test:
    py scripts\\fetch_fingertips.py --limit 5 --dry-run

    # Pick a specific profile (default: Local Health, which is the only one
    # with MSOA-level coverage):
    py scripts\\fetch_fingertips.py --profile-name "Local Health"
    py scripts\\fetch_fingertips.py --profile-id 130

OUTPUTS:
    ward_data.json                   patched in place (new ft_{id} fields)
    scripts/fingertips_metadata.json indicator names/units/polarities
    scripts/fingertips_msoa_raw.csv  raw MSOA pull (audit trail)

PREREQS:
    pip install fingertips_py pandas
    scripts/lsoa_to_msoa.csv must exist (built from ONS PCD_OA21 lookup
    — see scripts/import_fingertips_csv.py for instructions).
"""
from __future__ import annotations
import argparse
import json
import re
import sys
from pathlib import Path
from datetime import datetime

REPO = Path(__file__).resolve().parent.parent

# 8 NWL LADs (post-Camden purge). Names match what ONS uses.
NWL_LADS = {
    'E09000005': 'Brent',
    'E09000009': 'Ealing',
    'E09000013': 'Hammersmith and Fulham',
    'E09000015': 'Harrow',
    'E09000017': 'Hillingdon',
    'E09000018': 'Hounslow',
    'E09000020': 'Kensington and Chelsea',
    'E09000033': 'Westminster',
}

POLARITY_TO_WH = {
    'low is good':         True,
    'high is good':        False,
    'rag - low is good':   True,
    'rag - high is good':  False,
    'no judgement':        None,
    'not applicable':      None,
}

# Indicator names whose data we already have from BETTER sources in the
# dashboard (Census 2021, IMD 2019, ONS mid-2024 populations). Curated out
# by default to avoid dashboard clutter. Matched as case-insensitive
# substrings. Override with --keep-dupes to retain them.
DUPE_PATTERNS = [
    'resident population',                    # census_population + age splits
    'population density',                     # computable
    'index of multiple deprivation',          # imd_score
    'english indices of deprivation',         # imd_score (alt name)
    'indices of deprivation',                 # broad catch-all for imd variants
    'income deprivation',                     # income_score
    'employment deprivation',                 # employment_score
    'education deprivation',                  # education_score
    'health deprivation',                     # health_score
    'crime deprivation',                      # crime_score
    'barriers to housing',                    # barriers_score
    'living environment deprivation',         # environment_score
    'idaci',                                  # children-in-IMD subset of imd
    'idaopi',                                 # older-people-in-IMD subset
    'ethnic group',                           # census_white_pct etc.
    'country of birth',                       # census_born_outside_uk_pct
    'people aged ',                           # age %s, already have
    'persons aged ',                          # age %s, already have
    '% aged ',                                # age %s, already have
]

# Drop indicators whose most-recent published period predates this year.
# 2018 keeps almost everything Fingertips currently publishes (Local Health
# data ranges 2018-2024 typically) while culling anything genuinely ancient.
STALE_CUTOFF = 2018


def _import():
    try:
        import fingertips_py as ftp
    except ImportError as e:
        print("ERROR: fingertips_py not installed in this Python.", file=sys.stderr)
        print(f"This Python is at: {sys.executable}", file=sys.stderr)
        print("Install with:", file=sys.stderr)
        print(f"  \"{sys.executable}\" -m pip install fingertips_py pandas", file=sys.stderr)
        sys.exit(2)
    import pandas as pd
    return ftp, pd


# ──────────────────────────────────────────────────────────────────────────
# Inputs from the dashboard repo
# ──────────────────────────────────────────────────────────────────────────
def load_lsoa_msoa():
    p = REPO / 'scripts' / 'lsoa_to_msoa.csv'
    if not p.exists():
        print(f"ERROR: {p} not found.", file=sys.stderr)
        print("Build it first — see scripts/import_fingertips_csv.py docstring.", file=sys.stderr)
        sys.exit(2)
    import csv
    with open(p) as f:
        rdr = csv.DictReader(f)
        return {r['LSOA21CD']: r['MSOA21CD'] for r in rdr}


def load_lsoa_ward_and_pop():
    html = (REPO / 'index.html').read_text(encoding='utf-8')
    m = re.search(r'const LSOA_IMD\s*=\s*(\{.*?\});', html, re.DOTALL)
    if not m:
        sys.exit("LSOA_IMD not found in index.html")
    gj = json.loads(m.group(1))
    lsoa_to_ward = {}
    for f in gj.get('features', []):
        p = f.get('properties') or {}
        c, w = p.get('code'), p.get('ward_code')
        if c and w:
            lsoa_to_ward[c] = w
    ld = json.loads((REPO / 'lsoa_data.json').read_text(encoding='utf-8'))
    lsoa_pop = {}
    for code, row in ld.items():
        ind = row.get('indicators') if isinstance(row, dict) and 'indicators' in row else row
        if not isinstance(ind, dict):
            continue
        v = ind.get('imd_denominator_mid2022') or ind.get('census_population')
        if v is not None:
            try:
                lsoa_pop[code] = float(v)
            except (ValueError, TypeError):
                pass
    return lsoa_to_ward, lsoa_pop


# ──────────────────────────────────────────────────────────────────────────
# Discover Local Health profile + MSOA area type
# ──────────────────────────────────────────────────────────────────────────
def find_profile(ftp, profile_id, profile_name):
    """Resolve a profile to {'Id', 'Name'}. The DHSC fork of fingertips_py has
    moved between several response shapes for get_all_profiles() over the
    years (list-of-dicts, dict-of-id-to-dict, list-of-ids). Handle all of
    them, and fall back to a hardcoded id (Local Health = 130) if name
    lookup fails."""
    raw = ftp.get_all_profiles()

    # Normalise to a list of {'Id', 'Name'} dicts.
    profiles = []
    if isinstance(raw, dict):
        for pid, val in raw.items():
            if isinstance(val, dict):
                profiles.append({'Id': val.get('Id', pid), 'Name': val.get('Name', '')})
            else:
                profiles.append({'Id': int(pid), 'Name': str(val)})
    elif isinstance(raw, list):
        for p in raw:
            if isinstance(p, dict):
                profiles.append({'Id': p.get('Id'), 'Name': p.get('Name', '')})
            elif isinstance(p, int):
                profiles.append({'Id': p, 'Name': ''})
            else:
                profiles.append({'Id': None, 'Name': str(p)})
    else:
        print(f"  unexpected profiles shape: {type(raw)} — using profile_id only", file=sys.stderr)
        profiles = []

    if profile_id is not None:
        # Exact match by id, otherwise fabricate one (the API will accept it)
        for p in profiles:
            if p.get('Id') == profile_id:
                return p
        return {'Id': profile_id, 'Name': f'Profile {profile_id}'}

    target = (profile_name or '').lower().strip()
    for p in profiles:
        if (p.get('Name') or '').lower().strip() == target:
            return p
    # Fuzzy
    for p in profiles:
        if target and target in (p.get('Name') or '').lower():
            return p
    # Last-resort fallback: Local Health is well-known to be profile_id 130.
    if 'local health' in target:
        print("  name lookup failed, falling back to profile_id=130 (Local Health)", file=sys.stderr)
        return {'Id': 130, 'Name': 'Local Health'}
    sys.exit(f"Profile '{profile_name}' not found in {len(profiles)} profiles. "
             f"Try: --profile-id 130")


def find_msoa_area_type_id(ftp):
    """Return the area_type_id for MSOA.

    NOTE: As of late 2025, the Local Health profile (id=143) only publishes
    against legacy MSOA boundaries (id=3, "Middle Super Output Area"). Area
    type id=213 ("MSOA 2021") exists in the Fingertips area-type catalogue
    but Local Health hasn't been re-published there yet. So we deliberately
    PREFER the legacy id=3 over 213 for this profile. MSOA 2011 codes are
    >95% identical to MSOA 2021 codes for NWL boroughs (boundary changes
    are rare in inner London), so the LSOA21→MSOA21 lookup we built from
    the ONS PCD file still hits for the vast majority of LSOAs.

    Override with --msoa-area-type-id if a different version becomes
    available in future."""
    raw = None
    for fn_name in ('get_area_types_as_dict', 'get_area_types_as_dataframe', 'get_all_areas'):
        fn = getattr(ftp, fn_name, None)
        if fn is None:
            continue
        try:
            raw = fn()
            print(f"  used ftp.{fn_name}() (returned {type(raw).__name__})")
            break
        except Exception as e:
            print(f"  ftp.{fn_name}() failed: {e}")
    if raw is None:
        # Last resort — direct API call
        import requests
        r = requests.get('https://fingertips.phe.org.uk/api/area_types', timeout=30)
        r.raise_for_status()
        raw = r.json()
        print("  fell back to direct /api/area_types call")

    # Normalise to list of {Id, Name}
    rows = []
    if isinstance(raw, dict):
        for k, v in raw.items():
            if isinstance(v, dict):
                rows.append({'Id': v.get('Id', k), 'Name': v.get('Name', ''), 'ShortName': v.get('ShortName', '')})
            else:
                rows.append({'Id': int(k) if str(k).isdigit() else k, 'Name': str(v), 'ShortName': ''})
    elif isinstance(raw, list):
        for r in raw:
            if isinstance(r, dict):
                rows.append({'Id': r.get('Id'), 'Name': r.get('Name', ''), 'ShortName': r.get('ShortName', '')})
            elif hasattr(r, '__getitem__'):
                rows.append({'Id': r[0] if len(r) > 0 else None,
                             'Name': r[1] if len(r) > 1 else '',
                             'ShortName': r[2] if len(r) > 2 else ''})
    elif hasattr(raw, 'iterrows'):
        for _, r in raw.iterrows():
            rows.append({'Id': r.get('Id') or r.get('id'),
                         'Name': r.get('Name') or r.get('name') or '',
                         'ShortName': r.get('ShortName') or ''})
    else:
        sys.exit(f"Unrecognised area_types shape: {type(raw)}")

    # Print what we got, for diagnostics
    print(f"  area types: {len(rows)} entries")

    candidates = []
    for r in rows:
        name = (r.get('Name') or '').lower()
        sn   = (r.get('ShortName') or '').lower()
        if 'msoa' in name or 'msoa' in sn or 'middle' in name:
            tid = r.get('Id')
            try:
                candidates.append((int(tid), name))
            except Exception:
                pass
    if not candidates:
        print("  no MSOA match — area types found:", file=sys.stderr)
        for r in rows[:30]:
            print(f"    id={r.get('Id'):>5}  name={r.get('Name')}", file=sys.stderr)
        sys.exit("No MSOA area type found")

    # Prefer legacy MSOA (id=3) — that's where Local Health actually publishes.
    # See module docstring for why. If id=3 not in catalogue, fall back to
    # whatever has 'msoa' in the name with the lowest id (older = stabler).
    for tid, name in candidates:
        if tid == 3:
            print(f"  using MSOA area type id={tid} ({name}) [Local Health publishes here]")
            return tid
    candidates.sort()
    tid, name = candidates[0]
    print(f"  using MSOA area type id={tid} ({name}) [legacy id=3 not in catalogue]")
    return tid


# ──────────────────────────────────────────────────────────────────────────
# Fetch data
# ──────────────────────────────────────────────────────────────────────────
def fetch_indicator_metadata(ftp, profile_id):
    """Return DataFrame of indicators in the profile. fingertips_py exposes
    get_metadata_for_profile_as_dataframe; signatures vary between versions
    (profile_id kwarg / profile_ids kwarg / positional / list-required).
    Try them all."""
    fn = ftp.get_metadata_for_profile_as_dataframe
    candidates = [
        lambda: fn(profile_id),                     # positional
        lambda: fn(profile_id=profile_id),          # keyword (old)
        lambda: fn(profile_ids=profile_id),         # keyword plural (singular value)
        lambda: fn(profile_ids=[profile_id]),       # keyword plural list
        lambda: fn([profile_id]),                   # positional list
    ]
    last_err = None
    for c in candidates:
        try:
            return c()
        except (TypeError, ValueError) as e:
            last_err = e
    sys.exit(f"All call signatures for get_metadata_for_profile_as_dataframe "
             f"failed. Last error: {last_err}")


def find_lad_area_type_id(ftp):
    """Find the area_type_id for English LAD (UA + London Borough + Met Dist).
    Same defensive shape-handling as find_msoa_area_type_id. Common labels:
    'Districts and Unitary Authorities', 'Local Authority District (UA)',
    'County and UA', 'Upper-tier Local Authority'."""
    raw = None
    for fn_name in ('get_area_types_as_dict', 'get_area_types_as_dataframe', 'get_all_areas'):
        fn = getattr(ftp, fn_name, None)
        if fn is None: continue
        try:
            raw = fn()
            break
        except Exception:
            pass
    if raw is None:
        import requests
        r = requests.get('https://fingertips.phe.org.uk/api/area_types', timeout=30)
        r.raise_for_status()
        raw = r.json()
    rows = []
    if isinstance(raw, dict):
        for k, v in raw.items():
            if isinstance(v, dict):
                rows.append({'Id': v.get('Id', k), 'Name': v.get('Name', '')})
            else:
                rows.append({'Id': int(k) if str(k).isdigit() else k, 'Name': str(v)})
    elif isinstance(raw, list):
        for r in raw:
            if isinstance(r, dict):
                rows.append({'Id': r.get('Id'), 'Name': r.get('Name', '')})
    elif hasattr(raw, 'iterrows'):
        for _, r in raw.iterrows():
            rows.append({'Id': r.get('Id') or r.get('id'),
                         'Name': r.get('Name') or r.get('name') or ''})
    # Pick the most recent "Lower tier local authorities" — London boroughs
    # are LADs at lower tier. Fingertips re-versions these every couple of
    # boundary cycles (101 → 301 → 401 → 501 …); prefer the highest-numbered
    # match so we always pick the latest vintage. Skip Combined Authorities,
    # Counties, Upper-tier — those have different parent codes.
    lower_tier = [r for r in rows
                  if 'lower tier' in (r.get('Name') or '').lower()
                  or 'lower-tier' in (r.get('Name') or '').lower()]
    if lower_tier:
        # Highest id = most recent boundaries (e.g. 501 post-4/23 > 401 4/21-3/23)
        try:
            best = max(lower_tier, key=lambda r: int(r.get('Id') or 0))
            tid = int(best.get('Id'))
            print(f"  using LAD area type id={tid} ({best.get('Name')})")
            return tid
        except Exception:
            pass
    # Fallback for older/older-named area types
    for kw in ('london borough', 'districts and unitary authorities',
               'local authority district'):
        for r in rows:
            if kw in (r.get('Name') or '').lower():
                tid = r.get('Id')
                try:
                    print(f"  using LAD area type id={int(tid)} ({r.get('Name')})")
                    return int(tid)
                except Exception:
                    pass
    print("  WARN: no LAD area type found; defaulting to 501 (lower tier post 4/23)", file=sys.stderr)
    return 501


def fetch_msoa_data_for_lad(ftp, indicator_ids, msoa_area_type_id, lad_atid, lad_code, profile_id):
    """Direct API call to Fingertips' all_data/csv endpoint.
    Required params: indicator_ids, child_area_type_id, parent_area_type_id,
    parent_area_code. profile_id is optional but speeds the query.
    """
    import requests
    import pandas as pd
    import io
    url = 'https://fingertipsws.phe.org.uk/api/all_data/csv/by_indicator_id'
    params = {
        'indicator_ids':       ','.join(str(i) for i in indicator_ids),
        'child_area_type_id':  msoa_area_type_id,
        'parent_area_type_id': lad_atid,
        'parent_area_code':    lad_code,
        'profile_id':          profile_id,
        # Some Fingertips profile/area combos require these — empty/default
        # values match the working example URL in the API docs.
        'category_area_code':  '',
        'sex_id':              4,   # 4 = Persons (default for indicators not split by sex)
        'age_id':              1,   # 1 = All ages (default for indicators not split by age)
    }
    r = requests.get(url, params=params, timeout=120)
    if r.status_code != 200:
        # Print the response body so we can see WHY the API rejected the
        # request. 400 errors typically come back with a JSON or plain-text
        # message naming the offending parameter.
        body_snippet = r.text[:600] if r.text else '(empty body)'
        print(f"      HTTP {r.status_code}; response body: {body_snippet!r}")
        r.raise_for_status()
    text = r.text
    # The response is sometimes prefixed with non-CSV diagnostic lines (a
    # blank line, a copyright header, etc.). Skip until we hit a line that
    # looks like a CSV header — i.e. starts with "Indicator ID" or similar.
    lines = text.splitlines()
    start = 0
    for i, line in enumerate(lines):
        low = line.lower().lstrip('"')
        if low.startswith('indicator id') or low.startswith('indicator_id') \
           or low.startswith('"indicator'):
            start = i
            break
    csv_text = '\n'.join(lines[start:]) if start > 0 else text
    if not csv_text.strip():
        return None
    try:
        df = pd.read_csv(io.StringIO(csv_text), dtype=str, low_memory=False,
                         on_bad_lines='skip')
    except Exception as e:
        print(f"      pandas parse failed: {e}; first 500 chars of response:")
        print(f"      {text[:500]!r}")
        return None
    if df is None or df.empty:
        return None
    # Filter to MSOA rows for this LAD. The 'Area Code' starting with E02 is
    # a reliable MSOA filter; if a 'Parent Code' column is present, AND on
    # that for tighter filtering (some endpoints return all NWL MSOAs even
    # when parent_area_code is set).
    code_col = next((c for c in df.columns
                     if c.lower().replace(' ', '') == 'areacode'), None)
    parent_col = next((c for c in df.columns
                       if 'parent' in c.lower() and 'code' in c.lower()), None)
    if code_col is None:
        return df
    mask = df[code_col].astype(str).str.startswith('E02')
    if parent_col:
        mask &= df[parent_col].astype(str) == lad_code
    return df[mask].copy()


# ──────────────────────────────────────────────────────────────────────────
# Aggregate
# ──────────────────────────────────────────────────────────────────────────
def aggregate_to_ward(msoa_data, lsoa_to_msoa, lsoa_to_ward, lsoa_pop):
    out = {}
    for iid, msoa_vals in msoa_data.items():
        ward_acc = {}
        for lsoa, wcode in lsoa_to_ward.items():
            msoa = lsoa_to_msoa.get(lsoa)
            if not msoa:
                continue
            v = msoa_vals.get(msoa)
            if v is None:
                continue
            # Skip NaN (suppressed Fingertips values) — they propagate through
            # the pop-weighted mean and produce NaN ward values, which then
            # break browser JSON parsing because Python writes NaN as a bare
            # token that JSON.parse rejects.
            if isinstance(v, float) and v != v:
                continue
            pop = lsoa_pop.get(lsoa)
            if pop is None or pop <= 0:
                continue
            slot = ward_acc.setdefault(wcode, [0.0, 0.0])
            slot[0] += pop * v
            slot[1] += pop
        out[iid] = {w: (n/d) for w, (n, d) in ward_acc.items() if d > 0}
    return out


# ──────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--profile-id',   type=int)
    ap.add_argument('--profile-name', default='Local health')
    ap.add_argument('--limit',        type=int)
    ap.add_argument('--dry-run',      action='store_true')
    ap.add_argument('--list-area-types', action='store_true',
                    help='print all Fingertips area types with IDs and exit')
    ap.add_argument('--lad-area-type-id', type=int,
                    help='override the LAD area type ID auto-discovery')
    ap.add_argument('--msoa-area-type-id', type=int,
                    help='override the MSOA area type ID (try 3 if 213 errors)')
    ap.add_argument('--keep-dupes', action='store_true',
                    help='keep indicators that duplicate existing dashboard fields '
                         '(population, IMD, ethnicity, age splits)')
    ap.add_argument('--stale-cutoff', type=int, default=STALE_CUTOFF,
                    help=f'drop indicators with latest period before this year '
                         f'(default {STALE_CUTOFF})')
    args = ap.parse_args()

    ftp, pd = _import()

    if args.list_area_types:
        print("\n=== Fingertips area types ===")
        try:
            d = ftp.get_area_types_as_dict()
            rows = []
            if isinstance(d, dict):
                for k, v in d.items():
                    if isinstance(v, dict):
                        rows.append((v.get('Id', k), v.get('Name', '')))
                    else:
                        rows.append((int(k) if str(k).isdigit() else k, str(v)))
            for tid, name in sorted(rows, key=lambda x: int(x[0]) if str(x[0]).isdigit() else 0):
                print(f"  id={tid:>5}  {name}")
        except Exception as e:
            print(f"  error: {e}")
        return

    print("\n=== STEP 1: profile + area type ===")
    profile = find_profile(ftp, args.profile_id, args.profile_name)
    print(f"  profile: id={profile['Id']}  name={profile.get('Name')}")
    if args.msoa_area_type_id:
        msoa_atid = args.msoa_area_type_id
        print(f"  using MSOA area type id={msoa_atid} (from --msoa-area-type-id)")
    else:
        msoa_atid = find_msoa_area_type_id(ftp)
    if args.lad_area_type_id:
        lad_atid = args.lad_area_type_id
        print(f"  using LAD area type id={lad_atid} (from --lad-area-type-id)")
    else:
        lad_atid = find_lad_area_type_id(ftp)

    print("\n=== STEP 2: indicator metadata ===")
    meta_df = fetch_indicator_metadata(ftp, profile_id=profile['Id'])
    iid_col = next((c for c in meta_df.columns if c.lower().replace(' ', '') == 'indicatorid'), None)
    nm_col  = next((c for c in meta_df.columns if c.lower() in ('indicator', 'indicator name')), None)
    pol_col = next((c for c in meta_df.columns if c.lower() == 'polarity'), None)
    unit_col= next((c for c in meta_df.columns if c.lower() in ('unit', 'value type')), None)
    if not iid_col:
        print(f"  metadata cols: {list(meta_df.columns)}", file=sys.stderr)
        sys.exit("Indicator ID column not found in metadata")
    indicators = meta_df[[iid_col, nm_col or iid_col, pol_col or iid_col, unit_col or iid_col]].drop_duplicates(subset=[iid_col])
    indicators.columns = ['indicator_id', 'name', 'polarity', 'unit']
    indicators['indicator_id'] = indicators['indicator_id'].astype(int)
    if args.limit:
        indicators = indicators.head(args.limit)
    print(f"  {len(indicators)} unique indicators in profile")

    # Save metadata sidecar
    meta_records = [{
        'indicator_id': int(r['indicator_id']),
        'name':         str(r['name']),
        'unit':         str(r['unit']) if r['unit'] != r['indicator_id'] else '',
        'polarity':     str(r['polarity']) if r['polarity'] != r['indicator_id'] else '',
        'wh':           POLARITY_TO_WH.get(str(r['polarity']).lower())
                          if r['polarity'] != r['indicator_id'] else None,
    } for _, r in indicators.iterrows()]

    print("\n=== STEP 3: load lookups ===")
    lsoa_to_msoa = load_lsoa_msoa()
    lsoa_to_ward, lsoa_pop = load_lsoa_ward_and_pop()
    print(f"  LSOA→MSOA: {len(lsoa_to_msoa)}")
    print(f"  LSOA→ward: {len(lsoa_to_ward)}")
    print(f"  LSOA pops: {len(lsoa_pop)}")

    nwl_msoas = {lsoa_to_msoa[c] for c in lsoa_to_ward if c in lsoa_to_msoa}
    print(f"  NWL MSOAs: {len(nwl_msoas)}")

    print("\n=== STEP 4: fetch MSOA data ===")
    iid_list = indicators['indicator_id'].tolist()
    msoa_data = {iid: {} for iid in iid_list}
    raw_rows = []

    for lad_code, lad_name in NWL_LADS.items():
        print(f"  fetching MSOA data for {lad_name} ({lad_code}) …")
        try:
            df = fetch_msoa_data_for_lad(
                ftp,
                indicator_ids=iid_list,
                msoa_area_type_id=msoa_atid,
                lad_atid=lad_atid,
                lad_code=lad_code,
                profile_id=profile['Id'],
            )
        except Exception as e:
            print(f"    FAILED: {e}")
            continue
        if df is None or df.empty:
            print(f"    empty")
            continue
        # Find columns
        c_iid    = next((c for c in df.columns if c.lower().replace(' ', '') == 'indicatorid'), None)
        c_code   = next((c for c in df.columns if c.lower().replace(' ', '') == 'areacode'), None)
        c_period = next((c for c in df.columns if 'time' in c.lower() and 'period' in c.lower()), None)
        c_value  = next((c for c in df.columns if c.lower() == 'value'), None)
        if not all([c_iid, c_code, c_value]):
            print(f"    schema mismatch, skipping. cols={list(df.columns)[:8]}")
            continue
        for _, row in df.iterrows():
            try:
                iid = int(row[c_iid])
                code = str(row[c_code])
                if not code.startswith('E02'):
                    continue
                v = float(row[c_value])
            except (ValueError, TypeError):
                continue
            period = str(row[c_period]) if c_period else ''
            existing = msoa_data.get(iid, {}).get(code)
            if existing and period < existing.get('period', ''):
                continue
            msoa_data.setdefault(iid, {})[code] = {'value': v, 'period': period}
            raw_rows.append({'indicator_id': iid, 'msoa_code': code, 'value': v, 'period': period, 'lad': lad_code})
        print(f"    rows kept: {sum(1 for k,v in msoa_data.items() if v)} indicators × MSOA pairs ({len(df)} rows fetched)")

    # msoa_simple is built by the curation step below (after dupe/stale drops).

    # Diagnostic: what time period(s) does each indicator's data come from?
    print("\n=== Indicator vintage summary ===")
    for iid, vals in msoa_data.items():
        periods = sorted({rec['period'] for rec in vals.values() if rec.get('period')})
        name = next((m['name'] for m in meta_records if m['indicator_id'] == iid), '?')
        if periods:
            span = periods[0] if len(periods) == 1 else f'{periods[0]} … {periods[-1]}'
            print(f"  ft_{iid:>5}  vintage={span:<20}  ({name[:60]})")
        else:
            print(f"  ft_{iid:>5}  vintage=(none)            ({name[:60]})")

    # ── CURATION ────────────────────────────────────────────────────────
    # Drop indicators that duplicate existing dashboard fields, and any
    # indicator whose latest published period predates --stale-cutoff.
    print("\n=== Curation ===")
    keep_iids = []
    drops = []  # (iid, name, reason)
    name_lookup = {m['indicator_id']: m['name'] for m in meta_records}
    import re as _re
    def _is_dupe(name):
        if args.keep_dupes:
            return False
        nm = (name or '').lower()
        for pat in DUPE_PATTERNS:
            if pat in nm:
                return pat
        return False
    def _latest_year(periods):
        # Fingertips period strings come in many shapes:
        #   '2022'                — single year
        #   '2018 - 22'           — range, end as 2-digit
        #   '2022/23'             — financial year, end as 2-digit
        #   '2016/17 - 20/21'     — range of financial years, both 2-digit suffixes
        #   '2018-19', '2017-19'  — ditto
        # Strategy: find every 4-digit year (20XX). Then for any 2-digit token
        # that follows '/', '-', ' ' and isn't part of a 4-digit year,
        # promote it to 20XX (assume 2000s; reject impossibly small values).
        years = []
        for p in periods:
            ps = str(p)
            # 4-digit years anywhere in the string
            for m in _re.finditer(r'(?<!\d)(20\d{2})(?!\d)', ps):
                years.append(int(m.group(1)))
            # 2-digit years preceded by /, -, or space, not part of a longer number
            for m in _re.finditer(r'[/\- ](\d{2})(?!\d)', ps):
                yr = int(m.group(1))
                if 0 <= yr <= 30:    # 00-30 → 2000-2030
                    years.append(2000 + yr)
        return max(years) if years else None

    for iid, vals in msoa_data.items():
        name = name_lookup.get(iid, '')
        periods = sorted({rec['period'] for rec in vals.values() if rec.get('period')})
        dupe_match = _is_dupe(name)
        latest = _latest_year(periods)
        if dupe_match:
            drops.append((iid, name, f'DUPE  match="{dupe_match}"'))
            continue
        if latest is not None and latest < args.stale_cutoff:
            drops.append((iid, name, f'STALE latest={latest} < cutoff={args.stale_cutoff}'))
            continue
        keep_iids.append(iid)

    print(f"  KEEP:  {len(keep_iids)} indicators")
    print(f"  DROP:  {len(drops)} indicators")
    if drops:
        print("  Dropped:")
        for iid, name, reason in drops:
            print(f"    ft_{iid:>5}  {reason:<45}  ({(name or '')[:55]})")

    # Filter aggregation pool to kept indicators
    msoa_simple = {iid: {c: rec['value'] for c, rec in vals.items()}
                   for iid, vals in msoa_data.items() if iid in keep_iids}
    # Filter metadata sidecar too
    meta_records = [m for m in meta_records if m['indicator_id'] in keep_iids]
    (REPO / 'scripts' / 'fingertips_metadata.json').write_text(
        json.dumps(meta_records, indent=2), encoding='utf-8')

    print("\n=== STEP 5: aggregate MSOA → ward ===")
    ward_data = aggregate_to_ward(msoa_simple, lsoa_to_msoa, lsoa_to_ward, lsoa_pop)
    n_with = sum(1 for v in ward_data.values() if v)
    print(f"  ward aggregates produced for {n_with}/{len(ward_data)} indicators")

    # Persist outputs even on dry-run for inspection
    print("\n=== STEP 6: write metadata + raw CSV ===")
    (REPO / 'scripts' / 'fingertips_metadata.json').write_text(
        json.dumps(meta_records, indent=2), encoding='utf-8')
    if raw_rows:
        pd.DataFrame(raw_rows).to_csv(REPO / 'scripts' / 'fingertips_msoa_raw.csv', index=False)
    print(f"  wrote scripts/fingertips_metadata.json")
    print(f"  wrote scripts/fingertips_msoa_raw.csv ({len(raw_rows)} rows)")

    if args.dry_run:
        print("\n=== --dry-run: skipping ward_data.json patch ===")
        print("\nSample (first 5 indicators × 3 wards):")
        for iid in list(ward_data.keys())[:5]:
            name = next((m['name'] for m in meta_records if m['indicator_id'] == iid), '?')
            print(f"  ft_{iid}  ({name[:55]})")
            for wcode, v in list(ward_data[iid].items())[:3]:
                print(f"    {wcode}: {v:.3f}")
        return

    print("\n=== STEP 7: patch ward_data.json ===")
    wpath = REPO / 'ward_data.json'
    wd = json.loads(wpath.read_text(encoding='utf-8'))
    n_cells = 0
    n_wards = set()
    n_skipped_nan = 0
    for iid, ward_vals in ward_data.items():
        key = f'ft_{iid}'
        for wcode, v in ward_vals.items():
            if v is None or wcode not in wd['wards']:
                continue
            # Skip NaN (Python serialises as bare 'NaN' which is invalid JSON
            # and breaks the dashboard's fetch + parse). Defensive — the
            # aggregation step also filters NaN now.
            if isinstance(v, float) and v != v:
                n_skipped_nan += 1
                continue
            wd['wards'][wcode].setdefault('indicators', {})
            wd['wards'][wcode]['indicators'][key] = round(v, 4)
            n_cells += 1
            n_wards.add(wcode)
    # Strip any pre-existing NaN values from prior fingertips fields — patcher
    # may have written them in earlier runs, before this fix existed.
    n_cleaned = 0
    for w in wd['wards'].values():
        inds = w.get('indicators') or {}
        for k in list(inds.keys()):
            v = inds[k]
            if isinstance(v, float) and v != v:
                del inds[k]
                n_cleaned += 1
    wd.setdefault('metadata', {})
    wd['metadata']['fingertips_added']           = datetime.utcnow().isoformat() + 'Z'
    wd['metadata']['fingertips_indicator_count'] = n_with
    wd['metadata']['fingertips_profile']         = profile.get('Name')
    # allow_nan=False makes json.dumps RAISE if any NaN/Infinity sneaks in,
    # rather than silently producing invalid JSON. Belt-and-braces.
    wpath.write_text(
        json.dumps(wd, ensure_ascii=False, indent=1, allow_nan=False),
        encoding='utf-8',
    )
    print(f"  patched {n_cells} cells across {len(n_wards)} wards "
          f"(skipped {n_skipped_nan} NaN values during patch, "
          f"cleaned {n_cleaned} pre-existing NaN values)")
    print(f"\nDONE. Now run: py scripts/wire_fingertips_ui.py")


if __name__ == '__main__':
    main()
