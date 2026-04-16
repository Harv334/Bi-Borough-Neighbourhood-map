"""
Refresh pharmacies.json for the NW London map.

Usage (from repo root, on any Windows / Mac / Linux machine with plain internet):

    python scripts/refresh_pharmacies.py

What it does:
  1. Downloads the current NHS BSA edispensary.csv (every NHS-contracted
     dispensing pharmacy in England, refreshed monthly).
  2. Geocodes each row's postcode via the ONSPD lookup that's already
     bundled in pipeline/.cache/onspd (falls back to a postcodes.io batch
     lookup if the ONSPD parquet isn't there).
  3. Filters to the 9 NW London local authorities.
  4. Writes pharmacies.json — the map reads it at load time and shows a
     purple pin layer.

Run this again whenever you want a fresh list (maybe once a quarter).
NHS BSA sometimes publishes the monthly file a few days into the month,
so the script walks back up to 6 months looking for the latest.
"""
from __future__ import annotations

import csv
import io
import json
import sys
from datetime import datetime
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent

# --- NHS BSA dispensary feed -------------------------------------------------
NHSBSA_PATTERN = "https://www.nhsbsa.nhs.uk/sites/default/files/{ym}/edispensary.csv"

# --- NW London catchment -----------------------------------------------------
NW_LADS = {
    "E09000005",  # Brent
    "E09000007",  # Camden
    "E09000009",  # Ealing
    "E09000013",  # Hammersmith and Fulham
    "E09000015",  # Harrow
    "E09000017",  # Hillingdon
    "E09000018",  # Hounslow
    "E09000020",  # Kensington and Chelsea
    "E09000033",  # Westminster
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}


def fetch_bsa_csv() -> list[dict]:
    """Walk back month-by-month for up to 6 months to find the latest publication."""
    now = datetime.utcnow()
    for delta in range(0, 6):
        year = now.year if now.month - delta > 0 else now.year - 1
        month = (now.month - delta - 1) % 12 + 1
        ym = f"{year}-{month:02d}"
        url = NHSBSA_PATTERN.format(ym=ym)
        print(f"  trying {url}", flush=True)
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            if r.status_code == 200 and len(r.content) > 1000:
                print(f"  [ok] {len(r.content):,} bytes")
                rows = list(csv.DictReader(io.StringIO(r.text)))
                return rows
        except requests.RequestException as exc:
            print(f"  [err] {exc}")
    raise RuntimeError("Could not find a recent NHS BSA edispensary.csv")


def postcodes_io_batch(postcodes: list[str]) -> dict[str, dict]:
    """Geocode a list of postcodes via postcodes.io (no key required)."""
    out: dict[str, dict] = {}
    CHUNK = 100
    for i in range(0, len(postcodes), CHUNK):
        chunk = postcodes[i:i + CHUNK]
        r = requests.post(
            "https://api.postcodes.io/postcodes",
            json={"postcodes": chunk},
            timeout=30,
        )
        r.raise_for_status()
        for item in r.json().get("result", []):
            pc = (item.get("query") or "").upper().replace(" ", "")
            res = item.get("result")
            if not res:
                continue
            out[pc] = {
                "lat": res.get("latitude"),
                "lng": res.get("longitude"),
                "lad": (res.get("codes") or {}).get("admin_district"),
                "lsoa": (res.get("codes") or {}).get("lsoa"),
                "ward": (res.get("codes") or {}).get("admin_ward"),
            }
        print(f"  geocoded {min(i+CHUNK, len(postcodes))}/{len(postcodes)}", flush=True)
    return out


def norm_pc(pc: str) -> str:
    return (pc or "").upper().replace(" ", "").strip()


def titleise(s: str) -> str:
    # Don't title-case all-caps abbreviations inside an address unduly;
    # leave numerics alone. Good-enough for a map popup.
    return " ".join(w.capitalize() if w.isalpha() else w for w in (s or "").split())


def main() -> int:
    print("Fetching NHS BSA edispensary feed...")
    rows = fetch_bsa_csv()
    print(f"Got {len(rows):,} rows total.")

    # Dedupe & collect unique postcodes
    postcodes = sorted({norm_pc(r.get("Postcode", "")) for r in rows if r.get("Postcode")})
    print(f"Unique postcodes: {len(postcodes):,}")

    print("Geocoding via postcodes.io...")
    lookup = postcodes_io_batch(postcodes)

    out: list[dict] = []
    for r in rows:
        pc = norm_pc(r.get("Postcode", ""))
        hit = lookup.get(pc)
        if not hit:
            continue
        if hit.get("lad") not in NW_LADS:
            continue
        addr = ", ".join(
            filter(
                None,
                [
                    titleise(r.get("Address1", "")),
                    titleise(r.get("Address2", "")),
                    titleise(r.get("Address3", "")),
                ],
            )
        )
        out.append(
            {
                "n": titleise(r.get("Pharmacy Name") or r.get("Name") or ""),
                "a": addr,
                "pc": (r.get("Postcode") or "").strip(),
                "tel": (r.get("Telephone") or "").strip(),
                "lat": hit["lat"],
                "lng": hit["lng"],
                "lad": hit.get("lad"),
                "lsoa": hit.get("lsoa"),
                "ward": hit.get("ward"),
            }
        )

    print(f"Kept {len(out):,} NW London pharmacies.")

    out_path = ROOT / "pharmacies.json"
    out_path.write_text(json.dumps(out, separators=(",", ":")))
    print(f"Wrote {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
