"""
GP practices fetcher — REFERENCE IMPLEMENTATION.

Source:  NHS ODS EPRACCUR.zip (https://digital.nhs.uk/services/organisation-data-service/...)
Joins:   NHS postcode -> ONSPD -> (lat/lng, LSOA21CD, WD25CD, LAD25CD)
Output:  data/healthcare/gp_practices.parquet

Filters:
  StatusCode == 'A'              (active)
  PrescribingSetting == '4'      (GP practice, not clinic / community / other)
  LAD25CD in NW_LADS             (one of the 9 NW London boroughs)
"""
from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path

import pandas as pd
import requests
import yaml

from ...core import BaseFetcher, get_lookup, normalise_postcode


EPRACCUR_URL = (
    "https://files.digital.nhs.uk/assets/ods/current/epraccur.zip"
)


class GpPracticesFetcher(BaseFetcher):
    source_id = "gp_practices"
    category = "healthcare"
    required_cols = [
        "code", "name", "addr", "postcode", "tel",
        "lat", "lng", "LSOA21CD", "WD25CD", "LAD25CD",
    ]

    # EPRACCUR has 27 columns and no header. Indexes per the schema doc:
    EPRACCUR_HEADER = [
        "OrganisationCode", "Name", "NationalGrouping", "HighLevelHealthGeography",
        "AddressLine1", "AddressLine2", "AddressLine3", "AddressLine4", "AddressLine5",
        "Postcode", "OpenDate", "CloseDate", "StatusCode", "OrganisationSubTypeCode",
        "Commissioner", "JoinProviderDate", "LeftProviderDate", "ContactTelephoneNumber",
        "_n1", "_n2", "_n3", "AmendedRecordIndicator", "_n4",
        "ProviderPurchaser", "_n5", "PrescribingSetting", "_n6",
    ]

    def fetch_raw(self) -> pd.DataFrame:
        """Download the latest EPRACCUR zip and return a DataFrame of all UK practices."""
        cache = self.cache_dir / "epraccur.zip"
        if not cache.exists():
            # NHS Digital's CDN 403s the default python-requests UA.
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "application/zip,application/octet-stream,*/*",
            }
            r = requests.get(EPRACCUR_URL, timeout=60, headers=headers)
            r.raise_for_status()
            cache.write_bytes(r.content)

        with zipfile.ZipFile(cache) as z:
            with z.open("epraccur.csv") as f:
                df = pd.read_csv(
                    io.TextIOWrapper(f, encoding="latin-1"),
                    header=None,
                    names=self.EPRACCUR_HEADER,
                    dtype=str,
                    keep_default_na=False,
                )
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # 1. Filter to active GP practices
        df = df[df["StatusCode"] == "A"]
        df = df[df["PrescribingSetting"] == "4"]

        # 2. Lookup postcode -> coords + admin codes (in-memory ONSPD)
        lookup = get_lookup(str(self.repo_root))

        rows = []
        for _, row in df.iterrows():
            pc_norm = normalise_postcode(row["Postcode"])
            hit = lookup.get(pc_norm)
            if not hit:
                continue
            lat, lng, lsoa, lad, wd = hit
            if lad not in self._nw_lads():
                continue
            rows.append({
                "code": row["OrganisationCode"],
                "name": row["Name"].title(),
                "addr": ", ".join(filter(None, [
                    row["AddressLine1"].title() if row["AddressLine1"] else "",
                    row["AddressLine2"].title() if row["AddressLine2"] else "",
                    row["AddressLine3"].title() if row["AddressLine3"] else "",
                ])),
                "postcode": row["Postcode"],
                "tel": row["ContactTelephoneNumber"],
                "lat": lat,
                "lng": lng,
                "LSOA21CD": lsoa,
                "WD25CD": wd,
                "LAD25CD": lad,
            })

        out = pd.DataFrame(rows)

        # 3. Optional: enrich with ward / LAD display names from boundaries (point-in-polygon)
        # This is more reliable than the ONSPD code in edge cases on borough boundaries.
        try:
            from ...core.geo import load_boundary
            wards = load_boundary(str(self.repo_root), "wards")
            for i, row in out.iterrows():
                hit = wards.find(row["lng"], row["lat"])
                if hit:
                    out.at[i, "ward"] = hit.get("WD25NM") or hit.get("WD24NM") or ""
                    out.at[i, "lad"] = hit.get("LAD") or hit.get("LAD25NM") or ""
        except FileNotFoundError:
            # boundaries fetcher hasn't run yet; ward / lad will be empty
            out["ward"] = ""
            out["lad"] = ""

        return out

    @staticmethod
    def _nw_lads() -> set[str]:
        """Pull the LAD25CD set from conf/boroughs.yml so we don't hardcode it twice."""
        repo_root = Path(__file__).resolve().parents[4]
        with open(repo_root / "pipeline" / "conf" / "boroughs.yml") as f:
            cfg = yaml.safe_load(f)
        return {b["lad25cd"] for b in cfg["boroughs"]}
