"""
CQC care directory — care homes & domiciliary care providers.

Source: https://www.cqc.org.uk/about-us/transparency/using-cqc-data
The CQC publishes a monthly XLSX listing every active care provider with lat/lng
and service breakdown.

Bulk URL changes monthly:
  https://www.cqc.org.uk/sites/default/files/<MMM_YYYY>_HSCA_Active_Locations.ods (or .xlsx)
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import yaml

from ...core import BaseFetcher
from ...core.geo import load_boundary

CQC_URL_PATTERN = (
    "https://www.cqc.org.uk/sites/default/files/{mmm_yyyy}_HSCA_Active_Locations.xlsx"
)


class CareHomesFetcher(BaseFetcher):
    source_id = "care_homes"
    category = "healthcare"
    required_cols = ["location_id", "name", "addr", "postcode", "lat", "lng",
                     "service_types", "regulated_activities"]

    def fetch_raw(self) -> pd.DataFrame:
        cache = self.cache_dir / "cqc_active_locations.xlsx"
        if cache.exists():
            return pd.read_excel(cache, sheet_name="HSCA Active Locations", dtype=str)

        now = datetime.utcnow()
        for delta in range(0, 4):
            year = now.year if now.month - delta > 0 else now.year - 1
            month = (now.month - delta - 1) % 12 + 1
            stamp = datetime(year, month, 1).strftime("%B_%Y")
            r = requests.get(CQC_URL_PATTERN.format(mmm_yyyy=stamp), timeout=60)
            if r.status_code == 200 and len(r.content) > 10000:
                cache.write_bytes(r.content)
                return pd.read_excel(cache, sheet_name="HSCA Active Locations", dtype=str)
        raise RuntimeError("Could not find recent CQC HSCA Active Locations file")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        boroughs = load_boundary(str(self.repo_root), "boroughs")
        rows = []
        for _, r in df.iterrows():
            try:
                lat = float(r.get("Location Latitude", ""))
                lng = float(r.get("Location Longitude", ""))
            except (TypeError, ValueError):
                continue
            hit = boroughs.find(lng, lat)
            if not hit:
                continue
            rows.append({
                "location_id": r.get("Location ID", ""),
                "name": r.get("Location Name", ""),
                "addr": ", ".join(filter(None, [
                    r.get("Location Street Address", ""),
                    r.get("Location Address Line 2", ""),
                    r.get("Location City", ""),
                ])),
                "postcode": r.get("Location Postal Code", ""),
                "lat": lat, "lng": lng,
                "service_types": r.get("Service Types") or "",
                "regulated_activities": r.get("Regulated Activities") or "",
                "lad": hit.get("name", ""),
            })
        return pd.DataFrame(rows)
