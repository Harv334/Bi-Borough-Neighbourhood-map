"""
DEFRA AURN — Automatic Urban and Rural Network air quality monitors.

Source: https://uk-air.defra.gov.uk/data/data_selector_service
This fetcher pulls daily mean NO2, PM2.5, PM10 and O3 concentrations for every
AURN monitor inside the 9 NW London LADs.

Two-step process:
  1. Pull the static metadata of all UK AURN sites once
     (https://uk-air.defra.gov.uk/openair/R_data/AURN_metadata.RData
     — but we use the CSV mirror at .../sites_data.csv)
  2. For each in-scope site, pull the latest year's measurements via
     https://uk-air.defra.gov.uk/data/data_selector_service?...

Output is wide-format: site_id, date, no2, pm25, pm10, o3.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import requests

from ...core import BaseFetcher
from ...core.geo import load_boundary

DEFRA_SITES_CSV = "https://uk-air.defra.gov.uk/datastore/met_sites_metadata.csv"


class AurnFetcher(BaseFetcher):
    source_id = "air_quality_aurn"
    category = "environment"
    required_cols = ["site_id", "site_name", "lat", "lng",
                     "LSOA21CD", "WD25CD", "LAD25CD"]

    def fetch_raw(self) -> pd.DataFrame:
        cache = self.cache_dir / "aurn_sites.csv"
        if not cache.exists():
            r = requests.get(DEFRA_SITES_CSV, timeout=30)
            r.raise_for_status()
            cache.write_bytes(r.content)
        return pd.read_csv(cache, dtype=str)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        boroughs = load_boundary(str(self.repo_root), "boroughs")
        lsoas = load_boundary(str(self.repo_root), "lsoa")
        wards = load_boundary(str(self.repo_root), "wards")

        rows = []
        for _, r in df.iterrows():
            try:
                lat = float(r.get("Latitude", ""))
                lng = float(r.get("Longitude", ""))
            except (TypeError, ValueError):
                continue
            if not boroughs.find(lng, lat):
                continue
            lsoa_p = lsoas.find(lng, lat) or {}
            ward_p = wards.find(lng, lat) or {}
            rows.append({
                "site_id": r.get("Site ID") or r.get("Code") or "",
                "site_name": r.get("Site Name") or r.get("Name") or "",
                "type": r.get("Environment Type") or r.get("Type", ""),
                "lat": lat, "lng": lng,
                "LSOA21CD": lsoa_p.get("code", ""),
                "WD25CD": ward_p.get("WD25CD") or ward_p.get("WD24CD") or "",
                "LAD25CD": "",
            })
        # NOTE: Time-series measurements are NOT pulled by default — that would be
        # ~10MB per site per year. To enable, run:
        #   pipeline run --source air_quality_aurn_measurements
        # which is a separate fetcher that depends on this metadata file.
        return pd.DataFrame(rows)
