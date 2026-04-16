"""
TfL rail stations — Tube + Overground + Elizabeth Line + DLR + National Rail in NW London.

Source: TfL Unified API https://api.tfl.gov.uk
No auth needed for read-only endpoints.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import requests

from ...core import BaseFetcher
from ...core.geo import load_boundary

TFL_STOPPOINTS = (
    "https://api.tfl.gov.uk/StopPoint/Mode/"
    "tube,overground,elizabeth-line,dlr,national-rail"
)


class StationsFetcher(BaseFetcher):
    source_id = "tfl_stations"
    category = "transport"
    required_cols = ["naptan_id", "name", "modes", "lat", "lng",
                     "LSOA21CD", "WD25CD", "LAD25CD"]

    def fetch_raw(self) -> dict:
        cache = self.cache_dir / "stoppoints.json"
        if not cache.exists():
            r = requests.get(TFL_STOPPOINTS, timeout=60)
            r.raise_for_status()
            cache.write_bytes(r.content)
        return json.loads(cache.read_text())

    def transform(self, raw: dict) -> pd.DataFrame:
        boroughs = load_boundary(str(self.repo_root), "boroughs")
        lsoas = load_boundary(str(self.repo_root), "lsoa")
        wards = load_boundary(str(self.repo_root), "wards")

        rows = []
        for sp in raw.get("stopPoints", []):
            lat = sp.get("lat"); lng = sp.get("lon")
            if lat is None or lng is None:
                continue
            if not boroughs.find(lng, lat):
                continue
            lsoa_p = lsoas.find(lng, lat) or {}
            ward_p = wards.find(lng, lat) or {}
            rows.append({
                "naptan_id": sp.get("naptanId") or sp.get("id"),
                "name": sp.get("commonName"),
                "modes": ",".join(sp.get("modes", [])),
                "lat": lat, "lng": lng,
                "LSOA21CD": lsoa_p.get("code", ""),
                "WD25CD": ward_p.get("WD25CD") or ward_p.get("WD24CD") or "",
                "LAD25CD": "",
            })
        return pd.DataFrame(rows)
