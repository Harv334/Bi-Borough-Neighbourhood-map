"""
Public Transport Accessibility Levels (PTAL) — TfL.

Source: https://data.london.gov.uk/dataset/public-transport-accessibility-levels
Bulk download is a CSV of 100m grid cells with PTAL value 0 (worst) - 6b (best).

For population-health joins we aggregate to LSOA mean.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import requests

from ...core import BaseFetcher
from ...core.geo import load_boundary

# London Datastore stable URL — currently 2015 release; updated quarterly
PTAL_URL = (
    "https://data.london.gov.uk/download/public-transport-accessibility-levels/"
    "12300d05-e8b1-4527-bbef-39f7e84a1cf6/PTAL_100m_Grid.csv"
)

PTAL_BAND_VALUE = {
    "0": 0, "1a": 1, "1b": 1.5, "2": 2, "3": 3, "4": 4,
    "5": 5, "6a": 6, "6b": 6.5,
}


class PtalFetcher(BaseFetcher):
    source_id = "ptal"
    category = "transport"
    required_cols = ["LSOA21CD", "ptal_mean", "ptal_min", "ptal_max"]

    def fetch_raw(self) -> pd.DataFrame:
        cache = self.cache_dir / "ptal_grid.csv"
        if not cache.exists():
            r = requests.get(PTAL_URL, timeout=120)
            r.raise_for_status()
            cache.write_bytes(r.content)
        return pd.read_csv(cache, dtype={"PTAL": str})

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        lsoas = load_boundary(str(self.repo_root), "lsoa")

        # Map PTAL band to numeric
        df["ptal_num"] = df["PTAL"].map(PTAL_BAND_VALUE)

        # Spatial join: for each grid cell point, find LSOA
        per_lsoa: dict[str, list[float]] = {}
        for _, row in df.iterrows():
            try:
                lng = float(row["X"]); lat = float(row["Y"])
                v = float(row["ptal_num"])
            except (KeyError, ValueError, TypeError):
                continue
            hit = lsoas.find(lng, lat)
            if not hit:
                continue
            per_lsoa.setdefault(hit["code"], []).append(v)

        rows = [
            {
                "LSOA21CD": lsoa_cd,
                "ptal_mean": sum(vs) / len(vs),
                "ptal_min": min(vs),
                "ptal_max": max(vs),
                "n_cells": len(vs),
            }
            for lsoa_cd, vs in per_lsoa.items()
        ]
        return pd.DataFrame(rows)
