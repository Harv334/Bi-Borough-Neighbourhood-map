"""
London Atmospheric Emissions Inventory (LAEI) — modelled NO2 / PM2.5 grids.

Source: https://data.london.gov.uk/dataset/london-atmospheric-emissions-inventory--laei--2019
Provides 20m-resolution rasters of annual mean concentration. Aggregated to LSOA
mean for cross-joins with health outcomes.

DISABLED in conf/sources.yml until raster dependencies (rasterio, rioxarray) are
added — they're large (~80MB) and only needed by this one fetcher. Install with:
    pip install nw-london-health-pipeline[raster]
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from ...core import BaseFetcher


class LaeiFetcher(BaseFetcher):
    source_id = "laei"
    category = "environment"
    required_cols = ["LSOA21CD", "no2_mean", "pm25_mean"]

    def fetch_raw(self):
        raise NotImplementedError(
            "LAEI fetcher is a stub. Implement using rioxarray once raster deps are added.\n"
            "Workflow: download GeoTIFF -> reproject to BNG -> zonal_stats per LSOA polygon."
        )

    def transform(self, raw) -> pd.DataFrame:
        raise NotImplementedError("LAEI fetcher is a stub.")
