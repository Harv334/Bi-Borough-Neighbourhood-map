"""
DESNZ — Sub-regional fuel poverty statistics (Low Income Low Energy
Efficiency / LILEE measure), per LSOA.

Source: https://www.gov.uk/government/collections/fuel-poverty-sub-regional-statistics
Currently the latest is the 2023 release (covering 2021 data); DESNZ refreshes
annually around February/March.

The published file is XLSX with one tab per geography level. The "Sub-regional
fuel poverty data 2023 - LSOA" tab is what we want.

Output: per-LSOA n_households, n_fuel_poor, pct_fuel_poor.
"""
from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pandas as pd
import requests

from ...core import BaseFetcher

# Latest DESNZ release URL — update annually. Stored here as a fallback if
# the env override DESNZ_FUEL_POVERTY_URL isn't set.
DEFAULT_URL = (
    "https://assets.publishing.service.gov.uk/media/"
    "65d2bc14e1bdec0011fc4be4/sub-regional-fuel-poverty-2024-tables.xlsx"
)


class FuelPovertyFetcher(BaseFetcher):
    source_id = "fuel_poverty"
    category = "housing"
    required_cols = ["LSOA21CD", "n_households", "n_fuel_poor", "pct_fuel_poor"]

    def fetch_raw(self) -> pd.DataFrame:
        import os
        url = os.environ.get("DESNZ_FUEL_POVERTY_URL", DEFAULT_URL)
        cache = self.cache_dir / "fuel_poverty.xlsx"
        if not cache.exists():
            r = requests.get(url, timeout=120)
            r.raise_for_status()
            cache.write_bytes(r.content)

        # Try to load the LSOA tab. Tab naming varies year to year — find one
        # that mentions "LSOA".
        xl = pd.ExcelFile(cache)
        lsoa_tab = next((s for s in xl.sheet_names if "LSOA" in s.upper()), None)
        if not lsoa_tab:
            raise RuntimeError(
                f"No LSOA tab found in DESNZ workbook; available: {xl.sheet_names}"
            )
        return pd.read_excel(cache, sheet_name=lsoa_tab, header=None)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # The header row is somewhere in the first ~5 rows; find it by looking
        # for "LSOA" and "households" together.
        header_row = None
        for i in range(min(8, len(df))):
            row_text = " ".join(str(x) for x in df.iloc[i].fillna("")).lower()
            if "lsoa" in row_text and ("household" in row_text or "code" in row_text):
                header_row = i
                break
        if header_row is None:
            raise RuntimeError("Could not locate header row in DESNZ workbook")

        df.columns = df.iloc[header_row].astype(str).tolist()
        df = df.iloc[header_row + 1:].copy()

        # Find the right columns by name (DESNZ rename them every year)
        code_col = next((c for c in df.columns if "lsoa" in c.lower() and "code" in c.lower()), None)
        nh_col = next((c for c in df.columns if "household" in c.lower() and "number" in c.lower()), None)
        nfp_col = next((c for c in df.columns
                        if ("fuel poor" in c.lower() or "fuel-poor" in c.lower())
                        and "number" in c.lower()), None)
        pct_col = next((c for c in df.columns
                        if "proportion" in c.lower() or "percentage" in c.lower()
                        or "%" in c.lower()), None)

        if not code_col:
            raise RuntimeError(f"No LSOA code column found; cols={list(df.columns)}")

        out = pd.DataFrame({"LSOA21CD": df[code_col].astype(str).str.strip()})
        out["n_households"] = pd.to_numeric(df[nh_col], errors="coerce") if nh_col else None
        out["n_fuel_poor"] = pd.to_numeric(df[nfp_col], errors="coerce") if nfp_col else None
        out["pct_fuel_poor"] = pd.to_numeric(df[pct_col], errors="coerce") if pct_col else None
        return out.dropna(subset=["LSOA21CD"])
