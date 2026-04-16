"""
Index of Multiple Deprivation 2025 (IoD2025) — MHCLG.

Source:
    https://www.gov.uk/government/statistics/english-indices-of-deprivation-2025

Provides per-LSOA deprivation scores and ranks across 7 domains:
  - Income
  - Employment
  - Education, Skills & Training
  - Health & Disability
  - Crime
  - Barriers to Housing & Services
  - Living Environment

We pull "File 7: All ranks, deciles and scores for the indices of
deprivation, and population denominators" — the all-in-one workbook —
and keep the headline IMD score, decile, and rank for England.

Note: as of writing the official 2025 release URL is published on GOV.UK
under "english-indices-of-deprivation-2025"; the asset filename pattern
follows the 2019 release. Fall back to env override IMD_2025_URL if
the default location 404s.
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import requests

from ...core import BaseFetcher

DEFAULT_URL = (
    "https://assets.publishing.service.gov.uk/media/"
    "65f9c44b9c5b7f0011c0fa3a/File_7_-_All_IoD2025_Scores__Ranks__Deciles_and_Population_Denominators.xlsx"
)


class Imd2025Fetcher(BaseFetcher):
    source_id = "imd2025"
    category = "demographics"
    required_cols = ["LSOA21CD", "imd_score", "imd_decile", "imd_rank",
                     "income_score", "health_score", "crime_score"]

    def fetch_raw(self) -> pd.DataFrame:
        url = os.environ.get("IMD_2025_URL", DEFAULT_URL)
        cache = self.cache_dir / "imd2025.xlsx"
        if not cache.exists():
            r = requests.get(url, timeout=120)
            r.raise_for_status()
            cache.write_bytes(r.content)

        # The headline tab is usually "IoD2025 Scores" or "IMD". Find it.
        xl = pd.ExcelFile(cache)
        score_tab = next(
            (s for s in xl.sheet_names
             if "iod" in s.lower() or "imd" in s.lower() and "score" in s.lower()),
            xl.sheet_names[0],
        )
        return pd.read_excel(cache, sheet_name=score_tab)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=self.required_cols)

        # Normalise column names: MHCLG uses verbose headers
        cols = {c: c.strip() for c in df.columns}
        df = df.rename(columns=cols)

        code_col = next((c for c in df.columns if "lsoa" in c.lower() and "code" in c.lower()), None)
        if not code_col:
            return pd.DataFrame(columns=self.required_cols)

        score_col = next((c for c in df.columns
                          if "index of multiple deprivation" in c.lower() and "score" in c.lower()), None)
        decile_col = next((c for c in df.columns
                           if "decile" in c.lower() and "imd" in c.lower()), None)
        rank_col = next((c for c in df.columns
                         if "rank" in c.lower() and "imd" in c.lower()), None)
        income_col = next((c for c in df.columns
                           if "income" in c.lower() and "score" in c.lower()), None)
        health_col = next((c for c in df.columns
                           if "health" in c.lower() and "score" in c.lower()), None)
        crime_col = next((c for c in df.columns
                          if "crime" in c.lower() and "score" in c.lower()), None)

        out = pd.DataFrame({
            "LSOA21CD": df[code_col].astype(str).str.strip(),
            "imd_score": pd.to_numeric(df[score_col], errors="coerce") if score_col else pd.NA,
            "imd_decile": pd.to_numeric(df[decile_col], errors="coerce") if decile_col else pd.NA,
            "imd_rank": pd.to_numeric(df[rank_col], errors="coerce") if rank_col else pd.NA,
            "income_score": pd.to_numeric(df[income_col], errors="coerce") if income_col else pd.NA,
            "health_score": pd.to_numeric(df[health_col], errors="coerce") if health_col else pd.NA,
            "crime_score": pd.to_numeric(df[crime_col], errors="coerce") if crime_col else pd.NA,
        })
        return out.dropna(subset=["LSOA21CD"])
