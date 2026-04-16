"""
OHID Fingertips fetcher — REFERENCE IMPLEMENTATION for an API-based source.

Pulls a curated list of indicators (defined in fingertips_indicators.yml) at
Local Authority granularity for the 9 NW London LADs, and writes a long-format
Parquet (one row per LAD x indicator x time period).

API docs: https://fingertips.phe.org.uk/api
Endpoint structure:
  /api/all_data/csv/by_indicator_id?indicator_ids={id}&child_area_type_id=202&parent_area_type_id=15
  area_type_id=202 -> Districts and Unitary Authorities (post-Apr 2023)
  parent_area_code=E12000007 -> London region

The API is open and unauthenticated. Polite rate-limiting (1s between calls) is
applied because we hit it ~30+ times per run.
"""
from __future__ import annotations

import io
import time
from pathlib import Path

import pandas as pd
import requests
import yaml

from ...core import BaseFetcher


FT_BASE = "https://fingertips.phe.org.uk/api"
LONDON_REGION = "E12000007"
AREA_TYPE_LA = 502  # Upper-tier LAs (post-Apr 2023). Use 202 for districts.


class FingertipsFetcher(BaseFetcher):
    source_id = "fingertips"
    category = "outcomes"
    required_cols = [
        "LAD25CD", "indicator_id", "indicator_short", "indicator_name",
        "value", "lower_ci", "upper_ci", "period",
    ]

    def fetch_raw(self) -> list[tuple[dict, pd.DataFrame]]:
        """Fetch one CSV per indicator. Returns list of (indicator_meta, dataframe)."""
        ind_path = Path(__file__).parent / "fingertips_indicators.yml"
        with open(ind_path) as f:
            indicators = yaml.safe_load(f)["indicators"]

        results = []
        for ind in indicators:
            url = (
                f"{FT_BASE}/all_data/csv/by_indicator_id"
                f"?indicator_ids={ind['id']}"
                f"&child_area_type_id={AREA_TYPE_LA}"
                f"&parent_area_type_id=15"
            )
            cache = self.cache_dir / f"ind_{ind['id']}.csv"
            if not cache.exists():
                r = requests.get(url, timeout=30)
                r.raise_for_status()
                cache.write_bytes(r.content)
                time.sleep(1.0)  # rate limit

            try:
                df = pd.read_csv(cache, dtype=str, low_memory=False)
                results.append((ind, df))
            except pd.errors.EmptyDataError:
                continue
        return results

    def transform(self, raw: list[tuple[dict, pd.DataFrame]]) -> pd.DataFrame:
        nw_lads = self._nw_lads()
        out_rows = []
        for ind, df in raw:
            # Filter to NW London LAs and to the latest period available
            df = df[df["Area Code"].isin(nw_lads)]
            if df.empty:
                continue
            # Latest period per area
            df = df.sort_values("Time period Sortable").groupby("Area Code", as_index=False).tail(1)

            for _, row in df.iterrows():
                out_rows.append({
                    "LAD25CD": row["Area Code"],
                    "lad_name": row["Area Name"],
                    "indicator_id": int(ind["id"]),
                    "indicator_short": ind["short_name"],
                    "indicator_name": ind["description"],
                    "value": _to_float(row.get("Value")),
                    "lower_ci": _to_float(row.get("Lower CI 95.0 limit")),
                    "upper_ci": _to_float(row.get("Upper CI 95.0 limit")),
                    "period": row.get("Time period"),
                    "sex": row.get("Sex"),
                    "age": row.get("Age"),
                })

        out = pd.DataFrame(out_rows)
        # Drop the deliberate duplicate marker
        out = out[out["indicator_short"] != "life_expectancy_male_dup"]
        return out

    @staticmethod
    def _nw_lads() -> set[str]:
        repo_root = Path(__file__).resolve().parents[4]
        with open(repo_root / "pipeline" / "conf" / "boroughs.yml") as f:
            cfg = yaml.safe_load(f)
        return {b["lad25cd"] for b in cfg["boroughs"]}


def _to_float(v) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None
