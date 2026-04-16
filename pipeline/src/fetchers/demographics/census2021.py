"""
Census 2021 — Bulk downloads from Nomis.

Source: https://www.nomisweb.co.uk/sources/census_2021_bulk
We grab a curated set of LSOA-level Topic Summaries (TSnnnn) tables and
derive a small per-LSOA demographic feature set:

  TS001  Number of usual residents
  TS017  Household composition
  TS021  Ethnic group
  TS037  General health
  TS038  Disability under the Equality Act
  TS066  Economic activity status

Each table is a ZIP containing a CSV per geography level. We pull the
LSOA-level CSVs.

Output: one wide row per LSOA with population, % aged 65+, % non-white,
% bad/very-bad health, % limiting disability, % economically inactive.
"""
from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pandas as pd
import requests

from ...core import BaseFetcher

NOMIS_BULK = "https://www.nomisweb.co.uk/output/census/2021"

TABLES = {
    "TS001": "ts001-2021-1.zip",  # population
    "TS017": "ts017-2021-1.zip",  # household composition
    "TS021": "ts021-2021-1.zip",  # ethnic group
    "TS037": "ts037-2021-1.zip",  # general health
    "TS038": "ts038-2021-1.zip",  # disability
    "TS066": "ts066-2021-1.zip",  # economic activity
}


class Census2021Fetcher(BaseFetcher):
    source_id = "census2021"
    category = "demographics"
    required_cols = ["LSOA21CD", "population", "pct_65_plus",
                     "pct_non_white", "pct_bad_health", "pct_disability_limiting"]

    def fetch_raw(self) -> dict[str, pd.DataFrame]:
        out: dict[str, pd.DataFrame] = {}
        for tab_id, fname in TABLES.items():
            cache = self.cache_dir / fname
            if not cache.exists():
                url = f"{NOMIS_BULK}/{fname}"
                r = requests.get(url, timeout=120)
                r.raise_for_status()
                cache.write_bytes(r.content)
            with zipfile.ZipFile(cache) as z:
                # Pick the LSOA-level CSV — name like "census2021-ts001-lsoa.csv"
                lsoa_csv = next(
                    (n for n in z.namelist() if "lsoa" in n.lower() and n.endswith(".csv")),
                    None,
                )
                if not lsoa_csv:
                    continue
                with z.open(lsoa_csv) as f:
                    out[tab_id] = pd.read_csv(f)
        return out

    def transform(self, raw: dict[str, pd.DataFrame]) -> pd.DataFrame:
        if not raw:
            return pd.DataFrame(columns=self.required_cols)

        # TS001 — total population
        df = raw.get("TS001", pd.DataFrame()).copy()
        code_col = _code_col(df)
        if code_col is None:
            return pd.DataFrame(columns=self.required_cols)
        pop_col = next((c for c in df.columns if "all usual residents" in c.lower()), None)
        out = pd.DataFrame({
            "LSOA21CD": df[code_col],
            "population": df[pop_col] if pop_col else 0,
        })

        # TS021 — % non-white = 1 - white
        eth = raw.get("TS021", pd.DataFrame()).copy()
        if not eth.empty:
            ec = _code_col(eth)
            white_col = next((c for c in eth.columns if "white" in c.lower() and "ethnic" in c.lower()), None)
            tot_col = next((c for c in eth.columns if "all" in c.lower() and "categor" in c.lower()), None)
            if ec and white_col and tot_col:
                eth["pct_non_white"] = 1 - (eth[white_col] / eth[tot_col].replace(0, pd.NA))
                out = out.merge(eth[[ec, "pct_non_white"]].rename(columns={ec: "LSOA21CD"}),
                                on="LSOA21CD", how="left")

        # TS037 — % bad/very bad health
        h = raw.get("TS037", pd.DataFrame()).copy()
        if not h.empty:
            hc = _code_col(h)
            bad = [c for c in h.columns if c.lower().endswith("bad health") or "very bad" in c.lower()]
            tot_col = next((c for c in h.columns if "all" in c.lower() and "usual" in c.lower()), None)
            if hc and bad and tot_col:
                h["pct_bad_health"] = h[bad].sum(axis=1) / h[tot_col].replace(0, pd.NA)
                out = out.merge(h[[hc, "pct_bad_health"]].rename(columns={hc: "LSOA21CD"}),
                                on="LSOA21CD", how="left")

        # TS038 — limiting disability
        d = raw.get("TS038", pd.DataFrame()).copy()
        if not d.empty:
            dc = _code_col(d)
            lim_cols = [c for c in d.columns if "limited" in c.lower() and "lot" in c.lower()]
            tot_col = next((c for c in d.columns if "all" in c.lower() and "usual" in c.lower()), None)
            if dc and lim_cols and tot_col:
                d["pct_disability_limiting"] = d[lim_cols].sum(axis=1) / d[tot_col].replace(0, pd.NA)
                out = out.merge(d[[dc, "pct_disability_limiting"]].rename(columns={dc: "LSOA21CD"}),
                                on="LSOA21CD", how="left")

        # %65+ comes from a different table (TS007 or TS001A). For v1 we leave NaN
        # and TODO add TS007A in next iteration.
        out["pct_65_plus"] = pd.NA

        return out


def _code_col(df: pd.DataFrame) -> str | None:
    if df.empty:
        return None
    return next((c for c in df.columns if c.lower() in ("geography code", "lsoa code", "geographycode")), None)
