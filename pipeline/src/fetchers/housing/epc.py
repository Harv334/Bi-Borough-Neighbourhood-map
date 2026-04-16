"""
Energy Performance Certificates (EPC) — Domestic certificates per LAD.

Source: https://epc.opendatacommunities.org/docs/api
Endpoint:
    GET https://epc.opendatacommunities.org/api/v1/domestic/search
        ?local-authority={ladcd}&size=5000

Auth: Bearer token via env var EPC_AUTH_TOKEN. Sign up free at
      https://epc.opendatacommunities.org/login.

Each row is one certificate. We aggregate to LSOA: count, mean SAP score,
% rated D or worse (proxy for fuel-poverty risk and damp/cold homes).

Note: a single dwelling can have multiple certificates over time. We keep the
most recent per UPRN.

Output: per-LSOA aggregates (default) and a row-level parquet (optional).
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pandas as pd
import requests

from ...core import BaseFetcher

API = "https://epc.opendatacommunities.org/api/v1/domestic/search"


class EpcFetcher(BaseFetcher):
    source_id = "epc_domestic"
    category = "housing"
    required_cols = ["LSOA21CD", "n_certs", "n_dwellings",
                     "mean_current_sap", "pct_d_or_worse"]

    def fetch_raw(self) -> pd.DataFrame:
        token = os.environ.get("EPC_AUTH_TOKEN")
        if not token:
            raise RuntimeError(
                "EPC_AUTH_TOKEN env var not set. Sign up at "
                "https://epc.opendatacommunities.org/login and add the token "
                "as a GitHub Actions secret named EPC_AUTH_TOKEN."
            )
        headers = {
            "Authorization": f"Basic {token}",
            "Accept": "application/json",
        }

        # Read NW London LADs from boroughs.yml
        from ...core.config import load_boroughs
        lads = load_boroughs(self.repo_root)

        frames: list[pd.DataFrame] = []
        for borough_name, ladcd in lads:
            cache = self.cache_dir / f"{ladcd}.csv"
            if not cache.exists():
                params = {"local-authority": ladcd, "size": 5000}
                # EPC API supports search-after pagination via the response header
                # "X-Next-Search-After"; for v1 of this pipeline we just take the first page.
                r = requests.get(API, params=params, headers=headers,
                                 timeout=120)
                r.raise_for_status()
                cache.write_bytes(r.content)
                time.sleep(1.0)
            try:
                df = pd.read_csv(cache)
            except (pd.errors.EmptyDataError, pd.errors.ParserError):
                # API returns CSV by default; if header was JSON, it will fail
                continue
            df["_lad"] = ladcd
            frames.append(df)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=self.required_cols)

        # The EPC CSV uses LSOA code under "lsoa-code" (sometimes "LSOA Code")
        lsoa_col = next((c for c in df.columns if c.lower().replace("-", "") == "lsoacode"), None)
        sap_col = next((c for c in df.columns if c.lower().replace("-", "") == "currentenergyefficiency"), None)
        rating_col = next((c for c in df.columns if c.lower().replace("-", "") == "currentenergyrating"), None)
        uprn_col = next((c for c in df.columns if c.lower() == "uprn"), None)
        date_col = next((c for c in df.columns if "lodgement" in c.lower()), None)

        if not lsoa_col or not sap_col or not rating_col:
            return pd.DataFrame(columns=self.required_cols)

        # Most recent cert per UPRN
        if uprn_col and date_col:
            df = df.sort_values(date_col).drop_duplicates(uprn_col, keep="last")

        out = df.groupby(lsoa_col).agg(
            n_certs=(sap_col, "size"),
            n_dwellings=(uprn_col, "nunique") if uprn_col else (sap_col, "size"),
            mean_current_sap=(sap_col, "mean"),
        ).reset_index().rename(columns={lsoa_col: "LSOA21CD"})

        # Pct D or worse (D, E, F, G)
        d_or_worse = df[df[rating_col].isin(["D", "E", "F", "G"])]
        denom = df.groupby(lsoa_col).size().rename("denom")
        numer = d_or_worse.groupby(lsoa_col).size().rename("numer")
        ratio = (numer / denom).fillna(0).rename("pct_d_or_worse").reset_index()
        ratio = ratio.rename(columns={lsoa_col: "LSOA21CD"})

        return out.merge(ratio, on="LSOA21CD", how="left")
