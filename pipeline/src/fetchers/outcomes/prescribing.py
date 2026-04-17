"""
OpenPrescribing API — practice-level prescribing data, useful as a proxy for
chronic disease prevalence and prescribing patterns.

Source: https://openprescribing.net/api/
Docs:   https://openprescribing.net/api/1.0/

We pull a small curated set of indicators per NW London CCG/sub-ICB:
  - Items per 1000 patients (raw, all BNF chapters)
  - Antidepressant items (BNF 4.3)
  - Antibiotic items (BNF 5.1)
  - Hypertension drugs (BNF 2.5)
  - Diabetes drugs (BNF 6.1.2)

Output: one row per (sicbl_code, bnf_section, month) with cost, items, quantity,
list_size, items_per_1000.
"""
from __future__ import annotations

import time
from pathlib import Path

import pandas as pd
import requests

from ...core import BaseFetcher, browser_session

# OpenPrescribing measure IDs we care about
MEASURES = {
    "antidepressants_total": "Antidepressants — total items",
    "antibiotics_total": "Antibiotics — total items",
    "ace_arb": "ACE inhibitor / ARB items (hypertension)",
    "diabetes_drugs": "Antidiabetic drugs — total items",
    "opioidper1000": "High-dose opioid prescribing",
    "statins": "Statins — total items",
}

# NW London — sub-ICB Locations (formerly CCGs).
# Codes are stable ODS codes; the OpenPrescribing slug uses the ODS code lowercased.
NW_LONDON_SICBLS = {
    "W2U3Z": "NHS NW London ICB — Brent",
    "W2U3Z_HARROW": "NHS NW London ICB — Harrow",
    "W2U3Z_EALING": "NHS NW London ICB — Ealing",
    # OpenPrescribing now uses single ICB W2U3Z covering all NW London;
    # finer borough breakdowns come via PCN slugs (E38000xxx).
}

ICB_CODE = "QRV"  # NHS North West London ICB

API_BASE = "https://openprescribing.net/api/1.0"


class PrescribingFetcher(BaseFetcher):
    source_id = "prescribing"
    category = "outcomes"
    required_cols = ["org_code", "org_name", "measure_id", "month", "numerator",
                     "denominator", "calc_value", "percentile"]

    def fetch_raw(self) -> pd.DataFrame:
        """Pull a measure timeseries from each measure for the NW London ICB."""
        frames: list[pd.DataFrame] = []
        for measure_id, label in MEASURES.items():
            cache = self.cache_dir / f"{measure_id}__{ICB_CODE}.csv"
            if not cache.exists():
                url = (
                    f"{API_BASE}/measure_by_sicbl/"
                    f"?org={ICB_CODE}&measure={measure_id}&format=csv"
                )
                r = self._sess.get(url, timeout=60)
                r.raise_for_status()
                cache.write_bytes(r.content)
                time.sleep(0.5)  # be polite
            try:
                df = pd.read_csv(cache)
                df["measure_id"] = measure_id
                df["measure_label"] = label
                frames.append(df)
            except pd.errors.EmptyDataError:
                continue
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        # Normalise column names from OpenPrescribing's API
        rename = {
            "org_id": "org_code",
            "date": "month",
            "value": "calc_value",
        }
        df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

        keep = [c for c in self.required_cols if c in df.columns] + ["measure_label"]
        return df[keep].copy()
