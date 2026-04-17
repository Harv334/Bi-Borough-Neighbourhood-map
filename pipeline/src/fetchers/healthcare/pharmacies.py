"""
NHS BSA dispensing pharmacies (epharmacylist).

The NHS BSA publishes a monthly CSV of every dispensing contractor in England.
Postcode-only — geocode via ONSPD.

Bulk URL pattern:
  https://www.nhsbsa.nhs.uk/sites/default/files/<YYYY>-<MM>/edispensary.csv

Because the URL changes monthly, we discover the latest by walking back month-by-month.
"""
from __future__ import annotations

import io
from datetime import datetime

import pandas as pd
import requests
import yaml
from pathlib import Path

from ...core import BaseFetcher, browser_session, get_lookup, normalise_postcode

NHSBSA_PATTERN = "https://www.nhsbsa.nhs.uk/sites/default/files/{ym}/edispensary.csv"


class PharmaciesFetcher(BaseFetcher):
    _session = None

    @property
    def _sess(self):
        if type(self)._session is None:
            type(self)._session = browser_session(referer="https://www.nhsbsa.nhs.uk/")
        return type(self)._session

    source_id = "pharmacies"
    category = "healthcare"
    required_cols = ["code", "name", "addr", "postcode", "lat", "lng",
                     "LSOA21CD", "WD25CD", "LAD25CD"]

    def fetch_raw(self) -> pd.DataFrame:
        cache = self.cache_dir / "edispensary.csv"
        if cache.exists():
            return pd.read_csv(cache, dtype=str)

        # Walk back up to 6 months looking for a published file
        now = datetime.utcnow()
        for delta in range(0, 6):
            year = now.year if now.month - delta > 0 else now.year - 1
            month = (now.month - delta - 1) % 12 + 1
            ym = f"{year}-{month:02d}"
            url = NHSBSA_PATTERN.format(ym=ym)
            r = self._sess.get(url, timeout=30)
            if r.status_code == 200 and len(r.content) > 1000:
                cache.write_bytes(r.content)
                return pd.read_csv(cache, dtype=str)
        raise RuntimeError("Could not find a recent NHS BSA edispensary.csv")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        lookup = get_lookup(str(self.repo_root))
        nw_lads = self._nw_lads()

        rows = []
        for _, r in df.iterrows():
            pc = normalise_postcode(r.get("Postcode", ""))
            hit = lookup.get(pc)
            if not hit:
                continue
            lat, lng, lsoa, lad, wd = hit
            if lad not in nw_lads:
                continue
            rows.append({
                "code": r.get("Pharmacy Code") or r.get("Code") or "",
                "name": (r.get("Pharmacy Name") or r.get("Name") or "").title(),
                "addr": ", ".join(filter(None, [
                    (r.get("Address1") or "").title(),
                    (r.get("Address2") or "").title(),
                    (r.get("Town") or "").title(),
                ])),
                "postcode": r.get("Postcode", ""),
                "tel": r.get("Telephone", ""),
                "lat": lat, "lng": lng,
                "LSOA21CD": lsoa, "WD25CD": wd, "LAD25CD": lad,
            })
        return pd.DataFrame(rows)

    @staticmethod
    def _nw_lads() -> set[str]:
        repo_root = Path(__file__).resolve().parents[4]
        with open(repo_root / "pipeline" / "conf" / "boroughs.yml") as f:
            cfg = yaml.safe_load(f)
        return {b["lad25cd"] for b in cfg["boroughs"]}
