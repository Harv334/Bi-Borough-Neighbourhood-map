"""
NHS general dental practitioners — same shape as pharmacies.
Source: NHS BSA dental contractor list, monthly.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import yaml

from ...core import BaseFetcher, get_lookup, normalise_postcode

NHSBSA_DENTAL = "https://www.nhsbsa.nhs.uk/sites/default/files/{ym}/dental_contractor_list.csv"


class DentistsFetcher(BaseFetcher):
    source_id = "dentists"
    category = "healthcare"
    required_cols = ["code", "name", "addr", "postcode", "lat", "lng",
                     "LSOA21CD", "WD25CD", "LAD25CD"]

    def fetch_raw(self) -> pd.DataFrame:
        cache = self.cache_dir / "dental_contractors.csv"
        if cache.exists():
            return pd.read_csv(cache, dtype=str)
        now = datetime.utcnow()
        for delta in range(0, 6):
            year = now.year if now.month - delta > 0 else now.year - 1
            month = (now.month - delta - 1) % 12 + 1
            ym = f"{year}-{month:02d}"
            r = requests.get(NHSBSA_DENTAL.format(ym=ym), timeout=30)
            if r.status_code == 200 and len(r.content) > 1000:
                cache.write_bytes(r.content)
                return pd.read_csv(cache, dtype=str)
        raise RuntimeError("Could not find recent dental_contractor_list.csv")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        lookup = get_lookup(str(self.repo_root))
        nw_lads = self._nw_lads()
        rows = []
        for _, r in df.iterrows():
            pc = normalise_postcode(r.get("Practice Postcode", ""))
            hit = lookup.get(pc)
            if not hit:
                continue
            lat, lng, lsoa, lad, wd = hit
            if lad not in nw_lads:
                continue
            rows.append({
                "code": r.get("Practice Code", ""),
                "name": (r.get("Practice Name") or "").title(),
                "addr": (r.get("Practice Address") or "").title(),
                "postcode": r.get("Practice Postcode", ""),
                "lat": lat, "lng": lng,
                "LSOA21CD": lsoa, "WD25CD": wd, "LAD25CD": lad,
            })
        return pd.DataFrame(rows)

    @staticmethod
    def _nw_lads() -> set[str]:
        repo_root = Path(__file__).resolve().parents[4]
        with open(repo_root / "pipeline" / "conf" / "boroughs.yml") as f:
            return {b["lad25cd"] for b in yaml.safe_load(f)["boroughs"]}
