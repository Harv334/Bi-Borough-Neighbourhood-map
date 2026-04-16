"""
NHS Hospital sites — pulled from the NHS website datasets (ETS.csv).

Source: https://www.nhs.uk/about-us/nhs-website-datasets/
ETS.csv contains every hospital site in England with native lat/lng (no ONSPD lookup needed).

Filter: only sites within the 9 NW London LADs (point-in-polygon against borough boundary).
"""
from __future__ import annotations

import io

import pandas as pd
import requests

from ...core import BaseFetcher
from ...core.geo import load_boundary

# NHS ETS bulk download URL (CSV-Pipe-Delimited; updated weekly by NHS England)
ETS_URL = "https://www.nhs.uk/aboutNHSChoicesProfilesData/Hospital.csv"


class HospitalsFetcher(BaseFetcher):
    source_id = "hospitals"
    category = "healthcare"
    required_cols = ["code", "name", "addr", "postcode", "lat", "lng", "type"]

    def fetch_raw(self) -> pd.DataFrame:
        cache = self.cache_dir / "hospital.csv"
        if not cache.exists():
            r = requests.get(ETS_URL, timeout=60)
            r.raise_for_status()
            cache.write_bytes(r.content)
        # NHS files are pipe-delimited and ¬-quoted ("¬" = chr 172)
        return pd.read_csv(cache, sep="¬", dtype=str, encoding="latin-1", engine="python")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        boroughs = load_boundary(str(self.repo_root), "boroughs")

        rows = []
        for _, row in df.iterrows():
            try:
                lat = float(row.get("Latitude", ""))
                lng = float(row.get("Longitude", ""))
            except (TypeError, ValueError):
                continue
            hit = boroughs.find(lng, lat)
            if not hit:
                continue
            rows.append({
                "code": row.get("OrganisationCode", ""),
                "name": (row.get("OrganisationName", "") or "").title(),
                "addr": ", ".join(filter(None, [
                    (row.get("Address1") or "").title(),
                    (row.get("Address2") or "").title(),
                    (row.get("Address3") or "").title(),
                ])),
                "postcode": row.get("Postcode", ""),
                "lat": lat,
                "lng": lng,
                "type": row.get("OrganisationType") or "Hospital",
                "subtype": row.get("SubType", ""),
                "lad": hit.get("name", ""),
            })
        return pd.DataFrame(rows)
