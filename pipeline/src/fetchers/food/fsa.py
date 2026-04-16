"""
Food Standards Agency — Food Hygiene Rating Scheme (FHRS) per local authority.

Source: https://api.ratings.food.gov.uk/help
Endpoint:
    GET https://api.ratings.food.gov.uk/Establishments
        ?localAuthorityId={id}&pageSize=5000

We pull all establishments for each NW London LA. Each record has lat/lng,
business type, and a 0-5 rating (or "Awaiting Inspection"/"Exempt").

Rating <= 2 is the typical "fails" threshold. Useful as a proxy for food
environment quality (especially fast-food density × hygiene).

Output: one row per establishment.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import pandas as pd
import requests

from ...core import BaseFetcher
from ...core.geo import load_boundary

API = "https://api.ratings.food.gov.uk/Establishments"
HEADERS = {
    "x-api-version": "2",
    "accept": "application/json",
    "user-agent": "nw-london-health-pipeline/0.1 (+github.com/Harv334)",
}

# FSA assigns a numeric LocalAuthorityId per LA. These are the NW London ones.
NW_LONDON_LA_IDS = {
    "Brent": 196,
    "Camden": 197,
    "City of Westminster": 217,
    "Ealing": 198,
    "Hammersmith and Fulham": 199,
    "Harrow": 200,
    "Hillingdon": 202,
    "Hounslow": 203,
    "Kensington and Chelsea": 204,
}


class FsaFetcher(BaseFetcher):
    source_id = "fsa_food_hygiene"
    category = "food"
    required_cols = ["fhrsid", "business_name", "business_type",
                     "rating_value", "lat", "lng",
                     "LSOA21CD", "WD25CD", "LAD25CD"]

    def fetch_raw(self) -> list[dict]:
        out: list[dict] = []
        for la_name, la_id in NW_LONDON_LA_IDS.items():
            cache = self.cache_dir / f"la_{la_id}.json"
            if not cache.exists():
                params = {"localAuthorityId": la_id, "pageSize": 5000}
                r = requests.get(API, params=params, headers=HEADERS, timeout=120)
                r.raise_for_status()
                cache.write_bytes(r.content)
                time.sleep(0.5)
            try:
                data = json.loads(cache.read_text())
            except json.JSONDecodeError:
                continue
            for est in data.get("establishments", []):
                est["_la_name"] = la_name
                est["_la_id"] = la_id
            out.extend(data.get("establishments", []))
        return out

    def transform(self, raw: list[dict]) -> pd.DataFrame:
        lsoas = load_boundary(str(self.repo_root), "lsoa")
        wards = load_boundary(str(self.repo_root), "wards")
        boroughs = load_boundary(str(self.repo_root), "boroughs")

        rows = []
        for est in raw:
            geo = est.get("geocode") or {}
            try:
                lat = float(geo.get("latitude"))
                lng = float(geo.get("longitude"))
            except (TypeError, ValueError):
                continue
            b_hit = boroughs.find(lng, lat)
            if not b_hit:
                continue
            lsoa_p = lsoas.find(lng, lat) or {}
            ward_p = wards.find(lng, lat) or {}
            rows.append({
                "fhrsid": est.get("FHRSID"),
                "business_name": est.get("BusinessName", ""),
                "business_type": est.get("BusinessType", ""),
                "rating_value": est.get("RatingValue", ""),
                "rating_date": est.get("RatingDate", ""),
                "lat": lat, "lng": lng,
                "LSOA21CD": lsoa_p.get("code", ""),
                "WD25CD": ward_p.get("WD25CD") or ward_p.get("WD24CD") or "",
                "LAD25CD": b_hit.get("LAD25CD") or b_hit.get("LAD24CD") or "",
                "la_name": est.get("_la_name", ""),
            })
        return pd.DataFrame(rows)
