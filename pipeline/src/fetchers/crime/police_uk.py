"""
data.police.uk — recorded street-level crime per LAD per month.

Source: https://data.police.uk/docs/method/crimes-street/
Endpoint:
    GET https://data.police.uk/api/crimes-street/all-crime
        ?poly=lat1,lng1:lat2,lng2:...&date=YYYY-MM

Free, no auth. The polygon-mode endpoint accepts any closed lat/lng polygon
and returns ALL recorded crimes inside it for the month. We use the borough
(LAD) polygon for each NW London borough.

Limit: ~10,000 crimes per request. NW London boroughs are usually under that
per month; if not, we'd need to split.

Output: one row per crime (category, location_type, lat, lng, street, date,
borough, joined to LSOA/ward via point-in-polygon).
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import pandas as pd
import requests

from ...core import BaseFetcher
from ...core.geo import load_boundary

API = "https://data.police.uk/api/crimes-street/all-crime"


class PoliceUkFetcher(BaseFetcher):
    source_id = "police_uk_crime"
    category = "crime"
    required_cols = ["category", "lat", "lng", "month",
                     "LSOA21CD", "WD25CD", "LAD25CD", "borough_name"]

    # How many recent months to pull. The API only retains last ~36 months.
    months_back: int = 12

    def fetch_raw(self) -> list[dict]:
        boroughs = load_boundary(str(self.repo_root), "boroughs")

        # Build list of (borough_name, polygon_string) tuples
        polys = []
        for feat in boroughs.features:
            props = feat.get("properties", {})
            name = props.get("LAD25NM") or props.get("name") or ""
            ring = feat["geometry"]["coordinates"][0]
            if isinstance(ring[0][0], list):  # multipolygon — flatten first ring
                ring = ring[0]
            # data.police.uk wants lat,lng pairs colon-separated
            poly_str = ":".join(f"{p[1]:.5f},{p[0]:.5f}" for p in ring[::5])  # subsample
            polys.append((name, props.get("LAD25CD", ""), poly_str))

        months = _recent_months(self.months_back)
        all_crimes: list[dict] = []
        for name, lad_cd, poly in polys:
            for ym in months:
                cache = self.cache_dir / f"{lad_cd}__{ym}.json"
                if not cache.exists():
                    try:
                        r = requests.post(
                            API, data={"poly": poly, "date": ym}, timeout=60
                        )
                        r.raise_for_status()
                        cache.write_bytes(r.content)
                        time.sleep(0.5)
                    except requests.HTTPError:
                        continue
                try:
                    data = json.loads(cache.read_text())
                except json.JSONDecodeError:
                    continue
                for c in data:
                    c["_borough_name"] = name
                    c["_borough_code"] = lad_cd
                    c["_month"] = ym
                all_crimes.extend(data)
        return all_crimes

    def transform(self, raw: list[dict]) -> pd.DataFrame:
        lsoas = load_boundary(str(self.repo_root), "lsoa")
        wards = load_boundary(str(self.repo_root), "wards")

        rows = []
        for c in raw:
            loc = c.get("location") or {}
            try:
                lat = float(loc.get("latitude"))
                lng = float(loc.get("longitude"))
            except (TypeError, ValueError):
                continue
            lsoa_p = lsoas.find(lng, lat) or {}
            ward_p = wards.find(lng, lat) or {}
            rows.append({
                "category": c.get("category", ""),
                "lat": lat, "lng": lng,
                "month": c.get("_month", ""),
                "street_name": (loc.get("street") or {}).get("name", ""),
                "LSOA21CD": lsoa_p.get("code", ""),
                "WD25CD": ward_p.get("WD25CD") or ward_p.get("WD24CD") or "",
                "LAD25CD": c.get("_borough_code", ""),
                "borough_name": c.get("_borough_name", ""),
            })
        return pd.DataFrame(rows)


def _recent_months(n: int) -> list[str]:
    """Return last n months as YYYY-MM strings, lagged by 2 months (publication delay)."""
    today = pd.Timestamp.utcnow().normalize()
    out = []
    for i in range(2, 2 + n):
        d = today - pd.DateOffset(months=i)
        out.append(d.strftime("%Y-%m"))
    return out
