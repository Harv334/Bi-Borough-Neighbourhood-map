"""
OS Open Greenspace — parks, sports pitches, allotments, playgrounds.

Source: https://osdatahub.os.uk/downloads/open/OpenGreenspace
The bulk download is a ~80MB zip containing GeoPackage + Shapefile.

Because the OS Data Hub requires a free signup to get a programmatic download
URL, the auto-update path is:
  1. GitHub Actions runs `pipeline fetch-greenspace --token $OS_DATA_HUB_TOKEN`
  2. The token is stored as a repo Secret in GitHub (Settings -> Secrets)
  3. If the token isn't present, the fetcher falls back to a cached upload
     in .cache/greenspace/

Output: polygon centroids per greenspace, with type and area, joined to LSOA/ward.
"""
from __future__ import annotations

import os
import zipfile
from pathlib import Path

import pandas as pd
import requests
import shapefile  # pyshp

from ...core import BaseFetcher, bng_to_wgs84
from ...core.geo import load_boundary

OS_DOWNLOAD_API = (
    "https://api.os.uk/downloads/v1/products/OpenGreenspace/downloads"
    "?area=GB&format=ESRI%C2%AE+Shapefile&redirect"
)


class GreenspaceFetcher(BaseFetcher):
    source_id = "greenspace"
    category = "environment"
    required_cols = ["id", "name", "type", "area_m2", "lat", "lng",
                     "LSOA21CD", "WD25CD", "LAD25CD"]

    def fetch_raw(self) -> Path:
        """Returns the path to the extracted shapefile dir."""
        cache_zip = self.cache_dir / "opngrnspc.zip"
        if not cache_zip.exists():
            token = os.environ.get("OS_DATA_HUB_TOKEN")
            if not token:
                raise RuntimeError(
                    "OS_DATA_HUB_TOKEN env var not set and no cached zip in "
                    f"{cache_zip}. Sign up free at osdatahub.os.uk and add the "
                    f"key as a GitHub Actions secret."
                )
            url = f"{OS_DOWNLOAD_API}&key={token}"
            r = requests.get(url, timeout=120)
            r.raise_for_status()
            cache_zip.write_bytes(r.content)
        # Extract once
        extract_dir = self.cache_dir / "extracted"
        if not extract_dir.exists():
            with zipfile.ZipFile(cache_zip) as z:
                z.extractall(extract_dir)
        return extract_dir

    def transform(self, raw_dir: Path) -> pd.DataFrame:
        # Find the GreenspaceSite shapefile
        shp_path = next(raw_dir.rglob("GreenspaceSite.shp"), None)
        if shp_path is None:
            raise FileNotFoundError("GreenspaceSite.shp not found in extracted OS data")

        boroughs = load_boundary(str(self.repo_root), "boroughs")
        lsoas = load_boundary(str(self.repo_root), "lsoa")
        wards = load_boundary(str(self.repo_root), "wards")

        rows = []
        with shapefile.Reader(str(shp_path)) as r:
            field_names = [f[0] for f in r.fields[1:]]
            for shape_rec in r.iterShapeRecords():
                rec = dict(zip(field_names, shape_rec.record))
                # OS Greenspace is in BNG (EPSG:27700) — get centroid in BNG, transform
                pts = shape_rec.shape.points
                if not pts:
                    continue
                cx = sum(p[0] for p in pts) / len(pts)
                cy = sum(p[1] for p in pts) / len(pts)
                lng, lat = bng_to_wgs84(cx, cy)
                if not boroughs.find(lng, lat):
                    continue
                lsoa_p = lsoas.find(lng, lat) or {}
                ward_p = wards.find(lng, lat) or {}
                rows.append({
                    "id": rec.get("id"),
                    "name": rec.get("distName1") or rec.get("name") or "",
                    "type": rec.get("function") or "",
                    "area_m2": _polygon_area(pts),
                    "lat": lat, "lng": lng,
                    "LSOA21CD": lsoa_p.get("code", ""),
                    "WD25CD": ward_p.get("WD25CD") or ward_p.get("WD24CD") or "",
                    "LAD25CD": "",  # filled at join time if needed
                })
        return pd.DataFrame(rows)


def _polygon_area(points: list[tuple[float, float]]) -> float:
    """Shoelace formula in BNG metres = result in m²."""
    n = len(points)
    if n < 3:
        return 0.0
    s = 0.0
    for i in range(n):
        x1, y1 = points[i]
        x2, y2 = points[(i + 1) % n]
        s += (x1 * y2) - (x2 * y1)
    return abs(s) / 2.0
