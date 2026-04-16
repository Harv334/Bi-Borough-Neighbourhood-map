"""
Geo helpers shared by all fetchers.

Two main jobs:
  1. Coordinate transforms (BNG <-> WGS84)
  2. Point-in-polygon assignment of POIs to wards / LSOAs / boroughs
"""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from pyproj import Transformer
from shapely.geometry import Point, shape
from shapely.strtree import STRtree

# Reusable transformers (creating these is expensive, so cache)
_BNG_TO_WGS84 = Transformer.from_crs("EPSG:27700", "EPSG:4326", always_xy=True)
_WGS84_TO_BNG = Transformer.from_crs("EPSG:4326", "EPSG:27700", always_xy=True)


def bng_to_wgs84(easting: float, northing: float) -> tuple[float, float]:
    """British National Grid -> WGS84. Returns (lng, lat)."""
    return _BNG_TO_WGS84.transform(easting, northing)


def wgs84_to_bng(lng: float, lat: float) -> tuple[float, float]:
    """WGS84 -> British National Grid. Returns (easting, northing)."""
    return _WGS84_TO_BNG.transform(lng, lat)


# ─── Point-in-polygon assignment ──────────────────────────────────────────
class PolygonIndex:
    """Wraps a GeoJSON FeatureCollection for fast point-in-polygon lookup.

    Usage:
        idx = PolygonIndex.from_geojson_file("data/boundaries/lsoa.geojson")
        props = idx.find(lng=-0.14, lat=51.50)   # returns the LSOA's properties dict
    """

    def __init__(self, features: list[dict]):
        self._geoms = [shape(f["geometry"]) for f in features]
        self._props = [f["properties"] for f in features]
        self._tree = STRtree(self._geoms)

    @classmethod
    def from_geojson_file(cls, path: Path | str) -> "PolygonIndex":
        with open(path, encoding="utf-8") as f:
            fc = json.load(f)
        return cls(fc["features"])

    def find(self, lng: float, lat: float) -> dict | None:
        """Returns the first containing polygon's properties, or None."""
        pt = Point(lng, lat)
        for idx in self._tree.query(pt):
            if self._geoms[idx].contains(pt):
                return self._props[idx]
        return None


@lru_cache(maxsize=4)
def load_boundary(repo_root: str, kind: str) -> PolygonIndex:
    """Cached boundary loader. kind in {'lsoa', 'wards', 'boroughs'}."""
    path = Path(repo_root) / "data" / "boundaries" / f"{kind}.geojson"
    if not path.exists():
        raise FileNotFoundError(
            f"Boundary file not found: {path}\n"
            f"Run `pipeline run --source boundaries` first to generate it."
        )
    return PolygonIndex.from_geojson_file(path)
