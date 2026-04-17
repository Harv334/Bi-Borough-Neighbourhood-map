"""
Geometry helpers:
  - BNG <-> WGS84 coordinate conversion
  - PolygonIndex for point-in-polygon lookup (STRtree-backed)
  - load_boundary(): cached access to wards/lsoa/boroughs GeoJSON
"""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from pyproj import Transformer
from shapely.geometry import Point, shape
from shapely.strtree import STRtree

_BNG_TO_WGS84 = Transformer.from_crs(27700, 4326, always_xy=True)
_WGS84_TO_BNG = Transformer.from_crs(4326, 27700, always_xy=True)


def bng_to_wgs84(e: float, n: float) -> tuple[float, float]:
    lng, lat = _BNG_TO_WGS84.transform(e, n)
    return lat, lng


def wgs84_to_bng(lat: float, lng: float) -> tuple[float, float]:
    return _WGS84_TO_BNG.transform(lng, lat)


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
    def from_geojson_file(cls, path) -> "PolygonIndex":
        with open(path, encoding="utf-8") as f:
            fc = json.load(f)
        return cls(fc["features"])

    def find(self, lng: float, lat: float):
        """Returns the first containing polygon's properties, or None."""
        pt = Point(lng, lat)
        for idx in self._tree.query(pt):
            if self._geoms[idx].contains(pt):
                return self._props[idx]
        return None

    @property
    def features(self) -> list[dict]:
        """Reconstruct GeoJSON-style features for callers that need geometry +
        properties together (e.g. police_uk polygon-string builder)."""
        from shapely.geometry import mapping
        return [
            {"type": "Feature", "geometry": mapping(g), "properties": p}
            for g, p in zip(self._geoms, self._props)
        ]


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
