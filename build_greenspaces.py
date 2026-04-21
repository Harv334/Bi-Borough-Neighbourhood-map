#!/usr/bin/env python3
"""
build_greenspaces.py
====================

One-shot builder: converts the OS Open Greenspace TQ tile shapefile into a
NW-London-clipped, simplified GeoJSON that the map toggles on/off.

Input:
    data/environment/opgrsp_essh_tq.zip
        — OS Open Greenspace ESRI Shape File, TQ 100km grid square
          (covers London, Kent, Surrey). Source:
          https://www.ordnancesurvey.co.uk/products/os-open-greenspace

Clip:
    Uses the embedded LSOA_IMD GeoJSON inside index.html to compute the NW
    London bbox. Any greenspace polygon that intersects this bbox (+500m
    buffer so edge parks stay whole) is retained.

Output:
    greenspaces.geojson  (at repo root; served next to index.html)
        Features carry: id, function, name (distName1), distName2..4 joined
        if present. Geometry simplified at 1.5 m tolerance (27700) to keep
        the web payload small without visibly degrading park outlines.

Run:
    python3 build_greenspaces.py
"""
from __future__ import annotations

import json
import re
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path

REPO = Path(__file__).resolve().parent
ZIP_PATH = REPO / "data" / "environment" / "opgrsp_essh_tq.zip"
HTML_PATH = REPO / "index.html"
OUT_PATH = REPO / "greenspaces.geojson"

# Bbox buffer so parks that straddle the NW London boundary aren't cut off
# mid-polygon — 500 m in BNG (EPSG:27700) translates to ~0.007 degrees.
BBOX_BUFFER_M = 500.0

# Simplify tolerance in metres (applied in EPSG:27700 before reprojection).
# 1.5 m keeps park outlines crisp at zoom 14+ while trimming ~30% of vertices.
SIMPLIFY_M = 1.5

# Categories we drop — these are tiny, noisy, or not health-relevant and
# mostly contribute filesize without decision-support value. Keep if you want
# a full-fidelity view.
DROP_FUNCTIONS = {
    "Tennis Court",
    "Bowling Green",
}


def _bbox_from_lsoa_imd(html: str) -> tuple[float, float, float, float]:
    """Extract the LSOA_IMD embedded GeoJSON and compute its bbox in 4326."""
    m = re.search(r"const\s+LSOA_IMD\s*=\s*(\{)", html)
    if not m:
        sys.exit("Could not find 'const LSOA_IMD = {' in index.html")
    start = m.end() - 1  # position of the opening brace
    depth = 0
    i = start
    in_str = False
    esc = False
    while i < len(html):
        c = html[i]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
        else:
            if c == '"':
                in_str = True
            elif c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    obj_json = html[start:i + 1]
                    gj = json.loads(obj_json)
                    xs, ys = [], []
                    for f in gj.get("features", []):
                        geom = f.get("geometry") or {}
                        coords = geom.get("coordinates")
                        if not coords:
                            continue
                        # Recursive walk to collect all lon/lat pairs
                        stack = [coords]
                        while stack:
                            item = stack.pop()
                            if (
                                isinstance(item, list)
                                and len(item) >= 2
                                and all(isinstance(x, (int, float)) for x in item[:2])
                            ):
                                xs.append(item[0])
                                ys.append(item[1])
                            elif isinstance(item, list):
                                stack.extend(item)
                    return min(xs), min(ys), max(xs), max(ys)
        i += 1
    sys.exit("Failed to parse LSOA_IMD object (brace mismatch)")


def main() -> None:
    try:
        import geopandas as gpd
        from shapely.geometry import box
    except ImportError:
        sys.exit(
            "geopandas + shapely required. Install with:\n"
            "  pip install geopandas shapely pyproj --break-system-packages"
        )

    if not ZIP_PATH.exists():
        sys.exit(f"{ZIP_PATH} not found")
    if not HTML_PATH.exists():
        sys.exit(f"{HTML_PATH} not found")

    # 1. NW London bbox in EPSG:4326, then buffered in 27700
    print(f"Reading LSOA_IMD bbox from {HTML_PATH.name} ...")
    html = HTML_PATH.read_text(encoding="utf-8")
    lon_min, lat_min, lon_max, lat_max = _bbox_from_lsoa_imd(html)
    print(f"  NW London bbox (4326): {lon_min:.4f},{lat_min:.4f} -> {lon_max:.4f},{lat_max:.4f}")

    bbox_4326 = gpd.GeoSeries([box(lon_min, lat_min, lon_max, lat_max)], crs="EPSG:4326")
    bbox_27700 = bbox_4326.to_crs("EPSG:27700").buffer(BBOX_BUFFER_M).iloc[0]
    print(f"  Buffered bbox (27700): {bbox_27700.bounds}")

    # 2. Unzip to tempdir & load shapefile
    with tempfile.TemporaryDirectory() as td:
        tdp = Path(td)
        print(f"Unzipping {ZIP_PATH.name} ...")
        with zipfile.ZipFile(ZIP_PATH) as z:
            for name in z.namelist():
                if "GreenspaceSite" in name:
                    z.extract(name, tdp)
        shp = next(tdp.rglob("TQ_GreenspaceSite.shp"), None)
        if shp is None:
            sys.exit("TQ_GreenspaceSite.shp not found in zip")
        print(f"  Loading {shp.name} ...")
        gdf = gpd.read_file(shp)
    print(f"  Loaded {len(gdf):,} features, CRS={gdf.crs}")

    # 3. Clip to NW London (spatial index accelerates this).
    print(f"Clipping to NW London bbox (+{BBOX_BUFFER_M:.0f} m buffer) ...")
    gdf = gdf[gdf.intersects(bbox_27700)].copy()
    print(f"  {len(gdf):,} features inside NW London bbox")

    # 4. Drop unwanted categories.
    if "function" in gdf.columns and DROP_FUNCTIONS:
        before = len(gdf)
        gdf = gdf[~gdf["function"].isin(DROP_FUNCTIONS)].copy()
        print(f"  Dropped {before - len(gdf):,} features in categories {sorted(DROP_FUNCTIONS)}")

    # 5. Simplify in 27700 (metres are intuitive), then reproject to 4326.
    print(f"Simplifying at {SIMPLIFY_M:.1f} m tolerance ...")
    gdf["geometry"] = gdf.geometry.simplify(SIMPLIFY_M, preserve_topology=True)
    gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notna()].copy()

    print("Reprojecting to EPSG:4326 ...")
    gdf = gdf.to_crs("EPSG:4326")

    # 6. Slim the attribute set. OS ships: id, function, distName1..4.
    wanted = [c for c in ("id", "function", "distName1", "distName2", "distName3", "distName4") if c in gdf.columns]
    gdf = gdf[wanted + ["geometry"]].copy()

    # Merge distName1..4 into a single display 'name' (join with ' / '),
    # dropping blanks. OS uses these for aliases (e.g. "Hyde Park" vs
    # "The Royal Parks"). Keep only the first as the primary name; expose
    # aliases as "aliases".
    def _primary(row):
        for c in ("distName1", "distName2", "distName3", "distName4"):
            v = row.get(c) if c in row else None
            if v and str(v).strip():
                return str(v).strip()
        return ""

    def _aliases(row):
        seen = []
        for c in ("distName1", "distName2", "distName3", "distName4"):
            v = row.get(c) if c in row else None
            if v and str(v).strip():
                s = str(v).strip()
                if s not in seen:
                    seen.append(s)
        return " / ".join(seen[1:]) if len(seen) > 1 else ""

    gdf["name"] = gdf.apply(_primary, axis=1)
    gdf["aliases"] = gdf.apply(_aliases, axis=1)
    keep_cols = ["id", "function", "name", "aliases", "geometry"]
    gdf = gdf[[c for c in keep_cols if c in gdf.columns]].copy()

    # Distribution summary
    if "function" in gdf.columns:
        print("Category breakdown:")
        for cat, n in gdf["function"].value_counts().items():
            print(f"  {n:>5}  {cat}")

    # 7. Write out. Use compact separators to shrink payload.
    print(f"Writing {OUT_PATH} ...")
    # GeoPandas' to_file uses Fiona which writes pretty GeoJSON. For a
    # smaller web payload, we write manually via __geo_interface__.
    feats = []
    for _, row in gdf.iterrows():
        geom = row.geometry.__geo_interface__ if row.geometry else None
        if geom is None:
            continue
        props = {
            "id": row.get("id", ""),
            "function": row.get("function", ""),
            "name": row.get("name", ""),
        }
        if row.get("aliases"):
            props["aliases"] = row["aliases"]
        feats.append({"type": "Feature", "properties": props, "geometry": geom})

    fc = {"type": "FeatureCollection", "features": feats}
    OUT_PATH.write_text(json.dumps(fc, separators=(",", ":")), encoding="utf-8")
    size_kb = OUT_PATH.stat().st_size / 1024
    print(f"  {len(feats):,} features written, {size_kb:,.0f} KB")


if __name__ == "__main__":
    main()
