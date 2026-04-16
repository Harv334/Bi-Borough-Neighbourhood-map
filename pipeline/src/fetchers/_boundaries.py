"""
Boundaries refresher — pulls authoritative LSOA, ward, and LAD GeoJSONs from
ONS Open Geography Portal and writes them to data/boundaries/.

Source:  https://geoportal.statistics.gov.uk/
Vintages used (UK):
  LSOA   — 2021 Boundaries Generalised Clipped (BGC) Dec 2021
  Wards  — May 2025 BGC
  LADs   — May 2025 BGC

Filtered to NW London (the 9 LADs in conf/boroughs.yml).

This fetcher MUST run before any other fetcher whose `transform()` calls
`load_boundary()`, because all of those expect data/boundaries/{lsoa,wards,
boroughs}.geojson to exist.

Output: data/boundaries/{lsoa,wards,boroughs}.geojson
        + a stub parquet at data/boundaries/_meta.parquet so the BaseFetcher
        contract holds (one parquet per source).
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import requests

from ..core import BaseFetcher, load_boroughs

ARCGIS_BASE = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services"

# These FeatureServer URLs are stable for the named layer/version. If ONS
# republishes a new vintage, update the layer name (e.g. WD24 -> WD25).
LAYERS = {
    "lsoa": (
        f"{ARCGIS_BASE}/Lower_layer_Super_Output_Areas_Dec_2021_Boundaries_Generalised_Clipped_EW_BGC_V3"
        "/FeatureServer/0/query"
    ),
    "wards": (
        f"{ARCGIS_BASE}/Wards_May_2025_Boundaries_UK_BGC"
        "/FeatureServer/0/query"
    ),
    "boroughs": (
        f"{ARCGIS_BASE}/Local_Authority_Districts_May_2025_Boundaries_UK_BGC"
        "/FeatureServer/0/query"
    ),
}

# WHERE clause per layer to keep only NW London features
def _where(lads: list[str], code_field: str) -> str:
    return code_field + " IN (" + ",".join(f"'{c}'" for c in lads) + ")"


class BoundariesFetcher(BaseFetcher):
    source_id = "boundaries"
    category = "geo"
    required_cols = ["kind", "n_features", "out_path"]

    def fetch_raw(self) -> dict[str, dict]:
        lads = [code for _, code in load_boroughs(str(self.repo_root))]
        # LSOA 2021 layer has no LAD code field — filter via LSOA21NM prefix instead.
        # LSOA names on ONS layers are "<Borough Name> NNNA", e.g. "Brent 001A".
        lsoa_name_prefixes = [
            "Brent", "Camden", "Ealing",
            "Hammersmith and Fulham", "Harrow", "Hillingdon",
            "Hounslow", "Kensington and Chelsea", "Westminster",
        ]
        lsoa_where = " OR ".join(
            f"LSOA21NM LIKE '{p}%'" for p in lsoa_name_prefixes
        )

        out: dict[str, dict] = {}
        for kind, url in LAYERS.items():
            cache = self.cache_dir / f"{kind}.geojson"
            if not cache.exists():
                params = {
                    "outFields": "*",
                    "f": "geojson",
                    "outSR": "4326",
                }
                if kind in ("boroughs", "wards"):
                    # LAD25CD is present on both May 2025 layers
                    params["where"] = _where(lads, "LAD25CD")
                else:  # lsoa
                    params["where"] = lsoa_where
                # ArcGIS REST has a default 2,000 feature page size — paginate
                features = _paginate_arcgis(url, params)
                cache.write_text(json.dumps({"type": "FeatureCollection", "features": features}))
            out[kind] = json.loads(cache.read_text())
        return out

    def transform(self, raw: dict[str, dict]) -> pd.DataFrame:
        # Write each FeatureCollection to data/boundaries/<kind>.geojson
        boundaries_dir = Path(self.repo_root) / "data" / "boundaries"
        boundaries_dir.mkdir(parents=True, exist_ok=True)

        rows = []
        for kind, fc in raw.items():
            out_path = boundaries_dir / f"{kind}.geojson"
            out_path.write_text(json.dumps(fc, separators=(",", ":")))
            rows.append({
                "kind": kind,
                "n_features": len(fc.get("features", [])),
                "out_path": str(out_path.relative_to(self.repo_root)),
            })
        return pd.DataFrame(rows)


def _paginate_arcgis(url: str, params: dict) -> list[dict]:
    """Pull all features from an ArcGIS REST endpoint, paginating."""
    out: list[dict] = []
    offset = 0
    while True:
        p = dict(params)
        p["resultOffset"] = offset
        p["resultRecordCount"] = 2000
        r = requests.get(url, params=p, timeout=120)
        r.raise_for_status()
        data = r.json()
        feats = data.get("features", [])
        if not feats:
            break
        out.extend(feats)
        if not data.get("exceededTransferLimit"):
            break
        offset += len(feats)
    return out
