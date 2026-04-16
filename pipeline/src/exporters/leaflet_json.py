"""
Leaflet JSON exporter — converts the Parquet stores into the JSON files the
existing index.html expects. Run as a final step after all fetchers complete.

Output shape:
    ward_data.json   – {WD25CD: {name, lad, indicators: {...}}, ...}
    lsoa_data.json   – {LSOA21CD: {name, ward, borough, imd, indicators: {...}}, ...}

Plus the spliced-into-index.html constants are regenerated:
    GJ          (188 wards GeoJSON)
    LSOA_IMD    (1313 LSOAs GeoJSON, with imd + key indicators on each feature)
    BOROUGH_GJ  (9 boroughs GeoJSON)

The map .html stays the same; only its embedded data blobs are refreshed.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd
from rich.console import Console

console = Console()


def _load_parquet_safe(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        console.print(f"[yellow]missing: {path}[/]")
        return None
    return pd.read_parquet(path)


def build_ward_data(repo_root: Path) -> dict:
    """Aggregate every ward-keyed dataset into a single ward_data.json."""
    out: dict[str, dict] = {}

    fingertips = _load_parquet_safe(repo_root / "data/outcomes/fingertips.parquet")
    if fingertips is not None:
        # TODO: pivot fingertips long-format into per-ward indicator dicts
        pass

    # GP counts per ward
    gps = _load_parquet_safe(repo_root / "data/healthcare/gp_practices.parquet")
    if gps is not None:
        per_ward = gps.groupby("WD25CD").size().to_dict()
        for wd, n in per_ward.items():
            out.setdefault(wd, {}).setdefault("indicators", {})["gp_practice_count"] = n

    return out


def build_lsoa_data(repo_root: Path) -> dict:
    """Aggregate every LSOA-keyed dataset into a single lsoa_data.json."""
    out: dict[str, dict] = {}

    imd = _load_parquet_safe(repo_root / "data/demographics/imd2025.parquet")
    if imd is not None:
        for _, row in imd.iterrows():
            out[row["LSOA21CD"]] = {
                "imd_decile": int(row["imd_decile"]) if pd.notna(row["imd_decile"]) else None,
                "imd_rank": int(row["imd_rank"]) if pd.notna(row["imd_rank"]) else None,
            }

    census = _load_parquet_safe(repo_root / "data/demographics/census2021.parquet")
    if census is not None:
        for _, row in census.iterrows():
            d = out.setdefault(row["LSOA21CD"], {})
            for col in row.index:
                if col == "LSOA21CD":
                    continue
                d[col] = row[col] if pd.notna(row[col]) else None

    return out


def write_leaflet_outputs(repo_root: Path) -> dict:
    """Write ward_data.json and lsoa_data.json to the repo root (where index.html
    expects to find them)."""
    repo_root = Path(repo_root)

    ward_data = build_ward_data(repo_root)
    lsoa_data = build_lsoa_data(repo_root)

    ward_path = repo_root / "ward_data.json"
    lsoa_path = repo_root / "lsoa_data.json"

    with open(ward_path, "w") as f:
        json.dump(ward_data, f)
    with open(lsoa_path, "w") as f:
        json.dump(lsoa_data, f)

    console.print(f"[green][OK][/] ward_data.json: {len(ward_data)} wards")
    console.print(f"[green][OK][/] lsoa_data.json: {len(lsoa_data)} LSOAs")

    return {
        "ward_data": str(ward_path.relative_to(repo_root)),
        "lsoa_data": str(lsoa_path.relative_to(repo_root)),
        "ward_count": len(ward_data),
        "lsoa_count": len(lsoa_data),
    }


def splice_index_html(repo_root: Path) -> None:
    """Re-splice the GP/HOSP/etc constants in index.html from the Parquet stores.

    This replaces the existing splice scripts in scripts_and_work/ — the pipeline
    is now the single way constants get updated.
    """
    repo_root = Path(repo_root)
    index_path = repo_root / "index.html"
    if not index_path.exists():
        console.print(f"[yellow]index.html not found at {index_path}; skipping splice[/]")
        return

    with open(index_path, encoding="utf-8") as f:
        html = f.read()

    # GPS array
    gps = _load_parquet_safe(repo_root / "data/healthcare/gp_practices.parquet")
    if gps is not None:
        gps_js = "const GPS = " + json.dumps(
            gps[["name", "addr", "lat", "lng", "postcode", "code", "ward", "lad", "tel"]]
            .rename(columns={"name": "n", "addr": "a", "postcode": "pc"})
            .to_dict(orient="records"),
            ensure_ascii=False,
        ) + ";"
        html = re.sub(
            r"const GPS = \[(?:\{[^\n]*\},?\s*)+\];",
            gps_js,
            html,
            count=1,
        )

    # HOSP array (similar)
    hosp = _load_parquet_safe(repo_root / "data/healthcare/hospitals.parquet")
    if hosp is not None:
        hosp_js = "const HOSP = " + json.dumps(
            hosp[["name", "addr", "lat", "lng", "type"]]
            .rename(columns={"name": "n", "addr": "a", "type": "t"})
            .to_dict(orient="records"),
            ensure_ascii=False,
        ) + ";"
        html = re.sub(
            r"const HOSP = \[(?:\{[^\n]*\},?\s*)+\];",
            hosp_js,
            html,
            count=1,
        )

    with open(index_path, "w", encoding="utf-8") as f:
        f.write(html)
    console.print(f"[green][OK][/] re-spliced index.html")
