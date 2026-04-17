"""
Leaflet JSON exporter - converts the Parquet stores into the JSON files the
existing index.html expects. Run as a final step after all fetchers complete.

Output shape:
    ward_data.json   - {WD25CD: {name, lad, indicators: {...}}, ...}
    lsoa_data.json   - {LSOA21CD: {name, ward, borough, imd, indicators: {...}}, ...}
    pharmacies.json  - [{n, a, pc, tel, lat, lng, lad, lsoa, ward}, ...]

Plus the spliced-into-index.html constants are regenerated (GPS, HOSP arrays).
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd
from rich.console import Console

console = Console()


def _load_parquet_safe(path: Path):
    if not path.exists():
        console.print(f"[yellow]missing: {path}[/]")
        return None
    return pd.read_parquet(path)


def build_ward_data(repo_root: Path) -> dict:
    out: dict[str, dict] = {}

    fingertips = _load_parquet_safe(repo_root / "data/outcomes/fingertips.parquet")
    if fingertips is not None:
        # TODO: pivot fingertips long-format into per-ward indicator dicts
        pass

    gps = _load_parquet_safe(repo_root / "data/healthcare/gp_practices.parquet")
    if gps is not None and "WD25CD" in gps.columns:
        per_ward = gps.groupby("WD25CD").size().to_dict()
        for wd, n in per_ward.items():
            out.setdefault(wd, {}).setdefault("indicators", {})["gp_practice_count"] = int(n)

    return out


def build_lsoa_data(repo_root: Path) -> dict:
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


def build_pharmacies_json(repo_root: Path) -> list:
    """Convert pharmacies parquet to the JSON shape the map expects."""
    pharm = _load_parquet_safe(repo_root / "data/healthcare/pharmacies.parquet")
    if pharm is None:
        return []
    keep = ["name", "addr", "postcode", "tel", "lat", "lng", "LAD25CD", "LSOA21CD", "WD25CD"]
    cols = [c for c in keep if c in pharm.columns]
    df = pharm[cols].rename(columns={
        "name": "n", "addr": "a", "postcode": "pc",
        "LAD25CD": "lad", "LSOA21CD": "lsoa", "WD25CD": "ward",
    })
    return df.to_dict(orient="records")


def write_leaflet_outputs(repo_root: Path) -> dict:
    """Write ward_data.json, lsoa_data.json and pharmacies.json to the repo root."""
    repo_root = Path(repo_root)

    ward_data = build_ward_data(repo_root)
    lsoa_data = build_lsoa_data(repo_root)
    pharmacies = build_pharmacies_json(repo_root)

    ward_path = repo_root / "ward_data.json"
    lsoa_path = repo_root / "lsoa_data.json"
    pharm_path = repo_root / "pharmacies.json"

    with open(ward_path, "w") as f:
        json.dump(ward_data, f)
    with open(lsoa_path, "w") as f:
        json.dump(lsoa_data, f)
    with open(pharm_path, "w") as f:
        json.dump(pharmacies, f, separators=(",", ":"))

    console.print(f"[green][OK][/] ward_data.json: {len(ward_data)} wards")
    console.print(f"[green][OK][/] lsoa_data.json: {len(lsoa_data)} LSOAs")
    console.print(f"[green][OK][/] pharmacies.json: {len(pharmacies)} pharmacies")

    return {
        "ward_data": str(ward_path.relative_to(repo_root)),
        "lsoa_data": str(lsoa_path.relative_to(repo_root)),
        "pharmacies": str(pharm_path.relative_to(repo_root)),
        "ward_count": len(ward_data),
        "lsoa_count": len(lsoa_data),
        "pharmacy_count": len(pharmacies),
    }


def splice_index_html(repo_root: Path) -> None:
    """Re-splice the GPS/HOSP constants in index.html from the Parquet stores."""
    repo_root = Path(repo_root)
    index_path = repo_root / "index.html"
    if not index_path.exists():
        console.print(f"[yellow]index.html not found at {index_path}; skipping splice[/]")
        return

    with open(index_path, encoding="utf-8") as f:
        html = f.read()

    gps = _load_parquet_safe(repo_root / "data/healthcare/gp_practices.parquet")
    if gps is not None:
        cols = [c for c in ["name", "addr", "lat", "lng", "postcode", "code", "ward", "lad", "tel"]
                if c in gps.columns]
        gps_js = "const GPS = " + json.dumps(
            gps[cols].rename(columns={"name": "n", "addr": "a", "postcode": "pc"})
               .to_dict(orient="records"),
            ensure_ascii=False,
        ) + ";"
        html = re.sub(
            r"const GPS = \[(?:\{[^\n]*\},?\s*)+\];",
            gps_js,
            html,
            count=1,
        )

    hosp = _load_parquet_safe(repo_root / "data/healthcare/hospitals.parquet")
    if hosp is not None:
        cols = [c for c in ["name", "addr", "lat", "lng", "type"] if c in hosp.columns]
        hosp_js = "const HOSP = " + json.dumps(
            hosp[cols].rename(columns={"name": "n", "addr": "a", "type": "t"})
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
