"""
Power BI index generator.

Produces data/_meta/powerbi_index.csv — a flat list of every Parquet output with
its raw GitHub URL, suitable for pasting into Power BI's "Get Data > Web" picker
or for use with the M-language `Csv.Document(Web.Contents(url))` pattern.

Once the pipeline has run at least once and committed to GitHub, Power BI users
can connect to this index and load any (or all) of the datasets without needing
direct repo access.
"""
from __future__ import annotations

import csv
from pathlib import Path

import yaml
from rich.console import Console

console = Console()


GITHUB_RAW_BASE = (
    "https://raw.githubusercontent.com/Harv334/Bi-Borough-Neighbourhood-map/main"
)


def write_powerbi_index(repo_root: Path) -> Path:
    repo_root = Path(repo_root)
    sources_yml = repo_root / "pipeline" / "conf" / "sources.yml"
    out_path = repo_root / "data" / "_meta" / "powerbi_index.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with open(sources_yml) as f:
        cfg = yaml.safe_load(f)

    rows = []
    for src in cfg["sources"]:
        rel = f"data/{src['category']}/{src['id']}.parquet"
        local_path = repo_root / rel
        rows.append({
            "id": src["id"],
            "category": src["category"],
            "cadence": src["cadence"],
            "join_keys": ", ".join(src.get("join_keys", [])),
            "parquet_url": f"{GITHUB_RAW_BASE}/{rel}",
            "exists_locally": "Y" if local_path.exists() else "N",
            "rows": _count_rows(local_path) if local_path.exists() else "",
            "source_url": src.get("source_url", ""),
        })

    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)

    console.print(f"[green][OK][/] Power BI index: {out_path.relative_to(repo_root)}")
    return out_path


def _count_rows(parquet_path: Path) -> int:
    try:
        import pyarrow.parquet as pq
        return pq.ParquetFile(parquet_path).metadata.num_rows
    except Exception:
        return -1
