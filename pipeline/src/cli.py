"""
Pipeline CLI.

  pipeline run                       # run every enabled fetcher
  pipeline run --source gp_practices # run a single fetcher
  pipeline run --cadence daily       # run all enabled daily fetchers
  pipeline run --category healthcare # run all enabled fetchers in a category
  pipeline export                    # regenerate Leaflet JSON + Power BI index
  pipeline status                    # print the manifest

The single entry point GitHub Actions calls.
"""
from __future__ import annotations

import importlib
from pathlib import Path

import click
import yaml
from rich.console import Console
from rich.table import Table

from .core import manifest as manifest_mod
from .exporters.leaflet_json import splice_index_html, write_leaflet_outputs
from .exporters.powerbi_index import write_powerbi_index

console = Console()


def _repo_root() -> Path:
    """Walk upward from this file to find the repo root (the folder containing
    pipeline/ and data/)."""
    here = Path(__file__).resolve()
    for p in [here, *here.parents]:
        if (p / "pipeline").is_dir() and (p / "data").is_dir():
            return p
    raise RuntimeError("Could not locate repo root from " + str(here))


def _load_sources() -> list[dict]:
    with open(_repo_root() / "pipeline" / "conf" / "sources.yml") as f:
        return yaml.safe_load(f)["sources"]


def _import_fetcher(dotted_path: str):
    mod_path, cls_name = dotted_path.rsplit(".", 1)
    mod = importlib.import_module(mod_path)
    return getattr(mod, cls_name)


def _filter_sources(sources, source_id, cadence, category, only_enabled=True):
    out = []
    for s in sources:
        if only_enabled and not s.get("enabled", True):
            continue
        if source_id and s["id"] != source_id:
            continue
        if cadence and s.get("cadence") != cadence:
            continue
        if category and s.get("category") != category:
            continue
        out.append(s)
    return out


@click.group()
def main():
    """NW London Health Pipeline."""


@main.command()
@click.option("--source", "source_id", help="Run a single fetcher by id.")
@click.option("--cadence", help="daily / weekly / monthly / quarterly / annual / on_demand")
@click.option("--category", help="healthcare / outcomes / etc.")
@click.option("--no-export", is_flag=True, help="Skip leaflet JSON + Power BI index regeneration.")
def run(source_id, cadence, category, no_export):
    repo = _repo_root()
    config = _load_sources()
    targets = _filter_sources(config, source_id, cadence, category)

    if not targets:
        console.print("[red]No fetchers matched[/]")
        return

    console.print(f"Running {len(targets)} fetcher(s)")
    failed = []
    for src in targets:
        try:
            cls = _import_fetcher(src["module"])
            instance = cls(repo_root=repo, config=src)
            result = instance.run()
            manifest_mod.update(repo, result)
        except Exception as e:
            console.print(f"[red][FAIL][/] {src['id']}: {e}")
            failed.append(src["id"])

    if not no_export:
        console.rule("[bold]Exports[/]")
        write_leaflet_outputs(repo)
        splice_index_html(repo)
        write_powerbi_index(repo)

    if failed:
        console.print(f"[red]\nFailed sources: {failed}[/]")
        raise SystemExit(1)


@main.command()
def export():
    """Regenerate exports without running any fetchers."""
    repo = _repo_root()
    write_leaflet_outputs(repo)
    splice_index_html(repo)
    write_powerbi_index(repo)


@main.command()
def status():
    """Show the current manifest."""
    repo = _repo_root()
    m = manifest_mod.load(repo)
    if not m["sources"]:
        console.print("No sources have run yet.")
        return
    t = Table(title=f"Pipeline status (last run: {m['last_run']})")
    t.add_column("source")
    t.add_column("rows", justify="right")
    t.add_column("fetched_at")
    t.add_column("duration", justify="right")
    for sid, info in sorted(m["sources"].items()):
        t.add_row(sid, f"{info['rows_written']:,}", info["fetched_at"], f"{info['duration_s']}s")
    console.print(t)


if __name__ == "__main__":
    main()
