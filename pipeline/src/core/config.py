"""
Configuration helpers — read project-level YAML config (boroughs, sources)
in one canonical place so individual fetchers don't reinvent the wheel.
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml


@lru_cache(maxsize=4)
def load_boroughs(repo_root: str | Path) -> list[tuple[str, str]]:
    """Returns [(borough_name, lad25cd), ...] for the in-scope LADs."""
    p = Path(repo_root) / "pipeline" / "conf" / "boroughs.yml"
    with open(p) as f:
        data = yaml.safe_load(f)
    return [(b["name"], b["lad25cd"]) for b in data.get("boroughs", [])]


@lru_cache(maxsize=4)
def load_borough_postcode_areas(repo_root: str | Path) -> list[str]:
    """Returns the list of postcode area letters in scope (HA, NW, ...)."""
    p = Path(repo_root) / "pipeline" / "conf" / "boroughs.yml"
    with open(p) as f:
        data = yaml.safe_load(f)
    return data.get("postcode_prefixes_all", [])


@lru_cache(maxsize=4)
def load_sources_config(repo_root: str | Path) -> dict:
    """Returns the parsed sources.yml dispatch table."""
    p = Path(repo_root) / "pipeline" / "conf" / "sources.yml"
    with open(p) as f:
        return yaml.safe_load(f)
