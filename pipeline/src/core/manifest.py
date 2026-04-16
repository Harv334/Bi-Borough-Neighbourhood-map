"""
Update manifest — tracks which fetcher last ran when, with what result.

Persisted as data/_meta/manifest.json so it's checked into the repo on every
pipeline run. That gives you a full audit trail in git history.
"""
from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .base import FetcherResult


def manifest_path(repo_root: Path) -> Path:
    return Path(repo_root) / "data" / "_meta" / "manifest.json"


def load(repo_root: Path) -> dict:
    p = manifest_path(repo_root)
    if not p.exists():
        return {"sources": {}, "last_run": None}
    with open(p) as f:
        return json.load(f)


def update(repo_root: Path, result: "FetcherResult") -> None:
    """Add/overwrite an entry for this fetcher, then persist."""
    m = load(repo_root)
    m["sources"][result.source_id] = {
        "rows_written": result.rows_written,
        "output_path": str(result.output_path.relative_to(repo_root)),
        "fetched_at": result.fetched_at,
        "duration_s": round(result.duration_s, 1),
        "schema": result.schema,
        "notes": result.notes,
    }
    m["last_run"] = datetime.now(timezone.utc).isoformat()

    p = manifest_path(repo_root)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w") as f:
        json.dump(m, f, indent=2, sort_keys=True)
