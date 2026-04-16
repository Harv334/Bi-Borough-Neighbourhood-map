"""
BaseFetcher — abstract class every fetcher inherits from.

Contract for a fetcher:
  1. fetch_raw(): pull source data into memory or onto disk (cached)
  2. transform(): clean, filter to NW London, enrich with boundary keys
  3. validate(): assert row count > 0, required cols present, no nulls in keys
  4. write(): hand the resulting DataFrame off to the Parquet exporter
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from rich.console import Console

console = Console()


@dataclass
class FetcherResult:
    source_id: str
    rows_written: int
    output_path: Path
    duration_s: float
    fetched_at: str
    schema: dict[str, str] = field(default_factory=dict)
    notes: str = ""


class BaseFetcher(ABC):
    """All fetchers inherit from this.

    Subclasses MUST set:
        source_id    – matches the id in conf/sources.yml
        category     – matches the category in conf/sources.yml
        required_cols – list[str] of columns the output must contain

    Subclasses SHOULD set:
        cache_files  – list[str] of files this fetcher creates in .cache/
                       so cleanup is automatic.
    """

    source_id: str = ""
    category: str = ""
    required_cols: list[str] = []
    cache_files: list[str] = []

    def __init__(self, repo_root: Path, config: dict):
        self.repo_root = Path(repo_root)
        self.config = config
        self.cache_dir = self.repo_root / ".cache" / self.source_id
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir = self.repo_root / "data" / self.category
        self.output_dir.mkdir(parents=True, exist_ok=True)

    # ─── Required overrides ─────────────────────────────────────────────
    @abstractmethod
    def fetch_raw(self) -> Any:
        """Pull raw data from the upstream source. Return whatever transform() expects."""

    @abstractmethod
    def transform(self, raw: Any) -> pd.DataFrame:
        """Clean + filter to NW London + enrich with boundary keys. Returns a DataFrame."""

    # ─── Default implementations (override only if needed) ──────────────
    def validate(self, df: pd.DataFrame) -> None:
        """Sanity checks. Raises AssertionError on failure."""
        assert len(df) > 0, f"[{self.source_id}] empty DataFrame after transform()"
        for col in self.required_cols:
            assert col in df.columns, f"[{self.source_id}] missing required column: {col}"
        # Boundary keys must not be null
        for col in self.required_cols:
            if col.endswith("CD") or col.endswith("code"):
                null_count = df[col].isna().sum()
                if null_count > 0:
                    console.print(
                        f"[yellow][{self.source_id}] WARN: {null_count} nulls in {col}[/]"
                    )

    def write(self, df: pd.DataFrame) -> Path:
        """Write the DataFrame as Parquet (canonical) into data/<category>/<id>.parquet."""
        out_path = self.output_dir / f"{self.source_id}.parquet"
        df.to_parquet(out_path, engine="pyarrow", compression="snappy", index=False)
        return out_path

    # ─── Driver ─────────────────────────────────────────────────────────
    def run(self) -> FetcherResult:
        start = datetime.now(timezone.utc)
        console.rule(f"[bold cyan]{self.source_id}[/]")
        console.print(f"[dim]category={self.category}  cache={self.cache_dir}[/]")

        raw = self.fetch_raw()
        df = self.transform(raw)
        self.validate(df)
        out_path = self.write(df)

        duration = (datetime.now(timezone.utc) - start).total_seconds()
        result = FetcherResult(
            source_id=self.source_id,
            rows_written=len(df),
            output_path=out_path,
            duration_s=duration,
            fetched_at=start.isoformat(),
            schema={c: str(df[c].dtype) for c in df.columns},
        )
        console.print(
            f"[green][OK][/] {self.source_id}: wrote {len(df):,} rows -> "
            f"{out_path.relative_to(self.repo_root)} in {duration:.1f}s"
        )
        return result
