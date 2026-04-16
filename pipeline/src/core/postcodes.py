"""
ONSPD postcode lookup wrapper.

The pipeline downloads the latest ONSPD release from the ONS Open Geography Portal,
extracts only the NW London postcode-area CSVs (NW, W, WC, HA, UB, TW + N, SW edges),
and exposes a fast in-memory dict lookup: postcode -> (lat, lng, lsoa21cd, lad25cd, wd25cd).

Schema reference:
    https://www.arcgis.com/sharing/rest/content/items/<docid>/data
    See ONSPD User Guide PDF for column definitions.
"""
from __future__ import annotations

import csv
import io
import zipfile
from datetime import datetime
from functools import lru_cache
from pathlib import Path

import requests
from rich.console import Console

console = Console()

ONSPD_PORTAL = "https://geoportal.statistics.gov.uk"

# ONSPD per-area CSVs to extract (NW London catchment + edges)
ONSPD_AREAS = ["NW", "W", "WC", "HA", "UB", "TW", "N", "SW"]


def normalise_postcode(pc: str) -> str:
    """Normalise to no-spaces uppercase form ('NW1 6XE' -> 'NW16XE')."""
    return (pc or "").replace(" ", "").upper().strip()


def latest_onspd_url() -> str:
    """Return the URL of the most recent ONSPD release.

    ONS releases ONSPD quarterly (Feb/May/Aug/Nov). The current month/year is
    inferred from today's date — if you need a specific release pin it via env var
    ONSPD_URL_OVERRIDE.

    NOTE: Release naming on ArcGIS Online uses titles like
    "ONS Postcode Directory November 2025". The download URL is on the dataset's
    "About" page. This function returns the dataset *page* URL; the actual
    download URL is resolved at fetch time by scraping the page.

    For GitHub Actions, prefer hard-coding the latest known release in
    conf/sources.yml under env: ONSPD_URL.
    """
    import os

    if "ONSPD_URL_OVERRIDE" in os.environ:
        return os.environ["ONSPD_URL_OVERRIDE"]

    # Map month -> ONS quarterly release month
    month_to_release = {
        1: "november", 2: "november",
        3: "february", 4: "february",
        5: "may", 6: "may", 7: "may",
        8: "august", 9: "august", 10: "august",
        11: "november", 12: "november",
    }
    today = datetime.utcnow()
    release_month = month_to_release[today.month]
    release_year = today.year
    if today.month <= 2:
        release_year -= 1  # November release of previous year
    return (
        f"{ONSPD_PORTAL}/datasets/"
        f"ons-postcode-directory-{release_month}-{release_year}/about"
    )


class PostcodeLookup:
    """In-memory postcode -> coords + admin codes lookup."""

    def __init__(self):
        self._lookup: dict[str, tuple[float, float, str, str, str]] = {}

    def __len__(self) -> int:
        return len(self._lookup)

    def __contains__(self, pc: str) -> bool:
        return normalise_postcode(pc) in self._lookup

    def get(self, pc: str) -> tuple[float, float, str, str, str] | None:
        """Returns (lat, lng, lsoa21cd, lad25cd, wd25cd) or None."""
        return self._lookup.get(normalise_postcode(pc))

    @classmethod
    def from_zip(cls, zip_path: Path | str, areas: list[str] = None) -> "PostcodeLookup":
        """Build the lookup by reading the requested per-area CSVs from a downloaded
        ONSPD zip without unzipping the whole thing."""
        areas = areas or ONSPD_AREAS
        lk = cls()
        with zipfile.ZipFile(zip_path) as z:
            for member in z.namelist():
                # Match Data/multi_csv/ONSPD_<MONTH>_<YEAR>_UK_<AREA>.csv
                if not member.endswith(".csv"):
                    continue
                if "/multi_csv/" not in member:
                    continue
                # Extract area from filename
                stem = Path(member).stem  # e.g. ONSPD_NOV_2025_UK_NW
                parts = stem.split("_")
                area = parts[-1]
                if area not in areas:
                    continue

                with z.open(member) as raw:
                    text = io.TextIOWrapper(raw, encoding="utf-8")
                    r = csv.DictReader(text)
                    for row in r:
                        if row.get("doterm", "").strip():
                            continue  # terminated postcode
                        try:
                            lat = float(row["lat"])
                            lng = float(row["long"])
                        except (ValueError, TypeError):
                            continue
                        if lat == 99.999999:
                            continue  # ONSPD sentinel for "no grid ref"
                        pcd = normalise_postcode(row["pcds"])
                        lk._lookup[pcd] = (
                            lat,
                            lng,
                            row.get("lsoa21cd", ""),
                            row.get("lad25cd", ""),
                            row.get("wd25cd", ""),
                        )
        console.print(f"[dim]ONSPD: loaded {len(lk):,} active postcodes[/]")
        return lk


@lru_cache(maxsize=1)
def get_lookup(repo_root: str) -> PostcodeLookup:
    """Cached singleton. Looks for the latest ONSPD zip in .cache/onspd/."""
    cache = Path(repo_root) / ".cache" / "onspd"
    zips = sorted(cache.glob("ONSPD_*_UK.zip"))
    if not zips:
        raise FileNotFoundError(
            f"No ONSPD zip found in {cache}.\n"
            f"Run `pipeline fetch-onspd` to download the latest release, "
            f"or place a release zip there manually."
        )
    return PostcodeLookup.from_zip(zips[-1])
