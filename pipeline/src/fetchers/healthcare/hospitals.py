"""
NHS Hospital sites — pulled from the NHS website datasets.

Source (landing page, discoverable):
  https://www.nhs.uk/about-us/nhs-website-datasets/

The dataset file itself has moved a few times over the years. We try a list of
candidate URLs with a browser-like session (NHS CDN sometimes 403s a bare
python-requests UA).

ETS.csv / Hospital.csv is pipe-delimited (|) or chr(172)-delimited depending
on the era; we probe both. Native lat/lng, so no ONSPD lookup needed.

Filter: only sites inside one of the 9 NW London LADs (point-in-polygon
against the boroughs GeoJSON).
"""
from __future__ import annotations

import io

import pandas as pd

from ...core import BaseFetcher, browser_session
from ...core.geo import load_boundary

# Candidate URLs, tried in order. First to return HTTP 200 + >1KB wins.
CANDIDATE_URLS = [
    # Current (as of 2025) — NHS website data portal
    "https://media.nhswebsite.nhs.uk/data/foi/Hospital.csv",
    "https://media.nhswebsite.nhs.uk/data/foi/ETS.csv",
    # Older path — NHS Choices profiles data
    "https://www.nhs.uk/aboutNHSChoicesProfilesData/Hospital.csv",
]


def _try_parse(raw_bytes):
    """Probe both pipe- and chr(172)-delimited parsers."""
    for sep in ["¬", "|", ","]:
        try:
            df = pd.read_csv(
                io.BytesIO(raw_bytes),
                sep=sep,
                dtype=str,
                encoding="latin-1",
                engine="python",
                on_bad_lines="skip",
            )
        except Exception:
            continue
        if "Latitude" in df.columns or "latitude" in df.columns:
            return df
    return None


class HospitalsFetcher(BaseFetcher):
    _session = None

    @property
    def _sess(self):
        if type(self)._session is None:
            type(self)._session = browser_session(
                referer="https://www.nhs.uk/about-us/nhs-website-datasets/"
            )
        return type(self)._session

    source_id = "hospitals"
    category = "healthcare"
    required_cols = ["code", "name", "addr", "postcode", "lat", "lng", "type"]

    def fetch_raw(self) -> pd.DataFrame:
        cache = self.cache_dir / "hospital.csv"
        if cache.exists():
            df = _try_parse(cache.read_bytes())
            if df is not None:
                return df
            cache.unlink()

        tried = []
        for url in CANDIDATE_URLS:
            try:
                r = self._sess.get(url, timeout=60)
            except Exception as e:
                tried.append(f"  {url}  -> {type(e).__name__}: {e}")
                continue
            tried.append(f"  {url}  -> HTTP {r.status_code} ({len(r.content)}B)")
            if r.status_code != 200 or len(r.content) < 1024:
                continue
            df = _try_parse(r.content)
            if df is None:
                continue
            cache.write_bytes(r.content)
            return df

        raise RuntimeError(
            "Could not fetch NHS hospital dataset from any candidate URL.\n"
            "Attempts:\n" + "\n".join(tried) + "\n"
            "Discover the current URL at "
            "https://www.nhs.uk/about-us/nhs-website-datasets/ and add it to "
            "CANDIDATE_URLS in this file."
        )

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        boroughs = load_boundary(str(self.repo_root), "boroughs")

        cols = {c.lower(): c for c in df.columns}
        def col(name):
            return cols.get(name.lower())

        rows = []
        for _, row in df.iterrows():
            try:
                lat = float(row.get(col("Latitude") or "Latitude", "") or "")
                lng = float(row.get(col("Longitude") or "Longitude", "") or "")
            except (TypeError, ValueError):
                continue
            hit = boroughs.find(lng, lat)
            if not hit:
                continue
            rows.append({
                "code": row.get(col("OrganisationCode") or "OrganisationCode", ""),
                "name": (row.get(col("OrganisationName") or "OrganisationName", "") or "").title(),
                "addr": ", ".join(filter(None, [
                    (row.get(col("Address1") or "Address1") or "").title(),
                    (row.get(col("Address2") or "Address2") or "").title(),
                    (row.get(col("Address3") or "Address3") or "").title(),
                ])),
                "postcode": row.get(col("Postcode") or "Postcode", ""),
                "lat": lat,
                "lng": lng,
                "type": row.get(col("OrganisationType") or "OrganisationType") or "Hospital",
                "subtype": row.get(col("SubType") or "SubType", ""),
                "lad": hit.get("name", ""),
            })
        return pd.DataFrame(rows)
