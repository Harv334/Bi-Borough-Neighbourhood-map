# Pipeline Troubleshooting

Running the pipeline end-to-end pulls from ~18 upstream sources. Many of these
are government data portals with flaky URLs, rotating dataset names, or
anti-bot protection. This doc explains what breaks, why, and what to do.

## Quick triage

If `pipeline run` (or `scripts\refresh.bat`) is exploding, the most common
causes are:

1. **Python isn't installed, or the Microsoft Store stub is in PATH.**  Run
   `py -3 --version`. If you get _"Python was not found; run without arguments
   to install from the Microsoft Store"_ you've hit the stub — install real
   Python from python.org (or `winget install -e --id Python.Python.3.12`) and
   turn off the App execution alias (Settings → Apps → Advanced app settings
   → App execution aliases → both "python.exe" entries off).
2. **The sandbox has no network.**  This pipeline makes outbound HTTPS calls
   to 15+ different hosts.  A corporate proxy or firewall will 403 them.  Run
   it from a machine with unrestricted internet.
3. **A specific fetcher 403s / 404s.**  Per-fetcher status below.

## Per-fetcher status (as of 2026-04)

| Source | Category | Status | Notes |
| --- | --- | --- | --- |
| `gp_practices` | healthcare | fixed | NHS Digital ZIP.  Was 403ing on bare python-requests UA; now wraps a `browser_session` with full Chrome headers + Referer. |
| `pharmacies` | healthcare | fixed | NHS BSA edispensary.  URL rotates monthly; fetcher walks back 6 months.  Uses `browser_session`. |
| `hospitals` | healthcare | fixed | Old NHS Choices URL is 404.  Now probes `media.nhswebsite.nhs.uk` + fallbacks.  Uses `browser_session`.  Also parses ¬/\|/, separators (the file has shipped all three at various times). |
| `dentists` | healthcare | URL dead | ODS/NHS Digital dentist CSV moved.  Needs a visit to https://digital.nhs.uk/services/organisation-data-service to find the current download URL, then drop into `dentists.py`. |
| `care_homes` | healthcare | URL dead | CQC publishes a monthly "Care directory with filters" XLSX.  URL rotates; add candidate list of recent months like pharmacies does. |
| `fingertips` | outcomes | fixed | The YAML config was parsing broken (unquoted colons inside `description:` values).  All descriptions now quoted. |
| `prescribing` | outcomes | fixed | OpenPrescribing BigQuery exports.  Added `browser_session` with Referer=openprescribing.net. |
| `census2021` | demographics | needs research | Nomis bulk download URLs changed when ONS migrated the portal.  Need to rediscover the current `NM_*` table IDs from https://www.nomisweb.co.uk/. |
| `imd2025` | demographics | URL dead | MHCLG published IMD 2025 (was 2019 rebaseline).  The `imd2025.py` fetcher points at a URL that was provisional pre-release.  Replace with the GOV.UK statistics collection URL once published. |
| `police_uk` | crime | fixed | Fetcher called `.features` on `PolygonIndex`; class was missing that attribute.  Added `@property features` returning GeoJSON-shape features. |
| `air_quality_aurn` | environment | needs research | Defra AURN site changed its CSV endpoint.  Needs fresh URLs from https://uk-air.defra.gov.uk/data/data_selector_service. |
| `greenspace` | environment | needs API key | OS Open Greenspace requires `OS_DATA_HUB_TOKEN` env var.  Get a free token at https://osdatahub.os.uk/. |
| `laei` | environment | flaky | LAEI (London Atmospheric Emissions Inventory) ZIP is hosted on data.london.gov.uk; works most of the time but occasionally 504s.  Retry. |
| `fsa` | food | logic bug | Empty-df handling crashes when a LAD returns zero hygiene-rated establishments.  Guard with `if df.empty: return df`. |
| `epc` | housing | needs API key | EPC Open Data requires an `EPC_AUTH_TOKEN` env var.  Register at https://epc.opendatacommunities.org/. |
| `fuel_poverty` | housing | URL dead | BEIS/DESNZ fuel poverty LSOA-level dataset moved.  Find current URL at https://www.gov.uk/government/collections/fuel-poverty-sub-regional-statistics. |
| `ptal` | transport | URL dead | TfL PTAL CSV URL rotated.  New URL lives under https://content.tfl.gov.uk/ptal-*.  Update `PTAL_URL` in `ptal.py`. |
| `stations` | transport | flaky | TfL unified-API.  Occasional 500s — transient, retry in a few minutes. |

## Patterns / conventions introduced this week

**`browser_session` helper** (in `pipeline/src/core/base.py`) — returns a
`requests.Session` with Chrome 124 headers.  Use it from any fetcher hitting
a CDN that rejects bare python-requests:

```python
from ...core import BaseFetcher, browser_session

class MyFetcher(BaseFetcher):
    _session = None

    @property
    def _sess(self):
        if type(self)._session is None:
            type(self)._session = browser_session(referer="https://example.gov.uk/")
        return type(self)._session

    def fetch_raw(self):
        r = self._sess.get(url, timeout=60)
        ...
```

**Candidate URL lists** — for sources where the dataset URL rotates monthly or
has shifted hosts, keep a list of URLs (newest first) and probe each in order
until one returns 200 + reasonable byte count.  `pharmacies.py` (rotates by
month) and `hospitals.py` (rotates by host) are the reference implementations.

**YAML gotcha** — any value containing `: ` (colon + space) must be quoted, or
PyYAML will try to parse it as a nested mapping.  Example that breaks:

```yaml
description: Year 6: Prevalence of obesity   # BAD
```

Fix:

```yaml
description: "Year 6: Prevalence of obesity"  # OK
```

## When a fetcher fails mid-run

The pipeline is designed to keep going when a single fetcher fails — each
fetcher writes its own Parquet and has no hard dependency on the others
(except boundaries, which must run first).  So a failure on e.g. `fsa` does
not block `gp_practices`.

Check `pipeline status` (or `pipeline run --status`) after a run for the
green/red grid.  Rerun individual sources with `pipeline run --source fsa`.

## When Cloudflare 403s you

NHS/gov.uk CDNs increasingly use Cloudflare Bot Management.  Symptoms:
instant 403 with "Just a moment..." HTML body.  Fixes in order of effort:

1. Use `browser_session` (sets Chrome-like headers + Referer).  Fixes most.
2. Add `Sec-CH-UA*` client-hint headers to the session.  Fixes some more.
3. Swap `requests` → `httpx` with HTTP/2 enabled.  Fixes most remaining.
4. As a last resort, use `curl_cffi` (chromium TLS fingerprint impersonation).
   Bigger dependency; reserve for sources that refuse everything else.
