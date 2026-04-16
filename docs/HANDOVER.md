# Handover — NW London Population Health Pipeline

This document is the single source of truth for whoever takes over the
pipeline. If something's not in here, please add it.

## What this repo does

It pulls 18+ public population-health datasets (NHS, ONS, OHID, MHCLG,
DEFRA, OS, TfL, FSA, MHCLG, Met Police) on a schedule and writes them as
Parquet files into `data/`. The interactive Leaflet map at the repo root
(`index.html`) is regenerated automatically from those Parquets, and a
`data/_meta/powerbi_index.csv` is published so anyone with Power BI can
plug straight in.

The whole thing runs on GitHub Actions cron — there is no server, no
database, no manual trigger needed. It will keep itself up to date for
years.

## Repo layout

```
.
├── index.html                   # the Leaflet map (regenerated from data/)
├── data/                        # canonical Parquet outputs (committed)
│   ├── boundaries/              # LSOA/ward/borough GeoJSONs (run boundaries first!)
│   ├── healthcare/*.parquet
│   ├── outcomes/*.parquet
│   ├── demographics/*.parquet
│   ├── environment/*.parquet
│   ├── transport/*.parquet
│   ├── crime/*.parquet
│   ├── food/*.parquet
│   ├── housing/*.parquet
│   └── _meta/
│       ├── manifest.json        # last refresh timestamp + row counts per source
│       ├── powerbi_index.csv    # all parquets exposed for Power BI
│       └── ward_data.json       # derived for the Leaflet map
│       └── lsoa_data.json
├── pipeline/
│   ├── pyproject.toml
│   ├── conf/
│   │   ├── boroughs.yml         # 9 in-scope LADs + postcode prefixes
│   │   └── sources.yml          # the dispatch table — add new sources here
│   └── src/
│       ├── core/                # BaseFetcher, geo, postcodes, manifest, config
│       ├── fetchers/
│       │   ├── _boundaries.py   # MUST run first
│       │   ├── healthcare/
│       │   ├── outcomes/
│       │   ├── demographics/
│       │   ├── environment/
│       │   ├── transport/
│       │   ├── crime/
│       │   ├── food/
│       │   └── housing/
│       ├── exporters/
│       │   ├── leaflet_json.py  # writes ward_data.json + lsoa_data.json
│       │   └── powerbi_index.py # writes powerbi_index.csv
│       └── cli.py               # the `pipeline` command
└── .github/workflows/
    ├── pipeline-daily.yml
    ├── pipeline-weekly.yml
    ├── pipeline-monthly.yml
    ├── pipeline-quarterly.yml
    ├── pipeline-annual.yml
    ├── pipeline-on-demand.yml
    └── pipeline-ci.yml
```

## Cron schedule (UTC)

| Cadence    | When                                | Sources                                    |
|------------|-------------------------------------|--------------------------------------------|
| daily      | 05:30 every day                     | air_quality_aurn                           |
| weekly     | Mon 06:00                           | fsa_food_hygiene                           |
| monthly    | 5th 04:00                           | gp, hospitals, pharmacies, dentists, care_homes, fingertips, prescribing, police_uk_crime |
| quarterly  | 8 Jan/Apr/Jul/Oct 04:00             | greenspace, tfl_stations, epc_domestic     |
| annual     | 1 March 04:00                       | census2021, imd2025, ptal, fuel_poverty    |

Manual: any source any time via the Actions tab → "Pipeline (on demand)".

## Required GitHub Secrets

| Secret               | Why                                       | How to get          |
|----------------------|-------------------------------------------|---------------------|
| `OS_DATA_HUB_TOKEN`  | OS Open Greenspace bulk download          | osdatahub.os.uk free signup |
| `EPC_AUTH_TOKEN`     | EPC API (housing energy efficiency)        | epc.opendatacommunities.org free signup |

If a secret is missing the corresponding fetcher errors but the rest of
the pipeline keeps running.

## What "running" looks like

A typical monthly run on GitHub Actions:

1. `actions/checkout@v4`
2. `pip install -e pipeline/`
3. `pipeline run --source boundaries`  (idempotent — only fetches if the
    ONS Open Geography Portal has a newer vintage)
4. `pipeline run --cadence monthly`
   - For each enabled monthly fetcher: download → transform → validate
   - On success, write `data/<category>/<id>.parquet` and update
     `data/_meta/manifest.json`
5. Exporters run automatically:
   - `pipeline.src.exporters.leaflet_json.write_leaflet_outputs()`
   - `pipeline.src.exporters.leaflet_json.splice_index_html()` —
     regenerates the `GPS = [...]` and `HOSP = [...]` arrays in
     `index.html` from current parquets
   - `pipeline.src.exporters.powerbi_index.write_powerbi_index()`
6. `git-auto-commit-action` commits the changed parquets/json/html.

## Common operations

### Refresh one source right now
GitHub → Actions → "Pipeline (on demand)" → Run workflow → enter source id
(e.g. `gp_practices`).

### Disable a broken source
Edit `pipeline/conf/sources.yml` and set `enabled: false`. Push. The next
scheduled run will skip it.

### Add a new source
See `docs/ADDING_A_FETCHER.md`.

### Use the data in Power BI
See `docs/POWERBI.md`.

### Check what last ran successfully
```sh
pipeline status
```
or look at `data/_meta/manifest.json` directly.

## Troubleshooting

**A workflow failed.** Check the Actions tab → click the failed run →
expand the failed step. The most common failures are:

- **Upstream URL changed** — the gov.uk asset path can shift when DESNZ
  / MHCLG re-publish. Fix by updating the `DEFAULT_URL` constant in the
  fetcher (or set the override env var documented in the fetcher's
  docstring) and re-running.
- **Rate limit / 429** — fetchers have polite sleeps but bursts can
  trip API limits. Re-run from "Pipeline (on demand)".
- **Boundaries missing** — if a fetcher complains about
  `data/boundaries/lsoa.geojson not found`, run `pipeline run --source
  boundaries` first.

**The map is out of date.** Check whether the parquets in `data/` were
updated by the last run. If yes but `index.html` wasn't, the
`splice_index_html()` step probably failed silently — re-run the
exporter only:
```sh
pipeline export --leaflet
```

## Architecture decisions worth knowing

- **Single source of truth**: `pipeline/conf/sources.yml` drives
  everything. Adding a source = one yaml entry + one Python class.
- **Per-source caching**: each fetcher has a `.cache/<id>/` dir. A
  re-run of the same fetcher re-uses cached downloads unless they're
  deleted. This keeps GitHub Actions runs fast (and friendly to
  upstream APIs).
- **Parquet over CSV**: ~10× smaller in git, native to Power BI / Pandas
  / DuckDB, preserves dtypes. We never commit raw JSON/CSV to the repo
  beyond the cache.
- **LSOA21 is the join key**: every dataset that can be aggregated to
  LSOA gets joined on `LSOA21CD`. This is the smallest stable geography
  the ONS publishes.
- **Geographies vintage**: LADs and wards are May 2025 (current at
  build time), LSOAs are 2021 (won't change until next census).
- **Boundary fetcher must run first**: it writes
  `data/boundaries/{lsoa,wards,boroughs}.geojson` which all spatial
  fetchers depend on.

## Who to ask

This was originally built by Harvey (sevilleharvey@gmail.com).
After Harvey, the maintainer is whoever has push access to the repo.
