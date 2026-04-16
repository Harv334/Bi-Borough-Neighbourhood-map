# NW London Population Health — Bi-Borough Neighbourhood Map

Interactive map + auto-updating data pipeline for the 9 NW London local
authorities (Brent, Camden, Ealing, Hammersmith & Fulham, Harrow,
Hillingdon, Hounslow, Kensington & Chelsea, Westminster).

**Live map:** https://harv334.github.io/Bi-Borough-Neighbourhood-map/

## What's in this repo

| Path                  | What it is                                                  |
|-----------------------|-------------------------------------------------------------|
| `index.html`          | The map (single-file Leaflet, deployed to GitHub Pages)     |
| `data/`               | Refreshed Parquet files for every dataset (committed)       |
| `data/boundaries/`    | LSOA + ward + LAD GeoJSONs                                   |
| `data/_meta/`         | Manifest + Power BI index + map JSON                         |
| `pipeline/`           | The Python pipeline (`pipeline run`, `pipeline export`, ...) |
| `.github/workflows/`  | Cron jobs that keep `data/` and `index.html` fresh           |
| `docs/HANDOVER.md`    | Single source of truth for whoever maintains this next       |
| `docs/POWERBI.md`     | How to use the data in Power BI                              |
| `docs/ADDING_A_FETCHER.md` | How to add a new data source                            |

## Data sources

19 sources across 8 categories — a full inventory is in
`pipeline/conf/sources.yml` and reproduced here for browsing:

**Healthcare supply** — GP practices (NHS EPRACCUR), hospital sites
(NHS ETS), pharmacies (NHS BSA), dentists (NHS BSA), care homes (CQC).

**Outcomes** — OHID Fingertips (life expectancy, smoking, obesity, MH
prevalence, child immunisations, A&E rate, suicide rate, fuel poverty,
GP patient satisfaction), OpenPrescribing (chronic disease drug
volumes per ICB).

**Demographics** — Census 2021 (Nomis bulk: TS001, TS017, TS021, TS037,
TS038, TS066), MHCLG IoD 2025 (deprivation scores per LSOA).

**Environment** — OS Open Greenspace, DEFRA AURN air quality monitors,
LAEI modelled NO₂/PM2.5 (stub).

**Transport** — TfL PTAL grid (LSOA mean), TfL StopPoint API
(Tube/Overground/Elizabeth/DLR/National Rail).

**Crime** — data.police.uk (street-level crimes per borough per month).

**Food** — FSA Food Hygiene Ratings per local authority.

**Housing** — EPC domestic certificates, DESNZ sub-regional fuel
poverty.

## Run it

```sh
cd pipeline
pip install -e .

# All sources due this cadence
pipeline run --cadence monthly

# A single source
pipeline run --source gp_practices

# What's in the data/ directory and when was it last refreshed?
pipeline status
```

## Running on schedule

GitHub Actions runs every cadence on its own schedule —
see `.github/workflows/`. No manual intervention needed.

Required GitHub Secrets:
- `OS_DATA_HUB_TOKEN` — for OS Open Greenspace
- `EPC_AUTH_TOKEN` — for EPC certificates

## Use the data elsewhere

Power BI: `data/_meta/powerbi_index.csv` lists every dataset with
its raw GitHub URL — paste any URL into Power BI's "Get Data → Web".
See `docs/POWERBI.md`.

Pandas / DuckDB: every parquet at
`https://raw.githubusercontent.com/Harv334/Bi-Borough-Neighbourhood-map/main/data/<category>/<id>.parquet`

## Add a new data source

See `docs/ADDING_A_FETCHER.md` — needs ~50 lines of Python and one
yaml entry.

## Maintainer handover

See `docs/HANDOVER.md` for everything a new maintainer needs to know.
