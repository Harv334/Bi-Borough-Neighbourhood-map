# Handover — NW London Health Pipeline

This doc explains how to run the pipeline locally, how the map gets its data,
and what to do next. It deliberately assumes **no prior context** — if you're
walking into this fresh, start at the top.

---

## 1. What this repo actually is

Two things live in one repo:

1. **`index.html`** — the interactive Leaflet map (GitHub Pages deploys it).
   It's a single-file HTML app that reads a handful of JSON files at load
   time, plus some data that's spliced into it as JavaScript constants.

2. **`pipeline/`** — a Python package that fetches every underlying dataset
   (NHS BSA pharmacies, NHS Digital GP/hospital lists, ONS Census 2021,
   OHID Fingertips, IMD 2025, etc.), normalises them, and regenerates the
   JSON / spliced constants the map reads.

The pipeline used to run on a schedule in GitHub Actions. **It doesn't
anymore** — GitHub's sandboxed runners get throttled / blocked by some of
the gov.uk hosts, and a hosted runner costs money once you leave the free
tier. All the `*.yml.disabled` workflow files are intentional; don't
re-enable them without knowing what you're signing up for.

**So the pipeline runs locally on a dev machine, and the outputs are
committed back to the repo.** That's the entire deployment model.

---

## 2. Running the pipeline locally

### Prerequisites

- **Python 3.10 or later**, installed from python.org (tick "Add Python to PATH") or via `winget install Python.Python.3.12`.
- **Not the Microsoft Store `python` shim.** If typing `python` on Windows opens the Store and does nothing, disable it: Settings -> Apps -> Advanced app settings -> App execution aliases -> turn off the two "App Installer python.exe / python3.exe" switches.
- **Git**, `git --version` should work.
- An unrestricted internet connection (home / hotspot is fine; some corporate networks block gov.uk hosts).

### The one-liner

**Windows (Command Prompt *or* PowerShell):**

```cmd
scripts\refresh.bat
```

The script probes for the Python launcher in this order: `py -3` -> `python` -> `python3`, and gives explicit install instructions if none work.

**Mac / Linux:**

```bash
./scripts/refresh.sh
```

Both scripts:

1. Create a `.venv` in the repo root if it doesn't already exist.
2. `pip install -e .` the pipeline package.
3. Run `pipeline run` (every enabled fetcher).
4. Regenerate `ward_data.json`, `lsoa_data.json`, `pharmacies.json`.
5. Re-splice the GP and hospital constants into `index.html`.

When it finishes, `git status` will show what changed. Commit and push and
GitHub Pages picks it up within a minute.

### Running a single source

You don't always need to refresh everything. The CLI accepts filters:

```bash
pipeline run --source pharmacies           # one fetcher
pipeline run --cadence monthly             # everything monthly
pipeline run --category demographics       # everything in one category
pipeline run --no-export                   # skip the JSON/splice step
pipeline export                            # ONLY the JSON/splice step
pipeline status                            # show manifest of last successful runs
```

The list of source IDs is in `pipeline/conf/sources.yml`.

### Network the pipeline needs

The fetchers hit these hosts. If you're on a corporate network that blocks
any of them, the pipeline will fail fast with a clear message:

| Host                                       | Used for                                     |
| ------------------------------------------ | -------------------------------------------- |
| `files.digital.nhs.uk`                     | GP practices, hospitals, dentists (EPRACCUR) |
| `www.nhsbsa.nhs.uk`                        | Pharmacies (edispensary)                     |
| `assets.publishing.service.gov.uk`         | IMD 2025, fuel poverty                       |
| `www.nomisweb.co.uk`                       | Census 2021 bulk tables                      |
| `fingertips.phe.org.uk`                    | OHID Fingertips indicators                   |
| `api.postcodes.io`                         | Postcode geocoding fallback                  |
| `geoportal.statistics.gov.uk`, `services1.arcgis.com` | Ward/LSOA/LA boundaries             |
| `data.london.gov.uk`                       | GLA greenspace, LAEI (optional)              |
| `data.police.uk`                           | Crime data                                   |
| `api.ratings.food.gov.uk`                  | FSA hygiene ratings                          |
| `epc.opendatacommunities.org`              | Domestic EPC (needs a free API key)          |
| `openprescribing.net`                      | Prescribing (optional)                       |
| `uk-air.defra.gov.uk`                      | Air quality (AURN)                           |
| `api.tfl.gov.uk`                           | TfL stations / PTAL                          |
| `www.cqc.org.uk`                           | Care homes                                   |

A plain home internet connection is fine. A typical full run takes
10–20 minutes and downloads ~500 MB total (Census 2021 bulk tables are
the bulk of it).

### What gets written where

```
repo root/
├── index.html              ← map page (GPS + HOSP constants spliced in)
├── ward_data.json          ← per-ward indicators for the Indicators tab
├── lsoa_data.json          ← per-LSOA IMD + demographics
├── pharmacies.json         ← purple-pin layer
├── ward_geometries.json    ← GeoJSON for the ward boundary overlay
├── data/
│   ├── healthcare/*.parquet
│   ├── demographics/*.parquet
│   ├── outcomes/*.parquet
│   ├── environment/*.parquet
│   ├── transport/*.parquet
│   ├── crime/*.parquet
│   ├── food/*.parquet
│   ├── housing/*.parquet
│   └── _meta/manifest.json  ← last-run status for `pipeline status`
└── pipeline/.cache/         ← raw downloads, gitignored
```

The Parquet files are the source of truth. The JSON files and spliced
constants are derived from them and can always be regenerated with
`pipeline export`.

---

## 3. How the map reads the data

Open `index.html` and search for these names:

| What it draws                          | Where the data comes from                                      |
| -------------------------------------- | -------------------------------------------------------------- |
| GP pins (blue clusters)                | `const GPS = [...]` — spliced from `data/healthcare/gp_practices.parquet` |
| Hospital pins                          | `const HOSP = [...]` — spliced from `data/healthcare/hospitals.parquet`   |
| Pharmacy pins (amber clusters)         | `fetch('pharmacies.json')` at load time                        |
| Ward polygons + labels                 | `const GJ = {...}` embedded GeoJSON (188 wards)                |
| LSOA IMD heat layer                    | `const LSOA_IMD = {...}` embedded GeoJSON (1313 LSOAs)         |
| Borough polygons                       | `const BOROUGH_GJ = {...}`                                     |
| Ward click → key-stats strip           | `fetch('ward_data.json')` → `.indicators.{health_life_expectancy, census_over65_pct, gp_registered_patients}` |
| Ward report (opens in new window)      | Generated client-side from `ward_data.json` + the in-page constants |

The map currently works with partial data — if `ward_data.json` is empty
(`{}`), the Indicators tab just shows "No ward indicators loaded" but
everything else still works.

---

## 4. Current data status (as of 2026-04-16)

| Source          | State                            | Notes                                      |
| --------------- | -------------------------------- | ------------------------------------------ |
| boundaries      | ✅ cached in repo                | Auto-refresh not needed; ONS rarely changes |
| gp_practices    | ✅ spliced into index.html       | 409 practices embedded                     |
| hospitals       | ✅ spliced into index.html       | 20 NW London trust sites                   |
| pharmacies      | ⚠️ empty (`pharmacies.json = []`) | Run `pipeline run --source pharmacies`     |
| dentists        | ⚠️ no parquet yet                | Run `pipeline run --source dentists`       |
| care_homes      | ⚠️ no parquet yet                |                                            |
| fingertips      | ⚠️ no parquet yet                | Ward-level life expectancy lives here      |
| census2021      | ⚠️ no parquet yet                | LSOA-level pop / %65+ / %non-white / health |
| imd2025         | ⚠️ no parquet yet                | Decile + rank per LSOA                     |
| everything else | ⚠️ no parquet yet                | See `pipeline/conf/sources.yml`            |

**First action if you're picking this up:** run `scripts\refresh.bat` on a
Windows machine with unrestricted internet. Commit the resulting Parquets
and regenerated JSONs. You'll likely see a handful of fetchers fail — those
are usually upstream URL changes (NHS Digital moves files every few months)
and are fixed in the relevant `pipeline/src/fetchers/**/*.py` module. See
`docs/ADDING_A_FETCHER.md` for the pattern.

---

## 5. Common edits

### Adding a new indicator to the Indicators tab

1. Add a fetcher (see `docs/ADDING_A_FETCHER.md`) that writes a Parquet with
   at least `WD25CD` + your indicator column.
2. Update `pipeline/src/exporters/leaflet_json.py::build_ward_data` to pull
   the new column into `out[wd]['indicators'][<your_key>]`.
3. Add the indicator to the `CATS` array in `index.html` (search for
   `"Access to care"`). The map auto-generates the pill + legend.
4. Run `pipeline export`.

### Adding a new map layer (not a ward indicator)

The cleanest pattern is: produce a small JSON file (like `pharmacies.json`),
hand-wire a `loadXyzLayer()` function in `index.html` that does
`fetch('xyz.json').then(...)`. Add a checkbox in the Layers tab and a
legend entry. `pharmacies` is the reference implementation (~60 lines of JS).

### Fixing a broken fetcher

Most failures are URL drift. The pattern is:

1. `pipeline run --source <id>` — read the exact error.
2. Open `pipeline/src/fetchers/<category>/<id>.py`.
3. Update the URL / CSV column name / zipfile path.
4. Re-run.

All fetchers inherit `BaseFetcher` from `pipeline/src/core/base.py` which
handles retries, caching under `pipeline/.cache/`, schema validation
against `required_cols`, and Parquet writing with atomic rename.

---

## 6. Things that are intentionally out of scope

- **Real-time data.** Everything is nightly-at-best. NHS publishes most of
  this monthly or quarterly.
- **Authentication.** Sources that need API keys (Open Data Communities EPC)
  read from `.env` — see `.env.example`. Don't commit keys.
- **Power BI auto-refresh.** We write `data/_meta/powerbi_index.csv` which
  lists every Parquet path and its cadence. You wire Power BI to read the
  Parquets directly. That's it.
- **Auto-deploy of data changes.** Data commits on `main` trigger the Pages
  build, same as code commits. No separate deploy step.

---

## 7. File map (abridged)

```
pipeline/
├── conf/
│   ├── sources.yml          ← the canonical list of fetchers + cadences
│   ├── boroughs.yml         ← 9 NW London LADs
│   └── indicators.yml       ← which indicators appear in the map
├── src/
│   ├── cli.py               ← entry point (pipeline run / export / status)
│   ├── core/
│   │   ├── base.py          ← BaseFetcher abstract (retry, cache, validate, write)
│   │   ├── manifest.py      ← last-run bookkeeping
│   │   ├── geo.py           ← BNG ↔ WGS84, PolygonIndex for point-in-poly
│   │   ├── postcodes.py     ← ONSPD lookup (lat/lng + LSOA + ward + LAD)
│   │   └── config.py
│   ├── fetchers/            ← one module per source
│   └── exporters/
│       ├── leaflet_json.py  ← writes ward_data, lsoa_data, pharmacies.json,
│       │                      splices GPS/HOSP into index.html
│       └── powerbi_index.py ← writes data/_meta/powerbi_index.csv
└── tests/
scripts/
├── refresh.bat              ← Windows one-button refresh
├── refresh.sh               ← Mac/Linux one-button refresh
└── refresh_pharmacies.py    ← DEPRECATED shim → pipeline run --source pharmacies
docs/
├── HANDOVER.md              ← this file
├── ADDING_A_FETCHER.md      ← how to add a new source
├── DATA_SOURCES.md          ← what each source contains, URL, cadence, known issues
└── POWERBI.md               ← Power BI wiring
```

---

## 8. If the map breaks on GitHub Pages

Quick check: open `index.html` locally (double-click) — does it work?

- **Yes, locally fine but broken on Pages:** probably a fetch path issue.
  `pharmacies.json`, `ward_data.json`, `lsoa_data.json` must be in the repo
  root for GitHub Pages to serve them at the same-origin URL the map uses.
- **No, also broken locally:** something mangled a constant when splicing.
  Run `git diff HEAD index.html` — the diff should only be inside
  `const GPS = [...]` / `const HOSP = [...]` blocks. If the diff sprawls
  across the file, `splice_index_html` blew its regex. The fix is almost
  always to widen the regex in `pipeline/src/exporters/leaflet_json.py`.

Worst case: `git checkout HEAD -- index.html` and re-run `pipeline export`.
