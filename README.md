# NW London Population Health - Bi-Borough Neighbourhood Map

Interactive Leaflet map + a single-file Python script that refreshes it.
Covers the 9 NW London local authorities: Brent, Camden, Ealing,
Hammersmith & Fulham, Harrow, Hillingdon, Hounslow, Kensington & Chelsea,
City of Westminster.

Live map: https://harv334.github.io/Bi-Borough-Neighbourhood-map/

## What's in this repo

| Path                | What it is |
|---------------------|----------------------------------------------------|
| `index.html`        | The map itself - single-file Leaflet, deployed to GitHub Pages |
| `fetch_all_data.py` | One script. Downloads everything, builds the JSON the map reads, re-splices `index.html`. |
| `ward_data.json`    | Ward-level indicators (188 wards) - consumed by the map at load |
| `lsoa_data.json`    | LSOA-level IMD scores + census columns (33,755 LSOAs) |
| `pharmacies.json`   | Pharmacy point data (~540 rows) |
| `data/`             | Intermediate Parquet files - one per source. Committed so you can open them in Power BI or pandas without rerunning fetches. |
| `data/boundaries/`  | LSOA + ward + LAD GeoJSONs |
| `.cache/`           | Raw downloads (gitignored). Drop manual files here - see below. |

## Quick start (one-time setup)

```bash
pip install pandas pyarrow requests pyproj shapely
```

Then drop these files into `.cache/`. All are free, public downloads.

| File | Where to drop it | Source |
|------|------------------|--------|
| **ONSPD zip** (required, ~250 MB) | `.cache/onspd/ONSPD_*_UK.zip` | https://geoportal.statistics.gov.uk - search "ONS Postcode Directory", download the latest quarterly "full" zip |
| **IMD 2025 File 7 CSV** (required, ~10 MB) | `.cache/imd2025/*.csv` | https://www.gov.uk/government/statistics/english-indices-of-deprivation-2025 - File 7 (all ranks/scores/deciles) |
| **EPRACCUR** (required, ~700 KB) | `.cache/gp_practices/epraccur.zip` | https://digital.nhs.uk/services/organisation-data-service/export-data-files/csv-downloads/gp-and-gp-practice-related-data |
| **edispensary CSV** (required, ~3.5 MB) | `.cache/pharmacies/edispensary.csv` | Latest monthly from https://www.nhsbsa.nhs.uk |
| Hospital CSV (optional) | `.cache/hospitals/Hospital.csv` | https://www.nhs.uk/about-us/nhs-website-datasets/ |

OHID Fingertips (health outcomes) and police.uk (crime) are API-backed -
the script hits them directly the first time and caches the responses.

## Running

```bash
# Run everything: fetches, transforms, writes ward/lsoa/pharmacy JSON.
python fetch_all_data.py

# Run a single source, then re-export:
python fetch_all_data.py --only imd

# Skip a slow source:
python fetch_all_data.py --skip crime

# Skip all fetching; just rebuild the JSON from cached Parquets:
python fetch_all_data.py --export-only
```

After running, refresh `index.html` in your browser (or push and let
GitHub Pages redeploy).

## When a source breaks

The script keeps going on per-source failures - a broken URL won't wipe
out your other outputs. The failing source's Parquet is left alone, so
the previous run's data survives.

To update one source:
1. Re-download the file from the URL in the table above
2. Overwrite it in `.cache/<source>/`
3. Run `python fetch_all_data.py --only <source>`

## Handing this over

For someone who just wants to refresh the map:

1. Clone the repo.
2. `pip install pandas pyarrow requests pyproj shapely`
3. Download the four required files listed in the table above and drop
   them into their respective `.cache/` subfolders.
4. `python fetch_all_data.py`
5. `git commit -am "data refresh YYYY-MM" && git push`

That's it. The script has a long docstring at the top restating every
download URL in case this README ever goes stale.
