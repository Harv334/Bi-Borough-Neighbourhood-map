# Using the data in Power BI

This pipeline publishes everything as Parquet files in the public
GitHub repo. Power BI can read Parquet directly over HTTPS — no
database, no gateway, no scheduled refresh hassle.

## The 60-second setup

1. **Open the index of available datasets:**
   `https://raw.githubusercontent.com/Harv334/Bi-Borough-Neighbourhood-map/main/data/_meta/powerbi_index.csv`

   This CSV lists every dataset, its category, refresh cadence, the
   columns it joins on, and the raw GitHub URL of the parquet.

2. **In Power BI Desktop:** *Home → Get Data → Web*
   - Paste the URL of any parquet from the index, e.g.:
     `https://raw.githubusercontent.com/Harv334/Bi-Borough-Neighbourhood-map/main/data/healthcare/gp_practices.parquet`
   - Power BI auto-detects the schema.

3. **Set scheduled refresh:** *Workspace → Dataset → Settings → Scheduled
   refresh → Daily*. Anonymous credentials work fine — these are public
   raw GitHub URLs.

## Recommended star schema

| Role          | Table              | Join column      |
|---------------|--------------------|------------------|
| Fact          | `gp_practices`     | LSOA21CD         |
| Fact          | `hospitals`        | LSOA21CD         |
| Fact          | `police_uk_crime`  | LSOA21CD         |
| Fact          | `fsa_food_hygiene` | LSOA21CD         |
| Dim — geog    | `boundaries/lsoa.geojson` | LSOA21CD  |
| Dim — demog   | `census2021`       | LSOA21CD         |
| Dim — depriv  | `imd2025`          | LSOA21CD         |

Most facts also carry `WD25CD` and `LAD25CD` so you can roll up to
ward or borough without an extra join.

## Joining datasets

Every dataset documented in `powerbi_index.csv` has a `join_keys`
column. Examples:

- Anything with `LSOA21CD` joins to anything else with `LSOA21CD`
- Anything with `WD25CD` (May 2025 wards) joins to other ward data
- Anything with `LAD25CD` joins to borough-level data
- `ODS_code` is the NHS practice/site identifier — used to join
  `gp_practices` to `prescribing`

## Worked example: fast-food density × child obesity by ward

```
let
    fsa  = Csv from https://.../data/food/fsa_food_hygiene.parquet,
    obs  = Csv from https://.../data/outcomes/fingertips.parquet,
    ff   = Table.SelectRows(fsa, each [business_type] = "Takeaway/sandwich shop"),
    by_ward = Table.Group(ff, {"WD25CD"}, {{"n_takeaways", each Table.RowCount(_)}}),
    obesity_yr6 = Table.SelectRows(obs, each [indicator_id] = 93108)
        // OHID PHOF: child obesity year 6
in
    Table.NestedJoin(by_ward, {"WD25CD"}, obesity_yr6, {"WD25CD"}, "obs")
```

## Live geography lookup

`data/boundaries/wards.geojson` etc. are GeoJSON. Power BI's built-in
shape map visual can consume these directly: *Visualizations → Filled
map → Add data layer → GeoJSON*.

## Refresh cadence

Each dataset in `powerbi_index.csv` has a `cadence` column. If you
schedule Power BI to refresh nightly, you'll always be at most a few
hours behind the latest GitHub Actions cron run.

| Cadence    | Pulled by GHA      | Schedule Power BI for      |
|------------|--------------------|----------------------------|
| daily      | 05:30 UTC          | 07:00 UTC                  |
| weekly     | Mon 06:00 UTC      | Mon 08:00 UTC              |
| monthly    | 5th of month       | 6th of month               |
| quarterly  | 8th Jan/Apr/Jul/Oct| 9th Jan/Apr/Jul/Oct        |
| annual     | 1 March            | 2 March                    |

## Authentication

Anonymous. The repo is public, the parquets are public, the URLs are
stable. No tokens. No service principals. No gateway.

## What about row-level security?

Not needed — there is no PII in any output. Everything is aggregated to
geography or counts establishments / health outcomes, never identifies
individuals.

## When something looks wrong

1. Check `data/_meta/manifest.json` — does the source's `last_run_utc`
   look recent? If not, the cron job didn't fire, check the Actions tab.
2. Check `data/_meta/powerbi_index.csv` — does the parquet have a
   non-zero row count?
3. Re-run a single source manually: Actions → "Pipeline (on demand)".
