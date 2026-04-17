# Data sources

Every source the pipeline pulls from, what it gives us, and what tends to
break. Ordered roughly by how much work they are to keep alive.

The canonical dispatch table is `pipeline/conf/sources.yml`. This doc is the
narrative companion.

## Healthcare

### `gp_practices` — NHS Digital EPRACCUR
- **URL:** `https://files.digital.nhs.uk/assets/ods/current/epraccur.zip`
- **Cadence:** monthly
- **Gives us:** every active GP practice in England — code, name, address,
  postcode, PCN, CCG, status. We geocode via ONSPD and filter to the 9 NW
  London LADs.
- **Output:** `data/healthcare/gp_practices.parquet`, spliced into
  `index.html` as `const GPS = [...]`.
- **Known issues:** NHS CDN returns 403 for requests without a browser
  `User-Agent`. Our fetcher sets one (commit 2537361). ZIP layout changes
  roughly yearly — the column headers are hardcoded in a CSV header row,
  not a docstring, so when they rename "Practice Code" → "Code" the pandas
  read silently produces NaNs. If the parquet row count crashes, diff the
  CSV headers first.

### `hospitals` — NHS Digital Estates Returns Information Collection (ERIC)
- **URL:** `https://files.digital.nhs.uk/...` (annual Excel)
- **Cadence:** annual (published ~October)
- **Gives us:** every NHS acute trust site. We filter to NW London
  catchment (includes Royal Free, Imperial, Chelsea & Westminster,
  North West London Hospitals, Central London Community Healthcare, etc.).
- **Output:** `data/healthcare/hospitals.parquet`, spliced as `const HOSP`.
- **Known issues:** Excel sheet name changes every year. When the fetcher
  fails, open the XLSX and update the `sheet_name=` in `hospitals.py`.

### `pharmacies` — NHS BSA edispensary
- **URL pattern:** `https://www.nhsbsa.nhs.uk/sites/default/files/<YYYY>-<MM>/edispensary.csv`
- **Cadence:** monthly (published 2–10 days into the month)
- **Gives us:** every NHS-contracted pharmacy. ~14,000 rows UK-wide;
  filters to NW London LADs after ONSPD postcode lookup.
- **Output:** `data/healthcare/pharmacies.parquet` and `pharmacies.json`.
- **Known issues:** The URL changes every month. Fetcher walks backwards
  up to 6 months to find the most recent published file.

### `dentists` — NHS Digital
- **Cadence:** monthly
- **Known issues:** Column names move around. The `--latest` URL
  redirects — use the versioned URL from the index page.

### `care_homes` — CQC
- **URL:** CQC public CSV export
- **Cadence:** monthly
- **Gives us:** all registered care homes, including Ofsted/CQC rating.
- **Known issues:** The CQC export link is behind a form; the stable URL
  lives at `/care-directory/care-directory-with-filters` and requires you
  to POST the filter form. Our fetcher bakes in the equivalent query
  string. If CQC redesign the site, this breaks loudly.

## Outcomes

### `fingertips` — OHID Fingertips API
- **URL:** `https://fingertips.phe.org.uk/api/...`
- **Cadence:** monthly
- **Gives us:** ~1500 population-health indicators per LTLA, some down to
  ward-level. We pull the curated list in
  `pipeline/src/fetchers/outcomes/fingertips_indicators.yml`.
- **Output:** `data/outcomes/fingertips.parquet` (long format: one row per
  indicator × area × time period).
- **Known issues:** Indicators get retired or renumbered every year or two.
  When a fetch returns 0 rows for a specific indicator, check the current
  indicator ID on the Fingertips website and update the YAML.

### `prescribing` — OpenPrescribing
- **URL:** `https://openprescribing.net/api/1.0/...`
- **Cadence:** monthly
- **Gives us:** GP practice × BNF chemical × month spend + items.
- **Output:** `data/outcomes/prescribing.parquet`.
- **Known issues:** Quota-limited but documented as "fair use". Fetcher
  throttles to 1 request/sec.

## Demographics

### `census2021` — Nomis bulk downloads
- **URL:** `https://www.nomisweb.co.uk/output/census/2021/<tsNNN>-2021-1.zip`
- **Cadence:** annual in theory, never changes in practice
- **Gives us:** population, age bands, ethnic group, general health,
  disability, economic activity — all at LSOA.
- **Output:** `data/demographics/census2021.parquet`.
- **Known issues:** The bulk ZIPs contain CSVs named differently from year
  to year. Fetcher picks the LSOA-level CSV by name match
  (`lsoa` substring).

### `imd2025` — MHCLG English Indices of Deprivation 2025
- **URL:** `https://assets.publishing.service.gov.uk/.../IoD2025...`
- **Cadence:** annual (actually published every 3–4 years; filename lies)
- **Gives us:** IMD rank + decile + 7 sub-domain scores per LSOA.
- **Output:** `data/demographics/imd2025.parquet`.

## Environment

### `greenspace` — OS Open Greenspace via GLA mirror
- **URL:** `data.london.gov.uk`
- **Cadence:** quarterly

### `air_quality_aurn` — Defra AURN
- **URL:** `https://uk-air.defra.gov.uk/...`
- **Cadence:** daily (for current year; historical is annual)

### `laei` — London Atmospheric Emissions Inventory (GLA) — DISABLED
- **Reason disabled:** 2GB+ per run, not worth the network cost for what
  the map actually shows. Enable in `sources.yml` if you need it.

## Transport

### `ptal` — TfL Public Transport Accessibility Level
- **URL:** `https://api.tfl.gov.uk/...`
- **Cadence:** annual
- **Gives us:** PTAL 0–6b per 100m grid cell. We aggregate to LSOA
  centroid lookups.

### `tfl_stations` — TfL station list
- **URL:** `https://api.tfl.gov.uk/StopPoint/...`
- **Cadence:** quarterly

## Crime

### `police_uk_crime` — data.police.uk
- **URL:** `https://data.police.uk/api/crimes-street/all-crime?...`
- **Cadence:** monthly
- **Gives us:** reported street-level crime per month per LSOA.
- **Known issues:** API is quota-limited to 100 requests / 15 minutes.
  Fetcher batches by bounding box.

## Food

### `fsa_food_hygiene` — Food Standards Agency
- **URL:** `https://api.ratings.food.gov.uk/Establishments?...`
- **Cadence:** weekly
- **Gives us:** every rated food establishment in NW London with hygiene
  score + last inspection date.

## Housing

### `epc_domestic` — Open Data Communities EPC
- **URL:** `https://epc.opendatacommunities.org/api/v1/domestic/search`
- **Cadence:** quarterly
- **Requires:** free API key in `.env` as `EPC_API_KEY=...`
- **Gives us:** Energy Performance Certificate band per property.
- **Known issues:** 5000 rows per request. Fetcher paginates.

### `fuel_poverty` — BEIS / DESNZ sub-regional fuel poverty
- **URL:** `https://assets.publishing.service.gov.uk/...`
- **Cadence:** annual
- **Gives us:** % households in fuel poverty by LSOA.

## Geography (not a "data source" per se)

### `boundaries` — ONS Open Geography Portal
- **URLs:** various ArcGIS feature service endpoints
- **Cadence:** effectively never (ward codes change in May of a gov election year)
- **Gives us:** 188 ward polygons (WD25CD), 1313 LSOA polygons (LSOA21CD),
  9 borough polygons (LAD25CD), all clipped to NW London.
- **Output:** spliced into `index.html` as `const GJ`, `const LSOA_IMD`,
  `const BOROUGH_GJ`.
- **Known issues:** The ArcGIS endpoints occasionally throttle. If a fetch
  returns "Error 500 service busy", just re-run.

## What nobody will maintain

If one of these breaks and you don't care about it, set `enabled: false`
in `sources.yml` and move on. The map degrades gracefully:

- **No pharmacies.json** → pharmacy layer checkbox just stays empty.
- **No ward_data.json** → Indicators tab shows "no data loaded".
- **No LSOA parquet** → the IMD heat layer uses the constants already
  embedded in `index.html` (last good refresh); it only goes stale, never
  breaks.

The only hard dependencies are `boundaries` and `gp_practices`. Everything
else is optional.
