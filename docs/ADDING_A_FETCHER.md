# Adding a new data source

The pipeline is designed so that adding a source = ~50 lines of
Python and one yaml entry. No changes to the CLI, the GHA workflows,
or the exporters are needed — they all read from `sources.yml`.

## Recipe (5 steps)

### 1. Pick a category & write the fetcher

Drop a new `.py` file under `pipeline/src/fetchers/<category>/`.
Subclass `BaseFetcher` and implement two methods:

```python
from pipeline.src.core import BaseFetcher
import pandas as pd
import requests

class MyNewFetcher(BaseFetcher):
    source_id = "my_new_source"
    category = "outcomes"
    required_cols = ["LSOA21CD", "value"]

    def fetch_raw(self):
        cache = self.cache_dir / "raw.csv"
        if not cache.exists():
            r = requests.get("https://example.com/data.csv", timeout=60)
            r.raise_for_status()
            cache.write_bytes(r.content)
        return pd.read_csv(cache)

    def transform(self, raw: pd.DataFrame) -> pd.DataFrame:
        # Reshape so the output has every column listed in required_cols
        return raw.rename(columns={"lsoa": "LSOA21CD", "score": "value"})
```

That's the whole contract. `BaseFetcher` will call:
- `fetch_raw()` — return whatever object you like (DataFrame, dict, Path, list)
- `transform(raw)` — return a `pd.DataFrame` with the required columns
- `validate(df)` — runs automatically; warns on null boundary keys
- `write(df)` — writes `data/<category>/<source_id>.parquet` automatically

### 2. Register it in `pipeline/conf/sources.yml`

```yaml
  - id: my_new_source
    category: outcomes
    module: pipeline.src.fetchers.outcomes.my_new.MyNewFetcher
    cadence: monthly
    source_url: https://example.com/about
    join_keys: [LSOA21CD]
    enabled: true
    notes: |
      Pulls the example dataset.  Ages well — only updated annually,
      but cadence kept at monthly to align with the dashboard refresh.
```

### 3. Smoke-test locally

```sh
cd pipeline
pip install -e .
pipeline run --source my_new_source
```

That writes `data/outcomes/my_new_source.parquet` and updates
`data/_meta/manifest.json`.

### 4. Decide if it gets a map representation

If your source is a **point-in-space** dataset (locations of things
with lat/lng), edit `pipeline/src/exporters/leaflet_json.py` to add
a new layer to the index.html splice. Existing examples: GPs,
hospitals.

If your source is a **per-LSOA aggregate**, no map work needed — it
appears automatically in `data/_meta/powerbi_index.csv` and Power BI
users can colour LSOAs by it.

### 5. Push and let GitHub Actions take over

The cron in `.github/workflows/pipeline-<cadence>.yml` will pick up
your new source on its next scheduled run. Or kick it off immediately
via the "Pipeline (on demand)" workflow.

## Handy patterns from existing fetchers

| Pattern                                  | See                                     |
|------------------------------------------|------------------------------------------|
| ZIP CSV download with header inference   | `outcomes/fingertips.py`                 |
| Postcode → lat/lng via ONSPD             | `healthcare/gp_practices.py`             |
| Walk back N months to find latest file   | `healthcare/pharmacies.py`               |
| Spatial point-in-polygon for LSOA join   | `environment/air_quality_aurn.py`        |
| BNG (EPSG:27700) → WGS84 transform       | `environment/greenspace.py`              |
| ArcGIS REST pagination                   | `_boundaries.py`                         |
| Polygon-mode API call (data.police.uk)   | `crime/police_uk.py`                     |
| API key via env var with helpful error   | `housing/epc.py`, `environment/greenspace.py` |
| Per-borough loop                         | `food/fsa.py`                            |
| Excel workbook with shifting headers     | `housing/fuel_poverty.py`                |

## What NOT to do

- **Don't hardcode credentials.** Use `os.environ.get(...)` and add the
  secret in GitHub Actions.
- **Don't skip caching.** Repeated calls to upstream APIs are rude
  (and on flaky days will fail your runs).
- **Don't write JSON or CSV directly to `data/`.** Always Parquet —
  the exporters take care of any JSON the map needs.
- **Don't change the `LSOA21CD` column name.** Every dataset must use
  the exact case-sensitive string `LSOA21CD` for joins to work.
- **Don't push your `.cache/` folder.** It's already in `.gitignore`.
