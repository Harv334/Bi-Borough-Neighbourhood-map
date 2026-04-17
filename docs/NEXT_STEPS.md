# Next steps — what I did while you slept, and what to do next

**TL;DR:** the pipeline package is now installable, the CLI works, the
exporter writes `pharmacies.json` automatically, and the handover docs
are rewritten for local-run mode. You need to run the pipeline on your
Windows box to actually populate the data.

## What changed in this session

### Working code + verification
- Reinstalled the `pipeline` package cleanly (`pip install -e .` works,
  `pipeline --help` / `pipeline status` / `pipeline export` all run).
- Fixed `pyproject.toml` (the previous file was truncated) and restored
  `.gitignore` and seven CI workflow files that prior sessions had left
  mangled by CRLF handling.
- Rebuilt `pipeline/src/exporters/leaflet_json.py` to also emit
  `pharmacies.json`. Now `pipeline run --source pharmacies` produces
  both the Parquet and the JSON the map reads — no separate step.

### Documentation
- **`docs/HANDOVER.md`** — complete rewrite. Assumes zero context.
  Explains the local-run model (the GitHub Actions workflows stay
  disabled), the one-button refresh scripts, every host the fetchers
  need, what the map reads where, and the common failure modes.
- **`docs/DATA_SOURCES.md`** — new. One entry per source: URL, cadence,
  what it gives us, known failure modes. Treat this as the reference
  when a fetcher breaks.
- **`docs/NEXT_STEPS.md`** — this file.

### New scripts
- **`scripts/refresh.bat`** — Windows one-button refresh (creates venv,
  installs pipeline, runs all fetchers, prints git status).
- **`scripts/refresh.sh`** — Mac/Linux equivalent.
- **`scripts/refresh_pharmacies.py`** — reduced to a 20-line
  deprecation shim that calls `pipeline run --source pharmacies`.

### What I tried but couldn't do from the sandbox
- **Fetching any real data.** Cowork's sandbox only allowlists
  `github.com`, `pypi.org`, `npmjs.org`, and a few Anthropic domains.
  Every NHS / ONS / gov.uk / postcodes.io host is blocked.
  So I couldn't populate `ward_data.json`, `lsoa_data.json`, or any
  of the Parquet files. All the scaffolding is ready; the data pull has
  to happen on your machine.
- **Committing.** `.git/index.lock` exists in the repo and the sandbox
  user doesn't have permission to delete it. You'll need to remove it
  before any `git` command will work — see step 1 below.

## What you need to do next

### 1. Unlock git

```powershell
Remove-Item .git\index.lock -Force
```

(You hit this once before — it's the same file, still stuck.)

### 2. Throw away the write-test cruft I couldn't delete

```powershell
Remove-Item pipeline\src\.writetest -Force -ErrorAction SilentlyContinue
```

It's a zero-byte file I created to test disk writability and couldn't
delete from the sandbox.

### 3. Commit the pipeline + docs work first (no data yet)

**PowerShell (one line, easiest):**

```powershell
git add pyproject.toml .gitignore .github/workflows/ pipeline/src/exporters/leaflet_json.py scripts/refresh.bat scripts/refresh.sh scripts/refresh_pharmacies.py docs/HANDOVER.md docs/DATA_SOURCES.md docs/NEXT_STEPS.md
git commit -m "infra(pipeline): local-run mode + unified refresh scripts"
```

**PowerShell (line-continued with backtick):**

```powershell
git add pyproject.toml .gitignore .github/workflows/ `
        pipeline/src/exporters/leaflet_json.py `
        scripts/refresh.bat scripts/refresh.sh scripts/refresh_pharmacies.py `
        docs/HANDOVER.md docs/DATA_SOURCES.md docs/NEXT_STEPS.md

git commit -m "infra(pipeline): local-run mode + unified refresh scripts"
```

**cmd.exe (line-continued with caret):**

```cmd
git add pyproject.toml .gitignore .github/workflows/ ^
        pipeline/src/exporters/leaflet_json.py ^
        scripts/refresh.bat scripts/refresh.sh scripts/refresh_pharmacies.py ^
        docs/HANDOVER.md docs/DATA_SOURCES.md docs/NEXT_STEPS.md

git commit -m "infra(pipeline): local-run mode + unified refresh scripts"
```

Full commit body if you want it:

```
- pipeline exporter now writes pharmacies.json alongside parquet
- scripts/refresh.bat + scripts/refresh.sh one-button runners
- scripts/refresh_pharmacies.py reduced to a deprecated shim
- docs/HANDOVER.md rewritten for local-run (GH Actions stays disabled)
- docs/DATA_SOURCES.md: source-by-source catalogue + failure modes
- pyproject.toml / .gitignore restored (were truncated)
```

If `git status` still shows a lot of `M pipeline/...` lines after this,
run `git add -A pipeline/` once — they're identical to HEAD (index
cache is stale because of the lock) and will fall off.

### 4. Do the actual data refresh

```cmd
scripts\refresh.bat
```

Expect it to take 10-20 minutes. Some fetchers will probably fail the
first time — NHS Digital's URLs drift. When one fails, open the matching
file under `pipeline/src/fetchers/<category>/<id>.py`, update the URL or
column name, and re-run just that source:

```cmd
pipeline run --source <id>
```

### 5. Commit the fresh data

```powershell
git add data/ ward_data.json lsoa_data.json pharmacies.json index.html
git commit -m "data: refresh $(Get-Date -Format yyyy-MM-dd)"
git push
```

GitHub Pages redeploys within a minute.

## Known issues I left for you

- **`ward_data.json` is currently `{}`** — the exporter works but had no
  parquets to read. Step 4 fixes this.
- **`build_ward_data` only populates `gp_practice_count`.** To wire up
  life-expectancy / %65+ / GP registered patients (the three stats the
  ward key-stats strip shows), someone needs to pivot the
  `data/outcomes/fingertips.parquet` long-format into per-ward dicts.
  There's a `# TODO` marker in `leaflet_json.py::build_ward_data`.
- **Fingertips ward-level coverage is patchy.** Many indicators are only
  published at LTLA. The map shows "--" when the indicator is missing,
  which is fine.
- **The disabled CI workflows are intentional.** Don't re-enable them.
  Costs money + hits sources that throttle GitHub's hosted runners.

## If you want to re-enable GitHub Actions later

It's possible — you'd need:
1. A self-hosted runner on a machine with unrestricted internet, OR
2. Workflow-dispatch only (no cron) so you trigger manually.

Start with `pipeline-on-demand.yml` which is the only enabled workflow.
The `.disabled` ones are archived for reference.

Sources:
- [docs/HANDOVER.md](../docs/HANDOVER.md)
- [docs/DATA_SOURCES.md](../docs/DATA_SOURCES.md)
