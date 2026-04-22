#!/usr/bin/env pwsh
# Push the recent UI fixes. Run from PowerShell:
#   cd C:\Users\harve\Downloads\nw-london-health-pipeline
#   powershell -ExecutionPolicy Bypass -File .\push_now.ps1

Set-Location $PSScriptRoot

# Remove any phantom locks left by the sandbox
Remove-Item -Force -ErrorAction SilentlyContinue .git\index.lock
Remove-Item -Force -ErrorAction SilentlyContinue .git\HEAD.lock

# Write commit message to a temp file to avoid PowerShell / git arg parsing
# issues with multi-line -m strings (e.g. tokens like --foo in the body get
# mis-parsed as flags).
$msgPath = Join-Path $env:TEMP "nwl_commit_msg.txt"
$msg = @'
data(scope,lookup): ONS WD24 best-fit lookup + drop Camden (NCL)

Adopts the official ONS LSOA (2021) -> Electoral Ward (May 2024)
best-fit lookup as the authoritative LSOA->ward mapping, replacing
the prior ONSPD postcode-modal heuristic. Scopes the dataset to
the 8-borough NW London ICS footprint; Camden is NCL, not NWL,
and is excluded everywhere.

Verified: 168 of 168 NWL wards now match the ICHT reference table
exactly (to 4 dp) on pop-weighted IMD score. Remaining deltas
under the previous ONSPD-derived lookup are eliminated.

Previous commit (retained): dual-population model
  * census_population       = ONS mid-2024 (display)
  * imd_denominator_mid2022 = MHCLG File_7 col 52 (IMD weight,
                              matches ICHT methodology)

Changes
-------
- LSOA->ward authority switched to ONS WD24 best-fit lookup
  (LSOA21CD -> WD24CD/WD24NM/LAD24CD/LAD24NM, 8 NWL LADs).
- Camden removed from all NWL surfaces:
  * index.html:
      - GJ (ward polygons): 188 -> 168 features (-20 Camden).
      - GPS (GP practices): 369 -> 337 (-32 Camden).
      - BOROUGH_GJ: 9 -> 8 features (-1 Camden).
      - LSOA_IMD: 1313 -> 1183 features (-130); remaining 175
        features' ward_code/ward/borough overwritten from ONS
        WD24 lookup.
      - #vcse-area-filter: Camden <option> removed.
      - LAD_NAME_TO_CODE: Camden entry removed.
  * ward_data.json: 188 -> 168 wards (20 Camden dropped).
    All 31 census_*_pct fields + 8 IMD domains re-aggregated
    under the new LSOA membership. Metadata adds
    lsoa_to_ward_lookup / lsoa_to_ward_lookup_year / scope /
    scope_lads fields.
  * pharmacies.json: 540 -> 480 (-60 Camden).
  * vcse_data.json: 9555 -> 8964 (-591 Camden-HQ charities;
    coverage-area references to Camden in retained records are
    intentional).
  * fetch_all_data.py: BOROUGHS list, NWL_LAD_CODES,
    NWL_AOO_NAMES all drop Camden. Doc comments note why
    (NCL ICS, not NWL).

New scripts
-----------
- scripts/reconfigure_to_ons_wd24_lookup.py: authoritative
  rebuild. Filters LSOA_IMD to 8 NWL LADs, rewrites ward/borough
  attribution from ONS WD24 lookup, re-aggregates ward_data.json
  (IMD pop-weighted on mid-2022; census_*_pct pop-weighted on
  mid-2024).

Final borough distribution (168 wards)
--------------------------------------
  Brent 22, Ealing 24, Hammersmith & Fulham 21, Harrow 22,
  Hillingdon 21, Hounslow 22, Kensington & Chelsea 18,
  Westminster 18.
'@
Set-Content -Path $msgPath -Value $msg -Encoding UTF8

git add -A
git commit -F $msgPath
git push origin main

Remove-Item -Force -ErrorAction SilentlyContinue $msgPath
