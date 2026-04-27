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
data+ui(crime,civic,esol,profile): MPS crime + civic + ESOL + ward A4 profile + PNG fix

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

UI additions in this push
-------------------------
- New "IMD composite score (ICHT methodology)" overlay exposed
  at ward level (key imd_score_ward), resolved via a new
  OV_FIELD_ALIAS map so the ward-aggregate (pop-weighted on
  mid-2022) and the LSOA-level imd_score can coexist without
  key collision. Wired through wardOvValue, lsoaOvValue,
  ovColor, getOvRange, the tercile classifier and the CSV
  export.
- Every option in #ov and #ov2 overlay dropdowns now carries a
  " · Ward" or " · LSOA" suffix so users can see the output
  level at a glance (67 options tagged). Optgroup label
  "(LSOA-level)" suffix removed where the per-option tag
  makes it redundant.
- Neighbourhoods tab relabelled from "Nbhds" to
  "Neighbourhoods" with an "IN DEV" chip, and the pane now
  opens with an amber "Neighbourhoods — section in
  development" banner explaining that only Westminster's
  4-neighbourhood preview is wired in.

PNG export — rewritten for PowerPoint-ready output
--------------------------------------------------
- `_downloadMapPng()` no longer captures the user's current
  viewport. Flow is now:
    1. save center/zoom,
    2. hide dl-badge + zoom control,
    3. fit-to-scope via `map.fitBounds(_pngScopeBounds(),
       {padding:[24,24], animate:false})` — scope is
       NW London or the focused borough,
    4. apply white polygon mask (outer world ring +
       borough holes) so everything outside NWL/borough
       renders pure white,
    5. wait for tiles + 220 ms settle,
    6. html2canvas capture at devicePixelRatio,
    7. tight-crop to the scope polygon's pixel bbox
       (16 px pad) via `map.latLngToContainerPoint()` and
       `ctx.drawImage(src, sx,sy,sw,sh, 0,0,dw,dh)`,
    8. stamp a rounded-pill caption at bottom-left
       (scope · overlay label · Month YYYY) with a red
       accent bar,
    9. download as blob,
   10. restore the user's original center/zoom with
       `map.setView(..., {animate:false})`.
- New helpers: `_pngNormBorough`, `_pngScopeFeatures`,
  `_pngScopeLabel`, `_pngScopeBounds`, `_pngCaption`,
  `_stampCaption`; `_buildWhiteMask` rewritten to consume
  `_pngScopeFeatures()` so NWL and borough-solo views share
  one code path.
- Filename now `nwl-map-<scope-slug>-<timestamp>.png`
  (e.g. `nwl-map-nw-london-20260422-1730.png` or
  `nwl-map-westminster-...`).
- Net effect: a single click produces a consistent,
  slide-ready image regardless of the user's current zoom
  level, with the map chrome hidden, the background white,
  and a caption that identifies the scope/overlay/date.

PNG export — dpr/scale crop fix
-------------------------------
- First version of the rewrite above shipped with a
  devicePixelRatio mismatch: html2canvas was called with
  both `width`/`height` AND `scale: dpr`. When both are
  passed, html2canvas produces a canvas at the LOGICAL
  size (clientWidth x clientHeight), not the dpr-scaled
  size. The crop rect multiplied CSS-pixel coords by dpr,
  so the source rect ran past the canvas and drawImage
  copied only the top-left region 1:1 into the
  destination — leaving the bottom-right of the PNG
  pure white (the pre-fill colour).
- Fix: drop the explicit width/height so html2canvas
  uses its default (clientWidth*scale, clientHeight*scale),
  then measure the true pixel ratio post-hoc via
  `fullCanvas.width / mapEl.clientWidth` (xScale / yScale)
  and use THOSE to convert CSS-pixel crop coords into
  canvas-pixel coords. Crop width/height are now also
  clamped to `fullCanvas.width - cx` / `fullCanvas.height
  - cy` so we never request out-of-bounds source pixels.
- Result: full scope renders correctly across all dpr
  values (1x, 1.25x, 1.5x, 2x).

PNG export — fit-to-scope timing + mask hardening
-------------------------------------------------
- Second pass fix after the dpr patch: export was still
  cropping to the pre-fit viewport because
  `map.fitBounds({animate:false})` is NOT reliably
  synchronous. Leaflet defers the actual move to the next
  animation frame, so `latLngToContainerPoint` and
  html2canvas both read the OLD view and captured the
  wrong region (user's previous pan/zoom, often showing
  south-London basemap through gaps).
- Fix:
    * `await` the `moveend` event after fitBounds (with a
      400 ms safety timeout for no-op fits).
    * Then force two `requestAnimationFrame` ticks so
      Leaflet has painted the new tile layout before the
      mask is added.
    * After the mask is added, another rAF pair + 260 ms
      settle.
- Mask outer ring expanded from the UK bbox
  (-2/-2 to 2/53) to full WGS84 world bounds
  (-180/-85 to 180/85) — belt-and-braces so no basemap
  can leak through at any zoom.

PNG export — simplified, capture-what-you-see
---------------------------------------------
- Dropped the fitBounds + polygon-bbox crop logic
  entirely. Per user clarification ("it just needs to
  show all the boroughs of NW London / the image on the
  user's screen"), the export now captures EXACTLY what
  the user is currently looking at in the map div, with
  the white mask applied so outside-NWL becomes white.
- Workflow: user zooms/pans to frame the view they want,
  then clicks PNG. No more fitBounds, no more
  latLngToContainerPoint math, no more tight-crop — the
  output is the literal map viewport.
- Caption pill still stamped at the bottom-left (scope ·
  overlay · month YYYY). _nwlDpr is now measured from
  `fullCanvas.width / mapEl.clientWidth` so caption text
  scales correctly at any devicePixelRatio.
- Map view no longer mutated: savedCenter/savedZoom are
  kept in scope as a defensive hook for future re-fits
  but are not applied.

PNG export — strip all Leaflet UI chrome from capture
-----------------------------------------------------
- Hide every Leaflet control corner during html2canvas:
  `.leaflet-top`, `.leaflet-bottom`, and
  `.leaflet-control-attribution`. This sweeps up the
  zoom control, the bottom-right legend control, and
  the attribution widget — so the PNG is clean map pixels
  only, with no overlaid UI. Previous visibility is
  stashed per-element and restored in finally.

PNG export — Canvas-2D mask + fit-to-NWL + tight crop
-----------------------------------------------------
- Previous Leaflet-SVG mask failed at large zoom-out
  views: with a world-sized outer ring and borough holes,
  Leaflet's SVG clipping produced malformed paths and the
  basemap leaked through for much of the image. Replaced
  with a post-capture Canvas-2D mask:
    * Project every NWL borough polygon to canvas-pixel
      coords using `map.latLngToContainerPoint` scaled by
      the measured html2canvas pixel ratio.
    * Build a single Path2D aggregating all boroughs
      (Polygon + MultiPolygon rings).
    * Fill `rect(0,0,W,H) + nwlPath` with even-odd rule
      → everything outside NWL is whited out. No SVG
      clipping, no extreme-coord math, no timing games.
- Initial re-add of fit-to-NWL via
  `setView(center, getBoundsZoom(...))` + awaited
  `moveend` + double-rAF — intended to centre the PNG
  on NWL regardless of the user's current pan.
- Tight-cropped to the NWL pixel bbox (+16 css-px pad)
  computed from the SAME projection pass used to build
  the mask, so the crop always matches the mask exactly.

PNG export — drop mask too (pure screenshot)
--------------------------------------------
- Canvas-2D mask failed in yet another way at large
  zoom-outs: even-odd fill rule produced inverted output
  (NWL whited out, surrounding basemap retained) when
  polygon pixel coords went well beyond the canvas. Every
  mask strategy tried (Leaflet SVG, Canvas-2D pre-
  capture, Canvas-2D post-capture) has had a different
  failure mode.
- Dropped the mask entirely. Final PNG flow is a pure
  screenshot of the map div:
    * waitForTiles + 160 ms settle + double-rAF,
    * html2canvas of the map element at devicePixelRatio,
    * Leaflet UI chrome hidden during capture (zoom
      control, legend, attribution, dl-badge),
    * caption pill stamped at bottom-left (scope · overlay
      · Month YYYY),
    * no fit, no setView, no crop, no mask, no view
      restore.
- Framing is the user's responsibility: zoom/pan to
  NWL (or a borough) first, then click PNG. This
  matches exactly what's on their screen.

Dental practices — expand to 730 + NHS/private filter
------------------------------------------------------
- `dental_practices.json` rebuilt from the CQC/NHSBSA
  `practices_2025_26.xlsx` register (sheet `Practices`):
    * National file: 10,098 rows. Filtered to the 8 NWL
      LADs (Brent, Ealing, H&F, Harrow, Hillingdon,
      Hounslow, K&C, Westminster) → 730 practices.
    * Previous JSON had only 257; new file has 730.
    * Every record has lat/lng: 254 reused from the
      previous JSON (postcode match); 476 geocoded from
      the local ONSPD cache (Feb 2026, incl. 6 recovered
      from terminated-postcode rows).
    * `imd_decile` preserved where available (254
      records); omitted for the 476 new rows rather than
      invented.
- New per-record field: `nhs_contracted` — boolean
  derived from the Excel `At Contract Postcode` column:
    * `Yes` → `true`  (marked "NHS / mixed" in UI)
    * `No`  → `false` (marked "Private / specialist
                       services" in UI)
  Distribution: 285 true / 445 false.
- By LAD: Westminster 256, Ealing 79, Brent 77, Harrow
  73, K&C 70, H&F 66, Hounslow 57, Hillingdon 52.

Dental UI — contract-type filter in the sidebar
-----------------------------------------------
- New `<select id="dental-contract-filter">` rendered
  directly below the "Dental practices" toggle row, with
  options:
    * All dental practices (default)
    * NHS / mixed (at contract postcode)
    * Private / specialist services
- `loadDentalLayer()` rewritten to:
    * read `nhs_contracted` off each record,
    * attach it to the marker's record in `dentalData`,
    * include a "Type" row in the marker popup + suffix
      the tooltip with "NHS / mixed" or "Private /
      specialist services",
    * delegate cluster membership to a new
      `_applyDentalFilter()` helper.
- `_applyDentalFilter()` clears + repopulates
  `dentalCluster` honouring the current
  `dentalContractFilter` ('all' | 'nhs' | 'private'),
  and updates the `#dental-count` badge to reflect the
  filtered count.
- `refreshPointVisibility()` updated to call
  `_applyDentalFilter()` instead of blindly re-adding
  every dental marker — so the NHS/private filter
  survives neighbourhood-focus state changes.
- Records with `nhs_contracted === null` are treated as
  non-NHS for the 'private' view (defensive; current
  dataset has no nulls).

PNG export — SVG-DOM overlay mask + fit + crop (final)
------------------------------------------------------
- Pure-screenshot version was too wide: users had to
  pre-frame the viewport and still got all of London +
  surrounds in the image. Restored fit-to-scope + mask +
  tight-crop behaviour but rebuilt the mask on top of a
  real SVG DOM element rather than Canvas-2D paths.
- Flow:
    1. Save current center/zoom.
    2. Hide dl-badge + every Leaflet control corner
       (.leaflet-top, .leaflet-bottom,
       .leaflet-control-attribution).
    3. fitBounds to _pngScopeBounds() (NWL or focused
       borough) with padding:[24,24], animate:false.
       Await 'moveend' (+ 500 ms safety timeout) then
       double-rAF + 160 ms settle before touching
       projection-dependent math.
    4. waitForTiles + 220 ms + double-rAF.
    5. Build an SVG element positioned absolute over
       the map div. Single `<path>` contains:
         * Outer rectangle (full map-div container
           pixel coords: 0,0 -> W,H),
         * Inner rings for every scope polygon (NWL
           boroughs OR the focused borough) projected
           via map.latLngToContainerPoint().
       fill-rule='evenodd' fills the gap between outer
       and inner rings white, so basemap outside
       NWL/borough becomes white; inside stays
       untouched.
    6. Another double-rAF so the SVG paints.
    7. html2canvas on the map div at devicePixelRatio,
       backgroundColor:'#fff'. Because the SVG is now
       a real DOM child, html2canvas rasterises the
       mask together with the map tiles in one pass —
       no inversion, no timing race, no SVG-clipping
       quirks.
    8. Measure actual xScale/yScale from
       fullCanvas.width / mapEl.clientWidth.
    9. Tight-crop to the polygon bbox (+14 css-px pad)
       using the SAME container-pixel coords collected
       in step 5 — crop and mask cannot drift because
       they share one projection pass.
   10. Stamp caption pill (scope · overlay · Month
       YYYY), download as blob.
   11. finally: remove mask SVG, restore dl-badge +
       control corners, setView back to savedCenter/
       savedZoom.
- Result: single-click PNG, map tightly framed to NWL
  or the selected borough, no basemap leakage around
  the boundary, no reliance on the user pre-framing,
  consistent across all devicePixelRatio values.

Civic strength (London Datastore, Round 3)
------------------------------------------
- `ward_data.json` enriched with 9 new ward-level
  indicators from the London Civic Strength Index
  (Round 3) on the London Datastore. 168/168 NWL wards
  matched on ward GSS code (E05xxxxxxx).
- Excluded by user request: `Recorded crime` and
  `Ballots cast in borough council elections 2022`.
- Keys added per ward (under `indicators`), prefixed
  with `cst_`:
    * cst_number_of_community_sport_and_physical_activity_offerings  (count)
    * cst_number_of_community_interest_companies_cics               (count)
    * cst_gentrification_change_in_occupational_classes             (index)
    * cst_number_and_proximity_of_libraries                         (score)
    * cst_number_and_proximity_of_community_centres                 (score)
    * cst_number_and_proximity_of_cultural_spaces                   (score)
    * cst_number_of_faith_centres                                   (count)
    * cst_passive_green_space                                       (proportion)
    * cst_public_transport_access_levels_ptals                      (string;
      retained in data but NOT wired into UI — PTAL is
      already excluded per task 49 and is string-typed
      so not choropleth-friendly).
- New metadata fields in `ward_data.json`: `cst_source`
  and `cst_metrics_added` noting the provenance
  ("London Civic Strength Index (Round 3), London
  Datastore").

Civic strength — UI wiring
--------------------------
- `OV_DOMAIN`: 8 new entries with observed NW-London
  min/max ranges and `wh:false` for the proximity /
  count metrics (higher = more civic assets = better).
  Gentrification is wh:false (descriptive, not
  loaded).
- `CATS`: new `civic_strength` category with icon 🏛,
  label "Civic strength (London Datastore, Round 3)",
  8 ward-level fields. Rendered in ward profiles.
- `OV_META`: 8 entries with
  `src: "London Civic Strength Index (Round 3),
  London Datastore"`, `yr: "2024"`, `g: "Ward"`,
  appropriate unit labels (count / index / score /
  proportion) and human-readable descriptions.
- `<select id="ov">` dropdown: new optgroup
  "Civic strength (London Datastore, Round 3)" with
  8 options tagged " · Ward".
- `<select id="ov2">` bivariate dropdown: new
  optgroup "Civic strength (London Datastore)" with
  7 options (gentrification omitted — bivariate is
  better kept to goal-aligned indicators).
- `fmtOv()`: passive_green_space and gentrification
  render with 2 decimals; count / proximity-score
  indicators render as integers.

ICHT methodology references scrubbed
------------------------------------
- Dropped all user-visible "ICHT methodology" /
  "Imperial College Healthcare Trust" references from
  the ward IMD composite overlay while keeping the
  overlay itself (key `imd_score_ward`, pop-weighted
  mid-2022 calculation) unchanged:
    * <select id="ov">  option text:
      "IMD composite score (ICHT methodology) · Ward"
      → "IMD composite score (ward, pop-weighted) · Ward"
    * <select id="ov2"> option text:
      "IMD composite (ICHT) · Ward"
      → "IMD composite (pop-weighted) · Ward"
    * CATS label: dropped "— ICHT methodology" suffix.
    * OV_META desc: replaced "matching the Imperial
      College Healthcare Trust NW London reference
      methodology" with "using MHCLG File_7 mid-2022
      denominators".
    * Code comments (OV_META header + OV_FIELD_ALIAS
      + _pngCaption example) also scrubbed.
- Factual ICHT hospital attributions for St Mary's,
  Charing Cross, Hammersmith, and Queen Charlotte's &
  Chelsea retained — those are trust operators, not
  methodology claims.

MPS crime — 24-month CSVs aggregated to 12mo by category
--------------------------------------------------------
- Replaced the legacy `crime_total` field (previously
  sourced from data.police.uk via `fetch_all_data.py`)
  with a fresh aggregation from the Met Police MPS
  "Ward Level Crime (most recent 24 months)" and "LSOA
  Level Crime (most recent 24 months)" CSVs published
  on the London Datastore. Window: latest 12 months
  Apr 2025 – Mar 2026 (sum of monthly counts
  202504..202603).
- Eleven Home Office major categories carried at WARD
  level, ten at LSOA level (Sexual Offences not
  published at LSOA per data protection rules):
    * Violence Against the Person
    * Theft (incl. shoplifting + bicycle)
    * Burglary (residential + business)
    * Robbery (personal + business)
    * Vehicle Offences
    * Drug Offences
    * Public Order Offences
    * Arson & Criminal Damage
    * Possession of Weapons
    * Sexual Offences (ward only)
    * Misc. Crimes Against Society
  Fraud & Forgery / NFIB Fraud excluded per user spec
  (transferred to NFIB nationally in 2013).
- Coverage:
    * Ward merge: 168/168 NWL wards matched in CSV.
    * LSOA merge: 4,988 NWL LSOAs with non-zero crime
      activity (out of 33,755 in the flat lsoa_data.json
      schema London-wide).
- `crime_total` is now overwritten as the sum of the 11
  category counts (range across NWL: min ≈ 327, median
  ≈ 1,087, max ≈ 29,923 for the busiest CBD wards).
- Per-record fields injected:
    * ward_data.json: indicators.crime_<cat>_12mo
      (11 fields) + crime_total recomputed.
    * lsoa_data.json (flat dict): crime_<cat>_12mo
      written at top level (10 fields, no
      sexual_offences) + crime_total recomputed.
- Metadata in ward_data.json: crime_source ("Met Police
  MPS · London Datastore"), crime_window ("Apr 2025 -
  Mar 2026"), crime_metrics_added (UTC timestamp).

MPS crime — UI wiring
---------------------
- `OV_DOMAIN`: 11 new entries (one per category) with
  observed NW-London max ranges (e.g. theft 0..1500,
  violence 0..800, weapons 0..30) and `wh:true`
  (lower = better). Existing `crime_total` range
  recalibrated to 200..5000 (was 500..8000) to reflect
  the new MPS-derived totals.
- `CATS`: new `crime_breakdown` category with icon 🚨,
  label "Crime — by category (MPS, last 12 months)",
  11 fields all tagged `g: "ward"`. Rendered in ward
  profiles. (LSOA-level values are still readable via
  the same key thanks to the `(d.indicators || d)`
  fallback in `wardOvValue` / `lsoaOvValue`, so LSOA
  choropleths render correctly without a duplicate
  CATS row.)
- `OV_META`: 11 new entries with
  `src: "Met Police MPS · London Datastore"`,
  `yr: "Apr 2025 – Mar 2026"`,
  `g: "Ward / LSOA"` (or just "Ward" for sexual
  offences), `u: "crimes"`, plus per-category desc
  strings explaining the Home Office sub-categories
  rolled up into each field. Existing `crime_total`
  OV_META updated from data.police.uk to MPS source.
- `<select id="ov">` dropdown: new optgroup
  "Crime — by category (MPS, last 12 months)" with all
  11 options tagged " · Ward", positioned right after
  the existing "Crime & deprivation" optgroup.
- `<select id="ov2">` bivariate dropdown: new optgroup
  "Crime — by category (MPS, 12mo)" with 6 high-impact
  options (violence, theft, burglary, robbery, vehicle,
  drug offences) — bivariate kept compact to avoid
  overwhelming the second-axis dropdown.

GLA ESOL Planning Map — 4 new point layers
------------------------------------------
- Adds four new point datasets to the map, sourced from
  the Greater London Authority's English language (ESOL)
  Planning Map (London Datastore, "ESOL Planning Map"):
    * schools.json             — 750 records
    * community_centres.json   — 190 records
    * libraries.json           —  81 records
    * esol_providers.json      —  58 records
- All four filtered to the 8-borough NWL ICS footprint
  (Brent, Ealing, H&F, Harrow, Hillingdon, Hounslow,
  K&C, Westminster) — Camden excluded per project scope.
- Geocoding strategy:
    * Schools: easting/northing in source CSV reprojected
      OSGB36 (EPSG:27700) -> WGS84 (EPSG:4326) via pyproj.
      Filtered to establishmentstatus__name_ == 'Open'.
    * Community centres / libraries: lat/lng provided
      directly in source CSV. Libraries additionally
      filtered to open_status == '1'.
    * Formal ESOL providers: only postcodes provided.
      Geocoded via local ONSPD (Feb 2026) cache with a
      3-tier fallback:
        1. Exact pcds match
        2. Sector centroid (1st 5 chars, e.g. NW10 6)
        3. Outward-code centroid (e.g. NW10)
      Hit rate: 57/58 (one terminated W1P 2PD postcode
      could not be resolved, dropped).
- School phase distribution (NWL): Primary 381,
  Secondary 108, Nursery 21, All-through 8, 16+/Sixth
  form 9, Special/Other 223. ESOL provider type
  distribution: Local Authority 29, FE College 20,
  Independent Training Provider 5, Institute of Adult
  Learning 2, Charity 1, University 1.

ESOL planning — UI wiring
-------------------------
- New "Civic & education" sidebar section (between
  Point layers and Borough boundaries) with 4 toggles:
    * Schools (#tsch, blue #1E5BB8) + phase filter
      dropdown (All / Primary / Secondary / Nursery /
      All-through / 16+ / Special-Other)
    * Community centres (#tcc, orange #D87C2D)
    * Libraries (#tlib, teal #00A0A0)
    * Formal ESOL providers (#tesol, red #B83C3C) +
      type filter dropdown (All / Local Authority /
      FE College / Independent Training Provider /
      Institute of Adult Learning / Charity /
      University)
- Each layer rendered via `L.markerClusterGroup` with
  consistent styling (matching existing dental/pharmacy
  pattern): 10×10 div-icon pins, cluster bubbles
  coloured to match the layer dot, count badges on
  the sidebar row.
- Marker popups include name, address, postcode, ward
  (where available), borough, and a website link if
  present. Schools popups additionally show phase,
  type, age range, pupils/capacity, and FSM%.
- Source attribution rendered in the sidebar:
  "Source: GLA ESOL Planning Map".
- Layers default to OFF (user toggles via sidebar);
  filter dropdowns re-render the cluster + update the
  count badge live.

New scripts (ESOL planning)
---------------------------
- scripts/build_esol_v2.py: canonical builder for the
  4 JSONs. Reads source CSVs from /uploads/, applies
  NWL filter, geocodes via ONSPD where needed,
  writes flat JSON arrays to repo root.
- scripts/build_esol_layers.py: kept as a thin stub
  that runpy-delegates to build_esol_v2.py for
  backwards compatibility.

PNG export — fix blank interior (tile CORS)
-------------------------------------------
- Root cause: Leaflet's tile layer was created without
  crossOrigin: 'anonymous'. Even though html2canvas
  was invoked with useCORS:true, the underlying <img>
  tags lacked CORS attribute, so the browser refused
  to copy tile pixels onto the export canvas. Result:
  the SVG mask rendered correctly (perfect NWL shape)
  but the interior was solid white because tiles were
  unreadable.
- Fix: switched the basemap from raw OSM tiles
  (https://{s}.tile.openstreetmap.org/...) to CARTO
  Voyager (https://{s}.basemaps.cartocdn.com/voyager/
  ...) with crossOrigin:'anonymous' and subdomains
  a..d. CARTO sends Access-Control-Allow-Origin: *
  on tile responses, so html2canvas can now copy the
  tile pixels into the export.
- Side benefit: Voyager has lighter, less visually
  noisy cartography than OSM-mapnik — better behind
  choropleth fills and cleaner as a slide background.
- Side benefit: OSM's tile usage policy explicitly
  forbids using their main tile servers for app
  rendering at any volume; CARTO is intended for
  embedding.

Ward profile — redesigned as A4 at-a-glance page
-------------------------------------------------
- Replaced the table-heavy ward report with a single
  print-ready A4 page (210x297mm @ 96dpi → 794x1123
  px). All charts inline SVG — no external chart
  library, single-file principle preserved.
- Layout (top to bottom):
    HERO — kicker, ward name, borough, population,
      IMD rank-in-NWL, neighbourhood pill, Core20
      badge, IMD-score badge, mini-map (240x140 px,
      ward in colour, borough context grey, pharmacy
      dots).
    KPI STRIP — 6 cards: Population, IMD score (with
      NWL-percentile micro-bar, lower=better gradient),
      Good health % (higher=better), Bad health %,
      Disability %, Core20 LSOAs ratio. Each card has
      a "you are here" coloured gradient bar showing
      this ward's percentile within NWL.
    SERVICES STRIP — 9 cards: GP, Pharmacy, Dental,
      Hospital, Schools, Community centres, Libraries,
      ESOL providers, VCSE orgs (counts inside ward
      polygon via spatial test against marker layers).
    DEMOGRAPHICS ROW — 3 panels: ethnicity stacked bar
      (5 bands), age stacked bar (3 bands + 85+ note),
      ward-vs-NWL-median compare bars (born outside UK,
      no qualifications, level 4+ qual, higher
      managerial, routine/semi-routine, no-people-
      English households).
    HEALTH & DEPRIVATION ROW — 2 panels: 7-axis IMD
      domain radar (income, employment, education,
      health, crime, barriers, environment) with NWL
      ward-mean reference ring; ward-vs-NWL compare
      bars for 8 health/economic indicators (good
      health, bad health, disability any, disability
      lot, unpaid care, fuel poverty, unemployed,
      claimant rate). Domain values computed as the
      mean over the ward's LSOA-level domain scores;
      reference = mean across all NWL LSOAs.
    CRIME — sorted horizontal bar of 11 MPS categories
      (last 12 months, Apr 2025–Mar 2026) with a
      dashed NWL-median tick on each row. Total
      annotated in the panel header.
    CIVIC STRENGTH + GREEN/BLUE — 2 panels: 7 CST
      indicators normalised to NWL percentile (0..100,
      higher = stronger), and 6 green/blue space
      access indicators (mean of ward LSOAs).
    PAGE 2 (print) — GP practices named list +
      pharmacies named list (page-break-before).
    FOOTER — full source attribution.
- New chart helpers (all inline SVG, all factored as
  closures inside generateWardReport):
    _chartHbar(rows, opts)     — horizontal bar +
                                 optional NWL-median
                                 reference tick.
    _chartCompare(rows, opts)  — ward-vs-reference
                                 bars; bar colours red
                                 when worse, green
                                 when better, given
                                 each row's polarity
                                 ('wh' flag).
    _chartRadar(axes, opts)    — N-axis radar/spider;
                                 4-ring grid, optional
                                 dashed NWL reference
                                 ring, polygon fill +
                                 dots for ward values.
    _chartStack(parts, opts)   — stacked horizontal
                                 bar with inline %
                                 labels and a 2-line
                                 legend underneath.
- New stat helpers:
    _wardVals(k)  — collect all NWL ward values for
                    indicator k.
    _median(vs)   — sample median.
    _max(vs)      — max.
    _pctile(k, v) — % of NWL wards below v on k.
    _wardLSOADomains(wcode)   — mean of LSOA-level
                                IMD domains for the
                                ward's LSOAs.
    _nwlLSOADomainMeans()     — NWL-wide LSOA-level
                                domain means (radar
                                reference ring).
    _domainMax(k)             — max observed across
                                ALL NWL LSOAs for k
                                (radar normaliser).
    _wardLsoaMean(field)      — mean of LSOA values
                                for the selected
                                ward (used for green/
                                blue space at ward
                                level).
- Service counts in the strip use the existing
  pointInWardLayer() spatial test plus pre-cached
  _lad attributes where present. The four GLA ESOL
  layers (schools, community centres, libraries,
  formal ESOL providers) are now also counted.
- Print styles use @page A4 portrait + 12mm margin,
  page-break-inside:avoid on every panel/row, and
  page-break-before:always on the GP/pharmacy lists
  so the at-a-glance view fits on one page and the
  detail lists overflow to page 2.
- LSOA report (generateLSOAReport) is unchanged for
  now — same redesign approach can be applied in a
  follow-up commit.
'@
Set-Content -Path $msgPath -Value $msg -Encoding UTF8

git add -A
git commit -F $msgPath
git push origin main

Remove-Item -Force -ErrorAction SilentlyContinue $msgPath
