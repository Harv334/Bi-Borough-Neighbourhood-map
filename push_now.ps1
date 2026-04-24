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
- Re-added fit-to-NWL so the PNG is always centred on
  NWL regardless of the user's current pan. Uses
  `setView(center, getBoundsZoom(...))` instead of
  `fitBounds` — more reliable, move completes within the
  awaited `moveend`.
- Tight-cropped to the NWL pixel bbox (+16 css-px pad)
  computed from the SAME projection pass used to build
  the mask, so the crop always matches the mask exactly.
- User's original center/zoom restored in finally so
  they're unaware the map moved during capture.
'@
Set-Content -Path $msgPath -Value $msg -Encoding UTF8

git add -A
git commit -F $msgPath
git push origin main

Remove-Item -Force -ErrorAction SilentlyContinue $msgPath
