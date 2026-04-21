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
fix(ui): harden outer layout with CSS grid so tabs can't disappear

Tabs (Layers/VCSE/Overlay/Nbhds/Wards) were intermittently pushed
behind the viewing strip + sidebar footer when a ward was deselected
- the entire UI slid upward leaving grey space under the map
controls. Regression traced to the interaction between the new
search/Reset/CB/PNG toolbar, collapsible sidebar sections, and the
old flex-based outer layout: flex children of .app / .main / .map-wrap
could be pushed out of their slots when viewing-strip display toggled
between none and flex.

Fix: convert all outer containers to rigid CSS grid with
minmax(0, 1fr) + overflow:hidden + min-height:0 so children can
never overflow their slots.

- .app: grid-template-rows auto minmax(0,1fr) auto (header / main /
  status) with height 100vh and overflow hidden.
- .main: grid-template-columns auto minmax(0,1fr) (sidebar / map).
- .map-wrap: grid-template-rows auto minmax(0,1fr) (strip / map).
- .sidebar: grid-template-rows auto auto minmax(0,1fr) auto with
  grid-template-areas focus/tabs/pane/footer; tabs and footer now
  live in rigid grid cells, pane auto-shrinks.
- Tab-pane toggles display none/flex via .active; min-height 0.
- Remove wireSidebarFooterMeasure IIFE and the sidebar-footer-h CSS
  var (obsolete with grid).
- Revert earlier relocation: Reset / CB safe / PNG / CSV + search
  are back in the top header toolbar (.hdr) where they were.
- Strip trailing virtiofs padding (null bytes + whitespace) from
  index.html tail.
- Fix white-screen-on-ward-click regression: the viewing-strip was
  being hidden/shown on ward & LSOA click, which changed the
  .map-wrap grid row height and caused Leaflet to paint tiles at
  stale dimensions (resulting in a blank white map). Fix: leave the
  viewing-strip visible at all times and just swap its digest text
  to "Ward: X" / "LSOA: X" / "All of NW London". Map-wrap grid
  geometry now never changes, so the map never needs to repaint on
  selection change.

Confirmed fix: ward deselect no longer pushes UI upward, tabs stay
pinned at all times.
'@
Set-Content -Path $msgPath -Value $msg -Encoding UTF8

git add -A
git commit -F $msgPath
git push origin main

Remove-Item -Force -ErrorAction SilentlyContinue $msgPath
