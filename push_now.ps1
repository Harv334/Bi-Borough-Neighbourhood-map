#!/usr/bin/env pwsh
# Push the recent UI fixes. Run from PowerShell:
#   cd C:\Users\harve\Downloads\nw-london-health-pipeline
#   powershell -ExecutionPolicy Bypass -File .\push_now.ps1

Set-Location $PSScriptRoot

# Remove any phantom locks left by the sandbox
Remove-Item -Force -ErrorAction SilentlyContinue .git\index.lock
Remove-Item -Force -ErrorAction SilentlyContinue .git\HEAD.lock

git add -A
git commit -m "fix(ui): remove top toolbar; relocate actions to sidebar footer

- Delete the heavy .hdr bar; replace with a thin logo strip so the
  sidebar tabs (Layers/VCSE/Overlay) have nothing above them that can
  overlap or visually push them off-screen.
- Move Reset / CB safe / PNG / CSV buttons into a 2x2 grid at the top
  of the sidebar footer (sb-sec), above the Fill/Border/Zoom sliders.
- Move the global search input into the sidebar footer. Make its
  results dropdown open UPWARD (bottom:100%) so it isn't clipped.
- Sidebar layout unchanged otherwise: absolute-positioned tabs + footer
  with ResizeObserver re-measuring footer height into the
  --sidebar-footer-h CSS var, so the tab-pane auto-shrinks to fit the
  new (taller) footer and tabs stay pinned.
- PNG export: _buildWhiteMask() now respects focusedBorough with name
  normalisation (City of Westminster vs Westminster). Borough-solo
  mode paints everything outside the chosen borough white for clean
  PowerPoint exports.
- Remove the Copy-link button + handler (can be re-wired later if
  shareable URLs are needed).
- Zoom + Border sliders on a 1-10 scale (zoom -> Leaflet 10-19,
  border -> 0.5-5.0 px stroke).
- Legend: drop Neighbourhoods section + (247) VCSE count; add LSOA row."

git push origin main
