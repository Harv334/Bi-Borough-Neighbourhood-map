#!/usr/bin/env pwsh
# Push the recent UI fixes. Run from PowerShell:
#   cd C:\Users\harve\Downloads\nw-london-health-pipeline
#   powershell -ExecutionPolicy Bypass -File .\push_now.ps1

Set-Location $PSScriptRoot

# Remove any phantom locks left by the sandbox
Remove-Item -Force -ErrorAction SilentlyContinue .git\index.lock
Remove-Item -Force -ErrorAction SilentlyContinue .git\HEAD.lock

git add -A
git commit -m "fix(ui): sidebar grid layout; sliders 1-10; legend cleanup

- .sidebar -> CSS grid (auto/auto/1fr/auto) so tabs + footer stay
  pinned no matter what the active tab-pane renders
- Zoom slider: 1-10 scale, maps to Leaflet zoom 10-19
- Border slider: 1-10 scale, maps to stroke weight 0.5-5.0 px
- Legend: remove Neighbourhoods section + VCSE count, add LSOA row"

git push origin main
