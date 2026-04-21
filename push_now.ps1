#!/usr/bin/env pwsh
# Push the recent UI fixes — run from the repo root in PowerShell:
#   cd C:\Users\harve\Downloads\nw-london-health-pipeline
#   .\push_now.ps1

Set-Location $PSScriptRoot

# Remove any phantom locks left by the sandbox
Remove-Item -Force -ErrorAction SilentlyContinue .git\index.lock
Remove-Item -Force -ErrorAction SilentlyContinue .git\HEAD.lock

git add -A
git commit -m "fix(ui): harden sidebar flex containment; simplify PNG; add zoom slider; remove top-bar badges

- html/body/.app: overflow:hidden, .app max-height:100vh
- .sidebar: height:100%, max-height:100%, min-height:0
- .tab-pane: flex:1 1 0, max-height:100%
- .ward-scroll: height:0, flex:1 1 0 (flex-grow sizes it)
  -> prevents ward/LSOA panel from pushing sb-tabs out of view
- Hide viewing-strip when ward/LSOA selected; restore on deselect
- Simplify PNG export to one-click with boundary mask
- Add Zoom slider next to Fill/Border in sidebar footer
- Remove neighbourhood badges from top bar"

git push origin main
