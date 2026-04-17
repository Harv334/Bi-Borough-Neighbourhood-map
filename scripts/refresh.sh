#\!/usr/bin/env bash
# =========================================================================
# NW London Health Pipeline - one-button refresh (Mac/Linux)
#
# Run from repo root:  ./scripts/refresh.sh
# =========================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

if [ \! -d .venv ]; then
    echo "Creating .venv..."
    python3 -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate

echo "Installing pipeline package..."
pip install --quiet --upgrade pip
pip install --quiet -e .

echo "Running all enabled fetchers..."
pipeline run "$@"

echo
echo "==== Done. Git status: ===="
git status --short
echo
echo "If the output looks right: git add . && git commit -m 'data: refresh $(date +%Y-%m-%d)'"
