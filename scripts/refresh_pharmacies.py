"""
DEPRECATED - kept as a thin wrapper for anyone running the old command.

The canonical way to refresh pharmacies is now:

    pipeline run --source pharmacies

which writes:
    - data/healthcare/pharmacies.parquet  (source of truth)
    - pharmacies.json                     (map reads this at load time)

See docs/HANDOVER.md for the full local-run recipe.
"""
from __future__ import annotations

import subprocess
import sys


def main() -> int:
    print("scripts/refresh_pharmacies.py is deprecated.", file=sys.stderr)
    print("Running `pipeline run --source pharmacies` instead.\n", file=sys.stderr)
    return subprocess.call([sys.executable, "-m", "pipeline.src.cli",
                            "run", "--source", "pharmacies"])


if __name__ == "__main__":
    sys.exit(main())
