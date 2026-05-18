from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from migrant_core.partial_reconfig import partial_reconfig_template_targets  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="List template pairs that can use partial MIG reconfiguration.")
    parser.add_argument("--json", action="store_true", help="print machine-readable JSON")
    args = parser.parse_args()

    targets = partial_reconfig_template_targets()
    if args.json:
        print(json.dumps(targets, indent=2, sort_keys=False))
        return 0

    for source, target_list in targets.items():
        value = ", ".join(target_list) if target_list else "-"
        print(f"{source}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
