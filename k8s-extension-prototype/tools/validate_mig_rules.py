from __future__ import annotations

import argparse
from pathlib import Path

import yaml


EXPECTED_ABSTRACT_TEMPLATE_COUNT = 14
EXPECTED_PHYSICAL_REALIZATION_COUNT = 19


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description="Validate prototype A100 MIG rules.")
    parser.add_argument(
        "--rules",
        type=Path,
        default=repo_root / "k8s-extension-prototype/mock/mig-rules/a100-40gb.yaml",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    with args.rules.open("r", encoding="utf-8") as f:
        rules = yaml.safe_load(f)

    profiles = {item["name"]: int(item["slices"]) for item in rules["profiles"]}
    templates = rules["templates"]
    errors = []

    if len(templates) != EXPECTED_ABSTRACT_TEMPLATE_COUNT:
        errors.append(f"expected {EXPECTED_ABSTRACT_TEMPLATE_COUNT} templates, got {len(templates)}")

    physical_count = 0
    for template in templates:
        name = template["name"]
        realizations = template.get("physicalRealizations", [])
        physical_count += len(realizations)
        if not realizations:
            errors.append(f"{name}: missing physicalRealizations")
        for idx, realization in enumerate(realizations):
            total = sum(profiles[p] for p in realization["profiles"])
            if total > int(rules["sliceCount"]):
                errors.append(f"{name}[{idx}]: uses {total} slices > {rules['sliceCount']}")
        capacity = template["capacity"]
        for profile_name, count in capacity.items():
            if profile_name not in profiles:
                errors.append(f"{name}: unknown capacity profile {profile_name}")
            if int(count) < 0:
                errors.append(f"{name}: negative count for {profile_name}")

    if physical_count != EXPECTED_PHYSICAL_REALIZATION_COUNT:
        errors.append(
            f"expected {EXPECTED_PHYSICAL_REALIZATION_COUNT} physical realizations, got {physical_count}"
        )

    if errors:
        print("MIG rules validation failed:")
        for error in errors:
            print(f"- {error}")
        return 1

    print(
        "MIG rules validation passed: "
        f"{len(templates)} abstract templates, {physical_count} physical realizations"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

