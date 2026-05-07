from __future__ import annotations

from pathlib import Path
from typing import Any

from io_utils import load_yaml
from models import PlanningScenario, ScenarioWorkloadDemand


def load_planning_scenario(path: str | Path) -> PlanningScenario:
    scenario_path = Path(path)
    raw = load_yaml(scenario_path)
    return planning_scenario_from_yaml(raw, base_dir=scenario_path.parent)


def planning_scenario_from_yaml(obj: dict[str, Any], base_dir: Path | None = None) -> PlanningScenario:
    name = _required_str(obj, "name")
    source_state_ref = _required_str(obj, "sourceStateRef")
    target_state_ref = _required_str(obj, "targetStateRef")

    workload_order = [str(x) for x in obj.get("workloadOrder", [])]
    source_arrival = _required_mapping(obj, "sourceArrival")
    target_arrival = _required_mapping(obj, "targetArrival")
    workload_refs = _required_mapping(obj, "workloadRefs")
    profile_catalog_refs = _required_mapping(obj, "profileCatalogRefs")

    if not workload_order:
        workload_order = list(target_arrival.keys())

    _validate_same_keys(
        expected=workload_order,
        mappings={
            "sourceArrival": source_arrival,
            "targetArrival": target_arrival,
            "workloadRefs": workload_refs,
            "profileCatalogRefs": profile_catalog_refs,
        },
    )

    workloads = [
        ScenarioWorkloadDemand(
            name=workload,
            source_arrival=float(source_arrival[workload]),
            target_arrival=float(target_arrival[workload]),
            workload_ref=_resolve_ref(str(workload_refs[workload]), base_dir),
            profile_catalog_ref=_resolve_ref(str(profile_catalog_refs[workload]), base_dir),
        )
        for workload in workload_order
    ]

    return PlanningScenario(
        name=name,
        description=obj.get("description"),
        policy_ref=_resolve_optional_ref(obj.get("policyRef"), base_dir),
        mig_rules_ref=_resolve_optional_ref(obj.get("migRulesRef"), base_dir),
        source_state_ref=_resolve_ref(source_state_ref, base_dir),
        target_state_ref=target_state_ref,
        workloads=workloads,
        transition=dict(obj.get("transition", {})),
    )


def scenario_summary_dict(scenario: PlanningScenario) -> dict[str, Any]:
    return {
        "kind": "PlanningScenarioSummary",
        "name": scenario.name,
        "description": scenario.description,
        "sourceStateRef": scenario.source_state_ref,
        "targetStateRef": scenario.target_state_ref,
        "policyRef": scenario.policy_ref,
        "migRulesRef": scenario.mig_rules_ref,
        "transition": scenario.transition,
        "workloads": [
            {
                "name": workload.name,
                "sourceArrival": workload.source_arrival,
                "targetArrival": workload.target_arrival,
                "delta": workload.delta,
                "workloadRef": workload.workload_ref,
                "profileCatalogRef": workload.profile_catalog_ref,
            }
            for workload in scenario.workloads
        ],
    }


def _required_str(obj: dict[str, Any], key: str) -> str:
    value = obj.get(key)
    if value is None:
        raise ValueError(f"PlanningScenario {key} is required")
    return str(value)


def _required_mapping(obj: dict[str, Any], key: str) -> dict[str, Any]:
    value = obj.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"PlanningScenario {key} must be a mapping")
    return value


def _validate_same_keys(expected: list[str], mappings: dict[str, dict[str, Any]]) -> None:
    expected_set = set(expected)
    for name, mapping in mappings.items():
        actual = set(mapping)
        missing = sorted(expected_set - actual)
        extra = sorted(actual - expected_set)
        if missing or extra:
            raise ValueError(
                f"PlanningScenario {name} keys must match workloadOrder; "
                f"missing={missing}, extra={extra}"
            )


def _resolve_optional_ref(value: Any, base_dir: Path | None) -> str | None:
    if value is None:
        return None
    return _resolve_ref(str(value), base_dir)


def _resolve_ref(value: str, base_dir: Path | None) -> str:
    path = Path(value)
    if path.is_absolute() or base_dir is None:
        return str(path)
    if value.startswith("target"):
        return value
    return str((base_dir / path).resolve())
