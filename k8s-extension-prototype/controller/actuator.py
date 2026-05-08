from __future__ import annotations

import sys
import time
from typing import Any

import yaml

from executor_preview import validate_executor_preview
from k8s_api import KubernetesClient, PythonKubernetesClient


def run_dry_run_actuator_loop(
    namespace: str = "or-sim",
    poll_interval_s: float = 10.0,
    max_cycles: int | None = None,
    client: KubernetesClient | None = None,
) -> dict[str, Any]:
    client = client or PythonKubernetesClient()
    cycle = 0
    last_summary: dict[str, Any] = {}
    while max_cycles is None or cycle < max_cycles:
        cycle += 1
        action_plans = client.list_migactionplans(namespace=namespace)
        summary = {
            "cycle": cycle,
            "namespace": namespace,
            "seen": len(action_plans),
            "succeeded": [],
            "blocked": [],
            "skipped": [],
            "errors": [],
        }

        for action_plan in action_plans:
            name = str(action_plan.get("metadata", {}).get("name", ""))
            if not name:
                continue
            if not should_actuate_dry_run(action_plan):
                summary["skipped"].append(name)
                continue
            try:
                status = validate_and_succeed_dry_run_action_plan(
                    action_plan=action_plan,
                    namespace=namespace,
                    client=client,
                )
                client.patch_migactionplan_status(name=name, namespace=namespace, status=status)
                if status["phase"] == "SucceededDryRun":
                    summary["succeeded"].append(name)
                else:
                    summary["blocked"].append({"name": name, "reasons": status.get("reasons", [])})
            except Exception as exc:
                status = {
                    "phase": "ExecutionBlocked",
                    "approved": bool(action_plan.get("status", {}).get("approved", False)),
                    "executed": False,
                    "message": str(exc),
                }
                try:
                    client.patch_migactionplan_status(name=name, namespace=namespace, status=status)
                except Exception as patch_exc:
                    status["patchError"] = str(patch_exc)
                summary["errors"].append({"name": name, "message": str(exc)})

        last_summary = summary
        _log_actuator_summary(summary)
        if max_cycles is not None and cycle >= max_cycles:
            break
        time.sleep(float(poll_interval_s))

    return {
        "kind": "MigActionPlanDryRunActuatorSummary",
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "lastCycle": last_summary,
    }


def should_actuate_dry_run(action_plan: dict[str, Any]) -> bool:
    spec = dict(action_plan.get("spec", {}))
    status = dict(action_plan.get("status", {}))
    return (
        bool(spec.get("dryRun", False))
        and status.get("phase") == "ApprovedDryRun"
        and bool(status.get("approved", False))
        and not bool(status.get("executed", False))
    )


def validate_and_succeed_dry_run_action_plan(
    action_plan: dict[str, Any],
    namespace: str,
    client: KubernetesClient,
) -> dict[str, Any]:
    metadata = dict(action_plan.get("metadata", {}))
    spec = dict(action_plan.get("spec", {}))
    status = dict(action_plan.get("status", {}))
    reasons = []

    if spec.get("executor") != "nvidia-gpu-operator":
        reasons.append(f"unsupported executor {spec.get('executor')}")
    if not spec.get("dryRun"):
        reasons.append("dryRun actuator refuses non-dry-run action plans")
    reasons.extend(validate_executor_preview(spec.get("executorPreview")))

    full_plan_name = spec.get("fullPlanConfigMap")
    canonical_state_name = spec.get("canonicalNextStateConfigMap")
    full_status = _load_full_plan_status(
        name=str(full_plan_name or ""),
        namespace=namespace,
        client=client,
        reasons=reasons,
    )
    _load_canonical_state(
        name=str(canonical_state_name or ""),
        namespace=namespace,
        client=client,
        reasons=reasons,
    )

    expected_action_count = int(spec.get("actionCount", 0))
    actual_action_count = len(list(full_status.get("actions", []))) if full_status else 0
    if full_status and actual_action_count != expected_action_count:
        reasons.append(
            f"full-plan action count {actual_action_count} does not match spec.actionCount {expected_action_count}"
        )

    if reasons:
        return {
            "phase": "ExecutionBlocked",
            "approved": bool(status.get("approved", False)),
            "executed": False,
            "policyRef": status.get("policyRef"),
            "reasons": reasons,
            "message": "Dry-run actuator validation failed; no hardware actions executed.",
        }

    return {
        "phase": "SucceededDryRun",
        "approved": True,
        "executed": True,
        "policyRef": status.get("policyRef"),
        "observedGeneration": int(metadata.get("generation", 0)),
        "validated": {
            "fullPlanConfigMap": full_plan_name,
            "canonicalNextStateConfigMap": canonical_state_name,
            "actionCount": expected_action_count,
            "executor": spec.get("executor"),
            "executorPreview": {
                "previewOnly": spec.get("executorPreview", {}).get("previewOnly"),
                "gpuTargetCount": len(spec.get("executorPreview", {}).get("gpuTargets", [])),
                "wouldPatchNodeLabelCount": len(
                    spec.get("executorPreview", {}).get("wouldPatchNodeLabels", {})
                ),
                "unresolvedPhysicalGpuIds": spec.get("executorPreview", {}).get(
                    "unresolvedPhysicalGpuIds", []
                ),
            },
        },
        "message": "Dry-run actuator validated the approved action plan. No hardware actions executed.",
    }


def _load_full_plan_status(
    name: str,
    namespace: str,
    client: KubernetesClient,
    reasons: list[str],
) -> dict[str, Any] | None:
    if not name:
        reasons.append("fullPlanConfigMap is required")
        return None
    try:
        configmap = client.get_configmap(name=name, namespace=namespace)
    except Exception as exc:
        reasons.append(f"fullPlanConfigMap {name} is not readable: {exc}")
        return None
    raw = dict(configmap.get("data", {})).get("status.yaml")
    if raw is None:
        reasons.append(f"fullPlanConfigMap {name} does not contain data.status.yaml")
        return None
    obj = yaml.safe_load(raw)
    if not isinstance(obj, dict):
        reasons.append(f"fullPlanConfigMap {name} data.status.yaml is not a YAML object")
        return None
    return obj


def _load_canonical_state(
    name: str,
    namespace: str,
    client: KubernetesClient,
    reasons: list[str],
) -> dict[str, Any] | None:
    if not name:
        reasons.append("canonicalNextStateConfigMap is required")
        return None
    try:
        configmap = client.get_configmap(name=name, namespace=namespace)
    except Exception as exc:
        reasons.append(f"canonicalNextStateConfigMap {name} is not readable: {exc}")
        return None
    raw = dict(configmap.get("data", {})).get("state.yaml")
    if raw is None:
        reasons.append(f"canonicalNextStateConfigMap {name} does not contain data.state.yaml")
        return None
    obj = yaml.safe_load(raw)
    if not isinstance(obj, dict):
        reasons.append(f"canonicalNextStateConfigMap {name} data.state.yaml is not a YAML object")
        return None
    return obj


def _log_actuator_summary(obj: dict[str, Any]) -> None:
    print(yaml.safe_dump(obj, sort_keys=False).strip(), file=sys.stderr, flush=True)
