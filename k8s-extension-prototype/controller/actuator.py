from __future__ import annotations

import sys
import time
from typing import Any

import yaml

from adapters.contracts import (
    DryRunMigGeometryAdapter,
    DryRunObserverAdapter,
    DryRunPodLifecycleAdapter,
    DryRunRouterDrainAdapter,
)
from adapters.observer_adapter import DryRunObservedStateBuilder
from adapters.pod_lifecycle_adapter import DryRunPodLifecyclePlanBuilder
from adapters.router_adapter import DryRunRouterPlanBuilder
from executor_preview import validate_action_previews, validate_executor_preview
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
    reasons.extend(validate_action_previews(spec))

    full_plan_name = spec.get("fullPlanConfigMap")
    canonical_state_name = spec.get("canonicalNextStateConfigMap")
    full_status = _load_full_plan_status(
        name=str(full_plan_name or ""),
        namespace=namespace,
        client=client,
        reasons=reasons,
    )
    canonical_state = _load_canonical_state(
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
    adapter_contracts = _run_adapter_dry_run_contracts(spec) if not reasons else {}
    router_plan_configmap = None
    pod_lifecycle_plan_configmap = None
    router_plan_crs: dict[str, list[str]] = {}
    pod_lifecycle_plan_crs: list[str] = []
    child_resource_statuses: list[dict[str, Any]] = []
    observed_state_preview: dict[str, Any] = {}
    observed_cluster_state_ref = None
    dry_run_execution_log_configmap = None
    dry_run_execution_log_summary: dict[str, Any] = {}
    if not reasons:
        action_plan_name = str(metadata.get("name", ""))
        action_plan_generation = int(metadata.get("generation", 0))
        router_plan = DryRunRouterPlanBuilder().build(
            action_plan_name=action_plan_name,
            namespace=namespace,
            traffic_and_drain_preview=dict(spec.get("trafficAndDrainPreview", {})),
        )
        pod_lifecycle_plan = DryRunPodLifecyclePlanBuilder().build(
            action_plan_name=action_plan_name,
            namespace=namespace,
            pod_lifecycle_preview=dict(spec.get("podLifecyclePreview", {})),
        )
        router_plan_configmap = _upsert_router_dry_run_plan_configmap(
            action_plan_name=str(metadata.get("name", "")),
            namespace=namespace,
            plan=router_plan,
            client=client,
        )
        pod_lifecycle_plan_configmap = _upsert_pod_lifecycle_dry_run_plan_configmap(
            action_plan_name=str(metadata.get("name", "")),
            namespace=namespace,
            plan=pod_lifecycle_plan,
            client=client,
        )
        router_plan_crs = _upsert_router_dry_run_plan_custom_resources(
            action_plan_name=action_plan_name,
            action_plan_generation=action_plan_generation,
            namespace=namespace,
            plan=router_plan,
            client=client,
        )
        pod_lifecycle_plan_crs = _upsert_pod_lifecycle_dry_run_plan_custom_resources(
            action_plan_name=action_plan_name,
            action_plan_generation=action_plan_generation,
            namespace=namespace,
            plan=pod_lifecycle_plan,
            client=client,
        )
        child_resource_statuses = _child_resource_statuses(
            router_plan_crs=router_plan_crs,
            pod_lifecycle_plan_crs=pod_lifecycle_plan_crs,
            action_plan_name=action_plan_name,
            action_plan_generation=action_plan_generation,
        )
        reasons.extend(_child_resource_status_reasons(child_resource_statuses))
        observed_state_preview = _build_observed_state_preview(spec, canonical_state)
        observed_cluster_state_ref = _upsert_observed_cluster_state_preview(
            action_plan_name=action_plan_name,
            action_plan_generation=action_plan_generation,
            namespace=namespace,
            spec=spec,
            observed_state_preview=observed_state_preview,
            client=client,
        )
        dry_run_execution_log = _build_dry_run_execution_log(
            action_plan_name=action_plan_name,
            action_plan_generation=action_plan_generation,
            spec=spec,
            full_status=full_status,
            canonical_state=canonical_state,
            child_resource_statuses=child_resource_statuses,
            observed_state_preview=observed_state_preview,
        )
        dry_run_execution_log_configmap = _upsert_dry_run_execution_log_configmap(
            action_plan_name=action_plan_name,
            namespace=namespace,
            log=dry_run_execution_log,
            client=client,
        )
        dry_run_execution_log_summary = _dry_run_execution_log_summary(dry_run_execution_log)

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
            "previews": {
                "migGeometryPreview": bool(spec.get("migGeometryPreview")),
                "trafficAndDrainPreview": bool(spec.get("trafficAndDrainPreview")),
                "podLifecyclePreview": bool(spec.get("podLifecyclePreview")),
                "abstractActionPreview": bool(spec.get("abstractActionPreview")),
                "adapterDryRunPreview": bool(spec.get("adapterDryRunPreview")),
                "observerPreview": bool(spec.get("observerPreview")),
            },
            "adapterContracts": {
                "mig": _adapter_contract_summary(adapter_contracts.get("mig", {})),
                "router": _adapter_contract_summary(adapter_contracts.get("router", {})),
                "pod": _adapter_contract_summary(adapter_contracts.get("pod", {})),
                "observer": _adapter_contract_summary(adapter_contracts.get("observer", {})),
            },
            "observedStatePreview": _observed_state_preview_summary(observed_state_preview),
            "observedClusterStateRef": observed_cluster_state_ref,
            "dryRunExecutionLogConfigMap": dry_run_execution_log_configmap,
            "dryRunExecutionLog": dry_run_execution_log_summary,
            "mockRouterPlanConfigMap": router_plan_configmap,
            "mockPodLifecyclePlanConfigMap": pod_lifecycle_plan_configmap,
            "workloadRoutePlans": router_plan_crs.get("workloadRoutePlans", []),
            "servingInstanceDrains": router_plan_crs.get("servingInstanceDrains", []),
            "podLifecyclePlans": pod_lifecycle_plan_crs,
            "childResources": {
                "total": len(child_resource_statuses),
                "succeededDryRunPreview": len(
                    [
                        item
                        for item in child_resource_statuses
                        if item.get("phase") == "SucceededDryRunPreview"
                    ]
                ),
                "items": child_resource_statuses,
            },
        },
        "message": "Dry-run actuator validated the approved action plan. No hardware actions executed.",
    }


def _run_adapter_dry_run_contracts(spec: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        "mig": DryRunMigGeometryAdapter().preview(dict(spec.get("migGeometryPreview", {}))),
        "router": DryRunRouterDrainAdapter().preview(dict(spec.get("trafficAndDrainPreview", {}))),
        "pod": DryRunPodLifecycleAdapter().preview(dict(spec.get("podLifecyclePreview", {}))),
        "observer": DryRunObserverAdapter().preview(dict(spec.get("observerPreview", {}))),
    }


def _adapter_contract_summary(result: dict[str, Any]) -> dict[str, Any]:
    summary = {"previewOnly": bool(result.get("previewOnly", False))}
    for key, value in result.items():
        if key == "previewOnly":
            continue
        if isinstance(value, list):
            summary[f"{key}Count"] = len(value)
        elif isinstance(value, dict):
            summary[f"{key}Count"] = len(value)
        elif value is not None:
            summary[key] = value
    return summary


def _build_observed_state_preview(
    spec: dict[str, Any],
    canonical_state: dict[str, Any] | None,
) -> dict[str, Any]:
    return DryRunObservedStateBuilder().build(
        observer_preview=dict(spec.get("observerPreview", {})),
        canonical_state=canonical_state,
    )


def _upsert_observed_cluster_state_preview(
    action_plan_name: str,
    action_plan_generation: int,
    namespace: str,
    spec: dict[str, Any],
    observed_state_preview: dict[str, Any],
    client: KubernetesClient,
) -> str:
    name = f"{action_plan_name}-observed-state"
    missing_inputs = list(observed_state_preview.get("missingRealClusterInputs", []))
    manifest = {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "ObservedClusterState",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "or-sim-mig-planner",
                "mig.or-sim.io/owner-action-plan": action_plan_name,
                "mig.or-sim.io/preview-only": "true",
            },
        },
        "spec": {
            "previewOnly": True,
            "source": "dry-run-observer-skeleton",
            "ownerActionPlan": action_plan_name,
            "observedState": dict(observed_state_preview.get("observedState", {})),
            "missingRealClusterInputs": missing_inputs,
            "canonicalizationRule": observed_state_preview.get("canonicalizationRule"),
            "canonicalNextStateConfigMap": spec.get("canonicalNextStateConfigMap"),
            "notes": [
                "Preview-only observer artifact for kind.",
                "A real observer must replace this with node/GPU/MIG/Pod/router observations before canonicalization is trusted.",
            ],
        },
    }
    client.apply_observedclusterstate(manifest)
    client.patch_observedclusterstate_status(
        name=name,
        namespace=namespace,
        status={
            "phase": "SucceededDryRunPreview",
            "previewOnly": True,
            "readyForCanonicalization": False,
            "ownerActionPlan": action_plan_name,
            "validatedBy": "mig-dry-run-actuator",
            "missingRealClusterInputCount": len(missing_inputs),
            "observedGeneration": action_plan_generation,
            "message": (
                "ObservedClusterState preview recorded. It is not ready for real "
                "canonicalization because live node/GPU/runtime inputs are missing."
            ),
        },
    )
    return name


def _build_dry_run_execution_log(
    action_plan_name: str,
    action_plan_generation: int,
    spec: dict[str, Any],
    full_status: dict[str, Any] | None,
    canonical_state: dict[str, Any] | None,
    child_resource_statuses: list[dict[str, Any]],
    observed_state_preview: dict[str, Any],
) -> dict[str, Any]:
    actions = list((full_status or {}).get("actions", []))
    return {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "DryRunExecutionObservationLog",
        "metadata": {
            "name": f"{action_plan_name}-execution-log",
        },
        "previewOnly": True,
        "source": "dry-run-simulator",
        "ownerActionPlan": action_plan_name,
        "ownerActionPlanGeneration": action_plan_generation,
        "notes": [
            "This log is a preview artifact only.",
            "canonicalNextState is a simulated next input in kind; real execution must observe post-action cluster state first.",
        ],
        "steps": [
            {
                "phase": "BeforeAction",
                "fullPlanConfigMap": spec.get("fullPlanConfigMap"),
                "actionCount": len(actions),
            },
            {
                "phase": "WouldApplyMigGeometry",
                "previewOnly": True,
                "geometryActionCount": len(
                    list(dict(spec.get("migGeometryPreview", {})).get("geometryActions", []))
                ),
                "wouldPatchNodeLabelCount": len(
                    dict(dict(spec.get("migGeometryPreview", {})).get("wouldPatchNodeLabels", {}))
                ),
            },
            {
                "phase": "WouldDrainTraffic",
                "previewOnly": True,
                "childRefs": [
                    {"kind": item["kind"], "name": item["name"], "phase": item["phase"]}
                    for item in child_resource_statuses
                    if item.get("kind") in {"WorkloadRoutePlan", "ServingInstanceDrain"}
                ],
            },
            {
                "phase": "WouldUpdatePods",
                "previewOnly": True,
                "childRefs": [
                    {"kind": item["kind"], "name": item["name"], "phase": item["phase"]}
                    for item in child_resource_statuses
                    if item.get("kind") == "PodLifecyclePlan"
                ],
            },
            {
                "phase": "SimulatedPostActionObservation",
                "previewOnly": True,
                "observedStatePreview": _observed_state_preview_summary(observed_state_preview),
                "missingRealClusterInputs": list(
                    observed_state_preview.get("missingRealClusterInputs", [])
                ),
            },
            {
                "phase": "SimulatedCanonicalization",
                "previewOnly": True,
                "canonicalNextStateConfigMap": spec.get("canonicalNextStateConfigMap"),
                "canonicalNextStateGpuCount": len(list((canonical_state or {}).get("gpus", []))),
                "warning": (
                    "This canonical next input is simulated in kind. In real execution it must be "
                    "derived only after actions execute and observer reports actual post-action state."
                ),
            },
        ],
    }


def _upsert_dry_run_execution_log_configmap(
    action_plan_name: str,
    namespace: str,
    log: dict[str, Any],
    client: KubernetesClient,
) -> str:
    name = str(log["metadata"]["name"])
    client.apply_configmap(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": {
                    "app.kubernetes.io/name": "or-sim-mig-planner",
                    "mig.or-sim.io/state-kind": "dry-run-execution-log",
                    "mig.or-sim.io/owner-action-plan": action_plan_name,
                },
            },
            "data": {
                "execution-log.yaml": yaml.safe_dump(log, sort_keys=False),
            },
        }
    )
    return name


def _dry_run_execution_log_summary(log: dict[str, Any]) -> dict[str, Any]:
    steps = list(log.get("steps", []))
    return {
        "previewOnly": bool(log.get("previewOnly", False)),
        "source": log.get("source"),
        "stepCount": len(steps),
        "phases": [str(step.get("phase")) for step in steps],
    }


def _upsert_router_dry_run_plan_configmap(
    action_plan_name: str,
    namespace: str,
    plan: dict[str, Any],
    client: KubernetesClient,
) -> str:
    name = str(plan["metadata"]["name"])
    client.apply_configmap(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": {
                    "app.kubernetes.io/name": "or-sim-mig-planner",
                    "mig.or-sim.io/state-kind": "router-dry-run-plan",
                    "mig.or-sim.io/owner-action-plan": action_plan_name,
                },
            },
            "data": {
                "router-plan.yaml": yaml.safe_dump(plan, sort_keys=False),
            },
        }
    )
    return name


def _upsert_router_dry_run_plan_custom_resources(
    action_plan_name: str,
    action_plan_generation: int,
    namespace: str,
    plan: dict[str, Any],
    client: KubernetesClient,
) -> dict[str, list[str]]:
    refs = {"workloadRoutePlans": [], "servingInstanceDrains": []}
    for child in list(plan.get("workloadRoutePlans", [])):
        manifest = _namespaced_child_cr(child, namespace=namespace, action_plan_name=action_plan_name)
        client.apply_workloadrouteplan(manifest)
        name = str(manifest["metadata"]["name"])
        client.patch_workloadrouteplan_status(
            name=name,
            namespace=namespace,
            status=_child_dry_run_status(
                action_plan_name=action_plan_name,
                action_plan_generation=action_plan_generation,
                kind="WorkloadRoutePlan",
            ),
        )
        refs["workloadRoutePlans"].append(name)
    for child in list(plan.get("servingInstanceDrains", [])):
        manifest = _namespaced_child_cr(child, namespace=namespace, action_plan_name=action_plan_name)
        client.apply_servinginstancedrain(manifest)
        name = str(manifest["metadata"]["name"])
        client.patch_servinginstancedrain_status(
            name=name,
            namespace=namespace,
            status=_child_dry_run_status(
                action_plan_name=action_plan_name,
                action_plan_generation=action_plan_generation,
                kind="ServingInstanceDrain",
            ),
        )
        refs["servingInstanceDrains"].append(name)
    return refs


def _upsert_pod_lifecycle_dry_run_plan_configmap(
    action_plan_name: str,
    namespace: str,
    plan: dict[str, Any],
    client: KubernetesClient,
) -> str:
    name = str(plan["metadata"]["name"])
    client.apply_configmap(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": {
                    "app.kubernetes.io/name": "or-sim-mig-planner",
                    "mig.or-sim.io/state-kind": "pod-lifecycle-dry-run-plan",
                    "mig.or-sim.io/owner-action-plan": action_plan_name,
                },
            },
            "data": {
                "pod-lifecycle-plan.yaml": yaml.safe_dump(plan, sort_keys=False),
            },
        }
    )
    return name


def _upsert_pod_lifecycle_dry_run_plan_custom_resources(
    action_plan_name: str,
    action_plan_generation: int,
    namespace: str,
    plan: dict[str, Any],
    client: KubernetesClient,
) -> list[str]:
    refs = []
    for child in list(plan.get("podLifecyclePlans", [])):
        manifest = _namespaced_child_cr(child, namespace=namespace, action_plan_name=action_plan_name)
        client.apply_podlifecycleplan(manifest)
        name = str(manifest["metadata"]["name"])
        client.patch_podlifecycleplan_status(
            name=name,
            namespace=namespace,
            status=_child_dry_run_status(
                action_plan_name=action_plan_name,
                action_plan_generation=action_plan_generation,
                kind="PodLifecyclePlan",
            ),
        )
        refs.append(name)
    return refs


def _child_dry_run_status(
    action_plan_name: str,
    action_plan_generation: int,
    kind: str,
) -> dict[str, Any]:
    return {
        "phase": "SucceededDryRunPreview",
        "previewOnly": True,
        "ownerActionPlan": action_plan_name,
        "validatedBy": "mig-dry-run-actuator",
        "observedGeneration": action_plan_generation,
        "message": f"{kind} dry-run contract accepted; no runtime changes executed.",
    }


def _child_resource_statuses(
    router_plan_crs: dict[str, list[str]],
    pod_lifecycle_plan_crs: list[str],
    action_plan_name: str,
    action_plan_generation: int,
) -> list[dict[str, Any]]:
    statuses = []
    for kind, key in (
        ("WorkloadRoutePlan", "workloadRoutePlans"),
        ("ServingInstanceDrain", "servingInstanceDrains"),
    ):
        for name in router_plan_crs.get(key, []):
            statuses.append(
                _child_resource_status_item(
                    kind=kind,
                    name=name,
                    action_plan_name=action_plan_name,
                    action_plan_generation=action_plan_generation,
                )
            )
    for name in pod_lifecycle_plan_crs:
        statuses.append(
            _child_resource_status_item(
                kind="PodLifecyclePlan",
                name=name,
                action_plan_name=action_plan_name,
                action_plan_generation=action_plan_generation,
            )
        )
    return statuses


def _child_resource_status_item(
    kind: str,
    name: str,
    action_plan_name: str,
    action_plan_generation: int,
) -> dict[str, Any]:
    return {
        "kind": kind,
        "name": name,
        **_child_dry_run_status(
            action_plan_name=action_plan_name,
            action_plan_generation=action_plan_generation,
            kind=kind,
        ),
    }


def _child_resource_status_reasons(child_resource_statuses: list[dict[str, Any]]) -> list[str]:
    reasons = []
    for item in child_resource_statuses:
        if item.get("phase") != "SucceededDryRunPreview":
            reasons.append(
                f"{item.get('kind')} {item.get('name')} did not reach SucceededDryRunPreview"
            )
        if item.get("previewOnly") is not True:
            reasons.append(f"{item.get('kind')} {item.get('name')} is not previewOnly")
    return reasons


def _namespaced_child_cr(
    child: dict[str, Any],
    namespace: str,
    action_plan_name: str,
) -> dict[str, Any]:
    manifest = _drop_none(dict(child))
    metadata = dict(manifest.get("metadata", {}))
    labels = dict(metadata.get("labels", {}))
    labels.update(
        {
            "app.kubernetes.io/name": "or-sim-mig-planner",
            "mig.or-sim.io/owner-action-plan": action_plan_name,
            "mig.or-sim.io/preview-only": "true",
        }
    )
    metadata["namespace"] = namespace
    metadata["labels"] = labels
    manifest["metadata"] = metadata
    return manifest


def _drop_none(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _drop_none(child) for key, child in value.items() if child is not None}
    if isinstance(value, list):
        return [_drop_none(child) for child in value]
    if isinstance(value, tuple):
        return [_drop_none(child) for child in value]
    return value


def _observed_state_preview_summary(preview: dict[str, Any]) -> dict[str, Any]:
    observed = dict(preview.get("observedState", {}))
    return {
        "previewOnly": bool(preview.get("previewOnly", False)),
        "migLayoutCount": len(list(observed.get("migLayouts", []))),
        "podReadinessCount": len(list(observed.get("podReadiness", []))),
        "podAssignmentCount": len(list(observed.get("podAssignments", []))),
        "routerStateCount": len(list(observed.get("routerState", []))),
        "inflightByInstanceCount": len(list(observed.get("inflightByInstance", []))),
        "queuedByWorkloadCount": len(list(observed.get("queuedByWorkload", []))),
        "missingRealClusterInputs": list(preview.get("missingRealClusterInputs", [])),
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
