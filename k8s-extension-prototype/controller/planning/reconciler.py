from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any

import yaml

from planning.executor_preview import (
    build_abstract_action_preview,
    build_adapter_dry_run_preview,
    build_gpu_operator_executor_preview,
    build_mig_geometry_preview,
    build_observer_preview,
    build_pod_lifecycle_preview,
    build_traffic_and_drain_preview,
)
from feasible_options import profile_catalog_from_yaml
from planning.k8s_adapter import cluster_state_from_dict, plan_scenario_as_migplan_status
from api.k8s_api import KubernetesClient, PythonKubernetesClient
from models import PlanningScenario, ScenarioWorkloadDemand
from migrant_core.transition_planners import canonical_planner_name
from observe.observed_state_adapter import cluster_state_from_observed_cluster_state
from scenario_loader import load_planning_scenario, planning_scenario_from_yaml


def run_controller_loop(
    namespace: str = "or-sim",
    scenario_root: str | Path | None = None,
    max_iters: int = 20,
    milp_time_limit_s: float | None = None,
    verbose: bool = False,
    poll_interval_s: float = 10.0,
    max_cycles: int | None = None,
    client: KubernetesClient | None = None,
) -> dict[str, Any]:
    client = client or PythonKubernetesClient()
    cycle = 0
    last_summary: dict[str, Any] = {}
    while max_cycles is None or cycle < max_cycles:
        cycle += 1
        migplans = client.list_migplans(namespace=namespace)
        summary = {
            "cycle": cycle,
            "namespace": namespace,
            "seen": len(migplans),
            "reconciled": [],
            "skipped": [],
            "errors": [],
        }

        for migplan in migplans:
            metadata = dict(migplan.get("metadata", {}))
            name = str(metadata.get("name", ""))
            if not name:
                continue
            if not should_reconcile_migplan(migplan):
                summary["skipped"].append(name)
                continue
            try:
                result = reconcile_migplan_once(
                    name=name,
                    namespace=namespace,
                    scenario_root=scenario_root,
                    max_iters=max_iters,
                    milp_time_limit_s=milp_time_limit_s,
                    verbose=verbose,
                    patch_status=True,
                    client=client,
                )
                summary["reconciled"].append(
                    {
                        "name": name,
                        "phase": result["status"].get("phase"),
                        "observedGeneration": result["status"].get("observedGeneration"),
                        "canonicalNextStateConfigMap": result["status"].get("canonicalNextStateConfigMap"),
                    }
                )
            except Exception as exc:
                generation = int(metadata.get("generation", 0))
                status = {
                    "phase": "Error",
                    "reachedTarget": False,
                    "observedGeneration": generation,
                    "migPlanName": name,
                    "message": str(exc),
                    "actions": [],
                }
                try:
                    client.patch_migplan_status(name=name, namespace=namespace, status=status)
                except Exception as patch_exc:
                    status["patchError"] = str(patch_exc)
                summary["errors"].append({"name": name, "message": str(exc)})

        last_summary = summary
        _log_controller_summary(
            {
                "cycle": summary["cycle"],
                "seen": summary["seen"],
                "reconciled": summary["reconciled"],
                "skipped": summary["skipped"],
                "errors": summary["errors"],
            }
        )
        if max_cycles is not None and cycle >= max_cycles:
            break
        time.sleep(float(poll_interval_s))

    return {
        "kind": "MigPlanControllerRunSummary",
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "lastCycle": last_summary,
    }


def run_watch_controller_loop(
    namespace: str = "or-sim",
    scenario_root: str | Path | None = None,
    max_iters: int = 20,
    milp_time_limit_s: float | None = None,
    verbose: bool = False,
    watch_timeout_s: int = 60,
    max_events: int | None = None,
    client: KubernetesClient | None = None,
) -> dict[str, Any]:
    client = client or PythonKubernetesClient()
    summary = {
        "namespace": namespace,
        "seenEvents": 0,
        "reconciled": [],
        "skipped": [],
        "deleted": [],
        "errors": [],
        "watchRestarts": 0,
    }

    for migplan in client.list_migplans(namespace=namespace):
        _handle_migplan_object(
            migplan=migplan,
            event_type="INITIAL",
            namespace=namespace,
            scenario_root=scenario_root,
            max_iters=max_iters,
            milp_time_limit_s=milp_time_limit_s,
            verbose=verbose,
            client=client,
            summary=summary,
        )
        if max_events is not None and _processed_event_count(summary) >= max_events:
            return _watch_summary(summary)

    while max_events is None or _processed_event_count(summary) < max_events:
        summary["watchRestarts"] += 1
        try:
            for event in client.watch_migplans(namespace=namespace, timeout_seconds=watch_timeout_s):
                summary["seenEvents"] += 1
                event_type = str(event.get("type", "UNKNOWN"))
                obj = event.get("object")
                if not isinstance(obj, dict):
                    continue
                _handle_migplan_object(
                    migplan=obj,
                    event_type=event_type,
                    namespace=namespace,
                    scenario_root=scenario_root,
                    max_iters=max_iters,
                    milp_time_limit_s=milp_time_limit_s,
                    verbose=verbose,
                    client=client,
                    summary=summary,
                )
                if max_events is not None and _processed_event_count(summary) >= max_events:
                    return _watch_summary(summary)
        except Exception as exc:
            summary["errors"].append({"event": "WATCH", "message": str(exc)})
            _log_controller_summary({"watchError": str(exc), "watchRestarts": summary["watchRestarts"]})
            time.sleep(1.0)

    return _watch_summary(summary)


def reconcile_migplan_once(
    name: str,
    namespace: str = "or-sim",
    scenario_root: str | Path | None = None,
    max_iters: int = 20,
    milp_time_limit_s: float | None = None,
    verbose: bool = False,
    patch_status: bool = True,
    client: KubernetesClient | None = None,
) -> dict[str, Any]:
    client = client or PythonKubernetesClient()
    migplan = client.get_migplan(name=name, namespace=namespace)
    spec = dict(migplan.get("spec", {}))
    _validate_supported_spec(spec)

    scenario = load_scenario_for_migplan_spec(
        spec=spec,
        namespace=namespace,
        scenario_root=scenario_root,
        client=client,
    )
    scenario.transition["transitionPlanner"] = str(spec.get("planner", "phase_greedy"))
    source_state = None
    if spec.get("sourceStateConfigMap"):
        source_state = load_cluster_state_from_configmap(
            name=str(spec["sourceStateConfigMap"]),
            namespace=namespace,
            client=client,
        )
    elif spec.get("observedStateRef") or spec.get("sourceStateRef") == "observed":
        source_state = load_cluster_state_from_observedclusterstate(
            name=str(spec.get("observedStateRef") or "cluster-observed-state"),
            namespace=namespace,
            client=client,
        )
    profile_catalogs_by_workload = load_profile_catalogs_for_scenario(
        scenario=scenario,
        namespace=namespace,
        client=client,
    )
    planned = plan_scenario_as_migplan_status(
        scenario=scenario,
        source_state_override=source_state,
        profile_catalogs_by_workload=profile_catalogs_by_workload,
        max_iters=max_iters,
        milp_time_limit_s=milp_time_limit_s,
        verbose=verbose,
    )

    full_status = dict(planned["status"])
    full_status["observedGeneration"] = int(migplan.get("metadata", {}).get("generation", 0))
    full_status["migPlanName"] = name
    output_configmap = _output_state_configmap_name(spec=spec, planned=planned)
    if output_configmap:
        full_status["canonicalNextStateConfigMap"] = output_configmap
    full_plan_configmap = _full_plan_configmap_name(spec=spec, migplan_name=name)
    full_status["fullPlanConfigMap"] = full_plan_configmap
    action_plan_name = _action_plan_name(spec=spec, migplan_name=name)
    full_status["actionPlanRef"] = action_plan_name
    status = compact_migplan_status_for_k8s(full_status)

    if patch_status:
        if output_configmap:
            upsert_cluster_state_configmap(
                name=output_configmap,
                namespace=namespace,
                state=full_status["canonicalNextState"],
                owner_migplan=name,
                client=client,
            )
        upsert_full_plan_configmap(
            name=full_plan_configmap,
            namespace=namespace,
            status=full_status,
            owner_migplan=name,
            client=client,
        )
        upsert_migactionplan(
            name=action_plan_name,
            namespace=namespace,
            owner_migplan=name,
            migplan_generation=int(migplan.get("metadata", {}).get("generation", 0)),
            auto_approval_policy_name=str(spec.get("autoApprovalPolicy", "default")),
            status=full_status,
            client=client,
        )
        client.patch_migplan_status(name=name, namespace=namespace, status=status)

    return {
        "apiVersion": planned["apiVersion"],
        "kind": planned["kind"],
        "metadata": {"name": name, "namespace": namespace},
        "spec": spec,
        "status": status,
    }


def _handle_migplan_object(
    migplan: dict[str, Any],
    event_type: str,
    namespace: str,
    scenario_root: str | Path | None,
    max_iters: int,
    milp_time_limit_s: float | None,
    verbose: bool,
    client: KubernetesClient,
    summary: dict[str, Any],
) -> None:
    metadata = dict(migplan.get("metadata", {}))
    name = str(metadata.get("name", ""))
    if not name:
        return
    if event_type == "DELETED":
        summary["deleted"].append(name)
        _log_controller_summary({"event": event_type, "name": name, "deleted": True})
        return
    if not should_reconcile_migplan(migplan):
        summary["skipped"].append(name)
        _log_controller_summary({"event": event_type, "name": name, "skipped": True})
        return
    try:
        result = reconcile_migplan_once(
            name=name,
            namespace=namespace,
            scenario_root=scenario_root,
            max_iters=max_iters,
            milp_time_limit_s=milp_time_limit_s,
            verbose=verbose,
            patch_status=True,
            client=client,
        )
        row = {
            "event": event_type,
            "name": name,
            "phase": result["status"].get("phase"),
            "observedGeneration": result["status"].get("observedGeneration"),
            "canonicalNextStateConfigMap": result["status"].get("canonicalNextStateConfigMap"),
        }
        summary["reconciled"].append(row)
        _log_controller_summary(row)
    except Exception as exc:
        generation = int(metadata.get("generation", 0))
        status = {
            "phase": "Error",
            "reachedTarget": False,
            "observedGeneration": generation,
            "migPlanName": name,
            "message": str(exc),
            "actions": [],
        }
        try:
            client.patch_migplan_status(name=name, namespace=namespace, status=status)
        except Exception as patch_exc:
            status["patchError"] = str(patch_exc)
        row = {"event": event_type, "name": name, "message": str(exc)}
        summary["errors"].append(row)
        _log_controller_summary(row)


def _processed_event_count(summary: dict[str, Any]) -> int:
    return (
        len(summary.get("reconciled", []))
        + len(summary.get("skipped", []))
        + len(summary.get("deleted", []))
        + len(summary.get("errors", []))
    )


def _watch_summary(summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "kind": "MigPlanWatchControllerRunSummary",
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "summary": summary,
    }


def _log_controller_summary(obj: dict[str, Any]) -> None:
    print(yaml.safe_dump(obj, sort_keys=False).strip(), file=sys.stderr, flush=True)


def load_cluster_state_from_configmap(
    name: str,
    namespace: str,
    client: KubernetesClient | None = None,
) -> Any:
    client = client or PythonKubernetesClient()
    configmap = client.get_configmap(name=name, namespace=namespace)
    data = dict(configmap.get("data", {}))
    raw = data.get("state.yaml")
    if raw is None:
        raise ValueError(f"ConfigMap {namespace}/{name} does not contain data.state.yaml")
    obj = yaml.safe_load(raw)
    if not isinstance(obj, dict):
        raise ValueError(f"ConfigMap {namespace}/{name} data.state.yaml is not a YAML object")
    return cluster_state_from_dict(obj)


def load_cluster_state_from_observedclusterstate(
    name: str,
    namespace: str,
    client: KubernetesClient | None = None,
) -> Any:
    client = client or PythonKubernetesClient()
    observed = client.get_observedclusterstate(name=name, namespace=namespace)
    return cluster_state_from_observed_cluster_state(observed)


def load_scenario_for_migplan_spec(
    spec: dict[str, Any],
    namespace: str,
    scenario_root: str | Path | None = None,
    client: KubernetesClient | None = None,
) -> Any:
    if spec.get("scenarioConfigMap"):
        client = client or PythonKubernetesClient()
        name = str(spec["scenarioConfigMap"])
        configmap = client.get_configmap(name=name, namespace=namespace)
        data = dict(configmap.get("data", {}))
        raw = data.get("scenario.yaml")
        if raw is None:
            raise ValueError(f"ConfigMap {namespace}/{name} does not contain data.scenario.yaml")
        obj = yaml.safe_load(raw)
        if not isinstance(obj, dict):
            raise ValueError(f"ConfigMap {namespace}/{name} data.scenario.yaml is not a YAML object")
        scenario = planning_scenario_from_yaml(obj, base_dir=_scenario_base_dir(scenario_root))
    else:
        scenario_path = _resolve_scenario_ref(spec.get("scenario"), scenario_root)
        scenario = load_planning_scenario(scenario_path)

    if spec.get("arrivalSnapshotRef"):
        client = client or PythonKubernetesClient()
        snapshot = load_arrival_snapshot_from_crd(
            name=str(spec["arrivalSnapshotRef"]),
            namespace=namespace,
            client=client,
        )
        scenario = apply_arrival_snapshot_to_scenario(scenario=scenario, snapshot=snapshot)
    elif spec.get("arrivalSnapshotConfigMap"):
        client = client or PythonKubernetesClient()
        snapshot = load_arrival_snapshot_from_configmap(
            name=str(spec["arrivalSnapshotConfigMap"]),
            namespace=namespace,
            client=client,
        )
        scenario = apply_arrival_snapshot_to_scenario(scenario=scenario, snapshot=snapshot)
    return scenario


def load_arrival_snapshot_from_crd(
    name: str,
    namespace: str,
    client: KubernetesClient | None = None,
) -> dict[str, Any]:
    client = client or PythonKubernetesClient()
    obj = client.get_arrivalsnapshot(name=name, namespace=namespace)
    spec = dict(obj.get("spec", {}))
    metadata = dict(obj.get("metadata", {}))
    return {
        "name": metadata.get("name", name),
        "namespace": metadata.get("namespace", namespace),
        "apiVersion": obj.get("apiVersion"),
        "kind": obj.get("kind"),
        "source": spec.get("source", "external"),
        "mode": spec.get("mode"),
        "epoch": spec.get("epoch"),
        "windowSeconds": spec.get("windowSeconds"),
        "unit": spec.get("unit", "requests_per_second"),
        "observedAt": spec.get("observedAt"),
        "targetStateRef": spec.get("targetStateRef"),
        "targetArrival": dict(spec.get("targetArrival", {})),
        "requestCount": dict(spec.get("requestCount", {})),
    }


def load_arrival_snapshot_from_configmap(
    name: str,
    namespace: str,
    client: KubernetesClient | None = None,
) -> dict[str, Any]:
    client = client or PythonKubernetesClient()
    configmap = client.get_configmap(name=name, namespace=namespace)
    data = dict(configmap.get("data", {}))
    raw = data.get("arrival-snapshot.yaml")
    if raw is None:
        raise ValueError(f"ConfigMap {namespace}/{name} does not contain data.arrival-snapshot.yaml")
    obj = yaml.safe_load(raw)
    if not isinstance(obj, dict):
        raise ValueError(f"ConfigMap {namespace}/{name} data.arrival-snapshot.yaml is not a YAML object")
    return obj


def apply_arrival_snapshot_to_scenario(
    scenario: PlanningScenario,
    snapshot: dict[str, Any],
) -> PlanningScenario:
    target_arrival = dict(snapshot.get("targetArrival", {}))
    if not target_arrival:
        raise ValueError("ArrivalSnapshot targetArrival is required")

    workload_names = [workload.name for workload in scenario.workloads]
    missing = sorted(set(workload_names) - set(target_arrival))
    extra = sorted(set(target_arrival) - set(workload_names))
    if missing or extra:
        raise ValueError(
            "ArrivalSnapshot targetArrival keys must match scenario workloads; "
            f"missing={missing}, extra={extra}"
        )

    transition = dict(scenario.transition)
    transition["arrivalSnapshot"] = {
        "name": str(snapshot.get("name", "arrival-snapshot")),
        "epoch": snapshot.get("epoch"),
        "source": snapshot.get("source", "external"),
        "previewOnly": bool(snapshot.get("previewOnly", True)),
    }
    return PlanningScenario(
        name=scenario.name,
        description=scenario.description,
        policy_ref=scenario.policy_ref,
        mig_rules_ref=scenario.mig_rules_ref,
        source_state_ref=scenario.source_state_ref,
        target_state_ref=str(snapshot.get("targetStateRef", scenario.target_state_ref)),
        workloads=[
            ScenarioWorkloadDemand(
                name=workload.name,
                source_arrival=workload.source_arrival,
                target_arrival=float(target_arrival[workload.name]),
                workload_ref=workload.workload_ref,
                profile_catalog_ref=workload.profile_catalog_ref,
                profile_catalog_configmap=workload.profile_catalog_configmap,
            )
            for workload in scenario.workloads
        ],
        transition=transition,
    )


def load_profile_catalogs_for_scenario(
    scenario: Any,
    namespace: str,
    client: KubernetesClient | None = None,
) -> dict[str, Any] | None:
    configmap_refs = {
        workload.name: workload.profile_catalog_configmap
        for workload in scenario.workloads
        if workload.profile_catalog_configmap
    }
    if not configmap_refs:
        return None

    client = client or PythonKubernetesClient()
    catalogs = {}
    for workload_name, configmap_name in configmap_refs.items():
        configmap = client.get_configmap(name=str(configmap_name), namespace=namespace)
        data = dict(configmap.get("data", {}))
        raw = data.get("catalog.yaml")
        if raw is None:
            raise ValueError(
                f"ConfigMap {namespace}/{configmap_name} does not contain data.catalog.yaml"
            )
        obj = yaml.safe_load(raw)
        if not isinstance(obj, dict):
            raise ValueError(
                f"ConfigMap {namespace}/{configmap_name} data.catalog.yaml is not a YAML object"
            )
        catalogs[workload_name] = profile_catalog_from_yaml(obj)
    return catalogs


def upsert_cluster_state_configmap(
    name: str,
    namespace: str,
    state: dict[str, Any],
    owner_migplan: str,
    client: KubernetesClient | None = None,
) -> None:
    client = client or PythonKubernetesClient()
    manifest = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "or-sim-mig-planner",
                "mig.or-sim.io/state-kind": "canonical-next-state",
                "mig.or-sim.io/owner-migplan": owner_migplan,
            },
        },
        "data": {
            "state.yaml": yaml.safe_dump(state, sort_keys=False),
        },
    }
    client.apply_configmap(manifest)


def upsert_full_plan_configmap(
    name: str,
    namespace: str,
    status: dict[str, Any],
    owner_migplan: str,
    client: KubernetesClient | None = None,
) -> None:
    client = client or PythonKubernetesClient()
    manifest = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "or-sim-mig-planner",
                "mig.or-sim.io/state-kind": "full-plan-debug",
                "mig.or-sim.io/owner-migplan": owner_migplan,
            },
        },
        "data": {
            "status.yaml": yaml.safe_dump(status, sort_keys=False),
        },
    }
    client.apply_configmap(manifest)


def upsert_migactionplan(
    name: str,
    namespace: str,
    owner_migplan: str,
    migplan_generation: int,
    auto_approval_policy_name: str,
    status: dict[str, Any],
    client: KubernetesClient | None = None,
) -> None:
    client = client or PythonKubernetesClient()
    metrics = dict(status.get("metrics", {}))
    summary = dict(status.get("planningSummary") or _planning_summary(status.get("planningTrace", {})))
    executor_preview = build_gpu_operator_executor_preview(status)
    mig_geometry_preview = build_mig_geometry_preview(status)
    traffic_and_drain_preview = build_traffic_and_drain_preview(status)
    pod_lifecycle_preview = build_pod_lifecycle_preview(status)
    abstract_action_preview = build_abstract_action_preview(status)
    adapter_dry_run_preview = build_adapter_dry_run_preview(status)
    observer_preview = build_observer_preview(status)
    manifest = {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "MigActionPlan",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "or-sim-mig-planner",
                "mig.or-sim.io/owner-migplan": owner_migplan,
            },
        },
        "spec": {
            "migPlanRef": owner_migplan,
            "migPlanGeneration": int(migplan_generation),
            "dryRun": True,
            "executor": "nvidia-gpu-operator",
            "phaseGate": "PendingApproval",
            "autoApprovalPolicyRef": auto_approval_policy_name,
            "fullPlanConfigMap": status.get("fullPlanConfigMap"),
            "canonicalNextStateConfigMap": status.get("canonicalNextStateConfigMap"),
            "actionCount": int(metrics.get("actionCount", 0)),
            "actionCountsByType": summary.get("actionCountsByType", {}),
            "chosenTemplates": summary.get("chosenTemplates", []),
            "targetGpuCount": int(metrics.get("gpuCount", 0)),
            "executorPreview": executor_preview,
            "migGeometryPreview": mig_geometry_preview,
            "trafficAndDrainPreview": traffic_and_drain_preview,
            "podLifecyclePreview": pod_lifecycle_preview,
            "abstractActionPreview": abstract_action_preview,
            "adapterDryRunPreview": adapter_dry_run_preview,
            "observerPreview": observer_preview,
            "notes": [
                "Dry-run action plan only; no MIG, Pod, or scheduler changes are executed.",
                "Future actuator should translate this plan through NVIDIA GPU Operator MIG Manager before considering direct host commands.",
            ],
        },
    }
    client.apply_migactionplan(manifest)
    policy = client.get_autoapprovalpolicy(name=auto_approval_policy_name, namespace=namespace)
    action_plan_status = evaluate_auto_approval_policy(policy=policy, action_plan=manifest)
    client.patch_migactionplan_status(name=name, namespace=namespace, status=action_plan_status)


def evaluate_auto_approval_policy(
    policy: dict[str, Any] | None,
    action_plan: dict[str, Any],
) -> dict[str, Any]:
    spec = dict(action_plan.get("spec", {}))
    if policy is None:
        return {
            "phase": "PendingApproval",
            "approved": False,
            "executed": False,
            "message": "No AutoApprovalPolicy found; waiting for approval.",
        }

    policy_spec = dict(policy.get("spec", {}))
    if not bool(policy_spec.get("enabled", False)):
        return {
            "phase": "PendingApproval",
            "approved": False,
            "executed": False,
            "policyRef": policy.get("metadata", {}).get("name"),
            "message": "AutoApprovalPolicy is disabled; waiting for approval.",
        }

    reasons = []
    if bool(policy_spec.get("dryRunOnly", True)) and not bool(spec.get("dryRun", False)):
        reasons.append("dryRunOnly policy rejected a non-dry-run action plan")

    max_action_count = policy_spec.get("maxActionCount")
    if max_action_count is not None and int(spec.get("actionCount", 0)) > int(max_action_count):
        reasons.append(
            f"actionCount {spec.get('actionCount', 0)} exceeds maxActionCount {max_action_count}"
        )

    allowed_executors = list(policy_spec.get("allowedExecutors", []))
    if allowed_executors and spec.get("executor") not in allowed_executors:
        reasons.append(f"executor {spec.get('executor')} is not allowed")

    if bool(policy_spec.get("requireFullPlanConfigMap", True)) and not spec.get("fullPlanConfigMap"):
        reasons.append("fullPlanConfigMap is required")

    if reasons:
        return {
            "phase": "ApprovalBlocked",
            "approved": False,
            "executed": False,
            "policyRef": policy.get("metadata", {}).get("name"),
            "reasons": reasons,
            "message": "AutoApprovalPolicy rejected the dry-run action plan.",
        }

    return {
        "phase": "ApprovedDryRun",
        "approved": True,
        "executed": False,
        "policyRef": policy.get("metadata", {}).get("name"),
        "message": "AutoApprovalPolicy approved this dry-run action plan. No hardware actions executed.",
    }


def compact_migplan_status_for_k8s(status: dict[str, Any]) -> dict[str, Any]:
    compact = {
        "phase": status.get("phase"),
        "reachedTarget": status.get("reachedTarget"),
        "message": status.get("message"),
        "metrics": dict(status.get("metrics", {})),
        "planningSummary": _planning_summary(status.get("planningTrace", {})),
        "currentStateFeasibility": status.get("currentStateFeasibility"),
        "observedGeneration": status.get("observedGeneration"),
        "migPlanName": status.get("migPlanName"),
        "canonicalNextStateConfigMap": status.get("canonicalNextStateConfigMap"),
        "fullPlanConfigMap": status.get("fullPlanConfigMap"),
        "actionPlanRef": status.get("actionPlanRef"),
    }
    milp = dict(status.get("milp", {}))
    compact["milp"] = {
        "status": milp.get("status"),
        "gpuCount": milp.get("gpuCount"),
        "chosenTemplates": milp.get("chosenTemplates", []),
        "KTotal": milp.get("KTotal", {}),
        "alloc": None,
    }
    compact.update(
        {
            "actions": None,
            "planningTrace": None,
            "targetState": None,
            "executedState": None,
            "canonicalNextState": None,
        }
    )
    return compact


def _planning_summary(trace: dict[str, Any]) -> dict[str, Any]:
    current_state = dict(trace.get("currentStateFeasibility", {}))
    feasible = dict(trace.get("feasibleOptions", {}))
    milp = dict(trace.get("milp", {}))
    target_build = dict(trace.get("targetBuild", {}))
    transition = dict(trace.get("transition", {}))
    canonicalization = dict(trace.get("canonicalization", {}))
    return {
        "pipeline": [
            {
                "stage": "current-state-feasibility",
                "phase": current_state.get("phase"),
                "recommendedAction": current_state.get("recommendedAction"),
                "feasible": current_state.get("feasible"),
            },
            {
                "stage": "feasible-options",
                "elapsedSec": feasible.get("elapsedSec"),
                "optionCount": feasible.get("optionCount"),
            },
            {
                "stage": "milp",
                "elapsedSec": milp.get("elapsedSec"),
                "status": milp.get("status"),
                "gpuCount": milp.get("gpuCount"),
                "totalInstances": milp.get("totalInstances"),
                "totalSlack": milp.get("totalSlack"),
            },
            {
                "stage": "target-build",
                "elapsedSec": target_build.get("elapsedSec"),
                "targetGpuCount": target_build.get("targetGpuCount"),
                "method": target_build.get("method"),
            },
            {
                "stage": "transition-planning",
                "elapsedSec": transition.get("elapsedSec"),
                "iterationCount": transition.get("iterationCount"),
                "actionCount": transition.get("actionCount"),
                "reachedTarget": transition.get("reachedTarget"),
            },
            {
                "stage": "canonicalization",
                "canonicalGpuCount": canonicalization.get("canonicalGpuCount"),
                "canonicalPhysicalIds": canonicalization.get("canonicalPhysicalIds"),
            },
        ],
        "actionCountsByType": transition.get("actionCountsByType", {}),
        "chosenTemplates": milp.get("chosenTemplates", []),
        "KTotal": milp.get("KTotal", {}),
        "fullTrace": "See status.fullPlanConfigMap data.status.yaml",
    }


def should_reconcile_migplan(migplan: dict[str, Any]) -> bool:
    metadata = dict(migplan.get("metadata", {}))
    status = dict(migplan.get("status", {}))
    generation = int(metadata.get("generation", 0))
    observed = int(status.get("observedGeneration", 0))
    if generation != observed:
        return True
    if not status.get("phase"):
        return True
    if status.get("phase") == "Error":
        return True
    return False


def _validate_supported_spec(spec: dict[str, Any]) -> None:
    if bool(spec.get("dryRun", True)) is not True:
        raise ValueError("Only dryRun MigPlan reconciliation is supported")
    try:
        canonical_planner_name(str(spec.get("planner", "phase_greedy")))
    except ValueError as exc:
        raise ValueError(
            "Unsupported planner. See migrant_core.transition_planners.PLANNER_CATALOG "
            "for supported transition planners and aliases."
        ) from exc
    if not spec.get("scenario") and not spec.get("scenarioConfigMap"):
        raise ValueError("MigPlan spec.scenario or spec.scenarioConfigMap is required")


def _output_state_configmap_name(spec: dict[str, Any], planned: dict[str, Any]) -> str | None:
    if spec.get("outputStateConfigMap"):
        return str(spec["outputStateConfigMap"])
    target_ref = planned.get("spec", {}).get("targetStateRef")
    if not target_ref:
        return None
    raw = str(target_ref).replace("_", "-")
    return f"{raw}-state"


def _full_plan_configmap_name(spec: dict[str, Any], migplan_name: str) -> str:
    if spec.get("fullPlanConfigMap"):
        return str(spec["fullPlanConfigMap"])
    return f"{migplan_name}-full-plan"


def _action_plan_name(spec: dict[str, Any], migplan_name: str) -> str:
    if spec.get("actionPlanName"):
        return str(spec["actionPlanName"])
    return f"{migplan_name}-action-plan"


def _resolve_scenario_ref(value: Any, scenario_root: str | Path | None) -> Path:
    raw = str(value)
    path = Path(raw)
    if path.is_absolute() or path.exists():
        return path

    root = _scenario_base_dir(scenario_root)
    if path.suffix:
        return root / path
    return root / f"{raw}.yaml"


def _scenario_base_dir(scenario_root: str | Path | None) -> Path:
    return Path(scenario_root) if scenario_root is not None else Path(__file__).resolve().parents[1] / "mock/scenarios"
