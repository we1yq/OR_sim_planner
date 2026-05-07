from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any

import yaml

from k8s_adapter import cluster_state_from_dict, plan_scenario_as_migplan_status
from k8s_api import KubernetesClient, PythonKubernetesClient
from scenario_loader import load_planning_scenario


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

    scenario_path = _resolve_scenario_ref(spec.get("scenario"), scenario_root)
    scenario = load_planning_scenario(scenario_path)
    source_state = None
    if spec.get("sourceStateConfigMap"):
        source_state = load_cluster_state_from_configmap(
            name=str(spec["sourceStateConfigMap"]),
            namespace=namespace,
            client=client,
        )
    planned = plan_scenario_as_migplan_status(
        scenario=scenario,
        source_state_override=source_state,
        max_iters=max_iters,
        milp_time_limit_s=milp_time_limit_s,
        verbose=verbose,
    )

    status = dict(planned["status"])
    status["observedGeneration"] = int(migplan.get("metadata", {}).get("generation", 0))
    status["migPlanName"] = name
    output_configmap = _output_state_configmap_name(spec=spec, planned=planned)
    if output_configmap:
        status["canonicalNextStateConfigMap"] = output_configmap

    if patch_status:
        if output_configmap:
            upsert_cluster_state_configmap(
                name=output_configmap,
                namespace=namespace,
                state=status["canonicalNextState"],
                owner_migplan=name,
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
    if str(spec.get("planner", "v3")) != "v3":
        raise ValueError("Only planner=v3 is supported")
    if not spec.get("scenario"):
        raise ValueError("MigPlan spec.scenario is required")


def _output_state_configmap_name(spec: dict[str, Any], planned: dict[str, Any]) -> str | None:
    if spec.get("outputStateConfigMap"):
        return str(spec["outputStateConfigMap"])
    target_ref = planned.get("spec", {}).get("targetStateRef")
    if not target_ref:
        return None
    raw = str(target_ref).replace("_", "-")
    return f"{raw}-state"


def _resolve_scenario_ref(value: Any, scenario_root: str | Path | None) -> Path:
    raw = str(value)
    path = Path(raw)
    if path.is_absolute() or path.exists():
        return path

    root = Path(scenario_root) if scenario_root is not None else Path(__file__).resolve().parents[1] / "mock/scenarios"
    if path.suffix:
        return root / path
    return root / f"{raw}.yaml"
