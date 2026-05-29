from __future__ import annotations

from datetime import datetime, timezone
import time
from typing import Any

import yaml

from api.k8s_api import KubernetesClient, PythonKubernetesClient
from actuators.mig_geometry.gpu_operator import apply_mig_labels_from_action_plan
from actuators.pod_lifecycle.kubernetes_pods import apply_pod_lifecycle_from_action_plan
from actuators.router_drain.runtime_router import apply_router_drain_from_action_plan
from cluster_state_manager.cluster_observer import observe_cluster_state_once
from cluster_state_manager.physical_gpu_registry import (
    mark_physical_gpu_active,
    mark_physical_gpu_pending,
    mark_physical_gpu_released,
    registry_queue_summary,
    sync_physical_gpu_registry,
)


MIG_ACTIONS = {
    "allocate_gpu",
    "configure_full_template",
    "configure_partial_profile",
    "place_target_layout",
    "clear_template",
}
ROUTER_ACTIONS = {"stop_gpu_traffic", "stop_accepting_new", "mark_draining_instance"}
POD_ACTIONS = {
    "place_instance",
    "bridge_place_instance",
    "deploy_target_workloads",
    "delete_pods",
    "delete_gpu_pods",
    "remove_instance",
    "delete_bridge_pod",
    "workload_change",
    "update_batch",
}
BOOKKEEPING_ACTIONS = {
    "clear_gpu",
    "clear_gpu_binding",
    "bind_target_gpu",
    "mark_reconfig_target_prepared",
    "return_gpu",
    "register_mig_devices",
    "observe_mig_devices",
    "activate_serving_route",
}
DEFER_ACTIONS = {"defer_remove_gpu", "defer_remove_instance", "defer_workload_change"}


class TransitionExecutorError(RuntimeError):
    pass


def run_transition_executor_loop(
    namespace: str = "or-sim",
    poll_interval_s: float = 10.0,
    max_cycles: int | None = None,
    confirm_real_execute: bool = False,
    allow_preview_instructions: bool = False,
    client: KubernetesClient | None = None,
) -> dict[str, Any]:
    client = client or PythonKubernetesClient()
    cycle = 0
    last_summary: dict[str, Any] = {}
    while max_cycles is None or cycle < max_cycles:
        cycle += 1
        summaries = []
        for action_plan in client.list_migactionplans(namespace=namespace):
            status = dict(action_plan.get("status", {}))
            if status.get("phase") not in {"ApprovedForRealExecution", "ExecutingRealPlan"}:
                continue
            summaries.append(
                step_transition_action_plan(
                    name=str(action_plan.get("metadata", {}).get("name")),
                    namespace=namespace,
                    confirm_real_execute=confirm_real_execute,
                    allow_preview_instructions=allow_preview_instructions,
                    client=client,
                )
            )
        last_summary = {
            "kind": "TransitionExecutorLoopSummary",
            "apiVersion": "mig.or-sim.io/v1alpha1",
            "cycle": cycle,
            "plans": summaries,
        }
        print(
            f"[transition-executor] cycle={cycle} plans={len(summaries)}",
            flush=True,
        )
        if max_cycles is not None and cycle >= max_cycles:
            break
        time.sleep(float(poll_interval_s))
    return last_summary


def step_transition_action_plan(
    name: str,
    namespace: str = "or-sim",
    confirm_real_execute: bool = False,
    allow_preview_instructions: bool = False,
    client: KubernetesClient | None = None,
) -> dict[str, Any]:
    client = client or PythonKubernetesClient()
    action_plan = client.get_migactionplan(name=name, namespace=namespace)
    spec = dict(action_plan.get("spec", {}))
    if bool(spec.get("dryRun", True)) and not allow_preview_instructions:
        raise TransitionExecutorError("Refusing to execute dryRun MigActionPlan without allow_preview_instructions=True.")
    if not confirm_real_execute:
        raise TransitionExecutorError("Refusing real transition execution without confirm_real_execute=True.")

    actions = _load_actions_from_action_plan(action_plan=action_plan, namespace=namespace, client=client)
    execution = _execution_state(action_plan=action_plan, actions=actions)
    ready = _ready_actions(actions=actions, execution=execution)
    dispatched = []
    if ready:
        action = ready[0]
        result = _dispatch_action(
            action_plan_name=name,
            namespace=namespace,
            action=action,
            action_plan=action_plan,
            allow_preview_instructions=allow_preview_instructions,
            client=client,
        )
        execution["actions"][_action_id(action)] = {
            **execution["actions"][_action_id(action)],
            "phase": "Completed" if result.get("success", True) else "Failed",
            "result": result,
            "completedAt": datetime.now(timezone.utc).isoformat(),
        }
        dispatched.append({"actionId": _action_id(action), "type": action.get("type"), "result": result})

    counts = _execution_counts(execution)
    phase = "SucceededRealPlan" if counts["completed"] == len(actions) and actions else "ExecutingRealPlan"
    if counts["failed"]:
        phase = "BlockedRealPlan"
    observed_ref = None
    if phase in {"SucceededRealPlan", "BlockedRealPlan"}:
        observed_ref = observe_postconditions_for_action_plan(
            name=name,
            namespace=namespace,
            client=client,
        )
    status = {
        **dict(action_plan.get("status", {})),
        "phase": phase,
        "executed": phase == "SucceededRealPlan",
        "observedGeneration": int(dict(action_plan.get("metadata", {})).get("generation", 0)),
        "transitionExecution": {
            **execution,
            "counts": counts,
            "lastSteppedAt": datetime.now(timezone.utc).isoformat(),
            "lastDispatched": dispatched,
            "observedPostconditionsRef": observed_ref,
        },
    }
    client.patch_migactionplan_status(name=name, namespace=namespace, status=status)
    return {
        "kind": "TransitionExecutorStepSummary",
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "actionPlan": name,
        "phase": phase,
        "counts": counts,
        "dispatched": dispatched,
        "observedPostconditionsRef": observed_ref,
    }


def observe_postconditions_for_action_plan(
    name: str,
    namespace: str = "or-sim",
    client: KubernetesClient | None = None,
) -> str:
    client = client or PythonKubernetesClient()
    observed_name = f"{name}-observed-state"
    observed = observe_cluster_state_once(
        namespace=namespace,
        name=observed_name,
        apply=True,
        client=client,
    )
    status = dict(observed.get("status", {}))
    if not status:
        status = {
            "phase": "Observed",
            "previewOnly": bool(dict(observed.get("spec", {})).get("previewOnly", False)),
            "readyForCanonicalization": False,
        }
    client.patch_observedclusterstate_status(
        name=observed_name,
        namespace=namespace,
        status=status,
    )
    return observed_name


def _dispatch_action(
    action_plan_name: str,
    namespace: str,
    action: dict[str, Any],
    action_plan: dict[str, Any],
    allow_preview_instructions: bool,
    client: KubernetesClient,
) -> dict[str, Any]:
    action_type = str(action.get("type") or "")
    if action_type in DEFER_ACTIONS:
        return {"success": False, "phase": "Blocked", "message": f"{action_type} is a deferred gate, not executable."}
    if action_type == "clear_gpu":
        mig_summary = apply_mig_labels_from_action_plan(
            name=action_plan_name,
            namespace=namespace,
            confirm_real_mig_apply=True,
            allow_preview_instructions=allow_preview_instructions,
            wait=True,
            client=client,
        )
        registry_summary = _apply_registry_bookkeeping(
            action=action,
            namespace=namespace,
            client=client,
        )
        return {
            "success": True,
            "phase": "Completed",
            "actuator": "mig-geometry+registry",
            "summary": mig_summary,
            "registry": registry_summary,
        }
    if action_type in MIG_ACTIONS:
        summary = apply_mig_labels_from_action_plan(
            name=action_plan_name,
            namespace=namespace,
            confirm_real_mig_apply=True,
            allow_preview_instructions=allow_preview_instructions,
            wait=True,
            client=client,
        )
        result = {"success": True, "phase": "Completed", "actuator": "mig-geometry", "summary": summary}
        if action_type in {"allocate_gpu", "configure_full_template", "configure_partial_profile", "place_target_layout", "clear_template"}:
            result["registry"] = _apply_registry_bookkeeping(
                action=action,
                namespace=namespace,
                client=client,
            )
        return result
    if action_type in ROUTER_ACTIONS:
        summary = apply_router_drain_from_action_plan(
            name=action_plan_name,
            namespace=namespace,
            confirm_real_router_apply=True,
            allow_preview_instructions=allow_preview_instructions,
            mode="annotation",
            client=client,
        )
        return {"success": bool(summary.get("success", False)), "phase": "Completed", "actuator": "router-drain", "summary": summary}
    if action_type in POD_ACTIONS:
        summary = apply_pod_lifecycle_from_action_plan(
            name=action_plan_name,
            namespace=namespace,
            confirm_real_pod_apply=True,
            allow_preview_instructions=allow_preview_instructions,
            client_=client,
        )
        return {"success": bool(summary.get("success", False)), "phase": "Completed", "actuator": "pod-lifecycle", "summary": summary}
    if action_type in BOOKKEEPING_ACTIONS:
        registry_summary = _apply_registry_bookkeeping(
            action=action,
            namespace=namespace,
            client=client,
        )
        return {
            "success": True,
            "phase": "Completed",
            "actuator": "registry-bookkeeping",
            "registry": registry_summary,
        }
    return {"success": False, "phase": "Failed", "message": f"Unsupported action type {action_type!r}."}


def _apply_registry_bookkeeping(
    action: dict[str, Any],
    namespace: str,
    client: KubernetesClient,
) -> dict[str, Any]:
    action_type = str(action.get("type") or "")
    physical_gpu_id = _physical_gpu_id(action)
    logical_gpu_id = _logical_gpu_id(action)
    if not physical_gpu_id:
        return {
            "phase": "Skipped",
            "message": f"{action_type} has no physical_gpu_id; registry was not changed.",
        }

    if action_type in {"bind_target_gpu", "mark_reconfig_target_prepared", "clear_gpu_binding", "clear_gpu", "clear_template"}:
        registry = mark_physical_gpu_pending(
            physical_gpu_id=physical_gpu_id,
            logical_gpu_id=logical_gpu_id,
            namespace=namespace,
            apply=True,
            client=client,
        )
        return {
            "phase": "MarkedPending",
            "physicalGpuId": physical_gpu_id,
            "logicalGpuId": logical_gpu_id,
            "queues": registry_queue_summary(registry),
        }
    if action_type == "return_gpu":
        if not _registry_binding_available_eligible(
            physical_gpu_id=physical_gpu_id,
            namespace=namespace,
            client=client,
        ):
            sync_physical_gpu_registry(
                namespace=namespace,
                apply=True,
                client=client,
            )
        registry = mark_physical_gpu_released(
            physical_gpu_id=physical_gpu_id,
            namespace=namespace,
            apply=True,
            client=client,
        )
        return {
            "phase": "Released",
            "physicalGpuId": physical_gpu_id,
            "queues": registry_queue_summary(registry),
        }
    if action_type == "activate_serving_route":
        registry = mark_physical_gpu_active(
            physical_gpu_id=physical_gpu_id,
            logical_gpu_id=logical_gpu_id,
            namespace=namespace,
            apply=True,
            client=client,
        )
        return {
            "phase": "MarkedActive",
            "physicalGpuId": physical_gpu_id,
            "logicalGpuId": logical_gpu_id,
            "queues": registry_queue_summary(registry),
        }
    if action_type in {"register_mig_devices", "observe_mig_devices"}:
        registry = sync_physical_gpu_registry(
            namespace=namespace,
            apply=True,
            client=client,
        )
        return {
            "phase": "RegistrySynced",
            "physicalGpuId": physical_gpu_id,
            "logicalGpuId": logical_gpu_id,
            "queues": registry_queue_summary(registry),
            "message": "MIG device registration was refreshed from observed cluster state.",
        }
    return {
        "phase": "Noop",
        "physicalGpuId": physical_gpu_id,
        "logicalGpuId": logical_gpu_id,
        "message": f"{action_type} has no registry mutation.",
    }


def _registry_binding_available_eligible(
    physical_gpu_id: str,
    namespace: str,
    client: KubernetesClient,
) -> bool:
    registry = client.get_physicalgpuregistry(name="default", namespace=namespace)
    binding = dict(dict((registry or {}).get("status", {})).get("bindings", {})).get(physical_gpu_id)
    return bool(dict(binding or {}).get("availableEligible", False))


def _load_actions_from_action_plan(
    action_plan: dict[str, Any],
    namespace: str,
    client: KubernetesClient,
) -> list[dict[str, Any]]:
    spec = dict(action_plan.get("spec", {}))
    full_plan_name = str(spec.get("fullPlanConfigMap") or "")
    if not full_plan_name:
        raise TransitionExecutorError("MigActionPlan.spec.fullPlanConfigMap is required.")
    configmap = client.get_configmap(full_plan_name, namespace)
    raw_status = dict(configmap.get("data", {})).get("status.yaml")
    if not raw_status:
        raise TransitionExecutorError(f"ConfigMap {namespace}/{full_plan_name} has no data.status.yaml.")
    parsed = yaml.safe_load(raw_status)
    if not isinstance(parsed, dict):
        raise TransitionExecutorError(f"ConfigMap {namespace}/{full_plan_name} status.yaml is not a YAML object.")
    return [dict(action) for action in list(parsed.get("actions", []))]


def _physical_gpu_id(action: dict[str, Any]) -> str:
    for field in ("physical_gpu_id", "target_physical_gpu_id", "new_physical_gpu_id", "source_physical_gpu_id"):
        value = action.get(field)
        if value is not None and str(value):
            return str(value)
    return ""


def _logical_gpu_id(action: dict[str, Any]) -> str | None:
    value = action.get("gpu_id")
    if value is None:
        return None
    return str(value)


def _execution_state(action_plan: dict[str, Any], actions: list[dict[str, Any]]) -> dict[str, Any]:
    existing = dict(dict(action_plan.get("status", {})).get("transitionExecution", {}))
    existing_actions = {
        str(action_id): dict(record)
        for action_id, record in dict(existing.get("actions", {})).items()
    }
    action_records = {}
    for action in actions:
        action_id = _action_id(action)
        action_records[action_id] = {
            "actionId": action_id,
            "type": action.get("type"),
            "phase": existing_actions.get(action_id, {}).get("phase", "Pending"),
            "dependsOn": _depends_on(action),
            **({"result": existing_actions[action_id]["result"]} if "result" in existing_actions.get(action_id, {}) else {}),
        }
    return {
        "version": "transition-executor/v1",
        "actions": action_records,
    }


def _ready_actions(actions: list[dict[str, Any]], execution: dict[str, Any]) -> list[dict[str, Any]]:
    records = dict(execution.get("actions", {}))
    ready = []
    for action in actions:
        action_id = _action_id(action)
        record = dict(records.get(action_id, {}))
        if record.get("phase") != "Pending":
            continue
        deps = _depends_on(action)
        if all(dict(records.get(dep, {})).get("phase") == "Completed" for dep in deps):
            ready.append(action)
    return ready


def _execution_counts(execution: dict[str, Any]) -> dict[str, int]:
    records = list(dict(execution.get("actions", {})).values())
    return {
        "pending": sum(1 for item in records if item.get("phase") == "Pending"),
        "completed": sum(1 for item in records if item.get("phase") == "Completed"),
        "failed": sum(1 for item in records if item.get("phase") == "Failed"),
        "running": sum(1 for item in records if item.get("phase") == "Running"),
    }


def _action_id(action: dict[str, Any]) -> str:
    if action.get("actionKey") is not None:
        return str(action["actionKey"])
    action_type = str(action.get("type") or "action")
    gpu_id = str(action.get("gpu_id", "gpu"))
    physical_id = str(action.get("physical_gpu_id", action.get("target_physical_gpu_id", "")))
    slot = "-".join(str(part) for part in list(action.get("slot") or []))
    return ":".join(part for part in [action_type, gpu_id, physical_id, slot] if part)


def _depends_on(action: dict[str, Any]) -> list[str]:
    return [str(item) for item in list(action.get("dependsOnActionKeys") or action.get("dependsOn") or [])]
