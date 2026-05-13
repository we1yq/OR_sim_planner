from __future__ import annotations

import hashlib
import re
from typing import Any


GPU_OPERATOR_MIG_CONFIG_LABEL = "nvidia.com/mig.config"


def build_gpu_operator_executor_preview(status: dict[str, Any]) -> dict[str, Any]:
    canonical_state = dict(status.get("canonicalNextState", {}))
    metadata = dict(canonical_state.get("metadata", {}))
    physical_id_map = {
        str(logical_gpu_id): str(physical_gpu_id)
        for logical_gpu_id, physical_gpu_id in dict(metadata.get("physical_id_map", {})).items()
    }
    physical_bindings = _physical_gpu_bindings(metadata)
    gpu_targets = []
    unresolved_physical_gpu_ids = []

    for raw_gpu in sorted(canonical_state.get("gpus", []), key=lambda gpu: int(gpu.get("gpuId", 0))):
        logical_gpu_id = str(raw_gpu.get("gpuId"))
        physical_gpu_id = physical_id_map.get(logical_gpu_id, logical_gpu_id)
        binding = dict(physical_bindings.get(physical_gpu_id, {}))
        node_name = binding.get("nodeName")
        device_index = binding.get("deviceIndex")
        target = {
            "logicalGpuId": int(raw_gpu.get("gpuId")),
            "physicalGpuId": physical_gpu_id,
            "pendingLogicalGpuId": None,
            "activeLogicalGpuId": int(raw_gpu.get("gpuId")),
            "bindingState": "active-target",
            "nodeName": node_name,
            "deviceIndex": device_index,
            "targetTemplate": _template_from_gpu(raw_gpu),
            "targetInstances": _target_instances(raw_gpu),
        }
        gpu_targets.append(target)
        if not node_name or device_index is None:
            unresolved_physical_gpu_ids.append(physical_gpu_id)

    node_targets: dict[str, list[dict[str, Any]]] = {}
    for target in gpu_targets:
        node_name = target.get("nodeName")
        if node_name and target.get("deviceIndex") is not None:
            node_targets.setdefault(str(node_name), []).append(target)

    mig_manager_target_configs = []
    would_patch_node_labels = {}
    for node_name, targets in sorted(node_targets.items()):
        config_name = _mig_config_name(node_name=node_name, targets=targets)
        mig_manager_target_configs.append(
            {
                "nodeName": node_name,
                "configName": config_name,
                "gpus": [
                    {
                        "deviceIndex": target["deviceIndex"],
                        "physicalGpuId": target["physicalGpuId"],
                        "targetTemplate": target["targetTemplate"],
                        "targetInstances": target["targetInstances"],
                    }
                    for target in sorted(targets, key=lambda item: int(item.get("deviceIndex", 0)))
                ],
            }
        )
        would_patch_node_labels[node_name] = {
            GPU_OPERATOR_MIG_CONFIG_LABEL: config_name,
        }

    return {
        "previewOnly": True,
        "executor": "nvidia-gpu-operator",
        "targetApi": "NVIDIA GPU Operator MIG Manager",
        "gpuOperatorLabel": GPU_OPERATOR_MIG_CONFIG_LABEL,
        "gpuTargets": gpu_targets,
        "migManagerTargetConfigs": mig_manager_target_configs,
        "wouldPatchNodeLabels": would_patch_node_labels,
        "requiredObserverFields": [
            "physicalGpuId",
            "nodeName",
            "deviceIndex",
            "observedMigInstances",
        ],
        "unresolvedPhysicalGpuIds": sorted(set(unresolved_physical_gpu_ids)),
        "notes": [
            "Preview only; the actuator must not patch Nodes or run hardware commands in dry-run mode.",
            "A real actuator must observe node/device bindings before converting this preview into MIG Manager input.",
            "After real execution, canonicalize the observed post-action MIG state, not the planned state.",
        ],
    }


def build_mig_geometry_preview(status: dict[str, Any]) -> dict[str, Any]:
    executor_preview = build_gpu_operator_executor_preview(status)
    geometry_action_types = {
        "allocate_gpu",
        "configure_full_template",
        "clear_gpu",
        "clear_gpu_binding",
        "clear_template",
        "return_gpu",
    }
    return {
        "previewOnly": True,
        "adapter": "mig-geometry",
        "executor": "nvidia-gpu-operator",
        "targetApi": executor_preview["targetApi"],
        "gpuTargets": executor_preview["gpuTargets"],
        "migManagerTargetConfigs": executor_preview["migManagerTargetConfigs"],
        "wouldPatchNodeLabels": executor_preview["wouldPatchNodeLabels"],
        "unresolvedPhysicalGpuIds": executor_preview["unresolvedPhysicalGpuIds"],
        "geometryActions": [
            _action_brief(action)
            for action in _actions(status)
            if str(action.get("type")) in geometry_action_types
        ],
        "internalStateActionsExcluded": [
            _action_brief(action)
            for action in _actions(status)
            if str(action.get("type")) in {"bind_target_gpu"}
        ],
        "notes": [
            "Only MIG geometry actions become GPU Operator/MIG Manager inputs.",
            "Logical GPU binding actions are internal planner/controller state and are not sent to MIG Manager.",
        ],
    }


def build_traffic_and_drain_preview(status: dict[str, Any]) -> dict[str, Any]:
    traffic_types = {
        "stop_gpu_traffic",
        "stop_accepting_new",
        "accept_queued_requests",
        "reroute_queued_tasks",
        "mark_draining_instance",
        "defer_remove_gpu",
        "defer_remove_instance",
        "defer_workload_change",
    }
    plan_items = _final_plan_items(status)
    return {
        "previewOnly": True,
        "adapter": "router-drain",
        "rules": [
            "Stop accepting new work before removing or replacing an active slot.",
            "Reroute queued requests to a stable serving slot when one exists.",
            "Wait for queued requests and running work to drain to zero before deleting pods or clearing MIG geometry.",
            "Defer the abstract action when reroute capacity or drain completion is missing.",
        ],
        "planItems": [
            {
                "id": item.get("id"),
                "type": item.get("type"),
                "currentPhase": item.get("current_phase"),
                "status": item.get("status"),
                "blockedBy": item.get("blocked_by"),
                "gpuId": item.get("gpu_id"),
                "physicalGpuId": item.get("physical_gpu_id"),
                "targetPhysicalGpuId": item.get("target_physical_gpu_id"),
                "slot": item.get("slot"),
                "workload": item.get("workload"),
                "rerouteDestination": item.get("rerouteDestination") or item.get("reroute_destination") or item.get("takeover"),
                "queued": item.get("queued"),
                "runningWork": item.get("runningWork") or item.get("running_work") or item.get("inflight"),
                "drainRemaining": item.get("drain_remaining"),
                "capacitySafe": item.get("capacity_safe"),
            }
            for item in plan_items
        ],
        "trafficActions": [
            _action_brief(action)
            for action in _actions(status)
            if str(action.get("type")) in traffic_types
        ],
    }


def build_pod_lifecycle_preview(status: dict[str, Any]) -> dict[str, Any]:
    actions = _actions(status)
    return {
        "previewOnly": True,
        "adapter": "pod-lifecycle",
        "policy": {
            "preferWarmPool": True,
            "deleteAfterDrainOnly": True,
            "doNotCreatePodsForMigGeometryOnlyActions": True,
        },
        "createOrReuse": [
            _pod_lifecycle_row(action, reason="target serving capacity required")
            for action in actions
            if str(action.get("type")) in {"place_instance", "bridge_place_instance", "deploy_target_workloads"}
        ],
        "drain": [
            _pod_lifecycle_row(action, reason="old serving instance must stop accepting and drain")
            for action in actions
            if str(action.get("type"))
            in {"stop_gpu_traffic", "stop_accepting_new", "accept_queued_requests", "reroute_queued_tasks", "mark_draining_instance"}
        ],
        "deleteOrRecycle": [
            _pod_lifecycle_row(action, reason="safe only after queued requests reroute and running work reaches zero")
            for action in actions
            if str(action.get("type")) in {"delete_pods", "remove_instance", "delete_gpu_pods", "delete_bridge_pod", "clear_gpu", "clear_gpu_binding"}
        ],
        "reloadInPlace": [
            _pod_lifecycle_row(action, reason="same workload slot; prefer runtime config reload")
            for action in actions
            if str(action.get("type")) in {"update_batch", "patch_batch_config", "apply_batch", "verify_batch"}
        ],
        "notes": [
            "Pod lifecycle is a future adapter preview; the current dry-run actuator does not create or delete Pods.",
            "Router cutover and Pod readiness should be observed before old Pods are deleted or MIG geometry is cleared.",
        ],
    }


def build_abstract_action_preview(status: dict[str, Any]) -> dict[str, Any]:
    coarse_actions = _final_coarse_actions(status)
    plan_items = _final_plan_items(status)
    return {
        "previewOnly": True,
        "source": "v3-final-plan",
        "actions": [
            _abstract_action_row(action, plan_items)
            for action in coarse_actions
        ],
        "rules": {
            "keep_gpu": "Source and target MIG layout plus workload payload are unchanged.",
            "create_gpu": "Source has no logical GPU and target has one; prepare target-side MIG geometry.",
            "remove_gpu": "Target no longer needs the GPU; stop/reroute/drain active slots before clearing geometry.",
            "reconfiguration": "Source and target MIG templates differ; choose target_first unless old workloads have unchanged stable reroute slots.",
            "instance_diff": "MIG geometry is unchanged but slot workload or batch payload differs.",
        },
        "notes": [
            "This is a readable report of planner rules, not an execution command stream.",
            "Detailed fine actions remain in the full-plan ConfigMap.",
        ],
    }


def build_adapter_dry_run_preview(status: dict[str, Any]) -> dict[str, Any]:
    mig = build_mig_geometry_preview(status)
    traffic = build_traffic_and_drain_preview(status)
    pod = build_pod_lifecycle_preview(status)
    return {
        "previewOnly": True,
        "adapters": {
            "mig": {
                "implementation": "nvidia-gpu-operator",
                "wouldPatchNodeLabels": mig.get("wouldPatchNodeLabels", {}),
                "wouldApplyMigManagerConfigs": mig.get("migManagerTargetConfigs", []),
                "blockedUntilObservedBindings": mig.get("unresolvedPhysicalGpuIds", []),
            },
            "router": {
                "implementation": "dry-run-router-adapter",
                "wouldStopAcceptingNew": [
                    action for action in traffic.get("trafficActions", [])
                    if action.get("type") == "stop_accepting_new"
                ],
                "wouldRerouteQueuedTasks": [
                    action for action in traffic.get("trafficActions", [])
                    if action.get("type") == "reroute_queued_tasks"
                ],
                "wouldStartDrains": [
                    action for action in traffic.get("trafficActions", [])
                    if action.get("type") == "mark_draining_instance"
                ],
            },
            "pod": {
                "implementation": "dry-run-pod-lifecycle-adapter",
                "wouldCreateOrReuse": pod.get("createOrReuse", []),
                "wouldDrain": pod.get("drain", []),
                "wouldDeleteOrRecycle": pod.get("deleteOrRecycle", []),
                "wouldReloadInPlace": pod.get("reloadInPlace", []),
            },
        },
        "notes": [
            "Adapter skeleton only; no Router, Pod, Node, or MIG changes are executed.",
            "kind can validate this object shape and actuator gating, but not real GPU hardware effects.",
        ],
    }


def build_observer_preview(status: dict[str, Any]) -> dict[str, Any]:
    mig = build_mig_geometry_preview(status)
    traffic = build_traffic_and_drain_preview(status)
    pod = build_pod_lifecycle_preview(status)
    workloads = sorted(
        {
            str(row.get("workload"))
            for section in [pod.get("createOrReuse", []), pod.get("drain", []), pod.get("deleteOrRecycle", [])]
            for row in section
            if row.get("workload") is not None
        }
    )
    return {
        "previewOnly": True,
        "requiredObservations": {
            "mig": [
                "physicalGpuId",
                "nodeName",
                "deviceIndex",
                "observedMigInstances",
                "gpuOperatorMigConfigState",
            ],
            "router": [
                "acceptingNewByInstance",
                "queuedByWorkload",
                "rerouteTargets",
            ],
            "pod": [
                "podReadiness",
                "podToMigInstanceAssignment",
                "servingInstanceId",
                "inflightByInstance",
            ],
        },
        "targetsToObserve": {
            "physicalGpuIds": sorted(
                {
                    str(target.get("physicalGpuId"))
                    for target in mig.get("gpuTargets", [])
                    if target.get("physicalGpuId") is not None
                }
            ),
            "workloads": workloads,
            "planItemIds": [
                item.get("id")
                for item in traffic.get("planItems", [])
                if item.get("id") is not None
            ],
        },
        "canonicalizationRule": "After real execution, canonicalize only observed post-action GPU/MIG/Pod/router state.",
    }


def validate_executor_preview(preview: dict[str, Any] | None) -> list[str]:
    if not isinstance(preview, dict):
        return ["executorPreview is required"]
    reasons = []
    if not bool(preview.get("previewOnly", False)):
        reasons.append("executorPreview.previewOnly must be true for the dry-run actuator")
    if preview.get("executor") != "nvidia-gpu-operator":
        reasons.append(f"executorPreview.executor {preview.get('executor')} is not supported")
    if preview.get("gpuOperatorLabel") != GPU_OPERATOR_MIG_CONFIG_LABEL:
        reasons.append(f"executorPreview.gpuOperatorLabel must be {GPU_OPERATOR_MIG_CONFIG_LABEL}")
    if not isinstance(preview.get("gpuTargets"), list):
        reasons.append("executorPreview.gpuTargets must be a list")
    if preview.get("wouldPatchNodeLabels") and preview.get("unresolvedPhysicalGpuIds"):
        reasons.append("executorPreview cannot patch node labels while physical GPU bindings are unresolved")
    return reasons


def validate_action_previews(spec: dict[str, Any]) -> list[str]:
    reasons = []
    for field in [
        "migGeometryPreview",
        "trafficAndDrainPreview",
        "podLifecyclePreview",
        "abstractActionPreview",
        "adapterDryRunPreview",
        "observerPreview",
    ]:
        preview = spec.get(field)
        if not isinstance(preview, dict):
            reasons.append(f"{field} is required")
        elif not bool(preview.get("previewOnly", False)):
            reasons.append(f"{field}.previewOnly must be true for the dry-run actuator")
    return reasons


def _actions(status: dict[str, Any]) -> list[dict[str, Any]]:
    return [dict(action) for action in list(status.get("actions", [])) if isinstance(action, dict)]


def _final_plan_items(status: dict[str, Any]) -> list[dict[str, Any]]:
    transition = dict(dict(status.get("planningTrace", {})).get("transition", {}))
    return [dict(item) for item in list(transition.get("finalPlanItems", [])) if isinstance(item, dict)]


def _final_coarse_actions(status: dict[str, Any]) -> list[dict[str, Any]]:
    transition = dict(dict(status.get("planningTrace", {})).get("transition", {}))
    return [dict(action) for action in list(transition.get("finalCoarseActions", [])) if isinstance(action, dict)]


def _abstract_action_row(action: dict[str, Any], plan_items: list[dict[str, Any]]) -> dict[str, Any]:
    action_type = str(action.get("type"))
    gpu_id = action.get("gpu_id")
    related_items = [
        _plan_item_brief(item)
        for item in plan_items
        if gpu_id is None or item.get("gpu_id") == gpu_id
    ]
    return {
        "type": action_type,
        "gpuId": gpu_id,
        "physicalGpuId": action.get("physical_gpu_id") or action.get("source_physical_gpu_id"),
        "targetPhysicalGpuId": action.get("new_physical_gpu_id"),
        "sourceTemplate": action.get("src_template"),
        "targetTemplate": action.get("tgt_template") or action.get("template"),
        "mode": action.get("mode"),
        "rule": _abstract_rule(action),
        "gates": _abstract_gates(action_type, action.get("mode")),
        "podImpact": _abstract_pod_impact(action_type, action.get("mode"), related_items),
        "migImpact": _abstract_mig_impact(action_type, action.get("mode")),
        "relatedPlanItems": related_items,
    }


def _plan_item_brief(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": item.get("id"),
        "type": item.get("type"),
        "currentPhase": item.get("current_phase"),
        "status": item.get("status"),
        "blockedBy": item.get("blocked_by"),
        "workload": item.get("workload"),
        "rerouteDestination": item.get("rerouteDestination") or item.get("reroute_destination") or item.get("takeover"),
        "queued": item.get("queued"),
        "runningWork": item.get("runningWork") or item.get("running_work") or item.get("inflight"),
        "drainRemaining": item.get("drain_remaining"),
    }


def _abstract_rule(action: dict[str, Any]) -> str:
    action_type = str(action.get("type"))
    mode = action.get("mode")
    if action_type == "reconfiguration" and mode == "target_first":
        return "No unchanged reroute destination exists for every old workload slot, so prepare target-side GPU before old-side cutover."
    if action_type == "reconfiguration" and mode == "in_place_old_first":
        return "Every old workload slot has unchanged reroute capacity, so drain old side and reconfigure in place."
    if action_type == "remove_gpu":
        return "All active slots must pass stop/reroute/drain barriers before old MIG geometry can be cleared."
    if action_type == "instance_diff":
        return "MIG geometry is stable; only slot workload or batch payload changes need router/pod handling."
    if action_type == "create_gpu":
        return "Allocate a free physical GPU and prepare the target MIG layout before serving traffic."
    if action_type == "keep_gpu":
        return "No migration gates are needed because source and target state match."
    return "Planner classified this GPU using phase-greedy source/target state comparison."


def _abstract_gates(action_type: str, mode: Any) -> list[str]:
    if action_type == "create_gpu":
        return ["observeFreePhysicalGpu", "prepareMigGeometry", "observeTargetMigReady", "prepareServingCapacity"]
    if action_type == "remove_gpu":
        return ["findStableRerouteCapacity", "stopAcceptingOldSlots", "rerouteQueuedRequests", "waitQueuedAndRunningWorkZero", "deleteOrRecyclePods", "clearOldMigGeometry"]
    if action_type == "reconfiguration" and mode == "target_first":
        return ["prepareTargetMigGeometry", "prepareTargetServingCapacity", "shiftRouting", "waitOldInflightZero", "clearOldMigGeometry", "observeAndBindTarget"]
    if action_type == "reconfiguration":
        return ["stopAcceptingOldSlots", "rerouteQueuedRequests", "waitQueuedAndRunningWorkZero", "clearOldMigGeometry", "prepareMigGeometryInPlace", "observeAndBindTarget"]
    if action_type == "instance_diff":
        return ["prepareTargetServingCapacity", "shiftRoutingOrReload", "waitOldInflightZero", "deleteOrRecycleOldPod"]
    return []


def _abstract_pod_impact(action_type: str, mode: Any, related_items: list[dict[str, Any]]) -> dict[str, Any]:
    blocked = any(item.get("status") == "blocked" for item in related_items)
    return {
        "mayCreateOrReuseBeforeCutover": action_type in {"create_gpu", "reconfiguration", "instance_diff"},
        "mustDrainOldBeforeDelete": action_type in {"remove_gpu", "reconfiguration", "instance_diff"},
        "preferWarmPool": action_type in {"create_gpu", "reconfiguration", "instance_diff"},
        "blockedByDrainOrTakeover": blocked,
    }


def _abstract_mig_impact(action_type: str, mode: Any) -> dict[str, Any]:
    return {
        "requiresMigManager": action_type in {"create_gpu", "remove_gpu", "reconfiguration"},
        "requiresNewTargetGpu": action_type == "create_gpu" or (action_type == "reconfiguration" and mode == "target_first"),
        "clearOldGpuAfterDrain": action_type in {"remove_gpu", "reconfiguration"},
        "inPlaceGeometryChange": action_type == "reconfiguration" and mode == "in_place_old_first",
    }


def _action_brief(action: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "type",
        "abstractAction",
        "gpu_id",
        "physical_gpu_id",
        "target_physical_gpu_id",
        "new_physical_gpu_id",
        "source_physical_gpu_id",
        "slot",
        "workload",
        "old_workload",
        "new_workload",
        "template",
        "batch",
        "old_batch",
        "new_batch",
        "logical_gpu_id",
        "pendingLogicalGpuId",
        "activeLogicalGpuId",
        "clearsPendingLogicalGpuId",
        "clearsActiveLogicalGpuId",
        "phase",
        "queued",
        "to",
        "target_gpu_id",
        "target_slot",
        "queue_transfer_id",
        "rounds",
        "safe_now",
        "drained",
        "bridged",
        "after_drain",
        "policy",
    ]
    return {key: action.get(key) for key in keys if key in action}


def _pod_lifecycle_row(action: dict[str, Any], reason: str) -> dict[str, Any]:
    row = _action_brief(action)
    row["reason"] = reason
    row["podAction"] = _pod_action_for_type(str(action.get("type")))
    row["preferWarmPool"] = str(action.get("type")) in {"place_instance", "bridge_place_instance", "deploy_target_workloads"}
    return row


def _pod_action_for_type(action_type: str) -> str:
    if action_type in {"place_instance", "bridge_place_instance", "deploy_target_workloads"}:
        return "create-or-reuse"
    if action_type in {"stop_gpu_traffic", "stop_accepting_new", "accept_queued_requests", "reroute_queued_tasks", "mark_draining_instance"}:
        return "drain"
    if action_type in {"delete_pods", "remove_instance", "delete_gpu_pods", "delete_bridge_pod", "clear_gpu", "clear_gpu_binding"}:
        return "delete-or-recycle"
    if action_type in {"update_batch", "patch_batch_config", "apply_batch", "verify_batch"}:
        return "reload-in-place"
    return "none"


def _physical_gpu_bindings(metadata: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw = (
        metadata.get("physical_gpu_bindings")
        or metadata.get("physicalGpuBindings")
        or metadata.get("physical_gpu_node_map")
        or metadata.get("physicalGpuNodeMap")
        or {}
    )
    bindings = {}
    for physical_gpu_id, value in dict(raw).items():
        if isinstance(value, str):
            bindings[str(physical_gpu_id)] = {"nodeName": value, "deviceIndex": None}
        elif isinstance(value, dict):
            bindings[str(physical_gpu_id)] = {
                "nodeName": value.get("nodeName") or value.get("node"),
                "deviceIndex": value.get("deviceIndex")
                if value.get("deviceIndex") is not None
                else value.get("gpuIndex"),
            }
    return bindings


def _template_from_gpu(raw_gpu: dict[str, Any]) -> str:
    profiles = [
        str(inst.get("profile"))
        for inst in sorted(raw_gpu.get("instances", []), key=lambda item: int(item.get("start", 0)))
        if str(inst.get("profile")) != "void"
    ]
    return "+".join(profiles)


def _target_instances(raw_gpu: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "start": int(inst.get("start")),
            "end": int(inst.get("end")),
            "profile": str(inst.get("profile")),
        }
        for inst in sorted(raw_gpu.get("instances", []), key=lambda item: int(item.get("start", 0)))
        if str(inst.get("profile")) != "void"
    ]


def _mig_config_name(node_name: str, targets: list[dict[str, Any]]) -> str:
    builtin = _builtin_gpu_operator_config_name(targets)
    if builtin is not None:
        return builtin
    fingerprint = "|".join(
        f"{target.get('deviceIndex')}={target.get('targetTemplate')}"
        for target in sorted(targets, key=lambda item: int(item.get("deviceIndex", 0)))
    )
    digest = hashlib.sha1(f"{node_name}|{fingerprint}".encode("utf-8")).hexdigest()[:10]
    return _label_value(f"or-sim-{digest}")


def _builtin_gpu_operator_config_name(targets: list[dict[str, Any]]) -> str | None:
    """Map simple A100-40GB full-GPU layouts to GPU Operator built-in configs."""
    templates = {
        str(target.get("targetTemplate", ""))
        for target in targets
        if target.get("targetTemplate") is not None
    }
    if len(templates) != 1:
        return None
    template = next(iter(templates))
    return {
        "": "or-sim-empty",
        "1g+1g+1g+1g+1g+1g+1g": "all-1g.5gb",
        "2g+2g+2g": "all-2g.10gb",
        "3g+3g": "all-3g.20gb",
        "4g": "all-4g.20gb",
        "7g": "all-7g.40gb",
        "4g+3g": "or-sim-4-3",
        "4g+2g+1g": "or-sim-4-2-1",
        "4g+1g+1g+1g": "or-sim-4-1-1-1",
        "3g+2g+1g": "or-sim-3-2-1",
        "3g+1g+1g+1g": "or-sim-3-1-1-1",
        "2g+2g+3g": "or-sim-2-2-3",
        "3g+2g+1g+1g": "or-sim-3-2-1-1",
        "3g+1g+1g+1g+1g": "or-sim-3-1-1-1-1",
        "2g+2g+2g+1g": "or-sim-2-2-2-1",
        "2g+2g+1g+1g+1g": "or-sim-2-2-1-1-1",
        "2g+1g+1g+1g+1g+1g": "or-sim-2-1-1-1-1-1",
    }.get(template)


def _label_value(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("._-")
    return cleaned[:63] or "or-sim-preview"
