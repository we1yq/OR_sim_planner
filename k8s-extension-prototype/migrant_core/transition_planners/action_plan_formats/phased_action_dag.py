from __future__ import annotations

from collections import Counter, defaultdict, deque
from typing import Any


def build_phased_action_plan(
    actions: list[dict[str, Any]],
    plan_items: list[dict[str, Any]] | None = None,
    name: str = "action-plan",
) -> dict[str, Any]:
    """Compile a linear action list into a conservative phase/DAG view.

    The compiler preserves the original actions and adds dependency edges for
    actions that share a root, GPU, physical GPU, or MIG slot. Independent roots
    can therefore land in the same phase, while per-resource order remains the
    same as the input action list.
    """

    nodes = []
    deps_by_node: dict[str, set[str]] = {}
    dependents_by_node: dict[str, set[str]] = defaultdict(set)
    last_by_resource: dict[str, str] = {}
    roots: dict[str, dict[str, Any]] = {}
    plan_item_by_root = {
        str(item.get("id")): dict(item)
        for item in list(plan_items or [])
        if isinstance(item, dict) and item.get("id")
    }

    for index, raw_action in enumerate(actions):
        action = _yamlable(dict(raw_action))
        root_id = _root_id_for_action(action)
        node_id = _node_id(index, action, root_id)
        resources = _resources_for_action(action, root_id)
        dependencies = {
            last_by_resource[resource]
            for resource in resources
            if resource in last_by_resource
        }
        for resource in _read_only_dependency_resources(action):
            if resource in last_by_resource:
                dependencies.add(last_by_resource[resource])
        for action_key in action.get("dependsOnActionKeys", []) or []:
            key_node_id = last_by_resource.get(f"action-key:{action_key}")
            if key_node_id is not None:
                dependencies.add(key_node_id)
        for dep in dependencies:
            dependents_by_node[dep].add(node_id)
        for resource in resources:
            last_by_resource[resource] = node_id
        if action.get("actionKey") is not None:
            last_by_resource[f"action-key:{action['actionKey']}"] = node_id
        deps_by_node[node_id] = set(dependencies)
        roots.setdefault(
            root_id,
            {
                "rootId": root_id,
                "nodeIds": [],
                "planItem": _yamlable(plan_item_by_root.get(root_id)),
            },
        )
        roots[root_id]["nodeIds"].append(node_id)
        nodes.append(
            {
                "id": node_id,
                "index": index,
                "rootId": root_id,
                "operationClass": _operation_class(action),
                "action": action,
                "resources": sorted(resources),
                "dependsOn": sorted(dependencies),
            }
        )

    phases = _topological_phases(nodes, deps_by_node, dependents_by_node)
    node_phase = {
        node_id: phase["phase"]
        for phase in phases
        for node_id in phase["nodeIds"]
    }
    for node in nodes:
        node["phase"] = int(node_phase.get(str(node["id"]), 0))
    for root in roots.values():
        root["phaseSpan"] = _phase_span(root["nodeIds"], node_phase)

    edge_count = sum(len(deps) for deps in deps_by_node.values())
    return {
        "representation": "migrant.phased-action-dag/v1",
        "name": name,
        "actionCount": len(actions),
        "nodeCount": len(nodes),
        "edgeCount": edge_count,
        "phaseCount": len(phases),
        "criticalPathLength": len(phases),
        "phases": phases,
        "roots": sorted(roots.values(), key=lambda item: str(item["rootId"])),
        "nodes": nodes,
    }


def compact_phased_action_plan(plan: dict[str, Any]) -> dict[str, Any]:
    phases = []
    for phase in list(plan.get("phases", [])):
        phases.append(
            {
                "phase": int(phase.get("phase", 0)),
                "nodeCount": int(phase.get("nodeCount", 0)),
                "actionCountsByType": dict(phase.get("actionCountsByType", {})),
                "rootIds": list(phase.get("rootIds", [])),
            }
        )
    return {
        "representation": plan.get("representation"),
        "actionCount": int(plan.get("actionCount", 0)),
        "nodeCount": int(plan.get("nodeCount", 0)),
        "edgeCount": int(plan.get("edgeCount", 0)),
        "phaseCount": int(plan.get("phaseCount", 0)),
        "criticalPathLength": int(plan.get("criticalPathLength", 0)),
        "phases": phases,
    }


def _topological_phases(
    nodes: list[dict[str, Any]],
    deps_by_node: dict[str, set[str]],
    dependents_by_node: dict[str, set[str]],
) -> list[dict[str, Any]]:
    remaining_deps = {node_id: set(deps) for node_id, deps in deps_by_node.items()}
    ready = deque(
        str(node["id"])
        for node in nodes
        if not remaining_deps.get(str(node["id"]), set())
    )
    by_id = {str(node["id"]): node for node in nodes}
    phases = []
    scheduled: set[str] = set()
    phase_idx = 0

    while ready:
        current = sorted({ready.popleft() for _ in range(len(ready))})
        if not current:
            continue
        scheduled.update(current)
        phase_nodes = [by_id[node_id] for node_id in current]
        phases.append(_phase_summary(phase_idx, phase_nodes))
        phase_idx += 1
        newly_ready = []
        for node_id in current:
            for dependent in sorted(dependents_by_node.get(node_id, set())):
                remaining_deps[dependent].discard(node_id)
                if not remaining_deps[dependent] and dependent not in scheduled:
                    newly_ready.append(dependent)
        ready.extend(sorted(set(newly_ready)))

    if len(scheduled) != len(nodes):
        unscheduled = [str(node["id"]) for node in nodes if str(node["id"]) not in scheduled]
        phases.append(
            {
                "phase": phase_idx,
                "name": f"cycle-break-{phase_idx:02d}",
                "nodeCount": len(unscheduled),
                "nodeIds": unscheduled,
                "rootIds": sorted({str(by_id[node_id].get("rootId")) for node_id in unscheduled}),
                "actionCountsByType": _counts(by_id[node_id]["action"] for node_id in unscheduled),
                "warning": "cycle detected; preserved unscheduled nodes in input order",
            }
        )
    return phases


def _phase_summary(phase_idx: int, phase_nodes: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "phase": phase_idx,
        "name": f"phase-{phase_idx:02d}",
        "nodeCount": len(phase_nodes),
        "nodeIds": [str(node["id"]) for node in phase_nodes],
        "rootIds": sorted({str(node.get("rootId")) for node in phase_nodes}),
        "actionCountsByType": _counts(node["action"] for node in phase_nodes),
        "operationClasses": _counts({"type": node.get("operationClass", "unknown")} for node in phase_nodes),
    }


def _counts(actions: Any) -> dict[str, int]:
    counter = Counter(str(action.get("type", "unknown")) for action in actions)
    return dict(sorted(counter.items()))


def _node_id(index: int, action: dict[str, Any], root_id: str) -> str:
    action_type = str(action.get("type", "unknown"))
    return f"a{index:04d}_{action_type}_{_slug(root_id)}"


def _root_id_for_action(action: dict[str, Any]) -> str:
    if action.get("abstractRoot") is not None:
        return str(action["abstractRoot"])
    action_type = str(action.get("type", "unknown"))
    gpu_id = action.get("gpu_id")
    slot = action.get("slot")
    if gpu_id is not None and isinstance(slot, list) and len(slot) >= 3:
        return f"SLOT_gpu{gpu_id}_{slot[0]}_{slot[1]}_{slot[2]}"
    if gpu_id is not None:
        return f"GPU_gpu{gpu_id}"
    if action.get("physical_gpu_id"):
        return f"PHY_{action.get('physical_gpu_id')}"
    return f"GLOBAL_{action_type}"


def _resources_for_action(action: dict[str, Any], root_id: str) -> set[str]:
    resources: set[str] = set()
    action_type = str(action.get("type", ""))
    if action.get("slot") is not None:
        resources.add(f"slot:{action.get('gpu_id')}:{tuple(action['slot'])}")
    if action.get("queue_transfer_id") is not None:
        resources.add(f"queue-transfer:{action['queue_transfer_id']}")
    if action_type in {
        "allocate_gpu",
        "configure_full_template",
        "observe_mig_devices",
        "deploy_target_workloads",
        "bind_target_gpu",
        "delete_pods",
        "delete_gpu_pods",
        "clear_gpu_binding",
        "clear_template",
        "return_gpu",
        "stop_gpu_traffic",
    } and action.get("physical_gpu_id") is not None:
        resources.add(f"physical:{action['physical_gpu_id']}")
    if action_type == "observe_mig_devices" and action.get("gpu_id") is not None and action.get("physical_gpu_id") is not None:
        resources.add(f"mig-devices:{action['gpu_id']}:{action['physical_gpu_id']}")
    if action_type in {"clear_gpu_binding", "bind_target_gpu"} and action.get("gpu_id") is not None:
        resources.add(f"logical-binding:{action['gpu_id']}")
    if action_type == "activate_serving_route" and action.get("physical_gpu_id") is not None and action.get("slot") is None:
        resources.add(f"physical:{action['physical_gpu_id']}")
    if action_type == "stop_gpu_traffic" and action.get("physical_gpu_id") is not None:
        resources.add(f"traffic:{action['physical_gpu_id']}")
    if action_type in {"reroute_queued_tasks", "mark_draining_instance"} and action.get("slot") is not None:
        resources.add(f"traffic-slot:{action.get('gpu_id')}:{tuple(action['slot'])}")
    return resources


def _read_only_dependency_resources(action: dict[str, Any]) -> set[str]:
    action_type = str(action.get("type", ""))
    if action_type in {"deploy_target_workloads", "place_instance", "bridge_place_instance", "workload_change"}:
        if action.get("gpu_id") is not None and action.get("physical_gpu_id") is not None:
            return {f"mig-devices:{action['gpu_id']}:{action['physical_gpu_id']}"}
    return set()


def _operation_class(action: dict[str, Any]) -> str:
    action_type = str(action.get("type", ""))
    if action_type in {"allocate_gpu", "configure_full_template", "place_target_layout", "observe_mig_devices"}:
        return "mig-geometry"
    if action_type in {"bind_target_gpu", "mark_reconfig_target_prepared", "unbind_target_gpu", "clear_gpu_binding", "return_gpu"}:
        return "binding-state"
    if action_type in {"stop_gpu_traffic", "stop_accepting_new", "accept_queued_requests", "reroute_queued_tasks", "mark_draining_instance", "activate_serving_route"}:
        return "router-drain"
    if action_type in {"place_instance", "bridge_place_instance", "update_batch", "patch_batch_config", "apply_batch", "verify_batch", "workload_change", "deploy_target_workloads"}:
        return "pod-lifecycle"
    if action_type in {"delete_pods", "delete_gpu_pods", "remove_instance", "delete_bridge_pod", "clear_gpu", "clear_template"}:
        return "cleanup"
    if action_type.startswith("defer_"):
        return "blocked"
    return "other"


def _phase_span(node_ids: list[str], node_phase: dict[str, int]) -> dict[str, int] | None:
    phases = [node_phase[node_id] for node_id in node_ids if node_id in node_phase]
    if not phases:
        return None
    return {"first": min(phases), "last": max(phases)}


def _slug(value: str) -> str:
    return "".join(ch if ch.isalnum() else "-" for ch in value).strip("-")


def _yamlable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _yamlable(v) for k, v in value.items()}
    if isinstance(value, tuple):
        return [_yamlable(v) for v in value]
    if isinstance(value, list):
        return [_yamlable(v) for v in value]
    if hasattr(value, "item"):
        return value.item()
    return value
