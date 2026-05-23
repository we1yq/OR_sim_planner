from __future__ import annotations

import time
from typing import Any

from ..partial_reconfig import build_partial_reconfig_plan
from ..physical_ids import bootstrap_physical_ids_for_state, ensure_state_metadata, get_physical_id
from ..state import ClusterState, GPUState, MigInstance, deepcopy_state, get_inst_by_slot, gpu_map_by_id
from ..transition_common import (
    alloc_from_free_pool,
    classify_gpu_change,
    diff_instances_within_same_template,
    matches_target_state,
    provided_by_workload,
    safe_after_removing_gpu,
    safe_after_removing_instance,
)
from ..transition_engine import prepare_transition_runtime, required_arrival_dict, simulate_transition_actions
from . import basic_dag
from .action_plan_formats import build_phased_action_plan, compact_phased_action_plan


NAME = "transition.effect_aware_dag"


def run(
    *,
    source_state: Any,
    target_state: Any,
    src_arrival: list[float] | tuple[float, ...] | dict[str, float],
    tgt_arrival: list[float] | tuple[float, ...] | dict[str, float],
    workload_names: list[str] | tuple[str, ...] | None = None,
    stage_name: str = "stage_effect_aware_dag",
    max_iters: int = 1,
    default_queued: int = 2,
    default_inflight: int = 1,
    override_existing_runtime_for_changed_slots: bool = False,
    **_: Any,
) -> dict[str, Any]:
    """Build a final transition DAG from explicit action effects.

    This planner keeps the existing final-DAG execution contract, but records
    each milestone's capacity, router, MIG, and physical-GPU effects directly on
    the action. Candidate selection is feasibility first: the lowering path
    preserves capacity gates before choosing partial, in-place, or bridge style
    reconfiguration.
    """

    start = time.perf_counter()
    current_state = prepare_transition_runtime(
        source_state,
        target_state,
        default_queued=default_queued,
        default_inflight=default_inflight,
        override_existing_changed_slots=override_existing_runtime_for_changed_slots,
    )
    ensure_state_metadata(current_state)
    bootstrap_physical_ids_for_state(current_state)
    target_state = deepcopy_state(target_state)
    ensure_state_metadata(target_state)

    required = required_arrival_dict(src_arrival, tgt_arrival, workload_names=workload_names)
    actions, plan_items, decision_trace = _build_effect_aware_actions(
        source_state=current_state,
        target_state=target_state,
        required=required,
    )
    actions = _annotate_effects(actions, current_state, target_state, required)
    _add_capacity_dependency_edges(actions, current_state, required)
    _add_physical_reuse_dependency_edges(actions)
    actions = basic_dag._coalesce_slot_delete_pods(actions)
    basic_dag._assert_reroute_destinations_stable(current_state, target_state, actions)
    planned_state = basic_dag._planned_state_for_actions(current_state, target_state, actions)
    executed_state = simulate_transition_actions(
        source_state=current_state,
        target_state=planned_state,
        fine_actions=actions,
        next_physical_idx=current_state.metadata.get("next_physical_idx", 0),
    )
    executed_state = basic_dag._drop_available_physical_gpus(executed_state)
    dag = build_phased_action_plan(actions, plan_items=plan_items, name=f"{stage_name}-final")
    peak_active_gpu = basic_dag._peak_serving_gpu_from_actions(current_state, actions)
    reached_target = matches_target_state(executed_state, target_state)
    final_plan = {
        "stage_name": stage_name,
        "required": required,
        "fine_actions": actions,
        "executed_actions": actions,
        "blocked_actions": [action for action in actions if str(action.get("type", "")).startswith("defer_")],
        "planned_state": planned_state,
        "executed_state": executed_state,
        "plan_items": plan_items,
        "planner_objective_order": [
            "capacity and runtime safety as hard constraints",
            "physical GPU availability and reuse constraints",
            "prefer feasible partial reconfiguration",
            "prefer feasible in-place reconfiguration",
            "use bridge reconfiguration only when target-side capacity must be built first",
        ],
        "runtime_assumptions": {
            "defaultQueued": int(default_queued),
            "defaultInflight": int(default_inflight),
            "overrideExistingChangedSlots": bool(override_existing_runtime_for_changed_slots),
        },
        "effect_model": {
            "capacity": "producesCapacity/consumesCapacity annotate route activation and serving removal",
            "router": "stop_accepting_new owns router queue redispatch when routerQueueRedispatch=true",
            "physicalGpu": "allocate_gpu and return_gpu carry physicalGpuEffect",
            "mig": "configure/observe/clear actions carry migEffect",
        },
        "candidate_decisions": decision_trace,
    }
    return {
        "stage_name": stage_name,
        "iterations": [
            {
                "iteration": 1,
                "candidate_actions": actions,
                "chosen_actions": actions,
                "state_before": deepcopy_state(current_state),
                "state_after": deepcopy_state(executed_state),
                "made_progress": True,
                "reached_target": reached_target,
                "phased_action_plan": dag,
                "phased_action_plan_summary": compact_phased_action_plan(dag),
                "candidate_decisions": decision_trace,
            }
        ],
        "iteration_count": 1,
        "reached_target": reached_target,
        "elapsed_sec": time.perf_counter() - start,
        "executed_actions": actions,
        "executed_state": executed_state,
        "target_state": deepcopy_state(target_state),
        "initial_runtime_state": deepcopy_state(current_state),
        "peak_active_gpu": peak_active_gpu,
        "source_active_gpu": len(basic_dag._active_serving_pid_set(current_state)),
        "final_active_gpu": len(basic_dag._active_serving_pid_set(executed_state)),
        "final_plan": final_plan,
        "phased_action_plan": dag,
        "phased_action_plan_summary": compact_phased_action_plan(dag),
        "transition_planner_module": NAME,
        "max_iters_ignored": max_iters,
    }


def _build_effect_aware_actions(
    *,
    source_state: ClusterState,
    target_state: ClusterState,
    required: dict[str, float],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    src_map = {
        gpu_id: gpu
        for gpu_id, gpu in gpu_map_by_id(source_state).items()
        if not basic_dag._is_available_physical_gpu(gpu)
    }
    tgt_map = gpu_map_by_id(target_state)
    all_gpu_ids = sorted(set(src_map) | set(tgt_map))
    free_pool = basic_dag._build_initial_available_pool(source_state, src_map)
    actions: list[dict[str, Any]] = []
    plan_items: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []

    classified = {gpu_id: classify_gpu_change(src_map.get(gpu_id), tgt_map.get(gpu_id)) for gpu_id in all_gpu_ids}

    for gpu_id in [gpu_id for gpu_id, kind in classified.items() if kind == "instance_diff"]:
        before = len(actions)
        _append_effect_instance_diff(actions, plan_items, source_state, target_state, gpu_id, required)
        decisions.append(_fixed_decision("instance_diff", gpu_id, actions[before:]))

    for gpu_id in [gpu_id for gpu_id, kind in classified.items() if kind == "remove_gpu"]:
        before = len(actions)
        basic_dag._append_delete_gpu_actions(actions, plan_items, source_state, target_state, gpu_id, required)
        physical_id = get_physical_id(source_state, gpu_id)
        if physical_id is not None:
            free_pool.append(physical_id)
        decisions.append(_fixed_decision("remove_gpu", gpu_id, actions[before:]))

    for gpu_id in [gpu_id for gpu_id, kind in classified.items() if kind == "reconfiguration"]:
        src_gpu = src_map[gpu_id]
        tgt_gpu = tgt_map[gpu_id]
        old_physical_id = get_physical_id(source_state, gpu_id)
        candidates = _reconfiguration_candidates(
            source_state=source_state,
            target_state=target_state,
            gpu_id=gpu_id,
            src_gpu=src_gpu,
            tgt_gpu=tgt_gpu,
            old_physical_id=old_physical_id,
            free_pool=free_pool,
            required=required,
        )
        feasible = [candidate for candidate in candidates if candidate["feasible"]]
        if not feasible:
            root = f"RECONF_BLOCKED_gpu{gpu_id}"
            actions.append(
                {
                    "type": "defer_reconfiguration",
                    "gpu_id": gpu_id,
                    "physical_gpu_id": old_physical_id,
                    "abstractRoot": root,
                    "transitionMode": "blocked_reconfiguration",
                    "abstractAction": "Blocked Reconfiguration",
                    "blockedByCapacity": True,
                    "reason": "no_feasible_reconfiguration_candidate",
                }
            )
            plan_items.append(
                {
                    **basic_dag._plan_item(root, "blocked_reconfiguration", gpu_id, old_physical_id),
                    "status": "blocked",
                    "blocked_by": "no_feasible_reconfiguration_candidate",
                }
            )
            decisions.append(_candidate_decision("reconfiguration", gpu_id, "blocked", candidates))
            continue
        chosen = min(feasible, key=lambda candidate: candidate["rank"])
        actions.extend(chosen["actions"])
        plan_items.extend(chosen["plan_items"])
        decisions.append(_candidate_decision("reconfiguration", gpu_id, chosen["mode"], candidates))
        if chosen["mode"] == "bridge_reconfiguration":
            alloc_from_free_pool(free_pool)
            if old_physical_id is not None:
                free_pool.append(old_physical_id)

    for gpu_id in [gpu_id for gpu_id, kind in classified.items() if kind == "create_gpu"]:
        before = len(actions)
        physical_id = alloc_from_free_pool(free_pool)
        basic_dag._append_create_target_gpu_actions(actions, plan_items, gpu_id, physical_id, tgt_map[gpu_id])
        decisions.append(_fixed_decision("create_gpu", gpu_id, actions[before:]))

    return actions, plan_items, decisions


def _append_effect_instance_diff(
    actions: list[dict[str, Any]],
    plan_items: list[dict[str, Any]],
    source_state: ClusterState,
    target_state: ClusterState,
    gpu_id: int,
    required: dict[str, float],
) -> None:
    src_gpu = gpu_map_by_id(source_state)[gpu_id]
    tgt_gpu = gpu_map_by_id(target_state)[gpu_id]
    physical_id = get_physical_id(source_state, gpu_id)
    for inst_action in diff_instances_within_same_template(src_gpu, tgt_gpu):
        if inst_action["type"] == "workload_change" and not _workload_replacement_possible(source_state, inst_action["src"], required):
            slot = inst_action["slot"]
            root = f"SLOT_gpu{gpu_id}_{slot[0]}_{slot[1]}_{slot[2]}"
            common = {"transitionMode": "workload_replacement", "abstractRoot": root}
            actions.extend(basic_dag._queue_and_drain_actions(source_state, target_state, gpu_id, physical_id, inst_action["src"], required, common))
            actions.append(
                {
                    "type": "defer_workload_change",
                    "gpu_id": gpu_id,
                    "physical_gpu_id": physical_id,
                    "slot": slot,
                    "workload": inst_action["src"].workload,
                    "new_workload": inst_action["tgt"].workload,
                    "abstractRoot": root,
                    "transitionMode": "workload_replacement",
                    "abstractAction": "Blocked Workload Replacement",
                    "blockedByCapacity": True,
                    "reason": "no_same_workload_capacity_producer",
                }
            )
            plan_items.append(
                {
                    **basic_dag._plan_item(root, "workload_replacement", gpu_id, physical_id, slot=slot, workload=inst_action["src"].workload),
                    "status": "blocked",
                    "blocked_by": "no_same_workload_capacity_producer",
                }
            )
            continue
        tmp_actions: list[dict[str, Any]] = []
        tmp_items: list[dict[str, Any]] = []
        basic_dag._append_instance_diff_actions(tmp_actions, tmp_items, source_state, target_state, gpu_id, required)
        if inst_action["type"] == "keep":
            continue
        slot = inst_action.get("slot")
        root = f"SLOT_gpu{gpu_id}_{slot[0]}_{slot[1]}_{slot[2]}" if slot is not None else None
        actions.extend([action for action in tmp_actions if root is None or action.get("abstractRoot") == root])
        plan_items.extend([item for item in tmp_items if root is None or item.get("id") == root])


def _workload_replacement_possible(source_state: ClusterState, src: MigInstance, required: dict[str, float]) -> bool:
    if safe_after_removing_instance(source_state, src, required):
        return True
    return _same_workload_producer_exists(source_state, src.workload, exclude=src)


def _same_workload_producer_exists(source_state: ClusterState, workload: str | None, exclude: MigInstance | None = None) -> bool:
    if workload is None:
        return True
    for gpu in source_state.real_gpus():
        for inst in gpu.instances:
            if inst is exclude:
                continue
            if inst.workload == workload and float(inst.mu) > 0.0:
                return True
    return False


def _reconfiguration_candidates(
    *,
    source_state: ClusterState,
    target_state: ClusterState,
    gpu_id: int,
    src_gpu: GPUState,
    tgt_gpu: GPUState,
    old_physical_id: str | None,
    free_pool: list[str],
    required: dict[str, float],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    partial_plan = build_partial_reconfig_plan(src_gpu, tgt_gpu)
    if partial_plan is not None:
        local_actions: list[dict[str, Any]] = []
        local_items: list[dict[str, Any]] = []
        basic_dag._append_partial_reconfiguration_actions(
            local_actions,
            local_items,
            source_state,
            target_state,
            gpu_id,
            old_physical_id,
            partial_plan,
        )
        _append_preserved_slot_serving_updates(
            local_actions,
            local_items,
            source_state,
            target_state,
            gpu_id,
            old_physical_id,
            partial_plan,
            required,
        )
        feasible = _partial_effect_feasible(source_state, src_gpu, partial_plan, required)
        candidates.append(_candidate("partial_reconfiguration", 0, feasible, local_actions, local_items))

    local_actions = []
    local_items = []
    basic_dag._append_in_place_reconfiguration_actions(
        local_actions,
        local_items,
        source_state,
        target_state,
        gpu_id,
        old_physical_id,
        tgt_gpu,
    )
    candidates.append(
        _candidate(
            "in_place_reconfiguration",
            1,
            safe_after_removing_gpu(source_state, src_gpu, required),
            local_actions,
            local_items,
        )
    )

    if free_pool:
        new_physical_id = free_pool[-1]
        local_actions = []
        local_items = []
        _append_effect_bridge_reconfiguration_actions(
            local_actions,
            local_items,
            source_state,
            target_state,
            gpu_id,
            old_physical_id,
            new_physical_id,
            tgt_gpu,
        )
        candidates.append(_candidate("bridge_reconfiguration", 2, True, local_actions, local_items))
    return candidates


def _append_effect_bridge_reconfiguration_actions(
    actions: list[dict[str, Any]],
    plan_items: list[dict[str, Any]],
    source_state: ClusterState,
    target_state: ClusterState,
    gpu_id: int,
    old_physical_id: str | None,
    new_physical_id: str,
    target_gpu: GPUState,
) -> None:
    src_gpu = gpu_map_by_id(source_state)[gpu_id]
    template = target_gpu.template_str()
    create_spec = basic_dag._gpu_create_spec(target_gpu)
    root = f"BRIDGE_RECONF_gpu{gpu_id}"
    common = {"transitionMode": "bridge_reconfiguration", "abstractRoot": root}
    slots = [(inst.start, inst.end, inst.profile) for inst in basic_dag._nonfree_instances(src_gpu)]
    actions.extend(
        [
            basic_dag._action(
                "allocate_gpu",
                gpu_id=gpu_id,
                physical_gpu_id=new_physical_id,
                logical_gpu_id=gpu_id,
                pendingLogicalGpuId=gpu_id,
                **common,
            ),
            basic_dag._action(
                "configure_full_template",
                gpu_id=gpu_id,
                physical_gpu_id=new_physical_id,
                logical_gpu_id=gpu_id,
                pendingLogicalGpuId=gpu_id,
                template=template,
                createSpec=create_spec,
                **common,
            ),
            basic_dag._action(
                "bind_target_gpu",
                gpu_id=gpu_id,
                physical_gpu_id=new_physical_id,
                logical_gpu_id=gpu_id,
                activeLogicalGpuId=gpu_id,
                clearsPendingLogicalGpuId=True,
                **common,
            ),
            *basic_dag._tag_actions(basic_dag._target_activation_actions(gpu_id, new_physical_id), common),
        ]
    )
    if slots:
        actions.append(
            basic_dag._action(
                "stop_gpu_traffic",
                gpu_id=gpu_id,
                physical_gpu_id=old_physical_id,
                slots=slots,
                slotCount=len(slots),
                **common,
            )
        )
    for inst in basic_dag._nonfree_instances(src_gpu):
        actions.extend(
            basic_dag._queue_and_drain_actions(
                source_state,
                target_state,
                gpu_id,
                old_physical_id,
                inst,
                {},
                common,
                stop_new=False,
                exclude_entire_gpu=True,
            )
        )
    actions.extend(
        [
            basic_dag._action("delete_pods", gpu_id=gpu_id, physical_gpu_id=old_physical_id, slots=slots, **common),
            basic_dag._action(
                "clear_gpu_binding",
                gpu_id=gpu_id,
                physical_gpu_id=old_physical_id,
                logical_gpu_id=gpu_id,
                pendingLogicalGpuId=gpu_id,
                clearsActiveLogicalGpuId=True,
                **common,
            ),
            basic_dag._action("clear_template", gpu_id=gpu_id, physical_gpu_id=old_physical_id, template=src_gpu.template_str(), **common),
            basic_dag._action(
                "return_gpu",
                gpu_id=gpu_id,
                physical_gpu_id=old_physical_id,
                clearsPendingLogicalGpuId=True,
                **common,
            ),
        ]
    )
    plan_items.append(
        basic_dag._plan_item(
            root,
            "bridge_reconfiguration",
            gpu_id,
            old_physical_id,
            target_physical_gpu_id=new_physical_id,
        )
    )


def _partial_effect_feasible(source_state: ClusterState, src_gpu: GPUState, partial_plan: Any, required: dict[str, float]) -> bool:
    if basic_dag._partial_reconfiguration_capacity_safe(source_state, src_gpu, partial_plan, required):
        return True
    delete_slots = set(partial_plan.delete_slots)
    for inst in src_gpu.instances:
        if (inst.start, inst.end, inst.profile) in delete_slots and not _same_workload_producer_exists(source_state, inst.workload, exclude=inst):
            return False
    return True


def _append_preserved_slot_serving_updates(
    actions: list[dict[str, Any]],
    plan_items: list[dict[str, Any]],
    source_state: ClusterState,
    target_state: ClusterState,
    gpu_id: int,
    physical_id: str | None,
    partial_plan: Any,
    required: dict[str, float],
) -> None:
    src_gpu = gpu_map_by_id(source_state)[gpu_id]
    tgt_gpu = gpu_map_by_id(target_state)[gpu_id]
    preserve_slots = set(partial_plan.preserve_slots)
    for inst_action in diff_instances_within_same_template(src_gpu, tgt_gpu):
        slot = inst_action.get("slot")
        if slot not in preserve_slots or inst_action.get("type") == "keep":
            continue
        root = f"PRESERVE_SLOT_gpu{gpu_id}_{slot[0]}_{slot[1]}_{slot[2]}"
        common = {
            "transitionMode": f"preserved_slot_{inst_action['type']}",
            "abstractRoot": root,
            "preservedSlotUpdate": True,
            "siblingOfPartialReconfiguration": True,
            "partialContextRoot": f"PARTIAL_RECONF_gpu{gpu_id}",
        }
        change_type = inst_action["type"]
        if change_type == "batch_change":
            src = inst_action["src"]
            tgt = inst_action["tgt"]
            actions.extend(
                [
                    basic_dag._action("patch_batch_config", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, old_batch=src.batch, new_batch=tgt.batch, workload=src.workload, **common),
                    basic_dag._action("apply_batch", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, old_batch=src.batch, new_batch=tgt.batch, workload=src.workload, **common),
                    basic_dag._action("verify_batch", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, old_batch=src.batch, new_batch=tgt.batch, workload=src.workload, **common),
                    basic_dag._action("activate_serving_route", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=src.workload, **common),
                ]
            )
            plan_items.append(basic_dag._plan_item(root, "preserved_slot_batch_update", gpu_id, physical_id, slot=slot, workload=src.workload))
            continue
        if change_type == "place_instance":
            tgt = inst_action["tgt"]
            actions.extend(
                [
                    basic_dag._action("place_instance", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=tgt.workload, batch=tgt.batch, **common),
                    basic_dag._action("activate_serving_route", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=tgt.workload, **common),
                ]
            )
            plan_items.append(basic_dag._plan_item(root, "preserved_slot_place_instance", gpu_id, physical_id, slot=slot, workload=tgt.workload))
            continue
        if change_type == "safe_remove_instance":
            src = inst_action["src"]
            actions.extend(basic_dag._queue_and_drain_actions(source_state, target_state, gpu_id, physical_id, src, required, common))
            actions.append(basic_dag._action("delete_pods", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, slots=[slot], workload=src.workload, **common))
            plan_items.append(basic_dag._plan_item(root, "preserved_slot_remove_instance", gpu_id, physical_id, slot=slot, workload=src.workload))
            continue
        if change_type == "workload_change":
            src = inst_action["src"]
            tgt = inst_action["tgt"]
            change_common = {**common, "transitionMode": "workload_change"}
            actions.extend(basic_dag._queue_and_drain_actions(source_state, target_state, gpu_id, physical_id, src, required, common))
            actions.extend(
                [
                    basic_dag._action("delete_pods", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, slots=[slot], workload=src.workload, **change_common),
                    basic_dag._action("workload_change", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=tgt.workload, batch=tgt.batch, **change_common),
                    basic_dag._action("activate_serving_route", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=tgt.workload, **change_common),
                ]
            )
            plan_items.append(basic_dag._plan_item(root, "preserved_slot_workload_replacement", gpu_id, physical_id, slot=slot, workload=src.workload))


def _annotate_effects(
    actions: list[dict[str, Any]],
    source_state: ClusterState,
    target_state: ClusterState,
    required: dict[str, float],
) -> list[dict[str, Any]]:
    source_map = gpu_map_by_id(source_state)
    target_map = gpu_map_by_id(target_state)
    out = []
    for idx, action in enumerate(actions):
        item = dict(action)
        item.setdefault("actionKey", _action_key(idx, item))
        effects = _effects_for_action(item, source_map, target_map, required)
        item.update(effects)
        out.append(item)
    return out


def _add_capacity_dependency_edges(
    actions: list[dict[str, Any]],
    source_state: ClusterState,
    required: dict[str, float],
) -> None:
    ready_capacity = provided_by_workload(source_state)
    dependency_context = _capacity_dependency_context(actions)
    producers_by_workload: dict[str, list[dict[str, Any]]] = {}
    seen_producer_keys: set[tuple[str, str]] = set()
    for action in actions:
        for produced in list(action.get("producesCapacity") or []):
            workload = produced.get("workload")
            key = action.get("actionKey")
            if workload is not None and key is not None and (str(workload), str(key)) not in seen_producer_keys:
                producers_by_workload.setdefault(str(workload), []).append(action)
                seen_producer_keys.add((str(workload), str(key)))

    for action in actions:
        consumed_by_workload = _sum_capacity(list(action.get("consumesCapacity") or []))
        if not consumed_by_workload:
            continue
        deps = set(str(key) for key in list(action.get("dependsOnActionKeys") or []))
        gate = dict(action.get("capacityGate") or {})
        blocked: dict[str, float] = {}
        selected: dict[str, list[str]] = {}
        for workload, consumed_mu in consumed_by_workload.items():
            remaining = float(ready_capacity.get(workload, 0.0)) - float(consumed_mu)
            required_mu = float(required.get(workload, 0.0) or 0.0)
            if remaining + 1e-9 >= required_mu:
                continue
            needed = required_mu - remaining
            selected_keys = []
            producers = sorted(
                producers_by_workload.get(workload, []),
                key=lambda producer: _capacity_producer_sort_key(producer, dependency_context),
            )
            for producer in producers:
                if producer is action:
                    continue
                if _same_physical_gpu(action, producer) and not _same_physical_capacity_dependency_allowed(action, producer):
                    continue
                if _capacity_dependency_would_cycle(action, producer, dependency_context):
                    continue
                key = producer.get("actionKey")
                if key is None:
                    continue
                produced_mu = sum(float(item.get("mu", 0.0) or 0.0) for item in list(producer.get("producesCapacity") or []) if item.get("workload") == workload)
                if produced_mu <= 0.0:
                    continue
                deps.add(str(key))
                selected_keys.append(str(key))
                needed -= produced_mu
                if needed <= 1e-9:
                    break
            if needed > 1e-9:
                blocked[workload] = needed
            if selected_keys:
                selected[workload] = selected_keys
        if deps:
            action["dependsOnActionKeys"] = sorted(deps)
        if selected:
            gate["selectedProducerActionKeys"] = selected
            action["capacityGate"] = gate
        if blocked:
            action["blockedByCapacity"] = True
            action["capacityGate"] = {**gate, "blockedCapacityDeficit": blocked}


def _capacity_dependency_context(actions: list[dict[str, Any]]) -> dict[str, Any]:
    dag = build_phased_action_plan(actions, name="capacity-dependency-base")
    key_to_node: dict[str, str] = {}
    phase_by_key: dict[str, int] = {}
    index_by_key: dict[str, int] = {}
    dependents: dict[str, set[str]] = {}
    for node in list(dag.get("nodes", [])):
        node_id = str(node.get("id"))
        action = dict(node.get("action") or {})
        key = action.get("actionKey")
        if key is not None:
            key_to_node[str(key)] = node_id
            phase_by_key[str(key)] = int(node.get("phase", 0) or 0)
            index_by_key[str(key)] = int(node.get("index", 0) or 0)
        for dep in list(node.get("dependsOn") or []):
            dependents.setdefault(str(dep), set()).add(node_id)
    return {
        "keyToNode": key_to_node,
        "phaseByKey": phase_by_key,
        "indexByKey": index_by_key,
        "dependents": dependents,
    }


def _capacity_producer_sort_key(producer: dict[str, Any], context: dict[str, Any]) -> tuple[int, int, float]:
    key = str(producer.get("actionKey"))
    produced_mu = sum(float(item.get("mu", 0.0) or 0.0) for item in list(producer.get("producesCapacity") or []))
    return (
        int(dict(context.get("phaseByKey") or {}).get(key, 1_000_000)),
        int(dict(context.get("indexByKey") or {}).get(key, 1_000_000)),
        -produced_mu,
    )


def _same_physical_gpu(left: dict[str, Any], right: dict[str, Any]) -> bool:
    left_physical = left.get("physical_gpu_id")
    right_physical = right.get("physical_gpu_id")
    if left_physical is None or right_physical is None:
        return False
    return str(left_physical) == str(right_physical)


def _same_physical_capacity_dependency_allowed(consumer: dict[str, Any], producer: dict[str, Any]) -> bool:
    """Same-GPU capacity can only protect partial-compatible transitions.

    Full in-place reconfiguration cannot use capacity produced on the same
    physical GPU to justify deleting the old side: the old side must be gone
    before that new capacity exists. Partial reconfiguration is different
    because preserved slots and locally-created slots can coexist after the
    partial patch, as long as the dependency does not form a structural cycle.
    """

    return _partial_capacity_context(consumer) and _partial_capacity_context(producer)


def _partial_capacity_context(action: dict[str, Any]) -> bool:
    if action.get("partial") or action.get("preservedSlotUpdate") or action.get("siblingOfPartialReconfiguration"):
        return True
    mode = str(action.get("transitionMode") or "")
    root = str(action.get("abstractRoot") or "")
    context_root = str(action.get("partialContextRoot") or "")
    return (
        mode == "partial_reconfiguration"
        or mode.startswith("preserved_slot_")
        or root.startswith("PARTIAL_RECONF_")
        or root.startswith("PRESERVE_SLOT_")
        or context_root.startswith("PARTIAL_RECONF_")
    )


def _capacity_dependency_would_cycle(
    consumer: dict[str, Any],
    producer: dict[str, Any],
    context: dict[str, Any],
) -> bool:
    consumer_key = consumer.get("actionKey")
    producer_key = producer.get("actionKey")
    if consumer_key is None or producer_key is None:
        return False
    key_to_node = dict(context.get("keyToNode") or {})
    consumer_node = key_to_node.get(str(consumer_key))
    producer_node = key_to_node.get(str(producer_key))
    if consumer_node is None or producer_node is None:
        return False
    if consumer_node == producer_node:
        return True
    return _node_reaches(consumer_node, producer_node, context)


def _node_reaches(src_node: str, dst_node: str, context: dict[str, Any]) -> bool:
    dependents = dict(context.get("dependents") or {})
    seen: set[str] = set()
    stack = [src_node]
    while stack:
        node = stack.pop()
        for nxt in sorted(dependents.get(node, set())):
            if nxt == dst_node:
                return True
            if nxt in seen:
                continue
            seen.add(nxt)
            stack.append(nxt)
    return False


def _add_physical_reuse_dependency_edges(actions: list[dict[str, Any]]) -> None:
    pending_release_key: str | None = None
    for action in actions:
        effect = dict(action.get("physicalGpuEffect") or {})
        if effect.get("type") == "release" and action.get("actionKey") is not None:
            pending_release_key = str(action["actionKey"])
            continue
        if effect.get("type") != "acquire" or pending_release_key is None:
            continue
        if action.get("capacityUrgent"):
            continue
        deps = set(str(key) for key in list(action.get("dependsOnActionKeys") or []))
        deps.add(pending_release_key)
        action["dependsOnActionKeys"] = sorted(deps)
        action["physicalGpuEffect"] = {**effect, "reuseDependencyActionKey": pending_release_key}
        pending_release_key = None


def _effects_for_action(
    action: dict[str, Any],
    source_map: dict[int, GPUState],
    target_map: dict[int, GPUState],
    required: dict[str, float],
) -> dict[str, Any]:
    action_type = str(action.get("type", ""))
    out: dict[str, Any] = {}
    if action_type == "allocate_gpu":
        out["physicalGpuEffect"] = {"type": "acquire", "physicalGpuId": action.get("physical_gpu_id")}
    elif action_type == "return_gpu":
        out["physicalGpuEffect"] = {"type": "release", "physicalGpuId": action.get("physical_gpu_id")}
    elif action_type in {"configure_full_template", "configure_partial_profile"}:
        out["migEffect"] = {"type": "create_mig_slot", "template": action.get("template")}
    elif action_type == "clear_template":
        out["migEffect"] = {"type": "delete_mig_slot", "template": action.get("template")}
    elif action_type == "stop_accepting_new":
        consumed = _capacity_for_action_source(action, source_map)
        if consumed:
            out["consumesCapacity"] = consumed
            out["capacityGate"] = _capacity_gate(consumed, required)
        out["routeEffect"] = {
            "type": "deactivate_route",
            "routerQueueRedispatch": bool(action.get("routerQueueRedispatch")),
        }
    elif action_type == "stop_gpu_traffic":
        consumed = _capacity_for_action_source(action, source_map)
        if consumed:
            out["consumesCapacity"] = consumed
            out["capacityGate"] = _capacity_gate(consumed, required)
        out["routeEffect"] = {"type": "deactivate_route"}
    elif action_type == "activate_serving_route":
        produced = _capacity_for_action_target(action, target_map)
        if produced:
            out["producesCapacity"] = produced
            out["routeEffect"] = {"type": "activate_route"}
    elif action_type.startswith("defer_"):
        out["blockedByCapacity"] = bool(action.get("blockedByCapacity", True))
        out["requiredRate"] = dict(required)
    return out


def _candidate(
    mode: str,
    rank: int,
    feasible: bool,
    actions: list[dict[str, Any]],
    plan_items: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "mode": mode,
        "rank": rank,
        "feasible": bool(feasible),
        "actions": actions,
        "plan_items": plan_items,
        "actionTypes": [str(action.get("type")) for action in actions],
    }


def _candidate_decision(decision_type: str, gpu_id: int, chosen_mode: str, candidates: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "type": decision_type,
        "gpuId": gpu_id,
        "chosenMode": chosen_mode,
        "candidates": [
            {
                "mode": candidate["mode"],
                "chosen": candidate["mode"] == chosen_mode,
                "feasible": candidate["feasible"],
                "rank": candidate["rank"],
                "actionTypes": candidate["actionTypes"],
            }
            for candidate in candidates
        ],
    }


def _fixed_decision(decision_type: str, gpu_id: int, actions: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "type": decision_type,
        "gpuId": gpu_id,
        "chosenMode": decision_type,
        "candidates": [
            {
                "mode": decision_type,
                "chosen": True,
                "feasible": True,
                "rank": 0,
                "actionTypes": [str(action.get("type")) for action in actions],
            }
        ],
    }


def _capacity_for_action_target(action: dict[str, Any], target_map: dict[int, GPUState]) -> list[dict[str, Any]]:
    gpu_id = action.get("gpu_id")
    if gpu_id is None:
        return []
    gpu = target_map.get(int(gpu_id))
    if gpu is None:
        return []
    slots = _action_slots(action)
    instances = _instances_for_slots(gpu, slots) if slots else list(gpu.instances)
    return [_capacity_record(inst) for inst in instances if inst.workload is not None]


def _capacity_for_action_source(action: dict[str, Any], source_map: dict[int, GPUState]) -> list[dict[str, Any]]:
    gpu_id = action.get("gpu_id")
    if gpu_id is None:
        return []
    gpu = source_map.get(int(gpu_id))
    if gpu is None:
        return []
    slots = _action_slots(action)
    instances = _instances_for_slots(gpu, slots) if slots else list(gpu.instances)
    return [_capacity_record(inst) for inst in instances if inst.workload is not None]


def _instances_for_slots(gpu: GPUState, slots: list[tuple[int, int, str]]) -> list[MigInstance]:
    return [inst for slot in slots if (inst := get_inst_by_slot(gpu, slot)) is not None]


def _action_slots(action: dict[str, Any]) -> list[tuple[int, int, str]]:
    raw_slots = action.get("slots")
    if raw_slots is None and action.get("slot") is not None:
        raw_slots = [action.get("slot")]
    out = []
    for slot in list(raw_slots or []):
        if isinstance(slot, (list, tuple)) and len(slot) >= 3:
            out.append((int(slot[0]), int(slot[1]), str(slot[2])))
    return out


def _capacity_record(inst: MigInstance) -> dict[str, Any]:
    return {
        "workload": inst.workload,
        "mu": float(inst.mu),
        "slot": [int(inst.start), int(inst.end), str(inst.profile)],
    }


def _capacity_gate(consumed: list[dict[str, Any]], required: dict[str, float]) -> dict[str, Any]:
    return {
        "requiredRate": dict(required),
        "consumedByWorkload": _sum_capacity(consumed),
        "policy": "ready_capacity_after_removal_must_cover_required_rate",
    }


def _sum_capacity(records: list[dict[str, Any]]) -> dict[str, float]:
    out: dict[str, float] = {}
    for record in records:
        workload = record.get("workload")
        if workload is None:
            continue
        out[str(workload)] = out.get(str(workload), 0.0) + float(record.get("mu", 0.0) or 0.0)
    return out


def _action_key(index: int, action: dict[str, Any]) -> str:
    action_type = str(action.get("type", "action"))
    root = str(action.get("abstractRoot", "global"))
    return f"{index:04d}:{root}:{action_type}"
