from __future__ import annotations

import time
from typing import Any

from .internal.partial_reconfig import build_partial_reconfig_plan
from ..physical_ids import PHYSICAL_ID_POOL, bootstrap_physical_ids_for_state, ensure_state_metadata, get_physical_id
from ..state import PROFILE_SIZE, ClusterState, GPUState, MigInstance, deepcopy_state, get_inst_by_slot, gpu_map_by_id
from .internal.state_diff import (
    alloc_from_free_pool,
    classify_gpu_change,
    diff_instances_within_same_template,
    matches_target_state,
    provided_by_workload,
    safe_after_removing_gpu,
    safe_after_removing_instance,
)
from .internal.action_simulator import prepare_transition_runtime, required_arrival_dict, simulate_transition_actions
from .internal import action_builder
from .internal.dag_format import build_phased_action_plan, compact_phased_action_plan


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
    transition_demand_policy: str = "min",
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

    required = required_arrival_dict(
        src_arrival,
        tgt_arrival,
        workload_names=workload_names,
        policy=transition_demand_policy,
    )
    actions, plan_items, decision_trace = _build_effect_aware_actions(
        source_state=current_state,
        target_state=target_state,
        required=required,
    )
    actions = _prepare_capacity_safe_actions(actions, plan_items, current_state, target_state, required)
    _assert_executable_actions(actions)
    _add_physical_reuse_dependency_edges(actions)
    actions = action_builder._preserve_independent_slot_deletes(actions)
    action_builder._assert_reroute_destinations_stable(current_state, target_state, actions)
    planned_state = action_builder._planned_state_for_actions(current_state, target_state, actions)
    executed_state = simulate_transition_actions(
        source_state=current_state,
        target_state=planned_state,
        fine_actions=actions,
        next_physical_idx=current_state.metadata.get("next_physical_idx", 0),
    )
    executed_state = action_builder._drop_available_physical_gpus(executed_state)
    dag = build_phased_action_plan(actions, plan_items=plan_items, name=f"{stage_name}-final")
    peak_active_gpu = action_builder._peak_serving_gpu_from_actions(current_state, actions)
    reached_target = matches_target_state(executed_state, target_state)
    final_plan = {
        "stage_name": stage_name,
        "required": required,
        "fine_actions": actions,
        "executed_actions": actions,
        "blocked_actions": [],
        "planned_state": planned_state,
        "executed_state": executed_state,
        "plan_items": plan_items,
        "planner_objective_order": [
            "active service capacity must cover the committed demand for every workload",
            "physical GPU availability and reuse constraints",
            "prefer feasible partial reconfiguration",
            "prefer feasible in-place reconfiguration",
            "use bridge reconfiguration only when target-side capacity must be built first",
        ],
        "runtime_assumptions": {
            "defaultQueued": int(default_queued),
            "defaultInflight": int(default_inflight),
            "overrideExistingChangedSlots": bool(override_existing_runtime_for_changed_slots),
            "transitionDemandPolicy": str(transition_demand_policy),
            "committedDemandPolicy": "component-wise min(source demand, target demand)",
        },
        "effect_model": {
            "capacity": "producesCapacity/consumesCapacity annotate route activation and serving removal",
            "router": "deactivate_instance_route owns router queue redispatch when routerQueueRedispatch=true",
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
        "source_active_gpu": len(action_builder._active_serving_pid_set(current_state)),
        "final_active_gpu": len(action_builder._active_serving_pid_set(executed_state)),
        "final_plan": final_plan,
        "phased_action_plan": dag,
        "phased_action_plan_summary": compact_phased_action_plan(dag),
        "transition_planner_module": NAME,
        "max_iters_ignored": max_iters,
        "transition_demand_policy": str(transition_demand_policy),
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
        if not action_builder._is_available_physical_gpu(gpu)
    }
    tgt_map = gpu_map_by_id(target_state)
    all_gpu_ids = sorted(set(src_map) | set(tgt_map))
    free_pool = action_builder._build_initial_available_pool(source_state, src_map)
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
        action_builder._append_delete_gpu_actions(actions, plan_items, source_state, target_state, gpu_id, required)
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
            decisions.append(_candidate_decision("reconfiguration", gpu_id, "blocked", candidates))
            raise RuntimeError(
                f"stage3 failed to build an executable reconfiguration DAG for gpu {gpu_id}: "
                "no feasible reconfiguration candidate"
            )
        chosen = min(feasible, key=lambda candidate: candidate["rank"])
        actions.extend(chosen["actions"])
        plan_items.extend(chosen["plan_items"])
        decisions.append(_candidate_decision("reconfiguration", gpu_id, chosen["mode"], candidates))
        if chosen["mode"] == "bridge_reconfiguration":
            alloc_from_free_pool(free_pool)
            if old_physical_id is not None:
                # Prefer never-used/previously-idle GPUs for later bridges.
                # A just-released GPU may still be the source side for capacity
                # handoff dependencies until its old routes are drained.
                free_pool.insert(0, old_physical_id)

    for gpu_id in [gpu_id for gpu_id, kind in classified.items() if kind == "create_gpu"]:
        before = len(actions)
        physical_id = alloc_from_free_pool(free_pool)
        action_builder._append_create_target_gpu_actions(actions, plan_items, gpu_id, physical_id, tgt_map[gpu_id])
        decisions.append(_fixed_decision("create_gpu", gpu_id, actions[before:]))

    return actions, plan_items, decisions


def _prepare_capacity_safe_actions(
    actions: list[dict[str, Any]],
    plan_items: list[dict[str, Any]],
    source_state: ClusterState,
    target_state: ClusterState,
    required: dict[str, float],
) -> list[dict[str, Any]]:
    for _ in range(12):
        annotated = _annotate_effects(actions, source_state, target_state, required)
        _add_capacity_dependency_edges(annotated, source_state, required)
        blocked = [action for action in annotated if bool(action.get("blockedByCapacity"))]
        dag = build_phased_action_plan(annotated, plan_items=plan_items, name="capacity-precheck")
        cycle_actions = _cycle_capacity_consumers(annotated, dag)
        if not blocked and not cycle_actions:
            return annotated
        added = _append_temporary_capacity_actions(
            actions,
            plan_items,
            source_state,
            target_state,
            blocked + cycle_actions,
        )
        if not added:
            return annotated
    annotated = _annotate_effects(actions, source_state, target_state, required)
    _add_capacity_dependency_edges(annotated, source_state, required)
    return annotated


def _cycle_capacity_consumers(actions: list[dict[str, Any]], dag: dict[str, Any]) -> list[dict[str, Any]]:
    by_key = {str(action.get("actionKey")): action for action in actions if action.get("actionKey") is not None}
    out: list[dict[str, Any]] = []
    for phase in list(dag.get("phases", [])):
        if not phase.get("warning"):
            continue
        representative: dict[str, Any] | None = None
        for node_id in list(phase.get("nodeIds", [])):
            node = next((item for item in dag.get("nodes", []) if str(item.get("id")) == str(node_id)), None)
            if node is None:
                continue
            action = dict(node.get("action") or {})
            key = action.get("actionKey")
            original = by_key.get(str(key)) if key is not None else None
            if original is None or not original.get("consumesCapacity"):
                continue
            if original.get("cleanupTemporaryCapacity"):
                continue
            representative = original
            break
        if representative is not None:
            out.append(representative)
    return out


def _append_temporary_capacity_actions(
    actions: list[dict[str, Any]],
    plan_items: list[dict[str, Any]],
    source_state: ClusterState,
    target_state: ClusterState,
    consumers: list[dict[str, Any]],
) -> bool:
    added = False
    existing_temp = {
        (str(action.get("protectsWorkload") or action.get("workload")), tuple(action.get("protectsSlot") or ()))
        for action in actions
        if action.get("temporaryCapacity") and action.get("type") == "activate_instance_route"
    }
    occupied_temp_slots = {
        (int(action["gpu_id"]), tuple(action.get("slot") or ()))
        for action in actions
        if action.get("temporaryCapacity") and action.get("slot") is not None and action.get("gpu_id") is not None
    }
    occupied_temp_gpu_ids = {
        int(action["gpu_id"])
        for action in actions
        if action.get("temporaryCapacity") and action.get("gpu_id") is not None
    }
    occupied_temp_physical_ids = {
        str(action["physical_gpu_id"])
        for action in actions
        if action.get("temporaryCapacity") and action.get("physical_gpu_id") is not None
    }
    for consumer in consumers:
        for record in list(consumer.get("consumesCapacity") or []):
            workload = record.get("workload")
            if workload is None:
                continue
            source_inst = _source_instance_for_capacity_record(source_state, consumer, record)
            if source_inst is None:
                continue
            if (str(workload), (source_inst.start, source_inst.end, source_inst.profile)) in existing_temp:
                continue
            temp = _temporary_capacity_target(
                source_state,
                target_state,
                source_inst,
                occupied_temp_slots=occupied_temp_slots,
                occupied_temp_gpu_ids=occupied_temp_gpu_ids,
                occupied_temp_physical_ids=occupied_temp_physical_ids,
            )
            if temp is None:
                continue
            temp_actions, temp_item = _temporary_capacity_actions(temp, source_inst)
            actions.extend(temp_actions)
            plan_items.append(temp_item)
            existing_temp.add((str(workload), (source_inst.start, source_inst.end, source_inst.profile)))
            occupied_temp_slots.add((int(temp["gpu_id"]), tuple(temp["slot"])))
            occupied_temp_gpu_ids.add(int(temp["gpu_id"]))
            occupied_temp_physical_ids.add(str(temp["physical_gpu_id"]))
            added = True
    return added


def _source_instance_for_capacity_record(
    source_state: ClusterState,
    consumer: dict[str, Any],
    record: dict[str, Any],
) -> MigInstance | None:
    gpu_id = consumer.get("gpu_id")
    if gpu_id is None:
        return None
    slots = _action_slots(consumer)
    if not slots and isinstance(record.get("slot"), (list, tuple)):
        raw = record["slot"]
        slots = [(int(raw[0]), int(raw[1]), str(raw[2]))]
    gpu = gpu_map_by_id(source_state).get(int(gpu_id))
    if gpu is None:
        return None
    for slot in slots:
        inst = get_inst_by_slot(gpu, slot)
        if inst is not None and inst.workload == record.get("workload"):
            return inst
    return None


def _temporary_capacity_target(
    source_state: ClusterState,
    target_state: ClusterState,
    source_inst: MigInstance,
    *,
    occupied_temp_slots: set[tuple[int, tuple[Any, ...]]] | None = None,
    occupied_temp_gpu_ids: set[int] | None = None,
    occupied_temp_physical_ids: set[str] | None = None,
) -> dict[str, Any] | None:
    void_slot = _find_void_temp_slot(source_state, target_state, source_inst, occupied_temp_slots or set())
    if void_slot is not None:
        return void_slot
    return _new_temp_gpu_target(
        source_state,
        target_state,
        source_inst,
        occupied_temp_gpu_ids=occupied_temp_gpu_ids or set(),
        occupied_temp_physical_ids=occupied_temp_physical_ids or set(),
    )


def _find_void_temp_slot(
    source_state: ClusterState,
    target_state: ClusterState,
    source_inst: MigInstance,
    occupied_temp_slots: set[tuple[int, tuple[Any, ...]]],
) -> dict[str, Any] | None:
    size = PROFILE_SIZE.get(source_inst.profile, source_inst.end - source_inst.start)
    target_map = gpu_map_by_id(target_state)
    for gpu in source_state.real_gpus():
        target_gpu = target_map.get(int(gpu.gpu_id))
        physical_id = get_physical_id(source_state, int(gpu.gpu_id))
        if physical_id is None:
            continue
        for inst in gpu.instances:
            if inst.profile != "void":
                continue
            if int(inst.end) - int(inst.start) < size:
                continue
            start = int(inst.start)
            slot = (start, start + size, source_inst.profile)
            if (int(gpu.gpu_id), tuple(slot)) in occupied_temp_slots:
                continue
            if target_gpu is not None:
                target_inst = get_inst_by_slot(target_gpu, slot)
                if target_inst is not None and target_inst.workload is not None:
                    continue
            return {
                "kind": "void_slot",
                "gpu_id": int(gpu.gpu_id),
                "physical_gpu_id": physical_id,
                "slot": slot,
            }
    return None


def _new_temp_gpu_target(
    source_state: ClusterState,
    target_state: ClusterState,
    source_inst: MigInstance,
    *,
    occupied_temp_gpu_ids: set[int],
    occupied_temp_physical_ids: set[str],
) -> dict[str, Any] | None:
    src_map = gpu_map_by_id(source_state)
    target_map = gpu_map_by_id(target_state)
    active_pids = {
        str(get_physical_id(source_state, gpu_id))
        for gpu_id in src_map
        if get_physical_id(source_state, gpu_id) is not None
    }
    free_pool = [
        str(pid)
        for pid in source_state.metadata.get("free_physical_gpu_pool", [])
        if pid is not None and str(pid) not in active_pids and str(pid) not in occupied_temp_physical_ids
    ]
    never_seen = [pid for pid in PHYSICAL_ID_POOL if pid not in active_pids and pid not in free_pool and pid not in occupied_temp_physical_ids]
    candidates = list(free_pool) + never_seen
    if not candidates:
        return None
    used_gpu_ids = set(src_map) | set(target_map) | set(occupied_temp_gpu_ids)
    temp_gpu_id = max(used_gpu_ids | {0}) + 1000
    while temp_gpu_id in used_gpu_ids:
        temp_gpu_id += 1
    return {
        "kind": "temp_gpu",
        "gpu_id": temp_gpu_id,
        "physical_gpu_id": candidates[0],
        "slot": (0, PROFILE_SIZE.get(source_inst.profile, source_inst.end - source_inst.start), source_inst.profile),
    }


def _temporary_capacity_actions(temp: dict[str, Any], source_inst: MigInstance) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    gpu_id = int(temp["gpu_id"])
    physical_id = str(temp["physical_gpu_id"])
    slot = tuple(temp["slot"])
    root = f"TEMP_CAPACITY_gpu{gpu_id}_{slot[0]}_{slot[1]}_{slot[2]}_{source_inst.workload}"
    common = {
        "transitionMode": "bridge_workload_replacement",
        "abstractRoot": root,
        "temporaryCapacity": True,
        "purpose": "bridge_workload_replacement",
        "protectsWorkload": source_inst.workload,
        "protectsSlot": [source_inst.start, source_inst.end, source_inst.profile],
    }
    instance = {
        "gpu_id": gpu_id,
        "start": int(slot[0]),
        "end": int(slot[1]),
        "profile": str(slot[2]),
        "workload": source_inst.workload,
        "batch": source_inst.batch,
        "mu": float(source_inst.mu),
    }
    actions: list[dict[str, Any]] = []
    if temp["kind"] == "temp_gpu":
        target_gpu = GPUState(gpu_id=gpu_id, instances=[MigInstance(int(slot[0]), int(slot[1]), str(slot[2]), source_inst.workload, source_inst.batch, float(source_inst.mu))])
        actions.extend(
            [
                action_builder._action("allocate_gpu", gpu_id=gpu_id, physical_gpu_id=physical_id, logical_gpu_id=gpu_id, pendingLogicalGpuId=gpu_id, **common),
                action_builder._action("configure_full_template", gpu_id=gpu_id, physical_gpu_id=physical_id, logical_gpu_id=gpu_id, pendingLogicalGpuId=gpu_id, template=target_gpu.template_str(), createSpec=action_builder._gpu_create_spec(target_gpu), **common),
                action_builder._action("bind_target_gpu", gpu_id=gpu_id, physical_gpu_id=physical_id, logical_gpu_id=gpu_id, activeLogicalGpuId=gpu_id, clearsPendingLogicalGpuId=True, **common),
            ]
        )
    else:
        actions.append(
            action_builder._action(
                "configure_partial_profile",
                gpu_id=gpu_id,
                physical_gpu_id=physical_id,
                logical_gpu_id=gpu_id,
                template=str(PROFILE_SIZE.get(str(slot[2]), int(slot[1]) - int(slot[0]))),
                createSlots=[list(slot)],
                deleteSlots=[],
                preserveSlots=[],
                **common,
            )
        )
    actions.extend(
        [
            action_builder._action("register_mig_devices", gpu_id=gpu_id, physical_gpu_id=physical_id, slots=[list(slot)], **common),
            action_builder._action("place_instance", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=source_inst.workload, batch=source_inst.batch, instance=instance, **common),
            action_builder._action("activate_instance_route", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=source_inst.workload, instance=instance, mu=float(source_inst.mu), **common),
        ]
    )
    cleanup_common = {**common, "cleanupTemporaryCapacity": True}
    actions.extend(
        [
            action_builder._action(
                "deactivate_instance_route",
                gpu_id=gpu_id,
                physical_gpu_id=physical_id,
                slot=slot,
                workload=source_inst.workload,
                instance=instance,
                mu=float(source_inst.mu),
                **cleanup_common,
            ),
            action_builder._action("wait_instance_drain", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=source_inst.workload, rounds=1, **cleanup_common),
            action_builder._action("delete_instance", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=source_inst.workload, **cleanup_common),
            action_builder._action("clear_template", gpu_id=gpu_id, physical_gpu_id=physical_id, template=str(PROFILE_SIZE.get(str(slot[2]), int(slot[1]) - int(slot[0]))), deleteSlots=[list(slot)], slotCount=1, **cleanup_common),
        ]
    )
    if temp["kind"] == "temp_gpu":
        actions.extend(
            [
                action_builder._action("clear_gpu_binding", gpu_id=gpu_id, physical_gpu_id=physical_id, logical_gpu_id=gpu_id, clearsActiveLogicalGpuId=True, slots=[list(slot)], **cleanup_common),
                action_builder._action("return_gpu", gpu_id=gpu_id, physical_gpu_id=physical_id, clearsPendingLogicalGpuId=True, **cleanup_common),
            ]
        )
    return actions, action_builder._plan_item(root, "bridge_workload_replacement", gpu_id, physical_id, slot=slot, workload=source_inst.workload)


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
        if inst_action["type"] == "keep":
            continue
        slot = inst_action.get("slot")
        if slot is None:
            continue
        root = f"SLOT_gpu{gpu_id}_{slot[0]}_{slot[1]}_{slot[2]}"
        common = {"transitionMode": inst_action["type"], "abstractRoot": root}
        change_type = inst_action["type"]
        if change_type == "batch_change":
            src = inst_action["src"]
            tgt = inst_action["tgt"]
            actions.extend(
                [
                    action_builder._action("patch_batch_config", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, old_batch=src.batch, new_batch=tgt.batch, workload=src.workload, **common),
                    action_builder._action("apply_batch", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, old_batch=src.batch, new_batch=tgt.batch, workload=src.workload, **common),
                    action_builder._action("verify_batch", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, old_batch=src.batch, new_batch=tgt.batch, workload=src.workload, **common),
                    action_builder._action("activate_instance_route", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=src.workload, **common),
                ]
            )
            plan_items.append(action_builder._plan_item(root, "batch_update", gpu_id, physical_id, slot=slot, workload=src.workload))
            continue
        if change_type == "place_instance":
            tgt = inst_action["tgt"]
            actions.extend(
                [
                    action_builder._action("place_instance", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=tgt.workload, batch=tgt.batch, **common),
                    action_builder._action("activate_instance_route", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=tgt.workload, **common),
                ]
            )
            plan_items.append(action_builder._plan_item(root, "place_instance", gpu_id, physical_id, slot=slot, workload=tgt.workload))
            continue
        if change_type == "safe_delete_instance":
            src = inst_action["src"]
            actions.extend(action_builder._queue_and_drain_actions(source_state, target_state, gpu_id, physical_id, src, required, common))
            actions.append(
                action_builder._action("delete_instance", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=src.workload, **common)
            )
            plan_items.append(action_builder._plan_item(root, "delete_instance", gpu_id, physical_id, slot=slot, workload=src.workload))
            continue
        if change_type == "workload_change":
            src = inst_action["src"]
            tgt = inst_action["tgt"]
            common = {"transitionMode": "workload_replacement", "abstractRoot": root}
            actions.extend(action_builder._queue_and_drain_actions(source_state, target_state, gpu_id, physical_id, src, required, common))
            actions.extend(
                [
                    action_builder._action("delete_instance", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=src.workload, **common),
                    action_builder._action("place_instance", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=tgt.workload, batch=tgt.batch, **common),
                    action_builder._action("activate_instance_route", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=tgt.workload, **common),
                ]
            )
            plan_items.append(action_builder._plan_item(root, "workload_replacement", gpu_id, physical_id, slot=slot, workload=src.workload))


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
        action_builder._append_partial_reconfiguration_actions(
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
    action_builder._append_in_place_reconfiguration_actions(
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
    create_spec = action_builder._gpu_create_spec(target_gpu)
    root = f"BRIDGE_RECONF_gpu{gpu_id}"
    common = {"transitionMode": "bridge_reconfiguration", "abstractRoot": root}
    slots = [(inst.start, inst.end, inst.profile) for inst in action_builder._nonfree_instances(src_gpu)]
    actions.extend(
        [
            action_builder._action(
                "allocate_gpu",
                gpu_id=gpu_id,
                physical_gpu_id=new_physical_id,
                logical_gpu_id=gpu_id,
                pendingLogicalGpuId=gpu_id,
                **common,
            ),
            action_builder._action(
                "configure_full_template",
                gpu_id=gpu_id,
                physical_gpu_id=new_physical_id,
                logical_gpu_id=gpu_id,
                pendingLogicalGpuId=gpu_id,
                template=template,
                createSpec=create_spec,
                **common,
            ),
            action_builder._action(
                "bind_target_gpu",
                gpu_id=gpu_id,
                physical_gpu_id=new_physical_id,
                logical_gpu_id=gpu_id,
                activeLogicalGpuId=gpu_id,
                clearsPendingLogicalGpuId=True,
                **common,
            ),
            *action_builder._tag_actions(action_builder._target_activation_actions(gpu_id, new_physical_id, target_gpu), common),
        ]
    )
    for inst in action_builder._nonfree_instances(src_gpu):
        actions.extend(
            action_builder._queue_and_drain_actions(
                source_state,
                target_state,
                gpu_id,
                old_physical_id,
                inst,
                {},
                common,
                stop_new=True,
                exclude_entire_gpu=True,
            )
        )
    actions.extend(
        [
            *[
                action_builder._action(
                    "delete_instance",
                    gpu_id=gpu_id,
                    physical_gpu_id=old_physical_id,
                    slot=(inst.start, inst.end, inst.profile),
                    workload=inst.workload,
                    **common,
                )
                for inst in action_builder._nonfree_instances(src_gpu)
            ],
            action_builder._action(
                "clear_gpu_binding",
                gpu_id=gpu_id,
                physical_gpu_id=old_physical_id,
                logical_gpu_id=gpu_id,
                pendingLogicalGpuId=gpu_id,
                clearsActiveLogicalGpuId=True,
                slots=slots,
                **common,
            ),
            action_builder._action(
                "clear_template",
                gpu_id=gpu_id,
                physical_gpu_id=old_physical_id,
                template=src_gpu.template_str(),
                deleteSlots=[list(slot) for slot in slots],
                slotCount=len(slots),
                **common,
            ),
            action_builder._action(
                "return_gpu",
                gpu_id=gpu_id,
                physical_gpu_id=old_physical_id,
                clearsPendingLogicalGpuId=True,
                **common,
            ),
        ]
    )
    plan_items.append(
        action_builder._plan_item(
            root,
            "bridge_reconfiguration",
            gpu_id,
            old_physical_id,
            target_physical_gpu_id=new_physical_id,
        )
    )


def _partial_effect_feasible(source_state: ClusterState, src_gpu: GPUState, partial_plan: Any, required: dict[str, float]) -> bool:
    if action_builder._partial_reconfiguration_capacity_safe(source_state, src_gpu, partial_plan, required):
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
                    action_builder._action("patch_batch_config", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, old_batch=src.batch, new_batch=tgt.batch, workload=src.workload, **common),
                    action_builder._action("apply_batch", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, old_batch=src.batch, new_batch=tgt.batch, workload=src.workload, **common),
                    action_builder._action("verify_batch", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, old_batch=src.batch, new_batch=tgt.batch, workload=src.workload, **common),
                    action_builder._action("activate_instance_route", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=src.workload, **common),
                ]
            )
            plan_items.append(action_builder._plan_item(root, "preserved_slot_batch_update", gpu_id, physical_id, slot=slot, workload=src.workload))
            continue
        if change_type == "place_instance":
            tgt = inst_action["tgt"]
            actions.extend(
                [
                    action_builder._action("place_instance", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=tgt.workload, batch=tgt.batch, **common),
                    action_builder._action("activate_instance_route", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=tgt.workload, **common),
                ]
            )
            plan_items.append(action_builder._plan_item(root, "preserved_slot_place_instance", gpu_id, physical_id, slot=slot, workload=tgt.workload))
            continue
        if change_type == "safe_delete_instance":
            src = inst_action["src"]
            actions.extend(action_builder._queue_and_drain_actions(source_state, target_state, gpu_id, physical_id, src, required, common))
            actions.append(action_builder._action("delete_instance", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=src.workload, **common))
            plan_items.append(action_builder._plan_item(root, "preserved_slot_delete_instance", gpu_id, physical_id, slot=slot, workload=src.workload))
            continue
        if change_type == "workload_change":
            src = inst_action["src"]
            tgt = inst_action["tgt"]
            change_common = {**common, "transitionMode": "workload_change"}
            actions.extend(action_builder._queue_and_drain_actions(source_state, target_state, gpu_id, physical_id, src, required, common))
            actions.extend(
                [
                    action_builder._action("delete_instance", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=src.workload, **change_common),
                    action_builder._action("place_instance", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=tgt.workload, batch=tgt.batch, **change_common),
                    action_builder._action("activate_instance_route", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=tgt.workload, **change_common),
                ]
            )
            plan_items.append(action_builder._plan_item(root, "preserved_slot_workload_replacement", gpu_id, physical_id, slot=slot, workload=src.workload))


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
            workload_producers = producers_by_workload.get(workload, [])
            prefer_temporary = (
                not action.get("cleanupTemporaryCapacity")
                and any(bool(producer.get("temporaryCapacity")) for producer in workload_producers)
            )
            producers = sorted(
                workload_producers,
                key=lambda producer: _capacity_producer_sort_key(producer, dependency_context, prefer_temporary=prefer_temporary),
            )
            for producer in producers:
                if producer is action:
                    continue
                if action.get("cleanupTemporaryCapacity") and producer.get("temporaryCapacity"):
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
        if blocked and not action.get("cleanupTemporaryCapacity"):
            action["blockedByCapacity"] = True
            action["capacityGate"] = {**gate, "blockedCapacityDeficit": blocked}
    _add_cumulative_capacity_dependency_edges(actions, source_state, required)
    _add_temporary_cleanup_dependency_edges(actions)


def _add_cumulative_capacity_dependency_edges(
    actions: list[dict[str, Any]],
    source_state: ClusterState,
    required: dict[str, float],
) -> None:
    virtual_capacity = {str(workload): float(value) for workload, value in provided_by_workload(source_state).items()}
    dependency_context = _capacity_dependency_context(actions)
    producers_by_workload: dict[str, list[dict[str, Any]]] = {}
    for action in actions:
        for produced in list(action.get("producesCapacity") or []):
            workload = produced.get("workload")
            key = action.get("actionKey")
            if workload is None or key is None:
                continue
            producers_by_workload.setdefault(str(workload), []).append(action)

    selected_producers: dict[str, set[str]] = {}
    selected_capacity: dict[str, dict[str, float]] = {}
    for action in actions:
        consumed_by_workload = _sum_capacity(list(action.get("consumesCapacity") or []))
        if not consumed_by_workload:
            continue
        deps = set(str(key) for key in list(action.get("dependsOnActionKeys") or []))
        gate = dict(action.get("capacityGate") or {})
        selected = dict(gate.get("selectedProducerActionKeys") or {})
        blocked: dict[str, float] = {}
        for workload, consumed_mu in consumed_by_workload.items():
            workload = str(workload)
            inherited_producer_keys = set() if action.get("cleanupTemporaryCapacity") else set(selected_producers.get(workload, set()))
            if inherited_producer_keys:
                selected.setdefault(workload, [])
                for key in sorted(inherited_producer_keys):
                    deps.add(key)
                    if key not in selected[workload]:
                        selected[workload].append(key)
            needed = float(required.get(workload, 0.0) or 0.0) - (virtual_capacity.get(workload, 0.0) - float(consumed_mu))
            if needed > 1e-9:
                selected.setdefault(workload, [])
                prefer_temporary = any(bool(producer.get("temporaryCapacity")) for producer in producers_by_workload.get(workload, []))
                for producer in sorted(
                    producers_by_workload.get(workload, []),
                    key=lambda item: _capacity_producer_sort_key(item, dependency_context, prefer_temporary=prefer_temporary),
                ):
                    key = producer.get("actionKey")
                    if key is None:
                        continue
                    key = str(key)
                    if key in selected_producers.get(workload, set()):
                        deps.add(key)
                        if key not in selected[workload]:
                            selected[workload].append(key)
                        if needed <= 1e-9:
                            break
                        continue
                    if producer is action:
                        continue
                    if action.get("cleanupTemporaryCapacity") and producer.get("temporaryCapacity"):
                        continue
                    if _same_physical_gpu(action, producer) and not _same_physical_capacity_dependency_allowed(action, producer):
                        continue
                    if _capacity_dependency_would_cycle(action, producer, dependency_context):
                        continue
                    produced_mu = sum(
                        float(item.get("mu", 0.0) or 0.0)
                        for item in list(producer.get("producesCapacity") or [])
                        if item.get("workload") == workload
                    )
                    if produced_mu <= 0.0:
                        continue
                    deps.add(key)
                    selected[workload].append(key)
                    selected_producers.setdefault(workload, set()).add(key)
                    selected_capacity.setdefault(workload, {})[key] = produced_mu
                    virtual_capacity[workload] = virtual_capacity.get(workload, 0.0) + produced_mu
                    needed -= produced_mu
                    if needed <= 1e-9:
                        break
            remaining_after = virtual_capacity.get(workload, 0.0) - float(consumed_mu)
            if needed > 1e-9 and not action.get("cleanupTemporaryCapacity"):
                blocked[workload] = needed
            virtual_capacity[workload] = remaining_after
        if deps:
            action["dependsOnActionKeys"] = sorted(deps)
        if selected:
            gate["selectedProducerActionKeys"] = selected
            action["capacityGate"] = gate
        if blocked:
            action["blockedByCapacity"] = True
            action["capacityGate"] = {**gate, "blockedCapacityDeficit": blocked}


def _assert_executable_actions(actions: list[dict[str, Any]]) -> None:
    blocked = [
        action
        for action in actions
        if str(action.get("type", "")).startswith("defer_") or bool(action.get("blockedByCapacity"))
    ]
    if not blocked:
        return
    summary = [
        {
            "type": action.get("type"),
            "gpu_id": action.get("gpu_id"),
            "physical_gpu_id": action.get("physical_gpu_id"),
            "slot": action.get("slot"),
            "reason": action.get("reason"),
            "capacityGate": action.get("capacityGate"),
        }
        for action in blocked[:5]
    ]
    raise RuntimeError(f"stage3 produced non-executable blocked actions: {summary}")


def _add_temporary_cleanup_dependency_edges(actions: list[dict[str, Any]]) -> None:
    non_cleanup_keys = {
        str(action["actionKey"])
        for action in actions
        if action.get("actionKey") is not None and not action.get("cleanupTemporaryCapacity")
    }
    temp_root_by_activation_key: dict[str, str] = {}
    cleanup_by_temp_root: dict[str, list[dict[str, Any]]] = {}
    final_activation_by_root: dict[str, str] = {}
    for action in actions:
        root = str(action.get("abstractRoot") or "")
        key = action.get("actionKey")
        if key is None:
            continue
        action_type = str(action.get("type") or "")
        if action.get("temporaryCapacity") and action_type == "activate_instance_route":
            temp_root_by_activation_key[str(key)] = root
        if action.get("cleanupTemporaryCapacity") and action_type == "deactivate_instance_route":
            cleanup_by_temp_root.setdefault(root, []).append(action)
        if not action.get("temporaryCapacity") and action_type == "activate_instance_route":
            final_activation_by_root[root] = str(key)

    deps_by_temp_root: dict[str, set[str]] = {}
    for action in actions:
        action_key = action.get("actionKey")
        if action_key is None:
            continue
        gate = dict(action.get("capacityGate") or {})
        selected = dict(gate.get("selectedProducerActionKeys") or {})
        selected_keys = [str(key) for keys in selected.values() for key in list(keys or [])]
        for selected_key in selected_keys:
            temp_root = temp_root_by_activation_key.get(selected_key)
            if temp_root is None:
                continue
            deps = deps_by_temp_root.setdefault(temp_root, set())
            deps.add(str(action_key))
            final_key = final_activation_by_root.get(str(action.get("abstractRoot") or ""))
            if final_key is not None:
                deps.add(final_key)

    for temp_root, cleanup_actions in cleanup_by_temp_root.items():
        deps_to_add = deps_by_temp_root.get(temp_root, set())
        deps_to_add.update(non_cleanup_keys)
        if not deps_to_add:
            continue
        for cleanup in cleanup_actions:
            cleanup_key = cleanup.get("actionKey")
            deps = set(str(key) for key in list(cleanup.get("dependsOnActionKeys") or []))
            deps.update(key for key in deps_to_add if cleanup_key is None or key != str(cleanup_key))
            cleanup["dependsOnActionKeys"] = sorted(deps)


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


def _capacity_producer_sort_key(
    producer: dict[str, Any],
    context: dict[str, Any],
    *,
    prefer_temporary: bool = False,
) -> tuple[int, int, int, float]:
    key = str(producer.get("actionKey"))
    produced_mu = sum(float(item.get("mu", 0.0) or 0.0) for item in list(producer.get("producesCapacity") or []))
    return (
        0 if prefer_temporary and producer.get("temporaryCapacity") else 1,
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
    """Return whether same-GPU produced capacity can protect a removal.

    Full in-place reconfiguration cannot use capacity produced on the same
    physical GPU to justify deleting the old side: the old side must be gone
    before that new capacity exists. Partial reconfiguration is different
    because preserved slots and locally-created slots can coexist after the
    partial patch, as long as the dependency does not form a structural cycle.

    Plain instance-diff transitions on an unchanged MIG template are also safe
    when the producer and consumer slots do not overlap. In that case the target
    instance can be placed and routed before the old instance is drained.

    """

    if _partial_capacity_context(consumer) and _partial_capacity_context(producer):
        return True
    return not _action_slots_overlap(consumer, producer)


def _action_slots_overlap(left: dict[str, Any], right: dict[str, Any]) -> bool:
    for left_slot in _action_slots(left):
        for right_slot in _action_slots(right):
            if str(left_slot[2]) != str(right_slot[2]):
                continue
            if max(int(left_slot[0]), int(right_slot[0])) < min(int(left_slot[1]), int(right_slot[1])):
                return True
    return False


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
    pending_release_physical: str | None = None
    for action in actions:
        effect = dict(action.get("physicalGpuEffect") or {})
        if effect.get("type") == "release" and action.get("actionKey") is not None:
            pending_release_key = str(action["actionKey"])
            pending_release_physical = str(effect.get("physicalGpuId"))
            continue
        if effect.get("type") != "acquire" or pending_release_key is None:
            continue
        if pending_release_physical is None or str(effect.get("physicalGpuId")) != pending_release_physical:
            continue
        if action.get("capacityUrgent"):
            continue
        deps = set(str(key) for key in list(action.get("dependsOnActionKeys") or []))
        deps.add(pending_release_key)
        action["dependsOnActionKeys"] = sorted(deps)
        action["physicalGpuEffect"] = {**effect, "reuseDependencyActionKey": pending_release_key}
        pending_release_key = None
        pending_release_physical = None


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
    elif action_type == "deactivate_instance_route":
        consumed = _temporary_capacity_record(action) if action.get("temporaryCapacity") else _capacity_for_action_source(action, source_map)
        if consumed:
            out["consumesCapacity"] = consumed
            out["capacityGate"] = _capacity_gate(consumed, required)
        out["routeEffect"] = {
            "type": "deactivate_instance_route",
            "routerQueueRedispatch": bool(action.get("routerQueueRedispatch")),
        }
    elif action_type == "activate_instance_route":
        produced = _temporary_capacity_record(action) if action.get("temporaryCapacity") else _capacity_for_action_target(action, target_map)
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


def _temporary_capacity_record(action: dict[str, Any]) -> list[dict[str, Any]]:
    workload = action.get("workload")
    if workload is None:
        instance = action.get("instance") if isinstance(action.get("instance"), dict) else {}
        workload = instance.get("workload")
    if workload is None:
        return []
    slot = _action_slots(action)
    if not slot:
        return []
    mu = action.get("mu")
    if mu is None and isinstance(action.get("instance"), dict):
        mu = action["instance"].get("mu")
    return [{"workload": str(workload), "mu": float(mu or 0.0), "slot": list(slot[0])}]


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
