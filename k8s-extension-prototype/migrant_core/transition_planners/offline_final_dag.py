from __future__ import annotations

import time
from typing import Any

from ..physical_ids import PHYSICAL_ID_POOL, bootstrap_physical_ids_for_state, ensure_state_metadata, get_physical_id
from ..state import ClusterState, MigInstance, deepcopy_state, get_inst_by_slot, gpu_map_by_id
from ..transition_common import (
    alloc_from_free_pool,
    classify_gpu_change,
    diff_instances_within_same_template,
    find_active_bridge_slot,
    matches_target_state,
    safe_after_removing_gpu,
    safe_after_removing_instance,
)
from ..transition_engine import (
    _action,
    _get_runtime_entry,
    _nonfree_instances,
    _reroute_destination_candidates,
    _reroute_destination_label,
    _target_activation_actions,
    prepare_transition_runtime,
    required_arrival_dict,
    simulate_transition_actions,
)
from .action_plan_formats import build_phased_action_plan, compact_phased_action_plan


NAME = "transition.offline_final_dag"


def run(
    *,
    source_state: Any,
    target_state: Any,
    src_arrival: list[float] | tuple[float, ...] | dict[str, float],
    tgt_arrival: list[float] | tuple[float, ...] | dict[str, float],
    workload_names: list[str] | tuple[str, ...] | None = None,
    stage_name: str = "stage_offline_final_dag",
    max_iters: int = 1,
    **_: Any,
) -> dict[str, Any]:
    """Build one final transition DAG from current state to the MILP target.

    This is intentionally not a receding-horizon executor. It reads the actual
    current state and the materialized target state once, chooses abstract
    transition actions with peak-GPU minimization first, lowers them to
    executable fine-grained actions, and compiles those actions into a DAG.
    """

    start = time.perf_counter()
    current_state = prepare_transition_runtime(source_state, target_state)
    ensure_state_metadata(current_state)
    bootstrap_physical_ids_for_state(current_state)
    target_state = deepcopy_state(target_state)
    ensure_state_metadata(target_state)

    required = required_arrival_dict(src_arrival, tgt_arrival, workload_names=workload_names)
    actions, plan_items = _build_final_actions(
        source_state=current_state,
        target_state=target_state,
        required=required,
    )
    planned_state = _planned_state_for_actions(current_state, target_state, actions)
    executed_state = simulate_transition_actions(
        source_state=current_state,
        target_state=planned_state,
        fine_actions=actions,
        next_physical_idx=current_state.metadata.get("next_physical_idx", 0),
    )
    executed_state = _drop_available_physical_gpus(executed_state)
    dag = build_phased_action_plan(actions, plan_items=plan_items, name=f"{stage_name}-final")
    peak_active_gpu = _peak_serving_gpu_from_actions(current_state, actions)
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
                "reached_target": matches_target_state(executed_state, target_state),
                "phased_action_plan": dag,
                "phased_action_plan_summary": compact_phased_action_plan(dag),
            }
        ],
        "iteration_count": 1,
        "reached_target": matches_target_state(executed_state, target_state),
        "elapsed_sec": time.perf_counter() - start,
        "executed_actions": actions,
        "executed_state": executed_state,
        "target_state": deepcopy_state(target_state),
        "initial_runtime_state": deepcopy_state(current_state),
        "peak_active_gpu": peak_active_gpu,
        "source_active_gpu": len(_active_serving_pid_set(current_state)),
        "final_active_gpu": len(_active_serving_pid_set(executed_state)),
        "final_plan": {
            "stage_name": stage_name,
            "required": required,
            "fine_actions": actions,
            "executed_actions": actions,
            "blocked_actions": [],
            "planned_state": planned_state,
            "executed_state": executed_state,
            "plan_items": plan_items,
            "planner_objective_order": [
                "minimize peak active physical GPUs",
                "preserve service capacity and queue/drain safety",
                "minimize transition time via DAG parallelism",
            ],
        },
        "phased_action_plan": dag,
        "phased_action_plan_summary": compact_phased_action_plan(dag),
        "transition_planner_module": NAME,
        "max_iters_ignored": max_iters,
    }


def _build_final_actions(
    *,
    source_state: ClusterState,
    target_state: ClusterState,
    required: dict[str, float],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    src_map = {
        gpu_id: gpu
        for gpu_id, gpu in gpu_map_by_id(source_state).items()
        if not _is_available_physical_gpu(gpu)
    }
    tgt_map = gpu_map_by_id(target_state)
    all_gpu_ids = sorted(set(src_map) | set(tgt_map))
    free_pool = _build_initial_available_pool(source_state, src_map)
    actions: list[dict[str, Any]] = []
    plan_items: list[dict[str, Any]] = []

    reconfig_ids = [
        gpu_id
        for gpu_id in all_gpu_ids
        if classify_gpu_change(src_map.get(gpu_id), tgt_map.get(gpu_id)) == "reconfiguration"
    ]
    remove_ids = [
        gpu_id
        for gpu_id in all_gpu_ids
        if classify_gpu_change(src_map.get(gpu_id), tgt_map.get(gpu_id)) == "remove_gpu"
    ]
    create_ids = [
        gpu_id
        for gpu_id in all_gpu_ids
        if classify_gpu_change(src_map.get(gpu_id), tgt_map.get(gpu_id)) == "create_gpu"
    ]
    instance_diff_ids = [
        gpu_id
        for gpu_id in all_gpu_ids
        if classify_gpu_change(src_map.get(gpu_id), tgt_map.get(gpu_id)) == "instance_diff"
    ]

    # Peak-GPU first: do work that can free or reuse capacity before allocating
    # brand-new physical GPUs. Reconfiguration chooses in-place whenever it is
    # service-safe; target-first is reserved for the cases where old capacity
    # must remain serving until a target side is ready.
    for gpu_id in instance_diff_ids:
        _append_instance_diff_actions(actions, plan_items, source_state, target_state, gpu_id, required)

    for gpu_id in remove_ids:
        _append_delete_gpu_actions(actions, plan_items, source_state, gpu_id, required)
        physical_id = get_physical_id(source_state, gpu_id)
        if physical_id is not None:
            free_pool.append(physical_id)

    for gpu_id in reconfig_ids:
        src_gpu = src_map[gpu_id]
        tgt_gpu = tgt_map[gpu_id]
        old_physical_id = get_physical_id(source_state, gpu_id)
        if safe_after_removing_gpu(source_state, src_gpu, required):
            _append_in_place_reconfiguration_actions(
                actions,
                plan_items,
                source_state,
                gpu_id,
                old_physical_id,
                tgt_gpu.template_str(),
            )
            continue
        new_physical_id = alloc_from_free_pool(free_pool)
        _append_target_first_reconfiguration_actions(
            actions,
            plan_items,
            source_state,
            gpu_id,
            old_physical_id,
            new_physical_id,
            tgt_gpu.template_str(),
        )
        free_pool.append(old_physical_id)

    for gpu_id in create_ids:
        tgt_gpu = tgt_map[gpu_id]
        physical_id = alloc_from_free_pool(free_pool)
        _append_create_target_gpu_actions(actions, plan_items, gpu_id, physical_id, tgt_gpu.template_str())

    return actions, plan_items


def _append_create_target_gpu_actions(
    actions: list[dict[str, Any]],
    plan_items: list[dict[str, Any]],
    gpu_id: int,
    physical_id: str,
    template: str,
) -> None:
    root = f"CREATE_gpu{gpu_id}"
    common = {"transitionMode": "create_target_gpu", "abstractRoot": root}
    actions.extend(
        [
            _action(
                "allocate_gpu",
                gpu_id=gpu_id,
                physical_gpu_id=physical_id,
                logical_gpu_id=gpu_id,
                pendingLogicalGpuId=gpu_id,
                **common,
            ),
            _action(
                "configure_full_template",
                gpu_id=gpu_id,
                physical_gpu_id=physical_id,
                logical_gpu_id=gpu_id,
                pendingLogicalGpuId=gpu_id,
                template=template,
                **common,
            ),
            _action(
                "bind_target_gpu",
                gpu_id=gpu_id,
                physical_gpu_id=physical_id,
                logical_gpu_id=gpu_id,
                activeLogicalGpuId=gpu_id,
                clearsPendingLogicalGpuId=True,
                **common,
            ),
            *_tag_actions(_target_activation_actions(gpu_id, physical_id), common),
        ]
    )
    plan_items.append(_plan_item(root, "create_target_gpu", gpu_id, physical_id, template=template))


def _append_delete_gpu_actions(
    actions: list[dict[str, Any]],
    plan_items: list[dict[str, Any]],
    source_state: ClusterState,
    gpu_id: int,
    required: dict[str, float],
) -> None:
    src_gpu = gpu_map_by_id(source_state)[gpu_id]
    physical_id = get_physical_id(source_state, gpu_id)
    root = f"DELETE_gpu{gpu_id}"
    common = {"transitionMode": "delete_gpu", "abstractRoot": root}
    slots = [(inst.start, inst.end, inst.profile) for inst in _nonfree_instances(src_gpu)]
    if slots:
        actions.append(
            _action("stop_gpu_traffic", gpu_id=gpu_id, physical_gpu_id=physical_id, slots=slots, slotCount=len(slots), **common)
        )
    for inst in _nonfree_instances(src_gpu):
        actions.extend(_queue_and_drain_actions(source_state, source_state, gpu_id, physical_id, inst, required, common, stop_new=False))
    actions.extend(
        [
            _action("delete_pods", gpu_id=gpu_id, physical_gpu_id=physical_id, slots=slots, **common),
            _action(
                "clear_gpu_binding",
                gpu_id=gpu_id,
                physical_gpu_id=physical_id,
                logical_gpu_id=gpu_id,
                pendingLogicalGpuId=gpu_id,
                clearsActiveLogicalGpuId=True,
                **common,
            ),
            _action("clear_template", gpu_id=gpu_id, physical_gpu_id=physical_id, template=src_gpu.template_str(), **common),
            _action(
                "return_gpu",
                gpu_id=gpu_id,
                physical_gpu_id=physical_id,
                clearsPendingLogicalGpuId=True,
                **common,
            ),
        ]
    )
    plan_items.append(_plan_item(root, "delete_gpu", gpu_id, physical_id))


def _append_in_place_reconfiguration_actions(
    actions: list[dict[str, Any]],
    plan_items: list[dict[str, Any]],
    source_state: ClusterState,
    gpu_id: int,
    physical_id: str,
    template: str,
) -> None:
    src_gpu = gpu_map_by_id(source_state)[gpu_id]
    root = f"INPLACE_RECONF_gpu{gpu_id}"
    common = {"transitionMode": "in_place_reconfiguration", "abstractRoot": root}
    slots = [(inst.start, inst.end, inst.profile) for inst in _nonfree_instances(src_gpu)]
    if slots:
        actions.append(
            _action("stop_gpu_traffic", gpu_id=gpu_id, physical_gpu_id=physical_id, slots=slots, slotCount=len(slots), **common)
        )
    for inst in _nonfree_instances(src_gpu):
        actions.extend(_queue_and_drain_actions(source_state, source_state, gpu_id, physical_id, inst, {}, common, stop_new=False))
    actions.extend(
        [
            _action("delete_pods", gpu_id=gpu_id, physical_gpu_id=physical_id, slots=slots, **common),
            _action(
                "clear_gpu_binding",
                gpu_id=gpu_id,
                physical_gpu_id=physical_id,
                logical_gpu_id=gpu_id,
                pendingLogicalGpuId=gpu_id,
                clearsActiveLogicalGpuId=True,
                **common,
            ),
            _action(
                "configure_full_template",
                gpu_id=gpu_id,
                physical_gpu_id=physical_id,
                logical_gpu_id=gpu_id,
                pendingLogicalGpuId=gpu_id,
                template=template,
                **common,
            ),
            _action(
                "bind_target_gpu",
                gpu_id=gpu_id,
                physical_gpu_id=physical_id,
                logical_gpu_id=gpu_id,
                activeLogicalGpuId=gpu_id,
                clearsPendingLogicalGpuId=True,
                **common,
            ),
            *_tag_actions(_target_activation_actions(gpu_id, physical_id), common),
        ]
    )
    plan_items.append(_plan_item(root, "in_place_reconfiguration", gpu_id, physical_id, template=template))


def _append_target_first_reconfiguration_actions(
    actions: list[dict[str, Any]],
    plan_items: list[dict[str, Any]],
    source_state: ClusterState,
    gpu_id: int,
    old_physical_id: str,
    new_physical_id: str,
    template: str,
) -> None:
    src_gpu = gpu_map_by_id(source_state)[gpu_id]
    root = f"TARGET_FIRST_RECONF_gpu{gpu_id}"
    common = {"transitionMode": "target_first_reconfiguration", "abstractRoot": root}
    actions.extend(
        [
            _action(
                "allocate_gpu",
                gpu_id=gpu_id,
                physical_gpu_id=new_physical_id,
                logical_gpu_id=gpu_id,
                pendingLogicalGpuId=gpu_id,
                **common,
            ),
            _action(
                "configure_full_template",
                gpu_id=gpu_id,
                physical_gpu_id=new_physical_id,
                logical_gpu_id=gpu_id,
                pendingLogicalGpuId=gpu_id,
                template=template,
                **common,
            ),
        ]
    )
    slots = [(inst.start, inst.end, inst.profile) for inst in _nonfree_instances(src_gpu)]
    if slots:
        actions.append(
            _action("stop_gpu_traffic", gpu_id=gpu_id, physical_gpu_id=old_physical_id, slots=slots, slotCount=len(slots), **common)
        )
    for inst in _nonfree_instances(src_gpu):
        actions.extend(_queue_and_drain_actions(source_state, source_state, gpu_id, old_physical_id, inst, {}, common, stop_new=False))
    actions.extend(
        [
            _action("delete_pods", gpu_id=gpu_id, physical_gpu_id=old_physical_id, slots=slots, **common),
            _action(
                "clear_gpu_binding",
                gpu_id=gpu_id,
                physical_gpu_id=old_physical_id,
                logical_gpu_id=gpu_id,
                pendingLogicalGpuId=gpu_id,
                clearsActiveLogicalGpuId=True,
                **common,
            ),
            _action(
                "bind_target_gpu",
                gpu_id=gpu_id,
                physical_gpu_id=new_physical_id,
                logical_gpu_id=gpu_id,
                activeLogicalGpuId=gpu_id,
                clearsPendingLogicalGpuId=True,
                **common,
            ),
            *_tag_actions(_target_activation_actions(gpu_id, new_physical_id), common),
            _action("clear_template", gpu_id=gpu_id, physical_gpu_id=old_physical_id, template=src_gpu.template_str(), **common),
            _action(
                "return_gpu",
                gpu_id=gpu_id,
                physical_gpu_id=old_physical_id,
                clearsPendingLogicalGpuId=True,
                **common,
            ),
        ]
    )
    plan_items.append(
        _plan_item(
            root,
            "target_first_reconfiguration",
            gpu_id,
            old_physical_id,
            target_physical_gpu_id=new_physical_id,
            template=template,
        )
    )


def _append_instance_diff_actions(
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
        slot = inst_action["slot"]
        change_type = inst_action["type"]
        if change_type == "keep":
            continue
        root = f"SLOT_gpu{gpu_id}_{slot[0]}_{slot[1]}_{slot[2]}"
        common = {"transitionMode": change_type, "abstractRoot": root}
        if change_type == "batch_change":
            src = inst_action["src"]
            tgt = inst_action["tgt"]
            actions.extend(
                [
                    _action("patch_batch_config", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, old_batch=src.batch, new_batch=tgt.batch, workload=src.workload, **common),
                    _action("apply_batch", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, old_batch=src.batch, new_batch=tgt.batch, workload=src.workload, **common),
                    _action("verify_batch", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, old_batch=src.batch, new_batch=tgt.batch, workload=src.workload, **common),
                    _action("activate_serving_route", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=src.workload, **common),
                ]
            )
            plan_items.append(_plan_item(root, "batch_update", gpu_id, physical_id, slot=slot, workload=src.workload))
            continue
        if change_type == "place_instance":
            tgt = inst_action["tgt"]
            actions.extend(
                [
                    _action("place_instance", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=tgt.workload, batch=tgt.batch, **common),
                    _action("activate_serving_route", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=tgt.workload, **common),
                ]
            )
            plan_items.append(_plan_item(root, "place_instance", gpu_id, physical_id, slot=slot, workload=tgt.workload))
            continue
        if change_type == "safe_remove_instance":
            src = inst_action["src"]
            actions.extend(_queue_and_drain_actions(source_state, target_state, gpu_id, physical_id, src, required, common))
            actions.append(
                _action("delete_pods", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, slots=[slot], workload=src.workload, **common)
            )
            plan_items.append(_plan_item(root, "delete_pods", gpu_id, physical_id, slot=slot, workload=src.workload))
            continue
        if change_type == "workload_change":
            _append_workload_replacement_actions(
                actions,
                plan_items,
                source_state,
                target_state,
                gpu_id,
                physical_id,
                inst_action["src"],
                inst_action["tgt"],
                required,
                root,
            )


def _append_workload_replacement_actions(
    actions: list[dict[str, Any]],
    plan_items: list[dict[str, Any]],
    source_state: ClusterState,
    target_state: ClusterState,
    gpu_id: int,
    physical_id: str,
    src: MigInstance,
    tgt: MigInstance,
    required: dict[str, float],
    root: str,
) -> None:
    slot = (src.start, src.end, src.profile)
    common = {"transitionMode": "workload_replacement", "abstractRoot": root}
    safe = safe_after_removing_instance(source_state, src, required)
    candidates = _reroute_destination_candidates(source_state, target_state, src.workload, exclude_gpu_id=gpu_id, exclude_slot=slot)
    if safe or candidates:
        actions.extend(_queue_and_drain_actions(source_state, target_state, gpu_id, physical_id, src, required, common))
        actions.extend(
            [
                _action("delete_pods", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, slots=[slot], workload=src.workload, **common),
                _action("place_instance", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=tgt.workload, batch=tgt.batch, **common),
                _action("activate_serving_route", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=tgt.workload, **common),
            ]
        )
        plan_items.append(_plan_item(root, "workload_replacement", gpu_id, physical_id, slot=slot, workload=src.workload))
        return

    bridge_slot = find_active_bridge_slot(
        source_state=source_state,
        target_state=target_state,
        profile=src.profile,
        avoid_gpu_id=gpu_id,
        avoid_slot=slot,
    )
    if bridge_slot is None:
        actions.extend(_queue_and_drain_actions(source_state, target_state, gpu_id, physical_id, src, required, common))
        bridge_mode = False
    else:
        bridge_pid = get_physical_id(source_state, int(bridge_slot["gpu_id"]))
        queue_transfer_id = f"bridge_gpu{gpu_id}_{slot[0]}_{slot[1]}_{slot[2]}_{src.workload}"
        actions.extend(
            [
                _action(
                    "bridge_place_instance",
                    gpu_id=int(bridge_slot["gpu_id"]),
                    physical_gpu_id=bridge_pid,
                    slot=bridge_slot["slot"],
                    workload=src.workload,
                    batch=src.batch,
                    mu=float(src.mu),
                    queue_transfer_id=queue_transfer_id,
                    **common,
                ),
                _action(
                    "stop_accepting_new",
                    gpu_id=gpu_id,
                    physical_gpu_id=physical_id,
                    slot=slot,
                    workload=src.workload,
                    **common,
                ),
                _action(
                    "reroute_queued_tasks",
                    gpu_id=gpu_id,
                    physical_gpu_id=physical_id,
                    slot=slot,
                    workload=src.workload,
                    to=f"bridge[{bridge_slot['gpu_id']}:{bridge_slot['slot']}]",
                    target_gpu_id=int(bridge_slot["gpu_id"]),
                    target_physical_gpu_id=bridge_pid,
                    target_slot=bridge_slot["slot"],
                    queue_transfer_id=queue_transfer_id,
                    **common,
                ),
                _action(
                    "accept_queued_requests",
                    gpu_id=int(bridge_slot["gpu_id"]),
                    physical_gpu_id=bridge_pid,
                    slot=bridge_slot["slot"],
                    workload=src.workload,
                    from_gpu_id=gpu_id,
                    from_physical_gpu_id=physical_id,
                    from_slot=slot,
                    queue_transfer_id=queue_transfer_id,
                    bridge=True,
                    **common,
                ),
                _action(
                    "mark_draining_instance",
                    gpu_id=gpu_id,
                    physical_gpu_id=physical_id,
                    slot=slot,
                    workload=src.workload,
                    rounds=max(1, int(_get_runtime_entry(source_state, gpu_id, slot).get("inflight", 0))),
                    **common,
                ),
            ]
        )
        bridge_mode = True
    actions.extend(
        [
            _action("delete_pods", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, slots=[slot], workload=src.workload, bridged=bridge_mode, **common),
            _action("place_instance", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=tgt.workload, batch=tgt.batch, bridged=bridge_mode, **common),
            _action("activate_serving_route", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=tgt.workload, **common),
        ]
    )
    if bridge_slot is not None:
        actions.extend(
            [
                _action(
                    "mark_draining_instance",
                    gpu_id=int(bridge_slot["gpu_id"]),
                    physical_gpu_id=get_physical_id(source_state, int(bridge_slot["gpu_id"])),
                    slot=bridge_slot["slot"],
                    workload=src.workload,
                    rounds=1,
                    bridge=True,
                    **common,
                ),
                _action(
                    "delete_bridge_pod",
                    gpu_id=int(bridge_slot["gpu_id"]),
                    physical_gpu_id=get_physical_id(source_state, int(bridge_slot["gpu_id"])),
                    slot=bridge_slot["slot"],
                    workload=src.workload,
                    **common,
                ),
            ]
        )
    plan_items.append(_plan_item(root, "bridge_workload_replacement" if bridge_mode else "workload_replacement", gpu_id, physical_id, slot=slot, workload=src.workload))


def _queue_and_drain_actions(
    source_state: ClusterState,
    target_state: ClusterState,
    gpu_id: int,
    physical_id: str,
    inst: MigInstance,
    required: dict[str, float],
    common: dict[str, Any],
    stop_new: bool = True,
) -> list[dict[str, Any]]:
    slot = (inst.start, inst.end, inst.profile)
    runtime = _get_runtime_entry(source_state, gpu_id, slot)
    actions: list[dict[str, Any]] = []
    if stop_new and bool(runtime.get("accepting_new", True)):
        actions.append(
            _action("stop_accepting_new", gpu_id=gpu_id, physical_gpu_id=physical_id, slot=slot, workload=inst.workload, **common)
        )
    queued = int(runtime.get("queued", 0) or 0)
    inflight = int(runtime.get("inflight", 0) or 0)
    candidates = _reroute_destination_candidates(
        source_state,
        target_state,
        inst.workload,
        exclude_gpu_id=gpu_id,
        exclude_slot=slot,
    )
    reroute_candidate = candidates[0] if candidates else None
    if queued > 0 and reroute_candidate is not None:
        transfer_id = f"queue_gpu{gpu_id}_{slot[0]}_{slot[1]}_{slot[2]}_{inst.workload}"
        actions.extend(
            [
                _action(
                    "accept_queued_requests",
                    gpu_id=reroute_candidate.get("gpu_id"),
                    physical_gpu_id=reroute_candidate.get("physical_gpu_id"),
                    slot=reroute_candidate.get("slot"),
                    workload=inst.workload,
                    queued=queued,
                    from_gpu_id=gpu_id,
                    from_physical_gpu_id=physical_id,
                    from_slot=slot,
                    queue_transfer_id=transfer_id,
                    **common,
                ),
                _action(
                    "reroute_queued_tasks",
                    gpu_id=gpu_id,
                    physical_gpu_id=physical_id,
                    slot=slot,
                    workload=inst.workload,
                    queued=queued,
                    to=_reroute_destination_label(candidates),
                    target_gpu_id=reroute_candidate.get("gpu_id"),
                    target_physical_gpu_id=reroute_candidate.get("physical_gpu_id"),
                    target_slot=reroute_candidate.get("slot"),
                    queue_transfer_id=transfer_id,
                    **common,
                ),
            ]
        )
    if inflight > 0 or (queued > 0 and reroute_candidate is None):
        drain_rounds = max(1, inflight + (queued if reroute_candidate is None else 0))
        actions.append(
            _action(
                "mark_draining_instance",
                gpu_id=gpu_id,
                physical_gpu_id=physical_id,
                slot=slot,
                workload=inst.workload,
                rounds=drain_rounds,
                capacitySafe=safe_after_removing_instance(source_state, inst, required) if required else None,
                **common,
            )
        )
    return actions


def _planned_state_for_actions(
    source_state: ClusterState,
    target_state: ClusterState,
    actions: list[dict[str, Any]],
) -> ClusterState:
    planned = deepcopy_state(target_state)
    target_pid_map: dict[int, str] = {}
    for action in actions:
        action_type = action.get("type")
        gpu_id = action.get("gpu_id")
        physical_id = action.get("physical_gpu_id")
        if gpu_id is None or physical_id is None:
            continue
        if action_type in {"bind_target_gpu", "activate_serving_route", "deploy_target_workloads", "place_instance"}:
            target_pid_map[int(gpu_id)] = str(physical_id)
    if not target_pid_map:
        target_pid_map = {
            int(gpu.gpu_id): get_physical_id(source_state, int(gpu.gpu_id))
            for gpu in planned.real_gpus()
            if get_physical_id(source_state, int(gpu.gpu_id)) is not None
        }
    planned.metadata["physical_id_map"] = target_pid_map
    planned.metadata["next_physical_idx"] = source_state.metadata.get("next_physical_idx", 0)
    return planned


def _drop_available_physical_gpus(state: ClusterState) -> ClusterState:
    out = deepcopy_state(state)
    out.gpus = [gpu for gpu in out.real_gpus() if not _is_available_physical_gpu(gpu)]
    active_ids = {int(gpu.gpu_id) for gpu in out.gpus}
    out.metadata["physical_id_map"] = {
        int(gpu_id): physical_id
        for gpu_id, physical_id in dict(out.metadata.get("physical_id_map", {})).items()
        if int(gpu_id) in active_ids
    }
    return out


def _is_available_physical_gpu(gpu: Any) -> bool:
    instances = list(getattr(gpu, "instances", []) or [])
    if not instances:
        return True
    return all(getattr(inst, "profile", None) == "void" and getattr(inst, "workload", None) is None for inst in instances)


def _build_initial_available_pool(source_state: ClusterState, active_src_map: dict[int, Any]) -> list[str]:
    active_pids = {
        get_physical_id(source_state, gpu_id)
        for gpu_id in active_src_map
        if get_physical_id(source_state, gpu_id) is not None
    }
    available_existing = [
        get_physical_id(source_state, int(gpu.gpu_id))
        for gpu in sorted(source_state.real_gpus(), key=lambda item: int(item.gpu_id), reverse=True)
        if _is_available_physical_gpu(gpu) and get_physical_id(source_state, int(gpu.gpu_id)) is not None
    ]
    never_seen = [physical_id for physical_id in reversed(PHYSICAL_ID_POOL) if physical_id not in active_pids and physical_id not in available_existing]
    return never_seen + available_existing


def _active_serving_pid_set(state: ClusterState) -> set[str]:
    return {
        str(get_physical_id(state, int(gpu.gpu_id)))
        for gpu in state.real_gpus()
        if not _is_available_physical_gpu(gpu) and get_physical_id(state, int(gpu.gpu_id)) is not None
    }


def _peak_serving_gpu_from_actions(state_before: ClusterState, actions: list[dict[str, Any]]) -> int:
    active = set(_active_serving_pid_set(state_before))
    peak = len(active)
    for action in actions:
        action_type = str(action.get("type", ""))
        physical_id = action.get("physical_gpu_id")
        if physical_id is None:
            continue
        if action_type == "allocate_gpu":
            active.add(str(physical_id))
        elif action_type in {"return_gpu", "clear_gpu_binding"}:
            active.discard(str(physical_id))
        peak = max(peak, len(active))
    return peak


def _tag_actions(actions: list[dict[str, Any]], tags: dict[str, Any]) -> list[dict[str, Any]]:
    tagged = []
    for action in actions:
        copied = dict(action)
        copied.update(tags)
        tagged.append(copied)
    return tagged


def _plan_item(
    item_id: str,
    item_type: str,
    gpu_id: int,
    physical_id: str,
    **kwargs: Any,
) -> dict[str, Any]:
    out = {
        "id": item_id,
        "type": item_type,
        "current_phase": "final_dag",
        "status": "ready",
        "blocked_by": None,
        "gpu_id": gpu_id,
        "physical_gpu_id": physical_id,
    }
    out.update(kwargs)
    return out
