from __future__ import annotations

import copy
import json
import time
from typing import Any

from .physical_ids import (
    bootstrap_physical_ids_for_state,
    ensure_state_metadata,
    get_physical_id,
    set_physical_id,
)
from .state import (
    ClusterState,
    GPUState,
    MigInstance,
    copy_inst_payload,
    deepcopy_state,
    get_inst_by_slot,
    gpu_map_by_id,
    replace_or_append_gpu,
)
from .transition_common import (
    alloc_from_free_pool,
    build_initial_free_pool,
    classify_gpu_change,
    diff_instances_within_same_template,
    find_active_bridge_slot,
    get_gpu_by_id_mut,
    matches_target_state,
    remove_gpu_if_bound_to_pid,
    required_arrival_dict,
    safe_after_removing_gpu,
    safe_after_removing_instance,
    state_semantic_signature,
)


def classify_workloads_by_arrival(
    src_arrival: list[float] | tuple[float, ...] | dict[str, float],
    tgt_arrival: list[float] | tuple[float, ...] | dict[str, float],
    workload_names: list[str] | tuple[str, ...] | None = None,
) -> dict[str, list[str]]:
    if isinstance(src_arrival, dict):
        src_dict = {str(k): float(v) for k, v in src_arrival.items()}
    else:
        if workload_names is None:
            raise ValueError("workload_names is required for vector src_arrival")
        src_dict = {str(w): float(src_arrival[idx]) for idx, w in enumerate(workload_names)}

    if isinstance(tgt_arrival, dict):
        tgt_dict = {str(k): float(v) for k, v in tgt_arrival.items()}
    else:
        if workload_names is None:
            raise ValueError("workload_names is required for vector tgt_arrival")
        tgt_dict = {str(w): float(tgt_arrival[idx]) for idx, w in enumerate(workload_names)}

    names = list(workload_names) if workload_names is not None else sorted(set(src_dict) | set(tgt_dict))
    out = {"shared": [], "new": [], "retiring": [], "inactive": []}
    for workload in names:
        src = float(src_dict.get(workload, 0.0))
        tgt = float(tgt_dict.get(workload, 0.0))
        if src > 0 and tgt > 0:
            out["shared"].append(str(workload))
        elif src <= 0 and tgt > 0:
            out["new"].append(str(workload))
        elif src > 0 and tgt <= 0:
            out["retiring"].append(str(workload))
        else:
            out["inactive"].append(str(workload))
    return out


def _workload_class_name(classes: dict[str, list[str]], workload: str | None) -> str:
    if workload is None:
        return "none"
    for class_name in ["shared", "new", "retiring", "inactive"]:
        if workload in classes.get(class_name, []):
            return class_name
    return "unknown"


def _slot_token(gpu_id: int, slot: tuple[int, int, str]) -> str:
    start, end, profile = slot
    return f"{int(gpu_id)}:{int(start)}:{int(end)}:{profile}"


def _drain_map(state: ClusterState) -> dict[str, int]:
    ensure_state_metadata(state)
    return state.metadata.setdefault("v3_draining_instances", {})


def _runtime_map(state: ClusterState) -> dict[str, dict[str, Any]]:
    ensure_state_metadata(state)
    return state.metadata.setdefault("v3_runtime_instances", {})


def _reconfig_map(state: ClusterState) -> dict[str, dict[str, Any]]:
    ensure_state_metadata(state)
    return state.metadata.setdefault("v3_reconfig_targets", {})


def _get_drain_remaining(
    state: ClusterState,
    gpu_id: int,
    slot: tuple[int, int, str],
) -> int | None:
    return _drain_map(state).get(_slot_token(gpu_id, slot))


def _start_drain(
    state: ClusterState,
    gpu_id: int,
    slot: tuple[int, int, str],
    rounds: int = 1,
) -> None:
    _drain_map(state)[_slot_token(gpu_id, slot)] = int(rounds)


def _clear_drain(state: ClusterState, gpu_id: int, slot: tuple[int, int, str]) -> None:
    _drain_map(state).pop(_slot_token(gpu_id, slot), None)


def _get_runtime_entry(
    state: ClusterState,
    gpu_id: int,
    slot: tuple[int, int, str],
) -> dict[str, Any]:
    token = _slot_token(gpu_id, slot)
    runtime = _runtime_map(state)
    if token not in runtime:
        runtime[token] = {"queued": 0, "inflight": 0, "accepting_new": True, "rerouted_to": None}
    return runtime[token]


def _clear_runtime_entry(state: ClusterState, gpu_id: int, slot: tuple[int, int, str]) -> None:
    _runtime_map(state).pop(_slot_token(gpu_id, slot), None)


def _nonfree_instances(gpu: GPUState) -> list[MigInstance]:
    return [inst for inst in gpu.instances if getattr(inst, "workload", None)]


def prepare_v3_source_runtime(
    source_state: ClusterState,
    target_state: ClusterState,
    default_queued: int = 2,
    default_inflight: int = 1,
) -> ClusterState:
    state = deepcopy_state(source_state)
    ensure_state_metadata(state)
    bootstrap_physical_ids_for_state(state)
    if state.metadata.get("v3_runtime_instances"):
        return state

    for gpu in state.real_gpus():
        for inst in _nonfree_instances(gpu):
            slot = (inst.start, inst.end, inst.profile)
            runtime = _get_runtime_entry(state, gpu.gpu_id, slot)
            runtime.update({"queued": 0, "inflight": 0, "accepting_new": True, "rerouted_to": None})

    src_map = gpu_map_by_id(state)
    tgt_map = gpu_map_by_id(target_state)
    for gpu_id in sorted(set(src_map) | set(tgt_map)):
        src_gpu = src_map.get(gpu_id)
        tgt_gpu = tgt_map.get(gpu_id)
        change = classify_gpu_change(src_gpu, tgt_gpu)
        if change in {"remove_gpu", "reconfiguration"} and src_gpu is not None:
            for inst in _nonfree_instances(src_gpu):
                slot = (inst.start, inst.end, inst.profile)
                runtime = _get_runtime_entry(state, gpu_id, slot)
                runtime["queued"] = max(int(runtime.get("queued", 0)), int(default_queued))
                runtime["inflight"] = max(int(runtime.get("inflight", 0)), int(default_inflight))
                runtime["accepting_new"] = True
        elif change == "instance_diff" and src_gpu is not None and tgt_gpu is not None:
            for inst_action in diff_instances_within_same_template(src_gpu, tgt_gpu):
                if inst_action["type"] in {"safe_remove_instance", "workload_change"} and inst_action.get("src") is not None:
                    slot = inst_action["slot"]
                    runtime = _get_runtime_entry(state, gpu_id, slot)
                    runtime["queued"] = max(int(runtime.get("queued", 0)), int(default_queued))
                    runtime["inflight"] = max(int(runtime.get("inflight", 0)), int(default_inflight))
                    runtime["accepting_new"] = True
    return state


def _advance_drain(state: ClusterState) -> None:
    drains = _drain_map(state)
    runtime = _runtime_map(state)
    for token in list(drains.keys()):
        drains[token] = max(0, int(drains[token]) - 1)
        if token in runtime:
            runtime[token]["inflight"] = int(drains[token])


def _progress_signature(state: ClusterState) -> tuple:
    drains = tuple(sorted((key, int(value)) for key, value in _drain_map(state).items()))
    runtime = tuple(
        sorted(
            (
                key,
                int(value.get("queued", 0)),
                int(value.get("inflight", 0)),
                bool(value.get("accepting_new", True)),
                str(value.get("rerouted_to")),
            )
            for key, value in _runtime_map(state).items()
        )
    )
    return state_semantic_signature(state), drains, runtime


def _active_pid_set(state: ClusterState) -> set[str]:
    active = {
        physical_id
        for _, physical_id in sorted(state.metadata.get("physical_id_map", {}).items())
        if physical_id is not None
    }
    for record in _reconfig_map(state).values():
        physical_id = record.get("physical_gpu_id")
        if physical_id is not None:
            active.add(physical_id)
    return active


def _peak_from_actions(state_before: ClusterState, executed_actions: list[dict[str, Any]]) -> int:
    active = set(_active_pid_set(state_before))
    peak = len(active)
    for action in executed_actions:
        action_type = action.get("type")
        if action_type == "allocate_gpu":
            physical_id = action.get("physical_gpu_id")
            if physical_id is not None:
                active.add(physical_id)
        elif action_type == "clear_gpu":
            physical_id = action.get("physical_gpu_id")
            if physical_id is not None and physical_id in active:
                active.remove(physical_id)
        peak = max(peak, len(active))
    return peak


def _takeover_candidates(
    source_state: ClusterState,
    target_state: ClusterState,
    workload: str,
    exclude_gpu_id: int | None = None,
    exclude_slot: tuple[int, int, str] | None = None,
) -> list[dict[str, Any]]:
    out = []
    seen = set()
    target_map = gpu_map_by_id(target_state)
    for gpu in source_state.real_gpus():
        physical_id = get_physical_id(source_state, gpu.gpu_id)
        target_gpu = target_map.get(gpu.gpu_id)
        for inst in _nonfree_instances(gpu):
            slot = (inst.start, inst.end, inst.profile)
            if gpu.gpu_id == exclude_gpu_id and slot == exclude_slot:
                continue
            if inst.workload != workload:
                continue
            target_inst = get_inst_by_slot(target_gpu, slot) if target_gpu is not None else None
            if target_inst is not None and target_inst.workload == workload:
                key = (gpu.gpu_id, slot)
                seen.add(key)
                out.append({"kind": "unchanged", "gpu_id": gpu.gpu_id, "slot": slot, "physical_gpu_id": physical_id})
    for gpu in target_state.real_gpus():
        for inst in _nonfree_instances(gpu):
            slot = (inst.start, inst.end, inst.profile)
            if gpu.gpu_id == exclude_gpu_id and slot == exclude_slot:
                continue
            if inst.workload != workload:
                continue
            key = (gpu.gpu_id, slot)
            if key in seen:
                continue
            out.append({"kind": "target-backed", "gpu_id": gpu.gpu_id, "slot": slot, "physical_gpu_id": None})
    return out


def _slot_label(gpu_id: int, slot: tuple[int, int, str], physical_gpu_id: str | None = None) -> str:
    start, end, profile = slot
    base = f"gpu{gpu_id}:{profile}[{start},{end})"
    return f"{base}@{physical_gpu_id}" if physical_gpu_id is not None else base


def _takeover_label(candidates: list[dict[str, Any]]) -> str | None:
    if not candidates:
        return None
    candidate = candidates[0]
    return f"{candidate['kind']}[{_slot_label(candidate['gpu_id'], candidate['slot'], candidate.get('physical_gpu_id'))}]"


def _plan_item(
    item_id: str,
    item_type: str,
    current_phase: str,
    status: str,
    blocked_by: str | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    out = {"id": item_id, "type": item_type, "current_phase": current_phase, "status": status, "blocked_by": blocked_by}
    out.update(kwargs)
    return out


def _split_actions(fine_actions: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    executed = []
    blocked = []
    for action in fine_actions:
        if str(action.get("type", "")).startswith("defer_"):
            blocked.append(action)
        else:
            executed.append(action)
    return executed, blocked


def simulate_v3_actions(
    source_state: ClusterState,
    target_state: ClusterState,
    fine_actions: list[dict[str, Any]],
    next_physical_idx: int,
) -> ClusterState:
    executed_state = deepcopy_state(source_state)
    ensure_state_metadata(executed_state)
    bootstrap_physical_ids_for_state(executed_state)
    ensure_state_metadata(target_state)
    target_map = gpu_map_by_id(target_state)
    executed_state.metadata["v3_draining_instances"] = dict(source_state.metadata.get("v3_draining_instances", {}))
    executed_state.metadata["v3_runtime_instances"] = copy.deepcopy(source_state.metadata.get("v3_runtime_instances", {}))
    executed_state.metadata["v3_reconfig_targets"] = copy.deepcopy(source_state.metadata.get("v3_reconfig_targets", {}))

    for action in fine_actions:
        action_type = action["type"]

        if action_type in {
            "allocate_gpu",
            "configure_full_template",
            "configure_partial_profile",
            "place_target_layout",
            "defer_remove_gpu",
            "defer_remove_instance",
            "defer_workload_change",
        }:
            continue

        if action_type == "stop_accepting_new":
            runtime = _get_runtime_entry(executed_state, int(action["gpu_id"]), tuple(action["slot"]))
            runtime["accepting_new"] = False
            continue

        if action_type == "reroute_queued_tasks":
            runtime = _get_runtime_entry(executed_state, int(action["gpu_id"]), tuple(action["slot"]))
            runtime["queued"] = 0
            runtime["rerouted_to"] = action.get("to")
            continue

        if action_type == "mark_draining_instance":
            gpu_id = int(action["gpu_id"])
            slot = tuple(action["slot"])
            runtime = _get_runtime_entry(executed_state, gpu_id, slot)
            runtime["accepting_new"] = False
            rounds = max(int(action.get("rounds", 1)), int(runtime.get("inflight", 0)))
            _start_drain(executed_state, gpu_id, slot, rounds=rounds)
            runtime["inflight"] = rounds
            continue

        if action_type == "mark_reconfig_target_prepared":
            _reconfig_map(executed_state)[str(int(action["gpu_id"]))] = {
                "physical_gpu_id": action["physical_gpu_id"],
                "template": action.get("template"),
            }
            continue

        if action_type == "bind_target_gpu":
            gpu_id = int(action["gpu_id"])
            physical_id = action["physical_gpu_id"]
            target_gpu = target_map.get(gpu_id)
            if target_gpu is None:
                continue
            replace_or_append_gpu(executed_state, copy.deepcopy(target_gpu))
            set_physical_id(executed_state, gpu_id, physical_id)
            for inst in _nonfree_instances(target_gpu):
                slot = (inst.start, inst.end, inst.profile)
                runtime = _get_runtime_entry(executed_state, gpu_id, slot)
                runtime.setdefault("queued", 0)
                runtime.setdefault("inflight", 0)
                runtime["accepting_new"] = True
            _reconfig_map(executed_state).pop(str(gpu_id), None)
            continue

        if action_type == "clear_gpu":
            gpu_id = int(action["gpu_id"])
            cur_gpu = get_gpu_by_id_mut(executed_state, gpu_id)
            if cur_gpu is not None:
                for inst in list(cur_gpu.instances):
                    slot = (inst.start, inst.end, inst.profile)
                    _clear_runtime_entry(executed_state, gpu_id, slot)
                    _clear_drain(executed_state, gpu_id, slot)
            remove_gpu_if_bound_to_pid(executed_state, gpu_id, action["physical_gpu_id"])
            continue

        if action_type == "clear_template":
            continue

        gpu_id = int(action["gpu_id"])
        physical_id = action["physical_gpu_id"]
        cur_gpu = get_gpu_by_id_mut(executed_state, gpu_id)
        if cur_gpu is None or get_physical_id(executed_state, gpu_id) != physical_id:
            continue

        slot = tuple(action.get("slot", ()))
        cur_inst = get_inst_by_slot(cur_gpu, slot) if slot else None
        target_gpu = target_map.get(gpu_id)
        target_inst = get_inst_by_slot(target_gpu, slot) if slot else None

        if action_type == "bridge_place_instance":
            if cur_inst is not None:
                cur_inst.workload = action.get("workload")
                cur_inst.batch = action.get("batch")
                cur_inst.mu = float(action.get("mu", 0.0))
                cur_inst.preserved = False
                runtime = _get_runtime_entry(executed_state, gpu_id, slot)
                runtime["accepting_new"] = True
            continue

        if action_type in {"update_batch", "place_instance", "workload_change"}:
            if cur_inst is not None and target_inst is not None:
                copy_inst_payload(cur_inst, target_inst)
                runtime = _get_runtime_entry(executed_state, gpu_id, slot)
                runtime.setdefault("queued", 0)
                runtime.setdefault("inflight", 0)
                runtime["accepting_new"] = True
            continue

        if action_type == "remove_instance":
            if cur_inst is not None:
                copy_inst_payload(cur_inst, None)
            _clear_drain(executed_state, gpu_id, slot)
            _clear_runtime_entry(executed_state, gpu_id, slot)
            continue

    executed_state.gpus = sorted(executed_state.real_gpus(), key=lambda x: x.gpu_id)
    executed_state.metadata["next_physical_idx"] = int(next_physical_idx)
    return executed_state


def plan_v3_full_action_plan(
    source_state: ClusterState,
    target_state: ClusterState,
    src_arrival: list[float] | tuple[float, ...] | dict[str, float],
    tgt_arrival: list[float] | tuple[float, ...] | dict[str, float],
    workload_names: list[str] | tuple[str, ...] | None = None,
    stage_name: str = "stage_v3",
) -> dict[str, Any]:
    source_state = prepare_v3_source_runtime(source_state, target_state)
    target_state = deepcopy_state(target_state)
    bootstrap_physical_ids_for_state(source_state)
    ensure_state_metadata(target_state)
    ensure_state_metadata(source_state)
    required = required_arrival_dict(src_arrival, tgt_arrival, workload_names=workload_names)
    workload_classes = classify_workloads_by_arrival(src_arrival, tgt_arrival, workload_names=workload_names)
    src_map = gpu_map_by_id(source_state)
    tgt_map = gpu_map_by_id(target_state)
    all_gpu_ids = sorted(set(src_map) | set(tgt_map))
    free_pool = build_initial_free_pool(source_state)
    coarse_actions = []
    phase_actions = []
    capacity_actions = []
    cleanup_actions = []
    finalize_actions = []
    blocked_actions = []
    target_pid_map = {}
    plan_items = []

    def add_slot_barrier(
        gpu_id: int,
        physical_id: str,
        slot: tuple[int, int, str],
        workload: str,
        item_type: str,
        item_id: str,
        safe_after_drain: bool = True,
        reroute_title: str = "reroute_old_queue",
    ) -> tuple[list[dict[str, Any]], str, str, str | None]:
        runtime = _get_runtime_entry(source_state, gpu_id, slot)
        remaining = _get_drain_remaining(source_state, gpu_id, slot)
        candidates = _takeover_candidates(
            source_state,
            target_state,
            workload,
            exclude_gpu_id=gpu_id,
            exclude_slot=slot,
        )
        takeover = _takeover_label(candidates)
        local_actions = []
        phase = "post_drain_ready"
        status = "ready"
        blocked_by = None
        if (
            int(runtime.get("queued", 0)) > 0
            or int(runtime.get("inflight", 0)) > 0
            or bool(runtime.get("accepting_new", True))
        ) and takeover is None:
            phase = "takeover_ready"
            status = "blocked"
            blocked_by = "no_takeover_capacity"
        else:
            if bool(runtime.get("accepting_new", True)):
                local_actions.append(
                    {
                        "type": "stop_accepting_new",
                        "gpu_id": gpu_id,
                        "physical_gpu_id": physical_id,
                        "slot": slot,
                        "workload": workload,
                    }
                )
                phase = "stop_accepting_new"
                status = "in_progress"
            if int(runtime.get("queued", 0)) > 0:
                local_actions.append(
                    {
                        "type": "reroute_queued_tasks",
                        "gpu_id": gpu_id,
                        "physical_gpu_id": physical_id,
                        "slot": slot,
                        "workload": workload,
                        "queued": int(runtime.get("queued", 0)),
                        "to": takeover,
                    }
                )
                phase = reroute_title
                status = "in_progress"
            if remaining is None and int(runtime.get("inflight", 0)) > 0:
                local_actions.append(
                    {
                        "type": "mark_draining_instance",
                        "gpu_id": gpu_id,
                        "physical_gpu_id": physical_id,
                        "slot": slot,
                        "workload": workload,
                        "rounds": int(runtime.get("inflight", 0)),
                    }
                )
                phase = "drain_old"
                status = "blocked"
                blocked_by = "drain_started"
            elif remaining is not None and int(remaining) > 0:
                phase = "drain_old"
                status = "blocked"
                blocked_by = "inflight_tasks_not_zero"
        plan_items.append(
            _plan_item(
                item_id=item_id,
                item_type=item_type,
                current_phase=phase,
                status=status,
                blocked_by=blocked_by,
                gpu_id=gpu_id,
                physical_gpu_id=physical_id,
                slot=slot,
                workload=workload,
                takeover=takeover,
                queued=int(runtime.get("queued", 0)),
                inflight=int(runtime.get("inflight", 0)),
                drain_remaining=remaining,
                capacity_safe=bool(safe_after_drain),
            )
        )
        return local_actions, phase, status, blocked_by

    for gpu_id in all_gpu_ids:
        src_gpu = src_map.get(gpu_id)
        tgt_gpu = tgt_map.get(gpu_id)
        change = classify_gpu_change(src_gpu, tgt_gpu)

        if change == "keep_gpu":
            physical_id = get_physical_id(source_state, gpu_id)
            target_pid_map[gpu_id] = physical_id
            coarse_actions.append({"type": "keep_gpu", "gpu_id": gpu_id, "physical_gpu_id": physical_id})
            continue

        if change == "create_gpu":
            physical_id = alloc_from_free_pool(free_pool)
            target_pid_map[gpu_id] = physical_id
            coarse_actions.append(
                {
                    "type": "create_gpu",
                    "gpu_id": gpu_id,
                    "new_physical_gpu_id": physical_id,
                    "template": tgt_gpu.template_str(),
                    "alloc_policy": "free_pool_lifo",
                }
            )
            capacity_actions.extend(
                [
                    {"type": "allocate_gpu", "physical_gpu_id": physical_id, "policy": "free_pool_lifo"},
                    {"type": "configure_full_template", "physical_gpu_id": physical_id, "template": tgt_gpu.template_str()},
                    {"type": "place_target_layout", "gpu_id": gpu_id, "physical_gpu_id": physical_id},
                    {"type": "bind_target_gpu", "gpu_id": gpu_id, "physical_gpu_id": physical_id},
                ]
            )
            plan_items.append(
                _plan_item(
                    item_id=f"CREATE_gpu{gpu_id}",
                    item_type="create_gpu",
                    current_phase="prepare_target_side",
                    status="ready",
                    gpu_id=gpu_id,
                    physical_gpu_id=physical_id,
                    template=tgt_gpu.template_str(),
                )
            )
            continue

        if change == "remove_gpu":
            physical_id = get_physical_id(source_state, gpu_id)
            safe = safe_after_removing_gpu(source_state, src_gpu, required)
            coarse_actions.append({"type": "remove_gpu", "gpu_id": gpu_id, "physical_gpu_id": physical_id, "safe_now": safe})
            slot_blocked = False
            any_stop = False
            any_reroute = False
            any_drain = False
            for inst in _nonfree_instances(src_gpu):
                slot = (inst.start, inst.end, inst.profile)
                acts, phase, status, _ = add_slot_barrier(
                    gpu_id,
                    physical_id,
                    slot,
                    inst.workload,
                    "remove_gpu_slot",
                    f"REMOVE_gpu{gpu_id}_{slot[0]}_{slot[1]}_{slot[2]}",
                    safe_after_drain=safe,
                )
                phase_actions.extend(acts)
                any_stop = any_stop or any(action["type"] == "stop_accepting_new" for action in acts)
                any_reroute = any_reroute or any(action["type"] == "reroute_queued_tasks" for action in acts)
                any_drain = any_drain or phase == "drain_old"
                slot_blocked = slot_blocked or status == "blocked"
            if slot_blocked or any_stop or any_reroute or any_drain:
                phase = "drain_old_gpu" if any_drain else ("reroute_old_queue" if any_reroute else ("stop_accepting_old_gpu" if any_stop else "takeover_ready"))
                blocked_actions.append({"type": "defer_remove_gpu", "gpu_id": gpu_id, "physical_gpu_id": physical_id, "phase": phase})
                plan_items.append(
                    _plan_item(
                        item_id=f"REMOVE_gpu{gpu_id}",
                        item_type="remove_gpu",
                        current_phase=phase,
                        status="blocked" if slot_blocked else "in_progress",
                        blocked_by="waiting_for_slot_drain" if slot_blocked else None,
                        gpu_id=gpu_id,
                        physical_gpu_id=physical_id,
                    )
                )
            else:
                cleanup_actions.extend(
                    [
                        {"type": "clear_gpu", "gpu_id": gpu_id, "physical_gpu_id": physical_id},
                        {"type": "clear_template", "gpu_id": gpu_id, "physical_gpu_id": physical_id},
                    ]
                )
                free_pool.append(physical_id)
                plan_items.append(
                    _plan_item(
                        item_id=f"REMOVE_gpu{gpu_id}",
                        item_type="remove_gpu",
                        current_phase="clear_old_gpu",
                        status="ready",
                        gpu_id=gpu_id,
                        physical_gpu_id=physical_id,
                    )
                )
            continue

        if change == "reconfiguration":
            old_physical_id = get_physical_id(source_state, gpu_id)
            in_place_ok = True
            for inst in _nonfree_instances(src_gpu):
                slot = (inst.start, inst.end, inst.profile)
                candidates = _takeover_candidates(source_state, target_state, inst.workload, exclude_gpu_id=gpu_id, exclude_slot=slot)
                if not any(candidate.get("kind") == "unchanged" for candidate in candidates):
                    in_place_ok = False
                    break
            if in_place_ok:
                new_physical_id = old_physical_id
                prepared = True
                target_pid_map[gpu_id] = old_physical_id
                coarse_actions.append(
                    {
                        "type": "reconfiguration",
                        "gpu_id": gpu_id,
                        "source_physical_gpu_id": old_physical_id,
                        "new_physical_gpu_id": old_physical_id,
                        "src_template": src_gpu.template_str(),
                        "tgt_template": tgt_gpu.template_str(),
                        "mode": "in_place_old_first",
                    }
                )
            else:
                record = _reconfig_map(source_state).get(str(gpu_id))
                if record is None:
                    new_physical_id = alloc_from_free_pool(free_pool)
                    prepared = False
                else:
                    new_physical_id = record.get("physical_gpu_id")
                    prepared = True
                target_pid_map[gpu_id] = new_physical_id
                coarse_actions.append(
                    {
                        "type": "reconfiguration",
                        "gpu_id": gpu_id,
                        "source_physical_gpu_id": old_physical_id,
                        "new_physical_gpu_id": new_physical_id,
                        "src_template": src_gpu.template_str(),
                        "tgt_template": tgt_gpu.template_str(),
                        "alloc_policy": "free_pool_lifo",
                        "mode": "target_first",
                    }
                )
                if not prepared:
                    capacity_actions.extend(
                        [
                            {"type": "allocate_gpu", "physical_gpu_id": new_physical_id, "policy": "free_pool_lifo"},
                            {"type": "configure_full_template", "physical_gpu_id": new_physical_id, "template": tgt_gpu.template_str()},
                            {"type": "place_target_layout", "gpu_id": gpu_id, "physical_gpu_id": new_physical_id},
                            {
                                "type": "mark_reconfig_target_prepared",
                                "gpu_id": gpu_id,
                                "physical_gpu_id": new_physical_id,
                                "template": tgt_gpu.template_str(),
                            },
                        ]
                    )
            slot_blocked = False
            any_stop = False
            any_reroute = False
            any_drain = False
            for inst in _nonfree_instances(src_gpu):
                slot = (inst.start, inst.end, inst.profile)
                acts, phase, status, _ = add_slot_barrier(
                    gpu_id,
                    old_physical_id,
                    slot,
                    inst.workload,
                    "reconfiguration_slot",
                    f"RECONF_gpu{gpu_id}_{slot[0]}_{slot[1]}_{slot[2]}",
                    safe_after_drain=True,
                )
                phase_actions.extend(acts)
                any_stop = any_stop or any(action["type"] == "stop_accepting_new" for action in acts)
                any_reroute = any_reroute or any(action["type"] == "reroute_queued_tasks" for action in acts)
                any_drain = any_drain or phase == "drain_old"
                slot_blocked = slot_blocked or status == "blocked"
            if slot_blocked or any_stop or any_reroute or any_drain:
                if in_place_ok:
                    phase = "drain_old_side" if any_drain else ("reroute_old_queue" if any_reroute else ("shift_routing" if any_stop else "in_place_reconfigure"))
                else:
                    phase = "drain_old_side" if any_drain else ("reroute_old_queue" if any_reroute else ("shift_routing" if any_stop else ("prepare_target_side" if not prepared else "target_side_prepared")))
                blocked_actions.append({"type": "defer_remove_gpu", "gpu_id": gpu_id, "physical_gpu_id": old_physical_id, "phase": phase})
                plan_items.append(
                    _plan_item(
                        item_id=f"RECONF_gpu{gpu_id}",
                        item_type="reconfiguration",
                        current_phase=phase,
                        status="blocked" if slot_blocked else "in_progress",
                        blocked_by="waiting_for_old_side_drain" if slot_blocked else None,
                        gpu_id=gpu_id,
                        physical_gpu_id=old_physical_id,
                        target_physical_gpu_id=new_physical_id,
                    )
                )
            else:
                cleanup_actions.extend(
                    [
                        {"type": "clear_gpu", "gpu_id": gpu_id, "physical_gpu_id": old_physical_id},
                        {"type": "clear_template", "gpu_id": gpu_id, "physical_gpu_id": old_physical_id},
                    ]
                )
                if in_place_ok:
                    finalize_actions.extend(
                        [
                            {"type": "configure_full_template", "physical_gpu_id": old_physical_id, "template": tgt_gpu.template_str()},
                            {"type": "place_target_layout", "gpu_id": gpu_id, "physical_gpu_id": old_physical_id},
                            {"type": "bind_target_gpu", "gpu_id": gpu_id, "physical_gpu_id": old_physical_id},
                        ]
                    )
                    plan_items.append(
                        _plan_item(
                            item_id=f"RECONF_gpu{gpu_id}",
                            item_type="reconfiguration",
                            current_phase="in_place_reconfigure",
                            status="ready",
                            gpu_id=gpu_id,
                            physical_gpu_id=old_physical_id,
                            target_physical_gpu_id=old_physical_id,
                        )
                    )
                else:
                    finalize_actions.append({"type": "bind_target_gpu", "gpu_id": gpu_id, "physical_gpu_id": new_physical_id})
                    free_pool.append(old_physical_id)
                    plan_items.append(
                        _plan_item(
                            item_id=f"RECONF_gpu{gpu_id}",
                            item_type="reconfiguration",
                            current_phase="finalize_target_side",
                            status="ready",
                            gpu_id=gpu_id,
                            physical_gpu_id=old_physical_id,
                            target_physical_gpu_id=new_physical_id,
                        )
                    )
            continue

        if change == "instance_diff":
            physical_id = get_physical_id(source_state, gpu_id)
            target_pid_map[gpu_id] = physical_id
            inst_actions = diff_instances_within_same_template(src_gpu, tgt_gpu)
            coarse_actions.append(
                {
                    "type": "instance_diff",
                    "gpu_id": gpu_id,
                    "physical_gpu_id": physical_id,
                    "template": tgt_gpu.template_str(),
                    "instance_changes": [action["type"] for action in inst_actions],
                }
            )
            for inst_action in inst_actions:
                slot = inst_action["slot"]
                if inst_action["type"] == "keep":
                    continue
                if inst_action["type"] == "batch_change":
                    capacity_actions.append(
                        {
                            "type": "update_batch",
                            "gpu_id": gpu_id,
                            "physical_gpu_id": physical_id,
                            "slot": slot,
                            "old_batch": inst_action["src"].batch,
                            "new_batch": inst_action["tgt"].batch,
                            "workload": inst_action["src"].workload,
                        }
                    )
                    continue
                if inst_action["type"] == "place_instance":
                    capacity_actions.append(
                        {
                            "type": "place_instance",
                            "gpu_id": gpu_id,
                            "physical_gpu_id": physical_id,
                            "slot": slot,
                            "workload": inst_action["tgt"].workload,
                            "batch": inst_action["tgt"].batch,
                        }
                    )
                    plan_items.append(
                        _plan_item(
                            item_id=f"PLACE_gpu{gpu_id}_{slot[0]}_{slot[1]}_{slot[2]}",
                            item_type="place_instance",
                            current_phase="prepare_target_side",
                            status="ready",
                            gpu_id=gpu_id,
                            physical_gpu_id=physical_id,
                            slot=slot,
                            workload=inst_action["tgt"].workload,
                        )
                    )
                    continue
                if inst_action["type"] == "safe_remove_instance":
                    safe = safe_after_removing_instance(source_state, inst_action["src"], required)
                    acts, phase, status, _ = add_slot_barrier(
                        gpu_id,
                        physical_id,
                        slot,
                        inst_action["src"].workload,
                        "remove_instance",
                        f"RM_gpu{gpu_id}_{slot[0]}_{slot[1]}_{slot[2]}",
                        safe_after_drain=safe,
                    )
                    phase_actions.extend(acts)
                    if status == "ready":
                        cleanup_actions.append(
                            {
                                "type": "remove_instance",
                                "gpu_id": gpu_id,
                                "physical_gpu_id": physical_id,
                                "slot": slot,
                                "workload": inst_action["src"].workload,
                                "safe_now": True,
                                "drained": True,
                            }
                        )
                        plan_items[-1]["current_phase"] = "remove_old_instance"
                    else:
                        blocked_actions.append(
                            {
                                "type": "defer_remove_instance",
                                "gpu_id": gpu_id,
                                "physical_gpu_id": physical_id,
                                "slot": slot,
                                "workload": inst_action["src"].workload,
                                "phase": phase,
                            }
                        )
                    continue
                if inst_action["type"] == "workload_change":
                    safe = safe_after_removing_instance(source_state, inst_action["src"], required)
                    acts, phase, status, _ = add_slot_barrier(
                        gpu_id,
                        physical_id,
                        slot,
                        inst_action["src"].workload,
                        "workload_change",
                        f"WC_gpu{gpu_id}_{slot[0]}_{slot[1]}_{slot[2]}",
                        safe_after_drain=safe,
                    )
                    phase_actions.extend(acts)
                    if status == "ready":
                        cleanup_actions.append(
                            {
                                "type": "remove_instance",
                                "gpu_id": gpu_id,
                                "physical_gpu_id": physical_id,
                                "slot": slot,
                                "workload": inst_action["src"].workload,
                                "safe_now": True,
                                "drained": True,
                            }
                        )
                        capacity_actions.append(
                            {
                                "type": "place_instance",
                                "gpu_id": gpu_id,
                                "physical_gpu_id": physical_id,
                                "slot": slot,
                                "workload": inst_action["tgt"].workload,
                                "batch": inst_action["tgt"].batch,
                                "after_drain": True,
                            }
                        )
                        plan_items[-1]["current_phase"] = "replace_slot"
                    else:
                        bridge_slot = None
                        if safe:
                            bridge_slot = None
                        elif status == "blocked" and phase == "takeover_ready":
                            bridge_slot = find_active_bridge_slot(
                                source_state=source_state,
                                target_state=target_state,
                                profile=inst_action["src"].profile,
                                avoid_gpu_id=gpu_id,
                                avoid_slot=slot,
                            )
                        if bridge_slot is not None:
                            bridge_pid = get_physical_id(source_state, bridge_slot["gpu_id"])
                            capacity_actions.extend(
                                [
                                    {
                                        "type": "bridge_place_instance",
                                        "gpu_id": int(bridge_slot["gpu_id"]),
                                        "physical_gpu_id": bridge_pid,
                                        "slot": bridge_slot["slot"],
                                        "workload": inst_action["src"].workload,
                                        "batch": inst_action["src"].batch,
                                        "mu": float(inst_action["src"].mu),
                                        "bridge_for": {
                                            "gpu_id": gpu_id,
                                            "slot": slot,
                                            "old_workload": inst_action["src"].workload,
                                            "new_workload": inst_action["tgt"].workload,
                                        },
                                    },
                                    {
                                        "type": "remove_instance",
                                        "gpu_id": gpu_id,
                                        "physical_gpu_id": physical_id,
                                        "slot": slot,
                                        "workload": inst_action["src"].workload,
                                        "safe_now": True,
                                        "bridged": True,
                                    },
                                    {
                                        "type": "place_instance",
                                        "gpu_id": gpu_id,
                                        "physical_gpu_id": physical_id,
                                        "slot": slot,
                                        "workload": inst_action["tgt"].workload,
                                        "batch": inst_action["tgt"].batch,
                                        "bridged": True,
                                    },
                                ]
                            )
                            plan_items[-1]["current_phase"] = "bridge_then_replace"
                            plan_items[-1]["status"] = "ready"
                            plan_items[-1]["blocked_by"] = None
                        else:
                            blocked_actions.append(
                                {
                                    "type": "defer_workload_change",
                                    "gpu_id": gpu_id,
                                    "physical_gpu_id": physical_id,
                                    "slot": slot,
                                    "old_workload": inst_action["src"].workload,
                                    "new_workload": inst_action["tgt"].workload,
                                    "phase": phase,
                                }
                            )
                    continue

    fine_actions = capacity_actions + phase_actions + cleanup_actions + finalize_actions + blocked_actions
    planned_state = deepcopy_state(target_state)
    ensure_state_metadata(planned_state)
    planned_state.metadata["physical_id_map"] = {int(gpu_id): physical_id for gpu_id, physical_id in target_pid_map.items()}
    planned_state.metadata["next_physical_idx"] = source_state.metadata.get("next_physical_idx", 0)
    executed_state = simulate_v3_actions(
        source_state=source_state,
        target_state=planned_state,
        fine_actions=fine_actions,
        next_physical_idx=source_state.metadata.get("next_physical_idx", 0),
    )
    executed_actions, blocked = _split_actions(fine_actions)
    return {
        "stage_name": stage_name,
        "required": required,
        "coarse_actions": coarse_actions,
        "fine_actions": fine_actions,
        "executed_actions": executed_actions,
        "blocked_actions": blocked,
        "planned_state": planned_state,
        "executed_state": executed_state,
        "workload_classes": workload_classes,
        "free_pool_after_plan": list(free_pool),
        "plan_items": plan_items,
    }


def _takeover_ready_score(source_state: ClusterState, target_state: ClusterState, item: dict[str, Any]) -> int:
    workload = item.get("workload")
    if not workload:
        return 0
    gpu_id = int(item.get("gpu_id")) if item.get("gpu_id") is not None else None
    slot = tuple(item.get("slot")) if item.get("slot") is not None else None
    candidates = _takeover_candidates(source_state, target_state, workload, exclude_gpu_id=gpu_id, exclude_slot=slot)
    score = 0
    for candidate in candidates:
        score = max(score, 3 if candidate.get("kind") == "unchanged" else 2)
    return score


def _capacity_headroom_score(source_state: ClusterState, target_state: ClusterState, item: dict[str, Any]) -> int:
    workload = item.get("workload")
    if not workload:
        return 0
    gpu_id = int(item.get("gpu_id")) if item.get("gpu_id") is not None else None
    slot = tuple(item.get("slot")) if item.get("slot") is not None else None
    candidates = _takeover_candidates(source_state, target_state, workload, exclude_gpu_id=gpu_id, exclude_slot=slot)
    stable = sum(1 for candidate in candidates if candidate.get("kind") == "unchanged")
    return min(stable, 2)


def _peak_gpu_delta_cost(item: dict[str, Any]) -> int:
    item_type = str(item.get("type", ""))
    phase = str(item.get("current_phase", ""))
    if item_type == "create_gpu":
        return 1
    if item_type == "reconfiguration" and phase == "prepare_target_side":
        return 1
    if item_type == "reconfiguration" and phase == "in_place_reconfigure":
        return 0
    return 0


def _drain_wait_cost(item: dict[str, Any]) -> int:
    remaining = item.get("drain_remaining")
    if remaining is not None:
        return int(remaining)
    inflight = item.get("inflight")
    if inflight is not None and "drain" in str(item.get("current_phase", "")):
        return int(inflight)
    return 0


def _unlock_count_score(items: list[dict[str, Any]], item: dict[str, Any]) -> int:
    item_type = str(item.get("type", ""))
    phase = str(item.get("current_phase", ""))
    if item_type == "create_gpu":
        return sum(1 for other in items if other.get("blocked_by") == "no_takeover_capacity")
    if item_type == "reconfiguration" and phase in {"clear_old_side", "in_place_reconfigure", "finalize_target_side"}:
        return sum(1 for other in items if other.get("status") == "blocked")
    if item_type in {"remove_gpu", "remove_gpu_slot", "workload_change", "remove_instance"} and "drain" in phase:
        return 1
    return 0


def _release_gpu_score(item: dict[str, Any]) -> int:
    item_type = str(item.get("type", ""))
    phase = str(item.get("current_phase", ""))
    if item_type == "remove_gpu" and phase == "clear_old_gpu":
        return 3
    if item_type == "reconfiguration" and phase in {"clear_old_side", "in_place_reconfigure", "finalize_target_side"}:
        return 2
    if item_type in {"workload_change", "remove_instance", "remove_gpu_slot", "reconfiguration_slot"} and phase == "post_drain_ready":
        return 1
    return 0


def _target_backed_enable_score(item: dict[str, Any]) -> int:
    item_type = str(item.get("type", ""))
    phase = str(item.get("current_phase", ""))
    if item_type == "create_gpu" and phase == "prepare_target_side":
        return 2
    if item_type == "reconfiguration" and phase == "prepare_target_side":
        return 2
    if item_type == "place_instance" and phase == "prepare_target_side":
        return 1
    return 0


def _in_place_reconfig_score(item: dict[str, Any]) -> int:
    return 3 if str(item.get("type", "")) == "reconfiguration" and str(item.get("current_phase", "")) == "in_place_reconfigure" else 0


def score_v3_plan_item(
    source_state: ClusterState,
    target_state: ClusterState,
    items: list[dict[str, Any]],
    item: dict[str, Any],
) -> dict[str, Any]:
    takeover_ready = _takeover_ready_score(source_state, target_state, item)
    capacity_headroom = _capacity_headroom_score(source_state, target_state, item)
    peak_gpu_delta = _peak_gpu_delta_cost(item)
    drain_wait = _drain_wait_cost(item)
    unlock_count = _unlock_count_score(items, item)
    release_gpu = _release_gpu_score(item)
    target_backed_enable = _target_backed_enable_score(item)
    in_place_bonus = _in_place_reconfig_score(item)
    total = (
        3 * takeover_ready
        + 2 * capacity_headroom
        + 2 * unlock_count
        + 3 * release_gpu
        + 2 * target_backed_enable
        + 2 * in_place_bonus
        - 4 * peak_gpu_delta
        - 2 * drain_wait
    )
    return {
        "id": item.get("id"),
        "type": item.get("type"),
        "phase": item.get("current_phase"),
        "status": item.get("status"),
        "score": total,
        "takeover_ready": takeover_ready,
        "capacity_headroom": capacity_headroom,
        "peak_gpu_delta": peak_gpu_delta,
        "drain_wait": drain_wait,
        "unlock_count": unlock_count,
        "release_gpu": release_gpu,
        "target_backed_enable": target_backed_enable,
        "in_place_bonus": in_place_bonus,
    }


def _root_id_from_item(item: dict[str, Any]) -> str:
    item_id = str(item.get("id", ""))
    if item_id.startswith("RECONF_gpu"):
        parts = item_id.split("_")
        return "_".join(parts[:2]) if len(parts) >= 2 else item_id
    if item_id.startswith("REMOVE_gpu"):
        parts = item_id.split("_")
        return "_".join(parts[:2]) if len(parts) >= 2 else item_id
    if item_id.startswith("CREATE_gpu"):
        return item_id
    if item_id.startswith(("WC_gpu", "RM_gpu", "PLACE_gpu")):
        parts = item_id.split("_")
        return "_".join(parts[:5]) if len(parts) >= 5 else item_id
    return item_id


def _group_scores(
    source_state: ClusterState,
    target_state: ClusterState,
    plan_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    scored = [score_v3_plan_item(source_state, target_state, plan_items, item) for item in plan_items]
    groups = {}
    for row, item in zip(scored, plan_items):
        root = _root_id_from_item(item)
        group = groups.setdefault(root, {"root_id": root, "score": 0, "rows": [], "gpu_id": item.get("gpu_id")})
        group["score"] += row["score"]
        group["rows"].append(row)
    out = list(groups.values())
    out.sort(key=lambda group: group["score"], reverse=True)
    return out


def _action_matches_root(action: dict[str, Any], root_id: str) -> bool:
    if root_id.startswith(("CREATE_gpu", "RECONF_gpu", "REMOVE_gpu")):
        gpu_id = int(root_id.split("gpu", 1)[1])
        return action.get("gpu_id") == gpu_id
    if root_id.startswith(("WC_gpu", "RM_gpu", "PLACE_gpu")):
        parts = root_id.split("_")
        gpu_id = int(parts[1].replace("gpu", ""))
        slot = (int(parts[2]), int(parts[3]), parts[4])
        return action.get("gpu_id") == gpu_id and tuple(action.get("slot", (None, None, None))) == slot
    return False


def _select_actions_for_root(plan: dict[str, Any], root_id: str) -> list[dict[str, Any]]:
    actions = []
    pending_prefix = []
    for action in plan.get("executed_actions", []):
        action_type = action.get("type")
        if action_type in {"allocate_gpu", "configure_full_template"}:
            pending_prefix.append(action)
            continue
        if action_type == "place_target_layout":
            gpu_id = action.get("gpu_id")
            if root_id.startswith(("CREATE_gpu", "RECONF_gpu")) and int(root_id.split("gpu", 1)[1]) == gpu_id:
                actions.extend(pending_prefix)
                pending_prefix = []
                actions.append(action)
            else:
                pending_prefix = []
            continue
        if _action_matches_root(action, root_id):
            actions.append(action)
        else:
            pending_prefix = []
    return actions


def _groups_conflict(group_1: dict[str, Any], group_2: dict[str, Any]) -> bool:
    gpu_id_1 = group_1.get("gpu_id")
    gpu_id_2 = group_2.get("gpu_id")
    if gpu_id_1 is not None and gpu_id_2 is not None and gpu_id_1 == gpu_id_2:
        return True
    root_1 = str(group_1.get("root_id", ""))
    root_2 = str(group_2.get("root_id", ""))
    if root_1.startswith("PLACE_gpu") and root_2.startswith("WC_gpu"):
        return root_1.split("_")[1:5] == root_2.split("_")[1:5]
    if root_2.startswith("PLACE_gpu") and root_1.startswith("WC_gpu"):
        return root_2.split("_")[1:5] == root_1.split("_")[1:5]
    return False


def _choose_nonconflicting_groups(groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    chosen = []
    positive_exists = any(group.get("score", 0) > 0 for group in groups)
    threshold = 1 if positive_exists else 0
    for group in groups:
        if group.get("score", 0) < threshold:
            continue
        if any(_groups_conflict(group, existing) for existing in chosen):
            continue
        chosen.append(group)
    return chosen if chosen else ([] if not groups else [groups[0]])


def run_v3_stage_iterative(
    source_state: ClusterState,
    target_state: ClusterState,
    src_arrival: list[float] | tuple[float, ...] | dict[str, float],
    tgt_arrival: list[float] | tuple[float, ...] | dict[str, float],
    workload_names: list[str] | tuple[str, ...] | None = None,
    stage_name: str = "stage_v3",
    max_iters: int = 20,
) -> dict[str, Any]:
    current_state = prepare_v3_source_runtime(source_state, target_state)
    ensure_state_metadata(current_state)
    initial_runtime_state = deepcopy_state(current_state)
    iterations = []
    all_executed_actions = []
    final_plan = None
    reached_target = matches_target_state(current_state, target_state) and len(_drain_map(current_state)) == 0
    peak_active_gpu = len(_active_pid_set(current_state))
    start = time.perf_counter()

    for iter_idx in range(1, max_iters + 1):
        if reached_target:
            break
        _advance_drain(current_state)
        full_plan = plan_v3_full_action_plan(
            current_state,
            target_state,
            src_arrival,
            tgt_arrival,
            workload_names=workload_names,
            stage_name=f"{stage_name}_iter{iter_idx}",
        )
        final_plan = full_plan
        plan_items = list(full_plan.get("plan_items", []))
        groups = _group_scores(current_state, target_state, plan_items)
        if not groups:
            break
        chosen_groups = _choose_nonconflicting_groups(groups)
        chosen_actions = []
        for group in chosen_groups:
            chosen_actions.extend(_select_actions_for_root(full_plan, group["root_id"]))

        dedup = []
        seen = set()
        for action in chosen_actions:
            key = json.dumps(action, sort_keys=True, default=str)
            if key in seen:
                continue
            seen.add(key)
            dedup.append(action)
        chosen_actions = dedup

        next_state = simulate_v3_actions(
            current_state,
            full_plan["planned_state"],
            chosen_actions,
            next_physical_idx=current_state.metadata.get("next_physical_idx", 0),
        )
        made_progress = _progress_signature(next_state) != _progress_signature(current_state)
        reached_target = matches_target_state(next_state, target_state) and len(_drain_map(next_state)) == 0
        iter_peak = _peak_from_actions(current_state, chosen_actions)
        peak_active_gpu = max(peak_active_gpu, iter_peak, len(_active_pid_set(next_state)))
        iterations.append(
            {
                "iteration": iter_idx,
                "full_plan": full_plan,
                "chosen_roots": chosen_groups,
                "chosen_actions": chosen_actions,
                "state_before": deepcopy_state(current_state),
                "state_after": deepcopy_state(next_state),
                "made_progress": made_progress,
                "reached_target": reached_target,
                "iter_peak_active_gpu": iter_peak,
                "active_gpu_after": len(_active_pid_set(next_state)),
            }
        )
        all_executed_actions.extend(chosen_actions)
        current_state = next_state
        if reached_target or not made_progress:
            break

    elapsed_sec = time.perf_counter() - start
    return {
        "stage_name": stage_name,
        "iterations": iterations,
        "iteration_count": len(iterations),
        "reached_target": reached_target,
        "elapsed_sec": elapsed_sec,
        "executed_actions": all_executed_actions,
        "executed_state": current_state,
        "target_state": deepcopy_state(target_state),
        "initial_runtime_state": initial_runtime_state,
        "peak_active_gpu": peak_active_gpu,
        "source_active_gpu": len(_active_pid_set(initial_runtime_state)),
        "final_active_gpu": len(_active_pid_set(current_state)),
        "final_plan": final_plan,
    }
