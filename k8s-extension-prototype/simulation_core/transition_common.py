from __future__ import annotations

import copy
from collections import defaultdict
from typing import Any

from .physical_ids import (
    PHYSICAL_ID_POOL,
    bootstrap_physical_ids_for_state,
    ensure_state_metadata,
    get_physical_id,
    remove_gpu_if_bound_to_physical_id,
)
from .state import (
    ClusterState,
    GPUState,
    MigInstance,
    PROFILE_SIZE,
    copy_inst_payload,
    deepcopy_state,
    get_inst_by_slot,
    gpu_map_by_id,
    replace_or_append_gpu,
)


def gpu_template_signature(gpu: GPUState) -> tuple[int, ...]:
    gpu.sort_instances()
    return tuple(sorted(PROFILE_SIZE[inst.profile] for inst in gpu.instances if inst.profile != "void"))


def same_template(src_gpu: GPUState | None, tgt_gpu: GPUState | None) -> bool:
    if src_gpu is None or tgt_gpu is None:
        return False
    return gpu_template_signature(src_gpu) == gpu_template_signature(tgt_gpu)


def slot_key(inst: MigInstance) -> tuple[int, int, str]:
    return inst.start, inst.end, inst.profile


def instance_payload(inst: MigInstance) -> tuple[str | None, int | None]:
    return inst.workload, inst.batch


def classify_gpu_change(src_gpu: GPUState | None, tgt_gpu: GPUState | None) -> str:
    if src_gpu is None and tgt_gpu is not None:
        return "create_gpu"
    if src_gpu is not None and tgt_gpu is None:
        return "remove_gpu"
    if src_gpu is None and tgt_gpu is None:
        return "none"
    if same_template(src_gpu, tgt_gpu):
        src_slots = {slot_key(inst): instance_payload(inst) for inst in src_gpu.instances}
        tgt_slots = {slot_key(inst): instance_payload(inst) for inst in tgt_gpu.instances}
        if src_slots == tgt_slots:
            return "keep_gpu"
        return "instance_diff"
    return "reconfiguration"


def diff_instances_within_same_template(
    src_gpu: GPUState,
    tgt_gpu: GPUState,
) -> list[dict[str, Any]]:
    src_by_slot = {slot_key(inst): inst for inst in src_gpu.instances}
    tgt_by_slot = {slot_key(inst): inst for inst in tgt_gpu.instances}
    all_slots = sorted(set(src_by_slot) | set(tgt_by_slot))
    out = []
    for slot in all_slots:
        src_inst = src_by_slot.get(slot)
        tgt_inst = tgt_by_slot.get(slot)
        if src_inst is None and tgt_inst is not None:
            out.append({"type": "instance_create", "slot": slot, "src": None, "tgt": tgt_inst})
            continue
        if src_inst is not None and tgt_inst is None:
            out.append({"type": "instance_remove", "slot": slot, "src": src_inst, "tgt": None})
            continue
        if src_inst.workload == tgt_inst.workload and src_inst.batch == tgt_inst.batch:
            out.append({"type": "keep", "slot": slot, "src": src_inst, "tgt": tgt_inst})
        elif src_inst.workload == tgt_inst.workload and src_inst.workload is not None and src_inst.batch != tgt_inst.batch:
            out.append({"type": "batch_change", "slot": slot, "src": src_inst, "tgt": tgt_inst})
        elif tgt_inst.workload is None:
            out.append({"type": "safe_remove_instance", "slot": slot, "src": src_inst, "tgt": tgt_inst})
        elif src_inst.workload is None:
            out.append({"type": "place_instance", "slot": slot, "src": src_inst, "tgt": tgt_inst})
        else:
            out.append({"type": "workload_change", "slot": slot, "src": src_inst, "tgt": tgt_inst})
    return out


def arrival_dict_from_vector(
    arrival_vector: list[float] | tuple[float, ...],
    workload_names: list[str] | tuple[str, ...],
) -> dict[str, float]:
    if len(arrival_vector) != len(workload_names):
        raise ValueError(
            f"arrival_vector length ({len(arrival_vector)}) does not match workload_names length ({len(workload_names)})"
        )
    return {str(workload): float(arrival_vector[idx]) for idx, workload in enumerate(workload_names)}


def required_arrival_dict(
    src_arrival: list[float] | tuple[float, ...] | dict[str, float],
    tgt_arrival: list[float] | tuple[float, ...] | dict[str, float],
    workload_names: list[str] | tuple[str, ...] | None = None,
) -> dict[str, float]:
    if isinstance(src_arrival, dict):
        src_dict = {str(k): float(v) for k, v in src_arrival.items()}
    else:
        if workload_names is None:
            raise ValueError("workload_names is required for vector src_arrival")
        src_dict = arrival_dict_from_vector(src_arrival, workload_names)

    if isinstance(tgt_arrival, dict):
        tgt_dict = {str(k): float(v) for k, v in tgt_arrival.items()}
    else:
        if workload_names is None:
            raise ValueError("workload_names is required for vector tgt_arrival")
        tgt_dict = arrival_dict_from_vector(tgt_arrival, workload_names)

    names = list(workload_names) if workload_names is not None else sorted(set(src_dict) | set(tgt_dict))
    return {str(workload): min(src_dict.get(workload, 0.0), tgt_dict.get(workload, 0.0)) for workload in names}


def provided_by_workload(state: ClusterState) -> dict[str, float]:
    provided = defaultdict(float)
    for gpu in state.real_gpus():
        for inst in gpu.instances:
            if inst.workload is not None:
                provided[inst.workload] += float(inst.mu)
    return dict(provided)


def safe_after_removing_instance(
    state: ClusterState,
    inst: MigInstance,
    required: dict[str, float],
) -> bool:
    if inst.workload is None:
        return True
    provided = provided_by_workload(state)
    provided[inst.workload] = provided.get(inst.workload, 0.0) - float(inst.mu)
    return provided.get(inst.workload, 0.0) + 1e-9 >= required.get(inst.workload, 0.0)


def safe_after_removing_gpu(
    state: ClusterState,
    gpu: GPUState,
    required: dict[str, float],
) -> bool:
    provided = provided_by_workload(state)
    for inst in gpu.instances:
        if inst.workload is not None:
            provided[inst.workload] = provided.get(inst.workload, 0.0) - float(inst.mu)
    for workload, required_rate in required.items():
        if provided.get(workload, 0.0) + 1e-9 < required_rate:
            return False
    return True


def find_free_profile_slots(state: ClusterState) -> list[dict[str, Any]]:
    slots = []
    for gpu in state.real_gpus():
        for inst in gpu.instances:
            if inst.workload is None:
                slots.append(
                    {
                        "gpu_id": gpu.gpu_id,
                        "slot": (inst.start, inst.end, inst.profile),
                        "profile": inst.profile,
                        "size": PROFILE_SIZE[inst.profile],
                        "inst": inst,
                    }
                )
    return slots


def find_active_bridge_slot(
    source_state: ClusterState,
    target_state: ClusterState,
    profile: str,
    avoid_gpu_id: int | None = None,
    avoid_slot: tuple[int, int, str] | None = None,
) -> dict[str, Any] | None:
    target_map = gpu_map_by_id(target_state)
    for candidate in find_free_profile_slots(source_state):
        if candidate["profile"] != profile:
            continue
        if avoid_gpu_id is not None and int(candidate["gpu_id"]) == int(avoid_gpu_id) and candidate["slot"] == avoid_slot:
            continue
        target_gpu = target_map.get(int(candidate["gpu_id"]))
        target_inst = get_inst_by_slot(target_gpu, candidate["slot"]) if target_gpu is not None else None
        if target_inst is None:
            continue
        if target_inst.workload is not None:
            continue
        return candidate
    return None


def build_initial_free_pool(state: ClusterState) -> list[str]:
    bootstrap_physical_ids_for_state(state)
    active = {get_physical_id(state, gpu.gpu_id) for gpu in state.real_gpus()}
    return [physical_id for physical_id in reversed(PHYSICAL_ID_POOL) if physical_id not in active]


def alloc_from_free_pool(free_pool: list[str]) -> str:
    if not free_pool:
        raise RuntimeError("Out of free physical GPUs in A-Z")
    return free_pool.pop()


def state_semantic_signature(state: ClusterState) -> tuple:
    rows = []
    for gpu in sorted(state.real_gpus(), key=lambda x: x.gpu_id):
        inst_rows = []
        for inst in sorted(gpu.instances, key=lambda x: (x.start, x.end, x.profile)):
            inst_rows.append(
                (
                    int(inst.start),
                    int(inst.end),
                    inst.profile,
                    inst.workload,
                    None if inst.batch is None else int(inst.batch),
                )
            )
        rows.append((int(gpu.gpu_id), tuple(inst_rows)))
    return tuple(rows)


def gpu_semantic_signature(gpu: GPUState | None) -> tuple | None:
    if gpu is None:
        return None
    inst_rows = []
    for inst in sorted(gpu.instances, key=lambda x: (x.start, x.end, x.profile)):
        inst_rows.append(
            (
                int(inst.start),
                int(inst.end),
                inst.profile,
                inst.workload,
                None if inst.batch is None else int(inst.batch),
            )
        )
    return tuple(inst_rows)


def matches_target_state(state: ClusterState, target_state: ClusterState) -> bool:
    return state_semantic_signature(state) == state_semantic_signature(target_state)


def mismatched_gpu_ids(state: ClusterState, target_state: ClusterState) -> list[int]:
    cur_map = gpu_map_by_id(state)
    target_map = gpu_map_by_id(target_state)
    all_ids = sorted(set(cur_map) | set(target_map))
    return [
        int(gpu_id)
        for gpu_id in all_ids
        if gpu_semantic_signature(cur_map.get(gpu_id)) != gpu_semantic_signature(target_map.get(gpu_id))
    ]


def get_gpu_by_id_mut(state: ClusterState, gpu_id: int) -> GPUState | None:
    for gpu in state.real_gpus():
        if int(gpu.gpu_id) == int(gpu_id):
            return gpu
    return None


def remove_gpu_if_bound_to_pid(
    state: ClusterState,
    gpu_id: int,
    physical_gpu_id: str,
) -> None:
    remove_gpu_if_bound_to_physical_id(state, gpu_id, physical_gpu_id)


def simulate_basic_fine_actions(
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

        if action_type == "bind_target_gpu":
            gpu_id = int(action["gpu_id"])
            physical_id = action["physical_gpu_id"]
            target_gpu = target_map.get(gpu_id)
            if target_gpu is None:
                continue
            replace_or_append_gpu(executed_state, copy.deepcopy(target_gpu))
            executed_state.metadata["physical_id_map"][gpu_id] = physical_id
            continue

        if action_type == "clear_gpu":
            remove_gpu_if_bound_to_pid(executed_state, int(action["gpu_id"]), action["physical_gpu_id"])
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
            continue

        if action_type in {"update_batch", "place_instance", "workload_change"}:
            if cur_inst is not None and target_inst is not None:
                copy_inst_payload(cur_inst, target_inst)
            continue

        if action_type == "remove_instance":
            if cur_inst is not None:
                copy_inst_payload(cur_inst, None)
            continue

    executed_state.gpus = sorted(executed_state.real_gpus(), key=lambda x: x.gpu_id)
    executed_state.metadata["next_physical_idx"] = int(next_physical_idx)
    return executed_state
