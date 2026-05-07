from __future__ import annotations

from collections import Counter
from typing import Any

from .state import ClusterState, GPUState, MigInstance, PROFILE_SIZE
from .templates import all_unique_physical_realizations


_OLD_SLOT_MAP_CACHE: dict[int, dict[tuple[int, int, int, str], MigInstance]] = {}
_PREV_INTERVALS_CACHE: dict[tuple[int, int], list[tuple[int, int, str]]] = {}


def has_prev(prev_state: ClusterState | None) -> bool:
    return prev_state is not None and len(prev_state.real_gpus()) > 0


def old_exact_slot_map(prev_state: ClusterState | None) -> dict[tuple[int, int, int, str], MigInstance]:
    if prev_state is None:
        return {}
    key = id(prev_state)
    if key in _OLD_SLOT_MAP_CACHE:
        return _OLD_SLOT_MAP_CACHE[key]
    out = {}
    for gpu in prev_state.real_gpus():
        for inst in gpu.instances:
            if inst.profile == "void":
                continue
            out[(gpu.gpu_id, inst.start, inst.end, inst.profile)] = inst
    _OLD_SLOT_MAP_CACHE[key] = out
    return out


def get_prev_gpu_intervals(prev_state: ClusterState | None, gpu_id: int) -> list[tuple[int, int, str]]:
    if prev_state is None:
        return []
    for gpu in prev_state.real_gpus():
        if int(gpu.gpu_id) == int(gpu_id):
            return [
                (inst.start, inst.end, inst.profile)
                for inst in sorted(gpu.instances, key=lambda x: (x.start, x.end))
            ]
    return []


def get_prev_gpu_intervals_cached(prev_state: ClusterState | None, gpu_id: int) -> list[tuple[int, int, str]]:
    key = (id(prev_state), int(gpu_id))
    if key not in _PREV_INTERVALS_CACHE:
        _PREV_INTERVALS_CACHE[key] = get_prev_gpu_intervals(prev_state, gpu_id)
    return _PREV_INTERVALS_CACHE[key]


def layout_score_vs_prev(
    intervals: list[tuple[int, int, str]],
    prev_intervals: list[tuple[int, int, str]],
) -> tuple[int, int, int, int]:
    exact = 0
    prefix = 0
    overlap_same_profile = 0

    new_nonvoid = [(s, e, p) for (s, e, p) in intervals if p != "void"]
    old_nonvoid = [(s, e, p) for (s, e, p) in prev_intervals if p != "void"]

    for new_interval in new_nonvoid:
        for old_interval in old_nonvoid:
            if new_interval == old_interval:
                exact += 1

    for idx in range(min(len(new_nonvoid), len(old_nonvoid))):
        if new_nonvoid[idx] == old_nonvoid[idx]:
            prefix += 1

    for s1, e1, p1 in new_nonvoid:
        for s2, e2, p2 in old_nonvoid:
            if p1 != p2:
                continue
            overlap_same_profile += max(0, min(e1, e2) - max(s1, s2))

    return (
        int(exact),
        int(len(new_nonvoid) == len(old_nonvoid)),
        int(prefix),
        int(overlap_same_profile),
    )


def physical_layout_candidates_for_gpu(
    abstract_template: str,
    gpu_id: int,
    prev_state: ClusterState | None,
    topk: int = 4,
) -> list[tuple[str, list[tuple[int, int, str]], tuple[int, int, int, int]]]:
    prev_intervals = get_prev_gpu_intervals(prev_state, gpu_id)
    candidates = []
    for physical_name, intervals in all_unique_physical_realizations(abstract_template):
        score = layout_score_vs_prev(intervals, prev_intervals)
        candidates.append((physical_name, intervals, score))
    candidates.sort(key=lambda x: (x[2], x[0]), reverse=True)
    return candidates[:topk]


def slot_preserve_match(
    slot: dict[str, Any],
    demand: dict[str, Any],
    prev_state: ClusterState | None,
    old_map: dict[tuple[int, int, int, str], MigInstance] | None = None,
) -> bool:
    if old_map is None:
        old_map = old_exact_slot_map(prev_state)
    old = old_map.get((slot["gpu_id"], slot["start"], slot["end"], slot["profile"]))
    if old is None:
        return False
    return old.workload == demand["workload"] and old.profile == demand["profile"]


def slot_upgrade_preserve_match(
    slot: dict[str, Any],
    demand: dict[str, Any],
    prev_state: ClusterState | None,
    old_map: dict[tuple[int, int, int, str], MigInstance] | None = None,
) -> bool:
    if demand["profile"] != "3g" or slot["profile"] != "4g":
        return False
    if old_map is None:
        old_map = old_exact_slot_map(prev_state)
    old = old_map.get((slot["gpu_id"], slot["start"], slot["end"], slot["profile"]))
    if old is None:
        return False
    return old.workload == demand["workload"] and old.profile == "4g"


def inst_preserve_match(
    inst: MigInstance,
    gpu_id: int,
    prev_state: ClusterState | None,
    old_map: dict[tuple[int, int, int, str], MigInstance] | None = None,
) -> bool:
    if old_map is None:
        old_map = old_exact_slot_map(prev_state)
    old = old_map.get((gpu_id, inst.start, inst.end, inst.profile))
    if old is None or inst.workload is None:
        return False
    return old.workload == inst.workload and old.profile == inst.profile


def gpu_logical_template(gpu: GPUState) -> tuple[str, ...]:
    profiles = [
        inst.profile
        for inst in sorted(gpu.instances, key=lambda x: (x.start, x.end))
        if inst.profile != "void"
    ]
    return tuple(sorted(profiles, key=lambda p: (-PROFILE_SIZE[p], p)))


def gpu_physical_template(gpu: GPUState) -> tuple[str, ...]:
    return tuple(
        inst.profile
        for inst in sorted(gpu.instances, key=lambda x: (x.start, x.end))
        if inst.profile != "void"
    )


def gpu_interval_profile_list(gpu: GPUState) -> list[tuple[int, int, str]]:
    return [
        (inst.start, inst.end, inst.profile)
        for inst in sorted(gpu.instances, key=lambda x: (x.start, x.end))
        if inst.profile != "void"
    ]


def gpu_match_score(old_gpu: GPUState, new_gpu: GPUState) -> int:
    old_logical = gpu_logical_template(old_gpu)
    new_logical = gpu_logical_template(new_gpu)
    old_physical = gpu_physical_template(old_gpu)
    new_physical = gpu_physical_template(new_gpu)

    score = 0
    if old_logical == new_logical:
        score += 1000
    if old_physical == new_physical:
        score += 200

    old_intervals = gpu_interval_profile_list(old_gpu)
    new_intervals = gpu_interval_profile_list(new_gpu)

    exact = 0
    overlap_same_profile = 0
    for old_interval in old_intervals:
        for new_interval in new_intervals:
            if old_interval == new_interval:
                exact += 1
            if old_interval[2] == new_interval[2]:
                overlap_same_profile += max(
                    0,
                    min(old_interval[1], new_interval[1]) - max(old_interval[0], new_interval[0]),
                )
    score += 20 * exact + overlap_same_profile

    old_profile_workload = Counter(
        (inst.profile, getattr(inst, "workload", None))
        for inst in old_gpu.instances
        if getattr(inst, "profile", None) not in (None, "void")
    )
    new_profile_workload = Counter(
        (inst.profile, getattr(inst, "workload", None))
        for inst in new_gpu.instances
        if getattr(inst, "profile", None) not in (None, "void")
    )

    common_profile_workload = 0
    for key in set(old_profile_workload) | set(new_profile_workload):
        common_profile_workload += min(old_profile_workload.get(key, 0), new_profile_workload.get(key, 0))
    score += 30 * common_profile_workload

    if old_profile_workload == new_profile_workload:
        score += 300

    return score


def reassign_gpu_ids_by_matching(target: ClusterState, prev_state: ClusterState | None) -> ClusterState:
    if prev_state is None:
        target.metadata["display_id_map"] = {
            gpu.gpu_id: idx for idx, gpu in enumerate(sorted(target.real_gpus(), key=lambda x: x.gpu_id))
        }
        return target

    old_gpus = list(sorted(prev_state.real_gpus(), key=lambda x: x.gpu_id))
    new_gpus = list(sorted(target.real_gpus(), key=lambda x: x.gpu_id))
    if not new_gpus:
        target.metadata["display_id_map"] = {}
        return target

    pairs = []
    for old_idx, old_gpu in enumerate(old_gpus):
        for new_idx, new_gpu in enumerate(new_gpus):
            pairs.append((gpu_match_score(old_gpu, new_gpu), old_idx, new_idx))
    pairs.sort(reverse=True)

    matched_old = set()
    matched_new = set()
    assignment = {}
    for _, old_idx, new_idx in pairs:
        if old_idx in matched_old or new_idx in matched_new:
            continue
        matched_old.add(old_idx)
        matched_new.add(new_idx)
        assignment[new_idx] = old_gpus[old_idx].gpu_id

    used_ids = set(assignment.values())
    next_id = max([gpu.gpu_id for gpu in old_gpus], default=-1) + 1
    for new_idx, _ in enumerate(new_gpus):
        if new_idx in assignment:
            continue
        while next_id in used_ids:
            next_id += 1
        assignment[new_idx] = next_id
        used_ids.add(next_id)
        next_id += 1

    old_to_new = {}
    for new_idx, new_gpu in enumerate(new_gpus):
        old_to_new[new_gpu.gpu_id] = assignment[new_idx]

    for gpu in target.real_gpus():
        new_id = old_to_new[gpu.gpu_id]
        gpu.gpu_id = new_id

    target.gpus = sorted(target.gpus, key=lambda x: (getattr(x, "source", "real"), x.gpu_id))
    target.metadata["display_id_map"] = {
        gpu.gpu_id: idx for idx, gpu in enumerate(sorted(target.real_gpus(), key=lambda x: x.gpu_id))
    }
    return target

