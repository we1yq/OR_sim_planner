from __future__ import annotations

import time
from collections import Counter, defaultdict
from dataclasses import dataclass, replace
from typing import Any

from .common import AllocationResult, LEGAL_SLOT_PATTERNS, PROFILE_SIZE, Slot


@dataclass(frozen=True)
class _Instance:
    instance_id: str
    workload: str
    profile: str
    start: int
    end: int
    mu: float
    gpu_id: int
    batch: int | None = None
    origin: str = "source"

    @property
    def size(self) -> int:
        return PROFILE_SIZE[self.profile]

    @property
    def key(self) -> tuple[str, str, int | None]:
        return self.workload, self.profile, self.batch


@dataclass(frozen=True)
class _ExchangePair:
    new: _Instance
    old: tuple[_Instance, ...]


def plan_exchange_and_compact(
    *,
    source_alloc: dict[str, Any] | AllocationResult,
    target_alloc: dict[str, Any] | AllocationResult,
    source_demand: dict[str, float],
    target_demand: dict[str, float],
    workload_names: list[str] | tuple[str, ...] | None = None,
    transition_id: str = "jormungandr_transition",
    default_queued: int = 2,
    default_inflight: int = 1,
) -> dict[str, Any]:
    """Implement Jormungandr's exchange-and-compact deployer protocol.

    The paper specifies two phases. Exchange first fixes per-model MIG instance
    deltas by creating replacement containers before deleting old containers.
    Compact then repartitions GPUs and migrates the surviving containers into
    the allocator's target deployment.
    """

    del default_queued, default_inflight
    start = time.perf_counter()
    names = list(workload_names) if workload_names is not None else sorted(set(source_demand) | set(target_demand))
    source = _flatten_allocation(source_alloc, origin="source")
    target = _flatten_allocation(target_alloc, origin="target")

    if source:
        matched, additions, removals = _diff_instances_by_model_and_profile(source, target)
        exchange_pairs, unpaired_additions, remaining_removals = _pair_exchange_instances(additions, removals)
        exchange_plan = _build_exchange_phase(
            source=source,
            matched=matched,
            exchange_pairs=exchange_pairs,
            unpaired_additions=unpaired_additions,
            remaining_removals=remaining_removals,
        )
        compact_plan = _build_compact_phase(
            target_alloc=target_alloc,
            carried_instances=exchange_plan["carried_instances"],
            exchange_gpu_ids=exchange_plan["exchange_gpu_ids"],
        )
    else:
        additions = []
        removals = []
        exchange_pairs = []
        unpaired_additions = []
        remaining_removals = []
        exchange_plan = {
            "actions": [],
            "carried_instances": [],
            "exchange_gpu_count": 0,
            "exchange_gpu_ids": [],
        }
        compact_plan = _build_bootstrap_compact_phase(target_alloc=target_alloc)
    actions = _with_action_ids([*exchange_plan["actions"], *compact_plan["actions"]])
    action_counts = Counter(str(action["type"]) for action in actions)
    elapsed = time.perf_counter() - start

    return {
        "stage_name": transition_id,
        "baseline": {
            "name": "jormungandr",
            "component": "deployer",
            "protocol": "exchange-and-compact",
            "implementation": "paper Section 6 protocol, independent of our transition planner",
            "target_allocation_role": "allocator output is the post-compact target deployment",
        },
        "required": {
            name: min(float(source_demand.get(name, 0.0)), float(target_demand.get(name, 0.0)))
            for name in names
        },
        "exchange_phase": {
            "delta_by_workload": _delta_summary(additions, removals),
            "pairs": [_pair_to_dict(pair) for pair in exchange_pairs],
            "unpaired_creates": [_instance_to_dict(inst) for inst in unpaired_additions],
            "remaining_deletes": [_instance_to_dict(inst) for inst in remaining_removals],
            "action_count": len(exchange_plan["actions"]),
        },
        "compact_phase": {
            "not_full_source_gpus": compact_plan["not_full_source_gpus"],
            "target_gpu_templates": compact_plan["target_gpu_templates"],
            "action_count": len(compact_plan["actions"]),
        },
        "fine_actions": actions,
        "executed_actions": actions,
        "action_counts": dict(action_counts),
        "iteration_count": 1,
        "reached_target": True,
        "elapsed_sec": elapsed,
        "peak_active_gpu": max(
            len({inst.gpu_id for inst in source}) + exchange_plan["exchange_gpu_count"],
            _allocation_gpu_count(target_alloc),
        ),
        "source_active_gpu": len({inst.gpu_id for inst in source}),
        "final_active_gpu": _allocation_gpu_count(target_alloc),
        "target_allocation": _allocation_to_dict(target_alloc),
        "phased_action_plan": {
            "name": f"{transition_id}-exchange-and-compact",
            "phases": [
                {"name": "exchange", "actions": [action["id"] for action in actions if action["phase"] == "exchange"]},
                {"name": "compact", "actions": [action["id"] for action in actions if action["phase"] == "compact"]},
            ],
        },
        "phased_action_plan_summary": {
            "phaseCount": 2,
            "actionCount": len(actions),
            "actionCounts": dict(action_counts),
        },
    }


def _flatten_allocation(allocation: dict[str, Any] | AllocationResult, *, origin: str) -> list[_Instance]:
    data = _allocation_to_dict(allocation)
    out: list[_Instance] = []
    for gpu in data.get("gpus", []):
        gpu_id = int(gpu["gpu_id"])
        for local_idx, inst in enumerate(gpu.get("instances", [])):
            workload = _normalize_workload(inst.get("workload"))
            if workload is None:
                continue
            profile = str(inst["profile"])
            out.append(
                _Instance(
                    instance_id=f"{origin}:gpu{gpu_id}:slot{local_idx}:{workload}:{profile}",
                    workload=workload,
                    profile=profile,
                    start=int(inst["start"]),
                    end=int(inst["end"]),
                    mu=float(inst.get("mu", 0.0)),
                    batch=int(inst["batch"]) if inst.get("batch") is not None else None,
                    gpu_id=gpu_id,
                    origin=origin,
                )
            )
    return out


def _diff_instances_by_model_and_profile(
    source: list[_Instance],
    target: list[_Instance],
) -> tuple[list[tuple[_Instance, _Instance]], list[_Instance], list[_Instance]]:
    source_by_key = _group_by_key(source)
    target_by_key = _group_by_key(target)
    matched: list[tuple[_Instance, _Instance]] = []
    additions: list[_Instance] = []
    removals: list[_Instance] = []

    for key in sorted(set(source_by_key) | set(target_by_key)):
        old = sorted(source_by_key.get(key, []), key=_instance_sort_key)
        new = sorted(target_by_key.get(key, []), key=_instance_sort_key)
        keep = min(len(old), len(new))
        matched.extend(zip(old[:keep], new[:keep], strict=False))
        removals.extend(old[keep:])
        additions.extend(new[keep:])
    return matched, additions, removals


def _pair_exchange_instances(
    additions: list[_Instance],
    removals: list[_Instance],
) -> tuple[list[_ExchangePair], list[_Instance], list[_Instance]]:
    removals_by_workload: dict[str, list[_Instance]] = defaultdict(list)
    for inst in removals:
        removals_by_workload[inst.workload].append(inst)
    for workload in removals_by_workload:
        removals_by_workload[workload].sort(key=lambda inst: (inst.mu, inst.size, inst.instance_id))

    pairs: list[_ExchangePair] = []
    unpaired_additions: list[_Instance] = []
    for new in sorted(additions, key=lambda inst: (inst.workload, -inst.mu, -inst.size, inst.instance_id)):
        old_list = removals_by_workload[new.workload]
        chosen: list[_Instance] = []
        total_mu = 0.0
        remaining = []
        for old in old_list:
            if total_mu + old.mu <= new.mu + 1e-9:
                chosen.append(old)
                total_mu += old.mu
            else:
                remaining.append(old)
        removals_by_workload[new.workload] = remaining
        if chosen:
            pairs.append(_ExchangePair(new=new, old=tuple(chosen)))
        else:
            unpaired_additions.append(new)

    remaining_removals = [
        inst
        for workload in sorted(removals_by_workload)
        for inst in removals_by_workload[workload]
    ]
    return pairs, unpaired_additions, remaining_removals


def _build_exchange_phase(
    *,
    source: list[_Instance],
    matched: list[tuple[_Instance, _Instance]],
    exchange_pairs: list[_ExchangePair],
    unpaired_additions: list[_Instance],
    remaining_removals: list[_Instance],
) -> dict[str, Any]:
    new_instances = [pair.new for pair in exchange_pairs] + list(unpaired_additions)
    temp_instances = _place_exchange_instances(new_instances, first_gpu_id=_next_gpu_id(source))
    temp_by_id = {inst.instance_id: inst for inst in temp_instances}

    actions: list[dict[str, Any]] = []
    carried_instances = [
        replace(old, instance_id=target.instance_id, origin="carried")
        for old, target in matched
    ]

    for pair in exchange_pairs:
        new = temp_by_id[pair.new.instance_id]
        actions.extend(_create_container_actions(new, phase="exchange", reason="paired_exchange_create"))
        carried_instances.append(new)
        for old in pair.old:
            actions.extend(_delete_container_actions(old, phase="exchange", reason="paired_exchange_delete"))

    for new_target in unpaired_additions:
        new = temp_by_id[new_target.instance_id]
        actions.extend(_create_container_actions(new, phase="exchange", reason="unpaired_exchange_create"))
        carried_instances.append(new)

    for old in remaining_removals:
        actions.extend(_delete_container_actions(old, phase="exchange", reason="remaining_exchange_delete"))

    return {
        "actions": actions,
        "carried_instances": carried_instances,
        "exchange_gpu_count": len({inst.gpu_id for inst in temp_instances}),
        "exchange_gpu_ids": sorted({inst.gpu_id for inst in temp_instances}),
    }


def _build_compact_phase(
    *,
    target_alloc: dict[str, Any] | AllocationResult,
    carried_instances: list[_Instance],
    exchange_gpu_ids: list[int],
) -> dict[str, Any]:
    actions: list[dict[str, Any]] = []
    carried_by_gpu: dict[int, list[_Instance]] = defaultdict(list)
    for inst in carried_instances:
        carried_by_gpu[inst.gpu_id].append(inst)
    for gpu_id in carried_by_gpu:
        carried_by_gpu[gpu_id].sort(key=lambda inst: (inst.start, inst.end, inst.workload))

    not_full_source_gpus = sorted(
        gpu_id
        for gpu_id, used in _used_slices_by_gpu(carried_instances).items()
        if 0 < used < 7
    )
    target_gpu_templates: dict[int, list[str]] = {}
    compacted: dict[int, list[_Instance]] = {}
    remaining_by_key = _pool_by_key(carried_instances)
    target_gpu_count = _allocation_gpu_count(target_alloc)
    target_sizes = sorted((sum(inst.size for inst in bucket) for bucket in _target_instances_by_gpu(target_alloc).values()), reverse=True)

    for gpu_id in sorted(carried_by_gpu):
        if gpu_id in compacted:
            continue
        repartition = _compact_gpu_once(
            gpu_id=gpu_id,
            carried_by_gpu=carried_by_gpu,
            remaining_by_key=remaining_by_key,
            compacted=compacted,
            target_sizes=target_sizes,
        )
        target_instances = compacted[gpu_id]
        if not target_instances and str(repartition.get("reason")) == "compact_empty_gpu":
            continue
        actions.append(repartition)
        target_gpu_templates[gpu_id] = [inst.profile for inst in target_instances]
        for move in repartition.get("moves", []):
            source = _instance_from_dict(dict(move["source_instance"]))
            target = _instance_from_dict(dict(move["target_instance"]))
            if str(move.get("type")) == "keep_container":
                actions.append(
                    {
                        "phase": "compact",
                        "type": "keep_container",
                        "workload": target.workload,
                        "profile": target.profile,
                        "source_instance": _instance_to_dict(source),
                        "target_instance": _instance_to_dict(target),
                    }
                )
            else:
                actions.append(
                    {
                        "phase": "compact",
                        "type": "migrate_container",
                        "workload": target.workload,
                        "profile": target.profile,
                        "from_instance": _instance_to_dict(source),
                        "to_instance": _instance_to_dict(target),
                        "reason": "compact_migration",
                    }
                )

    if len(compacted) > target_gpu_count:
        extra_gpus = sorted(compacted, key=lambda item: (sum(inst.size for inst in compacted[item]), item))[target_gpu_count:]
        for gpu_id in extra_gpus:
            target_gpu_templates.pop(gpu_id, None)
            for inst in compacted.pop(gpu_id, []):
                actions.extend(_delete_container_actions(inst, phase="compact", reason="compact_extra_gpu_delete"))

    if len(compacted) != target_gpu_count:
        raise RuntimeError(
            f"Jormungandr compact produced {len(compacted)} GPUs, expected {target_gpu_count}"
        )

    for gpu_id in exchange_gpu_ids:
        if gpu_id not in compacted:
            actions.append({"phase": "compact", "type": "return_extra_gpu", "gpu_id": gpu_id})

    return {
        "actions": actions,
        "not_full_source_gpus": not_full_source_gpus,
        "target_gpu_templates": target_gpu_templates,
    }


def _build_bootstrap_compact_phase(
    *,
    target_alloc: dict[str, Any] | AllocationResult,
) -> dict[str, Any]:
    target_by_gpu = _target_instances_by_gpu(target_alloc)
    actions: list[dict[str, Any]] = []
    target_gpu_templates: dict[int, list[str]] = {}
    for gpu_id, targets in sorted(target_by_gpu.items()):
        target_gpu_templates[gpu_id] = [target.profile for target in targets]
        actions.append(
            {
                "phase": "compact",
                "type": "repartition_gpu",
                "gpu_id": gpu_id,
                "old_profiles": [],
                "old_instances": [],
                "profiles": [target.profile for target in targets],
                "instances": [_instance_to_dict(target) for target in targets],
                "reason": "bootstrap_target_gpu",
            }
        )
    return {
        "actions": actions,
        "not_full_source_gpus": [],
        "target_gpu_templates": target_gpu_templates,
    }


def _compact_gpu_once(
    *,
    gpu_id: int,
    carried_by_gpu: dict[int, list[_Instance]],
    remaining_by_key: dict[tuple[str, str, int | None], list[_Instance]],
    compacted: dict[int, list[_Instance]],
    target_sizes: list[int],
) -> dict[str, Any]:
    old_instances = list(carried_by_gpu.get(gpu_id, []))
    preserved_sources = [
        inst
        for inst in old_instances
        if _remove_remaining_instance(remaining_by_key, inst)
    ]
    desired_used = _desired_compact_used_slices(sum(inst.size for inst in preserved_sources), target_sizes)
    selected = list(preserved_sources)
    while sum(inst.size for inst in selected) < desired_used:
        remaining_capacity = desired_used - sum(inst.size for inst in selected)
        candidate = _pop_best_compact_candidate(remaining_by_key, remaining_capacity)
        if candidate is None:
            break
        selected.append(candidate)
    if not selected:
        compacted[gpu_id] = []
        return {
            "phase": "compact",
            "type": "repartition_gpu",
            "gpu_id": gpu_id,
            "old_profiles": [inst.profile for inst in old_instances],
            "old_instances": [_instance_to_dict(inst) for inst in old_instances],
            "profiles": [],
            "instances": [],
            "preserve_instances": [],
            "create_instances": [],
            "delete_instances": [_instance_to_dict(source) for source in old_instances],
            "moves": [],
            "reason": "compact_empty_gpu",
        }

    target_slots = _materialize_slots_for_sizes([inst.size for inst in selected])
    ordered_sources = sorted(selected, key=lambda inst: (-inst.size, inst.workload, inst.instance_id))
    target_instances = [
        replace(source, gpu_id=gpu_id, start=slot.start, end=slot.end, origin="target")
        for slot, source in zip(target_slots, ordered_sources, strict=True)
    ]
    compacted[gpu_id] = target_instances
    source_target_pairs = list(zip(ordered_sources, target_instances, strict=True))
    preserved_targets = [
        target
        for source, target in source_target_pairs
        if _same_location(source, target) and source.key == target.key
    ]
    create_targets = [
        target
        for source, target in source_target_pairs
        if not (_same_location(source, target) and source.key == target.key)
    ]
    delete_sources = [
        source
        for source in old_instances
        if not any(source.instance_id == paired_source.instance_id and _same_location(source, target) for paired_source, target in source_target_pairs)
    ]
    return {
        "phase": "compact",
        "type": "repartition_gpu",
        "gpu_id": gpu_id,
        "old_profiles": [inst.profile for inst in old_instances],
        "old_instances": [_instance_to_dict(inst) for inst in old_instances],
        "profiles": [target.profile for target in target_instances],
        "instances": [_instance_to_dict(target) for target in target_instances],
        "preserve_instances": [_instance_to_dict(target) for target in preserved_targets],
        "create_instances": [_instance_to_dict(target) for target in create_targets],
        "delete_instances": [_instance_to_dict(source) for source in delete_sources],
        "moves": [
            {
                "source_instance": _instance_to_dict(source),
                "target_instance": _instance_to_dict(target),
                "type": "keep_container" if _same_location(source, target) and source.key == target.key else "migrate_container",
            }
            for source, target in source_target_pairs
        ],
        "reason": "compact_partial_repartition",
    }


def _desired_compact_used_slices(current_used: int, target_sizes: list[int]) -> int:
    for size in sorted(target_sizes):
        if size >= current_used:
            return size
    return max(current_used, target_sizes[-1] if target_sizes else current_used)


def _remove_remaining_instance(pool: dict[tuple[str, str, int | None], list[_Instance]], inst: _Instance) -> bool:
    bucket = pool.get(inst.key, [])
    for idx, candidate in enumerate(bucket):
        if candidate.instance_id == inst.instance_id:
            del bucket[idx]
            return True
    return False


def _pop_best_compact_candidate(pool: dict[tuple[str, str, int | None], list[_Instance]], capacity: int) -> _Instance | None:
    best_key = None
    best_idx = -1
    best_rank = None
    for key, bucket in pool.items():
        for idx, inst in enumerate(bucket):
            if inst.size > capacity:
                continue
            rank = (-inst.size, inst.workload, inst.profile, inst.instance_id)
            if best_rank is None or rank < best_rank:
                best_key = key
                best_idx = idx
                best_rank = rank
    if best_key is None:
        return None
    return pool[best_key].pop(best_idx)


def _source_target_pairs_for_compacted_gpu(
    old_instances: list[_Instance],
    target_instances: list[_Instance],
) -> list[tuple[_Instance, _Instance]]:
    old_by_key = _pool_by_key(old_instances)
    pairs: list[tuple[_Instance, _Instance]] = []
    for target in sorted(target_instances, key=_instance_sort_key):
        source = _pop_matching_instance(old_by_key, target)
        pairs.append((source, target))
    return pairs


def _place_exchange_instances(instances: list[_Instance], *, first_gpu_id: int) -> list[_Instance]:
    bins: list[list[_Instance]] = []
    for inst in sorted(instances, key=lambda item: (-item.size, item.workload, item.instance_id)):
        best_idx = None
        best_free = 8
        for idx, bucket in enumerate(bins):
            used = sum(item.size for item in bucket)
            free = 7 - used - inst.size
            if free >= 0 and free < best_free and _can_materialize_sizes([*(item.size for item in bucket), inst.size]):
                best_idx = idx
                best_free = free
        if best_idx is None:
            bins.append([inst])
        else:
            bins[best_idx].append(inst)

    out: list[_Instance] = []
    for offset, bucket in enumerate(bins):
        slots = _materialize_slots_for_sizes([inst.size for inst in bucket])
        for slot, inst in zip(slots, sorted(bucket, key=lambda item: (-item.size, item.instance_id)), strict=True):
            out.append(
                replace(
                    inst,
                    gpu_id=first_gpu_id + offset,
                    start=slot.start,
                    end=slot.end,
                    origin="exchange",
                )
            )
    return out


def _can_materialize_sizes(sizes: list[int]) -> bool:
    expected = tuple(sorted(sizes, reverse=True))
    return any(
        tuple(sorted((slot.size for slot in pattern), reverse=True)) == expected
        for pattern in LEGAL_SLOT_PATTERNS
    )


def _materialize_slots_for_sizes(sizes: list[int]) -> tuple[Slot, ...]:
    expected = tuple(sorted(sizes, reverse=True))
    candidates = [
        pattern
        for pattern in LEGAL_SLOT_PATTERNS
        if tuple(sorted((slot.size for slot in pattern), reverse=True)) == expected
    ]
    if not candidates:
        raise RuntimeError(f"Cannot materialize exchange GPU for MIG sizes {expected}")
    return tuple(sorted(min(candidates, key=lambda pattern: tuple(slot.start for slot in pattern)), key=lambda slot: (-slot.size, slot.start)))


def _create_container_actions(inst: _Instance, *, phase: str, reason: str) -> list[dict[str, Any]]:
    return [
        {
            "phase": phase,
            "type": "create_mig_instance",
            "gpu_id": inst.gpu_id,
            "profile": inst.profile,
            "start": inst.start,
            "end": inst.end,
            "reason": reason,
        },
        {
            "phase": phase,
            "type": "create_container",
            "workload": inst.workload,
            "profile": inst.profile,
            "instance": _instance_to_dict(inst),
            "reason": reason,
        },
    ]


def _delete_container_actions(inst: _Instance, *, phase: str, reason: str) -> list[dict[str, Any]]:
    return [
        {
            "phase": phase,
            "type": "delete_container",
            "workload": inst.workload,
            "profile": inst.profile,
            "instance": _instance_to_dict(inst),
            "reason": reason,
        },
        {
            "phase": phase,
            "type": "delete_mig_instance",
            "gpu_id": inst.gpu_id,
            "profile": inst.profile,
            "start": inst.start,
            "end": inst.end,
            "reason": reason,
        },
    ]


def _target_instances_by_gpu(allocation: dict[str, Any] | AllocationResult) -> dict[int, list[_Instance]]:
    out: dict[int, list[_Instance]] = defaultdict(list)
    for inst in _flatten_allocation(allocation, origin="target"):
        out[inst.gpu_id].append(inst)
    for gpu_id in out:
        out[gpu_id].sort(key=lambda inst: (inst.start, inst.end, inst.workload))
    return out


def _group_by_key(instances: list[_Instance]) -> dict[tuple[str, str, int | None], list[_Instance]]:
    out: dict[tuple[str, str, int | None], list[_Instance]] = defaultdict(list)
    for inst in instances:
        out[inst.key].append(inst)
    return out


def _pool_by_key(instances: list[_Instance]) -> dict[tuple[str, str, int | None], list[_Instance]]:
    pool = _group_by_key(instances)
    for key in pool:
        pool[key].sort(key=_instance_sort_key)
    return pool


def _pop_matching_instance(pool: dict[tuple[str, str, int | None], list[_Instance]], target: _Instance) -> _Instance:
    candidates = pool.get(target.key, [])
    if not candidates:
        raise RuntimeError(f"Jormungandr compact phase has no carried instance for {target.key}")
    best_idx = min(
        range(len(candidates)),
        key=lambda idx: (
            0 if _same_location(candidates[idx], target) else 1,
            abs(candidates[idx].gpu_id - target.gpu_id),
            candidates[idx].instance_id,
        ),
    )
    return candidates.pop(best_idx)


def _used_slices_by_gpu(instances: list[_Instance]) -> dict[int, int]:
    used: dict[int, int] = defaultdict(int)
    for inst in instances:
        used[inst.gpu_id] += inst.size
    return used


def _same_location(source: _Instance, target: _Instance) -> bool:
    return (
        source.gpu_id == target.gpu_id
        and source.profile == target.profile
        and source.start == target.start
        and source.end == target.end
    )


def _delta_summary(additions: list[_Instance], removals: list[_Instance]) -> dict[str, dict[str, int]]:
    out: dict[str, Counter[str]] = defaultdict(Counter)
    for inst in additions:
        out[inst.workload][f"+{inst.profile}"] += 1
    for inst in removals:
        out[inst.workload][f"-{inst.profile}"] += 1
    return {workload: dict(counter) for workload, counter in sorted(out.items())}


def _pair_to_dict(pair: _ExchangePair) -> dict[str, Any]:
    return {
        "new": _instance_to_dict(pair.new),
        "old": [_instance_to_dict(inst) for inst in pair.old],
        "new_throughput": pair.new.mu,
        "old_throughput": sum(inst.mu for inst in pair.old),
    }


def _instance_to_dict(inst: _Instance) -> dict[str, Any]:
    return {
        "instance_id": inst.instance_id,
        "workload": inst.workload,
        "profile": inst.profile,
        "start": inst.start,
        "end": inst.end,
        "mu": inst.mu,
        "batch": inst.batch,
        "gpu_id": inst.gpu_id,
        "origin": inst.origin,
    }


def _instance_from_dict(raw: dict[str, Any]) -> _Instance:
    return _Instance(
        instance_id=str(raw["instance_id"]),
        workload=str(raw["workload"]),
        profile=str(raw["profile"]),
        start=int(raw["start"]),
        end=int(raw["end"]),
        mu=float(raw.get("mu") or 0.0),
        batch=int(raw["batch"]) if raw.get("batch") is not None else None,
        gpu_id=int(raw["gpu_id"]),
        origin=str(raw.get("origin") or "carried"),
    )


def _instance_sort_key(inst: _Instance) -> tuple[int, int, str, str]:
    return inst.gpu_id, inst.start, inst.profile, inst.instance_id


def _with_action_ids(actions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for idx, action in enumerate(actions):
        out.append({"id": f"a{idx:04d}", **action})
    return out


def _allocation_gpu_count(allocation: dict[str, Any] | AllocationResult) -> int:
    return len(_allocation_to_dict(allocation).get("gpus", []))


def _next_gpu_id(instances: list[_Instance]) -> int:
    if not instances:
        return 0
    return max(inst.gpu_id for inst in instances) + 1


def _allocation_to_dict(allocation: dict[str, Any] | AllocationResult) -> dict[str, Any]:
    return allocation.to_dict() if isinstance(allocation, AllocationResult) else allocation


def _normalize_workload(workload: Any) -> str | None:
    if workload is None:
        return None
    value = str(workload)
    if "#" not in value:
        return value
    prefix, suffix = value.rsplit("#", 1)
    return prefix if suffix.isdigit() else value
