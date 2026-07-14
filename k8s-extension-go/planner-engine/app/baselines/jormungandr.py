from __future__ import annotations

from collections import Counter
from typing import Any

from .common import (
    AllocationResult,
    GPUAllocation,
    GPUConfig,
    all_demands_satisfied,
    covered_throughput,
    enumerate_gpu_configs_cached,
)


def utility(conf: GPUConfig, demand: dict[str, float], weights: dict[str, float]) -> float:
    """Jormungandr Figure 4 utility(conf, demands, weights)."""

    total = 0.0
    for workload, throughput in conf.throughput_by_workload.items():
        required = float(demand.get(workload, 0.0))
        if required <= 0:
            continue
        total += (float(throughput) / required) * float(weights.get(workload, 0.0))
    return total


def allocate_utility_first(
    *,
    scenario_id: str,
    demand: dict[str, float],
    options,
    max_gpus: int = 2000,
    max_models_per_gpu: int = 3,
    last_mile_extra_models: int = 1,
    last_mile_threshold: float = 0.10,
    source_alloc: dict[str, Any] | AllocationResult | None = None,
    top_k: int = 10,
) -> AllocationResult:
    """Reimplement Jormungandr's utility-first per-GPU configuration allocator.

    The allocator selects one GPU configuration at a time. A configuration
    specifies the MIG instance profiles and assigned workload/batch containers
    on that target GPU; exact slice positions are treated as canonical
    realization details needed by our local data model, not as allocator
    decisions. For adjacent deployments, top-K similarity therefore compares
    the workload/profile/batch multiset of a candidate GPU configuration
    against current per-GPU configurations.
    """

    return _allocate_utility_first_impl(
        scenario_id=scenario_id,
        demand=demand,
        options=options,
        max_gpus=max_gpus,
        max_models_per_gpu=max_models_per_gpu,
        last_mile_extra_models=last_mile_extra_models,
        last_mile_threshold=last_mile_threshold,
        source_alloc=source_alloc,
        top_k=top_k,
        similarity_mode="configuration",
    )


def allocate_utility_first_old(
    *,
    scenario_id: str,
    demand: dict[str, float],
    options,
    max_gpus: int = 2000,
    max_models_per_gpu: int = 3,
    last_mile_extra_models: int = 1,
    last_mile_threshold: float = 0.10,
    source_alloc: dict[str, Any] | AllocationResult | None = None,
    top_k: int = 10,
) -> AllocationResult:
    """Old Jormungandr allocator reproduction kept for comparison.

    This version interpreted similarity as exact physical partition overlap:
    it counts identical ``(profile, start, end)`` MIG instances. The newer
    ``allocate_utility_first`` uses workload/profile/batch configuration
    similarity and does not treat exact slice placement as an allocator output.
    """

    return _allocate_utility_first_impl(
        scenario_id=scenario_id,
        demand=demand,
        options=options,
        max_gpus=max_gpus,
        max_models_per_gpu=max_models_per_gpu,
        last_mile_extra_models=last_mile_extra_models,
        last_mile_threshold=last_mile_threshold,
        source_alloc=source_alloc,
        top_k=top_k,
        similarity_mode="physical_slice_old",
    )


def _allocate_utility_first_impl(
    *,
    scenario_id: str,
    demand: dict[str, float],
    options,
    max_gpus: int,
    max_models_per_gpu: int,
    last_mile_extra_models: int,
    last_mile_threshold: float,
    source_alloc: dict[str, Any] | AllocationResult | None,
    top_k: int,
    similarity_mode: str,
) -> AllocationResult:
    import time

    start = time.perf_counter()
    active_demand = {name: float(value) for name, value in demand.items() if float(value) > 0}
    conf_set = enumerate_gpu_configs_cached(options, max_distinct_workloads=max_models_per_gpu)
    last_mile_conf_set = conf_set
    if last_mile_extra_models > 0:
        last_mile_conf_set = enumerate_gpu_configs_cached(
            options,
            max_distinct_workloads=max_models_per_gpu + int(last_mile_extra_models),
        )
    weights = {workload: 1.0 for workload in active_demand}
    deployment = []
    used_last_mile_configs = False
    current_signatures = _current_signatures(source_alloc, mode=similarity_mode)

    conf_tiebreak = {id(conf): (conf.used_slices, idx) for idx, conf in enumerate(conf_set)}

    for _ in range(max_gpus):
        if not used_last_mile_configs and _in_last_mile(weights, threshold=last_mile_threshold):
            conf_set = last_mile_conf_set
            conf_tiebreak = {id(conf): (conf.used_slices, idx) for idx, conf in enumerate(conf_set)}
            used_last_mile_configs = True
        scored = []
        for conf in conf_set:
            score = utility(conf, active_demand, weights)
            if score <= 0:
                continue
            used_slices, idx = conf_tiebreak[id(conf)]
            key = (score, used_slices, -idx)
            scored.append((key, conf))
        if not scored:
            break
        scored.sort(key=lambda item: item[0], reverse=True)
        if current_signatures:
            shortlist = scored[: max(1, int(top_k))]
            _, best_conf = max(
                shortlist,
                key=lambda item: (
                    _similarity(item[1], current_signatures, mode=similarity_mode),
                    item[0],
                ),
            )
        else:
            _, best_conf = scored[0]
        deployment.append(best_conf.as_gpu(len(deployment)))
        for workload, throughput in best_conf.throughput_by_workload.items():
            required = active_demand.get(workload, 0.0)
            if required <= 0:
                continue
            weights[workload] = max(
                0.0,
                float(weights.get(workload, 0.0)) - min(float(throughput) / required, float(weights.get(workload, 0.0))),
            )
        if sum(weights.values()) <= 1e-9:
            break

    runtime_sec = time.perf_counter() - start
    covered = covered_throughput(deployment)
    feasible = all_demands_satisfied(covered, active_demand)
    is_old = similarity_mode == "physical_slice_old"
    return AllocationResult(
        method="jormungandr_old" if is_old else "jormungandr",
        scenario_id=scenario_id,
        feasible=feasible,
        gpus=tuple(deployment),
        runtime_sec=runtime_sec,
        metadata={
            "paper_algorithm": "Jormungandr utility-first search with last-mile mixing and continuous top-K similarity",
            "candidate_configurations": len(conf_set),
            "max_models_per_gpu": max_models_per_gpu,
            "last_mile_extra_models": int(last_mile_extra_models),
            "last_mile_threshold": float(last_mile_threshold),
            "used_last_mile_configs": bool(used_last_mile_configs),
            "top_k_similarity": int(top_k),
            "source_gpu_count_for_similarity": len(current_signatures),
            "allocator_version": "old_physical_slice_similarity" if is_old else "per_gpu_configuration_similarity",
            "allocator_output_semantics": (
                "canonical physical slices used by local objects; allocator decision is per-GPU workload/profile/batch configuration"
                if not is_old
                else "legacy local reproduction treated profile/start/end as allocator-visible"
            ),
            "similarity_function": (
                "max count of identical MIG instances (profile,start,end) against any current GPU partition"
                if is_old
                else "max count of identical config instances (profile,workload,batch) against any current GPU configuration"
            ),
            "target_solution_counts": _target_solution_counts(deployment),
            "covered_throughput": covered,
            "unsatisfied_weights": weights,
        },
    )


def _in_last_mile(weights: dict[str, float], threshold: float) -> bool:
    positive = [float(value) for value in weights.values() if float(value) > 1e-9]
    if not positive:
        return False
    return any(value <= float(threshold) for value in positive)


def _current_signatures(source_alloc: dict[str, Any] | AllocationResult | None, *, mode: str) -> list[tuple]:
    if source_alloc is None:
        return []
    if isinstance(source_alloc, AllocationResult):
        gpus = source_alloc.gpus
    else:
        gpus = list(source_alloc.get("gpus", []) or [])
    signatures = []
    for gpu in gpus:
        signature = _signature(gpu, mode=mode)
        if signature:
            signatures.append(signature)
    return signatures


def _signature(gpu: GPUConfig | GPUAllocation | dict[str, Any], *, mode: str) -> tuple:
    if isinstance(gpu, dict):
        instances = list(gpu.get("instances", []) or [])
        return tuple(sorted(_instance_signature_dict(inst, mode=mode) for inst in instances))
    return tuple(
        sorted(_instance_signature_obj(inst, mode=mode) for inst in gpu.instances)
    )


def _instance_signature_dict(inst: dict[str, Any], *, mode: str) -> tuple:
    if mode == "physical_slice_old":
        return (
            str(inst.get("profile")),
            int(inst.get("start", 0)),
            int(inst.get("end", 0)),
        )
    return (
        str(inst.get("profile")),
        str(inst.get("workload")),
        _batch_key(inst.get("batch")),
    )


def _instance_signature_obj(inst: Any, *, mode: str) -> tuple:
    if mode == "physical_slice_old":
        return (
            str(inst.profile),
            int(inst.start),
            int(inst.end),
        )
    return (
        str(inst.profile),
        str(inst.workload),
        _batch_key(inst.batch),
    )


def _batch_key(batch: Any) -> str:
    if batch is None:
        return ""
    try:
        return str(int(batch))
    except (TypeError, ValueError):
        return str(batch)


def _similarity(conf: GPUConfig, current_signatures: list[tuple], *, mode: str) -> int:
    candidate = Counter(_signature(conf, mode=mode))
    return max(
        (sum((candidate & Counter(signature)).values()) for signature in current_signatures),
        default=0,
    )


def _target_solution_counts(deployment: list[GPUAllocation]) -> list[dict[str, Any]]:
    counts = Counter()
    batch_values: dict[tuple[str, str, str], Any] = {}
    for gpu in deployment:
        for inst in gpu.instances:
            key = (str(inst.workload), str(inst.profile), _batch_key(inst.batch))
            counts[key] += 1
            batch_values[key] = inst.batch
    return [
        {
            "workload": workload,
            "profile": profile,
            "batch": batch_values[(workload, profile, batch_key)],
            "count": int(count),
        }
        for (workload, profile, batch_key), count in sorted(counts.items())
    ]


def _current_configuration_signatures(source_alloc: dict[str, Any] | AllocationResult | None) -> list[tuple]:
    return _current_signatures(source_alloc, mode="configuration")


def _configuration_similarity(conf: GPUConfig, current_signatures: list[tuple]) -> int:
    return _similarity(conf, current_signatures, mode="configuration")


def _current_partition_signatures_old(source_alloc: dict[str, Any] | AllocationResult | None) -> list[tuple]:
    return _current_signatures(source_alloc, mode="physical_slice_old")


def _partition_similarity_old(conf: GPUConfig, current_signatures: list[tuple]) -> int:
    return _similarity(conf, current_signatures, mode="physical_slice_old")
