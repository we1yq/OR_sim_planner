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
    """Reimplement Jormungandr's utility-first allocator.

    The empty-cluster case follows Figure 4. For adjacent deployments, it uses
    the paper's continuous-allocation policy: consider the top-K utility
    configurations and pick the one most similar to the current deployment.
    """

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
    current_signatures = _current_partition_signatures(source_alloc)

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
                    _partition_similarity(item[1], current_signatures),
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
    return AllocationResult(
        method="jormungandr",
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
            "similarity_function": "max count of identical MIG instances (profile,start,end) against any current GPU partition",
            "covered_throughput": covered,
            "unsatisfied_weights": weights,
        },
    )


def _in_last_mile(weights: dict[str, float], threshold: float) -> bool:
    positive = [float(value) for value in weights.values() if float(value) > 1e-9]
    if not positive:
        return False
    return any(value <= float(threshold) for value in positive)


def _current_partition_signatures(source_alloc: dict[str, Any] | AllocationResult | None) -> list[tuple[tuple[str, int, int], ...]]:
    if source_alloc is None:
        return []
    if isinstance(source_alloc, AllocationResult):
        gpus = source_alloc.gpus
    else:
        gpus = list(source_alloc.get("gpus", []) or [])
    signatures = []
    for gpu in gpus:
        signature = _partition_signature(gpu)
        if signature:
            signatures.append(signature)
    return signatures


def _partition_signature(gpu: GPUConfig | GPUAllocation | dict[str, Any]) -> tuple[tuple[str, int, int], ...]:
    if isinstance(gpu, dict):
        instances = list(gpu.get("instances", []) or [])
        return tuple(
            sorted(
                (
                    str(inst.get("profile")),
                    int(inst.get("start", 0)),
                    int(inst.get("end", 0)),
                )
                for inst in instances
            )
        )
    return tuple(
        sorted(
            (
                str(inst.profile),
                int(inst.start),
                int(inst.end),
            )
            for inst in gpu.instances
        )
    )


def _partition_similarity(conf: GPUConfig, current_signatures: list[tuple[tuple[str, int, int], ...]]) -> int:
    candidate = Counter(_partition_signature(conf))
    return max(
        (sum((candidate & Counter(signature)).values()) for signature in current_signatures),
        default=0,
    )
