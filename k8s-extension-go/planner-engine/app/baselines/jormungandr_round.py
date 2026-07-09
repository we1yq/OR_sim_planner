from __future__ import annotations

import time
from typing import Any

from .common import AllocationResult
from .jormungandr import allocate_utility_first
from .jormungandr_deployer import plan_exchange_and_compact


def plan_jormungandr_round(
    *,
    scenario_id: str,
    demand: dict[str, float],
    options,
    source_alloc: dict[str, Any] | AllocationResult | None = None,
    source_demand: dict[str, float] | None = None,
    workload_names: list[str] | tuple[str, ...] | None = None,
    transition_id: str | None = None,
    **allocator_kwargs: Any,
) -> dict[str, Any]:
    """Run the complete Jormungandr baseline for one planning round.

    The returned `target_allocation` is the allocator output and should be used
    for target-allocation/GPU-count comparisons. When `source_alloc` is given,
    `transition_plan` contains the exchange-and-compact deployment transition
    from the source deployment to that target.
    """

    start = time.perf_counter()
    target = allocate_utility_first(
        scenario_id=scenario_id,
        demand=demand,
        options=options,
        source_alloc=source_alloc,
        **allocator_kwargs,
    )
    transition_plan = None
    transition_runtime_sec = 0.0
    if source_alloc is not None:
        transition_start = time.perf_counter()
        transition_plan = plan_exchange_and_compact(
            source_alloc=source_alloc,
            target_alloc=target,
            source_demand=source_demand if source_demand is not None else demand,
            target_demand=demand,
            workload_names=workload_names,
            transition_id=transition_id or f"{scenario_id}_jormungandr_transition",
        )
        transition_runtime_sec = time.perf_counter() - transition_start

    return {
        "method": "jormungandr",
        "scenario_id": scenario_id,
        "feasible": bool(target.feasible),
        "target_allocation": target,
        "target_allocation_dict": target.to_dict(),
        "transition_plan": transition_plan,
        "runtime_sec": time.perf_counter() - start,
        "stage_runtime_sec": {
            "allocator_sec": float(target.runtime_sec),
            "deployer_sec": transition_runtime_sec,
            "allocator_plus_deployer_sec": float(target.runtime_sec) + transition_runtime_sec,
        },
        "metadata": {
            "baseline": "Jormungandr",
            "allocator": "utility-first search with last-mile mixing and continuous top-K similarity",
            "deployer": "paper Section 6 exchange-and-compact protocol",
            "target_allocation_role": "post-compact target deployment used for GPU-count comparisons",
            "has_transition": transition_plan is not None,
        },
    }
