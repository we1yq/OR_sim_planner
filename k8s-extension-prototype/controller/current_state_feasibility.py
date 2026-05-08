from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from models import PlanningScenario

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from simulation_core.state import ClusterState, assert_valid_cluster_state


def evaluate_current_state_feasibility(
    scenario: PlanningScenario,
    source_state: ClusterState,
    safety_factor: float = 1.0,
) -> dict[str, Any]:
    workloads = [workload.name for workload in scenario.workloads]
    target_by_workload = {
        workload.name: float(workload.target_arrival) * float(safety_factor)
        for workload in scenario.workloads
    }
    capacity_by_workload = {workload: 0.0 for workload in workloads}
    reasons = []

    try:
        assert_valid_cluster_state(source_state, slice_count=7)
        a100_valid_layout = True
    except Exception as exc:
        a100_valid_layout = False
        reasons.append(f"current state is not a valid A100 7-slice MIG layout: {exc}")

    gpu_model = str(source_state.metadata.get("gpuModel", source_state.metadata.get("gpu_model", "A100")))
    a100_only = "A100" in gpu_model.upper()
    if not a100_only:
        reasons.append(f"unsupported GPU model for this prototype: {gpu_model}")

    missing_capacity_metadata = []
    for gpu in source_state.real_gpus():
        for inst in gpu.instances:
            workload = inst.workload
            if workload not in capacity_by_workload:
                continue
            mu = float(getattr(inst, "mu", 0.0) or 0.0)
            if mu <= 0.0:
                missing_capacity_metadata.append(
                    {
                        "gpuId": int(gpu.gpu_id),
                        "slot": [int(inst.start), int(inst.end), inst.profile],
                        "workload": workload,
                    }
                )
                continue
            capacity_by_workload[workload] += mu

    workload_results = []
    all_workloads_satisfied = True
    for workload in scenario.workloads:
        name = workload.name
        target = float(target_by_workload[name])
        capacity = float(capacity_by_workload[name])
        satisfied = capacity >= target
        all_workloads_satisfied = all_workloads_satisfied and satisfied
        if not satisfied:
            reasons.append(f"{name} capacity {capacity:.6g} < target {target:.6g}")
        workload_results.append(
            {
                "workload": name,
                "targetArrival": float(workload.target_arrival),
                "requiredWithSafetyFactor": target,
                "currentCapacity": capacity,
                "satisfied": satisfied,
            }
        )

    if missing_capacity_metadata:
        reasons.append(
            "current serving instances are missing positive mu capacity metadata"
        )

    feasible = bool(a100_valid_layout and a100_only and all_workloads_satisfied and not missing_capacity_metadata)
    return {
        "phase": "CurrentStateSatisfiesDemand" if feasible else "ReconfigurationRequired",
        "feasible": feasible,
        "recommendedAction": "no-op" if feasible else "plan",
        "gpuModel": "A100",
        "safetyFactor": float(safety_factor),
        "a100ValidLayout": a100_valid_layout,
        "workloads": workload_results,
        "capacityByWorkload": capacity_by_workload,
        "targetByWorkload": target_by_workload,
        "reasons": reasons,
    }
