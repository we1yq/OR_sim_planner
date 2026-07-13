from __future__ import annotations

import copy
import contextlib
import io
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

CONTROLLER_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = Path(__file__).resolve().parents[2]
for root in (CONTROLLER_ROOT, PROJECT_ROOT):
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

from feasible_options import (
    apply_runtime_profile_correction,
    feasible_options_for_request,
    profile_catalog_from_yaml,
)
from io_utils import load_yaml
from models import PlanningScenario, ProfileOption
from state_adapter import gpu_state_from_mock_yaml, workload_request_from_k8s_object

from planning.current_state_feasibility import evaluate_current_state_feasibility
from migrant_core.allocation_optimizer.milp_solver import solve_milp_gurobi_batch_unified
from migrant_core.physical_ids import (
    bootstrap_physical_ids_for_state,
    canonicalize_state_for_next_round,
    ensure_state_metadata,
)
from migrant_core.state import (
    ClusterState,
    GPUState,
    MigInstance,
    assert_valid_cluster_state,
)
from migrant_core.target_materializer.target_builder import build_target_state_from_milp
from migrant_core.target_materializer.templates import PROFILE_ORDER, TEMPLATE_K
from migrant_core.transition_planner import canonical_planner_name, planner_runners


TRANSITION_PLANNERS = planner_runners(include_aliases=True)


def plan_scenario_as_migplan_status(
    scenario: PlanningScenario,
    source_state_override: ClusterState | None = None,
    profile_catalogs_by_workload: dict[str, list[ProfileOption]] | None = None,
    runtime_profile_correction: dict[str, Any] | None = None,
    max_iters: int = 20,
    milp_time_limit_s: float | None = None,
    verbose: bool = False,
) -> dict[str, Any]:
    workload_names = [workload.name for workload in scenario.workloads]
    target_arrival = [float(workload.target_arrival) for workload in scenario.workloads]
    source_arrival = dict(scenario.source_arrival)

    source_state = source_state_override or cluster_state_from_mock_yaml(load_yaml(scenario.source_state_ref))
    ensure_state_metadata(source_state)
    bootstrap_physical_ids_for_state(source_state)
    assert_valid_cluster_state(source_state)

    current_feasibility = evaluate_current_state_feasibility(
        scenario=scenario,
        source_state=source_state,
        safety_factor=float(scenario.transition.get("currentStateSafetyFactor", 1.0)),
    )
    if (
        current_feasibility["feasible"]
        and not _zero_arrival_requires_cleanup(scenario, source_state)
        and not bool(scenario.transition.get("forceReplan", False))
    ):
        canonical_next = canonicalize_state_for_next_round(source_state)
        return _status_from_current_state_noop(
            scenario=scenario,
            source_state=source_state,
            canonical_next=canonical_next,
            current_feasibility=current_feasibility,
            runtime_profile_correction=runtime_profile_correction,
        )

    feasible_start = time.perf_counter()
    feasible_option_df = build_feasible_option_dataframe(
        scenario,
        profile_catalogs_by_workload=profile_catalogs_by_workload,
        runtime_profile_correction=runtime_profile_correction,
    )
    feasible_elapsed_sec = time.perf_counter() - feasible_start
    milp_warm_start_res = build_milp_warm_start_from_current_allocation(
        source_state=source_state,
        feasible_option_df=feasible_option_df,
    )
    milp_res = _call_planner(
        solve_milp_gurobi_batch_unified,
        capture_stdout=not verbose,
        feasible_option_df=feasible_option_df,
        arrival_rate=target_arrival,
        n_workloads=len(workload_names),
        warm_start_res=milp_warm_start_res,
        time_limit_s=milp_time_limit_s,
        verbose=verbose,
    )
    if not milp_res.get("feasible"):
        return _status_from_infeasible_milp(scenario, milp_res)
    budget = observed_physical_gpu_budget(source_state)
    if _is_observed_cluster_state(source_state) and int(milp_res.get("gpu_count", 0)) > budget:
        budget_res = dict(milp_res)
        budget_res["feasible"] = False
        budget_res["status"] = "physical_gpu_budget_exceeded"
        budget_res["observedPhysicalGpuBudget"] = budget
        return _status_from_infeasible_milp(scenario, budget_res)

    target_state = _call_planner(
        build_target_state_from_milp,
        capture_stdout=not verbose,
        milp_res=milp_res,
        prev_state=source_state,
        feasible_option_df=feasible_option_df,
        workload_names=workload_names,
        arrival_rate=target_arrival,
        verbose=verbose,
    )
    ensure_state_metadata(target_state)

    transition_planner_name = _transition_planner_name(scenario)
    transition_res = TRANSITION_PLANNERS[transition_planner_name](
        source_state=source_state,
        target_state=target_state,
        src_arrival=source_arrival,
        tgt_arrival=dict(scenario.target_arrival),
        workload_names=workload_names,
        stage_name=scenario.name,
        max_iters=max_iters,
        **_transition_runtime_kwargs(scenario.transition),
    )
    transition_res["requested_transition_planner"] = transition_planner_name
    canonical_next = canonicalize_state_for_next_round(transition_res["executed_state"])
    return _migplan_status_from_results(
        scenario=scenario,
        source_state=source_state,
        feasible_option_df=feasible_option_df,
        feasible_elapsed_sec=feasible_elapsed_sec,
        milp_res=milp_res,
        target_state=target_state,
        transition_res=transition_res,
        canonical_next=canonical_next,
        current_feasibility=current_feasibility,
        runtime_profile_correction=runtime_profile_correction,
        milp_warm_start=milp_warm_start_res,
    )


def plan_scenario_chain_as_migplan_statuses(
    scenarios: list[PlanningScenario],
    max_iters: int = 20,
    milp_time_limit_s: float | None = None,
    verbose: bool = False,
) -> dict[str, Any]:
    statuses = []
    next_source = None
    for scenario in scenarios:
        status = plan_scenario_as_migplan_status(
            scenario=scenario,
            source_state_override=next_source,
            max_iters=max_iters,
            milp_time_limit_s=milp_time_limit_s,
            verbose=verbose,
        )
        statuses.append(status)
        next_source = cluster_state_from_status(status, "canonicalNextState")

    return {
        "kind": "MigPlanStageChain",
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "dryRun": True,
        "planner": "effect_aware_dag",
        "stageCount": len(statuses),
        "stages": statuses,
    }


def build_feasible_option_dataframe(
    scenario: PlanningScenario,
    profile_catalogs_by_workload: dict[str, list[ProfileOption]] | None = None,
    runtime_profile_correction: dict[str, Any] | None = None,
) -> Any:
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError(
            "The MILP adapter requires pandas. Install controller requirements before planning."
        ) from exc

    rows = []
    opt_idx = 0
    for w_idx, workload in enumerate(scenario.workloads):
        request = workload_request_from_k8s_object(load_yaml(workload.workload_ref))
        catalog = None
        if profile_catalogs_by_workload is not None:
            catalog = profile_catalogs_by_workload.get(workload.name)
        if catalog is None:
            if not workload.profile_catalog_ref:
                raise ValueError(
                    f"Workload {workload.name} requires profileCatalogConfigMaps "
                    "or profileCatalogRefs"
                )
            catalog = profile_catalog_from_yaml(load_yaml(workload.profile_catalog_ref))
        catalog, correction_summary = apply_runtime_profile_correction(
            catalog,
            runtime_profile_correction,
        )
        options = feasible_options_for_request(request, catalog)
        if not options:
            raise ValueError(f"No feasible profile options for workload {workload.name}")
        for option in options:
            row = {
                "opt_idx": opt_idx,
                "w_idx": w_idx,
                "workload": workload.name,
                "modelKey": option.model_key or request.model_key or request.model or workload.name,
                "placementGroup": (
                    option.placement_group
                    or request.placement_group
                    or option.model_key
                    or request.model_key
                    or request.model
                    or workload.name
                ),
                "family": option.family,
                "batch": int(option.batch),
                "profile": option.profile,
                "mu": float(option.mu),
            }
            if request.request_class is not None:
                row["requestClass"] = request.request_class
            if request.request_shape:
                row["requestShape"] = dict(request.request_shape)
            row.update(option.metrics)
            if correction_summary.get("appliedCount", 0) > 0:
                row["_runtimeProfileCorrection"] = correction_summary
            rows.append(row)
            opt_idx += 1
    return pd.DataFrame(rows)


def build_milp_warm_start_from_current_allocation(
    source_state: ClusterState,
    feasible_option_df: Any,
) -> dict[str, Any]:
    """Derive Gurobi Start values from the observed current allocation.

    The formal system source of truth is PhysicalGpuRegistry/currentAllocation,
    not the previous planner output.  This function maps the observed active
    runtime placements and GPU templates into the MILP variables used by the
    current feasible-option table.
    """
    template_capacity_to_idx = {
        tuple(int(capacity[profile]) for profile in PROFILE_ORDER): idx
        for idx, capacity in enumerate(TEMPLATE_K)
    }
    y_sol: Counter[int] = Counter()
    x_sol: Counter[int] = Counter()
    unmatched_instances = []
    template_misses = []

    option_index: dict[tuple[str, str, int], int] = {}
    if hasattr(feasible_option_df, "iterrows"):
        for _, row in feasible_option_df.iterrows():
            key = (str(row["workload"]), str(row["profile"]), int(row["batch"]))
            option_index.setdefault(key, int(row["opt_idx"]))

    for gpu in source_state.real_gpus():
        template_key = _template_capacity_key_for_gpu(gpu)
        if any(template_key):
            template_idx = template_capacity_to_idx.get(template_key)
            if template_idx is not None:
                y_sol[template_idx] += 1
            else:
                template_misses.append(
                    {
                        "gpuId": int(gpu.gpu_id),
                        "templateCapacity": dict(zip(PROFILE_ORDER, template_key)),
                    }
                )
        for inst in gpu.instances:
            if inst.profile == "void" or not inst.workload:
                continue
            key = (str(inst.workload), str(inst.profile), int(inst.batch or 1))
            opt_idx = option_index.get(key)
            if opt_idx is None:
                unmatched_instances.append(
                    {
                        "gpuId": int(gpu.gpu_id),
                        "workload": str(inst.workload),
                        "profile": str(inst.profile),
                        "batch": int(inst.batch or 1),
                    }
                )
                continue
            x_sol[opt_idx] += 1

    return {
        "source": "currentAllocation",
        "description": (
            "Warm start derived from observed runtime bindings and current MIG "
            "templates in PhysicalGpuRegistry/currentAllocation."
        ),
        "x_sol": dict(sorted(x_sol.items())),
        "y_sol": dict(sorted(y_sol.items())),
        "summary": {
            "sourceGpuCount": len(source_state.real_gpus()),
            "xSolCount": sum(x_sol.values()),
            "ySolCount": sum(y_sol.values()),
            "unmatchedInstanceCount": len(unmatched_instances),
            "templateMissCount": len(template_misses),
            "unmatchedInstances": unmatched_instances,
            "templateMisses": template_misses,
        },
    }


def _template_capacity_key_for_gpu(gpu: GPUState) -> tuple[int, ...]:
    counts = Counter({profile: 0 for profile in PROFILE_ORDER})
    for inst in gpu.instances:
        if inst.profile != "void":
            counts[str(inst.profile)] += 1
    return tuple(int(counts[profile]) for profile in PROFILE_ORDER)


def cluster_state_from_mock_yaml(obj: dict[str, Any]) -> ClusterState:
    mock_state = gpu_state_from_mock_yaml(obj)
    gpus = []
    for raw_gpu in mock_state.gpus:
        instances = [
            MigInstance(
                start=inst.start,
                end=inst.end,
                profile=inst.profile,
                workload=inst.workload,
                batch=inst.batch,
                model_key=getattr(inst, "model_key", None),
                placement_group=getattr(inst, "placement_group", None),
            )
            for inst in raw_gpu.instances
        ]
        gpus.append(GPUState(gpu_id=raw_gpu.gpu_id, source="real", instances=instances))
    state = ClusterState(gpus=gpus, metadata=copy.deepcopy(obj.get("metadata", {})))
    ensure_state_metadata(state)
    return state


def cluster_state_from_status(status: dict[str, Any], key: str) -> ClusterState:
    return cluster_state_from_dict(status["status"][key])


def cluster_state_from_dict(obj: dict[str, Any]) -> ClusterState:
    gpus = []
    for raw_gpu in obj.get("gpus", []):
        instances = [
            MigInstance(
                start=int(inst["start"]),
                end=int(inst["end"]),
                profile=str(inst["profile"]),
                workload=inst.get("workload"),
                batch=(int(inst["batch"]) if inst.get("batch") is not None else None),
                model_key=inst.get("modelKey") or inst.get("model_key"),
                placement_group=inst.get("placementGroup") or inst.get("placement_group"),
                mu=float(inst.get("mu", 0.0)),
                preserved=bool(inst.get("preserved", False)),
            )
            for inst in raw_gpu.get("instances", [])
        ]
        gpus.append(GPUState(gpu_id=int(raw_gpu["gpuId"]), source="real", instances=instances))
    state = ClusterState(gpus=gpus, metadata=copy.deepcopy(obj.get("metadata", {})))
    ensure_state_metadata(state)
    state.metadata["physical_id_map"] = {
        int(gpu_id): physical_id
        for gpu_id, physical_id in dict(state.metadata.get("physical_id_map", {})).items()
    }
    return state


def cluster_state_to_dict(state: ClusterState) -> dict[str, Any]:
    ensure_state_metadata(state)
    return {
        "metadata": _to_yamlable(state.metadata),
        "gpus": [
            {
                "gpuId": int(gpu.gpu_id),
                "source": "planned",
                "instances": [
                    {
                        "start": int(inst.start),
                        "end": int(inst.end),
                        "profile": inst.profile,
                        "workload": inst.workload,
                        "batch": inst.batch,
                        "modelKey": getattr(inst, "model_key", None),
                        "placementGroup": getattr(inst, "placement_group", None),
                        "mu": float(getattr(inst, "mu", 0.0)),
                        "preserved": bool(getattr(inst, "preserved", False)),
                    }
                    for inst in sorted(gpu.instances, key=lambda x: (x.start, x.end, x.profile))
                ],
            }
            for gpu in sorted(state.real_gpus(), key=lambda x: x.gpu_id)
        ],
    }


def _migplan_status_from_results(
    scenario: PlanningScenario,
    source_state: ClusterState,
    feasible_option_df: Any,
    feasible_elapsed_sec: float,
    milp_res: dict[str, Any],
    target_state: ClusterState,
    transition_res: dict[str, Any],
    canonical_next: ClusterState,
    current_feasibility: dict[str, Any],
    runtime_profile_correction: dict[str, Any] | None = None,
    milp_warm_start: dict[str, Any] | None = None,
) -> dict[str, Any]:
    actions = [_to_yamlable(action) for action in transition_res.get("executed_actions", [])]
    stage1_elapsed_sec = float(milp_res.get("elapsed", 0.0))
    stage2_elapsed_sec = float(
        target_state.metadata.get("build_metrics", {}).get("elapsed_time_sec", 0.0)
    )
    stage3_elapsed_sec = float(transition_res.get("elapsed_sec", 0.0))
    planner_makespan_sec = stage1_elapsed_sec + stage2_elapsed_sec + stage3_elapsed_sec
    metrics = {
        "gpuCount": int(milp_res.get("gpu_count", 0)),
        "iterationCount": int(transition_res.get("iteration_count", 0)),
        "actionCount": len(actions),
        "peakActiveGpu": int(transition_res.get("peak_active_gpu", 0)),
        "sourceActiveGpu": int(transition_res.get("source_active_gpu", 0)),
        "finalActiveGpu": int(transition_res.get("final_active_gpu", 0)),
        "elapsedSec": float(transition_res.get("elapsed_sec", 0.0)),
        "plannerMakespanSec": planner_makespan_sec,
        "stage1ElapsedSec": stage1_elapsed_sec,
        "stage2ElapsedSec": stage2_elapsed_sec,
        "stage3ElapsedSec": stage3_elapsed_sec,
        "milpElapsedSec": stage1_elapsed_sec,
        "targetBuildElapsedSec": stage2_elapsed_sec,
        "transitionPlanningElapsedSec": stage3_elapsed_sec,
        "feasibleOptionBuildElapsedSec": float(feasible_elapsed_sec),
    }
    planning_trace = _planning_trace(
        scenario=scenario,
        source_state=source_state,
        feasible_option_df=feasible_option_df,
        feasible_elapsed_sec=feasible_elapsed_sec,
        milp_res=milp_res,
        target_state=target_state,
        transition_res=transition_res,
        canonical_next=canonical_next,
        actions=actions,
        current_feasibility=current_feasibility,
        runtime_profile_correction=runtime_profile_correction,
        milp_warm_start=milp_warm_start,
    )
    reached = bool(transition_res.get("reached_target", False))
    return {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "MigPlan",
        "metadata": {"name": f"{scenario.name}-planner-engine"},
        "spec": {
            "dryRun": False,
            "planner": _transition_planner_name(scenario),
            "scenario": scenario.name,
            "sourceStateRef": scenario.source_state_ref,
            "targetStateRef": scenario.target_state_ref,
        },
        "status": {
            "phase": "ReachedTarget" if reached else "InProgress",
            "reachedTarget": reached,
            "message": (
                f"{scenario.name}: planned with real MILP target builder and "
                f"{_transition_planner_name(scenario)} transition planner"
            ),
            "actions": actions,
            "metrics": metrics,
            "planningTrace": planning_trace,
            "currentStateFeasibility": current_feasibility,
            "milp": {
                "status": milp_res.get("status"),
                "gpuCount": milp_res.get("gpu_count"),
                "chosenTemplates": list(milp_res.get("chosen_templates", [])),
                "KTotal": dict(milp_res.get("K_total", {})),
                "alloc": _to_yamlable(milp_res.get("alloc", [])),
                "rawSolution": {
                    "x_sol": _to_yamlable(milp_res.get("x_sol", {})),
                    "y_sol": _to_yamlable(milp_res.get("y_sol", {})),
                },
                "warmStart": _milp_warm_start_summary(milp_warm_start),
            },
            "targetState": cluster_state_to_dict(target_state),
            "executedState": cluster_state_to_dict(transition_res["executed_state"]),
            "canonicalNextState": cluster_state_to_dict(canonical_next),
        },
    }


def _status_from_current_state_noop(
    scenario: PlanningScenario,
    source_state: ClusterState,
    canonical_next: ClusterState,
    current_feasibility: dict[str, Any],
    runtime_profile_correction: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state = cluster_state_to_dict(source_state)
    canonical = cluster_state_to_dict(canonical_next)
    return {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "MigPlan",
        "metadata": {"name": f"{scenario.name}-dry-run"},
        "spec": {
            "dryRun": True,
            "planner": "effect_aware_dag",
            "scenario": scenario.name,
            "sourceStateRef": scenario.source_state_ref,
            "targetStateRef": scenario.target_state_ref,
        },
        "status": {
            "phase": "SucceededNoOp",
            "reachedTarget": True,
            "message": (
                f"{scenario.name}: current observed A100 state satisfies target arrival; "
                "no MIG, router, or Pod changes required"
            ),
            "actions": [],
            "metrics": {
                "gpuCount": len(source_state.real_gpus()),
                "iterationCount": 0,
                "actionCount": 0,
                "peakActiveGpu": len(source_state.real_gpus()),
                "sourceActiveGpu": len(source_state.real_gpus()),
                "finalActiveGpu": len(source_state.real_gpus()),
                "elapsedSec": 0.0,
                "plannerMakespanSec": 0.0,
                "stage1ElapsedSec": 0.0,
                "stage2ElapsedSec": 0.0,
                "stage3ElapsedSec": 0.0,
                "milpElapsedSec": 0.0,
                "targetBuildElapsedSec": 0.0,
                "transitionPlanningElapsedSec": 0.0,
                "feasibleOptionBuildElapsedSec": 0.0,
                "noOp": True,
            },
            "planningTrace": {
                "scenario": scenario.name,
                "pipeline": "source -> current-state-feasibility -> no-op",
                "inputs": {
                    "sourceStateRef": scenario.source_state_ref,
                    "targetStateRef": scenario.target_state_ref,
                    "workloadCount": len(scenario.workloads),
                    "workloads": [
                        {
                            "name": workload.name,
                            "sourceArrival": float(workload.source_arrival),
                            "targetArrival": float(workload.target_arrival),
                            "delta": float(workload.delta),
                            "workloadRef": workload.workload_ref,
                            "profileCatalogRef": workload.profile_catalog_ref,
                            "profileCatalogConfigMap": workload.profile_catalog_configmap,
                        }
                        for workload in scenario.workloads
                    ],
                    "sourceGpuCount": len(source_state.real_gpus()),
                    "sourcePhysicalIds": _physical_id_map(source_state),
                },
                "currentStateFeasibility": current_feasibility,
                "runtimeProfileCorrection": _runtime_profile_correction_summary(runtime_profile_correction),
                "noOpDecision": {
                    "recommendedAction": "no-op",
                    "reason": "current observed A100 state satisfies target arrival",
                },
                "canonicalization": {
                    "canonicalGpuCount": len(canonical_next.real_gpus()),
                    "canonicalPhysicalIds": _physical_id_map(canonical_next),
                    "freePhysicalGpuPool": list(
                        canonical_next.metadata.get("free_physical_gpu_pool", [])
                    ),
                    "note": (
                        "No-op uses the current observed state as the next input. "
                        "With real hardware this still requires observer-confirmed health."
                    ),
                },
            },
            "currentStateFeasibility": current_feasibility,
            "milp": {
                "status": "SKIPPED_CURRENT_STATE_FEASIBLE",
                "gpuCount": len(source_state.real_gpus()),
                "chosenTemplates": [],
                "KTotal": {},
                "alloc": None,
            },
            "targetState": state,
            "executedState": state,
            "canonicalNextState": canonical,
        },
    }


def _status_from_infeasible_milp(scenario: PlanningScenario, milp_res: dict[str, Any]) -> dict[str, Any]:
    return {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "MigPlan",
        "metadata": {"name": f"{scenario.name}-dry-run"},
        "spec": {"dryRun": True, "planner": "effect_aware_dag", "scenario": scenario.name},
        "status": {
            "phase": "Infeasible",
            "reachedTarget": False,
            "message": f"MILP did not produce a feasible target: {milp_res.get('status')}",
            "actions": [],
            "metrics": {
                "plannerMakespanSec": float(milp_res.get("elapsed", 0.0)),
                "stage1ElapsedSec": float(milp_res.get("elapsed", 0.0)),
                "stage2ElapsedSec": 0.0,
                "stage3ElapsedSec": 0.0,
                "milpElapsedSec": float(milp_res.get("elapsed", 0.0)),
                "targetBuildElapsedSec": 0.0,
                "transitionPlanningElapsedSec": 0.0,
                "observedPhysicalGpuBudget": milp_res.get("observedPhysicalGpuBudget"),
            },
            "planningTrace": {
                "scenario": scenario.name,
                "pipeline": "feasible-options -> milp",
                "milp": {
                    "method": milp_res.get("method"),
                    "status": milp_res.get("status"),
                    "feasible": False,
                    "elapsedSec": float(milp_res.get("elapsed", 0.0)),
                    "gpuCount": milp_res.get("gpu_count"),
                    "observedPhysicalGpuBudget": milp_res.get("observedPhysicalGpuBudget"),
                },
            },
        },
    }


def _zero_arrival_requires_cleanup(
    scenario: PlanningScenario,
    source_state: ClusterState,
) -> bool:
    if any(float(workload.target_arrival) > 0.0 for workload in scenario.workloads):
        return False
    for gpu in source_state.real_gpus():
        if gpu.instances:
            return True
    return False


def _planning_trace(
    scenario: PlanningScenario,
    source_state: ClusterState,
    feasible_option_df: Any,
    feasible_elapsed_sec: float,
    milp_res: dict[str, Any],
    target_state: ClusterState,
    transition_res: dict[str, Any],
    canonical_next: ClusterState,
    actions: list[dict[str, Any]],
    current_feasibility: dict[str, Any],
    runtime_profile_correction: dict[str, Any] | None = None,
    milp_warm_start: dict[str, Any] | None = None,
) -> dict[str, Any]:
    build_metrics = dict(target_state.metadata.get("build_metrics", {}))
    stage1_elapsed_sec = float(milp_res.get("elapsed", 0.0))
    stage2_elapsed_sec = float(build_metrics.get("elapsed_time_sec", 0.0))
    stage3_elapsed_sec = float(transition_res.get("elapsed_sec", 0.0))
    return {
        "scenario": scenario.name,
        "pipeline": (
            "source -> feasible-options -> milp -> target-build -> "
            f"{transition_res.get('requested_transition_planner', 'effect_aware_dag')} -> "
            "canonical-next-state"
        ),
        "inputs": {
            "sourceStateRef": scenario.source_state_ref,
            "targetStateRef": scenario.target_state_ref,
            "workloadCount": len(scenario.workloads),
            "workloads": [
                {
                    "name": workload.name,
                    "sourceArrival": float(workload.source_arrival),
                    "targetArrival": float(workload.target_arrival),
                    "delta": float(workload.delta),
                    "workloadRef": workload.workload_ref,
                    "profileCatalogRef": workload.profile_catalog_ref,
                    "profileCatalogConfigMap": workload.profile_catalog_configmap,
                }
                for workload in scenario.workloads
            ],
            "sourceGpuCount": len(source_state.real_gpus()),
            "sourcePhysicalIds": _physical_id_map(source_state),
        },
        "feasibleOptions": _feasible_option_summary(
            feasible_option_df=feasible_option_df,
            elapsed_sec=feasible_elapsed_sec,
        ),
        "plannerTiming": {
            "plannerMakespanSec": stage1_elapsed_sec + stage2_elapsed_sec + stage3_elapsed_sec,
            "stage1ElapsedSec": stage1_elapsed_sec,
            "stage2ElapsedSec": stage2_elapsed_sec,
            "stage3ElapsedSec": stage3_elapsed_sec,
            "stage1Name": "target_allocation_milp",
            "stage2Name": "target_materialization",
            "stage3Name": str(transition_res.get("requested_transition_planner", "effect_aware_dag")),
            "excludedCommonPreprocessingSec": float(feasible_elapsed_sec),
        },
        "runtimeProfileCorrection": _runtime_profile_correction_summary(runtime_profile_correction),
        "currentStateFeasibility": current_feasibility,
        "milp": {
            "method": milp_res.get("method"),
            "status": milp_res.get("status"),
            "feasible": bool(milp_res.get("feasible")),
            "elapsedSec": float(milp_res.get("elapsed", 0.0)),
            "warmStart": _milp_warm_start_summary(milp_warm_start),
            "objectiveGpuCount": milp_res.get("objective"),
            "gpuCount": milp_res.get("gpu_count"),
            "totalInstances": milp_res.get("total_instances"),
            "totalSlack": milp_res.get("total_slack"),
            "totalElasticSlack": milp_res.get("total_elastic_slack"),
            "totalRemainingSlots": milp_res.get("total_remaining_slots"),
            "usedProfileTypes": milp_res.get("used_profile_types"),
            "chosenTemplates": list(milp_res.get("chosen_templates", [])),
            "KTotal": _to_yamlable(milp_res.get("K_total", {})),
            "allocSummary": _milp_alloc_summary(milp_res.get("alloc", [])),
        },
        "targetBuild": {
            "elapsedSec": float(build_metrics.get("elapsed_time_sec", 0.0)),
            "method": target_state.metadata.get("build_method"),
            "targetGpuCount": len(target_state.real_gpus()),
            "physicalTemplateCount": len(build_metrics.get("ordered_physical_templates", [])),
            "orderedPhysicalTemplates": list(build_metrics.get("ordered_physical_templates", [])),
            "preservation": {
                "exact": build_metrics.get("exact_preserve"),
                "upgrade": build_metrics.get("upgrade_preserve"),
                "sameGpu": build_metrics.get("same_gpu_preserve"),
                "collocatePairs": build_metrics.get("collocate_pairs"),
                "mixedGpuCount": build_metrics.get("mixed_gpu_count"),
                "scoreTuple": _to_yamlable(build_metrics.get("score_tuple")),
            },
            "targetPhysicalIds": _physical_id_map(target_state),
        },
        "transition": {
            "stageName": transition_res.get("stage_name"),
            "planner": transition_res.get("requested_transition_planner"),
            "plannerModule": transition_res.get("transition_planner_module", "transition.effect_aware_dag"),
            "elapsedSec": float(transition_res.get("elapsed_sec", 0.0)),
            "reachedTarget": bool(transition_res.get("reached_target", False)),
            "iterationCount": int(transition_res.get("iteration_count", 0)),
            "actionCount": len(actions),
            "actionCountsByType": _action_counts_by_type(actions),
            "sourceActiveGpu": int(transition_res.get("source_active_gpu", 0)),
            "peakActiveGpu": int(transition_res.get("peak_active_gpu", 0)),
            "finalActiveGpu": int(transition_res.get("final_active_gpu", 0)),
            "iterations": _transition_iteration_summary(transition_res.get("iterations", [])),
            "executedPhysicalIds": _physical_id_map(transition_res["executed_state"]),
            "finalCoarseActions": _to_yamlable(
                dict(transition_res.get("final_plan") or {}).get("coarse_actions", [])
            ),
            "finalPlanItems": _to_yamlable(
                dict(transition_res.get("final_plan") or {}).get("plan_items", [])
            ),
            "candidateDecisions": _to_yamlable(
                dict(transition_res.get("final_plan") or {}).get("candidate_decisions", [])
            ),
            "runtimeAssumptions": _to_yamlable(
                dict(transition_res.get("final_plan") or {}).get("runtime_assumptions", {})
            ),
            "phasedActionPlanSummary": _to_yamlable(transition_res.get("phased_action_plan_summary")),
            "phasedActionPlan": _to_yamlable(transition_res.get("phased_action_plan")),
        },
        "canonicalization": {
            "canonicalGpuCount": len(canonical_next.real_gpus()),
            "canonicalPhysicalIds": _physical_id_map(canonical_next),
            "freePhysicalGpuPool": list(canonical_next.metadata.get("free_physical_gpu_pool", [])),
            "note": "Planner-engine canonicalization predicts the post-action state. The transition-executor validates the actual post-action GPU/MIG state after hardware execution.",
        },
    }


def _feasible_option_summary(feasible_option_df: Any, elapsed_sec: float) -> dict[str, Any]:
    rows = []
    by_workload = []
    by_profile = []
    if hasattr(feasible_option_df, "to_dict"):
        rows = feasible_option_df.to_dict("records")
    for workload in sorted({str(row.get("workload")) for row in rows}):
        workload_rows = [row for row in rows if str(row.get("workload")) == workload]
        by_workload.append(
            {
                "workload": workload,
                "optionCount": len(workload_rows),
                "profiles": sorted({str(row.get("profile")) for row in workload_rows}),
                "batches": sorted({int(row.get("batch")) for row in workload_rows}),
                "maxMu": max(float(row.get("mu", 0.0)) for row in workload_rows),
            }
        )
    for profile in sorted({str(row.get("profile")) for row in rows}):
        profile_rows = [row for row in rows if str(row.get("profile")) == profile]
        by_profile.append({"profile": profile, "optionCount": len(profile_rows)})
    corrections = []
    for row in rows:
        raw = row.get("_runtimeProfileCorrection")
        if isinstance(raw, dict) and raw.get("appliedCount", 0) > 0:
            corrections.append(raw)
    return {
        "elapsedSec": float(elapsed_sec),
        "optionCount": len(rows),
        "byWorkload": by_workload,
        "byProfile": by_profile,
        "runtimeProfileCorrectionApplied": max(
            [int(item.get("appliedCount", 0) or 0) for item in corrections] or [0]
        ),
    }


def _runtime_profile_correction_summary(correction: dict[str, Any] | None) -> dict[str, Any]:
    if not correction:
        return {"available": False, "observationCount": 0}
    observations = list(correction.get("observations") or [])
    return {
        "available": bool(correction.get("available", bool(observations))),
        "policy": correction.get("policy"),
        "muPolicy": correction.get("muPolicy"),
        "latencyPolicy": correction.get("latencyPolicy"),
        "observationCount": len(observations),
        "models": sorted({str(row.get("model")) for row in observations if isinstance(row, dict) and row.get("model")}),
    }


def _milp_alloc_summary(alloc: Any) -> list[dict[str, Any]]:
    out = []
    for row in list(alloc or []):
        instances = list(row.get("instances", []))
        out.append(
            {
                "workload": row.get("workload"),
                "arrival": row.get("arrival"),
                "provided": row.get("provided"),
                "slack": row.get("slack"),
                "instanceCount": sum(int(inst.get("count", 0)) for inst in instances),
                "instances": _to_yamlable(instances),
            }
        )
    return out


def _milp_warm_start_summary(warm_start: dict[str, Any] | None) -> dict[str, Any]:
    if not warm_start:
        return {"available": False, "source": "none", "xSolCount": 0, "ySolCount": 0}
    summary = dict(warm_start.get("summary") or {})
    return {
        "available": bool(warm_start.get("x_sol") or warm_start.get("y_sol")),
        "source": str(warm_start.get("source") or "currentAllocation"),
        "xSolCount": int(summary.get("xSolCount", 0) or 0),
        "ySolCount": int(summary.get("ySolCount", 0) or 0),
        "unmatchedInstanceCount": int(summary.get("unmatchedInstanceCount", 0) or 0),
        "templateMissCount": int(summary.get("templateMissCount", 0) or 0),
        "unmatchedInstances": _to_yamlable(summary.get("unmatchedInstances", [])),
        "templateMisses": _to_yamlable(summary.get("templateMisses", [])),
    }


def _action_counts_by_type(actions: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for action in actions:
        action_type = str(action.get("type", "unknown"))
        counts[action_type] = counts.get(action_type, 0) + 1
    return dict(sorted(counts.items()))


def _transition_planner_name(scenario: PlanningScenario) -> str:
    raw = str(
        scenario.transition.get("transitionPlanner")
        or scenario.transition.get("actionPlanner")
        or scenario.transition.get("planner")
        or "effect_aware_dag"
    )
    return canonical_planner_name(raw)


def _transition_runtime_kwargs(transition: dict[str, Any]) -> dict[str, Any]:
    runtime = dict(transition.get("runtime") or transition.get("runtimeAssumptions") or {})
    kwargs: dict[str, Any] = {}
    if "defaultQueued" in runtime:
        kwargs["default_queued"] = int(runtime["defaultQueued"])
    if "defaultInflight" in runtime:
        kwargs["default_inflight"] = int(runtime["defaultInflight"])
    if "overrideExistingChangedSlots" in runtime:
        kwargs["override_existing_runtime_for_changed_slots"] = bool(runtime["overrideExistingChangedSlots"])
    if "transitionDemandPolicy" in transition:
        kwargs["transition_demand_policy"] = str(transition["transitionDemandPolicy"])
    elif "transitionDemandPolicy" in runtime:
        kwargs["transition_demand_policy"] = str(runtime["transitionDemandPolicy"])
    return kwargs


def _transition_iteration_summary(iterations: Any) -> list[dict[str, Any]]:
    out = []
    for iteration in list(iterations or []):
        chosen_actions = list(iteration.get("chosen_actions", []))
        candidate_actions = list(iteration.get("candidate_actions", []))
        if not candidate_actions:
            candidate_actions = list(dict(iteration.get("full_plan") or {}).get("executed_actions", []))
        out.append(
            {
                "iteration": int(iteration.get("iteration", 0)),
                "candidateActionCount": len(candidate_actions),
                "candidateActions": _to_yamlable(candidate_actions),
                "candidateActionCountsByType": _action_counts_by_type(_to_yamlable(candidate_actions)),
                "chosenRootCount": len(iteration.get("chosen_roots", [])),
                "chosenRoots": _to_yamlable(iteration.get("chosen_roots", [])),
                "chosenActionCount": len(chosen_actions),
                "chosenActions": _to_yamlable(chosen_actions),
                "actionCountsByType": _action_counts_by_type(_to_yamlable(chosen_actions)),
                "phasedActionPlan": _to_yamlable(iteration.get("phased_action_plan_summary")),
                "candidateDecisions": _to_yamlable(iteration.get("candidate_decisions", [])),
                "stateBefore": cluster_state_to_dict(iteration["state_before"]) if iteration.get("state_before") is not None else None,
                "stateAfter": cluster_state_to_dict(iteration["state_after"]) if iteration.get("state_after") is not None else None,
                "madeProgress": bool(iteration.get("made_progress", False)),
                "reachedTarget": bool(iteration.get("reached_target", False)),
                "iterPeakActiveGpu": int(iteration.get("iter_peak_active_gpu", 0)),
                "activeGpuAfter": int(iteration.get("active_gpu_after", 0)),
            }
        )
    return out


def _physical_id_map(state: ClusterState) -> dict[str, Any]:
    ensure_state_metadata(state)
    return {
        str(gpu_id): physical_id
        for gpu_id, physical_id in sorted(
            dict(state.metadata.get("physical_id_map", {})).items(),
            key=lambda item: int(item[0]),
        )
    }


def _is_observed_cluster_state(state: ClusterState) -> bool:
    return str(state.metadata.get("source", "")).startswith("go-cluster-state-manager")


def observed_physical_gpu_budget(state: ClusterState) -> int:
    ensure_state_metadata(state)
    observed: set[str] = set()
    for gpu in state.real_gpus():
        physical_id = state.metadata.get("physical_id_map", {}).get(int(gpu.gpu_id))
        if physical_id:
            observed.add(str(physical_id))
    for physical_id in state.metadata.get("free_physical_gpu_pool", []) or []:
        if physical_id:
            observed.add(str(physical_id))
    return len(observed)


def _to_yamlable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _to_yamlable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_yamlable(v) for v in value]
    if hasattr(value, "item"):
        return value.item()
    return value


def _call_planner(fn: Any, capture_stdout: bool, *args: Any, **kwargs: Any) -> Any:
    if not capture_stdout:
        return fn(*args, **kwargs)
    with contextlib.redirect_stdout(io.StringIO()):
        return fn(*args, **kwargs)
