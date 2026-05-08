from __future__ import annotations

import copy
import contextlib
import io
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from feasible_options import feasible_options_for_request, profile_catalog_from_yaml
from io_utils import load_yaml
from models import PlanningScenario, ProfileOption
from state_adapter import gpu_state_from_mock_yaml, workload_request_from_k8s_object

from current_state_feasibility import evaluate_current_state_feasibility
from simulation_core.milp_solver import solve_milp_gurobi_batch_unified
from simulation_core.physical_ids import (
    bootstrap_physical_ids_for_state,
    canonicalize_state_for_next_round,
    ensure_state_metadata,
)
from simulation_core.state import (
    ClusterState,
    GPUState,
    MigInstance,
    assert_valid_cluster_state,
)
from simulation_core.target_builder import build_target_state_from_milp
from simulation_core.v3_transition import run_v3_stage_iterative


def plan_scenario_as_migplan_status(
    scenario: PlanningScenario,
    source_state_override: ClusterState | None = None,
    profile_catalogs_by_workload: dict[str, list[ProfileOption]] | None = None,
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
    if current_feasibility["feasible"] and not bool(scenario.transition.get("forceReplan", False)):
        canonical_next = canonicalize_state_for_next_round(source_state)
        return _status_from_current_state_noop(
            scenario=scenario,
            source_state=source_state,
            canonical_next=canonical_next,
            current_feasibility=current_feasibility,
        )

    feasible_start = time.perf_counter()
    feasible_option_df = build_feasible_option_dataframe(
        scenario,
        profile_catalogs_by_workload=profile_catalogs_by_workload,
    )
    feasible_elapsed_sec = time.perf_counter() - feasible_start
    milp_res = _call_planner(
        solve_milp_gurobi_batch_unified,
        capture_stdout=not verbose,
        feasible_option_df=feasible_option_df,
        arrival_rate=target_arrival,
        n_workloads=len(workload_names),
        time_limit_s=milp_time_limit_s,
        verbose=verbose,
    )
    if not milp_res.get("feasible"):
        return _status_from_infeasible_milp(scenario, milp_res)

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

    transition_res = run_v3_stage_iterative(
        source_state=source_state,
        target_state=target_state,
        src_arrival=source_arrival,
        tgt_arrival=dict(scenario.target_arrival),
        workload_names=workload_names,
        stage_name=scenario.name,
        max_iters=max_iters,
    )
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
        "planner": "v3",
        "stageCount": len(statuses),
        "stages": statuses,
    }


def build_feasible_option_dataframe(
    scenario: PlanningScenario,
    profile_catalogs_by_workload: dict[str, list[ProfileOption]] | None = None,
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
        options = feasible_options_for_request(request, catalog)
        if not options:
            raise ValueError(f"No feasible profile options for workload {workload.name}")
        for option in options:
            row = {
                "opt_idx": opt_idx,
                "w_idx": w_idx,
                "workload": workload.name,
                "family": option.family,
                "batch": int(option.batch),
                "profile": option.profile,
                "mu": float(option.mu),
            }
            row.update(option.metrics)
            rows.append(row)
            opt_idx += 1
    return pd.DataFrame(rows)


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
                "source": "dry-run",
                "instances": [
                    {
                        "start": int(inst.start),
                        "end": int(inst.end),
                        "profile": inst.profile,
                        "workload": inst.workload,
                        "batch": inst.batch,
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
) -> dict[str, Any]:
    actions = [_to_yamlable(action) for action in transition_res.get("executed_actions", [])]
    metrics = {
        "gpuCount": int(milp_res.get("gpu_count", 0)),
        "iterationCount": int(transition_res.get("iteration_count", 0)),
        "actionCount": len(actions),
        "peakActiveGpu": int(transition_res.get("peak_active_gpu", 0)),
        "sourceActiveGpu": int(transition_res.get("source_active_gpu", 0)),
        "finalActiveGpu": int(transition_res.get("final_active_gpu", 0)),
        "elapsedSec": float(transition_res.get("elapsed_sec", 0.0)),
        "milpElapsedSec": float(milp_res.get("elapsed", 0.0)),
        "targetBuildElapsedSec": float(
            target_state.metadata.get("build_metrics", {}).get("elapsed_time_sec", 0.0)
        ),
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
    )
    reached = bool(transition_res.get("reached_target", False))
    return {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "MigPlan",
        "metadata": {"name": f"{scenario.name}-dry-run"},
        "spec": {
            "dryRun": True,
            "planner": "v3",
            "scenario": scenario.name,
            "sourceStateRef": scenario.source_state_ref,
            "targetStateRef": scenario.target_state_ref,
        },
        "status": {
            "phase": "ReachedTarget" if reached else "InProgress",
            "reachedTarget": reached,
            "message": f"{scenario.name}: planned with real MILP target builder and V3 transition planner",
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
) -> dict[str, Any]:
    state = cluster_state_to_dict(source_state)
    canonical = cluster_state_to_dict(canonical_next)
    return {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "MigPlan",
        "metadata": {"name": f"{scenario.name}-dry-run"},
        "spec": {
            "dryRun": True,
            "planner": "v3",
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
                "milpElapsedSec": 0.0,
                "targetBuildElapsedSec": 0.0,
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
        "spec": {"dryRun": True, "planner": "v3", "scenario": scenario.name},
        "status": {
            "phase": "Infeasible",
            "reachedTarget": False,
            "message": f"MILP did not produce a feasible target: {milp_res.get('status')}",
            "actions": [],
            "metrics": {"milpElapsedSec": float(milp_res.get("elapsed", 0.0))},
            "planningTrace": {
                "scenario": scenario.name,
                "pipeline": "feasible-options -> milp",
                "milp": {
                    "method": milp_res.get("method"),
                    "status": milp_res.get("status"),
                    "feasible": False,
                    "elapsedSec": float(milp_res.get("elapsed", 0.0)),
                },
            },
        },
    }


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
) -> dict[str, Any]:
    build_metrics = dict(target_state.metadata.get("build_metrics", {}))
    return {
        "scenario": scenario.name,
        "pipeline": "source -> feasible-options -> milp -> target-build -> v3-transition -> canonical-next-state",
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
        "currentStateFeasibility": current_feasibility,
        "milp": {
            "method": milp_res.get("method"),
            "status": milp_res.get("status"),
            "feasible": bool(milp_res.get("feasible")),
            "elapsedSec": float(milp_res.get("elapsed", 0.0)),
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
        },
        "canonicalization": {
            "canonicalGpuCount": len(canonical_next.real_gpus()),
            "canonicalPhysicalIds": _physical_id_map(canonical_next),
            "freePhysicalGpuPool": list(canonical_next.metadata.get("free_physical_gpu_pool", [])),
            "note": "Dry-run uses the simulated executed state. A real actuator must canonicalize only after observing the actual post-action GPU/MIG state.",
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
    return {
        "elapsedSec": float(elapsed_sec),
        "optionCount": len(rows),
        "byWorkload": by_workload,
        "byProfile": by_profile,
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


def _action_counts_by_type(actions: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for action in actions:
        action_type = str(action.get("type", "unknown"))
        counts[action_type] = counts.get(action_type, 0) + 1
    return dict(sorted(counts.items()))


def _transition_iteration_summary(iterations: Any) -> list[dict[str, Any]]:
    out = []
    for iteration in list(iterations or []):
        chosen_actions = list(iteration.get("chosen_actions", []))
        out.append(
            {
                "iteration": int(iteration.get("iteration", 0)),
                "chosenRootCount": len(iteration.get("chosen_roots", [])),
                "chosenActionCount": len(chosen_actions),
                "actionCountsByType": _action_counts_by_type(_to_yamlable(chosen_actions)),
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
