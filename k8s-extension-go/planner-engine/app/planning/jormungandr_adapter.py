from __future__ import annotations

import copy
import os
import time
from collections import Counter, defaultdict
from typing import Any

from baselines.common import AllocationResult, GPUAllocation, WorkloadInstance, serving_options_from_dataframe
from baselines.jormungandr_round import plan_jormungandr_round
from migrant_core.physical_ids import alloc_from_free_pool_or_new, ensure_state_metadata, get_physical_id, set_physical_id
from migrant_core.state import ClusterState, GPUState, MigInstance


LOGICAL_PROFILE_SIZE = {"7g": 7, "4g": 4, "3g": 3, "2g": 2, "1g": 1}
PHYSICAL_PROFILE_SIZE = {"7g": 8, "4g": 4, "3g": 4, "2g": 2, "1g": 1}


class UnsupportedJormungandrReplayError(RuntimeError):
    def __init__(self, message: str, *, unsupported: list[dict[str, Any]] | None = None) -> None:
        super().__init__(message)
        self.unsupported = unsupported or [{"type": "jormungandr_replay", "reason": message}]


def plan_jormungandr_as_migplan_status(
    *,
    scenario: Any,
    source_state: ClusterState,
    feasible_option_df: Any,
) -> dict[str, Any]:
    start = time.perf_counter()
    ensure_state_metadata(source_state)
    workload_names = [workload.name for workload in scenario.workloads]
    source_demand = dict(scenario.source_arrival)
    target_demand = dict(scenario.target_arrival)
    source_alloc = allocation_result_from_cluster_state(source_state, scenario_id=f"{scenario.name}-source")
    physical_gpu_budget = observed_physical_gpu_budget(source_state)
    jorm = plan_jormungandr_round(
        scenario_id=scenario.name,
        demand=target_demand,
        options=serving_options_from_dataframe(feasible_option_df),
        source_alloc=source_alloc,
        source_demand=source_demand,
        workload_names=workload_names,
        transition_id=f"{scenario.name}-jormungandr",
        max_gpus=physical_gpu_budget,
    )
    allocator_elapsed_sec = float((jorm.get("stage_runtime_sec") or {}).get("allocator_sec", 0.0))
    deployer_elapsed_sec = float((jorm.get("stage_runtime_sec") or {}).get("deployer_sec", 0.0))
    planner_makespan_sec = allocator_elapsed_sec + deployer_elapsed_sec
    adapter_start = time.perf_counter()
    target_alloc = jorm["target_allocation"]
    if not bool(getattr(target_alloc, "feasible", False)) or int(target_alloc.gpu_count) > int(physical_gpu_budget):
        return _status_from_infeasible_jormungandr(
            scenario=scenario,
            target_alloc=target_alloc,
            physical_gpu_budget=physical_gpu_budget,
            allocator_elapsed_sec=allocator_elapsed_sec,
            deployer_elapsed_sec=deployer_elapsed_sec,
            planner_makespan_sec=planner_makespan_sec,
            wall_clock_sec=time.perf_counter() - start,
        )
    physical_state = copy.deepcopy(source_state)
    ensure_state_metadata(physical_state)
    target_state, final_physical_ids = target_state_from_allocation(target_alloc, physical_state)
    transition_plan = dict(jorm.get("transition_plan") or {})
    peak_active_gpu = int(transition_plan.get("peak_active_gpu") or max(len(source_state.real_gpus()), int(target_alloc.gpu_count)))
    if peak_active_gpu > int(physical_gpu_budget):
        return _status_from_unsupported_native_jormungandr(
            scenario=scenario,
            target_alloc=target_alloc,
            physical_gpu_budget=physical_gpu_budget,
            allocator_elapsed_sec=allocator_elapsed_sec,
            deployer_elapsed_sec=deployer_elapsed_sec,
            planner_makespan_sec=planner_makespan_sec,
            wall_clock_sec=time.perf_counter() - start,
            unsupported=[
                {
                    "phase": "transition",
                    "type": "exchange_capacity",
                    "peakActiveGpu": peak_active_gpu,
                    "observedPhysicalGpuBudget": int(physical_gpu_budget),
                    "reason": "jormungandr_exchange_requires_more_physical_gpus_than_observed",
                }
            ],
            transition_plan=transition_plan,
        )
    unsupported_native = unsupported_native_jorm_primitives(transition_plan)
    if unsupported_native and not allow_common_executor_fallback():
        return _status_from_unsupported_native_jormungandr(
            scenario=scenario,
            target_alloc=target_alloc,
            physical_gpu_budget=physical_gpu_budget,
            allocator_elapsed_sec=allocator_elapsed_sec,
            deployer_elapsed_sec=deployer_elapsed_sec,
            planner_makespan_sec=planner_makespan_sec,
            wall_clock_sec=time.perf_counter() - start,
            unsupported=unsupported_native,
            transition_plan=transition_plan,
        )
    try:
        actions, action_dag = lower_jormungandr_transition(
            source_state=source_state,
            target_state=target_state,
            transition_plan=transition_plan,
            final_physical_ids=final_physical_ids,
            physical_state=physical_state,
        )
    except UnsupportedJormungandrReplayError as exc:
        return _status_from_unsupported_native_jormungandr(
            scenario=scenario,
            target_alloc=target_alloc,
            physical_gpu_budget=physical_gpu_budget,
            allocator_elapsed_sec=allocator_elapsed_sec,
            deployer_elapsed_sec=deployer_elapsed_sec,
            planner_makespan_sec=planner_makespan_sec,
            wall_clock_sec=time.perf_counter() - start,
            unsupported=exc.unsupported,
            transition_plan=transition_plan,
        )
    canonical_next = copy.deepcopy(target_state)
    adapter_elapsed_sec = time.perf_counter() - adapter_start
    planning_trace = {
        "pipeline": "jormungandr_allocator -> exchange_and_compact_deployer -> canonical-action-dag-adapter",
        "planner": "jormungandr",
        "plannerTiming": {
            "plannerMakespanSec": planner_makespan_sec,
            "allocatorElapsedSec": allocator_elapsed_sec,
            "deployerElapsedSec": deployer_elapsed_sec,
            "stage1Name": "jormungandr_allocator",
            "stage2Name": "jormungandr_exchange_and_compact_deployer",
            "adapterElapsedSec": adapter_elapsed_sec,
            "excludedAdapterSec": adapter_elapsed_sec,
        },
        "target": {
            "method": target_alloc.method,
            "gpuCount": target_alloc.gpu_count,
            "maxGpus": physical_gpu_budget,
            "allocatedSlices": target_alloc.allocated_slices,
            "sliceUtilization": target_alloc.slice_utilization,
            "metadata": dict(target_alloc.metadata),
        },
        "transition": {
            "planner": "jormungandr",
            "plannerModule": "baselines.jormungandr_round",
            "transitionPlanner": "jormungandr_exchange_and_compact_adapter",
            "phasedActionPlan": action_dag,
            "phasedActionPlanSummary": compact_action_dag(action_dag),
            "sourceActionCounts": dict(transition_plan.get("action_counts") or {}),
            "canonicalActionCounts": dict(Counter(str(action.get("type")) for action in actions)),
            "exchangeAndCompact": {
                "exchangePhase": transition_plan.get("exchange_phase"),
                "compactPhase": transition_plan.get("compact_phase"),
            },
        },
        "canonicalization": {
            "canonicalGpuCount": len(canonical_next.real_gpus()),
            "canonicalPhysicalIds": {str(k): v for k, v in final_physical_ids.items()},
            "freePhysicalGpuPool": list(canonical_next.metadata.get("free_physical_gpu_pool", [])),
            "note": "Jormungandr target allocation is executed through the common Go executor using a canonical DAG adapter.",
        },
    }
    actions_yaml = [_yamlable(action) for action in actions]
    status = {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "MigPlan",
        "metadata": {"name": f"{scenario.name}-jormungandr"},
        "spec": {
            "dryRun": False,
            "planner": "jormungandr",
            "scenario": scenario.name,
            "sourceStateRef": scenario.source_state_ref,
            "targetStateRef": scenario.target_state_ref,
        },
        "status": {
            "phase": "ReachedTarget",
            "reachedTarget": True,
            "message": f"{scenario.name}: planned with Jormungandr allocator and exchange-and-compact deployer",
            "actions": actions_yaml,
            "metrics": {
                "gpuCount": int(target_alloc.gpu_count),
                "iterationCount": 1,
                "actionCount": len(actions),
                "peakActiveGpu": int(transition_plan.get("peak_active_gpu") or max(len(source_state.real_gpus()), target_alloc.gpu_count)),
                "sourceActiveGpu": len(source_state.real_gpus()),
                "finalActiveGpu": int(target_alloc.gpu_count),
                "elapsedSec": float(jorm.get("runtime_sec", 0.0)),
                "plannerMakespanSec": planner_makespan_sec,
                "jormungandrAllocatorElapsedSec": allocator_elapsed_sec,
                "jormungandrDeployerElapsedSec": deployer_elapsed_sec,
                "adapterElapsedSec": adapter_elapsed_sec,
                "plannerWallClockSec": time.perf_counter() - start,
            },
            "planningTrace": planning_trace,
            "currentStateFeasibility": {
                "feasible": False,
                "planner": "jormungandr",
                "note": "Jormungandr baseline always replans so that its allocator/deployer path is measured.",
            },
            "targetState": cluster_state_to_dict(target_state),
            "executedState": cluster_state_to_dict(target_state),
            "canonicalNextState": cluster_state_to_dict(canonical_next),
            "jormungandr": {
                "targetAllocation": target_alloc.to_dict(),
                "transitionPlan": transition_plan,
            },
        },
    }
    return status


def allow_common_executor_fallback() -> bool:
    value = str(os.environ.get("ALLOW_JORM_COMMON_EXECUTOR_FALLBACK", "")).strip().lower()
    if value in {"0", "false", "no", "off"}:
        return False
    return True


def unsupported_native_jorm_primitives(transition_plan: dict[str, Any]) -> list[dict[str, Any]]:
    unsupported = []
    for raw in list(transition_plan.get("fine_actions") or []):
        action = dict(raw)
        action_type = str(action.get("type") or "")
        if action_type in {"repartition_gpu", "migrate_container", "keep_container"}:
            unsupported.append({
                "id": action.get("id"),
                "phase": action.get("phase"),
                "type": action_type,
                "gpu_id": action.get("gpu_id"),
                "reason": action.get("reason"),
            })
    return unsupported


def _status_from_unsupported_native_jormungandr(
    *,
    scenario: Any,
    target_alloc: AllocationResult,
    physical_gpu_budget: int,
    allocator_elapsed_sec: float,
    deployer_elapsed_sec: float,
    planner_makespan_sec: float,
    wall_clock_sec: float,
    unsupported: list[dict[str, Any]],
    transition_plan: dict[str, Any],
) -> dict[str, Any]:
    return {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "MigPlan",
        "metadata": {"name": f"{scenario.name}-jormungandr"},
        "spec": {
            "dryRun": True,
            "planner": "jormungandr",
            "scenario": scenario.name,
            "sourceStateRef": scenario.source_state_ref,
            "targetStateRef": scenario.target_state_ref,
        },
        "status": {
            "phase": "Unsupported",
            "reachedTarget": False,
            "message": (
                f"{scenario.name}: Jormungandr native transition primitives require "
                "native executor support; refusing common-executor delete/create fallback"
            ),
            "actions": [],
            "metrics": {
                "gpuCount": int(target_alloc.gpu_count),
                "observedPhysicalGpuBudget": int(physical_gpu_budget),
                "iterationCount": 1,
                "actionCount": 0,
                "plannerMakespanSec": planner_makespan_sec,
                "jormungandrAllocatorElapsedSec": allocator_elapsed_sec,
                "jormungandrDeployerElapsedSec": deployer_elapsed_sec,
                "adapterElapsedSec": 0.0,
                "plannerWallClockSec": wall_clock_sec,
                "unsupportedNativePrimitiveCount": len(unsupported),
            },
            "planningTrace": {
                "pipeline": "jormungandr_allocator -> exchange_and_compact_deployer",
                "planner": "jormungandr",
                "target": {
                    "method": target_alloc.method,
                    "feasible": bool(target_alloc.feasible),
                    "gpuCount": int(target_alloc.gpu_count),
                    "maxGpus": int(physical_gpu_budget),
                },
                "transition": {
                    "nativePrimitivesRequired": True,
                    "unsupportedNativePrimitives": unsupported,
                    "sourceActionCounts": dict(transition_plan.get("action_counts") or {}),
                    "note": (
                        "repartition_gpu, migrate_container, and keep_container are Jormungandr "
                        "native transition primitives. The common Kubernetes/MIG executor does "
                        "not currently implement native container migration, so this adapter does "
                        "not lower them to delete_instance/place_instance by default."
                    ),
                },
            },
            "jormungandr": {
                "targetAllocation": target_alloc.to_dict(),
                "transitionPlan": transition_plan,
            },
        },
    }


def _status_from_infeasible_jormungandr(
    *,
    scenario: Any,
    target_alloc: AllocationResult,
    physical_gpu_budget: int,
    allocator_elapsed_sec: float,
    deployer_elapsed_sec: float,
    planner_makespan_sec: float,
    wall_clock_sec: float,
) -> dict[str, Any]:
    return {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "MigPlan",
        "metadata": {"name": f"{scenario.name}-jormungandr"},
        "spec": {
            "dryRun": True,
            "planner": "jormungandr",
            "scenario": scenario.name,
            "sourceStateRef": scenario.source_state_ref,
            "targetStateRef": scenario.target_state_ref,
        },
        "status": {
            "phase": "Infeasible",
            "reachedTarget": False,
            "message": (
                f"{scenario.name}: Jormungandr target allocation is infeasible "
                f"within observed physical GPU budget {physical_gpu_budget}"
            ),
            "actions": [],
            "metrics": {
                "gpuCount": int(target_alloc.gpu_count),
                "observedPhysicalGpuBudget": int(physical_gpu_budget),
                "iterationCount": 1,
                "actionCount": 0,
                "plannerMakespanSec": planner_makespan_sec,
                "jormungandrAllocatorElapsedSec": allocator_elapsed_sec,
                "jormungandrDeployerElapsedSec": deployer_elapsed_sec,
                "adapterElapsedSec": 0.0,
                "plannerWallClockSec": wall_clock_sec,
            },
            "planningTrace": {
                "pipeline": "jormungandr_allocator -> exchange_and_compact_deployer",
                "planner": "jormungandr",
                "target": {
                    "method": target_alloc.method,
                    "feasible": bool(target_alloc.feasible),
                    "gpuCount": int(target_alloc.gpu_count),
                    "maxGpus": int(physical_gpu_budget),
                    "coveredThroughput": dict(target_alloc.metadata.get("covered_throughput") or {}),
                    "unmetDemand": dict(target_alloc.metadata.get("unmet_demand") or {}),
                },
            },
            "jormungandr": {
                "targetAllocation": target_alloc.to_dict(),
            },
        },
    }


def allocation_result_from_cluster_state(state: ClusterState, *, scenario_id: str) -> AllocationResult:
    gpus: list[GPUAllocation] = []
    for gpu in sorted(state.real_gpus(), key=lambda item: int(item.gpu_id)):
        instances = []
        for inst in sorted(gpu.instances, key=lambda item: (item.start, item.end, item.profile, str(item.workload))):
            if inst.profile == "void" or inst.workload is None:
                continue
            instances.append(
                WorkloadInstance(
                    workload=str(inst.workload),
                    profile=str(inst.profile),
                    start=int(inst.start),
                    end=_logical_end(int(inst.start), str(inst.profile)),
                    batch=int(inst.batch or 1),
                    mu=float(getattr(inst, "mu", 0.0)),
                )
            )
        if instances:
            gpus.append(GPUAllocation(gpu_id=int(gpu.gpu_id), instances=tuple(instances)))
    return AllocationResult(
        method="current",
        scenario_id=scenario_id,
        feasible=True,
        gpus=tuple(gpus),
        runtime_sec=0.0,
    )


def observed_physical_gpu_budget(state: ClusterState) -> int:
    ensure_state_metadata(state)
    observed: set[str] = set()
    physical_map = dict(state.metadata.get("physical_id_map") or {})
    for gpu in state.real_gpus():
        physical_id = get_physical_id(state, int(gpu.gpu_id))
        if not physical_id:
            physical_id = physical_map.get(str(gpu.gpu_id), physical_map.get(int(gpu.gpu_id)))
        if physical_id:
            observed.add(str(physical_id))
    for physical_id in state.metadata.get("free_physical_gpu_pool", []) or []:
        if physical_id:
            observed.add(str(physical_id))
    return max(1, len(observed))


def target_state_from_allocation(
    allocation: AllocationResult,
    physical_state: ClusterState,
) -> tuple[ClusterState, dict[int, str]]:
    ensure_state_metadata(physical_state)
    gpus = []
    physical_ids: dict[int, str] = {}
    for gpu in sorted(allocation.gpus, key=lambda item: int(item.gpu_id)):
        gpu_id = int(gpu.gpu_id)
        physical_id = get_physical_id(physical_state, gpu_id)
        if physical_id is None:
            physical_id = alloc_from_free_pool_or_new(physical_state)
            set_physical_id(physical_state, gpu_id, physical_id)
        physical_ids[gpu_id] = physical_id
        gpus.append(
            GPUState(
                gpu_id=gpu_id,
                source="real",
                instances=[
                    MigInstance(
                        start=int(inst.start),
                        end=_physical_end(int(inst.start), str(inst.profile)),
                        profile=str(inst.profile),
                        workload=str(inst.workload),
                        batch=int(inst.batch or 1),
                        mu=float(inst.mu),
                    )
                    for inst in sorted(gpu.instances, key=lambda item: (item.start, item.end, item.profile, item.workload))
                ],
            )
        )
    target = ClusterState(
        gpus=gpus,
        metadata={
            **copy.deepcopy(physical_state.metadata),
            "physical_id_map": dict(physical_ids),
            "free_physical_gpu_pool": [
                physical_id
                for physical_id in list(physical_state.metadata.get("free_physical_gpu_pool") or [])
                if str(physical_id) not in set(physical_ids.values())
            ],
        },
    )
    ensure_state_metadata(target)
    return target, physical_ids


def lower_jormungandr_transition(
    *,
    source_state: ClusterState,
    target_state: ClusterState,
    transition_plan: dict[str, Any],
    final_physical_ids: dict[int, str],
    physical_state: ClusterState,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    node_deps: dict[str, list[str]] = {}
    source_physical_ids = {
        int(gpu.gpu_id): get_physical_id(source_state, int(gpu.gpu_id))
        for gpu in source_state.real_gpus()
    }
    exchange_physical_ids: dict[int, str] = {}
    exchange_creates: dict[int, list[dict[str, Any]]] = defaultdict(list)
    exchange_containers: dict[int, list[dict[str, Any]]] = defaultdict(list)
    exchange_deletes: list[dict[str, Any]] = []
    exchange_delete_slots: dict[tuple[int, str], list[tuple[int, int, str]]] = defaultdict(list)
    compact_repartitions: list[dict[str, Any]] = []
    compact_moves: list[dict[str, Any]] = []
    compact_returns: list[dict[str, Any]] = []
    predeleted_source_keys: set[tuple[str, int, int, str, str]] = set()

    for raw in list(transition_plan.get("fine_actions") or []):
        action = dict(raw)
        phase = str(action.get("phase") or "")
        action_type = str(action.get("type") or "")
        if phase == "exchange" and action_type == "create_mig_instance":
            exchange_creates[int(action["gpu_id"])].append(action)
        elif phase == "exchange" and action_type == "create_container":
            inst = dict(action.get("instance") or {})
            exchange_containers[int(inst.get("gpu_id", action.get("gpu_id", -1)))].append(action)
        elif phase == "exchange" and action_type == "delete_container":
            exchange_deletes.append(action)
        elif phase == "exchange" and action_type == "delete_mig_instance":
            gpu_id = int(action["gpu_id"])
            physical_id = source_physical_ids.get(gpu_id)
            if physical_id:
                exchange_delete_slots[(gpu_id, physical_id)].append(_physical_slot(action["start"], action["end"], action["profile"]))
        elif phase == "compact" and action_type == "repartition_gpu":
            compact_repartitions.append(action)
        elif phase == "compact" and action_type in {"keep_container", "migrate_container"}:
            compact_moves.append(action)
        elif phase == "compact" and action_type == "return_extra_gpu":
            compact_returns.append(action)

    def add(action: dict[str, Any], deps: list[str] | None = None) -> str:
        node_id = str(action.get("id") or f"jorm-{len(actions):04d}-{action.get('type')}")
        action["id"] = node_id
        if deps:
            action["dependsOn"] = list(dict.fromkeys(deps))
        actions.append(action)
        node_deps[node_id] = list(action.get("dependsOn") or [])
        return node_id

    def add_source_instance_deletes(root: str, physical_id: str, deps: list[str]) -> list[str]:
        delete_nodes: list[str] = []
        for source_gpu in sorted(source_state.real_gpus(), key=lambda item: int(item.gpu_id)):
            source_gpu_id = int(source_gpu.gpu_id)
            if source_physical_ids.get(source_gpu_id) != physical_id:
                continue
            for inst in sorted(source_gpu.instances, key=lambda item: (int(item.start), int(item.end), str(item.profile), str(item.workload))):
                if inst.profile == "void" or inst.workload is None:
                    continue
                slot = _physical_slot(int(inst.start), int(inst.end), str(inst.profile))
                workload = str(inst.workload)
                source_key = (physical_id, slot[0], slot[1], slot[2], workload)
                if source_key in predeleted_source_keys:
                    continue
                predeleted_source_keys.add(source_key)
                old_root = f"{root}_OLD_{source_gpu_id}_{slot[0]}_{slot[1]}_{slot[2]}_{workload}"
                deactivate = add(_route_action("deactivate_instance_route", old_root, source_gpu_id, physical_id, slot, workload, "jormungandr_exchange"), deps)
                wait = add(_route_action("wait_instance_drain", old_root, source_gpu_id, physical_id, slot, workload, "jormungandr_exchange"), [deactivate])
                delete_nodes.append(add(_delete_action(old_root, source_gpu_id, physical_id, slot, workload, "jormungandr_exchange"), [wait]))
        return delete_nodes

    def choose_reused_final_physical_id(slots: list[tuple[int, int, str]]) -> str:
        requested = {(int(start), int(end), str(profile)) for start, end, profile in slots}
        fallback = ""
        for gpu in sorted(target_state.real_gpus(), key=lambda item: int(item.gpu_id)):
            physical_id = final_physical_ids.get(int(gpu.gpu_id))
            if not physical_id:
                continue
            if not fallback:
                fallback = physical_id
            target_slots = {
                (int(inst.start), int(inst.end), str(inst.profile))
                for inst in gpu.instances
                if inst.profile != "void"
            }
            if requested and requested.issubset(target_slots):
                return physical_id
        if fallback:
            return fallback
        raise RuntimeError("Jormungandr exchange requires a physical GPU but no final target GPU is available to reuse")

    exchange_terminals: list[str] = []
    for gpu_id in sorted(set(exchange_creates) | set(exchange_containers)):
        slots = sorted(
            {
                _physical_slot(item["start"], item["end"], item["profile"])
                for item in exchange_creates.get(gpu_id, [])
            }
        )
        physical_id = exchange_physical_ids.get(gpu_id)
        if physical_id is None:
            if gpu_id in final_physical_ids:
                physical_id = final_physical_ids[gpu_id]
            elif list(physical_state.metadata.get("free_physical_gpu_pool") or []):
                physical_id = alloc_from_free_pool_or_new(physical_state)
            else:
                physical_id = choose_reused_final_physical_id(slots)
            exchange_physical_ids[gpu_id] = physical_id
        logical_id = gpu_id if gpu_id in final_physical_ids else f"jorm-exchange-{gpu_id}"
        if not slots:
            continue
        root = f"JORM_EXCHANGE_GPU_{gpu_id}"
        alloc = add(_action("allocate_gpu", root, logical_id, physical_id, transitionMode="jormungandr_exchange"))
        configure_deps = [alloc]
        if physical_id in set(source_physical_ids.values()):
            source_delete_nodes = add_source_instance_deletes(root, physical_id, [alloc])
            clear = add(_action("clear_template", root, logical_id, physical_id, transitionMode="jormungandr_exchange"), source_delete_nodes or [alloc])
            configure_deps = [clear]
        configure = add(
            _action(
                "configure_full_template",
                root,
                logical_id,
                physical_id,
                template=_template(slots),
                createSpec=_slot_spec(slots),
                slots=[list(slot) for slot in slots],
                transitionMode="jormungandr_exchange",
            ),
            configure_deps,
        )
        bind = add(_action("bind_target_gpu", root, logical_id, physical_id, transitionMode="jormungandr_exchange"), [configure])
        register = add(
            _action("register_mig_devices", root, logical_id, physical_id, slots=[list(slot) for slot in slots], transitionMode="jormungandr_exchange"),
            [bind],
        )
        for create in sorted(exchange_containers.get(gpu_id, []), key=lambda item: str(item.get("id"))):
            inst = dict(create.get("instance") or {})
            slot = _physical_slot(inst["start"], inst["end"], inst["profile"])
            workload = str(inst["workload"])
            batch = int(inst.get("batch") or 1)
            mu = float(inst.get("mu") or 0.0)
            place = add(
                _instance_action("place_instance", root, logical_id, physical_id, slot, workload, batch, mu, "jormungandr_exchange"),
                [register],
            )
            exchange_terminals.append(
                add(_instance_action("activate_instance_route", root, logical_id, physical_id, slot, workload, batch, mu, "jormungandr_exchange"), [place])
            )

    for delete in sorted(exchange_deletes, key=lambda item: str(item.get("id"))):
        inst = dict(delete.get("instance") or {})
        gpu_id = int(inst["gpu_id"])
        physical_id = source_physical_ids.get(gpu_id)
        if not physical_id:
            continue
        slot = _physical_slot(inst["start"], inst["end"], inst["profile"])
        workload = str(inst["workload"])
        if (physical_id, slot[0], slot[1], slot[2], workload) in predeleted_source_keys:
            continue
        root = f"JORM_EXCHANGE_DELETE_GPU_{gpu_id}_{slot[0]}_{slot[1]}_{slot[2]}"
        deactivate = add(_route_action("deactivate_instance_route", root, gpu_id, physical_id, slot, workload, "jormungandr_exchange"), exchange_terminals)
        wait = add(_route_action("wait_instance_drain", root, gpu_id, physical_id, slot, workload, "jormungandr_exchange"), [deactivate])
        exchange_terminals.append(add(_delete_action(root, gpu_id, physical_id, slot, workload, "jormungandr_exchange"), [wait]))

    for (gpu_id, physical_id), slots in sorted(exchange_delete_slots.items()):
        deps = [
            action["id"]
            for action in actions
            if action.get("type") == "delete_instance" and action.get("physical_gpu_id") == physical_id
        ] or exchange_terminals
        exchange_terminals.extend(deps)

    if not exchange_terminals:
        exchange_terminals = []
    compact_terminals: list[str] = []
    compact_source_by_target: dict[tuple[int, int, int, str, str], tuple[str, dict[str, Any]]] = {}
    compact_target_by_source: dict[tuple[str, int, int, str, str], tuple[str, dict[str, Any]]] = {}
    for move in compact_moves:
        move_type = str(move.get("type"))
        source_key = "source_instance" if str(move.get("type")) == "keep_container" else "from_instance"
        target_key = "target_instance" if str(move.get("type")) == "keep_container" else "to_instance"
        source_inst = dict(move.get(source_key) or {})
        target_inst = dict(move.get(target_key) or {})
        if source_inst and target_inst and target_inst.get("gpu_id") is not None:
            compact_source_by_target[
                (
                    int(target_inst["gpu_id"]),
                    int(target_inst["start"]),
                    int(target_inst["end"]),
                    str(target_inst["profile"]),
                    str(target_inst["workload"]),
                )
            ] = (move_type, source_inst)
        if source_inst and target_inst and source_inst.get("gpu_id") is not None:
            source_origin = str(source_inst.get("origin") or "")
            source_gpu = int(source_inst["gpu_id"])
            source_physical = exchange_physical_ids.get(source_gpu) if source_origin == "exchange" else source_physical_ids.get(source_gpu)
            if source_physical:
                source_slot = _physical_slot(source_inst["start"], source_inst["end"], source_inst["profile"])
                compact_target_by_source[
                    (
                        source_physical,
                        source_slot[0],
                        source_slot[1],
                        source_slot[2],
                        str(source_inst["workload"]),
                    )
                ] = (move_type, target_inst)

    def target_instance_key(inst: dict[str, Any]) -> tuple[int, int, int, str, str]:
        return (
            int(inst["gpu_id"]),
            int(inst["start"]),
            int(inst["end"]),
            str(inst["profile"]),
            str(inst["workload"]),
        )

    def repartition_ready(repartition: dict[str, Any], activated_targets: dict[tuple[int, int, int, str, str], str]) -> tuple[bool, list[dict[str, Any]]]:
        target_slots = [
            _physical_slot(inst["start"], inst["end"], inst["profile"])
            for inst in list(repartition.get("instances") or [])
        ]
        old_sources = list(repartition.get("old_instances") or [])
        old_slots = [
            _physical_slot(inst["start"], inst["end"], inst["profile"])
            for inst in old_sources
        ]
        layout_unchanged = sorted(old_slots) == sorted(target_slots)
        blockers: list[dict[str, Any]] = []
        for inst in old_sources:
            old = dict(inst)
            old_origin = str(old.get("origin") or "")
            old_gpu = int(old.get("gpu_id", repartition.get("gpu_id", 0)))
            old_physical = exchange_physical_ids.get(old_gpu) if old_origin == "exchange" else source_physical_ids.get(old_gpu)
            if not old_physical:
                continue
            old_slot = _physical_slot(old["start"], old["end"], old["profile"])
            old_key = (old_physical, old_slot[0], old_slot[1], old_slot[2], str(old["workload"]))
            move_info = compact_target_by_source.get(old_key)
            if move_info is None:
                continue
            move_type, target_inst = move_info
            if move_type == "keep_container" and _same_instance_location(old, target_inst) and layout_unchanged:
                continue
            target_key = target_instance_key(target_inst)
            if target_key not in activated_targets:
                blockers.append(
                    {
                        "phase": "compact",
                        "type": "repartition_gpu",
                        "gpu_id": repartition.get("gpu_id"),
                        "sourceInstance": _yamlable(old),
                        "targetInstance": _yamlable(target_inst),
                        "moveType": move_type,
                        "reason": "target_not_active_before_repartition",
                    }
                )
        return (not blockers, blockers)

    target_activation_nodes: dict[tuple[int, int, int, str, str], str] = {}
    remaining_repartitions = sorted(compact_repartitions, key=lambda item: int(item.get("gpu_id", 0)))
    while remaining_repartitions:
        selected_index = -1
        selected_blockers: list[dict[str, Any]] = []
        for idx, candidate in enumerate(remaining_repartitions):
            ready, blockers = repartition_ready(candidate, target_activation_nodes)
            if ready:
                selected_index = idx
                break
            selected_blockers.extend(blockers)
        if selected_index < 0:
            raise UnsupportedJormungandrReplayError(
                (
                    "Jormungandr compact repartitions have a migration cycle that cannot be "
                    "faithfully replayed without native container migration."
                ),
                unsupported=selected_blockers[:8],
            )
        repartition = remaining_repartitions.pop(selected_index)
        gpu_id = int(repartition["gpu_id"])
        physical_id = final_physical_ids[gpu_id]
        root = f"JORM_COMPACT_REPARTITION_GPU_{gpu_id}"
        entry_deps = list(exchange_terminals)
        pre_clear_delete_nodes: list[str] = []
        seen_old_sources: set[tuple[str, int, int, str, str]] = set()
        old_sources = list(repartition.get("old_instances") or [])
        target_slots = [
            _physical_slot(inst["start"], inst["end"], inst["profile"])
            for inst in list(repartition.get("instances") or [])
        ]
        preserve_slots = [
            _physical_slot(inst["start"], inst["end"], inst["profile"])
            for inst in list(repartition.get("preserve_instances") or [])
        ]
        create_slots = [
            _physical_slot(inst["start"], inst["end"], inst["profile"])
            for inst in list(repartition.get("create_instances") or [])
        ]
        delete_slots = [
            _physical_slot(inst["start"], inst["end"], inst["profile"])
            for inst in list(repartition.get("delete_instances") or [])
        ]
        old_slots = [
            _physical_slot(inst["start"], inst["end"], inst["profile"])
            for inst in old_sources
        ]
        layout_unchanged = sorted(old_slots) == sorted(target_slots)
        for inst in old_sources:
            old = dict(inst)
            old_origin = str(old.get("origin") or "")
            old_gpu = int(old.get("gpu_id", gpu_id))
            old_physical = exchange_physical_ids.get(old_gpu) if old_origin == "exchange" else source_physical_ids.get(old_gpu)
            if not old_physical:
                continue
            slot = _physical_slot(old["start"], old["end"], old["profile"])
            workload = str(old["workload"])
            old_key = (old_physical, slot[0], slot[1], slot[2], workload)
            if old_key in seen_old_sources:
                continue
            seen_old_sources.add(old_key)
            move_info = compact_target_by_source.get(old_key)
            if move_info is not None:
                move_type, target_inst = move_info
                if move_type == "keep_container" and _same_instance_location(old, target_inst) and layout_unchanged:
                    continue
                target_dep = target_activation_nodes.get(target_instance_key(target_inst))
                if not target_dep:
                    raise UnsupportedJormungandrReplayError(
                        (
                            "Jormungandr compact repartition would destroy a surviving source instance "
                            "before its target is active."
                        ),
                        unsupported=[
                            {
                                "phase": "compact",
                                "type": "repartition_gpu",
                                "gpu_id": gpu_id,
                                "physical_gpu_id": physical_id,
                                "sourceInstance": _yamlable(old),
                                "targetInstance": _yamlable(target_inst),
                                "moveType": move_type,
                                "reason": "target_not_active_before_repartition",
                            }
                        ],
                    )
                delete_deps = [target_dep]
            else:
                delete_deps = entry_deps
            if old_key in predeleted_source_keys:
                continue
            predeleted_source_keys.add(old_key)
            old_root = f"{root}_OLD_{old_gpu}_{slot[0]}_{slot[1]}_{slot[2]}"
            deactivate = add(_route_action("deactivate_instance_route", old_root, old_gpu, old_physical, slot, workload, "jormungandr_compact"), delete_deps)
            wait = add(_route_action("wait_instance_drain", old_root, old_gpu, old_physical, slot, workload, "jormungandr_compact"), [deactivate])
            pre_clear_delete_nodes.append(add(_delete_action(old_root, old_gpu, old_physical, slot, workload, "jormungandr_compact"), [wait]))
        if layout_unchanged and not pre_clear_delete_nodes:
            register_deps = entry_deps
        elif preserve_slots:
            patch = add(
                _action(
                    "configure_partial_profile",
                    root,
                    gpu_id,
                    physical_id,
                    template=_template(target_slots),
                    deleteSlots=[list(slot) for slot in sorted(set(delete_slots))],
                    createSlots=[list(slot) for slot in sorted(set(create_slots))],
                    preserveSlots=[list(slot) for slot in sorted(set(preserve_slots))],
                    deleteSpec=_slot_spec(sorted(set(delete_slots))),
                    createSpec=_slot_spec(sorted(set(create_slots))),
                    preserveSpec=_slot_spec(sorted(set(preserve_slots))),
                    transitionMode="jormungandr_compact",
                ),
                pre_clear_delete_nodes or entry_deps,
            )
            register = add(
                _action("register_mig_devices", root, gpu_id, physical_id, slots=[list(slot) for slot in target_slots], transitionMode="jormungandr_compact"),
                [patch],
            )
            register_deps = [register]
        else:
            clear = add(
                _action(
                    "clear_template",
                    root,
                    gpu_id,
                    physical_id,
                    template="+".join(str(_profile_size(slot[2])) for slot in target_slots),
                    deleteSlots=[list(slot) for slot in target_slots],
                    slotCount=len(target_slots),
                    transitionMode="jormungandr_compact",
                ),
                pre_clear_delete_nodes or entry_deps,
            )
            configure = add(
                _action(
                    "configure_full_template",
                    root,
                    gpu_id,
                    physical_id,
                    template=_template(target_slots),
                    createSpec=_slot_spec(target_slots),
                    slots=[list(slot) for slot in target_slots],
                    transitionMode="jormungandr_compact",
                ),
                [clear],
            )
            register = add(
                _action("register_mig_devices", root, gpu_id, physical_id, slots=[list(slot) for slot in target_slots], transitionMode="jormungandr_compact"),
                [configure],
            )
            register_deps = [register]
        for raw_inst in sorted(list(repartition.get("instances") or []), key=lambda item: (int(item["start"]), int(item["end"]), str(item["workload"]))):
            slot = _physical_slot(raw_inst["start"], raw_inst["end"], raw_inst["profile"])
            workload = str(raw_inst["workload"])
            batch = int(raw_inst.get("batch") or 1)
            mu = float(raw_inst.get("mu") or 0.0)
            move_info = compact_source_by_target.get((gpu_id, int(raw_inst["start"]), int(raw_inst["end"]), str(raw_inst["profile"]), workload))
            if move_info is not None:
                move_type, source_inst = move_info
                if move_type == "keep_container" and _same_instance_location(source_inst, raw_inst) and layout_unchanged:
                    continue
            place = add(_instance_action("place_instance", root, gpu_id, physical_id, slot, workload, batch, mu, "jormungandr_compact"), register_deps)
            activate = add(_instance_action("activate_instance_route", root, gpu_id, physical_id, slot, workload, batch, mu, "jormungandr_compact"), [place])
            compact_terminals.append(activate)
            target_activation_nodes[target_instance_key(raw_inst)] = activate
            if move_info is None:
                continue
            move_type, source_inst = move_info
            old = dict(source_inst)
            old_origin = str(old.get("origin") or "")
            old_gpu = int(old.get("gpu_id", gpu_id))
            old_physical = exchange_physical_ids.get(old_gpu) if old_origin == "exchange" else source_physical_ids.get(old_gpu)
            if not old_physical:
                continue
            old_slot = _physical_slot(old["start"], old["end"], old["profile"])
            old_workload = str(old["workload"])
            old_key = (old_physical, old_slot[0], old_slot[1], old_slot[2], old_workload)
            if old_key in predeleted_source_keys:
                continue
            predeleted_source_keys.add(old_key)
            old_root = f"{root}_MIGRATE_SOURCE_{old_gpu}_{old_slot[0]}_{old_slot[1]}_{old_slot[2]}"
            deactivate = add(_route_action("deactivate_instance_route", old_root, old_gpu, old_physical, old_slot, old_workload, "jormungandr_compact"), [activate])
            wait = add(_route_action("wait_instance_drain", old_root, old_gpu, old_physical, old_slot, old_workload, "jormungandr_compact"), [deactivate])
            compact_terminals.append(add(_delete_action(old_root, old_gpu, old_physical, old_slot, old_workload, "jormungandr_compact"), [wait]))

    final_physical_set = set(final_physical_ids.values())
    for item in compact_returns:
        gpu_id = int(item["gpu_id"])
        physical_id = exchange_physical_ids.get(gpu_id)
        if not physical_id:
            continue
        if gpu_id in final_physical_ids or physical_id in final_physical_set:
            continue
        root = f"JORM_RETURN_EXCHANGE_GPU_{gpu_id}"
        return_delete_nodes: list[str] = []
        delete_entry_deps = compact_terminals or exchange_terminals
        for create in sorted(exchange_containers.get(gpu_id, []), key=lambda action: str(action.get("id"))):
            inst = dict(create.get("instance") or {})
            slot = _physical_slot(inst["start"], inst["end"], inst["profile"])
            workload = str(inst["workload"])
            old_root = f"{root}_OLD_{slot[0]}_{slot[1]}_{slot[2]}_{workload}"
            deactivate = add(
                _route_action("deactivate_instance_route", old_root, f"jorm-exchange-{gpu_id}", physical_id, slot, workload, "jormungandr_compact"),
                delete_entry_deps,
            )
            wait = add(_route_action("wait_instance_drain", old_root, f"jorm-exchange-{gpu_id}", physical_id, slot, workload, "jormungandr_compact"), [deactivate])
            return_delete_nodes.append(add(_delete_action(old_root, f"jorm-exchange-{gpu_id}", physical_id, slot, workload, "jormungandr_compact"), [wait]))
        clear = add(
            _action("clear_template", root, f"jorm-exchange-{gpu_id}", physical_id, transitionMode="jormungandr_compact"),
            return_delete_nodes or compact_terminals or exchange_terminals,
        )
        add(_action("return_gpu", root, f"jorm-exchange-{gpu_id}", physical_id, transitionMode="jormungandr_compact"), [clear])

    exchange_physical_set = set(exchange_physical_ids.values())
    for source_gpu_id, physical_id in sorted(source_physical_ids.items()):
        if not physical_id or physical_id in final_physical_set or physical_id in exchange_physical_set:
            continue
        root = f"JORM_RETURN_SOURCE_GPU_{source_gpu_id}"
        deps = [
            action["id"]
            for action in actions
            if action.get("type") == "delete_instance" and action.get("physical_gpu_id") == physical_id
        ] or compact_terminals or exchange_terminals
        clear = add(_action("clear_template", root, source_gpu_id, physical_id, transitionMode="jormungandr_compact"), deps)
        add(_action("return_gpu", root, source_gpu_id, physical_id, transitionMode="jormungandr_compact"), [clear])

    return actions, explicit_action_dag(actions, node_deps, name=str(transition_plan.get("stage_name") or "jormungandr-canonical-dag"))


def _action(action_type: str, root: str, logical_gpu_id: Any, physical_id: str, **fields: Any) -> dict[str, Any]:
    return {
        "type": action_type,
        "gpu_id": logical_gpu_id,
        "logical_gpu_id": logical_gpu_id,
        "physical_gpu_id": physical_id,
        "abstractRoot": root,
        **fields,
    }


def _instance_action(
    action_type: str,
    root: str,
    logical_gpu_id: Any,
    physical_id: str,
    slot: tuple[int, int, str],
    workload: str,
    batch: int,
    mu: float,
    mode: str,
) -> dict[str, Any]:
    action = _route_action(action_type, root, logical_gpu_id, physical_id, slot, workload, mode)
    action["batch"] = int(batch)
    if action_type in {"place_instance", "activate_instance_route"}:
        action["producesCapacity"] = [
            {
                "workload": workload,
                "slot": list(slot),
                "batch": int(batch),
                "mu": float(mu),
            }
        ]
    return action


def _route_action(action_type: str, root: str, logical_gpu_id: Any, physical_id: str, slot: tuple[int, int, str], workload: str, mode: str) -> dict[str, Any]:
    return _action(
        action_type,
        root,
        logical_gpu_id,
        physical_id,
        slot=list(slot),
        workload=workload,
        model=workload,
        transitionMode=mode,
    )


def _delete_action(root: str, logical_gpu_id: Any, physical_id: str, slot: tuple[int, int, str], workload: str, mode: str) -> dict[str, Any]:
    return _route_action("delete_instance", root, logical_gpu_id, physical_id, slot, workload, mode)


def explicit_action_dag(actions: list[dict[str, Any]], node_deps: dict[str, list[str]], *, name: str) -> dict[str, Any]:
    nodes = []
    for idx, action in enumerate(actions):
        node_id = str(action["id"])
        nodes.append(
            {
                "id": node_id,
                "index": idx,
                "type": str(action["type"]),
                "rootId": str(action.get("abstractRoot") or node_id),
                "action": _yamlable(action),
                "dependsOn": list(node_deps.get(node_id, [])),
            }
        )
    return {
        "representation": "migrant.phased-action-dag/v1",
        "name": name,
        "nodeCount": len(nodes),
        "actionCount": len(actions),
        "nodes": nodes,
    }


def compact_action_dag(action_dag: dict[str, Any]) -> dict[str, Any]:
    return {
        "representation": action_dag.get("representation"),
        "actionCount": int(action_dag.get("actionCount", 0)),
        "nodeCount": int(action_dag.get("nodeCount", 0)),
        "edgeCount": sum(len(node.get("dependsOn") or []) for node in action_dag.get("nodes", [])),
    }


def cluster_state_to_dict(state: ClusterState) -> dict[str, Any]:
    ensure_state_metadata(state)
    return {
        "metadata": _yamlable(state.metadata),
        "gpus": [
            {
                "gpuId": int(gpu.gpu_id),
                "source": "planned",
                "instances": [
                    {
                        "start": int(inst.start),
                        "end": int(inst.end),
                        "profile": str(inst.profile),
                        "workload": inst.workload,
                        "batch": inst.batch,
                        "mu": float(getattr(inst, "mu", 0.0)),
                        "preserved": bool(getattr(inst, "preserved", False)),
                    }
                    for inst in sorted(gpu.instances, key=lambda item: (item.start, item.end, item.profile, str(item.workload)))
                ],
            }
            for gpu in sorted(state.real_gpus(), key=lambda item: int(item.gpu_id))
        ],
    }


def _template(slots: list[tuple[int, int, str]]) -> str:
    return "+".join(str(_profile_size(slot[2])) for slot in sorted(slots, key=lambda item: (item[0], item[1], item[2])))


def _slot_spec(slots: list[tuple[int, int, str]] | tuple[tuple[int, int, str], ...]) -> str:
    return ",".join(f"{start}:{end - start}:{profile}" for start, end, profile in sorted(slots))


def _profile_size(profile: str) -> int:
    return LOGICAL_PROFILE_SIZE.get(str(profile), 0)


def _physical_slot(start: Any, end: Any, profile: Any) -> tuple[int, int, str]:
    profile_name = str(profile)
    slot_start = int(start)
    return (slot_start, _physical_end(slot_start, profile_name), profile_name)


def _same_instance_location(source: dict[str, Any], target: dict[str, Any]) -> bool:
    return (
        int(source.get("gpu_id", -1)) == int(target.get("gpu_id", -2))
        and int(source.get("start", -1)) == int(target.get("start", -2))
        and int(source.get("end", -1)) == int(target.get("end", -2))
        and str(source.get("profile")) == str(target.get("profile"))
        and str(source.get("workload")) == str(target.get("workload"))
    )


def _physical_end(start: int, profile: str) -> int:
    return start + PHYSICAL_PROFILE_SIZE.get(str(profile), int(PHYSICAL_PROFILE_SIZE["1g"]))


def _logical_end(start: int, profile: str) -> int:
    return start + LOGICAL_PROFILE_SIZE.get(str(profile), int(LOGICAL_PROFILE_SIZE["1g"]))


def _yamlable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _yamlable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_yamlable(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)
