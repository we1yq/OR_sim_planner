from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable

from ..physical_ids import bootstrap_physical_ids_for_state, ensure_state_metadata, get_physical_id
from ..state import ClusterState, MigInstance, deepcopy_state, gpu_map_by_id
from ..transition_common import (
    alloc_from_free_pool,
    classify_gpu_change,
    diff_instances_within_same_template,
    matches_target_state,
    safe_after_removing_gpu,
    safe_after_removing_instance,
)
from ..transition_engine import (
    _get_runtime_entry,
    _nonfree_instances,
    _reroute_destination_candidates,
    prepare_transition_runtime,
    required_arrival_dict,
    simulate_transition_actions,
)
from . import basic_dag
from .action_plan_formats import build_phased_action_plan, compact_phased_action_plan


NAME = "transition.cost_aware_dag"


@dataclass(frozen=True)
class CandidateScore:
    feasible: bool
    peak_active_gpu: int
    service_risk: int
    queued_wait: int
    drain_rounds: int
    reroutes: int
    bridges: int
    pod_deletes: int
    reconfig_seconds: float
    action_count: int

    def as_tuple(self) -> tuple[Any, ...]:
        # Service feasibility is a hard gate, then the paper objective starts:
        # minimize peak active GPUs before minimizing time and disruption.
        return (
            0 if self.feasible else 1,
            self.peak_active_gpu,
            self.service_risk,
            self.queued_wait,
            self.drain_rounds,
            self.reconfig_seconds,
            self.reroutes,
            self.bridges,
            self.pod_deletes,
            self.action_count,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "feasible": self.feasible,
            "peakActiveGpu": self.peak_active_gpu,
            "serviceRisk": self.service_risk,
            "queuedWait": self.queued_wait,
            "drainRounds": self.drain_rounds,
            "reroutes": self.reroutes,
            "bridges": self.bridges,
            "podDeletes": self.pod_deletes,
            "reconfigSeconds": self.reconfig_seconds,
            "actionCount": self.action_count,
            "scoreTuple": list(self.as_tuple()),
        }


def run(
    *,
    source_state: Any,
    target_state: Any,
    src_arrival: list[float] | tuple[float, ...] | dict[str, float],
    tgt_arrival: list[float] | tuple[float, ...] | dict[str, float],
    workload_names: list[str] | tuple[str, ...] | None = None,
    stage_name: str = "stage_cost_aware_dag",
    max_iters: int = 1,
    default_queued: int = 2,
    default_inflight: int = 1,
    override_existing_runtime_for_changed_slots: bool = False,
    **_: Any,
) -> dict[str, Any]:
    """Build a final DAG by scoring transition-mode candidates.

    The baseline `basic_dag` compiles source/target diffs directly into fixed
    abstract actions. This planner keeps the same final-DAG output contract, but
    scores alternative transition modes before lowering them into fine-grained
    actions. The current score is deliberately conservative: service feasibility
    is a hard gate, then peak active GPU count, then queue/drain/reconfig costs.
    """

    start = time.perf_counter()
    current_state = prepare_transition_runtime(
        source_state,
        target_state,
        default_queued=default_queued,
        default_inflight=default_inflight,
        override_existing_changed_slots=override_existing_runtime_for_changed_slots,
    )
    ensure_state_metadata(current_state)
    bootstrap_physical_ids_for_state(current_state)
    target_state = deepcopy_state(target_state)
    ensure_state_metadata(target_state)

    required = required_arrival_dict(src_arrival, tgt_arrival, workload_names=workload_names)
    actions, plan_items, decision_trace = _build_cost_aware_actions(
        source_state=current_state,
        target_state=target_state,
        required=required,
    )
    actions = basic_dag._coalesce_slot_delete_pods(actions)
    basic_dag._assert_reroute_destinations_stable(current_state, target_state, actions)
    planned_state = basic_dag._planned_state_for_actions(current_state, target_state, actions)
    executed_state = simulate_transition_actions(
        source_state=current_state,
        target_state=planned_state,
        fine_actions=actions,
        next_physical_idx=current_state.metadata.get("next_physical_idx", 0),
    )
    executed_state = basic_dag._drop_available_physical_gpus(executed_state)
    dag = build_phased_action_plan(actions, plan_items=plan_items, name=f"{stage_name}-final")
    peak_active_gpu = basic_dag._peak_serving_gpu_from_actions(current_state, actions)
    final_plan = {
        "stage_name": stage_name,
        "required": required,
        "fine_actions": actions,
        "executed_actions": actions,
        "blocked_actions": [],
        "planned_state": planned_state,
        "executed_state": executed_state,
        "plan_items": plan_items,
        "planner_objective_order": [
            "filter service-infeasible candidates",
            "minimize peak active physical GPUs",
            "minimize queued wait and drain rounds",
            "minimize MIG benchmark reconfiguration time",
            "minimize reroute/bridge/pod-delete disruption",
        ],
        "runtime_assumptions": {
            "defaultQueued": int(default_queued),
            "defaultInflight": int(default_inflight),
            "overrideExistingChangedSlots": bool(override_existing_runtime_for_changed_slots),
        },
        "candidate_decisions": decision_trace,
    }
    return {
        "stage_name": stage_name,
        "iterations": [
            {
                "iteration": 1,
                "candidate_actions": actions,
                "chosen_actions": actions,
                "state_before": deepcopy_state(current_state),
                "state_after": deepcopy_state(executed_state),
                "made_progress": True,
                "reached_target": matches_target_state(executed_state, target_state),
                "phased_action_plan": dag,
                "phased_action_plan_summary": compact_phased_action_plan(dag),
                "candidate_decisions": decision_trace,
            }
        ],
        "iteration_count": 1,
        "reached_target": matches_target_state(executed_state, target_state),
        "elapsed_sec": time.perf_counter() - start,
        "executed_actions": actions,
        "executed_state": executed_state,
        "target_state": deepcopy_state(target_state),
        "initial_runtime_state": deepcopy_state(current_state),
        "peak_active_gpu": peak_active_gpu,
        "source_active_gpu": len(basic_dag._active_serving_pid_set(current_state)),
        "final_active_gpu": len(basic_dag._active_serving_pid_set(executed_state)),
        "final_plan": final_plan,
        "phased_action_plan": dag,
        "phased_action_plan_summary": compact_phased_action_plan(dag),
        "transition_planner_module": NAME,
        "max_iters_ignored": max_iters,
    }


def _build_cost_aware_actions(
    *,
    source_state: ClusterState,
    target_state: ClusterState,
    required: dict[str, float],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    src_map = {
        gpu_id: gpu
        for gpu_id, gpu in gpu_map_by_id(source_state).items()
        if not basic_dag._is_available_physical_gpu(gpu)
    }
    tgt_map = gpu_map_by_id(target_state)
    all_gpu_ids = sorted(set(src_map) | set(tgt_map))
    free_pool = basic_dag._build_initial_available_pool(source_state, src_map)
    actions: list[dict[str, Any]] = []
    plan_items: list[dict[str, Any]] = []
    decision_trace: list[dict[str, Any]] = []

    instance_diff_ids = [
        gpu_id
        for gpu_id in all_gpu_ids
        if classify_gpu_change(src_map.get(gpu_id), tgt_map.get(gpu_id)) == "instance_diff"
    ]
    remove_ids = [
        gpu_id
        for gpu_id in all_gpu_ids
        if classify_gpu_change(src_map.get(gpu_id), tgt_map.get(gpu_id)) == "remove_gpu"
    ]
    reconfig_ids = [
        gpu_id
        for gpu_id in all_gpu_ids
        if classify_gpu_change(src_map.get(gpu_id), tgt_map.get(gpu_id)) == "reconfiguration"
    ]
    create_ids = [
        gpu_id
        for gpu_id in all_gpu_ids
        if classify_gpu_change(src_map.get(gpu_id), tgt_map.get(gpu_id)) == "create_gpu"
    ]

    for gpu_id in instance_diff_ids:
        _append_cost_aware_instance_diff(actions, plan_items, decision_trace, source_state, target_state, gpu_id, required)

    for gpu_id in remove_ids:
        before = len(actions)
        basic_dag._append_delete_gpu_actions(actions, plan_items, source_state, target_state, gpu_id, required)
        _record_fixed_decision(decision_trace, "delete_gpu", gpu_id, actions[before:], source_state)
        physical_id = get_physical_id(source_state, gpu_id)
        if physical_id is not None:
            free_pool.append(physical_id)

    for gpu_id in reconfig_ids:
        src_gpu = src_map[gpu_id]
        tgt_gpu = tgt_map[gpu_id]
        old_physical_id = get_physical_id(source_state, gpu_id)
        target_template = tgt_gpu.template_str()
        candidates: list[tuple[str, str | None, list[dict[str, Any]], list[dict[str, Any]], bool]] = []

        local_actions: list[dict[str, Any]] = []
        local_items: list[dict[str, Any]] = []
        basic_dag._append_in_place_reconfiguration_actions(
            local_actions,
            local_items,
            source_state,
            target_state,
            gpu_id,
            old_physical_id,
            target_template,
        )
        candidates.append(
            (
                "in_place_reconfiguration",
                old_physical_id,
                local_actions,
                local_items,
                safe_after_removing_gpu(source_state, src_gpu, required),
            )
        )

        if free_pool:
            new_physical_id = free_pool[-1]
            local_actions = []
            local_items = []
            basic_dag._append_bridge_reconfiguration_actions(
                local_actions,
                local_items,
                source_state,
                target_state,
                gpu_id,
                old_physical_id,
                new_physical_id,
                target_template,
            )
            candidates.append(("bridge_reconfiguration", new_physical_id, local_actions, local_items, True))

        chosen = _choose_candidate(source_state, actions, candidates)
        mode, physical_id, chosen_actions, chosen_items, _ = chosen
        actions.extend(chosen_actions)
        plan_items.extend(chosen_items)
        decision_trace.append(_decision_record("reconfiguration", gpu_id, mode, candidates, source_state, actions_before=actions[:-len(chosen_actions)] if chosen_actions else actions))
        if mode == "bridge_reconfiguration":
            # The bridge side consumes one available GPU; the old side returns.
            alloc_from_free_pool(free_pool)
            if old_physical_id is not None:
                free_pool.append(old_physical_id)

    for gpu_id in create_ids:
        tgt_gpu = tgt_map[gpu_id]
        physical_id = alloc_from_free_pool(free_pool)
        before = len(actions)
        basic_dag._append_create_target_gpu_actions(actions, plan_items, gpu_id, physical_id, tgt_gpu.template_str())
        _record_fixed_decision(decision_trace, "create_target_gpu", gpu_id, actions[before:], source_state)

    return actions, plan_items, decision_trace


def _append_cost_aware_instance_diff(
    actions: list[dict[str, Any]],
    plan_items: list[dict[str, Any]],
    decision_trace: list[dict[str, Any]],
    source_state: ClusterState,
    target_state: ClusterState,
    gpu_id: int,
    required: dict[str, float],
) -> None:
    src_gpu = gpu_map_by_id(source_state)[gpu_id]
    physical_id = get_physical_id(source_state, gpu_id)
    for inst_action in diff_instances_within_same_template(src_gpu, gpu_map_by_id(target_state)[gpu_id]):
        change_type = inst_action["type"]
        before = len(actions)
        if change_type == "workload_change":
            _append_cost_aware_workload_replacement(
                actions,
                plan_items,
                decision_trace,
                source_state,
                target_state,
                gpu_id,
                physical_id,
                inst_action["src"],
                inst_action["tgt"],
                required,
            )
            continue
        else:
            # Keep the baseline lowering for deterministic non-branching changes.
            tmp_actions: list[dict[str, Any]] = []
            tmp_items: list[dict[str, Any]] = []
            # Reuse the baseline GPU-level helper, then keep only this root.
            basic_dag._append_instance_diff_actions(tmp_actions, tmp_items, source_state, target_state, gpu_id, required)
            root = None
            slot = inst_action.get("slot")
            if slot is not None:
                root = f"SLOT_gpu{gpu_id}_{slot[0]}_{slot[1]}_{slot[2]}"
            actions.extend([action for action in tmp_actions if root is None or action.get("abstractRoot") == root])
            plan_items.extend([item for item in tmp_items if root is None or item.get("id") == root])
        if len(actions) > before:
            _record_fixed_decision(decision_trace, change_type, gpu_id, actions[before:], source_state)


def _append_cost_aware_workload_replacement(
    actions: list[dict[str, Any]],
    plan_items: list[dict[str, Any]],
    decision_trace: list[dict[str, Any]],
    source_state: ClusterState,
    target_state: ClusterState,
    gpu_id: int,
    physical_id: str,
    src: MigInstance,
    tgt: MigInstance,
    required: dict[str, float],
) -> None:
    slot = (src.start, src.end, src.profile)
    root = f"SLOT_gpu{gpu_id}_{slot[0]}_{slot[1]}_{slot[2]}"
    candidates: list[tuple[str, str | None, list[dict[str, Any]], list[dict[str, Any]], bool]] = []

    direct_actions: list[dict[str, Any]] = []
    direct_items: list[dict[str, Any]] = []
    basic_dag._append_workload_replacement_actions(
        direct_actions,
        direct_items,
        source_state,
        target_state,
        gpu_id,
        physical_id,
        src,
        tgt,
        required,
        root,
    )
    has_bridge = any(action.get("type") == "bridge_place_instance" for action in direct_actions)
    has_reroute = any(action.get("type") == "reroute_queued_tasks" for action in direct_actions)
    safe = safe_after_removing_instance(source_state, src, required)
    mode = "bridge_workload_replacement" if has_bridge else ("reroute_workload_replacement" if has_reroute else "direct_workload_replacement")
    candidates.append((mode, physical_id, direct_actions, direct_items, safe or has_reroute or has_bridge))

    chosen = _choose_candidate(source_state, actions, candidates)
    mode, _, chosen_actions, chosen_items, _ = chosen
    actions.extend(chosen_actions)
    plan_items.extend(chosen_items)
    decision_trace.append(_decision_record("workload_replacement", gpu_id, mode, candidates, source_state, actions_before=actions[:-len(chosen_actions)] if chosen_actions else actions))


def _choose_candidate(
    source_state: ClusterState,
    prefix_actions: list[dict[str, Any]],
    candidates: list[tuple[str, str | None, list[dict[str, Any]], list[dict[str, Any]], bool]],
) -> tuple[str, str | None, list[dict[str, Any]], list[dict[str, Any]], bool]:
    return min(
        candidates,
        key=lambda candidate: _score_actions(source_state, prefix_actions + candidate[2], candidate[4]).as_tuple(),
    )


def _decision_record(
    decision_type: str,
    gpu_id: int,
    chosen_mode: str,
    candidates: list[tuple[str, str | None, list[dict[str, Any]], list[dict[str, Any]], bool]],
    source_state: ClusterState,
    actions_before: list[dict[str, Any]],
) -> dict[str, Any]:
    records = []
    for mode, physical_id, candidate_actions, _, feasible in candidates:
        records.append(
            {
                "mode": mode,
                "physicalGpuId": physical_id,
                "chosen": mode == chosen_mode,
                "score": _score_actions(source_state, actions_before + candidate_actions, feasible).to_dict(),
                "actionTypes": [str(action.get("type")) for action in candidate_actions],
            }
        )
    return {"type": decision_type, "gpuId": gpu_id, "chosenMode": chosen_mode, "candidates": records}


def _record_fixed_decision(
    decision_trace: list[dict[str, Any]],
    decision_type: str,
    gpu_id: int,
    chosen_actions: list[dict[str, Any]],
    source_state: ClusterState,
) -> None:
    decision_trace.append(
        {
            "type": decision_type,
            "gpuId": gpu_id,
            "chosenMode": decision_type,
            "candidates": [
                {
                    "mode": decision_type,
                    "chosen": True,
                    "score": _score_actions(source_state, chosen_actions, True).to_dict(),
                    "actionTypes": [str(action.get("type")) for action in chosen_actions],
                }
            ],
        }
    )


def _score_actions(source_state: ClusterState, actions: list[dict[str, Any]], feasible: bool) -> CandidateScore:
    return CandidateScore(
        feasible=feasible,
        peak_active_gpu=basic_dag._peak_serving_gpu_from_actions(source_state, actions),
        service_risk=0 if feasible else 1,
        queued_wait=sum(_queued_wait(action) for action in actions),
        drain_rounds=sum(int(action.get("rounds", 0) or 0) for action in actions if action.get("type") == "mark_draining_instance"),
        reroutes=sum(1 for action in actions if action.get("type") == "reroute_queued_tasks"),
        bridges=sum(1 for action in actions if action.get("type") == "bridge_place_instance"),
        pod_deletes=sum(1 for action in actions if action.get("type") in {"delete_pods", "delete_bridge_pod"}),
        reconfig_seconds=sum(_estimated_reconfig_seconds(action) for action in actions),
        action_count=len(actions),
    )


def _queued_wait(action: dict[str, Any]) -> int:
    if action.get("type") == "mark_draining_instance":
        return max(0, int(action.get("rounds", 0) or 0) - int(action.get("inflight", 0) or 0))
    return 0


def _estimated_reconfig_seconds(action: dict[str, Any]) -> float:
    action_type = action.get("type")
    if action_type == "configure_full_template":
        return _TEMPLATE_ALLOCATABLE_SECONDS.get(_canonical_template(str(action.get("template", ""))), _DEFAULT_TEMPLATE_ALLOCATABLE_SECONDS)
    if action_type == "clear_template":
        return _EMPTY_SUCCESS_SECONDS.get(_canonical_template(str(action.get("template", ""))), _DEFAULT_EMPTY_SUCCESS_SECONDS)
    return 0.0


def _canonical_template(template: str) -> str:
    if not template:
        return ""
    parts = sorted((int(part) for part in template.split("+") if part), reverse=True)
    return "+".join(str(part) for part in parts)


_TEMPLATE_ALLOCATABLE_SECONDS = {
    "7": 102.539,
    "4+3": 120.651,
    "4+2+1": 112.627,
    "4+1+1+1": 112.629,
    "3+3": 110.611,
    "3+2+1": 112.603,
    "3+1+1+1": 114.648,
    "3+2+2": 112.619,
    "3+2+1+1": 112.612,
    "3+1+1+1+1": 112.652,
    "2+2+2+1": 112.609,
    "2+2+1+1+1": 114.619,
    "2+1+1+1+1+1": 120.811,
    "1+1+1+1+1+1+1": 112.613,
}
_EMPTY_SUCCESS_SECONDS = {
    "7": 42.237,
    "4+3": 40.229,
    "4+2+1": 40.238,
    "4+1+1+1": 40.237,
    "3+3": 40.236,
    "3+2+1": 40.238,
    "3+1+1+1": 40.238,
    "3+2+2": 40.235,
    "3+2+1+1": 40.244,
    "3+1+1+1+1": 40.237,
    "2+2+2+1": 40.235,
    "2+2+1+1+1": 42.247,
    "2+1+1+1+1+1": 40.236,
    "1+1+1+1+1+1+1": 40.236,
}
_DEFAULT_TEMPLATE_ALLOCATABLE_SECONDS = 113.203
_DEFAULT_EMPTY_SUCCESS_SECONDS = 40.5
