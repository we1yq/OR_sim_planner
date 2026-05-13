from __future__ import annotations

"""Baseline transition planners that differ only in root scheduling policy.

These are ablation-only planners. They all reuse the same full-action
generator and simulator, then choose a different amount of the root-scored
candidate plan to execute per iteration.
"""

import json
import time
from typing import Any

from ..physical_ids import ensure_state_metadata
from ..state import deepcopy_state
from ..transition_common import matches_target_state
from ..transition_engine import (
    _active_pid_set,
    _advance_drain,
    _choose_nonconflicting_groups,
    _drain_map,
    _group_scores,
    _peak_from_actions,
    _progress_signature,
    _select_actions_for_root,
    plan_full_action_plan,
    prepare_transition_runtime,
    simulate_transition_actions,
)


SERIAL_ROOT_NAME = "transition.serial_root_baseline"
DRAIN_AWARE_NAME = "transition.drain_aware_baseline"
FULL_PLAN_NAME = "transition.full_plan_baseline"

__all__ = [
    "run_serial_root_baseline",
    "run_drain_aware_baseline",
    "run_full_plan_baseline",
]


def _dedup_actions(actions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    seen = set()
    for action in actions:
        key = json.dumps(action, sort_keys=True, default=str)
        if key in seen:
            continue
        seen.add(key)
        out.append(action)
    return out


def _actions_for_groups(plan: dict[str, Any], groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    actions = []
    for group in groups:
        actions.extend(_select_actions_for_root(plan, group["root_id"]))
    return _dedup_actions(actions)


def run_serial_root_baseline(**kwargs: Any) -> dict[str, Any]:
    """Execute one scored root transition per iteration."""

    res = _run_root_scheduling_baseline(mode="serial_root_baseline", **kwargs)
    res["transition_planner_module"] = SERIAL_ROOT_NAME
    return res


def run_drain_aware_baseline(**kwargs: Any) -> dict[str, Any]:
    """Execute non-conflicting scored roots per iteration."""

    res = _run_root_scheduling_baseline(mode="drain_aware_baseline", **kwargs)
    res["transition_planner_module"] = DRAIN_AWARE_NAME
    return res


def run_full_plan_baseline(**kwargs: Any) -> dict[str, Any]:
    """Execute the whole candidate full plan each iteration."""

    res = _run_root_scheduling_baseline(mode="full_plan_baseline", **kwargs)
    res["transition_planner_module"] = FULL_PLAN_NAME
    return res


def _run_root_scheduling_baseline(
    *,
    source_state: Any,
    target_state: Any,
    src_arrival: list[float] | tuple[float, ...] | dict[str, float],
    tgt_arrival: list[float] | tuple[float, ...] | dict[str, float],
    workload_names: list[str] | tuple[str, ...] | None = None,
    stage_name: str = "stage_baseline",
    max_iters: int = 20,
    mode: str,
) -> dict[str, Any]:
    current_state = prepare_transition_runtime(source_state, target_state)
    ensure_state_metadata(current_state)
    initial_runtime_state = deepcopy_state(current_state)
    iterations = []
    all_executed_actions = []
    final_plan = None
    reached_target = matches_target_state(current_state, target_state) and len(_drain_map(current_state)) == 0
    peak_active_gpu = len(_active_pid_set(current_state))
    start = time.perf_counter()

    for iter_idx in range(1, max_iters + 1):
        if reached_target:
            break
        _advance_drain(current_state)
        full_plan = plan_full_action_plan(
            current_state,
            target_state,
            src_arrival,
            tgt_arrival,
            workload_names=workload_names,
            stage_name=f"{stage_name}_iter{iter_idx}",
        )
        final_plan = full_plan
        groups = _group_scores(current_state, target_state, list(full_plan.get("plan_items", [])))
        if not groups:
            break

        if mode == "serial_root_baseline":
            chosen_groups = [groups[0]]
            chosen_actions = _actions_for_groups(full_plan, chosen_groups)
        elif mode == "drain_aware_baseline":
            chosen_groups = _choose_nonconflicting_groups(groups)
            chosen_actions = _actions_for_groups(full_plan, chosen_groups)
        elif mode == "full_plan_baseline":
            chosen_groups = groups
            chosen_actions = _dedup_actions(list(full_plan.get("executed_actions", [])))
        else:
            raise ValueError(f"Unknown transition baseline mode: {mode}")

        next_state = simulate_transition_actions(
            current_state,
            full_plan["planned_state"],
            chosen_actions,
            next_physical_idx=current_state.metadata.get("next_physical_idx", 0),
        )
        made_progress = _progress_signature(next_state) != _progress_signature(current_state)
        reached_target = matches_target_state(next_state, target_state) and len(_drain_map(next_state)) == 0
        iter_peak = _peak_from_actions(current_state, chosen_actions)
        peak_active_gpu = max(peak_active_gpu, iter_peak, len(_active_pid_set(next_state)))
        iterations.append(
            {
                "iteration": iter_idx,
                "full_plan": full_plan,
                "chosen_roots": chosen_groups,
                "chosen_actions": chosen_actions,
                "state_before": deepcopy_state(current_state),
                "state_after": deepcopy_state(next_state),
                "made_progress": made_progress,
                "reached_target": reached_target,
                "iter_peak_active_gpu": iter_peak,
                "active_gpu_after": len(_active_pid_set(next_state)),
            }
        )
        all_executed_actions.extend(chosen_actions)
        current_state = next_state
        if reached_target or not made_progress:
            break

    return {
        "stage_name": stage_name,
        "iterations": iterations,
        "iteration_count": len(iterations),
        "reached_target": reached_target,
        "elapsed_sec": time.perf_counter() - start,
        "executed_actions": all_executed_actions,
        "executed_state": current_state,
        "target_state": deepcopy_state(target_state),
        "initial_runtime_state": initial_runtime_state,
        "peak_active_gpu": peak_active_gpu,
        "source_active_gpu": len(_active_pid_set(initial_runtime_state)),
        "final_active_gpu": len(_active_pid_set(current_state)),
        "final_plan": final_plan,
    }
