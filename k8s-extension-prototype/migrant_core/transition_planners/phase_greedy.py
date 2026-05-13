from __future__ import annotations

from typing import Any

from ..transition_engine import run_phase_greedy_stage
from .action_plan_formats import build_phased_action_plan, compact_phased_action_plan


NAME = "transition.phase_greedy"
DAG_OUTPUT_NAME = "transition.phase_greedy_with_dag_output"


def run(**kwargs: Any) -> dict[str, Any]:
    res = run_phase_greedy_stage(**kwargs)
    res["transition_planner_module"] = NAME
    return res


def run_with_dag_output(**kwargs: Any) -> dict[str, Any]:
    """Run phase-greedy unchanged, then attach a phased action DAG view."""

    res = run_phase_greedy_stage(**kwargs)
    _attach_phased_action_dag(res)
    res["transition_planner_module"] = DAG_OUTPUT_NAME
    return res


def _attach_phased_action_dag(res: dict[str, Any]) -> None:
    final_plan = dict(res.get("final_plan") or {})
    full_plan_items = list(final_plan.get("plan_items", []))

    for iteration in list(res.get("iterations", [])):
        iteration_actions = list(iteration.get("chosen_actions", []))
        phased = build_phased_action_plan(
            iteration_actions,
            plan_items=full_plan_items,
            name=f"{res.get('stage_name', 'stage')}-iter{iteration.get('iteration', 0)}",
        )
        iteration["phased_action_plan"] = phased
        iteration["phased_action_plan_summary"] = compact_phased_action_plan(phased)

    phased_action_plan = build_phased_action_plan(
        list(res.get("executed_actions", [])),
        plan_items=full_plan_items,
        name=f"{res.get('stage_name', 'stage')}-executed",
    )
    res["phased_action_plan"] = phased_action_plan
    res["phased_action_plan_summary"] = compact_phased_action_plan(phased_action_plan)
