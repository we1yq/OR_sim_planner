from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .interfaces import TransitionPlanner
from . import basic_dag, cost_aware_dag, phase_greedy, root_scheduling_baselines


PlannerRole = Literal["production", "compatibility-output", "ablation-baseline", "experimental"]


@dataclass(frozen=True)
class TransitionPlannerEntry:
    name: str
    runner: TransitionPlanner
    role: PlannerRole
    description: str
    aliases: tuple[str, ...] = ()


PLANNER_CATALOG: dict[str, TransitionPlannerEntry] = {
    "basic_dag": TransitionPlannerEntry(
        name="basic_dag",
        runner=basic_dag.run,
        role="production",
        description=(
            "MIGRANT baseline final-DAG planner: materialized MILP target -> "
            "rule-based abstract transition actions -> executable dependency DAG."
        ),
        aliases=(
            "offline_final_dag",
            "resource_aware_dag",
            "transition.resource_aware_dag",
            "transition.offline_final_dag",
            "transition.basic_dag",
            "final_dag",
        ),
    ),
    "cost_aware_dag": TransitionPlannerEntry(
        name="cost_aware_dag",
        runner=cost_aware_dag.run,
        role="experimental",
        description=(
            "Cost-aware DAG planner entry point. It will score transition "
            "candidates with queue, drain, profile, and MIG benchmark costs."
        ),
        aliases=("transition.cost_aware_dag",),
    ),
    "phase_greedy": TransitionPlannerEntry(
        name="phase_greedy",
        runner=phase_greedy.run,
        role="ablation-baseline",
        description="Current phase-greedy transition planner with linear action output.",
        aliases=("transition.phase_greedy",),
    ),
    "phase_greedy_with_dag_output": TransitionPlannerEntry(
        name="phase_greedy_with_dag_output",
        runner=phase_greedy.run_with_dag_output,
        role="compatibility-output",
        description=(
            "Phase-greedy execution semantics with an attached "
            "migrant.phased-action-dag/v1 output view."
        ),
        aliases=(
            "phase_greedy_dag",
            "phase_greedy_dag_output",
            "phased_dag",
            "dag",
            "transition.phase_greedy_dag",
            "transition.phase_greedy_with_dag_output",
        ),
    ),
    "serial_root_baseline": TransitionPlannerEntry(
        name="serial_root_baseline",
        runner=root_scheduling_baselines.run_serial_root_baseline,
        role="ablation-baseline",
        description="Ablation baseline that executes one scored root transition per iteration.",
        aliases=("transition.serial_root_baseline",),
    ),
    "drain_aware_baseline": TransitionPlannerEntry(
        name="drain_aware_baseline",
        runner=root_scheduling_baselines.run_drain_aware_baseline,
        role="ablation-baseline",
        description="Ablation baseline that executes non-conflicting scored roots per iteration.",
        aliases=("transition.drain_aware_baseline",),
    ),
    "full_plan_baseline": TransitionPlannerEntry(
        name="full_plan_baseline",
        runner=root_scheduling_baselines.run_full_plan_baseline,
        role="ablation-baseline",
        description="Ablation baseline that executes the whole candidate full plan each iteration.",
        aliases=("transition.full_plan_baseline",),
    ),
}


def planner_aliases() -> dict[str, str]:
    aliases: dict[str, str] = {}
    for name, entry in PLANNER_CATALOG.items():
        aliases[name] = name
        for alias in entry.aliases:
            aliases[alias] = name
    return aliases


def planner_runners(include_aliases: bool = True) -> dict[str, TransitionPlanner]:
    runners = {name: entry.runner for name, entry in PLANNER_CATALOG.items()}
    if include_aliases:
        for alias, name in planner_aliases().items():
            runners[alias] = PLANNER_CATALOG[name].runner
    return runners


def canonical_planner_name(name: str) -> str:
    aliases = planner_aliases()
    if name not in aliases:
        raise ValueError(
            f"Unknown transition planner {name!r}. "
            f"Supported planners: {', '.join(sorted(PLANNER_CATALOG))}"
        )
    return aliases[name]
