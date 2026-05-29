from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .interfaces import TransitionPlanner
from . import effect_aware_dag


PlannerRole = Literal["production"]


@dataclass(frozen=True)
class TransitionPlannerEntry:
    name: str
    runner: TransitionPlanner
    role: PlannerRole
    description: str
    aliases: tuple[str, ...] = ()


PLANNER_CATALOG: dict[str, TransitionPlannerEntry] = {
    "effect_aware_dag": TransitionPlannerEntry(
        name="effect_aware_dag",
        runner=effect_aware_dag.run,
        role="production",
        description=(
            "Final transition planner: lower current/target allocation diffs "
            "into an effect-aware executable action DAG with capacity, router, "
            "MIG, physical-GPU, and binding effects."
        ),
        aliases=("transition.effect_aware_dag", "effect_aware", "transition.effect_aware", "final"),
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
