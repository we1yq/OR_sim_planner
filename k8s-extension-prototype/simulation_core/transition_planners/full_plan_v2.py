from __future__ import annotations

from typing import Any

from ._iterative_baseline import run_iterative_baseline


NAME = "transition.full_plan_v2"


def run(**kwargs: Any) -> dict[str, Any]:
    res = run_iterative_baseline(mode="full_plan_v2", **kwargs)
    res["transition_planner_module"] = NAME
    return res
