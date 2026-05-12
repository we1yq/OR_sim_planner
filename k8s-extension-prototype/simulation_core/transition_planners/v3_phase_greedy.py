from __future__ import annotations

from typing import Any

from ..v3_transition import run_v3_stage_iterative


NAME = "transition.v3_phase_greedy"


def run(**kwargs: Any) -> dict[str, Any]:
    res = run_v3_stage_iterative(**kwargs)
    res["transition_planner_module"] = NAME
    return res
