from __future__ import annotations

from typing import Any

from ._iterative_baseline import run_iterative_baseline


NAME = "transition.serial_v0"


def run(**kwargs: Any) -> dict[str, Any]:
    res = run_iterative_baseline(mode="serial_v0", **kwargs)
    res["transition_planner_module"] = NAME
    return res
