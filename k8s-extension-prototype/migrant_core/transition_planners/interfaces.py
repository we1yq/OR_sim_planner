from __future__ import annotations

from typing import Any, Callable


TransitionPlanner = Callable[..., dict[str, Any]]


def run_transition(planner: TransitionPlanner, **kwargs: Any) -> dict[str, Any]:
    return planner(**kwargs)
