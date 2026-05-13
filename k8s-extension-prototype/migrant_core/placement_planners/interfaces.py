from __future__ import annotations

from typing import Any, Callable


PlacementResult = dict[str, Any]
PlacementSolver = Callable[..., PlacementResult]


def solve_placement(solver: PlacementSolver, **kwargs: Any) -> PlacementResult:
    return solver(**kwargs)
