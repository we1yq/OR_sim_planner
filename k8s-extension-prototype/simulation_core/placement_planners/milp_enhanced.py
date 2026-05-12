from __future__ import annotations

from typing import Any

from ..milp_solver import solve_milp_gurobi_batch_unified


NAME = "placement.milp_enhanced"


def solve(
    feasible_option_df: Any,
    arrival_rate: list[float] | tuple[float, ...],
    n_workloads: int | None = None,
    time_limit_s: float | None = None,
    verbose: bool = False,
    **kwargs: Any,
) -> dict[str, Any]:
    res = solve_milp_gurobi_batch_unified(
        feasible_option_df=feasible_option_df,
        arrival_rate=arrival_rate,
        n_workloads=n_workloads,
        time_limit_s=time_limit_s,
        verbose=verbose,
        **kwargs,
    )
    res["planner_module"] = NAME
    return res
