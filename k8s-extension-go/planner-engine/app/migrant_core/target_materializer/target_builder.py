from __future__ import annotations

from typing import Any

from .exact_milp_builder import build_target_state_exact_milp


def build_target_state_from_milp(
    milp_res: dict[str, Any],
    prev_state=None,
    feasible_option_df: Any | None = None,
    workload_names: list[str] | tuple[str, ...] | None = None,
    arrival_rate: list[float] | tuple[float, ...] | None = None,
    abstract_template_topk: int = 4,
    physical_layout_topk: int = 1,
    per_gpu_layout_topk: int = 4,
    placement_repair_rounds: int = 3,
    verbose: bool = False,
):
    """Build a physical target allocation with the exact global Stage 2 MILP."""

    return build_target_state_exact_milp(
        milp_res=milp_res,
        prev_state=prev_state,
        feasible_option_df=feasible_option_df,
        workload_names=workload_names,
        arrival_rate=arrival_rate,
        verbose=verbose,
    )


__all__ = [
    "build_target_state_from_milp",
    "build_target_state_exact_milp",
]
