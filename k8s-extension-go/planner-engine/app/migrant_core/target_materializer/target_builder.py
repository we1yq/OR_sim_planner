from __future__ import annotations

from typing import Any

from . import target_builder_legacy
from .current_guided_builder import build_target_state_current_guided_materialization


# Keep the old helpers import-compatible for code that still reaches into
# target_builder while making the public Stage 2 entry use the current method.
build_target_state_original = target_builder_legacy.build_target_state_from_milp
_apply_same_logical_template_order_fix = target_builder_legacy._apply_same_logical_template_order_fix
_score_tuple = target_builder_legacy._score_tuple
_solve_target_with_greedy_repair = target_builder_legacy._solve_target_with_greedy_repair
reassign_gpu_ids_by_matching = target_builder_legacy.reassign_gpu_ids_by_matching


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
    """Build a physical target allocation with the current Stage 2 method."""

    return build_target_state_current_guided_materialization(
        milp_res=milp_res,
        prev_state=prev_state,
        feasible_option_df=feasible_option_df,
        workload_names=workload_names,
        arrival_rate=arrival_rate,
        abstract_template_topk=abstract_template_topk,
        physical_layout_topk=physical_layout_topk,
        per_gpu_layout_topk=per_gpu_layout_topk,
        placement_repair_rounds=placement_repair_rounds,
        verbose=verbose,
    )


__all__ = [
    "build_target_state_from_milp",
    "build_target_state_current_guided_materialization",
    "build_target_state_original",
    "_apply_same_logical_template_order_fix",
    "_score_tuple",
    "_solve_target_with_greedy_repair",
    "reassign_gpu_ids_by_matching",
]
