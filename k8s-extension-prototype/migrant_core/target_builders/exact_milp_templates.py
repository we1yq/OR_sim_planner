from __future__ import annotations

import time
from typing import Any

from ..milp_extraction import (
    _arrival_dict_from_milp,
    _expand_demands_with_ids,
    _profile_need_from_instance_demands,
    extract_instance_demands_from_milp,
    extract_template_list_from_milp,
)
from ..physical_ids import ensure_state_metadata
from ..target_materialization import _score_tuple, _solve_target_with_greedy_repair
from ..templates import all_unique_physical_realizations


NAME = "target.exact_milp_templates"


def build(
    milp_res: dict[str, Any],
    prev_state: Any | None = None,
    feasible_option_df: Any | None = None,
    workload_names: list[str] | tuple[str, ...] | None = None,
    arrival_rate: list[float] | tuple[float, ...] | None = None,
    verbose: bool = False,
    **_: Any,
) -> Any:
    start = time.time()
    ordered_abstract = extract_template_list_from_milp(milp_res)
    instance_demands = extract_instance_demands_from_milp(milp_res, feasible_option_df)
    arrivals = _arrival_dict_from_milp(
        milp_res,
        workload_names=workload_names,
        arrival_rate=arrival_rate,
    )
    profile_need = _profile_need_from_instance_demands(instance_demands)
    demands = _expand_demands_with_ids(instance_demands)
    physical = [all_unique_physical_realizations(template)[0] for template in ordered_abstract]
    ordered_physical = [item[0] for item in physical]
    intervals_list = [item[1] for item in physical]

    target, metrics = _solve_target_with_greedy_repair(
        demands=demands,
        ordered_abstract_templates=ordered_abstract,
        ordered_physical_templates=ordered_physical,
        intervals_list=intervals_list,
        prev_state=None,
        native_profile_need=profile_need,
        layout_preserve_score=(0, 0, 0, 0),
        repair_rounds=0,
    )
    ensure_state_metadata(target)
    metrics["score_tuple"] = _score_tuple(metrics, prev_mode=False)
    metrics["elapsed_time_sec"] = time.time() - start
    metrics["preserve_disabled"] = True
    metrics["rewrite_disabled"] = True
    target.metadata["arrivals"] = dict(arrivals)
    target.metadata["build_metrics"] = dict(metrics)
    target.metadata["build_method"] = "exact_milp_templates"
    target.metadata["target_builder_module"] = NAME
    if verbose:
        print("[TARGET-STATE BUILDER] exact MILP templates")
        print(f"Chosen abstract templates: {ordered_abstract}")
        print(f"Chosen physical templates: {ordered_physical}")
    return target
