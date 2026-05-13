from __future__ import annotations

import copy
import time
from collections import defaultdict
from typing import Any

from .milp_extraction import (
    _arrival_dict_from_milp,
    _expand_demands_with_ids,
    _profile_need_from_instance_demands,
    extract_instance_demands_from_milp,
    extract_template_list_from_milp,
)
from .physical_ids import ensure_state_metadata
from .preserve import (
    gpu_logical_template,
    reassign_gpu_ids_by_matching,
)
from .state import ClusterState, PROFILE_SIZE
from .target_candidates import (
    _augment_candidate_abstract_template_sets,
    _enumerate_candidate_abstract_template_sets,
    _enumerate_physical_layout_combinations,
    _order_candidate_templates_for_gpu_ids,
)
from .target_materialization import _score_tuple, _solve_target_with_greedy_repair


def _apply_same_logical_template_order_fix(
    target: ClusterState,
    prev_state: ClusterState | None,
) -> ClusterState:
    if prev_state is None:
        return target
    prev_by_id = {gpu.gpu_id: gpu for gpu in prev_state.real_gpus()}
    for gpu in target.real_gpus():
        old_gpu = prev_by_id.get(gpu.gpu_id)
        if old_gpu is None:
            continue
        if gpu_logical_template(old_gpu) != gpu_logical_template(gpu):
            continue

        old_order = [
            inst.profile
            for inst in sorted(old_gpu.instances, key=lambda x: (x.start, x.end))
            if inst.profile != "void"
        ]
        new_instances = [
            copy.deepcopy(inst)
            for inst in sorted(gpu.instances, key=lambda x: (x.start, x.end))
            if inst.profile != "void"
        ]

        by_profile = defaultdict(list)
        for inst in new_instances:
            by_profile[inst.profile].append(inst)

        rebuilt = []
        cur = 0
        ok = True
        for profile in old_order:
            if not by_profile[profile]:
                ok = False
                break
            inst = by_profile[profile].pop(0)
            length = PROFILE_SIZE[profile]
            inst.start = cur
            inst.end = cur + length
            rebuilt.append(inst)
            cur += length
        if ok and cur == 7:
            gpu.instances = rebuilt
            gpu.sort_instances()
    return target


def build_target_state_from_milp(
    milp_res: dict[str, Any],
    prev_state: ClusterState | None = None,
    feasible_option_df: Any | None = None,
    workload_names: list[str] | tuple[str, ...] | None = None,
    arrival_rate: list[float] | tuple[float, ...] | None = None,
    abstract_template_topk: int = 64,
    physical_layout_topk: int = 32,
    per_gpu_layout_topk: int = 4,
    verbose: bool = True,
) -> ClusterState:
    start = time.time()

    milp_template_ref = extract_template_list_from_milp(milp_res)
    instance_demands = extract_instance_demands_from_milp(milp_res, feasible_option_df)
    arrivals = _arrival_dict_from_milp(
        milp_res,
        workload_names=workload_names,
        arrival_rate=arrival_rate,
    )

    gpu_count = len(milp_template_ref)
    profile_need = _profile_need_from_instance_demands(instance_demands)
    demands = _expand_demands_with_ids(instance_demands)

    candidate_abstract_sets = _enumerate_candidate_abstract_template_sets(
        gpu_count=gpu_count,
        profile_need=profile_need,
        milp_template_ref=milp_template_ref,
        prev_state=prev_state,
        max_candidates=abstract_template_topk,
    )
    candidate_abstract_sets = _augment_candidate_abstract_template_sets(
        candidate_sets=candidate_abstract_sets,
        milp_template_ref=milp_template_ref,
        prev_state=prev_state,
        max_candidates=abstract_template_topk,
    )

    prev_mode = prev_state is not None and len(prev_state.real_gpus()) > 0
    best_target = None
    best_metrics = None
    best_score = None

    for candidate_abstract in candidate_abstract_sets:
        ordered_abstract = _order_candidate_templates_for_gpu_ids(
            candidate_templates=candidate_abstract,
            gpu_count=gpu_count,
            prev_state=prev_state,
            milp_template_ref=milp_template_ref,
        )

        physical_layout_combos = _enumerate_physical_layout_combinations(
            ordered_abstract_templates=ordered_abstract,
            prev_state=prev_state,
            milp_template_ref=milp_template_ref,
            max_combos=physical_layout_topk,
            per_gpu_topk=per_gpu_layout_topk,
        )

        for combo in physical_layout_combos:
            try:
                target, metrics = _solve_target_with_greedy_repair(
                    demands=demands,
                    ordered_abstract_templates=ordered_abstract,
                    ordered_physical_templates=combo["physical_template_strs"],
                    intervals_list=combo["intervals_list"],
                    prev_state=prev_state,
                    native_profile_need=profile_need,
                    layout_preserve_score=combo["layout_score"],
                    repair_rounds=8,
                )
            except RuntimeError:
                continue

            score = _score_tuple(metrics, prev_mode)
            if best_score is None or score > best_score:
                best_score = score
                best_target = target
                best_metrics = metrics
                best_target.metadata["arrivals"] = dict(arrivals)

    if best_target is None:
        raise RuntimeError("Target-state build failed: no feasible candidate found")

    best_target = reassign_gpu_ids_by_matching(best_target, prev_state)
    best_target = _apply_same_logical_template_order_fix(best_target, prev_state)
    ensure_state_metadata(best_target)

    elapsed = time.time() - start
    best_target.metadata["build_metrics"] = dict(best_metrics)
    best_target.metadata["build_metrics"]["score_tuple"] = best_score
    best_target.metadata["build_metrics"]["elapsed_time_sec"] = elapsed
    best_target.metadata["build_method"] = "greedy"

    if verbose:
        print("=" * 90)
        print("[TARGET-STATE BUILDER] BEST RESULT (greedy)")
        print("=" * 90)
        print(f"GPU count fixed from MILP    : {gpu_count}")
        print(f"Profile need                : {profile_need}")
        print(f"MILP abstract template ref  : {milp_template_ref}")
        print(f"Chosen abstract templates   : {best_metrics['ordered_abstract_templates']}")
        print(f"Chosen physical templates   : {best_metrics['ordered_physical_templates']}")
        print(f"Layout preserve score       : {best_metrics['layout_preserve_score']}")
        print(f"Build score                 : {best_score}")
        print(f"Build time (s)              : {elapsed:.4f}")
        print(f"exact_preserve              : {best_metrics['exact_preserve']}")
        print(f"upgrade_preserve            : {best_metrics['upgrade_preserve']}")
        print(f"same_gpu_preserve           : {best_metrics['same_gpu_preserve']}")
        print(f"spread                      : {best_metrics['spread']}")
        print(f"collocate_pairs             : {best_metrics['collocate_pairs']}")
        print(f"mixed_gpu_count             : {best_metrics['mixed_gpu_count']}")
        print(f"template_match_count        : {best_metrics['template_match_count']}")
        print("=" * 90)

    return best_target
