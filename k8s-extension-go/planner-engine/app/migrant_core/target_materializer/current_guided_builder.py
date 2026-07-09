from __future__ import annotations

import copy
import math
import os
import time
from collections import Counter, defaultdict
from typing import Any


from . import target_builder_legacy as target_builder
from . import target_candidates
from .preserve import old_exact_slot_map
from .templates import (
    PROFILE_ORDER,
    PROFILE_SIZE,
    TEMPLATE_NAME_TO_K,
    template_name_list,
    template_to_parts,
)
from ..allocation_optimizer.milp_extraction import (
    _arrival_dict_from_milp,
    _expand_demands_with_ids,
    _profile_need_from_instance_demands,
    extract_instance_demands_from_milp,
    extract_template_list_from_milp,
)
from ..physical_ids import ensure_state_metadata
from ..state import ClusterState, GPUState, MigInstance, assert_valid_cluster_state


VARIANTS = [
    "current_existing",
    "seed_milp_augmented",
    "count_milp_pool",
    "preserve_first_selection",
    "transform_selection_ilp",
    "anchor_fill_fast",
    "anchor_local_materialization",
]


EMPTY_PROFILES = {"void", "unusable"}
ENABLE_ADDITIVE_VOID_TIEBREAKER = os.environ.get("STAGE2_ADDITIVE_VOID_TIEBREAKER", "0") != "0"


def is_real_profile(profile: str | None) -> bool:
    return profile not in EMPTY_PROFILES and profile is not None


def candidate_key(candidate: list[str]) -> tuple[str, ...]:
    return tuple(sorted(candidate))


def canonical_template_name(template: str) -> str:
    if template in TEMPLATE_NAME_TO_K:
        return template
    try:
        parts = [int(part) for part in str(template).split("+") if part]
    except Exception:
        return template
    parts.sort(reverse=True)
    candidate = "+".join(str(part) for part in parts)
    return candidate if candidate in TEMPLATE_NAME_TO_K else template


def dominates_need(candidate: list[str], profile_need: dict[str, int]) -> bool:
    cap = Counter()
    for template in candidate:
        for profile, count in TEMPLATE_NAME_TO_K[template].items():
            cap[profile] += count
    return all(cap[p] >= profile_need.get(p, 0) for p in PROFILE_ORDER)


def dedup_candidates(candidates: list[list[str]], max_candidates: int) -> list[list[str]]:
    out = []
    seen = set()
    for candidate in candidates:
        key = candidate_key(candidate)
        if key in seen:
            continue
        seen.add(key)
        out.append(list(candidate))
        if len(out) >= max_candidates:
            break
    return out


def seed_milp_augmented_candidates(
    gpu_count: int,
    profile_need: dict[str, int],
    milp_template_ref: list[str],
    prev_state,
    max_candidates: int = 64,
) -> list[list[str]]:
    candidates: list[list[str]] = []
    if len(milp_template_ref) == gpu_count and dominates_need(milp_template_ref, profile_need):
        candidates.append(list(milp_template_ref))

    prev_templates = target_candidates._prev_real_template_list(prev_state)
    if prev_templates:
        base = [template for template in prev_templates if template in TEMPLATE_NAME_TO_K][:gpu_count]
        fill_pool = list(milp_template_ref) + template_name_list()
        idx = 0
        while len(base) < gpu_count and idx < len(fill_pool) * 4:
            base.append(fill_pool[idx % len(fill_pool)])
            idx += 1
            if len(base) == gpu_count and not dominates_need(base, profile_need):
                base.pop()
        if len(base) == gpu_count and dominates_need(base, profile_need):
            candidates.append(base)

    if not candidates:
        return target_candidates._enumerate_candidate_abstract_template_sets(
            gpu_count=gpu_count,
            profile_need=profile_need,
            milp_template_ref=milp_template_ref,
            prev_state=prev_state,
            max_candidates=min(max_candidates, 1),
        )
    return dedup_candidates(candidates, max_candidates)


def count_milp_pool_candidates(
    gpu_count: int,
    profile_need: dict[str, int],
    milp_template_ref: list[str],
    prev_state,
    max_candidates: int = 64,
) -> list[list[str]]:
    try:
        import gurobipy as gp
        from gurobipy import GRB
    except Exception:
        return seed_milp_augmented_candidates(
            gpu_count=gpu_count,
            profile_need=profile_need,
            milp_template_ref=milp_template_ref,
            prev_state=prev_state,
            max_candidates=max_candidates,
        )

    templates = template_name_list()
    prev_counter = Counter(target_candidates._prev_real_template_list(prev_state))
    milp_counter = Counter(milp_template_ref)

    model = gp.Model("stage2_template_count_pool")
    model.Params.OutputFlag = 0
    model.Params.PoolSearchMode = 2
    model.Params.PoolSolutions = max(32, max_candidates * 4)
    model.Params.TimeLimit = 5

    count_vars = {
        template: model.addVar(vtype=GRB.INTEGER, lb=0, ub=gpu_count, name=f"c_{template}")
        for template in templates
    }
    prev_match_vars = {}
    milp_match_vars = {}
    for template in templates:
        prev_match_vars[template] = model.addVar(vtype=GRB.INTEGER, lb=0, ub=prev_counter.get(template, 0), name=f"p_{template}")
        milp_match_vars[template] = model.addVar(vtype=GRB.INTEGER, lb=0, ub=milp_counter.get(template, 0), name=f"m_{template}")
        model.addConstr(prev_match_vars[template] <= count_vars[template])
        model.addConstr(milp_match_vars[template] <= count_vars[template])

    model.addConstr(gp.quicksum(count_vars.values()) == gpu_count)
    capacity_exprs = {}
    for profile in PROFILE_ORDER:
        expr = gp.quicksum(TEMPLATE_NAME_TO_K[template][profile] * count_vars[template] for template in templates)
        capacity_exprs[profile] = expr
        model.addConstr(expr >= int(profile_need.get(profile, 0)))

    prev_score = gp.quicksum(prev_match_vars.values())
    milp_score = gp.quicksum(milp_match_vars.values())
    over = gp.quicksum(capacity_exprs[profile] - int(profile_need.get(profile, 0)) for profile in PROFILE_ORDER)
    upgrade_count = gp.quicksum(
        count_vars[template]
        for template in templates
        if template in target_candidates.UPGRADE_REWRITE_TEMPLATE_MAP.values()
    )
    part_penalty = gp.quicksum(len(template_to_parts(template)) * count_vars[template] for template in templates)
    stable_tie = gp.quicksum((len(templates) - idx) * count_vars[template] for idx, template in enumerate(templates))

    model.setObjective(
        1_000_000 * prev_score
        + 10_000 * milp_score
        + 100 * upgrade_count
        - 10 * over
        - part_penalty
        + 1e-3 * stable_tie,
        GRB.MAXIMIZE,
    )
    model.optimize()

    candidates = []
    for sol_idx in range(model.SolCount):
        model.Params.SolutionNumber = sol_idx
        candidate = []
        for template in templates:
            n = int(round(count_vars[template].Xn))
            candidate.extend([template] * n)
        if len(candidate) == gpu_count and dominates_need(candidate, profile_need):
            candidates.append(candidate)

    if len(milp_template_ref) == gpu_count and dominates_need(milp_template_ref, profile_need):
        candidates.insert(0, list(milp_template_ref))
    candidates = dedup_candidates(candidates, max_candidates)
    if not candidates:
        return seed_milp_augmented_candidates(
            gpu_count=gpu_count,
            profile_need=profile_need,
            milp_template_ref=milp_template_ref,
            prev_state=prev_state,
            max_candidates=max_candidates,
        )
    return candidates


def template_capacity(candidate: list[str]) -> Counter:
    cap = Counter()
    for template in candidate:
        for profile, count in TEMPLATE_NAME_TO_K[template].items():
            cap[profile] += count
    return cap


def remaining_need_after(candidate: list[str], profile_need: dict[str, int]) -> dict[str, int]:
    cap = template_capacity(candidate)
    return {
        profile: max(0, int(profile_need.get(profile, 0)) - int(cap.get(profile, 0)))
        for profile in PROFILE_ORDER
    }


def template_cover_score(template: str, need: dict[str, int], milp_counter: Counter) -> tuple[int, int, int, int, str]:
    cap = TEMPLATE_NAME_TO_K[template]
    useful = sum(min(cap[p], need.get(p, 0)) * PROFILE_SIZE[p] for p in PROFILE_ORDER)
    over = sum(max(0, cap[p] - need.get(p, 0)) for p in PROFILE_ORDER)
    return (
        useful,
        milp_counter.get(template, 0),
        -over,
        -len(template_to_parts(template)),
        template,
    )


def greedy_fill_templates(rem_gpus: int, need: dict[str, int], milp_template_ref: list[str]) -> list[str] | None:
    if rem_gpus < 0:
        return None
    if rem_gpus == 0:
        return [] if all(v <= 0 for v in need.values()) else None

    pool = []
    seen = set()
    for template in milp_template_ref + template_name_list():
        if template in TEMPLATE_NAME_TO_K and template not in seen:
            seen.add(template)
            pool.append(template)
    milp_counter = Counter(milp_template_ref)

    out = []
    cur_need = dict(need)
    for _ in range(rem_gpus):
        ranked = sorted(
            pool,
            key=lambda template: template_cover_score(template, cur_need, milp_counter),
            reverse=True,
        )
        pick = ranked[0]
        out.append(pick)
        for profile, count in TEMPLATE_NAME_TO_K[pick].items():
            cur_need[profile] = max(0, cur_need.get(profile, 0) - count)
    if any(v > 0 for v in cur_need.values()):
        return None
    return out


def template_active_slices(template: str) -> int:
    return sum(PROFILE_SIZE[profile] * count for profile, count in TEMPLATE_NAME_TO_K[template].items())


def template_has_unusable_fragment(template: str) -> bool:
    return template_active_slices(template) < 7


def count_level_residual_template_candidates(
    rem_gpus: int,
    profile_need: dict[str, int],
    milp_template_ref: list[str],
    prev_state,
    max_candidates: int = 4,
    avoid_unusable_fragments: bool = True,
) -> list[list[str]]:
    if rem_gpus < 0:
        return []
    if rem_gpus == 0:
        return [[]] if all(int(v) <= 0 for v in profile_need.values()) else []

    try:
        import gurobipy as gp
        from gurobipy import GRB
    except Exception:
        return []

    templates = [
        template
        for template in template_name_list()
        if template in TEMPLATE_NAME_TO_K
        and (not avoid_unusable_fragments or not template_has_unusable_fragment(template))
    ]
    if not templates:
        return []

    prev_counter = Counter(target_candidates._prev_real_template_list(prev_state))
    milp_counter = Counter(milp_template_ref)

    model = gp.Model("stage2_residual_template_counts")
    model.Params.OutputFlag = 0
    model.Params.PoolSearchMode = 2
    model.Params.PoolSolutions = max(16, max_candidates * 8)
    model.Params.TimeLimit = 1

    count_vars = {
        template: model.addVar(vtype=GRB.INTEGER, lb=0, ub=rem_gpus, name=f"c_{template}")
        for template in templates
    }
    prev_match_vars = {}
    milp_match_vars = {}
    for template in templates:
        prev_match_vars[template] = model.addVar(
            vtype=GRB.INTEGER,
            lb=0,
            ub=min(rem_gpus, int(prev_counter.get(template, 0))),
            name=f"p_{template}",
        )
        milp_match_vars[template] = model.addVar(
            vtype=GRB.INTEGER,
            lb=0,
            ub=min(rem_gpus, int(milp_counter.get(template, 0))),
            name=f"m_{template}",
        )
        model.addConstr(prev_match_vars[template] <= count_vars[template])
        model.addConstr(milp_match_vars[template] <= count_vars[template])

    model.addConstr(gp.quicksum(count_vars.values()) == rem_gpus)
    capacity_exprs = {}
    for profile in PROFILE_ORDER:
        expr = gp.quicksum(TEMPLATE_NAME_TO_K[template][profile] * count_vars[template] for template in templates)
        capacity_exprs[profile] = expr
        model.addConstr(expr >= int(profile_need.get(profile, 0)))

    over = gp.quicksum(
        capacity_exprs[profile] - int(profile_need.get(profile, 0))
        for profile in PROFILE_ORDER
    )
    part_count = gp.quicksum(len(template_to_parts(template)) * count_vars[template] for template in templates)
    stable_tie = gp.quicksum((len(templates) - idx) * count_vars[template] for idx, template in enumerate(templates))
    model.setObjective(
        1_000_000 * gp.quicksum(prev_match_vars.values())
        + 100_000 * gp.quicksum(milp_match_vars.values())
        - 1_000 * over
        - 10 * part_count
        + 1e-3 * stable_tie,
        GRB.MAXIMIZE,
    )
    model.optimize()

    if model.SolCount <= 0:
        return []

    candidates = []
    for sol_idx in range(model.SolCount):
        model.Params.SolutionNumber = sol_idx
        candidate = []
        for template in templates:
            n = int(round(count_vars[template].Xn))
            candidate.extend([template] * n)
        if len(candidate) == rem_gpus and dominates_need(candidate, profile_need):
            candidates.append(candidate)

    return dedup_candidates(candidates, max_candidates)


def score_prev_gpu_for_preserve(gpu, demand_counter: Counter, profile_need: dict[str, int]) -> tuple[int, int, int, int, int]:
    exact = 0
    upgrade = 0
    profile_useful = 0
    partition_count = 0
    for inst in getattr(gpu, "instances", []):
        if not is_real_profile(inst.profile) or inst.workload is None:
            continue
        partition_count += 1
        if demand_counter.get((inst.workload, inst.profile), 0) > 0:
            exact += 1
        if inst.profile == "4g" and demand_counter.get((inst.workload, "3g"), 0) > 0:
            upgrade += 1
        profile_useful += min(1, int(profile_need.get(inst.profile, 0))) * PROFILE_SIZE[inst.profile]
    return exact, upgrade, profile_useful, partition_count, -int(gpu.gpu_id)


def selected_preserve_score(gpus: list[Any], demand_counter: Counter) -> tuple[int, int]:
    remaining = Counter(demand_counter)
    exact = 0
    upgrade = 0
    for gpu in gpus:
        for inst in getattr(gpu, "instances", []):
            if not is_real_profile(inst.profile) or inst.workload is None:
                continue
            key = (inst.workload, inst.profile)
            if remaining.get(key, 0) > 0:
                remaining[key] -= 1
                exact += 1
                continue
            if inst.profile == "4g":
                upgrade_key = (inst.workload, "3g")
                if remaining.get(upgrade_key, 0) > 0:
                    remaining[upgrade_key] -= 1
                    upgrade += 1
    return exact, upgrade


def preserve_first_candidate_sets(
    gpu_count: int,
    profile_need: dict[str, int],
    milp_template_ref: list[str],
    prev_state,
    demands: list[dict[str, Any]],
    max_candidates: int = 64,
) -> list[list[str]]:
    prev_gpus = list(prev_state.real_gpus()) if prev_state is not None else []
    demand_counter = Counter((d["workload"], d["profile"]) for d in demands)
    scored = [
        (score_prev_gpu_for_preserve(gpu, demand_counter, profile_need), gpu)
        for gpu in prev_gpus
    ]
    scored.sort(key=lambda item: item[0], reverse=True)
    ordered_gpus = [gpu for _, gpu in scored]

    candidate_records = []
    max_keep = min(gpu_count, len(ordered_gpus))
    for keep_count in range(max_keep, -1, -1):
        kept_gpus = ordered_gpus[:keep_count]
        kept_templates = [gpu.template_str() for gpu in kept_gpus if gpu.template_str() in TEMPLATE_NAME_TO_K]
        if len(kept_templates) != keep_count:
            continue
        rem_gpus = gpu_count - keep_count
        rem_need = remaining_need_after(kept_templates, profile_need)
        filler = greedy_fill_templates(rem_gpus, rem_need, milp_template_ref)
        if filler is None:
            try:
                filler_candidates = count_milp_pool_candidates(
                    gpu_count=rem_gpus,
                    profile_need=rem_need,
                    milp_template_ref=milp_template_ref,
                    prev_state=None,
                    max_candidates=1,
                )
                filler = filler_candidates[0]
            except Exception:
                continue
        candidate = kept_templates + filler
        if len(candidate) != gpu_count or not dominates_need(candidate, profile_need):
            continue
        exact, upgrade = selected_preserve_score(kept_gpus, demand_counter)
        cap = template_capacity(candidate)
        over = sum(max(0, cap[p] - profile_need.get(p, 0)) for p in PROFILE_ORDER)
        milp_counter = Counter(milp_template_ref)
        milp_match = sum(min(Counter(candidate)[t], milp_counter[t]) for t in set(candidate))
        candidate_records.append(((exact, upgrade, keep_count, milp_match, -over, -sum(len(template_to_parts(t)) for t in candidate)), candidate))

    # Always include the count-level solver and MILP ref as safety candidates.
    for fallback in count_milp_pool_candidates(
        gpu_count=gpu_count,
        profile_need=profile_need,
        milp_template_ref=milp_template_ref,
        prev_state=prev_state,
        max_candidates=min(4, max_candidates),
    ):
        if len(fallback) == gpu_count and dominates_need(fallback, profile_need):
            candidate_records.append(((0, 0, 0, 0, 0, 0), fallback))
    if len(milp_template_ref) == gpu_count and dominates_need(milp_template_ref, profile_need):
        candidate_records.append(((0, 0, 0, 0, 0, 0), list(milp_template_ref)))

    candidate_records.sort(key=lambda item: item[0], reverse=True)
    return dedup_candidates([candidate for _, candidate in candidate_records], max_candidates)


def reverse_upgrade_map() -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for base, upgraded in target_candidates.UPGRADE_REWRITE_TEMPLATE_MAP.items():
        out.setdefault(upgraded, []).append(base)
    return out


def profile_counter(template: str) -> Counter:
    if template in TEMPLATE_NAME_TO_K:
        return Counter(TEMPLATE_NAME_TO_K[template])
    out = Counter()
    try:
        for part in str(template).split("+"):
            if not part:
                continue
            out[f"{int(part)}g"] += 1
    except Exception:
        pass
    return out


def logical_template_overlap(left: str, right: str) -> int:
    a = profile_counter(left)
    b = profile_counter(right)
    return sum(min(a[p], b[p]) * PROFILE_SIZE[p] for p in PROFILE_ORDER)


def nearby_templates_for_current_template(current_template: str, profile_need: dict[str, int], limit: int = 6) -> list[str]:
    current_template = canonical_template_name(current_template)
    candidates = set()
    if current_template in TEMPLATE_NAME_TO_K:
        candidates.add(current_template)
    if current_template in target_candidates.UPGRADE_REWRITE_TEMPLATE_MAP:
        candidates.add(target_candidates.UPGRADE_REWRITE_TEMPLATE_MAP[current_template])
    for base in reverse_upgrade_map().get(current_template, []):
        candidates.add(base)

    all_templates = template_name_list()
    ranked = sorted(
        all_templates,
        key=lambda template: (
            logical_template_overlap(current_template, template),
            template_cover_score(template, profile_need, Counter())[0],
            -sum(abs(TEMPLATE_NAME_TO_K[current_template].get(p, 0) - TEMPLATE_NAME_TO_K[template].get(p, 0)) for p in PROFILE_ORDER)
            if current_template in TEMPLATE_NAME_TO_K
            else 0,
            -len(template_to_parts(template)),
            template,
        ),
        reverse=True,
    )
    for template in ranked[:limit]:
        candidates.add(template)
    return list(sorted(candidates))


def transform_score(current_template: str, target_template: str, gpu, demand_counter: Counter, profile_need: dict[str, int]) -> int:
    current_template = canonical_template_name(current_template)
    exact_template = int(current_template == target_template)
    upgrade_relation = int(target_candidates.UPGRADE_REWRITE_TEMPLATE_MAP.get(current_template) == target_template)
    reverse_upgrade = int(target_candidates.UPGRADE_REWRITE_TEMPLATE_MAP.get(target_template) == current_template)
    overlap = logical_template_overlap(current_template, target_template) if current_template in TEMPLATE_NAME_TO_K else 0
    useful = template_cover_score(target_template, profile_need, Counter())[0]
    workload_exact = 0
    workload_upgrade = 0
    target_profile_budget = Counter(TEMPLATE_NAME_TO_K[target_template])
    for inst in getattr(gpu, "instances", []):
        if not is_real_profile(inst.profile) or inst.workload is None:
            continue
        if target_profile_budget.get(inst.profile, 0) > 0 and demand_counter.get((inst.workload, inst.profile), 0) > 0:
            workload_exact += 1
            target_profile_budget[inst.profile] -= 1
        elif inst.profile == "4g" and target_profile_budget.get("4g", 0) > 0 and demand_counter.get((inst.workload, "3g"), 0) > 0:
            workload_upgrade += 1
            target_profile_budget["4g"] -= 1
    over = sum(max(0, TEMPLATE_NAME_TO_K[target_template][p] - profile_need.get(p, 0)) for p in PROFILE_ORDER)
    return (
        1_000_000 * workload_exact
        + 100_000 * workload_upgrade
        + 20_000 * exact_template
        + 8_000 * upgrade_relation
        + 4_000 * reverse_upgrade
        + 500 * overlap
        + 20 * useful
        - 5 * over
    )


def transform_selection_ilp_candidates(
    gpu_count: int,
    profile_need: dict[str, int],
    milp_template_ref: list[str],
    prev_state,
    demands: list[dict[str, Any]],
    max_candidates: int = 64,
) -> list[list[str]]:
    try:
        import gurobipy as gp
        from gurobipy import GRB
    except Exception:
        return preserve_first_candidate_sets(
            gpu_count=gpu_count,
            profile_need=profile_need,
            milp_template_ref=milp_template_ref,
            prev_state=prev_state,
            demands=demands,
            max_candidates=max_candidates,
        )

    prev_gpus = list(prev_state.real_gpus()) if prev_state is not None else []
    demand_counter = Counter((d["workload"], d["profile"]) for d in demands)
    templates = template_name_list()
    milp_counter = Counter(milp_template_ref)

    model = gp.Model("stage2_transform_selection")
    model.Params.OutputFlag = 0
    model.Params.PoolSearchMode = 2
    model.Params.PoolSolutions = max(32, max_candidates * 4)
    model.Params.TimeLimit = 8

    x = {}
    for idx, gpu in enumerate(prev_gpus):
        current_template = canonical_template_name(gpu.template_str())
        for target_template in nearby_templates_for_current_template(current_template, profile_need, limit=5):
            x[(idx, target_template)] = model.addVar(vtype=GRB.BINARY, name=f"x_{idx}_{target_template}")
        if any((idx, t) in x for t in templates):
            model.addConstr(gp.quicksum(var for (gpu_idx, _), var in x.items() if gpu_idx == idx) <= 1)

    y = {
        template: model.addVar(vtype=GRB.INTEGER, lb=0, ub=gpu_count, name=f"new_{template}")
        for template in templates
    }

    model.addConstr(gp.quicksum(x.values()) + gp.quicksum(y.values()) == gpu_count)

    for profile in PROFILE_ORDER:
        model.addConstr(
            gp.quicksum(TEMPLATE_NAME_TO_K[target_template][profile] * var for (_, target_template), var in x.items())
            + gp.quicksum(TEMPLATE_NAME_TO_K[template][profile] * y[template] for template in templates)
            >= int(profile_need.get(profile, 0))
        )

    transform_obj = gp.quicksum(
        transform_score(canonical_template_name(prev_gpus[idx].template_str()), target_template, prev_gpus[idx], demand_counter, profile_need) * var
        for (idx, target_template), var in x.items()
    )
    new_obj = gp.quicksum(
        (
            100 * milp_counter.get(template, 0)
            + 10 * template_cover_score(template, profile_need, milp_counter)[0]
            - len(template_to_parts(template))
        )
        * y[template]
        for template in templates
    )
    capacity_over = gp.quicksum(
        (
            gp.quicksum(TEMPLATE_NAME_TO_K[target_template][profile] * var for (_, target_template), var in x.items())
            + gp.quicksum(TEMPLATE_NAME_TO_K[template][profile] * y[template] for template in templates)
            - int(profile_need.get(profile, 0))
        )
        for profile in PROFILE_ORDER
    )
    model.setObjective(transform_obj + new_obj - capacity_over, GRB.MAXIMIZE)
    model.optimize()

    candidates = []
    for sol_idx in range(model.SolCount):
        model.Params.SolutionNumber = sol_idx
        candidate = []
        for (_, target_template), var in x.items():
            if var.Xn > 0.5:
                candidate.append(target_template)
        for template, var in y.items():
            n = int(round(var.Xn))
            candidate.extend([template] * n)
        if len(candidate) == gpu_count and dominates_need(candidate, profile_need):
            candidates.append(candidate)

    # Add robust fallbacks so the downstream materializer can still recover if
    # the transform-local neighborhood misses a useful global template.
    candidates.extend(
        count_milp_pool_candidates(
            gpu_count=gpu_count,
            profile_need=profile_need,
            milp_template_ref=milp_template_ref,
            prev_state=prev_state,
            max_candidates=min(4, max_candidates),
        )
    )
    return dedup_candidates(candidates, max_candidates)


def nonvoid_workload_instances(gpu) -> list[Any]:
    return [
        inst
        for inst in sorted(getattr(gpu, "instances", []), key=lambda x: (x.start, x.end, x.profile))
        if is_real_profile(inst.profile) and inst.workload is not None
    ]


def anchor_gpu_counts(gpu) -> Counter:
    return Counter((inst.workload, inst.profile) for inst in nonvoid_workload_instances(gpu))


def anchor_gpu_profile_counts(gpu) -> Counter:
    return Counter(inst.profile for inst in nonvoid_workload_instances(gpu))


def select_full_gpu_anchor_sets(
    prev_state,
    demands: list[dict[str, Any]],
    gpu_count: int,
    max_sets: int = 4,
    force_include_empty: bool = True,
) -> list[list[Any]]:
    if prev_state is None:
        return [[]]

    demand_counter = Counter((d["workload"], d["profile"]) for d in demands)
    candidates = []
    for gpu in prev_state.real_gpus():
        template = canonical_template_name(gpu.template_str())
        if template not in TEMPLATE_NAME_TO_K:
            continue
        insts = nonvoid_workload_instances(gpu)
        if not insts:
            continue
        counts = anchor_gpu_counts(gpu)
        if any(counts[key] > demand_counter.get(key, 0) for key in counts):
            continue
        score = (
            len(insts),
            sum(PROFILE_SIZE[inst.profile] for inst in insts),
            len(template_to_parts(template)),
            -int(gpu.gpu_id),
        )
        candidates.append((score, gpu))

    if not candidates:
        return [[]]

    try:
        import gurobipy as gp
        from gurobipy import GRB

        model = gp.Model("stage2_full_gpu_anchor_select")
        model.Params.OutputFlag = 0
        model.Params.PoolSearchMode = 2
        model.Params.PoolSolutions = max(8, max_sets * 4)
        model.Params.TimeLimit = 1

        x = {
            idx: model.addVar(vtype=GRB.BINARY, name=f"a_{idx}")
            for idx in range(len(candidates))
        }
        model.addConstr(gp.quicksum(x.values()) <= gpu_count)

        for key, limit in demand_counter.items():
            model.addConstr(
                gp.quicksum(anchor_gpu_counts(gpu).get(key, 0) * x[idx] for idx, (_, gpu) in enumerate(candidates))
                <= int(limit)
            )

        model.setObjective(
            gp.quicksum(
                (
                    1_000_000 * len(nonvoid_workload_instances(gpu))
                    + 10_000 * sum(PROFILE_SIZE[inst.profile] for inst in nonvoid_workload_instances(gpu))
                    - int(gpu.gpu_id)
                )
                * x[idx]
                for idx, (_, gpu) in enumerate(candidates)
            ),
            GRB.MAXIMIZE,
        )
        model.optimize()

        out = []
        seen = set()
        for sol_idx in range(model.SolCount):
            model.Params.SolutionNumber = sol_idx
            selected = [gpu for idx, (_, gpu) in enumerate(candidates) if x[idx].Xn > 0.5]
            key = tuple(sorted(int(gpu.gpu_id) for gpu in selected))
            if key in seen:
                continue
            seen.add(key)
            out.append(selected)
            if len(out) >= max_sets:
                break
        if out:
            if force_include_empty:
                empty_key = tuple()
                if empty_key not in seen:
                    out.append([])
                if len(out) > max_sets:
                    out = out[: max_sets - 1] + [[]]
                return out
            out.append([])
            return out[:max_sets]
    except Exception:
        pass

    ordered = [gpu for _, gpu in sorted(candidates, key=lambda item: item[0], reverse=True)]
    out = []
    remaining = Counter(demand_counter)
    selected = []
    for gpu in ordered:
        counts = anchor_gpu_counts(gpu)
        if len(selected) >= gpu_count:
            break
        if all(counts[key] <= remaining.get(key, 0) for key in counts):
            selected.append(gpu)
            for key, count in counts.items():
                remaining[key] -= count
    out.append(selected)
    out.append([])
    if force_include_empty and len(out) > max_sets:
        out = out[: max_sets - 1] + [[]]
    return out if force_include_empty else out[:max_sets]


def consume_anchor_demands(
    selected_anchors: list[Any],
    demands: list[dict[str, Any]],
) -> tuple[list[GPUState], list[dict[str, Any]]]:
    demand_buckets = defaultdict(list)
    for demand in demands:
        demand_buckets[(demand["workload"], demand["profile"])].append(demand)
    for key in demand_buckets:
        demand_buckets[key].sort(key=lambda d: (int(d["batch"]), int(d["demand_id"])))

    anchored_gpus = []
    consumed_ids = set()
    for gpu in selected_anchors:
        new_instances = []
        for inst in sorted(gpu.instances, key=lambda x: (x.start, x.end)):
            if not is_real_profile(inst.profile) or inst.workload is None:
                new_instances.append(copy.deepcopy(inst))
                continue
            bucket = demand_buckets[(inst.workload, inst.profile)]
            if not bucket:
                raise RuntimeError("Selected full-GPU anchor exceeds residual demand")
            demand = bucket.pop(0)
            consumed_ids.add(demand["demand_id"])
            new_instances.append(
                MigInstance(
                    start=inst.start,
                    end=inst.end,
                    profile=inst.profile,
                    workload=demand["workload"],
                    batch=int(demand["batch"]),
                    mu=float(demand["mu"]),
                    preserved=True,
                )
            )
        anchored_gpus.append(GPUState(gpu_id=int(gpu.gpu_id), source="real", instances=new_instances))

    residual = [demand for demand in demands if demand["demand_id"] not in consumed_ids]
    return anchored_gpus, residual


def profile_need_from_expanded_demands(demands: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(demand["profile"] for demand in demands)
    return {profile: int(counts.get(profile, 0)) for profile in PROFILE_ORDER}


def make_filler_prev_state(
    prev_state,
    selected_anchors: list[Any],
    rem_gpus: int,
    residual_demands: list[dict[str, Any]] | None = None,
    residual_need: dict[str, int] | None = None,
) -> tuple[ClusterState, dict[int, int]]:
    selected_ids = {int(gpu.gpu_id) for gpu in selected_anchors}
    remaining_gpus = [
        gpu for gpu in sorted(prev_state.real_gpus(), key=lambda g: int(g.gpu_id))
        if int(gpu.gpu_id) not in selected_ids
    ] if prev_state is not None else []
    if residual_demands is not None:
        residual_counter = Counter((d["workload"], d["profile"]) for d in residual_demands)
        residual_need = residual_need or profile_need_from_expanded_demands(residual_demands)
        remaining_gpus.sort(
            key=lambda gpu: score_prev_gpu_for_preserve(gpu, residual_counter, residual_need),
            reverse=True,
        )
    chosen = remaining_gpus[:rem_gpus]
    temp_to_old = {}
    filler_gpus = []
    for temp_id, old_gpu in enumerate(chosen):
        new_gpu = copy.deepcopy(old_gpu)
        temp_to_old[temp_id] = int(old_gpu.gpu_id)
        new_gpu.gpu_id = temp_id
        filler_gpus.append(new_gpu)
    return ClusterState(gpus=filler_gpus, metadata={}), temp_to_old


def make_residual_reuse_prev_state(
    prev_state,
    selected_anchors: list[Any],
    rem_gpus: int,
    residual_demands: list[dict[str, Any]],
    residual_need: dict[str, int],
) -> tuple[ClusterState, dict[int, int]]:
    selected_ids = {int(gpu.gpu_id) for gpu in selected_anchors}
    remaining_gpus = [
        gpu for gpu in sorted(prev_state.real_gpus(), key=lambda g: int(g.gpu_id))
        if int(gpu.gpu_id) not in selected_ids
    ] if prev_state is not None else []
    if rem_gpus <= 0 or not remaining_gpus:
        return ClusterState(gpus=[], metadata={}), {}

    choose_count = min(rem_gpus, len(remaining_gpus))
    demand_counter = Counter((d["workload"], d["profile"]) for d in residual_demands)

    try:
        import gurobipy as gp
        from gurobipy import GRB

        model = gp.Model("stage2_residual_reuse_gpu_select")
        model.Params.OutputFlag = 0
        model.Params.TimeLimit = 1

        x = {
            idx: model.addVar(vtype=GRB.BINARY, name=f"x_{idx}")
            for idx in range(len(remaining_gpus))
        }
        model.addConstr(gp.quicksum(x.values()) == choose_count)

        exact_vars = {}
        for key, limit in demand_counter.items():
            exact_vars[key] = model.addVar(vtype=GRB.INTEGER, lb=0, ub=int(limit), name=f"exact_{key[0]}_{key[1]}")
            model.addConstr(
                exact_vars[key]
                <= gp.quicksum(
                    anchor_gpu_counts(gpu).get(key, 0) * x[idx]
                    for idx, gpu in enumerate(remaining_gpus)
                )
            )

        profile_vars = {}
        for profile in PROFILE_ORDER:
            limit = int(residual_need.get(profile, 0))
            profile_vars[profile] = model.addVar(vtype=GRB.INTEGER, lb=0, ub=limit, name=f"profile_{profile}")
            model.addConstr(
                profile_vars[profile]
                <= gp.quicksum(
                    anchor_gpu_profile_counts(gpu).get(profile, 0) * x[idx]
                    for idx, gpu in enumerate(remaining_gpus)
                )
            )

        upgrade_vars = {}
        upgrade_workloads = sorted({workload for workload, profile in demand_counter if profile == "3g"})
        for workload in upgrade_workloads:
            limit = int(demand_counter.get((workload, "3g"), 0))
            upgrade_vars[workload] = model.addVar(vtype=GRB.INTEGER, lb=0, ub=limit, name=f"upgrade_{workload}")
            model.addConstr(
                upgrade_vars[workload]
                <= gp.quicksum(
                    sum(
                        1
                        for inst in nonvoid_workload_instances(gpu)
                        if inst.profile == "4g" and inst.workload == workload
                    )
                    * x[idx]
                    for idx, gpu in enumerate(remaining_gpus)
                )
            )

        useful_slice_score = gp.quicksum(
            sum(
                min(1, int(residual_need.get(inst.profile, 0))) * PROFILE_SIZE[inst.profile]
                for inst in nonvoid_workload_instances(gpu)
            )
            * x[idx]
            for idx, gpu in enumerate(remaining_gpus)
        )
        stable_tie = gp.quicksum(-int(gpu.gpu_id) * x[idx] for idx, gpu in enumerate(remaining_gpus))
        model.setObjective(
            100_000 * gp.quicksum(exact_vars.values())
            + 10_000 * gp.quicksum(profile_vars.values())
            + 1_000 * gp.quicksum(upgrade_vars.values())
            + 10 * useful_slice_score
            + 1e-3 * stable_tie,
            GRB.MAXIMIZE,
        )
        model.optimize()
        selected = [
            gpu for idx, gpu in enumerate(remaining_gpus)
            if x[idx].X > 0.5
        ]
    except Exception:
        selected = sorted(
            remaining_gpus,
            key=lambda gpu: score_prev_gpu_for_preserve(gpu, demand_counter, residual_need),
            reverse=True,
        )[:choose_count]

    selected.sort(
        key=lambda gpu: score_prev_gpu_for_preserve(gpu, demand_counter, residual_need),
        reverse=True,
    )
    temp_to_old = {}
    filler_gpus = []
    for temp_id, old_gpu in enumerate(selected):
        new_gpu = copy.deepcopy(old_gpu)
        temp_to_old[temp_id] = int(old_gpu.gpu_id)
        new_gpu.gpu_id = temp_id
        filler_gpus.append(new_gpu)
    return ClusterState(gpus=filler_gpus, metadata={}), temp_to_old


def _candidate_template_score_for_gpu(
    gpu,
    template: str,
    residual_counter: Counter,
    residual_need: dict[str, int],
) -> tuple[int, int, int, int, int, int, str]:
    gpu_counts = anchor_gpu_counts(gpu)
    gpu_profile_counts = anchor_gpu_profile_counts(gpu)
    exact_wp = sum(min(gpu_counts.get(key, 0), residual_counter.get(key, 0)) for key in residual_counter)
    profile_fit = sum(
        min(gpu_profile_counts.get(profile, 0), TEMPLATE_NAME_TO_K[template].get(profile, 0), residual_need.get(profile, 0))
        * PROFILE_SIZE[profile]
        for profile in PROFILE_ORDER
    )
    template_overlap = logical_template_overlap(gpu.template_str(), template)
    additive_void = additive_void_opportunity_score(gpu, template, residual_need)
    cover = template_cover_score(template, residual_need, Counter())[0]
    over = sum(max(0, TEMPLATE_NAME_TO_K[template][profile] - residual_need.get(profile, 0)) for profile in PROFILE_ORDER)
    return (
        exact_wp,
        profile_fit,
        template_overlap,
        additive_void,
        cover,
        -over,
        template,
    )


def additive_void_opportunity_score(gpu, template: str, residual_need: dict[str, int]) -> int:
    if not ENABLE_ADDITIVE_VOID_TIEBREAKER:
        return 0
    current_slots = {
        (int(inst.start), int(inst.end), str(inst.profile))
        for inst in nonvoid_workload_instances(gpu)
    }
    best = 0
    for _, intervals, _ in target_candidates.physical_layout_candidates_for_gpu(
        abstract_template=template,
        gpu_id=int(gpu.gpu_id),
        prev_state=ClusterState(gpus=[copy.deepcopy(gpu)], metadata={}),
        topk=8,
    ):
        score = 0
        for start, end, profile in intervals:
            profile = str(profile)
            if not is_real_profile(profile):
                continue
            if (int(start), int(end), profile) in current_slots:
                continue
            if int(residual_need.get(profile, 0)) <= 0:
                continue
            if _slot_covered_by_void(gpu, int(start), int(end)):
                score += PROFILE_SIZE[profile]
        best = max(best, score)
    return best


def _slot_covered_by_void(gpu, start: int, end: int) -> bool:
    for slice_idx in range(start, end):
        if not any(
            str(inst.profile) == "void" and int(inst.start) <= slice_idx < int(inst.end)
            for inst in gpu.instances
        ):
            return False
    return True


def reuse_guided_fill_templates(
    rem_gpus: int,
    need: dict[str, int],
    residual_demands: list[dict[str, Any]],
    milp_template_ref: list[str],
    filler_prev_state: ClusterState,
) -> list[str] | None:
    if rem_gpus <= 0:
        return [] if all(v <= 0 for v in need.values()) else None

    residual_counter = Counter((d["workload"], d["profile"]) for d in residual_demands)

    pool = []
    seen = set()
    for template in milp_template_ref + template_name_list():
        if template in TEMPLATE_NAME_TO_K and template not in seen:
            seen.add(template)
            pool.append(template)

    chosen = []
    cur_need = dict(need)
    for gpu in filler_prev_state.real_gpus()[:rem_gpus]:
        ranked = sorted(
            pool,
            key=lambda template: _candidate_template_score_for_gpu(
                gpu,
                template,
                residual_counter,
                cur_need,
            ),
            reverse=True,
        )
        pick = ranked[0]
        chosen.append(pick)
        for profile, count in TEMPLATE_NAME_TO_K[pick].items():
            cur_need[profile] = max(0, cur_need.get(profile, 0) - count)

    tail = greedy_fill_templates(rem_gpus - len(chosen), cur_need, milp_template_ref)
    if tail is None:
        return None
    candidate = chosen + tail
    return candidate if len(candidate) == rem_gpus and dominates_need(candidate, need) else None


def _template_assignment_score(
    gpu,
    template: str,
    residual_counter: Counter,
    residual_need: dict[str, int],
) -> int:
    current_intervals = {
        (int(inst.start), int(inst.end), inst.profile): inst
        for inst in nonvoid_workload_instances(gpu)
    }
    best = 0
    for _, intervals, layout_score in target_candidates.physical_layout_candidates_for_gpu(
        abstract_template=template,
        gpu_id=int(gpu.gpu_id),
        prev_state=ClusterState(gpus=[copy.deepcopy(gpu)], metadata={}),
        topk=8,
    ):
        exact = 0
        partition = 0
        exact_slices = 0
        partition_slices = 0
        for start, end, profile in intervals:
            if not is_real_profile(profile):
                continue
            old = current_intervals.get((int(start), int(end), profile))
            if old is None:
                continue
            partition += 1
            partition_slices += PROFILE_SIZE[profile]
            if residual_counter.get((old.workload, old.profile), 0) > 0:
                exact += 1
                exact_slices += PROFILE_SIZE[profile]
        cover = template_cover_score(template, residual_need, Counter())[0]
        score = (
            1_000_000 * exact
            + 100_000 * exact_slices
            + 10_000 * partition
            + 1_000 * partition_slices
            + 500 * additive_void_opportunity_score(gpu, template, residual_need)
            + 100 * layout_score[3]
            + cover
        )
        best = max(best, score)
    return best


def order_templates_by_residual_reuse_matching(
    candidate_templates: list[str],
    filler_prev_state: ClusterState,
    residual_demands: list[dict[str, Any]],
    residual_need: dict[str, int],
) -> list[str]:
    templates = list(candidate_templates)
    gpus = list(sorted(filler_prev_state.real_gpus(), key=lambda gpu: int(gpu.gpu_id)))
    if not gpus:
        return list(templates)

    residual_counter = Counter((d["workload"], d["profile"]) for d in residual_demands)
    current_count = min(len(gpus), len(templates))

    try:
        import gurobipy as gp
        from gurobipy import GRB

        model = gp.Model("stage2_template_gpu_assignment")
        model.Params.OutputFlag = 0
        model.Params.TimeLimit = 1
        x = {}
        for gi in range(current_count):
            for tj, template in enumerate(templates):
                x[(gi, tj)] = model.addVar(vtype=GRB.BINARY, name=f"x_{gi}_{tj}")
        for gi in range(current_count):
            model.addConstr(gp.quicksum(x[(gi, tj)] for tj in range(len(templates))) == 1)
        for tj in range(len(templates)):
            model.addConstr(gp.quicksum(x[(gi, tj)] for gi in range(current_count)) <= 1)
        model.setObjective(
            gp.quicksum(
                _template_assignment_score(gpus[gi], templates[tj], residual_counter, residual_need) * x[(gi, tj)]
                for gi in range(current_count)
                for tj in range(len(templates))
            ),
            GRB.MAXIMIZE,
        )
        model.optimize()
        assigned = {}
        used_templates = set()
        for gi in range(current_count):
            for tj in range(len(templates)):
                if x[(gi, tj)].X > 0.5:
                    assigned[gi] = templates[tj]
                    used_templates.add(tj)
                    break
    except Exception:
        pairs = []
        for gi in range(current_count):
            for tj, template in enumerate(templates):
                pairs.append((_template_assignment_score(gpus[gi], template, residual_counter, residual_need), gi, tj))
        pairs.sort(reverse=True)
        assigned = {}
        used_templates = set()
        used_gpus = set()
        for _, gi, tj in pairs:
            if gi in used_gpus or tj in used_templates:
                continue
            assigned[gi] = templates[tj]
            used_gpus.add(gi)
            used_templates.add(tj)
            if len(assigned) >= current_count:
                break

    remaining_templates = [template for idx, template in enumerate(templates) if idx not in used_templates]
    remaining_templates.sort(
        key=lambda template: template_cover_score(template, residual_need, Counter()),
        reverse=True,
    )
    ordered = []
    for idx in range(len(templates)):
        if idx < current_count and idx in assigned:
            ordered.append(assigned[idx])
        else:
            ordered.append(remaining_templates.pop(0))
    return ordered


def next_unused_gpu_id(used: set[int]) -> int:
    cur = 0
    while cur in used:
        cur += 1
    used.add(cur)
    return cur


def combine_anchor_and_filler_state(
    anchored_gpus: list[GPUState],
    filler_state: ClusterState,
    temp_to_old: dict[int, int],
) -> ClusterState:
    used = {int(gpu.gpu_id) for gpu in anchored_gpus}
    out_gpus = [copy.deepcopy(gpu) for gpu in anchored_gpus]
    for gpu in sorted(filler_state.real_gpus(), key=lambda g: int(g.gpu_id)):
        new_gpu = copy.deepcopy(gpu)
        old_id = temp_to_old.get(int(gpu.gpu_id))
        if old_id is None or old_id in used:
            old_id = next_unused_gpu_id(used)
        else:
            used.add(old_id)
        new_gpu.gpu_id = old_id
        out_gpus.append(new_gpu)
    target = ClusterState(gpus=sorted(out_gpus, key=lambda g: int(g.gpu_id)), metadata={})
    assert_valid_cluster_state(target)
    return target


def refine_same_profile_void_placement(target: ClusterState, prev_state: ClusterState | None) -> ClusterState:
    if prev_state is None:
        return target
    old_map = old_exact_slot_map(prev_state)
    refined_gpus = []
    for gpu in target.real_gpus():
        instances = [copy.deepcopy(inst) for inst in sorted(gpu.instances, key=lambda x: (x.start, x.end, x.profile))]
        for profile in ["4g", "3g", "2g", "1g"]:
            size = PROFILE_SIZE[profile]
            candidates = [
                inst
                for inst in instances
                if inst.profile == profile or (inst.profile == "void" and int(inst.end) - int(inst.start) == size)
            ]
            workloads = [inst for inst in candidates if inst.profile == profile and inst.workload is not None]
            empty_count = sum(1 for inst in candidates if inst.workload is None)
            if not workloads or empty_count <= 0 or len(candidates) <= len(workloads):
                continue
            reassigned = _best_same_profile_void_assignment(int(gpu.gpu_id), profile, candidates, workloads, old_map)
            if reassigned is None:
                continue
            replacement_by_interval = {(inst.start, inst.end): inst for inst in reassigned}
            instances = [
                replacement_by_interval.get((inst.start, inst.end), inst)
                for inst in instances
            ]
        refined_gpus.append(GPUState(gpu_id=int(gpu.gpu_id), source=getattr(gpu, "source", "real"), instances=instances))
    refined = ClusterState(gpus=sorted(refined_gpus, key=lambda g: int(g.gpu_id)), metadata=copy.deepcopy(target.metadata))
    assert_valid_cluster_state(refined)
    return refined


def _best_same_profile_void_assignment(
    gpu_id: int,
    profile: str,
    candidates: list[MigInstance],
    workloads: list[MigInstance],
    old_map: dict[tuple[int, int, int, str], MigInstance],
) -> list[MigInstance] | None:
    slots = sorted(candidates, key=lambda inst: (inst.start, inst.end, inst.profile))
    items = sorted(workloads, key=lambda inst: (inst.workload, int(inst.batch or 0), -int(inst.preserved), inst.start, inst.end))
    memo: dict[tuple[int, int], tuple[tuple[int, ...], list[tuple[MigInstance, int]]] | None] = {}

    def score(item: MigInstance, slot: MigInstance) -> tuple[int, ...]:
        old = old_map.get((gpu_id, int(slot.start), int(slot.end), profile))
        exact = int(old is not None and old.workload == item.workload and old.profile == profile)
        partition = int(old is not None and old.profile == profile)
        movement = abs(int(slot.start) - int(item.start)) + abs(int(slot.end) - int(item.end))
        return (
            exact,
            partition,
            -movement,
            -int(slot.start),
        )

    def add_score(left: tuple[int, ...], right: tuple[int, ...]) -> tuple[int, ...]:
        return tuple(a + b for a, b in zip(left, right))

    def search(item_idx: int, used_mask: int) -> tuple[tuple[int, ...], list[tuple[MigInstance, int]]] | None:
        key = (item_idx, used_mask)
        if key in memo:
            return memo[key]
        if item_idx >= len(items):
            result = ((0, 0, 0, 0), [])
            memo[key] = result
            return result
        item = items[item_idx]
        best = None
        for slot_idx, slot in enumerate(slots):
            if used_mask & (1 << slot_idx):
                continue
            suffix = search(item_idx + 1, used_mask | (1 << slot_idx))
            if suffix is None:
                continue
            total = add_score(score(item, slot), suffix[0])
            assignment = [(item, slot_idx)] + suffix[1]
            if best is None or total > best[0]:
                best = (total, assignment)
        memo[key] = best
        return best

    result = search(0, 0)
    if result is None:
        return None
    used_slots = set()
    replacements = []
    for item, slot_idx in result[1]:
        slot = slots[slot_idx]
        used_slots.add(slot_idx)
        old = old_map.get((gpu_id, int(slot.start), int(slot.end), profile))
        replacements.append(
            MigInstance(
                start=slot.start,
                end=slot.end,
                profile=profile,
                workload=item.workload,
                batch=item.batch,
                mu=item.mu,
                preserved=bool(old is not None and old.workload == item.workload and old.profile == profile),
            )
        )
    for slot_idx, slot in enumerate(slots):
        if slot_idx in used_slots:
            continue
        replacements.append(
            MigInstance(
                start=slot.start,
                end=slot.end,
                profile="void",
                workload=None,
                batch=None,
                mu=0.0,
                preserved=False,
            )
        )
    return sorted(replacements, key=lambda inst: (inst.start, inst.end, inst.profile))


def _add_elapsed(step_times: dict[str, float], key: str, start: float) -> None:
    step_times[key] = step_times.get(key, 0.0) + (time.perf_counter() - start)


def select_local_physical_layout_combo(
    ordered_abstract_templates: list[str],
    prev_state: ClusterState | None,
    per_gpu_topk: int,
) -> dict[str, Any]:
    physical_templates = []
    intervals_list = []
    score_acc = (0, 0, 0, 0)
    for gpu_id, abstract_template in enumerate(ordered_abstract_templates):
        candidates = target_candidates.physical_layout_candidates_for_gpu(
            abstract_template=abstract_template,
            gpu_id=gpu_id,
            prev_state=prev_state,
            topk=max(1, per_gpu_topk),
        )
        if not candidates:
            raise RuntimeError(f"No physical realization for {abstract_template}")
        physical_template, intervals, layout_score = max(candidates, key=lambda item: (item[2], item[0]))
        physical_templates.append(physical_template)
        intervals_list.append(intervals)
        score_acc = tuple(a + b for a, b in zip(score_acc, layout_score))
    return {
        "physical_template_strs": physical_templates,
        "intervals_list": intervals_list,
        "layout_score": score_acc,
    }


def build_target_state_anchor_fill_fast(
    milp_res: dict[str, Any],
    prev_state=None,
    feasible_option_df: Any | None = None,
    workload_names: list[str] | tuple[str, ...] | None = None,
    arrival_rate: list[float] | tuple[float, ...] | None = None,
    abstract_template_topk: int = 4,
    physical_layout_topk: int = 2,
    per_gpu_layout_topk: int = 1,
    local_materialization_selection: bool = False,
    residual_reuse_matching: bool = False,
    enable_count_level_solver: bool = True,
    force_include_empty_anchor: bool = True,
    placement_repair_rounds: int = 3,
    verbose: bool = False,
):
    start = time.perf_counter()
    step_times: dict[str, float] = {}

    t0 = time.perf_counter()
    milp_template_ref = extract_template_list_from_milp(milp_res)
    instance_demands = extract_instance_demands_from_milp(milp_res, feasible_option_df)
    arrivals = _arrival_dict_from_milp(
        milp_res,
        workload_names=workload_names,
        arrival_rate=arrival_rate,
    )
    gpu_count = len(milp_template_ref)
    demands = _expand_demands_with_ids(instance_demands)
    _add_elapsed(step_times, "extract_logical_target", t0)

    best_target = None
    best_metrics = None
    best_score = None
    best_anchor_count = 0

    t0 = time.perf_counter()
    anchor_sets = select_full_gpu_anchor_sets(
        prev_state=prev_state,
        demands=demands,
        gpu_count=gpu_count,
        max_sets=abstract_template_topk,
        force_include_empty=force_include_empty_anchor,
    )
    _add_elapsed(step_times, "anchor_set_selection", t0)

    for selected_anchors in anchor_sets:
        t0 = time.perf_counter()
        try:
            anchored_gpus, residual_demands = consume_anchor_demands(selected_anchors, demands)
        except RuntimeError:
            continue
        _add_elapsed(step_times, "anchor_demand_consumption", t0)
        rem_gpus = gpu_count - len(anchored_gpus)
        if rem_gpus < 0:
            continue
        t0 = time.perf_counter()
        residual_need = profile_need_from_expanded_demands(residual_demands)
        _add_elapsed(step_times, "residual_need_build", t0)
        if rem_gpus == 0:
            if any(residual_need.values()):
                continue
            target = ClusterState(gpus=anchored_gpus, metadata={"arrivals": dict(arrivals)})
            ensure_state_metadata(target)
            metrics = {
                "ordered_abstract_templates": [gpu.template_str() for gpu in target.real_gpus()],
                "ordered_physical_templates": [gpu.template_str() for gpu in target.real_gpus()],
                "layout_preserve_score": (0, 0, 0, 0),
                "exact_preserve": sum(len(nonvoid_workload_instances(gpu)) for gpu in target.real_gpus()),
                "upgrade_preserve": 0,
                "same_gpu_preserve": 0,
                "spread": 0,
                "collocate_pairs": 0,
                "mixed_gpu_count": 0,
                "template_match_count": len(target.real_gpus()),
            }
            score = target_builder._score_tuple(metrics, True)
            if best_score is None or score > best_score:
                best_target = target
                best_metrics = metrics
                best_score = score
                best_anchor_count = len(anchored_gpus)
            continue

        filler_prev_builder = make_residual_reuse_prev_state if residual_reuse_matching else make_filler_prev_state
        filler_prev_options = [
            filler_prev_builder(
                prev_state,
                selected_anchors,
                rem_gpus,
                residual_demands=residual_demands,
                residual_need=residual_need,
            ),
        ]
        seen_prev_maps = set()
        for filler_prev, temp_to_old in filler_prev_options:
            prev_key = tuple(sorted(temp_to_old.items()))
            if prev_key in seen_prev_maps:
                continue
            seen_prev_maps.add(prev_key)

            t0 = time.perf_counter()
            filler = greedy_fill_templates(rem_gpus, residual_need, milp_template_ref)
            filler_candidates = []
            if filler is not None:
                filler_candidates.append(filler)
            if residual_reuse_matching:
                reuse_guided = reuse_guided_fill_templates(
                    rem_gpus=rem_gpus,
                    need=residual_need,
                    residual_demands=residual_demands,
                    milp_template_ref=milp_template_ref,
                    filler_prev_state=filler_prev,
                )
                if reuse_guided is not None:
                    filler_candidates.insert(0, reuse_guided)
            milp_slice = [
                template
                for template in milp_template_ref
                if template in TEMPLATE_NAME_TO_K
            ][:rem_gpus]
            if len(milp_slice) == rem_gpus and dominates_need(milp_slice, residual_need):
                filler_candidates.append(milp_slice)
            if enable_count_level_solver:
                count_level = count_level_residual_template_candidates(
                    rem_gpus=rem_gpus,
                    profile_need=residual_need,
                    milp_template_ref=milp_template_ref,
                    prev_state=filler_prev,
                    max_candidates=max(1, abstract_template_topk),
                    avoid_unusable_fragments=True,
                )
                filler_candidates.extend(count_level)
            filler_candidates = dedup_candidates(
                [c for c in filler_candidates if len(c) == rem_gpus and dominates_need(c, residual_need)],
                max_candidates=max(1, abstract_template_topk),
            )
            _add_elapsed(step_times, "residual_template_fill", t0)

            if not filler_candidates:
                continue

            t0 = time.perf_counter()
            filler_candidates = target_candidates._augment_candidate_abstract_template_sets(
                candidate_sets=filler_candidates,
                milp_template_ref=milp_template_ref,
                prev_state=filler_prev,
                max_candidates=max(1, abstract_template_topk),
            )
            _add_elapsed(step_times, "template_augmentation", t0)

            for candidate_abstract in filler_candidates[:abstract_template_topk]:
                t0 = time.perf_counter()
                if residual_reuse_matching:
                    ordered_abstract = order_templates_by_residual_reuse_matching(
                        candidate_templates=candidate_abstract,
                        filler_prev_state=filler_prev,
                        residual_demands=residual_demands,
                        residual_need=residual_need,
                    )
                else:
                    ordered_abstract = target_candidates._order_candidate_templates_for_gpu_ids(
                        candidate_templates=candidate_abstract,
                        gpu_count=rem_gpus,
                        prev_state=filler_prev,
                        milp_template_ref=milp_template_ref,
                    )
                _add_elapsed(step_times, "template_ordering", t0)

                t0 = time.perf_counter()
                if local_materialization_selection:
                    physical_layout_combos = [
                        select_local_physical_layout_combo(
                            ordered_abstract_templates=ordered_abstract,
                            prev_state=filler_prev,
                            per_gpu_topk=per_gpu_layout_topk,
                        )
                    ]
                else:
                    physical_layout_combos = target_candidates._enumerate_physical_layout_combinations(
                        ordered_abstract_templates=ordered_abstract,
                        prev_state=filler_prev,
                        milp_template_ref=milp_template_ref,
                        max_combos=physical_layout_topk,
                        per_gpu_topk=per_gpu_layout_topk,
                    )
                _add_elapsed(step_times, "physical_layout_selection", t0)
                for combo in physical_layout_combos[:physical_layout_topk]:
                    try:
                        t0 = time.perf_counter()
                        filler_state, filler_metrics = target_builder._solve_target_with_greedy_repair(
                            demands=residual_demands,
                            ordered_abstract_templates=ordered_abstract,
                            ordered_physical_templates=combo["physical_template_strs"],
                            intervals_list=combo["intervals_list"],
                            prev_state=filler_prev,
                            native_profile_need=residual_need,
                            layout_preserve_score=combo["layout_score"],
                            repair_rounds=placement_repair_rounds,
                        )
                        _add_elapsed(step_times, "preserve_aware_placement", t0)
                    except RuntimeError:
                        continue
                    t0 = time.perf_counter()
                    target = combine_anchor_and_filler_state(anchored_gpus, filler_state, temp_to_old)
                    target.metadata["arrivals"] = dict(arrivals)
                    target = target_builder.reassign_gpu_ids_by_matching(target, prev_state)
                    target = target_builder._apply_same_logical_template_order_fix(target, prev_state)
                    ensure_state_metadata(target)
                    _add_elapsed(step_times, "combine_and_id_matching", t0)

                    full_metrics = dict(filler_metrics)
                    full_metrics["exact_preserve"] = int(filler_metrics.get("exact_preserve", 0)) + sum(
                        len(nonvoid_workload_instances(gpu)) for gpu in anchored_gpus
                    )
                    full_metrics["template_match_count"] = int(filler_metrics.get("template_match_count", 0)) + len(anchored_gpus)
                    full_metrics["anchor_count"] = len(anchored_gpus)
                    full_metrics["ordered_abstract_templates"] = [
                        gpu.template_str() for gpu in sorted(target.real_gpus(), key=lambda g: int(g.gpu_id))
                    ]
                    full_metrics["ordered_physical_templates"] = [
                        gpu.template_str() for gpu in sorted(target.real_gpus(), key=lambda g: int(g.gpu_id))
                    ]
                    score = target_builder._score_tuple(full_metrics, True)
                    if best_score is None or score > best_score:
                        best_target = target
                        best_metrics = full_metrics
                        best_score = score
                        best_anchor_count = len(anchored_gpus)

    if best_target is None:
        fallback = target_builder.build_target_state_from_milp(
            milp_res=milp_res,
            prev_state=prev_state,
            feasible_option_df=feasible_option_df,
            workload_names=workload_names,
            arrival_rate=arrival_rate,
            abstract_template_topk=2,
            physical_layout_topk=2,
            per_gpu_layout_topk=1,
            verbose=verbose,
        )
        fallback.metadata["build_method"] = "current_guided_fallback_legacy"
        return fallback

    t0 = time.perf_counter()
    best_target = refine_same_profile_void_placement(best_target, prev_state)
    _add_elapsed(step_times, "same_profile_void_refinement", t0)
    elapsed = time.perf_counter() - start
    best_target.metadata["build_metrics"] = dict(best_metrics)
    best_target.metadata["build_metrics"]["score_tuple"] = best_score
    best_target.metadata["build_metrics"]["elapsed_time_sec"] = elapsed
    best_target.metadata["build_metrics"]["anchor_count"] = best_anchor_count
    best_target.metadata["build_metrics"]["step_times_sec"] = dict(sorted(step_times.items()))
    if residual_reuse_matching:
        best_target.metadata["build_method"] = "current_guided_materialization"
    else:
        best_target.metadata["build_method"] = "anchor_local_materialization" if local_materialization_selection else "anchor_fill_fast"
    return best_target


def build_target_state_current_guided_materialization(
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
    return build_target_state_anchor_fill_fast(
        milp_res=milp_res,
        prev_state=prev_state,
        feasible_option_df=feasible_option_df,
        workload_names=workload_names,
        arrival_rate=arrival_rate,
        abstract_template_topk=abstract_template_topk,
        physical_layout_topk=physical_layout_topk,
        per_gpu_layout_topk=per_gpu_layout_topk,
        local_materialization_selection=True,
        residual_reuse_matching=True,
        enable_count_level_solver=True,
        force_include_empty_anchor=True,
        placement_repair_rounds=placement_repair_rounds,
        verbose=verbose,
    )


def build_target_state_paper_anchor_local_materialization(
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
    target = build_target_state_anchor_fill_fast(
        milp_res=milp_res,
        prev_state=prev_state,
        feasible_option_df=feasible_option_df,
        workload_names=workload_names,
        arrival_rate=arrival_rate,
        abstract_template_topk=abstract_template_topk,
        physical_layout_topk=physical_layout_topk,
        per_gpu_layout_topk=per_gpu_layout_topk,
        local_materialization_selection=True,
        residual_reuse_matching=False,
        enable_count_level_solver=False,
        force_include_empty_anchor=False,
        placement_repair_rounds=placement_repair_rounds,
        verbose=verbose,
    )
    target.metadata["build_method"] = "paper_anchor_legacy"
    return target


def build_target_state_anchor_local_materialization(
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
    return build_target_state_paper_anchor_local_materialization(
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


def build_target_state_residual_reuse_matching(
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
    return build_target_state_anchor_fill_fast(
        milp_res=milp_res,
        prev_state=prev_state,
        feasible_option_df=feasible_option_df,
        workload_names=workload_names,
        arrival_rate=arrival_rate,
        abstract_template_topk=abstract_template_topk,
        physical_layout_topk=physical_layout_topk,
        per_gpu_layout_topk=per_gpu_layout_topk,
        local_materialization_selection=True,
        residual_reuse_matching=True,
        placement_repair_rounds=placement_repair_rounds,
        verbose=verbose,
    )


__all__ = [
    "build_target_state_anchor_fill_fast",
    "build_target_state_current_guided_materialization",
    "build_target_state_paper_anchor_local_materialization",
    "build_target_state_anchor_local_materialization",
    "build_target_state_residual_reuse_matching",
]
