from __future__ import annotations

from collections import Counter
from typing import Any

from .preserve import physical_layout_candidates_for_gpu
from .state import ClusterState, PROFILE_SIZE
from .templates import (
    PROFILE_ORDER,
    TEMPLATE_NAME_TO_K,
    template_name_list,
    template_to_parts,
)


UPGRADE_REWRITE_TEMPLATE_MAP = {
    "3+3": "4+3",
    "3+2+1": "4+2+1",
    "3+1+1+1": "4+1+1+1",
}


def _prev_real_template_list(prev_state: ClusterState | None) -> list[str]:
    if prev_state is None:
        return []
    return [gpu.template_str() for gpu in prev_state.real_gpus()]


def _template_usefulness_score(
    template_name: str,
    profile_need: dict[str, int],
    prev_templates_counter: Counter,
    milp_templates_counter: Counter,
) -> tuple[int, int, int, int, str]:
    cap = TEMPLATE_NAME_TO_K[template_name]
    cover = sum(
        min(cap[profile], profile_need.get(profile, 0)) * PROFILE_SIZE[profile]
        for profile in PROFILE_ORDER
    )
    return (
        1000 * int(prev_templates_counter[template_name] > 0),
        500 * int(milp_templates_counter[template_name] > 0),
        cover,
        -len(template_to_parts(template_name)),
        template_name,
    )


def _dominates_need(cur_cap: dict[str, int], profile_need: dict[str, int]) -> bool:
    return all(cur_cap.get(profile, 0) >= profile_need.get(profile, 0) for profile in PROFILE_ORDER)


def _enumerate_candidate_abstract_template_sets(
    gpu_count: int,
    profile_need: dict[str, int],
    milp_template_ref: list[str],
    prev_state: ClusterState | None,
    max_candidates: int = 64,
) -> list[list[str]]:
    all_templates = template_name_list()
    prev_templates = _prev_real_template_list(prev_state)
    prev_counter = Counter(prev_templates)
    milp_counter = Counter(milp_template_ref)

    pool = []
    seen = set()
    for template_name in prev_templates + milp_template_ref + all_templates:
        if template_name in TEMPLATE_NAME_TO_K and template_name not in seen:
            seen.add(template_name)
            pool.append(template_name)

    pool.sort(
        key=lambda template_name: _template_usefulness_score(
            template_name,
            profile_need,
            prev_counter,
            milp_counter,
        ),
        reverse=True,
    )

    max_per_gpu = {profile: 0 for profile in PROFILE_ORDER}
    for template_name in pool:
        cap = TEMPLATE_NAME_TO_K[template_name]
        for profile in PROFILE_ORDER:
            max_per_gpu[profile] = max(max_per_gpu[profile], cap[profile])

    candidates = []

    def optimistic_reachable(cur_cap: dict[str, int], remaining_gpus: int) -> bool:
        for profile in PROFILE_ORDER:
            if cur_cap[profile] + remaining_gpus * max_per_gpu[profile] < profile_need[profile]:
                return False
        return True

    def dfs(pos: int, chosen: list[str], cur_cap: dict[str, int], start_idx: int) -> None:
        if len(candidates) >= max_candidates * 8:
            return
        if pos == gpu_count:
            if _dominates_need(cur_cap, profile_need):
                candidates.append(list(chosen))
            return
        if not optimistic_reachable(cur_cap, gpu_count - pos):
            return

        for idx in range(start_idx, len(pool)):
            template_name = pool[idx]
            cap = TEMPLATE_NAME_TO_K[template_name]
            nxt = dict(cur_cap)
            for profile in PROFILE_ORDER:
                nxt[profile] += cap[profile]
            chosen.append(template_name)
            dfs(pos + 1, chosen, nxt, idx)
            chosen.pop()

    dfs(0, [], {profile: 0 for profile in PROFILE_ORDER}, 0)

    if not candidates:
        raise RuntimeError("No abstract template multiset can satisfy profile_need")

    def candidate_rank(candidate: list[str]) -> tuple[int, int, int, list[str]]:
        counter = Counter(candidate)
        over = sum(
            max(
                0,
                sum(TEMPLATE_NAME_TO_K[template_name][profile] * counter[template_name] for template_name in counter)
                - profile_need[profile],
            )
            for profile in PROFILE_ORDER
        )
        return (
            sum(min(counter[template_name], prev_counter[template_name]) for template_name in counter),
            sum(min(counter[template_name], milp_counter[template_name]) for template_name in counter),
            -over,
            sorted(candidate),
        )

    candidates.sort(key=candidate_rank, reverse=True)

    out = []
    seen_candidates = set()
    for candidate in candidates:
        key = tuple(sorted(candidate))
        if key in seen_candidates:
            continue
        seen_candidates.add(key)
        out.append(list(candidate))
        if len(out) >= max_candidates:
            break
    return out


def _augment_candidate_abstract_template_sets(
    candidate_sets: list[list[str]],
    milp_template_ref: list[str],
    prev_state: ClusterState | None,
    max_candidates: int = 64,
) -> list[list[str]]:
    prev_templates = _prev_real_template_list(prev_state)
    prev_counter = Counter(prev_templates)
    milp_counter = Counter(milp_template_ref)

    augmented = []
    seen = set()

    def add(candidate: list[str]) -> None:
        key = tuple(sorted(candidate))
        if key in seen:
            return
        seen.add(key)
        augmented.append(list(candidate))

    for candidate in candidate_sets:
        add(candidate)

    for candidate in candidate_sets:
        counter = Counter(candidate)
        for base_template, upgrade_template in UPGRADE_REWRITE_TEMPLATE_MAP.items():
            anchor = max(prev_counter.get(upgrade_template, 0), milp_counter.get(upgrade_template, 0))
            if counter.get(base_template, 0) <= 0 or anchor <= 0:
                continue
            limit = min(counter[base_template], anchor)
            for k in range(1, limit + 1):
                new_candidate = list(candidate)
                changed = 0
                for idx, template_name in enumerate(new_candidate):
                    if template_name == base_template and changed < k:
                        new_candidate[idx] = upgrade_template
                        changed += 1
                add(new_candidate)
                if len(augmented) >= max_candidates * 3:
                    break
            if len(augmented) >= max_candidates * 3:
                break
        if len(augmented) >= max_candidates * 3:
            break

    def augmented_rank(candidate: list[str]) -> tuple[int, int, int, int, tuple[str, ...]]:
        counter = Counter(candidate)
        upgrade_count = sum(counter.get(upgrade_template, 0) for upgrade_template in UPGRADE_REWRITE_TEMPLATE_MAP.values())
        return (
            sum(min(counter[template_name], prev_counter[template_name]) for template_name in counter),
            sum(min(counter[template_name], milp_counter[template_name]) for template_name in counter),
            upgrade_count,
            -sum(len(template_to_parts(template_name)) for template_name in candidate),
            tuple(sorted(candidate)),
        )

    augmented.sort(key=augmented_rank, reverse=True)
    return augmented[:max_candidates]


def _order_candidate_templates_for_gpu_ids(
    candidate_templates: list[str],
    gpu_count: int,
    prev_state: ClusterState | None,
    milp_template_ref: list[str],
) -> list[str]:
    remain = list(candidate_templates)
    prev_templates = _prev_real_template_list(prev_state)
    ordered = []

    for gpu_id in range(gpu_count):
        pick = None
        if gpu_id < len(prev_templates) and prev_templates[gpu_id] in remain:
            pick = prev_templates[gpu_id]
        elif gpu_id < len(milp_template_ref) and milp_template_ref[gpu_id] in remain:
            pick = milp_template_ref[gpu_id]
        else:
            remain.sort(key=lambda template_name: (len(template_to_parts(template_name)), template_name))
            pick = remain[0]
        ordered.append(pick)
        remain.remove(pick)

    return ordered


def _enumerate_physical_layout_combinations(
    ordered_abstract_templates: list[str],
    prev_state: ClusterState | None,
    milp_template_ref: list[str],
    max_combos: int = 32,
    per_gpu_topk: int = 4,
) -> list[dict[str, Any]]:
    per_gpu_candidates = []
    for gpu_id, abstract_template in enumerate(ordered_abstract_templates):
        per_gpu_candidates.append(
            physical_layout_candidates_for_gpu(
                abstract_template=abstract_template,
                gpu_id=gpu_id,
                prev_state=prev_state,
                topk=per_gpu_topk,
            )
        )

    combos = []

    def dfs(
        gpu_id: int,
        chosen_physical_templates: list[str],
        chosen_intervals: list[list[tuple[int, int, str]]],
        score_acc: tuple[int, int, int, int],
    ) -> None:
        if len(combos) >= max_combos * 8:
            return
        if gpu_id == len(ordered_abstract_templates):
            combos.append((score_acc, list(chosen_physical_templates), list(chosen_intervals)))
            return
        for physical_template, intervals, layout_score in per_gpu_candidates[gpu_id]:
            nxt = (
                score_acc[0] + layout_score[0],
                score_acc[1] + layout_score[1],
                score_acc[2] + layout_score[2],
                score_acc[3] + layout_score[3],
            )
            chosen_physical_templates.append(physical_template)
            chosen_intervals.append(intervals)
            dfs(gpu_id + 1, chosen_physical_templates, chosen_intervals, nxt)
            chosen_physical_templates.pop()
            chosen_intervals.pop()

    dfs(0, [], [], (0, 0, 0, 0))
    combos.sort(key=lambda combo: (combo[0], combo[1]), reverse=True)

    out = []
    seen = set()
    for score_acc, physical_templates, intervals in combos:
        key = tuple(physical_templates)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "physical_template_strs": list(physical_templates),
                "intervals_list": list(intervals),
                "layout_score": tuple(score_acc),
            }
        )
        if len(out) >= max_combos:
            break
    return out


def _make_slot_list_from_intervals_list(
    intervals_list: list[list[tuple[int, int, str]]],
) -> list[dict[str, Any]]:
    slots = []
    slot_id = 0
    for gpu_id, intervals in enumerate(intervals_list):
        for local_idx, (start, end, profile) in enumerate(intervals):
            if profile == "void":
                continue
            slots.append(
                {
                    "slot_id": slot_id,
                    "gpu_id": gpu_id,
                    "slot_idx": local_idx,
                    "start": start,
                    "end": end,
                    "profile": profile,
                }
            )
            slot_id += 1
    return slots
