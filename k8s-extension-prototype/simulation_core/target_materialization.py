from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any

from .preserve import (
    get_prev_gpu_intervals_cached,
    has_prev,
    inst_preserve_match,
    layout_score_vs_prev,
    old_exact_slot_map,
    slot_preserve_match,
    slot_upgrade_preserve_match,
)
from .state import ClusterState, GPUState, MigInstance, PROFILE_SIZE, assert_valid_cluster_state
from .target_candidates import _make_slot_list_from_intervals_list, _prev_real_template_list
from .templates import (
    VOID_LIKE_REWRITE_CANDIDATES,
    candidate_priority_no_prev,
    physical_profiles_to_intervals,
)


def _template_match_count(
    ordered_abstract_templates: list[str],
    ordered_physical_templates: list[str],
    prev_state: ClusterState | None,
) -> int:
    prev_templates = _prev_real_template_list(prev_state)
    if len(prev_templates) != len(ordered_abstract_templates):
        return 0
    count = 0
    for idx in range(len(ordered_abstract_templates)):
        if idx < len(prev_templates) and (
            prev_templates[idx] == ordered_abstract_templates[idx]
            or prev_templates[idx] == ordered_physical_templates[idx]
        ):
            count += 1
    return count


def _assignments_to_metrics(
    slots: list[dict[str, Any]],
    assigned: dict[int, dict[str, Any] | None],
    ordered_abstract_templates: list[str],
    ordered_physical_templates: list[str],
    layout_preserve_score: tuple[int, ...],
    prev_state: ClusterState | None,
    old_map: dict[tuple[int, int, int, str], MigInstance] | None = None,
) -> dict[str, Any]:
    if old_map is None:
        old_map = old_exact_slot_map(prev_state)

    exact_preserve = 0
    upgrade_preserve = 0
    workload_gpus = defaultdict(set)
    per_gpu_workload_count = defaultdict(Counter)

    for slot in slots:
        info = assigned.get(slot["slot_id"], None)
        if info is None:
            continue
        demand = info["demand"]
        workload_gpus[demand["workload"]].add(slot["gpu_id"])
        per_gpu_workload_count[slot["gpu_id"]][demand["workload"]] += 1

        if info.get("placement_mode") == "upgrade_preserve":
            upgrade_preserve += 1
            continue

        old = old_map.get((slot["gpu_id"], slot["start"], slot["end"], slot["profile"]))
        if old is not None and old.workload == demand["workload"] and old.profile == demand["profile"]:
            exact_preserve += 1

    spread = sum(len(gpus) for gpus in workload_gpus.values())

    collocate_pairs = 0
    mixed_gpu_count = 0
    for _, count_by_workload in per_gpu_workload_count.items():
        kinds = sum(1 for _, count in count_by_workload.items() if count > 0)
        if kinds >= 2:
            mixed_gpu_count += 1
        for _, count in count_by_workload.items():
            collocate_pairs += count * (count - 1) // 2

    return {
        "ordered_abstract_templates": list(ordered_abstract_templates),
        "ordered_physical_templates": list(ordered_physical_templates),
        "layout_preserve_score": tuple(layout_preserve_score),
        "exact_preserve": int(exact_preserve),
        "upgrade_preserve": int(upgrade_preserve),
        "same_gpu_preserve": 0,
        "spread": int(spread),
        "collocate_pairs": int(collocate_pairs),
        "mixed_gpu_count": int(mixed_gpu_count),
        "template_match_count": int(
            _template_match_count(
                ordered_abstract_templates,
                ordered_physical_templates,
                prev_state,
            )
        ),
    }


def _score_tuple(metrics: dict[str, Any], prev_mode: bool) -> tuple[int, ...]:
    if prev_mode:
        return (
            metrics["exact_preserve"],
            metrics["upgrade_preserve"],
            -metrics["spread"],
            metrics["collocate_pairs"],
            -metrics["mixed_gpu_count"],
            metrics["layout_preserve_score"][0],
            metrics["layout_preserve_score"][1],
            metrics["template_match_count"],
        )
    return (
        -metrics["spread"],
        metrics["collocate_pairs"],
        -metrics["mixed_gpu_count"],
        metrics["layout_preserve_score"][0],
        metrics["template_match_count"],
    )


def _profiles_string_from_instances(instances: list[MigInstance]) -> str:
    return "+".join(
        str(PROFILE_SIZE[inst.profile])
        for inst in sorted(instances, key=lambda x: (x.start, x.end))
        if inst.profile != "void"
    )


def _assign_instances_to_candidate_layout(
    gpu_id: int,
    old_assigned_insts: list[MigInstance],
    candidate_profiles: tuple[str, ...],
    prev_state: ClusterState | None,
    prefer_first_1g_idle: bool,
    old_map: dict[tuple[int, int, int, str], MigInstance] | None = None,
) -> list[MigInstance] | None:
    if old_map is None:
        old_map = old_exact_slot_map(prev_state)
    intervals = physical_profiles_to_intervals(candidate_profiles)
    candidate_slots = []
    oneg_rank = 0
    for start, end, profile in intervals:
        if profile == "void":
            continue
        if profile == "1g":
            oneg_rank += 1
            rank = oneg_rank
        else:
            rank = 0
        candidate_slots.append(
            {
                "start": start,
                "end": end,
                "profile": profile,
                "size": PROFILE_SIZE[profile],
                "oneg_rank": rank,
            }
        )

    assigned_work = [
        inst
        for inst in sorted(
            old_assigned_insts,
            key=lambda x: (-PROFILE_SIZE[x.profile], x.start, x.end),
        )
        if inst.workload is not None
    ]
    slot_used = [False] * len(candidate_slots)
    new_instances = []

    for inst in assigned_work:
        compatible = []
        for idx, slot in enumerate(candidate_slots):
            if slot_used[idx]:
                continue
            if slot["size"] < PROFILE_SIZE[inst.profile]:
                continue

            preserve = 0
            if prev_state is not None:
                old = old_map.get((gpu_id, slot["start"], slot["end"], slot["profile"]))
                if old is not None and old.workload == inst.workload and old.profile == inst.profile:
                    preserve = 1

            exact_profile_fit = int(slot["profile"] == inst.profile)
            size_waste = slot["size"] - PROFILE_SIZE[inst.profile]

            first_1g_penalty = 0
            if (
                prev_state is None
                and prefer_first_1g_idle
                and inst.profile == "1g"
                and slot["profile"] == "1g"
                and slot["oneg_rank"] == 1
            ):
                first_1g_penalty = 1

            compatible.append(
                (
                    preserve,
                    exact_profile_fit,
                    -size_waste,
                    -first_1g_penalty,
                    -slot["start"],
                    -idx,
                    idx,
                )
            )

        if not compatible:
            return None

        compatible.sort(reverse=True)
        chosen_idx = compatible[0][-1]
        slot = candidate_slots[chosen_idx]
        slot_used[chosen_idx] = True

        new_instances.append(
            MigInstance(
                start=slot["start"],
                end=slot["end"],
                profile=slot["profile"],
                workload=inst.workload,
                batch=inst.batch,
                mu=inst.mu,
                preserved=False,
            )
        )

    for idx, slot in enumerate(candidate_slots):
        if slot_used[idx]:
            continue
        new_instances.append(
            MigInstance(
                start=slot["start"],
                end=slot["end"],
                profile=slot["profile"],
                workload=None,
                batch=None,
                mu=0.0,
                preserved=False,
            )
        )

    new_instances = sorted(new_instances, key=lambda x: (x.start, x.end))
    cur = 0
    completed = []
    for inst in new_instances:
        if inst.start > cur:
            completed.append(
                MigInstance(
                    start=cur,
                    end=inst.start,
                    profile="void",
                    workload=None,
                    batch=None,
                    mu=0.0,
                    preserved=False,
                )
            )
        completed.append(inst)
        cur = inst.end
    if cur < 7:
        completed.append(
            MigInstance(
                start=cur,
                end=7,
                profile="void",
                workload=None,
                batch=None,
                mu=0.0,
                preserved=False,
            )
        )

    for inst in completed:
        if inst.workload is None or inst.profile == "void":
            inst.preserved = False
        else:
            inst.preserved = inst_preserve_match(inst, gpu_id, prev_state, old_map=old_map)
    return completed


def _rewrite_void_like_layout_for_gpu(
    gpu_id: int,
    instances: list[MigInstance],
    prev_state: ClusterState | None,
) -> list[MigInstance]:
    old_map = old_exact_slot_map(prev_state)
    current_template = _profiles_string_from_instances(instances)
    if current_template not in VOID_LIKE_REWRITE_CANDIDATES:
        return sorted(instances, key=lambda x: (x.start, x.end))

    candidates = VOID_LIKE_REWRITE_CANDIDATES[current_template]
    prev_intervals = get_prev_gpu_intervals_cached(prev_state, gpu_id)

    best = None
    best_key = None

    for candidate_profiles in candidates:
        prefer_first_1g_idle = prev_state is None and current_template in {"3+2+1", "3+1+1+1"}
        rewritten = _assign_instances_to_candidate_layout(
            gpu_id=gpu_id,
            old_assigned_insts=instances,
            candidate_profiles=candidate_profiles,
            prev_state=prev_state,
            prefer_first_1g_idle=prefer_first_1g_idle,
            old_map=old_map,
        )
        if rewritten is None:
            continue

        preserve_count = sum(
            1
            for inst in rewritten
            if inst.workload is not None
            and inst.profile != "void"
            and inst_preserve_match(inst, gpu_id, prev_state, old_map=old_map)
        )

        intervals = [(inst.start, inst.end, inst.profile) for inst in rewritten]
        layout_score = layout_score_vs_prev(intervals, prev_intervals)

        if prev_state is None:
            key = (0,) + tuple(-x for x in candidate_priority_no_prev(current_template, candidate_profiles))
        else:
            key = (
                preserve_count,
                layout_score[0],
                layout_score[1],
                layout_score[2],
                layout_score[3],
            )

        if best is None or key > best_key:
            best = rewritten
            best_key = key

    if best is None:
        return sorted(instances, key=lambda x: (x.start, x.end))
    return sorted(best, key=lambda x: (x.start, x.end))


def _assignments_to_target(
    slots: list[dict[str, Any]],
    assigned: dict[int, dict[str, Any] | None],
    ordered_abstract_templates: list[str],
    ordered_physical_templates: list[str],
    layout_preserve_score: tuple[int, ...],
    prev_state: ClusterState | None,
) -> tuple[ClusterState, dict[str, Any]]:
    old_map = old_exact_slot_map(prev_state)
    by_gpu = defaultdict(list)

    for slot in slots:
        info = assigned.get(slot["slot_id"], None)
        workload = None
        batch = None
        mu = 0.0
        preserved = False

        if info is not None:
            demand = info["demand"]
            workload = demand["workload"]
            batch = int(demand["batch"])
            mu = float(demand["mu"])
            preserved = slot_preserve_match(slot, demand, prev_state, old_map=old_map) or (
                info.get("placement_mode") == "upgrade_preserve"
            )

        by_gpu[slot["gpu_id"]].append(
            MigInstance(
                start=slot["start"],
                end=slot["end"],
                profile=slot["profile"],
                workload=workload,
                batch=batch,
                mu=mu,
                preserved=preserved,
            )
        )

    gpus = []
    gpu_ids = sorted({slot["gpu_id"] for slot in slots})
    actual_physical_templates = []

    for gpu_id in gpu_ids:
        instances = sorted(by_gpu[gpu_id], key=lambda x: (x.start, x.end))
        rewritten = _rewrite_void_like_layout_for_gpu(gpu_id, instances, prev_state)
        gpus.append(GPUState(gpu_id=gpu_id, source="real", instances=rewritten))
        actual_physical_templates.append(_profiles_string_from_instances(rewritten))

    target = ClusterState(gpus=gpus, metadata={})
    assert_valid_cluster_state(target)

    metrics = _assignments_to_metrics(
        slots=slots,
        assigned=assigned,
        ordered_abstract_templates=ordered_abstract_templates,
        ordered_physical_templates=actual_physical_templates,
        layout_preserve_score=layout_preserve_score,
        prev_state=prev_state,
        old_map=old_map,
    )
    metrics["ordered_physical_templates"] = list(actual_physical_templates)
    return target, metrics


def _exact_preserve_preassign(
    demands: list[dict[str, Any]],
    slots: list[dict[str, Any]],
    prev_state: ClusterState | None,
    native_profile_need: dict[str, int],
) -> tuple[dict[int, dict[str, Any] | None], list[dict[str, Any]]]:
    old_map = old_exact_slot_map(prev_state)

    demand_buckets = defaultdict(list)
    for demand in demands:
        demand_buckets[(demand["workload"], demand["profile"])].append(demand)

    for key in demand_buckets:
        demand_buckets[key].sort(key=lambda demand: (int(demand["batch"]), int(demand["demand_id"])))

    assigned = {slot["slot_id"]: None for slot in slots}
    preserved_ids = set()

    for slot in slots:
        old = old_map.get((slot["gpu_id"], slot["start"], slot["end"], slot["profile"]))
        if old is None or old.workload is None:
            continue
        key = (old.workload, old.profile)
        if len(demand_buckets[key]) == 0:
            continue
        demand = demand_buckets[key].pop(0)
        assigned[slot["slot_id"]] = {
            "slot": slot,
            "demand": demand,
            "placement_mode": "exact_preserve",
        }
        preserved_ids.add(demand["demand_id"])

    total_4g_slots = sum(1 for slot in slots if slot["profile"] == "4g")
    native_need_4g = int(native_profile_need.get("4g", 0))
    upgrade_budget = max(0, total_4g_slots - native_need_4g)

    if upgrade_budget > 0 and has_prev(prev_state):
        for slot in slots:
            if upgrade_budget <= 0:
                break
            if slot["profile"] != "4g" or assigned[slot["slot_id"]] is not None:
                continue
            old = old_map.get((slot["gpu_id"], slot["start"], slot["end"], slot["profile"]))
            if old is None or old.workload is None or old.profile != "4g":
                continue
            key = (old.workload, "3g")
            if len(demand_buckets[key]) == 0:
                continue
            demand = demand_buckets[key].pop(0)
            assigned[slot["slot_id"]] = {
                "slot": slot,
                "demand": demand,
                "placement_mode": "upgrade_preserve",
            }
            preserved_ids.add(demand["demand_id"])
            upgrade_budget -= 1

    residual_demands = [demand for demand in demands if demand["demand_id"] not in preserved_ids]
    return assigned, residual_demands


def _exact_preserve_preassign_exact_only(
    demands: list[dict[str, Any]],
    slots: list[dict[str, Any]],
    prev_state: ClusterState | None,
) -> tuple[dict[int, dict[str, Any] | None], list[dict[str, Any]]]:
    old_map = old_exact_slot_map(prev_state)
    demand_buckets = defaultdict(list)
    for demand in demands:
        demand_buckets[(demand["workload"], demand["profile"])].append(demand)
    for key in demand_buckets:
        demand_buckets[key].sort(key=lambda demand: (int(demand["batch"]), int(demand["demand_id"])))

    assigned = {slot["slot_id"]: None for slot in slots}
    preserved_ids = set()
    for slot in slots:
        old = old_map.get((slot["gpu_id"], slot["start"], slot["end"], slot["profile"]))
        if old is None or old.workload is None:
            continue
        key = (old.workload, old.profile)
        if len(demand_buckets[key]) == 0:
            continue
        demand = demand_buckets[key].pop(0)
        assigned[slot["slot_id"]] = {
            "slot": slot,
            "demand": demand,
            "placement_mode": "exact_preserve",
        }
        preserved_ids.add(demand["demand_id"])

    residual_demands = [demand for demand in demands if demand["demand_id"] not in preserved_ids]
    return assigned, residual_demands


@dataclass
class _BeamNode:
    assigned: dict[int, dict[str, Any] | None]
    used_slots: set[int]
    workload_gpus: dict[str, set[int]]
    per_gpu_workload_count: dict[int, Counter]

    def clone(self) -> "_BeamNode":
        return _BeamNode(
            assigned=dict(self.assigned),
            used_slots=set(self.used_slots),
            workload_gpus=defaultdict(set, {key: set(value) for key, value in self.workload_gpus.items()}),
            per_gpu_workload_count=defaultdict(Counter, {key: Counter(value) for key, value in self.per_gpu_workload_count.items()}),
        )


def _beam_seed_from_preassign(assigned: dict[int, dict[str, Any] | None]) -> _BeamNode:
    workload_gpus = defaultdict(set)
    per_gpu_workload_count = defaultdict(Counter)
    used = set()
    for slot_id, info in assigned.items():
        if info is None:
            continue
        slot = info["slot"]
        demand = info["demand"]
        used.add(slot_id)
        workload_gpus[demand["workload"]].add(slot["gpu_id"])
        per_gpu_workload_count[slot["gpu_id"]][demand["workload"]] += 1
    return _BeamNode(
        assigned=dict(assigned),
        used_slots=used,
        workload_gpus=workload_gpus,
        per_gpu_workload_count=per_gpu_workload_count,
    )


def _order_residual_demands_for_beam(
    demands: list[dict[str, Any]],
    slots: list[dict[str, Any]],
    prev_state: ClusterState | None,
) -> list[dict[str, Any]]:
    slots_by_profile = defaultdict(list)
    old_map = old_exact_slot_map(prev_state)
    for slot in slots:
        slots_by_profile[slot["profile"]].append(slot)

    def preserve_candidates(demand: dict[str, Any]) -> int:
        count = 0
        for slot in slots_by_profile[demand["profile"]]:
            old = old_map.get((slot["gpu_id"], slot["start"], slot["end"], slot["profile"]))
            if old is not None and old.workload == demand["workload"] and old.profile == demand["profile"]:
                count += 1
        return count

    ordered = list(demands)
    if has_prev(prev_state):
        ordered.sort(
            key=lambda demand: (
                -preserve_candidates(demand),
                -PROFILE_SIZE[demand["profile"]],
                demand["workload"],
                int(demand["batch"]),
                int(demand["demand_id"]),
            )
        )
    else:
        ordered.sort(
            key=lambda demand: (
                demand["workload"],
                -PROFILE_SIZE[demand["profile"]],
                int(demand["batch"]),
                int(demand["demand_id"]),
            )
        )
    return ordered


def _beam_slot_local_rank(
    demand: dict[str, Any],
    slot: dict[str, Any],
    node: _BeamNode,
    prev_state: ClusterState | None,
) -> tuple[int, ...]:
    gpu_id = slot["gpu_id"]
    workload = demand["workload"]
    same_workload = node.per_gpu_workload_count[gpu_id][workload]
    new_touch = 1 if gpu_id not in node.workload_gpus[workload] else 0
    distinct_before = len(node.per_gpu_workload_count[gpu_id])
    mixed_penalty = 1 if distinct_before >= 1 and same_workload == 0 else 0
    preserve_bonus = 1 if slot_preserve_match(slot, demand, prev_state) else 0
    return (
        preserve_bonus,
        same_workload,
        -new_touch,
        -mixed_penalty,
        -slot["slot_idx"],
    )


def _beam_node_score(
    node: _BeamNode,
    slots: list[dict[str, Any]],
    ordered_abstract_templates: list[str],
    ordered_physical_templates: list[str],
    layout_preserve_score: tuple[int, int, int, int],
    prev_state: ClusterState | None,
) -> tuple[int, ...]:
    _, metrics = _assignments_to_target(
        slots=slots,
        assigned=node.assigned,
        ordered_abstract_templates=ordered_abstract_templates,
        ordered_physical_templates=ordered_physical_templates,
        layout_preserve_score=layout_preserve_score,
        prev_state=prev_state,
    )
    return _score_tuple(metrics, has_prev(prev_state))


def _solve_target_with_preserve_first_beam(
    demands: list[dict[str, Any]],
    ordered_abstract_templates: list[str],
    ordered_physical_templates: list[str],
    intervals_list: list[list[tuple[int, int, str]]],
    prev_state: ClusterState | None,
    beam_width: int = 32,
    slot_choice_width: int = 8,
    layout_preserve_score: tuple[int, int, int, int] = (0, 0, 0, 0),
) -> tuple[ClusterState, dict[str, Any]]:
    slots = _make_slot_list_from_intervals_list(intervals_list)
    assigned0, residual = _exact_preserve_preassign_exact_only(demands, slots, prev_state)
    residual = _order_residual_demands_for_beam(residual, slots, prev_state)
    beam = [_beam_seed_from_preassign(assigned0)]

    slots_by_profile = defaultdict(list)
    for slot in slots:
        slots_by_profile[slot["profile"]].append(slot)

    for demand in residual:
        next_beam = []
        candidate_slots = slots_by_profile[demand["profile"]]
        for node in beam:
            free_slots = [slot for slot in candidate_slots if slot["slot_id"] not in node.used_slots]
            if not free_slots:
                continue
            ranked = sorted(
                free_slots,
                key=lambda slot: _beam_slot_local_rank(demand, slot, node, prev_state),
                reverse=True,
            )[:slot_choice_width]
            for slot in ranked:
                child = node.clone()
                slot_id = slot["slot_id"]
                child.assigned[slot_id] = {
                    "slot": slot,
                    "demand": demand,
                    "placement_mode": "beam",
                }
                child.used_slots.add(slot_id)
                child.workload_gpus[demand["workload"]].add(slot["gpu_id"])
                child.per_gpu_workload_count[slot["gpu_id"]][demand["workload"]] += 1
                next_beam.append(child)

        if not next_beam:
            raise RuntimeError("Preserve-first beam failed: no feasible extension found.")

        next_beam.sort(
            key=lambda node: _beam_node_score(
                node=node,
                slots=slots,
                ordered_abstract_templates=ordered_abstract_templates,
                ordered_physical_templates=ordered_physical_templates,
                layout_preserve_score=layout_preserve_score,
                prev_state=prev_state,
            ),
            reverse=True,
        )
        beam = next_beam[:beam_width]

    best = max(
        beam,
        key=lambda node: _beam_node_score(
            node=node,
            slots=slots,
            ordered_abstract_templates=ordered_abstract_templates,
            ordered_physical_templates=ordered_physical_templates,
            layout_preserve_score=layout_preserve_score,
            prev_state=prev_state,
        ),
    )
    return _assignments_to_target(
        slots=slots,
        assigned=best.assigned,
        ordered_abstract_templates=ordered_abstract_templates,
        ordered_physical_templates=ordered_physical_templates,
        layout_preserve_score=layout_preserve_score,
        prev_state=prev_state,
    )


def _order_residual_demands_for_greedy(
    demands: list[dict[str, Any]],
    slots: list[dict[str, Any]],
    prev_state: ClusterState | None,
) -> list[dict[str, Any]]:
    old_map = old_exact_slot_map(prev_state)

    def preserve_candidates(demand: dict[str, Any]) -> tuple[int, int]:
        exact_count = 0
        upgrade_count = 0
        for slot in slots:
            if slot_preserve_match(slot, demand, prev_state, old_map=old_map):
                exact_count += 1
            elif slot_upgrade_preserve_match(slot, demand, prev_state, old_map=old_map):
                upgrade_count += 1
        return exact_count, upgrade_count

    ordered = list(demands)
    if has_prev(prev_state):
        ordered.sort(
            key=lambda demand: (
                -preserve_candidates(demand)[0],
                -preserve_candidates(demand)[1],
                demand["workload"],
                -PROFILE_SIZE[demand["profile"]],
                int(demand["batch"]),
                int(demand["demand_id"]),
            )
        )
    else:
        ordered.sort(
            key=lambda demand: (
                demand["workload"],
                -PROFILE_SIZE[demand["profile"]],
                int(demand["batch"]),
                int(demand["demand_id"]),
            )
        )
    return ordered


def _template_combo_bonus(
    demand: dict[str, Any],
    slot: dict[str, Any],
    per_gpu_profiles: dict[int, dict[str, set[str]]],
) -> int:
    gpu_id = slot["gpu_id"]
    workload = demand["workload"]
    existing_profiles = per_gpu_profiles[gpu_id].get(workload, set())
    if len(existing_profiles) == 0:
        return 0
    profile = demand["profile"]
    return 1 if profile not in existing_profiles else 0


def _build_assignment_stats(
    assigned: dict[int, dict[str, Any] | None],
) -> tuple[dict[int, Counter], dict[str, set[int]], dict[int, dict[str, set[str]]]]:
    per_gpu_workload_count = defaultdict(Counter)
    workload_gpus = defaultdict(set)
    per_gpu_profiles = defaultdict(lambda: defaultdict(set))
    for _, info in assigned.items():
        if info is None:
            continue
        slot = info["slot"]
        demand = info["demand"]
        gpu_id = slot["gpu_id"]
        workload = demand["workload"]
        per_gpu_workload_count[gpu_id][workload] += 1
        workload_gpus[workload].add(gpu_id)
        per_gpu_profiles[gpu_id][workload].add(demand["profile"])
    return per_gpu_workload_count, workload_gpus, per_gpu_profiles


def _greedy_incremental_rank(
    demand: dict[str, Any],
    slot: dict[str, Any],
    assigned: dict[int, dict[str, Any] | None],
    prev_state: ClusterState | None,
    stats: tuple[dict[int, Counter], dict[str, set[int]], dict[int, dict[str, set[str]]]] | None = None,
    old_map: dict[tuple[int, int, int, str], MigInstance] | None = None,
) -> tuple[int, ...]:
    if stats is None:
        per_gpu_workload_count, workload_gpus, per_gpu_profiles = _build_assignment_stats(assigned)
    else:
        per_gpu_workload_count, workload_gpus, per_gpu_profiles = stats
    if old_map is None:
        old_map = old_exact_slot_map(prev_state)

    gpu_id = slot["gpu_id"]
    workload = demand["workload"]
    same_workload = per_gpu_workload_count[gpu_id][workload]
    new_touch = 1 if gpu_id not in workload_gpus[workload] else 0
    distinct_before = len(per_gpu_workload_count[gpu_id])
    mixed_penalty = 1 if distinct_before >= 1 and same_workload == 0 else 0
    preserve_bonus = 1 if slot_preserve_match(slot, demand, prev_state, old_map=old_map) else 0
    upgrade_bonus = 1 if slot_upgrade_preserve_match(slot, demand, prev_state, old_map=old_map) else 0
    combo_bonus = _template_combo_bonus(demand, slot, per_gpu_profiles)

    if has_prev(prev_state):
        return (
            preserve_bonus,
            upgrade_bonus,
            same_workload,
            -new_touch,
            combo_bonus,
            -mixed_penalty,
            -slot["slot_idx"],
        )
    return (
        same_workload,
        -new_touch,
        combo_bonus,
        -mixed_penalty,
        -slot["slot_idx"],
    )


def _solve_target_with_greedy_repair(
    demands: list[dict[str, Any]],
    ordered_abstract_templates: list[str],
    ordered_physical_templates: list[str],
    intervals_list: list[list[tuple[int, int, str]]],
    prev_state: ClusterState | None,
    native_profile_need: dict[str, int],
    layout_preserve_score: tuple[int, int, int, int] = (0, 0, 0, 0),
    repair_rounds: int = 8,
) -> tuple[ClusterState, dict[str, Any]]:
    slots = _make_slot_list_from_intervals_list(intervals_list)
    assigned, residual = _exact_preserve_preassign(demands, slots, prev_state, native_profile_need)
    old_map = old_exact_slot_map(prev_state)

    free_by_profile = defaultdict(list)
    for slot in slots:
        free_by_profile[slot["profile"]].append(slot)

    residual = _order_residual_demands_for_greedy(residual, slots, prev_state)
    per_gpu_workload_count, workload_gpus, per_gpu_profiles = _build_assignment_stats(assigned)

    total_4g_slots = len(free_by_profile["4g"])
    used_upgrade = sum(
        1
        for _, info in assigned.items()
        if info is not None and info.get("placement_mode") == "upgrade_preserve"
    )
    upgrade_budget_remaining = max(
        0,
        total_4g_slots - int(native_profile_need.get("4g", 0)) - used_upgrade,
    )

    for demand in residual:
        candidates = [
            slot
            for slot in free_by_profile[demand["profile"]]
            if assigned[slot["slot_id"]] is None
        ]

        if demand["profile"] == "3g" and upgrade_budget_remaining > 0 and has_prev(prev_state):
            for slot in free_by_profile["4g"]:
                if assigned[slot["slot_id"]] is not None:
                    continue
                if slot_upgrade_preserve_match(slot, demand, prev_state, old_map=old_map):
                    candidates.append(slot)

        dedup = {}
        for slot in candidates:
            dedup[slot["slot_id"]] = slot
        candidates = list(dedup.values())

        if not candidates:
            raise RuntimeError(
                f"Greedy failed: no free slot for "
                f"({demand['workload']}, {demand['profile']}, B{demand['batch']})"
            )

        stats = (per_gpu_workload_count, workload_gpus, per_gpu_profiles)
        candidates.sort(
            key=lambda slot: _greedy_incremental_rank(
                demand,
                slot,
                assigned,
                prev_state,
                stats=stats,
                old_map=old_map,
            ),
            reverse=True,
        )
        best = candidates[0]

        placement_mode = "greedy"
        if demand["profile"] == "3g" and best["profile"] == "4g":
            placement_mode = "upgrade_preserve"
            upgrade_budget_remaining -= 1

        assigned[best["slot_id"]] = {
            "slot": best,
            "demand": demand,
            "placement_mode": placement_mode,
        }
        gpu_id = best["gpu_id"]
        workload = demand["workload"]
        per_gpu_workload_count[gpu_id][workload] += 1
        workload_gpus[workload].add(gpu_id)
        per_gpu_profiles[gpu_id][workload].add(demand["profile"])

    prev_mode = has_prev(prev_state)

    def is_preserved(slot_id: int, info: dict[str, Any] | None) -> bool:
        if info is None:
            return False
        return info.get("placement_mode") == "upgrade_preserve" or slot_preserve_match(
            info["slot"],
            info["demand"],
            prev_state,
            old_map=old_map,
        )

    cur_metrics = _assignments_to_metrics(
        slots=slots,
        assigned=assigned,
        ordered_abstract_templates=ordered_abstract_templates,
        ordered_physical_templates=ordered_physical_templates,
        layout_preserve_score=layout_preserve_score,
        prev_state=prev_state,
        old_map=old_map,
    )
    cur_score = _score_tuple(cur_metrics, prev_mode)

    slot_map = {slot["slot_id"]: slot for slot in slots}
    slot_ids = [slot["slot_id"] for slot in slots]

    for _ in range(repair_rounds):
        improved = False
        best_assign = dict(assigned)
        best_metrics = dict(cur_metrics)
        best_score = cur_score

        for src in slot_ids:
            src_info = assigned[src]
            if src_info is None or is_preserved(src, src_info):
                continue
            src_profile = src_info["slot"]["profile"]

            for dst in slot_ids:
                if assigned[dst] is not None:
                    continue
                if slot_map[dst]["profile"] != src_profile:
                    continue

                trial = dict(assigned)
                trial[dst] = {
                    "slot": slot_map[dst],
                    "demand": trial[src]["demand"],
                    "placement_mode": trial[src].get("placement_mode", "greedy"),
                }
                trial[src] = None

                metrics = _assignments_to_metrics(
                    slots=slots,
                    assigned=trial,
                    ordered_abstract_templates=ordered_abstract_templates,
                    ordered_physical_templates=ordered_physical_templates,
                    layout_preserve_score=layout_preserve_score,
                    prev_state=prev_state,
                    old_map=old_map,
                )
                score = _score_tuple(metrics, prev_mode)
                if score > best_score:
                    best_score = score
                    best_metrics = metrics
                    best_assign = trial
                    improved = True

        for idx in range(len(slot_ids)):
            slot_id_1 = slot_ids[idx]
            info_1 = assigned[slot_id_1]
            if info_1 is None or is_preserved(slot_id_1, info_1):
                continue

            for jdx in range(idx + 1, len(slot_ids)):
                slot_id_2 = slot_ids[jdx]
                info_2 = assigned[slot_id_2]
                if info_2 is None or is_preserved(slot_id_2, info_2):
                    continue
                if info_1["slot"]["profile"] != info_2["slot"]["profile"]:
                    continue

                trial = dict(assigned)
                slot_1 = info_1["slot"]
                slot_2 = info_2["slot"]
                trial[slot_id_1] = {
                    "slot": slot_1,
                    "demand": info_2["demand"],
                    "placement_mode": "greedy",
                }
                trial[slot_id_2] = {
                    "slot": slot_2,
                    "demand": info_1["demand"],
                    "placement_mode": "greedy",
                }

                metrics = _assignments_to_metrics(
                    slots=slots,
                    assigned=trial,
                    ordered_abstract_templates=ordered_abstract_templates,
                    ordered_physical_templates=ordered_physical_templates,
                    layout_preserve_score=layout_preserve_score,
                    prev_state=prev_state,
                    old_map=old_map,
                )
                score = _score_tuple(metrics, prev_mode)
                if score > best_score:
                    best_score = score
                    best_metrics = metrics
                    best_assign = trial
                    improved = True

        if not improved:
            break

        assigned = best_assign
        cur_metrics = best_metrics
        cur_score = best_score

    return _assignments_to_target(
        slots=slots,
        assigned=assigned,
        ordered_abstract_templates=ordered_abstract_templates,
        ordered_physical_templates=ordered_physical_templates,
        layout_preserve_score=layout_preserve_score,
        prev_state=prev_state,
    )
