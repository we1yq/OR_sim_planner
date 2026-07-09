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
from ..state import ClusterState, GPUState, MigInstance, PROFILE_SIZE, assert_valid_cluster_state
from .target_candidates import _make_slot_list_from_intervals_list, _prev_real_template_list
from .templates import (
    FRAGMENTATION_AVOIDANCE_REWRITE_CANDIDATES,
    candidate_priority_no_prev,
    physical_profiles_to_intervals,
)


def _placement_key(demand_or_inst: Any) -> str:
    if isinstance(demand_or_inst, dict):
        return str(
            demand_or_inst.get("placementGroup")
            or demand_or_inst.get("modelKey")
            or demand_or_inst.get("workload")
            or ""
        )
    return str(
        getattr(demand_or_inst, "placement_group", None)
        or getattr(demand_or_inst, "model_key", None)
        or getattr(demand_or_inst, "workload", None)
        or ""
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
    placement_gpus = defaultdict(set)
    per_gpu_placement_count = defaultdict(Counter)

    for slot in slots:
        info = assigned.get(slot["slot_id"], None)
        if info is None:
            continue
        demand = info["demand"]
        placement_key = _placement_key(demand)
        placement_gpus[placement_key].add(slot["gpu_id"])
        per_gpu_placement_count[slot["gpu_id"]][placement_key] += 1

        if info.get("placement_mode") == "upgrade_preserve":
            upgrade_preserve += 1
            continue

        old = old_map.get((slot["gpu_id"], slot["start"], slot["end"], slot["profile"]))
        if old is not None and old.workload == demand["workload"] and old.profile == demand["profile"]:
            exact_preserve += 1

    spread = sum(len(gpus) for gpus in placement_gpus.values())

    collocate_pairs = 0
    mixed_gpu_count = 0
    for _, count_by_placement in per_gpu_placement_count.items():
        kinds = sum(1 for _, count in count_by_placement.items() if count > 0)
        if kinds >= 2:
            mixed_gpu_count += 1
        for _, count in count_by_placement.items():
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
        if inst.profile not in {"void", "unusable"}
    )


def _score_rewrite_assignment(
    gpu_id: int,
    inst: MigInstance,
    slot: dict[str, Any],
    prev_state: ClusterState | None,
    prefer_first_1g_idle: bool,
    old_map: dict[tuple[int, int, int, str], MigInstance],
) -> tuple[int, ...]:
    old = old_map.get((gpu_id, slot["start"], slot["end"], slot["profile"])) if prev_state is not None else None
    exact_preserve = int(old is not None and old.workload == inst.workload and old.profile == inst.profile)
    partition_preserve = int(old is not None and old.profile == slot["profile"])
    exact_profile_fit = int(slot["profile"] == inst.profile)
    size_waste = slot["size"] - PROFILE_SIZE[inst.profile]
    first_1g_penalty = int(
        prev_state is None
        and prefer_first_1g_idle
        and inst.profile == "1g"
        and slot["profile"] == "1g"
        and slot["oneg_rank"] == 1
    )
    movement = abs(int(slot["start"]) - int(inst.start)) + abs(int(slot["end"]) - int(inst.end))
    return (
        exact_preserve,
        partition_preserve,
        exact_profile_fit,
        -size_waste,
        -first_1g_penalty,
        -movement,
        -int(slot["start"]),
        -int(slot["slot_idx"]),
    )


def _add_score(left: tuple[int, ...], right: tuple[int, ...]) -> tuple[int, ...]:
    return tuple(a + b for a, b in zip(left, right))


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
        if profile in {"void", "unusable"}:
            continue
        if profile == "1g":
            oneg_rank += 1
            rank = oneg_rank
        else:
            rank = 0
        candidate_slots.append(
            {
                "slot_idx": len(candidate_slots),
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
    memo: dict[tuple[int, int], tuple[tuple[int, ...], list[tuple[MigInstance, int]]] | None] = {}

    def best_assignment(inst_idx: int, used_mask: int) -> tuple[tuple[int, ...], list[tuple[MigInstance, int]]] | None:
        key = (inst_idx, used_mask)
        if key in memo:
            return memo[key]
        if inst_idx >= len(assigned_work):
            result = ((0, 0, 0, 0, 0, 0, 0, 0), [])
            memo[key] = result
            return result

        inst = assigned_work[inst_idx]
        best: tuple[tuple[int, ...], list[tuple[MigInstance, int]]] | None = None
        for idx, slot in enumerate(candidate_slots):
            if used_mask & (1 << idx):
                continue
            if slot["size"] < PROFILE_SIZE[inst.profile]:
                continue
            suffix = best_assignment(inst_idx + 1, used_mask | (1 << idx))
            if suffix is None:
                continue
            local_score = _score_rewrite_assignment(
                gpu_id=gpu_id,
                inst=inst,
                slot=slot,
                prev_state=prev_state,
                prefer_first_1g_idle=prefer_first_1g_idle,
                old_map=old_map,
            )
            score = _add_score(local_score, suffix[0])
            assignment = [(inst, idx)] + suffix[1]
            if best is None or score > best[0]:
                best = (score, assignment)

        memo[key] = best
        return best

    result = best_assignment(0, 0)
    if result is None:
        return None

    slot_used = [False] * len(candidate_slots)
    new_instances = []

    for inst, chosen_idx in result[1]:
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
        if inst.workload is None or inst.profile in {"void", "unusable"}:
            inst.preserved = False
        else:
            inst.preserved = inst_preserve_match(inst, gpu_id, prev_state, old_map=old_map)
    return completed


def _apply_fragmentation_avoidance_rewrite_for_gpu(
    gpu_id: int,
    instances: list[MigInstance],
    prev_state: ClusterState | None,
) -> list[MigInstance]:
    old_map = old_exact_slot_map(prev_state)
    current_template = _profiles_string_from_instances(instances)
    if current_template not in FRAGMENTATION_AVOIDANCE_REWRITE_CANDIDATES:
        return sorted(instances, key=lambda x: (x.start, x.end))

    candidates = FRAGMENTATION_AVOIDANCE_REWRITE_CANDIDATES[current_template]
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
            and inst.profile not in {"void", "unusable"}
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
        model_key = None
        placement_group = None
        mu = 0.0
        preserved = False

        if info is not None:
            demand = info["demand"]
            workload = demand["workload"]
            batch = int(demand["batch"])
            model_key = demand.get("modelKey") or workload
            placement_group = demand.get("placementGroup") or model_key
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
                model_key=model_key,
                placement_group=placement_group,
                mu=mu,
                preserved=preserved,
            )
        )

    gpus = []
    gpu_ids = sorted({slot["gpu_id"] for slot in slots})
    actual_physical_templates = []

    for gpu_id in gpu_ids:
        instances = sorted(by_gpu[gpu_id], key=lambda x: (x.start, x.end))
        rewritten = _apply_fragmentation_avoidance_rewrite_for_gpu(gpu_id, instances, prev_state)
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
    placement_gpus: dict[str, set[int]]
    per_gpu_placement_count: dict[int, Counter]

    def clone(self) -> "_BeamNode":
        return _BeamNode(
            assigned=dict(self.assigned),
            used_slots=set(self.used_slots),
            placement_gpus=defaultdict(set, {key: set(value) for key, value in self.placement_gpus.items()}),
            per_gpu_placement_count=defaultdict(Counter, {key: Counter(value) for key, value in self.per_gpu_placement_count.items()}),
        )


def _beam_seed_from_preassign(assigned: dict[int, dict[str, Any] | None]) -> _BeamNode:
    placement_gpus = defaultdict(set)
    per_gpu_placement_count = defaultdict(Counter)
    used = set()
    for slot_id, info in assigned.items():
        if info is None:
            continue
        slot = info["slot"]
        demand = info["demand"]
        used.add(slot_id)
        placement_key = _placement_key(demand)
        placement_gpus[placement_key].add(slot["gpu_id"])
        per_gpu_placement_count[slot["gpu_id"]][placement_key] += 1
    return _BeamNode(
        assigned=dict(assigned),
        used_slots=used,
        placement_gpus=placement_gpus,
        per_gpu_placement_count=per_gpu_placement_count,
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
                _placement_key(demand),
                demand["workload"],
                int(demand["batch"]),
                int(demand["demand_id"]),
            )
        )
    else:
        ordered.sort(
            key=lambda demand: (
                _placement_key(demand),
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
    placement_key = _placement_key(demand)
    same_placement = node.per_gpu_placement_count[gpu_id][placement_key]
    new_touch = 1 if gpu_id not in node.placement_gpus[placement_key] else 0
    distinct_before = len(node.per_gpu_placement_count[gpu_id])
    mixed_penalty = 1 if distinct_before >= 1 and same_placement == 0 else 0
    preserve_bonus = 1 if slot_preserve_match(slot, demand, prev_state) else 0
    return (
        preserve_bonus,
        same_placement,
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
                placement_key = _placement_key(demand)
                child.placement_gpus[placement_key].add(slot["gpu_id"])
                child.per_gpu_placement_count[slot["gpu_id"]][placement_key] += 1
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
                _placement_key(demand),
                demand["workload"],
                -PROFILE_SIZE[demand["profile"]],
                int(demand["batch"]),
                int(demand["demand_id"]),
            )
        )
    else:
        ordered.sort(
            key=lambda demand: (
                _placement_key(demand),
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
    placement_key = _placement_key(demand)
    existing_profiles = per_gpu_profiles[gpu_id].get(placement_key, set())
    if len(existing_profiles) == 0:
        return 0
    profile = demand["profile"]
    return 1 if profile not in existing_profiles else 0


def _build_assignment_stats(
    assigned: dict[int, dict[str, Any] | None],
) -> tuple[dict[int, Counter], dict[str, set[int]], dict[int, dict[str, set[str]]]]:
    per_gpu_placement_count = defaultdict(Counter)
    placement_gpus = defaultdict(set)
    per_gpu_profiles = defaultdict(lambda: defaultdict(set))
    for _, info in assigned.items():
        if info is None:
            continue
        slot = info["slot"]
        demand = info["demand"]
        gpu_id = slot["gpu_id"]
        placement_key = _placement_key(demand)
        per_gpu_placement_count[gpu_id][placement_key] += 1
        placement_gpus[placement_key].add(gpu_id)
        per_gpu_profiles[gpu_id][placement_key].add(demand["profile"])
    return per_gpu_placement_count, placement_gpus, per_gpu_profiles


class _RepairScoreTracker:
    def __init__(
        self,
        slots: list[dict[str, Any]],
        assigned: dict[int, dict[str, Any] | None],
        ordered_abstract_templates: list[str],
        ordered_physical_templates: list[str],
        layout_preserve_score: tuple[int, int, int, int],
        prev_state: ClusterState | None,
        old_map: dict[tuple[int, int, int, str], MigInstance],
    ) -> None:
        self.slot_map = {slot["slot_id"]: slot for slot in slots}
        self.layout_preserve_score = tuple(layout_preserve_score)
        self.prev_mode = has_prev(prev_state)
        self.old_map = old_map
        self.template_match_count = _template_match_count(
            ordered_abstract_templates,
            ordered_physical_templates,
            prev_state,
        )
        self.exact_preserve = 0
        self.upgrade_preserve = 0
        self.spread = 0
        self.collocate_pairs = 0
        self.mixed_gpu_count = 0
        self.per_gpu_workload_count: dict[int, Counter] = defaultdict(Counter)
        self.workload_gpu_count: Counter = Counter()
        for slot_id, info in assigned.items():
            if info is not None:
                self.add(slot_id, info)

    @staticmethod
    def _pairs(count: int) -> int:
        return count * (count - 1) // 2

    def _is_exact(self, slot: dict[str, Any], info: dict[str, Any]) -> bool:
        if info.get("placement_mode") == "upgrade_preserve":
            return False
        demand = info["demand"]
        old = self.old_map.get((slot["gpu_id"], slot["start"], slot["end"], slot["profile"]))
        return old is not None and old.workload == demand["workload"] and old.profile == demand["profile"]

    def _score_tuple(self) -> tuple[int, ...]:
        if self.prev_mode:
            return (
                self.exact_preserve,
                self.upgrade_preserve,
                -self.spread,
                self.collocate_pairs,
                -self.mixed_gpu_count,
                self.layout_preserve_score[0],
                self.layout_preserve_score[1],
                self.template_match_count,
            )
        return (
            -self.spread,
            self.collocate_pairs,
            -self.mixed_gpu_count,
            self.layout_preserve_score[0],
            self.template_match_count,
        )

    def score(self) -> tuple[int, ...]:
        return self._score_tuple()

    def remove(self, slot_id: int, info: dict[str, Any]) -> None:
        slot = self.slot_map[slot_id]
        demand = info["demand"]
        workload = demand["workload"]
        gpu_id = slot["gpu_id"]
        if info.get("placement_mode") == "upgrade_preserve":
            self.upgrade_preserve -= 1
        elif self._is_exact(slot, info):
            self.exact_preserve -= 1

        counter = self.per_gpu_workload_count[gpu_id]
        old_count = counter[workload]
        old_kinds = sum(1 for count in counter.values() if count > 0)
        old_mixed = int(old_kinds >= 2)
        self.collocate_pairs += self._pairs(old_count - 1) - self._pairs(old_count)
        counter[workload] -= 1
        if counter[workload] == 0:
            self.workload_gpu_count[workload] -= 1
            self.spread -= 1
        new_kinds = sum(1 for count in counter.values() if count > 0)
        new_mixed = int(new_kinds >= 2)
        self.mixed_gpu_count += new_mixed - old_mixed

    def add(self, slot_id: int, info: dict[str, Any]) -> None:
        slot = self.slot_map[slot_id]
        demand = info["demand"]
        workload = demand["workload"]
        gpu_id = slot["gpu_id"]
        if info.get("placement_mode") == "upgrade_preserve":
            self.upgrade_preserve += 1
        elif self._is_exact(slot, info):
            self.exact_preserve += 1

        counter = self.per_gpu_workload_count[gpu_id]
        old_count = counter[workload]
        old_kinds = sum(1 for count in counter.values() if count > 0)
        old_mixed = int(old_kinds >= 2)
        if old_count == 0:
            self.workload_gpu_count[workload] += 1
            self.spread += 1
        counter[workload] += 1
        self.collocate_pairs += self._pairs(old_count + 1) - self._pairs(old_count)
        new_kinds = sum(1 for count in counter.values() if count > 0)
        new_mixed = int(new_kinds >= 2)
        self.mixed_gpu_count += new_mixed - old_mixed

    def trial_move(self, src: int, dst: int, new_info: dict[str, Any], old_info: dict[str, Any]) -> tuple[int, ...]:
        self.remove(src, old_info)
        self.add(dst, new_info)
        score = self.score()
        self.remove(dst, new_info)
        self.add(src, old_info)
        return score

    def trial_swap(
        self,
        slot_id_1: int,
        slot_id_2: int,
        info_1: dict[str, Any],
        info_2: dict[str, Any],
        new_info_1: dict[str, Any],
        new_info_2: dict[str, Any],
    ) -> tuple[int, ...]:
        self.remove(slot_id_1, info_1)
        self.remove(slot_id_2, info_2)
        self.add(slot_id_1, new_info_1)
        self.add(slot_id_2, new_info_2)
        score = self.score()
        self.remove(slot_id_2, new_info_2)
        self.remove(slot_id_1, new_info_1)
        self.add(slot_id_2, info_2)
        self.add(slot_id_1, info_1)
        return score


def _greedy_incremental_rank(
    demand: dict[str, Any],
    slot: dict[str, Any],
    assigned: dict[int, dict[str, Any] | None],
    prev_state: ClusterState | None,
    stats: tuple[dict[int, Counter], dict[str, set[int]], dict[int, dict[str, set[str]]]] | None = None,
    old_map: dict[tuple[int, int, int, str], MigInstance] | None = None,
) -> tuple[int, ...]:
    if stats is None:
        per_gpu_placement_count, placement_gpus, per_gpu_profiles = _build_assignment_stats(assigned)
    else:
        per_gpu_placement_count, placement_gpus, per_gpu_profiles = stats
    if old_map is None:
        old_map = old_exact_slot_map(prev_state)

    gpu_id = slot["gpu_id"]
    placement_key = _placement_key(demand)
    same_placement = per_gpu_placement_count[gpu_id][placement_key]
    new_touch = 1 if gpu_id not in placement_gpus[placement_key] else 0
    distinct_before = len(per_gpu_placement_count[gpu_id])
    mixed_penalty = 1 if distinct_before >= 1 and same_placement == 0 else 0
    preserve_bonus = 1 if slot_preserve_match(slot, demand, prev_state, old_map=old_map) else 0
    upgrade_bonus = 1 if slot_upgrade_preserve_match(slot, demand, prev_state, old_map=old_map) else 0
    combo_bonus = _template_combo_bonus(demand, slot, per_gpu_profiles)

    if has_prev(prev_state):
        return (
            preserve_bonus,
            upgrade_bonus,
            same_placement,
            -new_touch,
            combo_bonus,
            -mixed_penalty,
            -slot["slot_idx"],
        )
    return (
        same_placement,
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
    per_gpu_placement_count, placement_gpus, per_gpu_profiles = _build_assignment_stats(assigned)

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

        stats = (per_gpu_placement_count, placement_gpus, per_gpu_profiles)
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
        placement_key = _placement_key(demand)
        per_gpu_placement_count[gpu_id][placement_key] += 1
        placement_gpus[placement_key].add(gpu_id)
        per_gpu_profiles[gpu_id][placement_key].add(demand["profile"])

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

    slot_map = {slot["slot_id"]: slot for slot in slots}
    slot_ids = [slot["slot_id"] for slot in slots]
    tracker = _RepairScoreTracker(
        slots=slots,
        assigned=assigned,
        ordered_abstract_templates=ordered_abstract_templates,
        ordered_physical_templates=ordered_physical_templates,
        layout_preserve_score=layout_preserve_score,
        prev_state=prev_state,
        old_map=old_map,
    )
    cur_score = tracker.score()

    for _ in range(repair_rounds):
        improved = False
        best_score = cur_score
        best_op: tuple[Any, ...] | None = None

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

                moved_info = {
                    "slot": slot_map[dst],
                    "demand": src_info["demand"],
                    "placement_mode": src_info.get("placement_mode", "greedy"),
                }
                score = tracker.trial_move(src, dst, moved_info, src_info)
                if score > best_score:
                    best_score = score
                    best_op = ("move", src, dst, moved_info)
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

                slot_1 = info_1["slot"]
                slot_2 = info_2["slot"]
                new_info_1 = {
                    "slot": slot_1,
                    "demand": info_2["demand"],
                    "placement_mode": "greedy",
                }
                new_info_2 = {
                    "slot": slot_2,
                    "demand": info_1["demand"],
                    "placement_mode": "greedy",
                }

                score = tracker.trial_swap(slot_id_1, slot_id_2, info_1, info_2, new_info_1, new_info_2)
                if score > best_score:
                    best_score = score
                    best_op = ("swap", slot_id_1, slot_id_2, new_info_1, new_info_2)
                    improved = True

        if not improved:
            break

        if best_op is None:
            break
        if best_op[0] == "move":
            _, src, dst, moved_info = best_op
            src_info = assigned[src]
            tracker.remove(src, src_info)
            tracker.add(dst, moved_info)
            assigned[dst] = moved_info
            assigned[src] = None
        elif best_op[0] == "swap":
            _, slot_id_1, slot_id_2, new_info_1, new_info_2 = best_op
            info_1 = assigned[slot_id_1]
            info_2 = assigned[slot_id_2]
            tracker.remove(slot_id_1, info_1)
            tracker.remove(slot_id_2, info_2)
            tracker.add(slot_id_1, new_info_1)
            tracker.add(slot_id_2, new_info_2)
            assigned[slot_id_1] = new_info_1
            assigned[slot_id_2] = new_info_2
        cur_score = best_score

    return _assignments_to_target(
        slots=slots,
        assigned=assigned,
        ordered_abstract_templates=ordered_abstract_templates,
        ordered_physical_templates=ordered_physical_templates,
        layout_preserve_score=layout_preserve_score,
        prev_state=prev_state,
    )
