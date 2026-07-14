from __future__ import annotations

import time
from typing import Any

from .global_objective import profile_compatible
from .templates import (
    PhysicalLayout,
    current_gpu_physical_layout_key,
    fragment_free_physical_layouts,
)
from ..allocation_optimizer.milp_extraction import (
    _arrival_dict_from_milp,
    extract_instance_demands_from_milp,
)
from ..physical_ids import ensure_state_metadata
from ..state import ClusterState, GPUState, MigInstance, assert_valid_cluster_state, gpu_map_by_id


EMPTY_PROFILES = {"void", "unusable"}


def _status_name(gurobi_status: int, grb: Any) -> str:
    names = {
        grb.OPTIMAL: "OPTIMAL",
        grb.INFEASIBLE: "INFEASIBLE",
        grb.INF_OR_UNBD: "INF_OR_UNBD",
        grb.UNBOUNDED: "UNBOUNDED",
        grb.CUTOFF: "CUTOFF",
        grb.ITERATION_LIMIT: "ITERATION_LIMIT",
        grb.NODE_LIMIT: "NODE_LIMIT",
        grb.TIME_LIMIT: "TIME_LIMIT",
        grb.SOLUTION_LIMIT: "SOLUTION_LIMIT",
        grb.INTERRUPTED: "INTERRUPTED",
        grb.NUMERIC: "NUMERIC",
        grb.SUBOPTIMAL: "SUBOPTIMAL",
    }
    return names.get(int(gurobi_status), str(gurobi_status))


def _safe_mip_gap(model: Any) -> float | None:
    try:
        return float(model.MIPGap)
    except Exception:
        return None


def _current_layout_ids_by_gpu(
    prev_state: ClusterState | None,
    layouts: list[PhysicalLayout],
) -> dict[int, int]:
    if prev_state is None:
        return {}
    by_key = {layout.intervals: layout.layout_id for layout in layouts}
    out = {}
    for gpu in prev_state.real_gpus():
        key = current_gpu_physical_layout_key(gpu)
        if key not in by_key:
            raise ValueError(
                f"Current GPU {gpu.gpu_id} uses a layout outside the fragment-free catalog: {key}"
            )
        out[int(gpu.gpu_id)] = by_key[key]
    return out


def _prev_slot_map(prev_state: ClusterState | None) -> dict[tuple[int, int, int, str], MigInstance]:
    if prev_state is None:
        return {}
    out = {}
    for gpu in prev_state.real_gpus():
        for inst in gpu.instances:
            if inst.profile in EMPTY_PROFILES:
                continue
            out[(int(gpu.gpu_id), int(inst.start), int(inst.end), str(inst.profile))] = inst
    return out


def _mig_preserve_coeff(
    gpu_id: int,
    layout: PhysicalLayout,
    old_slots: dict[tuple[int, int, int, str], MigInstance],
) -> int:
    total = 0
    for start, end, profile in layout.slots:
        if (int(gpu_id), int(start), int(end), str(profile)) in old_slots:
            total += 1
    return total


def _exact_coeff(
    demand: dict[str, Any],
    gpu_id: int,
    slot: tuple[int, int, str],
    old_slots: dict[tuple[int, int, int, str], MigInstance],
) -> int:
    start, end, profile = slot
    old = old_slots.get((int(gpu_id), int(start), int(end), str(profile)))
    return int(
        old is not None
        and old.workload == demand["workload"]
        and old.profile == profile
        and demand["profile"] == profile
    )


def _upgrade_coeff(
    demand: dict[str, Any],
    gpu_id: int,
    slot: tuple[int, int, str],
    old_slots: dict[tuple[int, int, int, str], MigInstance],
) -> int:
    start, end, profile = slot
    old = old_slots.get((int(gpu_id), int(start), int(end), str(profile)))
    return int(
        demand["profile"] == "3g"
        and profile == "4g"
        and old is not None
        and old.profile == "4g"
        and old.workload == demand["workload"]
    )


def _extract_gpu_count(milp_res: dict[str, Any]) -> int:
    if "gpu_count" not in milp_res or milp_res["gpu_count"] is None:
        raise ValueError("build_target_state_exact_milp requires milp_res['gpu_count']")
    gpu_count = int(milp_res["gpu_count"])
    if gpu_count < 0:
        raise ValueError(f"gpu_count must be non-negative, got {gpu_count}")
    return gpu_count


def _normalize_demand_types(instance_demands: list[dict[str, Any]]) -> list[dict[str, Any]]:
    demand_types = []
    for type_idx, demand in enumerate(instance_demands):
        count = int(demand["count"])
        if count <= 0:
            continue
        demand_types.append(
            {
                "type_id": type_idx,
                "workload": str(demand["workload"]),
                "profile": str(demand["profile"]),
                "batch": int(demand["batch"]),
                "mu": float(demand["mu"]),
                "count": count,
            }
        )
    return demand_types


def build_target_state_exact_milp(
    milp_res: dict[str, Any],
    prev_state: ClusterState | None = None,
    feasible_option_df: Any | None = None,
    workload_names: list[str] | tuple[str, ...] | None = None,
    arrival_rate: list[float] | tuple[float, ...] | None = None,
    time_limit_s: float | None = None,
    mip_gap: float | None = 0.0,
    threads: int | None = None,
    verbose: bool = False,
) -> ClusterState:
    try:
        import gurobipy as gp
        from gurobipy import GRB
    except ImportError as exc:
        raise RuntimeError(
            "build_target_state_exact_milp requires gurobipy and a valid Gurobi installation"
        ) from exc

    return _build_target_state_exact_milp_aggregated(
        gp=gp,
        GRB=GRB,
        milp_res=milp_res,
        prev_state=prev_state,
        feasible_option_df=feasible_option_df,
        workload_names=workload_names,
        arrival_rate=arrival_rate,
        time_limit_s=time_limit_s,
        mip_gap=mip_gap,
        threads=threads,
        verbose=verbose,
    )

def _layout_profile_caps(layout: PhysicalLayout) -> dict[str, int]:
    caps: dict[str, int] = {}
    for _, _, profile in layout.slots:
        caps[str(profile)] = caps.get(str(profile), 0) + 1
    return caps


def _real_profiles(layouts: list[PhysicalLayout]) -> list[str]:
    profiles = sorted({profile for layout in layouts for _, _, profile in layout.slots})
    return sorted(profiles, key=lambda item: ({"7g": 0, "4g": 1, "3g": 2, "2g": 3, "1g": 4}.get(item, 99), item))


def _inst_from_demand(
    start: int,
    end: int,
    profile: str,
    demand: dict[str, Any] | None,
    old_slots: dict[tuple[int, int, int, str], MigInstance],
    gpu_id: int,
) -> MigInstance:
    if demand is None:
        return MigInstance(start=start, end=end, profile=profile)
    exact = bool(_exact_coeff(demand, gpu_id, (start, end, profile), old_slots))
    upgrade = bool(_upgrade_coeff(demand, gpu_id, (start, end, profile), old_slots))
    return MigInstance(
        start=start,
        end=end,
        profile=profile,
        workload=demand["workload"],
        batch=int(demand["batch"]),
        mu=float(demand["mu"]),
        preserved=exact or upgrade,
    )


def _build_target_state_exact_milp_aggregated(
    *,
    gp: Any,
    GRB: Any,
    milp_res: dict[str, Any],
    prev_state: ClusterState | None,
    feasible_option_df: Any | None,
    workload_names: list[str] | tuple[str, ...] | None,
    arrival_rate: list[float] | tuple[float, ...] | None,
    time_limit_s: float | None,
    mip_gap: float | None,
    threads: int | None,
    verbose: bool,
) -> ClusterState:
    start_time = time.time()
    gpu_count = _extract_gpu_count(milp_res)
    instance_demands = extract_instance_demands_from_milp(milp_res, feasible_option_df)
    demand_types = _normalize_demand_types(instance_demands)
    demand_count = sum(int(demand["count"]) for demand in demand_types)
    layouts = fragment_free_physical_layouts()
    profiles = _real_profiles(layouts)
    layout_caps = {layout.layout_id: _layout_profile_caps(layout) for layout in layouts}

    current_layout_id = _current_layout_ids_by_gpu(prev_state, layouts)
    prev_by_id = gpu_map_by_id(prev_state) if prev_state is not None else {}
    old_slots = _prev_slot_map(prev_state)
    current_ids = sorted(prev_by_id)
    cold_start_mode = len(current_ids) == 0 and gpu_count > 0
    modeled_gpu_ids = list(range(gpu_count)) if cold_start_mode else current_ids
    current_gpu_count = len(current_ids)
    reused_current_gpu_count = 0 if cold_start_mode else min(current_gpu_count, gpu_count)
    indexed_new_gpu_count = gpu_count if cold_start_mode else 0
    aggregated_new_gpu_count = 0 if cold_start_mode else max(0, gpu_count - current_gpu_count)
    new_gpu_count = indexed_new_gpu_count + aggregated_new_gpu_count
    selected_modeled_gpu_count = gpu_count if cold_start_mode else reused_current_gpu_count
    next_gpu_id = max(current_ids, default=-1) + 1

    if gpu_count == 0 and demand_count > 0:
        raise ValueError("gpu_count is 0 but Stage 1 produced demand instances")

    model = gp.Model("stage2_exact_global_milp_aggregated")
    if not verbose:
        model.Params.OutputFlag = 0
    model.Params.Seed = 1
    if time_limit_s is not None:
        model.Params.TimeLimit = float(time_limit_s)
    if mip_gap is not None:
        model.Params.MIPGap = float(mip_gap)
    if threads is not None:
        model.Params.Threads = int(threads)

    q = {gpu_id: model.addVar(vtype=GRB.BINARY, name=f"q_{gpu_id}") for gpu_id in modeled_gpu_ids}
    z = {
        (gpu_id, layout.layout_id): model.addVar(vtype=GRB.BINARY, name=f"z_{gpu_id}_{layout.layout_id}")
        for gpu_id in modeled_gpu_ids
        for layout in layouts
    }
    new_count = {
        layout.layout_id: model.addVar(vtype=GRB.INTEGER, lb=0, ub=gpu_count, name=f"new_count_{layout.layout_id}")
        for layout in layouts
    }

    c_cur = {}
    for type_idx, demand in enumerate(demand_types):
        for gpu_id in modeled_gpu_ids:
            for profile in profiles:
                if not profile_compatible(str(demand["profile"]), profile):
                    continue
                c_cur[(type_idx, gpu_id, profile)] = model.addVar(
                    vtype=GRB.INTEGER,
                    lb=0,
                    ub=int(demand["count"]),
                    name=f"c_{type_idx}_{gpu_id}_{profile}",
                )

    c_new = {}
    if not cold_start_mode:
        for type_idx, demand in enumerate(demand_types):
            for layout in layouts:
                for profile in profiles:
                    if layout_caps[layout.layout_id].get(profile, 0) <= 0:
                        continue
                    if not profile_compatible(str(demand["profile"]), profile):
                        continue
                    c_new[(type_idx, layout.layout_id, profile)] = model.addVar(
                        vtype=GRB.INTEGER,
                        lb=0,
                        ub=int(demand["count"]),
                        name=f"n_{type_idx}_{layout.layout_id}_{profile}",
                    )

    y = {}
    for type_idx, demand in enumerate(demand_types):
        for gpu_id in modeled_gpu_ids:
            for layout in layouts:
                for slot_idx, slot in enumerate(layout.slots):
                    if not profile_compatible(str(demand["profile"]), str(slot[2])):
                        continue
                    exact = _exact_coeff(demand, gpu_id, slot, old_slots)
                    upgrade = _upgrade_coeff(demand, gpu_id, slot, old_slots)
                    if not exact and not upgrade:
                        continue
                    y[(type_idx, gpu_id, layout.layout_id, slot_idx)] = model.addVar(
                        vtype=GRB.BINARY,
                        name=f"y_{type_idx}_{gpu_id}_{layout.layout_id}_{slot_idx}",
                    )

    model.addConstr(
        gp.quicksum(q.values()) + gp.quicksum(new_count.values()) == gpu_count,
        name="fixed_gpu_count",
    )
    model.addConstr(
        gp.quicksum(q.values()) == selected_modeled_gpu_count,
        name="fixed_modeled_gpu_count",
    )
    model.addConstr(
        gp.quicksum(new_count.values()) == aggregated_new_gpu_count,
        name="fixed_aggregated_new_gpu_count",
    )
    for gpu_id in modeled_gpu_ids:
        model.addConstr(
            gp.quicksum(z[(gpu_id, layout.layout_id)] for layout in layouts) == q[gpu_id],
            name=f"layout_select_{gpu_id}",
        )

    for type_idx, demand in enumerate(demand_types):
        cur_terms = [var for (idx, _, _), var in c_cur.items() if idx == type_idx]
        new_terms = [var for (idx, _, _), var in c_new.items() if idx == type_idx]
        y_terms = [var for (idx, _, _, _), var in y.items() if idx == type_idx]
        model.addConstr(
            gp.quicksum(cur_terms + new_terms + y_terms) == int(demand["count"]),
            name=f"demand_type_count_{type_idx}",
        )

    for (type_idx, gpu_id, layout_id, slot_idx), var in y.items():
        model.addConstr(var <= z[(gpu_id, layout_id)], name=f"preserve_slot_active_{type_idx}_{gpu_id}_{layout_id}_{slot_idx}")

    for gpu_id in modeled_gpu_ids:
        for profile in profiles:
            cur_terms = [
                var
                for (_, gid, prof), var in c_cur.items()
                if gid == gpu_id and prof == profile
            ]
            y_terms = [
                var
                for (_, gid, lid, slot_idx), var in y.items()
                if gid == gpu_id and layouts[lid].slots[slot_idx][2] == profile
            ]
            model.addConstr(
                gp.quicksum(cur_terms + y_terms)
                <= gp.quicksum(
                    int(layout_caps[layout.layout_id].get(profile, 0))
                    * z[(gpu_id, layout.layout_id)]
                    for layout in layouts
                ),
                name=f"cur_cap_{gpu_id}_{profile}",
            )

    for layout in layouts:
        for profile in profiles:
            cap = int(layout_caps[layout.layout_id].get(profile, 0))
            if cap <= 0:
                continue
            terms = [
                var
                for (_, lid, prof), var in c_new.items()
                if lid == layout.layout_id and prof == profile
            ]
            model.addConstr(
                gp.quicksum(terms) <= cap * new_count[layout.layout_id],
                name=f"new_cap_{layout.layout_id}_{profile}",
            )

    a = {}
    for gpu_id in current_ids:
        a[gpu_id] = model.addVar(vtype=GRB.BINARY, name=f"whole_gpu_{gpu_id}")
        layout_id = current_layout_id[gpu_id]
        layout = layouts[layout_id]
        model.addConstr(a[gpu_id] <= z[(gpu_id, layout_id)], name=f"whole_layout_{gpu_id}")
        prev_gpu = prev_by_id[gpu_id]
        occupied = [
            inst
            for inst in prev_gpu.instances
            if inst.profile not in EMPTY_PROFILES and inst.workload is not None
        ]
        slot_index_by_key = {
            (int(start), int(end), str(profile)): slot_idx
            for slot_idx, (start, end, profile) in enumerate(layout.slots)
        }
        for old_inst in occupied:
            key = (int(old_inst.start), int(old_inst.end), str(old_inst.profile))
            slot_idx = slot_index_by_key.get(key)
            if slot_idx is None:
                model.addConstr(a[gpu_id] <= 0, name=f"whole_missing_slot_{gpu_id}_{key}")
                continue
            terms = [
                y[(type_idx, gpu_id, layout_id, slot_idx)]
                for type_idx, demand in enumerate(demand_types)
                if (type_idx, gpu_id, layout_id, slot_idx) in y
                and demand["workload"] == old_inst.workload
                and demand["profile"] == old_inst.profile
            ]
            if terms:
                model.addConstr(a[gpu_id] <= gp.quicksum(terms), name=f"whole_slot_{gpu_id}_{key}")
            else:
                model.addConstr(a[gpu_id] <= 0, name=f"whole_no_exact_{gpu_id}_{key}")

        total_assign_terms = [
            var
            for (_, gid, _), var in c_cur.items()
            if gid == gpu_id
        ] + [
            var
            for (_, gid, lid, _), var in y.items()
            if gid == gpu_id and lid == layout_id
        ]
        model.addConstr(
            gp.quicksum(total_assign_terms) <= len(occupied) + 7 * (1 - a[gpu_id]),
            name=f"whole_no_extra_assignments_{gpu_id}",
        )

    model.ModelSense = GRB.MAXIMIZE
    cold_workload_gpu = {}
    cold_workload_gpu_incidence = None
    if cold_start_mode:
        workloads = sorted({str(demand["workload"]) for demand in demand_types})
        max_count_by_workload = {
            workload: sum(int(demand["count"]) for demand in demand_types if str(demand["workload"]) == workload)
            for workload in workloads
        }
        for workload in workloads:
            for gpu_id in modeled_gpu_ids:
                cold_workload_gpu[(workload, gpu_id)] = model.addVar(
                    vtype=GRB.BINARY,
                    name=f"cold_workload_gpu_{workload}_{gpu_id}",
                )
                assign_terms = [
                    var
                    for (type_idx, gid, _), var in c_cur.items()
                    if gid == gpu_id and str(demand_types[type_idx]["workload"]) == workload
                ]
                model.addConstr(
                    gp.quicksum(assign_terms) <= max_count_by_workload[workload] * cold_workload_gpu[(workload, gpu_id)],
                    name=f"cold_workload_gpu_active_{workload}_{gpu_id}",
                )
        cold_workload_gpu_incidence = gp.quicksum(cold_workload_gpu.values())
        model.setObjectiveN(
            -cold_workload_gpu_incidence,
            index=0,
            priority=1,
            weight=1.0,
            abstol=0.0,
            reltol=0.0,
            name="cold_workload_collocation",
        )
    else:
        p_gpu = gp.quicksum(a.values())
        p_exact = gp.quicksum(
            _exact_coeff(demand_types[type_idx], gpu_id, layouts[layout_id].slots[slot_idx], old_slots) * var
            for (type_idx, gpu_id, layout_id, slot_idx), var in y.items()
        )
        p_upgrade = gp.quicksum(
            _upgrade_coeff(demand_types[type_idx], gpu_id, layouts[layout_id].slots[slot_idx], old_slots) * var
            for (type_idx, gpu_id, layout_id, slot_idx), var in y.items()
        )
        p_mig = gp.quicksum(
            _mig_preserve_coeff(gpu_id, layout, old_slots) * z[(gpu_id, layout.layout_id)]
            for gpu_id in current_ids
            for layout in layouts
        )

        model.setObjectiveN(p_gpu, index=0, priority=4, weight=1.0, abstol=0.0, reltol=0.0, name="whole_gpu")
        model.setObjectiveN(p_exact, index=1, priority=3, weight=1.0, abstol=0.0, reltol=0.0, name="exact_workload")
        model.setObjectiveN(p_upgrade, index=2, priority=2, weight=1.0, abstol=0.0, reltol=0.0, name="upgrade_workload")
        model.setObjectiveN(p_mig, index=3, priority=1, weight=1.0, abstol=0.0, reltol=0.0, name="mig_placement")

    model.optimize()
    status = _status_name(model.Status, GRB)
    if model.SolCount <= 0:
        raise RuntimeError(f"Exact Stage 2 MILP produced no solution; status={status}")

    selected_layout_by_gpu: dict[int, PhysicalLayout] = {}
    active_current_ids = [gpu_id for gpu_id in modeled_gpu_ids if q[gpu_id].X > 0.5]
    for gpu_id in active_current_ids:
        selected = [layout for layout in layouts if z[(gpu_id, layout.layout_id)].X > 0.5]
        if len(selected) != 1:
            raise RuntimeError(f"GPU {gpu_id} has {len(selected)} selected layouts")
        selected_layout_by_gpu[gpu_id] = selected[0]

    assigned_specific: dict[tuple[int, int, int], dict[str, Any]] = {}
    for (type_idx, gpu_id, layout_id, slot_idx), var in y.items():
        if var.X > 0.5:
            assigned_specific[(gpu_id, layout_id, slot_idx)] = demand_types[type_idx]

    remaining_cur: dict[tuple[int, int, str], list[dict[str, Any]]] = {}
    for (type_idx, gpu_id, profile), var in c_cur.items():
        count = int(round(var.X))
        if count <= 0:
            continue
        remaining_cur.setdefault((gpu_id, profile), []).extend([demand_types[type_idx]] * count)

    gpus: list[GPUState] = []
    for gpu_id in sorted(active_current_ids):
        layout = selected_layout_by_gpu[gpu_id]
        instances = []
        real_slot_index = {
            (int(start), int(end), str(profile)): idx
            for idx, (start, end, profile) in enumerate(layout.slots)
        }
        for start, end, profile in layout.intervals:
            if profile == "void":
                instances.append(MigInstance(start=start, end=end, profile=profile))
                continue
            if profile == "unusable":
                raise RuntimeError("Fragment-free exact builder selected an unusable interval")
            slot_idx = real_slot_index[(int(start), int(end), str(profile))]
            demand = assigned_specific.get((gpu_id, layout.layout_id, slot_idx))
            if demand is None:
                bucket = remaining_cur.get((gpu_id, str(profile)), [])
                demand = bucket.pop(0) if bucket else None
            instances.append(_inst_from_demand(start, end, profile, demand, old_slots, gpu_id))
        gpu = GPUState(gpu_id=int(gpu_id), source="real", instances=instances)
        gpu.sort_instances()
        gpus.append(gpu)

    new_layout_counts = {
        layout.layout_id: int(round(new_count[layout.layout_id].X))
        for layout in layouts
        if int(round(new_count[layout.layout_id].X)) > 0
    }
    remaining_new: dict[tuple[int, str], list[dict[str, Any]]] = {}
    for (type_idx, layout_id, profile), var in c_new.items():
        count = int(round(var.X))
        if count <= 0:
            continue
        remaining_new.setdefault((layout_id, profile), []).extend([demand_types[type_idx]] * count)

    next_id = next_gpu_id
    for layout in layouts:
        for _ in range(new_layout_counts.get(layout.layout_id, 0)):
            gpu_id = next_id
            next_id += 1
            instances = []
            for start, end, profile in layout.intervals:
                if profile == "void":
                    instances.append(MigInstance(start=start, end=end, profile=profile))
                    continue
                if profile == "unusable":
                    raise RuntimeError("Fragment-free exact builder selected an unusable interval")
                bucket = remaining_new.get((layout.layout_id, str(profile)), [])
                demand = bucket.pop(0) if bucket else None
                instances.append(_inst_from_demand(start, end, profile, demand, old_slots, gpu_id))
            gpu = GPUState(gpu_id=int(gpu_id), source="real", instances=instances)
            gpu.sort_instances()
            gpus.append(gpu)
            selected_layout_by_gpu[gpu_id] = layout

    target = ClusterState(gpus=sorted(gpus, key=lambda item: int(item.gpu_id)), metadata={})
    try:
        target.metadata["arrivals"] = _arrival_dict_from_milp(
            milp_res,
            workload_names=workload_names,
            arrival_rate=arrival_rate,
        )
    except Exception:
        target.metadata["arrivals"] = {}
    target.metadata["build_method"] = "exact_global_milp_aggregated"

    if cold_start_mode:
        score_tuple = (
            -int(round(cold_workload_gpu_incidence.getValue())) if cold_workload_gpu_incidence is not None else 0,
        )
    else:
        score_tuple = (
            int(round(p_gpu.getValue())),
            int(round(p_exact.getValue())),
            int(round(p_upgrade.getValue())),
            int(round(p_mig.getValue())),
        )
    elapsed = time.time() - start_time
    target.metadata["build_metrics"] = {
        "whole_gpu_preserve": 0 if cold_start_mode else score_tuple[0],
        "exact_preserve": 0 if cold_start_mode else score_tuple[1],
        "upgrade_preserve": 0 if cold_start_mode else score_tuple[2],
        "mig_preserve": 0 if cold_start_mode else score_tuple[3],
        "score_tuple": score_tuple,
        "objective_mode": "cold_start_collocation" if cold_start_mode else "transition_preservation",
        "cold_workload_gpu_incidence": -score_tuple[0] if cold_start_mode else None,
        "elapsed_time_sec": elapsed,
        "solver_status": status,
        "mip_gap": _safe_mip_gap(model),
        "optimality_proven": bool(model.Status == GRB.OPTIMAL),
        "num_vars": int(model.NumVars),
        "num_constraints": int(model.NumConstrs),
        "gpu_count": int(gpu_count),
        "current_gpu_count": int(current_gpu_count),
        "reused_current_gpu_count": int(reused_current_gpu_count),
        "new_gpu_count": int(new_gpu_count),
        "indexed_new_gpu_count": int(indexed_new_gpu_count),
        "aggregated_new_gpu_count": int(aggregated_new_gpu_count),
        "demand_count": int(demand_count),
        "demand_type_count": int(len(demand_types)),
        "layout_count": int(len(layouts)),
        "selected_layouts": {
            int(gpu_id): selected_layout_by_gpu[gpu_id].name
            for gpu_id in sorted(selected_layout_by_gpu)
        },
    }
    target.metadata["stage2_demand_count"] = int(demand_count)
    target.metadata["stage2_demand_type_count"] = int(len(demand_types))

    assert_valid_cluster_state(target)
    ensure_state_metadata(target)
    return target


__all__ = ["build_target_state_exact_milp"]
