from __future__ import annotations

import time
from typing import Any

from .global_objective import profile_compatible
from .physical_capacity import assign_physical_capacity
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
STAGE2_OBJECTIVE_ORDER = ("exact_workload",)


def _is_empty_gpu(gpu: GPUState) -> bool:
    return all(
        inst.profile in EMPTY_PROFILES or inst.workload is None
        for inst in gpu.instances
    )


def _assign_target_physical_metadata(target: ClusterState, prev_state: ClusterState | None) -> None:
    ensure_state_metadata(target)
    if prev_state is None:
        return
    ensure_state_metadata(prev_state)
    prev_map = {
        int(gpu_id): str(physical_id)
        for gpu_id, physical_id in dict(prev_state.metadata.get("physical_id_map", {})).items()
    }
    pool = [str(item) for item in list(prev_state.metadata.get("free_physical_gpu_pool", []))]
    if str(prev_state.metadata.get("source", "")).startswith("go-cluster-state-manager"):
        # Stage3/action lowering consumes the observed free pool by reversing it
        # and popping from the end, so mirror that policy here.  Validation
        # targets must predict the same physical GPU IDs as the executable DAG.
        pool = list(reversed(pool))
    policy = str(prev_state.metadata.get("free_physical_gpu_pool_policy", "lifo"))
    if policy != "lifo":
        raise ValueError(f"Unsupported physical GPU free-pool policy: {policy}")

    assigned: dict[int, str] = {}
    used: set[str] = set()
    for gpu in sorted(target.real_gpus(), key=lambda item: int(item.gpu_id)):
        gpu_id = int(gpu.gpu_id)
        physical_id = prev_map.get(gpu_id)
        if physical_id is None:
            while pool and pool[-1] in used:
                pool.pop()
            if not pool:
                raise RuntimeError(f"No free physical GPU available for target logical GPU {gpu_id}")
            physical_id = pool.pop()
        assigned[gpu_id] = physical_id
        used.add(physical_id)

    target.metadata["physical_id_map"] = assigned
    target.metadata["free_physical_gpu_pool"] = [
        physical_id
        for physical_id in pool
        if physical_id not in used
    ]
    target.metadata["free_physical_gpu_pool_policy"] = policy
    target.metadata["next_physical_idx"] = int(prev_state.metadata.get("next_physical_idx", 0))


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
        if _is_empty_gpu(gpu):
            continue
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


def _extract_specific_assignments(
    variables: dict[tuple[int, int, int, int], Any],
    demand_types: list[dict[str, Any]],
) -> dict[tuple[int, int, int], dict[str, Any]]:
    assigned: dict[tuple[int, int, int], dict[str, Any]] = {}
    for (type_idx, gpu_id, layout_id, slot_idx), var in variables.items():
        if var.X <= 0.5:
            continue
        key = (gpu_id, layout_id, slot_idx)
        if key in assigned:
            raise RuntimeError(
                f"Multiple demand types assigned to preserved slot {key}: "
                f"{assigned[key]} and {demand_types[type_idx]}"
            )
        assigned[key] = demand_types[type_idx]
    return assigned


def build_target_state_exact_milp(
    milp_res: dict[str, Any],
    prev_state: ClusterState | None = None,
    feasible_option_df: Any | None = None,
    workload_names: list[str] | tuple[str, ...] | None = None,
    arrival_rate: list[float] | tuple[float, ...] | None = None,
    time_limit_s: float | None = None,
    mip_gap: float | None = 0.0,
    threads: int | None = 8,
    seed: int | None = 1,
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
        seed=seed,
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
    seed: int | None,
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
    prev_by_id = {
        gpu_id: gpu
        for gpu_id, gpu in (gpu_map_by_id(prev_state) if prev_state is not None else {}).items()
        if not _is_empty_gpu(gpu)
    }
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
    if seed is not None:
        model.Params.Seed = int(seed)
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

    physical_mu = {
        (str(row["workload"]), str(row["profile"]), int(row["batch"])): float(row["mu"])
        for row in feasible_option_df.to_dict("records")
    }
    required_capacity = _arrival_dict_from_milp(
        milp_res, workload_names=workload_names, arrival_rate=arrival_rate)
    for workload, required in required_capacity.items():
        terms = []
        for assignments in (c_cur, c_new):
            for (idx, _, profile), var in assignments.items():
                demand = demand_types[idx]
                if demand["workload"] == workload:
                    terms.append(physical_mu[(workload, profile, demand["batch"])] * var)
        for (idx, _, layout_id, slot_idx), var in y.items():
            demand = demand_types[idx]
            if demand["workload"] == workload:
                profile = layouts[layout_id].slots[slot_idx][2]
                terms.append(physical_mu[(workload, profile, demand["batch"])] * var)
        model.addConstr(gp.quicksum(terms) >= float(required), name=f"physical_capacity_{workload}")

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

    preserve_slot_terms: dict[tuple[int, int, int], list[Any]] = {}
    for (type_idx, gpu_id, layout_id, slot_idx), var in y.items():
        model.addConstr(var <= z[(gpu_id, layout_id)], name=f"preserve_slot_active_{type_idx}_{gpu_id}_{layout_id}_{slot_idx}")
        preserve_slot_terms.setdefault((gpu_id, layout_id, slot_idx), []).append(var)

    # Different batches (or exact/upgrade types) must not share a physical slot.
    for (gpu_id, layout_id, slot_idx), terms in preserve_slot_terms.items():
        model.addConstr(
            gp.quicksum(terms) <= z[(gpu_id, layout_id)],
            name=f"preserve_slot_exclusive_{gpu_id}_{layout_id}_{slot_idx}",
        )

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
    if cold_start_mode:
        model.setObjective(0.0)
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

        model.setObjectiveN(
            p_exact,
            index=0,
            priority=1,
            weight=1.0,
            abstol=0.0,
            reltol=0.0,
            name="exact_workload",
        )

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

    assigned_specific = _extract_specific_assignments(y, demand_types)

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
        score_tuple = (0,)
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
        "objective_mode": ("cold_start_feasibility" if cold_start_mode else "transition_preservation"),
        "objective_order": tuple(STAGE2_OBJECTIVE_ORDER),
        "cold_workload_gpu_incidence": None,
        "elapsed_time_sec": elapsed,
        "solver_status": status,
        "mip_gap": _safe_mip_gap(model),
        "optimality_proven": bool(model.Status == GRB.OPTIMAL),
        "gurobi_threads": int(threads) if threads is not None else 0,
        "gurobi_seed": int(seed) if seed is not None else None,
        "configured_mip_gap": float(mip_gap) if mip_gap is not None else None,
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

    assign_physical_capacity(target, feasible_option_df)
    assert_valid_cluster_state(target)
    _assign_target_physical_metadata(target, prev_state)
    return target


__all__ = ["build_target_state_exact_milp"]
