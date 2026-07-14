from __future__ import annotations

from typing import Any

from ..state import ClusterState, MigInstance, gpu_map_by_id


EMPTY_PROFILES = {"void", "unusable"}


def profile_compatible(demand_profile: str, slot_profile: str) -> bool:
    return demand_profile == slot_profile or (demand_profile == "3g" and slot_profile == "4g")


def _real_instances_by_slot(gpu: Any) -> dict[tuple[int, int, str], MigInstance]:
    out = {}
    for inst in getattr(gpu, "instances", []):
        if inst.profile in EMPTY_PROFILES:
            continue
        out[(int(inst.start), int(inst.end), str(inst.profile))] = inst
    return out


def _occupied_target_slots(target: ClusterState) -> list[dict[str, Any]]:
    slots = []
    slot_id = 0
    for gpu in sorted(target.real_gpus(), key=lambda item: int(item.gpu_id)):
        for inst in sorted(gpu.instances, key=lambda item: (item.start, item.end, item.profile)):
            if inst.profile in EMPTY_PROFILES or inst.workload is None:
                continue
            slots.append(
                {
                    "slot_id": slot_id,
                    "gpu_id": int(gpu.gpu_id),
                    "start": int(inst.start),
                    "end": int(inst.end),
                    "profile": str(inst.profile),
                    "workload": str(inst.workload),
                }
            )
            slot_id += 1
    return slots


def _target_real_slot_map(target: ClusterState) -> dict[int, dict[tuple[int, int, str], MigInstance]]:
    out = {}
    for gpu in target.real_gpus():
        out[int(gpu.gpu_id)] = _real_instances_by_slot(gpu)
    return out


def _mig_preserve(target: ClusterState, prev_state: ClusterState | None) -> int:
    if prev_state is None:
        return 0
    prev_by_id = gpu_map_by_id(prev_state)
    total = 0
    for gpu in target.real_gpus():
        prev_gpu = prev_by_id.get(int(gpu.gpu_id))
        if prev_gpu is None:
            continue
        prev_slots = set(_real_instances_by_slot(prev_gpu))
        for key in _real_instances_by_slot(gpu):
            if key in prev_slots:
                total += 1
    return total


def evaluate_global_stage2_objective(
    target: ClusterState,
    prev_state: ClusterState | None,
    demands: list[dict[str, Any]],
    *,
    time_limit_s: float | None = None,
) -> tuple[int, int, int, int]:
    """Evaluate the exact Stage 2 lexicographic preservation objective.

    The target state stores physical profiles. A 3g minimum demand placed in a
    physical 4g slot therefore needs to be recovered by matching occupied target
    slots back to the Stage 1 demand instances.
    """

    if prev_state is None:
        return (0, 0, 0, 0)

    try:
        import gurobipy as gp
        from gurobipy import GRB
    except ImportError as exc:
        raise RuntimeError("evaluate_global_stage2_objective requires gurobipy") from exc

    target_slots = _occupied_target_slots(target)
    if len(target_slots) != len(demands):
        raise ValueError(
            f"Target has {len(target_slots)} occupied slots but demands contain {len(demands)} instances"
        )

    prev_by_id = gpu_map_by_id(prev_state)
    prev_slot_by_gpu = {
        gpu_id: _real_instances_by_slot(gpu)
        for gpu_id, gpu in prev_by_id.items()
    }
    target_slot_by_gpu = _target_real_slot_map(target)

    model = gp.Model("stage2_global_objective_evaluator")
    model.Params.OutputFlag = 0
    model.Params.Seed = 1
    if time_limit_s is not None:
        model.Params.TimeLimit = float(time_limit_s)

    x = {}
    for demand_idx, demand in enumerate(demands):
        for slot_idx, slot in enumerate(target_slots):
            if str(demand["workload"]) != slot["workload"]:
                continue
            if not profile_compatible(str(demand["profile"]), slot["profile"]):
                continue
            x[(demand_idx, slot_idx)] = model.addVar(
                vtype=GRB.BINARY,
                name=f"x_{demand_idx}_{slot_idx}",
            )

    for demand_idx in range(len(demands)):
        terms = [var for (idx, _), var in x.items() if idx == demand_idx]
        if not terms:
            raise ValueError(f"Demand {demand_idx} has no compatible target slot")
        model.addConstr(gp.quicksum(terms) == 1, name=f"assign_demand_{demand_idx}")

    for slot_idx in range(len(target_slots)):
        terms = [var for (_, idx), var in x.items() if idx == slot_idx]
        if not terms:
            raise ValueError(f"Target slot {slot_idx} has no compatible demand")
        model.addConstr(gp.quicksum(terms) == 1, name=f"assign_slot_{slot_idx}")

    whole_gpu_vars = {}
    target_slots_by_identity_slot = {
        (slot["gpu_id"], slot["start"], slot["end"], slot["profile"]): idx
        for idx, slot in enumerate(target_slots)
    }

    for gpu_id, prev_gpu in prev_by_id.items():
        whole_gpu_vars[gpu_id] = model.addVar(vtype=GRB.BINARY, name=f"whole_{gpu_id}")
        prev_slots = prev_slot_by_gpu[gpu_id]
        target_real_slots = target_slot_by_gpu.get(gpu_id, {})
        if set(prev_slots) != set(target_real_slots):
            model.addConstr(whole_gpu_vars[gpu_id] <= 0, name=f"whole_layout_{gpu_id}")
            continue
        for key, prev_inst in prev_slots.items():
            target_inst = target_real_slots.get(key)
            if target_inst is None:
                model.addConstr(whole_gpu_vars[gpu_id] <= 0, name=f"whole_missing_{gpu_id}_{key}")
                continue
            if prev_inst.workload is None:
                if target_inst.workload is not None:
                    model.addConstr(whole_gpu_vars[gpu_id] <= 0, name=f"whole_empty_{gpu_id}_{key}")
                continue
            if target_inst.workload != prev_inst.workload:
                model.addConstr(whole_gpu_vars[gpu_id] <= 0, name=f"whole_workload_{gpu_id}_{key}")
                continue
            slot_idx = target_slots_by_identity_slot.get((gpu_id, key[0], key[1], key[2]))
            if slot_idx is None:
                model.addConstr(whole_gpu_vars[gpu_id] <= 0, name=f"whole_slot_{gpu_id}_{key}")
                continue
            terms = [
                x[(demand_idx, slot_idx)]
                for demand_idx, demand in enumerate(demands)
                if (demand_idx, slot_idx) in x
                and str(demand["workload"]) == str(prev_inst.workload)
                and str(demand["profile"]) == str(prev_inst.profile)
            ]
            if terms:
                model.addConstr(
                    whole_gpu_vars[gpu_id] <= gp.quicksum(terms),
                    name=f"whole_exact_{gpu_id}_{key}",
                )
            else:
                model.addConstr(whole_gpu_vars[gpu_id] <= 0, name=f"whole_no_exact_{gpu_id}_{key}")

    p_gpu = gp.quicksum(whole_gpu_vars.values())

    exact_terms = []
    upgrade_terms = []
    for (demand_idx, slot_idx), var in x.items():
        demand = demands[demand_idx]
        slot = target_slots[slot_idx]
        old = prev_slot_by_gpu.get(slot["gpu_id"], {}).get(
            (slot["start"], slot["end"], slot["profile"])
        )
        if old is None:
            continue
        if (
            old.workload == demand["workload"]
            and old.profile == slot["profile"]
            and demand["profile"] == slot["profile"]
        ):
            exact_terms.append(var)
        if (
            demand["profile"] == "3g"
            and slot["profile"] == "4g"
            and old.profile == "4g"
            and old.workload == demand["workload"]
        ):
            upgrade_terms.append(var)

    p_exact = gp.quicksum(exact_terms)
    p_upgrade = gp.quicksum(upgrade_terms)
    p_mig_const = _mig_preserve(target, prev_state)

    model.ModelSense = GRB.MAXIMIZE
    model.setObjectiveN(p_gpu, index=0, priority=4, weight=1.0, name="whole_gpu")
    model.setObjectiveN(p_exact, index=1, priority=3, weight=1.0, name="exact_workload")
    model.setObjectiveN(p_upgrade, index=2, priority=2, weight=1.0, name="upgrade_workload")
    model.setObjectiveN(float(p_mig_const), index=3, priority=1, weight=1.0, name="mig_placement")
    model.optimize()

    if model.SolCount <= 0:
        raise RuntimeError(f"Could not evaluate target objective; solver status={model.Status}")

    return (
        int(round(p_gpu.getValue())),
        int(round(p_exact.getValue())),
        int(round(p_upgrade.getValue())),
        int(p_mig_const),
    )
