"""Capacity of materialized instances, distinct from logical option capacity."""
import math


def assign_physical_capacity(state, feasible_option_df):
    if feasible_option_df is None:
        raise ValueError("Physical capacity assignment requires the serving-option catalog")
    lookup = {}
    for row in feasible_option_df.to_dict("records"):
        key = (str(row["workload"]), str(row["profile"]), int(row["batch"]))
        if key in lookup:
            raise ValueError(f"Ambiguous physical capacity catalog key: {key}")
        value = float(row["mu"])
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"Invalid physical capacity for {key}: {value}")
        lookup[key] = value
    updates = []
    logical = []
    for gpu in state.real_gpus():
        for inst in gpu.instances:
            if inst.workload is None:
                continue
            key = (str(inst.workload), str(inst.profile), int(inst.batch))
            if key not in lookup:
                raise ValueError(f"Missing physical capacity catalog key: {key}")
            physical_mu = lookup[key]
            logical.append({"gpu_id": int(gpu.gpu_id), "slot": [inst.start, inst.end, inst.profile],
                            "workload": inst.workload, "batch": inst.batch, "logical_mu": float(inst.mu)})
            updates.append((inst, physical_mu))
    for inst, physical_mu in updates:
        inst.mu = physical_mu
    state.metadata["logical_capacity_assignments"] = logical
    state.metadata["capacity_basis"] = "physical_profile_catalog"
