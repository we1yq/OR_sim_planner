from __future__ import annotations

import math
import time
from typing import Any

from ..milp_solver import build_allocation_from_x, milp_build_K_total
from ..templates import PROFILE_ORDER, TEMPLATE_K, TEMPLATES


NAME = "placement.milp_original"


def solve(
    feasible_option_df: Any,
    arrival_rate: list[float] | tuple[float, ...],
    n_workloads: int | None = None,
    time_limit_s: float | None = None,
    mip_gap: float | None = None,
    threads: int | None = None,
    verbose: bool = False,
    **_: Any,
) -> dict[str, Any]:
    """Notebook MILP baseline: min GPU, min slack, max remaining slots.

    This intentionally does not use option pruning, elastic-up objective, warm
    starts, or previous-state information.
    """
    try:
        import gurobipy as gp
        from gurobipy import GRB
    except ImportError as exc:
        raise RuntimeError("milp_original requires gurobipy and a valid Gurobi installation") from exc

    start = time.time()
    if n_workloads is None:
        n_workloads = len(arrival_rate)

    df = feasible_option_df.copy().reset_index(drop=True)
    opt_rows = list(range(len(df)))
    template_ids = list(range(len(TEMPLATES)))
    options_by_workload = {idx: [] for idx in range(n_workloads)}
    options_by_profile = {profile: [] for profile in PROFILE_ORDER}

    for row_idx in opt_rows:
        w_idx = int(df.loc[row_idx, "w_idx"])
        profile = str(df.loc[row_idx, "profile"])
        options_by_workload[w_idx].append(row_idx)
        options_by_profile[profile].append(row_idx)

    for idx in range(n_workloads):
        if len(options_by_workload[idx]) == 0:
            return {
                "method": "MILP-Gurobi-original(batch)",
                "planner_module": NAME,
                "feasible": False,
                "status": f"NO_OPTION_FOR_WORKLOAD_{idx}",
                "elapsed": time.time() - start,
                "effective_option_df": df,
            }

    model = gp.Model("milp_batch_original")
    if not verbose:
        model.Params.OutputFlag = 0
    if time_limit_s is not None:
        model.Params.TimeLimit = float(time_limit_s)
    if mip_gap is not None:
        model.Params.MIPGap = float(mip_gap)
    if threads is not None:
        model.Params.Threads = int(threads)

    y = model.addVars(template_ids, vtype=GRB.INTEGER, lb=0, name="y")
    total_gpu = model.addVar(vtype=GRB.INTEGER, lb=0, name="total_gpu")
    model.addConstr(total_gpu == gp.quicksum(y[t] for t in template_ids), name="def_total_gpu")

    cap = {}
    for profile in PROFILE_ORDER:
        cap[profile] = model.addVar(vtype=GRB.INTEGER, lb=0, name=f"cap_{profile}")
        model.addConstr(
            cap[profile] == gp.quicksum(int(TEMPLATE_K[t][profile]) * y[t] for t in template_ids),
            name=f"def_cap_{profile}",
        )

    ub_gpu_loose = 0
    for idx in range(n_workloads):
        group = df[df["w_idx"] == idx]
        best_mu = float(group["mu"].max())
        ub_gpu_loose += int(math.ceil(float(arrival_rate[idx]) / best_mu)) if best_mu > 0 else 0
    ub_gpu_loose = max(1, ub_gpu_loose)

    x = {}
    for row_idx in opt_rows:
        w_idx = int(df.loc[row_idx, "w_idx"])
        profile = str(df.loc[row_idx, "profile"])
        mu = float(df.loc[row_idx, "mu"])
        ub_by_demand = int(math.ceil(float(arrival_rate[w_idx]) / mu)) + 5 if mu > 0 else 0
        max_profile_per_gpu = max(int(TEMPLATE_K[t][profile]) for t in template_ids)
        ub_by_gpu = ub_gpu_loose * max_profile_per_gpu
        x[row_idx] = model.addVar(
            vtype=GRB.INTEGER,
            lb=0,
            ub=max(1, min(ub_by_demand, ub_by_gpu)),
            name=f"x_{row_idx}",
        )

    provided = {}
    slack = {}
    for idx in range(n_workloads):
        provided[idx] = model.addVar(vtype=GRB.CONTINUOUS, lb=0.0, name=f"provided_{idx}")
        slack[idx] = model.addVar(vtype=GRB.CONTINUOUS, lb=0.0, name=f"slack_{idx}")
        model.addConstr(
            provided[idx] == gp.quicksum(float(df.loc[row_idx, "mu"]) * x[row_idx] for row_idx in options_by_workload[idx]),
            name=f"def_provided_{idx}",
        )
        model.addConstr(provided[idx] >= float(arrival_rate[idx]), name=f"demand_{idx}")
        model.addConstr(slack[idx] == provided[idx] - float(arrival_rate[idx]), name=f"def_slack_{idx}")

    for profile in PROFILE_ORDER:
        model.addConstr(
            gp.quicksum(x[row_idx] for row_idx in options_by_profile[profile]) <= cap[profile],
            name=f"profile_cap_{profile}",
        )

    total_slack = model.addVar(vtype=GRB.CONTINUOUS, lb=0.0, name="total_slack")
    model.addConstr(
        total_slack == gp.quicksum(slack[idx] for idx in range(n_workloads)),
        name="def_total_slack",
    )
    total_instances = model.addVar(vtype=GRB.INTEGER, lb=0, name="total_instances")
    model.addConstr(total_instances == gp.quicksum(x[row_idx] for row_idx in opt_rows), name="def_total_instances")
    total_remaining_slots = model.addVar(vtype=GRB.INTEGER, lb=0, name="total_remaining_slots")
    model.addConstr(
        total_remaining_slots == gp.quicksum(cap[p] for p in PROFILE_ORDER) - gp.quicksum(x[row_idx] for row_idx in opt_rows),
        name="def_total_remaining_slots",
    )

    model.ModelSense = GRB.MINIMIZE
    model.setObjectiveN(total_gpu, index=0, priority=3, weight=1.0, name="obj_gpu")
    model.setObjectiveN(total_slack, index=1, priority=2, weight=1.0, name="obj_slack")
    model.setObjectiveN(total_remaining_slots, index=2, priority=1, weight=-1.0, name="obj_remaining")
    model.optimize()

    elapsed = time.time() - start
    status_str = {
        GRB.OPTIMAL: "OPTIMAL",
        GRB.TIME_LIMIT: "TIME_LIMIT",
        GRB.INFEASIBLE: "INFEASIBLE",
        GRB.INF_OR_UNBD: "INF_OR_UNBD",
        GRB.UNBOUNDED: "UNBOUNDED",
        GRB.INTERRUPTED: "INTERRUPTED",
    }.get(model.Status, str(model.Status))
    if model.SolCount == 0:
        return {
            "method": "MILP-Gurobi-original(batch)",
            "planner_module": NAME,
            "feasible": False,
            "status": status_str,
            "elapsed": elapsed,
            "effective_option_df": df,
        }

    y_sol = {int(t): int(round(y[t].X)) for t in template_ids if int(round(y[t].X)) > 0}
    x_sol = {
        int(df.loc[row_idx, "opt_idx"]): int(round(x[row_idx].X))
        for row_idx in opt_rows
        if int(round(x[row_idx].X)) > 0
    }
    k_total = milp_build_K_total(y_sol, TEMPLATE_K, PROFILE_ORDER)
    chosen_templates = []
    for t_idx, count in sorted(y_sol.items()):
        chosen_templates.extend([TEMPLATES[int(t_idx)][0]] * int(count))
    alloc = build_allocation_from_x(feasible_option_df, x_sol, arrival_rate)

    return {
        "method": "MILP-Gurobi-original(batch)",
        "planner_module": NAME,
        "feasible": True,
        "status": status_str,
        "elapsed": elapsed,
        "gpu_count": int(round(total_gpu.X)),
        "objective": int(round(total_gpu.X)),
        "chosen_templates": chosen_templates,
        "K_total": k_total,
        "x_sol": x_sol,
        "y_sol": y_sol,
        "alloc": alloc,
        "provided_rate": [float(row["provided"]) for row in alloc],
        "total_slack": float(total_slack.X),
        "total_elastic_slack": 0.0,
        "total_remaining_slots": int(round(total_remaining_slots.X)),
        "total_instances": int(round(total_instances.X)),
        "used_profile_types": sum(1 for p in PROFILE_ORDER if k_total[p] > 0),
        "effective_option_df": feasible_option_df,
        "arrival_rate": list(arrival_rate),
    }
