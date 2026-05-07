from __future__ import annotations

import math
import time
from typing import Any

from .milp_extraction import milp_expand_template_list
from .templates import PROFILE_ORDER, TEMPLATE_K, TEMPLATES


def milp_build_K_total(
    y_sol: dict[int, int],
    template_k: list[dict[str, int]] = TEMPLATE_K,
    profile_order: list[str] = PROFILE_ORDER,
) -> dict[str, int]:
    out = {profile: 0 for profile in profile_order}
    for template_idx, count in y_sol.items():
        for profile in profile_order:
            out[profile] += int(count) * int(template_k[int(template_idx)][profile])
    return out


def _profile_size_from_name(profile: str) -> int:
    profile = str(profile)
    if profile == "void":
        return 0
    return int(profile.replace("g", ""))


def prune_dominated_options(feasible_option_df: Any) -> Any:
    df = feasible_option_df.copy()
    keep_mask = [True] * len(df)

    for _, group in df.groupby("w_idx", sort=False):
        rows = group[["opt_idx", "mu", "profile"]].copy()
        rows["size"] = rows["profile"].map(_profile_size_from_name).astype(int)

        idxs = rows.index.tolist()
        for idx in idxs:
            if not keep_mask[idx]:
                continue
            mu_i = float(rows.loc[idx, "mu"])
            size_i = int(rows.loc[idx, "size"])

            dominated = False
            for other_idx in idxs:
                if idx == other_idx:
                    continue
                mu_j = float(rows.loc[other_idx, "mu"])
                size_j = int(rows.loc[other_idx, "size"])

                if (mu_j >= mu_i and size_j <= size_i) and (mu_j > mu_i or size_j < size_i):
                    dominated = True
                    break

            if dominated:
                keep_mask[idx] = False

    return df.loc[keep_mask].copy().reset_index(drop=True)


def compute_elastic_up_by_opt(base_option_df: Any) -> dict[int, float]:
    base_df = base_option_df.copy()
    delta_map = {}

    for _, group in base_df.groupby(["w_idx", "profile"], sort=False):
        group2 = group[["opt_idx", "batch", "mu"]].copy()
        group2["batch"] = group2["batch"].astype(int)
        group2["mu"] = group2["mu"].astype(float)
        group2 = group2.sort_values(["batch", "mu"]).reset_index(drop=True)

        mus = group2["mu"].tolist()
        opt_idxs = group2["opt_idx"].tolist()

        suffix_best = [0.0] * len(group2)
        best = -float("inf")
        for idx in range(len(group2) - 1, -1, -1):
            suffix_best[idx] = best
            best = max(best, mus[idx])

        for idx, opt_idx in enumerate(opt_idxs):
            cur_mu = mus[idx]
            future_best_mu = suffix_best[idx]
            if future_best_mu == -float("inf"):
                delta = 0.0
            else:
                delta = max(0.0, future_best_mu - cur_mu)
            delta_map[int(opt_idx)] = float(delta)

    for opt_idx in base_df["opt_idx"].tolist():
        delta_map.setdefault(int(opt_idx), 0.0)

    return delta_map


def build_allocation_from_x(
    feasible_option_df: Any,
    x_sol: dict[int, int],
    arrival_rate: list[float] | tuple[float, ...],
) -> list[dict[str, Any]]:
    alloc = []

    for workload_idx in range(len(arrival_rate)):
        group = feasible_option_df[feasible_option_df["w_idx"] == workload_idx]

        instances = []
        provided = 0.0

        for _, row in group.iterrows():
            opt_idx = int(row["opt_idx"])
            x_val = int(x_sol.get(opt_idx, 0))

            if x_val <= 0:
                continue

            mu = float(row["mu"])
            provided += x_val * mu

            instances.append(
                {
                    "batch": int(row["batch"]),
                    "profile": row["profile"],
                    "count": int(x_val),
                    "mu": mu,
                }
            )

        arrival = float(arrival_rate[workload_idx])
        alloc.append(
            {
                "workload": group["workload"].iloc[0],
                "arrival": arrival,
                "provided": provided,
                "slack": provided - arrival,
                "instances": instances,
            }
        )

    return alloc


def solve_milp_gurobi_batch_unified(
    feasible_option_df: Any,
    arrival_rate: list[float] | tuple[float, ...],
    templates: list[tuple[str, tuple[int, int, int, int, int]]] = TEMPLATES,
    template_k: list[dict[str, int]] = TEMPLATE_K,
    profile_order: list[str] = PROFILE_ORDER,
    n_workloads: int | None = None,
    time_limit_s: float | None = None,
    mip_gap: float | None = None,
    threads: int | None = None,
    verbose: bool = False,
    apply_option_pruning: bool = True,
    warm_start_res: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        import gurobipy as gp
        from gurobipy import GRB
    except ImportError as exc:
        raise RuntimeError(
            "solve_milp_gurobi_batch_unified requires gurobipy and a valid Gurobi installation"
        ) from exc

    start = time.time()
    if n_workloads is None:
        n_workloads = len(arrival_rate)

    base_df = feasible_option_df.copy()
    elastic_up_map = compute_elastic_up_by_opt(base_df)

    if apply_option_pruning:
        df = prune_dominated_options(base_df).reset_index(drop=True)
    else:
        df = base_df.reset_index(drop=True)

    df["elastic_up"] = df["opt_idx"].map(lambda opt_idx: float(elastic_up_map.get(int(opt_idx), 0.0)))

    opt_rows = list(range(len(df)))
    template_ids = list(range(len(templates)))

    options_by_workload = {workload_idx: [] for workload_idx in range(n_workloads)}
    options_by_profile = {profile: [] for profile in profile_order}

    for row_idx in opt_rows:
        workload_idx = int(df.loc[row_idx, "w_idx"])
        profile = df.loc[row_idx, "profile"]
        options_by_workload[workload_idx].append(row_idx)
        options_by_profile[profile].append(row_idx)

    for workload_idx in range(n_workloads):
        if len(options_by_workload[workload_idx]) == 0:
            return {
                "method": "MILP-Gurobi(batch)",
                "feasible": False,
                "status": f"NO_OPTION_FOR_WORKLOAD_{workload_idx}",
                "elapsed": time.time() - start,
                "effective_option_df": df,
            }

    model = gp.Model("milp_batch_elastic")

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
    for profile in profile_order:
        cap[profile] = model.addVar(vtype=GRB.INTEGER, lb=0, name=f"cap_{profile}")
        model.addConstr(
            cap[profile]
            == gp.quicksum(int(template_k[t][profile]) * y[t] for t in template_ids),
            name=f"def_cap_{profile}",
        )

    ub_gpu_loose = 0
    for workload_idx in range(n_workloads):
        group = df[df["w_idx"] == workload_idx]
        best_mu = float(group["mu"].max())
        ub_gpu_loose += int(math.ceil(float(arrival_rate[workload_idx]) / best_mu)) if best_mu > 0 else 0
    ub_gpu_loose = max(1, ub_gpu_loose)

    x = {}
    for row_idx in opt_rows:
        workload_idx = int(df.loc[row_idx, "w_idx"])
        profile = df.loc[row_idx, "profile"]
        mu = float(df.loc[row_idx, "mu"])

        ub_by_demand = int(math.ceil(float(arrival_rate[workload_idx]) / mu)) + 2 if mu > 0 else 0
        max_profile_per_gpu = max(int(template_k[t][profile]) for t in template_ids)
        ub_by_gpu = ub_gpu_loose * max_profile_per_gpu
        ub = max(1, min(ub_by_demand, ub_by_gpu))

        x[row_idx] = model.addVar(vtype=GRB.INTEGER, lb=0, ub=ub, name=f"x_{row_idx}")

    provided = {}
    slack = {}
    for workload_idx in range(n_workloads):
        provided[workload_idx] = model.addVar(vtype=GRB.CONTINUOUS, lb=0.0, name=f"provided_{workload_idx}")
        slack[workload_idx] = model.addVar(vtype=GRB.CONTINUOUS, lb=0.0, name=f"slack_{workload_idx}")

        expr = gp.quicksum(
            float(df.loc[row_idx, "mu"]) * x[row_idx] for row_idx in options_by_workload[workload_idx]
        )
        model.addConstr(provided[workload_idx] == expr, name=f"def_provided_{workload_idx}")
        model.addConstr(provided[workload_idx] >= float(arrival_rate[workload_idx]), name=f"demand_{workload_idx}")
        model.addConstr(
            slack[workload_idx] == provided[workload_idx] - float(arrival_rate[workload_idx]),
            name=f"def_slack_{workload_idx}",
        )

    for profile in profile_order:
        model.addConstr(
            gp.quicksum(x[row_idx] for row_idx in options_by_profile[profile]) <= cap[profile],
            name=f"profile_cap_{profile}",
        )

    total_slack = model.addVar(vtype=GRB.CONTINUOUS, lb=0.0, name="total_slack")
    model.addConstr(
        total_slack == gp.quicksum(slack[workload_idx] for workload_idx in range(n_workloads)),
        name="def_total_slack",
    )

    elastic_up = {row_idx: float(df.loc[row_idx, "elastic_up"]) for row_idx in opt_rows}
    total_elastic_slack = model.addVar(vtype=GRB.CONTINUOUS, lb=0.0, name="total_elastic_slack")
    model.addConstr(
        total_elastic_slack == gp.quicksum(elastic_up[row_idx] * x[row_idx] for row_idx in opt_rows),
        name="def_total_elastic_slack",
    )

    total_instances = model.addVar(vtype=GRB.INTEGER, lb=0, name="total_instances")
    model.addConstr(
        total_instances == gp.quicksum(x[row_idx] for row_idx in opt_rows),
        name="def_total_instances",
    )

    remaining = {}
    for profile in profile_order:
        remaining[profile] = model.addVar(vtype=GRB.INTEGER, lb=0, name=f"remaining_{profile}")
        model.addConstr(
            remaining[profile]
            == cap[profile] - gp.quicksum(x[row_idx] for row_idx in options_by_profile[profile]),
            name=f"def_remaining_{profile}",
        )

    total_remaining_slots = model.addVar(vtype=GRB.INTEGER, lb=0, name="total_remaining_slots")
    model.addConstr(
        total_remaining_slots
        == gp.quicksum(_profile_size_from_name(profile) * remaining[profile] for profile in profile_order),
        name="def_total_remaining_slots",
    )

    model.setObjectiveN(total_gpu, index=0, priority=3, weight=1.0, name="obj_gpu")
    model.setObjectiveN(total_elastic_slack, index=1, priority=2, weight=-1.0, name="obj_elastic_slack")
    model.setObjectiveN(total_remaining_slots, index=2, priority=1, weight=-1.0, name="obj_remaining")

    if warm_start_res is not None:
        prev_x = dict(warm_start_res.get("x_sol", {}) or {})
        prev_y = dict(warm_start_res.get("y_sol", {}) or {})
        global_to_local = {int(df.loc[row_idx, "opt_idx"]): row_idx for row_idx in opt_rows}

        for template_idx in template_ids:
            if template_idx in prev_y:
                y[template_idx].Start = float(prev_y[template_idx])

        for global_opt_idx, val in prev_x.items():
            if global_opt_idx in global_to_local:
                row_idx = global_to_local[global_opt_idx]
                x[row_idx].Start = float(val)

    model.optimize()

    elapsed = time.time() - start
    status_str = {
        GRB.OPTIMAL: "OPTIMAL",
        GRB.TIME_LIMIT: "TIME_LIMIT",
        GRB.SUBOPTIMAL: "SUBOPTIMAL",
        GRB.INFEASIBLE: "INFEASIBLE",
    }.get(model.Status, str(model.Status))

    if model.SolCount == 0:
        return {
            "method": "MILP-Gurobi(batch)",
            "feasible": False,
            "status": status_str,
            "elapsed": elapsed,
            "effective_option_df": df,
        }

    y_sol = {}
    for template_idx in template_ids:
        val = int(round(y[template_idx].X))
        if val > 0:
            y_sol[template_idx] = val

    x_sol = {}
    for row_idx in opt_rows:
        val = int(round(x[row_idx].X))
        if val > 0:
            global_opt_idx = int(df.loc[row_idx, "opt_idx"])
            x_sol[global_opt_idx] = val

    k_total = milp_build_K_total(y_sol, template_k, profile_order)
    chosen_templates = milp_expand_template_list(y_sol, templates)

    alloc = build_allocation_from_x(
        feasible_option_df=base_df,
        x_sol=x_sol,
        arrival_rate=arrival_rate,
    )
    provided_rate = [float(row["provided"]) for row in alloc]
    used_profile_types = sum(1 for profile in profile_order if k_total[profile] > 0)

    return {
        "method": "MILP-Gurobi(batch)",
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
        "provided_rate": provided_rate,
        "total_slack": float(total_slack.X),
        "total_elastic_slack": float(total_elastic_slack.X),
        "total_remaining_slots": int(round(total_remaining_slots.X)),
        "total_instances": int(round(total_instances.X)),
        "used_profile_types": used_profile_types,
        "effective_option_df": df,
        "arrival_rate": list(arrival_rate),
    }
