from __future__ import annotations

import time
from typing import Any

import numpy as np

from ..milp_solver import build_allocation_from_x
from ..templates import PROFILE_ORDER, TEMPLATE_K, TEMPLATES


NAME = "placement.greedy_two_phase"


def _expand_template_list(y_sol: dict[int, int]) -> list[str]:
    out = []
    for t_idx, count in sorted(y_sol.items()):
        out.extend([TEMPLATES[int(t_idx)][0]] * int(count))
    return out


def _build_k_total(y_sol: dict[int, int]) -> dict[str, int]:
    k_total = {profile: 0 for profile in PROFILE_ORDER}
    for t_idx, count in y_sol.items():
        for profile in PROFILE_ORDER:
            k_total[profile] += int(count) * int(TEMPLATE_K[int(t_idx)][profile])
    return k_total


def _allocate_given_capacity(
    feasible_option_df: Any,
    arrival_rate: list[float] | tuple[float, ...],
    k_total: dict[str, int],
    n_workloads: int,
    eps: float = 1e-12,
) -> dict[str, Any]:
    df = feasible_option_df.copy().reset_index(drop=True)
    local_to_global = {row_idx: int(df.loc[row_idx, "opt_idx"]) for row_idx in range(len(df))}
    residual = np.array(arrival_rate, dtype=float).copy()
    remain_slots = {profile: int(k_total[profile]) for profile in PROFILE_ORDER}
    x_local = {row_idx: 0 for row_idx in range(len(df))}
    provided = np.zeros(n_workloads, dtype=float)
    rows_by_workload = {idx: [] for idx in range(n_workloads)}
    for row_idx in range(len(df)):
        rows_by_workload[int(df.loc[row_idx, "w_idx"])].append(row_idx)

    while True:
        unmet = [idx for idx in range(n_workloads) if residual[idx] > eps]
        if len(unmet) == 0:
            break
        target_idx = max(
            unmet,
            key=lambda idx: (residual[idx] / max(float(arrival_rate[idx]), eps), residual[idx]),
        )
        best_row = None
        best_key = None
        for row_idx in rows_by_workload[target_idx]:
            profile = str(df.loc[row_idx, "profile"])
            mu = float(df.loc[row_idx, "mu"])
            batch = int(df.loc[row_idx, "batch"])
            if remain_slots[profile] <= 0:
                continue
            cover = min(mu, residual[target_idx])
            overshoot = max(0.0, mu - residual[target_idx])
            key = (overshoot, -cover, -mu, batch, int(df.loc[row_idx, "opt_idx"]))
            if best_key is None or key < best_key:
                best_key = key
                best_row = row_idx

        if best_row is None:
            global_best_row = None
            global_best_key = None
            for idx in unmet:
                for row_idx in rows_by_workload[idx]:
                    profile = str(df.loc[row_idx, "profile"])
                    mu = float(df.loc[row_idx, "mu"])
                    batch = int(df.loc[row_idx, "batch"])
                    if remain_slots[profile] <= 0:
                        continue
                    cover = min(mu, residual[idx])
                    overshoot = max(0.0, mu - residual[idx])
                    key = (overshoot, -cover, -mu, batch, int(df.loc[row_idx, "opt_idx"]))
                    if global_best_key is None or key < global_best_key:
                        global_best_key = key
                        global_best_row = row_idx
            if global_best_row is None:
                break
            best_row = global_best_row
            target_idx = int(df.loc[best_row, "w_idx"])

        profile = str(df.loc[best_row, "profile"])
        mu = float(df.loc[best_row, "mu"])
        x_local[best_row] += 1
        remain_slots[profile] -= 1
        provided[target_idx] += mu
        residual[target_idx] = max(0.0, float(arrival_rate[target_idx]) - provided[target_idx])

    x_sol = {local_to_global[row_idx]: int(count) for row_idx, count in x_local.items() if int(count) > 0}
    alloc = build_allocation_from_x(feasible_option_df, x_sol, arrival_rate)
    provided_rate = np.array([row["provided"] for row in alloc], dtype=float)
    feasible = all(row["provided"] + 1e-9 >= row["arrival"] for row in alloc)
    return {
        "feasible": feasible,
        "x_sol": x_sol,
        "alloc": alloc,
        "provided_rate": provided_rate,
        "total_slack": float(sum(row["slack"] for row in alloc)),
        "total_deficit": float(sum(max(0.0, row["arrival"] - row["provided"]) for row in alloc)),
        "total_instances": int(sum(x_sol.values())),
        "total_remaining_slots": int(sum(remain_slots[profile] for profile in PROFILE_ORDER)),
        "used_profile_types": sum(1 for profile in PROFILE_ORDER if k_total[profile] > 0),
    }


def _evaluate_y(
    y_sol: dict[int, int],
    feasible_option_df: Any,
    arrival_rate: list[float] | tuple[float, ...],
    n_workloads: int,
) -> dict[str, Any]:
    k_total = _build_k_total(y_sol)
    alloc_res = _allocate_given_capacity(feasible_option_df, arrival_rate, k_total, n_workloads)
    return {
        "y_sol": dict(y_sol),
        "gpu_count": int(sum(y_sol.values())),
        "K_total": k_total,
        "chosen_templates": _expand_template_list(y_sol),
        **alloc_res,
    }


def _rank_phase1(sol: dict[str, Any]) -> tuple[float, ...]:
    return (0 if sol["feasible"] else 1, sol["gpu_count"] if sol["feasible"] else 10**9, sol["total_deficit"], sol["total_instances"])


def _rank_phase2(sol: dict[str, Any]) -> tuple[float, ...]:
    return (sol["total_slack"], -sol["total_remaining_slots"], sol["total_instances"])


def _phase1_find_min_gpu(
    feasible_option_df: Any,
    arrival_rate: list[float] | tuple[float, ...],
    n_workloads: int,
    max_gpu_limit: int,
    verbose: bool = False,
) -> dict[str, Any] | None:
    y_sol: dict[int, int] = {}
    for step in range(max_gpu_limit):
        best_candidate = None
        for t_idx in range(len(TEMPLATES)):
            trial_y = dict(y_sol)
            trial_y[t_idx] = trial_y.get(t_idx, 0) + 1
            sol = _evaluate_y(trial_y, feasible_option_df, arrival_rate, n_workloads)
            key = _rank_phase1(sol)
            if best_candidate is None or key < best_candidate["key"]:
                best_candidate = {"sol": sol, "key": key, "t_idx": t_idx}
        if best_candidate is None:
            return None
        y_sol = dict(best_candidate["sol"]["y_sol"])
        if verbose:
            print(f"[Greedy2-P1] step={step + 1}, add={TEMPLATES[best_candidate['t_idx']][0]}, feasible={best_candidate['sol']['feasible']}, deficit={best_candidate['sol']['total_deficit']:.6f}, gpu={best_candidate['sol']['gpu_count']}")
        if best_candidate["sol"]["feasible"]:
            return best_candidate["sol"]
    return None


def _phase2_refine_fixed_gpu(
    init_sol: dict[str, Any],
    feasible_option_df: Any,
    arrival_rate: list[float] | tuple[float, ...],
    n_workloads: int,
    verbose: bool = False,
) -> dict[str, Any]:
    current = init_sol
    improved = True
    while improved:
        improved = False
        active = [t_idx for t_idx, count in current["y_sol"].items() if int(count) > 0]
        best_neighbor = None
        for t_rm in active:
            for t_add in range(len(TEMPLATES)):
                if t_add == t_rm:
                    continue
                trial_y = dict(current["y_sol"])
                trial_y[t_rm] -= 1
                if trial_y[t_rm] <= 0:
                    del trial_y[t_rm]
                trial_y[t_add] = trial_y.get(t_add, 0) + 1
                if sum(trial_y.values()) != current["gpu_count"]:
                    continue
                sol = _evaluate_y(trial_y, feasible_option_df, arrival_rate, n_workloads)
                if not sol["feasible"]:
                    continue
                key = _rank_phase2(sol)
                cur_key = _rank_phase2(current)
                if key < cur_key:
                    if best_neighbor is None or key < best_neighbor["key"]:
                        best_neighbor = {"sol": sol, "key": key, "move": (t_rm, t_add)}
        if best_neighbor is not None:
            current = best_neighbor["sol"]
            improved = True
            if verbose:
                t_rm, t_add = best_neighbor["move"]
                print(f"[Greedy2-P2] replace {TEMPLATES[t_rm][0]} -> {TEMPLATES[t_add][0]}, slack={current['total_slack']:.6f}, remaining={current['total_remaining_slots']}, instances={current['total_instances']}")
    return current


def solve(
    feasible_option_df: Any,
    arrival_rate: list[float] | tuple[float, ...],
    n_workloads: int | None = None,
    max_gpu_limit: int = 100,
    max_gpus: int | None = None,
    verbose: bool = False,
    **_: Any,
) -> dict[str, Any]:
    start = time.time()
    if n_workloads is None:
        n_workloads = len(arrival_rate)
    if max_gpus is not None:
        max_gpu_limit = int(max_gpus)
    df = feasible_option_df.copy()
    for idx in range(n_workloads):
        if not (df["w_idx"] == idx).any():
            return {"method": "Greedy-2Phase(batch)", "planner_module": NAME, "feasible": False, "elapsed": time.time() - start, "status": f"NO_OPTION_FOR_WORKLOAD_{idx}", "effective_option_df": feasible_option_df}
    phase1_sol = _phase1_find_min_gpu(feasible_option_df, arrival_rate, n_workloads, int(max_gpu_limit), verbose=verbose)
    if phase1_sol is None or not phase1_sol["feasible"]:
        return {"method": "Greedy-2Phase(batch)", "planner_module": NAME, "feasible": False, "elapsed": time.time() - start, "status": "PHASE1_FAILED", "effective_option_df": feasible_option_df}
    phase2_sol = _phase2_refine_fixed_gpu(phase1_sol, feasible_option_df, arrival_rate, n_workloads, verbose=verbose)
    return {
        "method": "Greedy-2Phase(batch)",
        "planner_module": NAME,
        "feasible": True,
        "elapsed": time.time() - start,
        "status": "OK",
        "gpu_count": phase2_sol["gpu_count"],
        "objective": phase2_sol["gpu_count"],
        "chosen_templates": phase2_sol["chosen_templates"],
        "K_total": phase2_sol["K_total"],
        "x_sol": phase2_sol["x_sol"],
        "y_sol": phase2_sol["y_sol"],
        "alloc": phase2_sol["alloc"],
        "provided_rate": phase2_sol["provided_rate"],
        "total_slack": phase2_sol["total_slack"],
        "total_deficit": phase2_sol["total_deficit"],
        "total_elastic_slack": 0.0,
        "total_remaining_slots": phase2_sol["total_remaining_slots"],
        "total_instances": phase2_sol["total_instances"],
        "used_profile_types": phase2_sol["used_profile_types"],
        "effective_option_df": feasible_option_df,
        "arrival_rate": list(arrival_rate),
    }
