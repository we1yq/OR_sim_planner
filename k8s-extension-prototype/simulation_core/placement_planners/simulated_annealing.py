from __future__ import annotations

import math
import random
import time
from typing import Any

import numpy as np

from ..milp_solver import build_allocation_from_x
from ..templates import PROFILE_ORDER, TEMPLATE_K, TEMPLATES


NAME = "placement.simulated_annealing"


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
        if not unmet:
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
    return (
        0 if sol["feasible"] else 1,
        sol["gpu_count"] if sol["feasible"] else 10**9,
        sol["total_deficit"],
        sol["total_instances"],
    )


def _rank_phase2(sol: dict[str, Any]) -> tuple[float, ...]:
    return (sol["total_slack"], -sol["total_remaining_slots"], sol["total_instances"])


def _energy_phase1(sol: dict[str, Any]) -> float:
    if sol["feasible"]:
        return 1e6 * float(sol["gpu_count"])
    return 1e12 + 1e8 * float(sol["total_deficit"]) + 1e2 * float(sol["total_instances"])


def _energy_phase2(sol: dict[str, Any]) -> float:
    return (
        1e5 * float(sol["total_slack"])
        - 1e2 * float(sol["total_remaining_slots"])
        + float(sol["total_instances"])
    )


def _normalize_y(y_sol: dict[int, int]) -> dict[int, int]:
    return {int(t): int(c) for t, c in y_sol.items() if int(c) > 0}


def _random_initial_solution(
    feasible_option_df: Any,
    arrival_rate: list[float] | tuple[float, ...],
    n_workloads: int,
    max_gpu_limit: int,
    rng: random.Random,
) -> dict[str, Any]:
    y_sol: dict[int, int] = {}
    last = _evaluate_y(y_sol, feasible_option_df, arrival_rate, n_workloads)
    for _ in range(max_gpu_limit):
        t_idx = rng.randrange(len(TEMPLATES))
        y_sol[t_idx] = y_sol.get(t_idx, 0) + 1
        last = _evaluate_y(y_sol, feasible_option_df, arrival_rate, n_workloads)
        if last["feasible"]:
            return last
    return last


def _neighbor_phase1(current_y: dict[int, int], max_gpu_limit: int, rng: random.Random) -> dict[int, int]:
    y = dict(current_y)
    active = [t for t, c in y.items() if int(c) > 0]
    moves = ["add"]
    if active:
        moves.extend(["remove", "swap", "replace"])
    move = rng.choice(moves)

    if move == "add":
        if sum(y.values()) >= max_gpu_limit:
            move = "replace" if active else "add"
        else:
            t_idx = rng.randrange(len(TEMPLATES))
            y[t_idx] = y.get(t_idx, 0) + 1
            return _normalize_y(y)

    if move == "remove" and active:
        t_rm = rng.choice(active)
        y[t_rm] -= 1
        return _normalize_y(y)

    if move == "swap" and active:
        t_rm = rng.choice(active)
        y[t_rm] -= 1
        t_add = rng.randrange(len(TEMPLATES))
        y[t_add] = y.get(t_add, 0) + 1
        return _normalize_y(y)

    if active:
        t_rm = rng.choice(active)
        candidates = [idx for idx in range(len(TEMPLATES)) if idx != t_rm]
        t_add = rng.choice(candidates)
        y[t_rm] -= 1
        y[t_add] = y.get(t_add, 0) + 1
    return _normalize_y(y)


def _neighbor_phase2(current_y: dict[int, int], fixed_gpu: int, rng: random.Random) -> dict[int, int]:
    y = dict(current_y)
    active = [t for t, c in y.items() if int(c) > 0]
    if not active:
        return y
    t_rm = rng.choice(active)
    candidates = [idx for idx in range(len(TEMPLATES)) if idx != t_rm]
    t_add = rng.choice(candidates)
    y[t_rm] -= 1
    y[t_add] = y.get(t_add, 0) + 1
    y = _normalize_y(y)
    if sum(y.values()) != fixed_gpu:
        return dict(current_y)
    return y


def _accept(delta: float, temp: float, rng: random.Random) -> bool:
    if delta <= 0:
        return True
    if temp <= 0:
        return False
    return rng.random() < math.exp(-delta / temp)


def _phase1_find_min_gpu(
    feasible_option_df: Any,
    arrival_rate: list[float] | tuple[float, ...],
    n_workloads: int,
    max_gpu_limit: int,
    seed: int,
    max_iter: int,
    init_temp: float,
    cooling: float,
    min_temp: float,
) -> dict[str, Any] | None:
    rng = random.Random(seed)
    current = _random_initial_solution(feasible_option_df, arrival_rate, n_workloads, max_gpu_limit, rng)
    best = current
    temp = float(init_temp)
    for _ in range(max_iter):
        cand_y = _neighbor_phase1(current["y_sol"], max_gpu_limit, rng)
        cand = _evaluate_y(cand_y, feasible_option_df, arrival_rate, n_workloads)
        delta = _energy_phase1(cand) - _energy_phase1(current)
        if _accept(delta, temp, rng):
            current = cand
        if _rank_phase1(cand) < _rank_phase1(best):
            best = cand
        temp = max(float(min_temp), temp * float(cooling))
    return best if best.get("feasible") else None


def _phase2_refine_fixed_gpu(
    init_sol: dict[str, Any],
    feasible_option_df: Any,
    arrival_rate: list[float] | tuple[float, ...],
    n_workloads: int,
    seed: int,
    max_iter: int,
    init_temp: float,
    cooling: float,
    min_temp: float,
) -> dict[str, Any]:
    rng = random.Random(seed + 1)
    current = init_sol
    best = init_sol
    fixed_gpu = int(init_sol["gpu_count"])
    temp = float(init_temp)
    for _ in range(max_iter):
        cand_y = _neighbor_phase2(current["y_sol"], fixed_gpu, rng)
        cand = _evaluate_y(cand_y, feasible_option_df, arrival_rate, n_workloads)
        if not cand["feasible"]:
            temp = max(float(min_temp), temp * float(cooling))
            continue
        delta = _energy_phase2(cand) - _energy_phase2(current)
        if _accept(delta, temp, rng):
            current = cand
        if _rank_phase2(cand) < _rank_phase2(best):
            best = cand
        temp = max(float(min_temp), temp * float(cooling))
    return best


def solve(
    feasible_option_df: Any,
    arrival_rate: list[float] | tuple[float, ...],
    n_workloads: int | None = None,
    max_gpu_limit: int = 100,
    max_gpus: int | None = None,
    seed: int = 42,
    phase1_max_iter: int = 300,
    phase2_max_iter: int = 300,
    phase1_init_temp: float = 3000.0,
    phase2_init_temp: float = 1000.0,
    cooling: float = 0.995,
    min_temp: float = 1e-3,
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
            return {
                "method": "SA-2Phase(batch)",
                "planner_module": NAME,
                "feasible": False,
                "elapsed": time.time() - start,
                "status": f"NO_OPTION_FOR_WORKLOAD_{idx}",
                "effective_option_df": feasible_option_df,
            }

    phase1_sol = _phase1_find_min_gpu(
        feasible_option_df,
        arrival_rate,
        n_workloads,
        int(max_gpu_limit),
        int(seed),
        int(phase1_max_iter),
        float(phase1_init_temp),
        float(cooling),
        float(min_temp),
    )
    if phase1_sol is None or not phase1_sol["feasible"]:
        return {
            "method": "SA-2Phase(batch)",
            "planner_module": NAME,
            "feasible": False,
            "elapsed": time.time() - start,
            "status": "PHASE1_FAILED",
            "effective_option_df": feasible_option_df,
        }

    phase2_sol = _phase2_refine_fixed_gpu(
        phase1_sol,
        feasible_option_df,
        arrival_rate,
        n_workloads,
        int(seed),
        int(phase2_max_iter),
        float(phase2_init_temp),
        float(cooling),
        float(min_temp),
    )
    return {
        "method": "SA-2Phase(batch)",
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
        "sa_seed": int(seed),
        "sa_phase1_max_iter": int(phase1_max_iter),
        "sa_phase2_max_iter": int(phase2_max_iter),
    }
