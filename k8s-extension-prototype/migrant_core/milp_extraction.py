from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from .state import PROFILE_SIZE
from .templates import PROFILE_ORDER, TEMPLATES


def milp_expand_template_list(
    y_sol: dict[int, int],
    templates: list[tuple[str, tuple[int, int, int, int, int]]] = TEMPLATES,
) -> list[str]:
    out = []
    for t_idx, cnt in sorted(y_sol.items()):
        if int(cnt) <= 0:
            continue
        tpl_name = templates[int(t_idx)][0]
        out.extend([tpl_name] * int(cnt))
    return out


def extract_template_list_from_milp(milp_res: dict[str, Any]) -> list[str]:
    if "chosen_templates" in milp_res and milp_res["chosen_templates"] is not None:
        return list(milp_res["chosen_templates"])
    if "y_sol" in milp_res and milp_res["y_sol"] is not None:
        return milp_expand_template_list(milp_res["y_sol"], TEMPLATES)
    raise ValueError("Cannot extract templates from milp_res")


def _option_df_from_milp(
    milp_res: dict[str, Any],
    feasible_option_df: Any | None,
) -> Any:
    if feasible_option_df is not None:
        return feasible_option_df
    if milp_res.get("effective_option_df") is not None:
        return milp_res["effective_option_df"]
    raise ValueError(
        "feasible_option_df is required when milp_res does not contain effective_option_df"
    )


def extract_instance_demands_from_milp(
    milp_res: dict[str, Any],
    feasible_option_df: Any | None = None,
) -> list[dict[str, Any]]:
    if "x_sol" not in milp_res or milp_res["x_sol"] is None:
        raise ValueError("milp_res does not contain x_sol")

    option_df = _option_df_from_milp(milp_res, feasible_option_df)
    x_sol = {int(opt_idx): int(count) for opt_idx, count in milp_res["x_sol"].items()}
    chosen = option_df[option_df["opt_idx"].isin(list(x_sol.keys()))].copy()

    found_opt_idxs = {int(opt_idx) for opt_idx in chosen["opt_idx"].tolist()}
    missing_opt_idxs = sorted(set(x_sol) - found_opt_idxs)
    if missing_opt_idxs:
        raise ValueError(f"Option dataframe is missing x_sol opt_idx values: {missing_opt_idxs}")

    agg = defaultdict(lambda: {"count": 0, "mu": None})
    for _, row in chosen.iterrows():
        opt_idx = int(row["opt_idx"])
        cnt = int(x_sol.get(opt_idx, 0))
        if cnt <= 0:
            continue
        key = (str(row["workload"]), str(row["profile"]), int(row["batch"]))
        agg[key]["count"] += cnt
        agg[key]["mu"] = float(row["mu"])

    out = []
    for (workload, profile, batch), info in sorted(agg.items()):
        out.append(
            {
                "workload": workload,
                "profile": profile,
                "batch": int(batch),
                "count": int(info["count"]),
                "mu": float(info["mu"]),
            }
        )
    return out


def _arrival_dict_from_alloc(alloc: list[dict[str, Any]]) -> dict[str, float]:
    return {str(row["workload"]): float(row["arrival"]) for row in alloc}


def _arrival_dict_from_milp(
    milp_res: dict[str, Any],
    workload_names: list[str] | tuple[str, ...] | None = None,
    arrival_rate: list[float] | tuple[float, ...] | None = None,
) -> dict[str, float]:
    if milp_res.get("alloc") is not None:
        return _arrival_dict_from_alloc(milp_res["alloc"])

    if milp_res.get("arrival_rate") is not None:
        vec = milp_res["arrival_rate"]
    elif arrival_rate is not None:
        vec = arrival_rate
    else:
        raise ValueError("arrival_rate is required when milp_res does not contain alloc/arrival_rate")

    if workload_names is None:
        raise ValueError("workload_names is required to convert arrival_rate vector to a dict")
    if len(workload_names) != len(vec):
        raise ValueError(
            f"workload_names length ({len(workload_names)}) does not match arrival_rate length ({len(vec)})"
        )
    return {str(workload): float(vec[idx]) for idx, workload in enumerate(workload_names)}


def _profile_need_from_instance_demands(
    instance_demands: list[dict[str, Any]],
) -> dict[str, int]:
    counter = Counter({profile: 0 for profile in PROFILE_ORDER})
    for demand in instance_demands:
        counter[demand["profile"]] += int(demand["count"])
    return dict(counter)


def _expand_demands_with_ids(instance_demands: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    demand_id = 0
    for demand in instance_demands:
        for _ in range(int(demand["count"])):
            out.append(
                {
                    "demand_id": demand_id,
                    "workload": demand["workload"],
                    "profile": demand["profile"],
                    "batch": int(demand["batch"]),
                    "mu": float(demand["mu"]),
                }
            )
            demand_id += 1

    out.sort(
        key=lambda demand: (
            -PROFILE_SIZE[demand["profile"]],
            demand["workload"],
            -demand["batch"],
            demand["demand_id"],
        )
    )
    return out


def _collect_instance_multiset_from_milp(
    milp_res: dict[str, Any],
    feasible_option_df: Any | None = None,
) -> Counter:
    counter = Counter()
    for demand in extract_instance_demands_from_milp(milp_res, feasible_option_df):
        counter[(demand["workload"], demand["profile"], int(demand["batch"]))] += int(
            demand["count"]
        )
    return counter
