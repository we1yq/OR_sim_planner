#!/usr/bin/env python3
"""Offline capacity-headroom sweep (no Kubernetes).

For each h, plan R1..R12 in sequence with the repo's planner-engine (Stage 1-3,
SliceWise), starting from an empty cluster and feeding each round's
canonicalNextState into the next, with the same planning input the live
runner sends (sourceArrival = previous target, transitionDemandPolicy=min,
forceReplan) plus capacityHeadroom=h, gpuBudget=3 and conservative3gMu on or
off.  The planner provisions for (1 + h') x demand, where h' is the largest
value <= h whose plan fits 3 GPUs (best-effort headroom).

Usage: headroom_offline_sweep.py [h ...]   (default 0 0.05 0.10 0.15 0.20 0.25)
Writes analysis/headroom_offline_sweep.csv next to this script.
"""
from __future__ import annotations

import contextlib
import csv
import importlib.util
import io
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
APP = HERE.parents[1] / "planner-engine" / "app"
sys.path.insert(0, str(APP))

import server  # noqa: E402
from planning.k8s_adapter import cluster_state_from_status, plan_scenario_as_migplan_status  # noqa: E402
from scenario_loader import load_planning_scenario  # noqa: E402

spec = importlib.util.spec_from_file_location("live_runner_sweep", HERE / "live_runner_20260926.py")
runner = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = runner
spec.loader.exec_module(runner)

GPU_BUDGET = 3


def sweep(h: float, rounds: list[dict[str, float]], conservative: bool = False) -> list[dict]:
    base = load_planning_scenario(str(APP / "mock" / "scenarios" / "real8gpu.yaml"))
    rows, prev_state, source = [], None, {k: 0.0 for k in runner.WORKLOAD_KEYS}
    for i, target in enumerate(rounds, start=1):
        scenario = server.apply_planning_input(base, {
            "sourceArrival": source, "targetArrival": target, "transitionDemandPolicy": "min",
            "stage3Variant": "slicewise", "forceReplan": True, "planner": "ours", "capacityHeadroom": h,
            "gpuBudget": GPU_BUDGET, "conservative3gMu": conservative,
            "epoch": f"sweep-h{h:g}-r{i:02d}",
        })
        with contextlib.redirect_stdout(io.StringIO()):
            try:
                status = plan_scenario_as_migplan_status(scenario, source_state_override=prev_state)["status"]
                error = ""
            except Exception as exc:  # planner failure (e.g. Stage 2 infeasible)
                status, error = None, f"{type(exc).__name__}: {exc}"[:200]
        row = {"h": h, "conservative3gMu": conservative, "live_round": i, "feasible": False, "gpu_count": None, "fits_3_gpus": False, "error": error,
               "phase": status.get("phase") if status else None}
        if status and status.get("targetState"):
            milp = status.get("milp") or {}
            gpus = status["targetState"].get("gpus", [])
            capacity = {k: 0.0 for k in runner.WORKLOAD_KEYS}
            for gpu in gpus:
                for inst in gpu.get("instances", []):
                    if inst.get("workload") in capacity:
                        capacity[inst["workload"]] += float(inst.get("mu") or 0.0)
            used = [g for g in gpus if g.get("instances")]
            row.update({"feasible": True, "gpu_count": milp.get("gpuCount", len(used)), "actions": len(status.get("actions") or []),
                        "h_effective": milp.get("capacityHeadroomEffective")})
            row["fits_3_gpus"] = int(row["gpu_count"] or 0) <= GPU_BUDGET
            for w in runner.WORKLOAD_KEYS:
                row[f"cap_over_demand_{w}"] = round(capacity[w] / target[w], 3) if target[w] > 0 else None
            prev_state = cluster_state_from_status({"status": status}, "canonicalNextState")
        else:
            row["error"] = row["error"] or (status or {}).get("message", "no target state")
            rows.append(row)
            break  # the chain cannot continue without a next state
        rows.append(row)
        source = target
    return rows


def main() -> int:
    hs = [float(x) for x in sys.argv[1:]] or [0.0, 0.05, 0.10, 0.15, 0.20, 0.25]
    demand, _, _ = runner.load_frozen_inputs(catalog_file="catalog_newest.csv")
    rounds = [runner.demand_rates(demand[i]) for i in range(12)]
    all_rows = []
    for conservative in (False, True):
        for h in hs:
            rows = sweep(h, rounds, conservative)
            all_rows += rows
            ok = sum(1 for r in rows if r["feasible"] and r["fits_3_gpus"])
            gpus = " ".join(str(r["gpu_count"]) if r["feasible"] else "x" for r in rows)
            heff = " ".join(f"{100 * (r.get('h_effective') or 0):.1f}" if r["feasible"] else "x" for r in rows)
            worst = min((v for r in rows if r["feasible"] for k, v in r.items() if k.startswith("cap_over_demand_") and v is not None), default=None)
            print(f"cons3g={'on ' if conservative else 'off'} h={h:4.2f}: {ok}/12 fit; GPUs {gpus}; h_eff% {heff}; min cap/demand {worst}"
                  + (f"; stopped R{rows[-1]['live_round']}: {rows[-1]['error']}" if not rows[-1]["feasible"] else ""), flush=True)
    out = HERE / "analysis"
    out.mkdir(exist_ok=True)
    fields = sorted({k for r in all_rows for k in r}, key=lambda k: (not k in ("h", "conservative3gMu", "h_effective", "live_round", "feasible", "gpu_count", "fits_3_gpus", "phase", "actions", "error"), k))
    with (out / "headroom_offline_sweep.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(all_rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
