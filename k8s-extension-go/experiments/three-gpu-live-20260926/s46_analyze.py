#!/usr/bin/env python3
"""Section 4.6 tables from E1 run directories (data only).

Usage: s46_analyze.py <run_dir> [<run_dir> ...]   (writes <run_dir>/analysis/)

From plans/rNN_terminal_plan.json (R1..R12; R13 cleanup excluded):
  s46_action_durations.csv   per Stage-3 action type x workload: n, median,
      p90, max seconds; `workload_differs` marks types whose per-workload
      medians differ by > 1.5x and > 0.5 s.  activate_instance_route is split
      by what precedes it: after place_instance it waits for the new runtime
      to load (seconds); after verify_batch it only re-activates the route.
  s46_makespan.csv           per round: measured makespan (first action start
      -> last action end), critical-path estimate (longest DAG path, each
      action weighted by the run-wide median of its type x workload), actions
      on that path, ratio measured / estimate, observed DAG width (max
      concurrent actions)
  s46_mig_lock.csv           MIG-mutating actions: same-GPU overlapping pairs
      (must be 0) and cross-GPU overlapping pairs (allowed)
  s46_route_updates.csv      route/batch actions per replica: overlapping
      route updates on one replica (must be 0), request failures other than
      sender pending-bound rejections inside each transition window
"""
from __future__ import annotations

import csv
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

from e1_analyze import load_requests

MIG_TYPES = {"configure_full_template", "configure_partial_profile", "clear_template", "clear_gpu_binding",
             "return_gpu", "register_mig_devices", "apply_slots", "patch_slots"}
ROUTE_TYPES = {"activate_instance_route", "deactivate_instance_route", "patch_batch_config", "apply_batch", "verify_batch"}


def write(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def overlaps(a: dict, b: dict) -> bool:
    return a["relativeStartSeconds"] < b["relativeEndSeconds"] and b["relativeStartSeconds"] < a["relativeEndSeconds"]


def action_type(a: dict, node: dict, status: dict) -> str:
    if a["type"] != "activate_instance_route":
        return a["type"]
    deps = {status[d]["type"] for d in node.get("dependsOn", []) if d in status}
    return a["type"] + (":after_batch" if "verify_batch" in deps else ":after_place")


def analyze(run: Path) -> dict:
    out = run / "analysis"
    out.mkdir(exist_ok=True)
    rounds = {}
    for rnd in range(1, 13):
        plan = json.loads((run / "plans" / f"r{rnd:02d}_terminal_plan.json").read_text())
        status = {a["id"]: a for a in plan["status"]["actionStatuses"]}
        nodes = {n["id"]: n for n in plan["spec"]["actionDag"]["nodes"]}
        rounds[rnd] = (status, nodes)

    # --- per-action durations
    by_type = defaultdict(list)
    by_type_workload = defaultdict(list)
    for status, nodes in rounds.values():
        for i, a in status.items():
            d = float(a.get("durationSeconds") or 0)
            t = action_type(a, nodes.get(i, {}), status)
            by_type[t].append(d)
            by_type_workload[(t, a.get("model") or "-")].append(d)
    median_tw = {k: statistics.median(v) for k, v in by_type_workload.items()}
    median = {t: statistics.median(v) for t, v in by_type.items()}
    differs = {}
    for t in by_type:
        meds = [statistics.median(v) for (tt, _), v in by_type_workload.items() if tt == t and len(v) >= 1]
        differs[t] = len(meds) > 1 and max(meds) > 1.5 * max(min(meds), 1e-9) and max(meds) - min(meds) > 0.5
    rows = []
    for (t, w), v in sorted(by_type_workload.items()):
        v = sorted(v)
        rows.append({"action_type": t, "workload": w, "n": len(v), "median_s": round(statistics.median(v), 3),
                     "p90_s": round(v[int(0.9 * (len(v) - 1))], 3), "max_s": round(v[-1], 3),
                     "type_median_s": round(median[t], 3), "workload_differs": differs[t]})
    write(out / "s46_action_durations.csv", rows)

    # --- makespan vs critical path
    mk_rows = []
    for rnd, (status, nodes) in rounds.items():
        if not status:
            continue
        start = min(a["relativeStartSeconds"] for a in status.values())
        end = max(a["relativeEndSeconds"] for a in status.values())
        memo = {}

        def longest(i):
            if i not in memo:
                deps = [d for d in nodes.get(i, {}).get("dependsOn", []) if d in status]
                best = max((longest(d) for d in deps), key=lambda x: x[0], default=(0.0, []))
                memo[i] = (best[0] + median_tw[(action_type(status[i], nodes.get(i, {}), status), status[i].get("model") or "-")], best[1] + [i])
            return memo[i]
        est, path = max((longest(i) for i in status), key=lambda x: x[0])
        points = sorted([(a["relativeStartSeconds"], 1) for a in status.values()] + [(a["relativeEndSeconds"], -1) for a in status.values()], key=lambda x: (x[0], x[1]))
        width = cur = 0
        for _, delta in points:
            cur += delta
            width = max(width, cur)
        mk_rows.append({"live_round": rnd, "actions": len(status), "measured_makespan_s": round(end - start, 3),
                        "critical_path_estimate_s": round(est, 3), "critical_path_actions": len(path),
                        "measured_over_estimate": round((end - start) / est, 3) if est else None,
                        "dag_width_observed": width,
                        "critical_path": " > ".join(f"{status[i]['type']}[{status[i].get('model') or '-'}]" for i in path)})
    write(out / "s46_makespan.csv", mk_rows)

    # --- MIG per-GPU lock
    lock_rows = []
    for rnd, (status, _) in rounds.items():
        mig = [a for a in status.values() if a["type"] in MIG_TYPES]
        same = cross = 0
        for i, a in enumerate(mig):
            for b in mig[i + 1:]:
                if overlaps(a, b):
                    if a.get("physicalGpuId") == b.get("physicalGpuId"):
                        same += 1
                    else:
                        cross += 1
        lock_rows.append({"live_round": rnd, "mig_actions": len(mig), "same_gpu_overlaps": same, "cross_gpu_overlaps": cross})
    write(out / "s46_mig_lock.csv", lock_rows)

    # --- route updates per replica
    requests = load_requests(run)
    fail = defaultdict(int)
    for r in requests:
        if r.get("phase") == "transition" and r.get("status") not in ("success", "rejected_pending_bound"):
            fail[int(r["live_round"])] += 1
    route_rows = []
    for rnd, (status, nodes) in rounds.items():
        per = defaultdict(list)
        for i, a in status.items():
            if a["type"] in ROUTE_TYPES:
                act = nodes.get(i, {}).get("action", {})
                rid = f"{act.get('physical_gpu_id')}:{act.get('slot')}:{act.get('workload')}"
                per[rid].append(a)
        bad = sum(1 for v in per.values() for x in range(len(v)) for y in v[x + 1:] if overlaps(v[x], y))
        route_rows.append({"live_round": rnd, "route_actions": sum(len(v) for v in per.values()), "replicas_touched": len(per),
                           "same_replica_overlapping_updates": bad, "transition_request_failures": fail.get(rnd, 0)})
    write(out / "s46_route_updates.csv", route_rows)
    return {"run": str(run), "types": len(by_type), "rounds": len(mk_rows)}


if __name__ == "__main__":
    for arg in sys.argv[1:]:
        print(json.dumps(analyze(Path(arg))))
