#!/usr/bin/env python3
"""Strict post-run audit for a completed no-traffic makespan experiment."""
from __future__ import annotations
import argparse, csv, json
from pathlib import Path

FIELDS = ("runtimeId", "gpu", "profile", "slotResource", "batchSize")

def load(p: Path):
    with p.open() as f: return json.load(f)

def main() -> int:
    ap = argparse.ArgumentParser(); ap.add_argument("run_dir", type=Path); a = ap.parse_args()
    root = a.run_dir; rows = list(csv.DictReader((root / "round_summary.csv").open()))
    report = {"runId": rows[0]["run_id"] if rows else None, "rounds": [], "errors": [], "sameGpuDistinctSlotPlaceOverlaps": []}
    for row in rows:
        r = int(row["live_round"]); term = load(root / "plans" / f"r{r:02d}_terminal_plan.json")
        snap = load(root / "snapshots" / f"r{r:02d}_after_transition.json")
        wanted = {x["runtimeId"]: x for x in term["spec"]["targetAllocationPlan"]["desiredRuntimes"]}
        actual = {x.get("runtimeId", x.get("runtime.runtimeId")): x for x in snap["routes"].get("routes", [])}
        mismatches = []
        if set(wanted) != set(actual): mismatches.append({"wantedOnly": sorted(set(wanted)-set(actual)), "actualOnly": sorted(set(actual)-set(wanted))})
        for ident, want in wanted.items():
            got = actual.get(ident)
            if not got: continue
            observed = {"runtimeId": ident, "gpu": got.get("gpu"), "profile": got.get("profile"), "slotResource": got.get("slotResource"), "batchSize": got.get("runtime.batchSize", got.get("batchSize"))}
            expected = {k: want.get(k) for k in FIELDS}
            if observed != expected: mismatches.append({"runtimeId": ident, "expected": expected, "observed": observed})
        sts = term.get("status", {}).get("actionStatuses", [])
        bad = [x for x in sts if x.get("status") != "completed"]
        final = term.get("status", {}).get("transitionExecution", {}).get("metrics", {}).get("finalValidation", {})
        nodes = {x["id"]: x.get("action", {}) for x in term["spec"].get("actionDag", {}).get("nodes", [])}
        places = []
        for s in sts:
            act = nodes.get(s.get("id"), {})
            if act.get("type") == "place_instance" and s.get("status") == "completed":
                places.append((act.get("physical_gpu_id"), str(act.get("slot")), s.get("relativeStartSeconds"), s.get("relativeEndSeconds"), s.get("id")))
        overlaps = []
        for i, x in enumerate(places):
            for y in places[i+1:]:
                if x[0] == y[0] and x[1] != y[1] and all(isinstance(z, (int,float)) for z in (x[2],x[3],y[2],y[3])):
                    amount = min(x[3],y[3])-max(x[2],y[2])
                    if amount > 0: overlaps.append({"gpu":x[0],"slotA":x[1],"slotB":y[1],"seconds":amount,"actionA":x[4],"actionB":y[4]})
        report["sameGpuDistinctSlotPlaceOverlaps"].extend([{"round":r, **x} for x in overlaps])
        item = {"round": r, "targetRuntimeCount":len(wanted), "actualRouteCount":len(actual), "targetMatchesActual":not mismatches, "mismatches":mismatches, "allActionsCompleted":not bad, "nonCompletedActions":bad, "executorFinalValidationOk":final.get("ok") is True, "makespanSeconds":float(row["makespan_seconds"]), "actionCount":int(row["action_count"]), "sameGpuDistinctSlotPlaceOverlapCount":len(overlaps)}
        report["rounds"].append(item)
        if not (item["targetMatchesActual"] and item["allActionsCompleted"] and item["executorFinalValidationOk"]): report["errors"].append({"round":r,"detail":item})
    report["ok"] = not report["errors"] and len(rows) == 12
    (root / "strict_runtime_audit.json").write_text(json.dumps(report, indent=2, sort_keys=True)+"\n")
    (root / "strict_runtime_audit.md").write_text("# Strict runtime audit\n\n" + f"- result: {'PASS' if report['ok'] else 'FAIL'}\n- rounds: {len(rows)}\n- same-GPU/distinct-slot placement overlaps: {len(report['sameGpuDistinctSlotPlaceOverlaps'])}\n")
    print(json.dumps({"ok":report["ok"],"rounds":len(rows),"overlaps":len(report["sameGpuDistinctSlotPlaceOverlaps"]),"errors":len(report["errors"])}, sort_keys=True))
    return 0 if report["ok"] else 1

if __name__ == "__main__": raise SystemExit(main())
