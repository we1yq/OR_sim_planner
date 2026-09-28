#!/usr/bin/env python3
"""Smoke test for the E1b fixes: deploy R1 from empty, drive R1 demand for a
short window through the router, report per-replica backlog/latency and
place_instance durations, then return the cluster to empty.  Not a
measurement; writes nothing under cluster_results."""
from __future__ import annotations

import json
import statistics
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import importlib.util

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("live_runner_smoke", HERE / "live_runner_20260926.py")
runner = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = runner
spec.loader.exec_module(runner)
traffic = runner.traffic

ROUTER = "http://115.145.179.144:10680"
TRAFFIC_SECONDS = 25.0


def main() -> int:
    kube = runner.Kubectl("or-sim-exp")
    router = runner.Router(ROUTER)
    demand, _, _ = runner.load_frozen_inputs(catalog_file="catalog_20260929_median_min.csv")
    zero = {k: 0.0 for k in runner.WORKLOAD_KEYS}
    r1 = runner.demand_rates(demand[0])
    out = Path("/tmp") / f"e1_smoke_{int(time.time())}"
    (out / "warmup").mkdir(parents=True)
    ctx = runner.RunContext("smoke" + str(int(time.time())), out, "or-sim-exp", ROUTER, 60, 30, 60, e1_mode=True)
    args = SimpleNamespace(controller_names=["planner-controller", "transition-executor", "cluster-state-manager", "runtime-router"],
                           placement_nodes=[], watchdog_seconds=1800.0, stage3_variant="slicewise")
    up = runner._e1_warmup_step(ctx, args, kube, router, label="up", number=80, source_rates=zero, target_rates=r1, require_empty=True)
    plan = json.loads((out / "warmup" / "up_terminal_plan.json").read_text())
    places = [a for a in plan["status"]["actionStatuses"] if a.get("type") == "place_instance"]
    print("place_instance durations (s):", sorted(round(float(a.get("durationSeconds") or 0), 2) for a in places))
    sender = traffic.BoundedAsyncSender(traffic.urllib_transport(ROUTER, timeout_s=300.0))
    gen = traffic.ContinuousRateSender(sender)
    gen.start()
    gen.set_rates(r1, live_round=1, window="steady")
    time.sleep(TRAFFIC_SECONDS)
    routes = router.get_json("/routes")
    gen.set_rates(zero, live_round=1, window="stopped")
    gen.stop()
    rows = sender.drain(timeout_s=300.0)
    sender.shutdown()
    print("routes at end of traffic window:")
    for r in routes.get("routes", []):
        print(f"  {r.get('model'):18s} {r.get('profile')} b{r.get('batchSize')} inflight={r.get('endpointInflight')} "
              f"queued={r.get('endpointQueued')} avgE2E={r.get('endpointAvgLatencyMs')} runtimeLat={round(float(r.get('runtime.runtimeLatencyMs') or 0),1)}")
    by = {}
    for row in rows:
        if row.get("status") == "success":
            by.setdefault(row["workload"], []).append((float(row["completion"]) - float(row["actual_send"])) * 1000)
    print("client e2e latency (ms) median/p95/max and success count:")
    for w, v in sorted(by.items()):
        v.sort()
        print(f"  {w:18s} n={len(v):5d} med={statistics.median(v):8.1f} p95={v[int(0.95*(len(v)-1))]:8.1f} max={v[-1]:8.1f}")
    import collections
    status = collections.Counter((r.get("workload"), r.get("status")) for r in rows)
    print("status by workload:")
    for (w, st), n in sorted(status.items()):
        print(f"  {w:18s} {st:24s} {n}")
    lag = sorted(float(r.get("send_lag_s") or 0) for r in rows if r.get("actual_send"))
    if lag:
        print(f"send lag p50/p99/max (ms): {lag[len(lag)//2]*1000:.1f} / {lag[int(0.99*(len(lag)-1))]*1000:.1f} / {lag[-1]*1000:.1f}")
    llm = {}
    for row in rows:
        if row.get("status") == "success" and row.get("family") == "llm":
            x = json.loads(row["response_json"])
            llm.setdefault(row["workload"], []).append((x["ttftMs"], x["tpotMs"], x["runtimeLatencyMs"], x["latencyMs"] - x["runtimeLatencyMs"]))
    print("llm runtime ttft / tpot / generate ms / wall-generate ms (medians):")
    for w, v in sorted(llm.items()):
        print(f"  {w:18s} " + " / ".join(f"{statistics.median(c):.1f}" for c in zip(*v)))
    router.wait_drained(timeout=300.0)
    down = runner._e1_warmup_step(ctx, args, kube, router, label="down", number=81, source_rates=r1, target_rates=zero, require_empty=False)
    print("up/down:", up, down)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
