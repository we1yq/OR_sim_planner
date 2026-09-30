#!/usr/bin/env python3
"""Capacity calibration on the E1 layouts: actual replica capacity vs catalog mu.

E1 drives each workload at its demand, so only replicas planned with a thin
margin ever saturate and most replicas' actual capacity is never observed
(none on rtx1 in the 2026-09-30 runs).  This run places the E1 layouts R1..R12
one after the other with the same transitions as E1 but no traffic while they
execute, then drives every workload at OVERLOAD x the catalog capacity placed
for it for WINDOW_S seconds, so every replica is saturated at once (at least as
busy a host as in E1).  Sender pending-bound rejections are expected.

Per replica (runtime, option, round): completion rate over [SKIP_S, WINDOW_S)
(vision: completions / seconds; LLM, one request at a time: 1 / median gap
between consecutive completions), share of full batches, median GPU time, and
factor = rate / catalog mu.

Usage: e1_capacity_calibration.py [--catalog catalog_newest_v3.csv] [--rounds 1-12]
Writes cluster_results/<utc>-capacity-calibration/{calibration.csv,summary.txt}.
"""
from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("live_runner_calibration", HERE / "live_runner_20260926.py")
runner = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = runner
spec.loader.exec_module(runner)
traffic = runner.traffic

ROUTER = "http://115.145.179.144:10680"
OVERLOAD = 1.3
WINDOW_S = 45.0
SKIP_S = 10.0
LLM = {"gpt2_p64_o64", "gpt2_p512_o512", "llama_p1024_o128", "llama_p2048_o64"}


def placed(router) -> list[dict]:
    routes = router.get_json("/routes").get("routes", [])
    return [r for r in routes if r.get("active") and r.get("acceptingNew") and not r.get("draining")]


def drive(rates: dict, live_round: int) -> list[dict]:
    sender = traffic.BoundedAsyncSender(traffic.urllib_transport(ROUTER, timeout_s=600.0))
    gen = traffic.ContinuousRateSender(sender)
    gen.start()
    gen.set_rates(rates, live_round=live_round, window="calibration")
    time.sleep(WINDOW_S)
    gen.stop()
    rows = sender.drain(timeout_s=600.0)
    sender.shutdown()
    return rows


def replica_rows(rows: list[dict], replicas: list[dict], mu: dict, live_round: int, t0: float) -> list[dict]:
    per: dict[str, list[tuple[float, dict]]] = {}
    for r in rows:
        if r.get("status") != "success":
            continue
        x = json.loads(r["response_json"])
        per.setdefault(x.get("runtimeId", ""), []).append((float(r["completion"]), x))
    out = []
    for rep in replicas:
        rid, w = rep["runtimeId"], rep["model"]
        samples = sorted((c, x) for c, x in per.get(rid, []) if t0 + SKIP_S <= c < t0 + WINDOW_S)
        if len(samples) < 3:
            continue
        batch = int(rep.get("batchSize") or 1)
        if w in LLM:
            # one request at a time: the median gap between completions is
            # robust to the few requests a 35 s window holds
            gaps = [b - a for (a, _), (b, _) in zip(samples, samples[1:])]
            rate = 1.0 / statistics.median(gaps) if gaps and statistics.median(gaps) > 0 else 0.0
        else:
            rate = len(samples) / (WINDOW_S - SKIP_S)
        full = sum(1 for _, x in samples if int(x.get("batchSize") or 1) >= batch) / len(samples)
        m = mu.get((w, rep.get("profile"), batch))
        out.append({"round": live_round, "workload": w, "runtime": rid, "gpu": rep.get("gpu"), "profile": rep.get("profile"),
                    "batch": batch, "completed": len(samples), "rate_rps": round(rate, 4), "catalog_mu": m,
                    "factor": round(rate / m, 4) if m else "", "full_batch_share": round(full, 3),
                    "gpu_ms_median": round(statistics.median(float(x.get("runtimeLatencyMs") or 0) for _, x in samples), 3)})
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default="catalog_newest_v3.csv")
    ap.add_argument("--rounds", default="1-12")
    a = ap.parse_args()
    lo, hi = (int(x) for x in a.rounds.split("-"))
    mu = {(r["workload"], r["profile"], int(r["batch"])): float(r["mu"]) for r in csv.DictReader((HERE / a.catalog).open())}
    out = HERE / "cluster_results" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-capacity-calibration")
    (out / "warmup").mkdir(parents=True)
    kube, router = runner.Kubectl("or-sim-exp"), runner.Router(ROUTER)
    demand, _, _ = runner.load_frozen_inputs(catalog_file=a.catalog)
    zero = {k: 0.0 for k in runner.WORKLOAD_KEYS}
    ctx = runner.RunContext("calib" + str(int(time.time())), out, "or-sim-exp", ROUTER, 60, 30, 60, e1_mode=True)
    args = SimpleNamespace(controller_names=["planner-controller", "transition-executor", "cluster-state-manager", "runtime-router"],
                           placement_nodes=[], watchdog_seconds=1800.0, stage3_variant="slicewise")
    rows_out: list[dict] = []
    prev = zero
    try:
        for rnd in range(1, hi + 1):
            target = runner.demand_rates(demand[rnd - 1])
            runner._e1_warmup_step(ctx, args, kube, router, label=f"r{rnd}", number=60 + rnd, source_rates=prev,
                                   target_rates=target, require_empty=(prev is zero))
            prev = target
            if rnd < lo:
                continue
            replicas = placed(router)
            cap: dict[str, float] = {}
            for rep in replicas:
                cap[rep["model"]] = cap.get(rep["model"], 0.0) + float(rep.get("capacity") or 0.0)
            rates = {w: OVERLOAD * cap.get(w, 0.0) for w in runner.WORKLOAD_KEYS}
            t0 = time.time()
            rows = drive(rates, rnd)
            got = replica_rows(rows, replicas, mu, rnd, t0)
            rows_out += got
            print(f"R{rnd}: {len(replicas)} replicas, " + ", ".join(
                f"{g['runtime'].replace('-image', '')}={g['factor']}" for g in got), flush=True)
            router.wait_drained(timeout=600.0)
            with (out / "calibration.csv").open("w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(rows_out[0].keys()))
                writer.writeheader()
                writer.writerows(rows_out)
    finally:
        runner._e1_warmup_step(ctx, args, kube, router, label="down", number=99, source_rates=prev, target_rates=zero,
                               require_empty=False)
    lines = []
    for host in ("rtx1", "ampere"):
        for fam in ("vision", "llm"):
            sel = [float(r["factor"]) for r in rows_out if r["factor"] != "" and (host in r["runtime"])
                   and ((r["workload"] in LLM) == (fam == "llm")) and (fam == "llm" or r["full_batch_share"] >= 0.95)]
            if sel:
                lines.append(f"{host} {fam}: n={len(sel)} min {min(sel):.3f} median {statistics.median(sel):.3f} max {max(sel):.3f}")
    (out / "summary.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
