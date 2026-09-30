#!/usr/bin/env python3
"""Aggregate an E1 run directory into the E1 deliverables (data only, no plots).

Usage: e1_analyze.py <run_dir> [<run_dir> ...]   (writes <run_dir>/analysis/)

Windows come from e1_windows.csv: transition = [transition switch, steady
switch), steady = [steady switch, dwell end).  A request belongs to the window
it was scheduled in (its `phase`/`live_round` tag).

Latency = completion - actual_send (router end to end, includes queueing).
SLO per request:
  vision: catalog metadata slo.latencyMs
  llm:    TTFT and TPOT checked separately against slo.ttftMs / slo.tpotMs;
          a request meets its SLO when both hold.  Requests are not streamed,
          so client TTFT = (e2e - runtimeLatencyMs) + runtime ttftMs, i.e.
          router queueing and network plus prefill; TPOT = runtime tpotMs.
          The end-to-end budget slo.ttftMs + (O - 1) * slo.tpotMs is kept as
          a reference column.
Outputs:
  language_transition_table.csv  per round x LLM workload, transition window:
      offered, completed, completed/offered, per completed request the
      fraction over the TTFT SLO / TPOT SLO / either, max TTFT / SLO,
      max TPOT / SLO, the same for the end-to-end budget, failures,
      commitment rate
  vision_timeseries.csv          per second x vision workload: offered (by
      scheduled second, including sender rejections), sent, completed
      (success, by completion second), commitment and target rate,
      ledger ready capacity, round/window
  ledger_capacity_per_second.csv per second x workload: ledger ready
      capacity (catalog mu sum over ready replicas) and commitment.  The
      runner's capacity_timeline.csv has the event times but not the
      replicas' profile/batch, so the ledger is rebuilt here: replicas and
      batches come from the route snapshots before (previous round's
      after.json) and after each round, the switch times from the
      capacity_timeline events (route_ready_add, stop_accepting_remove,
      batch_effective); mu from the run's catalog.
  min_ratio.csv                  per workload: min over transitions of the
      throughput / commitment ratio (vision: min per-second completed /
      commitment inside transition windows; llm: completed / offered)
  makespan_window_metrics.csv    per round x workload x window: "makespan" =
      requests sent while the cluster was changing (first action start to
      last action end of the round's plan; the sender switches to the
      commitment rate earlier, when the plan is approved), "steady" = the
      dwell after it.  Each offered request is on time, late (completed over
      the router-adjusted SLO, see makespan_window_metrics), failed by outage
      (HTTP 404: no ready replica) or by overload (sender pending bound);
      bad_fraction = not on time / offered, goodput = on time / offered.
      Requests the sender rejected have no send time; their scheduled time is
      used.
"""
from __future__ import annotations

import csv
import gzip
import json
import math
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
CATALOGS = HERE.parents[1] / "planner-engine" / "app" / "mock" / "profile-catalogs"
VISION = ("resnet50_image", "vgg16_image", "vit_base_image")
LLM_SHAPES = {"gpt2_p64_o64": 64, "gpt2_p512_o512": 512, "llama_p1024_o128": 128, "llama_p2048_o64": 64}
WORKLOADS = VISION + tuple(LLM_SHAPES)


def slo_ms() -> dict[str, float]:
    """Vision: latency SLO.  LLM: end-to-end budget TTFT + (O - 1) * TPOT."""
    out = {}
    for workload in WORKLOADS:
        slo = yaml.safe_load((CATALOGS / f"{workload}.yaml").read_text())["metadata"]["slo"]
        if workload in VISION:
            out[workload] = float(slo["latencyMs"])
        else:
            out[workload] = float(slo["ttftMs"]) + (LLM_SHAPES[workload] - 1) * float(slo["tpotMs"])
    return out


def llm_token_slo_ms() -> dict[str, tuple[float, float]]:
    out = {}
    for workload in LLM_SHAPES:
        slo = yaml.safe_load((CATALOGS / f"{workload}.yaml").read_text())["metadata"]["slo"]
        out[workload] = (float(slo["ttftMs"]), float(slo["tpotMs"]))
    return out


def llm_token_latency_ms(request: dict) -> tuple[float, float] | None:
    """(client TTFT, TPOT) in ms from a successful non-streamed LLM request."""
    try:
        response = json.loads(request.get("response_json") or "{}")
        e2e = (float(request["completion"]) - float(request["actual_send"])) * 1000.0
        ttft = e2e - float(response["runtimeLatencyMs"]) + float(response["ttftMs"])
        return ttft, float(response["tpotMs"])
    except (KeyError, TypeError, ValueError):
        return None


def frac(part: list, whole: list) -> float | None:
    return round(len(part) / len(whole), 4) if whole else None


def max_over(values: list[float], bound: float) -> float | None:
    return round(max(values) / bound, 4) if values else None


def utc(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


NUMERIC = {"live_round", "rate", "scheduled_send", "actual_send", "send_lag_s", "completion", "attempts"}


def load_requests(run: Path) -> list[dict]:
    """requests.jsonl if present, else requests.csv.gz (compact_requests.py);
    the compact rows get their response fields back as response_json."""
    raw = run / "requests.jsonl"
    if raw.exists():
        return [json.loads(line) for line in raw.read_text().splitlines() if line.strip()]
    rows = []
    with gzip.open(run / "requests.csv.gz", "rt", newline="") as f:
        for r in csv.DictReader(f):
            row, response = {}, {}
            for key, value in r.items():
                if key.startswith("response."):
                    if value != "":
                        response[key[len("response."):]] = float(value) if key != "response.runtimeId" and key != "response.routerDispatchAt" else value
                elif key in NUMERIC:
                    row[key] = (int(float(value)) if key in ("live_round", "attempts") else float(value)) if value != "" else None
                else:
                    row[key] = value
            row["response_json"] = json.dumps(response) if response else ""
            rows.append(row)
    return rows


def write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def catalog_mu(run: Path) -> dict[tuple[str, str, int], float]:
    name = json.loads((run / "environment.json").read_text()).get("catalog_file") or "catalog.csv"
    path = HERE / Path(name).name
    return {(r["workload"], r["profile"], int(r["batch"])): float(r["mu"]) for r in csv.DictReader(path.open())}


def ready_routes(snapshot_path: Path) -> dict[str, tuple[str, str, int]]:
    """runtimeId -> (workload, profile, batch) for routes accepting new work."""
    if not snapshot_path.exists():
        return {}
    routes = json.loads(snapshot_path.read_text()).get("routes") or {}
    routes = routes.get("routes", []) if isinstance(routes, dict) else routes
    return {r["runtimeId"]: (r["model"], str(r["profile"]), int(r["batchSize"]))
            for r in routes if r.get("active") and r.get("acceptingNew")}


def rebuild_ledger(run: Path, ledger: list[dict], rounds: list[int]) -> dict[str, list[tuple[float, float]]]:
    """Per workload, sorted (utc, ready capacity) steps."""
    mu = catalog_mu(run)
    steps: dict[str, list[tuple[float, float]]] = defaultdict(list)
    current: dict[str, tuple[str, str, int]] = {}
    for rnd in rounds:
        before = ready_routes(run / "snapshots" / f"r{rnd - 1:02d}_after.json") if rnd > 1 else {}
        after = ready_routes(run / "snapshots" / f"r{rnd:02d}_after.json")
        current = dict(before)
        rows = [r for r in ledger if int(r["live_round"]) == rnd and r.get("timestamp")]
        start = min((utc(r["timestamp"]) for r in rows if r["event_type"] == "source_baseline"), default=None)
        changes: list[tuple[float, str, str]] = []
        for r in rows:
            rid, kind = r.get("runtime_id") or "", r["event_type"]
            if kind == "route_ready_add" and rid in after and rid not in before:
                changes.append((utc(r["timestamp"]), "add", rid))
            elif kind == "stop_accepting_remove" and rid in before and rid not in after:
                changes.append((utc(r["timestamp"]), "remove", rid))
            elif kind == "batch_effective" and rid in before and rid in after and before[rid] != after[rid]:
                changes.append((utc(r["timestamp"]), "batch", rid))
        seen = {rid for _, _, rid in changes}
        # replicas that changed without a timeline event switch at round start (conservative for removes)
        for rid in set(before) | set(after):
            if rid not in seen and before.get(rid) != after.get(rid) and start is not None:
                changes.append((start, "remove" if rid not in after else ("add" if rid not in before else "batch"), rid))
        def total(workload: str) -> float:
            return sum(mu.get(v, 0.0) for v in current.values() if v[0] == workload)
        if start is not None:
            for workload in WORKLOADS:
                steps[workload].append((start, total(workload)))
        for t, kind, rid in sorted(changes):
            if kind == "remove":
                workload = current.pop(rid, before[rid])[0]
            else:
                current[rid] = after[rid]
                workload = after[rid][0]
            steps[workload].append((t, total(workload)))
    for workload in steps:
        steps[workload].sort()
    return steps


def plan_makespan_bounds(run: Path, rounds: list[int]) -> dict[int, tuple[float, float]]:
    """Per round: (first action start, last action end) in UTC seconds."""
    out = {}
    for rnd in rounds:
        path = run / "plans" / f"r{rnd:02d}_terminal_plan.json"
        if not path.exists():
            continue
        starts, ends = [], []

        def walk(o):
            if isinstance(o, dict):
                if "startedAt" in o and "finishedAt" in o and "type" in o and "id" in o:
                    starts.append(utc(o["startedAt"][:26].rstrip("Z") + "+00:00" if "." in o["startedAt"] else o["startedAt"]))
                    ends.append(utc(o["finishedAt"][:26].rstrip("Z") + "+00:00" if "." in o["finishedAt"] else o["finishedAt"]))
                for v in o.values():
                    walk(v)
            elif isinstance(o, list):
                for v in o:
                    walk(v)

        walk(json.loads(path.read_text()))
        if starts:
            out[rnd] = (min(starts), max(ends))
    return out


ROUTER_RTT_MS = 2.0


def makespan_window_metrics(run: Path, requests: list[dict], slo: dict, token_slo: dict) -> list[dict]:
    """Per round x workload x window (makespan, steady): every offered request
    is exactly one of on time, late (completed over the router-adjusted SLO),
    failed by outage (404) or failed by overload (sender pending bound).

    The catalog SLO bounds a batch's compute time, as profiled; requests also
    wait in the router, and part of that wait is there whatever the capacity.
    Vision threshold = SLO + (b - 1) / lambda (the first request of a batch
    waits for the other b - 1) + the batch compute time (the batch ahead of it
    in the two-deep pipeline) + round trip, with lambda the replica's
    completion rate in the window and the compute time its median.  LLM
    replicas run one request at a time: TTFT threshold = TTFT SLO + round
    trip, TPOT threshold = TPOT SLO.  over_slo_profile_basis counts completed
    requests whose compute time alone exceeds the SLO (the planner's check)."""
    rounds = sorted({r["live_round"] for r in requests if r.get("live_round")})
    mbounds = plan_makespan_bounds(run, rounds)
    sbounds = {}
    for w in csv.DictReader((run / "e1_windows.csv").open()):
        t1 = utc(w["steady_switch_utc"])
        sbounds[int(w["live_round"])] = (t1, t1 + float(w["dwell_end_offset"]) - float(w["steady_switch_offset"]))
    offsets = [r["actual_send"] - r["scheduled_send"] - (r.get("send_lag_s") or 0.0)
               for r in requests if r.get("actual_send") and r.get("scheduled_send") is not None]
    sender_start = sorted(offsets)[len(offsets) // 2] if offsets else 0.0

    cells: dict[tuple[int, str, str], list[tuple[dict, float]]] = defaultdict(list)
    for r in requests:
        rnd = r.get("live_round")
        phase = r.get("phase")
        if phase == "transition":
            window, bounds = "makespan", mbounds.get(rnd)
        elif phase == "steady":
            window, bounds = "steady", sbounds.get(rnd)
        else:
            continue
        sent = r.get("actual_send") or (sender_start + r["scheduled_send"] if r.get("scheduled_send") is not None else None)
        if bounds is None or sent is None or not (bounds[0] <= sent < bounds[1]):
            continue
        cells[(rnd, r["workload"], window)].append((r, sent))

    rows = []
    for (rnd, workload, window), items in sorted(cells.items()):
        span = (mbounds if window == "makespan" else sbounds)[rnd]
        span_s = span[1] - span[0]
        replica: dict[str, list[dict]] = defaultdict(list)
        for r, _ in items:
            if r.get("status") == "success":
                replica[json.loads(r["response_json"]).get("runtimeId", "")].append(r)
        threshold: dict[str, float] = {}
        for rid, done in replica.items():
            responses = [json.loads(r["response_json"]) for r in done]
            batch = int(max(x.get("maxBatchSize") or 1 for x in responses))
            compute = sorted(float(x.get("runtimeLatencyMs") or 0.0) for x in responses)[len(responses) // 2]
            lam = len(done) / span_s if span_s > 0 else 0.0
            fill = (batch - 1) / lam * 1000.0 if batch > 1 and lam > 0 else 0.0
            threshold[rid] = slo[workload] + fill + compute + ROUTER_RTT_MS
        on_time = late = outage = overload = other = profile_over = 0
        outage_sent = []
        for r, sent in items:
            error = r.get("error") or ""
            if r.get("status") == "success":
                x = json.loads(r["response_json"])
                if workload in VISION:
                    e2e = (float(r["completion"]) - float(r["actual_send"])) * 1000.0
                    ok = e2e <= threshold[x.get("runtimeId", "")]
                    profile_over += float(x.get("runtimeLatencyMs") or 0.0) > slo[workload]
                else:
                    tokens = llm_token_latency_ms(r)
                    ttft_slo, tpot_slo = token_slo[workload]
                    ok = tokens is not None and tokens[0] <= ttft_slo + ROUTER_RTT_MS and tokens[1] <= tpot_slo
                    profile_over += float(x.get("ttftMs") or 0.0) > ttft_slo or float(x.get("tpotMs") or 0.0) > tpot_slo
                on_time += ok
                late += not ok
            elif "404" in error:
                outage += 1
                outage_sent.append(sent)
            elif "pending" in error:
                overload += 1
            else:
                other += 1
        offered = len(items)
        completed = on_time + late
        rows.append({
            "live_round": rnd, "workload": workload, "window": window, "window_s": round(span_s, 3),
            "offered": offered, "on_time": on_time, "late": late,
            "failed_outage_404": outage, "failed_overload": overload, "failed_other": other,
            "bad_fraction": round((late + outage + overload + other) / offered, 4) if offered else None,
            "goodput": round(on_time / offered, 4) if offered else None,
            "late_fraction_of_completed": round(late / completed, 4) if completed else None,
            "over_slo_profile_basis_of_completed": round(profile_over / completed, 4) if completed else None,
            "outage_span_s": round(max(outage_sent) - min(outage_sent), 3) if outage_sent else None,
        })
    return rows


def analyze(run: Path) -> dict:
    out = run / "analysis"
    out.mkdir(exist_ok=True)
    slo = slo_ms()
    token_slo = llm_token_slo_ms()
    windows = list(csv.DictReader((run / "e1_windows.csv").open()))
    requests = load_requests(run)
    ledger = list(csv.DictReader((run / "capacity_timeline.csv").open()))

    # time base: UTC seconds; per-round window bounds in UTC
    bounds = {}
    for w in windows:
        t0 = utc(w["transition_switch_utc"])
        t1 = utc(w["steady_switch_utc"])
        t2 = t1 + (float(w["dwell_end_offset"]) - float(w["steady_switch_offset"]))
        bounds[int(w["live_round"])] = {
            "t0": t0, "t1": t1, "t2": t2,
            "commitment": json.loads(w["commitment_json"]), "target": json.loads(w["target_json"]),
        }
    run_start = min(b["t0"] for b in bounds.values())
    run_end = max(b["t2"] for b in bounds.values())

    def rates_at(t: float) -> tuple[int | None, str | None, dict, dict]:
        for rnd, b in sorted(bounds.items()):
            if b["t0"] <= t < b["t1"]:
                return rnd, "transition", b["commitment"], b["target"]
            if b["t1"] <= t < b["t2"]:
                return rnd, "steady", b["target"], b["target"]
        return None, None, {}, {}

    events = rebuild_ledger(run, ledger, sorted(bounds))

    def capacity_at(workload: str, t: float) -> float | None:
        value = None
        for ts, cap in events.get(workload, []):
            if ts <= t:
                value = cap
            else:
                break
        return value

    # --- language per-transition table
    lang_rows = []
    for rnd, b in sorted(bounds.items()):
        for workload in LLM_SHAPES:
            reqs = [r for r in requests if r.get("workload") == workload and int(r.get("live_round", -1)) == rnd and r.get("phase") == "transition"]
            done = [r for r in reqs if r.get("status") == "success"]
            lat = [(float(r["completion"]) - float(r["actual_send"])) * 1000.0 for r in done if r.get("actual_send")]
            tokens = [t for t in (llm_token_latency_ms(r) for r in done) if t is not None]
            ttft_slo, tpot_slo = token_slo[workload]
            ttft = [t for t, _ in tokens]
            tpot = [p for _, p in tokens]
            lang_rows.append({
                "live_round": rnd, "workload": workload,
                "window_seconds": round(b["t1"] - b["t0"], 3),
                "commitment_rps": b["commitment"].get(workload, 0.0),
                "offered": len(reqs), "completed": len(done),
                "completed_over_offered": round(len(done) / len(reqs), 4) if reqs else None,
                "ttft_slo_ms": ttft_slo, "tpot_slo_ms": tpot_slo,
                "over_ttft_slo_fraction": frac([t for t in ttft if t > ttft_slo], ttft),
                "over_tpot_slo_fraction": frac([p for p in tpot if p > tpot_slo], tpot),
                "over_token_slo_fraction": frac([1 for t, p in tokens if t > ttft_slo or p > tpot_slo], tokens),
                "max_ttft_over_slo": max_over(ttft, ttft_slo),
                "max_tpot_over_slo": max_over(tpot, tpot_slo),
                "e2e_budget_ms": slo[workload],
                "over_e2e_budget_fraction": frac([x for x in lat if x > slo[workload]], lat),
                "max_latency_over_e2e_budget": max_over(lat, slo[workload]),
                "token_latency_missing": len(done) - len(tokens),
                "failures": sum(1 for r in reqs if r.get("status") not in ("success",)),
            })
    write_csv(out / "language_transition_table.csv", lang_rows, list(lang_rows[0].keys()))

    # --- per-second grids
    # offered = requests the generator scheduled in that second, including
    # ones the sender rejected at its pending bound (they carry no
    # actual_send); sent = requests that actually left the sender.
    # scheduled_send is an offset from the sender start; map it to UTC with
    # the sent requests (actual_send - send_lag_s - scheduled_send).
    anchors = sorted(float(r["actual_send"]) - float(r["send_lag_s"]) - float(r["scheduled_send"])
                     for r in requests if r.get("actual_send") and r.get("send_lag_s") is not None and r.get("scheduled_send") is not None)
    sender_start_utc = anchors[len(anchors) // 2]
    first_sec, last_sec = math.floor(run_start), math.ceil(run_end)
    offered = defaultdict(int)
    sent = defaultdict(int)
    completed = defaultdict(int)
    for r in requests:
        w = r.get("workload")
        if r.get("scheduled_send") is not None:
            offered[(w, math.floor(sender_start_utc + float(r["scheduled_send"])))] += 1
        if r.get("actual_send"):
            sent[(w, math.floor(float(r["actual_send"])))] += 1
        if r.get("status") == "success" and r.get("completion"):
            completed[(w, math.floor(float(r["completion"])))] += 1
    vision_rows, ledger_rows = [], []
    for sec in range(first_sec, last_sec):
        mid = sec + 0.5
        rnd, window, rate, target = rates_at(mid)
        for workload in WORKLOADS:
            cap = capacity_at(workload, mid)
            ledger_rows.append({
                "t_utc": sec, "t_rel": sec - first_sec, "live_round": rnd, "window": window, "workload": workload,
                "ledger_capacity_rps": cap, "commitment_or_target_rps": rate.get(workload) if rate else None,
            })
            if workload in VISION:
                vision_rows.append({
                    "t_utc": sec, "t_rel": sec - first_sec, "live_round": rnd, "window": window, "workload": workload,
                    "offered": offered[(workload, sec)], "sent": sent[(workload, sec)],
                    "completed": completed[(workload, sec)],
                    "rate_rps": rate.get(workload) if rate else None, "target_rps": target.get(workload) if target else None,
                    "ledger_capacity_rps": cap,
                })
    write_csv(out / "vision_timeseries.csv", vision_rows, list(vision_rows[0].keys()))
    write_csv(out / "ledger_capacity_per_second.csv", ledger_rows, list(ledger_rows[0].keys()))

    # --- minimum throughput / commitment
    min_rows = []
    for workload in WORKLOADS:
        worst = None
        for rnd, b in sorted(bounds.items()):
            commit = float(b["commitment"].get(workload, 0.0))
            if commit <= 0:
                continue
            if workload in VISION:
                secs = [s for s in range(math.ceil(b["t0"]), math.floor(b["t1"]))]
                if not secs:
                    continue
                ratio = min(completed[(workload, s)] / commit for s in secs)
            else:
                row = next(x for x in lang_rows if x["live_round"] == rnd and x["workload"] == workload)
                if not row["offered"]:
                    continue
                ratio = row["completed"] / row["offered"]
            if worst is None or ratio < worst[0]:
                worst = (ratio, rnd)
        min_rows.append({
            "workload": workload,
            "metric": "min per-second completed / commitment in transition windows" if workload in VISION else "min completed / offered per transition window",
            "min_ratio": round(worst[0], 4) if worst else None, "at_round": worst[1] if worst else None,
        })
    write_csv(out / "min_ratio.csv", min_rows, list(min_rows[0].keys()))
    ms_rows = makespan_window_metrics(run, requests, slo, token_slo)
    if ms_rows:
        write_csv(out / "makespan_window_metrics.csv", ms_rows, list(ms_rows[0].keys()))
    return {"run": str(run), "rounds": len(bounds), "requests": len(requests), "min_ratio": min_rows}


if __name__ == "__main__":
    for arg in sys.argv[1:]:
        print(json.dumps(analyze(Path(arg)), indent=1))
