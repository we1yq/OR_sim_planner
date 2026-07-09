#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any


def main() -> int:
    args = parse_args()
    router = args.router_url.rstrip("/")
    plan = load_plan(args)
    apply_schedule_time_defaults(args, plan)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    payloads = json.loads(args.payloads_json) if args.payloads_json else {}
    rows: list[dict[str, Any]] = []
    route_samples: list[dict[str, Any]] = []
    started = time.monotonic()
    wall_started = time.time()
    stop_route_sampler = threading.Event()
    route_sampler_thread = None
    if args.route_sample_out:
        route_sampler_thread = threading.Thread(
            target=sample_routes_loop,
            args=(router, started, args.route_sample_s, args.control_timeout_s, stop_route_sampler, route_samples),
            daemon=True,
        )
        route_sampler_thread.start()

    monitor_started = False
    if args.monitor:
        post_json(
            f"{router}/control/monitor",
            {
                "phase": "start",
                "planName": args.monitor_name,
                "sourceArrival": plan.get("sourceArrival", {}),
                "targetArrival": first_stage_rates(plan),
                "registeredSLOMs": plan.get("registeredSLOMs", {}),
                "slo": first_stage_slo(plan),
            },
            timeout_s=args.control_timeout_s,
        )
        monitor_started = True

    try:
        with ThreadPoolExecutor(max_workers=args.max_workers) as pool:
            futures = []
            carry: dict[str, float] = {}
            seq_by_model: dict[str, int] = {}
            previous_target_rates = number_map(plan.get("sourceArrival") or {})
            for stage_idx, stage in enumerate(plan["stages"]):
                offset = scaled_stage_offset(stage, stage_idx, args)
                next_offset = scaled_stage_end_offset(plan["stages"], stage_idx, args)
                if args.duration_s > 0:
                    next_offset = min(next_offset, args.duration_s)
                if next_offset <= offset:
                    continue
                wait_until(started + offset)
                stage_rates = number_map(stage.get("targetArrival") or stage.get("targetDemand") or {})
                source_rates = stage_source_rates(stage, previous_target_rates, plan)
                stage_name = str(stage.get("epoch") or stage.get("name") or f"stage-{stage_idx}")
                print(
                    f"stage {stage_name}: source={source_rates} target={stage_rates} "
                    f"for {next_offset - offset:.3f}s",
                    flush=True,
                )
                new_futures = schedule_stage_requests(
                    pool=pool,
                    router=router,
                    stage=stage_name,
                    source_rates=source_rates,
                    target_rates=stage_rates,
                    start_at=started + offset,
                    end_at=started + next_offset,
                    payloads=payloads,
                    timeout_s=args.infer_timeout_s,
                    carry=carry,
                    seq_by_model=seq_by_model,
                    transition_rate_policy=args.transition_rate_policy,
                    monitor_poll_s=args.monitor_poll_s,
                    control_timeout_s=args.control_timeout_s,
                )
                futures.extend(new_futures)
                if args.wait_stage_complete:
                    rows.extend(collect_rows(futures))
                    futures.clear()
                previous_target_rates = stage_rates
            rows.extend(collect_rows(futures))
    finally:
        if monitor_started:
            try:
                post_json(
                    f"{router}/control/monitor",
                    {"phase": "finish", "planName": args.monitor_name},
                    timeout_s=args.control_timeout_s,
                )
            except Exception as exc:
                print(f"monitor finish failed: {exc}", flush=True)
        if route_sampler_thread is not None:
            stop_route_sampler.set()
            route_sampler_thread.join(timeout=max(1.0, args.route_sample_s + 1.0))

    summary = summarize(rows, wall_started)
    summary["timeCompression"] = args.time_compression
    summary["stageDurationSeconds"] = args.stage_duration_s
    write_csv(out, rows)
    summary_path = out.with_suffix(".summary.json")
    if args.route_sample_out:
        route_sample_path = Path(args.route_sample_out)
        route_sample_path.parent.mkdir(parents=True, exist_ok=True)
        write_route_samples(route_sample_path, route_samples)
        summary["routeSampleOut"] = str(route_sample_path)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True), flush=True)
    print(out, flush=True)
    print(summary_path, flush=True)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send deterministic fixed-rate traffic to runtime-router.")
    parser.add_argument("--router-url", default="http://runtime-router:8080")
    parser.add_argument("--schedule", default="", help="JSON schedule or ConfigMap YAML containing data.schedule.json")
    parser.add_argument("--rates-json", default="", help='Single-stage rates, e.g. {"llama":0.1,"gpt2":0.2}')
    parser.add_argument("--duration-s", type=float, default=0.0, help="Override total duration for rates-json or truncate schedule")
    parser.add_argument(
        "--time-compression",
        type=float,
        default=1.0,
        help="Compress schedule time by this factor; 30min->5min uses 6. Req/s values are unchanged.",
    )
    parser.add_argument(
        "--stage-duration-s",
        type=float,
        default=0.0,
        help="Override every schedule stage duration, e.g. 300 for compressed 30min epochs.",
    )
    parser.add_argument("--out", default="tmp/fixed-rate-router-traffic.csv")
    parser.add_argument("--payloads-json", default="", help="Optional per-model payload overrides")
    parser.add_argument("--max-workers", type=int, default=128)
    parser.add_argument("--infer-timeout-s", type=float, default=1800.0)
    parser.add_argument("--control-timeout-s", type=float, default=30.0)
    parser.add_argument("--monitor", action="store_true", help="Open a router monitor window for this traffic run")
    parser.add_argument("--monitor-name", default="fixed-rate-router-traffic")
    parser.add_argument(
        "--transition-rate-policy",
        choices=["min", "target"],
        default="min",
        help="When router monitor is active, min sends min(sourceArrival,targetArrival); target always sends targetArrival.",
    )
    parser.add_argument(
        "--monitor-poll-s",
        type=float,
        default=1.0,
        help="Polling/chunk size used to notice transition monitor start/finish while sending fixed-rate traffic.",
    )
    parser.add_argument("--route-sample-out", default="", help="Optional CSV path for service-rate over time sampled from /routes")
    parser.add_argument("--route-sample-s", type=float, default=1.0, help="Route/service-rate sampling interval")
    parser.add_argument("--wait-stage-complete", action="store_true", help="Wait for all requests from a stage before advancing")
    return parser.parse_args()


def load_plan(args: argparse.Namespace) -> dict[str, Any]:
    if args.rates_json:
        rates = number_map(json.loads(args.rates_json))
        duration = args.duration_s if args.duration_s > 0 else 60.0
        return {
            "sourceArrival": rates,
            "registeredSLOMs": {},
            "stages": [{"epoch": "fixed", "offsetSeconds": 0, "sourceArrival": rates, "targetArrival": rates}],
            "durationSeconds": duration,
        }
    if not args.schedule:
        raise SystemExit("--schedule or --rates-json is required")
    text = Path(args.schedule).read_text(encoding="utf-8")
    if args.schedule.endswith((".yaml", ".yml")):
        text = extract_schedule_json(text)
    plan = json.loads(text)
    if "stages" not in plan:
        raise SystemExit("schedule must contain stages")
    return plan


def apply_schedule_time_defaults(args: argparse.Namespace, plan: dict[str, Any]) -> None:
    if args.time_compression == 1.0 and plan.get("timeCompression") is not None:
        args.time_compression = float(plan["timeCompression"])
    if args.stage_duration_s <= 0 and plan.get("stageDurationSeconds") is not None:
        args.stage_duration_s = float(plan["stageDurationSeconds"])


def extract_schedule_json(text: str) -> str:
    marker = "schedule.json: |"
    if marker not in text:
        raise SystemExit("YAML input must contain schedule.json: |")
    raw = text.split(marker, 1)[1]
    lines = []
    for line in raw.splitlines():
        if line.startswith("    "):
            lines.append(line[4:])
        elif not line.strip() and lines:
            lines.append("")
        elif lines:
            break
    return "\n".join(lines)


def scaled_stage_offset(stage: dict[str, Any], idx: int, args: argparse.Namespace) -> float:
    if args.stage_duration_s > 0:
        return float(idx) * args.stage_duration_s
    return float(stage.get("offsetSeconds", 0.0) or 0.0) / max(1e-9, args.time_compression)


def scaled_stage_end_offset(stages: list[dict[str, Any]], idx: int, args: argparse.Namespace) -> float:
    if args.stage_duration_s > 0:
        return float(idx + 1) * args.stage_duration_s
    raw_end = stage_end_offset(stages, idx, args.duration_s * args.time_compression if args.duration_s > 0 else 0.0)
    return raw_end / max(1e-9, args.time_compression)


def stage_end_offset(stages: list[dict[str, Any]], idx: int, duration_s: float) -> float:
    if idx + 1 < len(stages):
        return float(stages[idx + 1].get("offsetSeconds", 0.0) or 0.0)
    if duration_s > 0:
        return duration_s
    window = float(stages[idx].get("windowSeconds", 60.0) or 60.0)
    return float(stages[idx].get("offsetSeconds", 0.0) or 0.0) + window


def schedule_model_requests(
    pool: ThreadPoolExecutor,
    router: str,
    model: str,
    rate: float,
    start_at: float,
    end_at: float,
    stage: str,
    payload: dict[str, Any],
    timeout_s: float,
    carry: float,
    seq_start: int,
):
    duration = max(0.0, end_at - start_at)
    desired = carry + duration * rate
    count = max(0, int(math.floor(desired + 1e-9)))
    next_carry = desired - count
    futures = []
    if count == 0:
        return futures, next_carry, seq_start
    interval = duration / count
    for idx in range(count):
        target_at = start_at + (idx + 0.5) * interval
        futures.append(pool.submit(send_at, router, model, stage, seq_start + idx, target_at, payload, timeout_s))
    return futures, next_carry, seq_start + count


def schedule_stage_requests(
    pool: ThreadPoolExecutor,
    router: str,
    stage: str,
    source_rates: dict[str, float],
    target_rates: dict[str, float],
    start_at: float,
    end_at: float,
    payloads: dict[str, Any],
    timeout_s: float,
    carry: dict[str, float],
    seq_by_model: dict[str, int],
    transition_rate_policy: str,
    monitor_poll_s: float,
    control_timeout_s: float,
):
    futures = []
    cursor = start_at
    poll_s = max(0.1, monitor_poll_s)
    pending_transition = transition_rate_policy == "min" and rates_differ(source_rates, target_rates)
    saw_monitor_active = False
    while cursor < end_at - 1e-9:
        wait_until(cursor)
        segment_end = min(end_at, cursor + poll_s)
        effective_rates = target_rates
        if transition_rate_policy == "min":
            snapshot = get_json(f"{router}/metrics/slo", timeout_s=control_timeout_s)
            monitor_active = bool(snapshot.get("active"))
            if monitor_active:
                saw_monitor_active = True
                effective_rates = min_transition_rates(
                    number_map(snapshot.get("sourceArrival") or {}),
                    number_map(snapshot.get("targetArrival") or {}),
                    target_rates,
                )
            elif pending_transition and saw_monitor_active:
                pending_transition = False
                effective_rates = target_rates
            elif pending_transition:
                effective_rates = min_transition_rates(source_rates, target_rates, target_rates)
        for model, rate in sorted(effective_rates.items()):
            if rate <= 0:
                continue
            new_futures, carry[model], seq_by_model[model] = schedule_model_requests(
                pool=pool,
                router=router,
                model=model,
                rate=rate,
                start_at=cursor,
                end_at=segment_end,
                stage=stage,
                payload=payload_for(model, payloads),
                timeout_s=timeout_s,
                carry=carry.get(model, 0.0),
                seq_start=seq_by_model.get(model, 0),
            )
            futures.extend(new_futures)
        cursor = segment_end
    return futures


def stage_source_rates(
    stage: dict[str, Any],
    previous_target_rates: dict[str, float],
    plan: dict[str, Any],
) -> dict[str, float]:
    explicit = number_map(
        stage.get("sourceArrival")
        or stage.get("currentArrival")
        or stage.get("sourceDemand")
        or stage.get("currentDemand")
        or {}
    )
    if explicit:
        return explicit
    if previous_target_rates:
        return previous_target_rates
    return number_map(plan.get("sourceArrival") or {})


def rates_differ(source: dict[str, float], target: dict[str, float]) -> bool:
    models = set(source) | set(target)
    for model in models:
        if abs(source.get(model, 0.0) - target.get(model, 0.0)) > 1e-9:
            return True
    return False


def min_transition_rates(
    source: dict[str, float],
    target: dict[str, float],
    fallback_target: dict[str, float],
) -> dict[str, float]:
    models = set(source) | set(target) | set(fallback_target)
    out = {}
    for model in models:
        src = source.get(model, fallback_target.get(model, 0.0))
        dst = target.get(model, fallback_target.get(model, 0.0))
        out[model] = min(src, dst)
    return out


def send_at(
    router: str,
    model: str,
    stage: str,
    seq: int,
    target_at: float,
    payload: dict[str, Any],
    timeout_s: float,
) -> dict[str, Any]:
    wait_until(target_at)
    sent_wall = time.time()
    started = time.perf_counter()
    row: dict[str, Any] = {
        "stage": stage,
        "model": model,
        "seq": seq,
        "scheduledAt": target_at,
        "sentAt": sent_wall,
        "ok": False,
        "status": "",
        "latencyMs": "",
        "runtimeLatencyMs": "",
        "error": "",
    }
    try:
        body = dict(payload)
        body.setdefault("benchmark", True)
        body["sentAt"] = sent_wall
        response = post_json(f"{router}/infer/{model}", body, timeout_s=timeout_s)
        row["ok"] = True
        row["status"] = 200
        row["runtimeLatencyMs"] = response.get("runtimeLatencyMs", "")
    except urllib.error.HTTPError as exc:
        row["status"] = exc.code
        row["error"] = str(exc)
    except Exception as exc:
        row["error"] = str(exc)
    row["latencyMs"] = round((time.perf_counter() - started) * 1000.0, 6)
    return row


def collect_rows(futures) -> list[dict[str, Any]]:
    rows = []
    for future in as_completed(futures):
        rows.append(future.result())
    rows.sort(key=lambda row: (str(row["stage"]), str(row["model"]), int(row["seq"])))
    return rows


def post_json(url: str, payload: dict[str, Any], timeout_s: float) -> dict[str, Any]:
    raw = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=raw, headers={"content-type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode())


def get_json(url: str, timeout_s: float) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode())


def sample_routes_loop(
    router: str,
    started: float,
    interval_s: float,
    timeout_s: float,
    stop: threading.Event,
    samples: list[dict[str, Any]],
) -> None:
    interval = max(0.1, interval_s)
    while not stop.is_set():
        sample_at = time.time()
        relative = time.monotonic() - started
        try:
            snapshot = get_json(f"{router}/routes", timeout_s=timeout_s)
            for row in service_rate_rows(snapshot):
                row["sampleAt"] = sample_at
                row["timeSeconds"] = round(relative, 6)
                samples.append(row)
        except Exception as exc:
            samples.append({
                "sampleAt": sample_at,
                "timeSeconds": round(relative, 6),
                "model": "",
                "serviceRate": "",
                "routeCount": 0,
                "error": str(exc),
            })
        stop.wait(interval)


def service_rate_rows(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    by_model: dict[str, dict[str, Any]] = {}
    for raw in snapshot.get("routes") or []:
        route = raw if isinstance(raw, dict) else {}
        model = str(route.get("model") or "")
        if not model:
            continue
        active = bool(route.get("active"))
        accepting = bool(route.get("acceptingNew"))
        draining = bool(route.get("draining"))
        if not active or not accepting or draining:
            continue
        row = by_model.setdefault(model, {"model": model, "serviceRate": 0.0, "routeCount": 0, "error": ""})
        row["serviceRate"] += float(route.get("capacity") or 0.0)
        row["routeCount"] += 1
    return list(by_model.values())


def wait_until(target: float) -> None:
    while True:
        remaining = target - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(remaining, 0.5))


def payload_for(model: str, payloads: dict[str, Any]) -> dict[str, Any]:
    payload = dict(payloads.get(model) or payloads.get("*") or {})
    if model in {"llama", "gpt2"}:
        payload.setdefault("prompt_len", 64)
        payload.setdefault("output_tokens", 64)
        payload.setdefault("batch", 1)
    return payload


def number_map(raw: Any) -> dict[str, float]:
    if not isinstance(raw, dict):
        return {}
    out = {}
    for key, value in raw.items():
        try:
            out[str(key)] = float(value)
        except (TypeError, ValueError):
            pass
    return out


def first_stage_rates(plan: dict[str, Any]) -> dict[str, float]:
    stages = list(plan.get("stages") or [])
    if not stages:
        return {}
    return number_map(dict(stages[0]).get("targetArrival") or {})


def first_stage_slo(plan: dict[str, Any]) -> dict[str, Any]:
    stages = list(plan.get("stages") or [])
    if not stages:
        return {}
    return dict(dict(stages[0]).get("slo") or {})


def summarize(rows: list[dict[str, Any]], started_wall: float) -> dict[str, Any]:
    by_model: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_model.setdefault(str(row["model"]), []).append(row)
    models = {}
    for model, items in sorted(by_model.items()):
        latencies = [float(row["latencyMs"]) for row in items if row.get("ok")]
        models[model] = {
            "requests": len(items),
            "ok": sum(1 for row in items if row.get("ok")),
            "errors": sum(1 for row in items if not row.get("ok")),
            "latencyMsAvg": percentile(latencies, -1),
            "latencyMsP50": percentile(latencies, 0.50),
            "latencyMsP95": percentile(latencies, 0.95),
            "latencyMsP99": percentile(latencies, 0.99),
        }
    return {
        "startedAt": started_wall,
        "finishedAt": time.time(),
        "requests": len(rows),
        "models": models,
    }


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    if q < 0:
        return round(sum(values) / len(values), 6)
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(math.ceil(q * len(ordered))) - 1))
    return round(ordered[idx], 6)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = ["stage", "model", "seq", "scheduledAt", "sentAt", "ok", "status", "latencyMs", "runtimeLatencyMs", "error"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_route_samples(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = ["sampleAt", "timeSeconds", "model", "serviceRate", "routeCount", "error"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    raise SystemExit(main())
