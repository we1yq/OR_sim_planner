#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any


TEST_ROOT = Path(__file__).resolve().parent
RESULT_ROOT = TEST_ROOT / "results/real_3gpu_k8s"

WORKLOADS = ("llama", "gpt2", "resnet50")
REGISTERED_SLO_MS = {"llama": 180.0, "gpt2": 50.0, "resnet50": 100.0}
REQUEST_CLASSES = {"llama": "p1024/o128", "gpt2": "p64/o64", "resnet50": "image batches"}

# The deployed planner-engine fixture is the real three-model system
# (llama/gpt2/resnet50). These points keep the online-3GPU shape: seven
# transitions plus a final shutdown, bounded by three physical A100s.
DEFAULT_STAGES: list[dict[str, float]] = [
    {"llama": 0.30, "gpt2": 0.50, "resnet50": 500.0},
    {"llama": 0.55, "gpt2": 0.75, "resnet50": 900.0},
    {"llama": 0.20, "gpt2": 0.95, "resnet50": 1300.0},
    {"llama": 0.62, "gpt2": 0.35, "resnet50": 700.0},
    {"llama": 0.42, "gpt2": 0.85, "resnet50": 1500.0},
    {"llama": 0.68, "gpt2": 0.25, "resnet50": 1100.0},
    {"llama": 0.18, "gpt2": 0.65, "resnet50": 300.0},
    {"llama": 0.0, "gpt2": 0.0, "resnet50": 0.0},
]


def main() -> int:
    args = parse_args()
    run_id = args.run_id or time.strftime("real3gpu-%Y%m%d-%H%M%S")
    out_dir = Path(args.out_dir) / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    stages = load_stages(args)
    write_json(out_dir / "schedule.json", {"runId": run_id, "steadySeconds": args.steady_seconds, "stages": stages})

    router = args.router_url.rstrip("/")
    kubectl(["get", "nodes", "-o", "wide"])
    assert_router(router)
    cleanup_router_routes(router)
    ensure_empty_cluster(args, run_id, out_dir)

    request_rows: list[dict[str, Any]] = []
    route_rows: list[dict[str, Any]] = []
    transition_rows: list[dict[str, Any]] = []
    action_rows: list[dict[str, Any]] = []
    gpu_rows: list[dict[str, Any]] = []
    allocation_snapshots: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    source = {workload: 0.0 for workload in WORKLOADS}
    for idx, target in enumerate(stages):
        epoch = f"{run_id}-e{idx:02d}"
        print(f"\n=== epoch {idx}: source={source} target={target} ===", flush=True)
        traffic = TrafficDriver(
            router=router,
            source=source,
            target=target,
            stage=epoch,
            request_rows=request_rows,
            route_rows=route_rows,
            sample_interval_s=args.route_sample_s,
            poll_s=args.traffic_poll_s,
            infer_timeout_s=args.infer_timeout_s,
        )
        traffic.start()
        try:
            snapshot_name = create_arrival_snapshot(args.namespace, epoch, source, target, args.planner)
            plan_name = "plan-" + snapshot_name
            plan = wait_plan(args.namespace, plan_name, timeout_s=args.transition_timeout_s)
            if plan_phase(plan) != "Executed":
                failure = collect_failure(args.namespace, plan_name, plan)
                failures.append(failure)
                write_json(out_dir / f"failure_epoch_{idx:02d}.json", failure)
                raise RuntimeError(f"{plan_name} failed: {failure.get('message')}")
            transition_row = transition_metric_row(idx, plan_name, plan)
            transition_row.update(p95_slo_metrics_for_stage(
                epoch,
                request_rows,
                transition_row.get("transitionStartedAt"),
                transition_row.get("transitionFinishedAt"),
            ))
            transition_rows.append(transition_row)
            action_rows.extend(action_status_rows(idx, plan_name, plan))
            gpu_rows.extend(gpu_count_rows_from_plan(args.namespace, idx, plan_name, plan))
            allocation_snapshots.append(target_allocation_snapshot(idx, plan_name, plan))
            traffic.enter_steady()
            print(f"epoch {idx} transition executed; steady {args.steady_seconds:.1f}s", flush=True)
            time.sleep(max(0.0, args.steady_seconds))
        finally:
            traffic.stop()
        source = dict(target)
        write_outputs(out_dir, request_rows, route_rows, transition_rows, action_rows, gpu_rows, allocation_snapshots, failures)

    write_outputs(out_dir, request_rows, route_rows, transition_rows, action_rows, gpu_rows, allocation_snapshots, failures)
    write_json(out_dir / "final_registry.json", kubectl_json(["get", "physicalgpuregistry", "default", "-n", args.namespace, "-o", "json"]))
    print(f"\nREAL_3GPU_K8S_RESULT_DIR={out_dir}", flush=True)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run real 3GPU continuous plan+transition experiment on Kubernetes.")
    parser.add_argument("--namespace", default="or-sim")
    parser.add_argument("--router-url", default="http://127.0.0.1:18080")
    parser.add_argument("--planner", default="ours")
    parser.add_argument("--steady-seconds", type=float, default=300.0)
    parser.add_argument("--transition-timeout-s", type=float, default=1800.0)
    parser.add_argument("--infer-timeout-s", type=float, default=1800.0)
    parser.add_argument("--traffic-poll-s", type=float, default=1.0)
    parser.add_argument("--route-sample-s", type=float, default=1.0)
    parser.add_argument("--out-dir", default=str(RESULT_ROOT))
    parser.add_argument("--run-id", default="")
    parser.add_argument("--stages-json", default="")
    parser.add_argument("--skip-reset", action="store_true")
    return parser.parse_args()


def load_stages(args: argparse.Namespace) -> list[dict[str, float]]:
    raw = json.loads(args.stages_json) if args.stages_json else DEFAULT_STAGES
    stages = []
    for stage in raw:
        stages.append({workload: float(dict(stage).get(workload, 0.0)) for workload in WORKLOADS})
    return stages


def create_arrival_snapshot(namespace: str, epoch: str, source: dict[str, float], target: dict[str, float], planner: str) -> str:
    name = sanitize(epoch)
    slo = {
        workload: {"demandRate": target[workload], "latencyMs": REGISTERED_SLO_MS[workload]}
        for workload in WORKLOADS
    }
    body = {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "ArrivalSnapshot",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "migrant-go",
                "mig.or-sim.io/component": "real-3gpu-k8s-runner",
                "experiment.or-sim.io/name": "real-3gpu-k8s",
            },
        },
        "spec": {
            "source": "real-3gpu-k8s-runner",
            "mode": "scheduled",
            "planner": planner,
            "epoch": epoch,
            "windowSeconds": 300,
            "unit": "requests_per_second",
            "observedAt": now_rfc3339(),
            "triggerReason": "real_3gpu_continuous_trace",
            "transitionDemandPolicy": "min",
            "profileCatalogRef": "default",
            "currentAllocationRef": "physicalgpuregistry/default",
            "registeredSLOMs": REGISTERED_SLO_MS,
            "sourceArrival": source,
            "targetArrival": target,
            "slo": slo,
            "placement": {"nodes": ["ampere", "rtx1-worker"]},
        },
    }
    kubectl_apply(body)
    return name


def wait_plan(namespace: str, plan_name: str, timeout_s: float) -> dict[str, Any]:
    deadline = time.time() + timeout_s
    last_phase = ""
    while time.time() < deadline:
        plan = kubectl_json(["get", "migactionplan", plan_name, "-n", namespace, "-o", "json"], check=False)
        if plan:
            phase = plan_phase(plan)
            if phase != last_phase:
                print(f"{plan_name}: phase={phase}", flush=True)
                last_phase = phase
            if phase in {"Executed", "Failed"}:
                return plan
        time.sleep(2.0)
    raise TimeoutError(f"timed out waiting for {plan_name}; last phase={last_phase}")


def ensure_empty_cluster(args: argparse.Namespace, run_id: str, out_dir: Path) -> None:
    if args.skip_reset:
        return
    registry = kubectl_json(["get", "physicalgpuregistry", "default", "-n", args.namespace, "-o", "json"])
    logical_count = int(((registry.get("status") or {}).get("currentAllocation") or {}).get("logicalGpuCount") or 0)
    routes = get_json(args.router_url.rstrip("/") + "/routes")
    route_count = len(routes.get("routes") or [])
    if logical_count == 0 and route_count == 0:
        print("cluster already empty", flush=True)
        return
    print(f"resetting cluster to empty: logicalGpuCount={logical_count} routeCount={route_count}", flush=True)
    zero = {workload: 0.0 for workload in WORKLOADS}
    snapshot = create_arrival_snapshot(args.namespace, f"{run_id}-reset-zero", zero, zero, args.planner)
    plan = wait_plan(args.namespace, "plan-" + snapshot, timeout_s=args.transition_timeout_s)
    write_json(out_dir / "reset_zero_plan.json", plan)
    if plan_phase(plan) != "Executed":
        raise RuntimeError("reset-to-zero plan failed: " + str((plan.get("status") or {}).get("message")))
    cleanup_router_routes(args.router_url.rstrip("/"))


class TrafficDriver:
    def __init__(
        self,
        router: str,
        source: dict[str, float],
        target: dict[str, float],
        stage: str,
        request_rows: list[dict[str, Any]],
        route_rows: list[dict[str, Any]],
        sample_interval_s: float,
        poll_s: float,
        infer_timeout_s: float,
    ) -> None:
        self.router = router
        self.source = dict(source)
        self.target = dict(target)
        self.stage = stage
        self.request_rows = request_rows
        self.route_rows = route_rows
        self.sample_interval_s = max(0.2, sample_interval_s)
        self.poll_s = max(0.2, poll_s)
        self.infer_timeout_s = infer_timeout_s
        self.stop_event = threading.Event()
        self.steady_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.started = 0.0
        self.carry = {workload: 0.0 for workload in WORKLOADS}
        self.seq = {workload: 0 for workload in WORKLOADS}

    def start(self) -> None:
        self.started = time.monotonic()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=max(2.0, self.poll_s + 1.0))

    def enter_steady(self) -> None:
        self.steady_event.set()

    def phase(self) -> str:
        return "steady" if self.steady_event.is_set() else "transition"

    def run(self) -> None:
        next_route_sample = time.monotonic()
        with ThreadPoolExecutor(max_workers=128) as pool:
            futures = []
            cursor = time.monotonic()
            while not self.stop_event.is_set():
                now = time.monotonic()
                if now >= next_route_sample:
                    self.sample_routes(now - self.started)
                    next_route_sample = now + self.sample_interval_s
                segment_end = min(cursor + self.poll_s, time.monotonic() + self.poll_s)
                rates = self.effective_rates()
                for model, rate in rates.items():
                    if rate <= 0:
                        continue
                    new_futures = self.schedule_model(pool, model, rate, cursor, segment_end)
                    futures.extend(new_futures)
                done = [future for future in futures if future.done()]
                futures = [future for future in futures if not future.done()]
                for future in done:
                    self.request_rows.append(future.result())
                sleep_until(segment_end)
                cursor = segment_end
            for future in as_completed(futures):
                self.request_rows.append(future.result())

    def effective_rates(self) -> dict[str, float]:
        if self.steady_event.is_set():
            return dict(self.target)
        return {
            workload: min(self.source.get(workload, 0.0), self.target.get(workload, 0.0))
            for workload in WORKLOADS
        }

    def schedule_model(self, pool: ThreadPoolExecutor, model: str, rate: float, start: float, end: float) -> list[Any]:
        duration = max(0.0, end - start)
        desired = self.carry.get(model, 0.0) + rate * duration
        count = int(math.floor(desired + 1e-9))
        self.carry[model] = desired - count
        if count <= 0:
            return []
        interval = duration / count
        futures = []
        for idx in range(count):
            target_at = start + (idx + 0.5) * interval
            seq = self.seq.get(model, 0)
            self.seq[model] = seq + 1
            futures.append(
                pool.submit(
                    send_request,
                    self.router,
                    self.stage,
                    self.phase(),
                    model,
                    seq,
                    target_at,
                    self.infer_timeout_s,
                )
            )
        return futures

    def sample_routes(self, relative_s: float) -> None:
        try:
            routes = get_json(self.router + "/routes", timeout_s=5.0).get("routes") or []
            by_model: dict[str, dict[str, Any]] = {}
            sampled_at = time.time()
            input_rates = self.effective_rates()
            for route in routes:
                if not isinstance(route, dict):
                    continue
                if not route.get("active") or not route.get("acceptingNew") or route.get("draining"):
                    continue
                model = str(route.get("model") or "")
                if not model:
                    continue
                row = by_model.setdefault(
                    model,
                    {
                        "stage": self.stage,
                        "phase": self.phase(),
                        "sampledAt": sampled_at,
                        "timeSeconds": relative_s,
                        "model": model,
                        "actualServiceRate": 0.0,
                        "capacity": 0.0,
                        "inputDemandRate": input_rates.get(model, 0.0),
                        "targetDemandRate": self.target.get(model, 0.0),
                        "routeCount": 0,
                    },
                )
                row["actualServiceRate"] += float(route.get("arrivalRate") or 0.0)
                row["capacity"] += float(route.get("capacity") or 0.0)
                row["routeCount"] += 1
            self.route_rows.extend(by_model.values())
        except Exception as exc:
            self.route_rows.append(
                {
                    "stage": self.stage,
                    "phase": self.phase(),
                    "sampledAt": time.time(),
                    "timeSeconds": relative_s,
                    "model": "",
                    "actualServiceRate": "",
                    "capacity": "",
                    "inputDemandRate": "",
                    "targetDemandRate": "",
                    "routeCount": 0,
                    "error": str(exc),
                }
            )


def send_request(router: str, stage: str, phase: str, model: str, seq: int, target_at: float, timeout_s: float) -> dict[str, Any]:
    sleep_until(target_at)
    sent = time.time()
    start = time.perf_counter()
    row = {"stage": stage, "phase": phase, "model": model, "seq": seq, "sentAt": sent, "ok": False, "status": "", "latencyMs": "", "runtimeLatencyMs": "", "error": ""}
    try:
        body: dict[str, Any] = {"benchmark": True, "sentAt": sent}
        if model == "gpt2":
            body.update({"prompt_len": 64, "output_tokens": 64, "batch": 1, "requestClass": REQUEST_CLASSES[model]})
        elif model == "llama":
            body.update({"prompt_len": 1024, "output_tokens": 128, "batch": 1, "requestClass": REQUEST_CLASSES[model]})
        elif model == "resnet50":
            body.update({"requestClass": REQUEST_CLASSES[model]})
        resp = post_json(router + "/infer/" + model, body, timeout_s=timeout_s)
        row["ok"] = True
        row["status"] = 200
        row["runtimeLatencyMs"] = resp.get("runtimeLatencyMs", "")
    except urllib.error.HTTPError as exc:
        row["status"] = exc.code
        row["error"] = str(exc)
    except Exception as exc:
        row["error"] = str(exc)
    row["latencyMs"] = round((time.perf_counter() - start) * 1000.0, 6)
    return row


def p95_slo_metrics_for_stage(
    stage: str,
    request_rows: list[dict[str, Any]],
    transition_started_at: Any = None,
    transition_finished_at: Any = None,
    bucket_seconds: float = 1.0,
) -> dict[str, Any]:
    window_start = parse_rfc3339_seconds(transition_started_at)
    window_end = parse_rfc3339_seconds(transition_finished_at)
    by_model_bucket: dict[str, dict[int, list[float]]] = {model: {} for model in WORKLOADS}
    for row in request_rows:
        if row.get("stage") != stage or row.get("phase") != "transition":
            continue
        model = str(row.get("model") or "")
        if model not in by_model_bucket:
            continue
        try:
            sent_at = float(row.get("sentAt"))
            latency = float(row.get("latencyMs"))
        except (TypeError, ValueError):
            continue
        if window_start is not None and sent_at < window_start:
            continue
        if window_end is not None and sent_at > window_end:
            continue
        bucket = int(math.floor(sent_at / bucket_seconds))
        by_model_bucket[model].setdefault(bucket, []).append(latency)

    intervals: list[tuple[float, float]] = []
    by_model: dict[str, Any] = {}
    for model, buckets in by_model_bucket.items():
        slo_ms = REGISTERED_SLO_MS[model]
        violating = []
        p95_values = []
        request_count = 0
        for bucket, values in sorted(buckets.items()):
            request_count += len(values)
            p95 = percentile(values, 95.0)
            p95_values.append(p95)
            if p95 > slo_ms:
                start = bucket * bucket_seconds
                end = start + bucket_seconds
                clipped_start = max(start, window_start) if window_start is not None else start
                clipped_end = min(end, window_end) if window_end is not None else end
                if clipped_end <= clipped_start:
                    continue
                intervals.append((clipped_start, clipped_end))
                violating.append({
                    "bucketStart": round(clipped_start, 6),
                    "bucketEnd": round(clipped_end, 6),
                    "p95LatencyMs": round(p95, 3),
                    "requestCount": len(values),
                })
        by_model[model] = {
            "latencySLOMs": slo_ms,
            "bucketSeconds": bucket_seconds,
            "requestCount": request_count,
            "bucketCount": len(buckets),
            "violatingBucketCount": len(violating),
            "p95SLOViolationSeconds": round(union_seconds([(float(v["bucketStart"]), float(v["bucketEnd"])) for v in violating]), 6),
            "maxBucketP95LatencyMs": round(max(p95_values), 3) if p95_values else 0.0,
            "violatingBuckets": violating[:20],
            "truncatedViolatingBuckets": max(0, len(violating) - 20),
        }

    return {
        "sloViolationDurationSec": round(union_seconds(intervals), 6),
        "sloViolationP95BucketSec": round(union_seconds(intervals), 6),
        "sloP95BucketSeconds": bucket_seconds,
        "sloP95ByModel": json.dumps(by_model, sort_keys=True, separators=(",", ":")),
    }


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(0, min(len(ordered) - 1, math.ceil((pct / 100.0) * len(ordered)) - 1))
    return ordered[rank]


def union_seconds(intervals: list[tuple[float, float]]) -> float:
    if not intervals:
        return 0.0
    intervals.sort()
    merged: list[list[float]] = []
    for start, end in intervals:
        if not merged or start > merged[-1][1]:
            merged.append([start, end])
        elif end > merged[-1][1]:
            merged[-1][1] = end
    return sum(end - start for start, end in merged)


def transition_metric_row(epoch: int, plan_name: str, plan: dict[str, Any]) -> dict[str, Any]:
    status = plan.get("status") or {}
    spec = plan.get("spec") or {}
    summary = spec.get("summary") or {}
    execution = status.get("transitionExecution") or {}
    metrics = execution.get("metrics") or {}
    router_slo = metrics.get("routerSLO") or metrics.get("routerMonitorFinal") or metrics.get("routerMonitor") or {}
    slo_models = router_slo.get("models") or {}
    violation_excess = sum(
        float(row.get("latencySLOViolationSeconds") or 0.0)
        for row in slo_models.values()
        if isinstance(row, dict)
    )
    violation_duration = slo_violation_wall_clock_seconds(slo_models)
    violation_count = sum(
        int(row.get("latencyViolationCount") or 0)
        for row in slo_models.values()
        if isinstance(row, dict)
    )
    transition_requests = sum(
        int(row.get("requests") or 0)
        for row in slo_models.values()
        if isinstance(row, dict)
    )
    transition_errors = sum(
        int(row.get("errors") or 0)
        for row in slo_models.values()
        if isinstance(row, dict)
    )
    return {
        "epoch": epoch,
        "plan": plan_name,
        "phase": status.get("phase"),
        "message": status.get("message"),
        "actionCount": spec.get("actionCount"),
        "sourceGpuCount": summary.get("sourceGpuCount"),
        "targetGpuCount": summary.get("targetGpuCount"),
        "planner": summary.get("planner"),
        "plannerMakespanSec": summary.get("plannerMakespanSec"),
        "transitionMakespanSec": (execution.get("durationsSeconds") or {}).get("total") or execution.get("makespanSec") or metrics.get("transitionMakespanSec"),
        "sloViolationDurationSec": round(violation_duration, 6),
        "sloViolationExcessSec": round(violation_excess, 6),
        "sloViolationCount": violation_count,
        "transitionRequestCount": transition_requests,
        "transitionErrorCount": transition_errors,
        "sloByModel": json.dumps(slo_models, sort_keys=True, separators=(",", ":")),
        "transitionStartedAt": router_slo.get("startedAt"),
        "transitionFinishedAt": router_slo.get("finishedAt"),
        "finalValidationOk": ((metrics.get("finalValidation") or {}).get("ok")),
    }


def slo_violation_wall_clock_seconds(slo_models: dict[str, Any]) -> float:
    intervals: list[tuple[float, float]] = []
    for row in slo_models.values():
        if not isinstance(row, dict):
            continue
        start = parse_rfc3339_seconds(row.get("firstViolationAt"))
        if start is None:
            continue
        wall = row.get("latencySLOViolationWallSeconds")
        if wall is not None:
            intervals.append((start, start + float(wall)))
            continue
        end = parse_rfc3339_seconds(row.get("lastViolationAt"))
        if end is not None and end >= start:
            intervals.append((start, end))
    if not intervals:
        return 0.0
    intervals.sort()
    merged: list[list[float]] = []
    for start, end in intervals:
        if not merged or start > merged[-1][1]:
            merged.append([start, end])
        elif end > merged[-1][1]:
            merged[-1][1] = end
    return sum(end - start for start, end in merged)


def parse_rfc3339_seconds(value: Any) -> float | None:
    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def action_status_rows(epoch: int, plan_name: str, plan: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for action in (plan.get("status") or {}).get("actionStatuses") or []:
        if isinstance(action, dict):
            rows.append({"epoch": epoch, "plan": plan_name, **action})
    return rows


def gpu_count_rows_from_plan(namespace: str, epoch: int, plan_name: str, plan: dict[str, Any]) -> list[dict[str, Any]]:
    execution = (plan.get("status") or {}).get("transitionExecution") or {}
    events = (
        ((execution.get("metrics") or {}).get("actionSummary") or {}).get("activeGpuCountOverTime")
        or []
    )
    started_at = (execution.get("timestamps") or {}).get("executorStartedAt")
    rows = []
    for event in events:
        if not isinstance(event, dict):
            continue
        relative = float(event.get("relativeSeconds") or 0.0)
        rows.append(
            {
                "epoch": epoch,
                "plan": plan_name,
                "active": event.get("activeGpuCount"),
                "reason": event.get("reason") or event.get("actionType"),
                "actionId": event.get("actionId"),
                "physicalGpuId": event.get("physicalGpuId"),
                "relativeSeconds": relative,
                "observedAt": add_rfc3339_seconds(started_at, relative),
            }
        )
    if rows:
        return rows

    reg = kubectl_json(["get", "physicalgpuregistry", "default", "-n", namespace, "-o", "json"])
    status = reg.get("status") or {}
    counts = status.get("queueCounts") or {}
    return [{
        "epoch": epoch,
        "plan": plan_name,
        "active": counts.get("active"),
        "available": counts.get("available"),
        "transitioning": counts.get("transitioning"),
        "observedAt": status.get("observedAt"),
    }]


def add_rfc3339_seconds(value: Any, seconds: float) -> str:
    if not value:
        return ""
    try:
        from datetime import datetime, timedelta

        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return (parsed + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")
    except (TypeError, ValueError):
        return str(value)


def target_allocation_snapshot(epoch: int, plan_name: str, plan: dict[str, Any]) -> dict[str, Any]:
    spec = plan.get("spec") or {}
    validation_targets = spec.get("validationTargets") or {}
    target = (
        spec.get("targetAllocationPlan")
        or validation_targets.get("targetAllocationPlan")
        or spec.get("targetAllocation")
        or spec.get("targetState")
        or (spec.get("summary") or {}).get("targetAllocation")
        or {}
    )
    return {"epoch": epoch, "plan": plan_name, "targetAllocation": target}


def allocation_similarity_rows(snapshots: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    previous: set[str] | None = None
    previous_epoch: int | None = None
    for snapshot in snapshots:
        current = allocation_signature(snapshot.get("targetAllocation"))
        if previous is not None:
            union = previous | current
            intersection = previous & current
            rows.append(
                {
                    "fromEpoch": previous_epoch,
                    "toEpoch": snapshot.get("epoch"),
                    "jaccardSimilarity": 1.0 if not union else round(len(intersection) / len(union), 6),
                    "intersectionSize": len(intersection),
                    "unionSize": len(union),
                }
            )
        previous = current
        previous_epoch = int(snapshot.get("epoch") or 0)
    return rows


def allocation_signature(value: Any) -> set[str]:
    if isinstance(value, dict):
        runtimes = value.get("desiredRuntimes")
        if isinstance(runtimes, list):
            out = set()
            for raw in runtimes:
                if not isinstance(raw, dict):
                    continue
                out.add(
                    "|".join(
                        str(raw.get(key, ""))
                        for key in ("gpu", "slotResource", "model", "batchSize")
                    )
                )
            return out
    return flatten_allocation(value)


def flatten_allocation(value: Any, prefix: str = "") -> set[str]:
    if isinstance(value, dict):
        out: set[str] = set()
        for key, item in sorted(value.items()):
            child_prefix = f"{prefix}.{key}" if prefix else str(key)
            out.update(flatten_allocation(item, child_prefix))
        return out
    if isinstance(value, list):
        out = set()
        for idx, item in enumerate(value):
            child_prefix = f"{prefix}[{idx}]"
            out.update(flatten_allocation(item, child_prefix))
        return out
    return {f"{prefix}={value}"}


def collect_failure(namespace: str, plan_name: str, plan: dict[str, Any]) -> dict[str, Any]:
    return {
        "plan": plan_name,
        "phase": plan_phase(plan),
        "message": (plan.get("status") or {}).get("message"),
        "status": plan.get("status"),
        "plannerControllerLogs": kubectl_text(["logs", "-n", namespace, "deployment/planner-controller", "--tail=120"], check=False),
        "executorLogs": kubectl_text(["logs", "-n", namespace, "deployment/transition-executor", "--tail=160"], check=False),
        "plannerEngineLogs": kubectl_text(["logs", "-n", namespace, "deployment/planner-engine", "--tail=160"], check=False),
    }


def write_outputs(
    out_dir: Path,
    requests: list[dict[str, Any]],
    routes: list[dict[str, Any]],
    transitions: list[dict[str, Any]],
    actions: list[dict[str, Any]],
    gpu: list[dict[str, Any]],
    allocations: list[dict[str, Any]],
    failures: list[dict[str, Any]],
) -> None:
    refresh_p95_slo_metrics(transitions, requests)
    write_csv(out_dir / "requests.csv", requests)
    write_csv(out_dir / "service_rate_samples.csv", routes)
    write_csv(out_dir / "transition_metrics.csv", transitions)
    write_csv(out_dir / "action_statuses.csv", actions)
    write_csv(out_dir / "gpu_counts.csv", gpu)
    write_csv(out_dir / "allocation_similarity.csv", allocation_similarity_rows(allocations))
    write_json(out_dir / "target_allocations.json", allocations)
    write_json(out_dir / "failures.json", failures)
    (RESULT_ROOT / "latest_path.txt").write_text(str(out_dir) + "\n", encoding="utf-8")


def refresh_p95_slo_metrics(transitions: list[dict[str, Any]], requests: list[dict[str, Any]]) -> None:
    for row in transitions:
        stage = stage_name_from_plan(str(row.get("plan") or ""))
        row.update(p95_slo_metrics_for_stage(
            stage,
            requests,
            row.get("transitionStartedAt"),
            row.get("transitionFinishedAt"),
        ))


def stage_name_from_plan(plan_name: str) -> str:
    return plan_name[5:] if plan_name.startswith("plan-") else plan_name


def assert_router(router: str) -> None:
    health = get_json(router + "/healthz", timeout_s=5.0)
    if not health.get("ok"):
        raise RuntimeError(f"router health check failed: {health}")


def cleanup_router_routes(router: str) -> None:
    for model in WORKLOADS:
        request(router + "/control/routes?model=" + model, method="DELETE", timeout_s=10.0)


def kubectl(args: list[str], check: bool = True) -> str:
    return kubectl_text(args, check=check)


def kubectl_text(args: list[str], check: bool = True) -> str:
    proc = subprocess.run(["kubectl", *args], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if check and proc.returncode != 0:
        raise RuntimeError(f"kubectl {' '.join(args)} failed: {proc.stderr.strip()}")
    return proc.stdout


def kubectl_json(args: list[str], check: bool = True) -> dict[str, Any]:
    text = kubectl_text(args, check=check)
    if not text.strip():
        return {}
    return json.loads(text)


def kubectl_apply(body: dict[str, Any]) -> None:
    proc = subprocess.run(["kubectl", "apply", "-f", "-"], input=json.dumps(body), text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode != 0:
        raise RuntimeError(f"kubectl apply failed: {proc.stderr.strip()}")
    print(proc.stdout.strip(), flush=True)


def get_json(url: str, timeout_s: float = 10.0) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode())


def post_json(url: str, payload: dict[str, Any], timeout_s: float) -> dict[str, Any]:
    raw = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=raw, headers={"content-type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode())


def request(url: str, method: str, timeout_s: float) -> None:
    req = urllib.request.Request(url, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout_s):
            pass
    except urllib.error.HTTPError as exc:
        if exc.code != 404:
            raise


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


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    keys: list[str] = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def plan_phase(plan: dict[str, Any]) -> str:
    return str((plan.get("status") or {}).get("phase") or "")


def sanitize(value: str) -> str:
    out = []
    for ch in value.lower():
        out.append(ch if ch.isalnum() or ch == "-" else "-")
    return "".join(out).strip("-")[:63]


def now_rfc3339() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def sleep_until(target: float) -> None:
    while True:
        remaining = target - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(remaining, 0.5))


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        raise
