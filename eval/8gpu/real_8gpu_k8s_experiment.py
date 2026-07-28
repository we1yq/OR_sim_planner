#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
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
RESULT_ROOT = TEST_ROOT / "results/real_8gpu_k8s"
DEFAULT_TRACE = TEST_ROOT / "demand_throughput_trace_30min.csv"

WORKLOADS = (
    "resnet50_image",
    "vgg16_image",
    "vit_base_image",
    "gpt2_p64_o64",
    "gpt2_p512_o512",
    "llama_p1024_o128",
    "llama_p2048_o64",
)
TRACE_WORKLOAD_COLUMNS = WORKLOADS

EVALUATION_SLOS = {
    "resnet50_image": {"model": "resnet50", "requestClass": "image batches", "latencyMs": 100.0, "tpotMs": None},
    "vgg16_image": {"model": "vgg16", "requestClass": "image batches", "latencyMs": 100.0, "tpotMs": None},
    "vit_base_image": {"model": "vit_base", "requestClass": "image batches", "latencyMs": 300.0, "tpotMs": None},
    "gpt2_p64_o64": {"model": "gpt2", "requestClass": "p64/o64", "promptLen": 64, "outputTokens": 64, "latencyMs": 50.0, "tpotMs": 20.0},
    "gpt2_p512_o512": {"model": "gpt2", "requestClass": "p512/o512", "promptLen": 512, "outputTokens": 512, "latencyMs": 100.0, "tpotMs": 20.0},
    "llama_p1024_o128": {"model": "llama", "requestClass": "p1024/o128", "promptLen": 1024, "outputTokens": 128, "latencyMs": 180.0, "tpotMs": 35.0},
    "llama_p2048_o64": {"model": "llama", "requestClass": "p2048/o64", "promptLen": 2048, "outputTokens": 64, "latencyMs": 250.0, "tpotMs": 35.0},
    "llama_p4096_o512": {"model": "llama", "requestClass": "p4096/o512", "promptLen": 4096, "outputTokens": 512, "latencyMs": 500.0, "tpotMs": 35.0},
}


def main() -> int:
    args = parse_args()
    run_id = args.run_id or time.strftime("real8gpu-%Y%m%d-%H%M%S")
    out_dir = Path(args.out_dir) / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    stages = load_stages(args)
    write_json(out_dir / "schedule.json", {"runId": run_id, "steadySeconds": args.steady_seconds, "stages": stages})

    router = args.router_url.rstrip("/")
    kubectl(["get", "nodes", "-o", "wide"])
    assert_router(router)
    ensure_empty_cluster(args, run_id, out_dir)
    if args.warmup:
        run_warmup(args, run_id, out_dir)

    request_rows: list[dict[str, Any]] = []
    route_rows: list[dict[str, Any]] = []
    transition_rows: list[dict[str, Any]] = []
    action_rows: list[dict[str, Any]] = []
    readiness_rows: list[dict[str, Any]] = []
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
            readiness_rows.extend(runtime_readiness_rows(idx, plan_name, plan, phase="experiment"))
            gpu_rows.extend(gpu_count_rows_from_plan(args.namespace, idx, plan_name, plan))
            allocation_snapshots.append(target_allocation_snapshot(idx, plan_name, plan))
            traffic.enter_steady()
            print(f"epoch {idx} transition executed; steady {args.steady_seconds:.1f}s", flush=True)
            time.sleep(max(0.0, args.steady_seconds))
        finally:
            traffic.stop()
        source = dict(target)
        write_outputs(out_dir, request_rows, route_rows, transition_rows, action_rows, readiness_rows, gpu_rows, allocation_snapshots, failures)

    write_outputs(out_dir, request_rows, route_rows, transition_rows, action_rows, readiness_rows, gpu_rows, allocation_snapshots, failures)
    write_json(out_dir / "final_registry.json", kubectl_json(["get", "physicalgpuregistry", "default", "-n", args.namespace, "-o", "json"]))
    print(f"\nREAL_8GPU_K8S_RESULT_DIR={out_dir}", flush=True)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run real 8GPU 24h trace plan+transition experiment on Kubernetes.")
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
    parser.add_argument("--trace-csv", default=str(DEFAULT_TRACE))
    parser.add_argument("--stages-json", default="")
    parser.add_argument("--skip-reset", action="store_true")
    parser.add_argument("--warmup", dest="warmup", action="store_true", default=True)
    parser.add_argument("--no-warmup", dest="warmup", action="store_false")
    return parser.parse_args()


def load_stages(args: argparse.Namespace) -> list[dict[str, float]]:
    raw = json.loads(args.stages_json) if args.stages_json else load_trace_stages(Path(args.trace_csv))
    stages = []
    for stage in raw:
        stages.append({workload: float(dict(stage).get(workload, 0.0)) for workload in WORKLOADS})
    return stages


def load_trace_stages(path: Path) -> list[dict[str, float]]:
    if not path.exists():
        raise FileNotFoundError(path)
    stages: list[dict[str, float]] = []
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        missing = [col for col in TRACE_WORKLOAD_COLUMNS if col not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(f"{path} missing trace columns: {missing}")
        for row in reader:
            stage = {column: float(row.get(column) or 0.0) for column in TRACE_WORKLOAD_COLUMNS}
            stages.append(stage)
    if not stages:
        raise ValueError(f"{path} has no stages")
    return stages


def slo_for_workload(workload: str) -> dict[str, Any]:
    if workload not in EVALUATION_SLOS:
        raise KeyError(f"No evaluation SLO registered for workload {workload!r}")
    return dict(EVALUATION_SLOS[workload])


def create_arrival_snapshot(namespace: str, epoch: str, source: dict[str, float], target: dict[str, float], planner: str) -> str:
    name = sanitize(epoch)
    slo = {}
    for workload in WORKLOADS:
        config = slo_for_workload(workload)
        row = {
            "demandRate": target[workload],
            "requestClass": config["requestClass"],
            "latencyMs": config["latencyMs"],
        }
        if config.get("tpotMs") is not None:
            row["ttftMs"] = config["latencyMs"]
            row["tpotMs"] = config["tpotMs"]
        slo[workload] = row
    body = {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "ArrivalSnapshot",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "migrant-go",
                "mig.or-sim.io/component": "real-8gpu-k8s-runner",
                "experiment.or-sim.io/name": "real-8gpu-k8s",
            },
        },
        "spec": {
            "source": "real-8gpu-k8s-runner",
            "mode": "scheduled",
            "planner": planner,
            "epoch": epoch,
            "windowSeconds": 300,
            "unit": "requests_per_second",
            "observedAt": now_rfc3339(),
            "triggerReason": "real_8gpu_24h_continuous_trace",
            "transitionDemandPolicy": "min",
            "forceReplan": True,
            "profileCatalogRef": "default",
            "scenarioPath": "mock/scenarios/real8gpu.yaml",
            "currentAllocationRef": "physicalgpuregistry/default",
            "registeredSLOMs": {workload: slo_for_workload(workload)["latencyMs"] for workload in WORKLOADS},
            "sourceArrival": source,
            "targetArrival": target,
            "slo": slo,
            "placement": {},
        },
    }
    kubectl_create(body)
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
    dirty_gpus = dirty_physical_gpus(registry)
    if dirty_gpus:
        names = ", ".join(gpu["physicalGpuId"] for gpu in dirty_gpus)
        print(f"repairing dirty GPUs before reset: {names}", flush=True)
        for gpu in dirty_gpus:
            plan_name = create_deep_repair_plan(args.namespace, run_id, gpu)
            plan = wait_plan(args.namespace, plan_name, timeout_s=args.transition_timeout_s)
            write_json(out_dir / f"{plan_name}.json", plan)
            if plan_phase(plan) != "Executed":
                raise RuntimeError(f"{plan_name} failed: " + str((plan.get("status") or {}).get("message")))
        registry = kubectl_json(["get", "physicalgpuregistry", "default", "-n", args.namespace, "-o", "json"])

    logical_count = int(((registry.get("status") or {}).get("currentAllocation") or {}).get("logicalGpuCount") or 0)
    routes = get_json(args.router_url.rstrip("/") + "/routes")
    route_count = len(routes.get("routes") or [])
    if logical_count == 0 and route_count == 0 and not dirty_physical_gpus(registry):
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


def run_warmup(args: argparse.Namespace, run_id: str, out_dir: Path) -> None:
    if args.skip_reset:
        print("skipping warm-up because --skip-reset is set", flush=True)
        return
    print("\n=== warm-up/prepull: pulling runtime images and initializing runtimes ===", flush=True)
    registry = kubectl_json(["get", "physicalgpuregistry", "default", "-n", args.namespace, "-o", "json"])
    nodes = gpu_node_names(registry)
    if nodes:
        for node in nodes:
            for image_kind, image in runtime_warmup_images().items():
                name = warmup_pod_name(run_id, image_kind, node)
                create_image_pull_pod(args.namespace, name, node, image)
                wait_pod_succeeded(args.namespace, name, timeout_s=600.0)
        write_json(out_dir / "warmup_prepull.json", {"nodes": nodes, "images": runtime_warmup_images()})
    else:
        print("warm-up/prepull: no GPU nodes found in registry; skipping image pull pods", flush=True)

    zero = {workload: 0.0 for workload in WORKLOADS}
    target = {workload: 0.0 for workload in WORKLOADS}
    target["resnet50_image"] = 50.0
    target["gpt2_p64_o64"] = 0.05
    target["llama_p1024_o128"] = 0.05
    snapshot = create_arrival_snapshot(args.namespace, f"{run_id}-warmup", zero, target, args.planner)
    plan = wait_plan(args.namespace, "plan-" + snapshot, timeout_s=args.transition_timeout_s)
    write_json(out_dir / "warmup_plan.json", plan)
    write_csv(out_dir / "warmup_runtime_readiness.csv", runtime_readiness_rows(-1, "plan-" + snapshot, plan, phase="warmup"))
    if plan_phase(plan) != "Executed":
        raise RuntimeError("warm-up plan failed: " + str((plan.get("status") or {}).get("message")))
    ensure_empty_cluster(args, f"{run_id}-post-warmup", out_dir)


def runtime_warmup_images() -> dict[str, str]:
    return {
        "vision": "localhost:10690/migrant-model-runtime:torchvision-20260602",
        "llm": "localhost:10690/migrant-model-runtime:llm-transformers-20260622",
    }


def warmup_pod_name(run_id: str, image_kind: str, node: str) -> str:
    digest = hashlib.sha1(f"{run_id}|{image_kind}|{node}".encode("utf-8")).hexdigest()[:10]
    return sanitize(f"warmup-{image_kind}-{node}-{digest}")


def gpu_node_names(registry: dict[str, Any]) -> list[str]:
    nodes: set[str] = set()
    for raw in ((registry.get("status") or {}).get("bindings") or {}).values():
        binding = raw if isinstance(raw, dict) else {}
        node = str(binding.get("node") or binding.get("nodeName") or "")
        if node:
            nodes.add(node)
    return sorted(nodes)


def create_image_pull_pod(namespace: str, name: str, node: str, image: str) -> None:
    body = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "migrant-warmup",
                "mig.or-sim.io/component": "real-8gpu-k8s-runner",
            },
        },
        "spec": {
            "restartPolicy": "Never",
            "nodeSelector": {"kubernetes.io/hostname": node},
            "tolerations": [{"operator": "Exists"}],
            "containers": [{
                "name": "pull",
                "image": image,
                "imagePullPolicy": "IfNotPresent",
                "command": ["sh", "-c", "true"],
            }],
        },
    }
    kubectl(["delete", "pod", name, "-n", namespace, "--ignore-not-found=true"], check=False)
    kubectl_create(body)


def wait_pod_succeeded(namespace: str, name: str, timeout_s: float) -> None:
    deadline = time.time() + timeout_s
    last_phase = ""
    while time.time() < deadline:
        pod = kubectl_json(["get", "pod", name, "-n", namespace, "-o", "json"], check=False)
        if pod:
            phase = str((pod.get("status") or {}).get("phase") or "")
            if phase != last_phase:
                print(f"{name}: phase={phase}", flush=True)
                last_phase = phase
            if phase == "Succeeded":
                return
            if phase == "Failed":
                raise RuntimeError(f"warm-up image pull pod {name} failed")
        time.sleep(2.0)
    raise TimeoutError(f"timed out waiting for warm-up image pull pod {name}; last phase={last_phase}")


def dirty_physical_gpus(registry: dict[str, Any]) -> list[dict[str, Any]]:
    status = registry.get("status") or {}
    bindings = (status.get("bindings") or {})
    dirty = []
    for physical_id, raw in sorted(bindings.items()):
        binding = raw if isinstance(raw, dict) else {}
        state = str(binding.get("state") or "")
        cleanliness = str(binding.get("cleanliness") or "")
        has_binding = bool(binding.get("activeLogicalGpuId") or binding.get("pendingLogicalGpuId") or binding.get("logicalBinding"))
        has_mig = bool(binding.get("migDevices") or binding.get("logicalMigSlots"))
        has_runtime = bool(binding.get("runtimeBindings"))
        if state != "available" or cleanliness not in {"", "empty"} or has_binding or has_mig or has_runtime:
            gpu = dict(binding)
            gpu["physicalGpuId"] = str(gpu.get("physicalGpuId") or physical_id)
            dirty.append(gpu)
    return dirty


def create_deep_repair_plan(namespace: str, run_id: str, gpu: dict[str, Any]) -> str:
    physical_id = str(gpu.get("physicalGpuId") or "")
    node = str(gpu.get("node") or gpu.get("nodeName") or physical_id.split("-gpu", 1)[0])
    gpu_index = int(gpu.get("gpuIndex") if gpu.get("gpuIndex") is not None else gpu.get("deviceIndex") or 0)
    safe_physical = sanitize(physical_id)
    name = sanitize(f"{run_id}-repair-{safe_physical}-{int(time.time())}")
    body = {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "MigActionPlan",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "migrant-go",
                "mig.or-sim.io/component": "real-8gpu-k8s-runner",
                "mig.or-sim.io/plan-type": "repair",
            },
        },
        "spec": {
            "abstractActions": [],
            "actionCount": 3,
            "actionDag": {
                "format": "migrant.action-dag/v1",
                "name": name,
                "nodes": [
                    {
                        "id": f"repair-delete-instances-{safe_physical}",
                        "index": 0,
                        "phase": 0,
                        "type": "delete_instance",
                        "action": repair_action("delete_instance", node, gpu_index, physical_id),
                    },
                    {
                        "id": f"repair-clear-template-{safe_physical}",
                        "index": 1,
                        "phase": 1,
                        "type": "clear_template",
                        "dependsOn": [f"repair-delete-instances-{safe_physical}"],
                        "action": repair_action("clear_template", node, gpu_index, physical_id),
                    },
                    {
                        "id": f"repair-return-gpu-{safe_physical}",
                        "index": 2,
                        "phase": 2,
                        "type": "return_gpu",
                        "dependsOn": [f"repair-clear-template-{safe_physical}"],
                        "action": repair_action("return_gpu", node, gpu_index, physical_id),
                    },
                ],
            },
            "currentAllocationRef": "physicalgpuregistries/default",
            "executor": "go-transition-executor",
            "phaseGate": "auto",
            "plannerMetadata": {
                "planner": "real-8gpu-k8s-runner",
                "reason": f"Delete residual runtimes and clear {physical_id} before experiment reset",
            },
            "summary": {"desiredRuntimes": [], "planType": "repair", "sourceGpuCount": 0, "targetGpuCount": 0},
            "targetGpuCount": 0,
        },
    }
    kubectl_create(body)
    return name


def repair_action(action_type: str, node: str, gpu_index: int, physical_id: str) -> dict[str, Any]:
    return {
        "type": action_type,
        "abstractAction": "Deep Repair Dirty GPU",
        "node": node,
        "gpuIndex": gpu_index,
        "gpu": physical_id,
        "physicalGpuId": physical_id,
        "physical_gpu_id": physical_id,
    }


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
        self.route_lock = threading.Lock()
        self.active_routes: dict[str, list[dict[str, Any]]] = {}

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
        with ThreadPoolExecutor(max_workers=256) as pool:
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
        if is_vision_workload(model):
            return self.schedule_vision_batches(pool, model, count, start, end)
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

    def schedule_vision_batches(self, pool: ThreadPoolExecutor, model: str, logical_count: int, start: float, end: float) -> list[Any]:
        duration = max(0.0, end - start)
        batches = vision_batches_for_routes(logical_count, self.routes_for_model(model))
        if not batches:
            batches = [logical_count]
        interval = duration / max(1, len(batches))
        futures = []
        for idx, batch_count in enumerate(batches):
            target_at = start + (idx + 0.5) * interval
            seq = self.seq.get(model, 0)
            self.seq[model] = seq + int(batch_count)
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
                    int(batch_count),
                )
            )
        return futures

    def routes_for_model(self, model: str) -> list[dict[str, Any]]:
        with self.route_lock:
            return [dict(route) for route in self.active_routes.get(model, [])]

    def sample_routes(self, relative_s: float) -> None:
        try:
            routes = get_json(self.router + "/routes", timeout_s=5.0).get("routes") or []
            by_model: dict[str, dict[str, Any]] = {}
            active_by_model: dict[str, list[dict[str, Any]]] = {}
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
                active_by_model.setdefault(model, []).append(route)
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
            with self.route_lock:
                self.active_routes = active_by_model
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


def send_request(router: str, stage: str, phase: str, model: str, seq: int, target_at: float, timeout_s: float, logical_count: int = 1) -> dict[str, Any]:
    logical_count = max(1, int(logical_count))
    sleep_until(target_at)
    sent = time.time()
    start = time.perf_counter()
    row = {
        "stage": stage,
        "phase": phase,
        "model": model,
        "seq": seq,
        "logicalRequestCount": logical_count,
        "sentAt": sent,
        "ok": False,
        "status": "",
        "latencyMs": "",
        "serviceLatencyMs": "",
        "queueWaitMs": "",
        "e2eLatencyMs": "",
        "routerBatchSize": "",
        "runtimeLatencyMs": "",
        "ttftMs": "",
        "tpotMs": "",
        "error": "",
    }
    try:
        body: dict[str, Any] = {"benchmark": True, "sentAt": sent}
        config = slo_for_workload(model)
        body.update({"requestClass": config["requestClass"]})
        if is_vision_workload(model) and logical_count > 1:
            body.update({"driverBatched": True, "logicalRequestCount": logical_count, "batch": logical_count})
        if config.get("promptLen") is not None:
            body.update({"prompt_len": int(config["promptLen"]), "output_tokens": int(config["outputTokens"]), "batch": 1})
        resp = post_json(router + "/infer/" + model, body, timeout_s=timeout_s)
        row["ok"] = True
        row["status"] = 200
        row["runtimeLatencyMs"] = resp.get("runtimeLatencyMs", "")
        row["serviceLatencyMs"] = first_present(resp, "serviceLatencyMs", "runtimeLatencyMs", "latencyMs")
        row["queueWaitMs"] = first_present(resp, "queueWaitMs")
        row["e2eLatencyMs"] = first_present(resp, "e2eLatencyMs")
        row["routerBatchSize"] = first_present(resp, "routerBatchSize")
        row["ttftMs"] = first_present(resp, "ttftMs", "ttft_ms", "runtimeTtftMs")
        row["tpotMs"] = first_present(resp, "tpotMs", "tpot_ms", "runtimeTpotMs")
    except urllib.error.HTTPError as exc:
        row["status"] = exc.code
        row["error"] = str(exc)
    except Exception as exc:
        row["error"] = str(exc)
    row["latencyMs"] = round((time.perf_counter() - start) * 1000.0, 6)
    return row


def is_vision_workload(model: str) -> bool:
    return model in {"resnet50", "vgg16", "vit_base", "resnet50_image", "vgg16_image", "vit_base_image"} or str(model).endswith("_image")


def vision_batches_for_routes(logical_count: int, routes: list[dict[str, Any]]) -> list[int]:
    logical_count = max(0, int(logical_count))
    if logical_count <= 0:
        return []
    active = [route for route in routes if route.get("active") and route.get("acceptingNew") and not route.get("draining")]
    if not active:
        raise RuntimeError("vision workload has demand but no active route with real batch metadata")
    weights = []
    for route in active:
        weight = optional_float(route.get("capacity"))
        if weight is None or weight <= 0:
            weight = optional_float(route.get("weight"))
        if weight is None or weight <= 0:
            weight = 1.0
        batch = int(optional_float(route.get("driverBatchSize")) or optional_float(route.get("batchSize")) or 0)
        if batch <= 0:
            raise RuntimeError(
                "vision route is missing a real batch size: "
                f"runtimeId={route.get('runtimeId')} model={route.get('model')} "
                f"batchSize={route.get('batchSize')} driverBatchSize={route.get('driverBatchSize')}"
            )
        weights.append({"weight": weight, "batch": max(1, batch)})
    total_weight = sum(item["weight"] for item in weights)
    remaining = logical_count
    assigned = []
    fractions = []
    for idx, item in enumerate(weights):
        exact = logical_count * item["weight"] / total_weight if total_weight > 0 else logical_count / len(weights)
        whole = int(math.floor(exact))
        assigned.append(whole)
        remaining -= whole
        fractions.append((exact - whole, idx))
    for _, idx in sorted(fractions, reverse=True):
        if remaining <= 0:
            break
        assigned[idx] += 1
        remaining -= 1
    batches: list[int] = []
    for count, item in zip(assigned, weights, strict=False):
        batches.extend(chunk_count(count, item["batch"]))
    return batches


def chunk_count(count: int, chunk_size: int) -> list[int]:
    count = max(0, int(count))
    chunk_size = max(1, int(chunk_size))
    out = []
    while count > 0:
        take = min(chunk_size, count)
        out.append(take)
        count -= take
    return out


def p95_slo_metrics_for_stage(
    stage: str,
    request_rows: list[dict[str, Any]],
    transition_started_at: Any = None,
    transition_finished_at: Any = None,
    bucket_seconds: float = 1.0,
) -> dict[str, Any]:
    window_start = parse_rfc3339_seconds(transition_started_at)
    window_end = parse_rfc3339_seconds(transition_finished_at)
    by_model_bucket: dict[str, dict[int, dict[str, list[float]]]] = {model: {} for model in WORKLOADS}
    for row in request_rows:
        if row.get("stage") != stage or row.get("phase") != "transition":
            continue
        model = str(row.get("model") or "")
        if model not in by_model_bucket:
            continue
        try:
            sent_at = float(row.get("sentAt"))
            latency = float(first_present(row, "serviceLatencyMs", "latencyMs"))
        except (TypeError, ValueError):
            continue
        if window_start is not None and sent_at < window_start:
            continue
        if window_end is not None and sent_at > window_end:
            continue
        bucket = int(math.floor(sent_at / bucket_seconds))
        metrics = by_model_bucket[model].setdefault(bucket, {"latency": [], "ttft": [], "tpot": []})
        logical_count = logical_request_count(row)
        metrics["latency"].extend([latency] * logical_count)
        ttft = optional_float(row.get("ttftMs"))
        tpot = optional_float(row.get("tpotMs"))
        config = slo_for_workload(model)
        if ttft is not None:
            metrics["ttft"].extend([ttft] * logical_count)
        elif config.get("tpotMs") is not None:
            metrics["ttft"].extend([latency] * logical_count)
        if tpot is not None:
            metrics["tpot"].extend([tpot] * logical_count)

    intervals: list[tuple[float, float]] = []
    by_model: dict[str, Any] = {}
    for model, buckets in by_model_bucket.items():
        config = slo_for_workload(model)
        latency_slo_ms = float(config["latencyMs"])
        tpot_slo_ms = config.get("tpotMs")
        violating = []
        p95_values = []
        ttft_p95_values = []
        tpot_p95_values = []
        epoch_latency_values: list[float] = []
        epoch_ttft_values: list[float] = []
        epoch_tpot_values: list[float] = []
        request_count = 0
        for bucket, metric_values in sorted(buckets.items()):
            latency_values = metric_values.get("latency") or []
            ttft_values = metric_values.get("ttft") or []
            tpot_values = metric_values.get("tpot") or []
            request_count += len(latency_values)
            epoch_latency_values.extend(latency_values)
            epoch_ttft_values.extend(ttft_values)
            epoch_tpot_values.extend(tpot_values)
            latency_p95 = percentile(latency_values, 95.0) if latency_values else 0.0
            ttft_p95 = percentile(ttft_values, 95.0) if ttft_values else 0.0
            tpot_p95 = percentile(tpot_values, 95.0) if tpot_values else 0.0
            p95_values.append(latency_p95)
            if ttft_values:
                ttft_p95_values.append(ttft_p95)
            if tpot_values:
                tpot_p95_values.append(tpot_p95)
            latency_violated = latency_p95 > latency_slo_ms
            ttft_violated = bool(ttft_values) and ttft_p95 > latency_slo_ms
            tpot_violated = tpot_slo_ms is not None and bool(tpot_values) and tpot_p95 > float(tpot_slo_ms)
            if latency_violated or ttft_violated or tpot_violated:
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
                    "p95LatencyMs": round(latency_p95, 3),
                    "p95TtftMs": round(ttft_p95, 3) if ttft_values else "",
                    "p95TpotMs": round(tpot_p95, 3) if tpot_values else "",
                    "latencyViolated": latency_violated,
                    "ttftViolated": ttft_violated,
                    "tpotViolated": tpot_violated,
                    "requestCount": len(latency_values),
                })
        epoch_latency_p95 = percentile(epoch_latency_values, 95.0) if epoch_latency_values else 0.0
        epoch_ttft_p95 = percentile(epoch_ttft_values, 95.0) if epoch_ttft_values else 0.0
        epoch_tpot_p95 = percentile(epoch_tpot_values, 95.0) if epoch_tpot_values else 0.0
        epoch_latency_violated = bool(epoch_latency_values) and epoch_latency_p95 > latency_slo_ms
        epoch_ttft_violated = bool(epoch_ttft_values) and epoch_ttft_p95 > latency_slo_ms
        epoch_tpot_violated = tpot_slo_ms is not None and bool(epoch_tpot_values) and epoch_tpot_p95 > float(tpot_slo_ms)
        epoch_p95_violated = epoch_latency_violated or epoch_ttft_violated or epoch_tpot_violated
        by_model[model] = {
            "latencySLOMs": latency_slo_ms,
            "requestClass": config["requestClass"],
            "ttftSLOMs": latency_slo_ms if config.get("tpotMs") is not None else None,
            "tpotSLOMs": tpot_slo_ms,
            "bucketSeconds": bucket_seconds,
            "requestCount": request_count,
            "bucketCount": len(buckets),
            "violatingBucketCount": len(violating),
            "p95SLOViolationSeconds": round(union_seconds([(float(v["bucketStart"]), float(v["bucketEnd"])) for v in violating]), 6),
            "maxBucketP95LatencyMs": round(max(p95_values), 3) if p95_values else 0.0,
            "maxBucketP95TtftMs": round(max(ttft_p95_values), 3) if ttft_p95_values else 0.0,
            "maxBucketP95TpotMs": round(max(tpot_p95_values), 3) if tpot_p95_values else 0.0,
            "epochP95LatencyMs": round(epoch_latency_p95, 3) if epoch_latency_values else 0.0,
            "epochP95TtftMs": round(epoch_ttft_p95, 3) if epoch_ttft_values else 0.0,
            "epochP95TpotMs": round(epoch_tpot_p95, 3) if epoch_tpot_values else 0.0,
            "epochP95Violated": epoch_p95_violated,
            "epochP95LatencyViolated": epoch_latency_violated,
            "epochP95TtftViolated": epoch_ttft_violated,
            "epochP95TpotViolated": epoch_tpot_violated,
            "violatingBuckets": violating[:20],
            "truncatedViolatingBuckets": max(0, len(violating) - 20),
        }

    out = {
        "sloViolationDurationSec": round(union_seconds(intervals), 6),
        "sloViolationP95BucketSec": round(union_seconds(intervals), 6),
        "sloP95BucketSeconds": bucket_seconds,
        "sloP95ByModel": json.dumps(by_model, sort_keys=True, separators=(",", ":")),
    }
    out.update(request_slo_metrics_for_stage(stage, request_rows, transition_started_at, transition_finished_at))
    return out


def request_slo_metrics_for_stage(
    stage: str,
    request_rows: list[dict[str, Any]],
    transition_started_at: Any = None,
    transition_finished_at: Any = None,
) -> dict[str, Any]:
    window_start = parse_rfc3339_seconds(transition_started_at)
    window_end = parse_rfc3339_seconds(transition_finished_at)
    by_model: dict[str, dict[str, Any]] = {
        model: {"requestCount": 0, "violationCount": 0, "errorCount": 0}
        for model in WORKLOADS
    }
    total = 0
    violated = 0
    errors = 0
    for row in request_rows:
        if row.get("stage") != stage or row.get("phase") != "transition":
            continue
        model = str(row.get("model") or "")
        if model not in by_model:
            continue
        try:
            sent_at = float(row.get("sentAt"))
        except (TypeError, ValueError):
            continue
        if window_start is not None and sent_at < window_start:
            continue
        if window_end is not None and sent_at > window_end:
            continue
        logical_count = logical_request_count(row)
        failed = not bool(row.get("ok")) or str(row.get("status") or "") not in {"", "200"}
        is_violation = request_violates_slo(model, row) or failed
        by_model[model]["requestCount"] += logical_count
        total += logical_count
        if failed:
            by_model[model]["errorCount"] += logical_count
            errors += logical_count
        if is_violation:
            by_model[model]["violationCount"] += logical_count
            violated += logical_count

    for model, stats in by_model.items():
        count = int(stats["requestCount"])
        stats["violationRate"] = round(float(stats["violationCount"]) / count, 6) if count else 0.0
        stats["latencySLOMs"] = slo_for_workload(model)["latencyMs"]
        stats["tpotSLOMs"] = slo_for_workload(model).get("tpotMs")
    return {
        "sloTransitionRequestCount": total,
        "sloViolationRequestCount": violated,
        "sloRequestErrorCount": errors,
        "sloViolationRate": round(float(violated) / total, 6) if total else 0.0,
        "sloViolationRateByModel": json.dumps(by_model, sort_keys=True, separators=(",", ":")),
    }


def request_violates_slo(model: str, row: dict[str, Any]) -> bool:
    config = slo_for_workload(model)
    latency_slo_ms = float(config["latencyMs"])
    tpot_slo_ms = config.get("tpotMs")
    service_latency = optional_float(first_present(row, "serviceLatencyMs", "latencyMs"))
    ttft = optional_float(first_present(row, "ttftMs", "ttft_ms", "runtimeTtftMs"))
    tpot = optional_float(first_present(row, "tpotMs", "tpot_ms", "runtimeTpotMs"))
    if tpot_slo_ms is None:
        return service_latency is not None and service_latency > latency_slo_ms
    ttft_value = ttft if ttft is not None else service_latency
    ttft_bad = ttft_value is not None and ttft_value > latency_slo_ms
    tpot_bad = tpot is not None and tpot > float(tpot_slo_ms)
    return bool(ttft_bad or tpot_bad)


def logical_request_count(row: dict[str, Any]) -> int:
    try:
        return max(1, int(float(row.get("logicalRequestCount") or 1)))
    except (TypeError, ValueError):
        return 1


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(0, min(len(ordered) - 1, math.ceil((pct / 100.0) * len(ordered)) - 1))
    return ordered[rank]


def optional_float(value: Any) -> float | None:
    try:
        if value in ("", None):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def first_present(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in ("", None):
            return value
    return ""


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
    action_summary = metrics.get("actionSummary") or {}
    dag_node_count = spec.get("actionCount")
    pod_create_count = int(action_summary.get("createdInstanceCount") or 0)
    pod_delete_count = int(action_summary.get("deletedInstanceCount") or 0)
    mig_reconfig_op_count = int(action_summary.get("reconfigurationNodes") or 0)
    mig_partition_create_count = int(action_summary.get("createdMIGSlotCount") or 0)
    mig_partition_delete_count = int(action_summary.get("deletedMIGSlotCount") or 0)
    physical_action_count = pod_create_count + pod_delete_count + mig_reconfig_op_count
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
        "actionCount": physical_action_count,
        "physicalActionCount": physical_action_count,
        "dagNodeCount": dag_node_count,
        "podCreateCount": pod_create_count,
        "podDeleteCount": pod_delete_count,
        "migReconfigOpCount": mig_reconfig_op_count,
        "migPartitionCreateCount": mig_partition_create_count,
        "migPartitionDeleteCount": mig_partition_delete_count,
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


def runtime_readiness_rows(epoch: int, plan_name: str, plan: dict[str, Any], phase: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for action in (plan.get("status") or {}).get("actionStatuses") or []:
        if not isinstance(action, dict):
            continue
        readiness = action.get("runtimeReadiness") or {}
        if not isinstance(readiness, dict):
            continue
        durations = action.get("durationsSeconds") or {}
        for runtime_id, raw in readiness.items():
            item = raw if isinstance(raw, dict) else {}
            row = {
                "phase": phase,
                "epoch": epoch,
                "plan": plan_name,
                "actionId": action.get("id"),
                "actionType": action.get("type"),
                "runtimeId": runtime_id,
                "model": item.get("model"),
                "node": item.get("node"),
                "gpu": item.get("gpu"),
                "profile": item.get("profile"),
                "slotResource": item.get("slotResource"),
                "deviceResource": item.get("deviceResource"),
                "hostPort": item.get("hostPort"),
                "pod": item.get("pod"),
                "containerReady": item.get("containerReady"),
                "runtimeReadyAndCUDAVerifySec": durations.get("runtimeReadyAndCUDAVerify"),
                "podCreatedSinceDeploymentSec": item.get("podCreatedAtSinceDeploymentSeconds"),
                "podScheduledSinceDeploymentSec": item.get("podScheduledAtSinceDeploymentSeconds"),
                "podStartSinceDeploymentSec": item.get("podStartTimeSinceDeploymentSeconds"),
                "containerStartedSinceDeploymentSec": item.get("containerStartedAtSinceDeploymentSeconds"),
                "healthReadySinceDeploymentSec": item.get("healthReadyAtSinceDeploymentSeconds"),
                "cudaProcessFoundSinceDeploymentSec": item.get("cudaProcessFoundAtSinceDeploymentSeconds"),
                "healthModelId": item.get("healthModelId"),
                "healthRuntimeMode": item.get("healthRuntimeMode"),
                "healthDevice": item.get("healthDevice"),
                "healthLoaded": item.get("healthLoaded"),
            }
            load_timings = item.get("loadTimings") if isinstance(item.get("loadTimings"), dict) else {}
            for key, value in load_timings.items():
                row[f"loadTiming_{key}"] = value
            row["runtimeWaitSec"] = first_present(row, "cudaProcessFoundSinceDeploymentSec", "healthReadySinceDeploymentSec", "containerStartedSinceDeploymentSec")
            rows.append(row)
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
    readiness: list[dict[str, Any]],
    gpu: list[dict[str, Any]],
    allocations: list[dict[str, Any]],
    failures: list[dict[str, Any]],
) -> None:
    refresh_p95_slo_metrics(transitions, requests)
    write_csv(out_dir / "requests.csv", requests)
    write_csv(out_dir / "service_rate_samples.csv", routes)
    write_csv(out_dir / "transition_metrics.csv", transitions)
    write_csv(out_dir / "action_statuses.csv", actions)
    write_csv(out_dir / "runtime_readiness.csv", readiness)
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


def kubectl_create(body: dict[str, Any]) -> None:
    proc = subprocess.run(["kubectl", "create", "-f", "-"], input=json.dumps(body), text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode != 0:
        name = ((body.get("metadata") or {}).get("name") or "").strip()
        hint = f" Object {name!r} already exists; choose a fresh --run-id." if "AlreadyExists" in proc.stderr else ""
        raise RuntimeError(f"kubectl create failed: {proc.stderr.strip()}{hint}")
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
	return "".join(out).strip("-")[:63].strip("-")


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
