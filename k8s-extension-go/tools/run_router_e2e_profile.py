#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import statistics
import time
import urllib.request
from pathlib import Path
from types import SimpleNamespace
from typing import Any


PROFILE_RUNNER = Path(__file__).with_name("run_k8s_profile_matrix.py")
spec = importlib.util.spec_from_file_location("run_k8s_profile_matrix", PROFILE_RUNNER)
if spec is None or spec.loader is None:
    raise RuntimeError(f"cannot load {PROFILE_RUNNER}")
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    node_ip = args.node_ip or runner.node_internal_ip(args.node)
    runtime_id = f"router-e2e-{args.model}-{args.profile}-p{args.prompt_len}-o{args.output_tokens}"
    slot_resource = f"or-sim.io/{args.node}-gpu{args.gpu_index}-s0-{runner.MIG_PLACEMENT_SIZE[args.profile]}-{args.profile}"
    route_model = args.route_model or args.model
    router = args.router_url.rstrip("/")
    runtime_args = SimpleNamespace(
        node=args.node,
        gpu_index=args.gpu_index,
        namespace=args.namespace,
        vision_image=args.vision_image,
        llm_image=args.llm_image,
        port=args.runtime_port,
        vision_warmup=args.warmup,
        llm_warmup=args.warmup,
    )
    item = build_item(args)
    rows_path = out_dir / f"{args.run_id}-raw.csv"
    report_path = out_dir / f"{args.run_id}.md"

    row: dict[str, Any] = {}
    try:
        runner.set_repair_paused(args.node, True)
        runner.cleanup_runtime_pod(args.namespace)
        slots = runner.configure_mig(runtime_args, args.profile, node_ip)
        slot = runner.pick_slot(slots, args.profile)
        mig_uuid = slot.get("migDeviceUuid", "")
        device_resource = runner.mig_uuid_resource(mig_uuid) if mig_uuid else ""
        allocation_resource = slot_resource
        runner.wait_allocatable(args.node, slot_resource, min_value=1, timeout_s=120)
        runner.deploy_runtime_pod(runtime_args, item, allocation_resource, mig_uuid, slot_resource)
        runner.wait_runtime_pod_running(args.namespace, timeout_s=120)
        runner.wait_health(args.namespace, node_ip, args.runtime_port, args.health_timeout_s)
        endpoint = f"http://{node_ip}:{args.runtime_port}"
        route = {
            "model": route_model,
            "runtimeId": runtime_id,
            "endpoint": endpoint,
            "weight": 1,
            "capacity": args.capacity,
            "profile": args.profile,
            "batchSize": item["batch"],
            "gpu": f"{args.node}-gpu{args.gpu_index}",
            "slotResource": slot_resource,
            "deviceResource": device_resource or allocation_resource,
            "expectedMigUuid": mig_uuid,
            "active": True,
            "acceptingNew": True,
        }
        delete_url(f"{router}/control/routes?model={route_model}", timeout_s=30)
        post_json(f"{router}/control/routes", route, timeout_s=30)
        latencies: list[float] = []
        runtime_latencies: list[float] = []
        errors = 0
        for _ in range(args.requests):
            started = time.perf_counter()
            try:
                payload = {"benchmark": True}
                if item["family"] == "llm":
                    payload.update({"prompt_len": item["prompt_len"], "output_tokens": item["output_tokens"], "batch": item["batch"]})
                response = post_json(f"{router}/infer/{route_model}", payload, timeout_s=args.infer_timeout_s)
                runtime_latencies.append(float(response.get("runtimeLatencyMs", 0.0) or 0.0))
            except Exception:
                errors += 1
            latencies.append((time.perf_counter() - started) * 1000.0)
        metrics = get_json(f"{endpoint}/metrics", timeout_s=30)
        routes = get_json(f"{router}/routes", timeout_s=30).get("routes", [])
        observations = get_json(f"{router}/metrics/profile-observations", timeout_s=30).get("observations", [])
        row = summarize(args, route_model, runtime_id, node_ip, endpoint, mig_uuid, slot_resource, device_resource or allocation_resource, latencies, runtime_latencies, errors, metrics)
        write_csv(rows_path, row)
        report_path.write_text(render_report(row, routes, observations), encoding="utf-8")
        print(report_path)
    finally:
        try:
            delete_url(f"{router}/control/routes?model={route_model}&runtimeId={runtime_id}", timeout_s=30)
        except Exception:
            pass
        runner.cleanup_runtime_pod(args.namespace)
        runner.set_repair_paused(args.node, False)
        try:
            runner.post_json_allow_error(f"http://{node_ip}:10684/clear?gpuIndex={args.gpu_index}", {}, timeout_s=180)
        except Exception:
            pass
    return 0


def build_item(args: argparse.Namespace) -> dict[str, Any]:
    if args.family == "vision":
        return {
            "family": "vision",
            "workload": args.model,
            "model_name": args.model,
            "batch": args.batch,
            "profile": args.profile,
        }
    model_id = "gpt2-medium" if args.model == "gpt2" else "meta-llama/Llama-3.2-3B"
    model_name = "gpt2-medium" if args.model == "gpt2" else "llama32_3b"
    return {
        "family": "llm",
        "workload": f"{args.model}_p{args.prompt_len}_o{args.output_tokens}",
        "model_name": model_name,
        "model_id": model_id,
        "batch": 1,
        "prompt_len": args.prompt_len,
        "output_tokens": args.output_tokens,
        "profile": args.profile,
    }


def summarize(
    args: argparse.Namespace,
    route_model: str,
    runtime_id: str,
    node_ip: str,
    endpoint: str,
    mig_uuid: str,
    slot_resource: str,
    device_resource: str,
    e2e: list[float],
    runtime: list[float],
    errors: int,
    metrics: dict[str, Any],
) -> dict[str, Any]:
    e2e_mean = mean(e2e)
    rt_mean = mean(runtime)
    overhead = [max(0.0, e - r) for e, r in zip(e2e, runtime)]
    return {
        "run_id": args.run_id,
        "model": route_model,
        "family": args.family,
        "profile": args.profile,
        "node": args.node,
        "node_ip": node_ip,
        "gpu_index": args.gpu_index,
        "runtime_id": runtime_id,
        "endpoint": endpoint,
        "mig_uuid": mig_uuid,
        "slot_resource": slot_resource,
        "device_resource": device_resource,
        "batch": args.batch,
        "prompt_len": args.prompt_len if args.family == "llm" else "",
        "output_tokens": args.output_tokens if args.family == "llm" else "",
        "requests": len(e2e),
        "errors": errors,
        "e2e_ms_mean": round(e2e_mean, 6),
        "e2e_ms_p50": round(percentile(e2e, 0.50), 6),
        "e2e_ms_p95": round(percentile(e2e, 0.95), 6),
        "e2e_ms_p99": round(percentile(e2e, 0.99), 6),
        "runtime_ms_mean": round(rt_mean, 6),
        "runtime_ms_p50": round(percentile(runtime, 0.50), 6),
        "runtime_ms_p95": round(percentile(runtime, 0.95), 6),
        "runtime_ms_p99": round(percentile(runtime, 0.99), 6),
        "router_overhead_ms_mean": round(max(0.0, e2e_mean - rt_mean), 6),
        "router_overhead_ms_p50": round(percentile(overhead, 0.50), 6),
        "router_overhead_ms_p95": round(percentile(overhead, 0.95), 6),
        "router_overhead_ms_p99": round(percentile(overhead, 0.99), 6),
        "e2e_throughput_rps": round(1000.0 / e2e_mean, 6) if e2e_mean > 0 else 0.0,
        "runtime_reported_latency_ms": metrics.get("runtimeLatencyMs", 0.0),
        "runtime_reported_throughput": metrics.get("runtimeThroughput", 0.0),
    }


def render_report(row: dict[str, Any], routes: list[dict[str, Any]], observations: list[dict[str, Any]]) -> str:
    lines = [
        f"# Router E2E Profile: {row['model']}",
        "",
        "| Metric | Value |",
        "|---|---:|",
    ]
    for key in [
        "profile",
        "batch",
        "prompt_len",
        "output_tokens",
        "requests",
        "errors",
        "e2e_ms_mean",
        "e2e_ms_p50",
        "e2e_ms_p95",
        "e2e_ms_p99",
        "runtime_ms_mean",
        "runtime_ms_p95",
        "router_overhead_ms_mean",
        "router_overhead_ms_p95",
        "e2e_throughput_rps",
    ]:
        lines.append(f"| {key} | {row[key]} |")
    lines.extend(["", "## Route Snapshot", "", "```json", json.dumps(routes, indent=2, sort_keys=True), "```"])
    lines.extend(["", "## Profile Observations", "", "```json", json.dumps(observations, indent=2, sort_keys=True), "```", ""])
    return "\n".join(lines)


def write_csv(path: Path, row: dict[str, Any]) -> None:
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def post_json(url: str, payload: dict[str, Any], timeout_s: int) -> dict[str, Any]:
    raw = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=raw, headers={"content-type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode())


def get_json(url: str, timeout_s: int) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode())


def delete_url(url: str, timeout_s: int) -> dict[str, Any]:
    req = urllib.request.Request(url, method="DELETE")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode())


def mean(values: list[float]) -> float:
    return statistics.mean(values) if values else 0.0


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * q))))
    return ordered[idx]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one router E2E profile point.")
    parser.add_argument("--node", default="ampere")
    parser.add_argument("--node-ip", default="", help="override Kubernetes InternalIP; useful when node addresses are stale")
    parser.add_argument("--gpu-index", default="0")
    parser.add_argument("--namespace", default="or-sim")
    parser.add_argument("--router-url", default="http://115.145.179.144:10680")
    parser.add_argument("--out-dir", default="profile/router-e2e")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--family", choices=["vision", "llm"], required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--route-model", default="")
    parser.add_argument("--profile", required=True)
    parser.add_argument("--batch", type=int, default=1)
    parser.add_argument("--prompt-len", type=int, default=64)
    parser.add_argument("--output-tokens", type=int, default=64)
    parser.add_argument("--requests", type=int, default=10)
    parser.add_argument("--capacity", type=float, default=1.0)
    parser.add_argument("--runtime-port", type=int, default=10680)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--health-timeout-s", type=int, default=1200)
    parser.add_argument("--infer-timeout-s", type=int, default=1800)
    parser.add_argument("--vision-image", default="localhost:10690/migrant-model-runtime:torchvision-20260602")
    parser.add_argument("--llm-image", default="localhost:10690/migrant-model-runtime:llm-transformers-20260622")
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
