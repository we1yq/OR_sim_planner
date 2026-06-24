#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import select
import socket
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


MIG_PLACEMENT_SIZE = {"1g": 1, "2g": 2, "3g": 4, "4g": 4, "7g": 8}
PROFILE_ORDER = ["1g", "2g", "3g", "4g", "7g"]

VISION_MODELS = [
    ("resnet50", "resnet50"),
    ("vgg16", "vgg16"),
    ("vit_base", "vit_base"),
]
LLM_MODELS = [
    ("gpt2", "gpt2-medium", "gpt2-medium"),
    ("llama32_3b", "llama32_3b", "meta-llama/Llama-3.2-3B"),
]


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = args.run_id or datetime.now(timezone.utc).strftime("profile-%Y%m%dT%H%M%SZ")
    rows_path = out_dir / f"{run_id}-runtime-raw.csv"
    summary_path = out_dir / f"{run_id}-capacity-summary.md"
    row_fieldnames = raw_fieldnames()

    node_ip = node_internal_ip(args.node)
    print(f"profile run_id={run_id} node={args.node} node_ip={node_ip} gpu_index={args.gpu_index}", flush=True)

    rows: list[dict[str, Any]] = []
    try:
        set_repair_paused(args.node, True)
        for item in selected_matrix(args):
            row = run_one_setting(args, run_id, node_ip, item)
            rows.append(row)
            append_csv(rows_path, row_fieldnames, row)
            write_summary(summary_path, rows)
            print(format_progress(row), flush=True)
    finally:
        cleanup_runtime_pod(args.namespace)
        set_repair_paused(args.node, False)

    write_summary(summary_path, rows)
    print(f"wrote raw rows: {rows_path}")
    print(f"wrote summary: {summary_path}")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Kubernetes pod-based MIG profile matrix.")
    parser.add_argument("--node", default="rtx1-worker")
    parser.add_argument("--gpu-index", default="0", help="default targets rtx1-worker-gpu0, an A100 with MIG mode enabled")
    parser.add_argument("--namespace", default="or-sim")
    parser.add_argument("--out-dir", default="profile/current")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--profiles", default=",".join(PROFILE_ORDER))
    parser.add_argument("--families", default="vision,llm", help="vision,llm")
    parser.add_argument("--vision-models", default="resnet50,vgg16,vit_base")
    parser.add_argument("--llm-models", default="gpt2,llama32_3b")
    parser.add_argument("--vision-batches", default="1,4,16,32,64")
    parser.add_argument("--llm-prompts", default="64,512,1024")
    parser.add_argument("--llm-outputs", default="64,128")
    parser.add_argument("--vision-image", default="localhost:10690/migrant-model-runtime:torchvision-20260602")
    parser.add_argument("--llm-image", default="localhost:10690/migrant-model-runtime:llm-transformers-20260622")
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--access-mode", choices=["direct", "port-forward"], default="direct")
    parser.add_argument("--vision-warmup", type=int, default=10)
    parser.add_argument("--vision-requests", type=int, default=50)
    parser.add_argument("--llm-warmup", type=int, default=1)
    parser.add_argument("--llm-requests", type=int, default=5)
    parser.add_argument("--health-timeout-s", type=int, default=900)
    parser.add_argument("--infer-timeout-s", type=int, default=600)
    parser.add_argument("--router-url", default="http://115.145.179.144:10680")
    parser.add_argument("--e2e-requests", type=int, default=0, help="if >0, also measure router E2E latency on the same runtime pod")
    parser.add_argument("--e2e-warmup", type=int, default=1)
    parser.add_argument("--quick", action="store_true", help="small readiness matrix")
    parser.add_argument("--skip-mig-config", action="store_true")
    return parser.parse_args()


def selected_matrix(args: argparse.Namespace) -> list[dict[str, Any]]:
    profiles = split_csv(args.profiles)
    families = set(split_csv(args.families))
    out: list[dict[str, Any]] = []
    if "vision" in families:
        allowed = set(split_csv(args.vision_models))
        batches = [int(x) for x in split_csv(args.vision_batches)]
        if args.quick:
            batches = [4]
        for workload, model_name in VISION_MODELS:
            if workload not in allowed:
                continue
            for batch in batches:
                for profile in profiles:
                    out.append({"family": "vision", "workload": workload, "model_name": model_name, "batch": batch, "profile": profile})
    if "llm" in families:
        allowed = set(split_csv(args.llm_models))
        prompts = [int(x) for x in split_csv(args.llm_prompts)]
        outputs = [int(x) for x in split_csv(args.llm_outputs)]
        if args.quick:
            prompts = [64]
            outputs = [64]
        for prefix, model_name, model_id in LLM_MODELS:
            if prefix not in allowed:
                continue
            for prompt_len in prompts:
                for output_tokens in outputs:
                    workload = f"{prefix}_p{prompt_len}_o{output_tokens}"
                    for profile in profiles:
                        out.append(
                            {
                                "family": "llm",
                                "workload": workload,
                                "model_name": model_name,
                                "model_id": model_id,
                                "batch": 1,
                                "prompt_len": prompt_len,
                                "output_tokens": output_tokens,
                                "profile": profile,
                            }
                        )
    return out


def run_one_setting(args: argparse.Namespace, run_id: str, node_ip: str, item: dict[str, Any]) -> dict[str, Any]:
    profile = item["profile"]
    status = "ok"
    error = ""
    mig_uuid = ""
    device_resource = ""
    slot_resource = f"or-sim.io/{args.node}-gpu{args.gpu_index}-s0-{MIG_PLACEMENT_SIZE[profile]}-{profile}"
    port_forward: subprocess.Popen[str] | None = None
    try:
        cleanup_runtime_pod(args.namespace)
        if not args.skip_mig_config:
            slots = configure_mig(args, profile, node_ip)
            slot = pick_slot(slots, profile)
            mig_uuid = slot.get("migDeviceUuid", "")
            if mig_uuid:
                device_resource = mig_uuid_resource(mig_uuid)
            wait_allocatable(args.node, slot_resource, min_value=1, timeout_s=120)
        allocation_resource = slot_resource
        for attempt in range(2):
            cleanup_runtime_pod(args.namespace)
            stop_port_forward(port_forward)
            port_forward = None
            deploy_runtime_pod(args, item, allocation_resource, mig_uuid, slot_resource)
            try:
                wait_runtime_pod_running(args.namespace, timeout_s=120)
                if args.access_mode == "port-forward":
                    port_forward, local_port = start_port_forward(args.namespace, args.port)
                    runtime_host = "127.0.0.1"
                    runtime_port = local_port
                else:
                    runtime_host = node_ip
                    runtime_port = args.port
                wait_health(args.namespace, runtime_host, runtime_port, args.health_timeout_s)
                break
            except RuntimeError as exc:
                stop_port_forward(port_forward)
                port_forward = None
                if attempt == 0 and is_retryable_pod_admission_error(str(exc)):
                    cleanup_runtime_pod(args.namespace)
                    time.sleep(5)
                    if not args.skip_mig_config:
                        post_json(f"http://{node_ip}:10684/refresh-cdi?gpuIndex={args.gpu_index}", {}, timeout_s=120)
                        wait_allocatable(args.node, allocation_resource, min_value=1, timeout_s=120)
                    continue
                raise
        if port_forward is None:
            if args.access_mode == "port-forward":
                raise RuntimeError("runtime port-forward was not established")
            runtime_host = node_ip
            runtime_port = args.port
        samples = run_samples(args, runtime_host, runtime_port, item)
        metrics = get_json(f"http://{runtime_host}:{runtime_port}/metrics", timeout_s=30)
        e2e_samples = run_e2e_samples(args, run_id, node_ip, item, mig_uuid, device_resource or allocation_resource, slot_resource, samples)
    except Exception as exc:
        status = "error"
        error = str(exc)
        samples = []
        metrics = {}
        e2e_samples = []
    finally:
        stop_port_forward(port_forward)
        cleanup_runtime_pod(args.namespace)

    return build_row(run_id, args, item, status, error, mig_uuid, device_resource, slot_resource, samples, metrics, e2e_samples)


def configure_mig(args: argparse.Namespace, profile: str, node_ip: str) -> list[dict[str, Any]]:
    post_json_allow_error(f"http://{node_ip}:10684/clear?gpuIndex={args.gpu_index}", {}, timeout_s=180)
    create = f"0:{MIG_PLACEMENT_SIZE[profile]}:{profile}"
    res = post_json_allow_error(f"http://{node_ip}:10684/apply-slots?gpuIndex={args.gpu_index}", {"create": create}, timeout_s=180)
    slots = res.get("migSlots") or []
    if not slots:
        raise RuntimeError(f"apply-slots failed for {profile}: {res.get('message')}")
    if not res.get("success") and not any(slot.get("profile") == profile for slot in slots):
        raise RuntimeError(f"apply-slots failed for {profile}: {res.get('message')}")
    # refresh-cdi also nudges the slot device plugin in this node-agent version.
    post_json(f"http://{node_ip}:10684/refresh-cdi?gpuIndex={args.gpu_index}", {}, timeout_s=120)
    return slots


def pick_slot(slots: list[dict[str, Any]], profile: str) -> dict[str, Any]:
    for slot in slots:
        if slot.get("profile") == profile:
            return slot
    raise RuntimeError(f"no slot with profile {profile}: {slots}")


def deploy_runtime_pod(args: argparse.Namespace, item: dict[str, Any], resource: str, mig_uuid: str, slot_resource: str) -> None:
    family = item["family"]
    image = args.vision_image if family == "vision" else args.llm_image
    env: list[dict[str, Any]] = [
        {"name": "MODEL_NAME", "value": item["model_name"]},
        {"name": "OR_SIM_RUNTIME_ID", "value": "or-sim-profile-runtime"},
        {"name": "BATCH_SIZE", "value": str(item["batch"])},
        {"name": "OR_SIM_PROFILE", "value": item["profile"]},
        {"name": "OR_SIM_MIG_UUID", "value": mig_uuid},
        {"name": "OR_SIM_SLOT_RESOURCE", "value": slot_resource},
        {"name": "OR_SIM_DEVICE_RESOURCE", "value": resource},
    ]
    volume_mounts = []
    volumes = []
    init_containers = []
    if family == "vision":
        env.extend(
            [
                {"name": "RUNTIME_MODE", "value": "torchvision"},
                {"name": "TORCHVISION_WEIGHTS", "value": "default"},
                {"name": "TORCHVISION_WARMUP_ITERS", "value": str(args.vision_warmup)},
            ]
        )
    else:
        env.extend(
            [
                {"name": "MODEL_ID", "value": item["model_id"]},
                {"name": "PROMPT_LEN", "value": str(item["prompt_len"])},
                {"name": "OUTPUT_TOKENS", "value": str(item["output_tokens"])},
                {"name": "LLM_WARMUP_ITERS", "value": str(args.llm_warmup)},
                {"name": "HF_HOME", "value": "/opt/hf-cache"},
                {"name": "HUGGINGFACE_HUB_CACHE", "value": "/opt/hf-cache/hub"},
                {"name": "HF_TOKEN", "valueFrom": {"secretKeyRef": {"name": "hf-token", "key": "HF_TOKEN"}}},
            ]
        )
        volume_mounts.append({"name": "hf-cache", "mountPath": "/opt/hf-cache"})
        volumes.append({"name": "hf-cache", "hostPath": {"path": "/var/lib/or-sim/hf-cache", "type": "DirectoryOrCreate"}})
        init_containers.append(
            {
                "name": "prepare-hf-cache",
                "image": "busybox:1.36",
                "command": ["sh", "-c", "mkdir -p /cache/hub && chown -R 65532:65532 /cache"],
                "volumeMounts": [{"name": "hf-cache", "mountPath": "/cache"}],
            }
        )
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "or-sim-profile-runtime",
            "namespace": args.namespace,
            "labels": {"app.kubernetes.io/name": "or-sim-profile-runtime"},
        },
        "spec": {
            "restartPolicy": "Never",
            "nodeSelector": {"kubernetes.io/hostname": args.node},
            "hostNetwork": True,
            "dnsPolicy": "ClusterFirstWithHostNet",
            "runtimeClassName": "nvidia",
            "tolerations": [{"operator": "Exists"}],
            "initContainers": init_containers,
            "containers": [
                {
                    "name": "runtime",
                    "image": image,
                    "imagePullPolicy": "IfNotPresent",
                    "args": [f"--addr=:{args.port}"],
                    "env": env,
                    "ports": [{"containerPort": args.port}],
                    "resources": {"requests": {resource: 1}, "limits": {resource: 1}},
                    "volumeMounts": volume_mounts,
                    "readinessProbe": {"httpGet": {"path": "/healthz", "port": args.port}, "periodSeconds": 2, "failureThreshold": 5},
                }
            ],
            "volumes": volumes,
        },
    }
    kubectl(["create", "-f", "-"], input_text=json.dumps(pod))


def run_samples(args: argparse.Namespace, host: str, port: int, item: dict[str, Any]) -> list[dict[str, Any]]:
    family = item["family"]
    requests = args.vision_requests if family == "vision" else args.llm_requests
    payload: dict[str, Any] = {"benchmark": True}
    if family == "llm":
        payload.update({"prompt_len": item["prompt_len"], "output_tokens": item["output_tokens"], "batch": item["batch"]})
    samples = []
    for _ in range(requests):
        samples.append(post_json(f"http://{host}:{port}/infer", payload, timeout_s=args.infer_timeout_s))
    return samples


def run_e2e_samples(
    args: argparse.Namespace,
    run_id: str,
    node_ip: str,
    item: dict[str, Any],
    mig_uuid: str,
    device_resource: str,
    slot_resource: str,
    runtime_samples: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if args.e2e_requests <= 0:
        return []
    router = args.router_url.rstrip("/")
    endpoint = f"http://{node_ip}:{args.port}"
    route_model = f"{run_id}-{args.node}-gpu{args.gpu_index}-{item['workload']}-{item['profile']}".replace("_", "-")
    runtime_id = f"e2e-{args.node}-gpu{args.gpu_index}-{item['workload']}-{item['profile']}".replace("_", "-")
    capacity = float(build_capacity(args, item, runtime_samples))
    route = {
        "model": route_model,
        "runtimeId": runtime_id,
        "endpoint": endpoint,
        "weight": 1,
        "capacity": capacity,
        "profile": item["profile"],
        "batchSize": item["batch"],
        "gpu": f"{args.node}-gpu{args.gpu_index}",
        "slotResource": slot_resource,
        "deviceResource": device_resource,
        "expectedMigUuid": mig_uuid,
        "active": True,
        "acceptingNew": True,
    }
    delete_url(f"{router}/control/routes?model={route_model}", timeout_s=30)
    post_json(f"{router}/control/routes", route, timeout_s=30)
    samples: list[dict[str, Any]] = []
    try:
        payload: dict[str, Any] = {"benchmark": True}
        if item["family"] == "llm":
            payload.update({"prompt_len": item["prompt_len"], "output_tokens": item["output_tokens"], "batch": item["batch"]})
        for idx in range(args.e2e_warmup + args.e2e_requests):
            started = time.perf_counter()
            response = post_json(f"{router}/infer/{route_model}", payload, timeout_s=args.infer_timeout_s)
            elapsed = (time.perf_counter() - started) * 1000.0
            if idx >= args.e2e_warmup:
                runtime_ms = float(response.get("runtimeLatencyMs", 0.0) or 0.0)
                samples.append({"e2eLatencyMs": elapsed, "runtimeLatencyMs": runtime_ms, "routerOverheadMs": max(0.0, elapsed - runtime_ms)})
    finally:
        try:
            delete_url(f"{router}/control/routes?model={route_model}&runtimeId={runtime_id}", timeout_s=30)
        except Exception:
            pass
    return samples


def build_capacity(args: argparse.Namespace, item: dict[str, Any], samples: list[dict[str, Any]]) -> float:
    row = build_row(
        "capacity-preview",
        args,
        item,
        "ok",
        "",
        "",
        "",
        "",
        samples,
        {},
        [],
    )
    return float(row.get("throughput_rps", 1.0) or 1.0)


def build_row(
    run_id: str,
    args: argparse.Namespace,
    item: dict[str, Any],
    status: str,
    error: str,
    mig_uuid: str,
    device_resource: str,
    slot_resource: str,
    samples: list[dict[str, Any]],
    metrics: dict[str, Any],
    e2e_samples: list[dict[str, Any]],
) -> dict[str, Any]:
    family = item["family"]
    if family == "vision":
        latencies = [float(s.get("runtimeLatencyMs", 0.0) or 0.0) for s in samples if s.get("runtimeLatencyMs") is not None]
        mean_ms = mean(latencies)
        throughput = (1000.0 * int(item["batch"]) / mean_ms) if mean_ms > 0 else 0.0
        ttft = tpot = decode_tps = service = 0.0
    else:
        service_values = [float(s.get("runtimeLatencyMs", 0.0) or 0.0) for s in samples if s.get("runtimeLatencyMs") is not None]
        ttft_values = [float(s.get("ttftMs", 0.0) or 0.0) for s in samples if s.get("ttftMs") is not None]
        tpot_values = [float(s.get("tpotMs", 0.0) or 0.0) for s in samples if s.get("tpotMs") is not None]
        latencies = service_values
        service = mean(service_values)
        ttft = mean(ttft_values)
        tpot = mean(tpot_values)
        decode_tps = 1000.0 / tpot if tpot > 0 else 0.0
        mean_ms = service
        throughput = (1000.0 * int(item["batch"]) / service) if service > 0 else 0.0
    peak_alloc = max([float(s.get("peakAllocMb", 0.0) or 0.0) for s in samples] + [0.0, float(metrics.get("peakAllocMb", 0.0) or 0.0)])
    peak_reserved = max([float(s.get("peakReservedMb", 0.0) or 0.0) for s in samples] + [0.0, float(metrics.get("peakReservedMb", 0.0) or 0.0)])
    e2e_latencies = [float(s.get("e2eLatencyMs", 0.0) or 0.0) for s in e2e_samples]
    e2e_runtime_latencies = [float(s.get("runtimeLatencyMs", 0.0) or 0.0) for s in e2e_samples]
    e2e_overheads = [float(s.get("routerOverheadMs", 0.0) or 0.0) for s in e2e_samples]
    e2e_mean = mean(e2e_latencies)
    e2e_throughput = (1000.0 * int(item["batch"]) / e2e_mean) if e2e_mean > 0 else 0.0
    return {
        "run_id": run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "node": args.node,
        "gpu_index": args.gpu_index,
        "mig_profile": item["profile"],
        "mig_uuid": mig_uuid,
        "device_resource": device_resource,
        "slot_resource": slot_resource,
        "workload": item["workload"],
        "model": item["model_name"],
        "model_id": item.get("model_id", ""),
        "family": family,
        "batch": item["batch"],
        "prompt_len": item.get("prompt_len", ""),
        "output_tokens": item.get("output_tokens", ""),
        "status": status,
        "error": error,
        "sample_count": len(samples),
        "latency_ms_mean": round(mean_ms, 6),
        "latency_ms_p50": round(percentile(latencies, 0.50), 6),
        "latency_ms_p95": round(percentile(latencies, 0.95), 6),
        "latency_ms_p99": round(percentile(latencies, 0.99), 6),
        "throughput_rps": round(throughput, 6),
        "ttft_ms_mean": round(ttft, 6),
        "tpot_ms_mean": round(tpot, 6),
        "decode_tps": round(decode_tps, 6),
        "service_time_ms": round(service, 6),
        "peak_alloc_mb": round(peak_alloc, 6),
        "peak_reserved_mb": round(peak_reserved, 6),
        "e2e_sample_count": len(e2e_samples),
        "e2e_ms_mean": round(e2e_mean, 6),
        "e2e_ms_p50": round(percentile(e2e_latencies, 0.50), 6),
        "e2e_ms_p95": round(percentile(e2e_latencies, 0.95), 6),
        "e2e_ms_p99": round(percentile(e2e_latencies, 0.99), 6),
        "e2e_runtime_ms_mean": round(mean(e2e_runtime_latencies), 6),
        "e2e_runtime_ms_p95": round(percentile(e2e_runtime_latencies, 0.95), 6),
        "router_overhead_ms_mean": round(mean(e2e_overheads), 6),
        "router_overhead_ms_p50": round(percentile(e2e_overheads, 0.50), 6),
        "router_overhead_ms_p95": round(percentile(e2e_overheads, 0.95), 6),
        "router_overhead_ms_p99": round(percentile(e2e_overheads, 0.99), 6),
        "e2e_throughput_rps": round(e2e_throughput, 6),
    }


def write_summary(path: Path, rows: list[dict[str, Any]]) -> None:
    lines = ["# Runtime-Side Capacity Summary", ""]
    for family in ["vision", "llm"]:
        family_rows = [r for r in rows if r.get("family") == family]
        if not family_rows:
            continue
        lines.extend([f"## {family.upper()}", ""])
        key_fields = ["workload", "batch"] if family == "vision" else ["workload", "prompt_len", "output_tokens"]
        grouped: dict[tuple[Any, ...], dict[str, dict[str, Any]]] = {}
        for row in family_rows:
            key = tuple(row.get(f, "") for f in key_fields)
            grouped.setdefault(key, {})[str(row["mig_profile"])] = row
        header = key_fields + [f"{p} req/s" for p in PROFILE_ORDER]
        lines.append("| " + " | ".join(header) + " |")
        lines.append("|" + "|".join(["---"] * len(header)) + "|")
        for key, by_profile in sorted(grouped.items()):
            vals = [str(x) for x in key]
            for profile in PROFILE_ORDER:
                row = by_profile.get(profile)
                if row is None:
                    vals.append("")
                elif row.get("status") != "ok":
                    vals.append("fail")
                else:
                    vals.append(f"{float(row.get('throughput_rps', 0.0)):.3f}")
            lines.append("| " + " | ".join(vals) + " |")
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def format_progress(row: dict[str, Any]) -> str:
    if row.get("status") != "ok":
        return f"{row['workload']} {row['mig_profile']} status=error error={row.get('error')}"
    return f"{row['workload']} b{row['batch']} {row['mig_profile']} {float(row['throughput_rps']):.3f} req/s p95={float(row['latency_ms_p95']):.3f}ms"


def cleanup_runtime_pod(namespace: str) -> None:
    subprocess.run(["kubectl", "delete", "pod", "-n", namespace, "or-sim-profile-runtime", "--ignore-not-found=true", "--wait=true"], check=False)
    subprocess.run(["kubectl", "wait", "-n", namespace, "--for=delete", "pod/or-sim-profile-runtime", "--timeout=120s"], check=False)


def wait_runtime_pod_running(namespace: str, timeout_s: int) -> None:
    deadline = time.time() + timeout_s
    last = ""
    while time.time() < deadline:
        phase, message = runtime_pod_phase(namespace)
        if phase == "Running":
            return
        if phase == "Failed":
            raise RuntimeError(f"runtime pod failed: {message}")
        last = phase or message
        time.sleep(1)
    raise RuntimeError(f"runtime pod did not reach Running: {last}")


def start_port_forward(namespace: str, pod_port: int) -> tuple[subprocess.Popen[str], int]:
    local_port = free_local_port()
    proc = subprocess.Popen(
        ["kubectl", "port-forward", "-n", namespace, "pod/or-sim-profile-runtime", f"{local_port}:{pod_port}"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    deadline = time.time() + 20
    output = []
    while time.time() < deadline:
        if proc.poll() is not None:
            remaining = proc.stdout.read() if proc.stdout else ""
            output.append(remaining)
            raise RuntimeError("runtime port-forward exited: " + "".join(output).strip())
        ready, _, _ = select.select([proc.stdout], [], [], 0.2) if proc.stdout else ([], [], [])
        if ready:
            line = proc.stdout.readline()
            output.append(line)
            if "Forwarding from" in line:
                return proc, local_port
    stop_port_forward(proc)
    raise RuntimeError("runtime port-forward did not become ready: " + "".join(output).strip())


def stop_port_forward(proc: subprocess.Popen[str] | None) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def free_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def wait_health(namespace: str, node_ip: str, port: int, timeout_s: int) -> None:
    deadline = time.time() + timeout_s
    last_error = ""
    while time.time() < deadline:
        phase, message = runtime_pod_phase(namespace)
        if phase == "Failed":
            raise RuntimeError(f"runtime pod failed: {message}")
        try:
            payload = get_health(f"http://{node_ip}:{port}/healthz", timeout_s=5)
            if payload.get("ok"):
                return
            last_error = str(payload.get("loadError") or payload)
        except Exception as exc:
            last_error = str(exc)
        time.sleep(2)
    raise RuntimeError(f"runtime health timeout: {last_error}")


def runtime_pod_phase(namespace: str) -> tuple[str, str]:
    try:
        raw = kubectl(["get", "pod", "-n", namespace, "or-sim-profile-runtime", "-o", "json"])
    except RuntimeError:
        return "", ""
    pod = json.loads(raw)
    status = pod.get("status", {})
    phase = status.get("phase", "")
    message = status.get("message") or status.get("reason") or ""
    return phase, message


def is_retryable_pod_admission_error(message: str) -> bool:
    retryable = [
        "UnexpectedAdmissionError",
        "no healthy devices present",
        "cannot allocate unhealthy devices",
    ]
    return any(token in message for token in retryable)


def wait_allocatable(node: str, resource: str, min_value: int, timeout_s: int) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        value = node_allocatable(node).get(resource, "0")
        try:
            if int(value) >= min_value:
                return
        except ValueError:
            pass
        time.sleep(2)
    raise RuntimeError(f"resource {resource} did not become allocatable on {node}")


def node_internal_ip(node: str) -> str:
    raw = kubectl(["get", "node", node, "-o", "json"])
    data = json.loads(raw)
    for address in data["status"]["addresses"]:
        if address["type"] == "InternalIP":
            return address["address"]
    raise RuntimeError(f"node {node} has no InternalIP")


def node_allocatable(node: str) -> dict[str, str]:
    raw = kubectl(["get", "node", node, "-o", "json"])
    return json.loads(raw)["status"]["allocatable"]


def set_repair_paused(node: str, paused: bool) -> None:
    if paused:
        kubectl(["label", "node", node, "mig.or-sim.io/repair-paused=true", "--overwrite"])
    else:
        kubectl(["label", "node", node, "mig.or-sim.io/repair-paused-", "--overwrite"])


def post_json(url: str, payload: dict[str, Any], timeout_s: int) -> dict[str, Any]:
    raw = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=raw, headers={"content-type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode())


def post_json_allow_error(url: str, payload: dict[str, Any], timeout_s: int) -> dict[str, Any]:
    raw = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=raw, headers={"content-type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode()
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            raise RuntimeError(f"HTTP {exc.code}: {body}") from exc


def get_json(url: str, timeout_s: int) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode())


def delete_url(url: str, timeout_s: int) -> dict[str, Any]:
    req = urllib.request.Request(url, method="DELETE")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode())


def get_health(url: str, timeout_s: int) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=timeout_s) as resp:
        body = resp.read().decode()
        if 200 <= resp.status < 300:
            try:
                payload = json.loads(body)
                if isinstance(payload, dict):
                    return payload
            except json.JSONDecodeError:
                return {"ok": True, "body": body}
        return {"ok": False, "status": resp.status, "body": body}


def kubectl(args: list[str], input_text: str | None = None) -> str:
    proc = subprocess.run(["kubectl", *args], input=input_text, text=True, capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())
    return proc.stdout


def split_csv(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def mig_uuid_resource(uuid: str) -> str:
    token = uuid.lower().removeprefix("mig-")
    return "or-sim.io/mig-" + token


def mean(values: list[float]) -> float:
    return statistics.mean(values) if values else 0.0


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * q))))
    return ordered[idx]


def append_csv(path: Path, fieldnames: list[str], row: dict[str, Any]) -> None:
    exists = path.exists()
    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def raw_fieldnames() -> list[str]:
    return [
        "run_id",
        "timestamp",
        "node",
        "gpu_index",
        "mig_profile",
        "mig_uuid",
        "device_resource",
        "slot_resource",
        "workload",
        "model",
        "model_id",
        "family",
        "batch",
        "prompt_len",
        "output_tokens",
        "status",
        "error",
        "sample_count",
        "latency_ms_mean",
        "latency_ms_p50",
        "latency_ms_p95",
        "latency_ms_p99",
        "throughput_rps",
        "ttft_ms_mean",
        "tpot_ms_mean",
        "decode_tps",
        "service_time_ms",
        "peak_alloc_mb",
        "peak_reserved_mb",
        "e2e_sample_count",
        "e2e_ms_mean",
        "e2e_ms_p50",
        "e2e_ms_p95",
        "e2e_ms_p99",
        "e2e_runtime_ms_mean",
        "e2e_runtime_ms_p95",
        "router_overhead_ms_mean",
        "router_overhead_ms_p50",
        "router_overhead_ms_p95",
        "router_overhead_ms_p99",
        "e2e_throughput_rps",
    ]


if __name__ == "__main__":
    raise SystemExit(main())
