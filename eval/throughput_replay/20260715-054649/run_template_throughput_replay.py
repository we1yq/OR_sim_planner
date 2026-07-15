#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


REPLAY_DIR = Path(__file__).resolve().parent
REPO_ROOT = REPLAY_DIR.parents[2]
RUNTIME_HOST_PORT_POOL = tuple(port for port in range(10681, 10721) if port not in {10684, 10690})
OUTPUT_FIELDS = [
    "template_id",
    "trial",
    "instance_idx",
    "mig_profile",
    "workload",
    "request_class",
    "batch_size",
    "measured_throughput_rps",
    "measurement_seconds",
    "physical_gpu_id",
    "server_id",
]


@dataclass(frozen=True)
class PhysicalGPU:
    physical_id: str
    node: str
    gpu_index: int


@dataclass(frozen=True)
class TemplateInstance:
    template_id: str
    instance_idx: int
    start_slice: int
    end_slice: int
    mig_profile: str
    workload: str
    request_class: str
    batch_size: int

    @property
    def executor_end_slice(self) -> int:
        if self.mig_profile == "3g":
            return self.start_slice + 4
        return self.end_slice

    @property
    def slot(self) -> tuple[int, int, str]:
        return (self.start_slice, self.executor_end_slice, self.mig_profile)


def main() -> int:
    args = parse_args()
    replay_dir = Path(args.replay_dir).resolve()
    templates = load_templates(replay_dir)
    if args.template_ids:
        wanted = set(split_csv(args.template_ids))
        templates = [tpl for tpl in templates if tpl["template_id"] in wanted]
    if args.limit_templates > 0:
        templates = templates[: args.limit_templates]
    if not templates:
        raise RuntimeError("no templates selected")

    gpu = select_physical_gpu(args)
    print(f"using physical GPU: {gpu.physical_id} on {gpu.node} gpu{gpu.gpu_index}", flush=True)
    out_path = replay_dir / "template_throughput_measurements.csv"
    completed = completed_measurements(out_path) if args.resume else set()

    cleanup_gpu(args, gpu, "initial-cleanup")
    try:
        for tpl_idx, template in enumerate(templates, start=1):
            template_id = template["template_id"]
            instances = [parse_instance(row) for row in template["instances"]]
            for trial in range(1, args.trials + 1):
                keys = {(template_id, trial, inst.instance_idx) for inst in instances}
                if args.resume and keys.issubset(completed):
                    print(f"=== template {tpl_idx}/{len(templates)} {template_id} trial {trial}/{args.trials} skipped ===", flush=True)
                    continue
                print(f"=== template {tpl_idx}/{len(templates)} {template_id} trial {trial}/{args.trials} ===", flush=True)
                try:
                    rows = run_template_trial(args, gpu, template_id, instances, trial)
                    append_rows(out_path, rows)
                    completed.update((row["template_id"], int(row["trial"]), int(row["instance_idx"])) for row in rows)
                finally:
                    cleanup_gpu(args, gpu, f"cleanup-{template_id}-t{trial}")
    finally:
        cleanup_gpu(args, gpu, "final-cleanup")
    print(f"wrote {out_path}", flush=True)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Measure real throughput for SliceWise per-GPU templates.")
    parser.add_argument("--replay-dir", default=str(REPLAY_DIR))
    parser.add_argument("--namespace", default="or-sim-exp")
    parser.add_argument("--run-id", default="template-throughput-" + time.strftime("%Y%m%d-%H%M%S"))
    parser.add_argument("--physical-gpu", default="", help="Physical GPU id, e.g. ampere-gpu0. Default: first available.")
    parser.add_argument("--template-ids", default="", help="Comma-separated template ids for smoke/partial runs.")
    parser.add_argument("--limit-templates", type=int, default=0)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--measurement-seconds", type=float, default=60.0)
    parser.add_argument("--warmup-seconds", type=float, default=10.0)
    parser.add_argument("--concurrency", type=int, default=0, help="Per-instance request concurrency. 0 chooses from workload type.")
    parser.add_argument("--infer-timeout-s", type=float, default=1800.0)
    parser.add_argument("--plan-timeout-s", type=float, default=1800.0)
    parser.add_argument("--poll-s", type=float, default=2.0)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--keep-plans", action="store_true")
    return parser.parse_args()


def load_templates(replay_dir: Path) -> list[dict[str, Any]]:
    return json.loads((replay_dir / "unique_gpu_templates.json").read_text(encoding="utf-8"))


def parse_instance(row: dict[str, Any]) -> TemplateInstance:
    return TemplateInstance(
        template_id=str(row["template_id"]),
        instance_idx=int(row["instance_idx"]),
        start_slice=int(row["start_slice"]),
        end_slice=int(row["end_slice"]),
        mig_profile=str(row["mig_profile"]),
        workload=str(row["workload"]),
        request_class=str(row["request_class"]),
        batch_size=int(row["batch_size"]),
    )


def run_template_trial(
    args: argparse.Namespace,
    gpu: PhysicalGPU,
    template_id: str,
    instances: list[TemplateInstance],
    trial: int,
) -> list[dict[str, Any]]:
    cleanup_gpu(args, gpu, f"preclean-{template_id}-t{trial}")
    runtimes = [runtime_for_instance(args, gpu, inst, trial) for inst in instances]
    create_spec = slots_to_create_spec([inst.slot for inst in instances])
    actions: list[dict[str, Any]] = [
        action_configure(gpu, create_spec, template_id),
        action_register(gpu, [inst.slot for inst in instances], template_id),
    ]
    for inst, runtime in zip(instances, runtimes, strict=True):
        actions.append(action_place(gpu, template_id, inst, runtime))
    for inst, runtime in zip(instances, runtimes, strict=True):
        actions.append(action_activate(gpu, template_id, inst, runtime))
    plan = action_plan(args, f"{template_id}-t{trial}-deploy", template_id, actions, runtimes)
    plan_name = create_plan(args, plan)
    wait_executed(args, plan_name, args.plan_timeout_s)
    if not args.keep_plans:
        delete_plan(args, plan_name)

    print(f"measuring {template_id} trial {trial}: {len(instances)} concurrent instances", flush=True)
    rows = measure_template(args, gpu, instances, runtimes, trial)
    return rows


def measure_template(
    args: argparse.Namespace,
    gpu: PhysicalGPU,
    instances: list[TemplateInstance],
    runtimes: list[dict[str, Any]],
    trial: int,
) -> list[dict[str, Any]]:
    with ThreadPoolExecutor(max_workers=len(instances)) as pool:
        futures = [
            pool.submit(measure_instance, args, gpu, inst, runtime, trial)
            for inst, runtime in zip(instances, runtimes, strict=True)
        ]
        rows = [future.result() for future in as_completed(futures)]
    rows.sort(key=lambda row: int(row["instance_idx"]))
    return rows


def measure_instance(
    args: argparse.Namespace,
    gpu: PhysicalGPU,
    inst: TemplateInstance,
    runtime: dict[str, Any],
    trial: int,
) -> dict[str, Any]:
    endpoint = f"http://{node_ip(gpu.node)}:{runtime['hostPort']}/infer"
    concurrency = args.concurrency if args.concurrency > 0 else default_concurrency(inst)
    stop_warmup = time.monotonic() + max(0.0, args.warmup_seconds)
    while time.monotonic() < stop_warmup:
        try:
            post_json(endpoint, payload_for_instance(inst), args.infer_timeout_s)
        except Exception:
            time.sleep(0.2)

    end_at = time.monotonic() + args.measurement_seconds
    completed = 0
    lock = threading.Lock()

    def worker() -> None:
        nonlocal completed
        while time.monotonic() < end_at:
            try:
                post_json(endpoint, payload_for_instance(inst), args.infer_timeout_s)
                logical = inst.batch_size if is_vision_workload(inst.workload) else 1
                with lock:
                    completed += logical
            except Exception:
                time.sleep(0.1)

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(concurrency)]
    started = time.monotonic()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    elapsed = max(1e-9, time.monotonic() - started)
    return {
        "template_id": inst.template_id,
        "trial": trial,
        "instance_idx": inst.instance_idx,
        "mig_profile": inst.mig_profile,
        "workload": inst.workload,
        "request_class": inst.request_class,
        "batch_size": inst.batch_size,
        "measured_throughput_rps": round(completed / elapsed, 6),
        "measurement_seconds": round(elapsed, 6),
        "physical_gpu_id": gpu.physical_id,
        "server_id": gpu.node,
    }


def payload_for_instance(inst: TemplateInstance) -> dict[str, Any]:
    body: dict[str, Any] = {"benchmark": True, "requestClass": inst.request_class}
    if is_vision_workload(inst.workload):
        body.update({"driverBatched": True, "logicalRequestCount": inst.batch_size, "batch": inst.batch_size})
        return body
    prompt_len, output_tokens = parse_request_class(inst.request_class)
    body.update({"prompt_len": prompt_len, "output_tokens": output_tokens, "batch": inst.batch_size})
    return body


def parse_request_class(value: str) -> tuple[int, int]:
    value = str(value).strip().lower()
    if not value.startswith("p") or "/o" not in value:
        raise ValueError(f"unsupported LLM request_class {value!r}")
    prompt, output = value[1:].split("/o", 1)
    return int(prompt), int(output)


def default_concurrency(inst: TemplateInstance) -> int:
    if is_vision_workload(inst.workload):
        return 4
    if inst.request_class == "p64/o64":
        return 2
    return 1


def runtime_for_instance(args: argparse.Namespace, gpu: PhysicalGPU, inst: TemplateInstance, trial: int) -> dict[str, Any]:
    start, end, profile = inst.slot
    slot_resource = slot_resource_name(gpu.physical_id, start, end, profile)
    runtime_model = runtime_model_for_workload(inst.workload)
    return {
        "runtimeId": sanitize(f"{args.run_id}-{inst.template_id}-{trial}-{inst.instance_idx}"),
        "model": inst.workload,
        "runtimeModel": runtime_model,
        "requestClass": inst.request_class,
        "promptLen": parse_request_class(inst.request_class)[0] if not is_vision_workload(inst.workload) else 0,
        "outputTokens": parse_request_class(inst.request_class)[1] if not is_vision_workload(inst.workload) else 0,
        "batchSize": inst.batch_size,
        "node": gpu.node,
        "gpu": gpu.physical_id,
        "profile": profile,
        "slotResource": slot_resource,
        "hostPort": runtime_host_port(gpu.physical_id, slot_resource),
        "weight": 1.0,
        "capacity": 1.0,
    }


def runtime_model_for_workload(workload: str) -> str:
    if workload.endswith("_image"):
        return workload.removesuffix("_image")
    if workload.startswith("gpt2"):
        return "gpt2"
    if workload.startswith("llama"):
        return "llama"
    return workload


def cleanup_gpu(args: argparse.Namespace, gpu: PhysicalGPU, label: str) -> None:
    template_id = sanitize(label)
    actions = [action_base(gpu, template_id, "delete_instance"), action_base(gpu, template_id, "clear_template"), action_base(gpu, template_id, "return_gpu")]
    plan = action_plan(args, label, template_id, actions, [])
    try:
        wait_executed(args, create_plan(args, plan), args.plan_timeout_s)
    except Exception as exc:
        print(f"warning: cleanup {label} failed: {exc}", flush=True)


def action_plan(args: argparse.Namespace, suffix: str, template_id: str, actions: list[dict[str, Any]], runtimes: list[dict[str, Any]]) -> dict[str, Any]:
    plan_name = sanitize(f"{args.run_id}-{suffix}-{int(time.time() * 1000)}")
    nodes = []
    prev_id = ""
    for idx, action in enumerate(actions):
        node_id = sanitize(f"a{idx:04d}-{action['type']}-{template_id}")[:60]
        node: dict[str, Any] = {"id": node_id, "index": idx, "phase": idx, "type": action["type"], "action": action}
        if prev_id:
            node["dependsOn"] = [prev_id]
        nodes.append(node)
        prev_id = node_id
    return {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "MigActionPlan",
        "metadata": {
            "name": plan_name,
            "namespace": args.namespace,
            "labels": {
                "app.kubernetes.io/name": "migrant-go",
                "mig.or-sim.io/component": "template-throughput-replay",
                "mig.or-sim.io/run-id": args.run_id,
                "mig.or-sim.io/template-id": template_id[:63],
            },
        },
        "spec": {
            "abstractActions": [],
            "actionCount": len(nodes),
            "actionDag": {"format": "migrant.action-dag/v1", "name": plan_name, "nodes": nodes},
            "currentAllocationRef": "physicalgpuregistries/default",
            "executor": "go-transition-executor",
            "phaseGate": "auto",
            "plannerMetadata": {"planner": "template-throughput-replay", "templateId": template_id},
            "planningInput": {"sourceArrival": {}, "targetArrival": {}, "registeredSLOMs": {}, "slo": {}},
            "podLifecyclePreview": {"desiredRuntimes": runtimes},
            "summary": {"desiredRuntimes": runtimes, "planType": "template-throughput-replay", "sourceGpuCount": 0, "targetGpuCount": 0},
            "targetGpuCount": 0,
        },
    }


def action_base(gpu: PhysicalGPU, template_id: str, action_type: str) -> dict[str, Any]:
    return {
        "type": action_type,
        "abstractAction": template_id,
        "node": gpu.node,
        "gpuIndex": gpu.gpu_index,
        "gpu": gpu.physical_id,
        "physicalGpuId": gpu.physical_id,
        "physical_gpu_id": gpu.physical_id,
    }


def action_configure(gpu: PhysicalGPU, create_spec: str, template_id: str) -> dict[str, Any]:
    action = action_base(gpu, template_id, "configure_full_template")
    action["createSpec"] = create_spec
    action["slots"] = [list(slot) for slot in slots_from_spec(create_spec)]
    return action


def action_register(gpu: PhysicalGPU, slots: list[tuple[int, int, str]], template_id: str) -> dict[str, Any]:
    action = action_base(gpu, template_id, "register_mig_devices")
    action["slots"] = [list(slot) for slot in slots]
    return action


def action_place(gpu: PhysicalGPU, template_id: str, inst: TemplateInstance, runtime: dict[str, Any]) -> dict[str, Any]:
    action = action_base(gpu, template_id, "place_instance")
    action.update({"model": runtime["model"], "workload": runtime["model"], "slot": list(inst.slot)})
    return action


def action_activate(gpu: PhysicalGPU, template_id: str, inst: TemplateInstance, runtime: dict[str, Any]) -> dict[str, Any]:
    action = action_base(gpu, template_id, "activate_instance_route")
    action.update({"model": runtime["model"], "workload": runtime["model"], "slot": list(inst.slot)})
    return action


def create_plan(args: argparse.Namespace, body: dict[str, Any]) -> str:
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
        yaml.safe_dump(body, f, sort_keys=False)
        path = f.name
    try:
        run(["kubectl", "create", "-f", path])
    finally:
        Path(path).unlink(missing_ok=True)
    return body["metadata"]["name"]


def wait_executed(args: argparse.Namespace, name: str, timeout_s: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    last_phase = ""
    while time.monotonic() < deadline:
        obj = kubectl_json(["get", "migactionplan", name, "-n", args.namespace, "-o", "json"])
        phase = str((obj.get("status") or {}).get("phase") or "")
        if phase and phase != last_phase:
            print(f"{name}: phase={phase}", flush=True)
            last_phase = phase
        if phase == "Executed":
            return obj
        if phase == "Failed":
            message = (obj.get("status") or {}).get("message")
            raise RuntimeError(f"{name} failed: {message}")
        time.sleep(args.poll_s)
    raise TimeoutError(f"timed out waiting for {name}; last phase={last_phase}")


def delete_plan(args: argparse.Namespace, name: str) -> None:
    run(["kubectl", "-n", args.namespace, "delete", "migactionplan", name, "--ignore-not-found=true"], check=False)


def select_physical_gpu(args: argparse.Namespace) -> PhysicalGPU:
    registry = kubectl_json(["-n", args.namespace, "get", "physicalgpuregistry", "default", "-o", "json"])
    bindings = ((registry.get("status") or {}).get("bindings") or {})
    candidates = []
    for physical_id, raw in sorted(bindings.items()):
        item = raw or {}
        if args.physical_gpu and physical_id != args.physical_gpu:
            continue
        state = str(item.get("state") or "")
        cleanliness = str(item.get("cleanliness") or "")
        if args.physical_gpu or (state == "available" and cleanliness in {"", "empty"}):
            candidates.append(PhysicalGPU(
                physical_id=physical_id,
                node=str(item.get("node") or item.get("nodeName") or physical_id.split("-gpu", 1)[0]),
                gpu_index=int(item.get("gpuIndex") if item.get("gpuIndex") is not None else item.get("deviceIndex") or 0),
            ))
    if not candidates:
        raise RuntimeError("no available physical GPU found; pass --physical-gpu or reset the cluster")
    return candidates[0]


def node_ip(node: str) -> str:
    obj = kubectl_json(["get", "node", node, "-o", "json"])
    for addr in (obj.get("status") or {}).get("addresses") or []:
        if addr.get("type") == "InternalIP":
            return str(addr.get("address"))
    raise RuntimeError(f"node {node} has no InternalIP")


def slots_to_create_spec(slots: list[tuple[int, int, str]]) -> str:
    return ",".join(f"{start}:{end - start}:{profile}" for start, end, profile in slots)


def slots_from_spec(spec: str) -> list[tuple[int, int, str]]:
    out = []
    for part in split_csv(spec):
        start, size, profile = part.split(":", 2)
        start_i = int(start)
        size_i = int(size)
        out.append((start_i, start_i + size_i, profile))
    return out


def slot_resource_name(physical_id: str, start: int, end: int, profile: str) -> str:
    return f"or-sim.io/{physical_id}-s{start}-{end}-{profile}"


def runtime_host_port(physical_id: str, slot_resource: str) -> int:
    import re

    gpu_match = re.search(r"-gpu(\d+)$", physical_id)
    slot_match = re.search(r"-s(\d+)-\d+-[a-z0-9]+$", slot_resource)
    if not gpu_match or not slot_match:
        return RUNTIME_HOST_PORT_POOL[0]
    index = int(gpu_match.group(1)) * 7 + int(slot_match.group(1))
    if index >= len(RUNTIME_HOST_PORT_POOL):
        raise ValueError(f"runtime host port pool exhausted for {physical_id} {slot_resource}")
    return RUNTIME_HOST_PORT_POOL[index]


def completed_measurements(path: Path) -> set[tuple[str, int, int]]:
    if not path.exists():
        return set()
    out = set()
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                out.add((row["template_id"], int(row["trial"]), int(row["instance_idx"])))
            except Exception:
                continue
    return out


def append_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)


def post_json(url: str, body: dict[str, Any], timeout_s: float) -> dict[str, Any]:
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode("utf-8"))


def is_vision_workload(workload: str) -> bool:
    return workload.endswith("_image")


def split_csv(value: str) -> list[str]:
    return [item.strip() for item in str(value).split(",") if item.strip()]


def sanitize(value: str) -> str:
    out = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value))
    out = "-".join(part for part in out.split("-") if part)
    return out[:230] or "x"


def kubectl_json(args: list[str]) -> dict[str, Any]:
    proc = run(["kubectl", *args], capture=True)
    return json.loads(proc.stdout)


def run(cmd: list[str], check: bool = True, capture: bool = False) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(cmd, text=True, stdout=subprocess.PIPE if capture else None, stderr=subprocess.PIPE)
    if check and proc.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} failed: {proc.stderr.strip()}")
    return proc


if __name__ == "__main__":
    raise SystemExit(main())
