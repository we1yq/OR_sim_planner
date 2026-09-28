#!/usr/bin/env python3
"""Isolated re-profiling of every catalog serving option on one A100.

Pods come from executor_deployments.json, which transition-executor's own
deployment() generated with the live or-sim-exp executor env, so image, env,
mounts and CPU exclusion match the live system.  Only one runtime runs on the
GPU at a time (isolated profiling).

Throughput per option = 1000 * batch / mean(runtimeLatencyMs).

Usage: run_catalog_profile.py <node> <gpu_index> [<gpu_index> ...]
"""
from __future__ import annotations

import csv
import json
import select
import socket
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
NODE_IPS = {"ampere": "115.145.135.205", "rtx1-worker": "115.145.179.130"}
SIZE = {"1g": 1, "2g": 2, "3g": 4, "4g": 4, "7g": 8}
PROFILE_ORDER = ["1g", "2g", "3g", "4g", "7g"]
PORT = 18080
VISION_WARMUP, VISION_SAMPLES = 10, 50
LLM_WARMUP, LLM_SAMPLES = 1, 10
LLM_SHAPES = {  # same as live_traffic_20260926._LLM_SPECS
    "gpt2_p64_o64": ("gpt2", 64, 64),
    "gpt2_p512_o512": ("gpt2", 512, 512),
    "llama_p1024_o128": ("llama", 1024, 128),
    "llama_p2048_o64": ("llama", 2048, 64),
    "llama_p2048_o48": ("llama", 2048, 48),
}
RAW_FIELDS = ["node", "gpu_index", "workload", "profile", "batch", "phase", "i", "runtimeLatencyMs", "ttftMs", "tpotMs",
              "mig_uuid", "cpuAffinity", "error"]


def log(msg: str) -> None:
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def kubectl(args: list[str], input_text: str | None = None, check: bool = True) -> str:
    proc = subprocess.run(["kubectl", *args], input=input_text, text=True, capture_output=True)
    if check and proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())
    return proc.stdout


def post_json(url: str, payload: dict, timeout: float) -> dict:
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers={"content-type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def get_json(url: str, timeout: float) -> dict:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


class GPU:
    def __init__(self, node: str, gpu_index: str):
        self.node, self.gpu = node, gpu_index
        self.ip = NODE_IPS[node]
        self.pod = f"catprof-{node}-gpu{gpu_index}"
        self.out = HERE / f"{node}-gpu{gpu_index}"
        self.out.mkdir(exist_ok=True)
        self.raw_path = self.out / "raw-samples.csv"
        self.templates = [d for d in json.loads((HERE / "executor_deployments.json").read_text())
                          if _env(d, "OR_SIM_GPU") == f"{node}-gpu{gpu_index}"]
        runtimes = json.loads((HERE / "runtimes.json").read_text())
        self.batches = {(r["model"], r["profile"]): r["_batches"] for r in runtimes if r["gpu"] == f"{node}-gpu{gpu_index}"}

    def agent(self, path: str, payload: dict, timeout: float = 180) -> dict:
        return post_json(f"http://{self.ip}:10684/{path}?gpuIndex={self.gpu}", payload, timeout)

    def clear(self) -> None:
        self.agent("clear", {})
        try:
            self.agent("refresh-cdi", {}, 120)
        except Exception:
            pass  # empty MIG state can make nvidia-ctk emit an invalid CDI spec; harmless here

    def create_slot(self, profile: str) -> str:
        slots = self.agent("apply-slots", {"create": f"0:{SIZE[profile]}:{profile}"}).get("migSlots", [])
        self.agent("refresh-cdi", {}, 120)
        return str(next(s for s in slots if s.get("profile") == profile)["migDeviceUuid"])

    def wait_allocatable(self, resource: str, timeout: float = 240) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            alloc = json.loads(kubectl(["get", "node", self.node, "-o", "json"]))["status"]["allocatable"]
            if int(alloc.get(resource, "0")) >= 1:
                return
            time.sleep(2)
        raise RuntimeError(f"{resource} not allocatable on {self.node}")

    def deploy(self, workload: str, profile: str, mig_uuid: str) -> None:
        dep = next(d for d in self.templates if _env(d, "OR_SIM_WORKLOAD") == workload and _env(d, "OR_SIM_PROFILE") == profile)
        device = "or-sim.io/mig-" + mig_uuid.lower().removeprefix("mig-")
        spec = json.loads(json.dumps(dep["spec"]["template"]["spec"]).replace("__DEVICE__", device).replace("__UUID__", mig_uuid))
        spec["restartPolicy"] = "Never"
        pod = {"apiVersion": "v1", "kind": "Pod",
               "metadata": {"name": self.pod, "namespace": "or-sim", "labels": {"app.kubernetes.io/name": "or-sim-catalog-profile"}},
               "spec": spec}
        self.wait_allocatable(device)
        kubectl(["create", "-f", "-"], input_text=json.dumps(pod))

    def wait_ready(self, timeout: float = 300) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            p = json.loads(kubectl(["get", "pod", "-n", "or-sim", self.pod, "-o", "json"]))
            st = p.get("status", {})
            if st.get("phase") in ("Failed", "Succeeded"):
                raise RuntimeError(f"pod ended: {st.get('phase')}")
            if any(c.get("type") == "Ready" and c.get("status") == "True" for c in st.get("conditions", [])):
                return
            time.sleep(2)
        raise RuntimeError("pod not ready")

    def cleanup_pod(self) -> None:
        kubectl(["delete", "pod", "-n", "or-sim", self.pod, "--ignore-not-found=true", "--wait=true"], check=False)

    def repair_paused(self, on: bool) -> None:
        kubectl(["label", "node", self.node, "mig.or-sim.io/repair-paused=true" if on else "mig.or-sim.io/repair-paused-", "--overwrite"], check=False)


def _env(dep: dict, name: str) -> str:
    for e in dep["spec"]["template"]["spec"]["containers"][0]["env"]:
        if e["name"] == name:
            return str(e.get("value", ""))
    return ""


def port_forward(pod: str) -> tuple[subprocess.Popen, int]:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    proc = subprocess.Popen(["kubectl", "port-forward", "-n", "or-sim", f"pod/{pod}", f"{port}:{PORT}"],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    deadline = time.time() + 30
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("port-forward exited: " + (proc.stdout.read() if proc.stdout else ""))
        if select.select([proc.stdout], [], [], 0.2)[0] and "Forwarding from" in proc.stdout.readline():
            return proc, port
    proc.kill()
    raise RuntimeError("port-forward not ready")


def llm_payload(workload: str) -> dict:
    model, p, o = LLM_SHAPES[workload]
    return {"benchmark": True, "model": model, "workload": workload, "request_class": f"p{p}/o{o}", "batch": 1,
            "logicalRequestCount": 1, "prompt_len": p, "output_tokens": o, "max_tokens": o, "prompt": "hello"}


def measure(g: GPU, workload: str, profile: str, mig_uuid: str, writer) -> None:
    pf, port = port_forward(g.pod)
    base = f"http://127.0.0.1:{port}"
    try:
        deadline = time.time() + 180
        health = {}
        while time.time() < deadline:
            try:
                health = get_json(base + "/healthz", 10)
                if health.get("ok") or health.get("loaded"):
                    break
            except Exception:
                pass
            time.sleep(1)
        affinity = str(health.get("cpuAffinity", ""))
        log(f"{g.node}/gpu{g.gpu} {workload} {profile} ready cpuAffinity={affinity} loadSec={health.get('loadTimings', {}).get('totalLoadSec')}")
        is_llm = workload in LLM_SHAPES
        for batch in g.batches[(workload, profile)]:
            row = {"node": g.node, "gpu_index": g.gpu, "workload": workload, "profile": profile, "batch": batch,
                   "mig_uuid": mig_uuid, "cpuAffinity": affinity}
            try:
                if is_llm:
                    payload, warm, n, timeout = llm_payload(workload), LLM_WARMUP, LLM_SAMPLES, 900
                else:
                    post_json(base + "/control/batch", {"batchSize": batch}, 120)
                    payload, warm, n, timeout = {"benchmark": True, "batch": batch}, VISION_WARMUP, VISION_SAMPLES, 300
                for phase, count in (("warmup", warm), ("sample", n)):
                    for i in range(count):
                        r = post_json(base + "/infer", payload, timeout)
                        writer.writerow({**row, "phase": phase, "i": i, "runtimeLatencyMs": r.get("runtimeLatencyMs"),
                                         "ttftMs": r.get("ttftMs", ""), "tpotMs": r.get("tpotMs", ""), "error": ""})
            except Exception as exc:  # record and continue with the next option
                writer.writerow({**row, "phase": "error", "i": -1, "error": f"{type(exc).__name__}: {exc}"[:300]})
                log(f"ERROR {g.node}/gpu{g.gpu} {workload} {profile} b{batch}: {exc}")
    finally:
        pf.terminate()


def run_gpu(g: GPU) -> None:
    by_profile: dict[str, list[str]] = {}
    for (w, p) in g.batches:
        by_profile.setdefault(p, []).append(w)
    new = not g.raw_path.exists()
    with g.raw_path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RAW_FIELDS)
        if new:
            writer.writeheader()
        g.repair_paused(True)
        try:
            for profile in PROFILE_ORDER:
                for workload in sorted(by_profile.get(profile, [])):
                    g.cleanup_pod()
                    g.clear()
                    mig_uuid = ""
                    try:
                        mig_uuid = g.create_slot(profile)
                        g.deploy(workload, profile, mig_uuid)
                        g.wait_ready()
                        measure(g, workload, profile, mig_uuid, writer)
                    except Exception as exc:
                        for batch in g.batches[(workload, profile)]:
                            writer.writerow({"node": g.node, "gpu_index": g.gpu, "workload": workload, "profile": profile,
                                             "batch": batch, "phase": "error", "i": -1, "mig_uuid": mig_uuid,
                                             "error": f"{type(exc).__name__}: {exc}"[:300]})
                        log(f"ERROR {g.node}/gpu{g.gpu} {workload} {profile}: {exc}")
                    finally:
                        f.flush()
                        g.cleanup_pod()
        finally:
            g.cleanup_pod()
            g.clear()
            g.repair_paused(False)
    log(f"done {g.node}/gpu{g.gpu}")


if __name__ == "__main__":
    node = sys.argv[1]
    for gpu_index in sys.argv[2:]:
        run_gpu(GPU(node, gpu_index))
