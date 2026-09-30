#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import queue
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


def parse_cpu_list(spec: str) -> set[int]:
    cpus: set[int] = set()
    for part in spec.replace(" ", "").split(","):
        if not part:
            continue
        if "-" in part:
            lo, hi = part.split("-", 1)
            cpus.update(range(int(lo), int(hi) + 1))
        else:
            cpus.add(int(part))
    return cpus


def format_cpu_list(cpus: list[int]) -> str:
    ranges: list[str] = []
    start = prev = None
    for cpu in sorted(cpus) + [None]:
        if prev is not None and cpu == prev + 1:
            prev = cpu
            continue
        if start is not None:
            ranges.append(str(start) if start == prev else f"{start}-{prev}")
        start = prev = cpu
    return ",".join(ranges)

def apply_cpu_exclude() -> str:
    """Apply OR_SIM_CPU_SET / OR_SIM_CPU_EXCLUDE to this process's CPU affinity.

    Runs before torch is imported so every thread torch, CUDA and the
    per-request HTTP handlers create inherits the result.  OR_SIM_CPU_SET
    (e.g. "4,60", one physical core) confines the runtime to a dedicated core;
    spreading runtime threads over many cores makes GPU-launch-bound latency
    bimodal.  OR_SIM_CPU_EXCLUDE (e.g. "1,33") drops host cores that
    measurably slow the GPU launch path.
    """
    for name in ("OR_SIM_CPU_SET", "OR_SIM_CPU_EXCLUDE"):
        spec = os.environ.get(name, "").strip()
        if not spec:
            continue
        try:
            allowed = os.sched_getaffinity(0)
            cpus = parse_cpu_list(spec)
            keep = allowed & cpus if name == "OR_SIM_CPU_SET" else allowed - cpus
            if keep and keep != allowed:
                os.sched_setaffinity(0, keep)
        except (OSError, ValueError) as exc:
            print(f"ignoring {name}={spec!r}: {exc}", file=sys.stderr, flush=True)
    return format_cpu_list(sorted(os.sched_getaffinity(0)))


CPU_AFFINITY = apply_cpu_exclude()

# The HTTP process (this module run as __main__) never touches CUDA, so it
# skips the model libraries; only the spawned inference child (which
# re-imports this module as __mp_main__) imports them.  Importing torch in
# both processes, one after the other, added seconds to every runtime start.
HTTP_PROCESS = __name__ == "__main__"

if HTTP_PROCESS:
    torch = None
    models = None
    IMPORT_ERROR = ""
else:
    try:
        import torch
        import torchvision.models as models
    except Exception as exc:  # pragma: no cover - surfaced through /healthz
        torch = None
        models = None
        IMPORT_ERROR = str(exc)
    else:
        IMPORT_ERROR = ""


MODEL_SPECS = {
    # Keep the canonical names aligned with torchvision model factory order used
    # by the profiling data. Aliases let workload/profile names stay stable.
    "resnet50": ("resnet50", "ResNet50_Weights"),
    "resnet101": ("resnet101", "ResNet101_Weights"),
    "vgg16": ("vgg16", "VGG16_Weights"),
    "mobilenet_v3_large": ("mobilenet_v3_large", "MobileNet_V3_Large_Weights"),
    "efficientnet_b0": ("efficientnet_b0", "EfficientNet_B0_Weights"),
    "vit_b_16": ("vit_b_16", "ViT_B_16_Weights"),
    "vit_base": ("vit_b_16", "ViT_B_16_Weights"),
    "convnext_tiny": ("convnext_tiny", "ConvNeXt_Tiny_Weights"),
}


class CudaWorker:
    """Runs every CUDA call of the runtime on one long-lived thread.

    ThreadingHTTPServer handles each request on a new thread, and the first
    CUDA call on a new thread sets up PyTorch's per-thread CUDA/cuBLAS/cuDNN
    state.  Doing inference there added ~6 ms of idle GPU time per call
    between the timing events (vgg16 b1 on 2g: 10.4 ms vs 4.2 ms on a fixed
    thread), inflating small-batch latencies.  Requests therefore hand their
    work to this thread and wait; it also executes one batch at a time.
    """

    def __init__(self) -> None:
        self.jobs: "queue.Queue" = queue.Queue()
        threading.Thread(target=self._loop, name="cuda-worker", daemon=True).start()

    def _loop(self) -> None:
        while True:
            fn, done, box = self.jobs.get()
            try:
                box["result"] = fn()
            except BaseException as exc:  # handed back to the request thread
                box["error"] = exc
            done.set()

    def run(self, fn):
        done, box = threading.Event(), {}
        self.jobs.put((fn, done, box))
        done.wait()
        if "error" in box:
            raise box["error"]
        return box["result"]


class RuntimeState:
    def __init__(self) -> None:
        self.model_name = env("MODEL_NAME", "resnet50")
        self.runtime_id = env("OR_SIM_RUNTIME_ID", self.model_name)
        self.batch_size = env_int("BATCH_SIZE", 4)
        self.runtime_mode = "torchvision"
        self.weights_mode = env("TORCHVISION_WEIGHTS", "default")
        self.image_size = env_int("TORCHVISION_IMAGE_SIZE", default_image_size(self.model_name))
        self.warmup_iters = env_int("TORCHVISION_WARMUP_ITERS", 5)
        self.started_at = time.time()
        self.lock = threading.Lock()
        # All CUDA work (load, warm-up, inputs, inference) runs on this one
        # thread, one batch at a time: the router keeps up to two batches in
        # flight per replica so the next one waits here instead of the GPU
        # idling through the router round trip.
        self.worker = CudaWorker()
        self.requests = 0
        self.errors = 0
        self.total_runtime_latency_ms = 0.0
        self.total_wall_latency_ms = 0.0
        self.last_runtime_latency_ms = 0.0
        self.device = "cuda" if torch is not None and torch.cuda.is_available() else "cpu"
        self.load_error = ""
        self.model = None
        self.input_tensors: dict[int, Any] = {}
        self.load_timings: dict[str, float] = {}
        self.worker.run(self.load_model)

    def load_model(self) -> None:
        if IMPORT_ERROR:
            self.load_error = IMPORT_ERROR
            return
        if self.model_name not in MODEL_SPECS:
            self.load_error = f"unsupported torchvision model {self.model_name!r}"
            return
        started = time.perf_counter()
        try:
            factory_name, weights_class_name = MODEL_SPECS[self.model_name]
            factory = getattr(models, factory_name)
            kwargs: dict[str, Any] = {}
            weights_started = time.perf_counter()
            if self.weights_mode.lower() in {"default", "pretrained", "true", "1"}:
                weights_cls = getattr(models, weights_class_name, None)
                if weights_cls is not None:
                    kwargs["weights"] = weights_cls.DEFAULT
                else:
                    kwargs["pretrained"] = True
            else:
                kwargs["weights"] = None
            self.load_timings["weightsResolveSec"] = time.perf_counter() - weights_started
            factory_started = time.perf_counter()
            model = factory(**kwargs)
            self.load_timings["modelFactorySec"] = time.perf_counter() - factory_started
            device_started = time.perf_counter()
            model.eval()
            model.to(self.device)
            if self.device == "cuda":
                torch.cuda.synchronize()
            self.load_timings["modelEvalAndDeviceSyncSec"] = time.perf_counter() - device_started
            self.model = model
            input_started = time.perf_counter()
            self.input_tensors[self.batch_size] = self.make_input(self.batch_size)
            self.load_timings["inputTensorBuildSec"] = time.perf_counter() - input_started
            warmup_started = time.perf_counter()
            self.warmup()
            self.load_timings["warmupSec"] = time.perf_counter() - warmup_started
            self.load_timings["totalLoadSec"] = time.perf_counter() - started
            self.load_timings["loadedAtSinceStartSec"] = time.time() - self.started_at
        except Exception as exc:
            self.load_error = str(exc)

    def make_input(self, batch_size: int):
        return torch.randn(
            max(1, batch_size),
            3,
            self.image_size,
            self.image_size,
            device=self.device,
        )

    def input_for_batch(self, batch_size: int):
        batch_size = max(1, batch_size)
        with self.lock:
            if batch_size not in self.input_tensors:
                self.input_tensors[batch_size] = self.make_input(batch_size)
            return self.input_tensors[batch_size]

    def warmup(self) -> None:
        if self.model is None:
            return
        input_tensor = self.input_for_batch(self.batch_size)
        with torch.inference_mode():
            for _ in range(max(0, self.warmup_iters)):
                _ = self.model(input_tensor)
            if self.device == "cuda":
                torch.cuda.synchronize()

    def control_batch(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        next_batch = int_value(payload.get("batchSize") or payload.get("batch"), 0)
        if next_batch <= 0:
            raise ValueError("batchSize must be positive")
        if self.model is None:
            raise RuntimeError(self.load_error or "model is not loaded")
        # Build the tensor before publishing the new default so the metrics update
        # only after this runtime can actually serve the requested batch.
        self.worker.run(lambda: self.input_for_batch(next_batch))
        with self.lock:
            previous = self.batch_size
            self.batch_size = next_batch
        return {
            "model": self.model_name,
            "runtimeId": self.runtime_id,
            "runtimeMode": self.runtime_mode,
            "previousBatchSize": previous,
            "batchSize": next_batch,
            "applied": True,
            "requiresRestart": False,
        }

    def infer(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        if self.model is None:
            raise RuntimeError(self.load_error or "model is not loaded")
        payload = payload or {}
        request_batch = int_value(payload.get("batch"), self.batch_size)
        queued_at = time.perf_counter()
        return self.worker.run(lambda: self._infer_on_worker(request_batch, (time.perf_counter() - queued_at) * 1000.0))

    def _infer_on_worker(self, request_batch: int, infer_queue_ms: float) -> dict[str, Any]:
        input_tensor = self.input_for_batch(request_batch)
        wall_start = time.perf_counter()
        if self.device == "cuda":
            start = torch.cuda.Event(enable_timing=True)
            end = torch.cuda.Event(enable_timing=True)
            with torch.inference_mode():
                start.record()
                output = self.model(input_tensor)
                end.record()
                torch.cuda.synchronize()
            runtime_latency_ms = float(start.elapsed_time(end))
        else:
            with torch.inference_mode():
                started = time.perf_counter()
                output = self.model(input_tensor)
                runtime_latency_ms = (time.perf_counter() - started) * 1000.0
        wall_latency_ms = (time.perf_counter() - wall_start) * 1000.0
        top_class = int(torch.argmax(output[0]).item()) if hasattr(output, "__getitem__") else 0
        self.record(runtime_latency_ms, wall_latency_ms, failed=False)
        return {
            "model": self.model_name,
            "runtimeId": self.runtime_id,
            "runtimeMode": self.runtime_mode,
            "batchSize": request_batch,
            "maxBatchSize": self.batch_size,
            "device": self.device,
            "runtimeLatencyMs": runtime_latency_ms,
            "latencyMs": wall_latency_ms,
            "inferQueueMs": infer_queue_ms,
            "topClass": top_class,
        }

    def record(self, runtime_latency_ms: float, wall_latency_ms: float, failed: bool) -> None:
        with self.lock:
            self.requests += 1
            if failed:
                self.errors += 1
            self.total_runtime_latency_ms += runtime_latency_ms
            self.total_wall_latency_ms += wall_latency_ms
            self.last_runtime_latency_ms = runtime_latency_ms

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            avg_runtime = self.total_runtime_latency_ms / self.requests if self.requests else 0.0
            avg_wall = self.total_wall_latency_ms / self.requests if self.requests else 0.0
            throughput = (1000.0 * self.batch_size / avg_runtime) if avg_runtime > 0 else 0.0
            return {
                "model": self.model_name,
                "torchvisionModel": MODEL_SPECS.get(self.model_name, ("", ""))[0],
                "runtimeId": self.runtime_id,
                "runtimeMode": self.runtime_mode,
                "weightsMode": self.weights_mode,
                "device": self.device,
                "imageSize": self.image_size,
                "uptimeSeconds": time.time() - self.started_at,
                "requests": self.requests,
                "errors": self.errors,
                "batchSize": self.batch_size,
                "avgLatencyMs": avg_wall,
                "runtimeLatencyMs": avg_runtime,
                "runtimeThroughput": throughput,
                "lastRuntimeLatencyMs": self.last_runtime_latency_ms,
                "migUuid": os.environ.get("OR_SIM_MIG_UUID", ""),
                "profile": os.environ.get("OR_SIM_PROFILE", ""),
                "slotResource": os.environ.get("OR_SIM_SLOT_RESOURCE", ""),
                "deviceResource": os.environ.get("OR_SIM_DEVICE_RESOURCE", ""),
                "expectedMigUuid": os.environ.get("OR_SIM_EXPECTED_MIG_UUID", ""),
                "physicalGpuId": os.environ.get("OR_SIM_PHYSICAL_GPU_ID", ""),
                "orSimMIGUUID": os.environ.get("OR_SIM_MIG_UUID", ""),
                "orSimSlot": os.environ.get("OR_SIM_SLOT", ""),
                "orSimSlotResource": os.environ.get("OR_SIM_SLOT_RESOURCE", ""),
                "orSimDeviceResource": os.environ.get("OR_SIM_DEVICE_RESOURCE", ""),
                "orSimExpectedMIGUUID": os.environ.get("OR_SIM_EXPECTED_MIG_UUID", ""),
                "orSimPhysicalGpuID": os.environ.get("OR_SIM_PHYSICAL_GPU_ID", ""),
                "loadTimings": dict(self.load_timings),
                "loadError": self.load_error,
                "cpuSet": os.environ.get("OR_SIM_CPU_SET", ""),
                "cpuExclude": os.environ.get("OR_SIM_CPU_EXCLUDE", ""),
                "cpuAffinity": CPU_AFFINITY,
                "loaded": self.model is not None,
            }


FRONTEND_CPU_AFFINITY = ""


def _apply_frontend_affinity() -> str:
    """Move this (HTTP) process to OR_SIM_FRONTEND_CPU_SET, minus
    OR_SIM_CPU_EXCLUDE.  Linux affinity is per thread and inherited, so this
    runs after the inference child is spawned and before any other thread
    starts."""
    spec = os.environ.get("OR_SIM_FRONTEND_CPU_SET", "").strip()
    if spec:
        try:
            cpus = parse_cpu_list(spec) - parse_cpu_list(os.environ.get("OR_SIM_CPU_EXCLUDE", ""))
            if cpus:
                os.sched_setaffinity(0, cpus)
        except (OSError, ValueError) as exc:
            print(f"ignoring OR_SIM_FRONTEND_CPU_SET={spec!r}: {exc}", file=sys.stderr, flush=True)
    return format_cpu_list(sorted(os.sched_getaffinity(0)))


def _inference_main(conn) -> None:
    """Child process: owns CUDA and runs requests one at a time.

    The module was re-imported under spawn, so apply_cpu_exclude() already
    confined this process to the runtime's core before torch was imported.
    No HTTP thread lives here, so nothing competes with the kernel-launching
    thread for the interpreter lock or the core.
    """
    state = RuntimeState()
    conn.send(("ready", {
        "model_name": state.model_name,
        "model_id": getattr(state, "model_id", None),
        "device": state.device,
        "loaded": state.model is not None,
        "load_error": state.load_error,
    }))
    while True:
        try:
            req_id, method, args, kwargs = conn.recv()
        except (EOFError, OSError):
            return
        try:
            conn.send((req_id, True, getattr(state, method)(*args, **kwargs)))
        except Exception as exc:  # returned to the HTTP process
            conn.send((req_id, False, (type(exc).__name__, str(exc))))


class InferenceProcess:
    """HTTP-process proxy for the RuntimeState running in the child.

    With the router keeping two requests in flight, the HTTP handler of the
    next request used to run in the same process (and on the same core) as
    the kernel launches of the current one; on a 2.2 GHz host that slowed
    launch-bound options by 9-34% (resnet50 1g b1: 5.3 -> 5.9-7.1 ms).
    Requests now go over a pipe to a spawned child; this process only parses
    HTTP, on its own cores (OR_SIM_FRONTEND_CPU_SET).
    """

    def __init__(self) -> None:
        global FRONTEND_CPU_AFFINITY
        ctx = mp.get_context("spawn")
        self._conn, child_conn = ctx.Pipe()
        self._proc = ctx.Process(target=_inference_main, args=(child_conn,), name="inference", daemon=True)
        self._proc.start()
        child_conn.close()
        FRONTEND_CPU_AFFINITY = _apply_frontend_affinity()
        self._send_lock = threading.Lock()
        self._waiters: dict[int, tuple[threading.Event, dict[str, Any]]] = {}
        self._next_id = 0
        self._dead = ""
        _, info = self._conn.recv()
        self.model_name = info["model_name"]
        self.model_id = info.get("model_id")
        self.device = info["device"]
        self.load_error = info.get("load_error", "")
        self.model = True if info["loaded"] else None
        threading.Thread(target=self._reader, name="inference-reader", daemon=True).start()
        self._snapshot_lock = threading.Lock()
        self._refreshing = False
        self._snapshot = self._call("snapshot")

    def _reader(self) -> None:
        try:
            while True:
                req_id, ok, value = self._conn.recv()
                event, box = self._waiters.pop(req_id)
                box["ok"], box["value"] = ok, value
                event.set()
        except (EOFError, OSError):
            self._proc.join(1.0)
            self._dead = f"inference process exited (code {self._proc.exitcode})"
            self.model = None
            for event, box in list(self._waiters.values()):
                box["ok"], box["value"] = False, ("RuntimeError", self._dead)
                event.set()
            # Let pending requests get their error, then exit so the container
            # restarts instead of serving 503 forever.
            print(self._dead, file=sys.stderr, flush=True)
            time.sleep(2.0)
            os._exit(1)

    def _call(self, method: str, *args: Any, **kwargs: Any) -> Any:
        if self._dead:
            raise RuntimeError(self._dead)
        event, box = threading.Event(), {}
        with self._send_lock:
            req_id = self._next_id
            self._next_id += 1
            self._waiters[req_id] = (event, box)
            self._conn.send((req_id, method, args, kwargs))
        event.wait()
        if box["ok"]:
            return box["value"]
        kind, message = box["value"]
        raise (ValueError if kind == "ValueError" else RuntimeError)(message)

    def infer(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        started = time.perf_counter()
        out = self._call("infer", payload)
        # queueing behind the in-flight request plus pipe transfer
        out["inferQueueMs"] = max(0.0, (time.perf_counter() - started) * 1000.0 - float(out.get("latencyMs") or 0.0))
        return out

    def control_batch(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        out = self._call("control_batch", payload)
        # verify_batch reads batchSize from /metrics right after this returns.
        self._snapshot = self._call("snapshot")
        return out

    def snapshot(self) -> dict[str, Any]:
        # /healthz and /metrics answer from a cached copy instead of queueing
        # behind the request the child is running: an LLM generate takes up to
        # several seconds and the router drops a route whose /healthz misses
        # its 3 s timeout three times in a row.  Each call starts at most one
        # background refresh, so the copy lags by at most one request.
        if self._dead:
            out = {"model": self.model_name, "loadError": self._dead, "loaded": False}
        else:
            self._refresh_snapshot_async()
            out = dict(self._snapshot)
        out["frontendCpuAffinity"] = FRONTEND_CPU_AFFINITY
        return out

    def _refresh_snapshot_async(self) -> None:
        with self._snapshot_lock:
            if self._refreshing:
                return
            self._refreshing = True
        threading.Thread(target=self._refresh_snapshot, name="snapshot-refresh", daemon=True).start()

    def _refresh_snapshot(self) -> None:
        try:
            self._snapshot = self._call("snapshot")
        except RuntimeError:
            pass
        finally:
            self._refreshing = False

    def record(self, *args: Any, **kwargs: Any) -> None:
        try:
            self._call("record", *args, **kwargs)
        except RuntimeError:
            pass


STATE: RuntimeState | InferenceProcess


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/healthz":
            payload = STATE.snapshot()
            payload.update(
                {
                    "ok": STATE.model is not None,
                    "nvidiaVisibleDevices": os.environ.get("NVIDIA_VISIBLE_DEVICES", ""),
                    "orSimMIGUUID": os.environ.get("OR_SIM_MIG_UUID", ""),
                    "orSimSlot": os.environ.get("OR_SIM_SLOT", ""),
                    "orSimSlotResource": os.environ.get("OR_SIM_SLOT_RESOURCE", ""),
                    "orSimDeviceResource": os.environ.get("OR_SIM_DEVICE_RESOURCE", ""),
                    "orSimExpectedMIGUUID": os.environ.get("OR_SIM_EXPECTED_MIG_UUID", ""),
                    "orSimPhysicalGpuID": os.environ.get("OR_SIM_PHYSICAL_GPU_ID", ""),
                }
            )
            self._json(200 if STATE.model is not None else 503, payload)
            return
        if self.path == "/metrics":
            self._json(200, STATE.snapshot())
            return
        self._json(404, {"error": "not found"})

    def do_POST(self) -> None:
        if self.path == "/infer":
            try:
                length = int(self.headers.get("content-length", "0"))
                payload: dict[str, Any] = {}
                if length > 0:
                    payload = json.loads(self.rfile.read(length).decode() or "{}")
                self._json(200, STATE.infer(payload))
            except Exception as exc:
                STATE.record(0.0, 0.0, failed=True)
                self._json(500, {"error": str(exc), "model": STATE.model_name})
            return
        if self.path == "/control/batch":
            try:
                length = int(self.headers.get("content-length", "0"))
                payload: dict[str, Any] = {}
                if length > 0:
                    payload = json.loads(self.rfile.read(length).decode() or "{}")
                self._json(200, STATE.control_batch(payload))
            except ValueError as exc:
                self._json(400, {"error": str(exc), "model": STATE.model_name})
            except Exception as exc:
                self._json(500, {"error": str(exc), "model": STATE.model_name})
            return
        self._json(404, {"error": "not found"})

    def do_PUT(self) -> None:
        if self.path == "/control/batch":
            self.do_POST()
            return
        self._json(404, {"error": "not found"})

    def log_message(self, fmt: str, *args: Any) -> None:
        print(fmt % args, file=sys.stderr, flush=True)

    def _json(self, status: int, payload: dict[str, Any]) -> None:
        raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


def default_image_size(model: str) -> int:
    if model == "efficientnet_b4":
        return 380
    return 224


def env(key: str, fallback: str) -> str:
    return os.environ.get(key) or fallback


def env_int(key: str, fallback: int) -> int:
    try:
        return int(os.environ.get(key, ""))
    except ValueError:
        return fallback


def int_value(value: Any, fallback: int) -> int:
    try:
        if value in (None, ""):
            return fallback
        return int(value)
    except (TypeError, ValueError):
        return fallback


def parse_addr(value: str) -> tuple[str, int]:
    if value.startswith(":"):
        return "", int(value[1:])
    host, _, port = value.rpartition(":")
    return host, int(port)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--addr", default=":8080")
    args = parser.parse_args()
    global STATE
    STATE = InferenceProcess()
    host, port = parse_addr(args.addr)
    print(
        f"torchvision runtime listening on {args.addr}, model={STATE.model_name}, "
        f"device={STATE.device}, loaded={STATE.model is not None}",
        flush=True,
    )
    ThreadingHTTPServer((host, port), Handler).serve_forever()


if __name__ == "__main__":
    main()
