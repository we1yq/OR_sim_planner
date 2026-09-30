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
    AutoModelForCausalLM = None
    AutoTokenizer = None
    StoppingCriteria = object
    StoppingCriteriaList = None
    IMPORT_ERROR = ""
else:
    try:
        import torch
        from transformers import AutoModelForCausalLM, AutoTokenizer, StoppingCriteria, StoppingCriteriaList
    except Exception as exc:  # pragma: no cover - surfaced through /healthz
        torch = None
        AutoModelForCausalLM = None
        AutoTokenizer = None
        StoppingCriteria = object
        StoppingCriteriaList = None
        IMPORT_ERROR = str(exc)
    else:
        IMPORT_ERROR = ""


class FirstTokenMark(StoppingCriteria):
    """Marks when generate() has produced its first token; never stops.

    generate() calls stopping criteria once per generated token, right after
    the token is selected, so the first call is the time to first token.  On
    CUDA the mark is an event on the stream (no extra synchronisation); on CPU
    it is a perf_counter reading.
    """

    def __init__(self, cuda: bool) -> None:
        self.cuda = cuda
        self.event = None
        self.at = None

    def __call__(self, input_ids, scores, **kwargs):
        if self.event is None and self.at is None:
            if self.cuda:
                self.event = torch.cuda.Event(enable_timing=True)
                self.event.record()
            else:
                self.at = time.perf_counter()
        return torch.zeros(input_ids.shape[0], dtype=torch.bool, device=input_ids.device)


MODEL_ALIASES = {
    "gpt2": "gpt2-medium",
    "gpt2-medium": "gpt2-medium",
    "llama": "meta-llama/Llama-3.2-3B",
    "llama32_3b": "meta-llama/Llama-3.2-3B",
    "llama-3.2-3b": "meta-llama/Llama-3.2-3B",
    "llama32_3b_instruct": "meta-llama/Llama-3.2-3B-Instruct",
}


class CudaWorker:
    """Runs every CUDA call of the runtime on one long-lived thread.

    ThreadingHTTPServer handles each request on a new thread, and the first
    CUDA call on a new thread sets up PyTorch's per-thread CUDA/cuBLAS/cuDNN
    state.  Doing inference there added ~6 ms of idle GPU time per call
    between the timing events (vgg16 b1 on 2g: 10.4 ms vs 4.2 ms on a fixed
    thread), inflating small-batch latencies (see torchvision_runtime.py).
    Requests hand their work to this thread and wait; it also runs one
    generate() at a time.
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
        self.model_name = env("MODEL_NAME", "gpt2-medium")
        self.model_id = env("MODEL_ID", MODEL_ALIASES.get(self.model_name, self.model_name))
        self.runtime_id = env("OR_SIM_RUNTIME_ID", self.model_name)
        self.batch_size = env_int("BATCH_SIZE", 1)
        self.prompt_len = env_positive_int("PROMPT_LEN", 64)
        self.output_tokens = env_positive_int("OUTPUT_TOKENS", 64)
        self.dtype_name = env("MODEL_DTYPE", "float16")
        self.warmup_iters = env_int("LLM_WARMUP_ITERS", 1)
        self.started_at = time.time()
        self.lock = threading.Lock()
        # All CUDA work runs on this one thread, one generate() at a time.
        self.worker = CudaWorker()
        self.requests = 0
        self.errors = 0
        self.total_ttft_ms = 0.0
        self.total_decode_ms = 0.0
        self.total_service_ms = 0.0
        self.last_ttft_ms = 0.0
        self.last_tpot_ms = 0.0
        self.last_service_ms = 0.0
        self.device = "cuda" if torch is not None and torch.cuda.is_available() else "cpu"
        self.load_error = ""
        self.load_timings: dict[str, float] = {}
        self.tokenizer = None
        self.model = None
        self.prompt_input_ids = None
        self.worker.run(self.load_model)

    def load_model(self) -> None:
        total_started = time.perf_counter()
        if IMPORT_ERROR:
            self.load_error = IMPORT_ERROR
            self.load_timings["totalLoadSec"] = time.perf_counter() - total_started
            return
        try:
            token = os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN")
            dtype = self.resolve_dtype()
            self.load_timings["startToLoadModelSec"] = time.time() - self.started_at
            started = time.perf_counter()
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_id, token=token)
            self.load_timings["tokenizerFromPretrainedSec"] = time.perf_counter() - started
            if self.tokenizer.pad_token_id is None:
                self.tokenizer.pad_token = self.tokenizer.eos_token
            kwargs: dict[str, Any] = {"token": token}
            if self.device == "cuda":
                kwargs["torch_dtype"] = dtype
                kwargs["device_map"] = "cuda"
            started = time.perf_counter()
            self.model = AutoModelForCausalLM.from_pretrained(self.model_id, **kwargs)
            self.load_timings["modelFromPretrainedSec"] = time.perf_counter() - started
            started = time.perf_counter()
            self.model.eval()
            if self.device != "cuda":
                self.model.to(self.device)
            if self.device == "cuda":
                torch.cuda.synchronize()
            self.load_timings["modelEvalAndDeviceSyncSec"] = time.perf_counter() - started
            started = time.perf_counter()
            self.prompt_input_ids = self.make_prompt(self.prompt_len, self.batch_size)
            if self.device == "cuda":
                torch.cuda.synchronize()
            self.load_timings["promptBuildSec"] = time.perf_counter() - started
            started = time.perf_counter()
            self.warmup()
            self.load_timings["warmupSec"] = time.perf_counter() - started
        except Exception as exc:
            self.load_error = str(exc)
        finally:
            self.load_timings["totalLoadSec"] = time.perf_counter() - total_started
            self.load_timings["loadedAtSinceStartSec"] = time.time() - self.started_at

    def resolve_dtype(self):
        if self.dtype_name.lower() in {"bfloat16", "bf16"}:
            return torch.bfloat16
        if self.dtype_name.lower() in {"float32", "fp32"}:
            return torch.float32
        return torch.float16

    def make_prompt(self, prompt_len: int, batch_size: int):
        assert self.tokenizer is not None
        text = " ".join(["hello"] * max(1, prompt_len))
        ids = self.tokenizer(text, return_tensors="pt", add_special_tokens=False).input_ids[:, :prompt_len]
        if ids.shape[1] < prompt_len:
            pad_id = self.tokenizer.eos_token_id or 0
            pad = torch.full((1, prompt_len - ids.shape[1]), pad_id, dtype=ids.dtype)
            ids = torch.cat([ids, pad], dim=1)
        ids = ids.repeat(batch_size, 1).to(self.device)
        return ids

    def warmup(self) -> None:
        if self.model is None or self.prompt_input_ids is None:
            return
        for _ in range(max(0, self.warmup_iters)):
            self.generate_once(max_new_tokens=min(4, max(1, self.output_tokens)))
        if self.device == "cuda":
            torch.cuda.empty_cache()

    def control_batch(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        next_batch = int_value(payload.get("batchSize") or payload.get("batch"), 0)
        if next_batch <= 0:
            raise ValueError("batchSize must be positive")
        if self.model is None or self.tokenizer is None:
            raise RuntimeError(self.load_error or "model is not loaded")
        # Batch is the only mutable serving capacity knob here. Prompt/output
        # length define the request class and must stay fixed for this runtime.
        def build():
            ids = self.make_prompt(self.prompt_len, next_batch)
            if self.device == "cuda":
                torch.cuda.synchronize()
            return ids
        prompt_input_ids = self.worker.run(build)
        with self.lock:
            previous = self.batch_size
            self.batch_size = next_batch
            self.prompt_input_ids = prompt_input_ids
        return {
            "model": self.model_name,
            "modelId": self.model_id,
            "runtimeId": self.runtime_id,
            "runtimeMode": "transformers",
            "promptLen": self.prompt_len,
            "outputTokens": self.output_tokens,
            "previousBatchSize": previous,
            "batchSize": next_batch,
            "applied": True,
            "requiresRestart": False,
        }

    def infer(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self.model is None or self.prompt_input_ids is None:
            raise RuntimeError(self.load_error or "model is not loaded")
        with self.lock:
            default_batch = self.batch_size
            default_input_ids = self.prompt_input_ids
        prompt_len = int_value(payload.get("prompt_len"), self.prompt_len)
        output_tokens = int_value(payload.get("output_tokens") or payload.get("max_tokens"), self.output_tokens)
        batch_size = int_value(payload.get("batch"), default_batch)
        queued_at = time.perf_counter()

        def run():
            input_ids = default_input_ids
            if prompt_len != self.prompt_len or batch_size != default_batch:
                input_ids = self.make_prompt(prompt_len, batch_size)
            if self.device == "cuda":
                torch.cuda.reset_peak_memory_stats()
            # One generate() per request: TTFT is marked inside it (first token
            # selected), TPOT = (total - TTFT) / (output_tokens - 1).
            started = time.perf_counter()
            total, prefill = self.generate_once(max_new_tokens=output_tokens, input_ids=input_ids, mark_first_token=True)
            wall = (time.perf_counter() - started) * 1000.0
            alloc = reserved = 0.0
            if self.device == "cuda":
                alloc = torch.cuda.max_memory_allocated() / (1024 * 1024)
                reserved = torch.cuda.max_memory_reserved() / (1024 * 1024)
            return total, prefill, wall, (started - queued_at) * 1000.0, alloc, reserved

        total_ms, prefill_ms, wall_ms, infer_queue_ms, peak_alloc_mb, peak_reserved_mb = self.worker.run(run)
        decode_ms = max(0.0, total_ms - prefill_ms)
        tpot_ms = decode_ms / max(1, output_tokens - 1)
        decode_tps = 1000.0 / tpot_ms if tpot_ms > 0 else 0.0
        self.record(prefill_ms, decode_ms, total_ms, failed=False)
        return {
            "model": self.model_name,
            "modelId": self.model_id,
            "runtimeId": self.runtime_id,
            "runtimeMode": "transformers",
            "batchSize": batch_size,
            "promptLen": prompt_len,
            "outputTokens": output_tokens,
            "device": self.device,
            "ttftMs": prefill_ms,
            "tpotMs": tpot_ms,
            "decodeTps": decode_tps,
            "runtimeLatencyMs": total_ms,
            "latencyMs": wall_ms,
            "inferQueueMs": infer_queue_ms,
            "peakAllocMb": peak_alloc_mb,
            "peakReservedMb": peak_reserved_mb,
        }

    def generate_once(self, max_new_tokens: int, input_ids=None, mark_first_token: bool = False):
        """Run one generate(); return total ms, or (total ms, first-token ms)
        when ``mark_first_token``."""
        if input_ids is None:
            input_ids = self.prompt_input_ids
        assert input_ids is not None
        cuda = self.device == "cuda"
        mark = FirstTokenMark(cuda) if mark_first_token else None
        kwargs = {
            "input_ids": input_ids,
            "max_new_tokens": max_new_tokens,
            "do_sample": False,
            "pad_token_id": self.tokenizer.eos_token_id if self.tokenizer is not None else None,
            "use_cache": True,
        }
        if mark is not None:
            kwargs["stopping_criteria"] = StoppingCriteriaList([mark])
        with torch.inference_mode():
            if cuda:
                start = torch.cuda.Event(enable_timing=True)
                end = torch.cuda.Event(enable_timing=True)
                start.record()
                _ = self.model.generate(**kwargs)
                end.record()
                torch.cuda.synchronize()
                total_ms = float(start.elapsed_time(end))
                first_ms = float(start.elapsed_time(mark.event)) if mark is not None and mark.event is not None else total_ms
            else:
                started = time.perf_counter()
                _ = self.model.generate(**kwargs)
                finished = time.perf_counter()
                total_ms = (finished - started) * 1000.0
                first_ms = ((mark.at - started) * 1000.0) if mark is not None and mark.at is not None else total_ms
        if mark_first_token:
            return total_ms, first_ms
        return total_ms

    def record(self, ttft_ms: float, decode_ms: float, service_ms: float, failed: bool) -> None:
        with self.lock:
            self.requests += 1
            if failed:
                self.errors += 1
            self.total_ttft_ms += ttft_ms
            self.total_decode_ms += decode_ms
            self.total_service_ms += service_ms
            self.last_ttft_ms = ttft_ms
            self.last_tpot_ms = decode_ms / max(1, self.output_tokens - 1)
            self.last_service_ms = service_ms

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            avg_ttft = self.total_ttft_ms / self.requests if self.requests else 0.0
            avg_decode = self.total_decode_ms / self.requests if self.requests else 0.0
            avg_service = self.total_service_ms / self.requests if self.requests else 0.0
            avg_tpot = avg_decode / max(1, self.output_tokens - 1)
            throughput = (1000.0 * self.batch_size / avg_service) if avg_service > 0 else 0.0
            return {
                "model": self.model_name,
                "modelId": self.model_id,
                "runtimeId": self.runtime_id,
                "runtimeMode": "transformers",
                "device": self.device,
                "dtype": self.dtype_name,
                "uptimeSeconds": time.time() - self.started_at,
                "requests": self.requests,
                "errors": self.errors,
                "batchSize": self.batch_size,
                "promptLen": self.prompt_len,
                "outputTokens": self.output_tokens,
                "ttftMs": avg_ttft,
                "tpotMs": avg_tpot,
                "decodeTps": 1000.0 / avg_tpot if avg_tpot > 0 else 0.0,
                "runtimeLatencyMs": avg_service,
                "runtimeThroughput": throughput,
                "lastTtftMs": self.last_ttft_ms,
                "lastTpotMs": self.last_tpot_ms,
                "lastRuntimeLatencyMs": self.last_service_ms,
                "loadTimings": dict(self.load_timings),
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
            payload.update({"ok": STATE.model is not None, "nvidiaVisibleDevices": os.environ.get("NVIDIA_VISIBLE_DEVICES", "")})
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
                payload = json.loads(self.rfile.read(length).decode()) if length > 0 else {}
                self._json(200, STATE.infer(payload))
            except Exception as exc:
                STATE.record(0.0, 0.0, 0.0, failed=True)
                self._json(500, {"error": str(exc), "model": STATE.model_name})
            return
        if self.path == "/control/batch":
            try:
                length = int(self.headers.get("content-length", "0"))
                payload = json.loads(self.rfile.read(length).decode()) if length > 0 else {}
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


def env(key: str, fallback: str) -> str:
    return os.environ.get(key) or fallback


def env_int(key: str, fallback: int) -> int:
    try:
        return int(os.environ.get(key, ""))
    except ValueError:
        return fallback


def env_positive_int(key: str, fallback: int) -> int:
    value = env_int(key, fallback)
    return value if value > 0 else fallback


def int_value(value: Any, fallback: int) -> int:
    try:
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
        f"transformers runtime listening on {args.addr}, model={STATE.model_name}, "
        f"model_id={STATE.model_id}, device={STATE.device}, loaded={STATE.model is not None}",
        flush=True,
    )
    ThreadingHTTPServer((host, port), Handler).serve_forever()


if __name__ == "__main__":
    main()
