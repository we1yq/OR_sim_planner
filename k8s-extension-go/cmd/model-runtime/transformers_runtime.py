#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

try:
    import torch
    from transformers import AutoModelForCausalLM, AutoTokenizer
except Exception as exc:  # pragma: no cover - surfaced through /healthz
    torch = None
    AutoModelForCausalLM = None
    AutoTokenizer = None
    IMPORT_ERROR = str(exc)
else:
    IMPORT_ERROR = ""


MODEL_ALIASES = {
    "gpt2": "gpt2-medium",
    "gpt2-medium": "gpt2-medium",
    "llama": "meta-llama/Llama-3.2-3B",
    "llama32_3b": "meta-llama/Llama-3.2-3B",
    "llama-3.2-3b": "meta-llama/Llama-3.2-3B",
    "llama32_3b_instruct": "meta-llama/Llama-3.2-3B-Instruct",
}


class RuntimeState:
    def __init__(self) -> None:
        self.model_name = env("MODEL_NAME", "gpt2-medium")
        self.model_id = env("MODEL_ID", MODEL_ALIASES.get(self.model_name, self.model_name))
        self.runtime_id = env("OR_SIM_RUNTIME_ID", self.model_name)
        self.batch_size = env_int("BATCH_SIZE", 1)
        self.prompt_len = env_int("PROMPT_LEN", 64)
        self.output_tokens = env_int("OUTPUT_TOKENS", 64)
        self.dtype_name = env("MODEL_DTYPE", "float16")
        self.warmup_iters = env_int("LLM_WARMUP_ITERS", 1)
        self.started_at = time.time()
        self.lock = threading.Lock()
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
        self.tokenizer = None
        self.model = None
        self.prompt_input_ids = None
        self.load_model()

    def load_model(self) -> None:
        if IMPORT_ERROR:
            self.load_error = IMPORT_ERROR
            return
        try:
            token = os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN")
            dtype = self.resolve_dtype()
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_id, token=token)
            if self.tokenizer.pad_token_id is None:
                self.tokenizer.pad_token = self.tokenizer.eos_token
            kwargs: dict[str, Any] = {"token": token}
            if self.device == "cuda":
                kwargs["torch_dtype"] = dtype
                kwargs["device_map"] = "cuda"
            self.model = AutoModelForCausalLM.from_pretrained(self.model_id, **kwargs)
            self.model.eval()
            if self.device != "cuda":
                self.model.to(self.device)
            self.prompt_input_ids = self.make_prompt(self.prompt_len, self.batch_size)
            self.warmup()
        except Exception as exc:
            self.load_error = str(exc)

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

    def infer(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self.model is None or self.prompt_input_ids is None:
            raise RuntimeError(self.load_error or "model is not loaded")
        prompt_len = int_value(payload.get("prompt_len"), self.prompt_len)
        output_tokens = int_value(payload.get("output_tokens") or payload.get("max_tokens"), self.output_tokens)
        batch_size = int_value(payload.get("batch"), self.batch_size)
        input_ids = self.prompt_input_ids
        if prompt_len != self.prompt_len or batch_size != self.batch_size:
            input_ids = self.make_prompt(prompt_len, batch_size)

        if self.device == "cuda":
            torch.cuda.reset_peak_memory_stats()
        started = time.perf_counter()
        prefill_ms = self.prefill(input_ids)
        total_ms = self.generate_once(max_new_tokens=output_tokens, input_ids=input_ids)
        wall_ms = (time.perf_counter() - started) * 1000.0
        decode_ms = max(0.0, total_ms - prefill_ms)
        tpot_ms = decode_ms / max(1, output_tokens)
        decode_tps = 1000.0 / tpot_ms if tpot_ms > 0 else 0.0
        peak_alloc_mb = 0.0
        peak_reserved_mb = 0.0
        if self.device == "cuda":
            peak_alloc_mb = torch.cuda.max_memory_allocated() / (1024 * 1024)
            peak_reserved_mb = torch.cuda.max_memory_reserved() / (1024 * 1024)
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
            "peakAllocMb": peak_alloc_mb,
            "peakReservedMb": peak_reserved_mb,
        }

    def prefill(self, input_ids) -> float:
        if self.device == "cuda":
            start = torch.cuda.Event(enable_timing=True)
            end = torch.cuda.Event(enable_timing=True)
            with torch.inference_mode():
                start.record()
                _ = self.model(input_ids=input_ids, use_cache=True)
                end.record()
                torch.cuda.synchronize()
            return float(start.elapsed_time(end))
        with torch.inference_mode():
            started = time.perf_counter()
            _ = self.model(input_ids=input_ids, use_cache=True)
            return (time.perf_counter() - started) * 1000.0

    def generate_once(self, max_new_tokens: int, input_ids=None) -> float:
        if input_ids is None:
            input_ids = self.prompt_input_ids
        assert input_ids is not None
        if self.device == "cuda":
            start = torch.cuda.Event(enable_timing=True)
            end = torch.cuda.Event(enable_timing=True)
            with torch.inference_mode():
                start.record()
                _ = self.model.generate(
                    input_ids=input_ids,
                    max_new_tokens=max_new_tokens,
                    do_sample=False,
                    pad_token_id=self.tokenizer.eos_token_id if self.tokenizer is not None else None,
                    use_cache=True,
                )
                end.record()
                torch.cuda.synchronize()
            return float(start.elapsed_time(end))
        with torch.inference_mode():
            started = time.perf_counter()
            _ = self.model.generate(input_ids=input_ids, max_new_tokens=max_new_tokens, do_sample=False, use_cache=True)
            return (time.perf_counter() - started) * 1000.0

    def record(self, ttft_ms: float, decode_ms: float, service_ms: float, failed: bool) -> None:
        with self.lock:
            self.requests += 1
            if failed:
                self.errors += 1
            self.total_ttft_ms += ttft_ms
            self.total_decode_ms += decode_ms
            self.total_service_ms += service_ms
            self.last_ttft_ms = ttft_ms
            self.last_tpot_ms = decode_ms / max(1, self.output_tokens)
            self.last_service_ms = service_ms

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            avg_ttft = self.total_ttft_ms / self.requests if self.requests else 0.0
            avg_decode = self.total_decode_ms / self.requests if self.requests else 0.0
            avg_service = self.total_service_ms / self.requests if self.requests else 0.0
            avg_tpot = avg_decode / max(1, self.output_tokens)
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
                "migUuid": os.environ.get("OR_SIM_MIG_UUID", ""),
                "slotResource": os.environ.get("OR_SIM_SLOT_RESOURCE", ""),
                "deviceResource": os.environ.get("OR_SIM_DEVICE_RESOURCE", ""),
                "loadError": self.load_error,
                "loaded": self.model is not None,
            }


STATE: RuntimeState


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
    STATE = RuntimeState()
    host, port = parse_addr(args.addr)
    print(
        f"transformers runtime listening on {args.addr}, model={STATE.model_name}, "
        f"model_id={STATE.model_id}, device={STATE.device}, loaded={STATE.model is not None}",
        flush=True,
    )
    ThreadingHTTPServer((host, port), Handler).serve_forever()


if __name__ == "__main__":
    main()
