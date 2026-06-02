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
        self.requests = 0
        self.errors = 0
        self.total_runtime_latency_ms = 0.0
        self.total_wall_latency_ms = 0.0
        self.last_runtime_latency_ms = 0.0
        self.device = "cuda" if torch is not None and torch.cuda.is_available() else "cpu"
        self.load_error = ""
        self.model = None
        self.input_tensor = None
        self.load_model()

    def load_model(self) -> None:
        if IMPORT_ERROR:
            self.load_error = IMPORT_ERROR
            return
        if self.model_name not in MODEL_SPECS:
            self.load_error = f"unsupported torchvision model {self.model_name!r}"
            return
        try:
            factory_name, weights_class_name = MODEL_SPECS[self.model_name]
            factory = getattr(models, factory_name)
            kwargs: dict[str, Any] = {}
            if self.weights_mode.lower() in {"default", "pretrained", "true", "1"}:
                weights_cls = getattr(models, weights_class_name, None)
                if weights_cls is not None:
                    kwargs["weights"] = weights_cls.DEFAULT
                else:
                    kwargs["pretrained"] = True
            else:
                kwargs["weights"] = None
            model = factory(**kwargs)
            model.eval()
            model.to(self.device)
            self.model = model
            self.input_tensor = torch.randn(
                self.batch_size,
                3,
                self.image_size,
                self.image_size,
                device=self.device,
            )
            self.warmup()
        except Exception as exc:
            self.load_error = str(exc)

    def warmup(self) -> None:
        if self.model is None or self.input_tensor is None:
            return
        with torch.inference_mode():
            for _ in range(max(0, self.warmup_iters)):
                _ = self.model(self.input_tensor)
            if self.device == "cuda":
                torch.cuda.synchronize()

    def infer(self) -> dict[str, Any]:
        if self.model is None or self.input_tensor is None:
            raise RuntimeError(self.load_error or "model is not loaded")
        wall_start = time.perf_counter()
        if self.device == "cuda":
            start = torch.cuda.Event(enable_timing=True)
            end = torch.cuda.Event(enable_timing=True)
            with torch.inference_mode():
                start.record()
                output = self.model(self.input_tensor)
                end.record()
                torch.cuda.synchronize()
            runtime_latency_ms = float(start.elapsed_time(end))
        else:
            with torch.inference_mode():
                started = time.perf_counter()
                output = self.model(self.input_tensor)
                runtime_latency_ms = (time.perf_counter() - started) * 1000.0
        wall_latency_ms = (time.perf_counter() - wall_start) * 1000.0
        top_class = int(torch.argmax(output[0]).item()) if hasattr(output, "__getitem__") else 0
        self.record(runtime_latency_ms, wall_latency_ms, failed=False)
        return {
            "model": self.model_name,
            "runtimeId": self.runtime_id,
            "runtimeMode": self.runtime_mode,
            "batchSize": self.batch_size,
            "device": self.device,
            "runtimeLatencyMs": runtime_latency_ms,
            "latencyMs": wall_latency_ms,
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
                if length > 0:
                    _ = self.rfile.read(length)
                self._json(200, STATE.infer())
            except Exception as exc:
                STATE.record(0.0, 0.0, failed=True)
                self._json(500, {"error": str(exc), "model": STATE.model_name})
            return
        if self.path == "/control/batch":
            self._json(409, {"error": "batch changes require runtime restart in torchvision mode"})
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
        f"torchvision runtime listening on {args.addr}, model={STATE.model_name}, "
        f"device={STATE.device}, loaded={STATE.model is not None}",
        flush=True,
    )
    ThreadingHTTPServer((host, port), Handler).serve_forever()


if __name__ == "__main__":
    main()
