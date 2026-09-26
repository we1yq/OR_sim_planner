#!/usr/bin/env python3
"""Tests for the in-place live profiler using only local fake HTTP servers."""

from __future__ import annotations

import importlib.util
import json
import sys
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


MODULE_PATH = Path(__file__).with_name("live_profile_20260926.py")
SPEC = importlib.util.spec_from_file_location("live_profile_20260926", MODULE_PATH)
assert SPEC and SPEC.loader
live_profile = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = live_profile
SPEC.loader.exec_module(live_profile)


class FakeState:
    def __init__(self, runtime_id: str, profile: str = "3g", batch: int = 2) -> None:
        self.runtime_id = runtime_id
        self.profile = profile
        self.batch = batch
        self.model = "resnet50"
        self.infer_count = 0
        self.active = 0
        self.max_active = 0
        self.payloads: list[dict[str, Any]] = []
        self.infer_started: list[float] = []
        self.fail_samples = False
        self.delay = 0.025
        self.change_batch_after = 0
        self.lock = threading.Lock()

    def snapshot(self) -> dict[str, Any]:
        return {
            "ok": True,
            "model": self.model,
            "runtimeId": self.runtime_id,
            "runtimeMode": "fake",
            "profile": self.profile,
            "batchSize": self.batch,
        }


class FakeHandler(BaseHTTPRequestHandler):
    server: "FakeServer"

    def do_GET(self) -> None:
        if self.path == "/healthz" or self.path == "/metrics":
            self._json(200, self.server.state.snapshot())
            return
        self._json(404, {"error": "not found"})

    def do_POST(self) -> None:
        if self.path != "/infer":
            self._json(404, {"error": "not found"})
            return
        length = int(self.headers.get("content-length", "0"))
        body = json.loads(self.rfile.read(length).decode() or "{}")
        state = self.server.state
        with state.lock:
            state.payloads.append(body)
            state.infer_started.append(time.monotonic())
            state.infer_count += 1
            state.active += 1
            state.max_active = max(state.max_active, state.active)
            count = state.infer_count
        try:
            if state.fail_samples and count > 1:
                self._json(500, {"error": "synthetic failure"})
                return
            time.sleep(state.delay)
            with state.lock:
                if state.change_batch_after and count >= state.change_batch_after:
                    state.batch += 1
            self._json(
                200,
                {
                    "model": state.model,
                    "runtimeId": state.runtime_id,
                    "profile": state.profile,
                    "batchSize": body.get("batch", state.batch),
                    "runtimeLatencyMs": state.delay * 1000.0,
                },
            )
        finally:
            with state.lock:
                state.active -= 1

    def _json(self, status: int, value: dict[str, Any]) -> None:
        raw = json.dumps(value).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, *_args: Any) -> None:
        return


class FakeServer(ThreadingHTTPServer):
    def __init__(self, state: FakeState) -> None:
        super().__init__(("127.0.0.1", 0), FakeHandler)
        self.state = state


class LiveProfileTests(unittest.TestCase):
    def start_server(self, state: FakeState) -> tuple[FakeServer, threading.Thread]:
        server = FakeServer(state)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        return server, thread

    @staticmethod
    def descriptor(server: FakeServer, state: FakeState) -> dict[str, Any]:
        return {
            "replicaId": state.runtime_id,
            "endpoint": f"http://127.0.0.1:{server.server_port}",
            "model": state.model,
            "runtimeId": state.runtime_id,
            "profile": state.profile,
            "batchSize": state.batch,
        }

    def tearDown(self) -> None:
        for server in getattr(self, "servers", []):
            server.shutdown()
            server.server_close()

    def setUp(self) -> None:
        self.servers: list[FakeServer] = []

    def test_workers_share_barrier_and_emit_complete_samples(self) -> None:
        states = [FakeState("r1"), FakeState("r2")]
        descriptors = []
        for state in states:
            server, _ = self.start_server(state)
            self.servers.append(server)
            descriptors.append(self.descriptor(server, state))

        report = live_profile.run_profile(
            descriptors,
            family="vision",
            warmup_requests=1,
            sample_window_seconds=0.16,
            timeout_seconds=2,
        )

        self.assertEqual(report["status"], "ok")
        self.assertEqual(len(report["replicas"]), 2)
        self.assertTrue(report["samples"])
        self.assertTrue(all(row["complete"] for row in report["samples"]))
        self.assertTrue(all(row["runtimeInferenceSeconds"] > 0 for row in report["samples"]))
        self.assertTrue(all(state.max_active == 1 for state in states))
        self.assertLess(abs(states[0].infer_started[1] - states[1].infer_started[1]), 0.05)
        self.assertGreaterEqual(len(states[0].payloads), 2)
        self.assertEqual(states[0].payloads[0]["batch"], 2)
        mus = [row["mu"] for row in report["replicas"]]
        self.assertTrue(all(mu and mu > 0 for mu in mus))

    def test_llm_payload_shape_is_preserved(self) -> None:
        state = FakeState("llm-1")
        server, _ = self.start_server(state)
        self.servers.append(server)
        descriptor = self.descriptor(server, state)
        descriptor["model"] = state.model
        report = live_profile.run_profile(
            [descriptor],
            family="llm",
            payload={"prompt_len": 32, "output_tokens": 8, "benchmark": True},
            warmup_requests=1,
            sample_window_seconds=0.07,
            timeout_seconds=2,
        )
        self.assertIn(report["status"], {"ok", "error"})
        self.assertEqual(state.payloads[0]["prompt_len"], 32)
        self.assertEqual(state.payloads[0]["output_tokens"], 8)
        self.assertEqual(state.payloads[0]["batch"], 2)

    def test_auto_profiles_mixed_vision_and_llm_replicas_together(self) -> None:
        vision = FakeState("vision-1")
        llm = FakeState("llm-1")
        llm.model = "llama"
        descriptors = []
        for state, family, request_payload in (
            (vision, "vision", {"image_size": 224}),
            (llm, "llama_p16_o4", {"prompt_len": 16, "output_tokens": 4}),
        ):
            server, _ = self.start_server(state)
            self.servers.append(server)
            descriptor = self.descriptor(server, state)
            if family in {"vision", "llm"}:
                descriptor["family"] = family
            else:
                descriptor["workload"] = family
            descriptor["payload"] = request_payload
            descriptors.append(descriptor)

        report = live_profile.run_profile(
            descriptors,
            family="auto",
            warmup_requests=1,
            sample_window_seconds=0.12,
            timeout_seconds=2,
        )

        self.assertEqual(report["status"], "ok")
        self.assertTrue(any("image_size" in payload for payload in vision.payloads))
        self.assertTrue(any("prompt_len" in payload for payload in llm.payloads))
        self.assertTrue(all(payload["batch"] == 2 for payload in vision.payloads + llm.payloads))
        self.assertEqual(len(report["replicas"]), 2)

    def test_metadata_mismatch_stops_before_infer(self) -> None:
        state = FakeState("actual")
        server, _ = self.start_server(state)
        self.servers.append(server)
        descriptor = self.descriptor(server, state)
        descriptor["runtimeId"] = "expected"
        report = live_profile.run_profile(
            [descriptor], family="vision", warmup_requests=1, sample_window_seconds=0.05
        )
        self.assertEqual(report["status"], "metadata_mismatch")
        self.assertEqual(state.infer_count, 0)
        self.assertTrue(any("runtime_id" in error for error in report["errors"]))

    def test_error_and_no_complete_sample_are_reported(self) -> None:
        state = FakeState("slow")
        state.delay = 0.12
        server, _ = self.start_server(state)
        self.servers.append(server)
        report = live_profile.run_profile(
            [self.descriptor(server, state)],
            family="vision",
            warmup_requests=0,
            sample_window_seconds=0.03,
            timeout_seconds=2,
            clock=iter((0.0, 1.0, 1.0)).__next__,
        )
        self.assertEqual(report["status"], "incomplete")
        self.assertEqual(report["replicas"][0]["sampleCount"], 0)
        self.assertIsNone(report["replicas"][0]["mu"])

        state.fail_samples = True
        state.delay = 0.001
        report = live_profile.run_profile(
            [self.descriptor(server, state)],
            family="vision",
            warmup_requests=0,
            sample_window_seconds=0.03,
            timeout_seconds=2,
        )
        self.assertEqual(report["status"], "error")
        self.assertTrue(report["errors"])


if __name__ == "__main__":
    unittest.main()
