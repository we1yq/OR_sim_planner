from __future__ import annotations

import importlib.util
import json
import threading
import time
import unittest
from unittest import mock
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("live_traffic_20260926.py")
SPEC = importlib.util.spec_from_file_location("live_traffic_20260926", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
traffic = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(traffic)


class TrafficPlanTests(unittest.TestCase):
    def test_demand_csv_is_validated_and_zero_rate_is_silent(self) -> None:
        header = ["live_round", "round", "hour", *traffic.WORKLOAD_KEYS]
        values = ["1", "4", "1.5", "0", "1", "0", "0", "0", "0", "0"]
        rows = traffic.load_demand_rows(",".join(header) + "\n" + ",".join(values) + "\n")
        self.assertEqual(rows[0]["live_round"], 1)
        self.assertEqual(traffic.request_offsets(0, "resnet50_image", 10), [])

    def test_seed_71_phase_and_fixed_spacing_are_stable(self) -> None:
        first = traffic.request_offsets(2.0, "resnet50_image", 3.0)
        second = traffic.request_offsets(2.0, "resnet50_image", 3.0)
        self.assertEqual(first, second)
        self.assertGreaterEqual(first[0], 0.0)
        self.assertLess(first[0], 0.5)
        for left, right in zip(first, first[1:]):
            self.assertAlmostEqual(right - left, 0.5)

    def test_paired_plans_share_sequence_offsets_and_payload_hashes(self) -> None:
        rates = {workload: 0.0 for workload in traffic.WORKLOAD_KEYS}
        rates["gpt2_p64_o64"] = 3.0
        plans = traffic.build_paired_request_plans(rates, 1.0)
        source = plans["source_control"]
        transition = plans["transition"]
        self.assertEqual(len(source), len(transition))
        for source_row, transition_row in zip(source, transition):
            self.assertEqual(source_row["sequence_id"], transition_row["sequence_id"])
            self.assertEqual(source_row["payload_hash"], transition_row["payload_hash"])
            self.assertEqual(source_row["scheduled_offset"], transition_row["scheduled_offset"])
            self.assertNotEqual(source_row["attempt_id"], transition_row["attempt_id"])

    def test_payload_shape_covers_three_vision_and_four_llm_classes(self) -> None:
        for workload in traffic.WORKLOAD_KEYS:
            payload = traffic.build_request_payload(workload, f"{workload}:0")
            self.assertEqual(payload["batch"], 1)
            self.assertEqual(payload["logicalRequestCount"], 1)
            if workload in traffic.VISION_WORKLOADS:
                self.assertEqual(payload["request_class"], "image")
                self.assertEqual(payload["image"]["encoding"], "synthetic")
            else:
                self.assertIn("prompt_len", payload)
                self.assertIn("output_tokens", payload)
                self.assertEqual(payload["max_tokens"], payload["output_tokens"])
            self.assertEqual(traffic.payload_hash(payload), traffic.payload_hash(json.loads(json.dumps(payload))))

    def test_router_uses_distinct_workload_paths_for_llm_shapes(self) -> None:
        class Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_: object) -> None:
                return None

            def read(self) -> bytes:
                return b"{}"

        sender = traffic.urllib_transport("http://router")
        rates = {workload: 0.0 for workload in traffic.WORKLOAD_KEYS}
        rates["gpt2_p64_o64"] = 1.0
        first = next(row for row in traffic.build_request_plan(rates, 1.0, phase="source_control") if row["workload"] == "gpt2_p64_o64")
        rates["gpt2_p64_o64"] = 0.0
        rates["gpt2_p512_o512"] = 1.0
        second = next(row for row in traffic.build_request_plan(rates, 1.0, phase="source_control") if row["workload"] == "gpt2_p512_o512")
        with mock.patch.object(traffic.urllib.request, "urlopen", return_value=Response()) as urlopen:
            sender(first)
            sender(second)
        self.assertEqual(urlopen.call_args_list[0].args[0].full_url, "http://router/infer/gpt2_p64_o64")
        self.assertEqual(urlopen.call_args_list[1].args[0].full_url, "http://router/infer/gpt2_p512_o512")


class SenderTests(unittest.TestCase):
    def _request(self, workload: str = "resnet50_image", sequence: str = "0") -> dict[str, object]:
        payload = traffic.build_request_payload(workload, f"{workload}:{sequence}")
        return {
            "phase": "source_control",
            "seed": 71,
            "workload": workload,
            "family": "vision" if workload in traffic.VISION_WORKLOADS else "llm",
            "model": payload["model"],
            "sequence_id": f"{workload}:{sequence}",
            "attempt_id": f"source_control:{workload}:{sequence}",
            "n": int(sequence),
            "rate": 1.0,
            "scheduled_offset": 0.0,
            "payload_hash": traffic.payload_hash(payload),
            "payload_json": json.dumps(payload, sort_keys=True, separators=(",", ":")),
        }

    def test_accounting_records_sends_success_failure_timeout_and_drain(self) -> None:
        calls: list[str] = []

        def transport(row: dict[str, object]) -> dict[str, object]:
            calls.append(str(row["sequence_id"]))
            sequence = str(row["sequence_id"])
            if sequence.endswith(":1"):
                raise TimeoutError("test timeout")
            if sequence.endswith(":2"):
                raise RuntimeError("test failure")
            return {"outputTokens": 64, "batchSize": 1}

        sender = traffic.BoundedAsyncSender(transport, max_workers=3)
        try:
            for sequence in ("0", "1", "2"):
                sender.submit(self._request(sequence=sequence))
            rows = sender.drain()
            summary = sender.accounting()
        finally:
            sender.shutdown()
        self.assertEqual(sorted(calls), ["resnet50_image:0", "resnet50_image:1", "resnet50_image:2"])
        self.assertEqual(summary["scheduled"], 3)
        self.assertEqual(summary["actual_sends"], 3)
        self.assertEqual(summary["attempts"], 3)
        self.assertEqual(summary["successes"], 1)
        self.assertEqual(summary["failures"], 2)
        self.assertEqual(summary["timeouts"], 1)
        self.assertEqual(summary["pending"], 0)
        self.assertEqual([row["status"] for row in rows], ["success", "timeout", "failure"])
        self.assertTrue(all("scheduled_send" in row and "actual_send" in row for row in rows))
        self.assertEqual(rows[0]["actual_tokens"], 64)

    def test_per_workload_and_total_pending_bounds_reject_without_retry(self) -> None:
        release = threading.Event()

        def blocked(_: dict[str, object]) -> dict[str, object]:
            release.wait(2.0)
            return {"batchSize": 1}

        sender = traffic.BoundedAsyncSender(
            blocked,
            max_pending_per_workload=2,
            max_pending_total=3,
            max_workers=3,
        )
        try:
            self.assertIsNotNone(sender.submit(self._request("resnet50_image", "0")))
            self.assertIsNotNone(sender.submit(self._request("resnet50_image", "1")))
            self.assertIsNone(sender.submit(self._request("resnet50_image", "2")))
            self.assertIsNotNone(sender.submit(self._request("vgg16_image", "0")))
            self.assertIsNone(sender.submit(self._request("vit_base_image", "0")))
            self.assertLessEqual(sender.pending_by_workload()["resnet50_image"], 2)
            self.assertLessEqual(sender.pending, 3)
            release.set()
            rows = sender.drain()
            summary = sender.accounting()
        finally:
            release.set()
            sender.shutdown()
        self.assertEqual(summary["scheduled"], 3)
        self.assertEqual(summary["actual_sends"], 3)
        self.assertEqual(summary["attempts"], 3)
        self.assertEqual(summary["rejected"], 2)
        self.assertTrue(summary["measurement_invalid"])
        self.assertEqual(summary["pending"], 0)
        self.assertEqual(len(rows), 5)
        self.assertEqual(sum(row["status"] == "rejected_pending_bound" for row in rows), 2)

    def test_sender_does_not_retry_a_failure(self) -> None:
        calls = 0

        def fail(_: dict[str, object]) -> dict[str, object]:
            nonlocal calls
            calls += 1
            raise RuntimeError("once")

        sender = traffic.BoundedAsyncSender(fail, max_workers=1)
        try:
            sender.submit(self._request())
            rows = sender.drain()
            summary = sender.accounting()
        finally:
            sender.shutdown()
        self.assertEqual(calls, 1)
        self.assertEqual(summary["attempts"], 1)
        self.assertEqual(summary["failures"], 1)
        self.assertEqual(rows[0]["status"], "failure")

    def test_stop_cancels_future_offsets_and_drain_is_prompt(self) -> None:
        stop_event = threading.Event()
        requests = [self._request(sequence=str(index)) for index in range(3)]
        for index, request in enumerate(requests):
            request["scheduled_offset"] = 30.0 + index
        sender = traffic.BoundedAsyncSender(lambda _: {"batchSize": 1}, max_workers=3)
        try:
            dispatcher = sender.submit_plan(requests, stop_event=stop_event)
            time.sleep(0.02)
            started = time.monotonic()
            stop_event.set()
            rows = sender.drain(timeout_s=1.0)
            elapsed = time.monotonic() - started
            summary = sender.accounting()
            dispatcher.join(timeout=1.0)
        finally:
            sender.stop_new_sends()
            sender.shutdown()
        self.assertLess(elapsed, 1.0)
        self.assertEqual([row["status"] for row in rows], ["cancelled_stopped"] * 3)
        self.assertEqual(summary["actual_sends"], 0)
        self.assertEqual(summary["attempts"], 0)
        self.assertEqual(summary["failures"], 0)
        self.assertEqual(summary["cancelled"], 3)
        self.assertEqual(summary["pending"], 0)

    def test_stop_new_sends_cancels_directly_submitted_future_worker(self) -> None:
        request = self._request()
        request["scheduled_offset"] = 30.0
        sender = traffic.BoundedAsyncSender(lambda _: {"batchSize": 1}, max_workers=1)
        try:
            sender.submit(request)
            sender.stop_new_sends()
            rows = sender.drain(timeout_s=1.0)
            summary = sender.accounting()
        finally:
            sender.shutdown()
        self.assertEqual(rows[0]["status"], "cancelled")
        self.assertFalse(rows[0]["failure"])
        self.assertEqual(summary["actual_sends"], 0)
        self.assertEqual(summary["cancelled"], 1)


class ContinuousRateSenderTest(unittest.TestCase):
    def test_rate_switch_tags_windows_and_keeps_fixed_spacing(self) -> None:
        sent = []
        sender = traffic.BoundedAsyncSender(lambda request: sent.append(request) or {"status": "success"})
        gen = traffic.ContinuousRateSender(sender, seed=71, tick_s=0.001)
        zero = {key: 0.0 for key in traffic.WORKLOAD_KEYS}
        gen.start()
        gen.set_rates({**zero, "gpt2_p64_o64": 50.0}, live_round=2, window="transition")
        time.sleep(0.25)
        switch = gen.set_rates({**zero, "gpt2_p64_o64": 100.0}, live_round=2, window="steady")
        time.sleep(0.25)
        gen.set_rates(zero, live_round=2, window="steady")
        time.sleep(0.05)
        gen.stop()
        rows = sender.drain(timeout_s=5.0)
        sender.shutdown()
        by_window = {w: sorted(float(r["scheduled_offset"]) for r in rows if r["phase"] == w) for w in ("transition", "steady")}
        self.assertTrue(all(r["workload"] == "gpt2_p64_o64" and r["live_round"] == 2 for r in rows))
        self.assertGreaterEqual(len(by_window["transition"]), 10)
        self.assertGreaterEqual(len(by_window["steady"]), 20)
        gaps = [b - a for a, b in zip(by_window["steady"], by_window["steady"][1:])]
        for gap in gaps:
            self.assertAlmostEqual(gap, 0.01, places=6)
        self.assertTrue(all(offset < switch["switched_at_offset"] for offset in by_window["transition"]))
        self.assertTrue(all(offset >= switch["switched_at_offset"] for offset in by_window["steady"]))
        self.assertEqual(len({r["sequence_id"] for r in rows}), len(rows))
        self.assertEqual(len(gen.events()), 3)


if __name__ == "__main__":
    unittest.main()
