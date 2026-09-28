"""Deterministic, open-loop traffic for the 2026-09-26 three-GPU run.

The module deliberately has no third-party dependencies.  Planning is pure and
the sender accepts an injected transport, which keeps unit tests independent of
Kubernetes and the network.
"""

from __future__ import annotations

import csv
import gc
import hashlib
import io
import json
import math
import socket
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, TextIO


SEED = 71
WORKLOAD_KEYS = (
    "resnet50_image",
    "vgg16_image",
    "vit_base_image",
    "gpt2_p64_o64",
    "gpt2_p512_o512",
    "llama_p1024_o128",
    "llama_p2048_o64",
)
VISION_WORKLOADS = frozenset(WORKLOAD_KEYS[:3])
LLM_WORKLOADS = frozenset(WORKLOAD_KEYS[3:])
PENDING_PER_WORKLOAD = 256
PENDING_TOTAL = 1024

_VISION_SPECS = {
    "resnet50_image": ("resnet50", 224),
    "vgg16_image": ("vgg16", 224),
    "vit_base_image": ("vit_base", 224),
}
_LLM_SPECS = {
    "gpt2_p64_o64": ("gpt2", 64, 64),
    "gpt2_p512_o512": ("gpt2", 512, 512),
    "llama_p1024_o128": ("llama", 1024, 128),
    "llama_p2048_o64": ("llama", 2048, 64),
}


def _number(value: Any, field: str, row_number: int) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"row {row_number}: {field} must be numeric") from exc
    if not math.isfinite(number):
        raise ValueError(f"row {row_number}: {field} must be finite")
    return number


def validate_demand_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Validate and normalize demand rows without changing their seven rates."""

    normalized: list[dict[str, Any]] = []
    seen_rounds: set[int] = set()
    for row_number, raw in enumerate(rows, 1):
        row = dict(raw)
        missing = [key for key in WORKLOAD_KEYS if key not in row]
        if missing:
            raise ValueError(f"row {row_number}: missing workload columns: {', '.join(missing)}")
        try:
            live_round = int(row.get("live_round"))
            trace_round = int(row.get("round"))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"row {row_number}: live_round and round must be integers") from exc
        if live_round <= 0 or trace_round <= 0:
            raise ValueError(f"row {row_number}: round numbers must be positive")
        if live_round in seen_rounds:
            raise ValueError(f"row {row_number}: duplicate live_round {live_round}")
        seen_rounds.add(live_round)
        hour = _number(row.get("hour", 0.0), "hour", row_number)
        if hour < 0:
            raise ValueError(f"row {row_number}: hour must be non-negative")
        normalized_row: dict[str, Any] = {
            "live_round": live_round,
            "round": trace_round,
            "hour": hour,
        }
        for workload in WORKLOAD_KEYS:
            rate = _number(row[workload], workload, row_number)
            if rate < 0:
                raise ValueError(f"row {row_number}: {workload} must be non-negative")
            normalized_row[workload] = rate
        normalized.append(normalized_row)
    if not normalized:
        raise ValueError("demand must contain at least one row")
    expected_rounds = list(range(1, len(normalized) + 1))
    actual_rounds = [row["live_round"] for row in normalized]
    if actual_rounds != expected_rounds:
        raise ValueError("live_round values must be contiguous and start at 1")
    return normalized


def load_demand_rows(source: str | Path | TextIO | Iterable[str]) -> list[dict[str, Any]]:
    """Load a CSV path, text stream, or iterable of CSV lines."""

    close_source = False
    if isinstance(source, Path):
        stream: TextIO = source.open("r", encoding="utf-8", newline="")
        close_source = True
    elif isinstance(source, str) and "\n" not in source and "\r" not in source and Path(source).exists():
        stream = Path(source).open("r", encoding="utf-8", newline="")
        close_source = True
    elif isinstance(source, str):
        stream = io.StringIO(source)
    else:
        stream = source  # type: ignore[assignment]
    try:
        reader = csv.DictReader(stream)
        if reader.fieldnames is None:
            raise ValueError("demand CSV has no header")
        missing = [key for key in WORKLOAD_KEYS if key not in reader.fieldnames]
        required_metadata = ["live_round", "round", "hour"]
        missing.extend(key for key in required_metadata if key not in reader.fieldnames)
        if missing:
            raise ValueError(f"demand CSV missing columns: {', '.join(missing)}")
        return validate_demand_rows(reader)
    finally:
        if close_source:
            stream.close()


def stable_hash_fraction(workload: str, seed: int = SEED) -> float:
    """Return a process-independent fraction in [0, 1) for a workload."""

    digest = hashlib.sha256(f"{int(seed)}:{workload}".encode("utf-8")).digest()
    return int.from_bytes(digest, "big") / float(1 << (8 * len(digest)))


def phase_offset(rate: float, workload: str, seed: int = SEED) -> float:
    """Return phi for ``offset = phi + n / rate``."""

    rate = _number(rate, "rate", 0)
    if rate < 0:
        raise ValueError("rate must be non-negative")
    if rate == 0:
        return 0.0
    return stable_hash_fraction(workload, seed) / rate


def request_offsets(rate: float, workload: str, duration_s: float, seed: int = SEED) -> list[float]:
    """Generate all fixed-rate offsets strictly inside [0, duration_s)."""

    rate = _number(rate, "rate", 0)
    duration_s = _number(duration_s, "duration_s", 0)
    if rate < 0:
        raise ValueError("rate must be non-negative")
    if duration_s < 0:
        raise ValueError("duration_s must be non-negative")
    if rate == 0 or duration_s == 0:
        return []
    phi = phase_offset(rate, workload, seed)
    offsets: list[float] = []
    n = 0
    while True:
        offset = phi + n / rate
        if offset >= duration_s:
            break
        offsets.append(offset)
        n += 1
    return offsets


def build_request_payload(workload: str, sequence_id: str) -> dict[str, Any]:
    """Build the stable request body for one logical sample."""

    if workload in _VISION_SPECS:
        model, image_size = _VISION_SPECS[workload]
        return {
            "benchmark": True,
            "model": model,
            "workload": workload,
            "request_class": "image",
            "batch": 1,
            "logicalRequestCount": 1,
            "driverBatched": False,
            "image": {
                "encoding": "synthetic",
                "id": sequence_id,
                "width": image_size,
                "height": image_size,
                "channels": 3,
            },
        }
    if workload in _LLM_SPECS:
        model, prompt_len, output_tokens = _LLM_SPECS[workload]
        return {
            "benchmark": True,
            "model": model,
            "workload": workload,
            "request_class": f"p{prompt_len}/o{output_tokens}",
            "batch": 1,
            "logicalRequestCount": 1,
            "prompt_len": prompt_len,
            "output_tokens": output_tokens,
            "max_tokens": output_tokens,
            "prompt": "hello",
        }
    raise ValueError(f"unknown workload: {workload}")


def payload_hash(payload: Mapping[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _rates(raw: Mapping[str, Any]) -> dict[str, float]:
    missing = [workload for workload in WORKLOAD_KEYS if workload not in raw]
    if missing:
        raise ValueError(f"missing workload rates: {', '.join(missing)}")
    result = {}
    for workload in WORKLOAD_KEYS:
        rate = _number(raw[workload], workload, 0)
        if rate < 0:
            raise ValueError(f"{workload} must be non-negative")
        result[workload] = rate
    return result


def build_request_plan(
    rates: Mapping[str, Any],
    duration_s: float,
    *,
    phase: str,
    seed: int = SEED,
    sequence_prefix: str = "",
) -> list[dict[str, Any]]:
    """Build scalar, JSONL/CSV-friendly rows for one traffic phase."""

    normalized_rates = _rates(rates)
    duration_s = _number(duration_s, "duration_s", 0)
    if duration_s < 0:
        raise ValueError("duration_s must be non-negative")
    if phase not in {"source_control", "transition", "transition_unpaired", "target_steady"}:
        raise ValueError(
            "phase must be source_control, transition, transition_unpaired, or target_steady"
        )
    rows: list[dict[str, Any]] = []
    for workload in WORKLOAD_KEYS:
        rate = normalized_rates[workload]
        for n, offset in enumerate(request_offsets(rate, workload, duration_s, seed)):
            sequence_id = f"{sequence_prefix}{workload}:{n}"
            payload = build_request_payload(workload, sequence_id)
            rows.append(
                {
                    "phase": phase,
                    "seed": int(seed),
                    "workload": workload,
                    "family": "vision" if workload in VISION_WORKLOADS else "llm",
                    "model": payload["model"],
                    "sequence_id": sequence_id,
                    "attempt_id": f"{phase}:{sequence_id}",
                    "n": n,
                    "rate": rate,
                    "scheduled_offset": offset,
                    "payload_hash": payload_hash(payload),
                    "payload_json": json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True),
                }
            )
    rows.sort(key=lambda row: (float(row["scheduled_offset"]), str(row["workload"]), int(row["n"])))
    return rows


def build_paired_request_plans(
    rates: Mapping[str, Any],
    duration_s: float,
    *,
    seed: int = SEED,
    sequence_prefix: str = "",
) -> dict[str, list[dict[str, Any]]]:
    """Build source-control and transition rows from one identical send file."""

    source = build_request_plan(rates, duration_s, phase="source_control", seed=seed, sequence_prefix=sequence_prefix)
    transition = build_request_plan(rates, duration_s, phase="transition", seed=seed, sequence_prefix=sequence_prefix)
    return {"source_control": source, "transition": transition}


def demand_rates(row: Mapping[str, Any]) -> dict[str, float]:
    """Extract and validate the seven rates from one normalized or raw row."""

    return _rates(row)


def urllib_transport(router_url: str, timeout_s: float = 900.0) -> Callable[[Mapping[str, Any]], Mapping[str, Any]]:
    """Create the production transport; one urllib call is exactly one attempt."""

    router = router_url.rstrip("/")

    def send(request: Mapping[str, Any]) -> Mapping[str, Any]:
        payload = json.loads(str(request["payload_json"]))
        body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        url = f"{router}/infer/{request['workload']}"
        http_request = urllib.request.Request(url, data=body, headers={"content-type": "application/json"}, method="POST")
        with urllib.request.urlopen(http_request, timeout=timeout_s) as response:
            raw = response.read().decode("utf-8")
            decoded = json.loads(raw) if raw else {}
            result = dict(decoded) if isinstance(decoded, Mapping) else {"response": decoded}
            result["http_status"] = int(response.status)
            return result

    return send


def _is_timeout(exc: BaseException) -> bool:
    if isinstance(exc, (TimeoutError, socket.timeout)):
        return True
    if isinstance(exc, urllib.error.URLError):
        return isinstance(exc.reason, (TimeoutError, socket.timeout)) or "timed out" in str(exc).lower()
    return "timed out" in str(exc).lower()


class BoundedAsyncSender:
    """Open-loop sender with explicit pending bounds and no retry behavior."""

    def __init__(
        self,
        transport: Callable[[Mapping[str, Any]], Any],
        *,
        max_pending_per_workload: int = PENDING_PER_WORKLOAD,
        max_pending_total: int = PENDING_TOTAL,
        max_workers: int | None = None,
        stop_event: threading.Event | None = None,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        # One worker per admissible pending request: the pending bounds are the
        # only admission limit, so one workload's backlog cannot delay other
        # workloads' open-loop sends by exhausting a smaller thread pool.
        if max_workers is None:
            max_workers = max_pending_total
        if max_pending_per_workload <= 0 or max_pending_total <= 0 or max_workers <= 0:
            raise ValueError("pending limits and max_workers must be positive")
        self.transport = transport
        self.max_pending_per_workload = int(max_pending_per_workload)
        self.max_pending_total = int(max_pending_total)
        self._clock = clock
        self._wall_clock = wall_clock
        self._sleep = sleep
        self.stop_event = stop_event or threading.Event()
        self._accepting = True
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._lock = threading.Lock()
        self._pending: dict[str, int] = {workload: 0 for workload in WORKLOAD_KEYS}
        self._scheduled_by_workload: dict[str, int] = {workload: 0 for workload in WORKLOAD_KEYS}
        self._accepted = 0
        self._scheduled = 0
        self._actual_sends = 0
        self._attempts = 0
        self._successes = 0
        self._failures = 0
        self._timeouts = 0
        self._rejected = 0
        self._cancelled = 0
        self._measurement_invalid = False
        self._futures: list[Future[dict[str, Any]]] = []
        self._completed: list[dict[str, Any]] = []
        self._plan_threads: list[threading.Thread] = []
        self._start = self._clock()

    def submit(self, request: Mapping[str, Any]) -> Future[dict[str, Any]] | None:
        workload = str(request.get("workload", ""))
        if workload not in WORKLOAD_KEYS:
            raise ValueError(f"unknown workload: {workload}")
        with self._lock:
            if not self._accepting or self.stop_event.is_set():
                self._record_cancelled_locked(request, "cancelled_stopped")
                return None
            total_pending = sum(self._pending.values())
            if self._pending[workload] >= self.max_pending_per_workload or total_pending >= self.max_pending_total:
                self._rejected += 1
                self._measurement_invalid = True
                rejected = dict(request)
                rejected.update(
                    {
                        "status": "rejected_pending_bound",
                        "error": "pending bound reached",
                        "attempts": 0,
                        "failure": True,
                        "timeout": False,
                        "measurement_invalid": True,
                        "scheduled_send": request.get("scheduled_offset"),
                        "actual_send": None,
                        "first_token": None,
                        "completion": None,
                    }
                )
                self._completed.append(rejected)
                return None
            self._pending[workload] += 1
            self._accepted += 1
            self._scheduled += 1
            self._scheduled_by_workload[workload] += 1
            future = self._executor.submit(self._send_one, dict(request), workload)
            self._futures.append(future)
            return future

    def stop_new_sends(self) -> None:
        """Stop admitting traffic and cancel requests not yet actually sent."""

        with self._lock:
            self._accepting = False
            self.stop_event.set()

    def submit_plan(
        self,
        plan: Iterable[Mapping[str, Any]],
        *,
        stop_event: threading.Event | None = None,
    ) -> threading.Thread:
        """Dispatch a long fixed-offset plan incrementally in the background."""

        rows = [dict(row) for row in plan]
        if stop_event is not None:
            watcher = threading.Thread(target=self._watch_stop_event, args=(stop_event,), daemon=True)
            watcher.start()

        def dispatch() -> None:
            for index, row in enumerate(rows):
                while True:
                    if self.stop_event.is_set():
                        for remaining in rows[index:]:
                            self._record_cancelled(remaining, "cancelled_stopped")
                        return
                    remaining = self._start + float(row.get("scheduled_offset", 0.0)) - self._clock()
                    if remaining <= 0:
                        break
                    self._sleep(min(remaining, 0.25))
                self.submit(row)

        thread = threading.Thread(target=dispatch, name="traffic-plan-dispatch", daemon=True)
        with self._lock:
            self._plan_threads.append(thread)
        thread.start()
        return thread

    def run(
        self,
        plan: Iterable[Mapping[str, Any]],
        *,
        stop_event: threading.Event | None = None,
        timeout_s: float | None = None,
    ) -> list[dict[str, Any]]:
        """Dispatch until the plan ends or an external stop signal arrives."""

        dispatcher = self.submit_plan(plan, stop_event=stop_event)
        dispatcher.join()
        return self.drain(timeout_s=timeout_s)

    def _watch_stop_event(self, external: threading.Event) -> None:
        external.wait()
        self.stop_new_sends()

    @property
    def pending(self) -> int:
        with self._lock:
            return sum(self._pending.values())

    def pending_by_workload(self) -> dict[str, int]:
        with self._lock:
            return dict(self._pending)

    def _record_cancelled_locked(self, request: Mapping[str, Any], status: str) -> dict[str, Any]:
        row = dict(request)
        row.update(
            {
                "status": status,
                "error": "sender stopped before scheduled send",
                "attempts": 0,
                "failure": False,
                "timeout": False,
                "cancelled": True,
                "measurement_invalid": False,
                "scheduled_send": request.get("scheduled_offset"),
                "actual_send": None,
                "actual_send_monotonic": None,
                "send_lag_s": None,
                "first_token": None,
                "completion": self._wall_clock(),
            }
        )
        self._cancelled += 1
        self._completed.append(row)
        return row

    def _record_cancelled(self, request: Mapping[str, Any], status: str) -> dict[str, Any]:
        workload = str(request.get("workload", ""))
        if workload not in WORKLOAD_KEYS:
            raise ValueError(f"unknown workload: {workload}")
        with self._lock:
            return self._record_cancelled_locked(request, status)

    def _send_one(self, request: dict[str, Any], workload: str) -> dict[str, Any]:
        scheduled_offset = float(request.get("scheduled_offset", 0.0))
        while True:
            if self.stop_event.is_set():
                return self._cancel_pending_request(request, workload)
            remaining = self._start + scheduled_offset - self._clock()
            if remaining <= 0:
                break
            self._sleep(min(remaining, 0.25))
        if self.stop_event.is_set():
            return self._cancel_pending_request(request, workload)
        actual_send = self._wall_clock()
        actual_monotonic = self._clock() - self._start
        base = dict(request)
        base.update(
            {
                "scheduled_send": scheduled_offset,
                "actual_send": actual_send,
                "actual_send_monotonic": actual_monotonic,
                "send_lag_s": actual_monotonic - scheduled_offset,
                "attempts": 1,
                "failure": False,
                "timeout": False,
                "measurement_invalid": False,
                "first_token": None,
                "completion": None,
                "error": "",
            }
        )
        with self._lock:
            self._actual_sends += 1
            self._attempts += 1
        try:
            response = self.transport(request)
            response_map = dict(response) if isinstance(response, Mapping) else {"response": response}
            status = int(response_map.get("http_status", response_map.get("status", 200)) or 200)
            if status >= 400 or response_map.get("ok") is False:
                raise RuntimeError(f"HTTP/status failure: {status}")
            base["status"] = "success"
            base["http_status"] = status
            base["response_json"] = json.dumps(response_map, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
            base["actual_tokens"] = _response_value(response_map, "actual_tokens", "actualTokens", "outputTokens", "tokens")
            base["sample_count"] = _response_value(response_map, "sample_count", "sampleCount", "batchSize")
            ttft_ms = _response_value(response_map, "ttft_ms", "ttftMs", "timeToFirstTokenMs")
            if ttft_ms is not None:
                base["first_token"] = actual_send + float(ttft_ms) / 1000.0
            with self._lock:
                self._successes += 1
        except BaseException as exc:  # worker must always release its pending slot
            base["failure"] = True
            base["timeout"] = _is_timeout(exc)
            base["status"] = "timeout" if base["timeout"] else "failure"
            base["error"] = f"{type(exc).__name__}: {exc}"
            with self._lock:
                self._failures += 1
                if base["timeout"]:
                    self._timeouts += 1
        finally:
            base["completion"] = self._wall_clock()
            with self._lock:
                self._pending[workload] -= 1
                self._completed.append(base)
        return base

    def _cancel_pending_request(self, request: Mapping[str, Any], workload: str) -> dict[str, Any]:
        with self._lock:
            row = self._record_cancelled_locked(request, "cancelled")
            self._pending[workload] -= 1
            return row

    def drain(self, timeout_s: float | None = None) -> list[dict[str, Any]]:
        """Wait for all admitted requests and return terminal rows in stable order."""

        started = self._clock()
        for thread in list(self._plan_threads):
            remaining = None if timeout_s is None else max(0.0, timeout_s - (self._clock() - started))
            thread.join(timeout=remaining)
        for future in list(self._futures):
            remaining = None if timeout_s is None else max(0.0, timeout_s - (self._clock() - started))
            future.result(timeout=remaining)
        with self._lock:
            rows = list(self._completed)
        return sorted(rows, key=lambda row: (str(row.get("workload")), str(row.get("sequence_id")), str(row.get("phase"))))

    def accounting(self) -> dict[str, Any]:
        with self._lock:
            by_workload = {
                workload: {
                    "pending": self._pending[workload],
                    "scheduled": self._scheduled_by_workload[workload],
                }
                for workload in WORKLOAD_KEYS
            }
            return {
                "scheduled": self._scheduled,
                "actual_sends": self._actual_sends,
                "attempts": self._attempts,
                "successes": self._successes,
                "failures": self._failures,
                "timeouts": self._timeouts,
                "pending": sum(self._pending.values()),
                "rejected": self._rejected,
                "cancelled": self._cancelled,
                "measurement_invalid": self._measurement_invalid,
                "by_workload": by_workload,
            }

    def shutdown(self, *, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait)

    def __enter__(self) -> "BoundedAsyncSender":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.shutdown()


AsyncSender = BoundedAsyncSender


def _response_value(response: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in response:
            return response[key]
    return None


class ContinuousRateSender:
    """E1 open-loop generator whose rates switch at run-time events.

    Each workload sends at fixed inter-arrival 1/d.  After ``set_rates`` at
    monotonic time t, workload w sends at ``t + phi_w(d) + n/d`` (n = 0, 1, ...),
    with phi from the traffic seed as in ``request_offsets``; d = 0 stops w.
    Every request is tagged at send time with the live round and window
    ("transition" or "steady") that were current when it was scheduled, so
    requests still in flight after a switch keep their original window.
    Sending goes through ``BoundedAsyncSender`` (pending bounds, no retries).

    Request records accumulate for the whole run; a full CPython collection
    over them stalls every thread (up to ~1 s late in a 12-round run), which
    shows up as gaps in the offered load.  The dispatcher therefore calls
    ``gc.freeze()`` every ``gc_freeze_s`` so collections only scan objects
    created since the last freeze (the records hold no reference cycles).
    """

    def __init__(self, sender: "BoundedAsyncSender", *, seed: int = SEED, tick_s: float = 0.002,
                 gc_freeze_s: float = 1.0) -> None:
        self.sender = sender
        self.seed = int(seed)
        self.tick_s = float(tick_s)
        self.gc_freeze_s = float(gc_freeze_s)
        self._lock = threading.Lock()
        self._schedule: dict[str, dict[str, Any]] = {}
        self._label: dict[str, Any] = {}
        self._events: list[dict[str, Any]] = []
        self._thread: threading.Thread | None = None
        self._stopped = threading.Event()

    def start(self) -> None:
        self._thread = threading.Thread(target=self._dispatch, name="e1-continuous-traffic", daemon=True)
        self._thread.start()

    def set_rates(self, rates: Mapping[str, Any], *, live_round: int, window: str) -> dict[str, Any]:
        """Switch every workload to ``rates`` now; return the switch event."""

        normalized = _rates(rates)
        now = self.sender._clock()
        event = {
            "live_round": int(live_round),
            "window": str(window),
            "rates": dict(normalized),
            "switched_at_offset": now - self.sender._start,
            "switched_at_utc": self.sender._wall_clock(),
        }
        with self._lock:
            self._label = {"live_round": int(live_round), "window": str(window)}
            for workload in WORKLOAD_KEYS:
                rate = normalized[workload]
                self._schedule[workload] = {
                    "rate": rate,
                    "base": now + (phase_offset(rate, workload, self.seed) if rate > 0 else 0.0),
                    "n": 0,
                    "label": dict(self._label),
                }
            self._events.append(event)
        return event

    def events(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(event) for event in self._events]

    def stop(self) -> None:
        """Stop scheduling new requests; in-flight requests keep running."""

        self._stopped.set()
        if self._thread is not None:
            self._thread.join(timeout=10.0)

    def _dispatch(self) -> None:
        last_freeze = self.sender._clock()
        while not self._stopped.is_set() and not self.sender.stop_event.is_set():
            now = self.sender._clock()
            if self.gc_freeze_s > 0 and now - last_freeze >= self.gc_freeze_s:
                gc.freeze()
                last_freeze = now
            due: list[dict[str, Any]] = []
            with self._lock:
                for workload, entry in self._schedule.items():
                    rate = float(entry["rate"])
                    if rate <= 0:
                        continue
                    while True:
                        at = float(entry["base"]) + int(entry["n"]) / rate
                        if at > now:
                            break
                        label = entry["label"]
                        n = int(entry["n"])
                        sequence_id = f"r{label['live_round']}:{label['window']}:{workload}:{n}"
                        payload = build_request_payload(workload, sequence_id)
                        due.append({
                            "phase": label["window"],
                            "live_round": label["live_round"],
                            "seed": self.seed,
                            "workload": workload,
                            "family": "vision" if workload in VISION_WORKLOADS else "llm",
                            "model": payload["model"],
                            "sequence_id": sequence_id,
                            "attempt_id": f"e1:{sequence_id}",
                            "n": n,
                            "rate": rate,
                            "scheduled_offset": at - self.sender._start,
                            "payload_hash": payload_hash(payload),
                            "payload_json": json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True),
                        })
                        entry["n"] = n + 1
            for row in sorted(due, key=lambda r: float(r["scheduled_offset"])):
                self.sender.submit(row)
            time.sleep(self.tick_s)


__all__ = [
    "AsyncSender",
    "BoundedAsyncSender",
    "ContinuousRateSender",
    "LLM_WORKLOADS",
    "PENDING_PER_WORKLOAD",
    "PENDING_TOTAL",
    "SEED",
    "VISION_WORKLOADS",
    "WORKLOAD_KEYS",
    "build_paired_request_plans",
    "build_request_payload",
    "build_request_plan",
    "demand_rates",
    "load_demand_rows",
    "payload_hash",
    "phase_offset",
    "request_offsets",
    "stable_hash_fraction",
    "urllib_transport",
    "validate_demand_rows",
]
