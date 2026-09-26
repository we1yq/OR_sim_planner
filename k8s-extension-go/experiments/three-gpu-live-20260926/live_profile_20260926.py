#!/usr/bin/env python3
"""Profile already-running model replicas without changing cluster state.

The profiler deliberately talks only to a replica's ``/healthz``, ``/metrics``
and ``/infer`` endpoints.  In particular, it does not know how to create a
pod, configure MIG, register a route, or change a runtime batch size.
"""

from __future__ import annotations

import argparse
import json
import re
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Callable, Mapping, Sequence


DEFAULT_SAMPLE_WINDOW_SECONDS = 60.0
DEFAULT_HTTP_TIMEOUT_SECONDS = 30.0


class ProfileValidationError(ValueError):
    """Raised for invalid profiler configuration before any requests run."""


class _Replica:
    def __init__(
        self,
        replica_id: str,
        endpoint: str,
        expected: dict[str, Any],
        family: str,
        request_payload: dict[str, Any],
        warmup_requests: int,
    ) -> None:
        self.replica_id = replica_id
        self.endpoint = endpoint
        self.expected = expected
        self.family = family
        self.request_payload = request_payload
        self.warmup_requests = warmup_requests


class _ReplicaRun:
    def __init__(self, replica: _Replica) -> None:
        self.replica = replica
        self.samples: list[dict[str, Any]] = []
        self.errors: list[str] = []
        self.sample_started: float | None = None
        self.sample_finished: float | None = None


_ALIASES: dict[str, tuple[str, ...]] = {
    "model": ("model", "modelName", "model_name"),
    "runtime_id": ("runtimeId", "runtime_id", "id"),
    "runtime_mode": ("runtimeMode", "runtime_mode"),
    "profile": (
        "profile",
        "physicalProfile",
        "physical_profile",
        "migProfile",
        "mig_profile",
        "orSimProfile",
        "OR_SIM_PROFILE",
    ),
    "batch_size": ("batchSize", "batch_size", "batch"),
    "mig_uuid": ("migUuid", "migUUID", "mig_uuid", "orSimMIGUUID"),
    "slot": ("slot", "orSimSlot"),
    "slot_resource": ("slotResource", "slot_resource", "orSimSlotResource"),
    "device_resource": ("deviceResource", "device_resource", "orSimDeviceResource"),
    "physical_gpu_id": ("physicalGpuId", "physicalGPUId", "physical_gpu_id", "orSimPhysicalGpuID"),
    "expected_mig_uuid": ("expectedMigUuid", "expectedMIGUUID", "expected_mig_uuid", "orSimExpectedMIGUUID"),
}


def _first(mapping: Mapping[str, Any], names: Sequence[str]) -> tuple[bool, Any]:
    for name in names:
        if name in mapping:
            return True, mapping[name]
    return False, None


def _same_value(left: Any, right: Any) -> bool:
    if isinstance(left, bool) or isinstance(right, bool):
        return left is right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return left == right
    return str(left) == str(right)


def _canonical_expected(descriptor: Mapping[str, Any]) -> dict[str, Any]:
    expected: dict[str, Any] = {}
    nested = descriptor.get("identity")
    if isinstance(nested, Mapping):
        expected.update(nested)
    nested = descriptor.get("expected")
    if isinstance(nested, Mapping):
        expected.update(nested)

    aliases_to_canonical = {
        alias: canonical for canonical, aliases in _ALIASES.items() for alias in aliases
    }
    for key, value in descriptor.items():
        canonical = aliases_to_canonical.get(key, key)
        if canonical in _ALIASES and value is not None and value != "":
            expected[canonical] = value
    normalized: dict[str, Any] = {}
    for key, value in expected.items():
        canonical = aliases_to_canonical.get(key, key)
        if canonical in _ALIASES and value is not None and value != "":
            normalized[canonical] = value
    return normalized


def _classify_family(value: Any) -> str | None:
    text = str(value).lower()
    if text in {"vision", "image", "cv"}:
        return "vision"
    if text in {"llm", "language", "text", "text-generation"}:
        return "llm"
    if any(token in text for token in ("llm", "gpt", "llama", "transformer", "prompt", "language")):
        return "llm"
    if any(token in text for token in ("vision", "resnet", "vgg", "vit", "image", "cv")):
        return "vision"
    return None


def _family_hint(descriptor: Mapping[str, Any]) -> str | None:
    for key in ("family", "workloadFamily", "workload_family", "requestClass", "request_class"):
        value = descriptor.get(key)
        if value:
            family = _classify_family(value)
            if family:
                return family
    workload = descriptor.get("workload")
    if workload:
        family = _classify_family(workload)
        if family:
            return family
    payload = descriptor.get("payload") or descriptor.get("requestPayload") or descriptor.get("request_payload")
    if isinstance(payload, Mapping):
        if "prompt_len" in payload or "output_tokens" in payload or "max_tokens" in payload:
            return "llm"
        if "image" in payload or "image_size" in payload:
            return "vision"
    return None


def _resolve_family(descriptor: Mapping[str, Any], default_family: str) -> str:
    hint = _family_hint(descriptor)
    candidate = hint if hint is not None else default_family.lower()
    if candidate == "auto":
        model_or_workload = " ".join(
            str(descriptor.get(key, "")) for key in ("model", "modelName", "workload")
        )
        candidate = _classify_family(model_or_workload) or "auto"
    if candidate not in {"vision", "llm"}:
        raise ProfileValidationError(
            "family must be 'vision', 'llm', or 'auto' with a per-replica family/workload"
        )
    return candidate


def _normalize_replica(
    item: Any,
    index: int,
    default_family: str,
    default_warmup_requests: int,
) -> _Replica:
    if isinstance(item, str):
        endpoint = item
        descriptor: Mapping[str, Any] = {}
    elif isinstance(item, Mapping):
        descriptor = item
        endpoint = str(
            descriptor.get("endpoint")
            or descriptor.get("url")
            or descriptor.get("baseUrl")
            or descriptor.get("base_url")
            or ""
        )
    else:
        raise ProfileValidationError(f"replica {index} must be a URL or mapping")
    endpoint = endpoint.rstrip("/")
    if not endpoint:
        raise ProfileValidationError(f"replica {index} has no endpoint")
    replica_id = str(
        descriptor.get("replicaId")
        or descriptor.get("replica_id")
        or descriptor.get("name")
        or descriptor.get("id")
        or descriptor.get("runtimeId")
        or descriptor.get("runtime_id")
        or f"replica-{index + 1}"
    )
    expected = _canonical_expected(descriptor)
    if "batch_size" not in expected:
        raise ProfileValidationError(f"{replica_id}: batchSize is required")
    try:
        expected["batch_size"] = int(expected["batch_size"])
    except (TypeError, ValueError) as exc:
        raise ProfileValidationError(f"{replica_id}: batchSize must be an integer") from exc
    if expected["batch_size"] <= 0:
        raise ProfileValidationError(f"{replica_id}: batchSize must be positive")
    family = _resolve_family(descriptor, default_family)
    try:
        warmup_requests = int(descriptor.get("warmupRequests", descriptor.get("warmup_requests", default_warmup_requests)))
    except (TypeError, ValueError) as exc:
        raise ProfileValidationError(f"{replica_id}: warmupRequests must be an integer") from exc
    if warmup_requests < 0:
        raise ProfileValidationError(f"{replica_id}: warmupRequests must be non-negative")
    request_payload = {}
    supplied_payload = (
        descriptor.get("payload")
        or descriptor.get("requestPayload")
        or descriptor.get("request_payload")
    )
    if supplied_payload is not None:
        if not isinstance(supplied_payload, Mapping):
            raise ProfileValidationError(f"{replica_id}: payload must be an object")
        request_payload = dict(supplied_payload)
    return _Replica(replica_id, endpoint, expected, family, request_payload, warmup_requests)


def _json_request(
    url: str,
    *,
    method: str = "GET",
    payload: Mapping[str, Any] | None = None,
    timeout_seconds: float,
) -> dict[str, Any]:
    data = None
    headers: dict[str, str] = {}
    if payload is not None:
        data = json.dumps(dict(payload), separators=(",", ":")).encode("utf-8")
        headers["content-type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8")
            value = json.loads(body) if body else {}
            if not isinstance(value, dict):
                raise RuntimeError(f"{url}: JSON response is not an object")
            return value
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{url}: HTTP {exc.code}: {body[:300]}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"{url}: {exc.reason}") from exc


def _snapshot_value(snapshot: Mapping[str, Any], canonical: str) -> tuple[bool, Any]:
    found, value = _first(snapshot, _ALIASES.get(canonical, (canonical,)))
    if found:
        return found, value
    # Router/runtime snapshots sometimes namespace runtime fields.
    runtime = snapshot.get("runtime")
    if isinstance(runtime, Mapping):
        found, value = _first(runtime, _ALIASES.get(canonical, (canonical,)))
        if found:
            return found, value
    if canonical == "profile":
        found, slot_resource = _first(snapshot, _ALIASES["slot_resource"])
        if found:
            match = re.search(r"-([0-9]g)$", str(slot_resource))
            if match:
                return True, match.group(1)
    return False, None


def _validate_snapshot(
    replica: _Replica,
    snapshot: Mapping[str, Any],
    source: str,
    *,
    baseline: Mapping[str, Any] | None = None,
) -> list[str]:
    errors: list[str] = []
    if source == "health" and snapshot.get("ok") is False:
        errors.append(f"{replica.replica_id}: /healthz reported ok=false")
    for canonical, expected in replica.expected.items():
        found, actual = _snapshot_value(snapshot, canonical)
        if not found:
            errors.append(f"{replica.replica_id}: {source} missing {canonical}")
        elif not _same_value(actual, expected):
            errors.append(
                f"{replica.replica_id}: {source} {canonical}={actual!r}, expected {expected!r}"
            )
    if baseline is not None:
        for canonical in ("model", "runtime_id", "runtime_mode", "profile", "batch_size"):
            before_found, before = _snapshot_value(baseline, canonical)
            after_found, after = _snapshot_value(snapshot, canonical)
            if before_found and (
                not after_found or not _same_value(before, after)
            ):
                errors.append(
                    f"{replica.replica_id}: {source} changed {canonical} from {before!r} to {after!r}"
                )
    return errors


def _payload_for(
    replica: _Replica,
    payload: Mapping[str, Any] | None,
) -> dict[str, Any]:
    body = dict(payload or {"benchmark": True})
    body.update(replica.request_payload)
    body["batch"] = replica.expected["batch_size"]
    if replica.family == "llm":
        body.setdefault("prompt_len", 64)
        body.setdefault("output_tokens", 64)
    return body


def _runtime_seconds(response: Mapping[str, Any]) -> float:
    for key in ("runtimeInferenceSeconds", "runtime_inference_seconds", "inferenceSeconds"):
        if key in response:
            return float(response[key])
    for key in ("runtimeLatencyMs", "runtime_latency_ms", "inferenceLatencyMs"):
        if key in response:
            return float(response[key]) / 1000.0
    raise RuntimeError("infer response has no runtime inference duration")


def _logical_samples(response: Mapping[str, Any], default_batch: int) -> int:
    for key in ("logicalSamples", "logicalSampleCount", "logicalRequestCount"):
        if key in response:
            return int(response[key])
    for key in ("batchSize", "batch_size", "batch"):
        if key in response:
            return int(response[key])
    return default_batch


def _response_metadata_errors(replica: _Replica, response: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    for canonical in ("model", "runtime_id", "profile", "batch_size"):
        expected = replica.expected.get(canonical)
        if expected is None:
            continue
        found, actual = _snapshot_value(response, canonical)
        if found and not _same_value(actual, expected):
            errors.append(
                f"{replica.replica_id}: infer {canonical}={actual!r}, expected {expected!r}"
            )
    return errors


def run_profile(
    replicas: Sequence[Any],
    *,
    family: str = "vision",
    payload: Mapping[str, Any] | None = None,
    warmup_requests: int = 1,
    sample_window_seconds: float = DEFAULT_SAMPLE_WINDOW_SECONDS,
    timeout_seconds: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    """Measure existing replicas concurrently and return a JSON-ready report.

    ``replicas`` accepts endpoint strings or mappings.  A mapping must contain
    ``endpoint`` and ``batchSize`` (or their snake-case forms); identity fields
    such as ``model``, ``runtimeId`` and ``profile`` are checked when supplied.
    ``family='auto'`` resolves each descriptor from ``family``, ``workload`` or
    request-class fields (and then from a recognizable model name).  A
    descriptor may include its own ``payload``/``requestPayload`` object.
    The optional ``payload`` is copied for every request.  LLM requests receive
    defaults for ``prompt_len`` and ``output_tokens``; vision requests retain
    the supplied shape and only force the configured batch.
    """
    if not isinstance(warmup_requests, int) or warmup_requests < 0:
        raise ProfileValidationError("warmup_requests must be a non-negative integer")
    if sample_window_seconds <= 0:
        raise ProfileValidationError("sample_window_seconds must be positive")
    if timeout_seconds <= 0:
        raise ProfileValidationError("timeout_seconds must be positive")
    if not replicas:
        raise ProfileValidationError("at least one replica is required")
    if family not in {"vision", "llm", "auto"}:
        raise ProfileValidationError("family must be 'vision', 'llm', or 'auto'")
    normalized = [
        _normalize_replica(item, index, family, warmup_requests)
        for index, item in enumerate(replicas)
    ]

    runs = [_ReplicaRun(replica) for replica in normalized]
    metadata_errors: list[str] = []
    baselines: dict[str, dict[str, Any]] = {}
    for run in runs:
        try:
            health = _json_request(
                run.replica.endpoint + "/healthz", timeout_seconds=timeout_seconds
            )
            metrics = _json_request(
                run.replica.endpoint + "/metrics", timeout_seconds=timeout_seconds
            )
        except Exception as exc:
            metadata_errors.append(f"{run.replica.replica_id}: preflight failed: {exc}")
            continue
        errors = _validate_snapshot(run.replica, health, "health")
        errors.extend(_validate_snapshot(run.replica, metrics, "metrics"))
        for canonical in ("model", "runtime_id", "runtime_mode", "profile", "batch_size"):
            health_found, health_value = _snapshot_value(health, canonical)
            metrics_found, metrics_value = _snapshot_value(metrics, canonical)
            if health_found != metrics_found or (
                health_found and not _same_value(health_value, metrics_value)
            ):
                errors.append(
                    f"{run.replica.replica_id}: health/metrics disagree on {canonical}"
                )
        metadata_errors.extend(errors)
        baselines[run.replica.replica_id] = dict(metrics)

    if metadata_errors:
        return {
            "status": "metadata_mismatch",
            "sampleWindowSeconds": sample_window_seconds,
            "warmupRequests": warmup_requests,
            "samples": [],
            "replicas": [_aggregate(run) for run in runs],
            "errors": metadata_errors,
        }

    stop_event = threading.Event()
    deadline_box: list[float] = []
    deadline_lock = threading.Lock()

    def release_barrier() -> None:
        with deadline_lock:
            deadline_box.append(clock() + sample_window_seconds)

    barrier = threading.Barrier(len(runs), action=release_barrier)

    def mark_error(run: _ReplicaRun, message: str) -> None:
        run.errors.append(message)
        stop_event.set()
        try:
            barrier.abort()
        except threading.BrokenBarrierError:
            pass

    def worker(run: _ReplicaRun) -> None:
        replica = run.replica
        try:
            warmup_body = _payload_for(replica, payload)
            for _ in range(replica.warmup_requests):
                _json_request(
                    replica.endpoint + "/infer",
                    method="POST",
                    payload=warmup_body,
                    timeout_seconds=timeout_seconds,
                )
        except Exception as exc:
            mark_error(run, f"warmup failed: {exc}")
            return
        try:
            barrier.wait()
        except threading.BrokenBarrierError:
            return
        with deadline_lock:
            deadline = deadline_box[0]
        run.sample_started = deadline - sample_window_seconds
        while not stop_event.is_set():
            started = clock()
            if started >= deadline:
                break
            try:
                response = _json_request(
                    replica.endpoint + "/infer",
                    method="POST",
                    payload=warmup_body,
                    timeout_seconds=timeout_seconds,
                )
                ended = clock()
                response_errors = _response_metadata_errors(replica, response)
                if response_errors:
                    mark_error(run, "; ".join(response_errors))
                    break
                runtime_seconds = _runtime_seconds(response)
                logical_samples = _logical_samples(response, replica.expected["batch_size"])
                if runtime_seconds <= 0 or logical_samples <= 0:
                    raise RuntimeError("infer response has non-positive duration or logical samples")
                run.samples.append(
                    {
                        "replicaId": replica.replica_id,
                        "endpoint": replica.endpoint,
                        "sequence": len(run.samples) + 1,
                        "startedAtSeconds": started - run.sample_started,
                        "endedAtSeconds": ended - run.sample_started,
                        "logicalSamples": logical_samples,
                        "runtimeInferenceSeconds": runtime_seconds,
                        "runtimeInferenceMs": runtime_seconds * 1000.0,
                        "batchSize": replica.expected["batch_size"],
                        "profile": replica.expected.get("profile"),
                        "complete": True,
                    }
                )
            except Exception as exc:
                mark_error(run, f"sample failed after {started - run.sample_started:.6f}s: {exc}")
                break
        run.sample_finished = clock()

    threads = [
        threading.Thread(target=worker, args=(run,), name=f"profile-{run.replica.replica_id}")
        for run in runs
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    for run in runs:
        try:
            metrics = _json_request(
                run.replica.endpoint + "/metrics", timeout_seconds=timeout_seconds
            )
        except Exception as exc:
            run.errors.append(f"{run.replica.replica_id}: postflight failed: {exc}")
            continue
        errors = _validate_snapshot(
            run.replica,
            metrics,
            "postflight metrics",
            baseline=baselines[run.replica.replica_id],
        )
        run.errors.extend(errors)

    all_errors = [error for run in runs for error in run.errors]
    any_missing = any(not run.samples for run in runs)
    status = "error" if all_errors else ("incomplete" if any_missing else "ok")
    return {
        "status": status,
        "sampleWindowSeconds": sample_window_seconds,
        "warmupRequests": warmup_requests,
        "warmupRequestsByReplica": {
            run.replica.replica_id: run.replica.warmup_requests for run in runs
        },
        "samples": [sample for run in runs for sample in run.samples],
        "replicas": [_aggregate(run) for run in runs],
        "errors": all_errors,
    }


def _aggregate(run: _ReplicaRun) -> dict[str, Any]:
    logical = sum(int(sample["logicalSamples"]) for sample in run.samples)
    runtime_seconds = sum(float(sample["runtimeInferenceSeconds"]) for sample in run.samples)
    return {
        "replicaId": run.replica.replica_id,
        "endpoint": run.replica.endpoint,
        "profile": run.replica.expected.get("profile"),
        "batchSize": run.replica.expected.get("batch_size"),
        "warmupRequests": run.replica.warmup_requests,
        "sampleCount": len(run.samples),
        "logicalSamples": logical,
        "runtimeInferenceSeconds": runtime_seconds,
        "mu": logical / runtime_seconds if runtime_seconds > 0 else None,
        "sampleWallSeconds": (
            run.sample_finished - run.sample_started
            if run.sample_started is not None and run.sample_finished is not None
            else None
        ),
        "errors": list(run.errors),
    }


# Friendly aliases for callers that prefer a noun phrase or an explicit name.
profile_replicas = run_profile
profile_existing_replicas = run_profile


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replicas", required=True, help="JSON file containing replica descriptors")
    parser.add_argument("--family", choices=("vision", "llm", "auto"), required=True)
    parser.add_argument("--payload-json", default="", help="optional JSON object copied into /infer")
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--sample-window", type=float, default=DEFAULT_SAMPLE_WINDOW_SECONDS)
    parser.add_argument("--timeout", type=float, default=DEFAULT_HTTP_TIMEOUT_SECONDS)
    parser.add_argument("--out", default="", help="optional report JSON path")
    args = parser.parse_args(argv)
    with open(args.replicas, encoding="utf-8") as stream:
        replicas = json.load(stream)
    payload = json.loads(args.payload_json) if args.payload_json else None
    report = run_profile(
        replicas,
        family=args.family,
        payload=payload,
        warmup_requests=args.warmup,
        sample_window_seconds=args.sample_window,
        timeout_seconds=args.timeout,
    )
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as stream:
            stream.write(rendered + "\n")
    print(rendered)
    return 0 if report["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
