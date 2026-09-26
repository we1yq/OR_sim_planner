"""Standard-library-only collectors for the three-GPU live experiment v2.

The live run produces several generations of JSON while the control plane is
being rolled forward.  These helpers intentionally accept both camelCase and
snake_case spellings and retain the source records instead of normalising them
away.  Missing observations are represented by ``None`` and a reason; an
inferred timestamp is never emitted.
"""

from __future__ import annotations

import csv
import json
import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


NULL = None

ACTION_FIELDS = (
    "action_id", "action_type", "attempt", "status", "phase", "category",
    "depends_on", "workload", "workloads", "physical_gpu_id", "logical_gpu_id",
    "slot", "started_at", "finished_at", "duration_seconds",
    "relative_start_seconds", "relative_end_seconds", "error", "errors",
)

LIFECYCLE_EVENTS = (
    "deployment_create_started_at", "deployment_created_at", "pod_ready_at",
    "model_cuda_verified_at", "route_activation_ack_at",
    "route_stop_accepting_effective_at", "drain_started_at", "drain_completed_at",
    "pod_delete_started_at", "pod_gone_confirmed_at",
)

EVENT_ALIASES = {
    "deployment_create_started_at": (
        "deploymentCreateStartedAt", "runtimeDeploymentCreateStartedAt", "create_started_at",
    ),
    "deployment_created_at": ("deploymentCreatedAt", "runtimeDeploymentCreatedAt", "created_at"),
    "pod_ready_at": (
        "podReadyAt", "runtimeReadyAt", "runtimeReadyAndCUDAVerifiedAt", "ready_at",
    ),
    "model_cuda_verified_at": (
        "modelCUDAVerifiedAt", "cudaVerifiedAt", "runtimeReadyAndCUDAVerifiedAt", "cuda_verified_at",
    ),
    "route_activation_ack_at": ("routeActivationAckAt", "routeSyncedAt", "route_active_at"),
    "route_stop_accepting_effective_at": (
        "routeStopAcceptingEffectiveAt", "stopAcceptingAt", "route_stop_at", "routeStoppedAcceptingAt",
    ),
    "drain_started_at": ("drainStartedAt",),
    "drain_completed_at": ("drainCompletedAt", "drainFinishedAt"),
    "pod_delete_started_at": ("podDeleteStartedAt", "deleteStartedAt"),
    "pod_gone_confirmed_at": ("podGoneConfirmedAt", "runtimePodGoneAt", "pod_gone_at"),
}

REQUIRED_OUTPUT_SCHEMAS = {
    # These are the formal artifacts named by section 8 of the runbook plus
    # the runner's machine-readable execution status.  Keep the order stable:
    # it is also the canonical order used by CSV writers.
    "environment.json": ("run_id", "started_at", "traffic_seed", "input_sha256", "solver"),
    "profile_protocol.json": (
        "mode", "sample_window_seconds", "warmup_requests", "family",
        "traffic_seed", "timing_boundary", "source", "request_shapes",
    ),
    "planned_requests.csv": (
        "run_id", "live_round", "trace_round", "phase", "traffic_seed",
        "workload", "sequence_id", "payload_hash", "scheduled_offset", "rate", "seed",
    ),
    "requests.jsonl": (
        "run_id", "live_round", "trace_round", "phase", "attempt_id",
        "sequence_id", "workload", "scheduled_send", "actual_send",
        "first_token", "completion", "status", "error", "sample_count",
    ),
    "actions.jsonl": (
        "run_id", "live_round", "trace_round", "phase", "plan_id", "action_id",
        "action_type", "attempt", "depends_on", "started_at", "finished_at",
        "status", "workload", "runtime_id", "physical_gpu_id", "logical_gpu_id", "slot", "error",
    ),
    "replica_lifecycle.csv": (
        "run_id", "live_round", "trace_round", "phase", "workload", "family",
        "runtime_id", "pod_uid", "node", "gpu_uuid", "mig_uuid", "slot",
        "physical_profile", "batch", "round", "action_id", "attempt",
        "deployment_create_started_at", "deployment_created_at", "pod_ready_at",
        "model_cuda_verified_at",
        "route_activation_ack_at", "route_stop_accepting_effective_at",
        "drain_started_at", "drain_completed_at", "pod_delete_started_at",
        "pod_gone_confirmed_at", "missing_event_reasons",
    ),
    "action_runtime_by_workload.csv": (
        "run_id", "live_round", "trace_round", "phase", "workload", "action_type",
        "count", "success_count", "failure_count", "duration_min_seconds",
        "duration_median_seconds", "duration_max_seconds",
    ),
    "runtime_events.jsonl": ("run_id", "live_round", "trace_round", "phase", "event_type", "timestamp"),
    "gpu_events.jsonl": (
        "run_id", "live_round", "trace_round", "event_type", "timestamp",
        "physical_gpu_id", "owner", "action_id",
    ),
    "profile_samples.csv": (
        "run_id", "live_round", "trace_round", "phase", "workload", "runtime_id",
        "logicalSamples", "runtimeInferenceSeconds", "complete",
    ),
    "capacity_results.csv": (
        "run_id", "live_round", "trace_round", "phase", "workload", "D",
        "C_pred", "C_measured", "C_pred_over_D", "C_measured_over_D",
        "sample_count", "complete", "unit",
    ),
    "capacity_timeline.csv": (
        "run_id", "live_round", "trace_round", "phase", "event_type", "timestamp",
        "timestamp_source", "runtime_id", "workload", "physical_profile", "batch",
        "mu", "delta", "capacity", "commitment", "capacity_ratio", "reason", "uncertain",
    ),
    "transition_requests_summary.csv": (
        "run_id", "live_round", "trace_round", "phase", "workload", "scheduled",
        "successes", "failures", "timeouts", "pending",
    ),
    "round_summary.csv": (
        "run_id", "live_round", "trace_round", "phase", "reached_target",
        "finalValidationOk", "makespan_seconds", "planner_makespan_seconds",
        "action_count", "action_counts", "strategy_counts", "failure_count",
        "peak_active_gpu_count", "headroom", "capacity_uncertain_events",
    ),
    "round_summary.json": ("run_id", "finalValidation", "makespan_seconds", "action_counts", "rounds"),
    "results.json": ("run_id", "completed_rounds", "expected_rounds", "ok", "failure"),
    "preflight.json": ("ok", "errors"),
    "readiness.md": ("__text__",),
    "results.md": ("__text__",),
}

ROUND_SCOPED_OUTPUTS = frozenset(
    name for name in REQUIRED_OUTPUT_SCHEMAS
    if name.endswith((".csv", ".jsonl")) and name not in {"environment.json", "profile_protocol.json"}
)


def _first(mapping: Any, *names: str, default: Any = None) -> Any:
    if not isinstance(mapping, Mapping):
        return default
    for name in names:
        if name in mapping and mapping[name] is not None:
            return mapping[name]
    return default


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _items(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, Mapping):
        # A single action/status is more useful as one row than as its keys.
        if any(key in value for key in ("id", "action_id", "type", "action_type", "status")):
            return [value]
        return [dict(item, **({"id": key} if isinstance(item, Mapping) and "id" not in item else {}))
                for key, item in value.items() if isinstance(item, Mapping)]
    return []


def _plan_status(plan: Mapping[str, Any]) -> Mapping[str, Any]:
    return _mapping(_first(plan, "status", "actionPlanStatus", default={}))


def _plan_id(plan: Mapping[str, Any]) -> Any:
    metadata = _mapping(_first(plan, "metadata", default={}))
    return _first(plan, "plan_id", "planId", "name", default=_first(metadata, "name", "uid"))


def _action_definitions(plan: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    spec = _mapping(_first(plan, "spec", default={}))
    raw = _first(spec, "actions", "actionDAG", "actionDag", default=_first(plan, "actions", default=[]))
    if isinstance(raw, Mapping) and isinstance(raw.get("nodes"), list):
        raw = raw["nodes"]
    out = []
    for item in _items(raw):
        if isinstance(item, Mapping):
            action = dict(item)
            nested = _mapping(action.get("action"))
            if nested:
                for key, value in nested.items():
                    action.setdefault(key, value)
            out.append(action)
    return out


def _action_statuses(plan: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    status = _plan_status(plan)
    raw = _first(status, "actionStatuses", "action_statuses", default=_first(plan, "actionStatuses", default=[]))
    out = []
    for item in _items(raw):
        if isinstance(item, Mapping):
            out.append(dict(item))
    return out


def _status_key(row: Mapping[str, Any]) -> str:
    return str(_first(row, "id", "action_id", "actionId", "name", default=""))


def _definition_key(row: Mapping[str, Any]) -> str:
    return str(_first(row, "id", "action_id", "actionId", "name", default=""))


def _action_runtime_id(action: Mapping[str, Any]) -> str | None:
    explicit = _first(action, "runtime_id", "runtimeId")
    if explicit:
        return str(explicit)
    workload = _first(action, "workload", "model")
    physical_id = _first(action, "physical_gpu_id", "physicalGpuId", "gpu")
    slot = _first(action, "slot", "slotResource")
    if workload and physical_id and isinstance(slot, (list, tuple)) and len(slot) >= 3:
        raw = f"{workload}-{physical_id}-s{slot[0]}-{slot[1]}-{slot[2]}"
        return re.sub(r"[^a-z0-9-]+", "-", raw.lower()).strip("-")
    return None


def _transition_execution(plan: Mapping[str, Any]) -> Mapping[str, Any]:
    status = _plan_status(plan)
    return _mapping(_first(status, "transitionExecution", "transition_execution",
                            default=_first(plan, "transitionExecution", default={})))


def _timestamp(value: Any) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _duration(start: Any, end: Any) -> float | None:
    first, last = _parse_time(start), _parse_time(end)
    if first is None or last is None:
        return None
    return (last - first).total_seconds()


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _integer(value: Any) -> int | None:
    number = _number(value)
    return int(number) if number is not None and number.is_integer() else None


def _round(value: Any, digits: int = 6) -> float | None:
    number = _number(value)
    return round(number, digits) if number is not None else None


def _nested_timestamp(container: Mapping[str, Any], key: str) -> str | None:
    aliases = (key,) + tuple(EVENT_ALIASES.get(key, ()))
    for candidate in aliases:
        value = _first(container, candidate)
        if value is not None:
            return _timestamp(value)
    return None


def _reason_for_missing(field: str, source: Mapping[str, Any], explicit_reason: Any = None) -> str:
    if explicit_reason:
        return str(explicit_reason)
    if source:
        return "event_not_observed"
    return "no_source_record"


def flatten_mig_action_plan(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Flatten action definitions/statuses and nested executor timing.

    One output row is produced per status/attempt.  An action definition with
    no status is retained as ``unobserved`` so plans cannot silently lose work.
    """
    definitions = {_definition_key(item): item for item in _action_definitions(plan)}
    statuses = _action_statuses(plan)
    status_keys = {_status_key(item) for item in statuses}
    rows: list[dict[str, Any]] = []
    combined: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
    for status in statuses:
        combined.append((definitions.get(_status_key(status), {}), status))
    for key, definition in definitions.items():
        if key not in status_keys:
            combined.append((definition, {"id": key, "status": "unobserved"}))

    for definition, status in combined:
        action = dict(definition)
        action.update({key: value for key, value in status.items() if value is not None})
        trace = _mapping(_first(status, "trace", "actionTrace", default={}))
        timing = _mapping(_first(status, "transitionExecution", "transition_execution", default={}))
        if not timing:
            timing = _transition_execution(plan)
        timestamps = _mapping(_first(status, "timestamps", default=_first(trace, "timestamps", default={})))
        durations = _mapping(_first(status, "durationsSeconds", "durations_seconds",
                                     default=_first(trace, "durationsSeconds", default={})))
        row: dict[str, Any] = {
            "plan_id": _plan_id(plan),
            "action_id": _first(action, "id", "action_id", "actionId", "name"),
            "action_type": _first(action, "type", "action_type", "actionType"),
            "attempt": _first(action, "attempt", "attemptNumber", "retry", default=1),
            "status": _first(action, "status", default="unobserved"),
            "phase": _first(action, "phase"),
            "category": _first(action, "category"),
            "depends_on": _first(action, "dependsOn", "depends_on", default=[]),
            "workload": _first(action, "workload", "model"),
            "runtime_id": _action_runtime_id(action),
            "physical_profile": _first(action, "profile", "physical_profile", "physicalProfile"),
            "batch": _first(action, "batch", "batchSize"),
            "old_batch": _first(action, "oldBatch", "old_batch", "sourceBatch"),
            "new_batch": _first(action, "newBatch", "new_batch", "targetBatch", default=_first(action, "batch", "batchSize")),
            "workloads": _first(action, "workloads", "affectedWorkloads", default=[]),
            "physical_gpu_id": _first(action, "physicalGpuId", "physical_gpu_id", "gpu"),
            "logical_gpu_id": _first(action, "logicalGpuId", "logical_gpu_id"),
            "slot": _first(action, "slot", "slotResource"),
            "started_at": _timestamp(_first(action, "startedAt", "startAt", "start")),
            "finished_at": _timestamp(_first(action, "finishedAt", "endAt", "end")),
            "duration_seconds": _round(_first(action, "durationSeconds", "duration_seconds")),
            "relative_start_seconds": _round(_first(action, "relativeStartSeconds", "relative_start_seconds")),
            "relative_end_seconds": _round(_first(action, "relativeEndSeconds", "relative_end_seconds")),
            "error": _first(action, "error", "message", "errorMessage"),
            "errors": _first(action, "errors", "attemptErrors", default=[]),
            "attempts": _first(action, "attempts", "retryAttempts", default=[]),
            "depends_on_raw": _first(action, "dependsOn", "depends_on", default=[]),
            "raw_status": dict(status),
        }
        if row["duration_seconds"] is None:
            row["duration_seconds"] = _round(_duration(row["started_at"], row["finished_at"]))
        # Trace timestamps are retained both as a nested object and flattened
        # under timing_* so CSV consumers do not need to parse JSON columns.
        row["attempt_errors"] = [item for item in _items(row["attempts"])
                                  if isinstance(item, Mapping) and _first(item, "error", "message")]
        row["timing_timestamps"] = dict(timestamps)
        row["timing_durations_seconds"] = dict(durations)
        row["timing_metrics"] = _first(status, "traceMetrics", "metrics", default=_first(trace, "metrics", default={}))
        row["timing_runtime_readiness"] = _first(status, "runtimeReadiness", "runtime_readiness",
                                                  default=_first(trace, "runtimeReadiness", default={}))
        row["transition_execution"] = dict(timing)
        for key, value in timestamps.items():
            row["timing_" + str(key)] = value
        for key, value in durations.items():
            row["duration_" + str(key)] = value
        # Executor retries commonly put the failure in a nested trace.
        if not row["error"]:
            row["error"] = _first(_mapping(row["timing_metrics"]), "error", "lastError")
        rows.append(row)
    return rows


def flatten_action_plan(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    return flatten_mig_action_plan(plan)


def collect_action_rows(plans: Mapping[str, Any] | Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return flatten_mig_action_plan(plans) if isinstance(plans, Mapping) else [
        row for plan in plans for row in flatten_mig_action_plan(plan)
    ]


def _record_event_source(record: Mapping[str, Any], event: str) -> tuple[str | None, str | None]:
    candidates: list[Mapping[str, Any]] = [record]
    for key in ("executorMetrics", "executor_metrics", "metrics", "traceMetrics", "timing_timestamps", "timestamps"):
        nested = _mapping(record.get(key))
        if nested:
            candidates.append(nested)
    for candidate in candidates:
        value = _nested_timestamp(candidate, event)
        if value is not None:
            return value, "observed"
    return None, _reason_for_missing(event, record)


def _runtime_id(record: Mapping[str, Any]) -> Any:
    return _first(record, "runtime_id", "runtimeId", "id", "name")


def build_replica_lifecycle_rows(
    replicas: Iterable[Mapping[str, Any]] | None = None,
    action_rows: Iterable[Mapping[str, Any]] | None = None,
    runtime_events: Iterable[Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Build one lifecycle row per runtime/workload attempt.

    ``replicas`` may be a snapshot's runtime list or executor metrics.  Action
    rows/events supplement it by identity, but never supply a missing time from
    another event's end time.
    """
    source_records = [dict(row) for row in (replicas or []) if isinstance(row, Mapping)]
    events = [dict(row) for row in (runtime_events or []) if isinstance(row, Mapping)]
    actions = [dict(row) for row in (action_rows or []) if isinstance(row, Mapping)]
    by_runtime: dict[str, dict[str, Any]] = {}
    for record in source_records:
        key = str(_runtime_id(record) or ("record-" + str(len(by_runtime))))
        by_runtime[key] = dict(record)
    for event in events:
        key = str(_runtime_id(event) or _first(event, "pod_uid", "podUid", default=""))
        if key:
            record = by_runtime.setdefault(key, {})
            record.update(event)
            canonical = {
                "deployment_create_started": "deployment_create_started_at",
                "deployment_created": "deployment_created_at",
                "pod_ready": "pod_ready_at",
                "route_activation": "route_activation_ack_at",
                "route_stop_accepting": "route_stop_accepting_effective_at",
                "drain_started": "drain_started_at",
                "drain_completed": "drain_completed_at",
                "pod_delete_started": "pod_delete_started_at",
                "pod_gone_confirmed": "pod_gone_confirmed_at",
            }.get(str(_first(event, "event_type", "type", default="")))
            if canonical and event.get("timestamp"):
                record[canonical] = event["timestamp"]
    for action in actions:
        key = str(_runtime_id(action) or _first(action, "runtime_id", "runtimeId", default=""))
        if key and key in by_runtime:
            by_runtime[key].setdefault("action_id", _first(action, "action_id", "actionId", "id"))
            by_runtime[key].update({"executorMetrics": action.get("timing_timestamps", {})})

    rows: list[dict[str, Any]] = []
    for key, record in by_runtime.items():
        family = _first(record, "family", "model_family")
        row: dict[str, Any] = {
            "workload": _first(record, "workload", "model", "runtimeModel"),
            "family": family,
            "runtime_id": _runtime_id(record) or key,
            "pod_uid": _first(record, "pod_uid", "podUid", "uid"),
            "node": _first(record, "node", "node_name", "nodeName"),
            "gpu_uuid": _first(record, "gpu_uuid", "gpuUuid", "gpu"),
            "mig_uuid": _first(record, "mig_uuid", "migUuid", "expectedMigUuid"),
            "slot": _first(record, "slot", "slotResource"),
            "physical_profile": _first(record, "physical_profile", "physicalProfile", "profile"),
            "batch": _first(record, "batch", "batchSize", "batch_size"),
            "round": _first(record, "round", "live_round", "liveRound"),
            "action_id": _first(record, "action_id", "actionId"),
            "attempt": _first(record, "attempt", "attemptNumber", default=1),
            "raw_record": dict(record),
        }
        missing: dict[str, str] = {}
        for event_name in LIFECYCLE_EVENTS:
            value, reason = _record_event_source(record, event_name)
            row[event_name] = value
            if value is None:
                missing[event_name] = reason or "event_not_observed"
            row[event_name + "_reason"] = None if value is not None else missing[event_name]
        row["missing_event_reasons"] = missing
        row["create_to_route_seconds"] = _round(_duration(
            row["deployment_create_started_at"], row["route_activation_ack_at"]))
        row["stop_to_pod_gone_seconds"] = _round(_duration(
            row["route_stop_accepting_effective_at"], row["pod_gone_confirmed_at"]))
        rows.append(row)
    return rows


def _action_timestamp(row: Mapping[str, Any], end: bool = True) -> str | None:
    return _timestamp(_first(row, "finished_at" if end else "started_at", "finishedAt" if end else "startedAt",
                             "end" if end else "start"))


def build_gpu_events(
    action_rows: Iterable[Mapping[str, Any]] | None = None,
    initial_gpu_ids: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    """Build acquisition/release events from explicit executor action ends."""
    rows: list[dict[str, Any]] = []
    for gpu_id in initial_gpu_ids or []:
        rows.append({"event_type": "baseline", "timestamp": None, "timestamp_source": "inventory",
                     "physical_gpu_id": gpu_id, "delta": 1, "owner": "source_inventory",
                     "action_id": None, "status": "observed"})
    for action in action_rows or []:
        action_type = str(_first(action, "action_type", "type", "actionType", default=""))
        if action_type not in {"allocate_gpu", "return_gpu"}:
            continue
        gpu_id = _first(action, "physical_gpu_id", "physicalGpuId", "gpu")
        event_type = "acquire" if action_type == "allocate_gpu" else "release"
        rows.append({
            "event_type": event_type,
            "timestamp": _action_timestamp(action),
            "timestamp_source": "action_finished_at" if _action_timestamp(action) else None,
            "physical_gpu_id": gpu_id,
            "delta": 1 if event_type == "acquire" else -1,
            "owner": _first(action, "workload", "owner", default="plan"),
            "action_id": _first(action, "action_id", "actionId", "id"),
            "attempt": _first(action, "attempt", default=1),
            "status": _first(action, "status"),
            "error": _first(action, "error", "message"),
        })
    return rows


def gpu_count_intervals(events: Iterable[Mapping[str, Any]], initial_count: int | None = None) -> list[dict[str, Any]]:
    """Return count intervals; an interval has ``None`` bounds if time is unknown."""
    rows = list(events)
    baseline = initial_count
    if baseline is None:
        baseline = sum(1 for row in rows if _first(row, "event_type", "type") == "baseline")
    timed = [row for row in rows if _parse_time(_first(row, "timestamp", "at")) is not None]
    timed.sort(key=lambda row: (_parse_time(_first(row, "timestamp", "at")), str(_first(row, "action_id", default=""))))
    if not timed:
        return [{"start_at": None, "end_at": None, "active_gpu_count": baseline,
                 "start_reason": "no_timestamped_gpu_events", "end_reason": "no_timestamped_gpu_events"}]
    output: list[dict[str, Any]] = []
    current = baseline
    previous: str | None = None
    for row in timed:
        at = _timestamp(_first(row, "timestamp", "at"))
        if previous != at:
            output.append({"start_at": previous, "end_at": at, "active_gpu_count": current,
                           "start_reason": None if previous else "baseline_time_unobserved",
                           "end_reason": "event_boundary"})
        current += _integer(_first(row, "delta", default=0)) or 0
        previous = at
    output.append({"start_at": previous, "end_at": None, "active_gpu_count": current,
                   "start_reason": "event_observed", "end_reason": "experiment_end_unobserved"})
    return output


def _catalog_rows(catalog: Any) -> list[Mapping[str, Any]]:
    if isinstance(catalog, (str, Path)):
        with Path(catalog).open(newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))
    if isinstance(catalog, Mapping):
        if "rows" in catalog:
            return [row for row in _items(catalog["rows"]) if isinstance(row, Mapping)]
        return [catalog]
    return [row for row in (catalog or []) if isinstance(row, Mapping)]


def _catalog_mu(catalog: Any, workload: Any, profile: Any, batch: Any) -> float | None:
    target_batch = _integer(batch)
    for row in _catalog_rows(catalog):
        if str(_first(row, "workload", "model")) != str(workload):
            continue
        if str(_first(row, "profile", "physical_profile", "physicalProfile")) != str(profile):
            continue
        row_batch = _integer(_first(row, "batch", "batchSize", "batch_size"))
        if row_batch == target_batch:
            return _number(_first(row, "mu", "capacity", "rate"))
    return None


def _replica_key(row: Mapping[str, Any]) -> str:
    return str(_first(row, "runtime_id", "runtimeId", "replica_id", "id", default=""))


def _capacity_from_row(row: Mapping[str, Any], catalog: Any) -> tuple[float | None, str | None]:
    explicit = _number(_first(row, "mu", "capacity", "catalog_mu", "predicted_mu"))
    if explicit is not None:
        return explicit, "observed_metric"
    mu = _catalog_mu(catalog, _first(row, "workload", "model"),
                     _first(row, "physical_profile", "physicalProfile", "profile"),
                     _first(row, "batch", "batchSize", "batch_size"))
    return (mu, "frozen_catalog") if mu is not None else (None, "catalog_match_missing")


def _event_row(event_type: str, at: Any, replica: Mapping[str, Any], delta: float,
               mu: float | None, reason: str | None = None) -> dict[str, Any]:
    return {
        "event_type": event_type, "timestamp": _timestamp(at), "timestamp_source": "observed" if at else None,
        "runtime_id": _replica_key(replica), "workload": _first(replica, "workload", "model"),
        "mu": mu, "delta": delta, "reason": reason, "physical_profile": _first(replica, "physical_profile", "profile"),
        "batch": _first(replica, "batch", "batchSize"),
    }


def derive_capacity_timeline(
    catalog: Any,
    replicas: Iterable[Mapping[str, Any]] | None = None,
    runtime_events: Iterable[Mapping[str, Any]] | None = None,
    initial_replicas: Iterable[Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Derive capacity from ready+route adds, stop-accepting removes and batch effects.

    An add requires both an observed ready and route activation timestamp.  A
    removal uses the observed stop-accepting timestamp, even when pod drain is
    still in progress.  Missing evidence creates a diagnostic row, never an
    invented time.
    """
    records = [dict(row) for row in (replicas or [])]
    if not records:
        records = [dict(row) for row in (initial_replicas or [])]
    events = [dict(item) for item in (runtime_events or []) if isinstance(item, Mapping)]
    records_by_id = {_replica_key(row): row for row in records if _replica_key(row)}
    for event in events:
        event_type = str(_first(event, "event_type", "type", "name", default="")).lower()
        runtime_id = str(_first(event, "runtime_id", "runtimeId", "replica_id", default=""))
        if not runtime_id or event_type in {"batch_apply", "batch_verify", "batch_effective"}:
            continue
        record = records_by_id.setdefault(runtime_id, dict(event))
        at = _first(event, "timestamp", "at", "observed_at")
        if "ready" in event_type:
            record["pod_ready_at"] = at
        elif "route" in event_type and ("activ" in event_type or "active" in event_type):
            record["route_activation_ack_at"] = at
        elif "stop" in event_type and "accept" in event_type:
            record["route_stop_accepting_effective_at"] = at
        for key in ("workload", "model", "profile", "physical_profile", "batch", "batchSize", "mu", "capacity"):
            if key in event and key not in record:
                record[key] = event[key]
    records = list(records_by_id.values())
    for record in records:
        record.setdefault("_capacity_mu", _capacity_from_row(record, catalog)[0])
    changes: list[dict[str, Any]] = []
    for replica in records:
        mu, mu_reason = _capacity_from_row(replica, catalog)
        ready = _first(replica, "pod_ready_at", "podReadyAt", "ready_at")
        route = _first(replica, "route_activation_ack_at", "routeActivationAckAt", "routeSyncedAt", "route_active_at")
        stop = _first(replica, "route_stop_accepting_effective_at", "routeStopAcceptingEffectiveAt", "stopAcceptingAt")
        ready_time, route_time = _parse_time(ready), _parse_time(route)
        if ready_time is not None and route_time is not None:
            add_at = max((ready_time, route_time))
            changes.append(_event_row("route_ready_add", add_at.isoformat().replace("+00:00", "Z"), replica,
                                      mu if mu is not None else 0.0, mu, None if mu is not None else mu_reason))
        else:
            changes.append(_event_row("capacity_add_unproven", None, replica, 0.0, mu,
                                      "missing_ready_or_route_activation"))
        if stop:
            changes.append(_event_row("stop_accepting_remove", stop, replica,
                                      -(mu if mu is not None else 0.0), mu, None if mu is not None else mu_reason))
        elif _first(replica, "pod_gone_confirmed_at", "podGoneConfirmedAt"):
            changes.append(_event_row("capacity_remove_unproven", None, replica, 0.0, mu,
                                      "pod_gone_observed_without_stop_accepting"))

    # Runtime event logs can be the only source for route/batch changes.
    for event in events:
        event_type = str(_first(event, "event_type", "type", "name", default=""))
        if event_type not in {"batch_apply", "batch_verify", "batch_effective", "route_activation", "route_stop_accepting"}:
            continue
        at = _first(event, "timestamp", "at", "observed_at")
        if not at:
            changes.append({"event_type": event_type, "timestamp": None, "delta": 0,
                            "reason": "event_timestamp_missing", "raw_event": event})
            continue
        if event_type == "batch_effective" or event_type == "batch_verify":
            old_mu = _number(_first(event, "old_mu", "oldCapacity"))
            new_mu = _number(_first(event, "new_mu", "newCapacity", "mu", "capacity"))
            workload = _first(event, "workload", "model")
            profile = _first(event, "profile", "physical_profile", "new_profile", "newPhysicalProfile")
            old_profile = _first(event, "old_profile", "oldPhysicalProfile", default=profile)
            new_batch = _first(event, "new_batch", "newBatch", "batch", "batchSize")
            old_batch = _first(event, "old_batch", "oldBatch", default=new_batch)
            if old_mu is None:
                old_mu = _catalog_mu(catalog, workload, old_profile, old_batch)
            if new_mu is None:
                new_mu = _catalog_mu(catalog, workload, profile, new_batch)
            delta = (new_mu - old_mu) if old_mu is not None and new_mu is not None else 0.0
            changes.append({"event_type": event_type, "timestamp": _timestamp(at), "timestamp_source": "observed",
                            "runtime_id": _first(event, "runtime_id", "runtimeId"), "workload": workload,
                            "mu": new_mu, "delta": delta,
                            "reason": None if old_mu is not None and new_mu is not None else "batch_capacity_missing",
                            "raw_event": event})

    timed = [item for item in changes if _parse_time(item.get("timestamp")) is not None]
    timed.sort(key=lambda item: (_parse_time(item["timestamp"]), str(item.get("runtime_id", "")), item["event_type"]))
    totals: dict[str, float] = defaultdict(float)
    output: list[dict[str, Any]] = []
    for item in timed:
        workload = item.get("workload")
        delta = _number(item.get("delta")) or 0.0
        if workload:
            totals[str(workload)] += delta
        item = dict(item)
        item["capacity_by_workload"] = dict(sorted(totals.items()))
        item["capacity_total"] = sum(totals.values())
        output.append(item)
    for item in changes:
        if item.get("timestamp") is None:
            item = dict(item)
            item.setdefault("capacity_by_workload", None)
            item.setdefault("capacity_total", None)
            output.append(item)
    return output


def build_capacity_timeline(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
    return derive_capacity_timeline(*args, **kwargs)


def capacity_intervals(timeline: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    events = [row for row in timeline if _parse_time(row.get("timestamp")) is not None]
    events.sort(key=lambda row: _parse_time(row["timestamp"]))
    intervals = []
    previous = None
    for row in events:
        intervals.append({"start_at": previous, "end_at": row.get("timestamp"),
                          "capacity_total": row.get("capacity_total"),
                          "capacity_by_workload": row.get("capacity_by_workload"),
                          "end_event": row.get("event_type")})
        previous = row.get("timestamp")
    if events:
        last = events[-1]
        intervals.append({"start_at": previous, "end_at": None,
                          "capacity_total": last.get("capacity_total"),
                          "capacity_by_workload": last.get("capacity_by_workload"),
                          "end_event": None})
    return intervals


def compute_capacity_results(
    catalog: Any,
    target_replicas: Iterable[Mapping[str, Any]],
    profile_samples: Iterable[Mapping[str, Any]] | None = None,
    demand: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Compute per-workload D, predicted capacity, measured capacity and ratios."""
    replicas = [dict(row) for row in target_replicas]
    samples = [dict(row) for row in (profile_samples or [])]
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for replica in replicas:
        workload = str(_first(replica, "workload", "model", default=""))
        mu, reason = _capacity_from_row(replica, catalog)
        replica["_catalog_mu"] = mu
        replica["_catalog_mu_reason"] = reason
        grouped[workload].append(replica)
    sample_by_runtime: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for sample in samples:
        sample_by_runtime[_replica_key(sample)].append(sample)
    workloads = sorted(set(grouped) | {str(key) for key in (demand or {})})
    result: list[dict[str, Any]] = []
    for workload in workloads:
        target = grouped.get(workload, [])
        predicted_values = [row["_catalog_mu"] for row in target if row["_catalog_mu"] is not None]
        predicted = sum(predicted_values) if len(predicted_values) == len(target) else None
        measured_values: list[float] = []
        sample_details = []
        complete = True
        for replica in target:
            key = _replica_key(replica)
            rows = sample_by_runtime.get(key, [])
            complete_rows = [
                row for row in rows
                if str(row.get("complete", True)).strip().lower() not in {"false", "0", "no"}
                and not _first(row, "error", "errors")
            ]
            count = sum((_number(_first(
                row, "sample_count", "samples", "logical_samples", "logicalSampleCount", "logicalSamples"
            )) or 0) for row in complete_rows)
            seconds = sum((_number(_first(
                row, "inference_seconds", "duration_seconds", "pure_inference_seconds", "duration",
                "runtimeInferenceSeconds",
            )) or 0) for row in complete_rows)
            measured = count / seconds if count > 0 and seconds > 0 else None
            if len(complete_rows) != len(rows):
                complete = False
            if measured is None:
                complete = False
            else:
                measured_values.append(measured)
            sample_details.append({"runtime_id": key, "complete_samples": count, "timing_seconds": seconds,
                                  "mu_measured": measured, "sample_count": len(complete_rows),
                                  "discarded_incomplete_samples": len(rows) - len(complete_rows),
                                  "reason": None if measured is not None and len(complete_rows) == len(rows)
                                  else "missing_or_incomplete_samples"})
        measured = sum(measured_values) if complete and target else (0.0 if not target else None)
        D = _number((demand or {}).get(workload))

        def ratio(numerator: float | None, denominator: float | None) -> float | None:
            return numerator / denominator if numerator is not None and denominator not in (None, 0) else None

        result.append({
            "workload": workload, "D": D, "C_pred": predicted, "C_measured": measured,
            "D_over_C_pred": ratio(D, predicted), "D_over_C_measured": ratio(D, measured),
            "C_pred_over_D": ratio(predicted, D), "C_measured_over_D": ratio(measured, D),
            "C_measured_over_C_pred": ratio(measured, predicted),
            "complete": complete and bool(target),
            "reason": None if complete and target else ("no_target_replicas" if not target else "missing_complete_samples"),
            "replicas": sample_details,
        })
    return result


def _validation_timestamp(validation: Any) -> str | None:
    if not isinstance(validation, Mapping):
        return None
    return _timestamp(_first(validation, "finishedAt", "validatedAt", "completedAt", "timestamp"))


def summarize_round(
    action_rows: Iterable[Mapping[str, Any]],
    final_validation: Mapping[str, Any] | None = None,
    transition_execution: Mapping[str, Any] | None = None,
    gpu_intervals: Iterable[Mapping[str, Any]] | None = None,
    capacity_timeline: Iterable[Mapping[str, Any]] | None = None,
    gpu_capacity: int | None = None,
    run_id: Any = None,
    live_round: Any = None,
    trace_round: Any = None,
) -> dict[str, Any]:
    rows = [dict(row) for row in action_rows]
    statuses = Counter(str(row.get("status") or "unknown") for row in rows)
    by_type = Counter(str(row.get("action_type") or "unknown") for row in rows)
    by_category = Counter(str(row.get("category") or "unknown") for row in rows)
    starts = [_parse_time(row.get("started_at")) for row in rows]
    ends = [_parse_time(row.get("finished_at")) for row in rows]
    starts = [item for item in starts if item]
    ends = [item for item in ends if item]
    transition = _mapping(transition_execution)
    if not transition and rows:
        transition = _mapping(rows[0].get("transition_execution"))
    transition_timestamps = _mapping(transition.get("timestamps"))
    if not starts:
        transition_start = _parse_time(_first(transition_timestamps, "executorStartedAt", "startedAt"))
        if transition_start:
            starts = [transition_start]
    validation_time = _validation_timestamp(final_validation)
    endpoint = _parse_time(validation_time)
    if endpoint is None and ends:
        endpoint = max(ends)
    if endpoint is None:
        transition_end = _parse_time(_first(transition_timestamps, "executorFinishedAt", "finishedAt"))
        if transition_end:
            endpoint = transition_end
    begin = min(starts) if starts else None
    makespan = (endpoint - begin).total_seconds() if begin and endpoint else None
    intervals = list(gpu_intervals or [])
    peak = max((_number(row.get("active_gpu_count")) or 0 for row in intervals), default=None)
    if peak is None:
        capacity_rows = list(capacity_timeline or [])
        peak = max((_number(row.get("active_gpu_count")) or 0 for row in capacity_rows), default=None)
    headroom = (gpu_capacity - peak) if gpu_capacity is not None and peak is not None else None
    return {
        "run_id": run_id, "live_round": live_round, "trace_round": trace_round,
        "finalValidation": dict(final_validation or {}),
        "makespan_seconds": _round(makespan),
        "makespan_reason": None if makespan is not None else "missing_action_start_or_completion_time",
        "action_counts": {"total": len(rows), "by_status": dict(sorted(statuses.items())),
                          "by_type": dict(sorted(by_type.items())), "by_category": dict(sorted(by_category.items()))},
        "peak_active_gpu_count": peak,
        "gpu_capacity": gpu_capacity,
        "headroom": headroom,
    }


def round_summary(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return summarize_round(*args, **kwargs)


def collect_replica_lifecycle(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
    return build_replica_lifecycle_rows(*args, **kwargs)


def collect_gpu_events(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
    return build_gpu_events(*args, **kwargs)


def compute_gpu_count_intervals(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
    return gpu_count_intervals(*args, **kwargs)


def compute_workload_capacity(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
    return compute_capacity_results(*args, **kwargs)


def snapshot(raw: Any, *, run_id: Any = None, live_round: Any = None, phase: Any = None,
             observed_at: Any = None, normalized_diff: Any = None) -> dict[str, Any]:
    return {"run_id": run_id, "live_round": live_round, "phase": phase,
            "observed_at": _timestamp(observed_at), "raw": raw,
            "normalized_diff": normalized_diff}


def build_snapshot(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return snapshot(*args, **kwargs)


def write_json(path: str | Path, value: Any) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2, sort_keys=True, ensure_ascii=True)
        handle.write("\n")
    return target


def write_jsonl(path: str | Path, rows: Iterable[Any]) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, ensure_ascii=True) + "\n")
    return target


def _csv_value(value: Any) -> Any:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":"))
    return value


def write_csv(path: str | Path, rows: Iterable[Mapping[str, Any]], fieldnames: Sequence[str] | None = None) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    materialized = [dict(row) for row in rows]
    if fieldnames is None:
        raise ValueError("write_csv requires explicit fieldnames for a stable schema")
    names = list(fieldnames)
    if not names or len(names) != len(set(names)):
        raise ValueError("write_csv fieldnames must be non-empty and unique")
    allowed = set(names)
    unknown = sorted({str(key) for row in materialized for key in row if key not in allowed})
    if unknown:
        raise ValueError("write_csv received unknown fields: " + ", ".join(unknown))
    with target.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=names, extrasaction="raise")
        writer.writeheader()
        for row in materialized:
            writer.writerow({key: _csv_value(row.get(key)) for key in names})
    return target


def write_snapshot(path: str | Path, value: Any) -> Path:
    return write_json(path, value)


def _schema_rows(value: Any) -> list[Mapping[str, Any]]:
    if isinstance(value, (str, Path)):
        path = Path(value)
        if path.suffix == ".jsonl":
            with path.open(encoding="utf-8") as handle:
                return [json.loads(line) for line in handle if line.strip()]
        if path.suffix == ".csv":
            with path.open(newline="", encoding="utf-8") as handle:
                return list(csv.DictReader(handle))
        if path.suffix == ".md":
            return [{"__text__": path.read_text(encoding="utf-8")}]
        with path.open(encoding="utf-8") as handle:
            loaded = json.load(handle)
            return _schema_rows(loaded)
    if isinstance(value, Mapping):
        return [value]
    return [row for row in (value or []) if isinstance(row, Mapping)]


def validate_required_outputs(outputs: Mapping[str, Any] | str | Path,
                              schemas: Mapping[str, Sequence[str]] | None = None,
                              *, expected_rounds: int = 12,
                              allow_partial: bool = False) -> list[str]:
    """Return schema errors, including row and formal round coverage errors.

    ``allow_partial`` is intended for a failed execution or a preflight-only
    directory.  It permits missing/empty artifacts and incomplete rounds, but
    never permits an existing row to omit a required field.
    """
    if expected_rounds < 1:
        raise ValueError("expected_rounds must be positive")
    schema = schemas or REQUIRED_OUTPUT_SCHEMAS
    errors: list[str] = []
    if isinstance(outputs, (str, Path)):
        root = Path(outputs)
        values = {name: root / name for name in schema}
        if not allow_partial:
            for marker in (root / "preflight.json", root / "failure.json", root / "results.json"):
                if not marker.exists():
                    continue
                try:
                    marker_value = json.loads(marker.read_text(encoding="utf-8"))
                except (OSError, ValueError, json.JSONDecodeError):
                    continue
                if marker.name == "preflight.json" and marker_value.get("ok") is False:
                    allow_partial = True
                if marker.name in {"failure.json", "results.json"} and (
                    marker.name == "failure.json" or marker_value.get("ok") is False
                ):
                    allow_partial = True
    else:
        values = outputs
    round_values: dict[str, list[Mapping[str, Any]]] = {}
    for name, fields in schema.items():
        if name not in values:
            errors.append(name + ": missing output")
            continue
        value = values[name]
        if value is None:
            errors.append(name + ": null output")
            continue
        try:
            rows = _schema_rows(value)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            errors.append(name + ": unreadable: " + str(exc))
            continue
        if not rows:
            if allow_partial:
                continue
            errors.append(name + ": no rows")
            continue
        for index, row in enumerate(rows, 1):
            missing = [field for field in fields if field not in row]
            if missing:
                errors.append(name + f": row {index} missing fields " + ", ".join(missing))
        if name in ROUND_SCOPED_OUTPUTS or name.endswith((".csv", ".jsonl")) or name == "round_summary.json":
            round_values[name] = rows
    if not allow_partial:
        expected = set(range(1, expected_rounds + 1))
        for name, rows in round_values.items():
            round_rows = list(rows)
            for row in rows:
                nested = row.get("rounds")
                if isinstance(nested, list):
                    round_rows.extend(item for item in nested if isinstance(item, Mapping))
            if not any("live_round" in row or "round" in row for row in round_rows):
                continue
            observed = set()
            for row in round_rows:
                value = row.get("live_round", row.get("round"))
                try:
                    observed.add(int(value))
                except (TypeError, ValueError):
                    pass
            missing_rounds = sorted(expected - observed)
            if missing_rounds:
                errors.append(name + ": missing rounds " + ", ".join(map(str, missing_rounds)))
    return errors


def validate_required_output_schema(outputs: Mapping[str, Any] | str | Path,
                                    schemas: Mapping[str, Sequence[str]] | None = None) -> list[str]:
    return validate_required_outputs(outputs, schemas)


def assert_required_outputs(outputs: Mapping[str, Any] | str | Path,
                            schemas: Mapping[str, Sequence[str]] | None = None) -> None:
    errors = validate_required_outputs(outputs, schemas)
    if errors:
        raise ValueError("required output schema validation failed: " + "; ".join(errors))


__all__ = [
    "ACTION_FIELDS", "LIFECYCLE_EVENTS", "REQUIRED_OUTPUT_SCHEMAS",
    "flatten_mig_action_plan", "flatten_action_plan", "collect_action_rows",
    "build_replica_lifecycle_rows", "build_gpu_events", "gpu_count_intervals",
    "collect_replica_lifecycle", "collect_gpu_events", "compute_gpu_count_intervals",
    "derive_capacity_timeline", "build_capacity_timeline", "capacity_intervals",
    "compute_capacity_results", "compute_workload_capacity", "summarize_round", "round_summary",
    "snapshot", "build_snapshot", "write_json", "write_jsonl", "write_csv",
    "write_snapshot", "validate_required_outputs", "validate_required_output_schema",
    "assert_required_outputs",
]
