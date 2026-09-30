"""Safe v2 orchestrator for the frozen three-GPU live experiment.

The runner is deliberately conservative.  It observes a cluster before doing
anything, keeps plans behind the manual phase gate until an audit passes, and
has no delete, repair, reset, or retry operation.  The default command only
performs preflight.  ``--execute`` is required for every cluster mutation.

Only the Python standard library is required.  The sibling traffic, profile,
and collector modules are loaded dynamically so this file remains usable while
those modules are being evolved independently.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import json
import os
import re
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence


ROOT = Path(__file__).resolve().parent
DEFAULT_NAMESPACE = "or-sim-exp"
DEFAULT_ROUTER_URL = os.environ.get("OR_SIM_ROUTER_URL", "")
MAX_PHYSICAL_GPUS = 3
TRAFFIC_SEED = 71
ROUND_COUNT = 12
GPU_WORKER_NODES = ("ampere", "rtx1-worker")
TERMINAL_PHASES = frozenset({"Executed", "Succeeded", "Completed"})
FAILED_PHASES = frozenset({"Failed", "Blocked", "Error", "Rejected"})
FORBIDDEN_ACTION_WORDS = frozenset({"blocked", "defer", "deferred", "workload_change"})
STATE_EVENTS = {
    "PREFLIGHT": frozenset({"snapshot_created", "failed"}),
    "SNAPSHOT_CREATED": frozenset({"plan_planned", "failed"}),
    "PLAN_PLANNED": frozenset({"audit_passed", "failed"}),
    "AUDITED": frozenset({"transition_sender_started", "approved", "failed"}),
    "TRANSITION_SENDER_STARTED": frozenset({"approved", "failed"}),
    "APPROVED": frozenset({"terminal", "failed"}),
    "TERMINAL": frozenset({"final_validated", "failed"}),
    "FINAL_VALIDATED": frozenset({"target_steady", "failed"}),
    "TARGET_STEADY": frozenset({"profiled", "failed"}),
    "PROFILED": frozenset({"round_saved", "failed"}),
    "ROUND_SAVED": frozenset({"next_round", "finished", "failed"}),
}

WORKLOAD_KEYS = (
    "resnet50_image",
    "vgg16_image",
    "vit_base_image",
    "gpt2_p64_o64",
    "gpt2_p512_o512",
    "llama_p1024_o128",
    "llama_p2048_o64",
)

# This is the immutable request/SLO contract used in every ArrivalSnapshot and
# in the profile descriptors.  It mirrors the seven planner workload examples.
WORKLOAD_CONTRACT: dict[str, dict[str, Any]] = {
    "resnet50_image": {
        "family": "vision", "model": "resnet50", "requestClass": "image batches",
        "imageSize": 224, "slo": {"e2eMs": 100.0, "latencyMs": 100.0},
    },
    "vgg16_image": {
        "family": "vision", "model": "vgg16", "requestClass": "image batches",
        "imageSize": 224, "slo": {"e2eMs": 100.0, "latencyMs": 100.0},
    },
    "vit_base_image": {
        "family": "vision", "model": "vit_base", "requestClass": "image batches",
        "imageSize": 224, "slo": {"e2eMs": 300.0, "latencyMs": 300.0},
    },
    "gpt2_p64_o64": {
        "family": "llm", "model": "gpt2", "requestClass": "p64/o64",
        "promptLen": 64, "outputTokens": 64, "slo": {"ttftMs": 50.0, "tpotMs": 20.0},
    },
    "gpt2_p512_o512": {
        "family": "llm", "model": "gpt2", "requestClass": "p512/o512",
        "promptLen": 512, "outputTokens": 512, "slo": {"ttftMs": 100.0, "tpotMs": 20.0},
    },
    "llama_p1024_o128": {
        "family": "llm", "model": "llama", "requestClass": "p1024/o128",
        "promptLen": 1024, "outputTokens": 128, "slo": {"ttftMs": 180.0, "tpotMs": 35.0},
    },
    "llama_p2048_o64": {
        "family": "llm", "model": "llama", "requestClass": "p2048/o64",
        "promptLen": 2048, "outputTokens": 64, "slo": {"ttftMs": 250.0, "tpotMs": 35.0},
    },
}


def _load_sibling(filename: str, module_name: str) -> Any:
    path = ROOT / filename
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load sibling module {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault(module_name, module)
    spec.loader.exec_module(module)
    return module


traffic = _load_sibling("live_traffic_20260926.py", "live_traffic_20260926")
profile = _load_sibling("live_profile_20260926.py", "live_profile_20260926")
collectors = _load_sibling("live_collectors_20260926.py", "live_collectors_20260926")

LIFECYCLE_FIELDS = (
    "run_id", "live_round", "trace_round", "phase", "workload", "family",
    "runtime_id", "pod_uid", "node", "gpu_uuid", "mig_uuid", "slot",
    "physical_profile", "batch", "round", "action_id", "attempt", "raw_record",
    *(name for event in collectors.LIFECYCLE_EVENTS for name in (event, event + "_reason")),
    "missing_event_reasons", "create_to_route_seconds", "stop_to_pod_gone_seconds",
)
PROFILE_SAMPLE_FIELDS = (
    "run_id", "live_round", "trace_round", "phase", "workload", "runtime_id",
    "replicaId", "endpoint", "sequence", "startedAtSeconds", "endedAtSeconds",
    "logicalSamples", "runtimeInferenceSeconds", "runtimeInferenceMs", "batchSize",
    "profile", "complete",
)
CAPACITY_RESULT_FIELDS = (
    "run_id", "live_round", "trace_round", "phase", "workload", "D", "C_pred",
    "C_measured", "D_over_C_pred", "D_over_C_measured", "C_pred_over_D",
    "C_measured_over_D", "C_measured_over_C_pred", "sample_count", "complete", "unit",
    "reason", "replicas",
)
CAPACITY_TIMELINE_FIELDS = (
    "run_id", "live_round", "trace_round", "phase", "event_type", "timestamp",
    "timestamp_source", "runtime_id", "workload", "physical_profile", "batch",
    "mu", "delta", "capacity", "commitment", "capacity_ratio", "reason", "uncertain",
)
ACTION_RUNTIME_FIELDS = (
    "run_id", "live_round", "trace_round", "phase", "workload", "action_type",
    "count", "success_count", "failure_count", "duration_min_seconds",
    "duration_median_seconds", "duration_max_seconds",
)
ROUND_SUMMARY_FIELDS = (
    "run_id", "live_round", "trace_round", "phase", "reached_target",
    "finalValidationOk", "makespan_seconds", "planner_makespan_seconds",
    "action_count", "action_counts", "strategy_counts", "failure_count",
    "peak_active_gpu_count", "headroom", "capacity_uncertain_events",
)
TRANSITION_REQUEST_FIELDS = (
    "run_id", "live_round", "trace_round", "phase", "workload", "scheduled",
    "successes", "failures", "timeouts", "pending",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _json(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def _first(mapping: Any, *keys: str, default: Any = None) -> Any:
    if not isinstance(mapping, Mapping):
        return default
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return default


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _items(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def verify_sha256sums(root: Path, sums_path: Path | None = None) -> dict[str, str]:
    """Verify every entry in SHA256SUMS and return the verified digest map."""

    sums_path = sums_path or root / "SHA256SUMS"
    verified: dict[str, str] = {}
    for line_number, line in enumerate(sums_path.read_text(encoding="utf-8").splitlines(), 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) != 2 or not re.fullmatch(r"[0-9a-fA-F]{64}", parts[0]):
            raise ValueError(f"invalid SHA256SUMS line {line_number}")
        name = parts[1].lstrip("*")
        candidate = (root / name).resolve()
        if candidate.parent != root.resolve() or not candidate.is_file():
            raise ValueError(f"SHA256SUMS references unavailable or unsafe file: {name}")
        digest = hashlib.sha256(candidate.read_bytes()).hexdigest()
        if digest.lower() != parts[0].lower():
            raise ValueError(f"SHA256 mismatch for {name}: expected {parts[0]}, got {digest}")
        verified[name] = digest
    if not verified:
        raise ValueError("SHA256SUMS contains no entries")
    return verified


def load_frozen_inputs(root: Path = ROOT, catalog_file: str = "catalog.csv") -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, str]]:
    """Verify and load the frozen demand and a catalog without rewriting either.

    ``catalog_file`` (relative to ``root``) selects the ledger catalog; a
    non-default file is hashed into the returned hashes.  The frozen files in
    SHA256SUMS are verified either way.
    """

    hashes = verify_sha256sums(root)
    demand_path = root / "selected_demand.csv"
    catalog_path = root / catalog_file
    if catalog_file != "catalog.csv":
        hashes = {**dict(hashes), catalog_file: hashlib.sha256(catalog_path.read_bytes()).hexdigest()}
    with demand_path.open(encoding="utf-8", newline="") as stream:
        demand = list(csv.DictReader(stream))
    with catalog_path.open(encoding="utf-8", newline="") as stream:
        catalog = list(csv.DictReader(stream))
    missing = [key for key in WORKLOAD_KEYS if key not in (demand[0] if demand else {})]
    if missing or len(demand) != ROUND_COUNT:
        raise ValueError(f"selected_demand.csv must have 12 rows and seven workloads; missing={missing}")
    if len(catalog) != 84:
        raise ValueError(f"catalog.csv must contain exactly 84 rows, got {len(catalog)}")
    if any(not row.get("workload") or not row.get("profile") or not row.get("batch") for row in catalog):
        raise ValueError("catalog.csv contains an incomplete profile option")
    return demand, catalog, hashes


def validate_workload_contract() -> None:
    if tuple(WORKLOAD_CONTRACT) != WORKLOAD_KEYS:
        raise ValueError("workload contract key order changed")
    for workload, spec in WORKLOAD_CONTRACT.items():
        if spec["family"] == "vision":
            if spec["imageSize"] != 224 or "promptLen" in spec:
                raise ValueError(f"invalid vision request shape for {workload}")
        elif spec.get("promptLen", 0) <= 0 or spec.get("outputTokens", 0) <= 0:
            raise ValueError(f"invalid LLM request shape for {workload}")


def demand_rates(row: Mapping[str, Any]) -> dict[str, float]:
    missing = [key for key in WORKLOAD_KEYS if key not in row]
    if missing:
        raise ValueError("missing workload rates: " + ", ".join(missing))
    return {key: _number(row[key]) for key in WORKLOAD_KEYS}


def _physical_ids(registry: Mapping[str, Any]) -> set[str]:
    status = _mapping(registry.get("status"))
    bindings = _mapping(status.get("bindings"))
    canonical = _mapping(_mapping(status.get("currentAllocation")).get("gpus"))
    # currentAllocation.gpus may briefly contain only active logical GPUs,
    # while bindings remains the inventory of every observed physical GPU.
    # Allocation actions are allowed to reference an available inventory GPU.
    return {str(key) for key in canonical} | {str(key) for key in bindings}


def observed_a100_ids(registry: Mapping[str, Any]) -> set[str]:
    status = _mapping(registry.get("status"))
    bindings = _mapping(status.get("bindings"))
    return {
        str(physical_id)
        for physical_id, raw in bindings.items()
        if "a100" in str(_first(_mapping(raw), "product", "gpuProduct", default="")).lower()
    }


def allocated_physical_ids(registry: Mapping[str, Any]) -> set[str]:
    status = _mapping(registry.get("status"))
    result: set[str] = set()
    for physical_id, raw in _mapping(status.get("bindings")).items():
        item = _mapping(raw)
        if _items(item.get("runtimeBindings")):
            result.add(str(physical_id))
    for physical_id, raw in _mapping(_mapping(status.get("currentAllocation")).get("gpus")).items():
        if _items(_mapping(raw).get("runtimeBindings")) or _items(_mapping(raw).get("logicalGpus")):
            result.add(str(physical_id))
    return result


def registry_health_errors(registry: Mapping[str, Any]) -> list[str]:
    status = _mapping(registry.get("status"))
    health = _mapping(status.get("health"))
    queue = _mapping(status.get("queueCounts"))
    errors: list[str] = []
    if health.get("stable") is not True:
        errors.append("registry health.stable is not true")
    if health.get("repairRequired") is True:
        errors.append("registry requests repair")
    if _items(health.get("requiredActions")):
        errors.append("registry has requiredActions")
    if _int(queue.get("transitioning")) != 0:
        errors.append("registry has transitioning GPUs")
    return errors


def registry_has_empty_allocation(registry: Mapping[str, Any]) -> bool:
    status = _mapping(registry.get("status"))
    current = _mapping(status.get("currentAllocation"))
    if _items(current.get("logicalGpus")):
        return False
    if any(_items(_mapping(raw).get("runtimeBindings")) for raw in _mapping(status.get("bindings")).values()):
        return False
    # A pre-existing empty MIG layout is still an empty allocation.  Runtime
    # bindings and logical allocation are the ownership signals; observing
    # configured MIG devices alone must not cause the runner to repair them.
    return True


def active_gpu_count(registry: Mapping[str, Any]) -> int:
    status = _mapping(registry.get("status"))
    queue = _mapping(status.get("queueCounts"))
    if "active" in queue:
        return _int(queue.get("active"))
    current = _mapping(status.get("currentAllocation"))
    logical = _items(current.get("logicalGpus"))
    physical = {
        str(_first(_mapping(item), "physicalGpuId", "physicalId", "gpu", default=""))
        for item in logical
    }
    return len(physical - {""})


def routes_are_empty(routes: Any) -> bool:
    if isinstance(routes, Mapping):
        routes = routes.get("routes", [])
    return len(_items(routes)) == 0


def transition_state(state: str, event: str) -> str:
    """Advance the executable state machine or raise before unsafe progress."""

    state = state.upper()
    event = event.lower()
    if event == "failed":
        return "FAILED"
    if event not in STATE_EVENTS.get(state, frozenset()):
        raise ValueError(f"invalid state transition {state} --{event}-->")
    if event == "snapshot_created":
        return "SNAPSHOT_CREATED"
    if event == "plan_planned":
        return "PLAN_PLANNED"
    if event == "audit_passed":
        return "AUDITED"
    if event == "transition_sender_started":
        return "TRANSITION_SENDER_STARTED"
    if event == "approved":
        return "APPROVED"
    if event == "terminal":
        return "TERMINAL"
    if event == "final_validated":
        return "FINAL_VALIDATED"
    if event == "target_steady":
        return "TARGET_STEADY"
    if event == "profiled":
        return "PROFILED"
    if event == "round_saved":
        return "ROUND_SAVED"
    if event in {"next_round", "finished"}:
        return "PREFLIGHT"
    raise ValueError(f"unhandled state event {event}")


def audit_preflight(
    registry: Mapping[str, Any],
    routes: Any,
    controllers: Mapping[str, Any],
    *,
    require_empty: bool = True,
    allow_inflight: bool = False,
) -> list[str]:
    """Audit only observations; this function never mutates cluster state.

    ``allow_inflight`` skips only the drained-routes check, for E1 where
    traffic deliberately continues across rounds.
    """

    errors = registry_health_errors(registry)
    a100s = observed_a100_ids(registry)
    if len(a100s) != 3:
        errors.append(f"expected exactly 3 observed A100s, got {len(a100s)}")
    if require_empty and not registry_has_empty_allocation(registry):
        errors.append("R1 registry allocation is not empty")
    if require_empty and not routes_are_empty(routes):
        errors.append("R1 router routes are not empty")
    if not require_empty and not allow_inflight:
        route_rows = _items(routes.get("routes", [])) if isinstance(routes, Mapping) else _items(routes)
        busy = [
            str(_first(_mapping(route), "runtimeId", "runtime_id", default="unknown"))
            for route in route_rows
            if _int(_first(_mapping(route), "endpointInflight", "inflight", default=0)) > 0
            or _int(_first(_mapping(route), "endpointQueued", "queued", default=0)) > 0
        ]
        if busy:
            errors.append("source check found non-drained routes: " + ", ".join(sorted(busy)))
    for name, raw in controllers.items():
        status = _mapping(_mapping(raw).get("status"))
        desired = _int(status.get("replicas"), 1)
        available = _int(status.get("availableReplicas"))
        if available < desired:
            errors.append(f"controller {name} is not healthy ({available}/{desired} available)")
    return errors


def _plan_actions(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    spec = _mapping(plan.get("spec"))
    dag = _mapping(spec.get("actionDag"))
    raw = dag.get("nodes", spec.get("abstractActions", []))
    out: list[dict[str, Any]] = []
    for item in _items(raw):
        if isinstance(item, Mapping):
            action = dict(item)
            nested = _mapping(action.get("action"))
            for key, value in nested.items():
                action.setdefault(key, value)
            out.append(action)
    return out


def _action_id(action: Mapping[str, Any]) -> str:
    return str(_first(action, "id", "actionId", "action_id", "name", default=""))


def _action_type(action: Mapping[str, Any]) -> str:
    return str(_first(action, "type", "actionType", "action_type", default="")).lower()


def _dependency_list(action: Mapping[str, Any]) -> list[str]:
    raw = _first(action, "dependsOn", "depends_on", "dependencies", default=[])
    return [str(value.get("id")) if isinstance(value, Mapping) else str(value) for value in _items(raw)]


def _recursive_strings(value: Any) -> Iterable[str]:
    if isinstance(value, Mapping):
        for key, item in value.items():
            yield str(key)
            yield from _recursive_strings(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _recursive_strings(item)
    elif isinstance(value, str):
        yield value


def _planner_status(plan: Mapping[str, Any]) -> str | None:
    candidates: list[Any] = []
    spec = _mapping(plan.get("spec"))
    planner_metadata = _mapping(spec.get("plannerMetadata"))
    planning_trace = _mapping(planner_metadata.get("planningTrace"))
    candidates.extend([
        _mapping(planning_trace.get("milp")),
        planner_metadata,
        _mapping(spec.get("optimizerMetadata")),
        _mapping(plan.get("metadata")),
    ])
    keys = ("status", "solverStatus", "terminationCondition", "termination_status", "plannerStatus", "optimizationStatus")
    for mapping in candidates:
        for key in keys:
            value = mapping.get(key)
            if isinstance(value, str):
                return value.upper()
    return None


def _planner_makespan_seconds(plan: Mapping[str, Any]) -> float | None:
    spec = _mapping(plan.get("spec"))
    metadata = _mapping(spec.get("plannerMetadata"))
    metrics = _mapping(metadata.get("metrics"))
    summary = _mapping(spec.get("summary"))
    value = _first(
        metrics,
        "plannerMakespanSec",
        "planner_makespan_sec",
        default=_first(summary, "plannerMakespanSec", "planner_makespan_sec"),
    )
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _planned_stage3_variant(plan: Mapping[str, Any]) -> str | None:
    spec = _mapping(plan.get("spec"))
    metadata = _mapping(spec.get("plannerMetadata"))
    transition = _mapping(_mapping(metadata.get("planningTrace")).get("transition"))
    value = _first(transition, "stage3Variant", "stage3_variant", default=None)
    return str(value).strip().lower() if value is not None else None


def _physical_ids_in_plan(plan: Mapping[str, Any]) -> set[str]:
    names = {"physicalGpuId", "physical_gpu_id", "physicalID", "physicalId"}
    found: set[str] = set()
    def walk(value: Any) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                if key in names and isinstance(item, str) and item:
                    found.add(item)
                walk(item)
        elif isinstance(value, (list, tuple)):
            for item in value:
                walk(item)
    walk(plan.get("spec", plan))
    return found


def _topological_actions(actions: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    by_id = {_action_id(action): action for action in actions}
    pending = set(by_id)
    result: list[Mapping[str, Any]] = []
    while pending:
        ready = sorted(action_id for action_id in pending if set(_dependency_list(by_id[action_id])).issubset(set(by_id) - pending))
        if not ready:
            raise ValueError("action DAG contains a cycle or unresolved dependency")
        result.extend(by_id[action_id] for action_id in ready)
        pending.difference_update(ready)
    return result


def conservative_peak_gpu_count(plan: Mapping[str, Any], source_gpu_count: int | None = None) -> int:
    spec = _mapping(plan.get("spec"))
    summary = _mapping(spec.get("summary"))
    current = _int(source_gpu_count, _int(_first(summary, "sourceGpuCount", default=spec.get("sourceGpuCount"))))
    if current < 0:
        current = 0
    peak = current
    for action in _topological_actions(_plan_actions(plan)):
        action_type = _action_type(action)
        if action_type in {"allocate_gpu", "create_gpu"}:
            current += 1
        elif action_type in {"return_gpu", "remove_gpu", "release_gpu"}:
            current = max(0, current - 1)
        peak = max(peak, current)
    target = _int(_first(summary, "targetGpuCount", default=spec.get("targetGpuCount")))
    return max(peak, target)


def audit_plan(
    plan: Mapping[str, Any],
    registry: Mapping[str, Any],
    *,
    source_gpu_count: int | None = None,
    max_gpus: int = MAX_PHYSICAL_GPUS,
    require_nonzero_target: bool = False,
    expected_stage3_variant: str | None = None,
) -> dict[str, Any]:
    """Return a strict audit report; only a complete ``ok`` is executable."""

    errors: list[str] = []
    status = _planner_status(plan)
    if status != "OPTIMAL":
        errors.append(f"planner status must be OPTIMAL, got {status or 'missing'}")
    planned_variant = _planned_stage3_variant(plan)
    if expected_stage3_variant is not None and planned_variant != expected_stage3_variant:
        errors.append(
            "planner Stage 3 variant mismatch: "
            f"requested {expected_stage3_variant}, got {planned_variant or 'missing'}"
        )
    actions = _plan_actions(plan)
    ids = [_action_id(action) for action in actions]
    if len(ids) != len(set(ids)) or any(not action_id for action_id in ids):
        errors.append("action DAG IDs must be present and unique")
    known = set(ids)
    for action in actions:
        missing = [dep for dep in _dependency_list(action) if dep not in known]
        if missing:
            errors.append(f"action {_action_id(action)} has unresolved dependencies {missing}")
        action_type = _action_type(action)
        if any(word in action_type for word in FORBIDDEN_ACTION_WORDS):
            errors.append(f"forbidden executable action type: {action_type}")
        action_status = str(_first(action, "status", "phase", default="")).lower()
        if any(word in action_status for word in FORBIDDEN_ACTION_WORDS):
            errors.append(f"action {_action_id(action)} has forbidden executable status: {action_status}")
    try:
        _topological_actions(actions)
    except ValueError as exc:
        errors.append(str(exc))
    physical_ids = _physical_ids(registry)
    plan_physical_ids = _physical_ids_in_plan(plan)
    unknown = sorted(plan_physical_ids - physical_ids)
    if unknown:
        errors.append("plan contains physical IDs absent from registry: " + ", ".join(unknown))
    spec = _mapping(plan.get("spec"))
    target_gpu_count = _int(spec.get("targetGpuCount"))
    if require_nonzero_target and (target_gpu_count <= 0 or not actions):
        errors.append("nonzero initial demand produced an empty target/action DAG")
    if target_gpu_count > max_gpus:
        errors.append(f"targetGpuCount {target_gpu_count} exceeds {max_gpus}")
    try:
        peak = conservative_peak_gpu_count(plan, source_gpu_count)
    except ValueError as exc:
        peak = max_gpus + 1
        errors.append(str(exc))
    if peak > max_gpus:
        errors.append(f"conservative allocation/return peak {peak} exceeds {max_gpus}")
    return {
        "ok": not errors,
        "plannerStatus": status,
        "stage3Variant": planned_variant,
        "actionCount": len(actions),
        "physicalIds": sorted(plan_physical_ids),
        "targetGpuCount": target_gpu_count,
        "conservativePeakGpuCount": peak,
        "errors": errors,
    }


def independent_final_validation(plan: Mapping[str, Any], registry: Mapping[str, Any], routes: Any) -> dict[str, Any]:
    """Validate target state from fresh observations, independently of plan phase."""

    errors = registry_health_errors(registry)
    spec = _mapping(plan.get("spec"))
    target_ids = _physical_ids_in_plan(_mapping(spec.get("targetAllocationPlan")))
    actual_ids = _physical_ids(registry)
    if target_ids and not target_ids.issubset(actual_ids):
        errors.append("target physical IDs are not present in final registry")
    status = _mapping(plan.get("status"))
    execution = _mapping(status.get("transitionExecution"))
    metrics = _mapping(execution.get("metrics"))
    final = _first(metrics, "finalValidation", "final_validation", default=None)
    if not isinstance(final, Mapping):
        errors.append("executor finalValidation evidence is missing")
    elif final.get("ok") is not True:
        errors.append("executor finalValidation did not report ok=true")
    if isinstance(final, Mapping) and final.get("skipped") is True:
        errors.append("executor finalValidation was skipped")
    return {"ok": not errors, "errors": errors, "targetPhysicalIds": sorted(target_ids), "observedPhysicalIds": sorted(actual_ids), "routesObserved": len(_items(routes.get("routes", []) if isinstance(routes, Mapping) else routes))}


def wait_independent_final_validation(
    kube: Any,
    router: Any,
    plan: Mapping[str, Any],
    *,
    timeout: float = 60.0,
    poll_seconds: float = 2.0,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    deadline = time.monotonic() + timeout
    while True:
        registry = kube.get_json("physicalgpuregistries", "default")
        routes = router.get_json("/routes")
        validation = independent_final_validation(plan, registry, routes)
        if validation["ok"] or time.monotonic() >= deadline:
            return validation, registry, routes
        time.sleep(poll_seconds)


def source_state_signature(registry: Mapping[str, Any], routes: Mapping[str, Any]) -> dict[str, Any]:
    bindings = _mapping(_mapping(registry.get("status")).get("bindings"))
    gpu_rows: list[dict[str, Any]] = []
    for physical_id, raw in sorted(bindings.items()):
        binding = _mapping(raw)
        mig_devices = []
        for device in _items(binding.get("migDevices")):
            item = _mapping(device)
            mig_devices.append({
                "uuid": _first(item, "uuid", "migUuid", "mig_uuid"),
                "profile": _first(item, "profile", "migProfile"),
                "slot": _first(item, "slot", "slotResource"),
                "start": _first(item, "start", "startSlice"),
                "end": _first(item, "end", "endSlice"),
            })
        runtime_bindings = []
        for runtime in _items(binding.get("runtimeBindings")):
            item = _mapping(runtime)
            runtime_bindings.append({
                "runtime_id": _first(item, "runtimeId", "runtime_id", "id"),
                "workload": _first(item, "workload", "model"),
                "profile": _first(item, "profile", "physicalProfile"),
                "batch": _first(item, "batch", "batchSize"),
                "slot": _first(item, "slot", "slotResource"),
                "mig_uuid": _first(item, "migUuid", "mig_uuid"),
            })
        gpu_rows.append({
            "physical_gpu_id": physical_id,
            "mig_devices": sorted(mig_devices, key=lambda row: json.dumps(row, sort_keys=True, default=str)),
            "runtime_bindings": sorted(runtime_bindings, key=lambda row: json.dumps(row, sort_keys=True, default=str)),
        })
    route_rows = []
    for route in _items(routes.get("routes")):
        item = _mapping(route)
        route_rows.append({
            "runtime_id": _first(item, "runtimeId", "runtime_id", "id"),
            "workload": _first(item, "workload", "model"),
            "batch": _first(item, "batch", "batchSize"),
            "active": item.get("active"),
            "accepting_new": _first(item, "acceptingNew", "accepting_new"),
            "draining": item.get("draining"),
            "endpoint": item.get("endpoint"),
        })
    return {
        "gpus": gpu_rows,
        "routes": sorted(route_rows, key=lambda row: json.dumps(row, sort_keys=True, default=str)),
    }


def build_arrival_snapshot(
    name: str,
    live_round: int,
    source_rates: Mapping[str, float],
    target_rates: Mapping[str, float],
    *,
    namespace: str = DEFAULT_NAMESPACE,
    placement_nodes: Sequence[str] = (),
    stage3_variant: str = "slicewise",
    capacity_headroom: float | None = None,
    conservative_3g_mu: bool = False,
) -> dict[str, Any]:
    validate_workload_contract()
    snapshot = {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "ArrivalSnapshot",
        "metadata": {"name": name, "namespace": namespace, "labels": {"experiment.or-sim.io/name": "three-gpu-live-20260926-v2"}},
        "spec": {
            "source": "three-gpu-live-20260926-v2",
            "mode": "target",
            "planner": "ours",
            "planningMethod": "ours",
            "stage3Variant": stage3_variant,
            "forceReplan": True,
            "phaseGate": "manual",
            "epoch": f"three-gpu-live-20260926-r{live_round:02d}",
            "triggerReason": "frozen-selected-demand",
            "windowSeconds": 60,
            "unit": "requestsPerSecond",
            "observedAt": utc_now(),
            "placement": {"nodes": list(placement_nodes)},
            "scenarioPath": "mock/scenarios/real8gpu.yaml",
            "profileCatalogRef": "default",
            "calibrationOverlayRef": "physicalgpuregistry/default",
            "currentAllocationRef": "physicalgpuregistry/default",
            "sourceArrival": dict(source_rates),
            "currentDemand": dict(source_rates),
            "targetArrival": dict(target_rates),
            "targetDemand": dict(target_rates),
            "transitionDemandPolicy": "min",
            "registeredSLOMs": {key: max(spec["slo"].values()) for key, spec in WORKLOAD_CONTRACT.items()},
            "slo": {key: dict(spec["slo"]) for key, spec in WORKLOAD_CONTRACT.items()},
            "notes": [
                "v2 manual gate; no automatic repair, delete, reset, or workload substitution",
                f"Stage 3 variant: {stage3_variant}",
            ],
        },
    }
    # Planner-side knobs: provision for (1 + h) x demand (best-effort under the
    # GPU budget) and use min(mu_3g, mu_4g) for 3g options in Stage 1.
    if capacity_headroom is not None:
        snapshot["spec"]["capacityHeadroom"] = float(capacity_headroom)
    if conservative_3g_mu:
        snapshot["spec"]["conservative3gMu"] = True
    return snapshot


def planning_knobs(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "capacity_headroom": getattr(args, "capacity_headroom", None),
        "conservative_3g_mu": bool(getattr(args, "conservative_3g_mu", False)),
    }


def snapshot_name_for_run(run_id: str, live_round: int) -> str:
    run_slug = re.sub(r"[^a-z0-9-]+", "", run_id.lower())
    return f"live-v2-{run_slug}-r{live_round:02d}"


def _json_output(path: Path, value: Any) -> None:
    writer = getattr(collectors, "write_json", None)
    if callable(writer):
        writer(path, value)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _jsonl_output(path: Path, rows: Iterable[Any]) -> None:
    writer = getattr(collectors, "write_jsonl", None)
    if callable(writer):
        writer(path, rows)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, sort_keys=True, default=str) + "\n")


def _csv_output(path: Path, rows: Iterable[Mapping[str, Any]], fieldnames: Sequence[str] | None = None) -> None:
    rows = list(rows)
    if fieldnames is None:
        raise ValueError(f"CSV schema is required for {path.name}")
    writer = getattr(collectors, "write_csv", None)
    if callable(writer):
        writer(path, rows, fieldnames=fieldnames)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    names = list(fieldnames)
    if not names or len(names) != len(set(names)):
        raise ValueError(f"CSV schema must contain unique fields for {path.name}")
    unknown = sorted({str(key) for row in rows for key in row if key not in names})
    if unknown:
        raise ValueError(f"CSV row has unknown fields for {path.name}: {', '.join(unknown)}")
    with path.open("w", encoding="utf-8", newline="") as stream:
        output = csv.DictWriter(stream, fieldnames=names)
        output.writeheader()
        output.writerows({key: row.get(key) for key in names} for row in rows)


class Kubectl:
    """Small injectable command boundary.  Only get/apply/patch are exposed."""

    def __init__(self, namespace: str, command: Callable[..., Any] | None = None) -> None:
        self.namespace = namespace
        self.command = command or self._subprocess

    @staticmethod
    def _subprocess(argv: Sequence[str], *, input_text: str | None = None, timeout: float | None = None) -> Any:
        return subprocess.run(list(argv), input=input_text, text=True, capture_output=True, timeout=timeout, check=False)

    def run(self, args: Sequence[str], *, input_text: str | None = None, timeout: float | None = None) -> str:
        result = self.command(["kubectl", *args], input_text=input_text, timeout=timeout)
        returncode = getattr(result, "returncode", 0)
        stdout = getattr(result, "stdout", "") or ""
        stderr = getattr(result, "stderr", "") or ""
        if returncode:
            raise RuntimeError(f"kubectl {' '.join(args)} failed ({returncode}): {stderr.strip()}")
        return stdout

    def get_json(self, resource: str, name: str | None = None) -> dict[str, Any]:
        target = resource + (("/" + name) if name else "")
        raw = self.run(["get", target, "-n", self.namespace, "-o", "json"])
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError(f"kubectl returned non-object for {target}")
        return value

    def get_json_optional(self, resource: str, name: str | None = None) -> dict[str, Any]:
        return self.get_json(resource, name)

    def apply(self, body: Mapping[str, Any]) -> None:
        self.run(["apply", "-f", "-"], input_text=json.dumps(body, sort_keys=True))

    def approve(self, plan_name: str) -> None:
        patch = json.dumps({"spec": {"phaseGate": "approved"}}, separators=(",", ":"))
        self.run(["patch", "migactionplan", plan_name, "-n", self.namespace, "--type", "merge", "-p", patch])


class Router:
    def __init__(self, base_url: str, opener: Callable[..., Any] | None = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.opener = opener or urllib.request.urlopen

    def get_json(self, path: str = "/routes", timeout: float = 30.0) -> dict[str, Any]:
        request = urllib.request.Request(self.base_url + path, method="GET")
        with self.opener(request, timeout=timeout) as response:
            value = json.loads(response.read().decode("utf-8") or "{}")
        return value if isinstance(value, dict) else {"value": value}

    def routes(self) -> list[dict[str, Any]]:
        value = self.get_json("/routes")
        return [dict(row) for row in _items(value.get("routes")) if isinstance(row, Mapping)]

    def wait_drained(self, *, timeout: float = 900.0, poll_seconds: float = 0.25) -> list[dict[str, Any]]:
        deadline = time.monotonic() + timeout
        last: list[dict[str, Any]] = []
        while time.monotonic() <= deadline:
            last = self.routes()
            if not last or all(
                _int(_first(route, "endpointInflight", "inflight", default=0)) == 0
                and _int(_first(route, "endpointQueued", "queued", default=0)) == 0
                for route in last
            ):
                return last
            time.sleep(poll_seconds)
        raise TimeoutError("router did not reach an observed drained state")


@dataclass
class RunContext:
    run_id: str
    output_dir: Path
    namespace: str
    router_url: str
    source_control_seconds: float
    target_steady_seconds: float
    profile_seconds: float
    stop_event: threading.Event = field(default_factory=threading.Event)
    # This is intentionally a single cohesive mode: it measures control-plane
    # convergence only, not serving capacity or traffic continuity.
    makespan_mode: bool = False
    post_target_dwell_seconds: float = 5.0
    # E1: one continuous open-loop generator for the whole run; commitment
    # traffic during each transition, new demand for dwell_seconds after it.
    e1_mode: bool = False
    dwell_seconds: float = 30.0
    e1_poll_seconds: float = 0.25
    catalog_path: str = "catalog.csv"


def make_run_context(args: argparse.Namespace, output_root: Path) -> RunContext:
    resume_dir = getattr(args, "resume_run_dir", None)
    if resume_dir is not None:
        output_dir = Path(resume_dir).resolve()
        if not output_dir.is_dir():
            raise ValueError(f"--resume-run-dir does not exist: {output_dir}")
        environment_path = output_dir / "environment.json"
        if not environment_path.is_file():
            raise ValueError(f"--resume-run-dir has no environment.json: {output_dir}")
        environment = json.loads(environment_path.read_text(encoding="utf-8"))
        run_id = str(environment.get("run_id") or "")
        if not run_id:
            raise ValueError(f"--resume-run-dir environment.json has no run_id: {output_dir}")
        return RunContext(
            run_id, output_dir, args.namespace, args.router_url,
            args.source_control_seconds, args.target_steady_seconds, args.profile_seconds,
            makespan_mode=bool(args.makespan_mode),
            post_target_dwell_seconds=float(args.post_target_dwell_seconds),
            e1_mode=bool(getattr(args, "e1", False)),
            dwell_seconds=float(getattr(args, "dwell_seconds", 30.0)),
            catalog_path=str(getattr(args, "catalog", "catalog.csv")),
        )
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    output_dir = output_root / run_id
    output_dir.mkdir(parents=True, exist_ok=False)
    for directory in ("plans", "snapshots"):
        (output_dir / directory).mkdir()
    return RunContext(
        run_id, output_dir, args.namespace, args.router_url,
        args.source_control_seconds, args.target_steady_seconds, args.profile_seconds,
        makespan_mode=bool(args.makespan_mode),
        post_target_dwell_seconds=float(args.post_target_dwell_seconds),
        e1_mode=bool(getattr(args, "e1", False)),
        dwell_seconds=float(getattr(args, "dwell_seconds", 30.0)),
        catalog_path=str(getattr(args, "catalog", "catalog.csv")),
    )


def _experiment_mode(ctx: RunContext) -> str:
    if ctx.e1_mode:
        return "e1_continuous_traffic"
    return "transition_makespan_no_traffic_no_profile" if ctx.makespan_mode else "full_traffic_and_profile"


def _write_initial_outputs(ctx: RunContext, args: argparse.Namespace, hashes: Mapping[str, str]) -> None:
    _json_output(ctx.output_dir / "environment.json", {
        "run_id": ctx.run_id, "started_at": utc_now(), "namespace": ctx.namespace,
        "router_url": ctx.router_url, "traffic_seed": TRAFFIC_SEED,
        "source_control_seconds": ctx.source_control_seconds, "target_steady_seconds": ctx.target_steady_seconds,
        "profile_seconds": ctx.profile_seconds, "input_sha256": dict(hashes),
        "experiment_mode": _experiment_mode(ctx),
        "makespan_mode": ctx.makespan_mode,
        "post_target_dwell_seconds": ctx.post_target_dwell_seconds if ctx.makespan_mode else None,
        "e1_mode": ctx.e1_mode,
        "e1_dwell_seconds": ctx.dwell_seconds if ctx.e1_mode else None,
        "e1_completion_poll_seconds": ctx.e1_poll_seconds if ctx.e1_mode else None,
        "catalog_file": ctx.catalog_path,
        "stage3_variant": getattr(args, "stage3_variant", "slicewise"),
        "capacity_headroom": getattr(args, "capacity_headroom", None),
        "conservative_3g_mu": bool(getattr(args, "conservative_3g_mu", False)),
        "e1_arrivals": getattr(args, "arrivals", "fixed") if ctx.e1_mode else None,
        "e1_first_round": int(getattr(args, "e1_first_round", 1)) if ctx.e1_mode else None,
        "solver": {"threads": 8, "seed": 1, "mip_gap": 0, "accepted_status": "OPTIMAL"},
    })
    _json_output(ctx.output_dir / "profile_protocol.json", {
        "mode": "skipped_for_transition_makespan" if (ctx.makespan_mode or ctx.e1_mode) else "in_place_existing_replicas",
        "sample_window_seconds": None if (ctx.makespan_mode or ctx.e1_mode) else ctx.profile_seconds,
        "status": "skipped_no_profile" if (ctx.makespan_mode or ctx.e1_mode) else "configured",
        "warmup_requests": {"vision": 10, "llm": 1}, "family": "auto", "traffic_seed": TRAFFIC_SEED,
        "timing_boundary": "runtimeInferenceSeconds with runtime CUDA synchronization",
        "source": "k8s-extension-go/tools/run_k8s_profile_matrix.py defaults",
        "request_shapes": _json(WORKLOAD_CONTRACT),
    })
    _jsonl_output(ctx.output_dir / "requests.jsonl", [])
    _jsonl_output(ctx.output_dir / "actions.jsonl", [])
    _jsonl_output(ctx.output_dir / "runtime_events.jsonl", [])
    _jsonl_output(ctx.output_dir / "gpu_events.jsonl", [])
    _csv_output(ctx.output_dir / "planned_requests.csv", [], ["run_id", "live_round", "trace_round", "phase", "traffic_seed", "workload", "sequence_id", "payload_hash", "scheduled_offset", "rate", "seed"])
    _csv_output(ctx.output_dir / "replica_lifecycle.csv", [], LIFECYCLE_FIELDS)
    _csv_output(ctx.output_dir / "action_runtime_by_workload.csv", [], ACTION_RUNTIME_FIELDS)
    _csv_output(ctx.output_dir / "profile_samples.csv", [], PROFILE_SAMPLE_FIELDS)
    _csv_output(ctx.output_dir / "capacity_results.csv", [], CAPACITY_RESULT_FIELDS)
    _csv_output(ctx.output_dir / "capacity_timeline.csv", [], CAPACITY_TIMELINE_FIELDS)
    _csv_output(ctx.output_dir / "transition_requests_summary.csv", [], TRANSITION_REQUEST_FIELDS)
    _csv_output(ctx.output_dir / "round_summary.csv", [], ROUND_SUMMARY_FIELDS)
    _json_output(ctx.output_dir / "round_summary.json", {"run_id": ctx.run_id, "finalValidation": {}, "makespan_seconds": None, "action_counts": {}})
    (ctx.output_dir / "results.md").write_text("# Results\n\nRun has not completed. Partial outputs are preserved.\n", encoding="utf-8")


def _local_command(*argv: str) -> str | None:
    result = subprocess.run(list(argv), cwd=ROOT.parent.parent, text=True, capture_output=True, check=False)
    return result.stdout.strip() if result.returncode == 0 else None


def _pod_image_inventory(pods: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for pod in _items(pods.get("items")):
        metadata = _mapping(_mapping(pod).get("metadata"))
        spec = _mapping(_mapping(pod).get("spec"))
        status = _mapping(_mapping(pod).get("status"))
        by_name = {
            str(_mapping(item).get("name")): _mapping(item)
            for item in _items(status.get("containerStatuses"))
        }
        for container in _items(spec.get("containers")):
            container = _mapping(container)
            name = str(container.get("name", ""))
            container_status = by_name.get(name, {})
            rows.append({
                "namespace": metadata.get("namespace"),
                "pod": metadata.get("name"),
                "node": spec.get("nodeName"),
                "container": name,
                "image": container.get("image"),
                "image_id": container_status.get("imageID"),
                "ready": container_status.get("ready"),
            })
    return rows


def _pod_labels(pod: Mapping[str, Any]) -> Mapping[str, Any]:
    return _mapping(_mapping(pod).get("metadata")).get("labels", {})


def _pod_node(pod: Mapping[str, Any]) -> str:
    return str(_mapping(_mapping(pod).get("spec")).get("nodeName", ""))


def _pod_name(pod: Mapping[str, Any]) -> str:
    return str(_mapping(_mapping(pod).get("metadata")).get("name", ""))


def _pod_component(pod: Mapping[str, Any]) -> str:
    labels = _pod_labels(pod)
    values = [
        str(labels.get("app.kubernetes.io/component", "")),
        str(labels.get("app.kubernetes.io/name", "")),
        _pod_name(pod),
    ]
    text = " ".join(values).lower()
    if "mig-node-agent" in text:
        return "mig-node-agent"
    if "slot-device-plugin" in text or "device-plugin" in text:
        return "device-plugin"
    return ""


def _pod_readiness_evidence(pod: Mapping[str, Any]) -> dict[str, Any]:
    status = _mapping(_mapping(pod).get("status"))
    container_statuses = [_mapping(item) for item in _items(status.get("containerStatuses"))]
    conditions = {
        str(_mapping(item).get("type")): str(_mapping(item).get("status", ""))
        for item in _items(status.get("conditions"))
    }
    ready = bool(
        str(status.get("phase", "")) == "Running"
        and container_statuses
        and all(item.get("ready") is True for item in container_statuses)
        and conditions.get("Ready", "True") == "True"
    )
    image_ids = [str(item.get("imageID")) for item in container_statuses if item.get("imageID")]
    return {
        "pod": _pod_name(pod),
        "node": _pod_node(pod),
        "component": _pod_component(pod),
        "phase": status.get("phase"),
        "ready": ready,
        "image_ids": image_ids,
        "mirror_pull_verified": bool(image_ids) and ready,
    }


def assess_runtime_image_readiness(
    system_pods: Mapping[str, Any],
    required_tags: Sequence[str],
    *,
    worker_nodes: Sequence[str] = GPU_WORKER_NODES,
) -> dict[str, Any]:
    """Return registry and worker evidence without performing cluster I/O.

    The registry is queried by the caller exactly once.  This function keeps
    registry availability separate from worker readiness so a centralized
    control-plane registry does not masquerade as a worker health check.
    """

    pods = [dict(_mapping(raw)) for raw in _items(system_pods.get("items"))]
    registry_pods = [
        pod for pod in pods
        if "migrant-local-registry" in " ".join(
            [_pod_name(pod), *[str(value) for value in _pod_labels(pod).values()]]
        ).lower()
    ]
    registry_pods.sort(key=lambda pod: (not _pod_readiness_evidence(pod)["ready"], _pod_name(pod)))
    selected_registry = registry_pods[0] if registry_pods else None
    registry_node = _pod_node(selected_registry) if selected_registry else None
    registry_mode = "centralized" if registry_node and registry_node not in set(worker_nodes) else "worker_local"

    worker_evidence: dict[str, dict[str, Any]] = {}
    for node in worker_nodes:
        components: dict[str, dict[str, Any]] = {}
        for pod in pods:
            if _pod_node(pod) != node:
                continue
            component = _pod_component(pod)
            if component and component not in components:
                components[component] = _pod_readiness_evidence(pod)
        worker_evidence[node] = {
            "node": node,
            "mig_node_agent": components.get("mig-node-agent", {"ready": False, "pod": None, "node": node, "component": "mig-node-agent", "mirror_pull_verified": False}),
            "device_plugin": components.get("device-plugin", {"ready": False, "pod": None, "node": node, "component": "device-plugin", "mirror_pull_verified": False}),
        }
        worker_evidence[node]["ready"] = all(
            _mapping(worker_evidence[node][component]).get("ready") is True
            for component in ("mig_node_agent", "device_plugin")
        )
        worker_evidence[node]["mirror_pull_verified"] = all(
            _mapping(worker_evidence[node][component]).get("mirror_pull_verified") is True
            for component in ("mig_node_agent", "device_plugin")
        )

    required = sorted(set(str(tag) for tag in required_tags if str(tag)))
    return {
        "registry_mode": registry_mode if selected_registry else "missing",
        "registry_node": registry_node,
        "registry_pod": _pod_name(selected_registry) if selected_registry else None,
        "registry_pods_discovered": [_pod_name(pod) for pod in registry_pods],
        "registry_query_count": 0,
        "required_tags": required,
        "registry_tags": [],
        "missing_required_tags": required,
        "worker_nodes": list(worker_nodes),
        "worker_readiness": worker_evidence,
        "worker_components_ready": all(_mapping(row).get("ready") is True for row in worker_evidence.values()),
        "worker_mirror_pull_verified": all(_mapping(row).get("mirror_pull_verified") is True for row in worker_evidence.values()),
        "registry_pod_found": selected_registry is not None,
    }


def _enrich_environment(
    ctx: RunContext,
    kube: Kubectl,
    readiness: Mapping[str, Any],
) -> dict[str, Any]:
    path = ctx.output_dir / "environment.json"
    environment = json.loads(path.read_text(encoding="utf-8"))
    observations: dict[str, Any] = {}
    observation_errors: list[dict[str, str]] = []
    system_kube = Kubectl("or-sim", command=kube.command) if isinstance(kube, Kubectl) else kube
    system_namespace = str(getattr(system_kube, "namespace", ctx.namespace))
    for name, getter in (
        ("kubernetes_version", lambda: json.loads(kube.run(["version", "-o", "json"]))),
        ("nodes", lambda: kube.get_json("nodes")),
        ("experiment_pods", lambda: kube.get_json("pods")),
        ("system_pods", lambda: system_kube.get_json("pods")),
        ("daemonsets", lambda: system_kube.get_json("daemonsets")),
    ):
        try:
            observations[name] = getter()
        except BaseException as exc:
            observation_errors.append({"source": name, "error_type": type(exc).__name__, "error": str(exc)})
    system_pods = _mapping(observations.get("system_pods"))
    ntp_synchronized_by_node: dict[str, str] = {}
    for raw_pod in _items(system_pods.get("items")):
        pod = _mapping(raw_pod)
        pod_name = _pod_name(pod)
        node_name = _pod_node(pod)
        try:
            if _pod_component(pod) == "mig-node-agent" and node_name in set(GPU_WORKER_NODES):
                ntp_synchronized_by_node[node_name] = system_kube.run([
                    "exec", "-n", system_namespace, pod_name, "--", "chroot", "/host",
                    "timedatectl", "show", "-p", "NTPSynchronized", "--value",
                ]).strip()
        except BaseException as exc:
            observation_errors.append({
                "source": f"node_observation:{node_name}:{pod_name}",
                "error_type": type(exc).__name__,
                "error": str(exc),
            })
    executor = _mapping(_mapping(readiness.get("controllers")).get("transition-executor"))
    executor_containers = _items(_mapping(_mapping(executor.get("spec")).get("template")).get("spec", {}).get("containers"))
    executor_env: dict[str, Any] = {}
    if executor_containers:
        executor_env = {
            str(_mapping(item).get("name")): _mapping(item).get("value")
            for item in _items(_mapping(executor_containers[0]).get("env"))
        }
    required_runtime_tags = sorted({
        str(value).rsplit(":", 1)[-1]
        for name, value in executor_env.items()
        if name in {"VISION_MODEL_RUNTIME_IMAGE", "GPT2_MODEL_RUNTIME_IMAGE", "LLAMA_MODEL_RUNTIME_IMAGE"}
        and isinstance(value, str) and ":" in value
    })
    runtime_image_readiness = assess_runtime_image_readiness(
        system_pods,
        required_runtime_tags,
        worker_nodes=GPU_WORKER_NODES,
    )
    registry_query_error: str | None = None
    if runtime_image_readiness["registry_pod_found"]:
        try:
            raw_tags = system_kube.run([
                "exec", "-n", system_namespace, str(runtime_image_readiness["registry_pod"]), "--",
                "wget", "-qO-", "http://127.0.0.1:10690/v2/migrant-model-runtime/tags/list",
            ])
            registry_tags = sorted(str(tag) for tag in _items(json.loads(raw_tags).get("tags")))
            runtime_image_readiness.update({
                "registry_tags": registry_tags,
                "registry_tags_by_node": {str(runtime_image_readiness["registry_node"]): registry_tags},
                "registry_query_count": 1,
                "missing_required_tags": sorted(set(required_runtime_tags) - set(registry_tags)),
            })
        except BaseException as exc:
            registry_query_error = f"registry tag query failed: {type(exc).__name__}: {exc}"
            runtime_image_readiness.update({
                "registry_query_count": 1,
                "registry_query_error": registry_query_error,
            })
            observation_errors.append({
                "source": f"registry_tag_query:{runtime_image_readiness['registry_pod']}",
                "error_type": type(exc).__name__,
                "error": str(exc),
            })
    runtime_image_readiness["ok"] = bool(
        runtime_image_readiness["registry_pod_found"]
        and runtime_image_readiness["registry_query_count"] == 1
        and not runtime_image_readiness["missing_required_tags"]
        and registry_query_error is None
    )
    environment.update({
        "finished_preflight_at": utc_now(),
        "command": [sys.executable, *sys.argv],
        "git": {
            "head": _local_command("git", "rev-parse", "HEAD"),
            "status_short": (_local_command("git", "status", "--short") or "").splitlines(),
            "diff_stat": (_local_command("git", "diff", "--stat") or "").splitlines(),
        },
        "harness": {
            "runner": str(Path(__file__).relative_to(ROOT.parent.parent)),
            "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "profile_source": "k8s-extension-go/tools/run_k8s_profile_matrix.py",
            "profile_adapter": str((ROOT / "live_profile_20260926.py").relative_to(ROOT.parent.parent)),
        },
        "registry": readiness.get("registry"),
        "observed_a100_ids": readiness.get("observedA100Ids"),
        "controllers": readiness.get("controllers"),
        "kubernetes_version": observations.get("kubernetes_version"),
        "nodes": observations.get("nodes"),
        "daemonsets": observations.get("daemonsets"),
        "pod_images": [
            *_pod_image_inventory(_mapping(observations.get("experiment_pods"))),
            *_pod_image_inventory(_mapping(observations.get("system_pods"))),
        ],
        "clock_alignment": {
            "status": "worker_ntp_verified" if ntp_synchronized_by_node and all(
                str(value).lower() == "yes" for value in ntp_synchronized_by_node.values()
            ) else "not_verified",
            "required_absolute_offset_ms": 10,
            "worker_ntp_synchronized": ntp_synchronized_by_node,
            "reason": "worker NTP synchronization is checked on host clocks; numeric offsets are unavailable. Executor action ordering uses one control-plane process clock and durations use monotonic clocks",
        },
        "runtime_image_readiness": runtime_image_readiness,
        "observation_errors": observation_errors,
    })
    _json_output(path, environment)
    return environment


def _write_readiness(ctx: RunContext, readiness: Mapping[str, Any], environment: Mapping[str, Any]) -> None:
    status = "PASS" if readiness.get("ok") else "BLOCKED"
    errors = [str(item) for item in _items(readiness.get("errors"))]
    images = [
        row for row in _items(environment.get("pod_images"))
        if _mapping(row).get("image_id")
    ]
    lines = [
        "# Readiness",
        "",
        f"- 状态：{status}",
        f"- 运行 ID：`{ctx.run_id}`",
        f"- namespace：`{ctx.namespace}`",
        f"- router：`{ctx.router_url}`",
        f"- 实际入口：`{' '.join(str(item) for item in environment.get('command', []))}`",
        "- profiling 原入口：`k8s-extension-go/tools/run_k8s_profile_matrix.py`",
        "- 本轮 in-place 实现：`live_profile_20260926.py`，vision warmup=10、LLM warmup=1、共同 barrier、runtime CUDA 同步计时。",
        f"- 观测到 A100：`{', '.join(str(item) for item in _items(readiness.get('observedA100Ids')))}`",
        f"- 带 digest 的运行中容器记录：{len(images)} 条，详见 `environment.json`。",
        "- 时钟限制：尚无跨节点 offset 实测；跨节点 UTC 次序保留此限制，单进程 duration 使用 monotonic clock。",
        "- In-place transition 未额外构造覆盖；本次只记录冻结 12 轮自然触发的动作。",
    ]
    if errors:
        lines.extend(["", "## Blockers", "", *(f"- {item}" for item in errors)])
    (ctx.output_dir / "readiness.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def environment_readiness_errors(environment: Mapping[str, Any]) -> list[str]:
    """Translate environment evidence into explicit, non-overlapping blockers."""

    errors = [
        f"environment observation failed: {_mapping(item).get('source')}: {_mapping(item).get('error')}"
        for item in _items(environment.get("observation_errors"))
    ]
    image = _mapping(environment.get("runtime_image_readiness"))
    if not image.get("ok"):
        errors.append(
            "runtime image tags unavailable from discovered "
            f"{image.get('registry_mode', 'unknown')} registry pod "
            f"{image.get('registry_pod') or '<none>'} on node "
            f"{image.get('registry_node') or '<unknown>'}; "
            f"missing={','.join(str(item) for item in _items(image.get('missing_required_tags')))}"
        )
    if image.get("worker_components_ready") is not True:
        errors.append("mig-node-agent and device-plugin must both be Ready on ampere and rtx1-worker")
    if image.get("worker_mirror_pull_verified") is not True:
        errors.append("independent worker mirror-pull evidence is missing for mig-node-agent/device-plugin on both GPU workers")
    if _mapping(environment.get("clock_alignment")).get("status") != "worker_ntp_verified":
        errors.append("worker host NTP synchronization could not be verified")
    return errors


def _merge_observed_pod_images(ctx: RunContext, pods: Mapping[str, Any]) -> None:
    path = ctx.output_dir / "environment.json"
    environment = json.loads(path.read_text(encoding="utf-8"))
    rows = [*_items(environment.get("pod_images")), *_pod_image_inventory(pods)]
    unique: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for raw in rows:
        row = dict(_mapping(raw))
        key = tuple(str(row.get(name) or "") for name in ("namespace", "pod", "container", "image_id"))
        unique[key] = row
    environment["pod_images"] = list(unique.values())
    _json_output(path, environment)


def _append_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    existing = []
    if path.exists() and path.stat().st_size:
        existing = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    _jsonl_output(path, [*existing, *list(rows)])


def _append_csv(path: Path, rows: Iterable[Mapping[str, Any]], fieldnames: Sequence[str]) -> None:
    existing: list[dict[str, Any]] = []
    if path.exists() and path.stat().st_size:
        with path.open(encoding="utf-8", newline="") as stream:
            existing = list(csv.DictReader(stream))
    _csv_output(path, [*existing, *list(rows)], fieldnames)


def _record_planned_requests(ctx: RunContext, live_round: int, trace_round: int, phase: str, rows: Sequence[Mapping[str, Any]]) -> None:
    planned = [{
        "run_id": ctx.run_id, "live_round": live_round, "trace_round": trace_round,
        "phase": row.get("phase", phase), "traffic_seed": TRAFFIC_SEED, "workload": row.get("workload"),
        "sequence_id": row.get("sequence_id"), "payload_hash": row.get("payload_hash"),
        "scheduled_offset": row.get("scheduled_offset"), "rate": row.get("rate"), "seed": row.get("seed"),
    } for row in rows]
    _append_csv(ctx.output_dir / "planned_requests.csv", planned, ["run_id", "live_round", "trace_round", "phase", "traffic_seed", "workload", "sequence_id", "payload_hash", "scheduled_offset", "rate", "seed"])


def _record_plan_artifacts(ctx: RunContext, live_round: int, trace_round: int, plan: Mapping[str, Any], registry: Mapping[str, Any]) -> None:
    action_rows = getattr(collectors, "flatten_action_plan")(plan)
    enriched = [{"run_id": ctx.run_id, "live_round": live_round, "trace_round": trace_round, "phase": "transition", **row} for row in action_rows]
    _append_jsonl(ctx.output_dir / "actions.jsonl", enriched)
    gpu_rows = getattr(collectors, "build_gpu_events")(enriched, initial_gpu_ids=sorted(allocated_physical_ids(registry)))
    execution = _mapping(_mapping(plan.get("status")).get("transitionExecution"))
    executor_started_at = _first(_mapping(execution.get("timestamps")), "executorStartedAt", "startedAt")
    for row in gpu_rows:
        if row.get("event_type") == "baseline":
            row["timestamp"] = executor_started_at
            row["timestamp_source"] = "executor_started_at" if executor_started_at else None
    _append_jsonl(ctx.output_dir / "gpu_events.jsonl", [{"run_id": ctx.run_id, "live_round": live_round, "trace_round": trace_round, **row} for row in gpu_rows])


def _executor_final_validation(plan: Mapping[str, Any]) -> dict[str, Any]:
    execution = _mapping(_mapping(plan.get("status")).get("transitionExecution"))
    value = _first(_mapping(execution.get("metrics")), "finalValidation", "final_validation", default={})
    return dict(value) if isinstance(value, Mapping) else {}


def _catalog_mu(catalog: Sequence[Mapping[str, Any]], descriptor: Mapping[str, Any]) -> float | None:
    workload = str(_first(descriptor, "workload", "model", default=""))
    profile_name = str(_first(descriptor, "physical_profile", "physicalProfile", "profile", default=""))
    batch = _int(_first(descriptor, "batch", "batchSize", "batch_size", default=0))
    for row in catalog:
        if (
            str(row.get("workload", "")) == workload
            and str(row.get("profile", "")) == profile_name
            and _int(row.get("batch")) == batch
        ):
            return _number(row.get("mu"))
    return None


def _capacity_ledger_rows(
    ctx: RunContext,
    live_round: int,
    trace_round: int,
    terminal: Mapping[str, Any],
    source_descriptors: Sequence[Mapping[str, Any]],
    final_descriptors: Sequence[Mapping[str, Any]],
    lifecycle_rows: Sequence[Mapping[str, Any]],
    runtime_events: Sequence[Mapping[str, Any]],
    catalog: Sequence[Mapping[str, Any]],
    source_rates: Mapping[str, float],
    target_rates: Mapping[str, float],
) -> list[dict[str, Any]]:
    execution = _mapping(_mapping(terminal.get("status")).get("transitionExecution"))
    timestamps = _mapping(execution.get("timestamps"))
    baseline_at = _first(timestamps, "executorStartedAt", "startedAt")
    source_ids = {str(_first(row, "runtimeId", "runtime_id", default="")) for row in source_descriptors}
    final_ids = {str(_first(row, "runtimeId", "runtime_id", default="")) for row in final_descriptors}
    environment = json.loads((ctx.output_dir / "environment.json").read_text(encoding="utf-8"))
    clock_uncertain = _mapping(environment.get("clock_alignment")).get("status") not in {
        "synchronized", "worker_ntp_verified",
    }
    capacities = {workload: 0.0 for workload in WORKLOAD_KEYS}
    rows: list[dict[str, Any]] = []

    for descriptor in source_descriptors:
        workload = str(descriptor.get("workload", ""))
        mu = _catalog_mu(catalog, descriptor)
        if workload in capacities and mu is not None:
            capacities[workload] += mu
    for workload in WORKLOAD_KEYS:
        commitment = min(float(source_rates.get(workload, 0.0)), float(target_rates.get(workload, 0.0)))
        capacity = capacities[workload]
        rows.append({
            "run_id": ctx.run_id, "live_round": live_round, "trace_round": trace_round,
            "phase": "transition", "event_type": "source_baseline", "timestamp": baseline_at,
            "timestamp_source": "executor_started_at" if baseline_at else None,
            "runtime_id": None, "workload": workload, "physical_profile": None, "batch": None,
            "mu": capacity, "delta": 0.0, "capacity": capacity, "commitment": commitment,
            "capacity_ratio": capacity / commitment if commitment > 0 else None,
            "reason": (
                "executor_start_timestamp_missing" if not baseline_at
                else ("cross_node_clock_offset_unverified" if clock_uncertain else None)
            ),
            "uncertain": baseline_at is None or clock_uncertain,
        })

    changes = collectors.derive_capacity_timeline(
        catalog,
        lifecycle_rows,
        runtime_events=runtime_events,
    )
    event_evidence: dict[tuple[str, str], Mapping[str, Any]] = {}
    for event in runtime_events:
        event_evidence[(str(event.get("runtime_id") or ""), str(event.get("event_type") or ""))] = event
    filtered: list[dict[str, Any]] = []
    for raw in changes:
        event = dict(raw)
        runtime_id = str(event.get("runtime_id") or "")
        event_type = str(event.get("event_type") or "")
        if event_type in {"route_ready_add", "capacity_add_unproven"} and runtime_id in source_ids:
            continue
        if event_type in {"stop_accepting_remove", "capacity_remove_unproven"} and runtime_id in final_ids:
            continue
        filtered.append(event)
    filtered.sort(key=lambda row: (row.get("timestamp") is None, str(row.get("timestamp") or ""), str(row.get("runtime_id") or "")))
    for event in filtered:
        workload = str(event.get("workload") or "")
        if workload not in capacities:
            continue
        timestamp = event.get("timestamp")
        delta = _number(event.get("delta")) if timestamp else 0.0
        capacities[workload] += delta
        commitment = min(float(source_rates.get(workload, 0.0)), float(target_rates.get(workload, 0.0)))
        capacity = capacities[workload] if timestamp else None
        evidence_type = {
            "route_ready_add": "route_activation",
            "stop_accepting_remove": "route_stop_accepting",
        }.get(str(event.get("event_type") or ""), str(event.get("event_type") or ""))
        evidence = _mapping(event_evidence.get((str(event.get("runtime_id") or ""), evidence_type)))
        uncertain = timestamp is None or evidence.get("uncertain") is True or clock_uncertain
        rows.append({
            "run_id": ctx.run_id, "live_round": live_round, "trace_round": trace_round,
            "phase": "transition", "event_type": event.get("event_type"), "timestamp": timestamp,
            "timestamp_source": evidence.get("timestamp_source") or event.get("timestamp_source"), "runtime_id": event.get("runtime_id"),
            "workload": workload, "physical_profile": event.get("physical_profile"),
            "batch": event.get("batch"), "mu": event.get("mu"), "delta": delta if timestamp else None,
            "capacity": capacity, "commitment": commitment,
            "capacity_ratio": capacity / commitment if capacity is not None and commitment > 0 else None,
            "reason": event.get("reason") or (
                "cross_node_clock_offset_unverified" if clock_uncertain
                else ("request_ack_interval" if uncertain and timestamp else None)
            ),
            "uncertain": uncertain,
        })
    return rows


def _runtime_events_from_actions(
    ctx: RunContext,
    live_round: int,
    trace_round: int,
    action_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []

    def add(action: Mapping[str, Any], event_type: str, timestamp: Any, source: str, **extra: Any) -> None:
        events.append({
            "run_id": ctx.run_id,
            "live_round": live_round,
            "trace_round": trace_round,
            "phase": "transition",
            "event_type": event_type,
            "timestamp": timestamp,
            "timestamp_source": source,
            "action_id": action.get("action_id"),
            "runtime_id": action.get("runtime_id"),
            "workload": action.get("workload"),
            "physical_gpu_id": action.get("physical_gpu_id"),
            "slot": action.get("slot"),
            "profile": action.get("physical_profile"),
            "batch": action.get("batch"),
            **extra,
        })

    for action in action_rows:
        action_type = str(action.get("action_type") or "")
        if action_type == "place_instance":
            add(action, "deployment_create_started", action.get("timing_runtimeDeploymentCreateStartedAt"), "executor_observed")
            add(action, "deployment_created", action.get("timing_runtimeDeploymentCreatedAt"), "kubernetes_api_ack")
        elif action_type == "activate_instance_route":
            ready_at = action.get("timing_runtimeReadyAndCUDAVerifiedAt")
            route_at = action.get("timing_routeSyncedAt")
            if ready_at:
                add(action, "pod_ready", ready_at, "executor_runtime_cuda_verified")
            if route_at:
                add(action, "route_activation", route_at, "router_upsert_ack")
        elif action_type == "deactivate_instance_route":
            # The executor currently exposes the request/ACK interval, not a
            # separate router effective timestamp.  The earliest timestamp is
            # used only for the conservative capacity ledger and is marked.
            add(
                action,
                "route_stop_accepting",
                action.get("started_at"),
                "action_request_started_conservative",
                acknowledged_at=action.get("finished_at"),
                uncertain=True,
            )
        elif action_type == "wait_instance_drain":
            if action.get("timing_drainWaitStartedAt"):
                add(action, "drain_started", action.get("timing_drainWaitStartedAt"), "executor_observed")
            if action.get("timing_drainWaitFinishedAt"):
                add(action, "drain_completed", action.get("timing_drainWaitFinishedAt"), "executor_observed")
        elif action_type == "delete_instance":
            add(action, "pod_delete_started", action.get("started_at"), "executor_action_started")
            add(action, "pod_gone_confirmed", action.get("finished_at"), "executor_delete_and_wait_completed")
        elif action_type == "apply_batch":
            add(action, "batch_apply", action.get("timing_batchApplyFinishedAt"), "router_apply_ack", new_batch=action.get("new_batch"))
        elif action_type == "verify_batch":
            add(
                action,
                "batch_effective",
                action.get("timing_batchVerifyFinishedAt"),
                "runtime_verify_ack",
                old_batch=action.get("old_batch"),
                new_batch=action.get("new_batch"),
            )
    return events


def _record_round_artifacts(
    ctx: RunContext,
    live_round: int,
    trace_round: int,
    terminal: Mapping[str, Any],
    source_registry: Mapping[str, Any],
    source_routes: Sequence[Mapping[str, Any]],
    final_routes: Sequence[Mapping[str, Any]],
    profile_report: Mapping[str, Any],
    source_rates: Mapping[str, float],
    target_rates: Mapping[str, float],
    catalog: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    action_rows = collectors.flatten_action_plan(terminal)
    source_descriptors = _descriptors_from_routes(source_routes)
    descriptors = _descriptors_from_routes(final_routes)
    combined_by_runtime = {
        str(_first(row, "runtimeId", "runtime_id", default="")): row
        for row in [*source_descriptors, *descriptors]
    }
    descriptors_by_runtime = {
        str(_first(row, "runtimeId", "runtime_id", default="")): row for row in descriptors
    }

    runtime_events = _runtime_events_from_actions(ctx, live_round, trace_round, action_rows)
    lifecycle = collectors.build_replica_lifecycle_rows(
        combined_by_runtime.values(), action_rows, runtime_events
    )
    lifecycle_rows = [{
        "run_id": ctx.run_id, "live_round": live_round, "trace_round": trace_round,
        "phase": "target", **row,
    } for row in lifecycle]
    _append_csv(ctx.output_dir / "replica_lifecycle.csv", lifecycle_rows, LIFECYCLE_FIELDS)

    _append_jsonl(ctx.output_dir / "runtime_events.jsonl", runtime_events)
    capacity_timeline_rows = _capacity_ledger_rows(
        ctx,
        live_round,
        trace_round,
        terminal,
        source_descriptors,
        descriptors,
        lifecycle_rows,
        runtime_events,
        catalog,
        source_rates,
        target_rates,
    )
    _append_csv(ctx.output_dir / "capacity_timeline.csv", capacity_timeline_rows, CAPACITY_TIMELINE_FIELDS)

    samples: list[dict[str, Any]] = []
    for raw in _items(profile_report.get("samples")):
        sample = dict(_mapping(raw))
        runtime_id = str(_first(sample, "replicaId", "runtimeId", default=""))
        descriptor = _mapping(descriptors_by_runtime.get(runtime_id))
        samples.append({
            "run_id": ctx.run_id, "live_round": live_round, "trace_round": trace_round,
            "phase": "profile_target", "workload": descriptor.get("workload"),
            "runtime_id": runtime_id, **sample,
        })
    _append_csv(ctx.output_dir / "profile_samples.csv", samples, PROFILE_SAMPLE_FIELDS)

    capacity = collectors.compute_capacity_results(catalog, descriptors, samples, target_rates)
    capacity_rows = []
    for row in capacity:
        replicas = _items(row.get("replicas"))
        capacity_rows.append({
            "run_id": ctx.run_id,
            "live_round": live_round,
            "trace_round": trace_round,
            "phase": "profile_target",
            **row,
            "sample_count": sum(_int(_mapping(replica).get("complete_samples")) for replica in replicas),
            "unit": "logical_samples_per_runtime_inference_second",
        })
    _append_csv(ctx.output_dir / "capacity_results.csv", capacity_rows, CAPACITY_RESULT_FIELDS)

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in action_rows:
        key = (str(row.get("workload") or "GPU-scope"), str(row.get("action_type") or "unknown"))
        grouped.setdefault(key, []).append(row)
    action_runtime_rows = []
    for (workload, action_type), rows in sorted(grouped.items()):
        successful = [row for row in rows if str(row.get("status", "")).lower() in {"completed", "executed", "succeeded", "success"}]
        durations = [float(row["duration_seconds"]) for row in successful if row.get("duration_seconds") is not None]
        action_runtime_rows.append({
            "run_id": ctx.run_id, "live_round": live_round, "trace_round": trace_round,
            "phase": "transition", "workload": workload, "action_type": action_type,
            "count": len(rows), "success_count": len(successful), "failure_count": len(rows) - len(successful),
            "duration_min_seconds": min(durations) if durations else None,
            "duration_median_seconds": statistics.median(durations) if durations else None,
            "duration_max_seconds": max(durations) if durations else None,
        })
    _append_csv(
        ctx.output_dir / "action_runtime_by_workload.csv",
        action_runtime_rows,
        ACTION_RUNTIME_FIELDS,
    )

    gpu_events = collectors.build_gpu_events(
        action_rows, initial_gpu_ids=sorted(allocated_physical_ids(source_registry))
    )
    gpu_intervals = collectors.gpu_count_intervals(
        gpu_events, initial_count=len(allocated_physical_ids(source_registry))
    )
    final_validation = _executor_final_validation(terminal)
    summary = collectors.summarize_round(
        action_rows,
        final_validation=final_validation,
        transition_execution=_mapping(_mapping(terminal.get("status")).get("transitionExecution")),
        gpu_intervals=gpu_intervals,
        capacity_timeline=capacity_timeline_rows,
        gpu_capacity=MAX_PHYSICAL_GPUS,
        run_id=ctx.run_id,
        live_round=live_round,
        trace_round=trace_round,
    )
    action_counts = _mapping(summary.get("action_counts"))
    failed_actions = sum(
        count for status, count in _mapping(action_counts.get("by_status")).items()
        if str(status).lower() not in {"completed", "executed", "succeeded", "success"}
    )
    summary.update({
        "phase": f"R{live_round}",
        "reached_target": True,
        "finalValidationOk": final_validation.get("ok") is True,
        "planner_makespan_seconds": _planner_makespan_seconds(terminal),
        "capacity_uncertain_events": sum(1 for row in capacity_timeline_rows if row.get("uncertain")),
    })
    _json_output(ctx.output_dir / "plans" / f"r{live_round:02d}_round_summary.json", summary)
    _append_csv(
        ctx.output_dir / "round_summary.csv",
        [{
            "run_id": ctx.run_id, "live_round": live_round, "trace_round": trace_round,
            "phase": f"R{live_round}", "reached_target": True,
            "finalValidationOk": final_validation.get("ok") is True,
            "makespan_seconds": summary.get("makespan_seconds"),
            "planner_makespan_seconds": summary.get("planner_makespan_seconds"),
            "action_count": action_counts.get("total"),
            "action_counts": action_counts.get("by_type"),
            "strategy_counts": action_counts.get("by_category"),
            "failure_count": failed_actions,
            "peak_active_gpu_count": summary.get("peak_active_gpu_count"),
            "headroom": summary.get("headroom"),
            "capacity_uncertain_events": summary.get("capacity_uncertain_events"),
        }],
        ROUND_SUMMARY_FIELDS,
    )
    return summary


def _finalize_run_outputs(ctx: RunContext, result: Mapping[str, Any]) -> None:
    summary_rows: list[dict[str, Any]] = []
    summary_path = ctx.output_dir / "round_summary.csv"
    if summary_path.exists() and summary_path.stat().st_size:
        with summary_path.open(encoding="utf-8", newline="") as stream:
            summary_rows = list(csv.DictReader(stream))
    _json_output(ctx.output_dir / "round_summary.json", {
        "run_id": ctx.run_id,
        "finalValidation": {
            "ok": bool(result.get("ok")),
            "completedRounds": result.get("completed_rounds"),
            "expectedRounds": result.get("expected_rounds"),
        },
        "makespan_seconds": None,
        "action_counts": {},
        "rounds": summary_rows,
    })
    status = "完成" if result.get("ok") else "未完成"
    limitations: list[str] = []
    if ctx.makespan_mode:
        limitations.append("transition-makespan模式：未发送服务流量、未执行容量profile；结果只用于动作/收敛时间，不是吞吐或零亏空证明")
    if not result.get("ok"):
        limitations.append(str(result.get("failure") or "运行提前停止"))
    limitation = "；".join(limitations) if limitations else "无"
    (ctx.output_dir / "results.md").write_text(
        "# 三卡连续实验结果\n\n"
        f"- 运行 ID：`{ctx.run_id}`\n"
        f"- 状态：{status}\n"
        f"- 已完成轮数：{result.get('completed_rounds', 0)}/{result.get('expected_rounds', ROUND_COUNT)}\n"
        f"- 限制或失败：{limitation}\n\n"
        "本目录只保存原始数据和文字汇总，不在控制平面生成图。\n",
        encoding="utf-8",
    )
    digest_lines = []
    for path in sorted(item for item in ctx.output_dir.rglob("*") if item.is_file() and item.name != "SHA256SUMS"):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        digest_lines.append(f"{digest}  {path.relative_to(ctx.output_dir)}")
    (ctx.output_dir / "SHA256SUMS").write_text("\n".join(digest_lines) + "\n", encoding="utf-8")


def _validate_and_record_outputs(ctx: RunContext, result: dict[str, Any]) -> dict[str, Any]:
    """Validate both successful and partial runs, preserving validator errors."""

    finalization_errors: list[str] = []
    try:
        _finalize_run_outputs(ctx, result)
    except BaseException as exc:
        finalization_errors.append(f"finalization failed before validation: {type(exc).__name__}: {exc}")
    try:
        if ctx.e1_mode:
            artifact_errors = _validate_makespan_outputs(ctx.output_dir, expected_rounds=int(result.get("expected_rounds", ROUND_COUNT)),
                                                         first_round=int(result.get("first_round", 1)))
            for name in ("requests.jsonl", "e1_rate_events.jsonl", "e1_windows.csv"):
                if not (ctx.output_dir / name).exists():
                    artifact_errors.append(f"missing {name}")
        elif ctx.makespan_mode:
            artifact_errors = _validate_makespan_outputs(ctx.output_dir, expected_rounds=int(result.get("expected_rounds", ROUND_COUNT)))
        else:
            artifact_errors = collectors.validate_required_outputs(
                ctx.output_dir, expected_rounds=ROUND_COUNT,
                allow_partial=not bool(result.get("ok")) or bool(result.get("range_run")),
            )
    except BaseException as exc:
        artifact_errors = [f"validator raised {type(exc).__name__}: {exc}"]
    errors = [*finalization_errors, *list(artifact_errors)]
    validation = {
        "run_id": ctx.run_id,
        "at": utc_now(),
        "expected_rounds": ROUND_COUNT,
        "allow_partial": not bool(result.get("ok")) or bool(result.get("range_run")) or ctx.makespan_mode or ctx.e1_mode,
        "experiment_mode": _experiment_mode(ctx),
        "ok": not errors,
        "errors": errors,
    }
    try:
        _json_output(ctx.output_dir / "output_validation.json", validation)
    except BaseException as exc:
        validation["ok"] = False
        validation["errors"].append(f"could not write output_validation.json: {type(exc).__name__}: {exc}")
    if errors and result.get("ok"):
        result["ok"] = False
        result["failure"] = "required output validation failed: " + "; ".join(errors)
    result["artifact_validation_ok"] = not errors
    try:
        _json_output(ctx.output_dir / "results.json", result)
        _finalize_run_outputs(ctx, result)
    except BaseException as exc:
        result["artifact_validation_ok"] = False
        result.setdefault("failure", f"final result persistence failed: {type(exc).__name__}: {exc}")
    return result


def _validate_makespan_outputs(root: Path, *, expected_rounds: int, first_round: int = 1) -> list[str]:
    """Strictly validate convergence artifacts without requiring traffic/profile files."""
    errors: list[str] = []
    try:
        rows = list(csv.DictReader((root / "round_summary.csv").open(encoding="utf-8", newline="")))
    except (OSError, ValueError) as exc:
        return [f"round_summary.csv unreadable: {exc}"]
    if len(rows) != expected_rounds:
        errors.append(f"round_summary.csv expected {expected_rounds} rows, got {len(rows)}")
    for round_number in range(first_round, first_round + expected_rounds):
        row = next((x for x in rows if str(x.get("live_round")) == str(round_number)), None)
        if row is None:
            errors.append(f"round_summary.csv missing round {round_number}"); continue
        if str(row.get("reached_target")).lower() != "true" or str(row.get("finalValidationOk")).lower() != "true" or str(row.get("failure_count")) not in {"0", "0.0"}:
            errors.append(f"round {round_number} summary is not successful")
        for path in (root / "plans" / f"r{round_number:02d}_terminal_plan.json", root / "snapshots" / f"r{round_number:02d}_after_transition.json"):
            if not path.exists(): errors.append(f"missing {path.relative_to(root)}")
        if not (root / "plans" / f"r{round_number:02d}_terminal_plan.json").exists(): continue
        try:
            plan = json.loads((root / "plans" / f"r{round_number:02d}_terminal_plan.json").read_text(encoding="utf-8"))
            if _mapping(_mapping(plan).get("status")).get("phase") != "Executed": errors.append(f"round {round_number} terminal plan not Executed")
            statuses = _items(_mapping(_mapping(plan).get("status")).get("actionStatuses"))
            # a round whose demand the current layout already covers has no actions
            planned = _items(_mapping(_mapping(_mapping(plan).get("spec")).get("actionDag")).get("nodes"))
            if (planned and not statuses) or any(_mapping(item).get("status") != "completed" for item in statuses): errors.append(f"round {round_number} has incomplete actions")
            final = _mapping(_mapping(_mapping(_mapping(plan).get("status")).get("transitionExecution")).get("metrics")).get("finalValidation")
            if not _mapping(final).get("ok"): errors.append(f"round {round_number} lacks successful executor finalValidation")
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            errors.append(f"round {round_number} terminal plan unreadable: {exc}")
    for name in ("environment.json", "profile_protocol.json", "actions.jsonl", "strict_runtime_audit.json"):
        if not (root / name).exists(): errors.append(f"missing {name}")
    return errors


def _controller_map(value: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(_mapping(item.get("metadata")).get("name", index)): dict(item) for index, item in enumerate(_items(value.get("items"))) if isinstance(item, Mapping)}


def preflight(
    kube: Kubectl,
    router: Router,
    *,
    controllers: Sequence[str],
    require_empty: bool = True,
    allow_inflight: bool = False,
) -> dict[str, Any]:
    registry = kube.get_json("physicalgpuregistries", "default")
    routes = router.get_json("/routes")
    deployments = _controller_map(kube.get_json("deployments"))
    selected = {name: deployments[name] for name in controllers if name in deployments}
    errors = audit_preflight(registry, routes, selected, require_empty=require_empty, allow_inflight=allow_inflight)
    missing = [name for name in controllers if name not in deployments]
    errors.extend("controller missing: " + name for name in missing)
    return {"ok": not errors, "errors": errors, "registry": registry, "routes": routes, "controllers": selected, "observedA100Ids": sorted(observed_a100_ids(registry))}


def wait_for_plan(
    kube: Kubectl,
    name: str,
    *,
    timeout: float = 1800.0,
    poll_seconds: float = 2.0,
    accept_planned: bool = True,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    deadline = clock() + timeout
    last: dict[str, Any] = {}
    while clock() <= deadline:
        try:
            last = kube.get_json("migactionplans", name)
        except RuntimeError as exc:
            if "NotFound" not in str(exc):
                raise
            time.sleep(poll_seconds)
            continue
        phase = str(_mapping(last.get("status")).get("phase", ""))
        if (accept_planned and phase == "Planned") or phase in TERMINAL_PHASES or phase in FAILED_PHASES:
            return last
        time.sleep(poll_seconds)
    raise TimeoutError(f"timed out waiting for MigActionPlan/{name}; last phase={_mapping(last.get('status')).get('phase')}")


def _run_sender(plan_rows: Sequence[Mapping[str, Any]], router_url: str, stop_event: threading.Event, *, timeout: float = 900.0) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    sender = traffic.BoundedAsyncSender(traffic.urllib_transport(router_url, timeout_s=timeout))
    try:
        rows = sender.run(plan_rows, stop_event=stop_event, timeout_s=timeout)
        return rows, sender.accounting()
    finally:
        sender.shutdown()


def _start_transition_sender(plan_rows: Sequence[Mapping[str, Any]], router_url: str, stop_event: threading.Event) -> tuple[Any, threading.Thread]:
    sender = traffic.BoundedAsyncSender(traffic.urllib_transport(router_url, timeout_s=900.0))
    result: dict[str, Any] = {}
    def run() -> None:
        try:
            result["rows"] = sender.run(plan_rows, stop_event=stop_event, timeout_s=900.0)
            result["accounting"] = sender.accounting()
        except BaseException as exc:
            result["error"] = repr(exc)
    thread = threading.Thread(target=run, name="transition-traffic", daemon=True)
    thread.result = result  # type: ignore[attr-defined]
    thread.sender = sender  # type: ignore[attr-defined]
    thread.start()
    return sender, thread


def _start_continuous_transition_sender(
    first_window: Sequence[Mapping[str, Any]],
    rates: Mapping[str, float],
    router_url: str,
    stop_event: threading.Event,
    *,
    window_seconds: float,
) -> tuple[Any, threading.Thread]:
    """Send the paired first window, then identical-rate unpaired windows until stopped."""

    sender = traffic.BoundedAsyncSender(traffic.urllib_transport(router_url, timeout_s=900.0))
    result: dict[str, Any] = {}

    def run() -> None:
        try:
            window = 0
            rows = [dict(row) for row in first_window]
            while not sender.stop_event.is_set() and not stop_event.is_set():
                if window:
                    rows = traffic.build_request_plan(
                        rates,
                        window_seconds,
                        phase="transition_unpaired",
                        seed=TRAFFIC_SEED,
                        sequence_prefix=f"tail{window}:",
                    )
                    offset = window * window_seconds
                    for row in rows:
                        row["scheduled_offset"] = float(row["scheduled_offset"]) + offset
                dispatcher = sender.submit_plan(rows, stop_event=stop_event)
                dispatcher.join()
                if sender.stop_event.is_set() or stop_event.is_set():
                    break
                window += 1
            result["rows"] = sender.drain(timeout_s=900.0)
            result["accounting"] = sender.accounting()
        except BaseException as exc:
            result["error"] = repr(exc)

    thread = threading.Thread(target=run, name="continuous-transition-traffic", daemon=True)
    thread.result = result  # type: ignore[attr-defined]
    thread.sender = sender  # type: ignore[attr-defined]
    thread.start()
    return sender, thread


def stop_and_drain(sender: Any, thread: threading.Thread | None = None, *, timeout: float = 900.0) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    sender.stop_new_sends()
    if thread is not None:
        thread.join(timeout=timeout)
    rows = getattr(thread, "result", {}).get("rows", []) if thread is not None else []
    error = getattr(thread, "result", {}).get("error") if thread is not None else None
    if thread is None:
        rows = sender.drain(timeout_s=timeout)
    accounting = sender.accounting()
    sender.shutdown()
    if thread is not None and thread.is_alive():
        raise TimeoutError("transition traffic did not drain before timeout")
    if error:
        raise RuntimeError(f"transition traffic failed: {error}")
    return rows, accounting


def _descriptors_from_routes(routes: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    descriptors = []
    for route in routes:
        if route.get("active") is False or route.get("acceptingNew") is False or route.get("draining") is True:
            continue
        workload = str(_first(route, "workload", "model", "requestClass", default=""))
        matching = workload if workload in WORKLOAD_CONTRACT else next((key for key, spec in WORKLOAD_CONTRACT.items() if spec["model"] == workload), "")
        if not matching:
            continue
        spec = WORKLOAD_CONTRACT[matching]
        descriptor = dict(route)
        descriptor.update({
            "workload": matching,
            "family": spec["family"],
            "model": spec["model"],
            "batchSize": _int(_first(route, "batchSize", "batch", default=1), 1),
            "warmupRequests": 10 if spec["family"] == "vision" else 1,
            "payload": traffic.build_request_payload(matching, f"profile:{matching}"),
        })
        descriptors.append(descriptor)
    return descriptors


def _profile_target(routes: Sequence[Mapping[str, Any]], seconds: float) -> dict[str, Any]:
    descriptors = _descriptors_from_routes(routes)
    if not descriptors:
        return {"status": "incomplete", "errors": ["no profiled routes observed"], "samples": [], "replicas": []}
    try:
        return profile.run_profile(
            descriptors,
            family="auto",
            warmup_requests=1,
            sample_window_seconds=seconds,
            timeout_seconds=900.0,
        )
    except BaseException as exc:
        # The caller writes this object before applying the stop rule.  Keep
        # the exception and descriptors as raw evidence instead of losing the
        # profile attempt to an adapter/network exception.
        return {
            "status": "error",
            "errors": [f"profile adapter raised {type(exc).__name__}: {exc}"],
            "samples": [],
            "replicas": [],
            "descriptors": _json(descriptors),
            "exception_type": type(exc).__name__,
        }


def _save_plan_and_audit(ctx: RunContext, live_round: int, plan: Mapping[str, Any], audit: Mapping[str, Any]) -> None:
    _json_output(ctx.output_dir / "plans" / f"r{live_round:02d}_plan.json", plan)
    _json_output(ctx.output_dir / "plans" / f"r{live_round:02d}_audit.json", audit)


def _capture_round_failure(
    ctx: RunContext,
    kube: Kubectl,
    router: Router,
    *,
    live_round: int,
    trace_round: int,
    phase: str,
    error: BaseException,
    plan_name: str | None,
    terminal: Mapping[str, Any] | None,
    profile_report: Mapping[str, Any] | None,
) -> None:
    """Persist best-effort evidence without masking the original failure."""

    evidence: dict[str, Any] = {
        "run_id": ctx.run_id,
        "at": utc_now(),
        "live_round": live_round,
        "trace_round": trace_round,
        "phase": phase,
        "error_type": type(error).__name__,
        "error": str(error),
        "plan_name": plan_name,
        "terminal": dict(terminal or {}),
        "profile_report": dict(profile_report or {}),
        "observation_errors": [],
    }
    for key, getter in (
        ("registry", lambda: kube.get_json("physicalgpuregistries", "default")),
        ("routes", lambda: router.get_json("/routes")),
        ("pods", lambda: kube.get_json("pods")),
    ):
        try:
            evidence[key] = getter()
        except BaseException as observation_error:
            evidence["observation_errors"].append({
                "source": key,
                "error_type": type(observation_error).__name__,
                "error": str(observation_error),
            })
    _json_output(ctx.output_dir / "snapshots" / f"r{live_round:02d}_failure.json", evidence)


def _write_partial_round_summary(
    ctx: RunContext,
    *,
    live_round: int,
    trace_round: int,
    phase: str,
    error: BaseException,
    terminal: Mapping[str, Any] | None = None,
) -> None:
    """Write a schema-complete summary even when a round stops mid-phase."""

    row = {
        "run_id": ctx.run_id,
        "live_round": live_round,
        "trace_round": trace_round,
        "phase": phase,
        "reached_target": False,
        "finalValidationOk": False,
        "makespan_seconds": None,
        "planner_makespan_seconds": _planner_makespan_seconds(terminal or {}),
        "action_count": len(collectors.flatten_action_plan(terminal or {})),
        "action_counts": {},
        "strategy_counts": {},
        "failure_count": 1,
        "peak_active_gpu_count": None,
        "headroom": None,
        "capacity_uncertain_events": None,
    }
    summary_path = ctx.output_dir / "round_summary.csv"
    existing: list[Mapping[str, Any]] = []
    if summary_path.exists() and summary_path.stat().st_size:
        with summary_path.open(encoding="utf-8", newline="") as stream:
            existing = list(csv.DictReader(stream))
    if not any(_int(item.get("live_round")) == live_round for item in existing):
        _append_csv(summary_path, [row], ROUND_SUMMARY_FIELDS)
    _json_output(ctx.output_dir / "plans" / f"r{live_round:02d}_round_summary.json", {
        **row,
        "error_type": type(error).__name__,
        "error": str(error),
        "partial": True,
    })


def _record_requests(ctx: RunContext, live_round: int, trace_round: int, phase: str, rows: Sequence[Mapping[str, Any]], accounting: Mapping[str, Any]) -> None:
    enriched = [{"run_id": ctx.run_id, "live_round": live_round, "trace_round": trace_round, "phase": phase, **dict(row)} for row in rows]
    _append_jsonl(ctx.output_dir / "requests.jsonl", enriched)
    summary_rows = []
    by_workload = {key: {"scheduled": 0, "successes": 0, "failures": 0, "timeouts": 0, "pending": 0} for key in WORKLOAD_KEYS}
    for row in rows:
        workload = str(row.get("workload", ""))
        if workload in by_workload:
            by_workload[workload]["scheduled"] += 1
            status = str(row.get("status", ""))
            if status == "success": by_workload[workload]["successes"] += 1
            if status in {"failure", "rejected_pending_bound"}: by_workload[workload]["failures"] += 1
            if status == "timeout": by_workload[workload]["timeouts"] += 1
    for workload, values in by_workload.items():
        workload_accounting = _mapping(_mapping(accounting.get("by_workload")).get(workload))
        values["pending"] = _int(workload_accounting.get("pending"))
        summary_rows.append({"run_id": ctx.run_id, "live_round": live_round, "trace_round": trace_round, "phase": phase, "workload": workload, **values})
    summary_path = ctx.output_dir / "transition_requests_summary.csv"
    existing = []
    if summary_path.stat().st_size:
        with summary_path.open(encoding="utf-8", newline="") as stream:
            existing = list(csv.DictReader(stream))
    _csv_output(summary_path, [*existing, *summary_rows], TRANSITION_REQUEST_FIELDS)


def execute_experiment(
    ctx: RunContext,
    args: argparse.Namespace,
    kube: Kubectl,
    router: Router,
    demand: Sequence[Mapping[str, Any]],
    catalog: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    start_round = int(getattr(args, "start_round", 1))
    end_round = int(getattr(args, "end_round", ROUND_COUNT))
    previous_rates = {key: 0.0 for key in WORKLOAD_KEYS} if start_round == 1 else demand_rates(demand[start_round - 2])
    completed_rounds = 0
    failure: str | None = None
    expected_source_signature: dict[str, Any] | None = None
    controller_names = tuple(args.controller_names)
    for live_round, raw_target in enumerate(demand, 1):
        if live_round < start_round or live_round > end_round:
            continue
        trace_round = _int(raw_target.get("round"))
        target_rates = demand_rates(raw_target)
        source_rates = dict(previous_rates)
        phase = "R1" if live_round == 1 else f"R{live_round}"
        plan_name: str | None = None
        terminal: dict[str, Any] | None = None
        profile_report: dict[str, Any] | None = None
        try:
            current_preflight = preflight(
                kube,
                router,
                controllers=controller_names,
                require_empty=(live_round == 1),
            )
            current_preflight["pods"] = kube.get_json("pods")
            observed_source_signature = source_state_signature(
                _mapping(current_preflight.get("registry")),
                _mapping(current_preflight.get("routes")),
            )
            source_check = {
                "expected": expected_source_signature,
                "observed": observed_source_signature,
                "matches": expected_source_signature is None or expected_source_signature == observed_source_signature,
            }
            current_preflight["sourceCheck"] = source_check
            _json_output(ctx.output_dir / "snapshots" / f"r{live_round:02d}_before_preflight.json", current_preflight)
            if not current_preflight["ok"]:
                raise RuntimeError("preflight failed before " + phase + ": " + "; ".join(current_preflight["errors"]))
            if not source_check["matches"]:
                raise RuntimeError(f"{phase} source layout/runtime/route/batch drifted after the previous round")
            paired: dict[str, Sequence[Mapping[str, Any]]] = {}
            source_rows: list[dict[str, Any]] = []
            source_accounting: Mapping[str, Any] = {"scheduled": 0}
            if not ctx.makespan_mode:
                paired = traffic.build_paired_request_plans(
                    target_rates if live_round == 1 else {key: min(source_rates[key], target_rates[key]) for key in WORKLOAD_KEYS},
                    ctx.source_control_seconds,
                    seed=TRAFFIC_SEED,
                    sequence_prefix=f"r{live_round}:",
                )
            if live_round > 1 and not ctx.makespan_mode:
                # Source control precedes planning so the planner observes the
                # same settled source that the paired measurement describes.
                source_rows, source_accounting = _run_sender(paired["source_control"], ctx.router_url, ctx.stop_event)
                router.wait_drained(timeout=900.0)
                _record_planned_requests(ctx, live_round, trace_round, "source_control", paired["source_control"])
                _record_requests(ctx, live_round, trace_round, "source_control", source_rows, source_accounting)
            snapshot_name = snapshot_name_for_run(ctx.run_id, live_round)
            snapshot = build_arrival_snapshot(
                snapshot_name,
                live_round,
                source_rates,
                target_rates,
                namespace=ctx.namespace,
                placement_nodes=args.placement_nodes,
                stage3_variant=args.stage3_variant,
                **planning_knobs(args),
            )
            _json_output(ctx.output_dir / "snapshots" / f"r{live_round:02d}_arrival_snapshot.json", snapshot)
            kube.apply(snapshot)
            plan_name = "plan-" + snapshot_name
            plan = wait_for_plan(kube, plan_name, timeout=args.watchdog_seconds, poll_seconds=args.poll_seconds)
            _json_output(ctx.output_dir / "snapshots" / f"r{live_round:02d}_plan_observed.json", plan)
            registry = kube.get_json("physicalgpuregistries", "default")
            audit = audit_plan(
                plan,
                registry,
                source_gpu_count=active_gpu_count(registry),
                require_nonzero_target=(
                    live_round == 1
                    and sum(float(value) for value in target_rates.values()) > 0.0
                ),
                expected_stage3_variant=args.stage3_variant,
            )
            _save_plan_and_audit(ctx, live_round, plan, audit)
            # Persist the observed plan before any audit/approval failure can
            # discard the only action definitions returned by the controller.
            _record_plan_artifacts(ctx, live_round, trace_round, plan, registry)
            if not audit["ok"]:
                raise RuntimeError("plan audit failed: " + "; ".join(audit["errors"]))
            source_rows, source_accounting = ([], {"scheduled": 0}) if live_round == 1 else (source_rows, source_accounting)
            commitment = {key: min(source_rates[key], target_rates[key]) for key in WORKLOAD_KEYS}
            transition_sender, transition_thread = (
                _start_continuous_transition_sender(
                    paired["transition"],
                    commitment,
                    ctx.router_url,
                    ctx.stop_event,
                    window_seconds=ctx.source_control_seconds,
                )
                if live_round > 1 and not ctx.makespan_mode else (None, None)
            )
            wait_error: BaseException | None = None
            traffic_stop_error: BaseException | None = None
            try:
                kube.approve(plan_name)
                terminal = wait_for_plan(
                    kube,
                    plan_name,
                    timeout=args.watchdog_seconds,
                    poll_seconds=args.poll_seconds,
                    accept_planned=False,
                )
            except BaseException as exc:
                wait_error = exc
            finally:
                if transition_sender is not None:
                    try:
                        transition_rows, transition_accounting = stop_and_drain(transition_sender, transition_thread)
                        router.wait_drained(timeout=900.0)
                        _record_planned_requests(ctx, live_round, trace_round, "transition", transition_rows)
                        _record_requests(ctx, live_round, trace_round, "transition", transition_rows, transition_accounting)
                    except BaseException as exc:
                        traffic_stop_error = exc
            if terminal is None:
                try:
                    terminal = kube.get_json("migactionplans", plan_name)
                except BaseException:
                    terminal = None
            if terminal:
                _json_output(ctx.output_dir / "plans" / f"r{live_round:02d}_terminal_plan.json", terminal)
                _record_plan_artifacts(ctx, live_round, trace_round, terminal, registry)
            if wait_error is not None:
                raise wait_error
            if traffic_stop_error is not None:
                raise traffic_stop_error
            if terminal is None:
                raise RuntimeError(f"{phase} plan terminal state was not observable")
            if str(_mapping(terminal.get("status")).get("phase")) not in TERMINAL_PHASES:
                raise RuntimeError(f"{phase} plan ended in non-terminal success phase: {_mapping(terminal.get('status')).get('phase')}")
            final_validation, final_registry, final_routes = wait_independent_final_validation(
                kube,
                router,
                terminal,
                timeout=60.0,
                poll_seconds=args.poll_seconds,
            )
            _json_output(ctx.output_dir / "snapshots" / f"r{live_round:02d}_after_transition.json", {"plan": terminal, "registry": final_registry, "routes": final_routes, "finalValidation": final_validation})
            if not final_validation["ok"]:
                raise RuntimeError("independent final validation failed: " + "; ".join(final_validation["errors"]))
            if ctx.makespan_mode:
                dwell_started_at = utc_now()
                if ctx.post_target_dwell_seconds:
                    time.sleep(ctx.post_target_dwell_seconds)
                profile_report = {
                    "status": "skipped_no_profile",
                    "reason": "transition makespan mode disables traffic and in-place profiling",
                    "samples": [],
                    "replicas": [],
                    "postTargetDwellSeconds": ctx.post_target_dwell_seconds,
                    "dwellStartedAt": dwell_started_at,
                    "dwellFinishedAt": utc_now(),
                }
            else:
                target_plan = traffic.build_request_plan(target_rates, ctx.target_steady_seconds, phase="target_steady", seed=TRAFFIC_SEED, sequence_prefix=f"r{live_round}:target:")
                _record_planned_requests(ctx, live_round, trace_round, "target_steady", target_plan)
                target_rows, target_accounting = _run_sender(target_plan, ctx.router_url, ctx.stop_event)
                router.wait_drained(timeout=900.0)
                _record_requests(ctx, live_round, trace_round, "target_steady", target_rows, target_accounting)
                profile_report = _profile_target(router.routes(), ctx.profile_seconds)
            _json_output(ctx.output_dir / "snapshots" / f"r{live_round:02d}_profile.json", profile_report)
            if not ctx.makespan_mode and profile_report.get("status") != "ok":
                raise RuntimeError("target profile failed: " + "; ".join(str(item) for item in profile_report.get("errors", [])))
            after_pods = kube.get_json("pods")
            after_registry = kube.get_json("physicalgpuregistries", "default")
            after_routes = router.get_json("/routes")
            _json_output(ctx.output_dir / "snapshots" / f"r{live_round:02d}_after.json", {
                "registry": after_registry,
                "routes": after_routes,
                "pods": after_pods,
            })
            _merge_observed_pod_images(ctx, after_pods)
            _record_round_artifacts(
                ctx,
                live_round,
                trace_round,
                terminal,
                registry,
                _items(_mapping(current_preflight.get("routes")).get("routes")),
                _items(final_routes.get("routes")),
                profile_report,
                source_rates,
                target_rates,
                catalog,
            )
            completed_rounds += 1
            previous_rates = target_rates
            expected_source_signature = source_state_signature(after_registry, after_routes)
        except BaseException as exc:
            failure = f"{phase}: {exc}"
            ctx.stop_event.set()
            _capture_round_failure(
                ctx,
                kube,
                router,
                live_round=live_round,
                trace_round=trace_round,
                phase=phase,
                error=exc,
                plan_name=plan_name,
                terminal=terminal,
                profile_report=profile_report,
            )
            _write_partial_round_summary(
                ctx,
                live_round=live_round,
                trace_round=trace_round,
                phase=phase,
                error=exc,
                terminal=terminal,
            )
            _json_output(ctx.output_dir / "failure.json", {"run_id": ctx.run_id, "at": utc_now(), "phase": phase, "error": failure, "safe_stop": "new traffic stopped; partial outputs preserved"})
            break
    cleanup_summary: dict[str, Any] | None = None
    if failure is None and end_round == ROUND_COUNT:
        try:
            cleanup_summary = execute_r13_cleanup(ctx, args, kube, router, demand)
            if cleanup_summary.get("ok") is not True:
                failure = "R13 cleanup did not leave the cluster empty: " + "; ".join(
                    str(item) for item in _items(cleanup_summary.get("errors"))
                )
        except BaseException as exc:
            failure = f"R13 cleanup: {exc}"
            cleanup_summary = {
                "runId": ctx.run_id,
                "round": 13,
                "purpose": "explicit-zero-demand-cleanup",
                "includedInMeasuredTwelveRoundAggregate": False,
                "ok": False,
                "errors": [str(exc)],
            }
            _json_output(ctx.output_dir / "r13_cleanup_summary.json", cleanup_summary)
    expected_rounds = end_round - start_round + 1
    result = {
        "run_id": ctx.run_id,
        "completed_rounds": completed_rounds,
        "expected_rounds": expected_rounds,
        "ok": failure is None and completed_rounds == expected_rounds,
        "failure": failure,
        "range_run": start_round != 1 or end_round != ROUND_COUNT,
        "r13_cleanup": cleanup_summary,
    }
    return _validate_and_record_outputs(ctx, result)


E1_WINDOW_FIELDS = [
    "run_id", "live_round", "trace_round", "stage3_variant",
    "transition_switch_offset", "transition_switch_utc",
    "executor_started_at", "executor_finished_at",
    "steady_switch_offset", "steady_switch_utc", "completion_detect_lag_seconds",
    "dwell_seconds", "dwell_end_offset", "commitment_json", "target_json",
]


def _utc_seconds(value: Any) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _e1_warmup_step(
    ctx: RunContext,
    args: argparse.Namespace,
    kube: Kubectl,
    router: Router,
    *,
    label: str,
    number: int,
    source_rates: Mapping[str, float],
    target_rates: Mapping[str, float],
    require_empty: bool,
) -> dict[str, Any]:
    """Run one untrafficked transition used only to warm the system."""

    before = preflight(kube, router, controllers=tuple(args.controller_names), require_empty=require_empty)
    if not before["ok"]:
        raise RuntimeError(f"warm-up {label} preflight failed: " + "; ".join(before["errors"]))
    snapshot_name = f"{snapshot_name_for_run(ctx.run_id, number)}-warmup"
    snapshot = build_arrival_snapshot(
        snapshot_name, number, dict(source_rates), dict(target_rates),
        namespace=ctx.namespace, placement_nodes=args.placement_nodes,
        stage3_variant=args.stage3_variant, **planning_knobs(args),
    )
    snapshot["spec"]["triggerReason"] = f"e1-warmup-{label}"
    _json_output(ctx.output_dir / "warmup" / f"{label}_arrival_snapshot.json", snapshot)
    kube.apply(snapshot)
    plan_name = "plan-" + snapshot_name
    plan = wait_for_plan(kube, plan_name, timeout=args.watchdog_seconds, poll_seconds=ctx.e1_poll_seconds)
    registry = kube.get_json("physicalgpuregistries", "default")
    audit = audit_plan(plan, registry, source_gpu_count=active_gpu_count(registry),
                       require_nonzero_target=False, expected_stage3_variant=args.stage3_variant)
    _json_output(ctx.output_dir / "warmup" / f"{label}_plan.json", plan)
    if not audit["ok"]:
        raise RuntimeError(f"warm-up {label} plan audit failed: " + "; ".join(audit["errors"]))
    kube.approve(plan_name)
    terminal = wait_for_plan(kube, plan_name, timeout=args.watchdog_seconds,
                             poll_seconds=ctx.e1_poll_seconds, accept_planned=False)
    _json_output(ctx.output_dir / "warmup" / f"{label}_terminal_plan.json", terminal)
    if str(_mapping(terminal.get("status")).get("phase")) not in TERMINAL_PHASES:
        raise RuntimeError(f"warm-up {label} plan did not succeed: {_mapping(terminal.get('status')).get('message')}")
    validation, _, _ = wait_independent_final_validation(kube, router, terminal, timeout=60.0, poll_seconds=ctx.e1_poll_seconds)
    if not validation.get("ok"):
        raise RuntimeError(f"warm-up {label} final validation failed: " + "; ".join(str(e) for e in _items(validation.get("errors"))))
    return {"label": label, "plan": plan_name, "makespanSeconds": _planner_makespan_seconds(terminal),
            "actionCount": len(_items(_mapping(terminal.get("status")).get("actionStatuses")))}


def e1_warmup(ctx: RunContext, args: argparse.Namespace, kube: Kubectl, router: Router,
              demand: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Warm every path R1 uses (images, model page cache, planner, executor,
    MIG/CDI, containers) with empty -> R1 target -> empty, without traffic.

    Excluded from all measured outputs; saved under warmup/.
    """

    (ctx.output_dir / "warmup").mkdir(exist_ok=True)
    zero = {key: 0.0 for key in WORKLOAD_KEYS}
    r1 = demand_rates(demand[int(getattr(args, "e1_first_round", 1)) - 1])
    started = utc_now()
    steps = [
        _e1_warmup_step(ctx, args, kube, router, label="up", number=90, source_rates=zero,
                        target_rates=r1, require_empty=True),
        _e1_warmup_step(ctx, args, kube, router, label="down", number=91, source_rates=r1,
                        target_rates=zero, require_empty=False),
    ]
    summary = {"runId": ctx.run_id, "purpose": "e1-warmup-excluded-from-measurement",
               "startedAt": started, "finishedAt": utc_now(), "steps": steps}
    _json_output(ctx.output_dir / "warmup" / "warmup_summary.json", summary)
    return summary


def execute_e1_experiment(
    ctx: RunContext,
    args: argparse.Namespace,
    kube: Kubectl,
    router: Router,
    demand: Sequence[Mapping[str, Any]],
    catalog: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """E1: R1 plus 11 transitions under continuous open-loop traffic.

    One generator runs for the whole run.  At each approval its rates switch
    to the commitment min(D_old, D_new); when the executor reports the plan
    terminal they switch to D_new for ``ctx.dwell_seconds`` before the next
    round is planned from the observed source.  No drain, source control or
    profiling happens between rounds.  Requests keep the round/window they
    were scheduled in.  After R12 the generator stops, in-flight requests
    finish, and the zero-demand R13 cleanup empties the cluster.
    """

    zero = {key: 0.0 for key in WORKLOAD_KEYS}
    # --e1-first-round N skips R1..R(N-1): RN is deployed from empty, as R1 is.
    first_round = int(getattr(args, "e1_first_round", 1))
    expected_rounds = ROUND_COUNT - first_round + 1
    if getattr(args, "e1_warmup", True):
        e1_warmup(ctx, args, kube, router, demand)
    sender = traffic.BoundedAsyncSender(traffic.urllib_transport(ctx.router_url, timeout_s=900.0))
    generator = traffic.ContinuousRateSender(sender, seed=TRAFFIC_SEED, arrivals=getattr(args, "arrivals", "fixed"))
    generator.start()
    previous_rates = dict(zero)
    completed_rounds = 0
    failure: str | None = None
    expected_source_signature: dict[str, Any] | None = None
    controller_names = tuple(args.controller_names)
    windows: list[dict[str, Any]] = []
    trace_rounds: dict[int, int] = {}
    try:
        for live_round, raw_target in enumerate(demand, 1):
            if live_round < first_round:
                continue
            trace_round = _int(raw_target.get("round"))
            trace_rounds[live_round] = trace_round
            target_rates = demand_rates(raw_target)
            source_rates = dict(previous_rates)
            phase = f"R{live_round}"
            plan_name: str | None = None
            terminal: dict[str, Any] | None = None
            try:
                current_preflight = preflight(
                    kube, router, controllers=controller_names,
                    require_empty=(live_round == first_round), allow_inflight=(live_round > first_round),
                )
                current_preflight["pods"] = kube.get_json("pods")
                observed_source_signature = source_state_signature(
                    _mapping(current_preflight.get("registry")), _mapping(current_preflight.get("routes")),
                )
                source_check = {
                    "expected": expected_source_signature,
                    "observed": observed_source_signature,
                    "matches": expected_source_signature is None or expected_source_signature == observed_source_signature,
                }
                current_preflight["sourceCheck"] = source_check
                _json_output(ctx.output_dir / "snapshots" / f"r{live_round:02d}_before_preflight.json", current_preflight)
                if not current_preflight["ok"]:
                    raise RuntimeError("preflight failed before " + phase + ": " + "; ".join(current_preflight["errors"]))
                if not source_check["matches"]:
                    raise RuntimeError(f"{phase} source layout/runtime/route/batch drifted after the previous round")
                snapshot_name = snapshot_name_for_run(ctx.run_id, live_round)
                snapshot = build_arrival_snapshot(
                    snapshot_name, live_round, source_rates, target_rates,
                    namespace=ctx.namespace, placement_nodes=args.placement_nodes,
                    stage3_variant=args.stage3_variant, **planning_knobs(args),
                )
                _json_output(ctx.output_dir / "snapshots" / f"r{live_round:02d}_arrival_snapshot.json", snapshot)
                kube.apply(snapshot)
                plan_name = "plan-" + snapshot_name
                plan = wait_for_plan(kube, plan_name, timeout=args.watchdog_seconds, poll_seconds=ctx.e1_poll_seconds)
                _json_output(ctx.output_dir / "snapshots" / f"r{live_round:02d}_plan_observed.json", plan)
                registry = kube.get_json("physicalgpuregistries", "default")
                audit = audit_plan(
                    plan, registry, source_gpu_count=active_gpu_count(registry),
                    require_nonzero_target=(live_round == first_round and sum(float(v) for v in target_rates.values()) > 0.0),
                    expected_stage3_variant=args.stage3_variant,
                )
                _save_plan_and_audit(ctx, live_round, plan, audit)
                _record_plan_artifacts(ctx, live_round, trace_round, plan, registry)
                if not audit["ok"]:
                    raise RuntimeError("plan audit failed: " + "; ".join(audit["errors"]))
                commitment = {key: min(source_rates[key], target_rates[key]) for key in WORKLOAD_KEYS}
                switch_in = generator.set_rates(commitment, live_round=live_round, window="transition")
                kube.approve(plan_name)
                terminal = wait_for_plan(
                    kube, plan_name, timeout=args.watchdog_seconds,
                    poll_seconds=ctx.e1_poll_seconds, accept_planned=False,
                )
                status = _mapping(terminal.get("status"))
                if str(status.get("phase")) not in TERMINAL_PHASES:
                    raise RuntimeError(f"{phase} plan ended in phase {status.get('phase')}: {status.get('message')}")
                switch_out = generator.set_rates(target_rates, live_round=live_round, window="steady")
                _json_output(ctx.output_dir / "plans" / f"r{live_round:02d}_terminal_plan.json", terminal)
                _record_plan_artifacts(ctx, live_round, trace_round, terminal, registry)
                final_validation, final_registry, final_routes = wait_independent_final_validation(
                    kube, router, terminal, timeout=60.0, poll_seconds=ctx.e1_poll_seconds,
                )
                _json_output(ctx.output_dir / "snapshots" / f"r{live_round:02d}_after_transition.json", {
                    "plan": terminal, "registry": final_registry, "routes": final_routes, "finalValidation": final_validation,
                })
                if not final_validation["ok"]:
                    raise RuntimeError("independent final validation failed: " + "; ".join(final_validation["errors"]))
                remaining = ctx.dwell_seconds - (sender._clock() - sender._start - float(switch_out["switched_at_offset"]))
                if remaining > 0:
                    time.sleep(remaining)
                timestamps = _mapping(_mapping(status.get("transitionExecution")).get("timestamps"))
                finished_at = _utc_seconds(timestamps.get("executorFinishedAt"))
                windows.append({
                    "run_id": ctx.run_id, "live_round": live_round, "trace_round": trace_round,
                    "stage3_variant": args.stage3_variant,
                    "transition_switch_offset": switch_in["switched_at_offset"],
                    "transition_switch_utc": datetime.fromtimestamp(switch_in["switched_at_utc"], timezone.utc).isoformat(),
                    "executor_started_at": timestamps.get("executorStartedAt"),
                    "executor_finished_at": timestamps.get("executorFinishedAt"),
                    "steady_switch_offset": switch_out["switched_at_offset"],
                    "steady_switch_utc": datetime.fromtimestamp(switch_out["switched_at_utc"], timezone.utc).isoformat(),
                    "completion_detect_lag_seconds": (switch_out["switched_at_utc"] - finished_at) if finished_at else None,
                    "dwell_seconds": ctx.dwell_seconds,
                    "dwell_end_offset": sender._clock() - sender._start,
                    "commitment_json": json.dumps(commitment, sort_keys=True),
                    "target_json": json.dumps(target_rates, sort_keys=True),
                })
                _csv_output(ctx.output_dir / "e1_windows.csv", windows, E1_WINDOW_FIELDS)
                after_registry = kube.get_json("physicalgpuregistries", "default")
                after_routes = router.get_json("/routes")
                after_pods = kube.get_json("pods")
                _json_output(ctx.output_dir / "snapshots" / f"r{live_round:02d}_after.json", {
                    "registry": after_registry, "routes": after_routes, "pods": after_pods,
                })
                _merge_observed_pod_images(ctx, after_pods)
                profile_report = {
                    "status": "skipped_no_profile",
                    "reason": "E1 measures serving under continuous traffic; no in-place profiling",
                    "samples": [], "replicas": [],
                }
                _json_output(ctx.output_dir / "snapshots" / f"r{live_round:02d}_profile.json", profile_report)
                _record_round_artifacts(
                    ctx, live_round, trace_round, terminal, registry,
                    _items(_mapping(current_preflight.get("routes")).get("routes")),
                    _items(final_routes.get("routes")),
                    profile_report, source_rates, target_rates, catalog,
                )
                completed_rounds += 1
                previous_rates = target_rates
                expected_source_signature = source_state_signature(after_registry, after_routes)
            except BaseException as exc:
                failure = f"{phase}: {exc}"
                generator.set_rates(zero, live_round=live_round, window="stopped_on_failure")
                ctx.stop_event.set()
                _capture_round_failure(
                    ctx, kube, router, live_round=live_round, trace_round=trace_round, phase=phase,
                    error=exc, plan_name=plan_name, terminal=terminal, profile_report=None,
                )
                _write_partial_round_summary(ctx, live_round=live_round, trace_round=trace_round, phase=phase, error=exc, terminal=terminal)
                _json_output(ctx.output_dir / "failure.json", {"run_id": ctx.run_id, "at": utc_now(), "phase": phase, "error": failure, "safe_stop": "new traffic stopped; partial outputs preserved"})
                break
    finally:
        generator.set_rates(zero, live_round=ROUND_COUNT + 1, window="stopped")
        generator.stop()
        rows = sender.drain(timeout_s=900.0)
        accounting = sender.accounting()
        sender.shutdown()
        _jsonl_output(ctx.output_dir / "e1_rate_events.jsonl", generator.events())
        by_group: dict[tuple[int, str], list[dict[str, Any]]] = {}
        for row in rows:
            by_group.setdefault((_int(row.get("live_round")), str(row.get("phase"))), []).append(row)
        for (live_round, window), group in sorted(by_group.items()):
            _record_planned_requests(ctx, live_round, trace_rounds.get(live_round, 0), window, group)
            _record_requests(ctx, live_round, trace_rounds.get(live_round, 0), window, group, accounting)
        _json_output(ctx.output_dir / "e1_traffic_accounting.json", accounting)
    cleanup_summary: dict[str, Any] | None = None
    if failure is None:
        try:
            router.wait_drained(timeout=900.0)
            cleanup_summary = execute_r13_cleanup(ctx, args, kube, router, demand)
            if cleanup_summary.get("ok") is not True:
                failure = "R13 cleanup did not leave the cluster empty: " + "; ".join(str(item) for item in _items(cleanup_summary.get("errors")))
        except BaseException as exc:
            failure = f"R13 cleanup: {exc}"
            cleanup_summary = {"runId": ctx.run_id, "round": 13, "purpose": "explicit-zero-demand-cleanup",
                               "includedInMeasuredTwelveRoundAggregate": False, "ok": False, "errors": [str(exc)]}
            _json_output(ctx.output_dir / "r13_cleanup_summary.json", cleanup_summary)
    if completed_rounds == expected_rounds:
        # Post-run strict audit (target vs routes, action completion, executor
        # validation, same-GPU distinct-slot parallelism); writes
        # strict_runtime_audit.json, which output validation requires.
        audit = subprocess.run(
            [sys.executable, str(ROOT / "audit_makespan_run.py"), str(ctx.output_dir), "--expected-rounds", str(expected_rounds)],
            text=True, capture_output=True, check=False,
        )
        if audit.returncode != 0 and failure is None:
            failure = "strict runtime audit failed: " + (audit.stderr or audit.stdout).strip()[-400:]
    result = {
        "run_id": ctx.run_id,
        "completed_rounds": completed_rounds,
        "expected_rounds": expected_rounds,
        "first_round": first_round,
        "ok": failure is None and completed_rounds == expected_rounds,
        "failure": failure,
        "range_run": False,
        "r13_cleanup": cleanup_summary,
        "experiment_mode": "e1_continuous_traffic",
        "stage3_variant": args.stage3_variant,
    }
    return _validate_and_record_outputs(ctx, result)


def execute_r13_cleanup(
    ctx: RunContext,
    args: argparse.Namespace,
    kube: Kubectl,
    router: Router,
    demand: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Execute the mandatory zero-demand cleanup after the measured R1-R12 run."""

    live_round = ROUND_COUNT + 1
    source_rates = demand_rates(demand[-1])
    target_rates = {key: 0.0 for key in WORKLOAD_KEYS}
    before = preflight(
        kube,
        router,
        controllers=tuple(args.controller_names),
        require_empty=False,
    )
    before["pods"] = kube.get_json("pods")
    _json_output(ctx.output_dir / "snapshots" / "r13_cleanup_before.json", before)
    if not before["ok"]:
        raise RuntimeError("R13 preflight failed: " + "; ".join(before["errors"]))

    snapshot_name = snapshot_name_for_run(ctx.run_id, live_round)
    snapshot = build_arrival_snapshot(
        snapshot_name,
        live_round,
        source_rates,
        target_rates,
        namespace=ctx.namespace,
        placement_nodes=args.placement_nodes,
    )
    snapshot["spec"]["triggerReason"] = "explicit-r13-cleanup"
    snapshot["spec"]["notes"] = [
        "R13 is a mandatory zero-demand cleanup after measured R1-R12",
        "R13 makespan is reported separately and excluded from the 12-round aggregate",
    ]
    _json_output(ctx.output_dir / "snapshots" / "r13_cleanup_arrival_snapshot.json", snapshot)
    kube.apply(snapshot)
    plan_name = "plan-" + snapshot_name
    plan = wait_for_plan(kube, plan_name, timeout=args.watchdog_seconds, poll_seconds=args.poll_seconds)
    registry = kube.get_json("physicalgpuregistries", "default")
    audit = audit_plan(
        plan,
        registry,
        source_gpu_count=active_gpu_count(registry),
        require_nonzero_target=False,
    )
    _json_output(ctx.output_dir / "plans" / "r13_cleanup_plan.json", plan)
    _json_output(ctx.output_dir / "plans" / "r13_cleanup_audit.json", audit)
    if not audit["ok"]:
        raise RuntimeError("R13 plan audit failed: " + "; ".join(audit["errors"]))

    kube.approve(plan_name)
    terminal = wait_for_plan(
        kube,
        plan_name,
        timeout=args.watchdog_seconds,
        poll_seconds=args.poll_seconds,
        accept_planned=False,
    )
    _json_output(ctx.output_dir / "plans" / "r13_cleanup_terminal_plan.json", terminal)
    if str(_mapping(terminal.get("status")).get("phase")) not in TERMINAL_PHASES:
        raise RuntimeError("R13 cleanup plan did not succeed")

    validation, final_registry, final_routes = wait_independent_final_validation(
        kube,
        router,
        terminal,
        timeout=60.0,
        poll_seconds=args.poll_seconds,
    )
    all_pods = kube.get_json("pods")
    runtime_pods = [
        pod for pod in _items(all_pods.get("items"))
        if _mapping(_mapping(pod).get("metadata")).get("labels", {}).get("app.kubernetes.io/name")
        == "migrant-model-runtime"
    ]
    queue_counts = _mapping(_mapping(final_registry.get("status")).get("queueCounts"))
    routes = _items(_mapping(final_routes).get("routes"))
    errors = list(_items(validation.get("errors")))
    if _int(queue_counts.get("active"), -1) != 0:
        errors.append(f"registry active={queue_counts.get('active')}, want 0")
    if _int(queue_counts.get("transitioning"), -1) != 0:
        errors.append(f"registry transitioning={queue_counts.get('transitioning')}, want 0")
    if _int(queue_counts.get("available"), -1) != MAX_PHYSICAL_GPUS:
        errors.append(f"registry available={queue_counts.get('available')}, want {MAX_PHYSICAL_GPUS}")
    if routes:
        errors.append(f"router still has {len(routes)} routes")
    if runtime_pods:
        errors.append(f"cluster still has {len(runtime_pods)} runtime pods")

    action_statuses = _items(_mapping(terminal.get("status")).get("actionStatuses"))
    action_counts: dict[str, int] = {}
    for item in action_statuses:
        action_type = str(_mapping(item).get("type", "unknown"))
        action_counts[action_type] = action_counts.get(action_type, 0) + 1
    summary = {
        "runId": ctx.run_id,
        "round": live_round,
        "purpose": "explicit-zero-demand-cleanup",
        "includedInMeasuredTwelveRoundAggregate": False,
        "ok": validation.get("ok") is True and not errors,
        "errors": errors,
        "planName": plan_name,
        "planPhase": _mapping(terminal.get("status")).get("phase"),
        "makespanSeconds": _planner_makespan_seconds(terminal),
        "actionCount": len(action_statuses),
        "actionCounts": action_counts,
        "allActionsCompleted": all(str(_mapping(item).get("status")) == "completed" for item in action_statuses),
        "finalValidation": validation,
        "queueCounts": dict(queue_counts),
        "routeCount": len(routes),
        "runtimePodCount": len(runtime_pods),
    }
    _json_output(ctx.output_dir / "snapshots" / "r13_cleanup_after.json", {
        "registry": final_registry,
        "routes": final_routes,
        "runtimePods": runtime_pods,
        "finalValidation": validation,
    })
    _json_output(ctx.output_dir / "r13_cleanup_summary.json", summary)
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--execute", action="store_true", help="enable cluster mutations and live traffic")
    mode.add_argument(
        "--preflight-only",
        action="store_true",
        help="perform read-only cluster checks and write readiness artifacts without creating plans or traffic",
    )
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    parser.add_argument("--router-url", default=DEFAULT_ROUTER_URL)
    parser.add_argument("--output-root", type=Path, default=ROOT / "cluster_results")
    parser.add_argument(
        "--resume-run-dir", type=Path,
        help="continue a previously initialized run directory; requires --execute and --start-round > 1",
    )
    parser.add_argument("--source-control-seconds", type=float, default=60.0)
    parser.add_argument("--target-steady-seconds", type=float, default=30.0)
    parser.add_argument("--profile-seconds", type=float, default=60.0)
    parser.add_argument(
        "--makespan-mode", action="store_true",
        help="run only transition convergence: no source/transition/target traffic and no profile; retain validation and collectors",
    )
    parser.add_argument(
        "--post-target-dwell-seconds", type=float, default=5.0,
        help="idle dwell after strict target validation in --makespan-mode (default: 5)",
    )
    parser.add_argument(
        "--e1", action="store_true",
        help="E1: continuous open-loop traffic; commitment during each transition, new demand for --dwell-seconds after completion; no drain/source-control/profile",
    )
    parser.add_argument("--dwell-seconds", type=float, default=30.0, help="E1 new-demand dwell after each transition (default: 30)")
    parser.add_argument(
        "--no-e1-warmup", dest="e1_warmup", action="store_false",
        help="skip the untrafficked empty->R1->empty warm-up before E1's R1 (default: run it)",
    )
    parser.add_argument("--capacity-headroom", type=float, default=None,
                        help="planner provisions for (1 + h) x demand, lowered per round to fit the GPU budget (default: off)")
    parser.add_argument("--conservative-3g-mu", action="store_true",
                        help="planner Stage 1 uses min(mu_3g, mu_4g) for 3g options (default: off)")
    parser.add_argument("--e1-first-round", type=int, default=1,
                        help="E1: first live round; it is deployed from empty and earlier rounds are skipped (default: 1)")
    parser.add_argument("--arrivals", choices=["fixed", "poisson"], default="fixed",
                        help="E1 inter-arrival times: fixed 1/d, or exponential with mean 1/d (default: fixed)")
    parser.add_argument("--catalog", default="catalog.csv", help="ledger catalog file in the experiment directory (default: frozen catalog.csv)")
    parser.add_argument("--watchdog-seconds", type=float, default=1800.0)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--start-round", type=int, default=1, help="first live round to execute; default runs from R1")
    parser.add_argument("--end-round", type=int, default=ROUND_COUNT, help="last live round to execute; default runs through R12")
    parser.add_argument("--placement-node", dest="placement_nodes", action="append", default=[])
    parser.add_argument(
        "--stage3-variant",
        choices=("slicewise", "sw-c"),
        default="slicewise",
        help="Stage 3 dependency variant; sw-c is an intentionally unsafe negative control",
    )
    parser.add_argument(
        "--allow-unsafe-sw-c",
        action="store_true",
        help="required with --execute --stage3-variant sw-c",
    )
    parser.add_argument("--controller-name", dest="controller_names", action="append", default=["planner-controller", "transition-executor", "cluster-state-manager", "runtime-router"])
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.execute and args.stage3_variant == "sw-c" and not args.allow_unsafe_sw_c:
        print("SW-C execution requires --allow-unsafe-sw-c", file=sys.stderr)
        return 2
    if args.e1 and args.makespan_mode:
        print("--e1 and --makespan-mode are exclusive", file=sys.stderr)
        return 2
    if args.e1 and args.dwell_seconds <= 0:
        print("--dwell-seconds must be positive", file=sys.stderr)
        return 2
    if not args.makespan_mode and min(args.source_control_seconds, args.target_steady_seconds, args.profile_seconds) <= 0:
        print("duration overrides must be positive", file=sys.stderr)
        return 2
    if args.post_target_dwell_seconds < 0:
        print("--post-target-dwell-seconds must be non-negative", file=sys.stderr)
        return 2
    if args.start_round < 1 or args.end_round > ROUND_COUNT or args.start_round > args.end_round:
        print(f"round range must satisfy 1 <= start <= end <= {ROUND_COUNT}", file=sys.stderr)
        return 2
    if args.resume_run_dir is not None and (not args.execute or args.start_round <= 1):
        print("--resume-run-dir requires --execute and --start-round > 1", file=sys.stderr)
        return 2
    ctx: RunContext | None = None
    try:
        validate_workload_contract()
        demand, catalog, hashes = load_frozen_inputs(catalog_file=args.catalog)
        ctx = make_run_context(args, args.output_root)
        if args.resume_run_dir is None:
            _write_initial_outputs(ctx, args, hashes)
        else:
            _json_output(ctx.output_dir / "resume.json", {
                "run_id": ctx.run_id,
                "resumed_at": utc_now(),
                "start_round": args.start_round,
                "end_round": args.end_round,
                "makespan_mode": ctx.makespan_mode,
                "post_target_dwell_seconds": ctx.post_target_dwell_seconds,
            })
        kube = Kubectl(args.namespace)
        if not args.router_url:
            raise ValueError("--router-url (or OR_SIM_ROUTER_URL) is required for preflight")
        router = Router(args.router_url)
        readiness = preflight(
            kube,
            router,
            controllers=tuple(args.controller_names),
            require_empty=(args.start_round == 1),
        )
        environment = _enrich_environment(ctx, kube, readiness)
        environment_errors = environment_readiness_errors(environment)
        readiness["errors"] = [*_items(readiness.get("errors")), *environment_errors]
        readiness["ok"] = not readiness["errors"]
        _json_output(ctx.output_dir / "preflight.json", readiness)
        _write_readiness(ctx, readiness, environment)
        if not readiness["ok"] and args.execute:
            result = _validate_and_record_outputs(ctx, {
                "run_id": ctx.run_id,
                "completed_rounds": 0,
                "expected_rounds": ROUND_COUNT,
                "ok": False,
                "failure": "preflight failed: " + "; ".join(readiness["errors"]),
            })
            print(json.dumps({**result, "output": str(ctx.output_dir)}, sort_keys=True))
            return 1
        if not args.execute:
            print(json.dumps({"run_id": ctx.run_id, "preflight": readiness["ok"], "execute": False, "output": str(ctx.output_dir)}, sort_keys=True))
            return 0 if readiness["ok"] else 1
        runner_fn = execute_e1_experiment if ctx.e1_mode else execute_experiment
        result = runner_fn(ctx, args, kube, router, demand, catalog)
        print(json.dumps({**result, "output": str(ctx.output_dir)}, sort_keys=True))
        return 0 if result["ok"] else 1
    except (OSError, ValueError, RuntimeError, TimeoutError, json.JSONDecodeError) as exc:
        if ctx is not None and args.execute:
            _validate_and_record_outputs(ctx, {
                "run_id": ctx.run_id,
                "completed_rounds": 0,
                "expected_rounds": ROUND_COUNT,
                "ok": False,
                "failure": str(exc),
            })
        print(f"runner blocked safely: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
