from __future__ import annotations

import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


def observe_pod_assignments(
    pods: list[dict[str, Any]],
    logical_mig_slots: list[dict[str, Any]],
    configmaps: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    slots_by_uuid = {
        str(slot.get("migDeviceUuid") or ""): dict(slot)
        for slot in logical_mig_slots
        if slot.get("migDeviceUuid")
    }
    configmaps_by_name = {
        str(dict(configmap.get("metadata", {})).get("name") or ""): dict(configmap)
        for configmap in list(configmaps or [])
    }
    assignments = []
    unassigned = []
    for pod in pods:
        row = _pod_assignment_row(
            pod=pod,
            slots_by_uuid=slots_by_uuid,
            configmaps_by_name=configmaps_by_name,
        )
        if row is None:
            continue
        if row.get("logicalSlot") is None:
            unassigned.append(row)
        else:
            assignments.append(row)
    return {
        "podAssignments": assignments,
        "unassignedGpuPods": unassigned,
        "inflightByInstance": [
            {
                "podName": row.get("podName"),
                "workload": row.get("workload"),
                "inflight": row.get("inflight"),
            }
            for row in assignments
            if row.get("inflight") is not None
        ],
        "queuedByWorkload": _queued_by_workload(assignments),
    }


def _pod_assignment_row(
    pod: dict[str, Any],
    slots_by_uuid: dict[str, dict[str, Any]],
    configmaps_by_name: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    metadata = dict(pod.get("metadata", {}))
    spec = dict(pod.get("spec", {}))
    status = dict(pod.get("status", {}))
    labels = dict(metadata.get("labels", {}))
    annotations = dict(metadata.get("annotations", {}))
    pod_name = str(metadata.get("name") or "")
    namespace = str(metadata.get("namespace") or "")
    workload = labels.get("mig.or-sim.io/workload") or annotations.get("mig.or-sim.io/workload")
    actual_uuids = _csv_annotation(annotations.get("mig.or-sim.io/actual-mig-device-uuids"))
    expected_uuid = annotations.get("mig.or-sim.io/expected-mig-device-uuid")
    if expected_uuid and expected_uuid not in actual_uuids:
        actual_uuids.append(str(expected_uuid))
    if not actual_uuids and not workload:
        return None

    logical_slot = None
    matched_uuid = None
    for uuid in actual_uuids:
        if uuid in slots_by_uuid:
            logical_slot = dict(slots_by_uuid[uuid])
            matched_uuid = uuid
            break

    batch = annotations.get("mig.or-sim.io/batch-size")
    configmap_name = _first_configmap_volume_name(spec)
    if batch is None and configmap_name:
        data = dict(configmaps_by_name.get(configmap_name, {}).get("data", {}))
        batch = data.get("batch_size")

    endpoint = (
        annotations.get("mig.or-sim.io/endpoint")
        or annotations.get("or-sim.io/endpoint")
    )
    metrics = _runtime_metrics(annotations=annotations, endpoint=endpoint)
    accepting = _parse_bool_or_none(annotations.get("mig.or-sim.io/accepting-new"))
    inflight = _parse_int_or_none(annotations.get("mig.or-sim.io/inflight"))
    queued = _parse_int_or_none(annotations.get("mig.or-sim.io/queued"))
    if metrics:
        accepting = _parse_bool_or_none(metrics.get("accepting")) if metrics.get("accepting") is not None else accepting
        inflight = _parse_int_or_none(metrics.get("inflight")) if metrics.get("inflight") is not None else inflight
        queued = _parse_int_or_none(metrics.get("queued")) if metrics.get("queued") is not None else queued
    ready = _pod_ready(status)
    return {
        "podName": pod_name,
        "namespace": namespace,
        "nodeName": spec.get("nodeName"),
        "workload": workload,
        "batch": _parse_int_or_none(batch),
        "batchSource": "configMap" if batch is not None and configmap_name else "annotation",
        "configMapName": configmap_name,
        "endpoint": endpoint,
        "ready": ready,
        "phase": status.get("phase"),
        "acceptingNew": accepting,
        "inflight": inflight,
        "queued": queued,
        "runtimeMetrics": metrics or None,
        "actualMigDeviceUuids": actual_uuids,
        "matchedMigDeviceUuid": matched_uuid,
        "logicalSlot": logical_slot,
        "physicalGpuId": None if logical_slot is None else logical_slot.get("physicalGpuId"),
        "slot": None if logical_slot is None else logical_slot.get("slot"),
        "profile": None if logical_slot is None else logical_slot.get("profile"),
        "placementVerified": _parse_bool_or_none(annotations.get("mig.or-sim.io/placement-verified")),
        "assignmentSource": (
            "pod-placement-annotations"
            if actual_uuids
            else "workload-label-without-mig-uuid"
        ),
    }


def _runtime_metrics(annotations: dict[str, Any], endpoint: str | None) -> dict[str, Any]:
    enabled = _parse_bool_or_none(annotations.get("mig.or-sim.io/observe-metrics"))
    metrics_endpoint = annotations.get("mig.or-sim.io/metrics-endpoint")
    if not metrics_endpoint and enabled and endpoint:
        metrics_endpoint = f"{str(endpoint).rstrip('/')}/metrics"
    if not enabled or not metrics_endpoint:
        return {}
    try:
        with urlopen(str(metrics_endpoint), timeout=0.5) as response:
            data = json.loads(response.read().decode("utf-8"))
            return dict(data) if isinstance(data, dict) else {}
    except (HTTPError, URLError, TimeoutError, OSError, ValueError):
        return {"metricsError": "unavailable"}


def _queued_by_workload(assignments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    totals: dict[str, int] = {}
    for row in assignments:
        workload = row.get("workload")
        queued = row.get("queued")
        if workload is None or queued is None:
            continue
        totals[str(workload)] = totals.get(str(workload), 0) + int(queued)
    return [
        {"workload": workload, "queued": queued}
        for workload, queued in sorted(totals.items())
    ]


def _first_configmap_volume_name(spec: dict[str, Any]) -> str | None:
    for volume in list(spec.get("volumes", [])):
        configmap = dict(volume.get("configMap", {}))
        name = configmap.get("name")
        if name:
            return str(name)
    return None


def _pod_ready(status: dict[str, Any]) -> bool:
    for condition in list(status.get("conditions", [])):
        if condition.get("type") == "Ready":
            return condition.get("status") == "True"
    return False


def _csv_annotation(value: Any) -> list[str]:
    return [part.strip() for part in str(value or "").split(",") if part.strip()]


def _parse_bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    lowered = str(value).lower()
    if lowered in {"true", "1", "yes"}:
        return True
    if lowered in {"false", "0", "no"}:
        return False
    return None


def _parse_int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
