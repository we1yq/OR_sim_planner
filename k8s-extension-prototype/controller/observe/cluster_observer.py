from __future__ import annotations

import math
import re
from typing import Any

from api.k8s_api import KubernetesClient, PythonKubernetesClient
from observe.observed_layout import logical_mig_slots_from_bindings
from observe.pod_assignment_observer import observe_pod_assignments


def observe_cluster_state_once(
    namespace: str = "or-sim",
    name: str = "cluster-observed-state",
    apply: bool = False,
    client: KubernetesClient | None = None,
) -> dict[str, Any]:
    client = client or PythonKubernetesClient()
    nodes = client.list_nodes()
    pods = client.list_pods(namespace=namespace)
    configmaps = _safe_configmap_list(client, namespace)
    gpu_inventory_skipped = _nodes_have_pending_mig_config(nodes)
    gpu_inventory = [] if gpu_inventory_skipped else _safe_gpu_operator_inventory(client)
    manifest = build_observed_cluster_state_from_k8s_lists(
        name=name,
        namespace=namespace,
        nodes=nodes,
        pods=pods,
        configmaps=configmaps,
        gpu_inventory=gpu_inventory,
        gpu_inventory_skipped=gpu_inventory_skipped,
    )
    if apply:
        client.apply_observedclusterstate(manifest)
        client.patch_observedclusterstate_status(
            name=name,
            namespace=namespace,
            status=observed_cluster_state_status(manifest),
        )
    return manifest


def build_observed_cluster_state_from_k8s_lists(
    name: str,
    namespace: str,
    nodes: list[dict[str, Any]],
    pods: list[dict[str, Any]],
    configmaps: list[dict[str, Any]] | None = None,
    gpu_inventory: list[dict[str, Any]] | None = None,
    gpu_inventory_skipped: bool = False,
) -> dict[str, Any]:
    node_inventory = [_node_inventory_row(node) for node in nodes]
    mig_layouts = [layout for node in nodes if (layout := _mig_layout_row(node)) is not None]
    physical_gpu_binding_result = _physical_gpu_binding_observation(
        nodes=nodes,
        gpu_inventory=gpu_inventory or [],
    )
    physical_gpu_bindings = dict(physical_gpu_binding_result["bindings"])
    logical_mig_slots = logical_mig_slots_from_bindings(
        bindings=physical_gpu_bindings,
        observed_state={"migLayouts": mig_layouts},
    )
    unresolved_physical_gpu_devices = list(physical_gpu_binding_result["unresolved"])
    ignored_gpu_devices = list(physical_gpu_binding_result["ignored"])
    pod_readiness = [_pod_readiness_row(pod) for pod in pods]
    pod_assignment_result = observe_pod_assignments(
        pods=pods,
        logical_mig_slots=logical_mig_slots,
        configmaps=list(configmaps or []),
    )
    missing_inputs = [
        "router queue and inflight metrics",
    ]
    if pod_assignment_result["unassignedGpuPods"]:
        missing_inputs.insert(0, "pod-to-MIG device assignment for some GPU Pods")
    if not logical_mig_slots and any(list(binding.get("migDevices", [])) for binding in physical_gpu_bindings.values()):
        missing_inputs.insert(0, "MIG logical slot inventory")
    if unresolved_physical_gpu_devices:
        missing_inputs.insert(0, "physical GPU UUID")
    observer_kind = "kubernetes-mig-node" if mig_layouts else "kubernetes-node-pod-smoke"
    source = f"{observer_kind}-observer"
    return {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "ObservedClusterState",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "or-sim-mig-planner",
                "mig.or-sim.io/observer-kind": observer_kind,
                "mig.or-sim.io/preview-only": "false",
            },
        },
        "spec": {
            "previewOnly": False,
            "source": source,
            "observedState": {
                "nodeInventory": node_inventory,
                "physicalGpuBindings": physical_gpu_bindings,
                "physicalGpuBindingList": [dict(value) for value in physical_gpu_bindings.values()],
                "logicalMigSlots": logical_mig_slots,
                "unresolvedPhysicalGpuDevices": unresolved_physical_gpu_devices,
                "ignoredGpuDevices": ignored_gpu_devices,
                "gpuOperatorInventory": list(gpu_inventory or []),
                "gpuOperatorInventoryFresh": not gpu_inventory_skipped,
                "gpuOperatorInventorySkippedReason": (
                    "mig_config_pending" if gpu_inventory_skipped else None
                ),
                "podReadiness": pod_readiness,
                "migLayouts": mig_layouts,
                "podAssignments": list(pod_assignment_result["podAssignments"]),
                "unassignedGpuPods": list(pod_assignment_result["unassignedGpuPods"]),
                "routerState": [],
                "inflightByInstance": list(pod_assignment_result["inflightByInstance"]),
                "queuedByWorkload": list(pod_assignment_result["queuedByWorkload"]),
            },
            "missingRealClusterInputs": missing_inputs,
            "canonicalizationRule": "do not canonicalize until MIG, pod assignment, and router observations are present",
            "notes": [
                "Read-only Kubernetes hardware observation.",
                "MIG profile inventory is derived from node labels and capacity/allocatable resources when available.",
                (
                    "GPU Operator exec inventory was skipped because at least one node has nvidia.com/mig.config.state=pending."
                    if gpu_inventory_skipped
                    else "GPU Operator exec inventory was collected for GPU and MIG UUIDs."
                ),
            ],
        },
    }


def observed_cluster_state_status(manifest: dict[str, Any]) -> dict[str, Any]:
    spec = dict(manifest.get("spec", {}))
    missing_inputs = list(spec.get("missingRealClusterInputs", []))
    observed = dict(spec.get("observedState", {}))
    mig_layouts = list(observed.get("migLayouts", []))
    validated_by = str(spec.get("source") or "kubernetes-node-pod-smoke-observer")
    return {
        "phase": "MigNodeInventoryObserved" if mig_layouts else "NodePodInventoryObserved",
        "previewOnly": bool(spec.get("previewOnly", False)),
        "readyForCanonicalization": False,
        "validatedBy": validated_by,
        "missingRealClusterInputCount": len(missing_inputs),
        "message": (
            "Kubernetes nodes, pods, and MIG profile resources observed; "
            "MIG device placement, pod assignment, and router observations are still required."
            if mig_layouts
            else "Kubernetes nodes and pods observed; MIG/device/router observations are still required."
        ),
    }


def _node_inventory_row(node: dict[str, Any]) -> dict[str, Any]:
    metadata = dict(node.get("metadata", {}))
    status = dict(node.get("status", {}))
    allocatable = dict(status.get("allocatable", {}))
    capacity = dict(status.get("capacity", {}))
    return {
        "nodeName": metadata.get("name"),
        "labels": dict(metadata.get("labels", {})),
        "gpuCapacity": capacity.get("nvidia.com/gpu"),
        "gpuAllocatable": allocatable.get("nvidia.com/gpu"),
        "conditions": [
            {
                "type": condition.get("type"),
                "status": condition.get("status"),
                "reason": condition.get("reason"),
            }
            for condition in list(status.get("conditions", []))
        ],
    }


def _nodes_have_pending_mig_config(nodes: list[dict[str, Any]]) -> bool:
    for node in nodes:
        labels = dict(dict(node.get("metadata", {})).get("labels", {}))
        if labels.get("nvidia.com/mig.config.state") == "pending":
            return True
    return False


def _mig_layout_row(node: dict[str, Any]) -> dict[str, Any] | None:
    metadata = dict(node.get("metadata", {}))
    labels = dict(metadata.get("labels", {}))
    status = dict(node.get("status", {}))
    capacity = dict(status.get("capacity", {}))
    allocatable = dict(status.get("allocatable", {}))
    profiles = []
    for key, value in sorted(labels.items()):
        if not key.startswith("nvidia.com/mig-") or not key.endswith(".count"):
            continue
        profile = key.removeprefix("nvidia.com/mig-").removesuffix(".count")
        profile_row = _mig_profile_row(
            profile=profile,
            label_count=value,
            labels=labels,
            capacity=capacity,
            allocatable=allocatable,
        )
        profiles.append(profile_row)
    if not profiles:
        return None
    return {
        "nodeName": metadata.get("name"),
        "migStrategy": labels.get("nvidia.com/mig.strategy"),
        "migConfig": labels.get("nvidia.com/mig.config"),
        "migConfigState": labels.get("nvidia.com/mig.config.state"),
        "profiles": profiles,
    }


def _mig_profile_row(
    profile: str,
    label_count: Any,
    labels: dict[str, Any],
    capacity: dict[str, Any],
    allocatable: dict[str, Any],
) -> dict[str, Any]:
    prefix = f"nvidia.com/mig-{profile}"
    row: dict[str, Any] = {
        "profile": profile,
        "labelCount": _parse_int(label_count),
        "capacity": _parse_int(capacity.get(prefix)),
        "allocatable": _parse_int(allocatable.get(prefix)),
        "product": labels.get(f"{prefix}.product"),
        "memoryMiB": _parse_int(labels.get(f"{prefix}.memory")),
        "multiprocessors": _parse_int(labels.get(f"{prefix}.multiprocessors")),
        "replicas": _parse_int(labels.get(f"{prefix}.replicas")),
        "slices": {
            "gi": _parse_int(labels.get(f"{prefix}.slices.gi")),
            "ci": _parse_int(labels.get(f"{prefix}.slices.ci")),
        },
        "engines": {
            "copy": _parse_int(labels.get(f"{prefix}.engines.copy")),
            "decoder": _parse_int(labels.get(f"{prefix}.engines.decoder")),
            "encoder": _parse_int(labels.get(f"{prefix}.engines.encoder")),
            "jpeg": _parse_int(labels.get(f"{prefix}.engines.jpeg")),
            "ofa": _parse_int(labels.get(f"{prefix}.engines.ofa")),
        },
        "sharingStrategy": labels.get(f"{prefix}.sharing-strategy"),
    }
    return row


def _physical_gpu_binding_observation(
    nodes: list[dict[str, Any]],
    gpu_inventory: list[dict[str, Any]],
) -> dict[str, Any]:
    bindings: dict[str, dict[str, Any]] = {}
    unresolved: list[dict[str, Any]] = []
    ignored: list[dict[str, Any]] = []
    for row in gpu_inventory:
        for binding in _bindings_from_gpu_operator_inventory(row):
            if not _is_a100_product(binding.get("product")):
                ignored.append({**binding, "reason": "non-A100 GPU ignored by MIGRANT MIG planner"})
                continue
            physical_id = str(binding.get("physicalGpuId") or "")
            if physical_id and binding.get("gpuUuid"):
                bindings[physical_id] = {"physicalGpuId": physical_id, **binding}
    nodes_with_inventory = {
        str(value.get("nodeName"))
        for value in bindings.values()
        if value.get("nodeName") is not None
    }
    for node in sorted(nodes, key=lambda item: str(dict(item.get("metadata", {})).get("name") or "")):
        metadata = dict(node.get("metadata", {}))
        labels = dict(metadata.get("labels", {}))
        annotations = dict(metadata.get("annotations", {}))
        status = dict(node.get("status", {}))
        capacity = dict(status.get("capacity", {}))
        node_name = str(metadata.get("name") or "")
        if not node_name:
            continue
        if node_name in nodes_with_inventory:
            continue

        explicit = _explicit_physical_gpu_bindings(
            node_name=node_name,
            labels=labels,
            annotations=annotations,
        )
        for binding in explicit:
            if not _is_a100_product(binding.get("product")):
                ignored.append({**binding, "reason": "non-A100 GPU ignored by MIGRANT MIG planner"})
                continue
            physical_id = str(binding.get("physicalGpuId") or "")
            if physical_id and binding.get("gpuUuid"):
                bindings[physical_id] = {"physicalGpuId": physical_id, **binding}
            else:
                unresolved.append(binding)
        if explicit:
            continue

        mig_count = _estimated_mig_physical_gpu_count(labels=labels, capacity=capacity)
        full_gpu_count = _parse_int(capacity.get("nvidia.com/gpu")) or 0
        for local_index in range(mig_count):
            product = _mig_product(labels)
            unresolved.append(
                {
                    "physicalGpuId": _physical_gpu_alias(node_name, local_index),
                    "nodeName": node_name,
                    "deviceIndex": local_index,
                    "gpuUuid": None,
                    "product": product,
                    "migCapable": True,
                    "migConfig": labels.get("nvidia.com/mig.config"),
                    "migConfigState": labels.get("nvidia.com/mig.config.state"),
                    "bindingSource": "kubernetes-node-labels-inferred",
                    "confidence": "node-device-index-inferred",
                    "reason": "Kubernetes node labels expose MIG resources but not the physical GPU UUID.",
                }
            )
        for offset in range(full_gpu_count):
            device_index = mig_count + offset
            ignored.append(
                {
                    "physicalGpuId": _physical_gpu_alias(node_name, device_index),
                    "nodeName": node_name,
                    "deviceIndex": device_index,
                    "gpuUuid": None,
                    "product": None,
                    "aggregateProductLabel": labels.get("nvidia.com/gpu.product"),
                    "migCapable": False,
                    "migConfig": None,
                    "migConfigState": None,
                    "bindingSource": "kubernetes-node-capacity-inferred",
                    "confidence": "node-device-index-inferred",
                    "reason": "non-A100 or unknown aggregate GPU capacity ignored by MIGRANT MIG planner",
                }
            )
    return {
        "bindings": bindings,
        "unresolved": _dedupe_gpu_rows(unresolved),
        "ignored": _dedupe_gpu_rows(ignored),
    }


def _dedupe_gpu_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (
            str(row.get("nodeName") or ""),
            str(row.get("deviceIndex") or ""),
            str(row.get("gpuUuid") or row.get("physicalGpuId") or ""),
        )
        existing = deduped.get(key)
        if existing is None:
            deduped[key] = dict(row)
            continue
        existing_source = str(existing.get("bindingSource") or "")
        row_source = str(row.get("bindingSource") or "")
        if "device-plugin" not in existing_source and "device-plugin" in row_source:
            deduped[key] = dict(row)
    return list(deduped.values())


def _safe_gpu_operator_inventory(client: KubernetesClient) -> list[dict[str, Any]]:
    inventory_fn = getattr(client, "list_gpu_operator_inventory", None)
    if inventory_fn is None:
        return []
    try:
        return list(inventory_fn())
    except Exception:
        return []


def _safe_configmap_list(client: KubernetesClient, namespace: str) -> list[dict[str, Any]]:
    list_fn = getattr(client, "list_configmaps", None)
    if list_fn is None:
        return []
    try:
        return list(list_fn(namespace=namespace))
    except Exception:
        return []


def _bindings_from_gpu_operator_inventory(row: dict[str, Any]) -> list[dict[str, Any]]:
    node_name = row.get("nodeName")
    source = row.get("source") or "gpu-operator-device-plugin-nvidia-smi"
    bindings: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for line in str(row.get("nvidiaSmiL") or "").splitlines():
        gpu_match = re.match(r"^GPU\s+(\d+):\s+(.+?)\s+\(UUID:\s+(GPU-[^)]+)\)", line)
        if gpu_match:
            device_index = int(gpu_match.group(1))
            product = gpu_match.group(2).strip()
            current = {
                "physicalGpuId": _physical_gpu_alias(str(node_name), device_index),
                "nodeName": node_name,
                "deviceIndex": device_index,
                "gpuUuid": gpu_match.group(3),
                "product": product,
                "migCapable": _is_a100_product(product),
                "migDevices": [],
                "bindingSource": source,
                "confidence": "gpu-operator-nvidia-smi",
            }
            bindings.append(current)
            continue
        mig_match = re.match(r"^\s+MIG\s+(.+?)\s+Device\s+(\d+):\s+\(UUID:\s+(MIG-[^)]+)\)", line)
        if mig_match and current is not None:
            current["migCapable"] = True
            current.setdefault("migDevices", []).append(
                {
                    "profile": mig_match.group(1).strip(),
                    "migDeviceIndex": int(mig_match.group(2)),
                    "migDeviceUuid": mig_match.group(3),
                }
            )
    return bindings


def _explicit_physical_gpu_bindings(
    node_name: str,
    labels: dict[str, Any],
    annotations: dict[str, Any],
) -> list[dict[str, Any]]:
    raw_count = (
        annotations.get("mig.or-sim.io/physical-gpu.count")
        or labels.get("mig.or-sim.io/physical-gpu.count")
    )
    count = _parse_int(raw_count) or 0
    bindings = []
    for device_index in range(count):
        prefix = f"mig.or-sim.io/physical-gpu.{device_index}."
        value_source = annotations if f"{prefix}uuid" in annotations else labels
        gpu_uuid = value_source.get(f"{prefix}uuid")
        bindings.append(
            {
                "physicalGpuId": _physical_gpu_alias(node_name, device_index),
                "nodeName": node_name,
                "deviceIndex": device_index,
                "gpuUuid": gpu_uuid,
                "product": value_source.get(f"{prefix}product"),
                "migCapable": _parse_bool(value_source.get(f"{prefix}mig-capable")),
                "migConfig": labels.get("nvidia.com/mig.config"),
                "migConfigState": labels.get("nvidia.com/mig.config.state"),
                "bindingSource": "mig-or-sim-node-annotation",
                "confidence": "explicit",
            }
        )
    return bindings


def _physical_gpu_alias(node_name: str, device_index: int) -> str:
    return f"{node_name}-gpu{device_index}"


def _is_a100_product(value: Any) -> bool:
    return "A100" in str(value or "").upper()


def _estimated_mig_physical_gpu_count(labels: dict[str, Any], capacity: dict[str, Any]) -> int:
    slice_total = 0
    profile_count_total = 0
    for key, value in labels.items():
        if not key.startswith("nvidia.com/mig-") or not key.endswith(".count"):
            continue
        profile = key.removeprefix("nvidia.com/mig-").removesuffix(".count")
        count = _parse_int(capacity.get(f"nvidia.com/mig-{profile}"))
        if count is None:
            count = _parse_int(value) or 0
        if count <= 0:
            continue
        profile_count_total += count
        gi_slices = _parse_int(labels.get(f"nvidia.com/mig-{profile}.slices.gi")) or 0
        slice_total += count * gi_slices
    if slice_total > 0:
        return max(1, math.ceil(slice_total / 7))
    return 1 if profile_count_total > 0 else 0


def _mig_product(labels: dict[str, Any]) -> str | None:
    for key, value in sorted(labels.items()):
        if key.startswith("nvidia.com/mig-") and key.endswith(".product"):
            product = str(value)
            return product.rsplit("-MIG-", 1)[0] if "-MIG-" in product else product
    return None


def _parse_bool(value: Any) -> bool | None:
    if value is None:
        return None
    lowered = str(value).strip().lower()
    if lowered in {"true", "1", "yes"}:
        return True
    if lowered in {"false", "0", "no"}:
        return False
    return None


def _parse_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value))
    except ValueError:
        return None


def _pod_readiness_row(pod: dict[str, Any]) -> dict[str, Any]:
    metadata = dict(pod.get("metadata", {}))
    status = dict(pod.get("status", {}))
    spec = dict(pod.get("spec", {}))
    return {
        "namespace": metadata.get("namespace"),
        "podName": metadata.get("name"),
        "nodeName": spec.get("nodeName"),
        "phase": status.get("phase"),
        "ready": _pod_ready(status),
    }


def _pod_ready(status: dict[str, Any]) -> bool:
    for condition in list(status.get("conditions", [])):
        if condition.get("type") == "Ready":
            return condition.get("status") == "True"
    return False
