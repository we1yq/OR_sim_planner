from __future__ import annotations

from typing import Any

from k8s_api import KubernetesClient, PythonKubernetesClient


def observe_cluster_state_once(
    namespace: str = "or-sim",
    name: str = "cluster-observed-state",
    apply: bool = False,
    client: KubernetesClient | None = None,
) -> dict[str, Any]:
    client = client or PythonKubernetesClient()
    nodes = client.list_nodes()
    pods = client.list_pods(namespace=namespace)
    manifest = build_observed_cluster_state_from_k8s_lists(
        name=name,
        namespace=namespace,
        nodes=nodes,
        pods=pods,
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
) -> dict[str, Any]:
    node_inventory = [_node_inventory_row(node) for node in nodes]
    mig_layouts = [layout for node in nodes if (layout := _mig_layout_row(node)) is not None]
    pod_readiness = [_pod_readiness_row(pod) for pod in pods]
    missing_inputs = [
        "physical GPU UUID to node/device binding",
        "MIG device UUID and placement inventory",
        "pod-to-MIG device assignment",
        "router queue and inflight metrics",
    ]
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
                "podReadiness": pod_readiness,
                "migLayouts": mig_layouts,
                "podAssignments": [],
                "routerState": [],
                "inflightByInstance": [],
                "queuedByWorkload": [],
            },
            "missingRealClusterInputs": missing_inputs,
            "canonicalizationRule": "do not canonicalize until MIG, pod assignment, and router observations are present",
            "notes": [
                "Read-only Kubernetes hardware observation.",
                "MIG profile inventory is derived from node labels and capacity/allocatable resources when available.",
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
