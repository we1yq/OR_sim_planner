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
    pod_readiness = [_pod_readiness_row(pod) for pod in pods]
    missing_inputs = [
        "physical GPU UUID to node/device binding",
        "actual MIG instance inventory",
        "pod-to-MIG device assignment",
        "router queue and inflight metrics",
    ]
    return {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "ObservedClusterState",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "or-sim-mig-planner",
                "mig.or-sim.io/observer-kind": "kubernetes-node-pod-smoke",
                "mig.or-sim.io/preview-only": "false",
            },
        },
        "spec": {
            "previewOnly": False,
            "source": "kubernetes-node-pod-smoke-observer",
            "observedState": {
                "nodeInventory": node_inventory,
                "podReadiness": pod_readiness,
                "migLayouts": [],
                "podAssignments": [],
                "routerState": [],
                "inflightByInstance": [],
                "queuedByWorkload": [],
            },
            "missingRealClusterInputs": missing_inputs,
            "canonicalizationRule": "do not canonicalize until MIG, pod assignment, and router observations are present",
            "notes": [
                "Read-only hardware smoke observation.",
                "This proves Kubernetes node/pod visibility before NVIDIA MIG-specific observer integration.",
            ],
        },
    }


def observed_cluster_state_status(manifest: dict[str, Any]) -> dict[str, Any]:
    spec = dict(manifest.get("spec", {}))
    missing_inputs = list(spec.get("missingRealClusterInputs", []))
    return {
        "phase": "NodePodInventoryObserved",
        "previewOnly": bool(spec.get("previewOnly", False)),
        "readyForCanonicalization": False,
        "validatedBy": "kubernetes-node-pod-smoke-observer",
        "missingRealClusterInputCount": len(missing_inputs),
        "message": "Kubernetes nodes and pods observed; MIG/device/router observations are still required.",
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
