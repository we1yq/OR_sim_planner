from __future__ import annotations

from typing import Any

from api.k8s_api import KubernetesClient, PythonKubernetesClient


def create_router_drain_smoke_action_plan(
    name: str,
    namespace: str = "or-sim",
    workload: str = "resnet50",
    source_pod: str = "router-workload-a",
    source_endpoint: str = "http://router-workload-a:8080",
    target_pod: str = "router-workload-b",
    target_endpoint: str = "http://router-workload-b:8080",
    router_endpoint: str = "http://or-sim-smoke-router:8080",
    client_: KubernetesClient | None = None,
) -> dict[str, Any]:
    client_ = client_ or PythonKubernetesClient()
    traffic_actions = [
        {
            "type": "stop_accepting_new",
            "workload": workload,
            "sourcePod": source_pod,
            "sourceEndpoint": source_endpoint,
            "targetPod": target_pod,
            "targetEndpoint": target_endpoint,
        },
        {
            "type": "reroute_queued_tasks",
            "workload": workload,
            "sourcePod": source_pod,
            "sourceEndpoint": source_endpoint,
            "targetPod": target_pod,
            "targetEndpoint": target_endpoint,
        },
        {
            "type": "mark_draining_instance",
            "workload": workload,
            "sourcePod": source_pod,
            "sourceEndpoint": source_endpoint,
            "targetPod": target_pod,
            "targetEndpoint": target_endpoint,
        },
    ]
    preview = {
        "previewOnly": True,
        "adapter": "router-drain",
        "trafficActions": traffic_actions,
        "notes": [
            "Synthetic router/drain smoke plan.",
            "This plan validates serving-instance routing between two MIG-backed Pods on one A100.",
        ],
    }
    manifest = {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "MigActionPlan",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "or-sim-mig-planner",
                "mig.or-sim.io/router-drain-smoke": "true",
            },
        },
        "spec": {
            "migPlanRef": f"{name}-synthetic-owner",
            "migPlanGeneration": 1,
            "dryRun": True,
            "executor": "router-drain",
            "phaseGate": "PendingApproval",
            "actionCount": len(traffic_actions),
            "actionCountsByType": {
                "stop_accepting_new": 1,
                "reroute_queued_tasks": 1,
                "mark_draining_instance": 1,
            },
            "trafficAndDrainPreview": preview,
            "executorPreview": {
                "previewOnly": True,
                "executor": "router-drain",
                "routerEndpoint": router_endpoint,
            },
            "notes": [
                "Synthetic dry-run action plan used to exercise the Router/Drain adapter.",
                "This object is created by the test harness, not by the production planner.",
            ],
        },
    }
    client_.apply_migactionplan(manifest)
    status = {
        "phase": "ApprovedDryRun",
        "approved": True,
        "executed": False,
        "policyRef": "router-drain-smoke",
        "message": "Synthetic router/drain smoke plan approved for explicit test execution.",
    }
    client_.patch_migactionplan_status(name=name, namespace=namespace, status=status)
    return {
        "kind": "RouterDrainSmokeActionPlan",
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "name": name,
        "namespace": namespace,
        "routerEndpoint": router_endpoint,
        "trafficAndDrainPreview": preview,
        "status": status,
    }
