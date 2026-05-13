from __future__ import annotations

import re
from typing import Any

from api.k8s_api import KubernetesClient, PythonKubernetesClient
from executors.pod_lifecycle_executor import DEFAULT_MIG_RESOURCE


def create_workload_lifecycle_smoke_action_plan(
    name: str,
    namespace: str = "or-sim",
    node_name: str = "rtx1-worker",
    mig_resource: str = DEFAULT_MIG_RESOURCE,
    workload: str = "smoke",
    initial_batch_size: str = "4",
    updated_batch_size: str = "8",
    client_: KubernetesClient | None = None,
) -> dict[str, Any]:
    client_ = client_ or PythonKubernetesClient()
    slot = _slot_from_mig_resource(mig_resource)
    preview = {
        "previewOnly": True,
        "adapter": "pod-lifecycle",
        "policy": {
            "preferWarmPool": True,
            "deleteAfterDrainOnly": True,
            "doNotCreatePodsForMigGeometryOnlyActions": True,
            "batchReloadMechanism": "configmap-volume",
        },
        "createOrReuse": [
            {
                "type": "place_instance",
                "gpu_id": 0,
                "physical_gpu_id": "rtx1-worker-gpu0",
                "slot": slot,
                "workload": workload,
                "batch": str(initial_batch_size),
                "podAction": "create-or-reuse",
                "preferWarmPool": True,
                "reason": "single-A100 workload lifecycle smoke test",
            }
        ],
        "reloadInPlace": [
            {
                "type": "update_batch",
                "gpu_id": 0,
                "physical_gpu_id": "rtx1-worker-gpu0",
                "slot": slot,
                "workload": workload,
                "old_batch": str(initial_batch_size),
                "new_batch": str(updated_batch_size),
                "podAction": "reload-in-place",
                "preferWarmPool": False,
                "reason": "single-A100 live batch-size smoke test",
            }
        ],
        "drain": [],
        "deleteOrRecycle": [],
    }
    manifest = {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "MigActionPlan",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "or-sim-mig-planner",
                "mig.or-sim.io/workload-lifecycle-smoke": "true",
            },
        },
        "spec": {
            "migPlanRef": f"{name}-synthetic-owner",
            "migPlanGeneration": 1,
            "dryRun": True,
            "executor": "pod-lifecycle",
            "phaseGate": "PendingApproval",
            "actionCount": 2,
            "actionCountsByType": {"place_instance": 1, "update_batch": 1},
            "chosenTemplates": [],
            "targetGpuCount": 1,
            "podLifecyclePreview": preview,
            "executorPreview": {
                "previewOnly": True,
                "executor": "pod-lifecycle",
                "nodeName": node_name,
                "migResource": mig_resource,
            },
            "notes": [
                "Synthetic dry-run action plan used to exercise the Pod lifecycle adapter.",
                "This object is created by the test harness, not by the production planner.",
            ],
        },
    }
    client_.apply_migactionplan(manifest)
    status = {
        "phase": "ApprovedDryRun",
        "approved": True,
        "executed": False,
        "policyRef": "workload-lifecycle-smoke",
        "message": "Synthetic workload lifecycle smoke plan approved for explicit test execution.",
    }
    client_.patch_migactionplan_status(name=name, namespace=namespace, status=status)
    return {
        "kind": "WorkloadLifecycleSmokeActionPlan",
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "name": name,
        "namespace": namespace,
        "nodeName": node_name,
        "migResource": mig_resource,
        "podLifecyclePreview": preview,
        "status": status,
    }


def _slot_from_mig_resource(mig_resource: str) -> list[Any]:
    match = re.search(r"mig-(\dg)\.", mig_resource)
    profile = match.group(1) if match else "3g"
    size = int(profile.removesuffix("g"))
    return [0, size, profile]
