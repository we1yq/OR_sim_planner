from __future__ import annotations

from typing import Any


class DryRunPodLifecyclePlanBuilder:
    def build(
        self,
        action_plan_name: str,
        namespace: str,
        pod_lifecycle_preview: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "apiVersion": "mig.or-sim.io/v1alpha1",
            "kind": "PodLifecycleDryRunPlan",
            "metadata": {
                "name": f"{action_plan_name}-pod-lifecycle-plan",
                "namespace": namespace,
            },
            "previewOnly": True,
            "podLifecyclePlans": (
                [
                    _pod_lifecycle_plan(action_plan_name, row, "CreateOrReuse")
                    for row in pod_lifecycle_preview.get("createOrReuse", [])
                ]
                + [
                    _pod_lifecycle_plan(action_plan_name, row, "Drain")
                    for row in pod_lifecycle_preview.get("drain", [])
                ]
                + [
                    _pod_lifecycle_plan(action_plan_name, row, "DeleteOrRecycle")
                    for row in pod_lifecycle_preview.get("deleteOrRecycle", [])
                ]
                + [
                    _pod_lifecycle_plan(action_plan_name, row, "ReloadInPlace")
                    for row in pod_lifecycle_preview.get("reloadInPlace", [])
                ]
            ),
            "notes": [
                "Mock Pod lifecycle plan only; no Pods are created, deleted, patched, or restarted.",
                "A real Pod adapter should replace this ConfigMap with Kubernetes Pod/Deployment or serving-runtime operations.",
            ],
        }


def _pod_lifecycle_plan(action_plan_name: str, row: dict[str, Any], action: str) -> dict[str, Any]:
    return {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "PodLifecyclePlan",
        "metadata": {
            "name": _pod_plan_name(action_plan_name, row, action),
        },
        "spec": {
            "previewOnly": True,
            "action": action,
            "workload": row.get("workload"),
            "instanceRef": _instance_ref(row),
            "preferWarmPool": bool(row.get("preferWarmPool", False)),
            "reason": row.get("reason"),
            "podAction": row.get("podAction"),
        },
    }


def _instance_ref(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "gpuId": row.get("gpu_id"),
        "physicalGpuId": row.get("physical_gpu_id"),
        "slot": row.get("slot"),
    }


def _pod_plan_name(action_plan_name: str, row: dict[str, Any], action: str) -> str:
    workload = row.get("workload") or "workload"
    gpu_id = row.get("gpu_id", "gpu")
    slot = row.get("slot") or []
    slot_token = "-".join(str(part) for part in slot) if slot else "noslot"
    return f"{action_plan_name}-{action}-{workload}-{gpu_id}-{slot_token}".replace("_", "-").lower()
