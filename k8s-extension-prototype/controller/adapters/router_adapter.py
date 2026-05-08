from __future__ import annotations

from typing import Any


class DryRunRouterPlanBuilder:
    def build(
        self,
        action_plan_name: str,
        namespace: str,
        traffic_and_drain_preview: dict[str, Any],
    ) -> dict[str, Any]:
        actions = list(traffic_and_drain_preview.get("trafficActions", []))
        return {
            "apiVersion": "mig.or-sim.io/v1alpha1",
            "kind": "RouterDryRunPlan",
            "metadata": {
                "name": f"{action_plan_name}-router-plan",
                "namespace": namespace,
            },
            "previewOnly": True,
            "workloadRoutePlans": [
                _workload_route_plan(action_plan_name, action)
                for action in actions
                if action.get("type") in {"stop_accepting_new", "reroute_queued_tasks"}
            ],
            "servingInstanceDrains": [
                _serving_instance_drain(action_plan_name, action)
                for action in actions
                if action.get("type") == "mark_draining_instance"
            ],
            "notes": [
                "Mock router plan only; no routing tables, queues, or serving runtimes are changed.",
                "A real router adapter should replace this ConfigMap with router/runtime API calls or CRDs.",
            ],
        }


def _workload_route_plan(action_plan_name: str, action: dict[str, Any]) -> dict[str, Any]:
    action_type = str(action.get("type"))
    return {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "WorkloadRoutePlan",
        "metadata": {
            "name": _route_plan_name(action_plan_name, action),
        },
        "spec": {
            "previewOnly": True,
            "action": "StopAcceptingNew" if action_type == "stop_accepting_new" else "RerouteQueuedTasks",
            "workload": action.get("workload"),
            "sourceInstanceRef": _instance_ref(action),
            "queued": action.get("queued"),
            "target": action.get("to"),
        },
    }


def _serving_instance_drain(action_plan_name: str, action: dict[str, Any]) -> dict[str, Any]:
    return {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "ServingInstanceDrain",
        "metadata": {
            "name": _route_plan_name(action_plan_name, action),
        },
        "spec": {
            "previewOnly": True,
            "workload": action.get("workload"),
            "sourceInstanceRef": _instance_ref(action),
            "targetInflight": 0,
            "currentInflightApprox": action.get("rounds"),
            "waitForInflightZero": True,
        },
    }


def _instance_ref(action: dict[str, Any]) -> dict[str, Any]:
    return {
        "gpuId": action.get("gpu_id"),
        "physicalGpuId": action.get("physical_gpu_id"),
        "slot": action.get("slot"),
    }


def _route_plan_name(action_plan_name: str, action: dict[str, Any]) -> str:
    action_type = str(action.get("type", "route"))
    gpu_id = action.get("gpu_id", "gpu")
    slot = action.get("slot") or []
    slot_token = "-".join(str(part) for part in slot) if slot else "noslot"
    return f"{action_plan_name}-{action_type}-{gpu_id}-{slot_token}".replace("_", "-")
