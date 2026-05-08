from __future__ import annotations

from typing import Any


class DryRunObservedStateBuilder:
    def build(
        self,
        observer_preview: dict[str, Any],
        canonical_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        targets = dict(observer_preview.get("targetsToObserve", {}))
        physical_gpu_ids = [str(value) for value in targets.get("physicalGpuIds", [])]
        workloads = [str(value) for value in targets.get("workloads", [])]
        plan_item_ids = [str(value) for value in targets.get("planItemIds", [])]
        canonical_state = canonical_state or {}
        return {
            "previewOnly": True,
            "source": "dry-run-observer-skeleton",
            "observedState": {
                "migLayouts": [
                    {
                        "physicalGpuId": physical_gpu_id,
                        "nodeName": None,
                        "deviceIndex": None,
                        "observedMigInstances": [],
                        "source": "missing-real-gpu-node-inventory",
                    }
                    for physical_gpu_id in physical_gpu_ids
                ],
                "podReadiness": [
                    {
                        "workload": workload,
                        "readyPods": None,
                        "source": "missing-pod-runtime-observer",
                    }
                    for workload in workloads
                ],
                "podAssignments": [],
                "routerState": [
                    {
                        "workload": workload,
                        "acceptingNew": None,
                        "rerouteTargets": [],
                        "source": "missing-router-runtime-observer",
                    }
                    for workload in workloads
                ],
                "inflightByInstance": [
                    {
                        "planItemId": plan_item_id,
                        "inflight": None,
                        "source": "missing-router-runtime-observer",
                    }
                    for plan_item_id in plan_item_ids
                ],
                "queuedByWorkload": [
                    {
                        "workload": workload,
                        "queued": None,
                        "source": "missing-router-runtime-observer",
                    }
                    for workload in workloads
                ],
                "canonicalNextStateGpuCount": len(list(canonical_state.get("gpus", []))),
            },
            "missingRealClusterInputs": [
                "nvidia GPU node inventory",
                "GPU Operator MIG Manager state",
                "pod readiness and pod-to-MIG assignment observer",
                "router queued/inflight runtime metrics",
            ],
            "canonicalizationRule": observer_preview.get(
                "canonicalizationRule",
                "After real execution, canonicalize only observed post-action state.",
            ),
        }
