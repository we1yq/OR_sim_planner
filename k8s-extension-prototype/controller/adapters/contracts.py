from __future__ import annotations

from typing import Any, Protocol


class MigGeometryAdapter(Protocol):
    def preview(self, mig_geometry_preview: dict[str, Any]) -> dict[str, Any]:
        ...


class RouterDrainAdapter(Protocol):
    def preview(self, traffic_and_drain_preview: dict[str, Any]) -> dict[str, Any]:
        ...


class PodLifecycleAdapter(Protocol):
    def preview(self, pod_lifecycle_preview: dict[str, Any]) -> dict[str, Any]:
        ...


class ObserverAdapter(Protocol):
    def preview(self, observer_preview: dict[str, Any]) -> dict[str, Any]:
        ...


class DryRunMigGeometryAdapter:
    def preview(self, mig_geometry_preview: dict[str, Any]) -> dict[str, Any]:
        return {
            "previewOnly": True,
            "wouldPatchNodeLabels": dict(mig_geometry_preview.get("wouldPatchNodeLabels", {})),
            "wouldApplyMigManagerConfigs": list(mig_geometry_preview.get("migManagerTargetConfigs", [])),
            "blockedUntilObservedBindings": list(mig_geometry_preview.get("unresolvedPhysicalGpuIds", [])),
        }


class DryRunRouterDrainAdapter:
    def preview(self, traffic_and_drain_preview: dict[str, Any]) -> dict[str, Any]:
        actions = list(traffic_and_drain_preview.get("trafficActions", []))
        return {
            "previewOnly": True,
            "wouldStopAcceptingNew": [row for row in actions if row.get("type") == "stop_accepting_new"],
            "wouldRerouteQueuedTasks": [row for row in actions if row.get("type") == "reroute_queued_tasks"],
            "wouldStartDrains": [row for row in actions if row.get("type") == "mark_draining_instance"],
        }


class DryRunPodLifecycleAdapter:
    def preview(self, pod_lifecycle_preview: dict[str, Any]) -> dict[str, Any]:
        return {
            "previewOnly": True,
            "wouldCreateOrReuse": list(pod_lifecycle_preview.get("createOrReuse", [])),
            "wouldDrain": list(pod_lifecycle_preview.get("drain", [])),
            "wouldDeleteOrRecycle": list(pod_lifecycle_preview.get("deleteOrRecycle", [])),
            "wouldReloadInPlace": list(pod_lifecycle_preview.get("reloadInPlace", [])),
        }


class DryRunObserverAdapter:
    def preview(self, observer_preview: dict[str, Any]) -> dict[str, Any]:
        return {
            "previewOnly": True,
            "requiredObservations": dict(observer_preview.get("requiredObservations", {})),
            "targetsToObserve": dict(observer_preview.get("targetsToObserve", {})),
            "canonicalizationRule": observer_preview.get("canonicalizationRule"),
        }
