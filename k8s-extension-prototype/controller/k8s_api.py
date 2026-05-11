from __future__ import annotations

from typing import Any, Protocol

class KubernetesClient(Protocol):
    def get_migplan(self, name: str, namespace: str) -> dict[str, Any]:
        ...

    def list_migplans(self, namespace: str) -> list[dict[str, Any]]:
        ...

    def list_migactionplans(self, namespace: str) -> list[dict[str, Any]]:
        ...

    def get_migactionplan(self, name: str, namespace: str) -> dict[str, Any]:
        ...

    def list_nodes(self) -> list[dict[str, Any]]:
        ...

    def list_pods(self, namespace: str) -> list[dict[str, Any]]:
        ...

    def get_node(self, name: str) -> dict[str, Any]:
        ...

    def patch_node_labels(
        self,
        name: str,
        labels: dict[str, str],
        remove_labels: list[str] | None = None,
    ) -> None:
        ...

    def patch_migplan_status(self, name: str, namespace: str, status: dict[str, Any]) -> None:
        ...

    def patch_migactionplan_status(self, name: str, namespace: str, status: dict[str, Any]) -> None:
        ...

    def patch_workloadrouteplan_status(self, name: str, namespace: str, status: dict[str, Any]) -> None:
        ...

    def patch_servinginstancedrain_status(self, name: str, namespace: str, status: dict[str, Any]) -> None:
        ...

    def patch_podlifecycleplan_status(self, name: str, namespace: str, status: dict[str, Any]) -> None:
        ...

    def patch_observedclusterstate_status(self, name: str, namespace: str, status: dict[str, Any]) -> None:
        ...

    def get_autoapprovalpolicy(self, name: str, namespace: str) -> dict[str, Any] | None:
        ...

    def get_configmap(self, name: str, namespace: str) -> dict[str, Any]:
        ...

    def get_clusterpolicy(self, name: str) -> dict[str, Any]:
        ...

    def apply_configmap(self, manifest: dict[str, Any]) -> None:
        ...

    def apply_migactionplan(self, manifest: dict[str, Any]) -> None:
        ...

    def apply_workloadrouteplan(self, manifest: dict[str, Any]) -> None:
        ...

    def apply_servinginstancedrain(self, manifest: dict[str, Any]) -> None:
        ...

    def apply_podlifecycleplan(self, manifest: dict[str, Any]) -> None:
        ...

    def apply_observedclusterstate(self, manifest: dict[str, Any]) -> None:
        ...

    def watch_migplans(self, namespace: str, timeout_seconds: int) -> Any:
        ...


class PythonKubernetesClient:
    group = "mig.or-sim.io"
    version = "v1alpha1"
    plural = "migplans"

    def __init__(self) -> None:
        from kubernetes import client, config
        from kubernetes.config.config_exception import ConfigException

        try:
            config.load_incluster_config()
        except ConfigException:
            config.load_kube_config()

        self.custom = client.CustomObjectsApi()
        self.core = client.CoreV1Api()
        self.api_client = client.ApiClient()
        self.api_exception = client.exceptions.ApiException

    def get_migplan(self, name: str, namespace: str) -> dict[str, Any]:
        obj = self.custom.get_namespaced_custom_object(
            group=self.group,
            version=self.version,
            namespace=namespace,
            plural=self.plural,
            name=name,
        )
        if not isinstance(obj, dict):
            raise ValueError(f"Kubernetes API returned a non-object MigPlan for {namespace}/{name}")
        return obj

    def list_migplans(self, namespace: str) -> list[dict[str, Any]]:
        obj = self.custom.list_namespaced_custom_object(
            group=self.group,
            version=self.version,
            namespace=namespace,
            plural=self.plural,
        )
        if not isinstance(obj, dict):
            raise ValueError(f"Kubernetes API returned a non-object MigPlanList for namespace {namespace}")
        return list(obj.get("items", []))

    def list_migactionplans(self, namespace: str) -> list[dict[str, Any]]:
        obj = self.custom.list_namespaced_custom_object(
            group=self.group,
            version=self.version,
            namespace=namespace,
            plural="migactionplans",
        )
        if not isinstance(obj, dict):
            raise ValueError(
                f"Kubernetes API returned a non-object MigActionPlanList for namespace {namespace}"
        )
        return list(obj.get("items", []))

    def get_migactionplan(self, name: str, namespace: str) -> dict[str, Any]:
        obj = self.custom.get_namespaced_custom_object(
            group=self.group,
            version=self.version,
            namespace=namespace,
            plural="migactionplans",
            name=name,
        )
        if not isinstance(obj, dict):
            raise ValueError(f"Kubernetes API returned a non-object MigActionPlan for {namespace}/{name}")
        return obj

    def list_nodes(self) -> list[dict[str, Any]]:
        obj = self.core.list_node()
        serialized = self.api_client.sanitize_for_serialization(obj)
        if not isinstance(serialized, dict):
            raise ValueError("Kubernetes API returned a non-object NodeList")
        return list(serialized.get("items", []))

    def get_node(self, name: str) -> dict[str, Any]:
        obj = self.core.read_node(name=name)
        serialized = self.api_client.sanitize_for_serialization(obj)
        if not isinstance(serialized, dict):
            raise ValueError(f"Kubernetes API returned a non-object Node for {name}")
        return serialized

    def patch_node_labels(
        self,
        name: str,
        labels: dict[str, str],
        remove_labels: list[str] | None = None,
    ) -> None:
        patch_labels: dict[str, Any] = dict(labels)
        for label in remove_labels or []:
            patch_labels[label] = None
        self.core.patch_node(name=name, body={"metadata": {"labels": patch_labels}})

    def list_pods(self, namespace: str) -> list[dict[str, Any]]:
        obj = self.core.list_namespaced_pod(namespace=namespace)
        serialized = self.api_client.sanitize_for_serialization(obj)
        if not isinstance(serialized, dict):
            raise ValueError(f"Kubernetes API returned a non-object PodList for namespace {namespace}")
        return list(serialized.get("items", []))

    def watch_migplans(self, namespace: str, timeout_seconds: int = 60) -> Any:
        from kubernetes import watch

        watcher = watch.Watch()
        yield from watcher.stream(
            self.custom.list_namespaced_custom_object,
            group=self.group,
            version=self.version,
            namespace=namespace,
            plural=self.plural,
            timeout_seconds=int(timeout_seconds),
        )

    def patch_migplan_status(self, name: str, namespace: str, status: dict[str, Any]) -> None:
        self.custom.patch_namespaced_custom_object_status(
            group=self.group,
            version=self.version,
            namespace=namespace,
            plural=self.plural,
            name=name,
            body={"status": status},
        )

    def patch_migactionplan_status(self, name: str, namespace: str, status: dict[str, Any]) -> None:
        self._patch_custom_object_status(
            plural="migactionplans",
            name=name,
            namespace=namespace,
            status=status,
        )

    def patch_workloadrouteplan_status(self, name: str, namespace: str, status: dict[str, Any]) -> None:
        self._patch_custom_object_status(
            plural="workloadrouteplans",
            name=name,
            namespace=namespace,
            status=status,
        )

    def patch_servinginstancedrain_status(self, name: str, namespace: str, status: dict[str, Any]) -> None:
        self._patch_custom_object_status(
            plural="servinginstancedrains",
            name=name,
            namespace=namespace,
            status=status,
        )

    def patch_podlifecycleplan_status(self, name: str, namespace: str, status: dict[str, Any]) -> None:
        self._patch_custom_object_status(
            plural="podlifecycleplans",
            name=name,
            namespace=namespace,
            status=status,
        )

    def patch_observedclusterstate_status(self, name: str, namespace: str, status: dict[str, Any]) -> None:
        self._patch_custom_object_status(
            plural="observedclusterstates",
            name=name,
            namespace=namespace,
            status=status,
        )

    def get_autoapprovalpolicy(self, name: str, namespace: str) -> dict[str, Any] | None:
        try:
            obj = self.custom.get_namespaced_custom_object(
                group=self.group,
                version=self.version,
                namespace=namespace,
                plural="autoapprovalpolicies",
                name=name,
            )
        except self.api_exception as exc:
            if int(getattr(exc, "status", 0)) == 404:
                return None
            raise
        if not isinstance(obj, dict):
            raise ValueError(
                f"Kubernetes API returned a non-object AutoApprovalPolicy for {namespace}/{name}"
            )
        return obj

    def get_configmap(self, name: str, namespace: str) -> dict[str, Any]:
        obj = self.core.read_namespaced_config_map(name=name, namespace=namespace)
        return self.api_client.sanitize_for_serialization(obj)

    def get_clusterpolicy(self, name: str) -> dict[str, Any]:
        obj = self.custom.get_cluster_custom_object(
            group="nvidia.com",
            version="v1",
            plural="clusterpolicies",
            name=name,
        )
        if not isinstance(obj, dict):
            raise ValueError(f"Kubernetes API returned a non-object ClusterPolicy for {name}")
        return obj

    def apply_configmap(self, manifest: dict[str, Any]) -> None:
        metadata = dict(manifest.get("metadata", {}))
        name = str(metadata["name"])
        namespace = str(metadata["namespace"])
        try:
            self.core.create_namespaced_config_map(namespace=namespace, body=manifest)
        except self.api_exception as exc:
            if int(getattr(exc, "status", 0)) != 409:
                raise
            self.core.patch_namespaced_config_map(name=name, namespace=namespace, body=manifest)

    def apply_migactionplan(self, manifest: dict[str, Any]) -> None:
        self._apply_custom_object(manifest=manifest, plural="migactionplans")

    def apply_workloadrouteplan(self, manifest: dict[str, Any]) -> None:
        self._apply_custom_object(manifest=manifest, plural="workloadrouteplans")

    def apply_servinginstancedrain(self, manifest: dict[str, Any]) -> None:
        self._apply_custom_object(manifest=manifest, plural="servinginstancedrains")

    def apply_podlifecycleplan(self, manifest: dict[str, Any]) -> None:
        self._apply_custom_object(manifest=manifest, plural="podlifecycleplans")

    def apply_observedclusterstate(self, manifest: dict[str, Any]) -> None:
        self._apply_custom_object(manifest=manifest, plural="observedclusterstates")

    def _apply_custom_object(self, manifest: dict[str, Any], plural: str) -> None:
        metadata = dict(manifest.get("metadata", {}))
        name = str(metadata["name"])
        namespace = str(metadata["namespace"])
        try:
            self.custom.create_namespaced_custom_object(
                group=self.group,
                version=self.version,
                namespace=namespace,
                plural=plural,
                body=manifest,
            )
        except self.api_exception as exc:
            if int(getattr(exc, "status", 0)) != 409:
                raise
            self.custom.patch_namespaced_custom_object(
                group=self.group,
                version=self.version,
                namespace=namespace,
                plural=plural,
                name=name,
                body=manifest,
            )

    def _patch_custom_object_status(
        self,
        plural: str,
        name: str,
        namespace: str,
        status: dict[str, Any],
    ) -> None:
        self.custom.patch_namespaced_custom_object_status(
            group=self.group,
            version=self.version,
            namespace=namespace,
            plural=plural,
            name=name,
            body={"status": status},
        )
