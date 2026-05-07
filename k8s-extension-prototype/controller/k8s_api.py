from __future__ import annotations

from typing import Any, Protocol

class KubernetesClient(Protocol):
    def get_migplan(self, name: str, namespace: str) -> dict[str, Any]:
        ...

    def list_migplans(self, namespace: str) -> list[dict[str, Any]]:
        ...

    def patch_migplan_status(self, name: str, namespace: str, status: dict[str, Any]) -> None:
        ...

    def get_configmap(self, name: str, namespace: str) -> dict[str, Any]:
        ...

    def apply_configmap(self, manifest: dict[str, Any]) -> None:
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

    def get_configmap(self, name: str, namespace: str) -> dict[str, Any]:
        obj = self.core.read_namespaced_config_map(name=name, namespace=namespace)
        return self.api_client.sanitize_for_serialization(obj)

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
