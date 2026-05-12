from __future__ import annotations

from datetime import datetime, timezone
import time
from typing import Any

from cluster_observer import observe_cluster_state_once
from k8s_api import KubernetesClient, PythonKubernetesClient
from observed_layout import logical_mig_slots_for_binding


DEFAULT_REGISTRY_NAME = "default"
EMPTY_MIG_CONFIG = "or-sim-empty"


def sync_physical_gpu_registry(
    namespace: str = "or-sim",
    registry_name: str = DEFAULT_REGISTRY_NAME,
    observed_state_name: str = "cluster-observed-state",
    apply: bool = False,
    client: KubernetesClient | None = None,
) -> dict[str, Any]:
    client = client or PythonKubernetesClient()
    observed = observe_cluster_state_once(
        namespace=namespace,
        name=observed_state_name,
        apply=True,
        client=client,
    )
    previous = client.get_physicalgpuregistry(name=registry_name, namespace=namespace)
    manifest = build_physical_gpu_registry(
        namespace=namespace,
        name=registry_name,
        observed_cluster_state=observed,
        previous_registry=previous,
    )
    if apply:
        status = dict(manifest.get("status", {}))
        spec_manifest = {key: value for key, value in manifest.items() if key != "status"}
        client.apply_physicalgpuregistry(spec_manifest)
        client.patch_physicalgpuregistry_status(
            name=registry_name,
            namespace=namespace,
            status=status,
        )
    return manifest


def run_physical_gpu_registry_monitor_loop(
    namespace: str = "or-sim",
    registry_name: str = DEFAULT_REGISTRY_NAME,
    observed_state_name: str = "cluster-observed-state",
    poll_interval_s: float = 30.0,
    max_cycles: int | None = None,
    client: KubernetesClient | None = None,
) -> dict[str, Any]:
    client = client or PythonKubernetesClient()
    cycle = 0
    last_summary: dict[str, Any] = {}
    while max_cycles is None or cycle < max_cycles:
        cycle += 1
        summary: dict[str, Any] = {
            "cycle": cycle,
            "namespace": namespace,
            "registryName": registry_name,
            "observedStateName": observed_state_name,
            "phase": "Unknown",
            "queues": {},
            "error": None,
        }
        try:
            registry = sync_physical_gpu_registry(
                namespace=namespace,
                registry_name=registry_name,
                observed_state_name=observed_state_name,
                apply=True,
                client=client,
            )
            summary["phase"] = str(dict(registry.get("status", {})).get("phase", "Synced"))
            summary["queues"] = registry_queue_summary(registry)
        except Exception as exc:
            summary["phase"] = "Error"
            summary["error"] = str(exc)
        last_summary = summary
        print(
            f"[physical-gpu-registry] cycle={cycle} "
            f"phase={summary['phase']} queues={summary.get('queues', {})} "
            f"error={summary.get('error')}",
            flush=True,
        )
        if max_cycles is not None and cycle >= max_cycles:
            break
        time.sleep(float(poll_interval_s))
    return {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "PhysicalGpuRegistryMonitorRunSummary",
        "lastCycle": last_summary,
    }


def build_physical_gpu_registry(
    namespace: str,
    name: str,
    observed_cluster_state: dict[str, Any],
    previous_registry: dict[str, Any] | None = None,
) -> dict[str, Any]:
    observed_spec = dict(observed_cluster_state.get("spec", {}))
    observed_state = dict(observed_spec.get("observedState", {}))
    previous_status = dict((previous_registry or {}).get("status", {}))
    previous_spec = dict((previous_registry or {}).get("spec", {}))
    policy = dict(previous_spec.get("policy", {})) or _default_policy()
    previous_active = [
        str(item)
        for item in list(previous_status.get("activeQueue", []))
    ]

    observed_bindings = {
        str(physical_id): dict(binding)
        for physical_id, binding in dict(observed_state.get("physicalGpuBindings", {})).items()
    }
    observed_bindings = _merge_cached_bindings_for_unresolved_devices(
        observed_state=observed_state,
        observed_bindings=observed_bindings,
        previous_status=previous_status,
    )
    discovered = sorted(observed_bindings)
    bindings: dict[str, dict[str, Any]] = {}
    active_queue = []
    available_queue = []
    transitioning_queue = []

    for physical_id in discovered:
        binding = dict(observed_bindings[physical_id])
        mig_devices = list(binding.get("migDevices", []))
        mig_config = _observed_mig_config(observed_state, binding)
        mig_config_state = _observed_mig_config_state(observed_state, binding)
        clean = len(mig_devices) == 0
        logical_slots = logical_mig_slots_for_binding(
            physical_gpu_id=physical_id,
            binding=binding,
            mig_config=mig_config,
            mig_config_state=mig_config_state,
        )
        available_eligible, availability_reason = _available_eligibility(
            clean=clean,
            mig_config=mig_config,
            mig_config_state=mig_config_state,
            policy=policy,
        )
        active = physical_id in previous_active
        if active:
            queue_state = "active"
            active_queue.append(physical_id)
        elif available_eligible:
            queue_state = "available"
            available_queue.append(physical_id)
        else:
            queue_state = "transitioning"
            transitioning_queue.append(physical_id)
        bindings[physical_id] = {
            "physicalGpuId": physical_id,
            "gpuUuid": binding.get("gpuUuid"),
            "nodeName": binding.get("nodeName"),
            "deviceIndex": binding.get("deviceIndex"),
            "product": binding.get("product"),
            "migCapable": bool(binding.get("migCapable", False)),
            "migDevices": mig_devices,
            "logicalMigSlots": logical_slots,
            "migDevicesFresh": bool(binding.get("migDevicesFresh", True)),
            "currentMigConfig": mig_config,
            "currentMigConfigState": mig_config_state,
            "cleanliness": "empty" if clean else "configured",
            "availableEligible": available_eligible,
            "availabilityReason": availability_reason,
            "state": queue_state,
            "bindingSource": binding.get("bindingSource"),
            "confidence": binding.get("confidence"),
        }

    missing_active = [physical_id for physical_id in previous_active if physical_id not in observed_bindings]
    ignored = list(observed_state.get("ignoredGpuDevices", []))
    status = {
        "phase": "Synced",
        "observedAt": datetime.now(timezone.utc).isoformat(),
        "observedClusterState": observed_cluster_state.get("metadata", {}).get("name"),
        "policy": policy,
        "discoveredA100": discovered,
        "activeQueue": active_queue,
        "availableQueue": available_queue,
        "transitioningQueue": transitioning_queue,
        "bindings": bindings,
        "ignoredGpuDevices": ignored,
        "missingActivePhysicalGpuIds": missing_active,
        "queueCounts": {
            "discovered": len(discovered),
            "active": len(active_queue),
            "available": len(available_queue),
            "transitioning": len(transitioning_queue),
            "ignored": len(ignored),
            "missingActive": len(missing_active),
        },
        "notes": [
            "activeQueue is planner-owned and preserved from the previous registry status.",
            f"availableQueue contains only observed A100 GPUs with no MIG devices, {EMPTY_MIG_CONFIG}=success, and not active.",
            "transitioningQueue contains observed A100 GPUs that must be cleaned or reconfigured before availability.",
            "During nvidia.com/mig.config.state=pending, GPU Operator exec inventory is skipped and stable GPU UUID bindings are reused from the previous registry.",
        ],
    }
    return {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "PhysicalGpuRegistry",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {"app.kubernetes.io/name": "or-sim-mig-planner"},
        },
        "spec": {"policy": policy},
        "status": status,
    }


def _merge_cached_bindings_for_unresolved_devices(
    observed_state: dict[str, Any],
    observed_bindings: dict[str, dict[str, Any]],
    previous_status: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    if bool(observed_state.get("gpuOperatorInventoryFresh", True)):
        return observed_bindings
    previous_bindings = {
        str(physical_id): dict(binding)
        for physical_id, binding in dict(previous_status.get("bindings", {})).items()
    }
    merged = dict(observed_bindings)
    for unresolved in list(observed_state.get("unresolvedPhysicalGpuDevices", [])):
        physical_id = str(unresolved.get("physicalGpuId") or "")
        cached = previous_bindings.get(physical_id)
        if not physical_id or cached is None or physical_id in merged:
            continue
        merged[physical_id] = {
            **cached,
            "physicalGpuId": physical_id,
            "nodeName": unresolved.get("nodeName", cached.get("nodeName")),
            "deviceIndex": unresolved.get("deviceIndex", cached.get("deviceIndex")),
            "product": unresolved.get("product", cached.get("product")),
            "migCapable": unresolved.get("migCapable", cached.get("migCapable")),
            "migConfig": unresolved.get("migConfig"),
            "migConfigState": unresolved.get("migConfigState"),
            "migDevices": [],
            "migDevicesFresh": False,
            "bindingSource": "previous-registry-cache-during-mig-pending",
            "confidence": "cached-gpu-uuid",
        }
    return merged


def registry_queue_summary(registry: dict[str, Any]) -> dict[str, Any]:
    status = dict(registry.get("status", {}))
    return {
        "discoveredA100": list(status.get("discoveredA100", [])),
        "activeQueue": list(status.get("activeQueue", [])),
        "availableQueue": list(status.get("availableQueue", [])),
        "transitioningQueue": list(status.get("transitioningQueue", [])),
        "ignoredGpuDevices": [
            {
                "physicalGpuId": item.get("physicalGpuId"),
                "product": item.get("product"),
                "reason": item.get("reason"),
            }
            for item in list(status.get("ignoredGpuDevices", []))
        ],
        "queueCounts": dict(status.get("queueCounts", {})),
    }


def mark_physical_gpu_active(
    physical_gpu_id: str,
    namespace: str = "or-sim",
    registry_name: str = DEFAULT_REGISTRY_NAME,
    apply: bool = False,
    client: KubernetesClient | None = None,
) -> dict[str, Any]:
    client = client or PythonKubernetesClient()
    registry = client.get_physicalgpuregistry(name=registry_name, namespace=namespace)
    if registry is None:
        raise ValueError(f"PhysicalGpuRegistry {namespace}/{registry_name} does not exist")
    status = dict(registry.get("status", {}))
    bindings = dict(status.get("bindings", {}))
    if physical_gpu_id not in bindings:
        raise ValueError(f"{physical_gpu_id} is not in registry bindings")
    active = _without(list(status.get("activeQueue", [])), physical_gpu_id) + [physical_gpu_id]
    available = _without(list(status.get("availableQueue", [])), physical_gpu_id)
    transitioning = _without(list(status.get("transitioningQueue", [])), physical_gpu_id)
    bindings[physical_gpu_id] = {**dict(bindings[physical_gpu_id]), "state": "active"}
    status.update(
        {
            "activeQueue": active,
            "availableQueue": available,
            "transitioningQueue": transitioning,
            "bindings": bindings,
        }
    )
    status["queueCounts"] = _queue_counts(status)
    if apply:
        client.patch_physicalgpuregistry_status(name=registry_name, namespace=namespace, status=status)
    return {**registry, "status": status}


def mark_physical_gpu_released(
    physical_gpu_id: str,
    namespace: str = "or-sim",
    registry_name: str = DEFAULT_REGISTRY_NAME,
    apply: bool = False,
    client: KubernetesClient | None = None,
) -> dict[str, Any]:
    client = client or PythonKubernetesClient()
    registry = client.get_physicalgpuregistry(name=registry_name, namespace=namespace)
    if registry is None:
        raise ValueError(f"PhysicalGpuRegistry {namespace}/{registry_name} does not exist")
    status = dict(registry.get("status", {}))
    bindings = dict(status.get("bindings", {}))
    if physical_gpu_id not in bindings:
        raise ValueError(f"{physical_gpu_id} is not in registry bindings")
    binding = dict(bindings[physical_gpu_id])
    active = _without(list(status.get("activeQueue", [])), physical_gpu_id)
    available = _without(list(status.get("availableQueue", [])), physical_gpu_id)
    transitioning = _without(list(status.get("transitioningQueue", [])), physical_gpu_id)
    if binding.get("availableEligible") is True:
        available.append(physical_gpu_id)
        binding["state"] = "available"
    else:
        transitioning.append(physical_gpu_id)
        binding["state"] = "transitioning"
        binding["requiredAction"] = "clear_template_before_available"
    bindings[physical_gpu_id] = binding
    status.update(
        {
            "activeQueue": active,
            "availableQueue": available,
            "transitioningQueue": transitioning,
            "bindings": bindings,
        }
    )
    status["queueCounts"] = _queue_counts(status)
    if apply:
        client.patch_physicalgpuregistry_status(name=registry_name, namespace=namespace, status=status)
    return {**registry, "status": status}


def _observed_mig_config(observed_state: dict[str, Any], binding: dict[str, Any]) -> str | None:
    binding_config = binding.get("migConfig")
    if binding_config:
        return str(binding_config)
    node_name = binding.get("nodeName")
    for layout in list(observed_state.get("migLayouts", [])):
        if layout.get("nodeName") == node_name:
            return layout.get("migConfig")
    return _node_label(observed_state, node_name=node_name, label="nvidia.com/mig.config")


def _observed_mig_config_state(observed_state: dict[str, Any], binding: dict[str, Any]) -> str | None:
    binding_state = binding.get("migConfigState")
    if binding_state:
        return str(binding_state)
    node_name = binding.get("nodeName")
    for layout in list(observed_state.get("migLayouts", [])):
        if layout.get("nodeName") == node_name:
            return layout.get("migConfigState")
    return _node_label(observed_state, node_name=node_name, label="nvidia.com/mig.config.state")


def _node_label(observed_state: dict[str, Any], node_name: Any, label: str) -> str | None:
    for node in list(observed_state.get("nodeInventory", [])):
        if node.get("nodeName") != node_name:
            continue
        labels = dict(node.get("labels", {}))
        value = labels.get(label)
        if value is not None:
            return str(value)
    return None


def _default_policy() -> dict[str, Any]:
    return {
        "gpuProductAllowlist": ["NVIDIA A100"],
        "allocationOrder": "first-observed",
        "requireEmptyBeforeAvailable": True,
        "emptyMigConfig": EMPTY_MIG_CONFIG,
        "requireMigConfigStateSuccessBeforeAvailable": True,
    }


def _available_eligibility(
    clean: bool,
    mig_config: str | None,
    mig_config_state: str | None,
    policy: dict[str, Any],
) -> tuple[bool, str]:
    if not clean:
        return False, "mig_devices_present"
    required_config = str(policy.get("emptyMigConfig") or EMPTY_MIG_CONFIG)
    if mig_config != required_config:
        return False, f"empty_config_not_applied:{mig_config or 'none'}"
    if bool(policy.get("requireMigConfigStateSuccessBeforeAvailable", True)) and mig_config_state != "success":
        return False, f"empty_config_not_success:{mig_config_state or 'none'}"
    return True, "empty_config_success"


def _without(values: list[Any], value: str) -> list[str]:
    return [str(item) for item in values if str(item) != value]


def _queue_counts(status: dict[str, Any]) -> dict[str, int]:
    return {
        "discovered": len(list(status.get("discoveredA100", []))),
        "active": len(list(status.get("activeQueue", []))),
        "available": len(list(status.get("availableQueue", []))),
        "transitioning": len(list(status.get("transitioningQueue", []))),
        "ignored": len(list(status.get("ignoredGpuDevices", []))),
        "missingActive": len(list(status.get("missingActivePhysicalGpuIds", []))),
    }
