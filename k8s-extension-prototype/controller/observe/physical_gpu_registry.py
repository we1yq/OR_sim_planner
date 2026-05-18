from __future__ import annotations

from datetime import datetime, timezone
import time
from typing import Any

from observe.cluster_observer import observe_cluster_state_once
from api.k8s_api import KubernetesClient, PythonKubernetesClient
from observe.observed_layout import logical_mig_slots_for_binding


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
    previous_transitioning = [
        str(item)
        for item in list(previous_status.get("transitioningQueue", []))
    ]
    previous_bindings = {
        str(physical_id): dict(binding)
        for physical_id, binding in dict(previous_status.get("bindings", {})).items()
    }
    previous_provisional = {
        str(physical_id): dict(value)
        for physical_id, value in dict(previous_status.get("provisionalAgentSlotMaps", {})).items()
    }

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
        previous_binding = dict(previous_bindings.get(physical_id, {}))
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
        verification = _slot_map_verification(
            observed_slots=logical_slots,
            provisional=previous_provisional.get(physical_id),
        )
        if verification == "verified":
            logical_slots = [{**dict(slot), "verification": "verified"} for slot in logical_slots]
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
        record = {
            "physicalGpuId": physical_id,
            "gpuUuid": binding.get("gpuUuid"),
            "nodeName": binding.get("nodeName"),
            "deviceIndex": binding.get("deviceIndex"),
            "product": binding.get("product"),
            "migCapable": bool(binding.get("migCapable", False)),
            "migDevices": mig_devices,
            "logicalMigSlots": logical_slots,
            "migDevicesFresh": bool(binding.get("migDevicesFresh", True)),
            "slotMapVerification": verification,
            "currentMigConfig": mig_config,
            "currentMigConfigState": mig_config_state,
            "cleanliness": "empty" if clean else "configured",
            "availableEligible": available_eligible,
            "availabilityReason": availability_reason,
            "state": queue_state,
            "bindingSource": binding.get("bindingSource"),
            "confidence": binding.get("confidence"),
        }
        mig_readiness = _mig_readiness_from_previous(
            previous_binding=previous_binding,
            verification=verification,
            logical_slots=logical_slots,
        )
        if mig_readiness:
            record["migReadiness"] = mig_readiness
        if queue_state == "active":
            active_logical_id = _logical_gpu_id_value(previous_binding.get("activeLogicalGpuId"))
            if active_logical_id is not None:
                record["activeLogicalGpuId"] = active_logical_id
        elif queue_state == "transitioning":
            pending_logical_id = _logical_gpu_id_value(previous_binding.get("pendingLogicalGpuId"))
            if pending_logical_id is not None or physical_id in previous_transitioning:
                record["pendingLogicalGpuId"] = pending_logical_id
        bindings[physical_id] = record

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
        "provisionalAgentSlotMaps": {
            physical_id: provisional
            for physical_id, provisional in previous_provisional.items()
            if physical_id in discovered
            and list(bindings.get(physical_id, {}).get("logicalMigSlots", []))
            and _slot_map_verification(
                observed_slots=list(bindings.get(physical_id, {}).get("logicalMigSlots", [])),
                provisional=provisional,
            )
            == "provisional"
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


def _slot_map_verification(
    observed_slots: list[dict[str, Any]],
    provisional: dict[str, Any] | None,
) -> str:
    if not provisional:
        return "observed"
    expected = _slot_signature_list(list(provisional.get("slots", [])))
    observed = _slot_signature_list(observed_slots)
    return "verified" if expected and expected == observed else "provisional"


def _slot_signature_list(slots: list[dict[str, Any]]) -> list[tuple[int, int, str, str]]:
    out = []
    for slot in slots:
        try:
            out.append(
                (
                    int(slot.get("slotStart")),
                    int(slot.get("slotEnd")),
                    str(slot.get("profile") or ""),
                    str(slot.get("migDeviceUuid") or ""),
                )
            )
        except (TypeError, ValueError):
            continue
    return sorted(out)


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


def apply_agent_mig_result_to_registry(
    physical_gpu_id: str,
    agent_result: dict[str, Any],
    namespace: str = "or-sim",
    registry_name: str = DEFAULT_REGISTRY_NAME,
    apply: bool = False,
    client: KubernetesClient | None = None,
) -> dict[str, Any]:
    client = client or PythonKubernetesClient()
    registry = client.get_physicalgpuregistry(name=registry_name, namespace=namespace)
    if registry is None:
        raise ValueError(f"PhysicalGpuRegistry {namespace}/{registry_name} does not exist")
    if not bool(agent_result.get("success", False)):
        raise ValueError("Refusing to write unsuccessful agent result to registry")

    status = dict(registry.get("status", {}))
    bindings = {
        str(physical_id): dict(binding)
        for physical_id, binding in dict(status.get("bindings", {})).items()
    }
    if physical_gpu_id not in bindings:
        raise ValueError(f"{physical_gpu_id} is not in registry bindings")

    binding = dict(bindings[physical_gpu_id])
    slots = [_slot_from_agent_result(physical_gpu_id, binding, item) for item in list(agent_result.get("migSlots", []))]
    command = str(agent_result.get("command") or "")
    if not slots and command != "clear":
        raise ValueError("Agent result does not include migSlots; cannot write provisional registry state")

    if command == "clear":
        observed_at = datetime.now(timezone.utc).isoformat()
        binding["migDevices"] = []
        binding["logicalMigSlots"] = []
        binding["migDevicesFresh"] = True
        binding["slotMapVerification"] = "empty"
        binding["slotMapSource"] = "fast-mig-node-agent"
        binding["state"] = "transitioning"
        binding.pop("migReadiness", None)
        binding.pop("activeLogicalGpuId", None)
        binding.pop("pendingLogicalGpuId", None)
        binding["lastAgentResult"] = {
            "command": agent_result.get("command"),
            "gpuIndex": agent_result.get("gpuIndex"),
            "profileIds": agent_result.get("profileIds"),
            "createSeconds": agent_result.get("createSeconds"),
            "deleteSeconds": agent_result.get("deleteSeconds"),
            "message": agent_result.get("message"),
            "observedAt": observed_at,
        }
        bindings[physical_gpu_id] = binding
        provisional = dict(status.get("provisionalAgentSlotMaps", {}))
        provisional.pop(physical_gpu_id, None)
        status["bindings"] = bindings
        status["provisionalAgentSlotMaps"] = provisional
        status["activeQueue"] = _without(list(status.get("activeQueue", [])), physical_gpu_id)
        status["availableQueue"] = _without(list(status.get("availableQueue", [])), physical_gpu_id)
        status["transitioningQueue"] = _without(list(status.get("transitioningQueue", [])), physical_gpu_id) + [physical_gpu_id]
        status["queueCounts"] = _queue_counts(status)
        status["phase"] = "AgentClearedMig"
        status["observedAt"] = observed_at
        if apply:
            client.patch_physicalgpuregistry_status(name=registry_name, namespace=namespace, status=status)
        return {**registry, "status": status}

    binding["migDevices"] = [
        {
            "profile": slot.get("gpuOperatorProfile"),
            "migDeviceIndex": idx,
            "migDeviceUuid": slot.get("migDeviceUuid"),
            "slotStart": slot.get("slotStart"),
            "slotEnd": slot.get("slotEnd"),
            "slot": slot.get("slot"),
            "gpuInstanceId": slot.get("gpuInstanceId"),
            "profileId": slot.get("profileId"),
            "placementSource": "fast-mig-node-agent-result",
        }
        for idx, slot in enumerate(slots)
    ]
    binding["logicalMigSlots"] = slots
    binding["migDevicesFresh"] = True
    binding["slotMapVerification"] = "provisional"
    binding["slotMapSource"] = "fast-mig-node-agent"
    binding["state"] = "transitioning"
    binding["lastAgentResult"] = {
        "command": agent_result.get("command"),
        "gpuIndex": agent_result.get("gpuIndex"),
        "profileIds": agent_result.get("profileIds"),
        "createSeconds": agent_result.get("createSeconds"),
        "deleteSeconds": agent_result.get("deleteSeconds"),
        "message": agent_result.get("message"),
        "observedAt": datetime.now(timezone.utc).isoformat(),
    }
    binding["migReadiness"] = {
        "phase": "uuid-provisional",
        "slotMapVerification": "provisional",
        "cdiRefreshed": False,
        "directUuidUsable": False,
        "source": "fast-mig-node-agent",
        "observedAt": binding["lastAgentResult"]["observedAt"],
        "message": "MIG UUIDs returned by agent; waiting for CDI refresh before direct CDI pod launch.",
    }
    bindings[physical_gpu_id] = binding
    provisional = dict(status.get("provisionalAgentSlotMaps", {}))
    provisional[physical_gpu_id] = {
        "physicalGpuId": physical_gpu_id,
        "verification": "provisional",
        "source": "fast-mig-node-agent",
        "observedAt": binding["lastAgentResult"]["observedAt"],
        "slots": slots,
    }
    status["bindings"] = bindings
    status["provisionalAgentSlotMaps"] = provisional
    status["activeQueue"] = _without(list(status.get("activeQueue", [])), physical_gpu_id)
    status["availableQueue"] = _without(list(status.get("availableQueue", [])), physical_gpu_id)
    status["transitioningQueue"] = _without(list(status.get("transitioningQueue", [])), physical_gpu_id) + [physical_gpu_id]
    status["queueCounts"] = _queue_counts(status)
    status["phase"] = "ProvisionalAgentSlotMap"
    status["observedAt"] = datetime.now(timezone.utc).isoformat()
    if apply:
        client.patch_physicalgpuregistry_status(name=registry_name, namespace=namespace, status=status)
    return {**registry, "status": status}


def mark_physical_gpu_mig_cdi_ready(
    physical_gpu_id: str,
    logical_gpu_id: Any | None = None,
    cdi_result: dict[str, Any] | None = None,
    namespace: str = "or-sim",
    registry_name: str = DEFAULT_REGISTRY_NAME,
    apply: bool = False,
    client: KubernetesClient | None = None,
) -> dict[str, Any]:
    client = client or PythonKubernetesClient()
    registry = client.get_physicalgpuregistry(name=registry_name, namespace=namespace)
    if registry is None:
        raise ValueError(f"PhysicalGpuRegistry {namespace}/{registry_name} does not exist")
    if cdi_result is not None and not bool(cdi_result.get("success", False)):
        raise ValueError("Refusing to mark CDI ready from an unsuccessful agent refresh-cdi result")

    status = dict(registry.get("status", {}))
    bindings = {
        str(item): dict(value)
        for item, value in dict(status.get("bindings", {})).items()
    }
    if physical_gpu_id not in bindings:
        raise ValueError(f"{physical_gpu_id} is not in registry bindings")

    binding = dict(bindings[physical_gpu_id])
    if not list(binding.get("logicalMigSlots", [])):
        raise ValueError(f"{physical_gpu_id} has no logicalMigSlots to expose through direct UUID binding")

    observed_at = datetime.now(timezone.utc).isoformat()
    verification = str(binding.get("slotMapVerification") or "unknown")
    readiness = {
        **dict(binding.get("migReadiness", {})),
        "phase": "cdi-ready",
        "slotMapVerification": verification,
        "cdiRefreshed": True,
        "directUuidUsable": True,
        "source": "fast-mig-node-agent",
        "observedAt": observed_at,
        "message": "CDI refreshed; direct CDI annotation pod binding is allowed.",
    }
    if cdi_result is not None:
        readiness["lastCdiRefresh"] = {
            "command": cdi_result.get("command"),
            "gpuIndex": cdi_result.get("gpuIndex"),
            "seconds": cdi_result.get("createSeconds"),
            "message": cdi_result.get("message"),
            "observedAt": observed_at,
        }
    binding["migReadiness"] = readiness
    binding["state"] = "active"
    active_logical_id = _logical_gpu_id_value(
        logical_gpu_id if logical_gpu_id is not None else binding.get("pendingLogicalGpuId")
    )
    if active_logical_id is not None:
        binding["activeLogicalGpuId"] = active_logical_id
    binding.pop("pendingLogicalGpuId", None)
    bindings[physical_gpu_id] = binding

    status["bindings"] = bindings
    status["activeQueue"] = _without(list(status.get("activeQueue", [])), physical_gpu_id) + [physical_gpu_id]
    status["availableQueue"] = _without(list(status.get("availableQueue", [])), physical_gpu_id)
    status["transitioningQueue"] = _without(list(status.get("transitioningQueue", [])), physical_gpu_id)
    status["queueCounts"] = _queue_counts(status)
    status["phase"] = "MigCdiReady"
    status["observedAt"] = observed_at
    if apply:
        client.patch_physicalgpuregistry_status(name=registry_name, namespace=namespace, status=status)
    return {**registry, "status": status}


def _slot_from_agent_result(
    physical_gpu_id: str,
    binding: dict[str, Any],
    item: dict[str, Any],
) -> dict[str, Any]:
    start = int(item.get("slotStart"))
    end = int(item.get("slotEnd"))
    profile = str(item.get("profile") or "")
    return {
        "physicalGpuId": physical_gpu_id,
        "nodeName": binding.get("nodeName"),
        "deviceIndex": binding.get("deviceIndex"),
        "gpuUuid": binding.get("gpuUuid"),
        "migConfig": binding.get("currentMigConfig") or binding.get("migConfig"),
        "migConfigState": binding.get("currentMigConfigState") or binding.get("migConfigState"),
        "slotStart": start,
        "slotEnd": end,
        "slot": [start, end, profile],
        "profile": profile,
        "gpuOperatorProfile": _gpu_operator_profile(profile),
        "migDeviceIndex": item.get("migDeviceIndex"),
        "migDeviceUuid": item.get("migDeviceUuid"),
        "gpuInstanceId": item.get("gpuInstanceId"),
        "profileId": item.get("profileId"),
        "bindingSource": "fast-mig-node-agent-result",
        "confidence": "provisional-agent-returned-slot-map",
        "verification": "provisional",
    }


def _gpu_operator_profile(profile: str) -> str:
    return {
        "1g": "1g.5gb",
        "2g": "2g.10gb",
        "3g": "3g.20gb",
        "4g": "4g.20gb",
        "7g": "7g.40gb",
    }.get(str(profile), str(profile))


def mark_physical_gpu_active(
    physical_gpu_id: str,
    logical_gpu_id: Any | None = None,
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
    binding = {**dict(bindings[physical_gpu_id]), "state": "active"}
    active_logical_id = _logical_gpu_id_value(
        logical_gpu_id if logical_gpu_id is not None else binding.get("pendingLogicalGpuId")
    )
    if active_logical_id is not None:
        binding["activeLogicalGpuId"] = active_logical_id
    binding.pop("pendingLogicalGpuId", None)
    bindings[physical_gpu_id] = binding
    status.update(
        {
            "activeQueue": active,
            "availableQueue": available,
            "transitioningQueue": transitioning,
            "bindings": bindings,
        }
    )
    provisional = dict(status.get("provisionalAgentSlotMaps", {}))
    provisional.pop(physical_gpu_id, None)
    status["provisionalAgentSlotMaps"] = provisional
    status["queueCounts"] = _queue_counts(status)
    if apply:
        client.patch_physicalgpuregistry_status(name=registry_name, namespace=namespace, status=status)
    return {**registry, "status": status}


def mark_physical_gpu_pending(
    physical_gpu_id: str,
    logical_gpu_id: Any | None = None,
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
    active = _without(list(status.get("activeQueue", [])), physical_gpu_id)
    available = _without(list(status.get("availableQueue", [])), physical_gpu_id)
    transitioning = _without(list(status.get("transitioningQueue", [])), physical_gpu_id) + [physical_gpu_id]
    binding = {**dict(bindings[physical_gpu_id]), "state": "transitioning"}
    pending_logical_id = _logical_gpu_id_value(
        logical_gpu_id if logical_gpu_id is not None else binding.get("activeLogicalGpuId")
    )
    if pending_logical_id is not None:
        binding["pendingLogicalGpuId"] = pending_logical_id
    binding.pop("activeLogicalGpuId", None)
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
    binding.pop("activeLogicalGpuId", None)
    binding.pop("migReadiness", None)
    if binding.get("availableEligible") is True:
        available.append(physical_gpu_id)
        binding["state"] = "available"
        binding.pop("pendingLogicalGpuId", None)
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


def _logical_gpu_id_value(value: Any) -> Any | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return str(value)


def _mig_readiness_from_previous(
    previous_binding: dict[str, Any],
    verification: str,
    logical_slots: list[dict[str, Any]],
) -> dict[str, Any]:
    previous = dict(previous_binding.get("migReadiness", {}))
    if not previous:
        return {}
    if not logical_slots:
        return {}
    readiness = dict(previous)
    readiness["slotMapVerification"] = verification
    if verification == "verified" and readiness.get("phase") == "cdi-ready":
        readiness["directUuidUsable"] = True
        readiness["cdiRefreshed"] = True
        readiness["message"] = "CDI refreshed and slot map verified by observer."
    elif verification == "provisional":
        readiness["slotMapVerification"] = "provisional"
    return readiness


def _queue_counts(status: dict[str, Any]) -> dict[str, int]:
    return {
        "discovered": len(list(status.get("discoveredA100", []))),
        "active": len(list(status.get("activeQueue", []))),
        "available": len(list(status.get("availableQueue", []))),
        "transitioning": len(list(status.get("transitioningQueue", []))),
        "ignored": len(list(status.get("ignoredGpuDevices", []))),
        "missingActive": len(list(status.get("missingActivePhysicalGpuIds", []))),
    }
