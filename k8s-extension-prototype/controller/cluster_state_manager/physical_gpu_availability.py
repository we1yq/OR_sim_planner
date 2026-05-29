from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import time
from typing import Any

from api.k8s_api import KubernetesClient, PythonKubernetesClient
from executors.mig_config_manager import ensure_gpu_operator_configs
from cluster_state_manager.physical_gpu_registry import DEFAULT_REGISTRY_NAME, EMPTY_MIG_CONFIG


MIG_CONFIG_LABEL = "nvidia.com/mig.config"
MIG_CONFIG_STATE_LABEL = "nvidia.com/mig.config.state"


class PhysicalGpuAvailabilityError(RuntimeError):
    pass


def run_physical_gpu_availability_controller_loop(
    namespace: str = "or-sim",
    registry_name: str = DEFAULT_REGISTRY_NAME,
    poll_interval_s: float = 30.0,
    max_cycles: int | None = None,
    confirm_real_mig_apply: bool = False,
    wait: bool = True,
    timeout_s: float = 900.0,
    client: KubernetesClient | None = None,
) -> dict[str, Any]:
    client = client or PythonKubernetesClient()
    cycle = 0
    last_summary: dict[str, Any] = {}
    while max_cycles is None or cycle < max_cycles:
        cycle += 1
        try:
            last_summary = ensure_transitioning_gpus_empty(
                namespace=namespace,
                registry_name=registry_name,
                confirm_real_mig_apply=confirm_real_mig_apply,
                wait=wait,
                timeout_s=timeout_s,
                client=client,
            )
            last_summary["cycle"] = cycle
        except Exception as exc:
            last_summary = {
                "kind": "PhysicalGpuAvailabilityControllerSummary",
                "apiVersion": "mig.or-sim.io/v1alpha1",
                "cycle": cycle,
                "phase": "Error",
                "error": str(exc),
            }
        print(
            f"[physical-gpu-availability] cycle={cycle} "
            f"phase={last_summary.get('phase')} actions={len(last_summary.get('actions', []))} "
            f"skipped={len(last_summary.get('skipped', []))} error={last_summary.get('error')}",
            flush=True,
        )
        if max_cycles is not None and cycle >= max_cycles:
            break
        time.sleep(float(poll_interval_s))
    return last_summary


def ensure_transitioning_gpus_empty(
    namespace: str = "or-sim",
    registry_name: str = DEFAULT_REGISTRY_NAME,
    confirm_real_mig_apply: bool = False,
    wait: bool = True,
    timeout_s: float = 900.0,
    client: KubernetesClient | None = None,
) -> dict[str, Any]:
    if not confirm_real_mig_apply:
        raise PhysicalGpuAvailabilityError(
            "Refusing to change real MIG layout without confirm_real_mig_apply=True."
        )
    client = client or PythonKubernetesClient()
    registry = client.get_physicalgpuregistry(name=registry_name, namespace=namespace)
    if registry is None:
        raise PhysicalGpuAvailabilityError(f"PhysicalGpuRegistry {namespace}/{registry_name} does not exist")
    status = dict(registry.get("status", {}))
    bindings = {
        str(physical_id): dict(binding)
        for physical_id, binding in dict(status.get("bindings", {})).items()
    }
    active = {str(item) for item in list(status.get("activeQueue", []))}
    candidates = [
        binding
        for physical_id, binding in sorted(bindings.items())
        if physical_id not in active and _needs_empty_convergence(binding)
    ]
    by_node: dict[str, list[dict[str, Any]]] = {}
    skipped = []
    for binding in candidates:
        node_name = str(binding.get("nodeName") or "")
        if not node_name or binding.get("deviceIndex") is None:
            skipped.append({"physicalGpuId": binding.get("physicalGpuId"), "reason": "missing_node_or_device_index"})
            continue
        active_peers = [
            physical_id
            for physical_id, peer in bindings.items()
            if physical_id in active and peer.get("nodeName") == node_name
        ]
        if active_peers:
            skipped.append(
                {
                    "physicalGpuId": binding.get("physicalGpuId"),
                    "nodeName": node_name,
                    "reason": "active_peer_on_same_node",
                    "activePeers": active_peers,
                }
            )
            continue
        by_node.setdefault(node_name, []).append(binding)

    actions = []
    for node_name, node_bindings in sorted(by_node.items()):
        device_indexes = sorted(int(binding["deviceIndex"]) for binding in node_bindings)
        force_config = _force_empty_config_name(node_name=node_name, device_indexes=device_indexes)
        config_sync = ensure_gpu_operator_configs(
            client=client,
            desired_configs={
                force_config: [
                    {"devices": [device_index], "mig-enabled": True, "mig-devices": {}}
                    for device_index in device_indexes
                ],
            },
        )
        before = _node_mig_summary(client.get_node(node_name))
        if _needs_forced_label_change(before, node_bindings):
            client.patch_node_labels(
                name=node_name,
                labels={MIG_CONFIG_LABEL: force_config},
                remove_labels=[MIG_CONFIG_STATE_LABEL],
            )
            forced = _wait_for_node_config(
                client=client,
                node_name=node_name,
                target_config=force_config,
                wait=wait,
                timeout_s=timeout_s,
            )
        else:
            forced = before
        client.patch_node_labels(
            name=node_name,
            labels={MIG_CONFIG_LABEL: EMPTY_MIG_CONFIG},
            remove_labels=[MIG_CONFIG_STATE_LABEL],
        )
        final = _wait_for_node_config(
            client=client,
            node_name=node_name,
            target_config=EMPTY_MIG_CONFIG,
            wait=wait,
            timeout_s=timeout_s,
        )
        actions.append(
            {
                "nodeName": node_name,
                "physicalGpuIds": [binding.get("physicalGpuId") for binding in node_bindings],
                "deviceIndexes": device_indexes,
                "forceEmptyConfig": force_config,
                "configSync": config_sync,
                "before": before,
                "forced": forced,
                "final": final,
            }
        )

    return {
        "kind": "PhysicalGpuAvailabilityControllerSummary",
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "phase": "EnsuredEmpty" if actions else "Noop",
        "namespace": namespace,
        "registryName": registry_name,
        "observedAt": datetime.now(timezone.utc).isoformat(),
        "actions": actions,
        "skipped": skipped,
    }


def _needs_empty_convergence(binding: dict[str, Any]) -> bool:
    if binding.get("availableEligible") is True:
        return False
    if binding.get("state") == "active":
        return False
    if binding.get("requiredAction") == "clear_template_before_available":
        return True
    reason = str(binding.get("availabilityReason") or "")
    return reason in {
        "mig_devices_present",
        "empty_config_not_applied:none",
        "empty_config_not_success:none",
    } or reason.startswith("empty_config_not_applied:") or reason.startswith("empty_config_not_success:")


def _needs_forced_label_change(before: dict[str, Any], node_bindings: list[dict[str, Any]]) -> bool:
    if before.get("migConfig") != EMPTY_MIG_CONFIG:
        return False
    if before.get("migConfigState") != "success":
        return True
    return any(str(binding.get("availabilityReason") or "") == "mig_devices_present" for binding in node_bindings)


def _force_empty_config_name(node_name: str, device_indexes: list[int]) -> str:
    fingerprint = ",".join(str(item) for item in device_indexes)
    digest = hashlib.sha1(f"{node_name}|empty|{fingerprint}".encode("utf-8")).hexdigest()[:10]
    return f"or-sim-empty-{digest}"


def _wait_for_node_config(
    client: KubernetesClient,
    node_name: str,
    target_config: str,
    wait: bool,
    timeout_s: float,
) -> dict[str, Any]:
    if not wait:
        return _node_mig_summary(client.get_node(node_name))
    deadline = time.monotonic() + float(timeout_s)
    last = {}
    while time.monotonic() < deadline:
        last = _node_mig_summary(client.get_node(node_name))
        if last.get("migConfig") == target_config and last.get("migConfigState") == "success":
            return last
        time.sleep(5.0)
    raise PhysicalGpuAvailabilityError(
        f"Timed out waiting for {node_name} {MIG_CONFIG_LABEL}={target_config} "
        f"to reach state success. Last observed: {last}"
    )


def _node_mig_summary(node: dict[str, Any]) -> dict[str, Any]:
    labels = dict(dict(node.get("metadata", {})).get("labels", {}))
    return {
        "nodeName": dict(node.get("metadata", {})).get("name"),
        "migConfig": labels.get(MIG_CONFIG_LABEL),
        "migConfigState": labels.get(MIG_CONFIG_STATE_LABEL),
    }
