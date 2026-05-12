from __future__ import annotations

import re
import time
from typing import Any

from kubernetes import client, config, watch
from kubernetes.config.config_exception import ConfigException

from k8s_api import KubernetesClient, PythonKubernetesClient
from observed_layout import (
    expected_placement_from_row,
    find_logical_slot,
    logical_mig_slots_from_bindings,
    parse_mig_uuids_from_nvidia_smi_l,
)


class PodLifecycleApplyError(RuntimeError):
    pass


DEFAULT_IMAGE = "nvidia/cuda:12.4.1-base-ubuntu22.04"
DEFAULT_MIG_RESOURCE = "nvidia.com/mig-3g.20gb"
PROFILE_TO_A100_40GB_RESOURCE = {
    "1g": "nvidia.com/mig-1g.5gb",
    "2g": "nvidia.com/mig-2g.10gb",
    "3g": "nvidia.com/mig-3g.20gb",
    "4g": "nvidia.com/mig-4g.20gb",
    "7g": "nvidia.com/mig-7g.40gb",
}


def apply_pod_lifecycle_from_action_plan(
    name: str,
    namespace: str = "or-sim",
    confirm_real_pod_apply: bool = False,
    allow_preview_instructions: bool = False,
    node_name: str | None = None,
    mig_resource: str | None = None,
    image: str = DEFAULT_IMAGE,
    timeout_s: float = 300.0,
    poll_interval_s: float = 1.0,
    cleanup: bool = False,
    client_: KubernetesClient | None = None,
) -> dict[str, Any]:
    if not confirm_real_pod_apply:
        raise PodLifecycleApplyError(
            "Refusing to create/patch workload Pods without confirm_real_pod_apply=True."
        )

    client_ = client_ or PythonKubernetesClient()
    action_plan = client_.get_migactionplan(name=name, namespace=namespace)
    spec = dict(action_plan.get("spec", {}))
    if bool(spec.get("dryRun", True)) and not allow_preview_instructions:
        raise PodLifecycleApplyError(
            "MigActionPlan is marked dryRun=true. Pass allow_preview_instructions=True "
            "only for an explicit smoke test or controlled hardware validation."
        )

    preview = dict(spec.get("podLifecyclePreview", {}))
    if not preview:
        raise PodLifecycleApplyError("spec.podLifecyclePreview is empty.")

    k8s = _load_core_client()
    started = time.monotonic()
    chosen_node = node_name or _node_from_action_plan(spec)
    if not chosen_node:
        raise PodLifecycleApplyError(
            "No node selected. Pass node_name or include executorPreview.nodeName."
        )

    created_or_reused = []
    reloads = []
    deletes = []
    drains = [_drain_stub(row) for row in list(preview.get("drain", []))]

    for row in list(preview.get("createOrReuse", [])):
        created_or_reused.append(
            _create_or_reuse_workload_pod(
                core=k8s.core,
                client_=client_,
                namespace=namespace,
                action_plan_name=name,
                row=dict(row),
                node_name=chosen_node,
                mig_resource=mig_resource or _mig_resource_from_row(row),
                image=image,
                timeout_s=timeout_s,
            )
        )

    for row in list(preview.get("reloadInPlace", [])):
        reloads.append(
            _reload_batch_in_place(
                core=k8s.core,
                namespace=namespace,
                action_plan_name=name,
                row=dict(row),
                timeout_s=timeout_s,
                poll_interval_s=poll_interval_s,
            )
        )

    for row in list(preview.get("deleteOrRecycle", [])):
        deletes.append(
            _delete_or_recycle_pod(
                core=k8s.core,
                namespace=namespace,
                action_plan_name=name,
                row=dict(row),
                timeout_s=timeout_s,
            )
        )

    if cleanup:
        for item in created_or_reused:
            _delete_named_pod_and_configmap(
                core=k8s.core,
                namespace=namespace,
                pod_name=str(item["podName"]),
                configmap_name=str(item["configMapName"]),
                timeout_s=timeout_s,
            )

    summary = {
        "kind": "PodLifecycleApplySummary",
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "actionPlan": name,
        "namespace": namespace,
        "confirmedRealPodApply": True,
        "allowedPreviewInstructions": bool(allow_preview_instructions),
        "nodeName": chosen_node,
        "image": image,
        "createdOrReused": created_or_reused,
        "drains": drains,
        "reloads": reloads,
        "deleteOrRecycle": deletes,
        "cleanup": bool(cleanup),
        "timingsSeconds": {"total": round(time.monotonic() - started, 3)},
        "success": all(not item.get("podRecreatedDuringBatchUpdate", False) for item in reloads),
    }
    status = {
        "phase": "SucceededRealPodLifecycle" if summary["success"] else "FailedRealPodLifecycle",
        "approved": bool(action_plan.get("status", {}).get("approved", False)),
        "executed": bool(summary["success"]),
        "observedGeneration": int(action_plan.get("metadata", {}).get("generation", 0)),
        "validated": {"realPodLifecycle": summary},
        "message": "Real Pod lifecycle adapter executed workload create/reuse and reload-in-place steps.",
    }
    try:
        client_.patch_migactionplan_status(name=name, namespace=namespace, status=status)
    except Exception as exc:
        summary["statusPatchError"] = str(exc)
    return summary


def _load_core_client() -> Any:
    try:
        config.load_incluster_config()
    except ConfigException:
        config.load_kube_config()
    return type("CoreClientBundle", (), {"core": client.CoreV1Api()})()


def _create_or_reuse_workload_pod(
    core: client.CoreV1Api,
    client_: KubernetesClient,
    namespace: str,
    action_plan_name: str,
    row: dict[str, Any],
    node_name: str,
    mig_resource: str,
    image: str,
    timeout_s: float,
) -> dict[str, Any]:
    workload = str(row.get("workload") or row.get("new_workload") or "workload")
    batch_size = str(row.get("batch") or row.get("new_batch") or "1")
    pod_name = _workload_pod_name(action_plan_name, row, workload)
    configmap_name = f"{pod_name}-config"
    start = time.monotonic()
    expected_placement = expected_placement_from_row(row)
    placement_attempts = []
    reservations: list[dict[str, Any]] = []

    _apply_batch_configmap(core, namespace, configmap_name, batch_size, action_plan_name)
    max_attempts = int(row.get("placementRetryLimit") or (3 if expected_placement else 1))
    pod_uid = ""
    reused = False
    initial_observed_s = 0.0
    placement_verification: dict[str, Any] = {}
    for attempt in range(1, max_attempts + 1):
        _cleanup_slot_reservations(core=core, namespace=namespace, reservations=reservations, timeout_s=timeout_s)
        reservations = _prepare_slot_reservations(
            core=core,
            client_=client_,
            namespace=namespace,
            action_plan_name=action_plan_name,
            owner_pod_name=pod_name,
            row=row,
            node_name=node_name,
            mig_resource=mig_resource,
            image=image,
            timeout_s=timeout_s,
        )
        try:
            existing = core.read_namespaced_pod(pod_name, namespace)
            pod_uid = str(existing.metadata.uid)
            reused = True
        except client.exceptions.ApiException as exc:
            if int(getattr(exc, "status", 0)) != 404:
                raise
            core.create_namespaced_pod(
                namespace=namespace,
                body=_workload_pod_manifest(
                    pod_name=pod_name,
                    configmap_name=configmap_name,
                    action_plan_name=action_plan_name,
                    workload=workload,
                    node_name=node_name,
                    mig_resource=mig_resource,
                    image=image,
                ),
            )
            pod_uid = _wait_for_pod_ready(core, namespace, pod_name, timeout_s)
            reused = False

        initial_observed_s = _wait_for_log_text(
            core=core,
            namespace=namespace,
            pod_name=pod_name,
            text=f"batch_size={batch_size}",
            timeout_s=timeout_s,
            poll_interval_s=1.0,
        )
        placement_verification = _verify_expected_placement(
            core=core,
            client_=client_,
            namespace=namespace,
            pod_name=pod_name,
            row=row,
        )
        placement_verification["attempt"] = attempt
        placement_verification["reservations"] = list(reservations)
        placement_attempts.append(dict(placement_verification))
        if placement_verification["success"]:
            break
        if reused or attempt >= max_attempts:
            _cleanup_slot_reservations(core=core, namespace=namespace, reservations=reservations, timeout_s=timeout_s)
            raise PodLifecycleApplyError(
                "exact placement verification failed for "
                f"{namespace}/{pod_name}: {placement_verification}"
            )
        _delete_named_pod(core=core, namespace=namespace, pod_name=pod_name, timeout_s=timeout_s)
    _cleanup_slot_reservations(core=core, namespace=namespace, reservations=reservations, timeout_s=timeout_s)

    return {
        "workload": workload,
        "podName": pod_name,
        "configMapName": configmap_name,
        "nodeName": node_name,
        "requestedMigResource": mig_resource,
        "batchSize": batch_size,
        "podUid": pod_uid,
        "reused": reused,
        "readySeconds": round(time.monotonic() - start, 3),
        "initialBatchObservedSeconds": round(initial_observed_s, 3),
        "placementVerification": placement_verification,
        "placementAttempts": placement_attempts,
        "placementReservations": reservations,
        "recentLogs": _tail_logs(core, namespace, pod_name, lines=30),
    }


def _reload_batch_in_place(
    core: client.CoreV1Api,
    namespace: str,
    action_plan_name: str,
    row: dict[str, Any],
    timeout_s: float,
    poll_interval_s: float,
) -> dict[str, Any]:
    workload = str(row.get("workload") or row.get("new_workload") or "workload")
    new_batch = str(row.get("new_batch") or row.get("batch") or "1")
    pod_name = _workload_pod_name(action_plan_name, row, workload)
    configmap_name = f"{pod_name}-config"
    before_uid = _pod_uid(core, namespace, pod_name)
    start = time.monotonic()
    core.patch_namespaced_config_map(
        name=configmap_name,
        namespace=namespace,
        body={"data": {"batch_size": new_batch}},
    )
    observed_s = _wait_for_log_text(
        core=core,
        namespace=namespace,
        pod_name=pod_name,
        text=f"batch_size={new_batch}",
        timeout_s=timeout_s,
        poll_interval_s=poll_interval_s,
    )
    after_uid = _pod_uid(core, namespace, pod_name)
    return {
        "workload": workload,
        "podName": pod_name,
        "configMapName": configmap_name,
        "oldBatchSize": row.get("old_batch"),
        "newBatchSize": new_batch,
        "podUidBeforeUpdate": before_uid,
        "podUidAfterUpdate": after_uid,
        "podRecreatedDuringBatchUpdate": before_uid != after_uid,
        "observedSecondsAfterPatch": round(observed_s, 3),
        "totalSeconds": round(time.monotonic() - start, 3),
        "recentLogs": _tail_logs(core, namespace, pod_name, lines=30),
    }


def _delete_or_recycle_pod(
    core: client.CoreV1Api,
    namespace: str,
    action_plan_name: str,
    row: dict[str, Any],
    timeout_s: float,
) -> dict[str, Any]:
    workload = str(row.get("workload") or row.get("old_workload") or "workload")
    pod_name = _workload_pod_name(action_plan_name, row, workload)
    configmap_name = f"{pod_name}-config"
    start = time.monotonic()
    _delete_named_pod_and_configmap(core, namespace, pod_name, configmap_name, timeout_s)
    return {
        "workload": workload,
        "podName": pod_name,
        "configMapName": configmap_name,
        "deleted": True,
        "seconds": round(time.monotonic() - start, 3),
    }


def _drain_stub(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "workload": row.get("workload"),
        "podAction": row.get("podAction") or "drain",
        "previewOnly": False,
        "implementedAs": "no-traffic-stub",
        "status": "SucceededNoTraffic",
        "message": "No real router is configured; drain is treated as already empty.",
    }


def _apply_batch_configmap(
    core: client.CoreV1Api,
    namespace: str,
    name: str,
    batch_size: str,
    action_plan_name: str,
) -> None:
    body = {
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "or-sim-workload",
                "mig.or-sim.io/owner-action-plan": action_plan_name,
            },
        },
        "data": {"batch_size": str(batch_size)},
    }
    try:
        core.create_namespaced_config_map(namespace=namespace, body=body)
    except client.exceptions.ApiException as exc:
        if int(getattr(exc, "status", 0)) != 409:
            raise
        core.patch_namespaced_config_map(name=name, namespace=namespace, body=body)


def _workload_pod_manifest(
    pod_name: str,
    configmap_name: str,
    action_plan_name: str,
    workload: str,
    node_name: str,
    mig_resource: str,
    image: str,
) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": pod_name,
            "labels": {
                "app.kubernetes.io/name": "or-sim-workload",
                "mig.or-sim.io/owner-action-plan": action_plan_name,
                "mig.or-sim.io/workload": _label_value(workload),
            },
            "annotations": _placement_annotations(action_plan_name=action_plan_name),
        },
        "spec": {
            "restartPolicy": "Never",
            "nodeSelector": {"kubernetes.io/hostname": node_name},
            "containers": [
                {
                    "name": "worker",
                    "image": image,
                    "command": ["/bin/sh", "-c"],
                    "args": [
                        (
                            "set -eu; "
                            "echo 'or-sim workload lifecycle starting'; "
                            "nvidia-smi -L || true; "
                            "i=0; "
                            "while true; do "
                            "  batch=$(cat /etc/or-sim/batch_size 2>/dev/null || echo missing); "
                            "  echo \"tick=$i batch_size=$batch\"; "
                            "  i=$((i+1)); "
                            "  sleep 1; "
                            "done"
                        )
                    ],
                    "volumeMounts": [
                        {"name": "batch-config", "mountPath": "/etc/or-sim", "readOnly": True}
                    ],
                    "resources": {"limits": {mig_resource: 1}},
                }
            ],
            "volumes": [{"name": "batch-config", "configMap": {"name": configmap_name}}],
        },
    }


def _verify_expected_placement(
    core: client.CoreV1Api,
    client_: KubernetesClient,
    namespace: str,
    pod_name: str,
    row: dict[str, Any],
) -> dict[str, Any]:
    expected = expected_placement_from_row(row)
    pod = core.read_namespaced_pod(pod_name, namespace)
    observed_node = str(pod.spec.node_name or "")
    if not expected:
        return {
            "required": False,
            "success": True,
            "message": "No expectedPlacement or physical_gpu_id+slot was provided.",
            "podNodeName": observed_node,
        }

    registry = client_.get_physicalgpuregistry(name="default", namespace=namespace)
    registry_status = dict((registry or {}).get("status", {}))
    bindings = {
        str(physical_id): dict(binding)
        for physical_id, binding in dict(registry_status.get("bindings", {})).items()
    }
    logical_slots = []
    for binding in bindings.values():
        logical_slots.extend(list(binding.get("logicalMigSlots", [])))
    if not logical_slots:
        logical_slots = logical_mig_slots_from_bindings(bindings)
    expected_slot = find_logical_slot(logical_slots, expected)
    if expected_slot is None:
        return {
            "required": True,
            "success": False,
            "expectedPlacement": expected,
            "podNodeName": observed_node,
            "message": "Expected logical slot was not found in PhysicalGpuRegistry/default.",
        }

    expected_uuid = str(expected_slot.get("migDeviceUuid") or "")
    output = _pod_nvidia_smi_l(core=core, namespace=namespace, pod_name=pod_name)
    actual_uuids = parse_mig_uuids_from_nvidia_smi_l(output)
    success = bool(expected_uuid and expected_uuid in actual_uuids)
    annotations = {
        "mig.or-sim.io/placement-verified": "true" if success else "false",
        "mig.or-sim.io/expected-physical-gpu-id": str(expected_slot.get("physicalGpuId") or ""),
        "mig.or-sim.io/expected-slot": (
            f"{expected_slot.get('slotStart')}-{expected_slot.get('slotEnd')}-"
            f"{expected_slot.get('profile')}"
        ),
        "mig.or-sim.io/expected-mig-device-uuid": expected_uuid,
        "mig.or-sim.io/actual-mig-device-uuids": ",".join(actual_uuids),
    }
    core.patch_namespaced_pod(
        name=pod_name,
        namespace=namespace,
        body={"metadata": {"annotations": annotations}},
    )
    return {
        "required": True,
        "success": success,
        "expectedPlacement": expected,
        "resolvedLogicalSlot": expected_slot,
        "expectedMigDeviceUuid": expected_uuid,
        "actualMigDeviceUuids": actual_uuids,
        "podNodeName": observed_node,
        "nvidiaSmiL": output,
        "message": (
            "Pod is bound to the expected logical slot's current MIG UUID."
            if success
            else "Pod is not bound to the expected logical slot's current MIG UUID."
        ),
    }


def _prepare_slot_reservations(
    core: client.CoreV1Api,
    client_: KubernetesClient,
    namespace: str,
    action_plan_name: str,
    owner_pod_name: str,
    row: dict[str, Any],
    node_name: str,
    mig_resource: str,
    image: str,
    timeout_s: float,
) -> list[dict[str, Any]]:
    expected = expected_placement_from_row(row)
    if not expected:
        return []
    registry = client_.get_physicalgpuregistry(name="default", namespace=namespace)
    registry_status = dict((registry or {}).get("status", {}))
    bindings = {
        str(physical_id): dict(binding)
        for physical_id, binding in dict(registry_status.get("bindings", {})).items()
    }
    logical_slots = []
    for binding in bindings.values():
        logical_slots.extend(list(binding.get("logicalMigSlots", [])))
    if not logical_slots:
        logical_slots = logical_mig_slots_from_bindings(bindings)
    expected_slot = find_logical_slot(logical_slots, expected)
    if expected_slot is None:
        return []

    peer_slots = [
        dict(slot)
        for slot in logical_slots
        if str(slot.get("physicalGpuId") or "") == str(expected_slot.get("physicalGpuId") or "")
        and str(slot.get("gpuOperatorProfile") or "") == str(expected_slot.get("gpuOperatorProfile") or "")
        and str(slot.get("migDeviceUuid") or "") != str(expected_slot.get("migDeviceUuid") or "")
    ]
    reservations = []
    for idx, peer in enumerate(peer_slots):
        reservation = _create_slot_reservation(
            core=core,
            namespace=namespace,
            action_plan_name=action_plan_name,
            owner_pod_name=owner_pod_name,
            peer=peer,
            reservation_index=idx,
            node_name=node_name,
            mig_resource=mig_resource,
            image=image,
            timeout_s=timeout_s,
        )
        reservations.append(reservation)
    return reservations


def _create_slot_reservation(
    core: client.CoreV1Api,
    namespace: str,
    action_plan_name: str,
    owner_pod_name: str,
    peer: dict[str, Any],
    reservation_index: int,
    node_name: str,
    mig_resource: str,
    image: str,
    timeout_s: float,
) -> dict[str, Any]:
    expected_uuid = str(peer.get("migDeviceUuid") or "")
    pod_name = _dns1123(
        f"{owner_pod_name}-reserve-{reservation_index}-{peer.get('slotStart')}-{peer.get('slotEnd')}",
        max_len=63,
    )
    for attempt in range(1, 4):
        _delete_named_pod(core=core, namespace=namespace, pod_name=pod_name, timeout_s=timeout_s)
        core.create_namespaced_pod(
            namespace=namespace,
            body=_reservation_pod_manifest(
                pod_name=pod_name,
                action_plan_name=action_plan_name,
                node_name=node_name,
                mig_resource=mig_resource,
                image=image,
                peer=peer,
            ),
        )
        _wait_for_pod_ready(core, namespace, pod_name, timeout_s)
        output = _pod_nvidia_smi_l(core=core, namespace=namespace, pod_name=pod_name)
        actual_uuids = parse_mig_uuids_from_nvidia_smi_l(output)
        if expected_uuid in actual_uuids:
            return {
                "podName": pod_name,
                "expectedReservedMigDeviceUuid": expected_uuid,
                "actualMigDeviceUuids": actual_uuids,
                "logicalSlot": peer,
                "attempt": attempt,
                "success": True,
            }
    raise PodLifecycleApplyError(
        f"failed to reserve non-target logical slot {peer.get('slot')} with {pod_name}"
    )


def _reservation_pod_manifest(
    pod_name: str,
    action_plan_name: str,
    node_name: str,
    mig_resource: str,
    image: str,
    peer: dict[str, Any],
) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": pod_name,
            "labels": {
                "app.kubernetes.io/name": "or-sim-workload",
                "mig.or-sim.io/owner-action-plan": action_plan_name,
                "mig.or-sim.io/placement-reservation": "true",
            },
            "annotations": {
                "mig.or-sim.io/reserved-slot": (
                    f"{peer.get('slotStart')}-{peer.get('slotEnd')}-{peer.get('profile')}"
                ),
                "mig.or-sim.io/reserved-mig-device-uuid": str(peer.get("migDeviceUuid") or ""),
            },
        },
        "spec": {
            "restartPolicy": "Never",
            "nodeSelector": {"kubernetes.io/hostname": node_name},
            "containers": [
                {
                    "name": "worker",
                    "image": image,
                    "command": ["/bin/sh", "-c"],
                    "args": ["nvidia-smi -L; sleep 3600"],
                    "resources": {"limits": {mig_resource: 1}},
                }
            ],
        },
    }


def _cleanup_slot_reservations(
    core: client.CoreV1Api,
    namespace: str,
    reservations: list[dict[str, Any]],
    timeout_s: float,
) -> None:
    for reservation in reservations:
        pod_name = str(reservation.get("podName") or "")
        if pod_name:
            _delete_named_pod(core=core, namespace=namespace, pod_name=pod_name, timeout_s=timeout_s)


def _pod_nvidia_smi_l(
    core: client.CoreV1Api,
    namespace: str,
    pod_name: str,
) -> str:
    from kubernetes.stream import stream

    return str(
        stream(
            core.connect_get_namespaced_pod_exec,
            pod_name,
            namespace,
            container="worker",
            command=["nvidia-smi", "-L"],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
        )
    )


def _placement_annotations(action_plan_name: str) -> dict[str, str]:
    return {
        "mig.or-sim.io/owner-action-plan": action_plan_name,
        "mig.or-sim.io/placement-contract": "logical-slot-then-current-mig-uuid-verify",
    }


def _wait_for_pod_ready(
    core: client.CoreV1Api,
    namespace: str,
    pod_name: str,
    timeout_s: float,
) -> str:
    started = time.monotonic()
    watcher = watch.Watch()
    try:
        for event in watcher.stream(
            core.list_namespaced_pod,
            namespace=namespace,
            field_selector=f"metadata.name={pod_name}",
            timeout_seconds=max(1, int(timeout_s)),
        ):
            pod = event["object"]
            if pod.status.phase == "Failed":
                raise PodLifecycleApplyError(f"pod {namespace}/{pod_name} failed")
            for condition in pod.status.conditions or []:
                if condition.type == "Ready" and condition.status == "True":
                    return str(pod.metadata.uid)
            if time.monotonic() - started > timeout_s:
                break
    finally:
        watcher.stop()
    raise TimeoutError(f"timed out waiting for pod {namespace}/{pod_name} Ready")


def _wait_for_pod_deleted(
    core: client.CoreV1Api,
    namespace: str,
    pod_name: str,
    timeout_s: float,
) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            core.read_namespaced_pod(pod_name, namespace)
        except client.exceptions.ApiException as exc:
            if int(getattr(exc, "status", 0)) == 404:
                return
            raise
        time.sleep(1.0)
    raise TimeoutError(f"timed out waiting for pod {namespace}/{pod_name} deletion")


def _wait_for_log_text(
    core: client.CoreV1Api,
    namespace: str,
    pod_name: str,
    text: str,
    timeout_s: float,
    poll_interval_s: float,
) -> float:
    started = time.monotonic()
    while time.monotonic() - started <= timeout_s:
        logs = _tail_logs(core, namespace, pod_name, lines=80)
        if text in logs:
            return time.monotonic() - started
        time.sleep(poll_interval_s)
    raise TimeoutError(f"timed out waiting for log text {text!r}")


def _tail_logs(core: client.CoreV1Api, namespace: str, pod_name: str, lines: int) -> str:
    return core.read_namespaced_pod_log(
        name=pod_name,
        namespace=namespace,
        container="worker",
        tail_lines=lines,
    )


def _pod_uid(core: client.CoreV1Api, namespace: str, pod_name: str) -> str:
    return str(core.read_namespaced_pod(pod_name, namespace).metadata.uid)


def _delete_named_pod_and_configmap(
    core: client.CoreV1Api,
    namespace: str,
    pod_name: str,
    configmap_name: str,
    timeout_s: float,
) -> None:
    _delete_named_pod(core=core, namespace=namespace, pod_name=pod_name, timeout_s=timeout_s)
    try:
        core.delete_namespaced_config_map(name=configmap_name, namespace=namespace)
    except client.exceptions.ApiException as exc:
        if int(getattr(exc, "status", 0)) != 404:
            raise


def _delete_named_pod(
    core: client.CoreV1Api,
    namespace: str,
    pod_name: str,
    timeout_s: float,
) -> None:
    try:
        core.delete_namespaced_pod(name=pod_name, namespace=namespace, grace_period_seconds=0)
        _wait_for_pod_deleted(core, namespace, pod_name, timeout_s)
    except client.exceptions.ApiException as exc:
        if int(getattr(exc, "status", 0)) != 404:
            raise


def _node_from_action_plan(spec: dict[str, Any]) -> str | None:
    executor_preview = dict(spec.get("executorPreview", {}))
    node_name = executor_preview.get("nodeName")
    if node_name:
        return str(node_name)
    would_patch = dict(executor_preview.get("wouldPatchNodeLabels", {}))
    if len(would_patch) == 1:
        return str(next(iter(would_patch)))
    return None


def _mig_resource_from_row(row: dict[str, Any]) -> str:
    slot = row.get("slot") or []
    if isinstance(slot, list) and len(slot) >= 3:
        profile = str(slot[2])
        if profile in PROFILE_TO_A100_40GB_RESOURCE:
            return PROFILE_TO_A100_40GB_RESOURCE[profile]
    return DEFAULT_MIG_RESOURCE


def _workload_pod_name(action_plan_name: str, row: dict[str, Any], workload: str) -> str:
    slot = row.get("slot") or []
    slot_token = "-".join(str(part) for part in slot) if isinstance(slot, list) and slot else "noslot"
    return _dns1123(f"or-sim-{action_plan_name}-{workload}-{slot_token}", max_len=63)


def _dns1123(value: str, max_len: int = 63) -> str:
    cleaned = re.sub(r"[^a-z0-9-]+", "-", value.lower()).strip("-")
    cleaned = re.sub(r"-+", "-", cleaned)
    if len(cleaned) <= max_len:
        return cleaned or "or-sim-workload"
    return cleaned[:max_len].rstrip("-")


def _label_value(value: str) -> str:
    return _dns1123(value, max_len=63)
