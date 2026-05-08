from __future__ import annotations

import hashlib
import re
from typing import Any


GPU_OPERATOR_MIG_CONFIG_LABEL = "nvidia.com/mig.config"


def build_gpu_operator_executor_preview(status: dict[str, Any]) -> dict[str, Any]:
    canonical_state = dict(status.get("canonicalNextState", {}))
    metadata = dict(canonical_state.get("metadata", {}))
    physical_id_map = {
        str(logical_gpu_id): str(physical_gpu_id)
        for logical_gpu_id, physical_gpu_id in dict(metadata.get("physical_id_map", {})).items()
    }
    physical_bindings = _physical_gpu_bindings(metadata)
    gpu_targets = []
    unresolved_physical_gpu_ids = []

    for raw_gpu in sorted(canonical_state.get("gpus", []), key=lambda gpu: int(gpu.get("gpuId", 0))):
        logical_gpu_id = str(raw_gpu.get("gpuId"))
        physical_gpu_id = physical_id_map.get(logical_gpu_id, logical_gpu_id)
        binding = dict(physical_bindings.get(physical_gpu_id, {}))
        node_name = binding.get("nodeName")
        device_index = binding.get("deviceIndex")
        target = {
            "logicalGpuId": int(raw_gpu.get("gpuId")),
            "physicalGpuId": physical_gpu_id,
            "nodeName": node_name,
            "deviceIndex": device_index,
            "targetTemplate": _template_from_gpu(raw_gpu),
            "targetInstances": _target_instances(raw_gpu),
        }
        gpu_targets.append(target)
        if not node_name or device_index is None:
            unresolved_physical_gpu_ids.append(physical_gpu_id)

    node_targets: dict[str, list[dict[str, Any]]] = {}
    for target in gpu_targets:
        node_name = target.get("nodeName")
        if node_name and target.get("deviceIndex") is not None:
            node_targets.setdefault(str(node_name), []).append(target)

    mig_manager_target_configs = []
    would_patch_node_labels = {}
    for node_name, targets in sorted(node_targets.items()):
        config_name = _mig_config_name(node_name=node_name, targets=targets)
        mig_manager_target_configs.append(
            {
                "nodeName": node_name,
                "configName": config_name,
                "gpus": [
                    {
                        "deviceIndex": target["deviceIndex"],
                        "physicalGpuId": target["physicalGpuId"],
                        "targetTemplate": target["targetTemplate"],
                        "targetInstances": target["targetInstances"],
                    }
                    for target in sorted(targets, key=lambda item: int(item.get("deviceIndex", 0)))
                ],
            }
        )
        would_patch_node_labels[node_name] = {
            GPU_OPERATOR_MIG_CONFIG_LABEL: config_name,
        }

    return {
        "previewOnly": True,
        "executor": "nvidia-gpu-operator",
        "targetApi": "NVIDIA GPU Operator MIG Manager",
        "gpuOperatorLabel": GPU_OPERATOR_MIG_CONFIG_LABEL,
        "gpuTargets": gpu_targets,
        "migManagerTargetConfigs": mig_manager_target_configs,
        "wouldPatchNodeLabels": would_patch_node_labels,
        "requiredObserverFields": [
            "physicalGpuId",
            "nodeName",
            "deviceIndex",
            "observedMigInstances",
        ],
        "unresolvedPhysicalGpuIds": sorted(set(unresolved_physical_gpu_ids)),
        "notes": [
            "Preview only; the actuator must not patch Nodes or run hardware commands in dry-run mode.",
            "A real actuator must observe node/device bindings before converting this preview into MIG Manager input.",
            "After real execution, canonicalize the observed post-action MIG state, not the planned state.",
        ],
    }


def validate_executor_preview(preview: dict[str, Any] | None) -> list[str]:
    if not isinstance(preview, dict):
        return ["executorPreview is required"]
    reasons = []
    if not bool(preview.get("previewOnly", False)):
        reasons.append("executorPreview.previewOnly must be true for the dry-run actuator")
    if preview.get("executor") != "nvidia-gpu-operator":
        reasons.append(f"executorPreview.executor {preview.get('executor')} is not supported")
    if preview.get("gpuOperatorLabel") != GPU_OPERATOR_MIG_CONFIG_LABEL:
        reasons.append(f"executorPreview.gpuOperatorLabel must be {GPU_OPERATOR_MIG_CONFIG_LABEL}")
    if not isinstance(preview.get("gpuTargets"), list):
        reasons.append("executorPreview.gpuTargets must be a list")
    if preview.get("wouldPatchNodeLabels") and preview.get("unresolvedPhysicalGpuIds"):
        reasons.append("executorPreview cannot patch node labels while physical GPU bindings are unresolved")
    return reasons


def _physical_gpu_bindings(metadata: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw = (
        metadata.get("physical_gpu_bindings")
        or metadata.get("physicalGpuBindings")
        or metadata.get("physical_gpu_node_map")
        or metadata.get("physicalGpuNodeMap")
        or {}
    )
    bindings = {}
    for physical_gpu_id, value in dict(raw).items():
        if isinstance(value, str):
            bindings[str(physical_gpu_id)] = {"nodeName": value, "deviceIndex": None}
        elif isinstance(value, dict):
            bindings[str(physical_gpu_id)] = {
                "nodeName": value.get("nodeName") or value.get("node"),
                "deviceIndex": value.get("deviceIndex")
                if value.get("deviceIndex") is not None
                else value.get("gpuIndex"),
            }
    return bindings


def _template_from_gpu(raw_gpu: dict[str, Any]) -> str:
    profiles = [
        str(inst.get("profile"))
        for inst in sorted(raw_gpu.get("instances", []), key=lambda item: int(item.get("start", 0)))
        if str(inst.get("profile")) != "void"
    ]
    return "+".join(profiles) if profiles else "empty"


def _target_instances(raw_gpu: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "start": int(inst.get("start")),
            "end": int(inst.get("end")),
            "profile": str(inst.get("profile")),
        }
        for inst in sorted(raw_gpu.get("instances", []), key=lambda item: int(item.get("start", 0)))
        if str(inst.get("profile")) != "void"
    ]


def _mig_config_name(node_name: str, targets: list[dict[str, Any]]) -> str:
    fingerprint = "|".join(
        f"{target.get('deviceIndex')}={target.get('targetTemplate')}"
        for target in sorted(targets, key=lambda item: int(item.get("deviceIndex", 0)))
    )
    digest = hashlib.sha1(f"{node_name}|{fingerprint}".encode("utf-8")).hexdigest()[:10]
    return _label_value(f"or-sim-{digest}")


def _label_value(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("._-")
    return cleaned[:63] or "or-sim-preview"
