from __future__ import annotations

import time
from typing import Any

import yaml

from api.k8s_api import KubernetesClient, PythonKubernetesClient


MIG_CONFIG_LABEL = "nvidia.com/mig.config"
MIG_CONFIG_STATE_LABEL = "nvidia.com/mig.config.state"
GPU_OPERATOR_NAMESPACE = "gpu-operator"
GPU_OPERATOR_CLUSTERPOLICY = "cluster-policy"
OR_SIM_MIG_CONFIGMAP = "or-sim-mig-parted-config"


class MigLabelApplyError(RuntimeError):
    pass


def apply_mig_labels_from_action_plan(
    name: str,
    namespace: str = "or-sim",
    confirm_real_mig_apply: bool = False,
    allow_preview_instructions: bool = False,
    wait: bool = True,
    timeout_s: float = 900.0,
    poll_interval_s: float = 5.0,
    client: KubernetesClient | None = None,
) -> dict[str, Any]:
    if not confirm_real_mig_apply:
        raise MigLabelApplyError(
            "Refusing to patch node MIG labels without confirm_real_mig_apply=True."
        )
    client = client or PythonKubernetesClient()
    action_plan = client.get_migactionplan(name=name, namespace=namespace)
    spec = dict(action_plan.get("spec", {}))
    if bool(spec.get("dryRun", True)) and not allow_preview_instructions:
        raise MigLabelApplyError(
            "MigActionPlan is marked dryRun=true. Pass allow_preview_instructions=True "
            "only for an explicit hardware-interface smoke test."
        )
    executor_preview = dict(spec.get("executorPreview", {}))
    if executor_preview.get("executor") != "nvidia-gpu-operator":
        raise MigLabelApplyError(f"Unsupported executor: {executor_preview.get('executor')}")
    if executor_preview.get("gpuOperatorLabel") != MIG_CONFIG_LABEL:
        raise MigLabelApplyError(
            f"executorPreview.gpuOperatorLabel must be {MIG_CONFIG_LABEL}"
        )
    unresolved = list(executor_preview.get("unresolvedPhysicalGpuIds", []))
    if unresolved:
        raise MigLabelApplyError(f"Physical GPU bindings are unresolved: {unresolved}")
    would_patch = dict(executor_preview.get("wouldPatchNodeLabels", {}))
    if not would_patch:
        raise MigLabelApplyError("executorPreview.wouldPatchNodeLabels is empty.")

    patches = []
    preflight = _validate_gpu_operator_mig_configs(
        client=client,
        target_configs=_target_configs_from_would_patch(would_patch),
    )
    for node_name, labels in sorted(would_patch.items()):
        label_map = dict(labels)
        if set(label_map) != {MIG_CONFIG_LABEL}:
            raise MigLabelApplyError(
                f"Refusing to patch labels other than {MIG_CONFIG_LABEL}: {label_map}"
            )
        target_config = str(label_map[MIG_CONFIG_LABEL])
        before = _node_mig_summary(client.get_node(str(node_name)))
        client.patch_node_labels(
            name=str(node_name),
            labels={MIG_CONFIG_LABEL: target_config},
            remove_labels=[MIG_CONFIG_STATE_LABEL],
        )
        after_patch = _node_mig_summary(client.get_node(str(node_name)))
        observed = (
            _wait_for_mig_success(
                client=client,
                node_name=str(node_name),
                target_config=target_config,
                timeout_s=timeout_s,
                poll_interval_s=poll_interval_s,
            )
            if wait
            else after_patch
        )
        patches.append(
            {
                "nodeName": str(node_name),
                "targetMigConfig": target_config,
                "before": before,
                "afterPatch": after_patch,
                "observed": observed,
            }
        )

    return {
        "kind": "MigLabelApplySummary",
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "actionPlan": name,
        "namespace": namespace,
        "confirmedRealMigApply": True,
        "allowedPreviewInstructions": bool(allow_preview_instructions),
        "waitedForSuccess": bool(wait),
        "gpuOperatorPreflight": preflight,
        "patches": patches,
    }


def summarize_mig_labels_from_action_plan(
    name: str,
    namespace: str = "or-sim",
    client: KubernetesClient | None = None,
) -> dict[str, Any]:
    client = client or PythonKubernetesClient()
    action_plan = client.get_migactionplan(name=name, namespace=namespace)
    spec = dict(action_plan.get("spec", {}))
    executor_preview = dict(spec.get("executorPreview", {}))
    would_patch = dict(executor_preview.get("wouldPatchNodeLabels", {}))
    node_summaries = {
        str(node_name): _node_mig_summary(client.get_node(str(node_name)))
        for node_name in sorted(would_patch)
    }
    preflight = _summarize_gpu_operator_mig_configs(
        client=client,
        target_configs=_target_configs_from_would_patch(would_patch, strict=False),
    )
    return {
        "kind": "MigLabelApplyPlan",
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "actionPlan": name,
        "namespace": namespace,
        "dryRunActionPlan": bool(spec.get("dryRun", True)),
        "executor": executor_preview.get("executor"),
        "wouldPatchNodeLabels": would_patch,
        "gpuOperatorPreflight": preflight,
        "unresolvedPhysicalGpuIds": list(executor_preview.get("unresolvedPhysicalGpuIds", [])),
        "currentNodeMig": node_summaries,
        "notes": [
            "This summary is read-only.",
            "Real execution patches nvidia.com/mig.config and removes nvidia.com/mig.config.state.",
        ],
    }


def _target_configs_from_would_patch(
    would_patch: dict[str, Any],
    strict: bool = True,
) -> list[str]:
    target_configs = []
    for node_name, labels in sorted(would_patch.items()):
        if not isinstance(labels, dict) or MIG_CONFIG_LABEL not in labels:
            if strict:
                raise MigLabelApplyError(
                    f"Missing {MIG_CONFIG_LABEL} patch for node {node_name}: {labels}"
                )
            continue
        target_configs.append(str(labels[MIG_CONFIG_LABEL]))
    return target_configs


def _validate_gpu_operator_mig_configs(
    client: KubernetesClient,
    target_configs: list[str],
) -> dict[str, Any]:
    summary = _summarize_gpu_operator_mig_configs(
        client=client,
        target_configs=target_configs,
    )
    errors = list(summary.get("errors", []))
    if errors:
        raise MigLabelApplyError("GPU Operator MIG config preflight failed: " + "; ".join(errors))
    return summary


def _summarize_gpu_operator_mig_configs(
    client: KubernetesClient,
    target_configs: list[str],
) -> dict[str, Any]:
    clusterpolicy = client.get_clusterpolicy(GPU_OPERATOR_CLUSTERPOLICY)
    spec = dict(clusterpolicy.get("spec", {}))
    mig_manager = dict(spec.get("migManager", {}))
    config_ref = dict(mig_manager.get("config", {}))
    configured_name = str(config_ref.get("name", ""))
    configmap_name = configured_name or "default-mig-parted-config"
    configmap = client.get_configmap(configmap_name, GPU_OPERATOR_NAMESPACE)
    data = dict(configmap.get("data", {}))
    raw_config = data.get("config.yaml", "")
    mig_configs = _mig_configs_from_raw_config(raw_config)
    available = sorted(mig_configs)
    targets = sorted(set(target_configs))
    missing = [target for target in targets if target not in mig_configs]
    errors = []
    if configured_name != OR_SIM_MIG_CONFIGMAP:
        errors.append(
            f"ClusterPolicy {GPU_OPERATOR_CLUSTERPOLICY} points to "
            f"{configured_name or '<default>'}, expected {OR_SIM_MIG_CONFIGMAP}"
        )
    if missing:
        errors.append(
            "Target MIG configs missing from "
            f"{GPU_OPERATOR_NAMESPACE}/{configmap_name}: {missing}"
        )
    return {
        "clusterPolicy": GPU_OPERATOR_CLUSTERPOLICY,
        "namespace": GPU_OPERATOR_NAMESPACE,
        "configMap": configmap_name,
        "expectedConfigMap": OR_SIM_MIG_CONFIGMAP,
        "targetConfigs": targets,
        "missingTargetConfigs": missing,
        "availableConfigCount": len(available),
        "errors": errors,
    }


def _mig_configs_from_raw_config(raw_config: str) -> dict[str, Any]:
    if not raw_config:
        return {}
    parsed = yaml.safe_load(raw_config)
    if not isinstance(parsed, dict):
        return {}
    mig_configs = parsed.get("mig-configs", {})
    return mig_configs if isinstance(mig_configs, dict) else {}


def _wait_for_mig_success(
    client: KubernetesClient,
    node_name: str,
    target_config: str,
    timeout_s: float,
    poll_interval_s: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    last = {}
    while time.monotonic() < deadline:
        last = _node_mig_summary(client.get_node(node_name))
        if (
            last.get("migConfig") == target_config
            and last.get("migConfigState") == "success"
        ):
            return last
        time.sleep(poll_interval_s)
    raise MigLabelApplyError(
        f"Timed out waiting for {node_name} {MIG_CONFIG_LABEL}={target_config} "
        f"to reach state success. Last observed: {last}"
    )


def _node_mig_summary(node: dict[str, Any]) -> dict[str, Any]:
    metadata = dict(node.get("metadata", {}))
    labels = dict(metadata.get("labels", {}))
    status = dict(node.get("status", {}))
    capacity = dict(status.get("capacity", {}))
    allocatable = dict(status.get("allocatable", {}))
    mig_capacity = {
        key: value
        for key, value in sorted(capacity.items())
        if key.startswith("nvidia.com/mig-")
    }
    mig_allocatable = {
        key: value
        for key, value in sorted(allocatable.items())
        if key.startswith("nvidia.com/mig-")
    }
    profile_counts = {
        key: value
        for key, value in sorted(labels.items())
        if key.startswith("nvidia.com/mig-") and key.endswith(".count")
    }
    return {
        "nodeName": metadata.get("name"),
        "migConfig": labels.get(MIG_CONFIG_LABEL),
        "migConfigState": labels.get(MIG_CONFIG_STATE_LABEL),
        "profileCounts": profile_counts,
        "migCapacity": mig_capacity,
        "migAllocatable": mig_allocatable,
    }
