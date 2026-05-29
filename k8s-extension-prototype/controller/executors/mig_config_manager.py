from __future__ import annotations

import hashlib
from typing import Any

import yaml

from api.k8s_api import KubernetesClient


GPU_OPERATOR_NAMESPACE = "gpu-operator"
GPU_OPERATOR_CLUSTERPOLICY = "cluster-policy"
OR_SIM_MIG_CONFIGMAP = "or-sim-mig-parted-config"
EMPTY_MIG_CONFIG = "or-sim-empty"

PROFILE_TO_GPU_OPERATOR = {
    "1g": "1g.5gb",
    "2g": "2g.10gb",
    "3g": "3g.20gb",
    "4g": "4g.20gb",
    "7g": "7g.40gb",
}

MIGRANT_TEMPLATE_CONFIGS = {
    "or-sim-empty": [],
    "or-sim-4-3": ["4g", "3g"],
    "or-sim-4-2-1": ["4g", "2g", "1g"],
    "or-sim-4-1-1-1": ["4g", "1g", "1g", "1g"],
    "or-sim-3-2-1": ["3g", "2g", "1g"],
    "or-sim-3-1-1-1": ["3g", "1g", "1g", "1g"],
    "or-sim-2-2-3": ["2g", "2g", "3g"],
    "or-sim-3-2-1-1": ["3g", "2g", "1g", "1g"],
    "or-sim-3-1-1-1-1": ["3g", "1g", "1g", "1g", "1g"],
    "or-sim-2-2-2-1": ["2g", "2g", "2g", "1g"],
    "or-sim-2-2-1-1-1": ["2g", "2g", "1g", "1g", "1g"],
    "or-sim-2-1-1-1-1-1": ["2g", "1g", "1g", "1g", "1g", "1g"],
}


def config_for_profiles(profiles: list[str], devices: Any = "all") -> list[dict[str, Any]]:
    return [
        {
            "devices": devices,
            "mig-enabled": True,
            "mig-devices": _profile_counts(profiles),
        }
    ]


def config_for_gpu_targets(gpus: list[dict[str, Any]]) -> list[dict[str, Any]]:
    entries = []
    for gpu in sorted(gpus, key=lambda item: int(item.get("deviceIndex", 0))):
        device_index = int(gpu["deviceIndex"])
        template = str(gpu.get("targetTemplate") or "")
        entries.append(
            {
                "devices": [device_index],
                "mig-enabled": True,
                "mig-devices": _profile_counts(_profiles_from_template(template)),
            }
        )
    return entries


def dynamic_config_name(node_name: str, targets: list[dict[str, Any]]) -> str:
    fingerprint = "|".join(
        f"{target.get('deviceIndex')}={target.get('targetTemplate')}"
        for target in sorted(targets, key=lambda item: int(item.get("deviceIndex", 0)))
    )
    digest = hashlib.sha1(f"{node_name}|{fingerprint}".encode("utf-8")).hexdigest()[:10]
    return f"or-sim-{digest}"


def install_static_migrant_configs(cfg: dict[str, Any]) -> list[str]:
    mig_configs = cfg.setdefault("mig-configs", {})
    changed = []
    for name, profiles in MIGRANT_TEMPLATE_CONFIGS.items():
        desired = config_for_profiles(profiles)
        if mig_configs.get(name) != desired:
            mig_configs[name] = desired
            changed.append(name)
    return changed


def ensure_gpu_operator_configs_from_preview(
    client: KubernetesClient,
    mig_manager_target_configs: list[dict[str, Any]],
) -> dict[str, Any]:
    desired = {}
    for target in mig_manager_target_configs:
        config_name = str(target.get("configName") or "")
        gpus = [dict(item) for item in list(target.get("gpus", []))]
        if not config_name or not gpus:
            continue
        desired[config_name] = config_for_gpu_targets(gpus)
    return ensure_gpu_operator_configs(client=client, desired_configs=desired)


def ensure_gpu_operator_configs(
    client: KubernetesClient,
    desired_configs: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    configmap = client.get_configmap(OR_SIM_MIG_CONFIGMAP, GPU_OPERATOR_NAMESPACE)
    data = dict(configmap.get("data", {}))
    raw_config = data.get("config.yaml", "")
    cfg = yaml.safe_load(raw_config) if raw_config else {}
    if not isinstance(cfg, dict):
        cfg = {}
    changed = install_static_migrant_configs(cfg)
    mig_configs = cfg.setdefault("mig-configs", {})
    for name, desired in sorted(desired_configs.items()):
        if mig_configs.get(name) != desired:
            mig_configs[name] = desired
            changed.append(name)
    if changed:
        data["config.yaml"] = yaml.safe_dump(cfg, sort_keys=False)
        manifest = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": OR_SIM_MIG_CONFIGMAP,
                "namespace": GPU_OPERATOR_NAMESPACE,
                "labels": dict(configmap.get("metadata", {}).get("labels", {})),
            },
            "data": data,
        }
        client.apply_configmap(manifest)
    return {
        "configMap": OR_SIM_MIG_CONFIGMAP,
        "namespace": GPU_OPERATOR_NAMESPACE,
        "changed": sorted(set(changed)),
        "desiredConfigCount": len(desired_configs),
    }


def _profiles_from_template(template: str) -> list[str]:
    if not template:
        return []
    return [part for part in template.split("+") if part and part != "void"]


def _profile_counts(profiles: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for profile in profiles:
        gpu_operator_profile = PROFILE_TO_GPU_OPERATOR[str(profile)]
        counts[gpu_operator_profile] = counts.get(gpu_operator_profile, 0) + 1
    return counts
