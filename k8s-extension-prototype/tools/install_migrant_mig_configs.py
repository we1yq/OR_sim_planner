from __future__ import annotations

import argparse
from typing import Any

import yaml


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Install MIGRANT MIG Manager configs")
    parser.add_argument("--namespace", default="gpu-operator")
    parser.add_argument("--source-configmap", default="default-mig-parted-config")
    parser.add_argument("--target-configmap", default="or-sim-mig-parted-config")
    parser.add_argument("--patch-clusterpolicy", action="store_true")
    parser.add_argument("--clusterpolicy", default="cluster-policy")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    from kubernetes import client, config

    config.load_kube_config()
    core = client.CoreV1Api()
    custom = client.CustomObjectsApi()
    api_client = client.ApiClient()
    obj = core.read_namespaced_config_map(name=args.source_configmap, namespace=args.namespace)
    manifest = api_client.sanitize_for_serialization(obj)
    data = dict(manifest.get("data", {}))
    raw_config = data.get("config.yaml")
    if not raw_config:
        raise SystemExit(f"{args.namespace}/{args.source_configmap} has no data.config.yaml")
    cfg = yaml.safe_load(raw_config)
    mig_configs = cfg.setdefault("mig-configs", {})
    changed = []
    for name, profiles in MIGRANT_TEMPLATE_CONFIGS.items():
        desired = _config_for_profiles(profiles)
        if mig_configs.get(name) != desired:
            mig_configs[name] = desired
            changed.append(name)
    data["config.yaml"] = yaml.safe_dump(cfg, sort_keys=False)
    target_manifest = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": args.target_configmap, "namespace": args.namespace},
        "data": data,
    }
    if not args.dry_run:
        try:
            core.read_namespaced_config_map(name=args.target_configmap, namespace=args.namespace)
            core.patch_namespaced_config_map(
                name=args.target_configmap,
                namespace=args.namespace,
                body={"data": data},
            )
        except client.ApiException as exc:
            if exc.status != 404:
                raise
            core.create_namespaced_config_map(namespace=args.namespace, body=target_manifest)
        if args.patch_clusterpolicy:
            custom.patch_cluster_custom_object(
                group="nvidia.com",
                version="v1",
                plural="clusterpolicies",
                name=args.clusterpolicy,
                body={"spec": {"migManager": {"config": {"name": args.target_configmap}}}},
            )
    print(
        yaml.safe_dump(
            {
                "sourceConfigMap": args.source_configmap,
                "targetConfigMap": args.target_configmap,
                "patchedClusterPolicy": bool(args.patch_clusterpolicy and not args.dry_run),
                "changed": changed,
                "count": len(changed),
            },
            sort_keys=False,
        ),
        end="",
    )
    return 0


def _config_for_profiles(profiles: list[str]) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for profile in profiles:
        gpu_operator_profile = PROFILE_TO_GPU_OPERATOR[profile]
        counts[gpu_operator_profile] = counts.get(gpu_operator_profile, 0) + 1
    return [
        {
            "devices": "all",
            "mig-enabled": True,
            "mig-devices": counts,
        }
    ]


if __name__ == "__main__":
    raise SystemExit(main())
