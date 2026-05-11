from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from typing import Any

import yaml


TEMPLATE_TO_CONFIG = {
    "7": ("all-7g.40gb", {"nvidia.com/mig-7g.40gb": 1}, "7g"),
    "4+3": ("or-sim-4-3", {}, None),
    "4+2+1": ("or-sim-4-2-1", {}, None),
    "4+1+1+1": ("or-sim-4-1-1-1", {}, None),
    "3+3": ("all-3g.20gb", {"nvidia.com/mig-3g.20gb": 2}, "3g.20gb"),
    "3+2+1": ("or-sim-3-2-1", {}, None),
    "3+1+1+1": ("or-sim-3-1-1-1", {}, None),
    "2+2+3": ("or-sim-2-2-3", {}, None),
    "3+2+1+1": ("or-sim-3-2-1-1", {}, None),
    "3+1+1+1+1": ("or-sim-3-1-1-1-1", {}, None),
    "2+2+2+1": ("or-sim-2-2-2-1", {}, None),
    "2+2+1+1+1": ("or-sim-2-2-1-1-1", {}, None),
    "2+1+1+1+1+1": ("or-sim-2-1-1-1-1-1", {}, None),
    "1+1+1+1+1+1+1": ("all-1g.5gb", {"nvidia.com/mig-1g.5gb": 7}, "1g.5gb"),
}

PROFILE_RESOURCE = {
    "1g": "nvidia.com/mig-1g.5gb",
    "2g": "nvidia.com/mig-2g.10gb",
    "3g": "nvidia.com/mig-3g.20gb",
    "4g": "nvidia.com/mig-4g.20gb",
    "7g": "nvidia.com/mig-7g.40gb",
}


@dataclass
class K8s:
    core: Any
    api_client: Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark real MIG reconfiguration latency")
    parser.add_argument("--node", default="rtx1")
    parser.add_argument("--namespace", default="or-sim")
    parser.add_argument("--timeout-s", type=float, default=900.0)
    parser.add_argument("--poll-s", type=float, default=2.0)
    parser.add_argument("--run-rollback", action="store_true")
    parser.add_argument("--run-empty", action="store_true")
    parser.add_argument("--run-templates", action="store_true")
    parser.add_argument("--restore-config", default="all-2g.10gb")
    parser.add_argument("--output-jsonl", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    from kubernetes import client, config

    config.load_kube_config()
    k8s = K8s(core=client.CoreV1Api(), api_client=client.ApiClient())
    results = []

    if args.run_rollback:
        results.append(run_transition(k8s, args, "rollback-to-2g", "all-2g.10gb"))
    if args.run_empty:
        results.append(run_transition(k8s, args, "to-empty-mig-enabled", "all-enabled"))
    if args.run_templates:
        ensure_current_config(k8s, args, "all-enabled", results)
        for template, (config_name, _, _) in TEMPLATE_TO_CONFIG.items():
            results.append(run_transition(k8s, args, f"empty-to-{template}", config_name, template=template))
            results.append(run_transition(k8s, args, f"{template}-to-empty", "all-enabled", template=template))

    if args.restore_config:
        results.append(run_transition(k8s, args, "restore", args.restore_config))

    if args.output_jsonl:
        with open(args.output_jsonl, "w", encoding="utf-8") as f:
            for row in results:
                f.write(json.dumps(row, sort_keys=True) + "\n")

    print(yaml.safe_dump({"results": results}, sort_keys=False), end="")
    return 0


def ensure_current_config(k8s: K8s, args: argparse.Namespace, config_name: str, results: list[dict[str, Any]]) -> None:
    summary = node_summary(k8s, args.node)
    if summary.get("migConfig") != config_name or summary.get("migConfigState") != "success":
        results.append(run_transition(k8s, args, f"ensure-{config_name}", config_name))


def run_transition(
    k8s: K8s,
    args: argparse.Namespace,
    label: str,
    target_config: str,
    template: str | None = None,
) -> dict[str, Any]:
    before = node_summary(k8s, args.node)
    t0 = time.monotonic()
    wall_start = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    k8s.core.patch_node(
        name=args.node,
        body={"metadata": {"labels": {"nvidia.com/mig.config": target_config, "nvidia.com/mig.config.state": None}}},
    )

    pending_s = None
    success_s = None
    capacity_s = None
    allocatable_s = None
    last = {}
    expected = expected_resources_for_config(target_config, template)
    while time.monotonic() - t0 < args.timeout_s:
        last = node_summary(k8s, args.node)
        elapsed = time.monotonic() - t0
        state = last.get("migConfigState")
        if state == "pending" and pending_s is None:
            pending_s = elapsed
        if state == "success" and success_s is None:
            success_s = elapsed
        if expected_capacity_present(last.get("migCapacity", {}), expected) and capacity_s is None:
            capacity_s = elapsed
        if expected_resources_present(last.get("migAllocatable", {}), expected) and allocatable_s is None:
            allocatable_s = elapsed
        if success_s is not None and capacity_s is not None and allocatable_s is not None:
            break
        time.sleep(args.poll_s)

    timed_out = success_s is None or capacity_s is None or allocatable_s is None
    return {
        "label": label,
        "template": template,
        "targetConfig": target_config,
        "wallStart": wall_start,
        "before": before,
        "after": last,
        "pendingSeconds": _round(pending_s),
        "successSeconds": _round(success_s),
        "capacitySeconds": _round(capacity_s),
        "allocatableSeconds": _round(allocatable_s),
        "timedOut": timed_out,
    }


def expected_resources_for_config(target_config: str, template: str | None) -> dict[str, int]:
    if target_config == "all-enabled":
        return {}
    if target_config == "all-2g.10gb":
        return {"nvidia.com/mig-2g.10gb": 3}
    if template:
        parts = [f"{part}g" if not part.endswith("g") else part for part in template.split("+") if part]
        expected: dict[str, int] = {}
        for part in parts:
            resource = PROFILE_RESOURCE[part]
            expected[resource] = expected.get(resource, 0) + 1
        return expected
    for _, (config_name, expected, _) in TEMPLATE_TO_CONFIG.items():
        if config_name == target_config:
            return dict(expected)
    return {}


def expected_resources_present(observed: dict[str, str], expected: dict[str, int]) -> bool:
    if not expected:
        return all(int(str(value)) == 0 for key, value in observed.items() if key.startswith("nvidia.com/mig-"))
    return all(int(str(observed.get(key, 0))) == value for key, value in expected.items())


def expected_capacity_present(observed: dict[str, str], expected: dict[str, int]) -> bool:
    if not expected:
        # Device-plugin capacity can retain historical MIG resource keys after an
        # empty MIG config. For "MIG mode enabled, no instances", success plus
        # zero allocatable is the meaningful readiness signal.
        return True
    return expected_resources_present(observed, expected)


def node_summary(k8s: K8s, node_name: str) -> dict[str, Any]:
    node = k8s.api_client.sanitize_for_serialization(k8s.core.read_node(name=node_name))
    metadata = dict(node.get("metadata", {}))
    labels = dict(metadata.get("labels", {}))
    status = dict(node.get("status", {}))
    capacity = dict(status.get("capacity", {}))
    allocatable = dict(status.get("allocatable", {}))
    return {
        "migConfig": labels.get("nvidia.com/mig.config"),
        "migConfigState": labels.get("nvidia.com/mig.config.state"),
        "profileCounts": {
            key: value
            for key, value in sorted(labels.items())
            if key.startswith("nvidia.com/mig-") and key.endswith(".count")
        },
        "migCapacity": {
            key: value
            for key, value in sorted(capacity.items())
            if key.startswith("nvidia.com/mig-")
        },
        "migAllocatable": {
            key: value
            for key, value in sorted(allocatable.items())
            if key.startswith("nvidia.com/mig-")
        },
    }


def _round(value: float | None) -> float | None:
    return None if value is None else round(value, 3)


if __name__ == "__main__":
    raise SystemExit(main())
