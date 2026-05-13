from __future__ import annotations

import argparse
import time
from typing import Any

import yaml
from kubernetes import client, config, watch


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run an MIGRANT workload placement and live batch-size smoke test."
    )
    parser.add_argument("--namespace", default="or-sim")
    parser.add_argument("--pod-name", default="or-sim-workload-smoke")
    parser.add_argument("--configmap-name", default="or-sim-workload-smoke-config")
    parser.add_argument("--node-name", default="rtx1-worker")
    parser.add_argument("--mig-resource", default="nvidia.com/mig-3g.20gb")
    parser.add_argument("--image", default="nvidia/cuda:12.4.1-base-ubuntu22.04")
    parser.add_argument("--initial-batch-size", default="4")
    parser.add_argument("--updated-batch-size", default="8")
    parser.add_argument("--timeout-s", type=float, default=240.0)
    parser.add_argument("--poll-s", type=float, default=1.0)
    parser.add_argument("--cleanup", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config.load_kube_config()
    core = client.CoreV1Api()

    start = time.monotonic()
    cleanup_existing(core, args.namespace, args.pod_name, args.configmap_name)

    create_configmap(core, args.namespace, args.configmap_name, args.initial_batch_size)
    create_pod(core, args)
    pod_ready_s = wait_for_pod_ready(core, args.namespace, args.pod_name, args.timeout_s)
    first_batch_s = wait_for_log_text(
        core,
        args.namespace,
        args.pod_name,
        f"batch_size={args.initial_batch_size}",
        args.timeout_s,
        args.poll_s,
    )

    before_uid = pod_uid(core, args.namespace, args.pod_name)
    patch_configmap(core, args.namespace, args.configmap_name, args.updated_batch_size)
    batch_update_s = wait_for_log_text(
        core,
        args.namespace,
        args.pod_name,
        f"batch_size={args.updated_batch_size}",
        args.timeout_s,
        args.poll_s,
    )
    after_uid = pod_uid(core, args.namespace, args.pod_name)
    logs = tail_logs(core, args.namespace, args.pod_name, lines=30)
    pod = core.read_namespaced_pod(args.pod_name, args.namespace)
    pod_dict = client.ApiClient().sanitize_for_serialization(pod)

    result = {
        "kind": "WorkloadPartitionSmokeResult",
        "namespace": args.namespace,
        "podName": args.pod_name,
        "configMapName": args.configmap_name,
        "nodeName": pod_dict.get("spec", {}).get("nodeName"),
        "requestedMigResource": args.mig_resource,
        "image": args.image,
        "initialBatchSize": args.initial_batch_size,
        "updatedBatchSize": args.updated_batch_size,
        "podUidBeforeUpdate": before_uid,
        "podUidAfterUpdate": after_uid,
        "podRecreatedDuringBatchUpdate": before_uid != after_uid,
        "phase": pod_dict.get("status", {}).get("phase"),
        "timingsSeconds": {
            "podReady": round(pod_ready_s, 3),
            "initialBatchObserved": round(first_batch_s, 3),
            "batchUpdateObservedAfterPatch": round(batch_update_s, 3),
            "total": round(time.monotonic() - start, 3),
        },
        "recentLogs": logs,
        "success": before_uid == after_uid,
    }
    print(yaml.safe_dump(result, sort_keys=False), end="")

    if args.cleanup:
        cleanup_existing(core, args.namespace, args.pod_name, args.configmap_name)
    return 0 if result["success"] else 1


def cleanup_existing(
    core: client.CoreV1Api,
    namespace: str,
    pod_name: str,
    configmap_name: str,
) -> None:
    try:
        core.delete_namespaced_pod(name=pod_name, namespace=namespace, grace_period_seconds=0)
        wait_for_pod_deleted(core, namespace, pod_name, timeout_s=120.0)
    except client.ApiException as exc:
        if exc.status != 404:
            raise
    try:
        core.delete_namespaced_config_map(name=configmap_name, namespace=namespace)
    except client.ApiException as exc:
        if exc.status != 404:
            raise


def create_configmap(
    core: client.CoreV1Api,
    namespace: str,
    name: str,
    batch_size: str,
) -> None:
    core.create_namespaced_config_map(
        namespace=namespace,
        body={
            "metadata": {
                "name": name,
                "labels": {"app.kubernetes.io/name": "or-sim-workload-smoke"},
            },
            "data": {"batch_size": str(batch_size)},
        },
    )


def patch_configmap(
    core: client.CoreV1Api,
    namespace: str,
    name: str,
    batch_size: str,
) -> None:
    core.patch_namespaced_config_map(
        name=name,
        namespace=namespace,
        body={"data": {"batch_size": str(batch_size)}},
    )


def create_pod(core: client.CoreV1Api, args: argparse.Namespace) -> None:
    core.create_namespaced_pod(namespace=args.namespace, body=pod_manifest(args))


def pod_manifest(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": args.pod_name,
            "labels": {
                "app.kubernetes.io/name": "or-sim-workload-smoke",
                "mig.or-sim.io/workload": "smoke",
            },
        },
        "spec": {
            "restartPolicy": "Never",
            "nodeSelector": {"kubernetes.io/hostname": args.node_name},
            "containers": [
                {
                    "name": "worker",
                    "image": args.image,
                    "command": ["/bin/sh", "-c"],
                    "args": [
                        (
                            "set -eu; "
                            "echo 'or-sim workload smoke starting'; "
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
                        {
                            "name": "batch-config",
                            "mountPath": "/etc/or-sim",
                            "readOnly": True,
                        }
                    ],
                    "resources": {
                        "limits": {args.mig_resource: 1},
                    },
                }
            ],
            "volumes": [
                {
                    "name": "batch-config",
                    "configMap": {"name": args.configmap_name},
                }
            ],
        },
    }


def wait_for_pod_ready(
    core: client.CoreV1Api,
    namespace: str,
    pod_name: str,
    timeout_s: float,
) -> float:
    start = time.monotonic()
    watcher = watch.Watch()
    try:
        for event in watcher.stream(
            core.list_namespaced_pod,
            namespace=namespace,
            field_selector=f"metadata.name={pod_name}",
            timeout_seconds=int(timeout_s),
        ):
            pod = event["object"]
            phase = pod.status.phase
            if phase == "Failed":
                raise RuntimeError(f"pod {namespace}/{pod_name} failed")
            for condition in pod.status.conditions or []:
                if condition.type == "Ready" and condition.status == "True":
                    return time.monotonic() - start
            if time.monotonic() - start > timeout_s:
                break
    finally:
        watcher.stop()
    raise TimeoutError(f"timed out waiting for pod {namespace}/{pod_name} Ready")


def wait_for_pod_deleted(
    core: client.CoreV1Api,
    namespace: str,
    pod_name: str,
    timeout_s: float,
) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            core.read_namespaced_pod(pod_name, namespace)
        except client.ApiException as exc:
            if exc.status == 404:
                return
            raise
        time.sleep(1.0)
    raise TimeoutError(f"timed out waiting for pod {namespace}/{pod_name} deletion")


def wait_for_log_text(
    core: client.CoreV1Api,
    namespace: str,
    pod_name: str,
    text: str,
    timeout_s: float,
    poll_s: float,
) -> float:
    start = time.monotonic()
    while time.monotonic() - start <= timeout_s:
        logs = tail_logs(core, namespace, pod_name, lines=80)
        if text in logs:
            return time.monotonic() - start
        time.sleep(poll_s)
    raise TimeoutError(f"timed out waiting for log text {text!r}")


def tail_logs(
    core: client.CoreV1Api,
    namespace: str,
    pod_name: str,
    lines: int,
) -> str:
    return core.read_namespaced_pod_log(
        name=pod_name,
        namespace=namespace,
        container="worker",
        tail_lines=lines,
    )


def pod_uid(core: client.CoreV1Api, namespace: str, pod_name: str) -> str:
    return str(core.read_namespaced_pod(pod_name, namespace).metadata.uid)


if __name__ == "__main__":
    raise SystemExit(main())
