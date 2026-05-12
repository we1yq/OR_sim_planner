from __future__ import annotations

import json
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from k8s_api import KubernetesClient, PythonKubernetesClient


class RouterDrainApplyError(RuntimeError):
    pass


def apply_router_drain_from_action_plan(
    name: str,
    namespace: str = "or-sim",
    confirm_real_router_apply: bool = False,
    allow_preview_instructions: bool = False,
    router_endpoint: str | None = None,
    mode: str = "http",
    timeout_s: float = 120.0,
    poll_interval_s: float = 1.0,
    client: KubernetesClient | None = None,
) -> dict[str, Any]:
    if not confirm_real_router_apply:
        raise RouterDrainApplyError(
            "Refusing to modify router/drain state without confirm_real_router_apply=True."
        )
    if mode not in {"http", "annotation", "no-traffic"}:
        raise RouterDrainApplyError(f"unsupported router drain mode {mode!r}")

    client = client or PythonKubernetesClient()
    action_plan = client.get_migactionplan(name=name, namespace=namespace)
    spec = dict(action_plan.get("spec", {}))
    if bool(spec.get("dryRun", True)) and not allow_preview_instructions:
        raise RouterDrainApplyError(
            "MigActionPlan is marked dryRun=true. Pass allow_preview_instructions=True "
            "only for controlled validation."
        )

    preview = dict(spec.get("trafficAndDrainPreview", {}))
    actions = [dict(row) for row in list(preview.get("trafficActions", []))]
    if not actions:
        raise RouterDrainApplyError("spec.trafficAndDrainPreview.trafficActions is empty.")
    if mode == "http" and not router_endpoint:
        router_endpoint = str(dict(spec.get("executorPreview", {})).get("routerEndpoint") or "")
    if mode == "http" and not router_endpoint:
        raise RouterDrainApplyError("router_endpoint is required for mode=http.")

    started = time.monotonic()
    stop_results = []
    reroute_results = []
    drain_results = []
    verification_results = []

    for action in actions:
        action_type = str(action.get("type") or "")
        if action_type == "stop_accepting_new":
            result = _stop_accepting_new(
                client=client,
                namespace=namespace,
                action=action,
                mode=mode,
            )
            _record_workload_route_plan(
                client=client,
                namespace=namespace,
                action_plan_name=name,
                action_plan_generation=int(action_plan.get("metadata", {}).get("generation", 0)),
                action=action,
                result=result,
                route_action="StopAcceptingNew",
            )
            stop_results.append(result)
        elif action_type == "reroute_queued_tasks":
            result = _reroute_queued_tasks(
                action=action,
                mode=mode,
                router_endpoint=str(router_endpoint or ""),
            )
            _record_workload_route_plan(
                client=client,
                namespace=namespace,
                action_plan_name=name,
                action_plan_generation=int(action_plan.get("metadata", {}).get("generation", 0)),
                action=action,
                result=result,
                route_action="RerouteQueuedTasks",
            )
            reroute_results.append(result)
        elif action_type == "mark_draining_instance":
            result = _mark_draining_instance(
                client=client,
                namespace=namespace,
                action=action,
                mode=mode,
                timeout_s=timeout_s,
                poll_interval_s=poll_interval_s,
            )
            _record_serving_instance_drain(
                client=client,
                namespace=namespace,
                action_plan_name=name,
                action_plan_generation=int(action_plan.get("metadata", {}).get("generation", 0)),
                action=action,
                result=result,
            )
            drain_results.append(result)

    if mode == "http" and router_endpoint:
        for action in actions:
            if action.get("type") == "reroute_queued_tasks":
                workload = str(action.get("workload") or "")
                if workload:
                    verification_results.append(
                        _verify_router_target(
                            router_endpoint=str(router_endpoint),
                            workload=workload,
                        )
                    )

    summary = {
        "kind": "RouterDrainApplySummary",
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "actionPlan": name,
        "namespace": namespace,
        "mode": mode,
        "routerEndpoint": router_endpoint,
        "stopAcceptingNew": stop_results,
        "reroutes": reroute_results,
        "drains": drain_results,
        "verifications": verification_results,
        "timingsSeconds": {"total": round(time.monotonic() - started, 3)},
        "success": all(item.get("success", False) for item in stop_results + reroute_results + drain_results + verification_results),
    }
    status = {
        "phase": "SucceededRealRouterDrain" if summary["success"] else "FailedRealRouterDrain",
        "approved": bool(action_plan.get("status", {}).get("approved", False)),
        "executed": bool(summary["success"]),
        "observedGeneration": int(action_plan.get("metadata", {}).get("generation", 0)),
        "validated": {"realRouterDrain": summary},
        "message": "Router/drain adapter executed traffic and drain actions.",
    }
    try:
        client.patch_migactionplan_status(name=name, namespace=namespace, status=status)
    except Exception as exc:
        summary["statusPatchError"] = str(exc)
    return summary


def _stop_accepting_new(
    client: KubernetesClient,
    namespace: str,
    action: dict[str, Any],
    mode: str,
) -> dict[str, Any]:
    source_pod = str(action.get("sourcePod") or action.get("podName") or "")
    source_endpoint = str(action.get("sourceEndpoint") or "")
    response = {}
    if mode == "http" and source_endpoint:
        response = _http_json(f"{source_endpoint.rstrip('/')}/drain")
    if source_pod:
        _patch_pod_annotations(
            client=client,
            namespace=namespace,
            pod_name=source_pod,
            annotations={
                "mig.or-sim.io/accepting-new": "false",
                "mig.or-sim.io/draining": "true",
            },
        )
    return {
        "workload": action.get("workload"),
        "sourcePod": source_pod or None,
        "sourceEndpoint": source_endpoint or None,
        "mode": mode,
        "response": response,
        "success": True,
    }


def _reroute_queued_tasks(
    action: dict[str, Any],
    mode: str,
    router_endpoint: str,
) -> dict[str, Any]:
    workload = str(action.get("workload") or "")
    target_endpoint = str(action.get("targetEndpoint") or action.get("toEndpoint") or "")
    response = {}
    if mode == "http":
        if not workload or not target_endpoint:
            raise RouterDrainApplyError(
                "mode=http reroute requires workload and targetEndpoint."
            )
        response = _http_json(
            f"{router_endpoint.rstrip('/')}/reroute?"
            + urlencode({"workload": workload, "target": target_endpoint})
        )
    return {
        "workload": workload,
        "targetEndpoint": target_endpoint or None,
        "mode": mode,
        "response": response,
        "success": True,
    }


def _mark_draining_instance(
    client: KubernetesClient,
    namespace: str,
    action: dict[str, Any],
    mode: str,
    timeout_s: float,
    poll_interval_s: float,
) -> dict[str, Any]:
    source_pod = str(action.get("sourcePod") or action.get("podName") or "")
    source_endpoint = str(action.get("sourceEndpoint") or "")
    if source_pod:
        _patch_pod_annotations(
            client=client,
            namespace=namespace,
            pod_name=source_pod,
            annotations={
                "mig.or-sim.io/drain-state": "waiting-inflight-zero",
            },
        )

    started = time.monotonic()
    last_metrics: dict[str, Any] = {}
    while time.monotonic() - started <= timeout_s:
        if mode == "http" and source_endpoint:
            last_metrics = _http_json(f"{source_endpoint.rstrip('/')}/metrics")
            inflight = int(last_metrics.get("inflight", 0))
            queued = int(last_metrics.get("queued", 0))
        elif mode == "annotation" and source_pod:
            pod = client.get_pod(name=source_pod, namespace=namespace)
            annotations = dict(dict(pod.get("metadata", {})).get("annotations", {}))
            inflight = int(annotations.get("mig.or-sim.io/inflight", "0"))
            queued = int(annotations.get("mig.or-sim.io/queued", "0"))
            last_metrics = {"inflight": inflight, "queued": queued}
        else:
            inflight = 0
            queued = 0
            last_metrics = {"inflight": 0, "queued": 0}

        if inflight == 0 and queued == 0:
            if source_pod:
                _patch_pod_annotations(
                    client=client,
                    namespace=namespace,
                    pod_name=source_pod,
                    annotations={
                        "mig.or-sim.io/drain-state": "drained",
                        "mig.or-sim.io/inflight": "0",
                        "mig.or-sim.io/queued": "0",
                    },
                )
            return {
                "workload": action.get("workload"),
                "sourcePod": source_pod or None,
                "sourceEndpoint": source_endpoint or None,
                "mode": mode,
                "metrics": last_metrics,
                "waitSeconds": round(time.monotonic() - started, 3),
                "success": True,
            }
        time.sleep(poll_interval_s)
    return {
        "workload": action.get("workload"),
        "sourcePod": source_pod or None,
        "sourceEndpoint": source_endpoint or None,
        "mode": mode,
        "metrics": last_metrics,
        "waitSeconds": round(time.monotonic() - started, 3),
        "success": False,
    }


def _verify_router_target(router_endpoint: str, workload: str) -> dict[str, Any]:
    response = _http_json(
        f"{router_endpoint.rstrip('/')}/route?"
        + urlencode({"workload": workload, "ms": "50"})
    )
    return {
        "workload": workload,
        "response": response,
        "success": bool(response.get("ok", False)),
    }


def _record_workload_route_plan(
    client: KubernetesClient,
    namespace: str,
    action_plan_name: str,
    action_plan_generation: int,
    action: dict[str, Any],
    result: dict[str, Any],
    route_action: str,
) -> None:
    name = _child_name(action_plan_name, route_action, action)
    manifest = {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "WorkloadRoutePlan",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "or-sim-mig-planner",
                "mig.or-sim.io/owner-action-plan": action_plan_name,
                "mig.or-sim.io/real-router-drain": "true",
            },
        },
        "spec": {
            "previewOnly": False,
            "action": route_action,
            "workload": action.get("workload"),
            "sourceInstanceRef": _instance_ref(action),
            "queued": int(action.get("queued", 0) or 0),
            "target": action.get("targetEndpoint") or action.get("toEndpoint") or action.get("to"),
        },
    }
    client.apply_workloadrouteplan(manifest)
    client.patch_workloadrouteplan_status(
        name=name,
        namespace=namespace,
        status={
            "phase": "SucceededRealRouterDrain" if result.get("success") else "FailedRealRouterDrain",
            "previewOnly": False,
            "ownerActionPlan": action_plan_name,
            "validatedBy": "router-drain-executor",
            "observedGeneration": action_plan_generation,
            "message": f"{route_action} executed in {result.get('mode')} mode.",
            "result": result,
        },
    )


def _record_serving_instance_drain(
    client: KubernetesClient,
    namespace: str,
    action_plan_name: str,
    action_plan_generation: int,
    action: dict[str, Any],
    result: dict[str, Any],
) -> None:
    name = _child_name(action_plan_name, "ServingInstanceDrain", action)
    metrics = dict(result.get("metrics", {}))
    manifest = {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "ServingInstanceDrain",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "or-sim-mig-planner",
                "mig.or-sim.io/owner-action-plan": action_plan_name,
                "mig.or-sim.io/real-router-drain": "true",
            },
        },
        "spec": {
            "previewOnly": False,
            "workload": action.get("workload"),
            "sourceInstanceRef": _instance_ref(action),
            "targetInflight": 0,
            "currentInflightApprox": int(metrics.get("inflight", 0) or 0),
            "waitForInflightZero": True,
        },
    }
    client.apply_servinginstancedrain(manifest)
    client.patch_servinginstancedrain_status(
        name=name,
        namespace=namespace,
        status={
            "phase": "SucceededRealRouterDrain" if result.get("success") else "FailedRealRouterDrain",
            "previewOnly": False,
            "ownerActionPlan": action_plan_name,
            "validatedBy": "router-drain-executor",
            "observedGeneration": action_plan_generation,
            "message": f"Drain completed in {result.get('waitSeconds')}s.",
            "result": result,
        },
    )


def _instance_ref(action: dict[str, Any]) -> dict[str, Any]:
    return {
        "gpuId": action.get("gpu_id"),
        "physicalGpuId": action.get("physical_gpu_id"),
        "slot": action.get("slot"),
        "podName": action.get("sourcePod") or action.get("podName"),
        "endpoint": action.get("sourceEndpoint"),
    }


def _child_name(action_plan_name: str, action_name: str, action: dict[str, Any]) -> str:
    workload = str(action.get("workload") or "workload")
    source = str(action.get("sourcePod") or action.get("podName") or "source")
    raw = f"{action_plan_name}-{action_name}-{workload}-{source}"
    cleaned = "".join(ch if ch.isalnum() or ch == "-" else "-" for ch in raw.lower())
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned.strip("-")[:63].rstrip("-")


def _patch_pod_annotations(
    client: KubernetesClient,
    namespace: str,
    pod_name: str,
    annotations: dict[str, str],
) -> None:
    if not hasattr(client, "patch_pod_annotations"):
        raise RouterDrainApplyError("Kubernetes client does not support patch_pod_annotations.")
    client.patch_pod_annotations(name=pod_name, namespace=namespace, annotations=annotations)


def _http_json(url: str) -> dict[str, Any]:
    try:
        with urlopen(url, timeout=10.0) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RouterDrainApplyError(f"HTTP {exc.code} from {url}: {raw}") from exc
    except (URLError, TimeoutError) as exc:
        raise RouterDrainApplyError(f"HTTP request failed for {url}: {exc}") from exc
