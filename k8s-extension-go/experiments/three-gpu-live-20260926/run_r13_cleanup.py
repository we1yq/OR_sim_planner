#!/usr/bin/env python3
"""Run the explicit R13 zero-demand cleanup after the 12-round experiment."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping

import live_runner_20260926 as runner


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--namespace", default=runner.DEFAULT_NAMESPACE)
    parser.add_argument("--router-url", default=runner.DEFAULT_ROUTER_URL)
    parser.add_argument("--placement-node", dest="placement_nodes", action="append", default=[])
    parser.add_argument("--watchdog-seconds", type=float, default=1800.0)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    args = parser.parse_args()

    run_dir = args.run_dir.resolve()
    run_id = run_dir.name
    if not run_dir.is_dir() or not (run_dir / "results.json").is_file():
        raise SystemExit(f"not a completed experiment directory: {run_dir}")
    result = json.loads((run_dir / "results.json").read_text(encoding="utf-8"))
    if result.get("ok") is not True or int(result.get("completed_rounds", 0)) != runner.ROUND_COUNT:
        raise SystemExit("R13 cleanup requires a successful 12-round run")

    kube = runner.Kubectl(args.namespace)
    router = runner.Router(args.router_url)
    before = runner.preflight(
        kube,
        router,
        controllers=("planner-controller", "transition-executor", "cluster-state-manager", "runtime-router"),
        require_empty=False,
    )
    if not before["ok"]:
        raise SystemExit("R13 preflight failed: " + "; ".join(before["errors"]))

    demand, catalog, _ = runner.load_frozen_inputs()
    source_rates = runner.demand_rates(demand[-1])
    target_rates = {key: 0.0 for key in runner.WORKLOAD_KEYS}
    snapshot_name = runner.snapshot_name_for_run(run_id, 13)
    snapshot = runner.build_arrival_snapshot(
        snapshot_name,
        13,
        source_rates,
        target_rates,
        namespace=args.namespace,
        placement_nodes=args.placement_nodes,
    )
    spec = _mapping(snapshot.get("spec"))
    spec["triggerReason"] = "explicit-r13-cleanup"
    spec["notes"] = [
        "R13 is an explicit zero-demand cleanup after the measured R1-R12 transitions",
        "R13 makespan is reported separately and is excluded from the 12-round aggregate",
    ]

    runner._json_output(run_dir / "snapshots" / "r13_cleanup_before.json", before)
    runner._json_output(run_dir / "snapshots" / "r13_cleanup_arrival_snapshot.json", snapshot)
    kube.apply(snapshot)
    plan_name = "plan-" + snapshot_name
    plan = runner.wait_for_plan(kube, plan_name, timeout=args.watchdog_seconds, poll_seconds=args.poll_seconds)
    registry = kube.get_json("physicalgpuregistries", "default")
    audit = runner.audit_plan(
        plan,
        registry,
        source_gpu_count=runner.active_gpu_count(registry),
        require_nonzero_target=False,
    )
    runner._json_output(run_dir / "plans" / "r13_cleanup_plan.json", plan)
    runner._json_output(run_dir / "plans" / "r13_cleanup_audit.json", audit)
    if not audit["ok"]:
        raise SystemExit("R13 plan audit failed: " + "; ".join(audit["errors"]))

    kube.approve(plan_name)
    terminal = runner.wait_for_plan(
        kube,
        plan_name,
        timeout=args.watchdog_seconds,
        poll_seconds=args.poll_seconds,
        accept_planned=False,
    )
    runner._json_output(run_dir / "plans" / "r13_cleanup_terminal_plan.json", terminal)
    if str(_mapping(terminal.get("status")).get("phase")) not in runner.TERMINAL_PHASES:
        raise SystemExit("R13 cleanup plan did not succeed")

    validation, final_registry, final_routes = runner.wait_independent_final_validation(
        kube,
        router,
        terminal,
        timeout=60.0,
        poll_seconds=args.poll_seconds,
    )
    all_pods = kube.get_json("pods")
    runtime_pods = {
        **all_pods,
        "items": [
            pod for pod in _items(all_pods.get("items"))
            if _mapping(_mapping(pod).get("metadata")).get("labels", {}).get("app.kubernetes.io/name")
            == "migrant-model-runtime"
        ],
    }
    queue_counts = _mapping(_mapping(final_registry.get("status")).get("queueCounts"))
    cleanup_errors = list(validation.get("errors", []))
    if int(queue_counts.get("active", -1)) != 0:
        cleanup_errors.append(f"registry active={queue_counts.get('active')}, want 0")
    if int(queue_counts.get("transitioning", -1)) != 0:
        cleanup_errors.append(f"registry transitioning={queue_counts.get('transitioning')}, want 0")
    if len(_items(_mapping(final_routes).get("routes"))) != 0:
        cleanup_errors.append("router still has routes")
    if len(_items(runtime_pods.get("items"))) != 0:
        cleanup_errors.append("runtime pods still exist")

    action_statuses = _items(_mapping(terminal.get("status")).get("actionStatuses"))
    transition = _mapping(_mapping(terminal.get("status")).get("transitionExecution"))
    summary = {
        "runId": run_id,
        "round": 13,
        "purpose": "explicit-zero-demand-cleanup",
        "includedInMeasuredTwelveRoundAggregate": False,
        "ok": validation.get("ok") is True and not cleanup_errors,
        "errors": cleanup_errors,
        "planName": plan_name,
        "planPhase": _mapping(terminal.get("status")).get("phase"),
        "makespanSeconds": _mapping(transition.get("durations")).get("makespanSeconds"),
        "actionCount": len(action_statuses),
        "actionCounts": dict(__import__("collections").Counter(str(item.get("type")) for item in action_statuses)),
        "allActionsCompleted": all(str(item.get("status")) == "completed" for item in action_statuses),
        "finalValidation": validation,
        "queueCounts": dict(queue_counts),
        "routeCount": len(_items(_mapping(final_routes).get("routes"))),
        "runtimePodCount": len(_items(runtime_pods.get("items"))),
    }
    runner._json_output(run_dir / "snapshots" / "r13_cleanup_after.json", {
        "registry": final_registry,
        "routes": final_routes,
        "runtimePods": runtime_pods,
        "finalValidation": validation,
    })
    runner._json_output(run_dir / "r13_cleanup_summary.json", summary)
    print(json.dumps(summary, sort_keys=True))
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
