"""Capture immutable evidence from a terminal live-action-plan failure.

This helper performs only GET requests.  It is deliberately separate from the
runner so a failed controller-side plan can be preserved even when its local
orchestrator process disappears.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import urllib.request
from pathlib import Path
from typing import Any


def kubectl(namespace: str, *args: str) -> Any:
    raw = subprocess.check_output(["kubectl", "-n", namespace, *args], text=True)
    return json.loads(raw)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--router-url", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    output = args.output_dir
    snapshots = output / "snapshots"
    plans = output / "plans"
    snapshots.mkdir(exist_ok=True)
    plans.mkdir(exist_ok=True)
    plan = kubectl(args.namespace, "get", "migactionplan", args.plan, "-o", "json")
    registry = kubectl(args.namespace, "get", "physicalgpuregistries", "default", "-o", "json")
    pods = kubectl(args.namespace, "get", "pods", "-o", "json")
    events = kubectl(args.namespace, "get", "events", "-o", "json")
    with urllib.request.urlopen(args.router_url.rstrip("/") + "/routes", timeout=20) as response:
        routes = json.loads(response.read().decode("utf-8"))
    for path, value in {
        plans / "r01_terminal_plan.json": plan,
        snapshots / "r01_failure_terminal.json": {
            "plan": plan, "registry": registry, "routes": routes, "pods": pods, "events": events,
        },
    }.items():
        path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    statuses = plan.get("status", {}).get("actionStatuses", [])
    failed = [item for item in statuses if str(item.get("status", "")).lower() not in {"completed", "executed", "succeeded", "success"}]
    summary = {
        "plan": args.plan,
        "phase": plan.get("status", {}).get("phase"),
        "message": plan.get("status", {}).get("message"),
        "executorTimestamps": plan.get("status", {}).get("transitionExecution", {}).get("timestamps", {}),
        "actions": {"completed": len(statuses) - len(failed), "failed": len(failed), "total": len(plan.get("spec", {}).get("actionDag", {}).get("nodes", []))},
        "failedActions": failed,
        "routesObserved": len(routes.get("routes", [])),
        "registryHealth": registry.get("status", {}).get("health", {}),
    }
    (output / "r01_failure_capture_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
