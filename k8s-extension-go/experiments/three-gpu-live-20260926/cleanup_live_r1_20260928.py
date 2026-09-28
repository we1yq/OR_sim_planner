"""Generate and execute one audited zero-demand cleanup plan for a live run."""
from __future__ import annotations
import argparse, importlib.util, json, sys, time
from pathlib import Path

ROOT = Path(__file__).parent
spec = importlib.util.spec_from_file_location("live_runner_cleanup", ROOT / "live_runner_20260926.py")
assert spec and spec.loader
runner = importlib.util.module_from_spec(spec); sys.modules[spec.name] = runner; spec.loader.exec_module(runner)

def main() -> int:
    p = argparse.ArgumentParser(); p.add_argument("--namespace", default="or-sim-exp"); p.add_argument("--router-url", required=True); p.add_argument("--name", required=True); p.add_argument("--timeout", type=float, default=900)
    a = p.parse_args(); kube = runner.Kubectl(a.namespace); router = runner.Router(a.router_url)
    zero = {key: 0.0 for key in runner.WORKLOAD_KEYS}
    body = runner.build_arrival_snapshot(a.name, 0, zero, zero, namespace=a.namespace)
    kube.apply(body); plan_name = "plan-" + a.name
    plan = runner.wait_for_plan(kube, plan_name, timeout=a.timeout, poll_seconds=2)
    audit = runner.audit_plan(plan, kube.get_json("physicalgpuregistries", "default"), source_gpu_count=runner.active_gpu_count(kube.get_json("physicalgpuregistries", "default")))
    if not audit["ok"]: raise RuntimeError("cleanup audit failed: " + "; ".join(audit["errors"]))
    kube.approve(plan_name)
    terminal = runner.wait_for_plan(kube, plan_name, timeout=a.timeout, poll_seconds=2, accept_planned=False)
    if terminal.get("status", {}).get("phase") not in runner.TERMINAL_PHASES: raise RuntimeError("cleanup not successful: " + str(terminal.get("status", {})))
    registry = kube.get_json("physicalgpuregistries", "default"); routes = router.get_json("/routes"); pods = kube.get_json("pods")
    health = registry.get("status", {}).get("health", {}); q = registry.get("status", {}).get("queueCounts", {})
    runtimes = [x for x in pods.get("items", []) if x.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/name") == "migrant-model-runtime"]
    result = {"plan": terminal, "audit": audit, "registry": registry, "routes": routes, "runtimePodCount": len(runtimes)}
    print(json.dumps(result, sort_keys=True))
    if health.get("stable") is not True or q.get("available") != 3 or q.get("active") != 0 or routes.get("routes") or runtimes: raise RuntimeError("cleanup verification failed")
    return 0
if __name__ == "__main__": raise SystemExit(main())
