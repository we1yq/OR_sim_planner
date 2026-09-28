#!/usr/bin/env python3
"""Fail-closed, scoped Ampere smoke runner.

Default mode is read-only. --stage creates one manual-gated ArrivalSnapshot,
audits its plan, and stops. --execute creates one manual-gated ArrivalSnapshot
in or-sim-exp, audits the generated MigActionPlan, and only then approves it.
It never creates namespaces/controllers, changes images, or deletes workloads.
"""
from __future__ import annotations

import argparse
import atexit
import datetime as dt
import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any

NAMESPACE = "or-sim-exp"
NODE = "ampere"
CONTROLLERS = ("cluster-state-manager", "planner-controller", "transition-executor", "runtime-router")
BATCH_TYPES = ("patch_batch_config", "apply_batch", "verify_batch", "activate_instance_route")
GEOMETRY_TYPES = {"allocate_gpu", "return_gpu", "configure_full_template", "apply_slots", "configure_partial_profile", "patch_slots", "clear_full_template", "clear_gpu", "clear_template", "clear_gpu_binding", "register_mig_devices"}


class SmokeError(RuntimeError):
    pass


def now_slug() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


class Smoke:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.run_name = args.approve_staged or args.run_name or f"ampere-smoke-{now_slug()}"
        self.out = Path(args.output_dir or f"/tmp/{self.run_name}").resolve()
        self.out.mkdir(parents=True, exist_ok=False)
        self.snapshot = self.run_name
        self.plan = "plan-" + self.snapshot
        self.approved = False
        self.success = False
        atexit.register(self.cleanup_control_crs)

    def kubectl(self, *args: str, input_text: str | None = None) -> str:
        cmd = ["kubectl", *args]
        proc = subprocess.run(cmd, input=input_text, text=True, capture_output=True)
        if proc.returncode:
            raise SmokeError(f"{' '.join(cmd)} failed: {proc.stderr.strip()}")
        return proc.stdout

    def get(self, resource: str, name: str | None = None, *, namespace: str | None = NAMESPACE) -> dict[str, Any]:
        target = resource if name is None else f"{resource}/{name}"
        flags = ["get", target]
        if namespace:
            flags += ["-n", namespace]
        flags += ["-o", "json"]
        return json.loads(self.kubectl(*flags))

    def save(self, name: str, value: Any) -> None:
        (self.out / name).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    def router(self) -> dict[str, Any]:
        try:
            with urllib.request.urlopen(self.args.router_url.rstrip("/") + "/routes", timeout=10) as r:
                return json.loads(r.read().decode() or "{}")
        except Exception as exc:
            raise SmokeError(f"router observation failed: {exc}") from exc

    def cleanup_control_crs(self) -> None:
        # CR deletion cannot undo an approved transition.  It is deliberately
        # limited to this run's control CRs and never guesses at deployments,
        # routes, slots, or GPU geometry.
        if not (self.args.execute or self.args.stage or self.args.approve_staged) or self.success:
            return
        # Once approved, deleting the plan can race the executor and must never
        # be used as an error-path cleanup mechanism.
        if not self.approved:
            for resource, name in (("migactionplan", self.plan), ("arrivalsnapshot", self.snapshot)):
                subprocess.run(["kubectl", "-n", NAMESPACE, "delete", resource, name, "--ignore-not-found=true", "--wait=false"], text=True, capture_output=True)
        (self.out / "CLEANUP_REQUIRED.txt").write_text(
            "Control CRs for this run were deleted after failure. An approved plan may have changed workloads; inspect artifacts and restore only through a separately reviewed rollback plan.\n",
            encoding="utf-8",
        )

    def preflight(self) -> None:
        if self.args.namespace != NAMESPACE or self.args.node != NODE:
            raise SmokeError("this harness is hard-scoped to --namespace or-sim-exp and --node ampere")
        if not self.args.router_url:
            raise SmokeError("--router-url is required; do not infer or port-forward a router")
        self.save("namespace.json", self.get("namespace", NAMESPACE, namespace=None))
        self.save("node-ampere.json", self.get("node", NODE, namespace=None))
        deployments = self.get("deployments")
        self.save("deployments-before.json", deployments)
        by_name = {x.get("metadata", {}).get("name"): x for x in deployments.get("items", [])}
        for name in CONTROLLERS:
            item = by_name.get(name)
            if not item:
                raise SmokeError(f"required isolated controller deployment is absent: {name}")
            spec = item.get("spec", {})
            status = item.get("status", {})
            if int(status.get("availableReplicas", 0)) < int(spec.get("replicas", 1)):
                raise SmokeError(f"controller is not Ready: {name}")
        registry = self.get("physicalgpuregistries", "default")
        self.save("registry-before.json", registry)
        health = registry.get("status", {}).get("health", {})
        if health.get("stable") is not True or health.get("repairRequired") is True:
            raise SmokeError("registry is not stable or requires repair")
        plans = self.get("migactionplans")
        self.save("plans-before.json", plans)
        active = [
            p.get("metadata", {}).get("name", "?")
            for p in plans.get("items", [])
            if p.get("status", {}).get("phase") not in ("Executed", "Failed")
            and p.get("metadata", {}).get("name") != self.plan
            # Historical manual-gated plans cannot execute without an explicit
            # approval patch. They are inert and should not block an isolated
            # smoke run years later.
            and not (
                p.get("status", {}).get("phase") == "Planned"
                and p.get("spec", {}).get("phaseGate") == "manual"
            )
        ]
        if active:
            raise SmokeError("active plan(s) make smoke unsafe: " + ", ".join(active))
        routes = self.router()
        self.save("routes-before.json", routes)
        allowed = set(self.args.allow_runtime)
        for route in routes.get("routes", []):
            rid = str(route.get("runtimeId", route.get("runtime_id", "")))
            if rid not in allowed:
                raise SmokeError(f"route outside this smoke scope: {rid or '<missing runtimeId>'}")
            if int(route.get("endpointInflight", route.get("inflight", 0))) or int(route.get("endpointQueued", route.get("queued", 0))):
                raise SmokeError(f"allowed smoke route is not drained: {rid}")

    def snapshot_body(self) -> dict[str, Any]:
        source = json.loads(Path(self.args.source_arrival).read_text())
        target = json.loads(Path(self.args.target_arrival).read_text())
        if not isinstance(source, dict) or not isinstance(target, dict) or not target:
            raise SmokeError("source/target arrival files must be non-empty JSON objects")
        for mapping in (source, target):
            if any(not isinstance(k, str) or not isinstance(v, (int, float)) or v < 0 for k, v in mapping.items()):
                raise SmokeError("arrival values must be non-negative numeric workload rates")
        return {
            "apiVersion": "mig.or-sim.io/v1alpha1", "kind": "ArrivalSnapshot",
            "metadata": {"name": self.snapshot, "namespace": NAMESPACE,
                         "labels": {"experiment.or-sim.io/name": self.run_name, "or-sim.io/smoke": "ampere"}},
            "spec": {"source": "ampere-smoke", "mode": "target", "planner": "ours", "planningMethod": "ours",
                     "forceReplan": True, "phaseGate": "manual", "epoch": self.run_name,
                     "triggerReason": "scoped-ampere-smoke", "windowSeconds": 30, "unit": "requestsPerSecond",
                     "observedAt": dt.datetime.now(dt.timezone.utc).isoformat(), "placement": {"nodes": [NODE]},
                     "profileCatalogRef": "default", "scenarioPath": "mock/scenarios/real8gpu.yaml",
                     "calibrationOverlayRef": "physicalgpuregistry/default",
                     "currentAllocationRef": "physicalgpuregistry/default", "sourceArrival": source, "targetArrival": target,
                     "currentDemand": source, "targetDemand": target,
                     "registeredSLOMs": {"vgg16_image": 100, "vit_base_image": 300},
                     "slo": {"vgg16_image": {"e2eMs": 100, "latencyMs": 100},
                             "vit_base_image": {"e2eMs": 300, "latencyMs": 300}},
                     "transitionDemandPolicy": "min", "notes": ["manual gate", "scoped to ampere/or-sim-exp", "no traffic"]},
        }

    def wait_plan(self, timeout: float) -> dict[str, Any]:
        end = time.monotonic() + timeout
        last: dict[str, Any] = {}
        while time.monotonic() < end:
            try:
                last = self.get("migactionplans", self.plan)
            except SmokeError:
                time.sleep(1)
                continue
            phase = str(last.get("status", {}).get("phase", ""))
            if phase in ("Planned", "Executed", "Failed"):
                return last
            time.sleep(1)
        raise SmokeError(f"timed out waiting for {self.plan}; last phase={last.get('status', {}).get('phase')}")

    def wait_terminal_plan(self, timeout: float) -> dict[str, Any]:
        end = time.monotonic() + timeout
        last: dict[str, Any] = {}
        while time.monotonic() < end:
            last = self.get("migactionplans", self.plan)
            if str(last.get("status", {}).get("phase", "")) in ("Executed", "Failed"):
                return last
            time.sleep(1)
        raise SmokeError(f"timed out waiting for terminal {self.plan}; last phase={last.get('status', {}).get('phase')}")

    @staticmethod
    def actions(plan: dict[str, Any]) -> list[dict[str, Any]]:
        dag = plan.get("spec", {}).get("actionDag", {})
        return [x for x in dag.get("nodes", plan.get("spec", {}).get("abstractActions", [])) if isinstance(x, dict)]

    def audit(self, plan: dict[str, Any]) -> None:
        actions = self.actions(plan)
        if not actions:
            raise SmokeError("generated plan has no actions")
        types = [str(x.get("action", x).get("type", "")) for x in actions]
        physical = {str(x.get("action", x).get("physical_gpu_id", "")) for x in actions if x.get("action", x).get("physical_gpu_id")}
        if any(not p.startswith("ampere-") for p in physical):
            raise SmokeError("plan references a physical GPU outside ampere: " + repr(sorted(physical)))
        if self.args.mode == "batch":
            if any(t not in BATCH_TYPES for t in types):
                raise SmokeError("batch smoke permits only the four batch primitives; got " + repr(types))
            roots: dict[str, list[str]] = {}
            for node in actions:
                roots.setdefault(str(node.get("rootId", "")), []).append(str(node.get("action", node).get("type", "")))
            good = [root for root, seq in roots.items() if sorted(seq) == sorted(BATCH_TYPES)]
            if len(good) != self.args.expected_chains or len(roots) != len(good):
                raise SmokeError(f"expected exactly {self.args.expected_chains} complete independent batch chains, got {roots}")
        if self.args.mode == "cleanup":
            if any(t in ("allocate_gpu", "configure_full_template", "place_instance") for t in types):
                raise SmokeError("cleanup plan unexpectedly allocates/configures/places a workload")
        if self.args.mode in ("setup", "concurrency"):
            if self.args.mode == "concurrency" and any(t in GEOMETRY_TYPES for t in types):
                raise SmokeError("concurrency smoke cannot include geometry/lifecycle actions")
            placements = [x.get("action", x) for x in actions if str(x.get("action", x).get("type")) == "place_instance"]
            slots = {str(x.get("slot")) for x in placements}
            if len(slots) < 2:
                raise SmokeError("slot-concurrency smoke requires at least two distinct place_instance slots")
            placement_gpus = {str(x.get("physical_gpu_id")) for x in placements}
            if len(placement_gpus) != 1:
                raise SmokeError(f"slot-concurrency smoke requires exactly one physical GPU, got {sorted(placement_gpus)!r}")

    def verify_expected_batches(self) -> None:
        if not self.args.expect_batch:
            return
        registry = self.get("physicalgpuregistries", "default")
        routes = self.router()
        self.save("registry-after.json", registry); self.save("routes-after.json", routes)
        def registry_batches(value: Any, runtime_id: str) -> list[Any]:
            found: list[Any] = []
            if isinstance(value, dict):
                route = value.get("route")
                if isinstance(route, dict) and route.get("runtimeId", route.get("runtime_id")) == runtime_id:
                    found.append(value.get("observedBatchSize", value.get("batchSize")))
                for child in value.values():
                    found.extend(registry_batches(child, runtime_id))
            elif isinstance(value, list):
                for child in value:
                    found.extend(registry_batches(child, runtime_id))
            return found
        for pair in self.args.expect_batch:
            rid, raw = pair.split("=", 1)
            expected = int(raw)
            route = next((r for r in routes.get("routes", []) if r.get("runtimeId", r.get("runtime_id")) == rid), None)
            if route is None:
                raise SmokeError(f"expected runtime missing from router: {rid}")
            observed = route.get("runtime.batchSize", route.get("runtimeBatchSize"))
            if observed != expected:
                raise SmokeError(f"runtime metrics batch mismatch for {rid}: expected {expected}, got {observed!r}")
            observed_registry = registry_batches(registry, rid)
            if expected not in observed_registry:
                raise SmokeError(f"registry lacks an observed {expected} batch record for {rid}: {observed_registry!r}")

    def verify_concurrency_overlap(self, terminal: dict[str, Any]) -> None:
        if self.args.mode not in ("setup", "concurrency"):
            return
        placements: dict[str, tuple[str, str]] = {}
        for node in self.actions(terminal):
            action = node.get("action", node)
            if action.get("type") == "place_instance":
                placements[str(node.get("id"))] = (str(action.get("physical_gpu_id")), str(action.get("slot")))
        statuses = terminal.get("status", {}).get("actionStatuses", [])
        intervals: list[tuple[str, str, float, float]] = []
        for row in statuses:
            ident = str(row.get("id"))
            if ident not in placements or row.get("status") != "completed":
                continue
            start, end = row.get("relativeStartSeconds"), row.get("relativeEndSeconds")
            if not isinstance(start, (int, float)) or not isinstance(end, (int, float)):
                raise SmokeError(f"place_instance timing is missing for {ident}")
            gpu, slot = placements[ident]
            intervals.append((gpu, slot, float(start), float(end)))
        for i, (gpu_a, slot_a, start_a, end_a) in enumerate(intervals):
            for gpu_b, slot_b, start_b, end_b in intervals[i + 1:]:
                if gpu_a == gpu_b and slot_a != slot_b and min(end_a, end_b) > max(start_a, start_b):
                    self.save("concurrency-overlap.json", {"gpu": gpu_a, "slotA": slot_a, "slotB": slot_b,
                                                          "overlapSeconds": min(end_a, end_b) - max(start_a, start_b)})
                    return
        raise SmokeError("no positive same-GPU/distinct-slot place_instance overlap in executor timestamps")

    def run(self) -> None:
        self.preflight()
        if self.args.approve_staged:
            plan = self.get("migactionplans", self.plan)
            self.save("plan-planned.json", plan)
            if str(plan.get("status", {}).get("phase")) != "Planned":
                raise SmokeError("staged plan is not in Planned phase")
            self.audit(plan)
            self.kubectl("-n", NAMESPACE, "patch", "migactionplan", self.plan, "--type=merge", "-p", '{"spec":{"phaseGate":"approved"}}')
            self.approved = True
            terminal = self.wait_terminal_plan(self.args.timeout)
            self.save("plan-terminal.json", terminal)
            if str(terminal.get("status", {}).get("phase")) != "Executed":
                raise SmokeError("approved staged plan did not execute successfully")
            final = terminal.get("status", {}).get("transitionExecution", {}).get("metrics", {}).get("finalValidation", {})
            if final.get("ok") is not True:
                raise SmokeError("executor final validation was not successful")
            self.verify_expected_batches(); self.verify_concurrency_overlap(terminal)
            self.success = True
            return
        body = self.snapshot_body()
        self.save("arrival-snapshot.json", body)
        if not (self.args.execute or self.args.stage):
            self.save("README.txt", "Preflight passed. Re-run the identical command with --execute only in an exclusive maintenance window.\n")
            self.success = True
            return
        self.kubectl("apply", "-f", "-", input_text=json.dumps(body))
        plan = self.wait_plan(self.args.timeout)
        self.save("plan-planned.json", plan)
        if str(plan.get("status", {}).get("phase")) != "Planned":
            raise SmokeError("planner did not produce a manual-gated Planned action plan")
        self.audit(plan)
        if self.args.stage:
            self.save("STAGED.txt", "Plan is manual-gated and has not been approved. Delete these named CRs explicitly if the review rejects it.\n")
            self.success = True
            return
        self.kubectl("-n", NAMESPACE, "patch", "migactionplan", self.plan, "--type=merge", "-p", '{"spec":{"phaseGate":"approved"}}')
        self.approved = True
        terminal = self.wait_terminal_plan(self.args.timeout)
        self.save("plan-terminal.json", terminal)
        if str(terminal.get("status", {}).get("phase")) != "Executed":
            raise SmokeError("approved plan did not execute successfully")
        final = terminal.get("status", {}).get("transitionExecution", {}).get("metrics", {}).get("finalValidation", {})
        if final.get("ok") is not True:
            raise SmokeError("executor final validation was not successful")
        self.verify_expected_batches()
        self.verify_concurrency_overlap(terminal)
        self.success = True


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    phase = p.add_mutually_exclusive_group()
    phase.add_argument("--stage", action="store_true", help="create and audit one manual-gated plan, but never approve it")
    phase.add_argument("--execute", action="store_true", help="create, audit, and approve exactly one manual-gated smoke plan")
    phase.add_argument("--approve-staged", metavar="RUN_NAME", help="audit and approve an already staged plan of this exact run")
    p.add_argument("--namespace", default=NAMESPACE); p.add_argument("--node", default=NODE)
    p.add_argument("--router-url", required=True); p.add_argument("--source-arrival"); p.add_argument("--target-arrival")
    p.add_argument("--mode", choices=("setup", "batch", "concurrency", "cleanup"), required=True)
    p.add_argument("--expected-chains", type=int, default=2); p.add_argument("--expect-batch", action="append", default=[], metavar="RUNTIME_ID=BATCH")
    p.add_argument("--allow-runtime", action="append", default=[]); p.add_argument("--timeout", type=float, default=300)
    p.add_argument("--run-name"); p.add_argument("--output-dir")
    args = p.parse_args()
    if not args.approve_staged and (not args.source_arrival or not args.target_arrival):
        p.error("--source-arrival and --target-arrival are required unless --approve-staged is used")
    try:
        smoke = Smoke(args); smoke.run()
        print(json.dumps({"ok": True, "run": smoke.run_name, "artifacts": str(smoke.out), "executed": args.execute or bool(args.approve_staged), "staged": args.stage}))
        return 0
    except (SmokeError, OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"SMOKE BLOCKED/FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
