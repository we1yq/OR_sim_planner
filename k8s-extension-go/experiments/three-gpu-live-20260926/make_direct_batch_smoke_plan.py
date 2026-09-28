#!/usr/bin/env python3
"""Derive a two-runtime, batch-only MigActionPlan from an audited setup plan."""
import argparse, copy, json

p = argparse.ArgumentParser()
p.add_argument("--baseline", required=True)
p.add_argument("--name", required=True)
p.add_argument("--batch", required=True, type=int)
p.add_argument("--runtime", action="append", required=True)
p.add_argument("--output")
a = p.parse_args()
base = json.load(open(a.baseline, encoding="utf-8"))
source_spec = base["spec"]
target = copy.deepcopy(source_spec["validationTargets"]["targetAllocationPlan"])
selected = set(a.runtime)
chosen = []
for rt in target["desiredRuntimes"]:
    if rt.get("runtimeId") in selected:
        rt["batchSize"] = a.batch
        chosen.append(rt)
if {rt["runtimeId"] for rt in chosen} != selected:
    raise SystemExit("requested runtime is absent from baseline")

# TargetState is the executor's topology + workload validation input.
for gpu in target["targetState"].get("gpus", []):
    for inst in gpu.get("instances", []):
        for rt in chosen:
            if inst.get("workload") == rt.get("model") and int(inst.get("start", -1)) == int(rt["slotResource"].rsplit("-s", 1)[1].split("-", 1)[0]):
                inst["batch"] = a.batch

actions, nodes = [], []
for chain, rt in enumerate(chosen):
    slot_tail = rt["slotResource"].rsplit("-s", 1)[1].split("-")
    slot = [int(slot_tail[0]), int(slot_tail[1]), rt["profile"]]
    root = f"BATCH_SMOKE_{chain}_{rt['runtimeId']}"
    previous = None
    for kind in ("patch_batch_config", "apply_batch", "verify_batch", "activate_instance_route"):
        action = {"type": kind, "abstractRoot": root, "gpu_id": 0, "physical_gpu_id": rt["gpu"],
                  "slot": slot, "workload": rt["model"], "old_batch": 1 if a.batch != 1 else 32,
                  "new_batch": a.batch, "transitionMode": "batch_change"}
        ident = f"a{len(actions):04d}_{kind}_{chain}"
        node = {"id": ident, "index": len(actions), "rootId": root, "phase": len(actions) % 4,
                "action": action, "dependsOn": [] if previous is None else [previous]}
        actions.append(action); nodes.append(node); previous = ident

plan = {
  "apiVersion": "mig.or-sim.io/v1alpha1", "kind": "MigActionPlan",
  "metadata": {"name": a.name, "namespace": "or-sim-exp", "labels": {"or-sim.io/smoke": "ampere-batch-concurrency"}},
  "spec": {"executor": "go-transition-executor", "phaseGate": "manual", "actionCount": len(actions),
           "abstractActions": actions,
           "actionDag": {"representation": "migrant.phased-action-dag/v1", "nodes": nodes},
           "summary": {"sourceGpuCount": 1, "targetGpuCount": 1}, "targetGpuCount": 1,
           "validationTargets": {"targetAllocationPlan": target}, "targetAllocationPlan": target}
}
payload = json.dumps(plan) + "\n"
if a.output:
    open(a.output, "w", encoding="utf-8").write(payload)
else:
    print(payload, end="")
