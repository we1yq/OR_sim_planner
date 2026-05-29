#!/usr/bin/env python3
from __future__ import annotations

import copy
import json
import re
import sys
from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
PROTO_ROOT = REPO_ROOT / "k8s-extension-prototype"
CONTROLLER_ROOT = PROTO_ROOT / "controller"
for root in (CONTROLLER_ROOT, PROTO_ROOT):
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

from planning.k8s_adapter import (  # noqa: E402
    cluster_state_to_dict,
    plan_scenario_as_migplan_status,
)
from scenario_loader import load_planning_scenario  # noqa: E402
from models import PlanningScenario, ScenarioWorkloadDemand  # noqa: E402
from migrant_core.state import ClusterState, GPUState, MigInstance  # noqa: E402
from migrant_core.physical_ids import ensure_state_metadata  # noqa: E402


HOST_PORTS = {"llama": 10681, "gpt2": 10682, "resnet50": 10683, "vgg16": 10684, "vit_base": 10685}


def main() -> None:
    payload = json.load(sys.stdin)
    planning_input = dict(payload.get("planningInput", {}))
    scenario_path = Path(payload.get("scenarioPath") or "k8s-extension-prototype/mock/scenarios/stage0.yaml")
    if not scenario_path.is_absolute():
        scenario_path = REPO_ROOT / scenario_path
    scenario = load_planning_scenario(scenario_path)
    scenario = apply_planning_input(scenario, planning_input)
    current = current_allocation_to_cluster_state(dict(payload.get("currentAllocation", {})))
    planned = plan_scenario_as_migplan_status(
        scenario=scenario,
        source_state_override=current,
        max_iters=20,
        milp_time_limit_s=None,
        verbose=False,
    )
    status = dict(planned["status"])
    target_state = dict(status.get("targetState", {}))
    actions = list(status.get("actions", []))
    planning_trace = dict(status.get("planningTrace", {}))
    transition = dict(planning_trace.get("transition", {}))
    action_dag = transition.get("phasedActionPlan") or transition.get("phasedActionPlanSummary") or {}
    desired_runtimes = desired_runtimes_from_target_state(target_state)
    result = {
        "targetAllocationPlan": {
            "planner": "placement.milp_enhanced + target.preserve_greedy",
            "objective": "Gurobi MILP target allocation followed by notebook target materialization",
            "milp": status.get("milp", {}),
            "targetState": target_state,
            "desiredRuntimes": desired_runtimes,
        },
        "abstractActions": actions,
        "actionDag": action_dag,
        "validationTargets": {
            "targetAllocationPlan": {
                "targetState": target_state,
                "desiredRuntimes": desired_runtimes,
            }
        },
        "desiredRuntimes": desired_runtimes,
        "metadata": {
            "planner": "original-gurobi-milp-greedy-repair-effect-aware-dag",
            "pipeline": planning_trace.get("pipeline"),
            "metrics": status.get("metrics", {}),
            "planningTrace": planning_trace,
        },
    }
    print(json.dumps(result, separators=(",", ":"), sort_keys=True))


def apply_planning_input(scenario: PlanningScenario, planning_input: dict[str, Any]) -> PlanningScenario:
    target = dict(planning_input.get("targetArrival", {}))
    workloads = []
    for workload in scenario.workloads:
        workloads.append(
            ScenarioWorkloadDemand(
                name=workload.name,
                source_arrival=workload.source_arrival,
                target_arrival=float(target.get(workload.name, 0.0)),
                workload_ref=workload.workload_ref,
                profile_catalog_ref=workload.profile_catalog_ref,
                profile_catalog_configmap=workload.profile_catalog_configmap,
            )
        )
    transition = dict(scenario.transition)
    transition["transitionPlanner"] = "effect_aware_dag"
    transition["arrivalSnapshot"] = {
        "epoch": planning_input.get("epoch"),
        "source": planning_input.get("source", "runtime-router"),
        "triggerReason": planning_input.get("triggerReason"),
    }
    return PlanningScenario(
        name=f"epoch-{planning_input.get('epoch') or scenario.name}",
        description=scenario.description,
        policy_ref=scenario.policy_ref,
        mig_rules_ref=scenario.mig_rules_ref,
        source_state_ref=scenario.source_state_ref,
        target_state_ref=scenario.target_state_ref,
        workloads=workloads,
        transition=transition,
    )


def current_allocation_to_cluster_state(current: dict[str, Any]) -> ClusterState:
    raw_gpus = dict(current.get("gpus", {}))
    ordered = sorted(raw_gpus)
    physical_id_map = {}
    gpus = []
    for logical_id, physical_id in enumerate(ordered):
        gpu = dict(raw_gpus[physical_id])
        physical_id_map[logical_id] = physical_id
        instances = instances_from_current_gpu(gpu)
        gpus.append(GPUState(gpu_id=logical_id, source="real", instances=instances))
    state = ClusterState(
        gpus=gpus,
        metadata={
            "source": "go-cluster-state-manager",
            "physical_id_map": physical_id_map,
            "physicalGpuBindings": {
                physical_id: {
                    "nodeName": dict(raw_gpus[physical_id]).get("node"),
                    "deviceIndex": dict(raw_gpus[physical_id]).get("gpuIndex"),
                }
                for physical_id in ordered
            },
            "free_physical_gpu_pool": [
                physical_id
                for physical_id in ordered
                if dict(raw_gpus[physical_id]).get("state") == "available"
            ],
        },
    )
    ensure_state_metadata(state)
    return state


def instances_from_current_gpu(gpu: dict[str, Any]) -> list[MigInstance]:
    bindings = list(gpu.get("runtimeBindings") or [])
    slots = []
    for binding in bindings:
        binding = dict(binding)
        parsed = parse_slot_resource(str(binding.get("slotResource") or ""))
        if parsed is None:
            continue
        start, end, profile = parsed
        slots.append(
            MigInstance(
                start=start,
                end=end,
                profile=profile,
                workload=binding.get("model"),
                batch=int(binding.get("batch") or 1),
                mu=float(binding.get("mu") or 0.0),
            )
        )
    if not slots:
        return [MigInstance(start=0, end=7, profile="void")]
    slots.sort(key=lambda inst: (inst.start, inst.end, inst.profile))
    out = []
    cur = 0
    for inst in slots:
        if inst.start > cur:
            out.append(MigInstance(start=cur, end=inst.start, profile="void"))
        out.append(inst)
        cur = inst.end
    if cur < 7:
        out.append(MigInstance(start=cur, end=7, profile="void"))
    return out


def parse_slot_resource(value: str) -> tuple[int, int, str] | None:
    match = re.search(r"-s([0-9]+)-([0-9]+)-([0-9]g)$", value)
    if not match:
        return None
    return int(match.group(1)), int(match.group(2)), match.group(3)


def desired_runtimes_from_target_state(target_state: dict[str, Any]) -> list[dict[str, Any]]:
    metadata = dict(target_state.get("metadata", {}))
    physical_map = {int(k): v for k, v in dict(metadata.get("physical_id_map", {})).items()}
    runtimes = []
    for gpu in list(target_state.get("gpus", [])):
        gpu_id = int(gpu.get("gpuId"))
        physical_id = str(physical_map.get(gpu_id, f"gpu{gpu_id}"))
        node = physical_id.split("-gpu", 1)[0] if "-gpu" in physical_id else ""
        for inst in list(gpu.get("instances", [])):
            profile = str(inst.get("profile"))
            workload = inst.get("workload")
            if not workload or profile == "void":
                continue
            start, end = int(inst["start"]), int(inst["end"])
            model = str(workload)
            runtimes.append(
                {
                    "model": model,
                    "batchSize": int(inst.get("batch") or 1),
                    "node": node,
                    "hostPort": int(HOST_PORTS.get(model, 10680 + len(runtimes) + 1)),
                    "profile": profile,
                    "gpu": physical_id,
                    "slotResource": f"or-sim.io/{physical_id}-s{start}-{end}-{profile}",
                }
            )
    runtimes.sort(key=lambda row: row["model"])
    return runtimes


if __name__ == "__main__":
    main()
