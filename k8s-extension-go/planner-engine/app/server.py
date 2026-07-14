from __future__ import annotations

import json
import os
import re
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
for path in (ROOT,):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from planning.jormungandr_adapter import plan_jormungandr_as_migplan_status  # noqa: E402
from planning.k8s_adapter import build_feasible_option_dataframe, plan_scenario_as_migplan_status  # noqa: E402
from scenario_loader import load_planning_scenario  # noqa: E402
from models import PlanningScenario, ScenarioWorkloadDemand  # noqa: E402
from migrant_core.state import ClusterState, GPUState, MigInstance  # noqa: E402
from migrant_core.physical_ids import ensure_state_metadata  # noqa: E402


RUNTIME_HOST_PORT_POOL = tuple(
    port for port in range(10681, 10721) if port not in {10684, 10690}
)

ABSTRACT_PROFILE_SIZE = {"7g": 7, "4g": 4, "3g": 3, "2g": 2, "1g": 1}
PLACEMENT_PROFILE_SIZE = {"7g": 8, "4g": 4, "3g": 4, "2g": 2, "1g": 1}


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/healthz":
            self._json(200, {"ok": True, "component": "planner-engine"})
            return
        self._json(404, {"error": "not found"})

    def do_POST(self) -> None:
        if self.path != "/plan":
            self._json(404, {"error": "not found"})
            return
        try:
            length = int(self.headers.get("content-length", "0"))
            payload = json.loads(self.rfile.read(length) or b"{}")
            started = time.perf_counter()
            result = plan(payload)
            result.setdefault("metadata", {})["plannerEngineElapsedSec"] = time.perf_counter() - started
            self._json(200, result)
        except Exception as exc:
            self._json(500, {"error": str(exc)})

    def log_message(self, fmt: str, *args: Any) -> None:
        print(fmt % args, file=sys.stderr, flush=True)

    def _json(self, status: int, payload: dict[str, Any]) -> None:
        raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


def plan(payload: dict[str, Any]) -> dict[str, Any]:
    planning_input = dict(payload.get("planningInput", {}))
    scenario_path = Path(payload.get("scenarioPath") or "mock/scenarios/stage0.yaml")
    if not scenario_path.is_absolute():
        scenario_path = ROOT / scenario_path
    scenario = apply_planning_input(load_planning_scenario(scenario_path), planning_input)
    current = current_allocation_to_cluster_state(dict(payload.get("currentAllocation", {})))
    planner = normalize_planner_name(planning_input)
    runtime_profile_correction = {}
    if str(os.environ.get("ENABLE_RUNTIME_PROFILE_CORRECTION", "")).lower() == "true":
        runtime_profile_correction = dict(payload.get("runtimeProfileCorrection") or {})
    if planner == "jormungandr":
        feasible_option_df = build_feasible_option_dataframe(
            scenario,
            runtime_profile_correction=runtime_profile_correction,
        )
        planned = plan_jormungandr_as_migplan_status(
            scenario=scenario,
            source_state=current,
            feasible_option_df=feasible_option_df,
        )
    else:
        planned = plan_scenario_as_migplan_status(
            scenario=scenario,
            source_state_override=current,
            runtime_profile_correction=runtime_profile_correction,
            max_iters=int(payload.get("maxIters") or 20),
            milp_time_limit_s=payload.get("milpTimeLimitSeconds"),
            verbose=bool(payload.get("verbose", False)),
        )
    status = dict(planned["status"])
    target_state = dict(status.get("targetState", {}))
    actions = list(status.get("actions", []))
    planning_trace = dict(status.get("planningTrace", {}))
    apply_canonical_physical_ids(target_state, planning_trace)
    transition = dict(planning_trace.get("transition", {}))
    action_dag = transition.get("phasedActionPlan") or transition.get("phasedActionPlanSummary") or {}
    desired_runtimes = desired_runtimes_from_target_state(target_state, planning_trace)
    execution_runtimes = desired_runtimes_from_actions(actions, target_state)
    planner_label = (
        "jormungandr-exchange-and-compact-common-executor"
        if planner == "jormungandr"
        else "original-gurobi-milp-exact-stage2-effect-aware-dag"
    )
    objective = (
        "Jormungandr utility-first target allocation and exchange-and-compact transition, lowered to the common executor DAG"
        if planner == "jormungandr"
        else "Gurobi MILP target allocation followed by exact global Stage 2 materialization"
    )
    return {
        "targetAllocationPlan": {
            "planner": planner_label,
            "objective": objective,
            "milp": status.get("milp", {}),
            "targetState": target_state,
            "desiredRuntimes": desired_runtimes,
        },
        "abstractActions": actions,
        "actionDag": normalize_action_dag(action_dag, actions),
        "validationTargets": {
            "targetAllocationPlan": {
                "targetState": target_state,
                "desiredRuntimes": desired_runtimes,
            }
        },
        "desiredRuntimes": desired_runtimes,
        "executionRuntimes": execution_runtimes,
        "metadata": {
            "planner": planner_label,
            "requestedPlanner": planner,
            "pipeline": planning_trace.get("pipeline"),
            "metrics": status.get("metrics", {}),
            "planningTrace": planning_trace,
        },
    }


def normalize_planner_name(planning_input: dict[str, Any]) -> str:
    raw = (
        planning_input.get("planner")
        or planning_input.get("planningMethod")
        or planning_input.get("targetPlanner")
        or planning_input.get("plannerSelector")
        or "ours"
    )
    value = str(raw).strip().lower().replace("_", "-")
    if value in {"jorm", "jormungandr", "jormungandr-baseline"}:
        return "jormungandr"
    if value in {"ours", "migrant", "migrant-ours", "default"}:
        return "ours"
    raise ValueError(f"unsupported planner selector: {raw}")


def apply_canonical_physical_ids(target_state: dict[str, Any], planning_trace: dict[str, Any]) -> None:
    metadata = dict(target_state.get("metadata") or {})
    if metadata.get("physical_id_map"):
        target_state["metadata"] = metadata
        return
    canonical = dict(planning_trace.get("canonicalization") or {})
    physical_ids = dict(canonical.get("canonicalPhysicalIds") or {})
    if physical_ids:
        metadata["physical_id_map"] = {str(k): v for k, v in physical_ids.items()}
        target_state["metadata"] = metadata


def apply_planning_input(scenario: PlanningScenario, planning_input: dict[str, Any]) -> PlanningScenario:
    target = _arrival_map_from_planning_input(planning_input, source=False)
    source = _arrival_map_from_planning_input(planning_input, source=True)
    workloads = []
    for workload in scenario.workloads:
        workloads.append(
            ScenarioWorkloadDemand(
                name=workload.name,
                source_arrival=float(source.get(workload.name, workload.source_arrival)),
                target_arrival=float(target.get(workload.name, 0.0)),
                workload_ref=workload.workload_ref,
                profile_catalog_ref=workload.profile_catalog_ref,
                profile_catalog_configmap=workload.profile_catalog_configmap,
            )
        )
    transition = dict(scenario.transition)
    transition["transitionPlanner"] = "effect_aware_dag"
    transition["planner"] = normalize_planner_name(planning_input)
    transition["forceReplan"] = bool(planning_input.get("forceReplan", transition.get("forceReplan", False)))
    if planning_input.get("transitionDemandPolicy"):
        transition["transitionDemandPolicy"] = str(planning_input["transitionDemandPolicy"])
    transition["arrivalSnapshot"] = {
        "epoch": planning_input.get("epoch"),
        "source": planning_input.get("source", "runtime-router"),
        "triggerReason": planning_input.get("triggerReason"),
        "sourceArrival": source,
        "targetArrival": target,
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


def _arrival_map_from_planning_input(planning_input: dict[str, Any], source: bool) -> dict[str, float]:
    if source:
        keys = ("sourceArrival", "currentDemand", "currentArrival", "sourceDemand")
        slo_keys = ("sourceArrival", "currentDemandRate", "currentDemand", "sourceDemandRate", "sourceDemand")
    else:
        keys = ("targetArrival", "targetDemand")
        slo_keys = ("targetArrival", "demandRate", "targetDemandRate", "targetDemand")
    for key in keys:
        out = _number_map(planning_input.get(key))
        if out:
            return out
    return _demand_rate_map_from_slo(dict(planning_input.get("slo") or {}), slo_keys)


def _demand_rate_map_from_slo(slo: dict[str, Any], keys: tuple[str, ...]) -> dict[str, float]:
    for key in keys:
        out = _number_map(slo.get(key))
        if out:
            return out
    out: dict[str, float] = {}
    for model, raw in slo.items():
        if not isinstance(raw, dict):
            continue
        for key in keys:
            value = _number(raw.get(key))
            if value is not None:
                out[str(model)] = value
                break
    return out


def _number_map(raw: Any) -> dict[str, float]:
    if not isinstance(raw, dict):
        return {}
    out: dict[str, float] = {}
    for key, value in raw.items():
        parsed = _number(value)
        if parsed is not None:
            out[str(key)] = parsed
    return out


def _number(value: Any) -> float | None:
    try:
        if value is None or isinstance(value, bool):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def current_allocation_to_cluster_state(current: dict[str, Any]) -> ClusterState:
    raw = current.get("gpus", {})
    if isinstance(raw, list):
        raw_gpus = {}
        for idx, item in enumerate(raw):
            gpu = dict(item)
            raw_gpus[str(gpu.get("id") or gpu.get("physicalGPU") or f"gpu{idx}")] = gpu
    else:
        raw_gpus = dict(raw)
    allowed_nodes = set(current.get("allowedNodes") or [])
    free_pool = filter_physical_pool_by_nodes(
        list(current.get("freePhysicalGpuPool") or []),
        allowed_nodes,
    )
    logical_rows = list(current.get("logicalGpus") or [])
    if logical_rows:
        physical_id_map = {}
        gpus = []
        for row in sorted(logical_rows, key=lambda item: int(dict(item).get("logicalGpuId", 0))):
            row = dict(row)
            physical_id = str(row.get("physicalGpuId") or row.get("physicalGPU") or "")
            if not physical_id:
                continue
            source_gpu = dict(raw_gpus.get(physical_id) or {})
            merged = {**source_gpu, **row}
            if allowed_nodes and merged.get("node") not in allowed_nodes:
                continue
            logical_id = int(row.get("logicalGpuId"))
            physical_id_map[logical_id] = physical_id
            gpus.append(
                GPUState(
                    gpu_id=logical_id,
                    source="real",
                    instances=instances_from_current_gpu(merged),
                )
            )
        state = ClusterState(
            gpus=gpus,
            metadata={
                "source": "go-cluster-state-manager-canonical",
                "physical_id_map": physical_id_map,
                "physicalGpuBindings": {
                    physical_id: {
                        "nodeName": dict(raw_gpus.get(physical_id) or {}).get("node"),
                        "deviceIndex": dict(raw_gpus.get(physical_id) or {}).get("gpuIndex"),
                    }
                    for physical_id in physical_id_map.values()
                },
                "free_physical_gpu_pool": free_pool,
                "logical_id_map": dict(dict(current.get("metadata") or {}).get("logical_id_map") or {}),
            },
        )
        ensure_state_metadata(state)
        return state
    ordered = [
        gpu_id
        for gpu_id in sorted(raw_gpus)
        if not allowed_nodes or dict(raw_gpus[gpu_id]).get("node") in allowed_nodes
    ]
    physical_id_map = {}
    gpus = []
    for logical_id, physical_id in enumerate(ordered):
        gpu = dict(raw_gpus[physical_id])
        physical_id_map[logical_id] = physical_id
        gpus.append(
            GPUState(
                gpu_id=logical_id,
                source="real",
                instances=instances_from_current_gpu(gpu),
            )
        )
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
            "free_physical_gpu_pool": filter_physical_pool_by_nodes([
                physical_id
                for physical_id in ordered
                if dict(raw_gpus[physical_id]).get("state") == "available"
            ], allowed_nodes),
        },
    )
    ensure_state_metadata(state)
    return state


def filter_physical_pool_by_nodes(pool: list[Any], allowed_nodes: set[str]) -> list[str]:
    out = []
    for raw in pool:
        physical_id = str(raw)
        if not physical_id:
            continue
        node = physical_id.split("-gpu", 1)[0] if "-gpu" in physical_id else ""
        if allowed_nodes and node not in allowed_nodes:
            continue
        out.append(physical_id)
    return out


def instances_from_current_gpu(gpu: dict[str, Any]) -> list[MigInstance]:
    slots = []
    occupied = set()
    for binding in list(gpu.get("runtimeBindings") or []):
        binding = dict(binding)
        parsed = parse_slot_resource(str(binding.get("slotResource") or ""))
        if parsed is None:
            continue
        start, end, profile = parsed
        occupied.add((start, end, profile))
        slots.append(
            MigInstance(
                start=start,
                end=end,
                profile=profile,
                workload=binding.get("model"),
                batch=int(binding.get("batchSize") or binding.get("batch") or 1),
                model_key=binding.get("modelKey") or binding.get("model_key"),
                placement_group=binding.get("placementGroup") or binding.get("placement_group"),
                mu=float(binding.get("mu") or 0.0),
            )
        )
    for device in list(gpu.get("migDevices") or []):
        device = dict(device)
        profile = str(device.get("profile") or "")
        placement_start = int(device.get("start") or 0)
        placement_end = int(device.get("end") or 0)
        if not profile or placement_end <= placement_start:
            continue
        abstract_end = placement_start + ABSTRACT_PROFILE_SIZE.get(profile, placement_end - placement_start)
        if abstract_end > 7:
            continue
        key = (placement_start, abstract_end, profile)
        if key in occupied:
            continue
        slots.append(
            MigInstance(
                start=placement_start,
                end=abstract_end,
                profile=profile,
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
    start, observed_end, profile = int(match.group(1)), int(match.group(2)), match.group(3)
    abstract_end = start + ABSTRACT_PROFILE_SIZE.get(profile, observed_end - start)
    return start, abstract_end, profile


def exact_slot_resource(physical_id: str, start: int, _abstract_end: int, profile: str) -> str:
    placement_end = start + PLACEMENT_PROFILE_SIZE.get(profile, ABSTRACT_PROFILE_SIZE.get(profile, _abstract_end - start))
    return f"or-sim.io/{physical_id}-s{start}-{placement_end}-{profile}"


def desired_runtimes_from_target_state(target_state: dict[str, Any], planning_trace: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    metadata = dict(target_state.get("metadata", {}))
    physical_map = {int(k): v for k, v in dict(metadata.get("physical_id_map", {})).items()}
    transition = dict((planning_trace or {}).get("transition") or {})
    executed_physical_ids = {int(k): v for k, v in dict(transition.get("executedPhysicalIds") or {}).items()}
    runtimes = []
    for gpu in list(target_state.get("gpus", [])):
        gpu_id = int(gpu.get("gpuId"))
        physical_id = str(executed_physical_ids.get(gpu_id) or physical_map.get(gpu_id) or f"gpu{gpu_id}")
        node = physical_id.split("-gpu", 1)[0] if "-gpu" in physical_id else ""
        for inst in list(gpu.get("instances", [])):
            profile = str(inst.get("profile"))
            workload = inst.get("workload")
            if not workload or profile == "void":
                continue
            start, end = int(inst["start"]), int(inst["end"])
            model = str(workload)
            model_key = str(inst.get("modelKey") or inst.get("model_key") or model)
            placement_group = str(
                inst.get("placementGroup") or inst.get("placement_group") or model_key
            )
            runtime_id = runtime_id_for(model, physical_id, start, end, profile)
            mu = float(inst.get("mu") or 0.0)
            runtimes.append(
                {
                    "model": model,
                    "modelKey": model_key,
                    "placementGroup": placement_group,
                    "runtimeId": runtime_id,
                    "batchSize": int(inst.get("batch") or 1),
                    "node": node,
                    "profile": profile,
                    "gpu": physical_id,
                    "slotResource": exact_slot_resource(physical_id, start, end, profile),
                    "capacity": mu,
                    "weight": mu if mu > 0 else float(ABSTRACT_PROFILE_SIZE.get(profile, 1)),
                }
            )
    assign_runtime_host_ports(runtimes)
    runtimes.sort(key=lambda row: row["model"])
    return runtimes


def desired_runtimes_from_actions(actions: list[dict[str, Any]], target_state: dict[str, Any]) -> list[dict[str, Any]]:
    by_key = {}
    for action in actions:
        action = dict(action)
        physical_id = str(action.get("physical_gpu_id") or "")
        if not physical_id:
            continue
        node = physical_id.split("-gpu", 1)[0] if "-gpu" in physical_id else ""
        for record in list(action.get("producesCapacity") or []):
            record = dict(record)
            slot = list(record.get("slot") or [])
            workload = record.get("workload")
            if not workload or len(slot) < 3:
                continue
            start, end, profile = int(slot[0]), int(slot[1]), str(slot[2])
            model = str(workload)
            model_key = str(record.get("modelKey") or record.get("model_key") or model)
            placement_group = str(
                record.get("placementGroup") or record.get("placement_group") or model_key
            )
            runtime_id = runtime_id_for(model, physical_id, start, end, profile)
            mu = float(record.get("mu") or 0.0)
            by_key[(model, physical_id, start, end, profile)] = {
                "model": model,
                "modelKey": model_key,
                "placementGroup": placement_group,
                "runtimeId": runtime_id,
                "batchSize": int(record.get("batch") or 1),
                "node": node,
                "profile": profile,
                "gpu": physical_id,
                "slotResource": exact_slot_resource(physical_id, start, end, profile),
                "capacity": mu,
                "weight": mu if mu > 0 else float(ABSTRACT_PROFILE_SIZE.get(profile, 1)),
            }
    if by_key:
        out = list(by_key.values())
        assign_runtime_host_ports(out)
        out.sort(key=lambda row: row["model"])
        return out
    return desired_runtimes_from_target_state(target_state)


def assign_runtime_host_ports(runtimes: list[dict[str, Any]]) -> None:
    next_index_by_node: dict[str, int] = {}
    ordered = sorted(
        runtimes,
        key=lambda row: (
            str(row.get("node") or ""),
            str(row.get("gpu") or ""),
            str(row.get("slotResource") or ""),
            str(row.get("model") or ""),
        ),
    )
    for runtime in ordered:
        node = str(runtime.get("node") or "")
        index = runtime_host_port_index(runtime)
        if index is None:
            index = next_index_by_node.get(node, 0)
            next_index_by_node[node] = index + 1
        if index >= len(RUNTIME_HOST_PORT_POOL):
            raise ValueError(f"runtime host port pool exhausted on node {node!r}")
        runtime["hostPort"] = RUNTIME_HOST_PORT_POOL[index]


def runtime_host_port_index(runtime: dict[str, Any]) -> int | None:
    gpu = str(runtime.get("gpu") or "")
    slot_resource = str(runtime.get("slotResource") or "")
    gpu_match = re.search(r"-gpu(\d+)$", gpu)
    slot_match = re.search(r"-s(\d+)-\d+-[a-z0-9]+$", slot_resource)
    if not gpu_match or not slot_match:
        return None
    return int(gpu_match.group(1)) * 7 + int(slot_match.group(1))


def runtime_id_for(model: str, physical_id: str, start: int, end: int, profile: str) -> str:
    raw = f"{model}-{physical_id}-s{start}-{end}-{profile}"
    return re.sub(r"[^a-z0-9-]+", "-", raw.lower()).strip("-")


def normalize_action_dag(action_dag: Any, actions: list[dict[str, Any]]) -> dict[str, Any]:
    if isinstance(action_dag, dict) and action_dag:
        return action_dag
    return {
        "format": "migrant.phased-action-dag/v1",
        "nodes": [
            {
                "id": str(action.get("id") or f"action-{idx}"),
                "type": str(action.get("type")),
                "action": action,
                "dependsOn": list(action.get("dependsOn") or action.get("dependencies") or []),
            }
            for idx, action in enumerate(actions)
        ],
    }


if __name__ == "__main__":
    ThreadingHTTPServer(("", 8080), Handler).serve_forever()
