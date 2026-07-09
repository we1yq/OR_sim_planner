from __future__ import annotations

import csv
import math
import os
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


TEST_ROOT = Path(__file__).resolve().parent
EVAL_ROOT = TEST_ROOT.parent
REPO_ROOT = EVAL_ROOT.parent
ALGORITHM_ROOT = EVAL_ROOT / "algorithm"
PLANNER_ROOT = ALGORITHM_ROOT / "ours/planner_engine"
TRACE_ROOT = TEST_ROOT / "results/online_3gpu"
RESULT_ROOT = TEST_ROOT / "results/online_3gpu_replay"
FIGURE_ROOT = TEST_ROOT / "figures"

sys.path.insert(0, str(EVAL_ROOT))
sys.path.insert(0, str(ALGORITHM_ROOT))
sys.path.insert(0, str(PLANNER_ROOT))

import plan_quality_controlled as pq  # noqa: E402
from baselines.jormungandr_round import plan_jormungandr_round  # noqa: E402
from baselines.parvagpu import allocate_parvagpu_mig  # noqa: E402
from migrant_core.transition_planner import effect_aware_dag  # noqa: E402


WORKLOADS = pq.WORKLOADS
WORKLOAD_LABELS = pq.WORKLOAD_LABELS
EPOCH_SECONDS = 1800.0
DISPLAY_EPOCH_SECONDS = 300.0
FIGURE_PREFIX = "online_3gpu"
PLOT_EPOCH_COUNT = 0

METHOD_LABELS = {
    "ours": "SliceWise",
    "jormungandr": "Jorm.",
    "parvagpu_mig": "Parva",
}

METHOD_COLORS = {
    "ours": "#EF767A",
    "jormungandr": "#456990",
    "parvagpu_mig": "#48C0AA",
}

WORKLOAD_COLORS = {
    "resnet50_image": "#EF767A",
    "vgg16_image": "#456990",
    "vit_base_image": "#48C0AA",
    "gpt2_p64_o64": "#D87659",
    "gpt2_p512_o512": "#B395BD",
    "llama_p1024_o128": "#E9C46A",
    "llama_p2048_o64": "#299D8F",
}

# One place to revise the simulated execution model.
ACTION_DURATION_SEC = {
    "allocate_gpu": 0.05,
    "bind_target_gpu": 0.05,
    "clear_gpu_binding": 0.05,
    "return_gpu": 0.05,
    "return_extra_gpu": 0.05,
    "configure_full_template": 1.00,
    "configure_partial_profile": 1.10,
    "clear_template": 0.66,
    "create_mig_instance": 1.00,
    "delete_mig_instance": 0.66,
    "repartition_gpu": 1.10,
    "register_mig_devices": 0.20,
    "place_instance": 5.00,
    "create_container": 5.00,
    "delete_instance": 1.00,
    "delete_container": 1.00,
    "migrate_container": 5.00,
    "keep_container": 0.00,
    "deactivate_instance_route": 0.05,
    "activate_instance_route": 0.05,
    "wait_instance_drain": 0.50,
    "patch_batch_config": 0.10,
    "apply_batch": 0.10,
    "verify_batch": 0.10,
    "mark_reconfig_target_prepared": 0.05,
}


@dataclass
class SimState:
    active: dict[str, dict[str, Any]]
    pending: dict[str, dict[str, Any]]
    materialized_gpus: set[int]


def main() -> None:
    RESULT_ROOT.mkdir(parents=True, exist_ok=True)
    FIGURE_ROOT.mkdir(parents=True, exist_ok=True)
    out_dir = RESULT_ROOT / time.strftime("%Y%m%d-%H%M%S")
    out_dir.mkdir(parents=True, exist_ok=True)

    trace_dir = latest_online_trace_dir()
    trace = pd.read_csv(trace_dir / "request_rate_30min.csv")
    feasible_df = pq.build_feasible_option_df()
    options = pq.serving_options_from_df(feasible_df)

    epoch_rows: list[dict[str, Any]] = []
    transition_rows: list[dict[str, Any]] = []
    action_rows: list[dict[str, Any]] = []
    time_rows: list[dict[str, Any]] = []
    service_rows: list[dict[str, Any]] = []
    runtime_rows: list[dict[str, Any]] = []
    similarity_rows: list[dict[str, Any]] = []
    unused_rows: list[dict[str, Any]] = []

    ours_targets: list[Any] = []
    jorm_targets: list[Any] = []
    parva_targets: list[Any] = []

    ours_prev = pq.run_ours_target(
        scenario_id="online3gpu_E00",
        demand=demand_from_row(trace.iloc[0]),
        feasible_df=feasible_df,
        prev_state=None,
    )
    jorm_prev = plan_jormungandr_round(
        scenario_id="online3gpu_jorm_E00",
        demand=demand_from_row(trace.iloc[0]),
        options=options,
        source_alloc=None,
    )["target_allocation"]
    parva_prev = allocate_parvagpu_mig(
        scenario_id="online3gpu_parva_E00",
        demand=demand_from_row(trace.iloc[0]),
        options=options,
    )
    ours_targets.append(ours_prev)
    jorm_targets.append(jorm_prev)
    parva_targets.append(parva_prev)
    append_epoch_rows(epoch_rows, 0, trace.iloc[0], ours_prev.result, jorm_prev, parva_prev)

    append_constant_interval(time_rows, service_rows, 0.0, EPOCH_SECONDS, "ours", ours_prev.result, demand_from_row(trace.iloc[0]))
    append_constant_interval(time_rows, service_rows, 0.0, EPOCH_SECONDS, "jormungandr", jorm_prev, demand_from_row(trace.iloc[0]))
    append_constant_interval(time_rows, service_rows, 0.0, EPOCH_SECONDS, "parvagpu_mig", parva_prev, demand_from_row(trace.iloc[0]))

    for epoch in range(1, len(trace)):
        src_demand = demand_from_row(trace.iloc[epoch - 1])
        tgt_demand = demand_from_row(trace.iloc[epoch])
        required = {workload: min(src_demand[workload], tgt_demand[workload]) for workload in WORKLOADS}
        base_time = float(epoch) * EPOCH_SECONDS
        interval_end = float(epoch + 1) * EPOCH_SECONDS

        ours_target = pq.run_ours_target(
            scenario_id=f"online3gpu_E{epoch:02d}",
            demand=tgt_demand,
            feasible_df=feasible_df,
            prev_state=ours_prev.state,
        )
        stage3_start = time.perf_counter()
        ours_transition = effect_aware_dag.run(
            source_state=ours_prev.state,
            target_state=ours_target.state,
            src_arrival=src_demand,
            tgt_arrival=tgt_demand,
            workload_names=WORKLOADS,
            stage_name=f"online3gpu_E{epoch:02d}_ours_stage3",
            transition_demand_policy="min",
        )
        ours_stage3_sec = time.perf_counter() - stage3_start

        jorm_round = plan_jormungandr_round(
            scenario_id=f"online3gpu_jorm_E{epoch:02d}",
            demand=tgt_demand,
            options=options,
            source_alloc=jorm_prev,
            source_demand=src_demand,
            workload_names=WORKLOADS,
            transition_id=f"online3gpu_E{epoch:02d}_jormungandr",
        )
        jorm_target = jorm_round["target_allocation"]
        jorm_transition = jorm_round["transition_plan"]

        parva_target = allocate_parvagpu_mig(
            scenario_id=f"online3gpu_parva_E{epoch:02d}",
            demand=tgt_demand,
            options=options,
        )

        ours_sim = simulate_transition(
            method="ours",
            source_alloc=ours_prev.result,
            target_alloc=ours_target.result,
            target_state=ours_target.state,
            transition_plan=ours_transition,
            required=required,
            base_time=base_time,
        )
        jorm_sim = simulate_transition(
            method="jormungandr",
            source_alloc=jorm_prev,
            target_alloc=jorm_target,
            target_state=None,
            transition_plan=jorm_transition,
            required=required,
            base_time=base_time,
        )

        for sim in [ours_sim, jorm_sim]:
            transition_rows.append(
                {
                    "epoch": epoch,
                    "method": sim["method"],
                    "makespan_sec": sim["makespan_sec"],
                    "slo_violation_duration_sec": sim["slo_violation_duration_sec"],
                    "slo_violation_workload_sec": sim["slo_violation_workload_sec"],
                    "peak_active_gpu": sim["peak_active_gpu"],
                }
            )
            action_rows.append({"epoch": epoch, "method": sim["method"], **sim["action_counts"]})
            time_rows.extend(sim["time_rows"])
            service_rows.extend(sim["service_rows"])

        fill_after_transition(time_rows, service_rows, base_time, interval_end, "ours", ours_sim, ours_target.result, tgt_demand)
        fill_after_transition(time_rows, service_rows, base_time, interval_end, "jormungandr", jorm_sim, jorm_target, tgt_demand)
        append_constant_interval(time_rows, service_rows, base_time, interval_end, "parvagpu_mig", parva_target, tgt_demand)

        runtime_rows.append(
            {
                "epoch": epoch,
                "method": "ours",
                "planner_makespan_sec": float(ours_target.stage1_sec + ours_target.stage2_sec + ours_stage3_sec),
            }
        )
        runtime_rows.append(
            {
                "epoch": epoch,
                "method": "jormungandr",
                "planner_makespan_sec": float((jorm_round.get("stage_runtime_sec") or {}).get("allocator_plus_deployer_sec", jorm_round.get("runtime_sec", 0.0))),
            }
        )

        for method, result in [
            ("ours", ours_target.result),
            ("jormungandr", jorm_target),
            ("parvagpu_mig", parva_target),
        ]:
            source = ours_prev.result if method == "ours" else (jorm_prev if method == "jormungandr" else parva_prev)
            sim = pq.allocation_similarity(source, result)
            similarity_rows.append(
                {
                    "epoch": epoch,
                    "method": method,
                    "partition_similarity": sim["partition_similarity"],
                    "exact_workload_similarity": sim["exact_workload_similarity"],
                }
            )
            unused_rows.append(
                {
                    "epoch": epoch,
                    "method": method,
                    "unused_slices": int(7 * result.gpu_count - result.allocated_slices),
                }
            )

        append_epoch_rows(epoch_rows, epoch, trace.iloc[epoch], ours_target.result, jorm_target, parva_target)
        ours_targets.append(ours_target)
        jorm_targets.append(jorm_target)
        parva_targets.append(parva_target)
        ours_prev = ours_target
        jorm_prev = jorm_target
        parva_prev = parva_target

    write_outputs(
        out_dir,
        trace_dir,
        epoch_rows,
        transition_rows,
        action_rows,
        time_rows,
        service_rows,
        runtime_rows,
        similarity_rows,
        unused_rows,
    )
    plot_all(out_dir)
    (RESULT_ROOT / "latest_path.txt").write_text(str(out_dir) + "\n", encoding="utf-8")
    print(f"ONLINE_3GPU_REPLAY_DIR={out_dir}")


def latest_online_trace_dir() -> Path:
    latest = TRACE_ROOT / "latest_path.txt"
    if latest.exists():
        path = Path(latest.read_text(encoding="utf-8").strip())
        if path.exists():
            return path
    candidates = sorted(TRACE_ROOT.glob("20*"))
    if not candidates:
        raise FileNotFoundError("No online_3gpu trace directory found")
    return candidates[-1]


def demand_from_row(row: pd.Series) -> dict[str, float]:
    return {workload: float(row[workload]) for workload in WORKLOADS}


def append_epoch_rows(rows: list[dict[str, Any]], epoch: int, trace_row: pd.Series, ours: Any, jorm: Any, parva: Any) -> None:
    for method, result in [("ours", ours), ("jormungandr", jorm), ("parvagpu_mig", parva)]:
        rows.append(
            {
                "epoch": epoch,
                "time_sec": float(epoch) * EPOCH_SECONDS,
                "hour": float(trace_row["hour"]),
                "method": method,
                "gpu_count": int(result.gpu_count),
                "allocated_slices": int(result.allocated_slices),
                "unused_slices": int(7 * result.gpu_count - result.allocated_slices),
            }
        )


def action_duration(action: dict[str, Any]) -> float:
    action_type = str(action.get("type"))
    if action_type == "wait_instance_drain":
        return ACTION_DURATION_SEC[action_type] * max(1, int(action.get("rounds", 1)))
    return float(ACTION_DURATION_SEC.get(action_type, 0.10))


def simulate_transition(
    *,
    method: str,
    source_alloc: Any,
    target_alloc: Any,
    target_state: Any | None,
    transition_plan: dict[str, Any],
    required: dict[str, float],
    base_time: float,
) -> dict[str, Any]:
    state = SimState(
        active=instances_from_allocation(source_alloc),
        pending={},
        materialized_gpus=set(gpu_ids_from_allocation(source_alloc)),
    )
    target_lookup = instances_from_state(target_state) if target_state is not None else instances_from_allocation(target_alloc)
    rows: list[dict[str, Any]] = []
    service_rows: list[dict[str, Any]] = []
    violation_union = 0.0
    violation_workload = 0.0
    peak_active_gpu = len(state.materialized_gpus)
    elapsed = 0.0

    if method == "ours":
        phases = list((transition_plan.get("phased_action_plan") or {}).get("phases", []))
        node_by_id = {str(node["id"]): node for node in (transition_plan.get("phased_action_plan") or {}).get("nodes", [])}
        for phase in phases:
            actions = [node_by_id[str(node_id)]["action"] for node_id in phase.get("nodeIds", []) if str(node_id) in node_by_id]
            duration = max([action_duration(action) for action in actions] or [0.0])
            violation_union, violation_workload = append_segment(
                rows, service_rows, state, required, method, base_time + elapsed, base_time + elapsed + duration, violation_union, violation_workload
            )
            for action in sorted(actions, key=lambda item: str(item.get("actionKey", item.get("type", "")))):
                apply_action(state, action, target_lookup, method)
            peak_active_gpu = max(peak_active_gpu, len(state.materialized_gpus))
            elapsed += duration
    else:
        for action in transition_plan.get("executed_actions", []):
            duration = action_duration(action)
            violation_union, violation_workload = append_segment(
                rows, service_rows, state, required, method, base_time + elapsed, base_time + elapsed + duration, violation_union, violation_workload
            )
            apply_action(state, action, target_lookup, method)
            peak_active_gpu = max(peak_active_gpu, len(state.materialized_gpus))
            elapsed += duration

    action_counts = pq.paper_action_counts(method, transition_plan.get("executed_actions", []))
    return {
        "method": method,
        "makespan_sec": elapsed,
        "slo_violation_duration_sec": violation_union,
        "slo_violation_workload_sec": violation_workload,
        "peak_active_gpu": peak_active_gpu,
        "final_active_gpu": int(target_alloc.gpu_count),
        "time_rows": rows,
        "service_rows": service_rows,
        "action_counts": action_counts,
    }


def append_segment(
    rows: list[dict[str, Any]],
    service_rows: list[dict[str, Any]],
    state: SimState,
    required: dict[str, float],
    method: str,
    start: float,
    end: float,
    violation_union: float,
    violation_workload: float,
) -> tuple[float, float]:
    duration = max(0.0, float(end) - float(start))
    if duration <= 0:
        return violation_union, violation_workload
    capacity = active_capacity(state)
    violating = False
    for workload in WORKLOADS:
        req = float(required.get(workload, 0.0))
        cap = float(capacity.get(workload, 0.0))
        if req > 1e-9 and cap + 1e-9 < req:
            violating = True
            violation_workload += duration
        service_rows.append(
            {
                "start_sec": start,
                "end_sec": end,
                "method": method,
                "workload": workload,
                "service_rate": cap,
                "demand_rate": req,
                "service_rate_ratio": cap / req if req > 1e-9 else 1.0,
            }
        )
    if violating:
        violation_union += duration
    rows.append(
        {
            "start_sec": start,
            "end_sec": end,
            "method": method,
            "active_gpu_count": len(state.materialized_gpus),
        }
    )
    return violation_union, violation_workload


def fill_after_transition(
    time_rows: list[dict[str, Any]],
    service_rows: list[dict[str, Any]],
    base_time: float,
    interval_end: float,
    method: str,
    sim: dict[str, Any],
    target_alloc: Any,
    demand: dict[str, float],
) -> None:
    start = base_time + float(sim["makespan_sec"])
    if start >= interval_end:
        return
    append_constant_interval(time_rows, service_rows, start, interval_end, method, target_alloc, demand)


def append_constant_interval(
    time_rows: list[dict[str, Any]],
    service_rows: list[dict[str, Any]],
    start: float,
    end: float,
    method: str,
    allocation: Any,
    demand: dict[str, float],
) -> None:
    active = instances_from_allocation(allocation)
    state = SimState(active=active, pending={}, materialized_gpus=set(gpu_ids_from_allocation(allocation)))
    append_segment(time_rows, service_rows, state, demand, method, start, end, 0.0, 0.0)


def active_capacity(state: SimState) -> dict[str, float]:
    out: dict[str, float] = defaultdict(float)
    for inst in state.active.values():
        workload = inst.get("workload")
        if workload:
            out[str(workload)] += float(inst.get("mu", 0.0))
    return dict(out)


def apply_action(state: SimState, action: dict[str, Any], target_lookup: dict[str, dict[str, Any]], method: str) -> None:
    action_type = str(action.get("type"))
    if action.get("gpu_id") is not None:
        gpu_id = int(action["gpu_id"])
    else:
        gpu_id = None

    if action_type in {"allocate_gpu", "create_mig_instance"} and gpu_id is not None:
        state.materialized_gpus.add(gpu_id)
        return
    if action_type in {"return_gpu", "return_extra_gpu"} and gpu_id is not None:
        remove_gpu_instances(state, gpu_id)
        state.materialized_gpus.discard(gpu_id)
        return
    if action_type == "clear_template" and gpu_id is not None:
        remove_gpu_instances(state, gpu_id)
        return
    if action_type == "repartition_gpu" and gpu_id is not None:
        remove_gpu_instances(state, gpu_id)
        state.materialized_gpus.add(gpu_id)
        return
    if action_type == "place_instance":
        inst = lookup_action_instance(action, target_lookup)
        if inst:
            state.pending[instance_key(inst)] = inst
        return
    if action_type == "activate_instance_route":
        inst = lookup_action_instance(action, target_lookup)
        if inst:
            key = instance_key(inst)
            state.active[key] = state.pending.pop(key, inst)
        return
    if action_type in {"deactivate_instance_route", "delete_instance"}:
        remove_action_slot(state, action)
        return
    if action_type == "create_container":
        inst = normalize_instance(action.get("instance"))
        if inst:
            state.active[instance_key(inst)] = inst
            state.materialized_gpus.add(int(inst["gpu_id"]))
        return
    if action_type == "delete_container":
        inst = normalize_instance(action.get("instance"))
        if inst:
            state.active.pop(instance_key(inst), None)
        return
    if action_type == "migrate_container":
        src = normalize_instance(action.get("from_instance"))
        tgt = normalize_instance(action.get("to_instance"))
        if src:
            state.active.pop(instance_key(src), None)
        if tgt:
            state.active[instance_key(tgt)] = tgt
            state.materialized_gpus.add(int(tgt["gpu_id"]))
        return


def lookup_action_instance(action: dict[str, Any], target_lookup: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    if action.get("instance"):
        return normalize_instance(action.get("instance"))
    if action.get("slot") is None or action.get("gpu_id") is None:
        return None
    slot = tuple(action["slot"][:3])
    key = slot_key(int(action["gpu_id"]), slot)
    inst = target_lookup.get(key)
    if inst is not None:
        return dict(inst)
    workload = action.get("workload")
    if workload is None:
        return None
    start, end, profile = slot
    return {
        "gpu_id": int(action["gpu_id"]),
        "start": int(start),
        "end": int(end),
        "profile": str(profile),
        "workload": str(workload),
        "batch": action.get("batch"),
        "mu": 0.0,
    }


def remove_action_slot(state: SimState, action: dict[str, Any]) -> None:
    if action.get("slot") is None or action.get("gpu_id") is None:
        return
    gpu_id = int(action["gpu_id"])
    start, end, profile = tuple(action["slot"][:3])
    for key in list(state.active.keys()):
        inst = state.active[key]
        if int(inst["gpu_id"]) == gpu_id and int(inst["start"]) == int(start) and int(inst["end"]) == int(end) and str(inst["profile"]) == str(profile):
            state.active.pop(key, None)
    for key in list(state.pending.keys()):
        inst = state.pending[key]
        if int(inst["gpu_id"]) == gpu_id and int(inst["start"]) == int(start) and int(inst["end"]) == int(end) and str(inst["profile"]) == str(profile):
            state.pending.pop(key, None)


def remove_gpu_instances(state: SimState, gpu_id: int) -> None:
    for store in [state.active, state.pending]:
        for key in list(store.keys()):
            if int(store[key]["gpu_id"]) == int(gpu_id):
                store.pop(key, None)


def instances_from_state(state: Any) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for gpu in state.real_gpus():
        for inst in gpu.instances:
            if getattr(inst, "workload", None):
                item = {
                    "gpu_id": int(gpu.gpu_id),
                    "start": int(inst.start),
                    "end": int(inst.end),
                    "profile": str(inst.profile),
                    "workload": str(inst.workload),
                    "batch": int(inst.batch) if inst.batch is not None else None,
                    "mu": float(inst.mu),
                }
                out[slot_key(item["gpu_id"], (item["start"], item["end"], item["profile"]))] = item
    return out


def instances_from_allocation(allocation: Any) -> dict[str, dict[str, Any]]:
    data = allocation.to_dict() if hasattr(allocation, "to_dict") else allocation
    out: dict[str, dict[str, Any]] = {}
    for gpu in data.get("gpus", []):
        for inst in gpu.get("instances", []):
            if inst.get("workload") is None:
                continue
            item = normalize_instance({**inst, "gpu_id": gpu["gpu_id"]})
            if item:
                out[instance_key(item)] = item
    return out


def gpu_ids_from_allocation(allocation: Any) -> list[int]:
    data = allocation.to_dict() if hasattr(allocation, "to_dict") else allocation
    return [int(gpu["gpu_id"]) for gpu in data.get("gpus", [])]


def normalize_instance(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict) or raw.get("workload") is None:
        return None
    return {
        "gpu_id": int(raw["gpu_id"]),
        "start": int(raw["start"]),
        "end": int(raw["end"]),
        "profile": str(raw["profile"]),
        "workload": str(raw["workload"]),
        "batch": int(raw["batch"]) if raw.get("batch") is not None else None,
        "mu": float(raw.get("mu", 0.0)),
    }


def instance_key(inst: dict[str, Any]) -> str:
    return f"{inst['gpu_id']}:{inst['start']}:{inst['end']}:{inst['profile']}:{inst['workload']}:{inst.get('batch')}"


def slot_key(gpu_id: int, slot: tuple[Any, Any, Any]) -> str:
    return f"{int(gpu_id)}:{int(slot[0])}:{int(slot[1])}:{slot[2]}"


def write_outputs(
    out_dir: Path,
    trace_dir: Path,
    epoch_rows: list[dict[str, Any]],
    transition_rows: list[dict[str, Any]],
    action_rows: list[dict[str, Any]],
    time_rows: list[dict[str, Any]],
    service_rows: list[dict[str, Any]],
    runtime_rows: list[dict[str, Any]],
    similarity_rows: list[dict[str, Any]],
    unused_rows: list[dict[str, Any]],
) -> None:
    pd.DataFrame(epoch_rows).to_csv(out_dir / "epoch_allocations.csv", index=False)
    pd.DataFrame(transition_rows).to_csv(out_dir / "transition_metrics.csv", index=False)
    pd.DataFrame(action_rows).to_csv(out_dir / "transition_actions.csv", index=False)
    pd.DataFrame(time_rows).to_csv(out_dir / "active_gpu_timeline.csv", index=False)
    pd.DataFrame(service_rows).to_csv(out_dir / "service_rate_timeline.csv", index=False)
    pd.DataFrame(runtime_rows).to_csv(out_dir / "planner_makespan.csv", index=False)
    pd.DataFrame(similarity_rows).to_csv(out_dir / "allocation_similarity.csv", index=False)
    pd.DataFrame(unused_rows).to_csv(out_dir / "unused_slices.csv", index=False)
    (out_dir / "source_trace_dir.txt").write_text(str(trace_dir) + "\n", encoding="utf-8")


def plot_all(out_dir: Path) -> None:
    global PLOT_EPOCH_COUNT
    os.environ.setdefault("MPLCONFIGDIR", str(REPO_ROOT / ".matplotlib-cache"))
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch
    from matplotlib.ticker import PercentFormatter

    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 10,
            "axes.labelsize": 10,
            "legend.fontsize": 8,
            "xtick.labelsize": 9,
            "ytick.labelsize": 9,
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
        }
    )

    transitions = pd.read_csv(out_dir / "transition_metrics.csv")
    epochs = pd.read_csv(out_dir / "epoch_allocations.csv")
    runtime = pd.read_csv(out_dir / "planner_makespan.csv")
    time_df = pd.read_csv(out_dir / "active_gpu_timeline.csv")
    service = pd.read_csv(out_dir / "service_rate_timeline.csv")
    similarity = pd.read_csv(out_dir / "allocation_similarity.csv")
    unused = pd.read_csv(out_dir / "unused_slices.csv")
    actions = pd.read_csv(out_dir / "transition_actions.csv")
    PLOT_EPOCH_COUNT = infer_plot_epoch_count(epochs, transitions, service)

    boxplot_metric(transitions, "makespan_sec", "Transition makespan (s)", FIGURE_ROOT / f"{FIGURE_PREFIX}_transition_makespan")
    boxplot_metric(transitions, "slo_violation_duration_sec", "SLO violation duration (s)", FIGURE_ROOT / f"{FIGURE_PREFIX}_slo_violation")
    extra_gpu = compute_extra_gpu(transitions, epochs)
    extra_gpu.to_csv(out_dir / "transition_extra_gpu.csv", index=False)
    boxplot_metric(extra_gpu, "extra_gpu", "Extra GPUs", FIGURE_ROOT / f"{FIGURE_PREFIX}_extra_gpu", methods=["ours", "jormungandr"])
    boxplot_metric(runtime, "planner_makespan_sec", "Planner makespan (s)", FIGURE_ROOT / f"{FIGURE_PREFIX}_planner_makespan")
    boxplot_metric(
        similarity,
        "exact_workload_similarity",
        "Same-placement rate",
        FIGURE_ROOT / f"{FIGURE_PREFIX}_allocation_similarity",
        percent=True,
        methods=["ours", "jormungandr"],
    )
    boxplot_metric(
        unused,
        "unused_slices",
        "Non-serving MIG slices",
        FIGURE_ROOT / f"{FIGURE_PREFIX}_unused_slices",
        methods=["ours", "jormungandr"],
    )
    plot_active_gpu(epochs, transitions, FIGURE_ROOT / f"{FIGURE_PREFIX}_active_gpu_count")
    plot_service_rate_min_ratio(service, transitions, FIGURE_ROOT / f"{FIGURE_PREFIX}_service_rate_min_ratio")
    plot_service_rate_by_workload(service, transitions, FIGURE_ROOT)
    plot_transition_actions(actions, FIGURE_ROOT / f"{FIGURE_PREFIX}_transition_actions")


def infer_plot_epoch_count(epochs: pd.DataFrame, transitions: pd.DataFrame, service: pd.DataFrame) -> int:
    candidates = [0]
    if not epochs.empty:
        candidates.append(int(epochs["epoch"].max()) + 1)
    if not transitions.empty:
        candidates.append(int(transitions["epoch"].max()) + 1)
    if not service.empty:
        candidates.append(int(math.ceil(float(service["end_sec"].max()) / EPOCH_SECONDS)))
    return max(candidates)


def compute_extra_gpu(transitions: pd.DataFrame, epochs: pd.DataFrame) -> pd.DataFrame:
    rows = []
    counts = {
        (str(row["method"]), int(row["epoch"])): int(row["gpu_count"])
        for _, row in epochs.iterrows()
    }
    for _, row in transitions.iterrows():
        method = str(row["method"])
        epoch = int(row["epoch"])
        current_count = counts.get((method, epoch - 1), 0)
        target_count = counts.get((method, epoch), 0)
        base_count = max(current_count, target_count)
        peak = int(row["peak_active_gpu"])
        rows.append(
            {
                "epoch": epoch,
                "method": method,
                "current_gpu_count": current_count,
                "target_gpu_count": target_count,
                "base_gpu_count": base_count,
                "peak_active_gpu": peak,
                "extra_gpu": max(0, peak - base_count),
            }
        )
    return pd.DataFrame(rows)


def boxplot_metric(
    df: pd.DataFrame,
    value: str,
    ylabel: str,
    path: Path,
    percent: bool = False,
    methods: list[str] | None = None,
) -> None:
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter

    method_order = methods or ["ours", "jormungandr", "parvagpu_mig"]
    methods = [method for method in method_order if method in set(df["method"])]
    data = [df[df["method"] == method][value].to_numpy(dtype=float) for method in methods]
    fig, ax = plt.subplots(figsize=(3.3, 2.35))
    bp = ax.boxplot(data, patch_artist=True, widths=0.55, showfliers=False)
    for patch, method in zip(bp["boxes"], methods, strict=False):
        patch.set_facecolor(METHOD_COLORS[method])
        patch.set_alpha(0.78)
        patch.set_edgecolor("black")
        patch.set_linewidth(0.8)
    for key in ["whiskers", "caps", "medians"]:
        for artist in bp[key]:
            artist.set_color("black")
            artist.set_linewidth(0.8)
    ax.set_xticks(range(1, len(methods) + 1))
    ax.set_xticklabels([METHOD_LABELS[method] for method in methods])
    ax.set_ylabel(ylabel)
    if percent:
        ax.set_ylim(0, 1.05)
        ax.yaxis.set_major_formatter(PercentFormatter(xmax=1.0, decimals=0))
    ax.grid(axis="y", color="#D9DEE8", linewidth=0.55)
    ax.set_axisbelow(True)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    fig.tight_layout(pad=0.25)
    save_fig(fig, path)


def transition_spans_for_plot(transitions: pd.DataFrame, method: str | None = None) -> list[tuple[float, float]]:
    if method is not None:
        source = transitions[transitions["method"] == method]
        rows = [
            (int(row["epoch"]), float(row["makespan_sec"]))
            for _, row in source.iterrows()
        ]
    else:
        source = transitions[transitions["method"].isin(["ours", "jormungandr"])]
        rows = [
            (int(epoch), float(group["makespan_sec"].max()))
            for epoch, group in source.groupby("epoch")
        ]
    spans = []
    for epoch, makespan in rows:
        start = epoch * EPOCH_SECONDS
        end = start + makespan
        c_start = compressed_time(start)
        c_end = compressed_time(end)
        if c_end > c_start:
            spans.append((c_start, c_end))
    return spans


def add_transition_background(ax: Any, spans: list[tuple[float, float]]) -> None:
    for start, end in spans:
        ax.axvspan(start, end, color="#DDE7FF", alpha=0.70, linewidth=0, zorder=0)


def plot_active_gpu(epochs: pd.DataFrame, transitions: pd.DataFrame, path: Path) -> None:
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(6.2, 2.55))
    add_transition_background(ax, transition_spans_for_plot(transitions))
    for method in ["jormungandr", "ours"]:
        epoch_counts = (
            epochs[epochs["method"] == method]
            .sort_values("epoch")
            .set_index("epoch")["gpu_count"]
            .to_dict()
        )
        trans = transitions[transitions["method"] == method].set_index("epoch")
        if not epoch_counts:
            continue
        xs: list[float] = []
        ys: list[float] = []
        # Epoch 0 has no transition from a previous allocation.
        xs.extend([compressed_time(0.0), compressed_time(EPOCH_SECONDS)])
        ys.extend([float(epoch_counts.get(0, 0)), float(epoch_counts.get(0, 0))])
        for epoch in range(1, PLOT_EPOCH_COUNT):
            start = epoch * EPOCH_SECONDS
            end = (epoch + 1) * EPOCH_SECONDS
            target_count = float(epoch_counts.get(epoch, epoch_counts.get(epoch - 1, 0)))
            if epoch in trans.index:
                row = trans.loc[epoch]
                makespan = float(row["makespan_sec"])
                peak = float(row["peak_active_gpu"])
                xs.extend([compressed_time(start), compressed_time(start + makespan)])
                ys.extend([peak, peak])
                xs.extend([compressed_time(start + makespan), compressed_time(end)])
                ys.extend([target_count, target_count])
            else:
                xs.extend([compressed_time(start), compressed_time(end)])
                ys.extend([target_count, target_count])
        ax.plot(
            xs,
            ys,
            color=METHOD_COLORS[method],
            linewidth=1.7,
            linestyle="-",
            label=METHOD_LABELS[method],
            zorder={"jormungandr": 2, "ours": 3}[method],
        )
    ax.set_xlim(0, plot_end_time())
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Active GPU count")
    ax.grid(axis="y", color="#D9DEE8", linewidth=0.55)
    ax.grid(axis="x", color="#EEF2F7", linewidth=0.45)
    handles, labels = ax.get_legend_handles_labels()
    transition_patch = plt.Rectangle((0, 0), 1, 1, color="#DDE7FF", alpha=0.70, label="Transition")
    ax.legend(handles=[*handles, transition_patch], frameon=False, ncol=3, loc="upper left", bbox_to_anchor=(0.01, 0.99), borderaxespad=0.0)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    fig.tight_layout(pad=0.25)
    save_fig(fig, path)


def plot_service_rate_min_ratio(df: pd.DataFrame, transitions: pd.DataFrame, path: Path) -> None:
    import matplotlib.pyplot as plt

    sub = df[df["method"].isin(["ours", "jormungandr"])].copy()
    grouped = (
        sub.groupby(["method", "start_sec", "end_sec"], as_index=False)["service_rate_ratio"]
        .min()
        .sort_values(["method", "start_sec"])
    )
    fig, ax = plt.subplots(figsize=(6.2, 2.75))
    add_transition_background(ax, transition_spans_for_plot(transitions))
    for method in ["ours", "jormungandr"]:
        mdf = grouped[grouped["method"] == method].sort_values("start_sec")
        xs: list[float] = []
        ys: list[float] = []
        for _, row in mdf.iterrows():
            ratio = min(float(row["service_rate_ratio"]), 2.5)
            xs.extend([compressed_time(float(row["start_sec"])), compressed_time(float(row["end_sec"]))])
            ys.extend([ratio, ratio])
        ax.plot(xs, ys, color=METHOD_COLORS[method], linewidth=1.6, label=METHOD_LABELS[method])
    ax.axhline(1.0, color="#2A7F2E", linestyle="--", linewidth=0.9, label="Demand")
    ax.set_xlim(0, plot_end_time())
    ax.set_ylim(0, 2.55)
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Min service / demand")
    ax.grid(axis="y", color="#D9DEE8", linewidth=0.55)
    ax.grid(axis="x", color="#EEF2F7", linewidth=0.45)
    handles, labels = ax.get_legend_handles_labels()
    transition_patch = plt.Rectangle((0, 0), 1, 1, color="#DDE7FF", alpha=0.70, label="Transition")
    ax.legend(handles=[*handles, transition_patch], frameon=False, ncol=4, loc="upper left", bbox_to_anchor=(0.01, 0.99), borderaxespad=0.0)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    fig.tight_layout(pad=0.25)
    save_fig(fig, path)


def compressed_time(actual_sec: float) -> float:
    epoch = int(math.floor(float(actual_sec) / EPOCH_SECONDS))
    if PLOT_EPOCH_COUNT and epoch >= PLOT_EPOCH_COUNT:
        return plot_end_time()
    offset = float(actual_sec) - epoch * EPOCH_SECONDS
    return epoch * DISPLAY_EPOCH_SECONDS + min(offset, DISPLAY_EPOCH_SECONDS)


def plot_end_time() -> float:
    return float(PLOT_EPOCH_COUNT) * DISPLAY_EPOCH_SECONDS


def plot_service_rate_by_workload(df: pd.DataFrame, transitions: pd.DataFrame, figure_root: Path) -> None:
    import matplotlib.pyplot as plt

    sub = df[df["method"] == "ours"].copy()
    transition_spans = transition_spans_for_plot(transitions)
    for workload in WORKLOADS:
        wdf = sub[sub["workload"] == workload].sort_values("start_sec")
        fig, ax = plt.subplots(figsize=(6.2, 2.45))
        add_transition_background(ax, transition_spans)
        xs: list[float] = []
        service: list[float] = []
        demand: list[float] = []
        for _, row in wdf.iterrows():
            xs.extend([compressed_time(float(row["start_sec"])), compressed_time(float(row["end_sec"]))])
            service.extend([float(row["service_rate"]), float(row["service_rate"])])
            demand.extend([float(row["demand_rate"]), float(row["demand_rate"])])
        ax.plot(xs, service, color=WORKLOAD_COLORS[workload], linewidth=1.6, label="Active capacity")
        ax.plot(xs, demand, color="#2A7F2E", linewidth=1.0, linestyle="--", label="Demand rate")
        ax.set_xlim(0, plot_end_time())
        ymax = max(max(service or [0.0]), max(demand or [0.0]))
        ax.set_ylim(0, ymax * 1.12 if ymax > 0 else 1.0)
        ax.set_xlabel("Time (s)")
        ax.set_ylabel("Request rate (req/s)")
        ax.set_title(WORKLOAD_LABELS[workload], pad=2)
        ax.grid(axis="y", color="#D9DEE8", linewidth=0.55)
        ax.grid(axis="x", color="#EEF2F7", linewidth=0.45)
        handles, labels = ax.get_legend_handles_labels()
        transition_patch = plt.Rectangle((0, 0), 1, 1, color="#DDE7FF", alpha=0.70, label="Transition")
        ax.legend(
            handles=[*handles, transition_patch],
            frameon=False,
            ncol=3,
            loc="upper left",
            bbox_to_anchor=(0.01, 0.99),
            borderaxespad=0.0,
        )
        for spine in ("top", "right"):
            ax.spines[spine].set_visible(False)
        fig.tight_layout(pad=0.25)
        save_fig(fig, figure_root / f"{FIGURE_PREFIX}_service_rate_{workload}")


def plot_transition_actions(df: pd.DataFrame, path: Path) -> None:
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch

    components = [
        ("created_pods", "Create pod", "#66BC98"),
        ("deleted_pods", "Delete pod", "#AAD09D"),
        ("created_mig_instances", "Create MIG", "#E3EA96"),
        ("deleted_mig_instances", "Delete MIG", "#FCDCB9"),
    ]
    grouped = df.groupby("method", as_index=False)[[name for name, _, _ in components]].sum()
    methods = [method for method in ["ours", "jormungandr"] if method in set(grouped["method"])]
    x = np.arange(len(methods))
    width = 0.50
    fig, ax = plt.subplots(figsize=(3.2, 2.55))
    for idx, method in enumerate(methods):
        bottom = 0.0
        row = grouped[grouped["method"] == method].iloc[0]
        for value, _, color in components:
            ax.bar(idx, float(row[value]), width=width, bottom=bottom, color=color, edgecolor="black", linewidth=0.35)
            bottom += float(row[value])
    ax.set_xticks(x)
    ax.set_xticklabels([METHOD_LABELS[method] for method in methods])
    ax.set_ylabel("Transition actions")
    ax.grid(axis="y", color="#D9DEE8", linewidth=0.55)
    ax.set_axisbelow(True)
    handles = [Patch(facecolor=color, edgecolor="black", linewidth=0.35, label=label) for _, label, color in components]
    ax.legend(handles=handles, ncol=2, frameon=False, loc="upper left", bbox_to_anchor=(0.01, 0.99), borderaxespad=0.0)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    fig.tight_layout(pad=0.25)
    save_fig(fig, path)


def save_fig(fig: Any, path: Path) -> None:
    import matplotlib.pyplot as plt

    fig.savefig(path.with_suffix(".pdf"), bbox_inches="tight")
    fig.savefig(path.with_suffix(".png"), bbox_inches="tight", dpi=240)
    plt.close(fig)


if __name__ == "__main__":
    main()
