#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
PLANNER_APP = REPO_ROOT / "k8s-extension-go" / "planner-engine" / "app"
sys.path.insert(0, str(PLANNER_APP))

from migrant_core.state import GPUState, MigInstance  # noqa: E402
from migrant_core.target_materializer.templates import (  # noqa: E402
    all_unique_physical_realizations,
    template_name_list,
)
from migrant_core.transition_planner.internal.partial_reconfig import (  # noqa: E402
    agent_slot_spec,
    build_partial_reconfig_plan,
)


ALL_TEMPLATES = template_name_list()
DEFAULT_TEMPLATES = [
    "7",
    "4+3",
    "4+2+1",
    "4+1+1+1",
    "3+3",
    "3+2+1",
    "3+1+1+1",
    "2+2+3",
    "3+2+1+1",
    "3+1+1+1+1",
    "2+2+2+1",
    "2+2+1+1+1",
    "2+1+1+1+1+1",
    "1+1+1+1+1+1+1",
]
DEFAULT_PROFILES = ["1g", "2g", "3g", "4g", "7g"]
PROFILE_SLOT = {
    "1g": (0, 1, "1g"),
    "2g": (0, 2, "2g"),
    "3g": (0, 4, "3g"),
    "4g": (0, 4, "4g"),
    "7g": (0, 8, "7g"),
}
WORKLOADS = {
    "resnet50": {"model": "resnet50", "runtimeModel": "resnet50", "requestClass": "image-batches", "batchSize": 32},
    "vgg16": {"model": "vgg16", "runtimeModel": "vgg16", "requestClass": "image-batches", "batchSize": 32},
    "vit_base": {"model": "vit_base", "runtimeModel": "vit_base", "requestClass": "image-batches", "batchSize": 32},
    "gpt2_p64_o64": {
        "model": "gpt2_p64_o64",
        "runtimeModel": "gpt2",
        "requestClass": "p64/o64",
        "promptLen": 64,
        "outputTokens": 64,
        "batchSize": 1,
    },
    "gpt2_p512_o512": {
        "model": "gpt2_p512_o512",
        "runtimeModel": "gpt2",
        "requestClass": "p512/o512",
        "promptLen": 512,
        "outputTokens": 512,
        "batchSize": 1,
    },
    "llama_p1024_o128": {
        "model": "llama_p1024_o128",
        "runtimeModel": "llama",
        "requestClass": "p1024/o128",
        "promptLen": 1024,
        "outputTokens": 128,
        "batchSize": 1,
    },
    "llama_p2048_o64": {
        "model": "llama_p2048_o64",
        "runtimeModel": "llama",
        "requestClass": "p2048/o64",
        "promptLen": 2048,
        "outputTokens": 64,
        "batchSize": 1,
    },
    "llama_p4096_o512": {
        "model": "llama_p4096_o512",
        "runtimeModel": "llama",
        "requestClass": "p4096/o512",
        "promptLen": 4096,
        "outputTokens": 512,
        "batchSize": 1,
    },
}
INSTANCE_PROFILES = ["1g", "2g", "3g", "4g", "7g"]
RUNTIME_HOST_PORT_POOL = tuple(port for port in range(10681, 10721) if port not in {10684, 10690})


@dataclass(frozen=True)
class PhysicalGPU:
    physical_id: str
    node: str
    gpu_index: int


@dataclass(frozen=True)
class BenchCase:
    case_id: str
    action_type: str
    executor_action: str
    profile: str = ""
    template: str = ""
    source_template: str = ""
    target_template: str = ""
    workload: str = ""
    runtime_model: str = ""
    request_class: str = ""
    slot: tuple[int, int, str] | None = None
    create_spec: str = ""
    delete_spec: str = ""
    preserve_spec: str = ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run action-level latency microbenchmarks through MigActionPlan.")
    parser.add_argument("--namespace", default="or-sim-exp")
    parser.add_argument("--run-id", default="action-latency-" + time.strftime("%Y%m%d-%H%M%S"))
    parser.add_argument("--out-dir", default="")
    parser.add_argument("--suite", choices=["all", "mig-only", "instance-only", "route-only"], default="all")
    parser.add_argument("--trials", type=int, default=20)
    parser.add_argument("--llm-trials", type=int, default=5)
    parser.add_argument("--execute", action="store_true", help="Actually mutate the cluster. Without this, only writes the matrix.")
    parser.add_argument("--physical-gpu", default="", help="Physical GPU id to use, e.g. ampere-gpu0. Default: first available.")
    parser.add_argument("--profiles", default=",".join(DEFAULT_PROFILES))
    parser.add_argument("--instance-profiles", default=",".join(INSTANCE_PROFILES))
    parser.add_argument("--templates", default=",".join(DEFAULT_TEMPLATES))
    parser.add_argument("--workloads", default=",".join(WORKLOADS))
    parser.add_argument("--skip-partial", action="store_true")
    parser.add_argument("--max-partial-cases", type=int, default=0, help="0 means all generated partial cases.")
    parser.add_argument("--shard-count", type=int, default=1, help="Split matrix into N deterministic shards.")
    parser.add_argument("--shard-index", type=int, default=0, help="Run shard index in [0, shard-count).")
    parser.add_argument("--resume", action="store_true", help="Skip case/trial pairs already present as successful rows in action_latency_microbench.csv.")
    parser.add_argument("--continue-on-failure", action="store_true", help="Record failed case/trial rows and continue with the next trial.")
    parser.add_argument("--timeout-s", type=float, default=1200.0)
    parser.add_argument("--poll-s", type=float, default=2.0)
    parser.add_argument("--keep-plans", action="store_true")
    parser.add_argument("--no-cleanup", action="store_true", help="Do not cleanup after each case; only for debugging failed cases.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.shard_count < 1:
        raise ValueError("--shard-count must be >= 1")
    if args.shard_index < 0 or args.shard_index >= args.shard_count:
        raise ValueError("--shard-index must be in [0, shard-count)")
    out_dir = Path(args.out_dir or (REPO_ROOT / "eval" / "action_latency_microbench" / "results" / args.run_id))
    out_dir.mkdir(parents=True, exist_ok=True)
    matrix = build_matrix(args)
    write_matrix(out_dir / "action_latency_matrix.csv", matrix)
    (out_dir / "action_latency_matrix.json").write_text(json.dumps([case_to_row(c) for c in matrix], indent=2), encoding="utf-8")
    if not args.execute:
        print(f"wrote matrix only: {out_dir}")
        print("add --execute to run the benchmark against the cluster")
        return 0

    gpu = select_physical_gpu(args)
    print(f"using physical GPU: {gpu.physical_id} on {gpu.node} gpu{gpu.gpu_index}")
    result_path = out_dir / "action_latency_microbench.csv"
    rows: list[dict[str, Any]] = read_result_rows(result_path) if args.resume else []
    completed = completed_trials(rows) if args.resume else {}
    if args.resume and rows:
        print(f"resume: loaded {len(rows)} existing rows from {result_path}")
    try:
        cleanup_gpu(args, gpu, "initial-cleanup")
        for case in matrix:
            trial_count = trial_count_for_case(args, case)
            for trial in range(1, trial_count + 1):
                if trial in completed.get(case.case_id, set()):
                    print(f"=== {case.case_id} trial {trial}/{trial_count} skipped by --resume ===", flush=True)
                    continue
                print(f"=== {case.case_id} trial {trial}/{trial_count} ===", flush=True)
                try:
                    row = run_case(args, gpu, case, trial)
                    rows.extend(row)
                    append_rows(result_path, row)
                except Exception as exc:
                    fail_row = case_to_row(case)
                    fail_row.update({
                        "trial": trial,
                        "success": False,
                        "error": str(exc),
                        "physical_gpu": gpu.physical_id,
                        "node": gpu.node,
                    })
                    rows.append(fail_row)
                    append_rows(result_path, [fail_row])
                    if not args.continue_on_failure:
                        raise
                    print(f"warning: {case.case_id} trial {trial} failed and will be skipped: {exc}", file=sys.stderr, flush=True)
                finally:
                    if not args.no_cleanup:
                        cleanup_gpu(args, gpu, f"cleanup-{case.case_id}-t{trial}")
        write_summary(out_dir / "action_latency_summary.csv", rows)
    finally:
        cleanup_gpu(args, gpu, "final-cleanup")
    print(f"wrote benchmark results: {out_dir}")
    return 0


def build_matrix(args: argparse.Namespace) -> list[BenchCase]:
    profiles = split_csv(args.profiles)
    instance_profiles = split_csv(args.instance_profiles)
    templates = split_csv(args.templates)
    workloads = split_csv(args.workloads)
    cases: list[BenchCase] = []

    if args.suite in {"all", "mig-only"}:
        for profile in profiles:
            slot = PROFILE_SLOT[profile]
            spec = agent_slot_spec([slot])
            cases.append(BenchCase(
                case_id=f"profile-create-{profile}",
                action_type="create_mig",
                executor_action="configure_full_template",
                profile=profile,
                slot=slot,
                create_spec=spec,
            ))
            cases.append(BenchCase(
                case_id=f"profile-delete-{profile}",
                action_type="delete_mig",
                executor_action="patch_slots",
                profile=profile,
                slot=slot,
                delete_spec=spec,
            ))
        for template in templates:
            slots = slots_for_template(template)
            cases.append(BenchCase(
                case_id=f"full-template-{sanitize(template)}",
                action_type="configure_full_template",
                executor_action="configure_full_template",
                template=template,
                create_spec=agent_slot_spec(slots),
            ))
            cases.append(BenchCase(
                case_id=f"clear-template-{sanitize(template)}",
                action_type="clear_template",
                executor_action="clear_template",
                template=template,
                create_spec=agent_slot_spec(slots),
            ))
        if not args.skip_partial:
            partials = partial_cases(templates)
            if args.max_partial_cases > 0:
                partials = partials[: args.max_partial_cases]
            cases.extend(partials)

    if args.suite in {"all", "instance-only", "route-only"}:
        for workload in workloads:
            if workload not in WORKLOADS:
                raise ValueError(f"unknown workload {workload}; valid: {sorted(WORKLOADS)}")
            for profile in instance_profiles:
                if args.suite in {"all", "instance-only"}:
                    cases.append(BenchCase(
                        case_id=f"create-instance-{workload}-{profile}",
                        action_type="create_instance",
                        executor_action="place_instance+activate_instance_route",
                        workload=workload,
                        runtime_model=WORKLOADS[workload]["runtimeModel"],
                        request_class=WORKLOADS[workload]["requestClass"],
                        profile=profile,
                        slot=PROFILE_SLOT[profile],
                    ))
                    cases.append(BenchCase(
                        case_id=f"delete-instance-{workload}-{profile}",
                        action_type="delete_instance",
                        executor_action="deactivate_instance_route+wait_instance_drain+delete_instance",
                        workload=workload,
                        runtime_model=WORKLOADS[workload]["runtimeModel"],
                        request_class=WORKLOADS[workload]["requestClass"],
                        profile=profile,
                        slot=PROFILE_SLOT[profile],
                    ))
                if args.suite in {"all", "route-only"}:
                    cases.append(BenchCase(
                        case_id=f"route-activate-{workload}-{profile}",
                        action_type="route_activate",
                        executor_action="activate_instance_route",
                        workload=workload,
                        runtime_model=WORKLOADS[workload]["runtimeModel"],
                        request_class=WORKLOADS[workload]["requestClass"],
                        profile=profile,
                        slot=PROFILE_SLOT[profile],
                    ))
                    cases.append(BenchCase(
                        case_id=f"route-deactivate-drain-{workload}-{profile}",
                        action_type="route_deactivate_drain",
                        executor_action="deactivate_instance_route+wait_instance_drain",
                        workload=workload,
                        runtime_model=WORKLOADS[workload]["runtimeModel"],
                        request_class=WORKLOADS[workload]["requestClass"],
                        profile=profile,
                        slot=PROFILE_SLOT[profile],
                    ))
    if args.shard_count > 1:
        cases = [case for idx, case in enumerate(cases) if idx % args.shard_count == args.shard_index]
    return cases


def partial_cases(templates: list[str]) -> list[BenchCase]:
    out: list[BenchCase] = []
    for src in templates:
        for tgt in templates:
            if src == tgt:
                continue
            best = None
            for _, src_intervals in all_unique_physical_realizations(src):
                src_gpu = gpu_from_intervals(src_intervals)
                for _, tgt_intervals in all_unique_physical_realizations(tgt):
                    tgt_gpu = gpu_from_intervals(tgt_intervals)
                    plan = build_partial_reconfig_plan(src_gpu, tgt_gpu)
                    if plan is not None:
                        best = plan
                        break
                if best is not None:
                    break
            if best is None:
                continue
            fields = best.to_action_fields()
            out.append(BenchCase(
                case_id=f"partial-{sanitize(src)}-to-{sanitize(tgt)}",
                action_type="partial_reconfig",
                executor_action="patch_slots",
                source_template=src,
                target_template=tgt,
                create_spec=fields["createSpec"],
                delete_spec=fields["deleteSpec"],
                preserve_spec=fields["preserveSpec"],
            ))
    return out


def run_case(args: argparse.Namespace, gpu: PhysicalGPU, case: BenchCase, trial: int) -> list[dict[str, Any]]:
    if case.action_type == "create_mig":
        cleanup_gpu(args, gpu, f"setup-{case.case_id}-{trial}")
        slots = slots_from_spec(case.create_spec)
        actions = [action_configure(gpu, case.create_spec, case), action_register(gpu, slots, case)]
        plan = action_plan(args, f"{case.case_id}-t{trial}", case, actions, runtimes=register_runtimes_for_slots(gpu, slots))
        rows = execute_and_rows(args, case, trial, plan)
        rows.extend(composite_row(case, trial, rows, "create_mig_ready", ["configure_full_template", "register_mig_devices"]))
        return rows
    if case.action_type == "delete_mig":
        setup_slots(args, gpu, case.delete_spec, f"setup-{case.case_id}-{trial}")
        delete_spec = add_mig_uuids_to_spec(args, gpu, case.delete_spec)
        plan = one_action_plan(args, gpu, case, action_patch(gpu, delete_spec, "", "", case))
        return execute_and_rows(args, case, trial, plan)
    if case.action_type == "configure_full_template":
        cleanup_gpu(args, gpu, f"setup-{case.case_id}-{trial}")
        slots = slots_from_spec(case.create_spec)
        actions = [action_configure(gpu, case.create_spec, case), action_register(gpu, slots, case)]
        plan = action_plan(args, f"{case.case_id}-t{trial}", case, actions, runtimes=register_runtimes_for_slots(gpu, slots))
        rows = execute_and_rows(args, case, trial, plan)
        rows.extend(composite_row(case, trial, rows, "configure_full_template_ready", ["configure_full_template", "register_mig_devices"]))
        return rows
    if case.action_type == "clear_template":
        setup_slots(args, gpu, case.create_spec, f"setup-{case.case_id}-{trial}")
        plan = one_action_plan(args, gpu, case, action_clear(gpu, case))
        return execute_and_rows(args, case, trial, plan)
    if case.action_type == "partial_reconfig":
        setup_slots(args, gpu, partial_source_spec(case), f"setup-{case.case_id}-{trial}")
        delete_spec = add_mig_uuids_to_spec(args, gpu, case.delete_spec)
        preserve_spec = add_mig_uuids_to_spec(args, gpu, case.preserve_spec)
        target_slots = slots_from_spec(case.preserve_spec) + slots_from_spec(case.create_spec)
        actions = [action_patch(gpu, delete_spec, case.create_spec, preserve_spec, case), action_register(gpu, target_slots, case)]
        plan = action_plan(args, f"{case.case_id}-t{trial}", case, actions, runtimes=register_runtimes_for_slots(gpu, target_slots))
        rows = execute_and_rows(args, case, trial, plan)
        rows.extend(composite_row(case, trial, rows, "partial_reconfig_ready", ["patch_slots", "register_mig_devices"]))
        return rows
    if case.action_type == "create_instance":
        setup_slots(args, gpu, agent_slot_spec([case.slot]), f"setup-{case.case_id}-{trial}")
        runtime = runtime_for_case(args, gpu, case, trial)
        actions = [
            action_place(gpu, case, runtime),
            action_activate(gpu, case, runtime),
        ]
        plan = action_plan(args, f"{case.case_id}-t{trial}", case, actions, runtimes=[runtime])
        rows = execute_and_rows(args, case, trial, plan)
        rows.extend(composite_row(case, trial, rows, "create_serving_instance", ["place_instance", "activate_instance_route"]))
        return rows
    if case.action_type == "delete_instance":
        runtime = setup_instance(args, gpu, case, trial, f"setup-{case.case_id}-{trial}")
        actions = [
            action_deactivate(gpu, case, runtime),
            action_wait_drain(gpu, case, runtime),
            action_delete_instance(gpu, case, runtime),
        ]
        plan = action_plan(args, f"{case.case_id}-t{trial}", case, actions, runtimes=[runtime])
        rows = execute_and_rows(args, case, trial, plan)
        rows.extend(composite_row(case, trial, rows, "delete_serving_instance", ["deactivate_instance_route", "wait_instance_drain", "delete_instance"]))
        return rows
    if case.action_type == "route_activate":
        runtime = setup_instance_without_route(args, gpu, case, trial, f"setup-{case.case_id}-{trial}")
        plan = one_action_plan(args, gpu, case, action_activate(gpu, case, runtime), runtimes=[runtime])
        return execute_and_rows(args, case, trial, plan)
    if case.action_type == "route_deactivate_drain":
        runtime = setup_instance(args, gpu, case, trial, f"setup-{case.case_id}-{trial}")
        actions = [action_deactivate(gpu, case, runtime), action_wait_drain(gpu, case, runtime)]
        plan = action_plan(args, f"{case.case_id}-t{trial}", case, actions, runtimes=[runtime])
        rows = execute_and_rows(args, case, trial, plan)
        rows.extend(composite_row(case, trial, rows, "route_deactivate_drain_total", ["deactivate_instance_route", "wait_instance_drain"]))
        return rows
    raise ValueError(f"unsupported case {case}")


def setup_slots(args: argparse.Namespace, gpu: PhysicalGPU, create_spec: str, label: str) -> None:
    cleanup_gpu(args, gpu, label + "-preclean")
    case = BenchCase(case_id=label, action_type="setup_configure", executor_action="configure_full_template", create_spec=create_spec)
    plan = one_action_plan(args, gpu, case, action_configure(gpu, create_spec, case))
    wait_executed(args, create_plan(args, plan), args.timeout_s)


def setup_instance(args: argparse.Namespace, gpu: PhysicalGPU, case: BenchCase, trial: int, label: str) -> dict[str, Any]:
    runtime = setup_instance_without_route(args, gpu, case, trial, label)
    route_case = BenchCase(**{**case.__dict__, "case_id": label + "-activate-route"})
    plan = one_action_plan(args, gpu, route_case, action_activate(gpu, route_case, runtime), runtimes=[runtime])
    wait_executed(args, create_plan(args, plan), args.timeout_s)
    return runtime


def setup_instance_without_route(args: argparse.Namespace, gpu: PhysicalGPU, case: BenchCase, trial: int, label: str) -> dict[str, Any]:
    setup_slots(args, gpu, agent_slot_spec([case.slot]), label + "-slots")
    runtime = runtime_for_case(args, gpu, case, trial)
    setup_case = BenchCase(**{**case.__dict__, "case_id": label + "-place"})
    plan = one_action_plan(args, gpu, setup_case, action_place(gpu, setup_case, runtime), runtimes=[runtime])
    wait_executed(args, create_plan(args, plan), args.timeout_s)
    return runtime


def cleanup_gpu(args: argparse.Namespace, gpu: PhysicalGPU, label: str) -> None:
    case = BenchCase(case_id=label, action_type="cleanup", executor_action="delete_instance+clear_template+return_gpu")
    actions = [action_delete_all_instances(gpu, case), action_clear(gpu, case), action_return(gpu, case)]
    plan = action_plan(args, label, case, actions)
    try:
        wait_executed(args, create_plan(args, plan), args.timeout_s)
    except Exception as exc:
        if args.no_cleanup:
            raise
        print(f"warning: cleanup {label} failed: {exc}", file=sys.stderr)


def one_action_plan(args: argparse.Namespace, gpu: PhysicalGPU, case: BenchCase, action: dict[str, Any], runtimes: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return action_plan(args, f"{case.case_id}", case, [action], runtimes=runtimes or [])


def action_plan(args: argparse.Namespace, name_suffix: str, case: BenchCase, actions: list[dict[str, Any]], runtimes: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    plan_name = sanitize(f"{args.run_id}-{name_suffix}-{int(time.time() * 1000)}")
    nodes = []
    prev_id = ""
    for idx, action in enumerate(actions):
        action_type = action["type"]
        node_id = sanitize(f"a{idx:04d}-{action_type}-{case.case_id}")[:60]
        node = {
            "id": node_id,
            "index": idx,
            "phase": idx,
            "type": action_type,
            "action": action,
        }
        if prev_id:
            node["dependsOn"] = [prev_id]
        nodes.append(node)
        prev_id = node_id
    return {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "MigActionPlan",
        "metadata": {
            "name": plan_name,
            "namespace": args.namespace,
            "labels": {
                "app.kubernetes.io/name": "migrant-go",
                "mig.or-sim.io/component": "action-latency-microbench",
                "mig.or-sim.io/run-id": args.run_id,
                "mig.or-sim.io/case-id": case.case_id[:63],
            },
        },
        "spec": {
            "abstractActions": [],
            "actionCount": len(nodes),
            "actionDag": {"format": "migrant.action-dag/v1", "name": plan_name, "nodes": nodes},
            "currentAllocationRef": "physicalgpuregistries/default",
            "executor": "go-transition-executor",
            "phaseGate": "auto",
            "plannerMetadata": {"planner": "action-latency-microbench", "case": case_to_row(case)},
            "planningInput": {"sourceArrival": {}, "targetArrival": {}, "registeredSLOMs": {}, "slo": {}},
            "podLifecyclePreview": {"desiredRuntimes": runtimes or []},
            "summary": {"desiredRuntimes": runtimes or [], "planType": "action-latency-microbench", "sourceGpuCount": 0, "targetGpuCount": 0},
            "targetGpuCount": 0,
        },
    }


def action_base(gpu: PhysicalGPU, case: BenchCase, action_type: str) -> dict[str, Any]:
    return {
        "type": action_type,
        "abstractAction": case.case_id,
        "node": gpu.node,
        "gpuIndex": gpu.gpu_index,
        "gpu": gpu.physical_id,
        "physicalGpuId": gpu.physical_id,
        "physical_gpu_id": gpu.physical_id,
    }


def action_configure(gpu: PhysicalGPU, create_spec: str, case: BenchCase) -> dict[str, Any]:
    action = action_base(gpu, case, "configure_full_template")
    action["createSpec"] = create_spec
    action["slots"] = [list(s) for s in slots_from_spec(create_spec)]
    return action


def action_patch(gpu: PhysicalGPU, delete_spec: str, create_spec: str, preserve_spec: str, case: BenchCase) -> dict[str, Any]:
    action = action_base(gpu, case, "patch_slots")
    action["deleteSpec"] = delete_spec
    action["createSpec"] = create_spec
    action["preserveSpec"] = preserve_spec
    action["deleteSlots"] = [list(s) for s in slots_from_spec(delete_spec)]
    action["createSlots"] = [list(s) for s in slots_from_spec(create_spec)]
    action["preserveSlots"] = [list(s) for s in slots_from_spec(preserve_spec)]
    return action


def action_clear(gpu: PhysicalGPU, case: BenchCase) -> dict[str, Any]:
    return action_base(gpu, case, "clear_template")


def action_register(gpu: PhysicalGPU, slots: list[tuple[int, int, str]], case: BenchCase) -> dict[str, Any]:
    action = action_base(gpu, case, "register_mig_devices")
    action["slots"] = [list(slot) for slot in slots]
    return action


def action_return(gpu: PhysicalGPU, case: BenchCase) -> dict[str, Any]:
    return action_base(gpu, case, "return_gpu")


def action_delete_all_instances(gpu: PhysicalGPU, case: BenchCase) -> dict[str, Any]:
    return action_base(gpu, case, "delete_instance")


def action_place(gpu: PhysicalGPU, case: BenchCase, runtime: dict[str, Any]) -> dict[str, Any]:
    action = action_base(gpu, case, "place_instance")
    action.update({"model": runtime["model"], "workload": runtime["model"], "slot": list(case.slot)})
    return action


def action_activate(gpu: PhysicalGPU, case: BenchCase, runtime: dict[str, Any]) -> dict[str, Any]:
    action = action_base(gpu, case, "activate_instance_route")
    action.update({"model": runtime["model"], "workload": runtime["model"], "slot": list(case.slot)})
    return action


def action_deactivate(gpu: PhysicalGPU, case: BenchCase, runtime: dict[str, Any]) -> dict[str, Any]:
    action = action_base(gpu, case, "deactivate_instance_route")
    action.update({"model": runtime["model"], "workload": runtime["model"], "slot": list(case.slot)})
    return action


def action_wait_drain(gpu: PhysicalGPU, case: BenchCase, runtime: dict[str, Any]) -> dict[str, Any]:
    action = action_base(gpu, case, "wait_instance_drain")
    action.update({"model": runtime["model"], "workload": runtime["model"], "slot": list(case.slot)})
    return action


def action_delete_instance(gpu: PhysicalGPU, case: BenchCase, runtime: dict[str, Any]) -> dict[str, Any]:
    action = action_base(gpu, case, "delete_instance")
    action.update({"model": runtime["model"], "workload": runtime["model"], "slot": list(case.slot)})
    return action


def runtime_for_case(args: argparse.Namespace, gpu: PhysicalGPU, case: BenchCase, trial: int) -> dict[str, Any]:
    spec = dict(WORKLOADS[case.workload])
    start, end, profile = case.slot
    slot_resource = slot_resource_name(gpu.physical_id, start, end, profile)
    spec.update({
        "runtimeId": sanitize(f"{case.workload}-{profile}-{trial}-{int(time.time() * 1000)}"),
        "node": gpu.node,
        "gpu": gpu.physical_id,
        "profile": profile,
        "slotResource": slot_resource,
        "hostPort": runtime_host_port(gpu.physical_id, slot_resource),
        "weight": 1.0,
        "capacity": 1.0,
    })
    return spec


def register_runtimes_for_slots(gpu: PhysicalGPU, slots: list[tuple[int, int, str]]) -> list[dict[str, Any]]:
    out = []
    for start, end, profile in slots:
        slot_resource = slot_resource_name(gpu.physical_id, start, end, profile)
        out.append({
            "runtimeId": sanitize(f"bench-register-{gpu.physical_id}-s{start}-{end}-{profile}"),
            "model": sanitize(f"bench-register-s{start}-{end}-{profile}"),
            "runtimeModel": "noop",
            "requestClass": "register-only",
            "node": gpu.node,
            "gpu": gpu.physical_id,
            "profile": profile,
            "slotResource": slot_resource,
            "hostPort": runtime_host_port(gpu.physical_id, slot_resource),
            "weight": 1.0,
            "capacity": 1.0,
        })
    return out


def execute_and_rows(args: argparse.Namespace, case: BenchCase, trial: int, plan: dict[str, Any]) -> list[dict[str, Any]]:
    plan_name = create_plan(args, plan)
    result = wait_executed(args, plan_name, args.timeout_s)
    if not args.keep_plans:
        delete_plan(args, plan_name)
    rows = []
    for status in result.get("status", {}).get("actionStatuses", []) or []:
        row = case_to_row(case)
        row.update({
            "trial": trial,
            "success": status.get("status") == "completed",
            "error": "" if status.get("status") == "completed" else status.get("error", status.get("reason", "")),
            "plan": plan_name,
            "node_id": status.get("id", ""),
            "executor_action_observed": status.get("type", ""),
            "latency_sec": status.get("durationSeconds", ""),
            "relative_start_sec": status.get("relativeStartSeconds", ""),
            "relative_end_sec": status.get("relativeEndSeconds", ""),
            "physical_gpu": physical_from_plan(plan),
            "node": node_from_plan(plan),
        })
        rows.append(row)
    return rows


def composite_row(case: BenchCase, trial: int, rows: list[dict[str, Any]], label: str, action_types: list[str]) -> list[dict[str, Any]]:
    selected = [r for r in rows if r.get("executor_action_observed") in set(action_types)]
    if not selected:
        return []
    starts = [float(r["relative_start_sec"]) for r in selected if r.get("relative_start_sec") != ""]
    ends = [float(r["relative_end_sec"]) for r in selected if r.get("relative_end_sec") != ""]
    row = case_to_row(case)
    row.update({
        "action_type": label,
        "executor_action": "+".join(action_types),
        "executor_action_observed": "+".join(action_types),
        "trial": trial,
        "success": all(str(r.get("success")) == "True" or r.get("success") is True for r in selected),
        "latency_sec": (max(ends) - min(starts)) if starts and ends else "",
        "relative_start_sec": min(starts) if starts else "",
        "relative_end_sec": max(ends) if ends else "",
        "physical_gpu": selected[0].get("physical_gpu", ""),
        "node": selected[0].get("node", ""),
    })
    return [row]


def create_plan(args: argparse.Namespace, body: dict[str, Any]) -> str:
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
        yaml.safe_dump(body, f, sort_keys=False)
        path = f.name
    try:
        run(["kubectl", "create", "-f", path])
    finally:
        Path(path).unlink(missing_ok=True)
    return body["metadata"]["name"]


def wait_executed(args: argparse.Namespace, name: str, timeout_s: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    last_phase = ""
    while time.monotonic() < deadline:
        obj = kubectl_json(["get", "migactionplan", name, "-n", args.namespace, "-o", "json"])
        phase = str((obj.get("status") or {}).get("phase") or "")
        if phase and phase != last_phase:
            print(f"{name}: phase={phase}", flush=True)
            last_phase = phase
        if phase == "Executed":
            return obj
        if phase == "Failed":
            message = (obj.get("status") or {}).get("message")
            raise RuntimeError(f"{name} failed: {message}")
        time.sleep(args.poll_s)
    raise TimeoutError(f"timed out waiting for {name}; last phase={last_phase}")


def delete_plan(args: argparse.Namespace, name: str) -> None:
    run(["kubectl", "-n", args.namespace, "delete", "migactionplan", name, "--ignore-not-found=true"], check=False)


def select_physical_gpu(args: argparse.Namespace) -> PhysicalGPU:
    registry = kubectl_json(["-n", args.namespace, "get", "physicalgpuregistry", "default", "-o", "json"])
    bindings = ((registry.get("status") or {}).get("bindings") or {})
    candidates = []
    for physical_id, raw in sorted(bindings.items()):
        item = raw or {}
        if args.physical_gpu and physical_id != args.physical_gpu:
            continue
        state = str(item.get("state") or "")
        cleanliness = str(item.get("cleanliness") or "")
        if args.physical_gpu or (state == "available" and cleanliness in {"", "empty"}):
            candidates.append(PhysicalGPU(
                physical_id=physical_id,
                node=str(item.get("node") or item.get("nodeName") or physical_id.split("-gpu", 1)[0]),
                gpu_index=int(item.get("gpuIndex") if item.get("gpuIndex") is not None else item.get("deviceIndex") or 0),
            ))
    if not candidates:
        raise RuntimeError("no available physical GPU found; pass --physical-gpu or reset the cluster")
    return candidates[0]


def write_matrix(path: Path, cases: list[BenchCase]) -> None:
    write_csv(path, [case_to_row(c) for c in cases], MATRIX_FIELDS)


def append_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=RESULT_FIELDS, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)


def read_result_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def completed_trials(rows: list[dict[str, Any]]) -> dict[str, set[int]]:
    out: dict[str, set[int]] = {}
    for row in rows:
        if str(row.get("success")).lower() not in {"true", "1"}:
            continue
        case_id = str(row.get("case_id") or "")
        try:
            trial = int(row.get("trial") or 0)
        except ValueError:
            continue
        if case_id and trial > 0:
            out.setdefault(case_id, set()).add(trial)
    return out


def write_summary(path: Path, rows: list[dict[str, Any]]) -> None:
    groups: dict[tuple[str, str, str, str, str, str], list[float]] = {}
    for row in rows:
        if str(row.get("success")) not in {"True", "true", "1"} and row.get("success") is not True:
            continue
        try:
            value = float(row.get("latency_sec"))
        except (TypeError, ValueError):
            continue
        key = (
            str(row.get("action_type", "")),
            str(row.get("executor_action_observed") or row.get("executor_action", "")),
            str(row.get("profile", "")),
            str(row.get("template", "")),
            str(row.get("source_template", "")),
            str(row.get("target_template", "")),
        )
        groups.setdefault(key, []).append(value)
    out = []
    for key, values in sorted(groups.items()):
        out.append({
            "action_type": key[0],
            "executor_action": key[1],
            "profile": key[2],
            "template": key[3],
            "source_template": key[4],
            "target_template": key[5],
            "n": len(values),
            "median_sec": statistics.median(values),
            "p95_sec": percentile(values, 95),
            "mean_sec": statistics.mean(values),
            "std_sec": statistics.pstdev(values) if len(values) > 1 else 0.0,
        })
    write_csv(path, out, SUMMARY_FIELDS)


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def case_to_row(case: BenchCase) -> dict[str, Any]:
    return {
        "case_id": case.case_id,
        "action_type": case.action_type,
        "executor_action": case.executor_action,
        "profile": case.profile,
        "template": case.template,
        "source_template": case.source_template,
        "target_template": case.target_template,
        "workload": case.workload,
        "runtime_model": case.runtime_model,
        "request_class": case.request_class,
        "slot": json.dumps(case.slot) if case.slot else "",
        "create_spec": case.create_spec,
        "delete_spec": case.delete_spec,
        "preserve_spec": case.preserve_spec,
    }


def slots_for_template(template: str) -> list[tuple[int, int, str]]:
    _, intervals = all_unique_physical_realizations(template)[0]
    return [(int(s), int(e), str(p)) for s, e, p in intervals if p not in {"void", "unusable"}]


def slots_from_spec(spec: str) -> list[tuple[int, int, str]]:
    out = []
    for part in split_csv(spec):
        start, size, profile = part.split(":", 2)
        start_i = int(start)
        size_i = int(size)
        end = start_i + size_i
        if profile == "3g":
            end = start_i + 4
        out.append((start_i, end, profile))
    return out


def gpu_from_intervals(intervals: list[tuple[int, int, str]]) -> GPUState:
    return GPUState(
        gpu_id=0,
        instances=[MigInstance(start=s, end=e, profile=p) for s, e, p in intervals if p not in {"void", "unusable"}],
    )


def slot_resource_name(physical_id: str, start: int, end: int, profile: str) -> str:
    return f"or-sim.io/{physical_id}-s{start}-{end}-{profile}"


def partial_source_spec(case: BenchCase) -> str:
    return agent_slot_spec(slots_from_spec(case.delete_spec) + slots_from_spec(case.preserve_spec))


def add_mig_uuids_to_spec(args: argparse.Namespace, gpu: PhysicalGPU, spec: str) -> str:
    parts = split_csv(spec)
    if not parts:
        return spec
    devices = wait_registry_devices_for_spec(args, gpu, spec)
    out = []
    for part in parts:
        start, size, profile = part.split(":", 2)[:3]
        profile = profile.split(":", 1)[0]
        start_i = int(start)
        end_i = start_i + int(size)
        match = None
        for device in devices:
            if (
                int(device.get("start", -1)) == start_i
                and int(device.get("end", -1)) == end_i
                and str(device.get("profile", "")) == profile
            ):
                match = device
                break
        if match is None:
            raise RuntimeError(f"slot {part} not found in registry for {gpu.physical_id}")
        uuid = str(match.get("uuid") or match.get("migDeviceUuid") or match.get("migDeviceUUID") or "")
        if not uuid:
            raise RuntimeError(f"slot {part} in registry has no MIG UUID for {gpu.physical_id}")
        out.append(f"{start}:{size}:{profile}:{uuid}")
    return ",".join(out)


def wait_registry_devices_for_spec(args: argparse.Namespace, gpu: PhysicalGPU, spec: str) -> list[dict[str, Any]]:
    expected = []
    for part in split_csv(spec):
        start, size, profile = part.split(":", 2)[:3]
        expected.append((int(start), int(start) + int(size), profile.split(":", 1)[0]))
    deadline = time.monotonic() + min(args.timeout_s, 120.0)
    last_devices: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        registry = kubectl_json(["-n", args.namespace, "get", "physicalgpuregistry", "default", "-o", "json"])
        binding = (((registry.get("status") or {}).get("bindings") or {}).get(gpu.physical_id) or {})
        devices = binding.get("migDevices") or binding.get("logicalMigSlots") or []
        last_devices = [dict(item) for item in devices]
        if all(has_registry_slot(last_devices, start, end, profile) for start, end, profile in expected):
            return last_devices
        time.sleep(args.poll_s)
    observed = [
        f"{item.get('start')}:{int(item.get('end', 0)) - int(item.get('start', 0))}:{item.get('profile')}"
        for item in last_devices
        if item.get("start") is not None and item.get("end") is not None
    ]
    raise TimeoutError(
        f"timed out waiting for registry slots {split_csv(spec)} on {gpu.physical_id}; observed={observed}"
    )


def has_registry_slot(devices: list[dict[str, Any]], start: int, end: int, profile: str) -> bool:
    for device in devices:
        if (
            int(device.get("start", -1)) == start
            and int(device.get("end", -1)) == end
            and str(device.get("profile", "")) == profile
        ):
            return True
    return False


def runtime_host_port(physical_id: str, slot_resource: str) -> int:
    gpu_match = re.search(r"-gpu(\d+)$", physical_id)
    slot_match = re.search(r"-s(\d+)-\d+-[a-z0-9]+$", slot_resource)
    if not gpu_match or not slot_match:
        return RUNTIME_HOST_PORT_POOL[0]
    index = int(gpu_match.group(1)) * 7 + int(slot_match.group(1))
    if index >= len(RUNTIME_HOST_PORT_POOL):
        raise ValueError(f"runtime host port pool exhausted for {physical_id} {slot_resource}")
    return RUNTIME_HOST_PORT_POOL[index]


def physical_from_plan(plan: dict[str, Any]) -> str:
    nodes = (((plan.get("spec") or {}).get("actionDag") or {}).get("nodes") or [])
    if not nodes:
        return ""
    return str(((nodes[0].get("action") or {}).get("physicalGpuId")) or "")


def node_from_plan(plan: dict[str, Any]) -> str:
    nodes = (((plan.get("spec") or {}).get("actionDag") or {}).get("nodes") or [])
    if not nodes:
        return ""
    return str(((nodes[0].get("action") or {}).get("node")) or "")


def trial_count_for_case(args: argparse.Namespace, case: BenchCase) -> int:
    if case.runtime_model in {"gpt2", "llama"} or case.workload.startswith(("gpt2", "llama")):
        return min(args.trials, args.llm_trials)
    return args.trials


def split_csv(value: str) -> list[str]:
    return [item.strip() for item in str(value).split(",") if item.strip()]


def sanitize(value: str) -> str:
    out = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value))
    out = "-".join(part for part in out.split("-") if part)
    return out[:230] or "x"


def percentile(values: list[float], p: float) -> float:
    if not values:
        return math.nan
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * (p / 100.0)
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return ordered[lo]
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (rank - lo)


def kubectl_json(args: list[str]) -> dict[str, Any]:
    proc = run(["kubectl", *args], capture=True)
    return json.loads(proc.stdout)


def run(cmd: list[str], check: bool = True, capture: bool = False) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(cmd, text=True, stdout=subprocess.PIPE if capture else None, stderr=subprocess.PIPE)
    if check and proc.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} failed: {proc.stderr.strip()}")
    return proc


MATRIX_FIELDS = [
    "case_id",
    "action_type",
    "executor_action",
    "profile",
    "template",
    "source_template",
    "target_template",
    "workload",
    "runtime_model",
    "request_class",
    "slot",
    "create_spec",
    "delete_spec",
    "preserve_spec",
]
RESULT_FIELDS = [
    *MATRIX_FIELDS,
    "trial",
    "success",
    "error",
    "plan",
    "node_id",
    "executor_action_observed",
    "latency_sec",
    "relative_start_sec",
    "relative_end_sec",
    "physical_gpu",
    "node",
]
SUMMARY_FIELDS = [
    "action_type",
    "executor_action",
    "profile",
    "template",
    "source_template",
    "target_template",
    "n",
    "median_sec",
    "p95_sec",
    "mean_sec",
    "std_sec",
]


if __name__ == "__main__":
    raise SystemExit(main())
