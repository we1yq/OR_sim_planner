from __future__ import annotations

import argparse
from pathlib import Path

from actuator import run_dry_run_actuator_loop
from cluster_observer import observe_cluster_state_once
from io_utils import dump_yaml, load_yaml
from k8s_adapter import plan_scenario_as_migplan_status, plan_scenario_chain_as_migplan_statuses
from mig_label_executor import apply_mig_labels_from_action_plan, summarize_mig_labels_from_action_plan
from mig_rules import load_mig_rules, mig_rules_summary_dict, validate_gpu_state_against_mig_rules
from physical_gpu_registry import (
    mark_physical_gpu_active,
    mark_physical_gpu_released,
    registry_queue_summary,
    run_physical_gpu_registry_monitor_loop,
    sync_physical_gpu_registry,
)
from reconciler import reconcile_migplan_once, run_controller_loop, run_watch_controller_loop
from reconciler import upsert_migactionplan
from scenario_loader import load_planning_scenario, scenario_summary_dict
from state_adapter import gpu_state_from_mock_yaml
from executors.pod_lifecycle_executor import apply_pod_lifecycle_from_action_plan
from executors.router_drain_executor import apply_router_drain_from_action_plan
from test_harness.router_drain_smoke import create_router_drain_smoke_action_plan
from test_harness.workload_lifecycle_smoke import create_workload_lifecycle_smoke_action_plan


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Mock MIG planner controller")
    parser.add_argument(
        "--gpu-state",
        default=root / "mock/gpu-states/one-a100-empty.yaml",
        type=Path,
        help="Path to a mock GPU/MIG state YAML file.",
    )
    parser.add_argument(
        "--scenario",
        type=Path,
        help="Path to a multi-workload PlanningScenario YAML file. Prints a parsed summary.",
    )
    parser.add_argument(
        "--plan-scenario",
        type=Path,
        help="Path to a PlanningScenario YAML file. Runs MILP target build plus V3 dry-run planning.",
    )
    parser.add_argument(
        "--plan-scenario-chain",
        nargs="+",
        type=Path,
        help="PlanningScenario YAML files to run sequentially with canonicalized next-stage state.",
    )
    parser.add_argument(
        "--max-iters",
        type=int,
        default=20,
        help="Maximum V3 transition iterations per stage.",
    )
    parser.add_argument(
        "--milp-time-limit-s",
        type=float,
        help="Optional Gurobi MILP time limit per stage.",
    )
    parser.add_argument(
        "--verbose-planner",
        action="store_true",
        help="Print verbose MILP/target-builder logs.",
    )
    parser.add_argument(
        "--reconcile-migplan",
        help="Name of a MigPlan CR to reconcile once by patching status with a dry-run plan.",
    )
    parser.add_argument(
        "--namespace",
        default="or-sim",
        help="Kubernetes namespace for --reconcile-migplan.",
    )
    parser.add_argument(
        "--scenario-root",
        type=Path,
        help="Directory used to resolve MigPlan spec.scenario values such as stage0.",
    )
    parser.add_argument(
        "--print-reconcile-only",
        action="store_true",
        help="For --reconcile-migplan, print the planned status object without patching Kubernetes.",
    )
    parser.add_argument(
        "--run-controller",
        action="store_true",
        help="Run a polling MigPlan controller loop that patches dry-run status.",
    )
    parser.add_argument(
        "--run-watch-controller",
        action="store_true",
        help="Run a watch-based MigPlan controller loop that patches dry-run status on events.",
    )
    parser.add_argument(
        "--run-dry-run-actuator",
        action="store_true",
        help="Run a dry-run MigActionPlan actuator loop that validates approved plans without hardware changes.",
    )
    parser.add_argument(
        "--observe-cluster-state",
        action="store_true",
        help="Read Kubernetes Nodes/Pods and print an ObservedClusterState smoke object.",
    )
    parser.add_argument(
        "--observed-state-name",
        default="cluster-observed-state",
        help="ObservedClusterState name for --observe-cluster-state.",
    )
    parser.add_argument(
        "--apply-observed-state",
        action="store_true",
        help="For --observe-cluster-state, write the ObservedClusterState and status to Kubernetes.",
    )
    parser.add_argument(
        "--sync-physical-gpu-registry",
        action="store_true",
        help="Observe hardware and sync the PhysicalGpuRegistry status.",
    )
    parser.add_argument(
        "--run-physical-gpu-registry-monitor",
        action="store_true",
        help="Run a polling monitor that continuously syncs PhysicalGpuRegistry.",
    )
    parser.add_argument(
        "--registry-name",
        default="default",
        help="PhysicalGpuRegistry name for registry operations.",
    )
    parser.add_argument(
        "--apply-physical-gpu-registry",
        action="store_true",
        help="For --sync-physical-gpu-registry, write registry spec/status to Kubernetes.",
    )
    parser.add_argument(
        "--activate-physical-gpu",
        help="Move a physicalGpuId from available/transitioning to activeQueue for a planner ownership smoke test.",
    )
    parser.add_argument(
        "--release-physical-gpu",
        help="Move a physicalGpuId out of activeQueue; it returns to available only if observed clean.",
    )
    parser.add_argument(
        "--summarize-mig-labels-from-action-plan",
        help="Read a MigActionPlan and print the NVIDIA GPU Operator node-label patch it would apply.",
    )
    parser.add_argument(
        "--apply-mig-labels-from-action-plan",
        help="Apply a MigActionPlan executorPreview node-label patch to real Kubernetes Nodes.",
    )
    parser.add_argument(
        "--confirm-real-mig-apply",
        action="store_true",
        help="Required with --apply-mig-labels-from-action-plan because it changes real MIG layout.",
    )
    parser.add_argument(
        "--allow-preview-instructions",
        action="store_true",
        help="Allow applying instructions from a dryRun MigActionPlan for a controlled hardware smoke test.",
    )
    parser.add_argument(
        "--wait-mig-success",
        action="store_true",
        help="Wait for node nvidia.com/mig.config.state=success after applying MIG labels.",
    )
    parser.add_argument(
        "--mig-apply-timeout-s",
        type=float,
        default=900.0,
        help="Timeout for --wait-mig-success.",
    )
    parser.add_argument(
        "--create-mig-label-smoke-action-plan",
        help="Create a controlled MigActionPlan that targets one node/device MIG layout.",
    )
    parser.add_argument(
        "--create-workload-lifecycle-smoke-action-plan",
        help="Create a controlled MigActionPlan that targets one workload Pod and one live batch update.",
    )
    parser.add_argument(
        "--apply-pod-lifecycle-from-action-plan",
        dest="apply_pod_lifecycle_from_action_plan",
        help="Apply a MigActionPlan podLifecyclePreview to real Kubernetes Pods.",
    )
    parser.add_argument(
        "--create-router-drain-smoke-action-plan",
        help="Create a controlled MigActionPlan that reroutes one workload from source to target.",
    )
    parser.add_argument(
        "--apply-router-drain-from-action-plan",
        help="Apply a MigActionPlan trafficAndDrainPreview through the Router/Drain executor.",
    )
    parser.add_argument(
        "--confirm-real-router-apply",
        action="store_true",
        help="Required with --apply-router-drain-from-action-plan because it changes real router/drain state.",
    )
    parser.add_argument("--router-endpoint", help="Base URL for the router service.")
    parser.add_argument(
        "--router-drain-mode",
        default="http",
        choices=["http", "annotation", "no-traffic"],
        help="Router/Drain executor mode.",
    )
    parser.add_argument("--router-smoke-workload", default="resnet50")
    parser.add_argument("--router-smoke-source-pod", default="router-workload-a")
    parser.add_argument("--router-smoke-source-endpoint", default="http://router-workload-a:8080")
    parser.add_argument("--router-smoke-target-pod", default="router-workload-b")
    parser.add_argument("--router-smoke-target-endpoint", default="http://router-workload-b:8080")
    parser.add_argument(
        "--confirm-real-pod-apply",
        action="store_true",
        help="Required with --apply-pod-lifecycle-from-action-plan because it creates, patches, or deletes Pods.",
    )
    parser.add_argument(
        "--workload-smoke-node",
        default="rtx1-worker",
        help="Node name for workload lifecycle smoke plans.",
    )
    parser.add_argument(
        "--workload-smoke-mig-resource",
        default="nvidia.com/mig-3g.20gb",
        help="MIG resource name requested by workload lifecycle smoke Pods.",
    )
    parser.add_argument(
        "--workload-smoke-name",
        default="smoke",
        help="Workload name recorded in workload lifecycle smoke plans.",
    )
    parser.add_argument(
        "--workload-smoke-initial-batch-size",
        default="4",
        help="Initial batch size for workload lifecycle smoke plans.",
    )
    parser.add_argument(
        "--workload-smoke-updated-batch-size",
        default="8",
        help="Updated batch size for workload lifecycle smoke plans.",
    )
    parser.add_argument(
        "--workload-smoke-image",
        default="nvidia/cuda:12.4.1-base-ubuntu22.04",
        help="Container image for workload lifecycle smoke Pods.",
    )
    parser.add_argument(
        "--cleanup-workload-smoke",
        action="store_true",
        help="Delete workload lifecycle smoke Pods and ConfigMaps after applying the action plan.",
    )
    parser.add_argument("--mig-smoke-node", default="rtx1", help="Node name for MIG label smoke plan.")
    parser.add_argument("--mig-smoke-device-index", type=int, default=0, help="GPU device index for smoke plan.")
    parser.add_argument(
        "--mig-smoke-target-template",
        default="3g+3g",
        help="Target template for smoke plan, for example 3g+3g or 2g+2g+2g.",
    )
    parser.add_argument(
        "--mig-smoke-source-template",
        default="2g+2g+2g",
        help="Source template recorded in smoke plan metadata.",
    )
    parser.add_argument(
        "--poll-interval-s",
        type=float,
        default=10.0,
        help="Polling interval for --run-controller.",
    )
    parser.add_argument(
        "--controller-max-cycles",
        type=int,
        help="Optional cycle limit for --run-controller. Omit to run until interrupted.",
    )
    parser.add_argument(
        "--watch-timeout-s",
        type=int,
        default=60,
        help="Kubernetes watch timeout before reconnecting.",
    )
    parser.add_argument(
        "--watch-max-events",
        type=int,
        help="Optional processed-event limit for --run-watch-controller. Omit to run until interrupted.",
    )
    parser.add_argument(
        "--validate-mig-rules",
        type=Path,
        help="Path to a MIG rules YAML file. Prints a validation summary.",
    )
    parser.add_argument(
        "--validate-gpu-state",
        action="store_true",
        help="Validate --gpu-state against --validate-mig-rules.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.run_watch_controller:
        summary = run_watch_controller_loop(
            namespace=args.namespace,
            scenario_root=args.scenario_root,
            max_iters=args.max_iters,
            milp_time_limit_s=args.milp_time_limit_s,
            verbose=args.verbose_planner,
            watch_timeout_s=args.watch_timeout_s,
            max_events=args.watch_max_events,
        )
        print(dump_yaml(summary), end="")
        return 0

    if args.run_dry_run_actuator:
        summary = run_dry_run_actuator_loop(
            namespace=args.namespace,
            poll_interval_s=args.poll_interval_s,
            max_cycles=args.controller_max_cycles,
        )
        print(dump_yaml(summary), end="")
        return 0

    if args.observe_cluster_state:
        observed = observe_cluster_state_once(
            namespace=args.namespace,
            name=args.observed_state_name,
            apply=args.apply_observed_state,
        )
        print(dump_yaml(observed), end="")
        return 0

    if args.sync_physical_gpu_registry:
        registry = sync_physical_gpu_registry(
            namespace=args.namespace,
            registry_name=args.registry_name,
            observed_state_name=args.observed_state_name,
            apply=args.apply_physical_gpu_registry,
        )
        print(dump_yaml(registry_queue_summary(registry)), end="")
        return 0

    if args.run_physical_gpu_registry_monitor:
        summary = run_physical_gpu_registry_monitor_loop(
            namespace=args.namespace,
            registry_name=args.registry_name,
            observed_state_name=args.observed_state_name,
            poll_interval_s=args.poll_interval_s,
            max_cycles=args.controller_max_cycles,
        )
        print(dump_yaml(summary), end="")
        return 0

    if args.activate_physical_gpu:
        registry = mark_physical_gpu_active(
            physical_gpu_id=args.activate_physical_gpu,
            namespace=args.namespace,
            registry_name=args.registry_name,
            apply=args.apply_physical_gpu_registry,
        )
        print(dump_yaml(registry_queue_summary(registry)), end="")
        return 0

    if args.release_physical_gpu:
        registry = mark_physical_gpu_released(
            physical_gpu_id=args.release_physical_gpu,
            namespace=args.namespace,
            registry_name=args.registry_name,
            apply=args.apply_physical_gpu_registry,
        )
        print(dump_yaml(registry_queue_summary(registry)), end="")
        return 0

    if args.summarize_mig_labels_from_action_plan:
        summary = summarize_mig_labels_from_action_plan(
            name=args.summarize_mig_labels_from_action_plan,
            namespace=args.namespace,
        )
        print(dump_yaml(summary), end="")
        return 0

    if args.apply_mig_labels_from_action_plan:
        summary = apply_mig_labels_from_action_plan(
            name=args.apply_mig_labels_from_action_plan,
            namespace=args.namespace,
            confirm_real_mig_apply=args.confirm_real_mig_apply,
            allow_preview_instructions=args.allow_preview_instructions,
            wait=args.wait_mig_success,
            timeout_s=args.mig_apply_timeout_s,
        )
        print(dump_yaml(summary), end="")
        return 0

    if args.create_mig_label_smoke_action_plan:
        status = _mig_label_smoke_status(
            node_name=args.mig_smoke_node,
            device_index=args.mig_smoke_device_index,
            source_template=args.mig_smoke_source_template,
            target_template=args.mig_smoke_target_template,
        )
        upsert_migactionplan(
            name=args.create_mig_label_smoke_action_plan,
            namespace=args.namespace,
            owner_migplan=f"{args.create_mig_label_smoke_action_plan}-synthetic-owner",
            migplan_generation=1,
            auto_approval_policy_name="default",
            status=status,
        )
        print(dump_yaml(status), end="")
        return 0

    if args.create_workload_lifecycle_smoke_action_plan:
        summary = create_workload_lifecycle_smoke_action_plan(
            name=args.create_workload_lifecycle_smoke_action_plan,
            namespace=args.namespace,
            node_name=args.workload_smoke_node,
            mig_resource=args.workload_smoke_mig_resource,
            workload=args.workload_smoke_name,
            initial_batch_size=args.workload_smoke_initial_batch_size,
            updated_batch_size=args.workload_smoke_updated_batch_size,
        )
        print(dump_yaml(summary), end="")
        return 0

    if args.apply_pod_lifecycle_from_action_plan:
        summary = apply_pod_lifecycle_from_action_plan(
            name=args.apply_pod_lifecycle_from_action_plan,
            namespace=args.namespace,
            confirm_real_pod_apply=args.confirm_real_pod_apply,
            allow_preview_instructions=args.allow_preview_instructions,
            node_name=args.workload_smoke_node,
            mig_resource=args.workload_smoke_mig_resource,
            image=args.workload_smoke_image,
            timeout_s=args.mig_apply_timeout_s,
            cleanup=args.cleanup_workload_smoke,
        )
        print(dump_yaml(summary), end="")
        return 0

    if args.create_router_drain_smoke_action_plan:
        summary = create_router_drain_smoke_action_plan(
            name=args.create_router_drain_smoke_action_plan,
            namespace=args.namespace,
            workload=args.router_smoke_workload,
            source_pod=args.router_smoke_source_pod,
            source_endpoint=args.router_smoke_source_endpoint,
            target_pod=args.router_smoke_target_pod,
            target_endpoint=args.router_smoke_target_endpoint,
            router_endpoint=args.router_endpoint or "http://or-sim-smoke-router:8080",
        )
        print(dump_yaml(summary), end="")
        return 0

    if args.apply_router_drain_from_action_plan:
        summary = apply_router_drain_from_action_plan(
            name=args.apply_router_drain_from_action_plan,
            namespace=args.namespace,
            confirm_real_router_apply=args.confirm_real_router_apply,
            allow_preview_instructions=args.allow_preview_instructions,
            router_endpoint=args.router_endpoint,
            mode=args.router_drain_mode,
            timeout_s=args.mig_apply_timeout_s,
        )
        print(dump_yaml(summary), end="")
        return 0

    if args.run_controller:
        summary = run_controller_loop(
            namespace=args.namespace,
            scenario_root=args.scenario_root,
            max_iters=args.max_iters,
            milp_time_limit_s=args.milp_time_limit_s,
            verbose=args.verbose_planner,
            poll_interval_s=args.poll_interval_s,
            max_cycles=args.controller_max_cycles,
        )
        print(dump_yaml(summary), end="")
        return 0

    if args.reconcile_migplan is not None:
        plan = reconcile_migplan_once(
            name=args.reconcile_migplan,
            namespace=args.namespace,
            scenario_root=args.scenario_root,
            max_iters=args.max_iters,
            milp_time_limit_s=args.milp_time_limit_s,
            verbose=args.verbose_planner,
            patch_status=not args.print_reconcile_only,
        )
        print(dump_yaml(plan), end="")
        return 0

    if args.plan_scenario is not None:
        scenario = load_planning_scenario(args.plan_scenario)
        plan = plan_scenario_as_migplan_status(
            scenario,
            max_iters=args.max_iters,
            milp_time_limit_s=args.milp_time_limit_s,
            verbose=args.verbose_planner,
        )
        print(dump_yaml(plan), end="")
        return 0

    if args.plan_scenario_chain:
        scenarios = [load_planning_scenario(path) for path in args.plan_scenario_chain]
        plan = plan_scenario_chain_as_migplan_statuses(
            scenarios,
            max_iters=args.max_iters,
            milp_time_limit_s=args.milp_time_limit_s,
            verbose=args.verbose_planner,
        )
        print(dump_yaml(plan), end="")
        return 0

    if args.scenario is not None:
        scenario = load_planning_scenario(args.scenario)
        print(dump_yaml(scenario_summary_dict(scenario)), end="")
        return 0

    if args.validate_mig_rules is not None:
        rules = load_mig_rules(args.validate_mig_rules)
        summary = mig_rules_summary_dict(rules)
        if args.validate_gpu_state:
            gpu_state = gpu_state_from_mock_yaml(load_yaml(args.gpu_state))
            validate_gpu_state_against_mig_rules(gpu_state, rules)
            summary["gpuStateRef"] = str(args.gpu_state)
            summary["gpuStateValid"] = True
            summary["gpuCount"] = len(gpu_state.gpus)
        print(dump_yaml(summary), end="")
        return 0

    raise SystemExit("choose --scenario or --validate-mig-rules")


def _mig_label_smoke_status(
    node_name: str,
    device_index: int,
    source_template: str,
    target_template: str,
) -> dict:
    physical_gpu_id = "A"
    instances = _instances_from_template(target_template)
    canonical_next_state = {
        "metadata": {
            "physical_id_map": {"0": physical_gpu_id},
            "physicalGpuBindings": {
                physical_gpu_id: {
                    "nodeName": node_name,
                    "deviceIndex": int(device_index),
                }
            },
            "smokeTest": "single-node-mig-label-apply",
        },
        "gpus": [
            {
                "gpuId": 0,
                "source": "dry-run",
                "instances": instances,
            }
        ],
    }
    return {
        "metrics": {"actionCount": 3, "gpuCount": 1},
        "planningSummary": {
            "actionCountsByType": {
                "clear_template": 1,
                "configure_full_template": 1,
                "place_target_layout": 1,
            },
            "chosenTemplates": [target_template],
        },
        "actions": [
            {"type": "clear_template", "gpu_id": 0, "physical_gpu_id": physical_gpu_id},
            {
                "type": "configure_full_template",
                "physical_gpu_id": physical_gpu_id,
                "template": target_template,
            },
            {"type": "place_target_layout", "gpu_id": 0, "physical_gpu_id": physical_gpu_id},
        ],
        "planningTrace": {
            "scenario": "single-node-mig-label-smoke",
            "transition": {
                "finalCoarseActions": [
                    {
                        "type": "reconfiguration",
                        "gpu_id": 0,
                        "source_physical_gpu_id": physical_gpu_id,
                        "new_physical_gpu_id": physical_gpu_id,
                        "src_template": source_template,
                        "tgt_template": target_template,
                        "mode": "in_place_old_first",
                    }
                ],
                "finalPlanItems": [],
            },
        },
        "canonicalNextState": canonical_next_state,
    }


def _instances_from_template(template: str) -> list[dict]:
    cursor = 0
    instances = []
    for raw_profile in [part for part in template.split("+") if part]:
        profile = raw_profile if raw_profile.endswith("g") else f"{raw_profile}g"
        size = int(profile.removesuffix("g"))
        instances.append(
            {
                "start": cursor,
                "end": cursor + size,
                "profile": profile,
                "workload": None,
                "batch": None,
                "mu": 0.0,
                "preserved": False,
            }
        )
        cursor += size
    if cursor < 7:
        instances.append(
            {
                "start": cursor,
                "end": 7,
                "profile": "void",
                "workload": None,
                "batch": None,
                "mu": 0.0,
                "preserved": False,
            }
        )
    return instances


if __name__ == "__main__":
    raise SystemExit(main())
