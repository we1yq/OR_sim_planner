from __future__ import annotations

import argparse
from pathlib import Path

from actuator import run_dry_run_actuator_loop
from io_utils import dump_yaml, load_yaml
from k8s_adapter import plan_scenario_as_migplan_status, plan_scenario_chain_as_migplan_statuses
from mig_rules import load_mig_rules, mig_rules_summary_dict, validate_gpu_state_against_mig_rules
from reconciler import reconcile_migplan_once, run_controller_loop, run_watch_controller_loop
from scenario_loader import load_planning_scenario, scenario_summary_dict
from state_adapter import gpu_state_from_mock_yaml


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


if __name__ == "__main__":
    raise SystemExit(main())
