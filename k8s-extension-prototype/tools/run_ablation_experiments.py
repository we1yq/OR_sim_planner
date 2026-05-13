#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import csv
import html
import io
import json
import sys
import time
import zipfile
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape as xml_escape


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
CONTROLLER = ROOT / "controller"
if str(CONTROLLER) not in sys.path:
    sys.path.insert(0, str(CONTROLLER))

from planning.k8s_adapter import (  # noqa: E402
    build_feasible_option_dataframe,
    cluster_state_from_mock_yaml,
)
from io_utils import load_yaml  # noqa: E402
from scenario_loader import load_planning_scenario  # noqa: E402
from migrant_core.physical_ids import (  # noqa: E402
    bootstrap_physical_ids_for_state,
    canonicalize_state_for_next_round,
    ensure_state_metadata,
)
from migrant_core.state import assert_valid_cluster_state  # noqa: E402

from migrant_core.placement_planners import (  # noqa: E402
    greedy_two_phase,
    milp_enhanced,
    milp_original,
    simulated_annealing,
)
from migrant_core.target_builders import (  # noqa: E402
    beam_preserve,
    exact_milp_templates,
    no_preserve_greedy,
    preserve_greedy,
)
from migrant_core.transition_planners import PLANNER_CATALOG  # noqa: E402


REPORT_DIR = ROOT / "reports"

# Empirical constants from docs/mig-reconfig-benchmark-2026-05-10.md.
# These are intentionally rough: the simulation produces action plans, not real
# wall-clock GPU-operator reconciliations.
HARDWARE_SECONDS = {
    "configure_full_template": 113.2,
    "place_target_layout": 0.0,
    "bind_target_gpu": 0.0,
    "clear_gpu": 10.4,
    "delete_gpu_pods": 1.0,
    "clear_gpu_binding": 0.0,
    "clear_template": 0.0,
    "return_gpu": 0.0,
    "allocate_gpu": 0.0,
    "mark_reconfig_target_prepared": 0.0,
    "stop_gpu_traffic": 1.0,
    "stop_accepting_new": 1.0,
    "accept_queued_requests": 1.0,
    "reroute_queued_tasks": 1.0,
    "mark_draining_instance": 30.0,
    "remove_instance": 1.0,
    "delete_bridge_pod": 1.0,
    "place_instance": 1.0,
    "update_batch": 1.0,
    "patch_batch_config": 0.2,
    "apply_batch": 1.0,
    "verify_batch": 0.5,
    "workload_change": 1.0,
    "bridge_place_instance": 1.0,
}


VARIANTS = {
    "current_full": {
        "placement": milp_enhanced.solve,
        "target": preserve_greedy.build,
        "transition": PLANNER_CATALOG["phase_greedy"].runner,
        "description": "Current stack: enhanced MILP + preserve-greedy target builder + phase-greedy transition.",
    },
    "placement_milp_original": {
        "placement": milp_original.solve,
        "target": preserve_greedy.build,
        "transition": PLANNER_CATALOG["phase_greedy"].runner,
        "description": "Ablate placement: notebook original MILP without option pruning, elastic objective, warm-start hooks, or previous-state input.",
    },
    "placement_greedy_two_phase": {
        "placement": greedy_two_phase.solve,
        "target": preserve_greedy.build,
        "transition": PLANNER_CATALOG["phase_greedy"].runner,
        "description": "Ablate placement: notebook-style two-phase greedy placement, current target and transition.",
    },
    "placement_simulated_annealing": {
        "placement": simulated_annealing.solve,
        "target": preserve_greedy.build,
        "transition": PLANNER_CATALOG["phase_greedy"].runner,
        "description": "Ablate placement: notebook-style simulated annealing over two-phase template choices.",
    },
    "target_no_preserve": {
        "placement": milp_enhanced.solve,
        "target": no_preserve_greedy.build,
        "transition": PLANNER_CATALOG["phase_greedy"].runner,
        "description": "Ablate target preservation: ignore previous layout when materializing target state.",
    },
    "target_beam_preserve": {
        "placement": milp_enhanced.solve,
        "target": beam_preserve.build,
        "transition": PLANNER_CATALOG["phase_greedy"].runner,
        "description": "Ablate target search: notebook preserve-first beam solver.",
    },
    "target_exact_milp_templates": {
        "placement": milp_enhanced.solve,
        "target": exact_milp_templates.build,
        "transition": PLANNER_CATALOG["phase_greedy"].runner,
        "description": "Ablate target rewrite/search: use exact MILP template list and first physical realization.",
    },
    "transition_serial_root_baseline": {
        "placement": milp_enhanced.solve,
        "target": preserve_greedy.build,
        "transition": PLANNER_CATALOG["serial_root_baseline"].runner,
        "description": "Ablate transition: execute one root transition per iteration.",
    },
    "transition_drain_aware_baseline": {
        "placement": milp_enhanced.solve,
        "target": preserve_greedy.build,
        "transition": PLANNER_CATALOG["drain_aware_baseline"].runner,
        "description": "Ablate transition scoring: drain-aware non-conflicting groups without phased/DAG action-plan representation.",
    },
    "transition_full_plan_baseline": {
        "placement": milp_enhanced.solve,
        "target": preserve_greedy.build,
        "transition": PLANNER_CATALOG["full_plan_baseline"].runner,
        "description": "Ablate transition granularity: execute the whole full plan each iteration.",
    },
    "transition_phase_greedy_dag": {
        "placement": milp_enhanced.solve,
        "target": preserve_greedy.build,
        "transition": PLANNER_CATALOG["phase_greedy_with_dag_output"].runner,
        "description": "Ablate transition representation: phase-greedy execution plus explicit phased/DAG action-plan output.",
    },
}


VARIANT_GROUPS = {
    "placement": [
        "current_full",
        "placement_milp_original",
        "placement_greedy_two_phase",
        "placement_simulated_annealing",
    ],
    "target": [
        "current_full",
        "target_no_preserve",
        "target_beam_preserve",
        "target_exact_milp_templates",
    ],
    "transition": [
        "current_full",
        "transition_serial_root_baseline",
        "transition_drain_aware_baseline",
        "transition_full_plan_baseline",
        "transition_phase_greedy_dag",
    ],
}


GROUP_LABELS = {
    "placement": "Placement Planner 变体",
    "target": "Target Builder 变体",
    "transition": "Transition Planner 变体",
}


def _call_quiet(fn: Any, quiet: bool, **kwargs: Any) -> Any:
    if not quiet:
        return fn(**kwargs)
    with contextlib.redirect_stdout(io.StringIO()):
        return fn(**kwargs)


def _action_counts(actions: list[dict[str, Any]]) -> dict[str, int]:
    return dict(Counter(str(action.get("type", "unknown")) for action in actions))


def _estimate_seconds(actions: list[dict[str, Any]]) -> float:
    return float(sum(HARDWARE_SECONDS.get(str(action.get("type")), 1.0) for action in actions))


def _failure_row(
    variant_name: str,
    scenario_name: str,
    stage: str,
    message: str,
    start: float,
    **extra: Any,
) -> dict[str, Any]:
    row = {
        "variant": variant_name,
        "scenario": scenario_name,
        "feasible": False,
        "reached_target": False,
        "completed": False,
        "failure_stage": stage,
        "failure_reason": message,
        "algorithm_wall_sec": float(time.perf_counter() - start),
        "wall_sec": float(time.perf_counter() - start),
    }
    row.update(extra)
    return row


def _metrics_from_stage(
    variant_name: str,
    scenario_name: str,
    placement_res: dict[str, Any],
    target_state: Any,
    transition_res: dict[str, Any],
    wall_sec: float,
) -> dict[str, Any]:
    actions = list(transition_res.get("executed_actions", []))
    action_counts = _action_counts(actions)
    build_metrics = dict(target_state.metadata.get("build_metrics", {}))
    coarse_root_count = sum(
        len(iteration.get("chosen_roots", []))
        for iteration in transition_res.get("iterations", [])
    )
    return {
        "variant": variant_name,
        "scenario": scenario_name,
        "feasible": bool(placement_res.get("feasible")),
        "reached_target": bool(transition_res.get("reached_target")),
        "completed": bool(placement_res.get("feasible")) and bool(transition_res.get("reached_target")),
        "failure_stage": "" if transition_res.get("reached_target") else "transition",
        "failure_reason": "" if transition_res.get("reached_target") else "target state not reached within max iterations",
        "gpu_count": int(placement_res.get("gpu_count", len(target_state.real_gpus()))),
        "total_instances": int(placement_res.get("total_instances", 0) or 0),
        "total_slack": float(placement_res.get("total_slack", 0.0) or 0.0),
        "remaining_slots": int(placement_res.get("total_remaining_slots", 0) or 0),
        "chosen_templates": "+".join(str(x) for x in placement_res.get("chosen_templates", [])),
        "target_physical_templates": "+".join(str(x) for x in build_metrics.get("ordered_physical_templates", [])),
        "exact_preserve": int(build_metrics.get("exact_preserve", 0) or 0),
        "upgrade_preserve": int(build_metrics.get("upgrade_preserve", 0) or 0),
        "template_match_count": int(build_metrics.get("template_match_count", 0) or 0),
        "spread": int(build_metrics.get("spread", 0) or 0),
        "collocate_pairs": int(build_metrics.get("collocate_pairs", 0) or 0),
        "mixed_gpu_count": int(build_metrics.get("mixed_gpu_count", 0) or 0),
        "iteration_count": int(transition_res.get("iteration_count", 0) or 0),
        "coarse_root_action_count": int(coarse_root_count),
        "fine_action_count": len(actions),
        "action_count": len(actions),
        "configure_count": int(action_counts.get("configure_full_template", 0)),
        "clear_count": int(action_counts.get("clear_gpu", 0))
        + int(action_counts.get("clear_gpu_binding", 0))
        + int(action_counts.get("clear_template", 0)),
        "peak_active_gpu": int(transition_res.get("peak_active_gpu", 0) or 0),
        "source_active_gpu": int(transition_res.get("source_active_gpu", 0) or 0),
        "final_active_gpu": int(transition_res.get("final_active_gpu", 0) or 0),
        "placement_elapsed_sec": float(placement_res.get("elapsed", 0.0) or 0.0),
        "target_build_elapsed_sec": float(build_metrics.get("elapsed_time_sec", 0.0) or 0.0),
        "sim_transition_elapsed_sec": float(transition_res.get("elapsed_sec", 0.0) or 0.0),
        "estimated_hardware_sec": _estimate_seconds(actions),
        "algorithm_wall_sec": float(wall_sec),
        "wall_sec": float(wall_sec),
        "action_counts_json": json.dumps(action_counts, sort_keys=True),
    }


def run_variant(
    variant_name: str,
    scenario_paths: list[Path],
    max_iters: int,
    milp_time_limit_s: float | None,
    quiet: bool,
) -> list[dict[str, Any]]:
    spec = VARIANTS[variant_name]
    rows = []
    next_source = None
    for scenario_path in scenario_paths:
        scenario = load_planning_scenario(scenario_path)
        source_state = next_source or cluster_state_from_mock_yaml(load_yaml(scenario.source_state_ref))
        ensure_state_metadata(source_state)
        bootstrap_physical_ids_for_state(source_state)
        assert_valid_cluster_state(source_state)
        workload_names = [workload.name for workload in scenario.workloads]
        target_arrival = [float(workload.target_arrival) for workload in scenario.workloads]

        start = time.perf_counter()
        feasible_start = time.perf_counter()
        feasible_option_df = build_feasible_option_dataframe(scenario)
        feasible_elapsed_sec = time.perf_counter() - feasible_start
        try:
            placement_res = _call_quiet(
                spec["placement"],
                quiet=True,
                feasible_option_df=feasible_option_df,
                arrival_rate=target_arrival,
                n_workloads=len(workload_names),
                time_limit_s=milp_time_limit_s,
                verbose=False,
            )
        except Exception as exc:
            rows.append(
                _failure_row(
                    variant_name,
                    scenario.name,
                    "placement_exception",
                    f"{type(exc).__name__}: {exc}",
                    start,
                    feasible_option_elapsed_sec=float(feasible_elapsed_sec),
                )
            )
            break
        if not placement_res.get("feasible"):
            rows.append(
                _failure_row(
                    variant_name,
                    scenario.name,
                    "placement",
                    str(placement_res.get("status", "placement infeasible")),
                    start,
                    placement_elapsed_sec=float(placement_res.get("elapsed", 0.0) or 0.0),
                    feasible_option_elapsed_sec=float(feasible_elapsed_sec),
                )
            )
            break

        try:
            target_state = _call_quiet(
                spec["target"],
                quiet=quiet,
                milp_res=placement_res,
                prev_state=source_state,
                feasible_option_df=feasible_option_df,
                workload_names=workload_names,
                arrival_rate=target_arrival,
                verbose=False,
            )
        except Exception as exc:
            rows.append(
                _failure_row(
                    variant_name,
                    scenario.name,
                    "target_builder_exception",
                    f"{type(exc).__name__}: {exc}",
                    start,
                    placement_elapsed_sec=float(placement_res.get("elapsed", 0.0) or 0.0),
                    feasible_option_elapsed_sec=float(feasible_elapsed_sec),
                    chosen_templates="+".join(str(x) for x in placement_res.get("chosen_templates", [])),
                    gpu_count=int(placement_res.get("gpu_count", 0) or 0),
                )
            )
            break
        ensure_state_metadata(target_state)
        try:
            transition_res = spec["transition"](
                source_state=source_state,
                target_state=target_state,
                src_arrival=dict(scenario.source_arrival),
                tgt_arrival=dict(scenario.target_arrival),
                workload_names=workload_names,
                stage_name=scenario.name,
                max_iters=max_iters,
            )
        except Exception as exc:
            rows.append(
                _failure_row(
                    variant_name,
                    scenario.name,
                    "transition_exception",
                    f"{type(exc).__name__}: {exc}",
                    start,
                    placement_elapsed_sec=float(placement_res.get("elapsed", 0.0) or 0.0),
                    feasible_option_elapsed_sec=float(feasible_elapsed_sec),
                    chosen_templates="+".join(str(x) for x in placement_res.get("chosen_templates", [])),
                    gpu_count=int(placement_res.get("gpu_count", 0) or 0),
                    target_build_elapsed_sec=float(target_state.metadata.get("build_metrics", {}).get("elapsed_time_sec", 0.0) or 0.0),
                )
            )
            break
        wall_sec = time.perf_counter() - start
        row = _metrics_from_stage(
            variant_name=variant_name,
            scenario_name=scenario.name,
            placement_res=placement_res,
            target_state=target_state,
            transition_res=transition_res,
            wall_sec=wall_sec,
        )
        row["feasible_option_elapsed_sec"] = float(feasible_elapsed_sec)
        rows.append(row)
        if not bool(row.get("completed", False)):
            break
        next_source = canonicalize_state_for_next_round(transition_res["executed_state"])
    return rows


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    keys = sorted({key for row in rows for key in row.keys()})
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def _aggregate(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for variant in sorted({row["variant"] for row in rows}):
        subset = [row for row in rows if row["variant"] == variant]
        gpu_counts = [int(row.get("gpu_count", 0) or 0) for row in subset if row.get("gpu_count") not in (None, "")]
        failed = [row for row in subset if not bool(row.get("completed", row.get("reached_target", False)))]
        row_out = {
            "variant": variant,
            "stages": len(subset),
            "completed_all": len(subset) > 0 and all(bool(row.get("completed", row.get("reached_target", False))) for row in subset),
            "first_failed_stage": failed[0].get("scenario", "") if failed else "",
            "first_failure_phase": failed[0].get("failure_stage", "") if failed else "",
            "first_failure_reason": failed[0].get("failure_reason", failed[0].get("error", "")) if failed else "",
            "max_gpu_count": max(gpu_counts) if gpu_counts else 0,
            "iterations": sum(int(row.get("iteration_count", 0) or 0) for row in subset),
            "coarse_root_actions": sum(int(row.get("coarse_root_action_count", 0) or 0) for row in subset),
            "fine_actions": sum(int(row.get("fine_action_count", row.get("action_count", 0)) or 0) for row in subset),
            "configures": sum(int(row.get("configure_count", 0) or 0) for row in subset),
            "clears": sum(int(row.get("clear_count", 0) or 0) for row in subset),
            "estimated_hardware_sec": sum(float(row.get("estimated_hardware_sec", 0.0) or 0.0) for row in subset),
            "algorithm_wall_sec": sum(float(row.get("algorithm_wall_sec", row.get("wall_sec", 0.0)) or 0.0) for row in subset),
            "placement_elapsed_sec": sum(float(row.get("placement_elapsed_sec", 0.0) or 0.0) for row in subset),
            "target_build_elapsed_sec": sum(float(row.get("target_build_elapsed_sec", 0.0) or 0.0) for row in subset),
            "transition_sim_elapsed_sec": sum(float(row.get("sim_transition_elapsed_sec", 0.0) or 0.0) for row in subset),
        }
        for scenario in _scenario_names(subset):
            row_out[f"{scenario}_gpu_count"] = _stage_metric(subset, scenario, "gpu_count")
        out.append(row_out)
    return out


def _scenario_names(rows: list[dict[str, Any]]) -> list[str]:
    names = []
    seen = set()
    for row in rows:
        name = str(row.get("scenario", ""))
        if not name or name in seen:
            continue
        seen.add(name)
        names.append(name)
    return names


def _stage_metric(rows: list[dict[str, Any]], stage: str, key: str) -> Any:
    for row in rows:
        if row.get("scenario") == stage:
            return row.get(key, "")
    return ""


def _ordered_subset(rows: list[dict[str, Any]], variant_names: list[str]) -> list[dict[str, Any]]:
    by_variant = {row["variant"]: row for row in rows}
    return [by_variant[name] for name in variant_names if name in by_variant]


def _write_figures(aggregate_rows: list[dict[str, Any]], figure_dir: Path) -> list[Path]:
    figure_dir.mkdir(parents=True, exist_ok=True)
    figure_paths = []
    for metric, ylabel, filename in [
        ("estimated_hardware_sec", "Estimated hardware seconds", "ablation_estimated_hardware_sec.png"),
        ("iterations", "Transition iterations", "ablation_transition_iterations.png"),
        ("algorithm_wall_sec", "Planner algorithm seconds", "ablation_algorithm_wall_sec.png"),
        ("fine_actions", "Fine-grained executed actions", "ablation_action_count.png"),
    ]:
        figure_paths.append(_write_one_figure(aggregate_rows, metric, ylabel, figure_dir / filename))
        for group_name, variant_names in VARIANT_GROUPS.items():
            group_rows = _ordered_subset(aggregate_rows, variant_names)
            group_filename = filename.replace("ablation_", f"ablation_{group_name}_")
            figure_paths.append(
                _write_one_figure(
                    group_rows,
                    metric,
                    f"{GROUP_LABELS[group_name]} - {ylabel}",
                    figure_dir / group_filename,
                )
            )
    return figure_paths


def _write_one_figure(rows: list[dict[str, Any]], metric: str, ylabel: str, path: Path) -> Path:
    labels = [row["variant"] for row in rows]
    values = [float(row.get(metric, 0.0) or 0.0) for row in rows]
    try:
        import matplotlib.pyplot as plt

        width = max(7.0, min(13.0, 1.45 * max(1, len(labels))))
        plt.figure(figsize=(width, 4.8))
        plt.bar(labels, values, color="#4C78A8")
        plt.ylabel(ylabel)
        plt.xticks(rotation=25, ha="right")
        plt.tight_layout()
        plt.savefig(path, dpi=160)
        plt.close()
        return path
    except Exception:
        svg_path = path.with_suffix(".svg")
        _write_svg_bar_chart(svg_path, labels, values, ylabel)
        return svg_path


def _write_svg_bar_chart(path: Path, labels: list[str], values: list[float], ylabel: str) -> None:
    width = 1000
    height = 430
    left = 80
    bottom = 120
    top = 40
    chart_h = height - top - bottom
    chart_w = width - left - 40
    max_v = max(values) if values else 1.0
    max_v = max(max_v, 1.0)
    bar_gap = 18
    bar_w = max(16, int((chart_w - bar_gap * (len(labels) + 1)) / max(1, len(labels))))
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        f'<text x="{left}" y="24" font-family="sans-serif" font-size="18" fill="#222">{ylabel}</text>',
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + chart_h}" stroke="#333"/>',
        f'<line x1="{left}" y1="{top + chart_h}" x2="{left + chart_w}" y2="{top + chart_h}" stroke="#333"/>',
    ]
    for idx, (label, value) in enumerate(zip(labels, values)):
        x = left + bar_gap + idx * (bar_w + bar_gap)
        h = int(chart_h * (float(value) / max_v))
        y = top + chart_h - h
        parts.append(f'<rect x="{x}" y="{y}" width="{bar_w}" height="{h}" fill="#4C78A8"/>')
        parts.append(
            f'<text x="{x + bar_w / 2:.1f}" y="{max(top + 14, y - 6)}" text-anchor="middle" '
            f'font-family="sans-serif" font-size="12" fill="#222">{value:.1f}</text>'
        )
        parts.append(
            f'<text transform="translate({x + bar_w / 2:.1f},{top + chart_h + 18}) rotate(35)" '
            f'text-anchor="start" font-family="sans-serif" font-size="12" fill="#222">{label}</text>'
        )
    parts.append("</svg>")
    path.write_text("\n".join(parts), encoding="utf-8")


def _markdown_table(rows: list[dict[str, Any]], cols: list[str]) -> str:
    lines = ["| " + " | ".join(cols) + " |", "| " + " | ".join(["---"] * len(cols)) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(str(row.get(col, "")) for col in cols) + " |")
    return "\n".join(lines)


def _write_report(
    path: Path,
    rows: list[dict[str, Any]],
    aggregate_rows: list[dict[str, Any]],
    figure_paths: list[Path],
) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "# MIG Planner Ablation Report 2026-05-11 Round 1",
        "",
        f"Generated: {now}",
        "",
        "## Scope",
        "",
        "This round uses the existing stage0-stage3 chain and includes the phased/DAG action-plan representation as a transition ablation. "
        "The goal is to make the current stack modular and compare placement, target materialization, and transition-plan ablations with the same scenario inputs.",
        "",
        "## Variants",
        "",
    ]
    for name, spec in VARIANTS.items():
        lines.append(f"- `{name}`: {spec['description']}")
    lines.extend(
        [
            "",
            "## Aggregate Results",
            "",
            _markdown_table(
                aggregate_rows,
                [
                    "variant",
                    "all_reached",
                    "stage0_gpu_count",
                    "stage1_gpu_count",
                    "stage2_gpu_count",
                    "stage3_gpu_count",
                    "max_gpu_count",
                    "iterations",
                    "coarse_root_actions",
                    "fine_actions",
                    "configures",
                    "clears",
                    "algorithm_wall_sec",
                    "placement_elapsed_sec",
                    "target_build_elapsed_sec",
                    "transition_sim_elapsed_sec",
                    "estimated_hardware_sec",
                ],
            ),
            "",
            "## Per-Stage Resource And Cost Results",
            "",
            _markdown_table(
                rows,
                [
                    "variant",
                    "scenario",
                    "reached_target",
                    "gpu_count",
                    "total_instances",
                    "remaining_slots",
                    "placement_elapsed_sec",
                    "target_build_elapsed_sec",
                    "sim_transition_elapsed_sec",
                    "algorithm_wall_sec",
                    "estimated_hardware_sec",
                ],
            ),
            "",
            "## Per-Stage Transition Results",
            "",
            _markdown_table(
                rows,
                [
                    "variant",
                    "scenario",
                    "reached_target",
                    "exact_preserve",
                    "upgrade_preserve",
                    "iteration_count",
                    "coarse_root_action_count",
                    "fine_action_count",
                    "configure_count",
                    "clear_count",
                    "peak_active_gpu",
                ],
            ),
            "",
            "## Figures",
            "",
        ]
    )
    if figure_paths:
        for fig in figure_paths:
            lines.append(f"![{fig.stem}](figures/{fig.name})")
    else:
        lines.append("Matplotlib was not available; figures were skipped.")
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- `current_full` is the current phase-greedy baseline; `transition_phase_greedy_dag` keeps the same execution semantics but exposes phased/DAG action-plan structure.",
            "- `target_no_preserve` and `target_exact_milp_templates` isolate target-state materialization choices.",
            "- `transition_serial_root_baseline` and `transition_drain_aware_baseline` isolate transition ordering choices while keeping placement and target building fixed.",
            "- `estimated_hardware_sec` is a coarse estimate derived from the real single-server MIG benchmark constants, not a real hardware rerun.",
            "- The `simulated_annealing` placement module is present as an optional compatibility module but is not part of this first deterministic ablation matrix.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def _format_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def _html_table(rows: list[dict[str, Any]], cols: list[str], title: str) -> str:
    header = "".join(f"<th>{html.escape(col)}</th>" for col in cols)
    body = []
    for row in rows:
        classes = []
        if not bool(row.get("completed", row.get("reached_target", True))):
            classes.append("failed")
        cells = "".join(f"<td>{html.escape(_format_cell(row.get(col, '')))}</td>" for col in cols)
        class_attr = f' class="{" ".join(classes)}"' if classes else ""
        body.append(f"<tr{class_attr}>{cells}</tr>")
    return f"<section><h2>{html.escape(title)}</h2><table><thead><tr>{header}</tr></thead><tbody>{''.join(body)}</tbody></table></section>"


def _grouped_summary_tables(aggregate_rows: list[dict[str, Any]], summary_cols: list[str]) -> str:
    sections = []
    for group_name, variant_names in VARIANT_GROUPS.items():
        group_rows = _ordered_subset(aggregate_rows, variant_names)
        sections.append(_html_table(group_rows, summary_cols, GROUP_LABELS[group_name]))
        sections.append(_analysis_section(group_name, group_rows))
    return "\n".join(sections)


def _as_float(row: dict[str, Any], key: str) -> float:
    try:
        return float(row.get(key, 0.0) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _delta_text(value: float, base: float, suffix: str = "") -> str:
    delta = value - base
    sign = "+" if delta >= 0 else ""
    return f"{value:.2f}{suffix} ({sign}{delta:.2f}{suffix} vs current)"


def _analysis_section(group_name: str, group_rows: list[dict[str, Any]]) -> str:
    if not group_rows:
        return ""
    current = next((row for row in group_rows if row.get("variant") == "current_full"), group_rows[0])
    completed = [row for row in group_rows if bool(row.get("completed_all"))]
    failed = [row for row in group_rows if not bool(row.get("completed_all"))]
    fastest_algo = min(completed, key=lambda row: _as_float(row, "algorithm_wall_sec")) if completed else None
    lowest_hw = min(completed, key=lambda row: _as_float(row, "estimated_hardware_sec")) if completed else None
    fewest_actions = min(completed, key=lambda row: _as_float(row, "fine_actions")) if completed else None

    bullets = []
    bullets.append(
        f"本组共有 {len(group_rows)} 个变体，其中 {len(completed)} 个完成全部 stage，{len(failed)} 个未完成。"
    )
    if failed:
        failed_desc = "; ".join(
            f"{row['variant']} 在 {row.get('first_failed_stage', '?')} 失败：{row.get('first_failure_reason', '')}"
            for row in failed
        )
        bullets.append(f"未完成项主要用于暴露模块替换后的约束风险：{failed_desc}。")
    if fastest_algo is not None:
        bullets.append(
            f"算法端耗时最小的是 {fastest_algo['variant']}，"
            f"{_delta_text(_as_float(fastest_algo, 'algorithm_wall_sec'), _as_float(current, 'algorithm_wall_sec'), 's')}。"
        )
    if lowest_hw is not None:
        bullets.append(
            f"估算硬件重构时间最低的是 {lowest_hw['variant']}，"
            f"{_delta_text(_as_float(lowest_hw, 'estimated_hardware_sec'), _as_float(current, 'estimated_hardware_sec'), 's')}。"
        )
    if fewest_actions is not None:
        bullets.append(
            f"细粒度动作数最少的是 {fewest_actions['variant']}，"
            f"{_delta_text(_as_float(fewest_actions, 'fine_actions'), _as_float(current, 'fine_actions'))} 个动作。"
        )

    if group_name == "placement":
        bullets.append(
            "Placement 消融说明：这一组只替换第一阶段模板/实例选择，target builder 和 transition 保持 current。"
            "因此失败通常说明 placement 输出的模板组合虽然满足负载估计，但在后续物理池大小、保留策略或迁移过程里不可承受。"
        )
        if failed:
            bullets.append(
                "当前结果里存在未完成 placement 变体；需要结合 first_failed_stage 和 first_failure_reason "
                "区分 placement 不可行、物理 GPU 池耗尽、transition 无进展提前停止，以及真正达到迭代上限这几类原因。"
            )
        else:
            max_gpu_row = max(group_rows, key=lambda row: _as_float(row, "max_gpu_count"))
            slowest_algo = max(group_rows, key=lambda row: _as_float(row, "algorithm_wall_sec"))
            most_hw = max(group_rows, key=lambda row: _as_float(row, "estimated_hardware_sec"))
            bullets.append(
                "当前结果里所有 placement 变体都完成全部 stage；差异主要体现在 GPU 数、动作数、算法耗时和估算硬件重构时间。"
            )
            bullets.append(
                f"GPU 使用峰值最高的是 {max_gpu_row['variant']}，max_gpu_count={_as_float(max_gpu_row, 'max_gpu_count'):.0f}；"
                "这说明该 placement baseline 倾向用更多 GPU 换取简单可行的模板组合。"
            )
            bullets.append(
                f"算法端最慢的是 {slowest_algo['variant']}，"
                f"{_delta_text(_as_float(slowest_algo, 'algorithm_wall_sec'), _as_float(current, 'algorithm_wall_sec'), 's')}；"
                f"估算硬件重构时间最高的是 {most_hw['variant']}，"
                f"{_delta_text(_as_float(most_hw, 'estimated_hardware_sec'), _as_float(current, 'estimated_hardware_sec'), 's')}。"
            )
    elif group_name == "target":
        bullets.append(
            "Target builder 消融说明：这一组固定 current placement 和 current transition，只比较 MILP 结果如何被物化为具体 GPU/slot 布局。"
            "因此这里的动作数和估算硬件时间最能体现 target materialization 对迁移成本的影响。"
        )
        bullets.append(
            "`target_exact_milp_templates` 不是完全跳过 target builder；它直接采用 MILP 的 abstract templates，"
            "为每个模板选择第一个合法 physical realization，并用 repair_rounds=0 的最小 greedy 填 slot。"
            "它不做候选模板搜索、不使用 prev_state、不做 preserve rewrite，所以是“直接物化 MILP 模板”的 baseline。"
        )
        bullets.append(
            "`target_beam_preserve` 完成全部 stage，但 target build 时间显著更高；这说明 notebook beam 搜索扩大了布局搜索空间，"
            "可作为质量/成本权衡 baseline，而不适合直接当低延迟在线路径。"
        )
    elif group_name == "transition":
        bullets.append(
            "Transition 消融说明：这一组固定 current placement 和 target builder，只改变动作执行/排序策略。"
            "因此如果 fine actions 和 estimated hardware seconds 接近，说明它们最终执行了相似的重构集合，差异主要体现在迭代轮数和并行批处理粒度。"
        )
        bullets.append(
            "当前结果里 `transition_full_plan_baseline` 迭代数最少，`transition_serial_root_baseline` 迭代数最多；"
            "但硬件估算时间几乎相同，说明现有估算模型还没有把跨 GPU 并行执行收益纳入 makespan，只是在累加动作成本。"
        )

    comparison_cols = [
        "variant",
        "completed_all",
        "algorithm_wall_sec",
        "estimated_hardware_sec",
        "fine_actions",
        "iterations",
        "first_failed_stage",
        "first_failure_reason",
    ]
    bullet_html = "".join(f"<li>{html.escape(text)}</li>" for text in bullets)
    return (
        f'<section class="analysis"><h3>{html.escape(GROUP_LABELS[group_name])}分析</h3>'
        f"<ul>{bullet_html}</ul>"
        f"{_html_table(group_rows, comparison_cols, GROUP_LABELS[group_name] + '关键对比')}</section>"
    )


def _variant_stage_tables(rows: list[dict[str, Any]]) -> str:
    sections = []
    cols = [
        "scenario",
        "completed",
        "failure_stage",
        "failure_reason",
        "gpu_count",
        "chosen_templates",
        "target_physical_templates",
        "iteration_count",
        "coarse_root_action_count",
        "fine_action_count",
        "configure_count",
        "clear_count",
        "placement_elapsed_sec",
        "target_build_elapsed_sec",
        "sim_transition_elapsed_sec",
        "algorithm_wall_sec",
        "estimated_hardware_sec",
    ]
    for variant in sorted({row["variant"] for row in rows}):
        subset = [row for row in rows if row["variant"] == variant]
        sections.append(_html_table(subset, cols, f"{variant}: per-stage metrics"))
    return "\n".join(sections)


def _grouped_stage_tables(rows: list[dict[str, Any]]) -> str:
    sections = []
    cols = [
        "variant",
        "scenario",
        "completed",
        "failure_stage",
        "failure_reason",
        "gpu_count",
        "iteration_count",
        "fine_action_count",
        "configure_count",
        "clear_count",
        "placement_elapsed_sec",
        "target_build_elapsed_sec",
        "sim_transition_elapsed_sec",
        "algorithm_wall_sec",
        "estimated_hardware_sec",
    ]
    for group_name, variant_names in VARIANT_GROUPS.items():
        group_rows = [row for row in rows if row.get("variant") in set(variant_names)]
        group_rows.sort(key=lambda row: (variant_names.index(row["variant"]), row.get("scenario", "")))
        sections.append(_html_table(group_rows, cols, f"{GROUP_LABELS[group_name]}: 每阶段结果"))
    return "\n".join(sections)


def _figures_by_group(figure_paths: list[Path]) -> dict[str, list[Path]]:
    out = {"all": []}
    out.update({name: [] for name in VARIANT_GROUPS})
    for path in figure_paths:
        name = path.name
        matched = False
        for group_name in VARIANT_GROUPS:
            if name.startswith(f"ablation_{group_name}_"):
                out[group_name].append(path)
                matched = True
                break
        if not matched:
            out["all"].append(path)
    return out


def _figure_section(figure_paths: list[Path], output_dir: Path) -> str:
    figures = _figures_by_group(figure_paths)
    parts = ["<section><h2>图表</h2>"]
    labels = {"all": "全体总览", **GROUP_LABELS}
    for group_name in ["all", "placement", "target", "transition"]:
        if not figures[group_name]:
            continue
        parts.append(f"<h3>{html.escape(labels[group_name])}</h3>")
        for fig in figures[group_name]:
            rel = fig.relative_to(output_dir)
            parts.append(
                f'<figure><img src="{html.escape(str(rel))}" alt="{html.escape(fig.stem)}">'
                f"<figcaption>{html.escape(fig.stem)}</figcaption></figure>"
            )
    parts.append("</section>")
    return "\n".join(parts)


def _summary_columns(rows: list[dict[str, Any]]) -> list[str]:
    return [
        "variant",
        "completed_all",
        "first_failed_stage",
        "first_failure_phase",
        "first_failure_reason",
        "max_gpu_count",
        *[f"{scenario}_gpu_count" for scenario in _scenario_names(rows)],
        "iterations",
        "coarse_root_actions",
        "fine_actions",
        "configures",
        "clears",
        "placement_elapsed_sec",
        "target_build_elapsed_sec",
        "transition_sim_elapsed_sec",
        "algorithm_wall_sec",
        "estimated_hardware_sec",
    ]


def _write_html_report(
    path: Path,
    rows: list[dict[str, Any]],
    aggregate_rows: list[dict[str, Any]],
    figure_paths: list[Path],
    output_dir: Path,
) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    summary_cols = _summary_columns(rows)
    failures = [row for row in rows if not bool(row.get("completed", row.get("reached_target", False)))]
    failure_cols = ["variant", "scenario", "failure_stage", "failure_reason", "chosen_templates", "gpu_count", "algorithm_wall_sec"]
    variant_list = "".join(
        f"<li><code>{html.escape(name)}</code>: {html.escape(spec['description'])}</li>"
        for name, spec in VARIANTS.items()
    )
    grouped_variant_html = ""
    for group_name, variant_names in VARIANT_GROUPS.items():
        items = "".join(
            f"<li><code>{html.escape(name)}</code>: {html.escape(VARIANTS[name]['description'])}</li>"
            for name in variant_names
            if name in VARIANTS
        )
        grouped_variant_html += f"<h3>{html.escape(GROUP_LABELS[group_name])}</h3><ul>{items}</ul>"
    doc = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>MIG Planner 消融实验报告</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 28px; color: #1f2933; }}
    h1 {{ margin-bottom: 0.2rem; }}
    h2 {{ margin-top: 2rem; }}
    h3 {{ margin-top: 1.4rem; }}
    table {{ border-collapse: collapse; width: 100%; margin: 0.8rem 0 1.6rem; font-size: 13px; }}
    th, td {{ border: 1px solid #d8dee9; padding: 6px 8px; vertical-align: top; }}
    th {{ background: #eef2f7; position: sticky; top: 0; }}
    tr.failed {{ background: #fff1f2; }}
    code {{ background: #eef2f7; padding: 1px 4px; border-radius: 4px; }}
    .note {{ background: #f8fafc; border-left: 4px solid #4c78a8; padding: 10px 14px; }}
    .analysis {{ background: #fbfdff; border: 1px solid #d8e3ef; padding: 8px 14px; margin: 0.8rem 0 1.6rem; }}
    .analysis h3 {{ margin-top: 0.4rem; }}
    .analysis ul {{ margin-top: 0.4rem; }}
    figure {{ display: inline-block; margin: 14px 18px 20px 0; max-width: 520px; }}
    img {{ max-width: 520px; height: auto; border: 1px solid #d8dee9; }}
  </style>
</head>
<body>
  <h1>MIG Planner 消融实验报告</h1>
  <p>生成时间：{html.escape(now)}</p>
  <div class="note">
    本轮重新纳入 original MILP、greedy two-phase、simulated annealing、target builder 与 transition planner 的消融变体。
    同级 baseline 不读取 current 算法结果；失败项会保留失败阶段和原因。
  </div>
  <h2>变体说明</h2>
  {grouped_variant_html}
  <details><summary>全部变体平铺列表</summary><ul>{variant_list}</ul></details>
  <h2>汇总结果</h2>
  {_grouped_summary_tables(aggregate_rows, summary_cols)}
  {_html_table(aggregate_rows, summary_cols, "全体汇总")}
  <h2>每阶段结果</h2>
  {_grouped_stage_tables(rows)}
  <details><summary>按单个变体展开每阶段明细</summary>{_variant_stage_tables(rows)}</details>
  {_html_table(failures, failure_cols, "未完成项") if failures else "<section><h2>未完成项</h2><p>全部变体完成所有已执行阶段。</p></section>"}
  {_figure_section(figure_paths, output_dir)}
</body>
</html>
"""
    path.write_text(doc, encoding="utf-8")


def _xlsx_col_name(idx: int) -> str:
    name = ""
    while idx:
        idx, rem = divmod(idx - 1, 26)
        name = chr(65 + rem) + name
    return name


def _xlsx_sheet_xml(rows: list[list[Any]]) -> str:
    row_xml = []
    for r_idx, row in enumerate(rows, start=1):
        cells = []
        for c_idx, value in enumerate(row, start=1):
            ref = f"{_xlsx_col_name(c_idx)}{r_idx}"
            text = xml_escape(_format_cell(value))
            cells.append(f'<c r="{ref}" t="inlineStr"><is><t>{text}</t></is></c>')
        row_xml.append(f'<row r="{r_idx}">{"".join(cells)}</row>')
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<sheetData>{"".join(row_xml)}</sheetData></worksheet>'
    )


def _write_xlsx(path: Path, sheets: dict[str, tuple[list[str], list[dict[str, Any]]]]) -> None:
    safe_names = []
    for idx, name in enumerate(sheets, start=1):
        clean = "".join(ch for ch in name if ch not in "[]:*?/\\")[:31] or f"Sheet{idx}"
        safe_names.append(clean)
    workbook_sheets = "".join(
        f'<sheet name="{xml_escape(name)}" sheetId="{idx}" r:id="rId{idx}"/>'
        for idx, name in enumerate(safe_names, start=1)
    )
    rels = "".join(
        f'<Relationship Id="rId{idx}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{idx}.xml"/>'
        for idx in range(1, len(sheets) + 1)
    )
    rels += f'<Relationship Id="rId{len(sheets) + 1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'
    overrides = "".join(
        f'<Override PartName="/xl/worksheets/sheet{idx}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        for idx in range(1, len(sheets) + 1)
    )
    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
        f"{overrides}</Types>"
    )
    workbook = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        f"<sheets>{workbook_sheets}</sheets></workbook>"
    )
    styles = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>'
        '<fills count="1"><fill><patternFill patternType="none"/></fill></fills>'
        '<borders count="1"><border/></borders>'
        '<cellStyleXfs count="1"><xf/></cellStyleXfs>'
        '<cellXfs count="1"><xf xfId="0"/></cellXfs>'
        '</styleSheet>'
    )
    root_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
        '</Relationships>'
    )
    workbook_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">{rels}</Relationships>'
    )
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", content_types)
        zf.writestr("_rels/.rels", root_rels)
        zf.writestr("xl/workbook.xml", workbook)
        zf.writestr("xl/_rels/workbook.xml.rels", workbook_rels)
        zf.writestr("xl/styles.xml", styles)
        for idx, (headers, data_rows) in enumerate(sheets.values(), start=1):
            table = [headers]
            table.extend([[row.get(header, "") for header in headers] for row in data_rows])
            zf.writestr(f"xl/worksheets/sheet{idx}.xml", _xlsx_sheet_xml(table))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scenarios",
        nargs="*",
        default=[
            str(ROOT / "mock/scenarios/stage0.yaml"),
            str(ROOT / "mock/scenarios/stage1.yaml"),
            str(ROOT / "mock/scenarios/stage2.yaml"),
            str(ROOT / "mock/scenarios/stage3.yaml"),
        ],
    )
    parser.add_argument("--variants", nargs="*", default=list(VARIANTS.keys()))
    parser.add_argument("--max-iters", type=int, default=30)
    parser.add_argument("--milp-time-limit-s", type=float, default=30.0)
    parser.add_argument("--quiet", action="store_true", default=True)
    parser.add_argument("--output-prefix", default="ablation-2026-05-11-expanded")
    args = parser.parse_args()

    output_dir = REPORT_DIR / args.output_prefix
    figure_dir = output_dir / "figures"
    output_dir.mkdir(parents=True, exist_ok=True)
    scenario_paths = [Path(path) for path in args.scenarios]
    rows = []
    for variant in args.variants:
        if variant not in VARIANTS:
            raise ValueError(f"Unknown variant {variant}; choices={sorted(VARIANTS)}")
        print(f"[ablation] running {variant}", flush=True)
        rows.extend(
            run_variant(
                variant_name=variant,
                scenario_paths=scenario_paths,
                max_iters=args.max_iters,
                milp_time_limit_s=args.milp_time_limit_s,
                quiet=bool(args.quiet),
            )
        )

    aggregate_rows = _aggregate(rows)
    csv_path = output_dir / "ablation-results.csv"
    agg_csv_path = output_dir / "summary.csv"
    json_path = output_dir / "ablation-results.json"
    report_path = output_dir / "index.html"
    xlsx_path = output_dir / "ablation-results.xlsx"
    _write_csv(csv_path, rows)
    _write_csv(agg_csv_path, aggregate_rows)
    json_path.write_text(json.dumps({"rows": rows, "aggregate": aggregate_rows}, indent=2), encoding="utf-8")
    figures = _write_figures(aggregate_rows, figure_dir)
    _write_html_report(report_path, rows, aggregate_rows, figures, output_dir)
    summary_headers = _summary_columns(rows)
    stage_headers = [
        "variant",
        "scenario",
        "completed",
        "feasible",
        "reached_target",
        "failure_stage",
        "failure_reason",
        "gpu_count",
        "chosen_templates",
        "target_physical_templates",
        "total_instances",
        "remaining_slots",
        "exact_preserve",
        "upgrade_preserve",
        "iteration_count",
        "coarse_root_action_count",
        "fine_action_count",
        "configure_count",
        "clear_count",
        "peak_active_gpu",
        "placement_elapsed_sec",
        "target_build_elapsed_sec",
        "sim_transition_elapsed_sec",
        "algorithm_wall_sec",
        "estimated_hardware_sec",
        "action_counts_json",
    ]
    failures = [row for row in rows if not bool(row.get("completed", row.get("reached_target", False)))]
    failure_headers = ["variant", "scenario", "failure_stage", "failure_reason", "chosen_templates", "gpu_count", "algorithm_wall_sec"]
    _write_xlsx(
        xlsx_path,
        {
            "summary": (summary_headers, aggregate_rows),
            "stage_metrics": (stage_headers, rows),
            "failures": (failure_headers, failures),
            "summary_placement": (summary_headers, _ordered_subset(aggregate_rows, VARIANT_GROUPS["placement"])),
            "summary_target": (summary_headers, _ordered_subset(aggregate_rows, VARIANT_GROUPS["target"])),
            "summary_transition": (summary_headers, _ordered_subset(aggregate_rows, VARIANT_GROUPS["transition"])),
        },
    )
    print(f"[ablation] wrote {report_path}")
    print(f"[ablation] wrote {xlsx_path}")
    print(f"[ablation] wrote {csv_path}")
    print(f"[ablation] wrote {agg_csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
