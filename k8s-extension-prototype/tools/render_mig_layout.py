#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
CONTROLLER = ROOT / "controller"
for path in (ROOT, CONTROLLER):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from io_utils import load_yaml  # noqa: E402
from planning.k8s_adapter import (  # noqa: E402
    cluster_state_from_dict,
    cluster_state_from_mock_yaml,
    cluster_state_to_dict,
    plan_scenario_as_migplan_status,
)
from observe.observed_state_adapter import cluster_state_from_observed_cluster_state  # noqa: E402
from scenario_loader import load_planning_scenario  # noqa: E402
from migrant_core.physical_ids import bootstrap_physical_ids_for_state, ensure_state_metadata  # noqa: E402
from migrant_core.state import ClusterState, GPUState, MigInstance  # noqa: E402


SLICE_COUNT = 7


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Render MIGRANT ClusterState or MigPlan states as paper-style SVG layouts."
    )
    parser.add_argument("--state-yaml", help="YAML file containing a mock state, ObservedClusterState, or MigPlan.")
    parser.add_argument("--state-key", default="targetState", help="MigPlan status key to render when --state-yaml is a MigPlan.")
    parser.add_argument("--plan-scenario", help="PlanningScenario YAML. Emits source, target, and executed SVGs.")
    parser.add_argument("--current-state-yaml", help="Observed/current state YAML to render instead of the scenario source state.")
    parser.add_argument("--current-state-key", default="executedState", help="MigPlan status key used with --current-state-yaml.")
    parser.add_argument("--actual-state-yaml", help="Observed post-execution state YAML to render and compare with target.")
    parser.add_argument("--actual-state-key", default="executedState", help="MigPlan status key used with --actual-state-yaml.")
    parser.add_argument("--include-iterations", action="store_true", help="Render planner iteration before/after states when available.")
    parser.add_argument("--skip-match-report", action="store_true", help="Do not write exact target-vs-executed match reports.")
    parser.add_argument("--force-transition", action="store_true", help="Render the explicit transition even if current-state feasibility would no-op.")
    parser.add_argument("--planner", default="phase_greedy_with_dag_output", help="Planner used with --plan-scenario.")
    parser.add_argument("--title", default=None, help="Figure title. Defaults to state/scenario name.")
    parser.add_argument("--output", help="Output SVG for --state-yaml.")
    parser.add_argument("--output-dir", default=str(ROOT / "reports" / "layout-figures"), help="Output directory for --plan-scenario.")
    parser.add_argument("--columns", type=int, default=3, help="GPU panels per row.")
    parser.add_argument(
        "--gpu-alias",
        action="append",
        default=[],
        help="Override GPU title alias, e.g. --gpu-alias 0=rtx1gpu0. Can be repeated.",
    )
    args = parser.parse_args()

    logical_aliases, physical_aliases = _parse_aliases(args.gpu_alias)
    if args.plan_scenario:
        outputs = render_scenario(
            scenario_path=Path(args.plan_scenario),
            planner=args.planner,
            output_dir=Path(args.output_dir),
            current_state_path=Path(args.current_state_yaml) if args.current_state_yaml else None,
            current_state_key=args.current_state_key,
            actual_state_path=Path(args.actual_state_yaml) if args.actual_state_yaml else None,
            actual_state_key=args.actual_state_key,
            include_iterations=args.include_iterations,
            force_transition=args.force_transition,
            write_match_report=not args.skip_match_report,
            logical_aliases=logical_aliases,
            physical_aliases=physical_aliases,
            columns=args.columns,
        )
        for output in outputs:
            print(output)
        return

    if not args.state_yaml or not args.output:
        raise SystemExit("Use --state-yaml with --output, or use --plan-scenario with --output-dir.")

    obj = load_yaml(args.state_yaml)
    state = state_from_yaml_object(obj, state_key=args.state_key)
    title = args.title or _default_title(obj, args.state_key)
    svg = render_cluster_state_svg(
        state,
        title=title,
        logical_gpu_aliases=logical_aliases,
        physical_gpu_aliases=physical_aliases,
        columns=args.columns,
    )
    Path(args.output).write_text(svg, encoding="utf-8")
    print(args.output)


def render_scenario(
    *,
    scenario_path: Path,
    planner: str,
    output_dir: Path,
    current_state_path: Path | None,
    current_state_key: str,
    actual_state_path: Path | None,
    actual_state_key: str,
    include_iterations: bool,
    force_transition: bool,
    write_match_report: bool,
    logical_aliases: dict[int, str],
    physical_aliases: dict[str, str],
    columns: int,
) -> list[Path]:
    scenario = load_planning_scenario(scenario_path)
    scenario.transition["transitionPlanner"] = planner
    if force_transition:
        scenario.transition["forceReplan"] = True
    if current_state_path is not None:
        current_state = state_from_yaml_object(load_yaml(current_state_path), state_key=current_state_key)
        source_override = current_state
    else:
        source_override = _chain_source_override(scenario_path, scenario, planner, force_transition=force_transition)
        if source_override is not None:
            current_state = source_override
        else:
            current_state = cluster_state_from_mock_yaml(load_yaml(scenario.source_state_ref))
            ensure_state_metadata(current_state)
            bootstrap_physical_ids_for_state(current_state)
    status = plan_scenario_as_migplan_status(scenario, source_state_override=source_override)

    target_state = cluster_state_from_dict(status["status"]["targetState"])
    executed_state = cluster_state_from_dict(status["status"]["executedState"])
    _inherit_missing_physical_metadata(target_state, executed_state)
    actual_state = (
        state_from_yaml_object(load_yaml(actual_state_path), state_key=actual_state_key)
        if actual_state_path is not None
        else None
    )
    if actual_state is not None:
        _inherit_missing_physical_metadata(actual_state, target_state)
    state_specs = [
        ("current", f"{scenario.name} current", current_state),
        ("target", f"{scenario.name} target", target_state),
        ("planned-executed", f"{scenario.name} planned executed", executed_state),
    ]
    if actual_state is not None:
        state_specs.append(("actual-executed", f"{scenario.name} actual executed", actual_state))
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs = []
    for suffix, title, state in state_specs:
        path = output_dir / f"{scenario.name}-{suffix}-layout.svg"
        path.write_text(
            render_cluster_state_svg(
                state,
                title=title,
                logical_gpu_aliases=logical_aliases,
                physical_gpu_aliases=physical_aliases,
                columns=columns,
            ),
            encoding="utf-8",
        )
        outputs.append(path)
    if include_iterations:
        outputs.extend(
            render_iteration_layouts(
                status=status,
                output_dir=output_dir,
                scenario_name=scenario.name,
                target_state=target_state,
                logical_aliases=logical_aliases,
                physical_aliases=physical_aliases,
                columns=columns,
            )
        )
    if write_match_report:
        outputs.extend(
            write_exact_match_reports(
                output_dir=output_dir,
                target_state=target_state,
                planned_executed_state=executed_state,
                actual_state=actual_state,
            )
        )
    return outputs


def _chain_source_override(
    scenario_path: Path,
    scenario: Any,
    planner: str,
    *,
    force_transition: bool = False,
) -> ClusterState | None:
    source_ref = Path(str(scenario.source_state_ref))
    if source_ref.exists():
        return None
    name = str(scenario.name)
    if not name.startswith("stage"):
        return None
    try:
        stage_idx = int(name.removeprefix("stage"))
    except ValueError:
        return None
    if stage_idx <= 0:
        return None

    next_source = None
    for idx in range(stage_idx):
        prior_path = scenario_path.parent / f"stage{idx}.yaml"
        if not prior_path.exists():
            return None
        prior = load_planning_scenario(prior_path)
        prior.transition["transitionPlanner"] = planner
        if force_transition:
            prior.transition["forceReplan"] = True
        prior_status = plan_scenario_as_migplan_status(prior, source_state_override=next_source)
        next_source = cluster_state_from_dict(prior_status["status"]["canonicalNextState"])
    return next_source


def render_iteration_layouts(
    *,
    status: dict[str, Any],
    output_dir: Path,
    scenario_name: str,
    target_state: ClusterState,
    logical_aliases: dict[int, str],
    physical_aliases: dict[str, str],
    columns: int,
) -> list[Path]:
    outputs = []
    iterations = (
        dict(status.get("status", {}))
        .get("planningTrace", {})
        .get("transition", {})
        .get("iterations", [])
    )
    for iteration in list(iterations):
        idx = int(iteration.get("iteration", 0))
        for key, label in (("stateBefore", "before"), ("stateAfter", "after")):
            raw_state = iteration.get(key)
            if not raw_state:
                continue
            state = cluster_state_from_dict(raw_state)
            _inherit_missing_physical_metadata(state, target_state)
            path = output_dir / f"{scenario_name}-iteration-{idx:02d}-{label}-layout.svg"
            path.write_text(
                render_cluster_state_svg(
                    state,
                    title=f"{scenario_name} iteration {idx:02d} {label}",
                    logical_gpu_aliases=logical_aliases,
                    physical_gpu_aliases=physical_aliases,
                    columns=columns,
                ),
                encoding="utf-8",
            )
            outputs.append(path)
    return outputs


def write_exact_match_reports(
    *,
    output_dir: Path,
    target_state: ClusterState,
    planned_executed_state: ClusterState,
    actual_state: ClusterState | None,
) -> list[Path]:
    comparisons = {
        "target_vs_planned_executed": compare_exact_layout(target_state, planned_executed_state),
    }
    if actual_state is not None:
        comparisons["target_vs_actual_executed"] = compare_exact_layout(target_state, actual_state)
    json_path = output_dir / "exact-layout-match.json"
    md_path = output_dir / "exact-layout-match.md"
    json_path.write_text(json.dumps(comparisons, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(_match_report_markdown(comparisons), encoding="utf-8")
    return [json_path, md_path]


def state_from_yaml_object(obj: dict[str, Any], state_key: str) -> ClusterState:
    kind = str(obj.get("kind", ""))
    if kind == "MigPlan":
        return cluster_state_from_dict(dict(obj.get("status", {}))[state_key])
    if kind == "ObservedClusterState":
        return cluster_state_from_observed_cluster_state(obj)
    if "gpus" in obj:
        state = cluster_state_from_mock_yaml(obj)
        ensure_state_metadata(state)
        bootstrap_physical_ids_for_state(state)
        return state
    if "metadata" in obj and "gpus" in dict(obj.get("status", {})).get(state_key, {}):
        return cluster_state_from_dict(dict(obj.get("status", {}))[state_key])
    raise ValueError("Unsupported state YAML. Expected mock state, ObservedClusterState, or MigPlan.")


def _inherit_missing_physical_metadata(state: ClusterState, fallback: ClusterState) -> None:
    ensure_state_metadata(state)
    ensure_state_metadata(fallback)
    if not state.metadata.get("physical_id_map") and fallback.metadata.get("physical_id_map"):
        state.metadata["physical_id_map"] = dict(fallback.metadata["physical_id_map"])
    if not state.metadata.get("physicalGpuBindings") and fallback.metadata.get("physicalGpuBindings"):
        state.metadata["physicalGpuBindings"] = dict(fallback.metadata["physicalGpuBindings"])


def compare_exact_layout(target: ClusterState, actual: ClusterState) -> dict[str, Any]:
    target_rows = _layout_rows(target)
    actual_rows = _layout_rows(actual)
    target_set = {_row_key(row) for row in target_rows}
    actual_set = {_row_key(row) for row in actual_rows}
    missing = [row for row in target_rows if _row_key(row) not in actual_set]
    unexpected = [row for row in actual_rows if _row_key(row) not in target_set]
    return {
        "match": not missing and not unexpected,
        "targetInstanceCount": len(target_rows),
        "actualInstanceCount": len(actual_rows),
        "missingFromActual": missing,
        "unexpectedInActual": unexpected,
    }


def _layout_rows(state: ClusterState) -> list[dict[str, Any]]:
    ensure_state_metadata(state)
    rows = []
    physical_map = dict(state.metadata.get("physical_id_map", {}))
    for gpu in sorted(state.real_gpus(), key=lambda item: int(item.gpu_id)):
        gpu_id = int(gpu.gpu_id)
        physical_id = physical_map.get(gpu_id)
        if physical_id is None:
            physical_id = physical_map.get(str(gpu_id))
        for inst in sorted(gpu.instances, key=lambda item: (item.start, item.end, item.profile)):
            if inst.profile == "void":
                continue
            rows.append(
                {
                    "physicalGpuId": str(physical_id) if physical_id is not None else None,
                    "gpuId": gpu_id,
                    "start": int(inst.start),
                    "end": int(inst.end),
                    "profile": inst.profile,
                    "workload": inst.workload,
                    "batch": inst.batch,
                }
            )
    return rows


def _row_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("physicalGpuId"),
        int(row.get("start", 0)),
        int(row.get("end", 0)),
        str(row.get("profile")),
        row.get("workload"),
        row.get("batch"),
    )


def _match_report_markdown(comparisons: dict[str, Any]) -> str:
    lines = ["# Exact Layout Match Report", ""]
    for name, result in comparisons.items():
        lines.append(f"## {name}")
        lines.append("")
        lines.append(f"- match: `{str(bool(result['match'])).lower()}`")
        lines.append(f"- target instances: `{result['targetInstanceCount']}`")
        lines.append(f"- actual instances: `{result['actualInstanceCount']}`")
        if result["missingFromActual"]:
            lines.append("- missingFromActual:")
            for row in result["missingFromActual"]:
                lines.append(f"  - `{_row_label(row)}`")
        if result["unexpectedInActual"]:
            lines.append("- unexpectedInActual:")
            for row in result["unexpectedInActual"]:
                lines.append(f"  - `{_row_label(row)}`")
        if not result["missingFromActual"] and not result["unexpectedInActual"]:
            lines.append("- differences: none")
        lines.append("")
    return "\n".join(lines)


def _row_label(row: dict[str, Any]) -> str:
    return (
        f"{row.get('physicalGpuId')}:"
        f"{row.get('start')}-{row.get('end')}:{row.get('profile')}:"
        f"{row.get('workload')}:bs={row.get('batch')}"
    )


def render_cluster_state_svg(
    state: ClusterState,
    *,
    title: str,
    logical_gpu_aliases: dict[int, str] | None = None,
    physical_gpu_aliases: dict[str, str] | None = None,
    columns: int = 3,
) -> str:
    logical_gpu_aliases = logical_gpu_aliases or {}
    physical_gpu_aliases = physical_gpu_aliases or {}
    ensure_state_metadata(state)
    gpus = sorted(state.real_gpus(), key=lambda gpu: int(gpu.gpu_id))
    if not gpus:
        gpus = [GPUState(gpu_id=0, instances=[MigInstance(start=0, end=7, profile="void")])]

    panel_w = 300
    panel_h = 245
    margin_x = 42
    margin_y = 70
    title_h = 40
    columns = max(1, int(columns))
    rows = (len(gpus) + columns - 1) // columns
    width = margin_x * 2 + panel_w * min(columns, len(gpus))
    height = title_h + margin_y + panel_h * rows

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        "<defs>",
        '<pattern id="free-hatch" patternUnits="userSpaceOnUse" width="8" height="8" patternTransform="rotate(45)">',
        '<line x1="0" y1="0" x2="0" y2="8" stroke="#111827" stroke-width="2"/>',
        "</pattern>",
        "</defs>",
        '<rect x="0" y="0" width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="24" y="30" font-family="Arial, sans-serif" font-size="24" fill="#111827">{_esc(title)}</text>',
    ]

    for idx, gpu in enumerate(gpus):
        col = idx % columns
        row = idx // columns
        x = margin_x + col * panel_w
        y = title_h + margin_y + row * panel_h
        parts.extend(_render_gpu_panel(state, gpu, x, y, panel_w, logical_gpu_aliases, physical_gpu_aliases))

    parts.append("</svg>")
    return "\n".join(parts) + "\n"


def _render_gpu_panel(
    state: ClusterState,
    gpu: GPUState,
    x: int,
    y: int,
    panel_w: int,
    logical_gpu_aliases: dict[int, str],
    physical_gpu_aliases: dict[str, str],
) -> list[str]:
    box_x = x + 10
    box_y = y + 30
    box_w = panel_w - 35
    box_h = 125
    gpu.sort_instances()
    title = _gpu_title(state, gpu, logical_gpu_aliases, physical_gpu_aliases)
    template = _template_for_gpu(gpu)
    parts = [
        f'<text x="{box_x + box_w / 2:.1f}" y="{y + 22}" text-anchor="middle" font-family="Arial, sans-serif" font-size="18" fill="#111827">{_esc(title)}</text>',
        f'<rect x="{box_x}" y="{box_y}" width="{box_w}" height="{box_h}" fill="#ffffff" stroke="#111827" stroke-width="2"/>',
    ]
    for inst in gpu.instances:
        inst_x = box_x + box_w * inst.start / SLICE_COUNT
        inst_w = box_w * (inst.end - inst.start) / SLICE_COUNT
        fill = 'url(#free-hatch)' if _is_free(inst) else "#ffffff"
        parts.append(
            f'<rect x="{inst_x:.2f}" y="{box_y}" width="{inst_w:.2f}" height="{box_h}" fill="{fill}" stroke="#111827" stroke-width="1.5"/>'
        )
        label_lines = _instance_label_lines(inst)
        font_size = _font_size_for_width(inst_w)
        line_y = box_y + box_h / 2 - (len(label_lines) - 1) * (font_size + 1) / 2
        for line in label_lines:
            parts.append(
                f'<text x="{inst_x + inst_w / 2:.2f}" y="{line_y:.2f}" text-anchor="middle" dominant-baseline="middle" '
                f'font-family="Arial, sans-serif" font-size="{font_size}" fill="#111827">{_esc(line)}</text>'
            )
            line_y += font_size + 3
    parts.append(
        f'<text x="{box_x + box_w / 2:.1f}" y="{box_y + box_h + 22}" text-anchor="middle" '
        f'font-family="Arial, sans-serif" font-size="15" fill="#111827">template: {_esc(template)}</text>'
    )
    return parts


def _gpu_title(
    state: ClusterState,
    gpu: GPUState,
    logical_aliases: dict[int, str],
    physical_aliases: dict[str, str],
) -> str:
    gpu_id = int(gpu.gpu_id)
    alias = logical_aliases.get(gpu_id)
    physical_id = dict(state.metadata.get("physical_id_map", {})).get(gpu_id)
    if physical_id is None:
        physical_id = dict(state.metadata.get("physical_id_map", {})).get(str(gpu_id))
    if alias is None and physical_id is not None:
        alias = physical_aliases.get(str(physical_id))
    if alias is None:
        alias = str(physical_id) if physical_id is not None else f"GPU {gpu_id + 1}"
    return f"{alias}(gpu-{gpu_id})"


def _template_for_gpu(gpu: GPUState) -> str:
    values = [str(inst.end - inst.start) for inst in gpu.instances if inst.profile != "void"]
    return "+".join(values) if values else "empty"


def _instance_label_lines(inst: MigInstance) -> list[str]:
    if _is_free(inst):
        return ["free"]
    workload = str(inst.workload or "unassigned")
    batch = f"bs={inst.batch}" if inst.batch is not None else "bs=-"
    profile = inst.profile
    return [workload, batch, profile]


def _is_free(inst: MigInstance) -> bool:
    return inst.profile == "void" or not inst.workload


def _font_size_for_width(width: float) -> int:
    if width < 42:
        return 9
    if width < 58:
        return 10
    if width < 85:
        return 11
    return 13


def _parse_aliases(values: list[str]) -> tuple[dict[int, str], dict[str, str]]:
    logical_aliases = {}
    physical_aliases = {}
    for value in values:
        if "=" not in value:
            raise ValueError(f"Bad --gpu-alias {value!r}; expected GPU_ID=ALIAS")
        left, right = value.split("=", 1)
        try:
            logical_aliases[int(left)] = right
        except ValueError:
            physical_aliases[left] = right
    return logical_aliases, physical_aliases


def _default_title(obj: dict[str, Any], state_key: str) -> str:
    metadata = dict(obj.get("metadata", {}))
    if obj.get("kind") == "MigPlan":
        return f"{metadata.get('name', 'migplan')} {state_key}"
    return str(metadata.get("name") or metadata.get("scenario") or obj.get("kind") or "MIGRANT layout")


def _esc(value: Any) -> str:
    return html.escape(str(value), quote=True)


if __name__ == "__main__":
    main()
