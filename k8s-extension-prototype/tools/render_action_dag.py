#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import json
import math
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
CONTROLLER = ROOT / "controller"
TOOLS = ROOT / "tools"
for path in (ROOT, CONTROLLER, TOOLS):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from planning.k8s_adapter import cluster_state_from_dict, plan_scenario_as_migplan_status  # noqa: E402
from scenario_loader import load_planning_scenario  # noqa: E402
from render_mig_layout import _chain_source_override, _parse_aliases  # noqa: E402
from migrant_core.transition_planners.action_plan_formats import build_phased_action_plan  # noqa: E402


ACTION_SECONDS = {
    "configure_full_template": 113.203,
    "clear_template": 40.238,
    "clear_gpu": 10.365,
    "clear_gpu_binding": 0.2,
    "delete_gpu_pods": 1.0,
    "delete_pods": 1.0,
    "return_gpu": 0.2,
    "mark_draining_instance": 30.0,
    "stop_gpu_traffic": 1.0,
    "stop_accepting_new": 1.0,
    "accept_queued_requests": 1.0,
    "reroute_queued_tasks": 1.0,
    "remove_instance": 1.0,
    "delete_bridge_pod": 1.0,
    "place_instance": 1.0,
    "bridge_place_instance": 1.0,
    "workload_change": 1.0,
    "update_batch": 1.0,
    "patch_batch_config": 0.2,
    "apply_batch": 1.0,
    "verify_batch": 0.5,
    "allocate_gpu": 0.2,
    "place_target_layout": 0.2,
    "observe_mig_devices": 2.0,
    "deploy_target_workloads": 5.0,
    "activate_serving_route": 0.5,
    "bind_target_gpu": 0.2,
    "mark_reconfig_target_prepared": 0.2,
}

TEMPLATE_ALLOCATABLE_SECONDS = {
    "7": 102.539,
    "4+3": 120.651,
    "4+2+1": 112.627,
    "4+1+1+1": 112.629,
    "3+3": 110.611,
    "3+2+1": 112.603,
    "3+1+1+1": 114.648,
    "2+2+3": 112.619,
    "3+2+1+1": 112.612,
    "3+1+1+1+1": 112.652,
    "2+2+2+1": 112.609,
    "2+2+1+1+1": 114.619,
    "2+1+1+1+1+1": 120.811,
    "1+1+1+1+1+1+1": 112.613,
    "2+2+2": 112.620,
}

EMPTY_SUCCESS_SECONDS = {
    "7": 42.237,
    "4+3": 40.229,
    "4+2+1": 40.238,
    "4+1+1+1": 40.237,
    "3+3": 40.236,
    "3+2+1": 40.238,
    "3+1+1+1": 40.238,
    "2+2+3": 40.235,
    "3+2+1+1": 40.244,
    "3+1+1+1+1": 40.237,
    "2+2+2+1": 40.235,
    "2+2+1+1+1": 42.247,
    "2+1+1+1+1+1": 40.236,
    "1+1+1+1+1+1+1": 40.236,
}

CLASS_COLORS = {
    "mig-geometry": "#dbeafe",
    "binding-state": "#ede9fe",
    "pod-lifecycle": "#dcfce7",
    "router-drain": "#fef3c7",
    "cleanup": "#fee2e2",
    "blocked": "#e5e7eb",
    "other": "#f3f4f6",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Render MIGRANT phased action DAG views as SVG.")
    parser.add_argument("--plan-scenario", required=True, help="PlanningScenario YAML.")
    parser.add_argument("--planner", default="offline_final_dag", help="Transition planner to run.")
    parser.add_argument("--output-dir", default=str(ROOT / "reports" / "dag-figures"), help="Output directory.")
    parser.add_argument("--gpu-alias", action="append", default=[], help="GPU alias override: GPU_ID=alias or physicalId=alias.")
    parser.add_argument("--title", default=None, help="Figure title prefix.")
    parser.add_argument("--force-transition", action="store_true", help="Render the explicit transition even if current-state feasibility would no-op.")
    args = parser.parse_args()

    logical_aliases, physical_aliases = _parse_aliases(args.gpu_alias)
    scenario_path = Path(args.plan_scenario)
    status = _plan_status(scenario_path, args.planner, force_transition=args.force_transition)
    transition = status["status"]["planningTrace"]["transition"]
    dag = transition.get("phasedActionPlan") or {}
    executed_state = cluster_state_from_dict(status["status"]["executedState"])
    title = args.title or f"{status['spec']['scenario']} {status['spec']['planner']}"
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    outputs = {
        "final_execution_dag": output_dir / "final-execution-dag.svg",
    }
    outputs["final_execution_dag"].write_text(
        render_gpu_lane_timeline(dag, executed_state, title, logical_aliases, physical_aliases),
        encoding="utf-8",
    )

    manifest = {
        "scenario": status["spec"]["scenario"],
        "planner": status["spec"]["planner"],
        "plannerModule": transition.get("plannerModule"),
        "actionCount": dag.get("actionCount", 0),
        "phaseCount": dag.get("phaseCount", 0),
        "outputs": {name: str(path) for name, path in outputs.items()},
    }
    manifest_path = output_dir / "dag-figure-manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    for path in [*outputs.values(), manifest_path]:
        print(path)


def _plan_status(scenario_path: Path, planner: str, *, force_transition: bool = False) -> dict[str, Any]:
    scenario = load_planning_scenario(scenario_path)
    scenario.transition["transitionPlanner"] = planner
    if force_transition:
        scenario.transition["forceReplan"] = True
    source_override = _chain_source_override(scenario_path, scenario, planner, force_transition=force_transition)
    return plan_scenario_as_migplan_status(scenario, source_state_override=source_override)


def render_global_action_dag(dag: dict[str, Any], title: str) -> str:
    nodes = _timeline_nodes(list(dag.get("nodes", [])))
    phases = list(dag.get("phases", []))
    phase_index = {str(node_id): idx for idx, phase in enumerate(phases) for node_id in phase.get("nodeIds", [])}
    by_phase: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for node in nodes:
        by_phase[int(node.get("phase", phase_index.get(str(node.get("id")), 0)))].append(node)

    col_w = 180
    row_h = 76
    margin_x = 48
    margin_y = 76
    max_rows = max((len(items) for items in by_phase.values()), default=1)
    width = margin_x * 2 + col_w * max(1, len(by_phase))
    height = margin_y + 55 + row_h * max_rows
    positions: dict[str, tuple[float, float]] = {}
    parts = _svg_header(width, height, f"{title} - global action DAG")

    for phase, items in sorted(by_phase.items()):
        x = margin_x + phase * col_w
        parts.append(f'<text x="{x + 60}" y="58" text-anchor="middle" font-family="Arial" font-size="13" fill="#374151">d{phase}</text>')
        for row, node in enumerate(items):
            y = margin_y + row * row_h
            positions[str(node["id"])] = (x + 60, y + 24)

    for node in nodes:
        x2, y2 = positions.get(str(node.get("id")), (0, 0))
        for dep in node.get("dependsOn", []):
            if str(dep) not in positions:
                continue
            x1, y1 = positions[str(dep)]
            parts.append(f'<line x1="{x1 + 58}" y1="{y1}" x2="{x2 - 58}" y2="{y2}" stroke="#9ca3af" stroke-width="1.2" marker-end="url(#arrow)"/>')

    for node in nodes:
        x, y = positions[str(node["id"])]
        action = dict(node.get("action", {}))
        color = CLASS_COLORS.get(str(node.get("operationClass", "other")), "#f3f4f6")
        parts.append(f'<rect x="{x - 65}" y="{y - 23}" width="130" height="46" rx="4" fill="{color}" stroke="#111827" stroke-width="1"/>')
        parts.append(_svg_text(x, y - 6, _short_action(action), 10, anchor="middle"))
        parts.append(_svg_text(x, y + 9, _action_target(action), 9, anchor="middle", fill="#4b5563"))
    parts.append(_legend(width - 410, height - 28))
    return "\n".join(parts + ["</svg>"]) + "\n"


def render_gpu_lane_timeline(
    dag: dict[str, Any],
    executed_state: Any,
    title: str,
    logical_aliases: dict[int, str],
    physical_aliases: dict[str, str],
) -> str:
    nodes = _timeline_nodes(list(dag.get("nodes", [])))
    phases = int(dag.get("phaseCount", 0) or 0)
    lane_roles = _duplicate_logical_lane_roles(nodes)
    physical_reuse = _physical_reuse_labels(nodes, logical_aliases)
    lanes = _lanes_for_nodes(nodes, executed_state, logical_aliases, physical_aliases, lane_roles, physical_reuse)
    lane_names = list(lanes)
    x0 = 190
    y0 = 82
    phase_w = 112
    lane_h = 132
    stack_gap = 56
    width = x0 + max(phases, 1) * phase_w + 50
    height = y0 + len(lane_names) * lane_h + 70
    parts = _svg_header(width, height, f"{title} - GPU-lane dependency DAG")

    for phase in range(phases):
        x = x0 + phase * phase_w
        parts.append(f'<line x1="{x}" y1="62" x2="{x}" y2="{height - 48}" stroke="#e5e7eb" stroke-width="1"/>')
        parts.append(_svg_text(x + phase_w / 2, 58, f"d{phase}", 11, anchor="middle", fill="#4b5563"))
    for lane_idx, lane in enumerate(lane_names):
        y = y0 + lane_idx * lane_h
        parts.append(_svg_text(20, y + 28, lane, 12, anchor="start"))
        parts.append(f'<line x1="{x0}" y1="{y + 48}" x2="{width - 35}" y2="{y + 48}" stroke="#e5e7eb" stroke-width="1"/>')

    positions: dict[str, tuple[float, float, str]] = {}
    for node in nodes:
        action = dict(node.get("action", {}))
        lane = _lane_for_action(action, executed_state, logical_aliases, physical_aliases, lane_roles, physical_reuse)
        lane_idx = lane_names.index(lane) if lane in lane_names else 0
        phase = int(node.get("phase", 0))
        stack = _node_stack_index(node, nodes, lane, phase, executed_state, logical_aliases, physical_aliases, lane_roles, physical_reuse)
        y = y0 + lane_idx * lane_h + 7 + stack * stack_gap
        w = phase_w - 12
        x = x0 + phase * phase_w + 5
        positions[str(node.get("id"))] = (x + w / 2, y + 25, lane)
    reuse_markers: list[tuple[float, float, float]] = []
    for node in nodes:
        node_id = str(node.get("id"))
        if node_id not in positions:
            continue
        x2, y2, lane2 = positions[node_id]
        for dep in node.get("dependsOn", []):
            dep_id = str(dep)
            if dep_id not in positions:
                continue
            x1, y1, lane1 = positions[dep_id]
            action = dict(node.get("action", {}))
            dep_node = next((candidate for candidate in nodes if str(candidate.get("id")) == dep_id), {})
            dep_action = dict(dep_node.get("action", {}))
            dep_kind = str(dep_action.get("type", ""))
            action_kind = str(action.get("type", ""))
            style = _timeline_dependency_style(dep_kind, action_kind, lane1 == lane2)
            if dep_kind == "return_gpu" and action_kind == "allocate_gpu":
                reuse_markers.append(((x1 + x2) / 2, y1, y2))
                continue
            if style is None:
                continue
            parts.append(
                f'<path d="M{x1:.1f},{y1:.1f} C{x1 + 38:.1f},{y1:.1f} {x2 - 38:.1f},{y2:.1f} {x2:.1f},{y2:.1f}" '
                f'fill="none" stroke="{style["stroke"]}" stroke-width="{style["width"]}" '
                f'stroke-dasharray="{style["dash"]}" marker-end="url(#arrow)"/>'
            )
            if style.get("label"):
                label_x = (x1 + x2) / 2
                label_y = (y1 + y2) / 2 - 6
                parts.append(_svg_text(label_x, label_y, str(style["label"]), 8, anchor="middle", fill=style["stroke"]))

    for x, y1, y2 in reuse_markers:
        top = min(y1, y2) - 25
        bottom = max(y1, y2) + 25
        parts.append(
            f'<line x1="{x:.1f}" y1="{top:.1f}" x2="{x:.1f}" y2="{bottom:.1f}" '
            'stroke="#dc2626" stroke-width="1.8" stroke-dasharray="6 4"/>'
        )
        parts.append(_svg_text(x + 5, top - 4, "reuse", 8, anchor="start", fill="#dc2626"))

    for node in nodes:
        action = dict(node.get("action", {}))
        lane = _lane_for_action(action, executed_state, logical_aliases, physical_aliases, lane_roles, physical_reuse)
        lane_idx = lane_names.index(lane) if lane in lane_names else 0
        phase = int(node.get("phase", 0))
        stack = _node_stack_index(node, nodes, lane, phase, executed_state, logical_aliases, physical_aliases, lane_roles, physical_reuse)
        y = y0 + lane_idx * lane_h + 7 + stack * stack_gap
        duration = _duration(action)
        w = phase_w - 12
        x = x0 + phase * phase_w + 5
        color = CLASS_COLORS.get(str(node.get("operationClass", "other")), "#f3f4f6")
        details = _timeline_detail_lines(action, executed_state)
        full_title = " | ".join([_short_action(action), *details, f"{duration:g}s"])
        parts.append(f'<rect x="{x}" y="{y}" width="{w:.1f}" height="50" rx="4" fill="{color}" stroke="#111827" stroke-width="1"><title>{html.escape(full_title)}</title></rect>')
        parts.append(_svg_text(x + w / 2, y + 12, _short_action(action), 9, anchor="middle"))
        for line_idx, line in enumerate(details[:2]):
            parts.append(_svg_text(x + w / 2, y + 24 + line_idx * 11, line, 8, anchor="middle", fill="#374151"))
        parts.append(_svg_text(x + w / 2, y + 47, _duration_label(action, duration), 8, anchor="middle", fill="#4b5563"))
    parts.append(_legend(width - 410, height - 25))
    return "\n".join(parts + ["</svg>"]) + "\n"


def _node_stack_index(
    node: dict[str, Any],
    nodes: list[dict[str, Any]],
    lane: str,
    depth: int,
    state: Any,
    logical_aliases: dict[int, str],
    physical_aliases: dict[str, str],
    lane_roles: dict[tuple[int, str], str] | None,
    physical_reuse: dict[str, str] | None,
) -> int:
    node_id = str(node.get("id"))
    peers = []
    for candidate in nodes:
        action = dict(candidate.get("action", {}))
        candidate_lane = _lane_for_action(action, state, logical_aliases, physical_aliases, lane_roles, physical_reuse)
        if candidate_lane == lane and int(candidate.get("phase", 0)) == int(depth):
            peers.append(candidate)
    peers = sorted(peers, key=lambda item: int(item.get("index", 0)))
    for idx, candidate in enumerate(peers):
        if str(candidate.get("id")) == node_id:
            return idx
    return 0


def render_final_execution_gantt(
    dag: dict[str, Any],
    executed_state: Any,
    title: str,
    logical_aliases: dict[int, str],
    physical_aliases: dict[str, str],
) -> str:
    nodes = list(dag.get("nodes", []))
    schedule = _earliest_start_schedule(nodes)
    lane_roles = _duplicate_logical_lane_roles(nodes)
    lanes = _lanes_for_nodes(nodes, executed_state, logical_aliases, physical_aliases, lane_roles)
    lane_names = list(lanes)
    total = max((item["end"] for item in schedule.values()), default=1.0)
    scale = max(2.0, min(8.0, 1100.0 / max(total, 1.0)))
    x0 = 210
    y0 = 88
    lane_h = 78
    width = int(x0 + total * scale + 180)
    height = y0 + len(lane_names) * lane_h + 78
    parts = _svg_header(width, height, f"{title} - final execution Gantt DAG")

    tick_step = _tick_step(total)
    tick = 0.0
    while tick <= total + 0.001:
        x = x0 + tick * scale
        parts.append(f'<line x1="{x:.1f}" y1="58" x2="{x:.1f}" y2="{height - 52}" stroke="#e5e7eb" stroke-width="1"/>')
        parts.append(_svg_text(x, 56, f"{tick:g}s", 10, fill="#4b5563"))
        tick += tick_step
    for lane_idx, lane in enumerate(lane_names):
        y = y0 + lane_idx * lane_h
        parts.append(_svg_text(20, y + 32, lane, 12, anchor="start"))
        parts.append(f'<line x1="{x0}" y1="{y + 54}" x2="{width - 42}" y2="{y + 54}" stroke="#e5e7eb" stroke-width="1"/>')

    positions: dict[str, tuple[float, float, str]] = {}
    nodes_by_id = {str(node.get("id")): node for node in nodes}
    for node in nodes:
        node_id = str(node.get("id"))
        action = dict(node.get("action", {}))
        lane = _lane_for_action(action, executed_state, logical_aliases, physical_aliases, lane_roles)
        lane_idx = lane_names.index(lane) if lane in lane_names else 0
        item = schedule.get(node_id, {"start": 0.0, "end": _duration(action)})
        duration = _duration(action)
        x = x0 + float(item["start"]) * scale
        y = y0 + lane_idx * lane_h + 8
        w = max(3.0, duration * scale)
        positions[node_id] = (x + w, y + 25, lane)

    for node in nodes:
        node_id = str(node.get("id"))
        if node_id not in positions:
            continue
        x2_start = x0 + float(schedule.get(node_id, {}).get("start", 0.0)) * scale
        y2 = positions[node_id][1]
        lane2 = positions[node_id][2]
        action = dict(node.get("action", {}))
        action_kind = str(action.get("type", ""))
        for dep in node.get("dependsOn", []):
            dep_id = str(dep)
            if dep_id not in positions or dep_id not in nodes_by_id:
                continue
            x1, y1, lane1 = positions[dep_id]
            dep_action = dict(nodes_by_id[dep_id].get("action", {}))
            style = _gantt_dependency_style(str(dep_action.get("type", "")), action_kind, lane1 == lane2)
            if style is None:
                continue
            parts.append(
                f'<path d="M{x1:.1f},{y1:.1f} C{x1 + 28:.1f},{y1:.1f} {x2_start - 28:.1f},{y2:.1f} {x2_start:.1f},{y2:.1f}" '
                f'fill="none" stroke="{style["stroke"]}" stroke-width="{style["width"]}" '
                f'stroke-dasharray="{style["dash"]}" marker-end="url(#arrow)"/>'
            )
            if style.get("label"):
                parts.append(_svg_text((x1 + x2_start) / 2, (y1 + y2) / 2 - 6, style["label"], 8, fill=style["stroke"]))

    for node in nodes:
        node_id = str(node.get("id"))
        action = dict(node.get("action", {}))
        lane = _lane_for_action(action, executed_state, logical_aliases, physical_aliases, lane_roles)
        lane_idx = lane_names.index(lane) if lane in lane_names else 0
        item = schedule.get(node_id, {"start": 0.0, "end": _duration(action)})
        duration = _duration(action)
        x = x0 + float(item["start"]) * scale
        y = y0 + lane_idx * lane_h + 8
        w = max(3.0, duration * scale)
        color = CLASS_COLORS.get(str(node.get("operationClass", "other")), "#f3f4f6")
        details = _timeline_detail_lines(action, executed_state)
        full_title = " | ".join([_short_action(action), f"start {item['start']:.1f}s", f"duration {duration:g}s", *details])
        parts.append(f'<rect x="{x:.1f}" y="{y}" width="{w:.1f}" height="50" rx="4" fill="{color}" stroke="#111827" stroke-width="1"><title>{html.escape(full_title)}</title></rect>')
        label = _short_action(action)
        if w >= 42:
            parts.append(_svg_text(x + w / 2, y + 12, label, 9))
        if w >= 72:
            for line_idx, line in enumerate(details[:2]):
                parts.append(_svg_text(x + w / 2, y + 24 + line_idx * 11, line, 8, fill="#374151"))
        if w >= 32:
            parts.append(_svg_text(x + w / 2, y + 47, f"{duration:g}s", 8, fill="#4b5563"))
    parts.append(_legend(width - 410, height - 25))
    return "\n".join(parts + ["</svg>"]) + "\n"


def render_final_execution_sequence(
    dag: dict[str, Any],
    executed_state: Any,
    title: str,
    logical_aliases: dict[int, str],
    physical_aliases: dict[str, str],
) -> str:
    nodes = list(dag.get("nodes", []))
    columns = _dependency_columns(nodes)
    lane_roles = _duplicate_logical_lane_roles(nodes)
    lanes = _lanes_for_nodes(nodes, executed_state, logical_aliases, physical_aliases, lane_roles)
    lane_names = list(lanes)
    max_col = max(columns.values(), default=0)
    x0 = 210
    y0 = 88
    col_w = 126
    lane_h = 78
    box_w = 104
    box_h = 50
    width = x0 + (max_col + 1) * col_w + 80
    height = y0 + len(lane_names) * lane_h + 82
    parts = _svg_header(width, height, f"{title} - final execution sequence")
    parts.append(_svg_text(24, 52, "Fixed-width action boxes; columns are dependency order, not time.", 11, anchor="start", fill="#4b5563"))

    for col in range(max_col + 1):
        x = x0 + col * col_w
        parts.append(f'<line x1="{x}" y1="62" x2="{x}" y2="{height - 52}" stroke="#e5e7eb" stroke-width="1"/>')
        parts.append(_svg_text(x + box_w / 2, 58, f"d{col}", 11, fill="#4b5563"))
    for lane_idx, lane in enumerate(lane_names):
        y = y0 + lane_idx * lane_h
        parts.append(_svg_text(20, y + 32, lane, 12, anchor="start"))
        parts.append(f'<line x1="{x0}" y1="{y + 54}" x2="{width - 42}" y2="{y + 54}" stroke="#e5e7eb" stroke-width="1"/>')

    positions: dict[str, tuple[float, float, str]] = {}
    lane_col_counts: dict[tuple[str, int], int] = defaultdict(int)
    for node in sorted(nodes, key=lambda item: (columns.get(str(item.get("id")), 0), int(item.get("index", 0)))):
        action = dict(node.get("action", {}))
        lane = _lane_for_action(action, executed_state, logical_aliases, physical_aliases, lane_roles)
        lane_idx = lane_names.index(lane) if lane in lane_names else 0
        col = columns.get(str(node.get("id")), 0)
        stack = lane_col_counts[(lane, col)]
        lane_col_counts[(lane, col)] += 1
        x = x0 + col * col_w + 5
        y = y0 + lane_idx * lane_h + 6 + stack * 14
        positions[str(node.get("id"))] = (x + box_w / 2, y + box_h / 2, lane)

    by_id = {str(node.get("id")): node for node in nodes}
    for node in nodes:
        node_id = str(node.get("id"))
        if node_id not in positions:
            continue
        x2, y2, lane2 = positions[node_id]
        action = dict(node.get("action", {}))
        action_kind = str(action.get("type", ""))
        for dep in node.get("dependsOn", []):
            dep_id = str(dep)
            if dep_id not in positions or dep_id not in by_id:
                continue
            x1, y1, lane1 = positions[dep_id]
            dep_action = dict(by_id[dep_id].get("action", {}))
            style = _gantt_dependency_style(str(dep_action.get("type", "")), action_kind, lane1 == lane2)
            if style is None:
                continue
            parts.append(
                f'<path d="M{x1 + box_w / 2 - 8:.1f},{y1:.1f} C{x1 + 34:.1f},{y1:.1f} {x2 - 34:.1f},{y2:.1f} {x2 - box_w / 2 + 8:.1f},{y2:.1f}" '
                f'fill="none" stroke="{style["stroke"]}" stroke-width="{style["width"]}" '
                f'stroke-dasharray="{style["dash"]}" marker-end="url(#arrow)"/>'
            )
            if style.get("label"):
                parts.append(_svg_text((x1 + x2) / 2, (y1 + y2) / 2 - 6, style["label"], 8, fill=style["stroke"]))

    for node in sorted(nodes, key=lambda item: (columns.get(str(item.get("id")), 0), int(item.get("index", 0)))):
        action = dict(node.get("action", {}))
        lane = _lane_for_action(action, executed_state, logical_aliases, physical_aliases, lane_roles)
        lane_idx = lane_names.index(lane) if lane in lane_names else 0
        col = columns.get(str(node.get("id")), 0)
        stack_index = 0
        for other_id, (_, other_y, other_lane) in positions.items():
            if other_id == str(node.get("id")):
                break
            if other_lane == lane and columns.get(other_id, 0) == col:
                stack_index += 1
        x = x0 + col * col_w + 5
        y = y0 + lane_idx * lane_h + 6 + stack_index * 14
        color = CLASS_COLORS.get(str(node.get("operationClass", "other")), "#f3f4f6")
        details = _timeline_detail_lines(action, executed_state)
        full_title = " | ".join([_short_action(action), *details, f"{_duration(action):g}s estimate"])
        parts.append(f'<rect x="{x}" y="{y}" width="{box_w}" height="{box_h}" rx="4" fill="{color}" stroke="#111827" stroke-width="1"><title>{html.escape(full_title)}</title></rect>')
        parts.append(_svg_text(x + box_w / 2, y + 13, _short_action(action), 9))
        for line_idx, line in enumerate(details[:2]):
            parts.append(_svg_text(x + box_w / 2, y + 26 + line_idx * 10, line, 8, fill="#374151"))
    parts.append(_legend(width - 410, height - 25))
    return "\n".join(parts + ["</svg>"]) + "\n"


def render_planner_iteration_trace(transition: dict[str, Any], title: str) -> str:
    iterations = list(transition.get("iterations", []))
    col_w = 250
    row_h = 42
    x0 = 42
    y0 = 84
    max_rows = max((len(_abstract_iteration_rows(item, action_key="candidateActions")) for item in iterations), default=1)
    width = max(720, x0 * 2 + col_w * max(1, len(iterations)))
    height = y0 + max_rows * row_h + 95
    parts = _svg_header(width, height, f"{title} - planner iteration trace")
    parts.append(_svg_text(24, 52, "Each column shows the full candidate plan generated by that planner iteration.", 11, anchor="start", fill="#4b5563"))
    for idx, iteration in enumerate(iterations):
        x = x0 + idx * col_w
        parts.append(_svg_text(x + 92, 60, f"iteration {iteration.get('iteration', idx + 1)}", 13))
        parts.append(_svg_text(x + 92, 76, f"candidate: {iteration.get('candidateActionCount', 0)} fine actions", 9, fill="#4b5563"))
        rows = _abstract_iteration_rows(iteration, action_key="candidateActions")
        for row_idx, row in enumerate(rows):
            y = y0 + row_idx * row_h
            color = CLASS_COLORS.get(row.get("class", "other"), "#f3f4f6")
            parts.append(f'<rect x="{x}" y="{y}" width="184" height="30" rx="4" fill="{color}" stroke="#111827" stroke-width="1"/>')
            parts.append(_svg_text(x + 92, y + 19, row["label"], 10))
    return "\n".join(parts + ["</svg>"]) + "\n"


def render_selected_prefix_trace(
    transition: dict[str, Any],
    executed_state: Any,
    title: str,
    logical_aliases: dict[int, str],
    physical_aliases: dict[str, str],
) -> str:
    iterations = list(transition.get("iterations", []))
    blocks = []
    for iteration in iterations:
        actions = list(iteration.get("chosenActions", []))
        if not actions:
            continue
        dag = build_phased_action_plan(actions, name=f"iteration-{iteration.get('iteration', len(blocks) + 1)}")
        nodes = list(dag.get("nodes", []))
        schedule = _earliest_start_schedule(nodes)
        blocks.append((iteration, nodes, schedule))

    lane_roles = _duplicate_logical_lane_roles([node for _, nodes, _ in blocks for node in nodes])
    lanes: dict[str, None] = {}
    for _, nodes, _ in blocks:
        lanes.update(_lanes_for_nodes(nodes, executed_state, logical_aliases, physical_aliases, lane_roles))
    lane_names = list(dict(sorted(lanes.items(), key=lambda item: _lane_sort_key(item[0]))))
    block_w = 420
    x0 = 190
    y0 = 88
    lane_h = 66
    width = x0 + block_w * max(1, len(blocks)) + 70
    height = y0 + len(lane_names) * lane_h + 70
    parts = _svg_header(width, height, f"{title} - selected prefix trace")
    parts.append(_svg_text(24, 52, "Selected fine-grained executable prefix per planner iteration.", 11, anchor="start", fill="#4b5563"))
    for lane_idx, lane in enumerate(lane_names):
        y = y0 + lane_idx * lane_h
        parts.append(_svg_text(20, y + 28, lane, 12, anchor="start"))
        parts.append(f'<line x1="{x0}" y1="{y + 47}" x2="{width - 42}" y2="{y + 47}" stroke="#e5e7eb" stroke-width="1"/>')

    for block_idx, (iteration, nodes, schedule) in enumerate(blocks):
        block_x = x0 + block_idx * block_w
        total = max((item["end"] for item in schedule.values()), default=1.0)
        scale = min(5.0, (block_w - 55) / max(total, 1.0))
        parts.append(f'<line x1="{block_x - 12}" y1="58" x2="{block_x - 12}" y2="{height - 48}" stroke="#9ca3af" stroke-width="1.2" stroke-dasharray="5 4"/>')
        parts.append(_svg_text(block_x + 120, 58, f"iteration {iteration.get('iteration', block_idx + 1)}", 12))
        for node in nodes:
            action = dict(node.get("action", {}))
            lane = _lane_for_action(action, executed_state, logical_aliases, physical_aliases, lane_roles)
            lane_idx = lane_names.index(lane) if lane in lane_names else 0
            item = schedule[str(node.get("id"))]
            duration = _duration(action)
            x = block_x + float(item["start"]) * scale
            y = y0 + lane_idx * lane_h + 7
            w = max(3.0, duration * scale)
            color = CLASS_COLORS.get(str(node.get("operationClass", "other")), "#f3f4f6")
            parts.append(f'<rect x="{x:.1f}" y="{y}" width="{w:.1f}" height="38" rx="4" fill="{color}" stroke="#111827" stroke-width="1"><title>{html.escape(_short_action(action))}</title></rect>')
            if w >= 36:
                parts.append(_svg_text(x + w / 2, y + 16, _short_action(action), 8))
            if w >= 28:
                parts.append(_svg_text(x + w / 2, y + 29, f"{duration:g}s", 7, fill="#4b5563"))
    parts.append(_legend(width - 410, height - 25))
    return "\n".join(parts + ["</svg>"]) + "\n"


def render_phase_summary(dag: dict[str, Any], title: str) -> str:
    phases = list(dag.get("phases", []))
    width = 980
    phase_h = 64
    height = 90 + len(phases) * phase_h + 40
    parts = _svg_header(width, height, f"{title} - phase summary")
    x0 = 54
    for idx, phase in enumerate(phases):
        y = 78 + idx * phase_h
        duration = _phase_duration(dag, phase)
        counts = ", ".join(f"{k}:{v}" for k, v in dict(phase.get("actionCountsByType", {})).items())
        roots = ", ".join(str(root) for root in list(phase.get("rootIds", []))[:4])
        if len(list(phase.get("rootIds", []))) > 4:
            roots += ", ..."
        parts.append(f'<rect x="{x0}" y="{y}" width="{width - 110}" height="46" rx="5" fill="#f9fafb" stroke="#111827" stroke-width="1"/>')
        parts.append(_svg_text(x0 + 15, y + 18, f"d{phase.get('phase')} | est {duration:g}s | nodes {phase.get('nodeCount')}", 13, anchor="start"))
        parts.append(_svg_text(x0 + 15, y + 34, f"{counts} | roots: {roots}", 10, anchor="start", fill="#4b5563"))
    return "\n".join(parts + ["</svg>"]) + "\n"


def _timeline_nodes(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[int, Any], list[dict[str, Any]]] = defaultdict(list)
    consumed: set[str] = set()
    out: list[dict[str, Any]] = []
    for node in nodes:
        action = dict(node.get("action", {}))
        if action.get("type") == "stop_accepting_new" and action.get("gpu_id") is not None:
            grouped[(int(node.get("phase", 0)), action.get("gpu_id"))].append(node)
    for (phase, gpu_id), group in grouped.items():
        if len(group) <= 1:
            continue
        ids = {str(node.get("id")) for node in group}
        consumed.update(ids)
        first = min(group, key=lambda item: int(item.get("index", 0)))
        action = dict(first.get("action", {}))
        workloads = sorted({str(dict(node.get("action", {})).get("workload")) for node in group if dict(node.get("action", {})).get("workload")})
        slots = [dict(node.get("action", {})).get("slot") for node in group if dict(node.get("action", {})).get("slot") is not None]
        action.update(
            {
                "type": "stop_gpu_traffic",
                "abstractAction": "Stop GPU Traffic",
                "workload": ",".join(workloads) if workloads else action.get("workload"),
                "slotCount": len(slots),
                "slots": slots,
                "visualAggregate": True,
            }
        )
        deps = sorted(
            {
                str(dep)
                for node in group
                for dep in list(node.get("dependsOn", []))
                if str(dep) not in ids
            }
        )
        out.append(
            {
                **dict(first),
                "id": "visual_" + str(first.get("id")),
                "action": action,
                "dependsOn": deps,
                "operationClass": "router-drain",
                "visualAggregateOf": sorted(ids),
            }
        )
    out.extend(node for node in nodes if str(node.get("id")) not in consumed)
    return sorted(out, key=lambda item: (int(item.get("phase", 0)), int(item.get("index", 0))))


def render_workload_flow(dag: dict[str, Any], title: str) -> str:
    nodes = list(dag.get("nodes", []))
    by_workload: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for node in nodes:
        action = dict(node.get("action", {}))
        workload = str(action.get("workload") or action.get("target_workload") or "global")
        by_workload[workload].append(node)
    workloads = sorted(by_workload, key=lambda item: (item == "global", item))
    x0 = 170
    y0 = 84
    col_w = 112
    lane_h = 64
    max_phase = max((int(node.get("phase", 0)) for node in nodes), default=0)
    width = x0 + (max_phase + 1) * col_w + 60
    height = y0 + len(workloads) * lane_h + 70
    parts = _svg_header(width, height, f"{title} - workload flow")
    for phase in range(max_phase + 1):
        x = x0 + phase * col_w
        parts.append(f'<line x1="{x}" y1="62" x2="{x}" y2="{height - 48}" stroke="#e5e7eb" stroke-width="1"/>')
        parts.append(_svg_text(x + col_w / 2, 58, f"d{phase}", 11, anchor="middle", fill="#4b5563"))
    for lane_idx, workload in enumerate(workloads):
        y = y0 + lane_idx * lane_h
        parts.append(_svg_text(24, y + 30, workload, 12, anchor="start"))
        parts.append(f'<line x1="{x0}" y1="{y + 52}" x2="{width - 35}" y2="{y + 52}" stroke="#e5e7eb" stroke-width="1"/>')
        for node in sorted(by_workload[workload], key=lambda item: (int(item.get("phase", 0)), int(item.get("index", 0)))):
            action = dict(node.get("action", {}))
            phase = int(node.get("phase", 0))
            x = x0 + phase * col_w + 8
            color = CLASS_COLORS.get(str(node.get("operationClass", "other")), "#f3f4f6")
            parts.append(f'<rect x="{x}" y="{y + 9}" width="88" height="34" rx="4" fill="{color}" stroke="#111827" stroke-width="1"/>')
            parts.append(_svg_text(x + 44, y + 23, _short_action(action), 9, anchor="middle"))
            parts.append(_svg_text(x + 44, y + 35, _action_target(action), 8, anchor="middle", fill="#4b5563"))
    parts.append(_legend(width - 410, height - 25))
    return "\n".join(parts + ["</svg>"]) + "\n"


def _earliest_start_schedule(nodes: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    by_id = {str(node.get("id")): node for node in nodes}
    remaining = set(by_id)
    schedule: dict[str, dict[str, float]] = {}
    while remaining:
        progressed = False
        for node_id in sorted(remaining, key=lambda item: int(by_id[item].get("index", 0))):
            deps = [str(dep) for dep in list(by_id[node_id].get("dependsOn", [])) if str(dep) in by_id]
            if any(dep not in schedule for dep in deps):
                continue
            start = max((schedule[dep]["end"] for dep in deps), default=0.0)
            duration = _duration(dict(by_id[node_id].get("action", {})))
            schedule[node_id] = {"start": start, "end": start + duration}
            remaining.remove(node_id)
            progressed = True
        if not progressed:
            for node_id in sorted(remaining, key=lambda item: int(by_id[item].get("index", 0))):
                duration = _duration(dict(by_id[node_id].get("action", {})))
                schedule[node_id] = {"start": 0.0, "end": duration}
            break
    return schedule


def _dependency_columns(nodes: list[dict[str, Any]]) -> dict[str, int]:
    by_id = {str(node.get("id")): node for node in nodes}
    remaining = set(by_id)
    columns: dict[str, int] = {}
    while remaining:
        progressed = False
        for node_id in sorted(remaining, key=lambda item: int(by_id[item].get("index", 0))):
            deps = [str(dep) for dep in list(by_id[node_id].get("dependsOn", [])) if str(dep) in by_id]
            if any(dep not in columns for dep in deps):
                continue
            columns[node_id] = max((columns[dep] + 1 for dep in deps), default=0)
            remaining.remove(node_id)
            progressed = True
        if not progressed:
            for node_id in sorted(remaining, key=lambda item: int(by_id[item].get("index", 0))):
                columns[node_id] = max(columns.values(), default=-1) + 1
            break
    return columns


def _tick_step(total: float) -> float:
    if total <= 30:
        return 5.0
    if total <= 120:
        return 20.0
    if total <= 300:
        return 50.0
    return 100.0


def _abstract_iteration_rows(iteration: dict[str, Any], *, action_key: str) -> list[dict[str, str]]:
    actions = [dict(action) for action in list(iteration.get(action_key, []))]
    if not actions:
        return [{"label": "no action", "class": "other"}]
    rows = []
    last_label = None
    count = 0
    last_class = "other"
    for action in actions:
        label = str(action.get("abstractAction") or _abstract_label_for_action(action))
        op_class = _operation_class_for_type(str(action.get("type", "")))
        if label == last_label:
            count += 1
            continue
        if last_label is not None:
            rows.append({"label": f"{last_label} x{count}" if count > 1 else last_label, "class": last_class})
        last_label = label
        last_class = op_class
        count = 1
    if last_label is not None:
        rows.append({"label": f"{last_label} x{count}" if count > 1 else last_label, "class": last_class})
    return rows


def _abstract_label_for_action(action: dict[str, Any]) -> str:
    return {
        "allocate_gpu": "Allocate GPU",
        "configure_full_template": "Configure Template",
        "bind_target_gpu": "Bind GPU",
        "observe_mig_devices": "Resolve UUIDs",
        "deploy_target_workloads": "Deploy Pods",
        "activate_serving_route": "Activate Route",
        "stop_gpu_traffic": "Stop GPU Traffic",
        "stop_accepting_new": "Stop Slot Traffic",
        "accept_queued_requests": "Accept Queued Requests",
        "reroute_queued_tasks": "Reroute Queued Requests",
        "mark_draining_instance": "Wait Drain",
        "delete_pods": "Delete Pods",
        "delete_gpu_pods": "Delete Pods",
        "remove_instance": "Delete Pod",
        "clear_gpu_binding": "Clear GPU Binding",
        "clear_template": "Clear Template",
        "return_gpu": "Return GPU",
        "place_instance": "Deploy Pod",
        "bridge_place_instance": "Deploy Bridge Pod",
        "patch_batch_config": "Patch Config",
        "apply_batch": "Apply Batch",
        "verify_batch": "Verify Batch",
    }.get(str(action.get("type", "")), str(action.get("type", "Action")).replace("_", " ").title())


def _operation_class_for_type(action_type: str) -> str:
    if action_type in {"allocate_gpu", "configure_full_template", "place_target_layout", "observe_mig_devices"}:
        return "mig-geometry"
    if action_type in {"bind_target_gpu", "mark_reconfig_target_prepared", "unbind_target_gpu", "clear_gpu_binding", "return_gpu"}:
        return "binding-state"
    if action_type in {"stop_gpu_traffic", "stop_accepting_new", "accept_queued_requests", "reroute_queued_tasks", "mark_draining_instance", "activate_serving_route"}:
        return "router-drain"
    if action_type in {"place_instance", "bridge_place_instance", "update_batch", "patch_batch_config", "apply_batch", "verify_batch", "workload_change", "deploy_target_workloads"}:
        return "pod-lifecycle"
    if action_type in {"delete_pods", "delete_gpu_pods", "remove_instance", "delete_bridge_pod", "clear_gpu", "clear_template"}:
        return "cleanup"
    if action_type.startswith("defer_"):
        return "blocked"
    return "other"


def _gantt_dependency_style(dep_kind: str, action_kind: str, same_lane: bool) -> dict[str, str] | None:
    if dep_kind == "clear_gpu_binding" and action_kind == "bind_target_gpu":
        return {"stroke": "#7c3aed", "width": "1.6", "dash": "2 2", "label": "logical-id handoff"}
    if dep_kind == "accept_queued_requests" and action_kind == "reroute_queued_tasks":
        return {"stroke": "#d97706", "width": "1.4", "dash": "4 3", "label": "queue handoff"}
    if same_lane:
        return {"stroke": "#7c3aed", "width": "1.0", "dash": "4 3", "label": ""}
    return {"stroke": "#9ca3af", "width": "1.0", "dash": "4 3", "label": ""}


def _lanes_for_nodes(
    nodes: list[dict[str, Any]],
    state: Any,
    logical_aliases: dict[int, str],
    physical_aliases: dict[str, str],
    lane_roles: dict[tuple[int, str], str] | None = None,
    physical_reuse: dict[str, str] | None = None,
) -> dict[str, None]:
    lanes = {}
    for node in nodes:
        lane = _lane_for_action(dict(node.get("action", {})), state, logical_aliases, physical_aliases, lane_roles, physical_reuse)
        lanes[lane] = None
    return dict(sorted(lanes.items(), key=lambda item: _lane_sort_key(item[0])))


def _physical_reuse_labels(nodes: list[dict[str, Any]], logical_aliases: dict[int, str]) -> dict[str, str]:
    by_physical: dict[str, dict[str, set[int]]] = defaultdict(lambda: {"old": set(), "new": set()})
    for node in nodes:
        action = dict(node.get("action", {}))
        gpu_id = action.get("gpu_id")
        physical_id = action.get("physical_gpu_id")
        if gpu_id is None or physical_id is None:
            continue
        role = _lane_role_for_action(action)
        by_physical[str(physical_id)][role].add(int(gpu_id))
    labels = {}
    for physical_id, roles in by_physical.items():
        old_ids = sorted(roles["old"])
        new_ids = sorted(roles["new"])
        if not old_ids or not new_ids:
            continue
        if set(old_ids) == set(new_ids):
            continue
        old_label = ",".join(_logical_label(gpu_id, logical_aliases) for gpu_id in old_ids)
        new_label = ",".join(_logical_label(gpu_id, logical_aliases) for gpu_id in new_ids)
        labels[physical_id] = f"{old_label} old -> {new_label}"
    return labels


def _duplicate_logical_lane_roles(nodes: list[dict[str, Any]]) -> dict[tuple[int, str], str]:
    roles: dict[tuple[int, str], str] = {}
    by_logical: dict[int, set[str]] = defaultdict(set)
    for node in nodes:
        action = dict(node.get("action", {}))
        gpu_id = action.get("gpu_id")
        physical_id = action.get("physical_gpu_id")
        if gpu_id is None or physical_id is None:
            continue
        by_logical[int(gpu_id)].add(str(physical_id))
    duplicated = {gpu_id for gpu_id, physical_ids in by_logical.items() if len(physical_ids) > 1}
    if not duplicated:
        return roles
    for node in nodes:
        action = dict(node.get("action", {}))
        gpu_id = action.get("gpu_id")
        physical_id = action.get("physical_gpu_id")
        if gpu_id is None or physical_id is None or int(gpu_id) not in duplicated:
            continue
        key = (int(gpu_id), str(physical_id))
        role = _lane_role_for_action(action)
        if role == "old" or key not in roles:
            roles[key] = role
    return roles


def _lane_role_for_action(action: dict[str, Any]) -> str:
    action_type = str(action.get("type", ""))
    if action_type in {
        "allocate_gpu",
        "configure_full_template",
        "bind_target_gpu",
        "observe_mig_devices",
        "deploy_target_workloads",
        "activate_serving_route",
    } and action.get("clearsActiveLogicalGpuId") is not True:
        return "new"
    if action_type in {
        "stop_gpu_traffic",
        "stop_accepting_new",
        "reroute_queued_tasks",
        "mark_draining_instance",
        "delete_gpu_pods",
        "delete_pods",
        "remove_instance",
        "clear_gpu_binding",
        "clear_template",
        "return_gpu",
    }:
        return "old"
    return "new"


def _lane_for_action(
    action: dict[str, Any],
    state: Any,
    logical_aliases: dict[int, str],
    physical_aliases: dict[str, str],
    lane_roles: dict[tuple[int, str], str] | None = None,
    physical_reuse: dict[str, str] | None = None,
) -> str:
    physical_id = action.get("physical_gpu_id") or action.get("source_physical_gpu_id") or action.get("target_physical_gpu_id")
    gpu_id = action.get("gpu_id")
    if physical_id is None and gpu_id is not None:
        physical_id = dict(state.metadata.get("physical_id_map", {})).get(int(gpu_id))
        if physical_id is None:
            physical_id = dict(state.metadata.get("physical_id_map", {})).get(str(gpu_id))
    if gpu_id is None:
        gpu_id = _gpu_id_for_physical(state, physical_id)
    if physical_id is None and gpu_id is None:
        return "global"
    if physical_id is not None:
        physical = physical_aliases.get(str(physical_id), str(physical_id))
        reuse_label = (physical_reuse or {}).get(str(physical_id))
        if reuse_label:
            return f"{physical}({reuse_label})"
        if gpu_id is not None:
            logical = _logical_label(int(gpu_id), logical_aliases)
            role = (lane_roles or {}).get((int(gpu_id), str(physical_id)))
            suffix = f", {role}" if role else ""
            return f"{physical}({logical}{suffix})"
        return physical
    if gpu_id is not None:
        logical = _logical_label(int(gpu_id), logical_aliases)
        return f"?({logical})"
    return "global"


def _logical_label(gpu_id: int, logical_aliases: dict[int, str]) -> str:
    return logical_aliases.get(int(gpu_id), f"gpu-{gpu_id}")


def _gpu_id_for_physical(state: Any, physical_id: Any) -> int | None:
    if physical_id is None:
        return None
    for gpu_id, pid in dict(state.metadata.get("physical_id_map", {})).items():
        if str(pid) == str(physical_id):
            return int(gpu_id)
    return None


def _timeline_dependency_style(dep_kind: str, action_kind: str, same_lane: bool) -> dict[str, str] | None:
    if same_lane:
        if dep_kind == "deploy_target_workloads" and action_kind == "bind_target_gpu":
            return {"stroke": "#7c3aed", "width": "1.2", "dash": "4 3", "label": ""}
        if dep_kind == "bind_target_gpu" and action_kind == "activate_serving_route":
            return {"stroke": "#7c3aed", "width": "1.2", "dash": "4 3", "label": ""}
        return None
    if dep_kind == "clear_gpu_binding" and action_kind == "bind_target_gpu":
        return {"stroke": "#7c3aed", "width": "1.6", "dash": "2 2", "label": "logical-id handoff"}
    if dep_kind == "accept_queued_requests" and action_kind == "reroute_queued_tasks":
        return {"stroke": "#d97706", "width": "1.4", "dash": "4 3", "label": "queue handoff"}
    return None


def _lane_sort_key(lane: str) -> tuple[int, int, str]:
    if lane == "global":
        return (2, 0, lane)
    if lane.startswith("gpu-"):
        try:
            return (0, int(lane.removeprefix("gpu-")), lane)
        except ValueError:
            pass
    reuse_target = _reuse_target_gpu_id_from_lane(lane)
    if reuse_target is not None:
        return (0, reuse_target, f"1:{lane}")
    logical = _logical_gpu_id_from_lane(lane)
    if logical is not None:
        role_rank = 0 if ", old)" in lane else (1 if ", new)" in lane else 0)
        return (0, logical, f"{role_rank}:{lane}")
    return (1, 0, lane)


def _reuse_target_gpu_id_from_lane(lane: str) -> int | None:
    if "-> gpu-" not in lane:
        return None
    raw = lane.rsplit("-> gpu-", 1)[1].split(")", 1)[0].split(",", 1)[0].strip()
    try:
        return int(raw)
    except ValueError:
        return None


def _logical_gpu_id_from_lane(lane: str) -> int | None:
    if "(gpu-" in lane:
        raw = lane.rsplit("(gpu-", 1)[1].split(")", 1)[0].split(",", 1)[0]
    elif "(gpu" in lane:
        raw = lane.rsplit("(gpu", 1)[1].split(")", 1)[0].split(",", 1)[0]
    else:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _phase_duration(dag: dict[str, Any], phase: dict[str, Any]) -> float:
    nodes_by_id = {str(node.get("id")): node for node in dag.get("nodes", [])}
    durations = [
        _duration(dict(nodes_by_id[str(node_id)].get("action", {})))
        for node_id in phase.get("nodeIds", [])
        if str(node_id) in nodes_by_id
    ]
    return max(durations, default=0.0)


def _duration(action: dict[str, Any]) -> float:
    action_type = str(action.get("type", ""))
    template = _canonical_template_key(action.get("template"))
    if action_type == "configure_full_template":
        return float(TEMPLATE_ALLOCATABLE_SECONDS.get(template, ACTION_SECONDS["configure_full_template"]))
    if action_type == "clear_template":
        return float(EMPTY_SUCCESS_SECONDS.get(template, ACTION_SECONDS["clear_template"]))
    return float(ACTION_SECONDS.get(str(action.get("type", "")), 1.0))


def _canonical_template_key(value: Any) -> str:
    raw = str(value or "")
    try:
        parts = sorted((int(part) for part in raw.split("+") if part), reverse=True)
    except ValueError:
        return raw
    return "+".join(str(part) for part in parts)


def _duration_label(action: dict[str, Any], duration: float) -> str:
    action_type = str(action.get("type", ""))
    if action_type == "configure_full_template":
        return f"{duration:g}s alloc"
    if action_type == "clear_template":
        return f"{duration:g}s success"
    return f"{duration:g}s"


def _short_action(action: dict[str, Any]) -> str:
    value = str(action.get("type", "unknown"))
    aliases = {
        "configure_full_template": "configure",
        "place_target_layout": "desired-layout",
        "observe_mig_devices": "resolve-uuid",
        "deploy_target_workloads": "deploy-workload",
        "activate_serving_route": "activate-route",
        "mark_reconfig_target_prepared": "prepared",
        "clear_gpu_binding": "clear-binding",
        "delete_pods": "delete-pods",
        "delete_gpu_pods": "delete-pods",
        "return_gpu": "return-gpu",
        "stop_gpu_traffic": "stop-gpu-traffic",
        "stop_accepting_new": "stop-new",
        "accept_queued_requests": "accept-queue",
        "reroute_queued_tasks": "reroute",
        "mark_draining_instance": "drain",
        "place_instance": "place-pod",
        "bridge_place_instance": "bridge-pod",
        "delete_bridge_pod": "delete-bridge",
        "remove_instance": "delete-pods",
        "patch_batch_config": "patch-batch",
        "apply_batch": "apply-batch",
        "verify_batch": "verify-batch",
        "allocate_gpu": "allocate",
    }
    return aliases.get(value, value.replace("_", "-")[:18])


def _action_target(action: dict[str, Any]) -> str:
    parts = []
    if action.get("workload"):
        parts.append(str(action["workload"]))
    if action.get("physical_gpu_id"):
        parts.append(str(action["physical_gpu_id"]))
    if action.get("slot"):
        slot = action["slot"]
        parts.append(f"{slot[0]}-{slot[1]}:{slot[2]}")
    if action.get("template"):
        parts.append(str(action["template"]))
    return " ".join(parts)[:26]


def _timeline_detail_lines(action: dict[str, Any], state: Any) -> list[str]:
    action_type = str(action.get("type", ""))
    if action_type == "allocate_gpu":
        return [f"reserve {action.get('physical_gpu_id', '-')}", f"pending gpu{action.get('logical_gpu_id', action.get('gpu_id', '-'))}"]
    if action_type == "configure_full_template":
        return [f"template {action.get('template', '-')}", "wait allocatable"]
    if action_type == "clear_template":
        return [f"empty from {action.get('template', '-')}", "wait success"]
    if action_type == "bind_target_gpu":
        return [f"active gpu{action.get('gpu_id', '-')}", str(action.get("physical_gpu_id", "-"))]
    if action_type == "stop_gpu_traffic":
        return [f"{action.get('slotCount', '-')} pods", "stop new requests"]
    if action_type == "delete_pods":
        slots = _format_action_slots(action)
        return [slots, "delete pods"]
    if action_type == "clear_gpu_binding":
        return [f"clear active gpu{action.get('gpu_id', '-')}", str(action.get("physical_gpu_id", "-"))]
    if action_type == "return_gpu":
        return [f"return {action.get('physical_gpu_id', '-')}", "availableQueue"]
    if action_type == "accept_queued_requests":
        return [f"accept queued {action.get('queued', '-')}", f"from gpu{action.get('from_gpu_id', '-')}"]
    if action_type in {"place_target_layout", "mark_reconfig_target_prepared"}:
        summary = _gpu_layout_summary(state, action.get("gpu_id"), action.get("physical_gpu_id"))
        return [summary[0] if summary else "target layout", summary[1] if len(summary) > 1 else ""]
    if action_type == "observe_mig_devices":
        return [f"gpu{action.get('gpu_id', '-')}", "slot -> MIG UUID"]
    if action_type == "reroute_queued_tasks":
        return [f"queued {action.get('queued', '-')}", f"to {action.get('to') or 'stable slot'}"]
    if action_type == "deploy_target_workloads":
        return [f"gpu{action.get('gpu_id', '-')}", "pods ready"]
    if action_type == "activate_serving_route":
        return [f"gpu{action.get('gpu_id', '-')}", "serving active"]
    if action_type in {"patch_batch_config", "apply_batch", "verify_batch"}:
        return [
            str(action.get("workload") or "-"),
            f"bs {action.get('old_batch', '-')} -> {action.get('new_batch', '-')}",
        ]
    if action.get("slot"):
        slot = action["slot"]
        lines = [f"slot {slot[0]}-{slot[1]} {slot[2]}"]
        if action.get("workload"):
            lines.append(_workload_bs(action))
        return lines
    if action.get("workload"):
        return [_workload_bs(action)]
    if action.get("template"):
        return [f"template {action.get('template')}"]
    return [_action_target(action) or "-"]


def _gpu_layout_summary(state: Any, gpu_id: Any, physical_id: Any) -> list[str]:
    gpu = None
    if gpu_id is not None:
        for item in state.real_gpus():
            if int(item.gpu_id) == int(gpu_id):
                gpu = item
                break
    if gpu is None and physical_id is not None:
        mapped_gpu_id = _gpu_id_for_physical(state, physical_id)
        if mapped_gpu_id is not None:
            for item in state.real_gpus():
                if int(item.gpu_id) == int(mapped_gpu_id):
                    gpu = item
                    break
    if gpu is None:
        return []
    gpu.sort_instances()
    rows = []
    for inst in gpu.instances:
        if inst.profile == "void":
            continue
        label = f"{inst.start}-{inst.end}:{inst.profile}"
        if inst.workload:
            label += f" {inst.workload}"
            if inst.batch is not None:
                label += f" b{inst.batch}"
        rows.append(label)
    if not rows:
        return ["empty"]
    joined = "; ".join(rows)
    if len(joined) <= 26:
        return [joined]
    return [joined[:26], joined[26:52]]


def _workload_bs(action: dict[str, Any]) -> str:
    workload = str(action.get("workload") or action.get("target_workload") or "-")
    batch = action.get("batch") or action.get("target_batch")
    if batch is None:
        return workload
    return f"{workload} bs={batch}"


def _format_action_slots(action: dict[str, Any]) -> str:
    raw_slots = action.get("slots")
    if raw_slots is None and action.get("slot") is not None:
        raw_slots = [action.get("slot")]
    formatted = []
    for slot in list(raw_slots or []):
        if isinstance(slot, (list, tuple)) and len(slot) >= 3:
            formatted.append(f"{slot[0]}-{slot[1]} {slot[2]}")
    if not formatted:
        return "all pods"
    if len(formatted) <= 2:
        return ", ".join(formatted)
    return f"{len(formatted)} slots"


def _svg_header(width: int, height: int, title: str) -> list[str]:
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        "<defs>",
        '<marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto" markerUnits="strokeWidth">',
        '<path d="M0,0 L0,6 L7,3 z" fill="#9ca3af"/>',
        "</marker>",
        "</defs>",
        '<rect x="0" y="0" width="100%" height="100%" fill="#ffffff"/>',
        _svg_text(24, 30, title, 22, anchor="start"),
    ]


def _legend(x: int, y: int) -> str:
    labels = [
        ("mig-geometry", CLASS_COLORS["mig-geometry"]),
        ("binding/state", CLASS_COLORS["binding-state"]),
        ("workload/pod", CLASS_COLORS["pod-lifecycle"]),
        ("router-drain", CLASS_COLORS["router-drain"]),
        ("cleanup", CLASS_COLORS["cleanup"]),
    ]
    parts = []
    cur = x
    for label, color in labels:
        parts.append(f'<rect x="{cur}" y="{y - 12}" width="14" height="14" fill="{color}" stroke="#111827" stroke-width="0.8"/>')
        parts.append(_svg_text(cur + 18, y, label, 10, anchor="start", fill="#4b5563"))
        cur += 102
    return "\n".join(parts)


def _svg_text(x: float, y: float, text: Any, size: int, *, anchor: str = "middle", fill: str = "#111827") -> str:
    return f'<text x="{x:.1f}" y="{y:.1f}" text-anchor="{anchor}" font-family="Arial, sans-serif" font-size="{size}" fill="{fill}">{html.escape(str(text))}</text>'


if __name__ == "__main__":
    main()
