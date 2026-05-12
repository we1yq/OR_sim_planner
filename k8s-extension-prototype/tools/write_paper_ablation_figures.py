#!/usr/bin/env python3
from __future__ import annotations

import csv
import html
import argparse
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT / "reports/ablation-2026-05-11-expanded"

SHORT_LABELS = {
    "current_full": "Current",
    "placement_milp_original": "MILP-old",
    "placement_greedy_two_phase": "Greedy",
    "placement_simulated_annealing": "SA",
    "target_no_preserve": "No-preserve",
    "target_beam_preserve": "Beam",
    "target_exact_milp_templates": "Exact-template",
    "transition_serial_v0": "Serial",
    "transition_drain_v2": "Drain-aware",
    "transition_full_plan_v2": "Full-plan",
}

GROUPS = {
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
        "transition_serial_v0",
        "transition_drain_v2",
        "transition_full_plan_v2",
    ],
}

PALETTE = {
    "current_full": "#1f2933",
    "placement_milp_original": "#6b7280",
    "placement_greedy_two_phase": "#9ca3af",
    "placement_simulated_annealing": "#4b5563",
    "target_no_preserve": "#9ca3af",
    "target_beam_preserve": "#6b7280",
    "target_exact_milp_templates": "#4b5563",
    "transition_serial_v0": "#9ca3af",
    "transition_drain_v2": "#6b7280",
    "transition_full_plan_v2": "#4b5563",
}


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _num(row: dict[str, Any], key: str) -> float:
    try:
        return float(row.get(key, 0.0) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _fmt(value: float, unit: str = "") -> str:
    if abs(value) >= 100:
        text = f"{value:.0f}"
    elif abs(value) >= 10:
        text = f"{value:.1f}"
    else:
        text = f"{value:.2f}"
    return text + unit


def _svg_header(width: int, height: int) -> list[str]:
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        "<style>",
        "text { font-family: Arial, Helvetica, sans-serif; fill: #111827; }",
        ".title { font-size: 18px; font-weight: 700; }",
        ".subtitle { font-size: 12px; fill: #4b5563; }",
        ".axis { stroke: #374151; stroke-width: 1; }",
        ".grid { stroke: #e5e7eb; stroke-width: 1; }",
        ".label { font-size: 12px; }",
        ".tick { font-size: 11px; fill: #4b5563; }",
        ".value { font-size: 11px; font-weight: 600; }",
        ".caption { font-size: 11px; fill: #4b5563; }",
        "</style>",
        '<rect width="100%" height="100%" fill="white"/>',
    ]


def _write(path: Path, parts: list[str]) -> Path:
    parts.append("</svg>")
    path.write_text("\n".join(parts), encoding="utf-8")
    return path


def _bars_panel(
    parts: list[str],
    rows: list[dict[str, Any]],
    metric: str,
    x: int,
    y: int,
    width: int,
    height: int,
    title: str,
    unit: str = "",
) -> None:
    values = [_num(row, metric) for row in rows]
    max_value = max(values) if values else 1.0
    max_value = max(max_value, 1.0)
    label_w = 98
    axis_x = x + label_w
    plot_w = width - label_w - 54
    bar_h = 22
    gap = 12
    parts.append(f'<text class="subtitle" x="{x}" y="{y - 12}">{html.escape(title)}</text>')
    parts.append(f'<line class="axis" x1="{axis_x}" y1="{y}" x2="{axis_x}" y2="{y + height}"/>')
    for i in range(1, 5):
        gx = axis_x + plot_w * i / 4
        parts.append(f'<line class="grid" x1="{gx:.1f}" y1="{y}" x2="{gx:.1f}" y2="{y + height}"/>')
        tick = max_value * i / 4
        parts.append(f'<text class="tick" x="{gx:.1f}" y="{y + height + 16}" text-anchor="middle">{_fmt(tick, unit)}</text>')
    for idx, (row, value) in enumerate(zip(rows, values)):
        by = y + idx * (bar_h + gap) + 4
        bw = plot_w * value / max_value if max_value else 0
        variant = str(row["variant"])
        label = SHORT_LABELS.get(variant, variant)
        fill = PALETTE.get(variant, "#6b7280")
        parts.append(f'<text class="label" x="{x}" y="{by + 15}">{html.escape(label)}</text>')
        parts.append(f'<rect x="{axis_x}" y="{by}" width="{bw:.1f}" height="{bar_h}" fill="{fill}"/>')
        parts.append(f'<text class="value" x="{axis_x + bw + 6:.1f}" y="{by + 15}">{_fmt(value, unit)}</text>')


def _write_dual_bar_figure(
    path: Path,
    rows: list[dict[str, Any]],
    title: str,
    left_metric: str,
    left_title: str,
    right_metric: str,
    right_title: str,
    caption: str,
    left_unit: str = "",
    right_unit: str = "",
) -> Path:
    width, height = 980, 360
    parts = _svg_header(width, height)
    parts.append(f'<text class="title" x="32" y="34">{html.escape(title)}</text>')
    panel_h = 170
    _bars_panel(parts, rows, left_metric, 34, 82, 440, panel_h, left_title, left_unit)
    _bars_panel(parts, rows, right_metric, 522, 82, 420, panel_h, right_title, right_unit)
    parts.append(f'<text class="caption" x="32" y="332">{html.escape(caption)}</text>')
    return _write(path, parts)


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


def _stage_rows(rows: list[dict[str, Any]], variants: list[str], stages: list[str]) -> list[dict[str, Any]]:
    keep = set(variants)
    stage_set = set(stages)
    return [row for row in rows if row.get("variant") in keep and row.get("scenario") in stage_set]


def _grouped_stage_bar_panel(
    parts: list[str],
    rows: list[dict[str, Any]],
    variants: list[str],
    stages: list[str],
    metric: str,
    x: int,
    y: int,
    width: int,
    height: int,
    title: str,
    unit: str = "",
) -> None:
    by_key = {(row["variant"], row["scenario"]): row for row in rows}
    values = [
        _num(by_key[(variant, stage)], metric)
        for stage in stages
        for variant in variants
        if (variant, stage) in by_key
    ]
    max_value = max(values) if values else 1.0
    max_value = max(max_value, 1.0)
    left_pad = 50
    bottom_pad = 44
    top_pad = 22
    plot_x = x + left_pad
    plot_y = y + top_pad
    plot_w = width - left_pad - 18
    plot_h = height - top_pad - bottom_pad
    group_gap = 28
    group_w = (plot_w - group_gap * (len(stages) - 1)) / len(stages)
    bar_gap = 3
    bar_w = max(4.0, (group_w - bar_gap * (len(variants) - 1)) / len(variants))

    parts.append(f'<text class="subtitle" x="{x}" y="{y + 12}">{html.escape(title)}</text>')
    for i in range(5):
        value = max_value * i / 4
        gy = plot_y + plot_h - plot_h * i / 4
        parts.append(f'<line class="grid" x1="{plot_x}" y1="{gy:.1f}" x2="{plot_x + plot_w}" y2="{gy:.1f}"/>')
        parts.append(f'<text class="tick" x="{plot_x - 8}" y="{gy + 4:.1f}" text-anchor="end">{_fmt(value, unit)}</text>')
    parts.append(f'<line class="axis" x1="{plot_x}" y1="{plot_y}" x2="{plot_x}" y2="{plot_y + plot_h}"/>')
    parts.append(f'<line class="axis" x1="{plot_x}" y1="{plot_y + plot_h}" x2="{plot_x + plot_w}" y2="{plot_y + plot_h}"/>')

    for s_idx, stage in enumerate(stages):
        group_x = plot_x + s_idx * (group_w + group_gap)
        parts.append(
            f'<text class="label" x="{group_x + group_w / 2:.1f}" y="{plot_y + plot_h + 24}" text-anchor="middle">{stage}</text>'
        )
        for v_idx, variant in enumerate(variants):
            row = by_key.get((variant, stage))
            if row is None:
                continue
            value = _num(row, metric)
            bar_h = plot_h * value / max_value if max_value else 0
            bx = group_x + v_idx * (bar_w + bar_gap)
            by = plot_y + plot_h - bar_h
            fill = PALETTE.get(variant, "#6b7280")
            parts.append(f'<rect x="{bx:.1f}" y="{by:.1f}" width="{bar_w:.1f}" height="{bar_h:.1f}" fill="{fill}"/>')


def _legend(parts: list[str], variants: list[str], x: int, y: int, columns: int = 4) -> None:
    col_w = 118
    row_h = 20
    for idx, variant in enumerate(variants):
        col = idx % columns
        row = idx // columns
        lx = x + col * col_w
        ly = y + row * row_h
        fill = PALETTE.get(variant, "#6b7280")
        label = SHORT_LABELS.get(variant, variant)
        parts.append(f'<rect x="{lx}" y="{ly - 10}" width="14" height="10" fill="{fill}"/>')
        parts.append(f'<text class="tick" x="{lx + 20}" y="{ly}">{html.escape(label)}</text>')


def _write_stage_comparison_figure(
    path: Path,
    rows: list[dict[str, Any]],
    variants: list[str],
    stages: list[str],
    title: str,
    panels: list[tuple[str, str, str]],
    caption: str,
) -> Path:
    width = 1060
    panel_h = 245
    legend_h = 52
    height = 58 + legend_h + panel_h * len(panels) + 34
    parts = _svg_header(width, height)
    parts.append(f'<text class="title" x="32" y="34">{html.escape(title)}</text>')
    _legend(parts, variants, 32, 62, columns=4)
    y = 94
    for metric, panel_title, unit in panels:
        _grouped_stage_bar_panel(
            parts=parts,
            rows=rows,
            variants=variants,
            stages=stages,
            metric=metric,
            x=32,
            y=y,
            width=996,
            height=panel_h,
            title=panel_title,
            unit=unit,
        )
        y += panel_h
    parts.append(f'<text class="caption" x="32" y="{height - 18}">{html.escape(caption)}</text>')
    return _write(path, parts)


def _write_stage_gpu_figure(path: Path, stage_rows: list[dict[str, Any]], stages: list[str]) -> Path:
    width, height = 760, 420
    variants = GROUPS["placement"]
    by_variant_stage = {(row["variant"], row["scenario"]): row for row in stage_rows}
    max_gpu = max(
        _num(by_variant_stage[(variant, stage)], "gpu_count")
        for variant in variants
        for stage in stages
        if (variant, stage) in by_variant_stage
    )
    max_gpu = max(max_gpu, 1.0)
    left, top, plot_w, plot_h = 78, 72, 560, 260
    parts = _svg_header(width, height)
    parts.append('<text class="title" x="32" y="34">Placement ablation: GPU count by stage</text>')
    for i in range(0, int(max_gpu) + 1, max(1, int(max_gpu) // 4)):
        gy = top + plot_h - plot_h * i / max_gpu
        parts.append(f'<line class="grid" x1="{left}" y1="{gy:.1f}" x2="{left + plot_w}" y2="{gy:.1f}"/>')
        parts.append(f'<text class="tick" x="{left - 10}" y="{gy + 4:.1f}" text-anchor="end">{i}</text>')
    parts.append(f'<line class="axis" x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_h}"/>')
    parts.append(f'<line class="axis" x1="{left}" y1="{top + plot_h}" x2="{left + plot_w}" y2="{top + plot_h}"/>')
    x_positions = [left + idx * plot_w / (len(stages) - 1) for idx in range(len(stages))]
    for x_pos, stage in zip(x_positions, stages):
        parts.append(f'<text class="label" x="{x_pos:.1f}" y="{top + plot_h + 24}" text-anchor="middle">{stage}</text>')
    for variant in variants:
        points = []
        for x_pos, stage in zip(x_positions, stages):
            row = by_variant_stage.get((variant, stage))
            if row is None:
                continue
            y_pos = top + plot_h - plot_h * _num(row, "gpu_count") / max_gpu
            points.append((x_pos, y_pos, _num(row, "gpu_count")))
        if len(points) < 2:
            continue
        color = PALETTE.get(variant, "#6b7280")
        poly = " ".join(f"{x:.1f},{y:.1f}" for x, y, _ in points)
        dash = ' stroke-dasharray="5 4"' if variant != "current_full" else ""
        parts.append(f'<polyline points="{poly}" fill="none" stroke="{color}" stroke-width="2.4"{dash}/>')
        for x_pos, y_pos, value in points:
            parts.append(f'<circle cx="{x_pos:.1f}" cy="{y_pos:.1f}" r="4" fill="{color}"/>')
            parts.append(f'<text class="tick" x="{x_pos:.1f}" y="{y_pos - 8:.1f}" text-anchor="middle">{_fmt(value)}</text>')
    legend_x, legend_y = 662, 92
    for idx, variant in enumerate(variants):
        y = legend_y + idx * 24
        color = PALETTE.get(variant, "#6b7280")
        label = SHORT_LABELS.get(variant, variant)
        dash = ' stroke-dasharray="5 4"' if variant != "current_full" else ""
        parts.append(f'<line x1="{legend_x}" y1="{y}" x2="{legend_x + 28}" y2="{y}" stroke="{color}" stroke-width="2.4"{dash}/>')
        parts.append(f'<circle cx="{legend_x + 14}" cy="{y}" r="3.6" fill="{color}"/>')
        parts.append(f'<text class="label" x="{legend_x + 36}" y="{y + 4}">{html.escape(label)}</text>')
    parts.append('<text class="caption" x="32" y="390">Lower curves use fewer physical GPUs across the workload stages.</text>')
    return _write(path, parts)


def _paper_section(paths: list[Path], output_dir: Path) -> str:
    items = []
    for path in paths:
        rel = path.relative_to(output_dir)
        items.append(
            f'<figure><img src="{html.escape(str(rel))}" alt="{html.escape(path.stem)}">'
            f"<figcaption>{html.escape(path.stem)}</figcaption></figure>"
        )
    return (
        '<section><h2>论文图表</h2>'
        '<p>这些 SVG 是白底矢量图，使用更少颜色和更大的字号，适合放入论文或幻灯片。</p>'
        + "".join(items)
        + "</section>"
    )


def _inject_into_html(index_path: Path, output_dir: Path, paths: list[Path]) -> None:
    text = index_path.read_text(encoding="utf-8")
    marker = "<section><h2>图表</h2>"
    section = _paper_section(paths, output_dir)
    if "<section><h2>论文图表</h2>" in text:
        start = text.index("<section><h2>论文图表</h2>")
        end = text.index(marker, start)
        text = text[:start] + section + "\n" + text[end:]
    elif marker in text:
        text = text.replace(marker, section + "\n" + marker, 1)
    else:
        text = text.replace("</body>", section + "\n</body>")
    index_path.write_text(text, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report-dir", default=str(REPORT_DIR))
    args = parser.parse_args()

    output_dir = Path(args.report_dir)
    paper_dir = output_dir / "figures_paper"
    paper_dir.mkdir(parents=True, exist_ok=True)
    for old_svg in paper_dir.glob("*.svg"):
        old_svg.unlink()
    rows = _read_csv(output_dir / "ablation-results.csv")
    stages = _scenario_names(rows)

    placement_stage_rows = _stage_rows(rows, GROUPS["placement"], stages)
    target_stage_rows = _stage_rows(rows, GROUPS["target"], stages)
    transition_stage_rows = _stage_rows(rows, GROUPS["transition"], stages)

    paths = [
        _write_stage_comparison_figure(
            paper_dir / "fig1_placement_stage_time_gpu.svg",
            placement_stage_rows,
            GROUPS["placement"],
            stages,
            "Placement planner ablation by stage",
            [
                ("placement_elapsed_sec", "Placement algorithm time", "s"),
                ("gpu_count", "Selected GPU count", ""),
            ],
            "First-stage placement is evaluated by computation time and the number of GPUs selected in each workload stage.",
        ),
        _write_stage_comparison_figure(
            paper_dir / "fig2_target_stage_time_cost.svg",
            target_stage_rows,
            GROUPS["target"],
            stages,
            "Target materialization ablation by stage",
            [
                ("target_build_elapsed_sec", "Target-builder algorithm time", "s"),
                ("estimated_hardware_sec", "Estimated reconfiguration time", "s"),
                ("fine_action_count", "Executed fine-grained actions", ""),
            ],
            "Second-stage target materialization is compared by build time and the downstream transition cost it induces.",
        ),
        _write_stage_comparison_figure(
            paper_dir / "fig3_transition_stage_time_gpu_hw.svg",
            transition_stage_rows,
            GROUPS["transition"],
            stages,
            "Transition planner ablation by stage",
            [
                ("sim_transition_elapsed_sec", "Transition-planner algorithm time", "s"),
                ("peak_active_gpu", "Peak active physical GPUs", ""),
                ("estimated_hardware_sec", "Estimated reconfiguration time", "s"),
            ],
            "Third-stage transition planners are compared by scheduling computation, peak GPU footprint, and estimated hardware action cost.",
        ),
    ]
    readme = paper_dir / "README.md"
    readme.write_text(
        "\n".join(
            [
                "# Paper Figures",
                "",
                "These SVG figures are generated from the latest ablation CSV files.",
                "",
                "- `fig1_placement_stage_time_gpu.svg`: per-stage placement algorithm time and selected GPU count.",
                "- `fig2_target_stage_time_cost.svg`: per-stage target-builder time, estimated reconfiguration time, and fine actions.",
                "- `fig3_transition_stage_time_gpu_hw.svg`: per-stage transition-planner time, peak active GPUs, and estimated hardware time.",
                "",
                "The figures are vector graphics and can be imported directly into a paper or converted to PDF by the writing toolchain.",
            ]
        ),
        encoding="utf-8",
    )
    _inject_into_html(output_dir / "index.html", output_dir, paths)
    for path in paths:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
