#!/usr/bin/env python3
from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "reports" / "abstract-action-dags"


COLORS = {
    "mig": "#dbeafe",
    "pod": "#dcfce7",
    "router": "#fef3c7",
    "cleanup": "#fee2e2",
    "state": "#ede9fe",
    "blocked": "#e5e7eb",
}


DIAGRAMS: list[dict[str, Any]] = [
    {
        "name": "create-target-gpu",
        "title": "Create Target GPU",
        "description": "Provision a free physical GPU as a new logical GPU, deploy target workloads, then activate routing.",
        "nodes": [
            ("allocate", "Allocate GPU: reserve from availableQueue; assign pendingLogicalGpuId", "mig", 0, 0),
            ("configure", "Configure Template: move to transitionQueue; apply target MIG template", "mig", 1, 0),
            ("bind", "Bind GPU: bind activeLogicalGpuId; remove pendingLogicalGpuId; move to activeQueue", "state", 2, 0),
            ("resolve-uuid", "Resolve UUIDs: map target slots to real MIG device UUIDs", "mig", 3, 0),
            ("deploy", "Deploy Pods: deploy workload pods on resolved target slots", "pod", 4, 0),
            ("route", "Activate Route: route new requests to deployed pods", "router", 5, 0),
        ],
        "edges": [
            ("allocate", "configure"),
            ("configure", "bind"),
            ("bind", "resolve-uuid"),
            ("resolve-uuid", "deploy"),
            ("deploy", "route"),
        ],
    },
    {
        "name": "target-first-reconfiguration",
        "title": "Target-First Reconfiguration",
        "description": "Prepare a target GPU with pendingLogicalGpuId, drain and clear the old active binding, then bind the target as active.",
        "nodes": [
            ("allocate", "Allocate GPU: reserve from availableQueue; assign pendingLogicalGpuId", "mig", 0, 0),
            ("configure", "Configure Template: move to transitionQueue; apply target MIG template", "mig", 1, 0),
            ("bind", "Bind GPU: bind activeLogicalGpuId; remove pendingLogicalGpuId; move target to activeQueue", "state", 4, 0),
            ("resolve-new", "Resolve UUIDs: map target slots to real MIG device UUIDs", "mig", 5, 0),
            ("deploy-new", "Deploy Pods: deploy workload pods on resolved target slots", "pod", 6, 0),
            ("route", "Activate Route: route new requests to deployed pods", "router", 7, 0),
            ("stop-old", "Stop GPU Traffic: stop new requests entering source GPU pods", "router", 0, 1),
            ("drain-old", "Wait Drain: wait until queued requests and running work are zero", "router", 1, 1),
            ("delete-old", "Delete Pods: delete workload pods on the source GPU", "cleanup", 2, 1),
            ("clear-binding", "Clear GPU Binding: remove activeLogicalGpuId; assign pendingLogicalGpuId; move old GPU to transitionQueue", "cleanup", 3, 1),
            ("clear-template", "Clear Template: reset MIG template / set or-sim-empty", "cleanup", 4, 1),
            ("return-old", "Return GPU: remove pendingLogicalGpuId; move old GPU to availableQueue", "state", 5, 1),
        ],
        "edges": [
            ("allocate", "configure"),
            ("configure", "bind"),
            ("bind", "resolve-new"),
            ("resolve-new", "deploy-new"),
            ("deploy-new", "route"),
            ("stop-old", "drain-old"),
            ("drain-old", "delete-old"),
            ("delete-old", "clear-binding"),
            ("clear-binding", "clear-template"),
            ("clear-template", "return-old"),
            ("clear-binding", "bind"),
        ],
    },
    {
        "name": "in-place-reconfiguration",
        "title": "In-Place Reconfiguration",
        "description": "When all current pods can be deleted safely, delete the old side first and rebuild the same physical GPU in place.",
        "nodes": [
            ("accept", "Accept Queued Requests: stable serving slot accepts rerouted queued requests", "router", 1, 0, "optional"),
            ("stop", "Stop GPU Traffic: stop new requests entering this GPU", "router", 0, 1),
            ("reroute", "Reroute Queued Requests: move queued requests to stable serving slots", "router", 1, 1, "optional"),
            ("drain", "Wait Drain: wait until queued requests and running work are zero", "router", 2, 1),
            ("delete-pods", "Delete Pods: delete current workload pods", "cleanup", 3, 1),
            ("clear-binding", "Clear GPU Binding: remove activeLogicalGpuId; keep pendingLogicalGpuId; move GPU to transitionQueue", "cleanup", 4, 1),
            ("configure", "Configure Template: keep in transitionQueue; apply target MIG template", "mig", 5, 1),
            ("bind", "Bind GPU: bind activeLogicalGpuId; remove pendingLogicalGpuId; move to activeQueue", "state", 6, 1),
            ("resolve-uuid", "Resolve UUIDs: map target slots to real MIG device UUIDs", "mig", 7, 1),
            ("deploy", "Deploy Pods: deploy target workload pods", "pod", 8, 1),
            ("route", "Activate Route: route new requests to target pods", "router", 9, 1),
        ],
        "edges": [
            ("accept", "reroute"),
            ("stop", "reroute"),
            ("reroute", "drain"),
            ("stop", "drain"),
            ("drain", "delete-pods"),
            ("delete-pods", "clear-binding"),
            ("clear-binding", "configure"),
            ("configure", "bind"),
            ("bind", "resolve-uuid"),
            ("resolve-uuid", "deploy"),
            ("deploy", "route"),
        ],
        "optional_edges": {("accept", "reroute"), ("stop", "reroute"), ("reroute", "drain")},
    },
    {
        "name": "delete-gpu",
        "title": "Delete GPU",
        "description": "Stop traffic, optionally move queued requests to a stable serving slot, then drain and release the GPU.",
        "nodes": [
            ("accept", "Accept Queued Requests: stable serving slot accepts rerouted queued requests", "router", 1, 0, "optional"),
            ("stop", "Stop GPU Traffic: stop new requests entering source GPU pods", "router", 0, 1),
            ("reroute", "Reroute Queued Requests: move queued requests to stable serving slots", "router", 1, 1, "optional"),
            ("drain", "Wait Drain: wait until queued requests and running work are zero", "router", 2, 1),
            ("delete-pods", "Delete Pods: delete workload pods on the source GPU", "cleanup", 3, 1),
            ("clear-binding", "Clear GPU Binding: remove activeLogicalGpuId; assign pendingLogicalGpuId; move GPU to transitionQueue", "cleanup", 4, 1),
            ("clear-template", "Clear Template: reset MIG template / set or-sim-empty", "cleanup", 5, 1),
            ("available", "Return GPU: remove pendingLogicalGpuId; move GPU to availableQueue", "state", 6, 1),
        ],
        "edges": [
            ("accept", "reroute"),
            ("stop", "reroute"),
            ("reroute", "drain"),
            ("stop", "drain"),
            ("drain", "delete-pods"),
            ("delete-pods", "clear-binding"),
            ("clear-binding", "clear-template"),
            ("clear-template", "available"),
        ],
        "optional_edges": {("accept", "reroute"), ("stop", "reroute"), ("reroute", "drain")},
    },
    {
        "name": "workload-replacement",
        "title": "Workload Replacement on Existing Slot",
        "description": "Direct replacement when the old workload can be removed safely; optional reroute uses an existing target instance.",
        "nodes": [
            ("accept", "Accept Queued Requests: stable serving slot accepts rerouted queued requests", "router", 1, 0, "optional"),
            ("stop", "Stop Slot Traffic: stop new requests entering this slot/pod", "router", 0, 1),
            ("reroute", "Reroute Queued Requests: move queued requests to stable serving slots", "router", 1, 1, "optional"),
            ("drain", "Wait Drain: wait until queued requests and running work are zero", "router", 2, 1),
            ("remove", "Delete Pod: delete old workload pod", "cleanup", 3, 1),
            ("place", "Deploy Pod: deploy replacement workload pod on same slot", "pod", 4, 1),
            ("route", "Activate Route: route new requests to replacement pod", "router", 5, 1),
        ],
        "edges": [
            ("accept", "reroute"),
            ("stop", "reroute"),
            ("reroute", "drain"),
            ("stop", "drain"),
            ("drain", "remove"),
            ("remove", "place"),
            ("place", "route"),
        ],
        "optional_edges": {("accept", "reroute"), ("stop", "reroute"), ("reroute", "drain")},
    },
    {
        "name": "bridge-workload-replacement",
        "title": "Bridge-Assisted Workload Replacement",
        "description": "Notebook bridge case: temporarily place the old workload, replace the original slot, then remove the bridge pod in a later cleanup step.",
        "nodes": [
            ("bridge", "Deploy Bridge Pod: deploy temporary old-workload pod on compatible free slot", "pod", 0, 0),
            ("accept", "Accept Queued Requests: bridge pod accepts rerouted queued requests", "router", 2, 0),
            ("drain-bridge", "Drain Bridge: wait until bridge queued requests and running work are zero", "router", 3, 0),
            ("clear-bridge", "Delete Bridge Pod: delete temporary bridge pod", "cleanup", 4, 0),
            ("stop", "Stop Slot Traffic: stop new requests entering this slot/pod", "router", 0, 1),
            ("reroute", "Reroute Queued Requests: move queued requests to bridge pod", "router", 1, 1),
            ("drain", "Wait Drain: wait until original pod running work is zero", "router", 2, 1),
            ("remove", "Delete Pod: delete old workload pod", "cleanup", 3, 1),
            ("place", "Deploy Pod: deploy replacement workload pod on same slot", "pod", 4, 1),
            ("route", "Activate Route: route new requests to replacement pod", "router", 5, 1),
        ],
        "edges": [
            ("bridge", "accept"),
            ("stop", "reroute"),
            ("reroute", "accept"),
            ("reroute", "drain"),
            ("accept", "drain-bridge"),
            ("drain-bridge", "clear-bridge"),
            ("drain", "remove"),
            ("remove", "place"),
            ("place", "route"),
        ],
    },
    {
        "name": "batch-update",
        "title": "Batch Size Update",
        "description": "Hot-patch batch size when runtime supports it; otherwise fall back to workload replacement.",
        "nodes": [
            ("patch", "Patch Config: update batch size in workload/runtime config", "pod", 0, 0),
            ("reload", "Apply Batch: runtime reloads or applies new batch size without pod deletion", "pod", 1, 0),
            ("verify", "Verify Batch: confirm new batch size is active in serving/runtime metrics", "router", 2, 0),
            ("route", "Activate Route: keep or reactivate route to the updated pod", "router", 3, 0),
        ],
        "edges": [
            ("patch", "reload"),
            ("reload", "verify"),
            ("verify", "route"),
        ],
    },
]


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    outputs = []
    for diagram in DIAGRAMS:
        path = OUT_DIR / f"{diagram['name']}.svg"
        path.write_text(render_diagram(diagram), encoding="utf-8")
        outputs.append(path)
        print(path)
    manifest = {
        "description": "Paper-oriented abstract action DAG templates for MIGRANT transition actions.",
        "outputs": [str(path) for path in outputs],
    }
    (OUT_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    (OUT_DIR / "README.md").write_text(render_readme(outputs), encoding="utf-8")
    print(OUT_DIR / "manifest.json")
    print(OUT_DIR / "README.md")


def render_diagram(diagram: dict[str, Any]) -> str:
    nodes = {node[0]: node for node in diagram["nodes"]}
    node_w = 206
    node_h = 98
    col_w = 238
    row_h = 160
    margin_x = 52
    margin_y = 92
    max_col = max(node[3] for node in diagram["nodes"])
    max_row = max(node[4] for node in diagram["nodes"])
    width = margin_x * 2 + (max_col + 1) * col_w
    height = margin_y + (max_row + 1) * row_h + 76
    positions = {
        node_id: (
            margin_x + col * col_w + node_w / 2,
            margin_y + row * row_h + node_h / 2,
        )
        for node_id, _label, _kind, col, row, *_rest in diagram["nodes"]
    }
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        "<defs>",
        '<marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto" markerUnits="strokeWidth">',
        '<path d="M0,0 L0,6 L7,3 z" fill="#6b7280"/>',
        "</marker>",
        "</defs>",
        '<rect x="0" y="0" width="100%" height="100%" fill="#ffffff"/>',
        text(24, 32, diagram["title"], 22, anchor="start"),
        text(24, 55, diagram["description"], 12, anchor="start", fill="#4b5563"),
    ]
    optional_edges = {tuple(edge) for edge in diagram.get("optional_edges", set())}
    for src, dst in diagram["edges"]:
        x1, y1 = positions[src]
        x2, y2 = positions[dst]
        dashed = (src, dst) in optional_edges
        dash_attr = ' stroke-dasharray="5 4"' if dashed else ""
        parts.append(
            f'<line x1="{x1 + node_w / 2 - 4}" y1="{y1}" x2="{x2 - node_w / 2 + 4}" y2="{y2}" '
            f'stroke="#6b7280" stroke-width="1.4"{dash_attr} marker-end="url(#arrow)"/>'
        )
    for node_id, label, kind, _col, _row, *rest in diagram["nodes"]:
        x, y = positions[node_id]
        fill = COLORS[kind]
        optional = bool(rest and rest[0] == "optional")
        dash_attr = ' stroke-dasharray="5 4"' if optional else ""
        parts.append(f'<rect x="{x - node_w / 2}" y="{y - node_h / 2}" width="{node_w}" height="{node_h}" rx="5" fill="{fill}" stroke="#111827" stroke-width="1"{dash_attr}/>')
        title, detail = split_action_label(label)
        parts.append(f"<title>{html.escape(label)}</title>")
        title_lines = wrap(title, 26)[:2]
        detail_lines = wrap(detail, 31) if detail else []
        all_lines = [(line, 10, "#111827") for line in title_lines] + [(line, 8, "#374151") for line in detail_lines]
        start_y = y - ((len(all_lines) - 1) * 10) / 2 + 3
        for idx, (line, size, fill_color) in enumerate(all_lines):
            parts.append(text(x, start_y + idx * 11, line, size, fill=fill_color))
    parts.append(legend(width - 430, height - 25))
    parts.append("</svg>")
    return "\n".join(parts) + "\n"


def render_readme(outputs: list[Path]) -> str:
    rows = "\n".join(f"- `{path.name}`" for path in outputs)
    return (
        "# Abstract Action DAG Figures\n\n"
        "These SVGs are paper-oriented templates for MIGRANT transition roots. "
        "They describe action dependencies independent of any concrete stage.\n\n"
        "The canonical action wording and queue/binding semantics live in "
        "[`docs/abstract-transition-actions.md`](../../docs/abstract-transition-actions.md).\n\n"
        "## Generated SVGs\n\n"
        f"{rows}\n"
    )


def split_action_label(value: str) -> tuple[str, str]:
    if ":" not in value:
        return value, ""
    title, detail = value.split(":", 1)
    return title.strip(), detail.strip()


def wrap(value: str, width: int) -> list[str]:
    words = value.split()
    lines: list[str] = []
    cur: list[str] = []
    for word in words:
        candidate = " ".join([*cur, word])
        if cur and len(candidate) > width:
            lines.append(" ".join(cur))
            cur = [word]
        else:
            cur.append(word)
    if cur:
        lines.append(" ".join(cur))
    return lines or [value]


def legend(x: int, y: int) -> str:
    items = [
        ("MIG/state", COLORS["mig"]),
        ("binding/state", COLORS["state"]),
        ("workload/pod", COLORS["pod"]),
        ("router/drain", COLORS["router"]),
        ("cleanup", COLORS["cleanup"]),
        ("optional", "#ffffff"),
    ]
    parts = []
    cur = x
    for label, color in items:
        dash_attr = ' stroke-dasharray="3 2"' if label == "optional" else ""
        parts.append(f'<rect x="{cur}" y="{y - 12}" width="14" height="14" fill="{color}" stroke="#111827" stroke-width="0.8"{dash_attr}/>')
        parts.append(text(cur + 18, y, label, 10, anchor="start", fill="#4b5563"))
        cur += 84
    return "\n".join(parts)


def text(x: float, y: float, value: Any, size: int, *, anchor: str = "middle", fill: str = "#111827") -> str:
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" text-anchor="{anchor}" '
        f'font-family="Arial, sans-serif" font-size="{size}" fill="{fill}">'
        f"{html.escape(str(value))}</text>"
    )


if __name__ == "__main__":
    main()
