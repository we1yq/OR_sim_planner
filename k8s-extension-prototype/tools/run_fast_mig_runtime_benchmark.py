from __future__ import annotations

import argparse
import json
import subprocess
import time
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT_DIR = REPO_ROOT / "reports" / "fast-mig-runtime-controller-benchmark-2026-05-18"
DEFAULT_DOC = REPO_ROOT / "docs" / "fast-mig-runtime-controller-benchmark-2026-05-18.md"

TEMPLATES = [
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

TEMPLATE_SLOT_SPECS = {
    "7": "0:8:7g",
    "4+3": "0:4:4g,4:4:3g",
    "4+2+1": "0:4:4g,4:2:2g,6:1:1g",
    "4+1+1+1": "0:4:4g,4:1:1g,5:1:1g,6:1:1g",
    "3+3": "0:4:3g,4:4:3g",
    "3+2+1": "0:4:3g,4:2:2g,6:1:1g",
    "3+1+1+1": "0:4:3g,4:1:1g,5:1:1g,6:1:1g",
    "2+2+3": "0:2:2g,2:2:2g,4:4:3g",
    "3+2+1+1": "0:2:2g,2:1:1g,3:1:1g,4:4:3g",
    "3+1+1+1+1": "0:1:1g,1:1:1g,2:1:1g,3:1:1g,4:4:3g",
    "2+2+2+1": "0:2:2g,2:2:2g,4:2:2g,6:1:1g",
    "2+2+1+1+1": "0:2:2g,2:1:1g,3:1:1g,4:2:2g,6:1:1g",
    "2+1+1+1+1+1": "0:2:2g,2:1:1g,3:1:1g,4:1:1g,5:1:1g,6:1:1g",
    "1+1+1+1+1+1+1": "0:1:1g,1:1:1g,2:1:1g,3:1:1g,4:1:1g,5:1:1g,6:1:1g",
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", default="or-sim")
    parser.add_argument("--agent-pod", default="fast-mig-node-agent-46bgl")
    parser.add_argument("--physical-gpu-id", default="rtx1-worker-gpu0")
    parser.add_argument("--logical-gpu-prefix", default="bench")
    parser.add_argument("--gpu-index", default="0")
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--doc", type=Path, default=DEFAULT_DOC)
    parser.add_argument("--skip-partial", action="store_true")
    args = parser.parse_args()

    args.report_dir.mkdir(parents=True, exist_ok=True)

    results: dict[str, Any] = {
        "startedAt": datetime.now().isoformat(timespec="seconds"),
        "node": "rtx1-worker",
        "gpu": "NVIDIA A100-PCIE-40GB",
        "namespace": args.namespace,
        "agentPod": args.agent_pod,
        "physicalGpuId": args.physical_gpu_id,
        "emptyToTemplate": [],
        "templateToEmpty": [],
        "partialReconfiguration": [],
    }

    clear_to_available(args, args.report_dir / "initial-clear")

    observed_layouts: dict[str, list[dict[str, Any]]] = {}
    for template in TEMPLATES:
        empty_record = apply_template_ready(args, template, args.report_dir / f"empty-to-{safe_name(template)}")
        results["emptyToTemplate"].append(empty_record)
        observed_layouts[template] = list(empty_record.get("migSlots", []))

        clear_record = clear_to_available(args, args.report_dir / f"{safe_name(template)}-to-empty")
        clear_record["fromTemplate"] = template
        results["templateToEmpty"].append(clear_record)

    if not args.skip_partial:
        candidates = partial_candidates(observed_layouts)
        results["partialCandidates"] = candidates
        for candidate in candidates:
            source = candidate["sourceTemplate"]
            target = candidate["targetTemplate"]
            prefix = args.report_dir / f"partial-{safe_name(source)}-to-{safe_name(target)}"
            apply_template_ready(args, source, prefix / "setup-source")
            partial_record = patch_slots_ready(args, candidate, prefix)
            results["partialReconfiguration"].append(partial_record)
            clear_to_available(args, prefix / "cleanup")

    results["endedAt"] = datetime.now().isoformat(timespec="seconds")
    (args.report_dir / "results.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
    args.doc.write_text(render_markdown(results, args.report_dir), encoding="utf-8")
    print(json.dumps({"report": str(args.report_dir / "results.json"), "doc": str(args.doc)}, indent=2))
    return 0


def apply_template_ready(args: argparse.Namespace, template: str, out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    start = time.monotonic()
    apply_wall_start = time.monotonic()
    apply_result = agent_json(args, ["apply-slots", TEMPLATE_SLOT_SPECS[template]])
    apply_result["template"] = template
    apply_wall = time.monotonic() - apply_wall_start
    write_json(out_dir / "apply.json", apply_result)

    registry_apply_seconds = controller_seconds(
        [
            "--namespace",
            args.namespace,
            "--apply-agent-mig-result-to-registry",
            str(out_dir / "apply.json"),
            "--physical-gpu-id",
            args.physical_gpu_id,
            "--apply-physical-gpu-registry",
        ]
    )

    refresh_wall_start = time.monotonic()
    refresh_result = agent_json(args, ["refresh-cdi"])
    refresh_wall = time.monotonic() - refresh_wall_start
    write_json(out_dir / "refresh-cdi.json", refresh_result)

    mark_ready_seconds = controller_seconds(
        [
            "--namespace",
            args.namespace,
            "--mark-mig-cdi-ready",
            str(out_dir / "refresh-cdi.json"),
            "--physical-gpu-id",
            args.physical_gpu_id,
            "--logical-gpu-id",
            f"{args.logical_gpu_prefix}-{safe_name(template)}",
            "--apply-physical-gpu-registry",
        ]
    )
    total = time.monotonic() - start
    mig_create = float(apply_result.get("createSeconds") or 0.0)
    post_apply_observe = max(0.0, apply_wall - mig_create)
    refresh_cdi = float(refresh_result.get("createSeconds") or refresh_wall)
    observe = post_apply_observe + refresh_cdi + registry_apply_seconds + mark_ready_seconds
    return {
        "template": template,
        "success": bool(apply_result.get("success")) and bool(refresh_result.get("success")),
        "totalReadySeconds": total,
        "migCreateSeconds": mig_create,
        "observeMigDevicesSeconds": observe,
        "postApplyObserveSeconds": post_apply_observe,
        "refreshCdiSeconds": refresh_cdi,
        "registryApplySeconds": registry_apply_seconds,
        "markReadySeconds": mark_ready_seconds,
        "migSlots": apply_result.get("migSlots", []),
    }


def clear_to_available(args: argparse.Namespace, out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    start = time.monotonic()
    clear_wall_start = time.monotonic()
    clear_result = agent_json(args, ["clear"])
    clear_wall = time.monotonic() - clear_wall_start
    write_json(out_dir / "clear.json", clear_result)

    registry_clear_seconds = controller_seconds(
        [
            "--namespace",
            args.namespace,
            "--apply-agent-mig-result-to-registry",
            str(out_dir / "clear.json"),
            "--physical-gpu-id",
            args.physical_gpu_id,
            "--apply-physical-gpu-registry",
        ],
        check=False,
    )

    refresh_wall_start = time.monotonic()
    refresh_result = agent_json(args, ["refresh-cdi"])
    refresh_wall = time.monotonic() - refresh_wall_start
    write_json(out_dir / "refresh-cdi-after-clear.json", refresh_result)

    sync_seconds = controller_seconds(
        [
            "--namespace",
            args.namespace,
            "--sync-physical-gpu-registry",
            "--apply-physical-gpu-registry",
        ],
        check=False,
    )
    release_seconds = controller_seconds(
        [
            "--namespace",
            args.namespace,
            "--release-physical-gpu",
            args.physical_gpu_id,
            "--apply-physical-gpu-registry",
        ],
        check=False,
    )
    total = time.monotonic() - start
    refresh_cdi = float(refresh_result.get("createSeconds") or refresh_wall)
    other = registry_clear_seconds + refresh_cdi + sync_seconds + release_seconds
    return {
        "success": bool(clear_result.get("success")) and bool(refresh_result.get("success")),
        "totalAvailableSeconds": total,
        "migClearSeconds": clear_wall,
        "otherCleanupSeconds": other,
        "registryClearSeconds": registry_clear_seconds,
        "refreshCdiSeconds": refresh_cdi,
        "syncRegistrySeconds": sync_seconds,
        "releaseRegistrySeconds": release_seconds,
    }


def patch_slots_ready(args: argparse.Namespace, candidate: dict[str, Any], out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    start = time.monotonic()
    patch_wall_start = time.monotonic()
    patch_result = agent_json(
        args,
        [
            "patch-slots",
            candidate["deleteSpec"],
            candidate["createSpec"],
            candidate["preserveSpec"],
        ],
        check=False,
    )
    patch_wall = time.monotonic() - patch_wall_start
    write_json(out_dir / "patch-slots.json", patch_result)
    if not patch_result.get("success"):
        return {
            **candidate,
            "success": False,
            "totalReadySeconds": time.monotonic() - start,
            "migMutationSeconds": patch_wall,
            "observeMigDevicesSeconds": 0.0,
            "message": patch_result.get("message"),
        }

    registry_apply_seconds = controller_seconds(
        [
            "--namespace",
            args.namespace,
            "--apply-agent-mig-result-to-registry",
            str(out_dir / "patch-slots.json"),
            "--physical-gpu-id",
            args.physical_gpu_id,
            "--apply-physical-gpu-registry",
        ]
    )
    refresh_wall_start = time.monotonic()
    refresh_result = agent_json(args, ["refresh-cdi"])
    refresh_wall = time.monotonic() - refresh_wall_start
    write_json(out_dir / "refresh-cdi.json", refresh_result)
    mark_ready_seconds = controller_seconds(
        [
            "--namespace",
            args.namespace,
            "--mark-mig-cdi-ready",
            str(out_dir / "refresh-cdi.json"),
            "--physical-gpu-id",
            args.physical_gpu_id,
            "--logical-gpu-id",
            f"{args.logical_gpu_prefix}-{safe_name(candidate['targetTemplate'])}",
            "--apply-physical-gpu-registry",
        ]
    )
    total = time.monotonic() - start
    delete_seconds = float(patch_result.get("deleteSeconds") or 0.0)
    create_seconds = float(patch_result.get("createSeconds") or 0.0)
    mig_mutation = delete_seconds + create_seconds
    post_patch_observe = max(0.0, patch_wall - mig_mutation)
    refresh_cdi = float(refresh_result.get("createSeconds") or refresh_wall)
    observe = post_patch_observe + refresh_cdi + registry_apply_seconds + mark_ready_seconds
    return {
        **candidate,
        "success": bool(refresh_result.get("success")),
        "totalReadySeconds": total,
        "migMutationSeconds": mig_mutation,
        "migDeleteSeconds": delete_seconds,
        "migCreateSeconds": create_seconds,
        "observeMigDevicesSeconds": observe,
        "postPatchObserveSeconds": post_patch_observe,
        "refreshCdiSeconds": refresh_cdi,
        "registryApplySeconds": registry_apply_seconds,
        "markReadySeconds": mark_ready_seconds,
        "migSlots": patch_result.get("migSlots", []),
    }


def partial_candidates(layouts: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for source, source_slots_raw in layouts.items():
        source_slots = normalized_slots(source_slots_raw)
        for target, target_slots_raw in layouts.items():
            if source == target:
                continue
            target_slots = normalized_slots(target_slots_raw)
            preserve = [slot for slot in source_slots if slot in set(target_slots)]
            if not preserve:
                continue
            delete = [slot for slot in source_slots if slot not in set(preserve)]
            create = [slot for slot in target_slots if slot not in set(preserve)]
            if not delete and not create:
                continue
            if not all(slot_covered_by_union(slot, delete) for slot in create):
                continue
            out.append(
                {
                    "sourceTemplate": source,
                    "targetTemplate": target,
                    "deleteSlots": delete,
                    "createSlots": create,
                    "preserveSlots": preserve,
                    "deleteSpec": slot_spec(delete),
                    "createSpec": slot_spec(create),
                    "preserveSpec": slot_spec(preserve),
                }
            )
    return out


def normalized_slots(raw: list[dict[str, Any]]) -> list[tuple[int, int, str]]:
    return sorted(
        [
            (int(item["slotStart"]), int(item["slotEnd"]), str(item["profile"]))
            for item in raw
        ],
        key=lambda item: (item[0], item[1], item[2]),
    )


def slot_covered_by_union(slot: tuple[int, int, str], covering: list[tuple[int, int, str]]) -> bool:
    start, end, _ = slot
    for idx in range(start, end):
        if not any(candidate_start <= idx < candidate_end for candidate_start, candidate_end, _ in covering):
            return False
    return True


def slot_spec(slots: list[tuple[int, int, str]]) -> str:
    return ",".join(f"{start}:{end - start}:{profile}" for start, end, profile in slots) or "-"


def agent_json(args: argparse.Namespace, command: list[str], check: bool = True) -> dict[str, Any]:
    completed = run(
        [
            "kubectl",
            "exec",
            "-n",
            args.namespace,
            args.agent_pod,
            "--",
            "/usr/local/bin/fast-mig-node-agent",
            "-json",
            "-gpu-index",
            args.gpu_index,
            *command,
        ],
        check=check,
    )
    text = completed.stdout.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"success": False, "message": text + completed.stderr}


def controller_seconds(command: list[str], check: bool = True) -> float:
    start = time.monotonic()
    run(["python3", str(REPO_ROOT / "controller" / "main.py"), *command], check=check)
    return time.monotonic() - start


def run(command: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=REPO_ROOT.parent,
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(json.dumps(value, indent=2), encoding="utf-8")


def safe_name(value: str) -> str:
    return value.replace("+", "_")


def fmt_s(value: Any) -> str:
    try:
        return f"{float(value):.3f}s"
    except (TypeError, ValueError):
        return "-"


def format_slots(value: Any) -> str:
    slots = list(value or [])
    if not slots:
        return "-"
    out = []
    for slot in slots:
        try:
            start, end, profile = slot
            out.append(f"`{profile} [{start},{end})`")
        except (TypeError, ValueError):
            out.append(f"`{slot}`")
    return ", ".join(out)


def render_markdown(results: dict[str, Any], report_dir: Path) -> str:
    empty = list(results.get("emptyToTemplate", []))
    clear = list(results.get("templateToEmpty", []))
    partial = [item for item in list(results.get("partialReconfiguration", [])) if item.get("success")]
    partial_failed = [item for item in list(results.get("partialReconfiguration", [])) if not item.get("success")]

    lines = [
        "# Fast MIG Runtime Controller Benchmark on rtx1",
        "",
        f"Date: {results.get('startedAt')} to {results.get('endedAt')} KST",
        "",
        f"Node: `{results.get('node')}`",
        "",
        f"GPU: {results.get('gpu')}",
        "",
        f"Controller pod: `{results.get('namespace')}/{results.get('agentPod')}`",
        "",
        f"Raw data: `{report_dir.relative_to(REPO_ROOT) / 'results.json'}`",
        "",
        "This benchmark measures the fast local MIG runtime path, not the GPU Operator extended-resource path.",
        "",
        "Readiness definitions:",
        "",
        "- `MIG create` is the elapsed time reported by `nvidia-smi mig -cgi ... -C` inside the runtime controller.",
        "- `MIG clear` is the wall time of the controller `clear` command, including best-effort CI/GI deletion and postcondition `nvidia-smi -L` verification.",
        "- `Observe MIG devices` includes post-mutation `nvidia-smi -L`, `nvidia-smi mig -lgi`, slot-to-MIG-UUID mapping, CDI spec refresh at `/var/run/cdi/management.nvidia.com-gpu.yaml`, and registry writes/mark-ready.",
        "- After `Observe MIG devices` succeeds, MIGRANT marks the physical GPU `activeQueue` for configured layouts. Pods may then bind directly with CDI annotation to the returned MIG UUID.",
        "- For template-to-empty, `Other cleanup` includes registry clear, CDI refresh after deletion, registry sync, and release back to `availableQueue`.",
        "",
        "## Summary",
        "",
        "| Measurement | Count | Min | Avg | Max |",
        "| --- | ---: | ---: | ---: | ---: |",
        summary_row("empty -> template ready", empty, "totalReadySeconds"),
        summary_row("template -> empty available", clear, "totalAvailableSeconds"),
        summary_row("partial reconfig ready", partial, "totalReadySeconds"),
        "",
        "## Empty To Template",
        "",
        "| Template | Total ready | MIG create | Observe MIG devices | CDI refresh |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for item in empty:
        lines.append(
            f"| `{item['template']}` | {fmt_s(item.get('totalReadySeconds'))} | "
            f"{fmt_s(item.get('migCreateSeconds'))} | {fmt_s(item.get('observeMigDevicesSeconds'))} | "
            f"{fmt_s(item.get('refreshCdiSeconds'))} |"
        )

    lines.extend(
        [
            "",
            "## Template To Empty",
            "",
            "| From template | Total available | MIG clear | Other cleanup | CDI refresh |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for item in clear:
        lines.append(
            f"| `{item.get('fromTemplate')}` | {fmt_s(item.get('totalAvailableSeconds'))} | "
            f"{fmt_s(item.get('migClearSeconds'))} | {fmt_s(item.get('otherCleanupSeconds'))} | "
            f"{fmt_s(item.get('refreshCdiSeconds'))} |"
        )

    lines.extend(
        [
            "",
            "## Partial Reconfiguration",
            "",
            "The table contains every partial source-target pair that is executable against the controller-observed NVIDIA placement layout. The preserved slots are not deleted or recreated by the runtime controller.",
            "",
            "| From | To | Preserved slots | Total ready | MIG delete+create | MIG delete | MIG create | Observe MIG devices |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for item in partial:
        lines.append(
            f"| `{item.get('sourceTemplate')}` | `{item.get('targetTemplate')}` | "
            f"{format_slots(item.get('preserveSlots'))} | "
            f"{fmt_s(item.get('totalReadySeconds'))} | {fmt_s(item.get('migMutationSeconds'))} | "
            f"{fmt_s(item.get('migDeleteSeconds'))} | {fmt_s(item.get('migCreateSeconds'))} | "
            f"{fmt_s(item.get('observeMigDevicesSeconds'))} |"
        )

    if partial_failed:
        lines.extend(
            [
                "",
                "## Partial Failures",
                "",
                "These candidates were generated from observed layouts but rejected by the actuator or hardware.",
                "",
                "| From | To | Message |",
                "| --- | --- | --- |",
            ]
        )
        for item in partial_failed:
            message = str(item.get("message", "")).replace("\n", " ")
            lines.append(f"| `{item.get('sourceTemplate')}` | `{item.get('targetTemplate')}` | {message} |")

    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "This path bypasses GPU Operator MIG Manager and Kubernetes extended-resource synchronization for runtime changes. It still reuses the NVIDIA driver/runtime and CDI device injection mechanism.",
            "",
            "For configured templates, `ready` means MIG UUIDs are known, CDI has been refreshed, and the registry can mark the physical GPU active for direct CDI pod binding. It does not wait for Kubernetes `nvidia.com/mig-*` allocatable resources.",
            "",
            "For empty state, `available` means the physical GPU has no MIG devices and the registry release path can return it to `availableQueue`.",
            "",
        ]
    )
    return "\n".join(lines)


def summary_row(name: str, rows: list[dict[str, Any]], key: str) -> str:
    values = [float(row[key]) for row in rows if row.get("success") and row.get(key) is not None]
    if not values:
        return f"| {name} | 0 | - | - | - |"
    return f"| {name} | {len(values)} | {min(values):.3f}s | {mean(values):.3f}s | {max(values):.3f}s |"


if __name__ == "__main__":
    raise SystemExit(main())
