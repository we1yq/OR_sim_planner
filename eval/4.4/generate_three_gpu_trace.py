from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
TRACE_DIR = Path(__file__).resolve().parent / "three_gpu_executable_trace"

sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "algorithm"))
sys.path.insert(0, str(ROOT / "algorithm" / "ours" / "planner_engine"))

import plan_quality_controlled as pq  # noqa: E402


WORKLOADS = pq.WORKLOADS

# Real three-GPU runner workload names. The real runner maps these to
# llama_p1024/o128, gpt2_p64/o64, and ResNet50 image-serving workloads.
STAGES = [
    {
        "round": 0,
        "intent": "initial provision: mixed workload on one GPU",
        "resnet50": 500.0,
        "gpt2": 0.50,
        "llama": 0.30,
    },
    {
        "round": 1,
        "intent": "scale up and bridge reconfiguration",
        "resnet50": 1150.0,
        "gpt2": 0.90,
        "llama": 0.18,
    },
    {
        "round": 2,
        "intent": "language/vision mix shift with replacement pressure",
        "resnet50": 720.0,
        "gpt2": 0.25,
        "llama": 0.66,
    },
    {
        "round": 3,
        "intent": "scale down and compaction",
        "resnet50": 280.0,
        "gpt2": 0.62,
        "llama": 0.16,
    },
    {
        "round": 4,
        "intent": "shutdown and release resources",
        "resnet50": 0.0,
        "gpt2": 0.0,
        "llama": 0.0,
    },
]


def full_demand(stage: dict[str, Any]) -> dict[str, float]:
    demand = {workload: 0.0 for workload in WORKLOADS}
    demand["resnet50_image"] = float(stage["resnet50"])
    demand["gpt2_p64_o64"] = float(stage["gpt2"])
    demand["llama_p1024_o128"] = float(stage["llama"])
    return demand


def validate_trace() -> list[dict[str, Any]]:
    feasible_df = pq.build_feasible_option_df()
    prev_state = None
    rows: list[dict[str, Any]] = []
    for stage in STAGES:
        result = pq.run_ours_target(
            scenario_id=f"section44_R{int(stage['round']):02d}",
            demand=full_demand(stage),
            feasible_df=feasible_df,
            prev_state=prev_state,
        )
        allocation = result.result
        if int(allocation.gpu_count) > 3:
            raise RuntimeError(
                f"round {stage['round']} needs {allocation.gpu_count} GPUs, "
                "which exceeds the three-GPU executable testbed"
            )
        min_cov, _ = pq.service_rate_coverage(allocation, full_demand(stage))
        rows.append(
            {
                "round": int(stage["round"]),
                "intent": stage["intent"],
                "gpu_count": int(allocation.gpu_count),
                "allocated_slices": int(allocation.allocated_slices),
                "min_service_rate_ratio": float(min_cov),
            }
        )
        prev_state = result.state
    return rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    TRACE_DIR.mkdir(parents=True, exist_ok=True)
    validation = validate_trace()

    stages_for_runner = [
        {
            "resnet50": float(stage["resnet50"]),
            "gpt2": float(stage["gpt2"]),
            "llama": float(stage["llama"]),
        }
        for stage in STAGES
    ]
    (TRACE_DIR / "stages.json").write_text(json.dumps(stages_for_runner, indent=2) + "\n", encoding="utf-8")

    full_rows = []
    for stage in STAGES:
        row: dict[str, Any] = {
            "epoch": int(stage["round"]),
            "minute": int(stage["round"]) * 30,
            "hour": float(stage["round"]) * 0.5,
            "intent": stage["intent"],
        }
        row.update(full_demand(stage))
        full_rows.append(row)
    write_csv(TRACE_DIR / "request_rate_30min.csv", full_rows)
    write_csv(TRACE_DIR / "target_probe.csv", validation)

    metadata = {
        "experiment": "Section 4.4 real three-GPU executable replay",
        "planning_rounds": len(STAGES),
        "transitions": len(STAGES),
        "notes": [
            "The first round provisions from an empty cluster.",
            "The last round shuts down all workloads and releases resources.",
            "All SliceWise targets were validated to require at most three GPUs.",
        ],
        "active_workloads": {
            "resnet50": "resnet50_image",
            "gpt2": "gpt2_p64_o64",
            "llama": "llama_p1024_o128",
        },
    }
    (TRACE_DIR / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")

    readme = f"""# Section 4.4 Three-GPU Executable Trace

This folder contains the short trace for the Section 4.4 real three-GPU executable replay.

## Files

- `stages.json`: input for `eval/3gpu_test/real_3gpu_k8s_experiment.py --stages-json`.
- `request_rate_30min.csv`: full workload-schema demand trace.
- `target_probe.csv`: local SliceWise target probe for each planning round.
- `metadata.json`: trace metadata.

## Intended Use

Run the real executor with:

```bash
python eval/3gpu_test/real_3gpu_k8s_experiment.py \\
  --run-id real3gpu-section44-<date> \\
  --stages-json "$(cat eval/4.4/three_gpu_executable_trace/stages.json)"
```

The replay has {len(STAGES)} planning rounds. Round 0 provisions from an empty cluster,
rounds 1--4 exercise structural changes, and round 5 shuts the system down.
"""
    (TRACE_DIR / "README.md").write_text(readme, encoding="utf-8")
    print(f"Wrote {TRACE_DIR}")


if __name__ == "__main__":
    main()
