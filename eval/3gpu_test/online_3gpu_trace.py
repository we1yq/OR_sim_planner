from __future__ import annotations

import csv
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


TEST_ROOT = Path(__file__).resolve().parent
EVAL_ROOT = TEST_ROOT.parent
REPO_ROOT = EVAL_ROOT.parent
ALGORITHM_ROOT = EVAL_ROOT / "algorithm"
PLANNER_ROOT = ALGORITHM_ROOT / "ours/planner_engine"
SOURCE_ROOT = EVAL_ROOT / "results/online_24h"
RESULT_ROOT = TEST_ROOT / "results/online_3gpu"
FIGURE_ROOT = TEST_ROOT / "figures"
DEFAULT_SOURCE_TRACE_DIR = SOURCE_ROOT / "20260710-021814"

sys.path.insert(0, str(EVAL_ROOT))
sys.path.insert(0, str(ALGORITHM_ROOT))
sys.path.insert(0, str(PLANNER_ROOT))

import plan_quality_controlled as pq  # noqa: E402
from algorithm.baselines.parvagpu import allocate_parvagpu_mig  # noqa: E402


WORKLOADS = pq.WORKLOADS
WORKLOAD_LABELS = pq.WORKLOAD_LABELS

# Seven source points plus one zero-demand shutdown point produce seven
# 30-minute transitions after compression.
# Each selected source epoch is solved by SliceWise within 3 GPUs in the
# current online_24h trace, while retaining visible total-load and mix shifts.
SOURCE_EPOCHS = [1, 5, 13, 15, 37, 45, 47]
EPOCH_MINUTES = 30
MAX_GPUS = 3


def main() -> None:
    RESULT_ROOT.mkdir(parents=True, exist_ok=True)
    FIGURE_ROOT.mkdir(parents=True, exist_ok=True)

    source_dir = latest_online_24h_dir()
    source_trace = pd.read_csv(source_dir / "request_rate_30min.csv")
    trace = build_3gpu_trace(source_trace)

    timestamp = time.strftime("%Y%m%d-%H%M%S")
    out_dir = RESULT_ROOT / timestamp
    out_dir.mkdir(parents=True, exist_ok=True)

    feasible_df = pq.build_feasible_option_df()
    options = pq.serving_options_from_df(feasible_df)
    slicewise_probe, parva_probe = validate_gpu_counts(trace, feasible_df, options)

    trace.to_csv(out_dir / "request_rate_30min.csv", index=False)
    write_csv(out_dir / "request_rate_summary.csv", summarize_rates(trace))
    write_csv(out_dir / "slicewise_target_gpu_probe.csv", slicewise_probe)
    write_csv(out_dir / "parvagpu_target_gpu_probe.csv", parva_probe)

    metadata = {
        "source_trace_dir": str(source_dir),
        "source_epoch_count": int(len(source_trace)),
        "source_epochs": SOURCE_EPOCHS,
        "duration_hours": float(trace["hour"].max()),
        "epoch_minutes": EPOCH_MINUTES,
        "transition_count": int(len(trace) - 1),
        "shutdown": {
            "enabled": True,
            "epoch": int(trace.iloc[-1]["epoch"]),
            "hour": float(trace.iloc[-1]["hour"]),
            "reason": "drive all workload demand to zero so the system can return to the empty/base state",
        },
        "max_allowed_gpu_count": MAX_GPUS,
        "validation": {
            "slicewise_max_gpu_count": max(row["gpu_count"] for row in slicewise_probe),
            "parvagpu_max_gpu_count": max(row["gpu_count"] for row in parva_probe),
        },
        "intent": (
            "3-GPU short replay derived from selected 24h trace epochs; "
            "keeps request-rate shape changes while bounding target demand to 3 GPUs."
        ),
    }
    (out_dir / "trace_metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    (RESULT_ROOT / "latest_path.txt").write_text(str(out_dir) + "\n", encoding="utf-8")

    plot_request_rates(trace, FIGURE_ROOT / "online_3gpu_request_rate")
    plot_normalized_request_rates(trace, FIGURE_ROOT / "online_3gpu_request_rate_normalized")

    print(f"ONLINE_3GPU_DIR={out_dir}")
    print(f"REQUEST_RATE_FIGURE={FIGURE_ROOT / 'online_3gpu_request_rate.pdf'}")


def latest_online_24h_dir() -> Path:
    override = os.environ.get("ONLINE_3GPU_SOURCE_TRACE_DIR")
    if override:
        path = Path(override)
        if path.exists():
            return path
        raise FileNotFoundError(f"ONLINE_3GPU_SOURCE_TRACE_DIR does not exist: {path}")
    if DEFAULT_SOURCE_TRACE_DIR.exists():
        return DEFAULT_SOURCE_TRACE_DIR
    latest = SOURCE_ROOT / "latest_path.txt"
    if latest.exists():
        path = Path(latest.read_text(encoding="utf-8").strip())
        if path.exists():
            return path
    candidates = sorted(SOURCE_ROOT.glob("20*"))
    if not candidates:
        raise FileNotFoundError("No online_24h trace directory found")
    return candidates[-1]


def build_3gpu_trace(source_trace: pd.DataFrame) -> pd.DataFrame:
    missing = [epoch for epoch in SOURCE_EPOCHS if epoch not in set(source_trace["epoch"].astype(int))]
    if missing:
        raise ValueError(f"source trace is missing selected epochs: {missing}")

    rows: list[dict[str, Any]] = []
    by_epoch = source_trace.set_index(source_trace["epoch"].astype(int))
    for new_epoch, source_epoch in enumerate(SOURCE_EPOCHS):
        src = by_epoch.loc[source_epoch]
        row: dict[str, Any] = {
            "epoch": new_epoch,
            "minute": new_epoch * EPOCH_MINUTES,
            "hour": new_epoch * EPOCH_MINUTES / 60.0,
            "source_epoch": int(source_epoch),
            "source_hour": float(src["hour"]),
        }
        for workload in WORKLOADS:
            row[workload] = float(src[workload])
        rows.append(row)
    shutdown_epoch = len(rows)
    row = {
        "epoch": shutdown_epoch,
        "minute": shutdown_epoch * EPOCH_MINUTES,
        "hour": shutdown_epoch * EPOCH_MINUTES / 60.0,
        "source_epoch": -1,
        "source_hour": -1.0,
    }
    for workload in WORKLOADS:
        row[workload] = 0.0
    rows.append(row)
    return pd.DataFrame(rows)


def demand_from_row(row: pd.Series) -> dict[str, float]:
    return {workload: float(row[workload]) for workload in WORKLOADS}


def validate_gpu_counts(trace: pd.DataFrame, feasible_df: pd.DataFrame, options: list[Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    slicewise_rows: list[dict[str, Any]] = []
    parva_rows: list[dict[str, Any]] = []
    prev_state = None
    for _, row in trace.iterrows():
        demand = demand_from_row(row)
        epoch = int(row["epoch"])
        ours = pq.run_ours_target(
            scenario_id=f"online3gpu_E{epoch:02d}",
            demand=demand,
            feasible_df=feasible_df,
            prev_state=prev_state,
        )
        parva = allocate_parvagpu_mig(
            scenario_id=f"online3gpu_parva_E{epoch:02d}",
            demand=demand,
            options=options,
        )
        if int(ours.result.gpu_count) > MAX_GPUS:
            raise RuntimeError(f"SliceWise target for epoch {epoch} needs {ours.result.gpu_count} GPUs")
        if int(parva.gpu_count) > MAX_GPUS:
            raise RuntimeError(f"ParvaGPU target for epoch {epoch} needs {parva.gpu_count} GPUs")
        min_cov, _ = pq.service_rate_coverage(ours.result, demand)
        slicewise_rows.append(
            {
                "epoch": epoch,
                "hour": float(row["hour"]),
                "source_epoch": int(row["source_epoch"]),
                "source_hour": float(row["source_hour"]),
                "gpu_count": int(ours.result.gpu_count),
                "allocated_slices": int(ours.result.allocated_slices),
                "min_service_rate_ratio": float(min_cov),
            }
        )
        min_cov, _ = pq.service_rate_coverage(parva, demand)
        parva_rows.append(
            {
                "epoch": epoch,
                "hour": float(row["hour"]),
                "source_epoch": int(row["source_epoch"]),
                "source_hour": float(row["source_hour"]),
                "gpu_count": int(parva.gpu_count),
                "allocated_slices": int(parva.allocated_slices),
                "min_service_rate_ratio": float(min_cov),
            }
        )
        prev_state = ours.state
    return slicewise_rows, parva_rows


def summarize_rates(trace: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for workload in WORKLOADS:
        values = trace[workload].to_numpy(dtype=float)
        rows.append(
            {
                "workload": workload,
                "label": WORKLOAD_LABELS[workload],
                "mean_rps": float(np.mean(values)),
                "min_rps": float(np.min(values)),
                "max_rps": float(np.max(values)),
                "peak_to_mean": float(np.max(values) / max(np.mean(values), 1e-12)),
            }
        )
    return rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    keys = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def plot_request_rates(trace: pd.DataFrame, path: Path) -> None:
    os.environ.setdefault("MPLCONFIGDIR", str(REPO_ROOT / ".matplotlib-cache"))
    import matplotlib.pyplot as plt

    configure_matplotlib(plt)
    colors = workload_colors()
    fig, ax = plt.subplots(figsize=(5.3, 2.55))
    hours = trace["hour"].to_numpy(dtype=float)
    end_hour = float(hours[-1])
    step_hours = np.append(hours, end_hour)
    for workload in WORKLOADS:
        values = trace[workload].to_numpy(dtype=float)
        step_values = np.append(values, values[-1])
        ax.step(
            step_hours,
            step_values,
            where="post",
            label=WORKLOAD_LABELS[workload],
            color=colors[workload],
            linewidth=1.55,
        )
    ax.set_yscale("symlog", linthresh=0.01)
    ax.set_xlim(0, end_hour)
    ax.set_xticks(np.arange(0, end_hour + 0.001, 0.5))
    ax.set_xlabel("Hour")
    ax.set_ylabel("Request rate (req/s)")
    style_axes(ax)
    ax.legend(
        ncol=4,
        loc="lower left",
        bbox_to_anchor=(0.0, 1.02),
        frameon=False,
        borderaxespad=0.0,
        handlelength=1.3,
        handletextpad=0.4,
        columnspacing=0.85,
        labelspacing=0.2,
    )
    fig.tight_layout(pad=0.25)
    fig.savefig(path.with_suffix(".pdf"), bbox_inches="tight")
    fig.savefig(path.with_suffix(".png"), bbox_inches="tight", dpi=240)
    plt.close(fig)


def plot_normalized_request_rates(trace: pd.DataFrame, path: Path) -> None:
    os.environ.setdefault("MPLCONFIGDIR", str(REPO_ROOT / ".matplotlib-cache"))
    import matplotlib.pyplot as plt

    configure_matplotlib(plt)
    colors = workload_colors()
    fig, ax = plt.subplots(figsize=(5.3, 2.55))
    hours = trace["hour"].to_numpy(dtype=float)
    end_hour = float(hours[-1])
    step_hours = np.append(hours, end_hour)
    for workload in WORKLOADS:
        values = trace[workload].to_numpy(dtype=float)
        normalized = values / max(float(np.mean(values)), 1e-12)
        step_values = np.append(normalized, normalized[-1])
        ax.step(
            step_hours,
            step_values,
            where="post",
            label=WORKLOAD_LABELS[workload],
            color=colors[workload],
            linewidth=1.55,
        )
    ax.axhline(1.0, color="#555555", linewidth=0.7, linestyle="--")
    ax.set_xlim(0, end_hour)
    ax.set_xticks(np.arange(0, end_hour + 0.001, 0.5))
    ax.set_xlabel("Hour")
    ax.set_ylabel("Rate / mean")
    style_axes(ax)
    ax.legend(
        ncol=4,
        loc="lower left",
        bbox_to_anchor=(0.0, 1.02),
        frameon=False,
        borderaxespad=0.0,
        handlelength=1.3,
        handletextpad=0.4,
        columnspacing=0.85,
        labelspacing=0.2,
    )
    fig.tight_layout(pad=0.25)
    fig.savefig(path.with_suffix(".pdf"), bbox_inches="tight")
    fig.savefig(path.with_suffix(".png"), bbox_inches="tight", dpi=240)
    plt.close(fig)


def configure_matplotlib(plt: Any) -> None:
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 9,
            "axes.labelsize": 9,
            "legend.fontsize": 7,
            "xtick.labelsize": 8,
            "ytick.labelsize": 8,
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
        }
    )


def style_axes(ax: Any) -> None:
    ax.grid(axis="y", color="#D9DEE8", linewidth=0.55, which="major")
    ax.grid(axis="x", color="#EEF2F7", linewidth=0.45)
    ax.set_axisbelow(True)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)


def workload_colors() -> dict[str, str]:
    return {
        "resnet50_image": "#EF767A",
        "vgg16_image": "#456990",
        "vit_base_image": "#48C0AA",
        "gpt2_p64_o64": "#D87659",
        "gpt2_p512_o512": "#B395BD",
        "llama_p1024_o128": "#E9C46A",
        "llama_p2048_o64": "#299D8F",
    }


if __name__ == "__main__":
    main()
