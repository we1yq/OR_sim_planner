from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


WORKLOADS = [
    "resnet50_image",
    "vgg16_image",
    "vit_base_image",
    "gpt2_p64_o64",
    "gpt2_p512_o512",
    "llama_p1024_o128",
    "llama_p2048_o64",
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("replay_dir", type=Path)
    parser.add_argument(
        "--measurements",
        type=Path,
        default=None,
        help="Path to template_throughput_measurements.csv. Defaults to replay_dir/template_throughput_measurements.csv.",
    )
    args = parser.parse_args()

    replay_dir = args.replay_dir
    measurements_path = args.measurements or replay_dir / "template_throughput_measurements.csv"
    measurements = pd.read_csv(measurements_path)
    allocation_map = pd.read_csv(replay_dir / "allocation_template_map.csv")
    trace = pd.read_csv(replay_dir / "request_rate_30min.csv")

    required = {"template_id", "trial", "instance_idx", "workload", "measured_throughput_rps"}
    missing = sorted(required - set(measurements.columns))
    if missing:
        raise ValueError(f"{measurements_path} is missing required columns: {missing}")

    instance_mean = (
        measurements.groupby(["template_id", "instance_idx", "workload"], as_index=False)["measured_throughput_rps"]
        .mean()
        .rename(columns={"measured_throughput_rps": "mean_instance_throughput_rps"})
    )
    template_capacity = (
        instance_mean.groupby(["template_id", "workload"], as_index=False)["mean_instance_throughput_rps"]
        .sum()
        .rename(columns={"mean_instance_throughput_rps": "template_capacity_rps"})
    )

    expanded = allocation_map.merge(template_capacity, on="template_id", how="left")
    expanded["template_capacity_rps"] = expanded["template_capacity_rps"].fillna(0.0)
    epoch_capacity = (
        expanded.groupby(["method", "epoch", "workload"], as_index=False)["template_capacity_rps"]
        .sum()
        .rename(columns={"template_capacity_rps": "measured_capacity_rps"})
    )

    demand_rows = []
    for _, row in trace.iterrows():
        for workload in WORKLOADS:
            demand_rows.append(
                {
                    "epoch": int(row["epoch"]),
                    "workload": workload,
                    "demand_rate_rps": float(row[workload]),
                }
            )
    demand = pd.DataFrame(demand_rows)
    out = epoch_capacity.merge(demand, on=["epoch", "workload"], how="left")
    out["capacity_ratio"] = out.apply(
        lambda row: row["measured_capacity_rps"] / row["demand_rate_rps"]
        if float(row["demand_rate_rps"]) > 1e-12
        else 1.0,
        axis=1,
    )
    out = out[["method", "epoch", "workload", "demand_rate_rps", "measured_capacity_rps", "capacity_ratio"]]
    out.to_csv(replay_dir / "epoch_workload_measured_capacity.csv", index=False)

    nonzero = out[out["demand_rate_rps"] > 1e-12]
    min_ratio = (
        nonzero.groupby(["method", "epoch"], as_index=False)["capacity_ratio"]
        .min()
        .rename(columns={"capacity_ratio": "min_capacity_ratio"})
    )
    min_ratio.to_csv(replay_dir / "epoch_min_capacity_ratio.csv", index=False)
    template_capacity.to_csv(replay_dir / "template_workload_measured_capacity.csv", index=False)
    print(f"Wrote {replay_dir / 'epoch_workload_measured_capacity.csv'}")
    print(f"Wrote {replay_dir / 'epoch_min_capacity_ratio.csv'}")


if __name__ == "__main__":
    main()
