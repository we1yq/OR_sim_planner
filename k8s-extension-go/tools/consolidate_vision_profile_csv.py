#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path


PROFILE_NAMES = {
    "1g": "NVIDIA A100-PCIE-40GB MIG 1g.5gb",
    "2g": "NVIDIA A100-PCIE-40GB MIG 2g.10gb",
    "3g": "NVIDIA A100-PCIE-40GB MIG 3g.20gb",
    "4g": "NVIDIA A100-PCIE-40GB MIG 4g.20gb",
    "7g": "NVIDIA A100-PCIE-40GB MIG 7g.40gb",
}

CNN_MODELS = {"resnet50", "vgg16"}
VIT_MODELS = {"vit_base"}

COMMON_PREFIX = [
    "mig_uuid",
    "mig_name",
    "mig_profile",
    "node",
    "gpu_index",
    "model",
    "batch",
]

COMMON_SUFFIX = [
    "dtype",
    "engine",
    "status",
    "error",
    "sample_count",
    "time_ms_mean",
    "time_ms_p50",
    "time_ms_p95",
    "time_ms_p99",
    "throughput_rps",
    "peak_alloc_mb",
    "peak_reserved_mb",
    "free_mb_before",
    "free_mb_after",
    "source_run_id",
]

CNN_FIELDS = COMMON_PREFIX + ["input_hw"] + COMMON_SUFFIX
VIT_FIELDS = COMMON_PREFIX + ["image_size", "patch_size", "num_tokens"] + COMMON_SUFFIX


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert profile matrix raw CSVs into vision bench CSVs.")
    parser.add_argument("--raw-dir", required=True)
    parser.add_argument("--out-dir", default="profile/current")
    args = parser.parse_args()

    raw_dir = Path(args.raw_dir)
    out_dir = Path(args.out_dir)
    cnn_rows: list[dict[str, str]] = []
    vit_rows: list[dict[str, str]] = []

    for path in sorted(raw_dir.glob("*-runtime-raw.csv")):
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                if row.get("family") != "vision":
                    continue
                model = row.get("model", "")
                if model in CNN_MODELS:
                    cnn_rows.append(convert_cnn(row))
                elif model in VIT_MODELS:
                    vit_rows.append(convert_vit(row))

    cnn_rows.sort(key=sort_key)
    vit_rows.sort(key=sort_key)
    write(out_dir / "cnn_bench.csv", CNN_FIELDS, cnn_rows)
    write(out_dir / "vit_base_bench.csv", VIT_FIELDS, vit_rows)
    print(f"wrote {out_dir / 'cnn_bench.csv'}: {len(cnn_rows)} rows")
    print(f"wrote {out_dir / 'vit_base_bench.csv'}: {len(vit_rows)} rows")
    return 0


def convert_base(row: dict[str, str], model: str) -> dict[str, str]:
    profile = row.get("mig_profile", "")
    return {
        "mig_uuid": row.get("mig_uuid", ""),
        "mig_name": PROFILE_NAMES.get(profile, ""),
        "mig_profile": profile,
        "node": row.get("node", ""),
        "gpu_index": row.get("gpu_index", ""),
        "model": model,
        "batch": row.get("batch", ""),
        "dtype": "fp16",
        "engine": "torchvision",
        "status": row.get("status", ""),
        "error": row.get("error", ""),
        "sample_count": row.get("sample_count", ""),
        "time_ms_mean": row.get("latency_ms_mean", ""),
        "time_ms_p50": row.get("latency_ms_p50", ""),
        "time_ms_p95": row.get("latency_ms_p95", ""),
        "time_ms_p99": row.get("latency_ms_p99", ""),
        "throughput_rps": row.get("throughput_rps", ""),
        "peak_alloc_mb": row.get("peak_alloc_mb", ""),
        "peak_reserved_mb": row.get("peak_reserved_mb", ""),
        "free_mb_before": "",
        "free_mb_after": "",
        "source_run_id": row.get("run_id", ""),
    }


def convert_cnn(row: dict[str, str]) -> dict[str, str]:
    out = convert_base(row, row.get("model", ""))
    out["input_hw"] = "224x224"
    return out


def convert_vit(row: dict[str, str]) -> dict[str, str]:
    out = convert_base(row, "vit-base-patch16-224")
    out["image_size"] = "224"
    out["patch_size"] = "16"
    out["num_tokens"] = "197"
    return out


def write(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def sort_key(row: dict[str, str]) -> tuple[str, int, str, int, int]:
    return (
        row["model"],
        int(row["batch"]),
        row["node"],
        int(row["gpu_index"]),
        profile_order(row["mig_profile"]),
    )


def profile_order(profile: str) -> int:
    return {"1g": 1, "2g": 2, "3g": 3, "4g": 4, "7g": 7}.get(profile, 99)


if __name__ == "__main__":
    raise SystemExit(main())
