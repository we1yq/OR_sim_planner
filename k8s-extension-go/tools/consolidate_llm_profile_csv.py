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

MODEL_NAMES = {
    "gpt2-medium": "gpt2-medium",
    "llama32_3b": "Llama-3.2-3B",
}


FIELDNAMES = [
    "mig_uuid",
    "mig_name",
    "mig_profile",
    "node",
    "gpu_index",
    "model",
    "batch",
    "prompt_len",
    "output_tokens",
    "dtype",
    "engine",
    "status",
    "error",
    "sample_count",
    "ttft_ms",
    "ttft_ms_p50",
    "ttft_ms_p95",
    "ttft_ms_p99",
    "tpot_ms",
    "tpot_ms_p50",
    "tpot_ms_p95",
    "tpot_ms_p99",
    "decode_tps",
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert profile matrix raw CSVs into LLM bench CSVs.")
    parser.add_argument("--raw-dir", required=True)
    parser.add_argument("--out-dir", default="profile/current")
    args = parser.parse_args()

    raw_dir = Path(args.raw_dir)
    out_dir = Path(args.out_dir)
    by_output = {
        "gpt2m_streaming_bench.csv": [],
        "llama32_3b_streaming_bench.csv": [],
    }

    for path in sorted(raw_dir.glob("*-runtime-raw.csv")):
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                if row.get("family") != "llm":
                    continue
                output = output_name(row.get("model", ""))
                if output is None:
                    continue
                by_output[output].append(convert_row(row))

    for name, rows in by_output.items():
        rows.sort(key=lambda r: (r["model"], int(r["prompt_len"]), int(r["output_tokens"]), r["node"], int(r["gpu_index"]), profile_order(r["mig_profile"])))
        with (out_dir / name).open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerows(rows)
        print(f"wrote {out_dir / name}: {len(rows)} rows")
    return 0


def output_name(model: str) -> str | None:
    if model == "gpt2-medium":
        return "gpt2m_streaming_bench.csv"
    if model == "llama32_3b":
        return "llama32_3b_streaming_bench.csv"
    return None


def convert_row(row: dict[str, str]) -> dict[str, str]:
    profile = row.get("mig_profile", "")
    model = MODEL_NAMES.get(row.get("model", ""), row.get("model", ""))
    return {
        "mig_uuid": row.get("mig_uuid", ""),
        "mig_name": PROFILE_NAMES.get(profile, ""),
        "mig_profile": profile,
        "node": row.get("node", ""),
        "gpu_index": row.get("gpu_index", ""),
        "model": model,
        "batch": row.get("batch", "1"),
        "prompt_len": row.get("prompt_len", ""),
        "output_tokens": row.get("output_tokens", ""),
        "dtype": "fp16",
        "engine": "hf",
        "status": row.get("status", ""),
        "error": row.get("error", ""),
        "sample_count": row.get("sample_count", ""),
        "ttft_ms": row.get("ttft_ms_mean", ""),
        "ttft_ms_p50": row.get("ttft_ms_p50", ""),
        "ttft_ms_p95": row.get("ttft_ms_p95", ""),
        "ttft_ms_p99": row.get("ttft_ms_p99", ""),
        "tpot_ms": row.get("tpot_ms_mean", ""),
        "tpot_ms_p50": row.get("tpot_ms_p50", ""),
        "tpot_ms_p95": row.get("tpot_ms_p95", ""),
        "tpot_ms_p99": row.get("tpot_ms_p99", ""),
        "decode_tps": row.get("decode_tps", ""),
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


def profile_order(profile: str) -> int:
    order = {"1g": 1, "2g": 2, "3g": 3, "4g": 4, "7g": 7}
    return order.get(profile, 99)


if __name__ == "__main__":
    raise SystemExit(main())
