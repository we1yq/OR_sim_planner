#!/usr/bin/env python3
"""Build the catalog from profile/newest_v3 (wall-clock completion rate).

The runtime is saturated while profiling (vision: two requests in flight, the
next one already waiting at the runtime; LLM: back to back), so the rate at
which requests complete is the runtime's actual capacity: compute time plus the
per-request overhead that runtimeLatencyMs does not cover.

Per GPU and option, the completion intervals of the measured samples (sorted by
completion time) are grouped into windows of WINDOW_VISION (vision) or 1 (LLM)
consecutive intervals; window rate = batch * intervals / their total time.
mu per option = min over the three GPUs of the median window rate, 3 decimals.

Writes:
  - planner-engine/app/mock/profile-catalogs/<workload>.yaml for all 7
    workloads (in place: mu and serviceTimeMs of measured options)
  - experiments/three-gpu-live-20260926/catalog_newest_v3.csv (rows and
    columns of catalog.csv)
  - profile/newest_v3/catalog_report.csv: per GPU and option, the
    runtimeLatencyMs-based rate (as in newest_v2), the wall-clock rate, their
    ratio, median inferQueueMs and the window rate spread
"""
from __future__ import annotations

import csv
import statistics
from collections import defaultdict
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
K8S = ROOT / "k8s-extension-go"
CATALOG_DIR = K8S / "planner-engine" / "app" / "mock" / "profile-catalogs"
LIVE = K8S / "experiments" / "three-gpu-live-20260926"
GPUS = ["rtx1-worker-gpu0", "ampere-gpu0", "ampere-gpu1"]
WORKLOADS = ["resnet50_image", "vgg16_image", "vit_base_image", "gpt2_p64_o64",
             "gpt2_p512_o512", "llama_p1024_o128", "llama_p2048_o64"]
LLM = {"gpt2_p64_o64", "gpt2_p512_o512", "llama_p1024_o128", "llama_p2048_o64"}
WINDOW_VISION = 10


def window_rates(batch: int, completed: list[float], window: int) -> list[float]:
    t = sorted(completed)
    gaps = [b - a for a, b in zip(t, t[1:])]
    return [batch * len(g) / sum(g) for g in (gaps[i:i + window] for i in range(0, len(gaps), window)) if sum(g) > 0]


def per_gpu_rates() -> tuple[dict, list[dict]]:
    samples: dict[tuple[str, str, str, int], list[dict]] = defaultdict(list)
    for gpu in GPUS:
        for row in csv.DictReader((HERE / gpu / "raw-samples.csv").open()):
            if row["phase"] == "sample":
                samples[(gpu, row["workload"], row["profile"], int(row["batch"]))].append(row)
    rates: dict[tuple[str, str, int], dict[str, float]] = defaultdict(dict)
    report = []
    for (gpu, workload, profile, batch), rows in sorted(samples.items()):
        windows = window_rates(batch, [float(r["completedAt"]) for r in rows], 1 if workload in LLM else WINDOW_VISION)
        wall = statistics.median(windows)
        compute = 1000.0 * batch / statistics.median(float(r["runtimeLatencyMs"]) for r in rows)
        queue = [float(r["inferQueueMs"]) for r in rows if r["inferQueueMs"] not in ("", None)]
        rates[(workload, profile, batch)][gpu] = wall
        report.append({"gpu": gpu, "workload": workload, "profile": profile, "batch": batch, "samples": len(rows),
                       "compute_mu": round(compute, 3), "wall_mu": round(wall, 3), "wall_over_compute": round(wall / compute, 4),
                       "infer_queue_ms_median": round(statistics.median(queue), 3) if queue else "",
                       "windows": len(windows), "window_min": round(min(windows), 3), "window_max": round(max(windows), 3),
                       "window_min_over_median": round(min(windows) / wall, 4)})
    return rates, report


def main() -> None:
    rates, report = per_gpu_rates()
    mu = {}
    for key, by_gpu in rates.items():
        if len(by_gpu) != len(GPUS):
            raise SystemExit(f"{key}: measured on {sorted(by_gpu)}, need all three GPUs")
        mu[key] = round(min(by_gpu.values()), 3)

    with (HERE / "catalog_report.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(report[0].keys()))
        writer.writeheader()
        writer.writerows(report)

    for workload in WORKLOADS:
        path = CATALOG_DIR / f"{workload}.yaml"
        doc = yaml.safe_load(path.read_text())
        for option in doc["options"]:
            key = (option["workload"], option["profile"], int(option["batch"]))
            if key in mu:
                option["mu"] = mu[key]
                option["serviceTimeMs"] = round(1000.0 * key[2] / mu[key], 6)
        doc["metadata"]["source"] = ("profile/newest_v3 (two-process runtime, C6 enabled on both hosts, vision with two "
                                     "requests in flight; per-GPU median wall-clock completion rate, min over 3 GPUs)")
        doc["metadata"]["generatedBy"] = "profile/newest_v3/build_catalog_newest_v3.py"
        path.write_text(yaml.safe_dump(doc, sort_keys=False))

    rows = list(csv.DictReader((LIVE / "catalog.csv").open()))
    missing = [r for r in rows if (r["workload"], r["profile"], int(r["batch"])) not in mu]
    if missing:
        raise SystemExit(f"catalog.csv options without measurement: {missing[:3]}")
    for r in rows:
        r["mu"] = f"{mu[(r['workload'], r['profile'], int(r['batch']))]:.3f}"
    out = LIVE / "catalog_newest_v3.csv"
    with out.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"updated {len(WORKLOADS)} yaml catalogs; wrote {out.relative_to(ROOT)} ({len(rows)} rows) and catalog_report.csv")


if __name__ == "__main__":
    main()
