#!/usr/bin/env python3
"""Build the worker-runtime catalog.

Vision options: mu = min over the three GPUs of (1000 * batch / median
runtime latency on that GPU) from this directory (worker-thread runtime),
3 decimals.  LLM options keep their mu from catalog_20260929_median_min.csv
(profile/20260929; the per-request thread cost is negligible next to their
seconds-long generate()).

Writes:
  - planner-engine/app/mock/profile-catalogs/{resnet50,vgg16,vit_base}_image.yaml
    (in place: mu and serviceTimeMs of measured options)
  - experiments/three-gpu-live-20260926/catalog_20260929_worker.csv
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
VISION = ["resnet50_image", "vgg16_image", "vit_base_image"]


def measured_mu() -> dict[tuple[str, str, int], float]:
    lat: dict[tuple[str, str, str, int], list[float]] = defaultdict(list)
    for gpu in GPUS:
        for row in csv.DictReader((HERE / gpu / "raw-samples.csv").open()):
            if row["phase"] == "sample":
                lat[(gpu, row["workload"], row["profile"], int(row["batch"]))].append(float(row["runtimeLatencyMs"]))
    per_option: dict[tuple[str, str, int], dict[str, float]] = defaultdict(dict)
    for (gpu, workload, profile, batch), values in lat.items():
        per_option[(workload, profile, batch)][gpu] = 1000.0 * batch / statistics.median(values)
    out = {}
    for key, by_gpu in per_option.items():
        if len(by_gpu) != len(GPUS):
            raise SystemExit(f"{key}: measured on {sorted(by_gpu)}, need all three GPUs")
        out[key] = round(min(by_gpu.values()), 3)
    return out


def main() -> None:
    mu = measured_mu()
    for workload in VISION:
        path = CATALOG_DIR / f"{workload}.yaml"
        doc = yaml.safe_load(path.read_text())
        for option in doc["options"]:
            key = (option["workload"], option["profile"], int(option["batch"]))
            if key in mu:
                option["mu"] = mu[key]
                option["serviceTimeMs"] = round(1000.0 * key[2] / mu[key], 6)
        doc["metadata"]["source"] = "profile/20260929-worker (worker-thread runtime; per-GPU median latency, min over 3 GPUs)"
        doc["metadata"]["generatedBy"] = "profile/20260929-worker/build_catalog_worker.py"
        path.write_text(yaml.safe_dump(doc, sort_keys=False))

    rows = list(csv.DictReader((LIVE / "catalog_20260929_median_min.csv").open()))
    changed = 0
    for r in rows:
        key = (r["workload"], r["profile"], int(r["batch"]))
        if r["workload"] in VISION:
            if key not in mu:
                raise SystemExit(f"vision option without measurement: {key}")
            r["mu"] = f"{mu[key]:.3f}"
            changed += 1
    out = LIVE / "catalog_20260929_worker.csv"
    with out.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"updated {len(VISION)} vision yaml catalogs; wrote {out.relative_to(ROOT)} ({len(rows)} rows, {changed} vision mu replaced)")


if __name__ == "__main__":
    main()
