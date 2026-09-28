#!/usr/bin/env python3
"""Build the live-experiment catalog from this directory's measurements.

mu per option = min over the three GPUs of (1000 * batch / median runtime
latency on that GPU), 3 decimals.  Writes:
  - planner-engine/app/mock/profile-catalogs/<workload>.yaml (in place:
    mu and serviceTimeMs of measured options; fit flags and option set kept)
  - experiments/three-gpu-live-20260926/catalog_20260929_median_min.csv
    (same rows/columns as catalog.csv, new mu)
Options without a measurement here keep their old values and are listed.
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
RAW_DIRS = [HERE, HERE / "llama_p2048_o64_2g"]  # main run + llama p2048/o64 2g
WORKLOADS = ["resnet50_image", "vgg16_image", "vit_base_image", "gpt2_p64_o64",
             "gpt2_p512_o512", "llama_p1024_o128", "llama_p2048_o64"]


def measured_mu() -> dict[tuple[str, str, int], tuple[float, str]]:
    lat: dict[tuple[str, str, str, int], list[float]] = defaultdict(list)
    for base in RAW_DIRS:
        for gpu in GPUS:
            path = base / gpu / "raw-samples.csv"
            if not path.exists():
                continue
            for row in csv.DictReader(path.open()):
                if row["phase"] == "sample":
                    lat[(gpu, row["workload"], row["profile"], int(row["batch"]))].append(float(row["runtimeLatencyMs"]))
    per_option: dict[tuple[str, str, int], dict[str, float]] = defaultdict(dict)
    for (gpu, workload, profile, batch), values in lat.items():
        per_option[(workload, profile, batch)][gpu] = 1000.0 * batch / statistics.median(values)
    out = {}
    for key, by_gpu in per_option.items():
        if len(by_gpu) != len(GPUS):
            raise SystemExit(f"{key}: measured on {sorted(by_gpu)}, need all three GPUs")
        gpu = min(by_gpu, key=by_gpu.get)
        out[key] = (round(by_gpu[gpu], 3), gpu)
    return out


def main() -> None:
    mu = measured_mu()
    kept = []
    for workload in WORKLOADS:
        path = CATALOG_DIR / f"{workload}.yaml"
        doc = yaml.safe_load(path.read_text())
        for option in doc["options"]:
            key = (option["workload"], option["profile"], int(option["batch"]))
            if key not in mu:
                kept.append((key, option.get("fit")))
                continue
            value, _ = mu[key]
            option["mu"] = value
            option["serviceTimeMs"] = round(1000.0 * key[2] / value, 6)
        doc["metadata"]["source"] = "profile/20260929 (per-GPU median latency, min over 3 GPUs)"
        doc["metadata"]["generatedBy"] = "profile/20260929/build_catalog_median_min.py"
        path.write_text(yaml.safe_dump(doc, sort_keys=False))

    rows = list(csv.DictReader((LIVE / "catalog.csv").open()))
    missing = [r for r in rows if (r["workload"], r["profile"], int(r["batch"])) not in mu]
    if missing:
        raise SystemExit(f"catalog.csv options without measurement: {missing[:3]}")
    for r in rows:
        r["mu"] = f"{mu[(r['workload'], r['profile'], int(r['batch']))][0]:.3f}"
    out = LIVE / "catalog_20260929_median_min.csv"
    with out.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"updated {len(WORKLOADS)} yaml catalogs; wrote {out.relative_to(ROOT)} ({len(rows)} rows)")
    for key, fit in kept:
        print(f"  kept old mu (not measured): {key} fit={fit}")


if __name__ == "__main__":
    main()
