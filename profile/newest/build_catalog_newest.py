#!/usr/bin/env python3
"""Build the catalog from profile/newest (all 84 options, worker-thread runtime).

mu per option = min over the three GPUs of (1000 * batch / median runtime
latency on that GPU), 3 decimals.  Samples come from this directory and from
unmeasured_options/ (options marked fit=false, measured so the yaml carries a
measured mu for them too; their fit flags are unchanged because every one
exceeds its SLO).  vgg16_b1_3g4g_repeat/ is a repeat check, not an input.

Writes:
  - planner-engine/app/mock/profile-catalogs/<workload>.yaml for all 7
    workloads (in place: mu and serviceTimeMs of measured options)
  - experiments/three-gpu-live-20260926/catalog_newest.csv (rows and columns
    of catalog.csv)
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


def measured_mu() -> dict[tuple[str, str, int], float]:
    lat: dict[tuple[str, str, str, int], list[float]] = defaultdict(list)
    for base in (HERE, HERE / "unmeasured_options"):
        for gpu in GPUS:
            for row in csv.DictReader((base / gpu / "raw-samples.csv").open()):
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
    for workload in WORKLOADS:
        path = CATALOG_DIR / f"{workload}.yaml"
        doc = yaml.safe_load(path.read_text())
        for option in doc["options"]:
            key = (option["workload"], option["profile"], int(option["batch"]))
            if key in mu:
                option["mu"] = mu[key]
                option["serviceTimeMs"] = round(1000.0 * key[2] / mu[key], 6)
        doc["metadata"]["source"] = "profile/newest (worker-thread runtime; per-GPU median latency, min over 3 GPUs)"
        doc["metadata"]["generatedBy"] = "profile/newest/build_catalog_newest.py"
        path.write_text(yaml.safe_dump(doc, sort_keys=False))

    rows = list(csv.DictReader((LIVE / "catalog.csv").open()))
    missing = [r for r in rows if (r["workload"], r["profile"], int(r["batch"])) not in mu]
    if missing:
        raise SystemExit(f"catalog.csv options without measurement: {missing[:3]}")
    for r in rows:
        r["mu"] = f"{mu[(r['workload'], r['profile'], int(r['batch']))]:.3f}"
    changed = len(rows)
    out = LIVE / "catalog_newest.csv"
    with out.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"updated {len(WORKLOADS)} yaml catalogs; wrote {out.relative_to(ROOT)} ({changed} rows)")


if __name__ == "__main__":
    main()
