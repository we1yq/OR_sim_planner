#!/usr/bin/env python3
from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[2]
APP = ROOT / "k8s-extension-go/planner-engine/app"
PROFILE = ROOT / "profile/current"
WORKLOAD_DIR = APP / "manifests/examples/workloadrequests"
CATALOG_DIR = APP / "mock/profile-catalogs"
SCENARIO_DIR = APP / "mock/scenarios"


WORKLOADS = [
    {
        "name": "resnet50_image",
        "runtimeModel": "resnet50",
        "family": "cv",
        "requestClass": "image batches",
        "slo": {"e2eMs": 100.0, "latencyMs": 100.0},
        "source": "cnn_bench.csv",
        "csvModel": "resnet50",
    },
    {
        "name": "vgg16_image",
        "runtimeModel": "vgg16",
        "family": "cv",
        "requestClass": "image batches",
        "slo": {"e2eMs": 100.0, "latencyMs": 100.0},
        "source": "cnn_bench.csv",
        "csvModel": "vgg16",
    },
    {
        "name": "vit_base_image",
        "runtimeModel": "vit_base",
        "family": "cv",
        "requestClass": "image batches",
        "slo": {"e2eMs": 300.0, "latencyMs": 300.0},
        "source": "vit_base_bench.csv",
        "csvModel": "vit-base-patch16-224",
    },
    {
        "name": "gpt2_p64_o64",
        "runtimeModel": "gpt2",
        "family": "llm",
        "requestClass": "p64/o64",
        "requestShape": {"promptLen": 64, "outputTokens": 64},
        "slo": {"ttftMs": 50.0, "tpotMs": 20.0},
        "source": "gpt2m_streaming_bench.csv",
        "csvModel": "gpt2-medium",
    },
    {
        "name": "gpt2_p512_o512",
        "runtimeModel": "gpt2",
        "family": "llm",
        "requestClass": "p512/o512",
        "requestShape": {"promptLen": 512, "outputTokens": 512},
        "slo": {"ttftMs": 100.0, "tpotMs": 20.0},
        "source": "gpt2m_streaming_bench.csv",
        "csvModel": "gpt2-medium",
    },
    {
        "name": "llama_p1024_o128",
        "runtimeModel": "llama",
        "family": "llm",
        "requestClass": "p1024/o128",
        "requestShape": {"promptLen": 1024, "outputTokens": 128},
        "slo": {"ttftMs": 180.0, "tpotMs": 35.0},
        "source": "llama32_3b_streaming_bench.csv",
        "csvModel": "Llama-3.2-3B",
    },
    {
        "name": "llama_p2048_o64",
        "runtimeModel": "llama",
        "family": "llm",
        "requestClass": "p2048/o64",
        "requestShape": {"promptLen": 2048, "outputTokens": 64},
        "slo": {"ttftMs": 250.0, "tpotMs": 35.0},
        "source": "llama32_3b_streaming_bench.csv",
        "csvModel": "Llama-3.2-3B",
    },
]


def main() -> int:
    WORKLOAD_DIR.mkdir(parents=True, exist_ok=True)
    CATALOG_DIR.mkdir(parents=True, exist_ok=True)
    SCENARIO_DIR.mkdir(parents=True, exist_ok=True)
    for cfg in WORKLOADS:
        write_yaml(WORKLOAD_DIR / f"{cfg['name']}.yaml", workload_request(cfg))
        catalog = profile_catalog(cfg)
        if not catalog["options"]:
            raise RuntimeError(f"no profile options generated for {cfg['name']}")
        write_yaml(CATALOG_DIR / f"{cfg['name']}.yaml", catalog)
    write_yaml(SCENARIO_DIR / "real8gpu.yaml", scenario())
    print("generated 8GPU workload requests, profile catalogs, and scenario")
    return 0


def workload_request(cfg: dict[str, Any]) -> dict[str, Any]:
    batches = sorted({int(row["batch"]) for row in source_rows(cfg) if int_or_zero(row.get("batch")) > 0})
    if cfg["family"] == "llm":
        batches = [1]
    spec = {
        "model": cfg["runtimeModel"],
        "modelKey": cfg["runtimeModel"],
        "placementGroup": cfg["runtimeModel"],
        "family": cfg["family"],
        "arrivalRate": 0,
        "requestClass": cfg["requestClass"],
        "allowedBatches": batches,
        "priority": "normal",
        "slo": cfg["slo"],
    }
    if cfg.get("requestShape"):
        spec["requestShape"] = cfg["requestShape"]
    return {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "WorkloadRequest",
        "metadata": {"name": cfg["name"], "namespace": "or-sim"},
        "spec": spec,
    }


def profile_catalog(cfg: dict[str, Any]) -> dict[str, Any]:
    best: dict[tuple[str, int], dict[str, Any]] = {}
    for row in source_rows(cfg):
        if str(row.get("status")) != "ok":
            continue
        profile = str(row.get("mig_profile") or "")
        batch = int_or_zero(row.get("batch"))
        if not profile or batch <= 0:
            continue
        if cfg["family"] == "llm":
            shape = cfg["requestShape"]
            if int_or_zero(row.get("prompt_len")) != int(shape["promptLen"]):
                continue
            if int_or_zero(row.get("output_tokens")) != int(shape["outputTokens"]):
                continue
        option = option_from_row(cfg, row)
        key = (profile, batch)
        current = best.get(key)
        if current is None or float(option["mu"]) > float(current["mu"]):
            best[key] = option
    return {
        "metadata": {
            "source": f"profile/current/{cfg['source']}",
            "generatedBy": "eval/8gpu/build_8gpu_planner_fixture.py",
            "workload": cfg["name"],
            "modelMatch": cfg["runtimeModel"],
            "runtimeModel": cfg["runtimeModel"],
            "requestClass": cfg["requestClass"],
            "requestShape": cfg.get("requestShape", {}),
            "slo": cfg["slo"],
            "profileOrder": ["7g", "4g", "3g", "2g", "1g"],
        },
        "options": sorted(best.values(), key=lambda x: (profile_rank(str(x["profile"])), int(x["batch"]))),
    }


def source_rows(cfg: dict[str, Any]) -> list[dict[str, str]]:
    path = PROFILE / cfg["source"]
    with path.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    return [row for row in rows if str(row.get("model")) == str(cfg["csvModel"])]


def option_from_row(cfg: dict[str, Any], row: dict[str, str]) -> dict[str, Any]:
    family = cfg["family"]
    slo = cfg["slo"]
    if family == "llm":
        ttft = float_or_zero(row.get("ttft_ms_p95") or row.get("ttft_ms"))
        tpot = float_or_zero(row.get("tpot_ms_p95") or row.get("tpot_ms"))
        fit_slo = ttft <= float(slo["ttftMs"]) and tpot <= float(slo["tpotMs"])
        metrics = {
            "ttftMs": ttft,
            "ttftMsP95": ttft,
            "tpotMs": tpot,
            "tpotMsP95": tpot,
            "serviceTimeMs": float_or_zero(row.get("time_ms_mean")),
            "timeMsP95": float_or_zero(row.get("time_ms_p95")),
            "peakMemMb": float_or_zero(row.get("peak_alloc_mb")),
            "fitSlo": fit_slo,
        }
    else:
        latency = float_or_zero(row.get("time_ms_p95") or row.get("time_ms_mean"))
        fit_slo = latency <= float(slo["latencyMs"])
        metrics = {
            "latencyMs": latency,
            "latencyMsP95": latency,
            "e2eMs": latency,
            "e2eMsP95": latency,
            "serviceTimeMs": float_or_zero(row.get("time_ms_mean")),
            "timeMsP95": latency,
            "peakMemMb": float_or_zero(row.get("peak_alloc_mb")),
            "fitSlo": fit_slo,
        }
    return {
        "workload": cfg["name"],
        "modelMatch": cfg["runtimeModel"],
        "placementGroup": cfg["runtimeModel"],
        "family": family,
        "batch": int_or_zero(row.get("batch")),
        "profile": str(row.get("mig_profile")),
        "mu": float_or_zero(row.get("throughput_rps")),
        "fit": bool(fit_slo),
        "fitMem": True,
        "sourceCsv": cfg["source"],
        **metrics,
    }


def scenario() -> dict[str, Any]:
    names = [str(cfg["name"]) for cfg in WORKLOADS]
    return {
        "name": "real8gpu",
        "description": "8GPU 24h trace fixture with class-level LLM request workloads.",
        "policyRef": "../policies/migrant-default.yaml",
        "migRulesRef": "../mig-rules/a100-40gb.yaml",
        "sourceStateRef": "../gpu-states/migrant-empty-9-a100.yaml",
        "targetStateRef": "target0",
        "workloadOrder": names,
        "workloadRefs": {name: f"../../manifests/examples/workloadrequests/{name}.yaml" for name in names},
        "profileCatalogRefs": {name: f"../profile-catalogs/{name}.yaml" for name in names},
        "sourceArrival": {name: 0 for name in names},
        "targetArrival": {name: 0 for name in names},
        "transition": {
            "from": "source0",
            "to": "target0",
            "kind": "continuous_trace",
            "notes": ["ArrivalSnapshot overrides sourceArrival and targetArrival each epoch."],
        },
    }


def profile_rank(profile: str) -> int:
    order = {"1g": 1, "2g": 2, "3g": 3, "4g": 4, "7g": 7}
    return order.get(profile, 99)


def int_or_zero(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def float_or_zero(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def write_yaml(path: Path, data: dict[str, Any]) -> None:
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
