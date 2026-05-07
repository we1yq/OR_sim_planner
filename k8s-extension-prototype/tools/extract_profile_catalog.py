from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any

import yaml


PROFILE_ORDER = ["7g", "4g", "3g", "2g", "1g"]
PROFILE_MEM_MB = {
    "1g": 5 * 1024,
    "2g": 10 * 1024,
    "3g": 20 * 1024,
    "4g": 20 * 1024,
    "7g": 40 * 1024,
}

WORKLOAD_SPECS = [
    {
        "name": "llama",
        "family": "llm",
        "csv": "llama32_3b_streaming_bench.csv",
        "model_match": "Llama-3.2-3B-Instruct",
        "prompt_len": 1024,
        "output_tokens": 64,
        "ttft_slo_ms": 100.0,
        "tpot_slo_ms": 25.0,
    },
    {
        "name": "gpt2",
        "family": "llm",
        "csv": "gpt2m_streaming_bench.csv",
        "model_match": "gpt2-medium",
        "prompt_len": 64,
        "output_tokens": 64,
        "ttft_slo_ms": 20.0,
        "tpot_slo_ms": 15.0,
    },
    {
        "name": "vgg16",
        "family": "cv",
        "csv": "cnn_bench.csv",
        "model_match": "vgg16",
        "e2e_slo_ms": 50.0,
    },
    {
        "name": "resnet50",
        "family": "cv",
        "csv": "cnn_bench.csv",
        "model_match": "resnet50",
        "e2e_slo_ms": 50.0,
    },
    {
        "name": "vit_base",
        "family": "cv",
        "csv": "vit_base_bench.csv",
        "model_match": "vit-base-patch16-224",
        "e2e_slo_ms": 50.0,
    },
]


def normalize_mig_name(raw: str | None) -> str | None:
    if not raw:
        return None
    value = raw.lower().replace(" ", "")
    for profile in ["1g", "2g", "3g", "4g", "7g"]:
        if f"{profile}." in value or profile in value:
            return profile
    return None


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def to_float(row: dict[str, str], key: str, default: float = 0.0) -> float:
    value = row.get(key)
    if value in {None, ""}:
        return default
    return float(value)


def safe_mu(batch: int, service_time_ms: float) -> float:
    if service_time_ms <= 0:
        return 0.0
    return batch * 1000.0 / service_time_ms


def extract_cv_options(spec: dict[str, Any], rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    best: dict[tuple[int, str], dict[str, str]] = {}
    for row in rows:
        if row.get("model") != spec["model_match"]:
            continue
        profile = normalize_mig_name(row.get("mig_name"))
        if profile not in PROFILE_ORDER:
            continue
        batch = int(float(row["batch"]))
        key = (batch, profile)
        prev = best.get(key)
        if prev is None or to_float(row, "time_ms_mean") < to_float(prev, "time_ms_mean"):
            best[key] = row

    options = []
    for (batch, profile), row in sorted(best.items()):
        e2e_ms = to_float(row, "time_ms_mean")
        peak_mem_mb = to_float(row, "peak_reserved_mb", to_float(row, "peak_alloc_mb"))
        fit_mem = peak_mem_mb <= PROFILE_MEM_MB[profile]
        fit_slo = e2e_ms <= spec["e2e_slo_ms"]
        fit = bool(fit_mem and fit_slo)
        options.append(
            {
                "workload": spec["name"],
                "family": "cv",
                "batch": batch,
                "profile": profile,
                "mu": round(safe_mu(batch, e2e_ms), 6) if fit else 0.0,
                "fit": fit,
                "e2eMs": round(e2e_ms, 6),
                "peakMemMb": round(peak_mem_mb, 6),
                "fitMem": fit_mem,
                "fitSlo": fit_slo,
                "sourceCsv": spec["csv"],
            }
        )
    return options


def extract_llm_options(spec: dict[str, Any], rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    best: dict[tuple[int, str], dict[str, str]] = {}
    for row in rows:
        if row.get("model") != spec["model_match"]:
            continue
        if int(float(row.get("prompt_len", 0))) != int(spec["prompt_len"]):
            continue
        if int(float(row.get("output_tokens", 0))) != int(spec["output_tokens"]):
            continue
        profile = normalize_mig_name(row.get("mig_name"))
        if profile not in PROFILE_ORDER:
            continue
        batch = int(float(row["batch"]))
        key = (batch, profile)
        prev = best.get(key)
        if prev is None or to_float(row, "time_ms_mean") < to_float(prev, "time_ms_mean"):
            best[key] = row

    options = []
    for (batch, profile), row in sorted(best.items()):
        ttft_ms = to_float(row, "ttft_ms")
        decode_tps = to_float(row, "decode_tps")
        tpot_ms = 1000.0 / decode_tps if decode_tps > 0 else 0.0
        service_time_ms = ttft_ms + int(spec["output_tokens"]) * tpot_ms
        peak_mem_mb = to_float(row, "peak_reserved_mb", to_float(row, "peak_alloc_mb"))
        fit_mem = peak_mem_mb <= PROFILE_MEM_MB[profile]
        fit_slo = ttft_ms <= spec["ttft_slo_ms"] and tpot_ms <= spec["tpot_slo_ms"]
        fit = bool(fit_mem and fit_slo)
        options.append(
            {
                "workload": spec["name"],
                "family": "llm",
                "batch": batch,
                "profile": profile,
                "mu": round(safe_mu(batch, service_time_ms), 6) if fit else 0.0,
                "fit": fit,
                "ttftMs": round(ttft_ms, 6),
                "tpotMs": round(tpot_ms, 6),
                "serviceTimeMs": round(service_time_ms, 6),
                "peakMemMb": round(peak_mem_mb, 6),
                "fitMem": fit_mem,
                "fitSlo": fit_slo,
                "sourceCsv": spec["csv"],
            }
        )
    return options


def build_catalogs(profile_dir: Path) -> dict[str, dict[str, Any]]:
    catalogs = {}
    for spec in WORKLOAD_SPECS:
        rows = read_csv(profile_dir / spec["csv"])
        if spec["family"] == "llm":
            options = extract_llm_options(spec, rows)
        else:
            options = extract_cv_options(spec, rows)

        catalogs[spec["name"]] = {
            "metadata": {
                "source": f"profile/{spec['csv']}",
                "generatedBy": "k8s-extension-prototype/tools/extract_profile_catalog.py",
                "profileOrder": PROFILE_ORDER,
                "workload": spec["name"],
                "modelMatch": spec["model_match"],
            },
            "options": sorted(options, key=lambda x: (x["batch"], x["profile"])),
        }
    return catalogs


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description="Extract normalized ProfileCatalog from profile CSVs.")
    parser.add_argument("--profile-dir", type=Path, default=repo_root / "profile")
    parser.add_argument("--output-dir", type=Path, default=repo_root / "k8s-extension-prototype/mock/profile-catalogs")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    catalogs = build_catalogs(args.profile_dir)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    total = 0
    for workload, catalog in sorted(catalogs.items()):
        output = args.output_dir / f"{workload}.yaml"
        with output.open("w", encoding="utf-8") as f:
            yaml.safe_dump(catalog, f, sort_keys=False)
        total += len(catalog["options"])
        print(f"Wrote {len(catalog['options'])} options to {output}")
    print(f"Wrote {total} total options to {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
