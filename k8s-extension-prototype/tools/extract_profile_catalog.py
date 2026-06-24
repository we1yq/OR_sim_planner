from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any

import yaml


PROFILE_ORDER = ["1g", "2g", "3g", "4g", "7g"]
PROFILE_MEM_MB = {
    "1g": 5 * 1024,
    "2g": 10 * 1024,
    "3g": 20 * 1024,
    "4g": 20 * 1024,
    "7g": 40 * 1024,
}


CV_SPECS = [
    {"name": "resnet50", "csv": "cnn_bench.csv", "model": "resnet50"},
    {"name": "vgg16", "csv": "cnn_bench.csv", "model": "vgg16"},
    {"name": "vit_base", "csv": "vit_base_bench.csv", "model": "vit-base-patch16-224"},
]

LLM_SPECS = [
    {"prefix": "gpt2", "csv": "gpt2m_streaming_bench.csv", "model": "gpt2-medium"},
    {"prefix": "llama32_3b", "csv": "llama32_3b_streaming_bench.csv", "model": "Llama-3.2-3B"},
]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def to_float(row: dict[str, str], key: str, default: float = 0.0) -> float:
    value = row.get(key)
    if value in {None, ""}:
        return default
    return float(value)


def to_int(row: dict[str, str], key: str, default: int = 0) -> int:
    value = row.get(key)
    if value in {None, ""}:
        return default
    return int(float(value))


def safe_mu(batch: int, service_time_ms: float) -> float:
    return batch * 1000.0 / service_time_ms if service_time_ms > 0 else 0.0


def fit_memory(row: dict[str, str]) -> tuple[bool, float]:
    profile = row["mig_profile"]
    peak_mem_mb = to_float(row, "peak_reserved_mb", to_float(row, "peak_alloc_mb"))
    return peak_mem_mb <= PROFILE_MEM_MB[profile], peak_mem_mb


def row_ok(row: dict[str, str]) -> bool:
    return row.get("status", "ok") == "ok"


def build_option(workload: str, family: str, row: dict[str, str]) -> dict[str, Any]:
    batch = to_int(row, "batch", 1)
    service_ms = to_float(row, "time_ms_mean")
    fit_mem, peak_mem_mb = fit_memory(row)
    fit = row_ok(row) and fit_mem and service_ms > 0
    option: dict[str, Any] = {
        "workload": workload,
        "family": family,
        "batch": batch,
        "profile": row["mig_profile"],
        "mu": round(safe_mu(batch, service_ms), 6) if fit else 0.0,
        "fit": bool(fit),
        "fitMem": bool(fit_mem),
        "fitSlo": bool(row_ok(row)),
        "status": row.get("status", "ok"),
        "error": row.get("error", ""),
        "serviceTimeMs": round(service_ms, 6),
        "runtimeP50Ms": round(to_float(row, "time_ms_p50"), 6),
        "runtimeP95Ms": round(to_float(row, "time_ms_p95"), 6),
        "runtimeP99Ms": round(to_float(row, "time_ms_p99"), 6),
        "throughputRps": round(to_float(row, "throughput_rps"), 6),
        "peakMemMb": round(peak_mem_mb, 6),
        "sourceCsv": row.get("source_run_id", ""),
    }
    if family == "llm":
        option.update(
            {
                "promptLen": to_int(row, "prompt_len"),
                "outputTokens": to_int(row, "output_tokens"),
                "ttftMs": round(to_float(row, "ttft_ms"), 6),
                "tpotMs": round(to_float(row, "tpot_ms"), 6),
                "decodeTps": round(to_float(row, "decode_tps"), 6),
            }
        )
    return option


def best_rows(rows: list[dict[str, str]], keys: list[str]) -> dict[tuple[Any, ...], dict[str, str]]:
    out: dict[tuple[Any, ...], dict[str, str]] = {}
    for row in rows:
        key = tuple(row[k] for k in keys)
        prev = out.get(key)
        if prev is None:
            out[key] = row
            continue
        if row_ok(row) and not row_ok(prev):
            out[key] = row
            continue
        if row_ok(row) == row_ok(prev) and to_float(row, "time_ms_mean") < to_float(prev, "time_ms_mean"):
            out[key] = row
    return out


def build_cv_catalogs(profile_dir: Path) -> dict[str, dict[str, Any]]:
    catalogs: dict[str, dict[str, Any]] = {}
    for spec in CV_SPECS:
        rows = [r for r in read_csv(profile_dir / spec["csv"]) if r.get("model") == spec["model"]]
        selected = best_rows(rows, ["batch", "mig_profile"])
        options = [build_option(spec["name"], "cv", r) for r in selected.values()]
        catalogs[spec["name"]] = catalog(spec["name"], "cv", spec, options)
    return catalogs


def build_llm_catalogs(profile_dir: Path) -> dict[str, dict[str, Any]]:
    catalogs: dict[str, dict[str, Any]] = {}
    for spec in LLM_SPECS:
        rows = [r for r in read_csv(profile_dir / spec["csv"]) if r.get("model") == spec["model"]]
        shapes = sorted({(to_int(r, "prompt_len"), to_int(r, "output_tokens")) for r in rows})
        for prompt_len, output_tokens in shapes:
            workload = f"{spec['prefix']}_p{prompt_len}_o{output_tokens}"
            shape_rows = [
                r
                for r in rows
                if to_int(r, "prompt_len") == prompt_len and to_int(r, "output_tokens") == output_tokens
            ]
            selected = best_rows(shape_rows, ["batch", "mig_profile"])
            options = [build_option(workload, "llm", r) for r in selected.values()]
            catalogs[workload] = catalog(
                workload,
                "llm",
                {**spec, "promptLen": prompt_len, "outputTokens": output_tokens},
                options,
            )
    return catalogs


def catalog(workload: str, family: str, spec: dict[str, Any], options: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "metadata": {
            "source": f"profile/current/{spec['csv']}",
            "generatedBy": "k8s-extension-prototype/tools/extract_profile_catalog.py",
            "profileOrder": PROFILE_ORDER,
            "workload": workload,
            "family": family,
            "model": spec["model"],
            **({"promptLen": spec["promptLen"], "outputTokens": spec["outputTokens"]} if family == "llm" else {}),
        },
        "options": sorted(options, key=lambda x: (x["batch"], PROFILE_ORDER.index(x["profile"]))),
    }


def build_catalogs(profile_dir: Path) -> dict[str, dict[str, Any]]:
    catalogs = build_cv_catalogs(profile_dir)
    catalogs.update(build_llm_catalogs(profile_dir))
    return catalogs


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description="Extract normalized ProfileCatalog YAMLs from profile CSVs.")
    parser.add_argument("--profile-dir", type=Path, default=repo_root / "profile" / "current")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=repo_root / "k8s-extension-go" / "planner-engine" / "app" / "mock" / "profile-catalogs",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    catalogs = build_catalogs(args.profile_dir)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    total = 0
    for workload, data in sorted(catalogs.items()):
        output = args.output_dir / f"{workload}.yaml"
        with output.open("w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, sort_keys=False)
        total += len(data["options"])
        print(f"Wrote {len(data['options'])} options to {output}")
    print(f"Wrote {total} total options to {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
