#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import statistics
import time
import urllib.request
from pathlib import Path
from typing import Any

import yaml


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--router-url", default="http://runtime-router:8080")
    parser.add_argument("--model", required=True)
    parser.add_argument("--requests", type=int, default=50)
    parser.add_argument("--catalog", default="")
    parser.add_argument("--out", default="")
    args = parser.parse_args()

    router = args.router_url.rstrip("/")
    latencies = []
    for _ in range(args.requests):
        started = time.perf_counter()
        post_json(f"{router}/infer/{args.model}", {"benchmark": True, "sentAt": time.time()})
        latencies.append((time.perf_counter() - started) * 1000.0)

    observations = get_json(f"{router}/metrics/profile-observations").get("observations", [])
    model_observations = [row for row in observations if row.get("model") == args.model]
    routes = get_json(f"{router}/routes").get("routes", [])
    model_routes = [row for row in routes if row.get("model") == args.model]
    catalog_rows = read_catalog(args.catalog, args.model) if args.catalog else []
    report = render_report(args.model, latencies, model_observations, model_routes, catalog_rows)

    if args.out:
        path = Path(args.out)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(report, encoding="utf-8")
    print(report)


def post_json(url: str, payload: dict[str, Any]) -> dict[str, Any]:
    raw = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=raw, headers={"content-type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def get_json(url: str) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.loads(resp.read().decode())


def read_catalog(path: str, model: str) -> list[dict[str, Any]]:
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    rows = []
    for row in data.get("options", []):
        if row.get("workload") == model:
            rows.append(row)
    return rows


def render_report(
    model: str,
    client_latencies: list[float],
    observations: list[dict[str, Any]],
    routes: list[dict[str, Any]],
    catalog_rows: list[dict[str, Any]],
) -> str:
    p50 = statistics.median(client_latencies) if client_latencies else 0.0
    avg = statistics.mean(client_latencies) if client_latencies else 0.0
    p95 = percentile(client_latencies, 0.95)
    lines = [
        f"# Runtime Profile Benchmark: {model}",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| client requests | {len(client_latencies)} |",
        f"| client avg latency ms | {avg:.3f} |",
        f"| client p50 latency ms | {p50:.3f} |",
        f"| client p95 latency ms | {p95:.3f} |",
        "",
        "## Runtime Observations",
        "",
        "| Runtime | Profile | Batch | Runtime ms | Runtime throughput | Router avg ms | Network overhead ms | Samples |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in observations:
        lines.append(
            "| {runtime} | {profile} | {batch} | {rt_ms:.3f} | {rt_tp:.3f} | {router_ms:.3f} | {net_ms:.3f} | {samples} |".format(
                runtime=value(row, "runtimeId") or value(row, "runtime.runtimeId"),
                profile=value(row, "profile"),
                batch=int_value(row, "runtime.batchSize") or int_value(row, "batchSize"),
                rt_ms=float_value(row, "runtime.runtimeLatencyMs"),
                rt_tp=float_value(row, "runtime.runtimeThroughput"),
                router_ms=float_value(row, "avgLatencyMs"),
                net_ms=float_value(row, "networkOverheadMs"),
                samples=int_value(row, "sampleCount") or int_value(row, "runtime.requests"),
            )
        )
    if not observations:
        lines.append("| none | | 0 | 0 | 0 | 0 | 0 | 0 |")

    lines.extend(
        [
            "",
            "## Active Routes",
            "",
            "| Runtime | GPU | Profile | Batch | Weight | Runtime ms | Endpoint ms | Network overhead ms |",
            "|---|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in routes:
        lines.append(
            "| {runtime} | {gpu} | {profile} | {batch} | {weight:.3f} | {rt_ms:.3f} | {endpoint_ms:.3f} | {net_ms:.3f} |".format(
                runtime=value(row, "runtimeId"),
                gpu=value(row, "gpu"),
                profile=value(row, "profile"),
                batch=int_value(row, "batchSize"),
                weight=float_value(row, "weight"),
                rt_ms=float_value(row, "runtime.runtimeLatencyMs"),
                endpoint_ms=float_value(row, "endpointAvgLatencyMs"),
                net_ms=float_value(row, "networkOverheadMs"),
            )
        )

    if catalog_rows:
        lines.extend(
            [
                "",
                "## Profile Catalog",
                "",
                "| Profile | Batch | Catalog e2e ms | Catalog mu | Fit SLO |",
                "|---|---:|---:|---:|---|",
            ]
        )
        for row in catalog_rows:
            lines.append(
                f"| {row.get('profile', '')} | {row.get('batch', '')} | {float(row.get('e2eMs', 0.0)):.3f} | {float(row.get('mu', 0.0)):.3f} | {row.get('fitSlo', '')} |"
            )
    return "\n".join(lines) + "\n"


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * q))))
    return ordered[idx]


def value(row: dict[str, Any], key: str) -> str:
    raw = row.get(key, "")
    return str(raw) if raw is not None else ""


def float_value(row: dict[str, Any], key: str) -> float:
    try:
        return float(row.get(key, 0.0) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def int_value(row: dict[str, Any], key: str) -> int:
    try:
        return int(float(row.get(key, 0) or 0))
    except (TypeError, ValueError):
        return 0


if __name__ == "__main__":
    main()
