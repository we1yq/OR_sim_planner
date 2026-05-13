from __future__ import annotations

import argparse
from datetime import datetime, timezone
import math
import random
from typing import Any

import yaml


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate MIGRANT ArrivalSnapshot CRs for traffic experiments."
    )
    parser.add_argument("--namespace", default="or-sim")
    parser.add_argument("--name", default="arrival-snapshot")
    parser.add_argument("--mode", choices=["static", "poisson", "stress", "traceReplay"], default="static")
    parser.add_argument("--window-seconds", type=int, default=30)
    parser.add_argument("--epoch", default="0")
    parser.add_argument("--source", default=None)
    parser.add_argument(
        "--workload-rate",
        action="append",
        default=[],
        metavar="WORKLOAD=RATE",
        help="Workload arrival rate in requests/s. May be repeated.",
    )
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--burst-multiplier", type=float, default=3.0)
    parser.add_argument("--trace-window-yaml", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rates = _parse_workload_rates(args.workload_rate)
    if args.mode == "traceReplay":
        if not args.trace_window_yaml:
            raise SystemExit("--trace-window-yaml is required for mode=traceReplay")
        snapshot = _snapshot_from_trace_window(
            path=args.trace_window_yaml,
            name=args.name,
            namespace=args.namespace,
            epoch=args.epoch,
            window_seconds=args.window_seconds,
            source=args.source or "trace-replay",
        )
    else:
        if not rates:
            raise SystemExit("At least one --workload-rate WORKLOAD=RATE is required")
        snapshot = _synthetic_snapshot(
            name=args.name,
            namespace=args.namespace,
            mode=args.mode,
            rates=rates,
            window_seconds=args.window_seconds,
            epoch=args.epoch,
            source=args.source or f"{args.mode}-generator",
            seed=args.seed,
            burst_multiplier=args.burst_multiplier,
        )
    print(yaml.safe_dump(snapshot, sort_keys=False), end="")
    return 0


def _synthetic_snapshot(
    name: str,
    namespace: str,
    mode: str,
    rates: dict[str, float],
    window_seconds: int,
    epoch: str,
    source: str,
    seed: int,
    burst_multiplier: float,
) -> dict[str, Any]:
    rng = random.Random(seed + int(float(epoch)))
    target_arrival = dict(rates)
    if mode == "stress":
        target_arrival = {
            workload: float(rate) * float(burst_multiplier)
            for workload, rate in rates.items()
        }
    request_count = {}
    if mode == "poisson":
        request_count = {
            workload: _poisson(rng, max(0.0, rate * window_seconds))
            for workload, rate in target_arrival.items()
        }
        target_arrival = {
            workload: count / float(window_seconds)
            for workload, count in request_count.items()
        }
    else:
        request_count = {
            workload: int(round(max(0.0, rate * window_seconds)))
            for workload, rate in target_arrival.items()
        }
    return _arrival_snapshot_manifest(
        name=name,
        namespace=namespace,
        mode=mode,
        source=source,
        epoch=epoch,
        window_seconds=window_seconds,
        target_arrival=target_arrival,
        request_count=request_count,
    )


def _snapshot_from_trace_window(
    path: str,
    name: str,
    namespace: str,
    epoch: str,
    window_seconds: int,
    source: str,
) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        obj = yaml.safe_load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f"{path} must contain a YAML object")
    counts = dict(obj.get("requestCount", {}))
    rates = dict(obj.get("targetArrival", {}))
    if not rates and counts:
        rates = {
            workload: float(count) / float(window_seconds)
            for workload, count in counts.items()
        }
    if not counts:
        counts = {
            workload: int(round(float(rate) * window_seconds))
            for workload, rate in rates.items()
        }
    return _arrival_snapshot_manifest(
        name=name,
        namespace=namespace,
        mode="traceReplay",
        source=source,
        epoch=epoch,
        window_seconds=int(obj.get("windowSeconds", window_seconds)),
        target_arrival={workload: float(rate) for workload, rate in rates.items()},
        request_count={workload: int(count) for workload, count in counts.items()},
        observed_at=obj.get("observedAt"),
    )


def _arrival_snapshot_manifest(
    name: str,
    namespace: str,
    mode: str,
    source: str,
    epoch: str,
    window_seconds: int,
    target_arrival: dict[str, float],
    request_count: dict[str, int],
    observed_at: str | None = None,
) -> dict[str, Any]:
    return {
        "apiVersion": "mig.or-sim.io/v1alpha1",
        "kind": "ArrivalSnapshot",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "or-sim-mig-planner",
                "mig.or-sim.io/input-kind": "arrival-snapshot",
                "mig.or-sim.io/traffic-mode": mode,
            },
        },
        "spec": {
            "source": source,
            "mode": mode,
            "epoch": str(epoch),
            "windowSeconds": int(window_seconds),
            "unit": "requests_per_second",
            "observedAt": observed_at or datetime.now(timezone.utc).isoformat(),
            "targetArrival": {
                workload: float(rate)
                for workload, rate in sorted(target_arrival.items())
            },
            "requestCount": {
                workload: int(count)
                for workload, count in sorted(request_count.items())
            },
        },
    }


def _parse_workload_rates(values: list[str]) -> dict[str, float]:
    rates = {}
    for value in values:
        if "=" not in value:
            raise ValueError(f"Invalid --workload-rate {value!r}; expected WORKLOAD=RATE")
        workload, raw_rate = value.split("=", 1)
        workload = workload.strip()
        if not workload:
            raise ValueError(f"Invalid --workload-rate {value!r}; workload is empty")
        rates[workload] = float(raw_rate)
    return rates


def _poisson(rng: random.Random, lam: float) -> int:
    if lam <= 0:
        return 0
    if lam < 50.0:
        limit = math.exp(-lam)
        k = 0
        p = 1.0
        while p > limit:
            k += 1
            p *= rng.random()
        return k - 1
    return max(0, int(round(rng.gauss(lam, math.sqrt(lam)))))


if __name__ == "__main__":
    raise SystemExit(main())
