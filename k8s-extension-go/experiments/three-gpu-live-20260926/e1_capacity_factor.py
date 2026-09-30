#!/usr/bin/env python3
"""Actual replica capacity in an E1 run versus the catalog mu (data only).

Usage: e1_capacity_factor.py <run_dir> <catalog.csv> [queue_ms]
Writes <run_dir>/analysis/capacity_factor.csv and prints a summary.

A replica's actual capacity can only be observed while it is saturated, i.e.
while requests wait in the router queue: the router hands the next request to
a replica as soon as it has room, so each replica then completes requests as
fast as it can.  A second counts as saturated for a workload when the median
router queue wait (response.queueWaitMs) of the requests dispatched in that
second exceeds queue_ms (default 50) and at least 95% of them went out in full
batches.  The second condition matters for large batches: requests arrive one
by one, so a b16 replica whose batch takes about as long to fill as to run
queues its requests for the fill time and sends partial batches although it
keeps up (vit_base 1g b16: ~130 ms waits, 44% full batches).  Per replica, option (profile from the
runtime id, batch = the replica's configured maxBatchSize) and round,
capacity = requests completed in its saturated seconds / number of those seconds; the first and last
saturated second of each run of consecutive seconds are dropped as partial.
factor = capacity / catalog mu.
"""
from __future__ import annotations

import csv
import gzip
import statistics
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path


def main() -> int:
    run, catalog = Path(sys.argv[1]), Path(sys.argv[2])
    queue_ms = float(sys.argv[3]) if len(sys.argv) > 3 else 50.0
    mu = {(r["workload"], r["profile"], int(r["batch"])): float(r["mu"]) for r in csv.DictReader(catalog.open())}

    waits: dict[tuple[str, int], list[float]] = defaultdict(list)
    full: dict[tuple[str, int], list[bool]] = defaultdict(list)
    done: dict[tuple[str, int, str, int, int], int] = defaultdict(int)  # (workload, second, runtime, batch, round)
    for r in csv.DictReader(gzip.open(run / "requests.csv.gz", "rt")):
        if r["status"] != "success" or not r["response.routerDispatchAt"]:
            continue
        w = r["workload"]
        # RFC 3339 UTC with nanoseconds; keep microseconds for fromisoformat()
        dispatched = datetime.fromisoformat(r["response.routerDispatchAt"][:26].rstrip("Z") + "+00:00").timestamp()
        batch = int(r["response.maxBatchSize"] or r["response.batchSize"] or 1)
        waits[(w, int(dispatched))].append(float(r["response.queueWaitMs"] or 0))
        full[(w, int(dispatched))].append(int(r["response.batchSize"] or 1) >= batch)
        done[(w, int(float(r["completion"])), r["response.runtimeId"], batch, int(r["live_round"]))] += 1

    saturated = {k for k, v in waits.items()
                 if statistics.median(v) > queue_ms and sum(full[k]) >= 0.95 * len(full[k])}
    interior = set()
    for (w, s) in saturated:
        if (w, s - 1) in saturated and (w, s + 1) in saturated:
            interior.add((w, s))

    per: dict[tuple[str, str, int, int], dict[int, int]] = defaultdict(lambda: defaultdict(int))
    for (w, s, rid, b, rnd), n in done.items():
        if (w, s) in interior:
            per[(w, rid, b, rnd)][s] += n
    rows = []
    for (w, rid, b, rnd), by_s in sorted(per.items()):
        if len(by_s) < 5:
            continue
        profile = rid.rsplit("-", 1)[-1]
        m = mu.get((w, profile, b))
        if not m:
            continue
        cap = sum(by_s.values()) / len(by_s)
        rows.append({"workload": w, "runtime": rid, "profile": profile, "batch": b, "round": rnd,
                     "saturated_seconds": len(by_s), "capacity_rps": round(cap, 3), "catalog_mu": m,
                     "factor": round(cap / m, 4)})
    out = run / "analysis"
    out.mkdir(exist_ok=True)
    if rows:
        with (out / "capacity_factor.csv").open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
    print(f"{run.name}: saturated seconds per workload: "
          + str({w: sum(1 for (x, _) in interior if x == w) for w in sorted({w for w, _ in interior})}))
    for r in rows:
        print(f"  R{r['round']:<2} {r['workload']:15s} {r['runtime'].split('-image-')[-1]:24s} b{r['batch']:<2} "
              f"{r['saturated_seconds']:3d}s  actual {r['capacity_rps']:8.2f}  catalog {r['catalog_mu']:8.2f}  factor {r['factor']:.3f}")
    if rows:
        f = [r["factor"] for r in rows]
        print(f"factor over {len(rows)} replica-rounds: min {min(f):.3f}  median {statistics.median(f):.3f}  max {max(f):.3f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
