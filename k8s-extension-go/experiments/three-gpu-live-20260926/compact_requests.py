#!/usr/bin/env python3
"""Write <run_dir>/requests.csv.gz: the per-request fields the E1 / 4.6
analysis uses, from requests.jsonl (payloads and full responses dropped).

Usage: compact_requests.py <run_dir> [<run_dir> ...]
e1_analyze.py and s46_analyze.py read requests.csv.gz when requests.jsonl is
absent (see load_requests in e1_analyze.py).
"""
from __future__ import annotations

import csv
import gzip
import json
import sys
from pathlib import Path

REQUEST_FIELDS = ["sequence_id", "workload", "family", "live_round", "phase", "rate", "scheduled_send",
                  "actual_send", "send_lag_s", "completion", "status", "error", "attempts"]
RESPONSE_FIELDS = ["runtimeId", "batchSize", "maxBatchSize", "routerDispatchAt", "queueWaitMs", "serviceLatencyMs",
                   "latencyMs", "runtimeLatencyMs", "ttftMs", "tpotMs", "outputTokens"]


def compact(run: Path) -> Path:
    out = run / "requests.csv.gz"
    with (run / "requests.jsonl").open() as src, gzip.open(out, "wt", newline="") as dst:
        writer = csv.DictWriter(dst, fieldnames=REQUEST_FIELDS + ["response." + f for f in RESPONSE_FIELDS])
        writer.writeheader()
        for line in src:
            if not line.strip():
                continue
            r = json.loads(line)
            row = {f: r.get(f) for f in REQUEST_FIELDS}
            row["error"] = (row["error"] or "")[:120]
            response = json.loads(r.get("response_json") or "{}")
            row.update({"response." + f: response.get(f) for f in RESPONSE_FIELDS})
            writer.writerow(row)
    return out


if __name__ == "__main__":
    for arg in sys.argv[1:]:
        print(compact(Path(arg)))
