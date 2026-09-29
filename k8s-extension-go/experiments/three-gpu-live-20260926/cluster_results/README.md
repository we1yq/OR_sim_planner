# Live run results

| directory | content |
|---|---|
| `20260929T143422.387493Z` | **E1 / Section 4.6, SliceWise** (`--conservative-3g-mu`, catalog `catalog_newest.csv`) |
| `20260929T145358.996350Z` | **E1 / Section 4.6, SW-C** (same setup, `--stage3-variant sw-c`) |
| `success_r01_r12_20260926` | earlier 12-round makespan run (2026-09-26), unrelated to E1 |

The full process, the final results and the failed or superseded runs are described in
`../E1_EXPERIMENT_LOG.md`. The superseded runs were removed from the repository and
are summarised there.

Each E1 run holds:
- `plans/` and `snapshots/` per round;
- `requests.csv.gz`, the per-request fields used by the analysis (`compact_requests.py`). The raw `requests.jsonl` and `planned_requests.csv` are not committed.
- `analysis/`, produced by `e1_analyze.py` and `s46_analyze.py`;
- `strict_runtime_audit.md`.
