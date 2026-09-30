# Live run results

| directory | content |
|---|---|
| `20260930T154846.076040Z` | **Final E1 / Section 4.6, SliceWise** (catalog v3, Poisson arrivals, `--conservative-3g-mu`) |
| `20260930T161303.183531Z` | **Final E1 / Section 4.6, SW-C** (same setup, `--stage3-variant sw-c`) |
| `20260930T112125Z-capacity-calibration` | capacity calibration: every replica saturated on the E1 layouts (`e1_capacity_calibration.py`) |
| `20260929T143422.387493Z` | **E1 / Section 4.6, SliceWise** (`--conservative-3g-mu`, catalog `catalog_newest.csv`) |
| `20260929T145358.996350Z` | **E1 / Section 4.6, SW-C** (same setup, `--stage3-variant sw-c`) |
| `success_r01_r12_20260926` | earlier 12-round makespan run (2026-09-26), unrelated to E1 |

The final runs, the traffic generator, the router, the analysis method and the capacity factor are described in
`../E1_FINAL_20260930.md`; the earlier process and the failed or superseded runs in `../E1_EXPERIMENT_LOG.md`. The superseded runs were removed from the repository and
are summarised there.

Each E1 run holds:
- `plans/` and `snapshots/` per round;
- `requests.csv.gz`, the per-request fields used by the analysis (`compact_requests.py`). The raw `requests.jsonl` and `planned_requests.csv` are not committed.
- `analysis/`, produced by `e1_analyze.py` and `s46_analyze.py`;
- `strict_runtime_audit.md`.
