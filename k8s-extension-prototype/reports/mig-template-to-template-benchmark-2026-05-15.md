# MIG Template-to-Template Benchmark on rtx1

Date: 2026-05-15 KST

Node: `rtx1-worker`

Method: patch `nvidia.com/mig.config` on `rtx1-worker`, wait for `nvidia.com/mig.config.state=success`, then wait until Kubernetes `allocatable` matches the target MIG resources. Each measured row starts from the listed source template already ready.

## Results

| From template | To template | Source config | Target config | Success | Allocatable |
| --- | --- | --- | --- | ---: | ---: |
| `4+3` | `3+2+1` | `or-sim-4-3` | `or-sim-3-2-1` | 40.165s | 80.237s |
| `3+2+1` | `2+1+1+1+1+1` | `or-sim-3-2-1` | `or-sim-2-1-1-1-1-1` | 42.309s | 82.429s |
| `2+1+1+1+1+1` | `4+3` | `or-sim-2-1-1-1-1-1` | `or-sim-4-3` | 40.142s | 82.414s |
| `4+3` | `7` | `or-sim-4-3` | `all-7g.40gb` | 40.165s | 80.282s |

## Restore

| Target | Success | Allocatable-empty |
| --- | ---: | ---: |
| `or-sim-empty` | 39.458s | 10.607s |

## Notes

- These are direct template-to-template GPU Operator transitions; they include MIG Manager/GPU reset behavior and Kubernetes device-plugin allocatable refresh.
- `Success` is the MIG Manager label success time. `Allocatable` is the scheduling-ready time and should be used by the action planner for workload placement.
- This is a representative subset, not the full 14x13 transition matrix.
