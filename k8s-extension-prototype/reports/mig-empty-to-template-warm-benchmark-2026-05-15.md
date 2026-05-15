# Warm Empty-to-Template MIG Benchmark on rtx1

Date: 2026-05-15 KST

Node: `rtx1-worker`

Method: each row starts from `or-sim-empty` already ready on `rtx1-worker`, patches `nvidia.com/mig.config` to the target template, waits for `nvidia.com/mig.config.state=success`, then waits until Kubernetes `allocatable` matches the target MIG resources.

## Results

| From template | To template | Source config | Target config | Success | Allocatable |
| --- | --- | --- | --- | ---: | ---: |
| `empty` | `4+3` | `or-sim-empty` | `or-sim-4-3` | 40.157s | 78.177s |
| `empty` | `3+2+1` | `or-sim-empty` | `or-sim-3-2-1` | 71.796s | 114.022s |
| `empty` | `2+1+1+1+1+1` | `or-sim-empty` | `or-sim-2-1-1-1-1-1` | 71.874s | 111.999s |

## Final Restore

| Target | Success | Allocatable-empty |
| --- | ---: | ---: |
| `or-sim-empty` | 39.433s | 10.606s |

## Notes

- These are warm measurements taken after the cluster had already exercised MIG template changes.
- Compare against the original 2026-05-10/11 empty-to-template table before replacing planner constants.
