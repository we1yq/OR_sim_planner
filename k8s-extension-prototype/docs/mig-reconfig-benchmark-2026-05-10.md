# MIG Reconfiguration Benchmark on rtx1

Date: 2026-05-10 to 2026-05-11 KST

Node: `rtx1`

GPU: NVIDIA A100-PCIE-40GB

GPU Operator MIG Manager config source: `gpu-operator/or-sim-mig-parted-config`

Baseline meaning:

- `all-enabled`: MIG mode is enabled, but no MIG instances are deployed.
- `allocatableSeconds`: elapsed time until Kubernetes reports the target MIG resources as allocatable. This is the most useful number for scheduling workloads.
- Empty transitions use `all-enabled` as the target and are considered ready when all MIG allocatable resources are `0`.

All transitions completed without timeout. Final restore completed to `all-2g.10gb` with `nvidia.com/mig-2g.10gb=3` allocatable.

## Summary

| Measurement | Count | Min | Avg | Max |
| --- | ---: | ---: | ---: | ---: |
| Empty MIG to template, allocatable | 14 | 102.539s | 113.203s | 120.811s |
| Template to empty MIG, allocatable | 14 | 10.073s | 10.365s | 12.089s |

## Template Deployment Times

| Template | Target MIG config | Success | Allocatable |
| --- | --- | ---: | ---: |
| `7` | `all-7g.40gb` | 62.339s | 102.539s |
| `4+3` | `or-sim-4-3` | 72.405s | 120.651s |
| `4+2+1` | `or-sim-4-2-1` | 72.414s | 112.627s |
| `4+1+1+1` | `or-sim-4-1-1-1` | 72.415s | 112.629s |
| `3+3` | `all-3g.20gb` | 72.406s | 110.611s |
| `3+2+1` | `or-sim-3-2-1` | 72.397s | 112.603s |
| `3+1+1+1` | `or-sim-3-1-1-1` | 72.420s | 114.648s |
| `2+2+3` | `or-sim-2-2-3` | 72.410s | 112.619s |
| `3+2+1+1` | `or-sim-3-2-1-1` | 72.404s | 112.612s |
| `3+1+1+1+1` | `or-sim-3-1-1-1-1` | 74.430s | 112.652s |
| `2+2+2+1` | `or-sim-2-2-2-1` | 72.402s | 112.609s |
| `2+2+1+1+1` | `or-sim-2-2-1-1-1` | 74.411s | 114.619s |
| `2+1+1+1+1+1` | `or-sim-2-1-1-1-1-1` | 72.396s | 120.811s |
| `1+1+1+1+1+1+1` | `all-1g.5gb` | 74.414s | 112.613s |

## Empty MIG Times

| From template | Target MIG config | Success | Allocatable-empty |
| --- | --- | ---: | ---: |
| `7` | `all-enabled` | 42.237s | 10.076s |
| `4+3` | `all-enabled` | 40.229s | 10.073s |
| `4+2+1` | `all-enabled` | 40.238s | 10.079s |
| `4+1+1+1` | `all-enabled` | 40.237s | 10.082s |
| `3+3` | `all-enabled` | 40.236s | 12.089s |
| `3+2+1` | `all-enabled` | 40.238s | 10.078s |
| `3+1+1+1` | `all-enabled` | 40.238s | 10.079s |
| `2+2+3` | `all-enabled` | 40.235s | 10.079s |
| `3+2+1+1` | `all-enabled` | 40.244s | 12.089s |
| `3+1+1+1+1` | `all-enabled` | 40.237s | 10.079s |
| `2+2+2+1` | `all-enabled` | 40.235s | 10.078s |
| `2+2+1+1+1` | `all-enabled` | 42.247s | 10.082s |
| `2+1+1+1+1+1` | `all-enabled` | 40.236s | 10.076s |
| `1+1+1+1+1+1+1` | `all-enabled` | 40.236s | 10.076s |

## Restore

| Transition | Target MIG config | Success | Allocatable |
| --- | --- | ---: | ---: |
| empty MIG to `2+2+2` | `all-2g.10gb` | 72.401s | 112.620s |

## Template-to-Template Times

Date: 2026-05-15 KST

These measurements start from the listed source template already ready on
`rtx1-worker`, patch `nvidia.com/mig.config` directly to the target template,
and wait for both `nvidia.com/mig.config.state=success` and target Kubernetes
`allocatable` resources. The final state was restored to `or-sim-empty`.

| From template | To template | Source config | Target config | Success | Allocatable |
| --- | --- | --- | --- | ---: | ---: |
| `4+3` | `3+2+1` | `or-sim-4-3` | `or-sim-3-2-1` | 40.165s | 80.237s |
| `3+2+1` | `2+1+1+1+1+1` | `or-sim-3-2-1` | `or-sim-2-1-1-1-1-1` | 42.309s | 82.429s |
| `2+1+1+1+1+1` | `4+3` | `or-sim-2-1-1-1-1-1` | `or-sim-4-3` | 40.142s | 82.414s |
| `4+3` | `7` | `or-sim-4-3` | `all-7g.40gb` | 40.165s | 80.282s |

Restore after the representative run:

| Transition | Target MIG config | Success | Allocatable-empty |
| --- | --- | ---: | ---: |
| template to empty MIG | `or-sim-empty` | 39.458s | 10.607s |

## Warm Empty-to-Template Times

Date: 2026-05-15 KST

These measurements start from `or-sim-empty` already ready after prior MIG
reconfiguration experiments on the same cluster. They test whether repeated
empty-to-template transitions become faster after the GPU Operator/device
plugin path has been warmed.

| From template | To template | Source config | Target config | Success | Allocatable |
| --- | --- | --- | --- | ---: | ---: |
| `empty` | `4+3` | `or-sim-empty` | `or-sim-4-3` | 40.157s | 78.177s |
| `empty` | `3+2+1` | `or-sim-empty` | `or-sim-3-2-1` | 71.796s | 114.022s |
| `empty` | `2+1+1+1+1+1` | `or-sim-empty` | `or-sim-2-1-1-1-1-1` | 71.874s | 111.999s |

Final restore after the warm run:

| Transition | Target MIG config | Success | Allocatable-empty |
| --- | --- | ---: | ---: |
| template to empty MIG | `or-sim-empty` | 39.433s | 10.606s |

## Notes

- The first attempt failed because the GPU Operator reconciled `default-mig-parted-config` and removed the custom `or-sim-*` entries.
- The fix is to use a separate ConfigMap, `or-sim-mig-parted-config`, and point `ClusterPolicy.spec.migManager.config.name` to it.
- Kubernetes `capacity` can retain historical MIG resource keys. Use `allocatable` and `nvidia.com/mig.config.state=success` for readiness checks.
- Direct template-to-template transitions are faster than modeling reconfiguration
  as template-to-empty plus empty-to-template. Use the direct allocatable-ready
  time when estimating in-place template rewrite cost.
- Warm empty-to-template behavior is template-dependent. `empty -> 4+3` was much
  faster in the warm run, while `empty -> 3+2+1` and
  `empty -> 2+1+1+1+1+1` remained close to the original empty-to-template
  timings. Do not globally replace empty-to-template constants with the fastest
  warm result.
