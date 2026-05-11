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

## Notes

- The first attempt failed because the GPU Operator reconciled `default-mig-parted-config` and removed the custom `or-sim-*` entries.
- The fix is to use a separate ConfigMap, `or-sim-mig-parted-config`, and point `ClusterPolicy.spec.migManager.config.name` to it.
- Kubernetes `capacity` can retain historical MIG resource keys. Use `allocatable` and `nvidia.com/mig.config.state=success` for readiness checks.
