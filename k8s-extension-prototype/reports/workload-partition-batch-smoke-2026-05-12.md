# OR-SIM Workload Partition And Batch-Size Smoke Test Report

Date: 2026-05-12  
Cluster: `desktap` control-plane + `rtx1-worker` GPU worker  
Kubeconfig: `~/.kube/or-sim-edge.yaml`  
Test tool: `k8s-extension-prototype/tools/smoke/workload_partition_smoke.py`

## Summary

Result: PASS

This test validated two workload-facing behaviors on the migrated edge cluster:

- A workload Pod can be scheduled onto the A100 after OR-SIM changes the A100 from empty MIG mode to two `3g.20gb` instances.
- A running Pod can observe a batch-size change without Pod deletion or recreation by reading a projected ConfigMap volume.

Important scope note: Kubernetes GPU scheduling selected a `nvidia.com/mig-3g.20gb` resource instance, not a specific MIG UUID. Exact per-partition UUID pinning is not implemented yet.

## Preconditions

Before the test, `PhysicalGpuRegistry/default` showed the A100 as available and clean:

```text
availableQueue:
- rtx1-worker-gpu0
transitioningQueue: []
bindings.rtx1-worker-gpu0.currentMigConfig: or-sim-empty
bindings.rtx1-worker-gpu0.currentMigConfigState: success
```

## MIG Setup For Workload Test

### Create Test MIG Geometry

OR-SIM created a temporary `MigActionPlan` targeting `3g+3g` on the single A100:

```text
plan: workload-test-a100-3g3g
nodeName: rtx1-worker
deviceIndex: 0
sourceTemplate: empty
targetTemplate: 3g+3g
```

Real apply result:

```text
targetMigConfig: all-3g.20gb
before:
  migConfig: or-sim-empty
  migConfigState: success
observed:
  migConfig: all-3g.20gb
  migConfigState: success
elapsed_s=40.88
```

Device-plugin readiness after the MIG change:

```text
pod/nvidia-device-plugin-daemonset-mh8cs condition met
elapsed_s=23.13
```

Stable node resources:

```text
capacity:
  nvidia.com/mig-3g.20gb: 2
allocatable:
  nvidia.com/mig-3g.20gb: 2
```

## Workload Placement Test

The smoke tool created:

- ConfigMap: `or-sim-workload-smoke-config`
- Pod: `or-sim-workload-smoke`
- Node selector: `kubernetes.io/hostname=rtx1-worker`
- GPU limit: `nvidia.com/mig-3g.20gb: 1`
- Image: `nvidia/cuda:12.4.1-base-ubuntu22.04`

Command:

```text
python3 k8s-extension-prototype/tools/smoke/workload_partition_smoke.py \
  --namespace or-sim \
  --pod-name or-sim-workload-smoke \
  --configmap-name or-sim-workload-smoke-config \
  --node-name rtx1-worker \
  --mig-resource nvidia.com/mig-3g.20gb \
  --initial-batch-size 4 \
  --updated-batch-size 8 \
  --timeout-s 300
```

Result:

```yaml
success: true
podName: or-sim-workload-smoke
nodeName: rtx1-worker
requestedMigResource: nvidia.com/mig-3g.20gb
image: nvidia/cuda:12.4.1-base-ubuntu22.04
initialBatchSize: '4'
updatedBatchSize: '8'
podUidBeforeUpdate: 9b0352bc-41cf-495c-aebf-4fd97b4edf86
podUidAfterUpdate: 9b0352bc-41cf-495c-aebf-4fd97b4edf86
podRecreatedDuringBatchUpdate: false
phase: Running
timingsSeconds:
  podReady: 6.95
  initialBatchObserved: 0.016
  batchUpdateObservedAfterPatch: 1.01
  total: 8.015
```

Shell elapsed time:

```text
elapsed_s=8.33
```

Container evidence:

```text
or-sim workload smoke starting
GPU 0: NVIDIA A100-PCIE-40GB (UUID: GPU-03ca4983-f693-39d2-d7e0-25090fe07b2f)
  MIG 3g.20gb Device 0: (UUID: MIG-...)
tick=0 batch_size=4
tick=1 batch_size=8
```

Interpretation:

- The Pod landed on `rtx1-worker`.
- The container saw exactly one `MIG 3g.20gb` device.
- The Pod UID stayed unchanged, so the batch-size update did not delete or recreate the Pod.
- The batch-size update was observed about `1.01s` after the ConfigMap patch.

## Cleanup And Restore

The smoke Pod and ConfigMap were removed:

```text
pod "or-sim-workload-smoke" force deleted
configmap "or-sim-workload-smoke-config" deleted
```

The A100 was restored to empty MIG mode with another temporary `MigActionPlan`:

```text
plan: workload-test-a100-empty
targetMigConfig: or-sim-empty
before:
  migConfig: all-3g.20gb
  migConfigState: success
observed:
  migConfig: or-sim-empty
  migConfigState: success
elapsed_s=40.89
```

Post-restore device-plugin readiness:

```text
pod/nvidia-device-plugin-daemonset-v9wj4 condition met
elapsed_s=0.16
```

Post-restore GPU inventory:

```text
GPU 0: NVIDIA A100-PCIE-40GB
GPU 1: NVIDIA TITAN RTX
GPU 2: NVIDIA TITAN RTX
GPU 3: NVIDIA GeForce RTX 3090
```

No MIG child devices were present in `nvidia-smi -L`.

Post-restore registry status:

```text
queueCounts:
  discovered: 1
  available: 1
  active: 0
  transitioning: 0
  ignored: 3
availableQueue:
- rtx1-worker-gpu0
transitioningQueue: []
bindings.rtx1-worker-gpu0.currentMigConfig: or-sim-empty
bindings.rtx1-worker-gpu0.currentMigConfigState: success
```

Temporary action plans were deleted:

```text
migactionplan.mig.or-sim.io "workload-test-a100-3g3g" deleted
migactionplan.mig.or-sim.io "workload-test-a100-empty" deleted
```

## What This Proves

- The migrated architecture can reconfigure the single A100 into a workload-capable MIG geometry.
- A workload can request a MIG partition class and run on that partition.
- A batch-size update can be propagated into a running Pod without deleting or redeploying that Pod.
- The registry monitor automatically returns the A100 to `availableQueue` after restore to `or-sim-empty`.

## Remaining Gap

This is a smoke implementation, not the final workload lifecycle executor. The final system still needs to connect `MigActionPlan.spec.podLifecyclePreview` to a real workload lifecycle adapter that owns Pod creation, readiness checks, live batch update, router/drain coordination, and cleanup as part of one reconciled action flow.
