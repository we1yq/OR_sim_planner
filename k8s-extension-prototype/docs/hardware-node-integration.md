# Hardware Node Integration

This document defines the minimum handoff needed before the prototype can trust
real NVIDIA GPU nodes.

## Control-Plane Contract

The planner must not canonicalize the next source state from a planned target
alone. The required production path is:

```text
MigActionPlan
  -> child execution CRs
  -> real GPU/router/pod adapters execute
  -> observer writes ObservedClusterState
  -> ObservedClusterState.status.readyForCanonicalization: true
  -> canonicalize observed state for the next planning epoch
```

In kind, `ObservedClusterState` is preview-only and
`readyForCanonicalization: false`.

## ObservedClusterState Shape

A real observer should write:

```yaml
apiVersion: mig.or-sim.io/v1alpha1
kind: ObservedClusterState
metadata:
  name: <action-plan>-observed-state
  namespace: or-sim
spec:
  previewOnly: false
  source: real-cluster-observer
  ownerActionPlan: <action-plan>
  observedState:
    physicalGpuBindings:
      A:
        nodeName: gpu-node-0
        deviceIndex: 0
        gpuUuid: GPU-...
    migLayouts:
      - physicalGpuId: A
        nodeName: gpu-node-0
        deviceIndex: 0
        instances:
          - start: 0
            end: 4
            profile: 4g
            migDeviceUuid: MIG-...
    podReadiness:
      - workload: llama
        podName: llama-0
        ready: true
    podAssignments:
      - workload: llama
        podName: llama-0
        physicalGpuId: A
        migDeviceUuid: MIG-...
    routerState:
      - workload: llama
        acceptingNew: true
    inflightByInstance:
      - workload: llama
        podName: llama-0
        inflight: 0
    queuedByWorkload:
      - workload: llama
        queued: 0
  missingRealClusterInputs: []
  canonicalizationRule: canonicalize observed state only after real execution
status:
  phase: Observed
  previewOnly: false
  readyForCanonicalization: true
```

## Real Node Readiness Checklist

- `python3 k8s-extension-prototype/controller/main.py --observe-cluster-state --apply-observed-state`
  can list Kubernetes Nodes and Pods and write an `ObservedClusterState`.
- NVIDIA GPU nodes are visible through Kubernetes `Node` objects.
- GPU Operator and MIG Manager are installed and can apply MIG configs.
- The observer can map physical GPU IDs to `nodeName`, `deviceIndex`, and GPU
  UUID.
- The observer can list actual MIG instances and MIG device UUIDs.
- Serving Pods expose readiness.
- Pod-to-MIG assignment can be observed.
- Router exposes accepting-new, queue length, and inflight metrics.
- `ObservedClusterState.status.readyForCanonicalization` remains false until all
  required observations are present.

## Current Non-Hardware Behavior

The dry-run actuator writes an `ObservedClusterState` preview to validate CRD,
RBAC, and status flow. It deliberately sets:

```yaml
spec.previewOnly: true
status.readyForCanonicalization: false
```

This prevents the kind path from being mistaken for proof that real hardware
execution succeeded.
