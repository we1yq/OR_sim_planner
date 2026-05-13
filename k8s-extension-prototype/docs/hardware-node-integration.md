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
      gpu-node-0-gpu0:
        physicalGpuId: gpu-node-0-gpu0
        nodeName: gpu-node-0
        deviceIndex: 0
        gpuUuid: GPU-...
        product: NVIDIA A100-PCIE-40GB
        migCapable: true
        migDevices:
          - profile: 4g.20gb
            migDeviceUuid: MIG-...
    ignoredGpuDevices:
      - nodeName: gpu-node-0
        deviceIndex: 1
        product: NVIDIA TITAN RTX
        reason: non-A100 GPU ignored by MIGRANT MIG planner
    migLayouts:
      - physicalGpuId: gpu-node-0-gpu0
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
        physicalGpuId: gpu-node-0-gpu0
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

## PhysicalGpuRegistry

`ObservedClusterState` is a point-in-time hardware observation. The controller
also maintains a durable `PhysicalGpuRegistry` CR so the planner can distinguish
hardware identity from logical GPU numbering.

Stable identity rules:

- `gpuUuid` is the real NVIDIA UUID from `nvidia-smi -L`.
- `physicalGpuId` is a stable MIGRANT alias permanently bound to that UUID, such
  as `rtx1-gpu0`.
- planner `gpu_id` is not hardware identity. It is a logical position in the
  global canonical GPU queue and may change after canonicalization.
- only A100 devices are admitted into the registry queues. Non-A100 GPUs remain
  in `ignoredGpuDevices`.

Queue rules:

```yaml
status:
  discoveredA100:
    - rtx1-gpu0
  activeQueue: []
  availableQueue:
    - rtx1-gpu0
  transitioningQueue: []
  bindings:
    rtx1-gpu0:
      physicalGpuId: rtx1-gpu0
      gpuUuid: GPU-...
      nodeName: rtx1
      deviceIndex: 0
      product: NVIDIA A100-PCIE-40GB
      cleanliness: empty
      state: available
  ignoredGpuDevices:
    - physicalGpuId: rtx1-gpu1
      product: NVIDIA TITAN RTX
      reason: non-A100 GPU ignored by MIGRANT MIG planner
```

- `discoveredA100` is the observed inventory of relevant A100 hardware.
- `availableQueue` contains only observed A100 GPUs that are not active and have
  no MIG devices with `nvidia.com/mig.config=or-sim-empty` and
  `nvidia.com/mig.config.state=success`.
- `transitioningQueue` contains observed A100 GPUs that require cleanup or
  reconfiguration before they can become available.
- `activeQueue` is planner-owned. The monitor preserves it from registry status
  and does not infer activity merely because a MIG template exists.
- when the planner releases a GPU, the release path must clear its MIG template
  by applying `or-sim-empty`, which keeps MIG mode enabled while removing all
  MIG devices. If the GPU is still configured, disabled, failed, or pending, it
  stays in `transitioningQueue` with `requiredAction:
  clear_template_before_available`.

The first multi-server allocator should draw from `availableQueue`, append the
chosen physical IDs to the global active queue, then canonicalize the observed
active set into logical `gpu_id` values for the next planning epoch.

## Registry Monitor

`PhysicalGpuRegistry` is maintained by a small monitor loop, not by GPU
Operator directly. The current inventory provider is
`GpuOperatorExecProvider`: it execs a GPU Operator pod and runs `nvidia-smi -L`
to collect real GPU UUIDs and MIG device UUIDs. This provider is intentionally
behind the observer interface; a later production deployment can replace it with
a node agent/exporter while keeping the same `ObservedClusterState` and
`PhysicalGpuRegistry` shapes.

The monitor repeatedly:

1. observes Kubernetes nodes and GPU Operator inventory,
2. writes `ObservedClusterState/cluster-observed-state`,
3. preserves planner-owned `activeQueue`,
4. recomputes `discoveredA100`, `availableQueue`, `transitioningQueue`, and
   `ignoredGpuDevices`,
5. patches `PhysicalGpuRegistry/default`.

Run it locally for debugging:

```bash
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml python3 k8s-extension-prototype/controller/main.py \
  --namespace or-sim \
  --run-physical-gpu-registry-monitor \
  --poll-interval-s 30
```

Run one cycle:

```bash
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml python3 k8s-extension-prototype/controller/main.py \
  --namespace or-sim \
  --run-physical-gpu-registry-monitor \
  --controller-max-cycles 1
```

The deployment manifest is
`manifests/controller/registry-monitor-deployment.yaml`. In-cluster execution
needs read access to nodes and GPU Operator pods. If real GPU UUID collection is
enabled through `nvidia-smi -L`, the service account also needs narrowly-scoped
`pods/exec` permission for GPU Operator pods.

Apply the in-cluster monitor pieces:

```bash
kubectl apply -f k8s-extension-prototype/manifests/controller/registry-monitor-gpu-operator-rbac.yaml
kubectl apply -f k8s-extension-prototype/manifests/controller/registry-monitor-deployment.yaml
```

`registry-monitor-gpu-operator-rbac.yaml` creates a Role only in the
`gpu-operator` namespace and binds it to `or-sim/mig-planner-controller`. It
does not grant cluster-wide exec.

During MIG reconfiguration, the monitor avoids interfering with GPU Operator:

```text
if any node has nvidia.com/mig.config.state=pending:
  skip GPU Operator pod exec for this cycle
  reuse stable GPU UUID bindings from the previous PhysicalGpuRegistry
  mark cached MIG device inventory as not fresh
  keep the GPU out of availableQueue until or-sim-empty/template success
```

After the config reaches `success` or `failed`, the next cycle may exec again to
refresh MIG device UUIDs.

## Real Node Readiness Checklist

- `python3 k8s-extension-prototype/controller/main.py --observe-cluster-state --apply-observed-state`
  can list Kubernetes Nodes and Pods and write an `ObservedClusterState`.
- `python3 k8s-extension-prototype/controller/main.py --sync-physical-gpu-registry --apply-physical-gpu-registry`
  can write `PhysicalGpuRegistry/default`.
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
