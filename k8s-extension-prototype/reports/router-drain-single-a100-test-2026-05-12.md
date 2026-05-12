# Router/Drain Adapter Single-A100 Test Report

Date: 2026-05-12  
Cluster: `or-sim-edge` via `~/.kube/or-sim-edge.yaml`  
Control-plane: `or-sim-control-plane` on desktap  
GPU worker: `rtx1-worker`  
Test GPU: one NVIDIA A100, configured as `all-3g.20gb`

## Implementation Covered

- Implemented the real Router/Drain execution path in `controller/executors/router_drain_executor.py`.
- Added a router/drain smoke action-plan builder in `controller/test_harness/router_drain_smoke.py`.
- Added a small HTTP router/workload test program in `tools/test_workloads/simple_router_workload.c`.
- Updated adapter structure documentation to separate real runtime adapters from test-only harnesses.

The adapter now:

- stops new requests on the source serving instance;
- reroutes new/queued traffic to the target endpoint;
- waits for source `inflight=0` and `queued=0`;
- annotates the source Pod drain state;
- records real execution status in `MigActionPlan`, `WorkloadRoutePlan`, and `ServingInstanceDrain`.

## Test Topology

The test used a single A100 split into two `3g.20gb` MIG partitions.

| Pod | Node | GPU request | Role |
| --- | --- | --- | --- |
| `router-workload-a` | `rtx1-worker` | `nvidia.com/mig-3g.20gb: 1` | source serving instance |
| `router-workload-b` | `rtx1-worker` | `nvidia.com/mig-3g.20gb: 1` | target serving instance |
| `or-sim-smoke-router` | `rtx1-worker` | none | HTTP router |

Initial node check:

```text
nvidia.com/mig.config       all-3g.20gb
nvidia.com/mig.config.state success
capacity mig-3g.20gb        2
allocatable mig-3g.20gb     2
```

Pod placement:

```text
or-sim-smoke-router   Running   rtx1-worker
router-workload-a     Running   rtx1-worker
router-workload-b     Running   rtx1-worker
```

MIG device visibility inside the workload Pods:

```text
router-workload-a MIG UUID: MIG-41494cc9-8ea6-56db-972e-a67bf4756eb1
router-workload-b MIG UUID: MIG-19c09be3-6717-523e-ad80-1cc9416aa9df
```

## Test Procedure And Results

### 1. Pre-route Check

Request:

```text
GET http://127.0.0.1:18080/route?workload=resnet50&ms=50
```

Result:

```json
{
  "ok": true,
  "workload": "resnet50",
  "target": "http://router-workload-a:8080",
  "upstream": {
    "ok": true,
    "workload": "resnet50",
    "instance": "a",
    "ms": 50
  }
}
```

Elapsed time: `79.5 ms`  
Status: success

### 2. Router/Drain ActionPlan Creation

ActionPlan: `router-drain-smoke`  
Traffic actions:

- `stop_accepting_new`
- `reroute_queued_tasks`
- `mark_draining_instance`

Status after creation:

```text
phase: ApprovedDryRun
approved: true
executed: false
```

Status: success

### 3. Real Router/Drain Apply

Command path:

```text
python3 k8s-extension-prototype/controller/main.py \
  --namespace or-sim \
  --apply-router-drain-from-action-plan router-drain-smoke \
  --confirm-real-router-apply \
  --allow-preview-instructions \
  --router-endpoint http://127.0.0.1:18080 \
  --router-drain-mode http \
  --mig-apply-timeout-s 120
```

Result summary:

```text
stopAcceptingNew success: true
reroute success: true
drain success: true
verification success: true
adapter timing total: 0.149 s
outer command elapsed: 0.50 s
```

Drain metrics:

```json
{
  "ok": true,
  "instance": "a",
  "inflight": 0,
  "queued": 0,
  "accepting": false
}
```

Verification routed traffic to the target instance:

```json
{
  "ok": true,
  "workload": "resnet50",
  "target": "http://router-workload-b:8080",
  "upstream": {
    "ok": true,
    "workload": "resnet50",
    "instance": "b",
    "ms": 50
  }
}
```

Status: success

### 4. Post-route Check

Request:

```text
GET http://127.0.0.1:18080/route?workload=resnet50&ms=50
```

Result:

```json
{
  "ok": true,
  "workload": "resnet50",
  "target": "http://router-workload-b:8080",
  "upstream": {
    "ok": true,
    "workload": "resnet50",
    "instance": "b",
    "ms": 50
  }
}
```

Elapsed time: `76.6 ms`  
Status: success

### 5. Kubernetes CR Evidence

`WorkloadRoutePlan` objects were created for:

- `StopAcceptingNew`
- `RerouteQueuedTasks`

Both reached:

```text
phase: SucceededRealRouterDrain
previewOnly: false
validatedBy: router-drain-executor
```

`ServingInstanceDrain` reached:

```text
phase: SucceededRealRouterDrain
message: Drain completed in 0.014s.
previewOnly: false
validatedBy: router-drain-executor
```

Source Pod annotations after drain:

```text
mig.or-sim.io/accepting-new=false
mig.or-sim.io/draining=true
mig.or-sim.io/drain-state=drained
mig.or-sim.io/inflight=0
mig.or-sim.io/queued=0
```

Status: success

## What This Proves

This test proves that the current Router/Drain adapter can perform real
partition-to-partition traffic movement on one A100:

- source and target workload instances can run on separate MIG partitions;
- traffic initially reaches the source instance;
- the adapter can stop new requests on the source;
- the adapter can update router state to target another instance;
- the adapter waits for source inflight/queued work to clear;
- follow-up traffic reaches the target instance;
- execution status is recorded in Kubernetes CRs.

## What Still Requires Multi-GPU Or Production Runtime Testing

Not fully validated in this single-A100 test:

- routing between different physical GPUs;
- routing between different servers;
- integration with a production model-serving router or load balancer;
- integration with a production queue that owns queued requests;
- runtime-native inflight metrics from the actual serving framework;
- failure-mode handling for unreachable source, target, or router endpoints.

The adapter contract is endpoint-based, so it is designed to work for
multi-GPU and multi-server routing once those endpoints and runtime APIs are
available.
