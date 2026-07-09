# Go System Components

This directory is the production-oriented Go implementation track. It contains
only the deployed control-plane, node-agent, device-plugin, router, and runtime
paths used by the real cluster. A component is listed here only when it has
executable Go code and a Kubernetes deployment path in this tree.

## Fixed Pods

Deployments:

- `epoch-controller`: observes runtime-router demand metrics and creates
  `ArrivalSnapshot` objects when registered epoch triggers fire. It also acts as
  the reconciler for registry-reported repair work: when
  `PhysicalGpuRegistry.status.health.repairRequired=true`, it creates a repair
  `MigActionPlan` instead of invoking the optimization planner.
- `cluster-state-manager`: observes Kubernetes node/GPU labels and publishes the
  `PhysicalGpuRegistry` status used by the Go control plane. It classifies
  GPU health and writes `status.health.requiredActions`; it does not execute
  those actions itself.
- `planner-controller`: turns an `ArrivalSnapshot` and the current
  `PhysicalGpuRegistry` into a planner request, calls `planner-engine`, and
  emits `MigActionPlan` objects. If registry health is not stable, it waits for
  the repair path before starting demand-driven optimization.
- `planner-engine`: runs the original planner pipeline in this tree:
  Gurobi MILP target allocation, target materialization/greedy repair, transition
  action extraction, and effect-aware action DAG generation.
- `transition-executor`: executes `MigActionPlan` objects by calling node-agent,
  creating runtime Deployments with exact slot resources, and syncing routes.
- `runtime-router`: handles request routing, router control state, and demand
  metrics.

DaemonSets:

- `slot-device-plugin`: exposes exact logical MIG slot resources to kubelet.
- `mig-node-agent`: executes node-local MIG operations behind an API.

System-created workload Pods:

- `model-runtime`: serving Pods created by `transition-executor`. The runtime
  exposes HTTP inference endpoints. It supports two modes:
  `RUNTIME_MODE=synthetic` for fast control-plane smoke tests and
  `RUNTIME_MODE=torchvision` for real CUDA image-model inference with official
  torchvision `DEFAULT` weights cached in the image. In both modes, the runtime
  reports the assigned MIG UUID and can be verified through Kubernetes
  per-MIG-UUID resource allocation.

## Runtime Network Layout

`runtime-router` is a control-plane service. It is pinned to
`or-sim-control-plane` and uses host networking on port `10680` so clients and
controllers have a stable entry point:

```text
runtime-router Service
  --> or-sim-control-plane:10680
  --> runtime-router
```

Model runtime Pods are not manually pre-created. They are serving workloads that
the transition executor will create from an action plan, place onto GPU worker
nodes, and register with the router.

The router stores route groups, not a single backend per model:

```text
model
  --> runtime endpoint 0
  --> runtime endpoint 1
  --> ...
```

Each endpoint has a `runtimeId`, endpoint URL, profile, batch size, physical GPU,
MIG UUID resource, capacity, and weight. Planner/executor decide the route group;
the router only chooses among endpoints already admitted by the current plan.

Request selection uses planner-constrained weighted least-inflight:

```text
score(endpoint) = endpointInflight / endpointWeight
choose the active, acceptingNew, non-draining endpoint with the lowest score
```

This lets multiple runtime Pods for the same model share traffic according to
their planned capacity while still reacting to instantaneous queue pressure.

The current three-GPU cluster includes `ampere`, whose inbound firewall only
allows ports `10600-10800`. Runtime Pods placed on ampere must therefore either
use host-network ports in that range; ordinary cross-node Pod overlay traffic to
Pods on ampere is not a reliable backend discovery path.

## Image Distribution

The runtime path uses formal Go images:

```text
localhost:10690/migrant-runtime-router:go
localhost:10690/migrant-model-runtime:go
localhost:10690/migrant-control-plane:go
localhost:10690/migrant-node-agent:go
localhost:10690/migrant-planner-engine:go
```

`localhost:10690` is a node-local registry DaemonSet deployed with host
networking on every node. Each node pulls from its own local registry instance.
The build jobs use Kaniko and the `migrant-go-build-context` ConfigMap to
rebuild and push component images without mounting the host container runtime
socket. The GPU-node warmup build template intentionally builds only
`migrant-model-runtime:go` and `migrant-node-agent:go`; control-plane images are
built on the control-plane node, and the runtime router is not needed on GPU
nodes. After the GPU-node warmup build, run the model-runtime prepull Job on
each GPU node to move `migrant-model-runtime:go` from the node-local registry
into that node's containerd cache before experiments. Generate the build-context
ConfigMap from the current tree with `tools/create-build-context-configmap.sh`.

## Request And Metrics Path

```text
client request
  --> runtime-router
  --> model-runtime Pod
  --> runtime-router records arrival / latency / error
  --> /metrics/demand and /metrics/profile-observations
```

Torchvision runtime observations separate runtime-internal CUDA inference time
from router-visible end-to-end latency:

```text
model-runtime /metrics
  --> runtimeLatencyMs / runtimeThroughput
  --> runtime-router /metrics/profile-observations
  --> planner-controller runtimeProfileCorrection
```

The planner uses runtime-internal metrics for profile correction. Router
endpoint latency and network overhead remain visible for experiment reporting,
but they are not mixed into the model profile correction.

## Reconfiguration Path

```text
runtime-router demand metrics + cluster-state-manager state
  --> epoch-controller
  --> planner-controller
  --> planner-engine
  --> ActionPlan DAG
  --> transition-executor
```

State repair uses the same executable plan boundary, but bypasses the
optimization planner:

```text
node-agent / pods / runtime-router
  --> cluster-state-manager
  --> PhysicalGpuRegistry.status.health.requiredActions
  --> epoch-controller
  --> repair MigActionPlan
  --> transition-executor
```

For example, `clear_template_before_available` becomes an action DAG containing
`clear_template --> return_gpu`. The repair plan remains stored as a
`MigActionPlan`, with action results recorded in `status.actionStatuses`.

Scheduled or forecast-driven experiments use the same `ArrivalSnapshot` boundary.
The predictor is outside this system boundary; for experiments, a trace/stage
driver writes the predictor-equivalent request-rate windows into the
`arrival-trace-schedule` ConfigMap:

```text
trace / stage / external predictor output
  --> ConfigMap arrival-trace-schedule
  --> epoch-controller
  --> scheduled ArrivalSnapshot
  --> planner-controller
```

Each due stage creates exactly one `ArrivalSnapshot`, so replayed traces are
auditable in the Kubernetes API rather than being passed as hidden controller
state. See `manifests/experiments/arrival-trace-schedule-example.yaml` for the
experiment input shape.

Actuator calls:

```text
transition-executor
  --> miggeometry actuator
  --> mig-node-agent API
  --> node-local nvidia-smi MIG mutation

transition-executor
  --> podlifecycle actuator
  --> Kubernetes API
  --> exact slot resource request
  --> slot-device-plugin

transition-executor
  --> routerdrain actuator
  --> runtime-router control API
```

## Reconcile Triggering

Kubernetes object-driven paths use watch-triggered reconciliation with a 60s
resync fallback:

```text
ArrivalSnapshot / PhysicalGpuRegistry watch
  --> planner-controller

PhysicalGpuRegistry health watch
  --> epoch-controller repair reconciler

MigActionPlan watch
  --> transition-executor

Node / Pod watch
  --> cluster-state-manager

ConfigMap arrival-trace-schedule watch
  --> epoch-controller scheduled trace reconciler
```

The epoch-controller samples runtime-router HTTP metrics because demand is not a
Kubernetes object event.

`ArrivalSnapshot.spec.targetArrival` is the target request-rate vector for the
next allocation. `ArrivalSnapshot.spec.sourceArrival` is the current committed
request-rate vector at the start of the transition. The planner passes both to
stage3; the transition planner uses `min(sourceArrival, targetArrival)` as the
committed demand that must remain covered while moving between allocations.
For newer experiment manifests, the same target demand can also be expressed as
`spec.slo.<model>.demandRate`; `targetArrival` remains supported for older
manifests.

For fixed-rate experiments such as the 24h trace run, the request rate is an
experiment input, not something inferred from random arrivals. The runtime-router
still reports observed arrival rate for debugging, but marks demand-rate SLO as
`fixed_input_not_observed`. During transition execution, transition-executor
opens a router monitor window with `/control/monitor`; the router records
per-model latency samples and exposes transition latency SLO results at
`/metrics/slo`.

The fixed input traffic is generated outside the router. For local or in-cluster
experiments, run `tools/run_fixed_rate_router_traffic.py` against the same
arrival schedule used by the epoch-controller. The driver reads each stage's
`sourceArrival` and `targetArrival` vectors. While a transition is pending or the
router monitor is active, it sends `min(sourceArrival, targetArrival)`; after the
monitor finishes, it sends the stage's `targetArrival`. Fractional low request
rates carry over across stages so the long-run request count matches the
configured req/s.

Only the demand epoch timeline is compressed. For example, a 24h trace with one
new demand vector every 30 minutes can use `timeCompression: 6` or
`stageDurationSeconds: 300` so epochs arrive every 5 minutes. The req/s values
are not scaled, and transition execution itself is not compressed: drain,
MIG reconfiguration, runtime creation, readiness waits, and route sync all run
on real wall-clock time.

## Runtime Profile Correction

The profile catalog supplied at model registration remains the source of truth.
During serving, `cluster-state-manager` records runtime observations in
`PhysicalGpuRegistry.status.profileCalibrationOverlay` for compatibility with
older manifests. The planner-controller forwards those observations to
planner-engine as `runtimeProfileCorrection`.

The correction is conservative:

```text
effective throughput mu = min(original profile mu, observed mu)
effective latency       = max(original profile latency, observed latency)
```

This means runtime data can make a profile less optimistic, but it cannot improve
or overwrite the original offline profile. The planner trace records whether any
correction was available and how many profile options were affected.

## Execution Timing And Verification

`transition-executor` writes step timestamps and durations into
`MigActionPlan.status.transitionExecution`:

```text
executorStartedAt
  --> runtimePodsGoneAt
  --> clearFinishedAt
  --> slotsApplyFinishedAt
  --> runtimeDeploymentCreatedAt
  --> runtimeReadyAndCUDAVerifiedAt
  --> routeSyncedAt
  --> executorFinishedAt
```

Runtime Pods use a 1s readiness probe period. The executor also verifies the
assigned MIG UUID through runtime `/healthz` and confirms that node-agent reports
an active compute process for that MIG UUID before declaring the plan executed.

## Epoch Policy

The epoch-controller opens a new `ArrivalSnapshot` from runtime-router metrics
when any registered trigger fires:

```text
initial observed demand
  --> open an epoch

arrival-rate drift >= 30% from active epoch snapshot
  --> open an epoch, subject to minIntervalSeconds

runtime average latency > registered model SLO
  --> open an epoch, subject to minIntervalSeconds

runtime error rate > 0
  --> open an epoch, subject to minIntervalSeconds

runtime queued requests > 0
  --> open an epoch, subject to minIntervalSeconds

scheduled trace stage due
  --> open an epoch from arrival-trace-schedule
```
