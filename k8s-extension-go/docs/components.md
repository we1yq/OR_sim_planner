# Go System Components

This directory is the production-oriented Go implementation track. It contains
only the deployed control-plane, node-agent, device-plugin, router, and runtime
paths used by the real cluster. A component is listed here only when it has
executable Go code and a Kubernetes deployment path in this tree.

## Fixed Pods

Deployments:

- `epoch-controller`: observes runtime-router demand metrics and creates
  `ArrivalSnapshot` objects when registered epoch triggers fire.
- `cluster-state-manager`: observes Kubernetes node/GPU labels and publishes the
  `PhysicalGpuRegistry` status used by the Go control plane.
- `planner-controller`: turns an `ArrivalSnapshot` and the current
  `PhysicalGpuRegistry` into a planner request, calls `planner-engine`, and
  emits `MigActionPlan` objects.
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
  exposes HTTP inference endpoints and starts a CUDA worker so Kubernetes
  placement can be verified with real GPU processes.

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

## Reconfiguration Path

```text
runtime-router demand metrics + cluster-state-manager state
  --> epoch-controller
  --> planner-controller
  --> planner-engine
  --> ActionPlan DAG
  --> transition-executor
```

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

MigActionPlan watch
  --> transition-executor

Node / Pod watch
  --> cluster-state-manager
```

The epoch-controller samples runtime-router HTTP metrics because demand is not a
Kubernetes object event.

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
```
