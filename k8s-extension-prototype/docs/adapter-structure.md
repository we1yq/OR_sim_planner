# MIGRANT Adapter Structure

This document separates production execution components from dry-run previews and
smoke-test helpers.

## Production Runtime Components

These modules are intended to be part of the real controller or real actuator
path.

| Component | Path | Purpose |
| --- | --- | --- |
| MIG label executor | `controller/executors/mig_label_executor.py` | Applies `MigActionPlan.spec.executorPreview` by patching GPU Operator node labels such as `nvidia.com/mig.config`. |
| Pod lifecycle executor | `controller/executors/pod_lifecycle_executor.py` | Applies `MigActionPlan.spec.podLifecyclePreview` by creating/reusing workload Pods, patching live batch configuration, and deleting owned Pods when requested. |
| Router/drain executor | `controller/executors/router_drain_executor.py` | Applies `MigActionPlan.spec.trafficAndDrainPreview` by stopping new traffic on the source instance, rerouting queued/new traffic to a target endpoint, waiting for source inflight/queued work to drain, and recording `WorkloadRoutePlan` / `ServingInstanceDrain` status. |
| Physical GPU registry monitor | `controller/observe/physical_gpu_registry.py` | Maintains `PhysicalGpuRegistry/default`, including `availableQueue`, `activeQueue`, `transitioningQueue`, and stable `physicalGpuId -> gpuUuid` bindings. |
| Cluster observer | `controller/observe/cluster_observer.py` | Observes nodes, pods, GPU Operator inventory, physical GPU UUIDs, and MIG device UUIDs. |
| Observed logical layout mapper | `controller/observe/observed_layout.py` | Converts observed GPU/MIG UUID inventory into the stable MIGRANT layout contract: `physicalGpuId + slot/range + profile -> currentMigDeviceUuid`. |
| Pod assignment observer | `controller/observe/pod_assignment_observer.py` | Joins Pod labels/annotations, batch ConfigMaps, and logical MIG slots into `ObservedClusterState.spec.observedState.podAssignments`. |
| Observed state adapter | `controller/observe/observed_state_adapter.py` | Converts `ObservedClusterState` into `migrant_core.ClusterState` so real planning epochs can diff from actual cluster state. |
| Transition planner catalog | `migrant_core/transition_planners/catalog.py` | Lists canonical transition planners, compatibility aliases, roles, and runner functions so algorithm count is explicit even when variants share implementation modules. |
| Phased action DAG format | `migrant_core/transition_planners/action_plan_formats/phased_action_dag.py` | Compiles a linear action list into `migrant.phased-action-dag/v1` nodes, dependencies, phases, and resource-conflict summaries. This is a format/compiler helper, not a planner. |
| Phase-greedy transition planner | `migrant_core/transition_planners/phase_greedy.py` | Provides plain phase-greedy action planning and a `run_with_dag_output()` compatibility entry that attaches the phased action DAG representation for actuators and ablation. |

## Dry-Run Adapter Components

These components validate contracts and build preview artifacts. They should not
change real Pods, routing state, or MIG geometry.

| Adapter | Path | Purpose |
| --- | --- | --- |
| `DryRunMigGeometryAdapter` | `controller/adapters/contracts.py` | Summarizes node-label patches and MIG Manager configs that would be applied. |
| `DryRunRouterDrainAdapter` | `controller/adapters/contracts.py` | Summarizes route/drain actions such as stop accepting, reroute queued work, and start drain. |
| `DryRunPodLifecycleAdapter` | `controller/adapters/contracts.py` | Summarizes Pod lifecycle actions such as create/reuse, delete/recycle, and reload in place. |
| `DryRunObserverAdapter` | `controller/adapters/contracts.py` | Summarizes the observations required after real execution. |
| Router dry-run plan builder | `controller/adapters/router_adapter.py` | Builds preview-only `WorkloadRoutePlan` and `ServingInstanceDrain` resources. |
| Pod dry-run plan builder | `controller/adapters/pod_lifecycle_adapter.py` | Builds preview-only `PodLifecyclePlan` resources. |
| Observer dry-run builder | `controller/adapters/observer_adapter.py` | Builds preview-only observed-state skeletons. |

## Smoke-Test And Test Harness Components

These modules are for controlled tests only. They may create synthetic
`MigActionPlan` objects or short-lived test Pods, but they are not the production
planner output path.

| Component | Path | Purpose |
| --- | --- | --- |
| Workload lifecycle smoke action-plan builder | `controller/test_harness/workload_lifecycle_smoke.py` | Creates synthetic `MigActionPlan` resources with `createOrReuse` and `reloadInPlace` rows for single-A100 validation. |
| Router/drain smoke action-plan builder | `controller/test_harness/router_drain_smoke.py` | Creates synthetic `MigActionPlan` resources with stop-accepting-new, reroute, and drain rows for single-A100 validation. |
| Workload partition smoke test | `tools/smoke/workload_partition_smoke.py` | Standalone smoke tool for scheduling a Pod on a MIG resource and patching batch size. |
| CUDA profile test workload | `tools/test_workloads/cuda_profile_workload.cu` | Short CUDA program used to verify that a profile-backed workload option can run on a MIG device. |
| Router smoke workload | `tools/test_workloads/simple_router_workload.c` | Minimal HTTP workload/router used only for controlled router/drain adapter tests. |

## Naming Rule

- Files under `controller/executors/` are allowed to mutate real cluster state.
- Files under `controller/adapters/` are dry-run preview builders unless their
  name explicitly says otherwise.
- Files under `controller/test_harness/`, `tools/smoke/`, and
  `tools/test_workloads/` are test-only and should not be imported by the
  production reconcile loop.

## Current Adapter Inventory

Real execution adapters:

1. `mig_label_executor.py`
2. `executors/pod_lifecycle_executor.py`
3. `executors/router_drain_executor.py`

Supporting runtime mappers:

1. `observed_layout.py`
2. `pod_assignment_observer.py`
3. `observed_state_adapter.py`

Dry-run adapters:

1. `DryRunMigGeometryAdapter`
2. `DryRunRouterDrainAdapter`
3. `DryRunPodLifecycleAdapter`
4. `DryRunObserverAdapter`
5. `DryRunRouterPlanBuilder`
6. `DryRunPodLifecyclePlanBuilder`
7. `DryRunObservedStateBuilder`

Test harnesses:

1. `test_harness/workload_lifecycle_smoke.py`
2. `test_harness/router_drain_smoke.py`
3. `tools/smoke/workload_partition_smoke.py`
4. `tools/test_workloads/cuda_profile_workload.cu`
5. `tools/test_workloads/simple_router_workload.c`

## Router/Drain Scope

The real Router/Drain executor is endpoint-based, so the same control contract
works for routing between two MIG partitions on one A100, two GPUs in one node,
or GPUs on different worker nodes, as long as the serving endpoints are
reachable from the router runtime.

The current implementation completes the Kubernetes-native adapter contract:

- stop accepting new requests on the source instance through `/drain` or pod
  annotations;
- reroute traffic through a router HTTP endpoint or annotation-only mode;
- poll source instance `/metrics` or annotations until `inflight=0` and
  `queued=0`;
- record real execution status in `MigActionPlan`, `WorkloadRoutePlan`, and
  `ServingInstanceDrain`.

What remains outside MIGRANT until a production serving layer is integrated:

- a production queue/runtime API that owns real queued requests;
- production load-balancer or model-serving router configuration;
- runtime-native inflight metrics from the actual serving framework;
- cross-server network and failure-mode validation.

## Exact Layout Contract

MIGRANT's long-lived placement contract is logical, not UUID-first:

```text
physicalGpuId + slot/range + profile
```

For example:

```text
rtx1-worker-gpu0 slot 0-3 profile 3g
rtx1-worker-gpu0 slot 3-6 profile 3g
```

`controller/observe/observed_layout.py` resolves that logical placement to the current
runtime MIG UUID observed from GPU Operator `nvidia-smi -L` inventory. The UUID
is then used as execution evidence and verification material, not as the
planner's stable identity.

The real Pod lifecycle executor now verifies exact placement after Pod
creation/reuse:

1. read the expected logical placement from `expectedPlacement`, or from
   `physical_gpu_id + slot` in a `podLifecyclePreview` row;
2. resolve that logical slot through `PhysicalGpuRegistry/default`;
3. exec `nvidia-smi -L` inside the workload Pod;
4. compare the Pod's actual MIG UUID with the logical slot's current MIG UUID;
5. annotate the Pod with the expected and actual UUIDs;
6. fail the apply step if they differ.

This keeps the MILP/transition contract meaningful: the planner reasons about
stable physical GPU and slot identities, while the executor validates against
the actual runtime UUID that Kubernetes/NVIDIA assigned.

With the standard NVIDIA device plugin, Kubernetes still allocates a
profile-level MIG device. To make single-node experiments land on the requested
logical slot, the executor can create temporary reservation Pods for same-profile
non-target slots before creating the real workload Pod. After the real workload
passes UUID verification, those reservation Pods are deleted and the workload
keeps its assigned MIG device.

This reservation mechanism is an experiment-friendly bridge, not the preferred
production primitive. The production path should be an MIGRANT
scheduler/device-plugin/admission layer that reserves the exact logical slot
before the Pod is admitted.

## Actual Current State

Real planning epochs must start from `ObservedClusterState`, not from the
previous simulated stage. The observed state is assembled from:

- node and GPU Operator inventory;
- physical GPU identity from the registry/observer path;
- logical MIG slots resolved from the current template and `nvidia-smi -L`;
- Pod assignment rows joined by actual MIG UUID;
- batch size from Pod annotations or MIGRANT batch ConfigMaps;
- routing/drain annotations such as accepting-new, inflight, and queued.

The Pod assignment shape is:

```text
physicalGpuId
slot
profile
migDeviceUuid
workload
batch
podName
endpoint
ready
acceptingNew
inflight
queued
```

This is the state that the action planner should diff against the next MILP
target. Preserve decisions should compare stable logical identity and workload
payload:

```text
physicalGpuId + slot + profile + workload + batch
```

`migDeviceUuid` remains part of the actual state because it is needed to join
Pods back to logical slots and to verify execution, but it is not the long-lived
planner key.

## Queue Ownership

There are two different queue concepts:

- `PhysicalGpuRegistry.availableQueue`, `activeQueue`, and `transitioningQueue`
  are MIGRANT planner ownership queues. They are maintained by the registry
  monitor/controller from observed hardware state plus planner ownership
  actions.
- request queues and inflight serving requests belong to the serving
  router/runtime. MIGRANT observes or commands them through Router/Drain adapter
  APIs and records snapshots in `ObservedClusterState`, but MIGRANT should not be
  the production request queue itself unless a dedicated serving runtime is
  added.
