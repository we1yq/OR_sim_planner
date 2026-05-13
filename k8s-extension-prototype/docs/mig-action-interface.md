# MIG Planner Interface Contract

This document freezes the current single-cluster planner/action interface before
we extend the prototype to multiple GPU servers.

The goal is to keep module boundaries stable:

1. placement planner chooses abstract templates and serving options.
2. target builder materializes those templates into logical GPU/slot state.
3. transition planner converts source/target states into staged actions.
4. reconciler packages the plan into Kubernetes custom resources.
5. actuator/adapters translate approved action plans into executor-specific work.
6. observer confirms real post-action state before canonicalization.

## Terminology

| Term | Meaning now | Multi-server meaning |
| --- | --- | --- |
| logical GPU id | Planner-local `gpu_id` in `ClusterState.gpus[].gpu_id`. Stable only inside a planned state. | Still planner-local. Must not be used as a node/device identity. |
| physical GPU id | Simulation identity stored in `ClusterState.metadata.physical_id_map`, e.g. `A`, `B`. | Stable system alias for a real A100, e.g. `rtx1-gpu0`, permanently checked against `gpuUuid`. |
| node name | Optional binding in executor preview. Missing in simulation/dry-run until observer supplies it. | Required for real GPU Operator execution. |
| device index | GPU index on a Kubernetes node. Optional in simulation/dry-run. | Required for real MIG Manager config generation. |
| MIG slot | Tuple/list `[start, end, profile]`, where A100-40GB has 7 slices. | Same shape, but interpreted per physical GPU. |
| template | Abstract MIG profile layout such as `4+3`, `3+2+1+1`, `1+1+...`. | Same. Adapter maps it to GPU Operator config names. |
| serving instance | A MIG slot plus workload payload: `workload`, `batch`, `mu`. | Same, plus pod/router identity from observer. |

## Physical GPU Queues

Real execution uses `PhysicalGpuRegistry/default` as the hardware queue source.

| Queue | Owner | Meaning |
| --- | --- | --- |
| `discoveredA100` | observer | A100 devices currently visible from GPU Operator inventory. |
| `availableQueue` | monitor | Clean A100 devices with `or-sim-empty=success`, no MIG devices, and not claimed by the planner. |
| `transitioningQueue` | monitor/action executor | A100 devices that need cleanup or reconfiguration before availability. |
| `activeQueue` | planner | Physical GPUs currently claimed by the global canonical queue. |

The planner chooses from `availableQueue` when it needs a new physical GPU.
After the choice is accepted, the registry marks that physical GPU active and
the target builder maps a logical `gpu_id` to its `physicalGpuId`. Releasing a
GPU is also a planner action: if the GPU still has a template, the executor must
clear it first, then the monitor can place it back in `availableQueue`.
The empty state is `or-sim-empty`: MIG mode remains enabled, and
`mig-devices` is `{}`. Do not use `all-disabled` as the empty state because it
turns MIG mode off and can leave the node in `Disabled*`/failed reset state.

## Module Outputs

### 1. Placement Planner

Input:

- feasible option dataframe from workload/profile catalogs.
- target arrival vector.
- optional previous state for current/enhanced planner.

Output shape:

```yaml
feasible: true
status: OK
planner_module: placement.milp_enhanced
gpu_count: 9
chosen_templates:
  - 4+3
  - 1+1+1+1+1+1+1
K_total:
  7g: 0
  4g: 1
  3g: 1
  2g: 0
  1g: 7
x_sol:
  "12": 1
y_sol:
  "3": 2
alloc:
  - workload: llama
    arrival: 3.0
    provided: 3.1
    options: []
elapsed: 0.12
```

Stable rules:

- Placement is abstract. It does not choose Kubernetes nodes.
- `chosen_templates` are logical GPU templates, not physical device commands.
- `gpu_count` is a planning count, not a cluster capacity assertion.
- Baseline placement planners must not call or reuse current planner output.

### 2. Target Builder

Input:

- placement result.
- previous `ClusterState`.
- feasible options and workload order.

Output: `ClusterState`.

```yaml
metadata:
  physical_id_map:
    0: A
    1: B
  build_metrics:
    ordered_physical_templates:
      - 4+3
      - 1+1+1+1+1+1+1
    exact_preserve: 4
    upgrade_preserve: 1
gpus:
  - gpu_id: 0
    source: real
    instances:
      - start: 0
        end: 4
        profile: 4g
        workload: llama
        batch: 1
        mu: 0.765989
        preserved: true
```

Stable rules:

- `gpu_id` is logical and may be re-canonicalized between rounds.
- `metadata.physical_id_map[logical_gpu_id]` is the only supported link from
  logical GPU to physical identity.
- The target builder may preserve previous physical ids, but it must not invent
  Kubernetes `nodeName`/`deviceIndex` bindings.
- `build_metrics` are analysis/debug metrics, not actuator inputs.

### 3. Transition Planner

Input:

- source `ClusterState`.
- target `ClusterState`.
- source/target arrivals.

Output:

```yaml
reached_target: true
iteration_count: 3
executed_actions:
  - type: allocate_gpu
    physical_gpu_id: C
  - type: configure_full_template
    physical_gpu_id: C
    template: 4+3
  - type: place_target_layout
    gpu_id: 2
    physical_gpu_id: C
  - type: bind_target_gpu
    gpu_id: 2
    physical_gpu_id: C
  - type: stop_accepting_new
    gpu_id: 0
    physical_gpu_id: A
    slot: [0, 4, 4g]
    workload: llama
  - type: mark_draining_instance
    gpu_id: 0
    physical_gpu_id: A
    slot: [0, 4, 4g]
    workload: llama
    rounds: 1
planned_state: {}
executed_state: {}
final_plan:
  plan_items:
    - id: RECONF_gpu0
      type: reconfiguration
      current_phase: target_side_prepared
      status: blocked
      blocked_by: waiting_for_old_side_drain
```

Fine action types are grouped as follows.

| Group | Action types | Meaning |
| --- | --- | --- |
| MIG geometry | `allocate_gpu`, `configure_full_template`, `place_target_layout`, `clear_gpu`, `clear_template` | Candidate inputs to GPU Operator/MIG Manager. |
| Internal binding | `bind_target_gpu`, `mark_reconfig_target_prepared` | Controller bookkeeping only. Never send directly to MIG Manager. |
| Router/drain | `stop_accepting_new`, `reroute_queued_tasks`, `mark_draining_instance` | Traffic and drain gates before pod deletion or MIG clearing. |
| Pod/serving | `place_instance`, `bridge_place_instance`, `remove_instance`, `workload_change`, `update_batch` | Serving capacity and pod lifecycle intent. |
| Deferred gates | `defer_remove_gpu`, `defer_remove_instance`, `defer_workload_change` | Not executable; explain why the abstract action is blocked. |

Stable rules:

- Transition output may be iterative. `iteration_count` is part of the schedule,
  not the number of hardware operations.
- `executed_actions` are fine-grained planner actions. They are not all hardware
  commands.
- Hardware-facing adapters must filter by action group.
- `final_plan.plan_items` is the preferred structure for explaining blocking,
  drain, and takeover state.
- `clear_gpu` / `clear_template` must apply `or-sim-empty` before a physical
  GPU leaves the planner-owned `activeQueue` and returns to `availableQueue`.

## Kubernetes Resources

### MigPlan

`MigPlan` is the planner result carrier.

Important status fields:

- `status.actions`: fine action list from transition planner.
- `status.metrics`: aggregate metrics such as action count and GPU count.
- `status.planningTrace`: structured debug trace.
- `status.targetState`: materialized target `ClusterState`.
- `status.executedState`: simulated post-action state.
- `status.canonicalNextState`: canonicalized state for the next planning round.
- `status.fullPlanConfigMap`: ConfigMap with full debug `status.yaml`.
- `status.canonicalNextStateConfigMap`: ConfigMap with canonical state YAML.
- `status.actionPlanRef`: created `MigActionPlan`.

### MigActionPlan

`MigActionPlan` is the actuator-facing resource.

Important spec fields:

```yaml
spec:
  dryRun: true
  executor: nvidia-gpu-operator
  phaseGate: PendingApproval
  fullPlanConfigMap: stage0-full-plan
  canonicalNextStateConfigMap: target0-state
  actionCount: 42
  actionCountsByType: {}
  chosenTemplates: []
  targetGpuCount: 9
  executorPreview: {}
  migGeometryPreview: {}
  trafficAndDrainPreview: {}
  podLifecyclePreview: {}
  abstractActionPreview: {}
  adapterDryRunPreview: {}
  observerPreview: {}
```

Stable rules:

- `MigActionPlan` is the boundary between planning and actuation.
- Dry-run actuator requires every preview object to have `previewOnly: true`.
- Real actuator must reject unresolved physical bindings.
- `actionCount` must match `fullPlanConfigMap.data.status.yaml.actions`.

## Preview Interfaces

### executorPreview

Purpose: summarize what the NVIDIA GPU Operator executor would need.

Important fields:

- `executor: nvidia-gpu-operator`
- `targetApi: NVIDIA GPU Operator MIG Manager`
- `gpuOperatorLabel: nvidia.com/mig.config`
- `gpuTargets[]`
- `migManagerTargetConfigs[]`
- `wouldPatchNodeLabels`
- `unresolvedPhysicalGpuIds`

`gpuTargets[]` shape:

```yaml
- logicalGpuId: 0
  physicalGpuId: A
  nodeName: rtx1
  deviceIndex: 0
  targetTemplate: 4+3
  targetInstances:
    - start: 0
      end: 4
      profile: 4g
      workload: llama
      batch: 1
```

Rules:

- If any `physicalGpuId` lacks `nodeName` or `deviceIndex`, it must appear in
  `unresolvedPhysicalGpuIds`.
- `wouldPatchNodeLabels` must be empty when `unresolvedPhysicalGpuIds` is not
  empty.
- Real execution patches node labels only after observer binding is complete.

### migGeometryPreview

Purpose: geometry subset of the plan.

Important fields:

- `geometryActions`: only MIG geometry action briefs.
- `migManagerTargetConfigs`: desired MIG Manager configs.
- `wouldPatchNodeLabels`: node label patch preview.
- `internalStateActionsExcluded`: binding/bookkeeping actions that are not sent
  to MIG Manager.

### trafficAndDrainPreview

Purpose: router/drain subset of the plan.

Important fields:

- `planItems`: current phase and blocker per abstract root/slot.
- `trafficActions`: `stop_accepting_new`, `reroute_queued_tasks`,
  `mark_draining_instance`, and defer actions.

### podLifecyclePreview

Purpose: pod/serving lifecycle intent.

Sections:

- `createOrReuse`
- `drain`
- `deleteOrRecycle`
- `reloadInPlace`

Rules:

- Pod deletion/recycle must happen only after router cutover and inflight drain.
- `update_batch` prefers reload-in-place.
- MIG geometry-only actions must not create pods.

### observerPreview

Purpose: declares observations required before real canonicalization.

Required groups:

- MIG: `physicalGpuId`, `nodeName`, `deviceIndex`,
  `observedMigInstances`, `gpuOperatorMigConfigState`.
- Router: `acceptingNewByInstance`, `queuedByWorkload`, `rerouteTargets`.
- Pod: `podReadiness`, `podToMigInstanceAssignment`,
  `servingInstanceId`, `inflightByInstance`.

Canonicalization rule:

```text
After real execution, canonicalize only observed post-action GPU/MIG/Pod/router state.
```

## Real Multi-Server Extension Points

Before connecting multiple servers, the observer must populate physical bindings:

```yaml
spec:
  observedState:
    physicalGpuBindings:
      rtx1-gpu0:
        physicalGpuId: rtx1-gpu0
        nodeName: rtx1
        deviceIndex: 0
        gpuUuid: GPU-...
        product: NVIDIA A100-PCIE-40GB
        migCapable: true
        migDevices:
          - profile: 2g.10gb
            migDeviceUuid: MIG-...
    ignoredGpuDevices:
      - nodeName: rtx1
        deviceIndex: 1
        product: NVIDIA TITAN RTX
        reason: non-A100 GPU ignored by MIGRANT MIG planner
```

Planner state may carry the same binding under metadata for executor preview:

```yaml
metadata:
  physicalGpuBindings:
    rtx1-gpu0:
      physicalGpuId: rtx1-gpu0
      nodeName: rtx1
      deviceIndex: 0
      gpuUuid: GPU-...
      product: NVIDIA A100-PCIE-40GB
```

Rules:

- The planner may continue using logical `gpu_id`.
- The target builder may continue preserving `physicalGpuId`.
- The executor adapter must use `nodeName` + `deviceIndex` + `targetTemplate`.
- The real actuator must never infer node/device from logical `gpu_id`.
- `physicalGpuId` is a stable system alias bound to one real A100 GPU UUID.
- Non-A100 GPUs observed on shared lab servers are ignored by planner inputs.
- Canonical `gpu_id` is the position in the planner's global GPU queue and may
  change after canonicalization; `physicalGpuId` and `gpuUuid` must not.

## Canonical GPU Queue

The planner sees a global queue of logical GPU ids:

```yaml
metadata:
  physical_id_map:
    "0": rtx1-gpu0
    "1": rtx2-gpu0
```

Rules:

- The queue is global across all GPU servers, not per node.
- `gpu_id` is just the current queue position.
- Canonicalization compresses and reorders logical ids after each observed
  execution result.
- There is no hardware priority hidden in canonicalization. A newly available
  A100 receives a logical id when the system first allocates it into the active
  planner queue; later canonicalization only makes the queue compact and stable.
- The binding table keeps the durable identity:
  `gpu_id -> physicalGpuId -> gpuUuid/nodeName/deviceIndex`.

## Do Not Break These Invariants

1. Logical GPU id is not a Kubernetes node/device identity.
2. Physical GPU id is the bridge between planner and observer.
3. Missing physical binding means preview only; no real node label patch.
4. MIG geometry actions and internal binding actions are different.
5. Target state is planned state; canonical next state after real execution must
   come from observed state.
6. Baseline modules must be independent and must not consume current planner
   output unless explicitly testing a downstream module.
7. Reports may estimate hardware time, but actuators must treat real GPU
   Operator status as the source of truth.

## Next Work Items

- Extend `ObservedClusterState` builder to emit physical GPU bindings.
- Add a validation check that real `MigActionPlan` execution is blocked when
  `executorPreview.unresolvedPhysicalGpuIds` is non-empty.
- Add node/device fields to test fixtures for a multi-server dry-run.
- Keep dry-run and real-actuator inputs identical; differ only by execution
  permissions and observed-state confirmation.
