# MIGRANT Runtime Control Loop

This document describes the runtime path for a real cluster. It excludes
multi-GPU and multi-node hardware validation, which still needs additional
hardware.

## Loop

```text
arrival rates
  -> observe actual cluster
  -> convert ObservedClusterState to ClusterState
  -> run MILP / target builder
  -> diff actual source to target layout through transition planner
  -> compile actions into phased/DAG execution view when requested
  -> apply MIG geometry actions
  -> observe refreshed MIG UUIDs
  -> apply Pod lifecycle and exact placement verification
  -> apply Router/Drain actions
  -> observe final actual state
  -> mark success or repair
```

## Actual Source State

Real planning should use:

```yaml
spec:
  observedStateRef: cluster-observed-state
  arrivalSnapshotRef: arrival-t00030
```

or:

```yaml
spec:
  sourceStateRef: observed
```

`controller/planning/reconciler.py` loads that `ObservedClusterState` and converts it
with `controller/observe/observed_state_adapter.py`.

`ArrivalSnapshot` is the preferred traffic input CRD. Legacy
`arrivalSnapshotConfigMap` remains supported for old tests, but new experiments
should create one `ArrivalSnapshot` per control window.

The resulting `migrant_core.ClusterState` preserves the algorithm-facing
layout:

```text
gpuId
physicalGpuId
slot start/end/profile
workload
batch
```

Runtime-only details are retained in metadata:

```text
migDeviceUuid
gpuUuid
podName
endpoint
ready
acceptingNew
inflight
queued
```

The planner should make preserve decisions from stable logical identity:

```text
physicalGpuId + slot + profile + workload + batch
```

MIG UUIDs are current runtime evidence and execution verification material.

## Action Planner Modes

The source-of-truth planner list is
`migrant_core.transition_planners.PLANNER_CATALOG`. It separates canonical
planner names from backward-compatible aliases and labels each planner as
production, compatibility-output, ablation-baseline, or experimental.

MIGRANT keeps the old phase-greedy transition planner for compatibility and ablation:

```yaml
transition:
  transitionPlanner: phase_greedy
```

The new phased/DAG mode is selected per scenario:

```yaml
transition:
  transitionPlanner: phase_greedy_with_dag_output
```

`phase_greedy_with_dag_output` does not change the old phase-greedy execution
semantics. It runs the same phase-greedy action planner, then compiles the chosen linear actions into
`migrant.phased-action-dag/v1`, including nodes, dependency edges, roots,
resource conflicts, and phase summaries. This gives the real actuator a
parallelizable execution contract while preserving `phase_greedy` as the
baseline for ablation experiments. `phase_greedy_dag` remains accepted as a
backward-compatible alias.

The experimental planner is selected with:

```yaml
transition:
  transitionPlanner: cost_aware_dag
```

`basic_dag` is the baseline DAG compiler. It builds a final dependency DAG
from source/target layout differences and does not perform iterative prefix
execution. `cost_aware_dag` keeps the same final-DAG execution contract, but
scores candidate transition modes before lowering them into fine-grained
actions. Its current score first filters service-infeasible candidates, then
minimizes peak active physical GPUs, queued/drain work, MIG benchmark
reconfiguration time, and disruptive operations such as reroute, bridge, and
pod deletion.

## Who Reads UUIDs After Template Changes?

The observer path owns this:

```text
MIG Adapter patches GPU Operator labels
  -> MIG Manager reaches success
  -> ClusterObserver reads nvidia-smi -L
  -> observed_layout maps logical slots to current MIG UUIDs
  -> PhysicalGpuRegistry stores logicalMigSlots
```

The planner never guesses UUIDs.

## Pod Assignment Observer

`controller/observe/pod_assignment_observer.py` joins:

- Pod labels and annotations;
- Pod batch ConfigMaps;
- logical MIG slots;
- optional runtime metrics endpoint.

It writes:

```text
ObservedClusterState.spec.observedState.podAssignments
ObservedClusterState.spec.observedState.unassignedGpuPods
ObservedClusterState.spec.observedState.inflightByInstance
ObservedClusterState.spec.observedState.queuedByWorkload
```

The default observation path does not exec arbitrary workload Pods. Pod
assignment uses annotations written by the Pod lifecycle executor or by a future
runtime/admission sidecar. Runtime metrics are opt-in through:

```text
mig.or-sim.io/observe-metrics=true
mig.or-sim.io/metrics-endpoint=http://service:8080/metrics
```

or through `mig.or-sim.io/endpoint`, in which case `/metrics` is appended.

## Queue Ownership

MIGRANT owns GPU planning queues:

```text
PhysicalGpuRegistry.availableQueue
PhysicalGpuRegistry.activeQueue
PhysicalGpuRegistry.transitioningQueue
```

The serving runtime owns request queues and inflight requests. MIGRANT observes
or commands those through Router/Drain APIs.

## Current Production Gap

The current exact-placement implementation can verify the assigned MIG UUID and
uses temporary reservation Pods as an experiment-friendly bridge. Production
should replace that with one of:

- scheduler plugin;
- admission webhook;
- MIGRANT-aware device plugin;
- serving platform integration that can reserve logical slots before Pod start.

## Single-A100 Test Status

Validated on the current edge cluster:

- observer/registry sync;
- logical slot to MIG UUID mapping;
- Pod exact placement verification;
- reservation bridge for same-profile slots;
- batch reload without Pod recreation;
- router/drain endpoint switching between two MIG-backed Pods.

Not hardware-validated yet:

- multi-GPU action plans;
- multi-node routing;
- cross-server failure and rollback behavior.
