# Phase 3: Minimal Kubernetes Extension Architecture

This document defines the first real-system architecture for migrating the
simulation-stage MIG planning ideas into a Kubernetes controller/operator
prototype.

Current rule: the existing simulation notebook and profile CSV files are
read-only references. The prototype starts from mock data and dry-run plans.

## Goals

- Build a minimal Kubernetes-native prototype around the simulation ideas.
- Keep the first version safe: no real MIG reconfiguration, no scheduler
  modification, no workload deletion.
- Preserve only the V3 planning direction for action ordering.
- Separate external inputs from system-owned planning work.
- Make the architecture ready to later read real A100 MIG state.

## Non-Goals

- Do not modify `OR_sim_trasition_action.ipynb`.
- Do not directly port the notebook into production code.
- Do not change `kube-scheduler`.
- Do not execute real `nvidia-smi mig` reconfiguration commands.
- Do not install or operate GPU Operator in the local kind environment.
- Do not require a GPU on the local development machine.

## High-Level Component Diagram

```text
                       Kubernetes API
                             |
       +---------------------+----------------------+
       |                                            |
       v                                            v
 WorkloadRequest CR                         MockGpuState ConfigMap
 external input                             external/mock input
       |                                            |
       +---------------------+----------------------+
                             |
                             v
                  MIG Planner Controller
                             |
                             v
                  Input Normalizer / Validator
                             |
            +----------------+----------------+
            |                                 |
            v                                 v
     ProfileCatalog                    GpuMigStateAdapter
 external profile data                 mock now, real later
            |                                 |
            +----------------+----------------+
                             |
                             v
                  Feasible Option Builder
                             |
                             v
                   Target State Planner
                             |
                             v
                     V3 Action Planner
                             |
                             v
                   Dry-Run ActionPlan
                             |
                             v
             MigPlan status / logs / YAML output
```

## Data Flow

```text
1. User or test YAML creates WorkloadRequest.
2. MockGpuState describes the current GPU/MIG layout.
3. ProfileCatalog provides known model/profile/batch performance data.
4. Controller watches the Kubernetes API.
5. Controller normalizes all inputs into internal data models.
6. Feasible Option Builder computes valid workload/batch/profile options.
7. Target State Planner computes the desired MIG/workload placement.
8. V3 Action Planner compares current state and target state.
9. Controller writes a dry-run action plan to status/logs.
```

The first prototype should be deterministic and explainable. A user should be
able to read the input YAML and the generated plan and understand why the plan
was produced.

## External Inputs

These are facts or requests from outside the system. The controller may validate
and normalize them, but it should not invent them.

### WorkloadRequest

Represents desired workload demand.

Example fields:

```yaml
name: gpt2
model: gpt2
family: llm
arrivalRate: 20
slo:
  ttftMs: 100
  tpotMs: 50
allowedBatches: [1]
priority: normal
```

Simulation reference:

- `WORKLOAD_SPECS`
- `arrival_rate`
- LLM/CV SLO fields

### ProfileCatalog

Represents offline profiling knowledge.

Example fields:

```yaml
options:
  - workload: gpt2
    batch: 1
    profile: 1g
    mu: 1.13
    fit: true
    peakMemMb: 2100
```

Simulation reference:

- `profile/*.csv`
- `normalize_mig_name`
- `build_workload_batch_profile_tensors`
- `build_option_table`
- `feasible_option_df`

The controller should not run benchmarks in the first prototype.

### Current GpuMigState

Represents the current observed GPU/MIG state.

Mock-stage example:

```yaml
gpus:
  - gpuId: 0
    source: mock
    migEnabled: true
    instances:
      - start: 0
        end: 1
        profile: 1g
        workload: gpt2
      - start: 1
        end: 7
        profile: void
        workload: null
```

Simulation reference:

- `ClusterState`
- `GPUState`
- `MigInstance`

Future real sources:

- `nvidia-smi -L`
- `nvidia-smi mig -lgi`
- `nvidia-smi mig -lci`
- NVML
- NVIDIA device plugin resources
- GPU Operator node labels
- DCGM exporter metrics

### Policy

Represents administrative constraints.

Example fields:

```yaml
dryRun: true
allowMigReconfiguration: false
maxGpuCount: 1
planner: v3
```

In the first prototype, `dryRun` must stay true.

## System-Owned Work

These are responsibilities of the new controller/operator system.

### Input Normalizer / Validator

Responsibilities:

- Read `WorkloadRequest`.
- Read mock or real `GpuMigState`.
- Read `ProfileCatalog`.
- Validate required fields.
- Convert inputs into internal planner models.
- Reject unsafe settings in early phases.

### GpuMigStateAdapter

Responsibilities:

- Produce a normalized `GpuMigState`.
- Use mock YAML/ConfigMap in the local kind prototype.
- Later read real A100 state through read-only NVIDIA/Kubernetes sources.
- Avoid planning decisions.
- Avoid executing reconfiguration.

The adapter is intentionally thin. NVIDIA components should provide low-level
GPU discovery; this adapter translates those facts into planner-ready state.

### Feasible Option Builder

Responsibilities:

- Compute feasible `(workload, batch, profile)` options.
- Enforce memory and SLO fit.
- Compute or load `mu` values.

Simulation concepts to preserve:

- batch candidates
- normalized MIG profile names
- `fit_mem`
- `fit_slo`
- `fit`
- `mu_req_per_s`

### Target State Planner

Responsibilities:

- Convert workload demand and feasible options into desired state.
- Decide target MIG templates and workload placements.
- Produce `TargetState`.

Simulation concepts to preserve:

- profile order: `7g`, `4g`, `3g`, `2g`, `1g`
- GPU template capacity
- target `ClusterState`
- target verification rules

Prototype simplification:

- The first implementation may use a greedy planner.
- The interface should allow replacing it with MILP later.
- Do not require Gurobi in the first controller image.

### V3 Action Planner

Responsibilities:

- Compare current state and target state.
- Generate candidate transition actions.
- Score and choose action groups using V3 ordering logic.
- Produce a dry-run action plan.

Important design decision:

```text
The prototype exposes only V3 planner behavior.
V1/V2 planner variants are not part of the system surface.
```

Notebook V3 currently reuses V2 candidate-generation code internally. The
prototype should rename and isolate this as:

```text
CandidateActionGenerator
V3Scorer
V3Selector
V3Planner
```

So the public planner remains V3-only.

### Dry-Run ActionPlan Writer

Responsibilities:

- Write generated plan to logs or Kubernetes status.
- Mark all plans as dry-run.
- Include risk markers for actions that would be destructive in the future.
- Avoid executing Kubernetes Pod deletion or MIG commands.

Example:

```yaml
dryRun: true
planner: v3
actions:
  - type: drain_old
    gpuId: 0
    slot: [0, 1]
    workload: gpt2
    risk: future-destructive
  - type: reconfigure
    gpuId: 0
    fromTemplate: 1+1+1+1+1+1+1
    toTemplate: 4+3
    risk: future-destructive
```

## Kubernetes Mapping

### Controller

The first controller watches prototype resources and produces plans.

Responsibilities:

- Watch `WorkloadRequest` objects.
- Read mock GPU state.
- Invoke planner pipeline.
- Write dry-run plan output.

### CRDs

Likely first CRDs:

```text
WorkloadRequest
MigPlan
```

Possible later CRDs:

```text
GpuMigState
ProfileCatalog
MigPolicy
```

For the first version, `GpuMigState` and `ProfileCatalog` can be ConfigMaps or
plain YAML files to keep the system small.

### Scheduler Logic

In the prototype, scheduling logic lives inside the controller as planning
logic. It does not replace or modify `kube-scheduler`.

The controller computes:

- desired MIG layout
- desired workload/profile assignment
- dry-run transition steps

Kubernetes still schedules ordinary Pods using the default scheduler.

### GPU/MIG State Monitor

In local kind:

```text
MockGpuState ConfigMap/YAML -> GpuMigStateAdapter
```

On A100 dry-run server:

```text
nvidia-smi/NVML/device-plugin facts -> GpuMigStateAdapter
```

Later production:

```text
GPU Operator + device plugin + DCGM exporter -> GpuMigStateAdapter
```

### Action Executor

In the first prototype:

```text
No executor, dry-run only.
```

Later:

```text
ActionPlan -> Safety checks -> Kubernetes API / MIG Manager / executor
```

Dangerous future actions include:

- draining workload traffic
- deleting or recreating Pods
- changing node labels that trigger MIG Manager
- creating or deleting MIG instances
- restarting GPU workloads

These must remain disabled until explicit future phases.

## Mock vs Future A100 Boundary

| Area | Local Prototype | Future A100 Dry-Run | Future Execution |
| --- | --- | --- | --- |
| GPU state | Mock YAML/ConfigMap | Read-only `nvidia-smi`/NVML | Same plus executor feedback |
| Workload requests | YAML/CRD | YAML/CRD | Real CRD/API |
| Profile data | Static mock/profile CSV-derived data | Static profile catalog | Profile database |
| Target planner | Greedy/simple first | MILP-capable interface | Production planner |
| Action planner | V3 dry-run | V3 dry-run | V3 gated execution |
| MIG changes | None | None | Via MIG Manager or controlled executor |
| Scheduler | Default scheduler | Default scheduler | Possible scheduler integration later |

## Simulation Concepts to Migrate Later

Migrate as data structures or algorithms:

- `MigInstance`
- `GPUState`
- `ClusterState`
- `PROFILE_ORDER`
- `PROFILE_SIZE`
- legal MIG templates
- workload/profile/batch feasible option logic
- target state representation
- action plan representation
- V3 action group scoring and selection

Do not migrate directly:

- notebook cells
- print/report/demo functions
- V1/V2 comparison code
- hard-coded stage experiments
- global-variable-heavy execution flow
- Colab/Jupyter outputs
- simulation-only mutation helpers that hide real-world safety concerns

## Minimal Prototype Architecture

```text
k8s-extension-prototype/
  docs/
    phase3-architecture.md
  manifests/
    namespace.yaml
    workloadrequest-crd.yaml
    examples/
      workloadrequest-gpt2.yaml
      mock-gpu-state.yaml
  controller/
    main.py
    models.py
    state_adapter.py
    feasible_options.py
    mig_rules.py
    scenario_loader.py
  mock/
    profile_catalog.yaml
    gpu_state.yaml
  examples/
    dry-run-plan.yaml
```

This structure will be created and filled incrementally in later phases.

## Phase 3 Decision Summary

- Use a controller/operator path first.
- Do not modify kube-scheduler.
- Use mock GPU state locally.
- Keep real A100 integration read-only at first.
- Keep all actions dry-run.
- Expose only V3 planner behavior.
- Treat NVIDIA GPU Operator/device plugin/DCGM as future data sources or
  execution helpers, not as things to rewrite.
