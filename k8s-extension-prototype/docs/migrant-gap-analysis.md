# MIGRANT Gap Analysis

This note audits the current prototype against the simulation notebook and
identifies what still needs to be added before the Kubernetes controller matches
the important simulation logic.

## What The Prototype Already Captures

- Workload-level requests exist as Kubernetes-style YAML files.
- Profile catalogs are extracted from the real `profile/*.csv` files.
- A100 MIG profile sizes and memory limits are represented.
- The 14 MILP abstract templates are represented.
- The 19 transition physical realizations are represented.
- Special transition rewrite candidates are represented for:
  - `3+3 -> 4+3`
  - `3+2+1 -> 4+2+1`
  - `3+1+1+1 -> 4+1+1+1`
- Controller output is dry-run only.

## Major Missing Pieces

### 1. Multi-Workload Scenario Input

The notebook's main experiment is not five independent single-workload plans.
It is a multi-workload arrival vector:

```text
llama, gpt2, vgg16, resnet50, vit_base
```

The current prototype has one `WorkloadRequest` per workload, but it lacks a
single scenario object that says:

```text
At this planning tick, these five workloads are active with these arrival rates.
```

Add:

```text
mock/scenarios/stage0.yaml
mock/scenarios/stage1-up.yaml
mock/scenarios/stage2-down.yaml
mock/scenarios/stage3-up.yaml
```

Each scenario should carry all workload demands together.

### 2. Stage-To-Stage Transition Input

The notebook plans transitions:

```text
source0 -> target0
target0 -> target1
target1 -> target2
target2 -> target3
```

Current prototype only plans from one current GPU state to one workload demand.
It does not yet have:

- source arrival vector,
- target arrival vector,
- previous target state,
- canonicalized executed state,
- stage name.

Add a `PlanningScenario` data model:

```yaml
name: stage1-up
sourceArrival:
  llama: 3
  gpt2: 20
  vgg16: 300
  resnet50: 300
  vit_base: 3000
targetArrival:
  llama: 3.45
  gpt2: 22.4
  vgg16: 324
  resnet50: 324
  vit_base: 9000
sourceStateRef: target0
```

### 3. Target Planner Is Still Too Simple

The notebook target planner flow is:

```text
feasible options
  -> MILP abstract capacity result
  -> instance demands
  -> candidate abstract template sets
  -> physical layout combinations
  -> workload slot assignment
  -> target ClusterState
```

The current prototype only chooses a best single profile option and counts
instances. It does not yet compute a cluster-level target state.

Add separate modules:

```text
abstract_capacity_planner.py
physical_layout_planner.py
slot_assignment_planner.py
target_state.py
```

### 4. MILP Interface Is Missing

The notebook's MILP result contains:

- selected template counts,
- selected workload/profile/batch instance counts,
- provided throughput,
- slack,
- remaining slots,
- effective options.

The prototype should not require Gurobi yet, but it needs an interface shaped
like the MILP output:

```text
CapacityPlan
  feasible
  gpuCount
  chosenTemplates
  instanceDemands
  providedByWorkload
  slackByWorkload
```

Then a greedy implementation can fill this interface first. Gurobi can be added
later behind the same interface.

### 5. Relaxed Cover / Upgrade Placement Is Missing

The notebook allows relaxed verification:

```text
a native 3g demand may be realized on a 4g slot
```

This is central to transition preservation. Current prototype assumes exact
profile placement.

Add:

- exact placement mode,
- upgrade-preserve placement mode,
- relaxed cover verification.

### 6. Runtime State Is Missing

The phase-greedy planner depends on full-plan candidate-generated candidate plan items. Those plan items
include runtime concepts:

- accepting new requests,
- queued tasks,
- inflight tasks,
- drain remaining,
- takeover candidate,
- blocked reason.

Current `gpu-states/*.yaml` only stores static MIG instances.

Add runtime fields:

```yaml
runtime:
  acceptingNew: true
  queued: 0
  inflight: 0
  drainRemaining: null
```

### 7. Physical GPU Identity Is Missing

The notebook distinguishes logical `gpu_id` from physical identity labels such
as `A`, `B`, `C`.

This matters for:

- free pool allocation,
- reconfiguration target side,
- old-side cleanup,
- preserving GPU identity across target rebuilds.

Add to GPU state:

```yaml
physicalGpuId: A
```

### 8. Candidate Action Generator Is Missing

The public planner should remain phase-greedy, but phase-greedy still needs candidate items:

```text
create_gpu
remove_gpu
reconfiguration
place_instance
workload_change
remove_instance
```

The notebook currently gets these from `plan_full_action_plan`. The prototype
should rename this layer:

```text
CandidateActionGenerator
```

It is an internal phase-greedy dependency, not a full-plan candidate public planner.

### 9. phase-greedy Scoring Is Incomplete

The notebook phase-greedy score uses:

- takeover readiness,
- capacity headroom,
- peak GPU delta,
- drain wait cost,
- unlock count,
- release GPU,
- target-backed enable,
- in-place bonus.

Current prototype does not compute these fields.

Add:

```text
phase_greedy_scoring.py
phase_greedy_grouping.py
```

### 10. Expected Simulation Fixtures Are Missing

The notebook has known mainline target results:

```text
stage0 GPU count: 6
stage1 GPU count: 9
stage2 GPU count: 6
stage3 GPU count: 8 in the sequential phase-greedy path
```

The prototype should store expected fixtures for regression tests:

```text
examples/expected/stage0-target-summary.yaml
examples/expected/stage1-target-summary.yaml
examples/expected/stage2-target-summary.yaml
examples/expected/stage3-target-summary.yaml
```

These should include:

- GPU count,
- selected abstract templates,
- selected physical templates,
- per-workload instance counts,
- relaxed-cover status.

### 11. Demo Scenarios Are Missing

The notebook includes focused demos:

- minimal drain demo,
- reconfiguration demo,
- mixed reconfiguration/create/workload-change demo,
- better-than-full-plan candidate remove/create demo.

These should become scenario fixtures later:

```text
mock/scenarios/drain-demo.yaml
mock/scenarios/reconfiguration-demo.yaml
mock/scenarios/mixed-demo.yaml
mock/scenarios/remove-create-demo.yaml
```

They are useful because they test behavior that the mainline stages may not
isolate clearly.

### 12. Metrics Are Missing

The notebook compares planners with:

- `reached_target`,
- `iteration_count`,
- `action_count`,
- `peak_active_gpu`,
- `final_active_gpu`,
- `elapsed_sec`,
- `rerouted_queued_tasks_total`,
- `drain_wait_total`,
- `max_simultaneous_draining_slots`.

The prototype should emit these metrics in dry-run output.

## Recommended Next Implementation Order

1. Add scenario YAML files for stage0 to stage3.
2. Add `mig_rules.py` and state validation.
3. Add a multi-workload `PlanningScenario` loader.
4. Replace single-workload target demand with cluster-level `CapacityPlan`.
5. Add physical layout expansion from `mig-rules/a100-40gb.yaml`.
6. Add target state materialization and relaxed-cover verification.
7. Add candidate action generation.
8. Add phase-greedy scoring/grouping.
9. Add regression fixtures for known notebook stage summaries.

## Safety Boundary

All of the above remains dry-run. Even after the prototype can reproduce
simulation-like action plans, it should still not:

- modify kube-scheduler,
- delete Pods,
- create real GPU workloads,
- change MIG configuration,
- invoke GPU Operator/MIG Manager,
- run destructive `nvidia-smi mig` commands.
