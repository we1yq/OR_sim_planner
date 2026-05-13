# Notebook Algorithm Summary

This document records the source-of-truth algorithm in
`OR_sim_trasition_action.ipynb`. The Kubernetes extension should preserve this
logic and wrap it with Kubernetes input/output adapters. It should not replace
the planner with a greedy or approximate implementation.

## Scope

Only phase-greedy is carried forward as the planner path. legacy and full-plan candidate are useful notebook
history, but they are not part of the Kubernetes prototype target behavior.

The extension should reproduce the notebook experiment:

```text
source0(empty) -> target0 -> executed0 -> canonical source1
source1 -> target1 -> executed1 -> canonical source2
source2 -> target2 -> executed2 -> canonical source3
source3 -> target3 -> executed3
```

## Stage Arrivals

Workload order:

```text
llama, gpt2, vgg16, resnet50, vit_base
```

Baseline stage0 arrivals:

```text
llama: 3
gpt2: 20
vgg16: 300
resnet50: 300
vit_base: 3000
```

Stage1 scales the baseline:

```text
llama *= 1.15
gpt2 *= 1.12
vgg16 *= 1.08
resnet50 *= 1.08
vit_base *= 3
```

Stage2 scales stage1:

```text
llama *= 0.72
gpt2 *= 0.78
vgg16 *= 0.3
resnet50 *= 0.3
vit_base *= 0.3
```

Stage3 applies the stage1 up multipliers to stage2.

## MILP Target Planner

The target planner is the notebook's real Gurobi MILP:

```text
solve_milp_gurobi_batch_unified(...)
```

It jointly optimizes:

- `y[t]`: integer count of each abstract MIG template.
- `x[r]`: integer count of each feasible workload option.
- `cap[p]`: total capacity for each MIG profile.
- `provided[i]`: throughput provided to each workload.
- `slack[i]`: throughput above the arrival requirement.
- `remaining[p]`: unused profile capacity.

The core constraints are:

- `total_gpu == sum(y[t])`
- `cap[p] == sum(template_K[t][p] * y[t])`
- `provided[i] == sum(mu[r] * x[r])` for workload `i`
- `provided[i] >= arrival_rate[i]`
- `sum(x[r] for options using profile p) <= cap[p]`

The objective is multi-objective:

1. Minimize total GPU count.
2. Maximize elastic slack from future batch-up opportunities.
3. Maximize remaining slots/capacity.

This means a workload may use multiple `(batch, profile)` options in the same
stage. The Kubernetes prototype must preserve that behavior.

Known mainline MILP GPU counts:

```text
stage0: 6
stage1: 9
stage2: 6
stage3: 8
```

## Target Materialization

The MILP result is not the final physical target layout. The notebook then calls:

```text
build_target_state_from_milp(...)
```

This step:

- extracts abstract templates and instance demands from MILP output,
- enumerates candidate abstract template sets,
- augments candidates with transition-preserving alternatives,
- orders templates onto concrete GPU IDs,
- enumerates physical layouts for each GPU,
- assigns workload demands to physical slots,
- prefers preserving previous GPU/template/slot/workload/profile/batch when
  `prev_state` exists,
- rematches target GPU IDs to old GPU identities,
- applies logical-template ordering fixes.

Therefore the extension must distinguish:

```text
MILP abstract capacity result
materialized physical target state
```

## Canonical GPU IDs

Every stage-to-stage transition uses canonical GPU identity handling before the
next stage is planned. The executed state is not blindly reused.

The notebook keeps:

- logical `gpu_id`,
- physical GPU IDs such as `A`, `B`, `C`,
- `physical_id_map`,
- `display_id_map`.

Canonicalization/rematching keeps stage comparisons stable and allows
`build_target_state_from_milp(..., prev_state=sourceX)` to preserve the right
physical GPUs and slots.

Important functions/behaviors include:

- `_reassign_gpu_ids_by_matching(...)`
- `_apply_same_logical_template_order_fix(...)`
- `canonicalize_bind` actions
- physical ID map updates

The Kubernetes prototype must keep this state model in the dry-run layer.

## Physical GPU Free Pool

The notebook manages physical GPU IDs with a free pool. Released physical GPUs
can be reused. Allocation uses LIFO:

```text
policy: free_pool_lifo
```

This appears in actions such as:

```text
allocate_gpu physical_gpu_id=<id> policy=free_pool_lifo
```

The extension should model this explicitly, even while it remains dry-run:

```text
freePhysicalGpuPool:
  policy: lifo
  ids: [...]
```

## phase-greedy planner

The retained planner is:

```text
run_phase_greedy_stage(...)
```

phase-greedy uses the full-plan candidate full-plan/action item generation as candidate generation, then
adds scoring, grouping, conflict checks, and iterative execution.

Main phase-greedy flow:

```text
current_state
  -> plan_full_action_plan(...) produces full_plan and plan_items
  -> _group_scores(...)
  -> _choose_nonconflicting_groups(...)
  -> _select_actions_for_root(...)
  -> apply selected actions
  -> repeat until converged or max_iters
```

phase-greedy scoring includes:

- takeover readiness,
- capacity headroom,
- peak GPU delta cost,
- drain wait cost,
- unlock count,
- release GPU score,
- target-backed enable score,
- in-place reconfiguration score.

The extension should migrate this planner rather than generating a simplified
dry-run action list.

## Main Stage Behavior

The notebook comments summarize the main stage behavior:

- `stage0`: pure `create_gpu`; build the target side directly.
- `stage1`: create target side first, then stop/reroute/drain/remove old work.
- `stage2`: stop routing on old-side items first, wait on drain barriers, then
  clear/remove old side and fill remaining target placement.
- `stage3`: mostly target-side placement/create, with little visible drain chain.

## K8s Extension Boundary

The Kubernetes implementation should be a thin adapter:

```text
PlanningScenario / CR inputs
  -> notebook-compatible planning inputs
  -> solve_milp_gurobi_batch_unified
  -> build_target_state_from_milp
  -> run_phase_greedy_stage
  -> dry-run MigPlan/status output
```

No real MIG reconfiguration, scheduler modification, or Pod deletion should be
performed in the first Kubernetes prototype.
