# Transition Template Logic

The simulation uses two different template concepts after the MILP step. The
prototype must keep them separate.

## 1. MILP Templates: Abstract Capacity

The MILP uses `TEMPLATES` as capacity vectors.

Example:

```text
4+3 -> {7g: 0, 4g: 1, 3g: 1, 2g: 0, 1g: 0}
3+2+1+1 -> {7g: 0, 4g: 0, 3g: 1, 2g: 1, 1g: 2}
```

This answers:

```text
How many instances of each profile can this GPU template provide?
```

It does not answer:

```text
Where are those instances located on the 7 MIG slices?
```

In the notebook, this is the prepare/MILP-stage `TEMPLATES` and `TEMPLATE_K`.

## 2. Transition Templates: Physical Realizations

The transition stage introduces `ABSTRACT_TO_PHYSICAL`.

Example:

```text
3+2+1+1 -> [2g, 1g, 1g, 3g]
          [1g, 1g, 2g, 3g]
```

This answers:

```text
Which physical interval layout should be used on an A100?
```

Each physical realization expands into intervals:

```text
[0,2) 2g
[2,3) 1g
[3,4) 1g
[4,7) 3g
```

The controller needs this because transition planning is about preserving or
changing real MIG instances, not just satisfying aggregate capacity.

## 3. Target Builder Flow

After MILP, the notebook does not directly use the MILP template list as final
state. It runs a target-state builder:

```text
MILP result
  -> extract instance demands
  -> extract MILP abstract template reference
  -> compute profile_need
  -> enumerate candidate abstract template sets
  -> augment candidates for preserve/upgrade rewrites
  -> order templates onto concrete GPU ids
  -> enumerate physical layouts per GPU
  -> score layouts against previous state
  -> assign workload demands to physical slots
  -> repair placement
  -> materialize target ClusterState
  -> reassign GPU ids for preserve
```

Important functions in the notebook:

```text
extract_template_list_from_milp
extract_instance_demands_from_milp
_profile_need_from_instance_demands
_enumerate_candidate_abstract_template_sets
_augment_candidate_abstract_template_sets
_order_candidate_templates_for_gpu_ids
_enumerate_physical_layout_combinations
_solve_target_with_greedy_repair
build_target_state_from_milp
```

## 4. Why Candidate Templates Can Differ From MILP Templates

MILP optimizes abstract capacity. Transition planning optimizes operational
stability.

So the target builder may prefer a different abstract template multiset if it:

- still satisfies `profile_need`,
- preserves more existing GPU templates,
- better matches old GPU ids,
- reduces disruption,
- enables upgrade-preserve cases.

The notebook explicitly augments candidate sets with:

```text
3+3       -> 4+3
3+2+1     -> 4+2+1
3+1+1+1   -> 4+1+1+1
```

This lets a native `3g` demand stay on an old `4g` slot when preserving an
existing assignment is more valuable than using an exact-size `3g` slot.

## 5. Physical Rewrite / Legalization Fallback

Some six-slice abstract layouts are treated specially during transition.

The notebook has rewrite candidates such as:

```text
3+3       -> [4g, 3g]
3+2+1     -> [4g, 2g, 1g]
3+2+1     -> [1g, 1g, 2g, 3g]
3+2+1     -> [2g, 1g, 1g, 3g]
3+1+1+1   -> [4g, 1g, 1g, 1g]
3+1+1+1   -> [1g, 1g, 1g, 1g, 3g]
```

This is not the same as the MILP capacity template. It is a transition-time
choice to produce a legal and less disruptive physical target layout.

## 6. What The Prototype Should Do

The prototype should model three separate layers:

```text
MigRules
  profiles
  abstract templates
  physical realizations
  transition rewrite candidates

Target Planner
  chooses abstract capacity to satisfy workload demand

Physical Layout Planner
  chooses intervals and placements for transition planning
```

`kind` should not simulate MIG devices. The controller should simulate MIG
rules at the data/model layer.

## 7. Required Prototype Changes

Immediate:

- Keep `mig-rules/a100-40gb.yaml` as the source of A100 MIG profile/template
  rules.
- Include both `capacity` and `physicalRealizations`.
- Include transition rewrite candidates for the special rewrite families.
- Keep `gpu-states/*.yaml` focused on observed current state only.

Next:

- Add a `mig_rules.py` loader.
- Add validation that every GPU state interval uses legal profiles and covers
  7 slices.
- Replace the current simple slice-count planner with:

```text
Workload requests
  -> feasible options
  -> abstract capacity target
  -> physical target layout
  -> phase-greedy dry-run action plan
```

Later:

- Reimplement the target builder interface from the notebook without copying
  notebook code directly.
- Keep MILP optional behind an interface.
- Keep phase-greedy as the only public action planner.

