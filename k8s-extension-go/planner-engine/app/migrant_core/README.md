# MIGRANT Core

This package is the final planner-engine implementation used by the Go control
plane.

It should preserve notebook semantics and expose importable Python modules for
the Kubernetes extension. It is not a place for simplified replacement
planners.

Runtime contents:

- `state.py`: notebook-derived `MigInstance`, `GPUState`, `ClusterState`, and
  state helpers.
- `physical_ids.py`: physical GPU ID map, canonical GPU ID handling, and
  `free_pool_lifo` behavior.
- `allocation_optimizer/milp_solver.py`: notebook-derived real Gurobi MILP
  solver and direct helper functions, including dominated-option pruning,
  elastic-up scoring, capacity aggregation, allocation extraction, warm start
  support, and multi-objective optimization.
- `allocation_optimizer/milp_extraction.py`: notebook-derived MILP result
  extraction helpers for template expansion, instance-demand aggregation,
  arrival dictionaries, profile need counts, expanded demand IDs, and MILP
  instance multisets.
- `target_materializer/templates.py`: notebook abstract templates, physical realizations, and
  interval expansion helpers. It also includes the notebook's void-like rewrite
  candidate data used before target-state materialization validation.
- `target_materializer/preserve.py`: preserve checks, physical-layout scoring,
  and GPU ID rematching helpers used by target materialization.
- `target_materializer/target_candidates.py`: abstract template multiset
  search, upgrade-aware candidate augmentation, GPU-id ordering, physical layout
  combination enumeration, and slot-list expansion.
- `target_materializer/target_materialization.py`: assignment metrics, score
  tuple, void-like layout legalization, exact/upgrade preserve preassignment,
  workload-aware greedy fill, move/swap local repair, and `ClusterState`
  materialization.
- `target_materializer/target_builder.py`: public
  `build_target_state_from_milp` API that wires MILP extraction, candidate
  search, target materialization, GPU ID rematching, same-logical-template order
  fixing, and build metrics.
- `transition_planner/effect_aware_dag.py`: the public final transition planner
  entry point. It lowers current/target allocation diffs into an executable
  action DAG annotated with capacity, router, MIG, physical-GPU, and binding
  effects.
- `transition_planner/catalog.py`: source-of-truth registry for transition
  planner semantics, aliases, roles, and runner functions. The final runtime
  catalog exposes only `effect_aware_dag`.
- `transition_planner/internal/state_diff.py`: transition diff, throughput
  safety, workload arrival, free-pool, semantic-state, bridge-slot, and action
  helper functions.
- `transition_planner/internal/action_builder.py`: action-lowering helpers used
  by `effect_aware_dag`; it is not registered as a selectable planner.
- `transition_planner/internal/action_simulator.py`: shared runtime/action
  helper functions, including runtime/drain metadata, action simulation, and
  plan bookkeeping.
- `transition_planner/internal/partial_reconfig.py`: partial MIG
  reconfiguration analysis for patchable layout changes.
- `transition_planner/internal/dag_format.py`: compiler for the
  `migrant.phased-action-dag/v1` action-plan representation.
