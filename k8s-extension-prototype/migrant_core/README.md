# MIGRANT Core

This package is the migration target for the source-of-truth notebook algorithm
in `OR_sim_trasition_action.ipynb`.

It should preserve notebook semantics and expose importable Python modules for
the Kubernetes extension. It is not a place for simplified replacement
planners.

Current contents:

- `state.py`: notebook-derived `MigInstance`, `GPUState`, `ClusterState`, and
  state helpers.
- `physical_ids.py`: physical GPU ID map, canonical GPU ID handling, and
  `free_pool_lifo` behavior.
- `templates.py`: notebook abstract templates, physical realizations, and
  interval expansion helpers. It also includes the notebook's void-like rewrite
  candidate data used before target-state materialization validation.
- `preserve.py`: notebook-derived preserve checks, physical-layout scoring, and
  GPU ID rematching helpers used by target materialization.
- `milp_extraction.py`: notebook-derived MILP result extraction helpers for
  template expansion, instance-demand aggregation, arrival dictionaries, profile
  need counts, expanded demand IDs, and MILP instance multisets.
- `target_candidates.py`: notebook-derived abstract template multiset search,
  upgrade-aware candidate augmentation, GPU-id ordering, physical layout
  combination enumeration, and slot-list expansion.
- `target_materialization.py`: notebook-derived assignment metrics, score
  tuple, void-like layout legalization, exact/upgrade preserve preassignment,
  workload-aware greedy fill, move/swap local repair, and `ClusterState`
  materialization.
- `target_builder.py`: notebook-derived public `build_target_state_from_milp`
  API that wires MILP extraction, candidate search, target materialization, GPU
  ID rematching, same-logical-template order fixing, and build metrics.
- `milp_solver.py`: notebook-derived real Gurobi MILP solver and direct helper
  functions, including dominated-option pruning, elastic-up scoring, capacity
  aggregation, allocation extraction, warm start support, and multi-objective
  optimization.
- `transition_common.py`: notebook-derived transition diff, throughput safety,
  workload arrival, free-pool, semantic-state, bridge-slot, and basic action
  helpers used by the phase-greedy planner and the K8s adapter.
- `transition_engine.py`: notebook-derived phase-greedy transition engine with
  runtime/action dependencies integrated directly into the planner. It includes
  runtime/drain metadata, full action-plan generation, plan-item scoring,
  non-conflicting group selection, action simulation, and iterative execution.
- `transition_planners/catalog.py`: source-of-truth registry for transition
  planner semantics, aliases, roles, and runner functions. It makes the planner
  set explicit even when multiple ablation variants share one implementation
  module.
- `transition_planners/root_scheduling_baselines.py`: ablation-only transition
  planner module for serial-root, drain-aware, and full-plan root scheduling
  baselines. The three exported planners share one implementation and differ
  only in how much of the scored root plan is executed per iteration.
- `transition_planners/action_plan_formats/phased_action_dag.py`: compiler for
  the `migrant.phased-action-dag/v1` action-plan representation. This is a data
  format helper, not a transition planner.
- `transition_planners/phase_greedy.py`: phase-greedy transition planner. It
  exposes `run()` for the plain linear action output and `run_with_dag_output()`
  for the same execution semantics plus the phased action DAG representation.
- `transition_planners/basic_dag.py`: baseline DAG compiler that converts a
  materialized MILP target and observed source state into rule-based abstract
  transition actions, then lowers them into an executable dependency DAG.
- `transition_planners/cost_aware_dag.py`: first cost-aware final-DAG planner.
  It enumerates transition-mode candidates for reconfiguration and workload
  replacement, filters service-infeasible candidates, and scores the remaining
  choices by peak active GPU count, queue/drain cost, MIG benchmark time, and
  disruption.

Next migration targets:

- K8s extension thin adapter and kind experiment harness.
