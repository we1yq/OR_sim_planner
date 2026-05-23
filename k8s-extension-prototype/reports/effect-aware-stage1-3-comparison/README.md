# Effect-Aware Stage1-3 Comparison

Generated from the forced stage1-3 transition scenarios with router queue disabled (`defaultQueued=0`, `defaultInflight=0`).

| stage | planner | reached_target | actions | edges | phases | critical_path | peak_gpu | transition_elapsed_sec | planning_call_wall_sec | estimated_hardware_sec | blocked | workload_change | register_mig_devices | workload_bridge_action_count | capacity_gate_edges |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| stage1 | cost_aware_dag | True | 20 | 17 | 8 | 8 | 9 | 0.005235 | 1.308324 | 28.116 | 0 | 0 | 3 | 0 | 1 |
| stage1 | effect_aware_dag | True | 20 | 17 | 8 | 8 | 9 | 0.004996 | 1.092192 | 28.116 | 0 | 0 | 3 | 0 | 1 |
| stage2 | cost_aware_dag | True | 37 | 28 | 6 | 6 | 9 | 0.01101 | 1.599341 | 33.651 | 0 | 2 | 1 | 0 | 0 |
| stage2 | effect_aware_dag | True | 37 | 28 | 6 | 6 | 9 | 0.01105 | 1.620265 | 33.651 | 0 | 2 | 1 | 0 | 0 |
| stage3 | cost_aware_dag | True | 25 | 23 | 17 | 17 | 8 | 0.006257 | 2.243091 | 30.627 | 0 | 0 | 3 | 0 | 1 |
| stage3 | effect_aware_dag | True | 20 | 17 | 12 | 12 | 8 | 0.005527 | 2.306772 | 29.077 | 0 | 0 | 3 | 0 | 1 |

## Deltas

Effect-aware minus cost-aware.

| stage | action delta | edge delta | phase delta | critical path delta | capacity gate edge delta | transition elapsed sec delta | planning call wall sec delta | estimated hardware sec delta |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| stage1 | 0 | 0 | 0 | 0 | 0 | -0.000239 | -0.216132 | 0.0 |
| stage2 | 0 | 0 | 0 | 0 | 0 | 4e-05 | 0.020924 | 0.0 |
| stage3 | -5 | -6 | -5 | -5 | 0 | -0.00073 | 0.063681 | -1.55 |

## Notes

- `planning_call_wall_sec` is the wall time for this report script to call the full scenario planning adapter for that stage/planner pair.
- `estimated_hardware_sec` is a coarse action-cost sum, not measured hardware time.
- `capacity_gate_edges` counts explicit same-workload producer dependencies selected by the capacity safety gate.
