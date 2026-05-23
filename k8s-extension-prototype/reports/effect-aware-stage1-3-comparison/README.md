# Stage1-3 Effect-Aware vs Cost-Aware Transition Metrics

Generated from the current router-level queue semantics. `phase_count` is a derived topological view count, not an algorithmic phase in `effect_aware_dag`. Both planners now render explicit capacity-gate dependency edges before capacity-removing actions.

Important: these rows use each planner's own chained canonical source. Because `cost_aware_dag` does not reach the stage2 target, its stage3 input can differ from `effect_aware_dag`'s stage3 input. Use a shared-source run for strict per-stage algorithm comparison.

## Metrics

| stage | planner | reached_target | actions | edges | phases | critical_path | peak_gpu | transition_elapsed_sec | planning_call_wall_sec | estimated_hardware_sec | blocked | workload_change | register_mig_devices | workload_bridge_action_count | capacity_gate_edges |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| stage1 | `cost_aware_dag` | True | 21 | 18 | 9 | 9 | 9 | 0.004739 | 0.739955 | 28.122 | 0 | 0 | 3 | 0 | 1 |
| stage2 | `cost_aware_dag` | False | 37 | 39 | 7 | 7 | 9 | 0.009917 | 0.495667 | 35.272 | 0 | 0 | 1 | 0 | 0 |
| stage3 | `cost_aware_dag` | True | 39 | 40 | 18 | 18 | 8 | 0.007503 | 0.599704 | 45.637 | 0 | 0 | 3 | 0 | 1 |
| stage1 | `effect_aware_dag` | True | 21 | 18 | 9 | 9 | 9 | 0.004624 | 0.805697 | 28.122 | 0 | 0 | 3 | 0 | 1 |
| stage2 | `effect_aware_dag` | True | 50 | 51 | 7 | 7 | 9 | 0.010739 | 0.519436 | 48.349 | 0 | 2 | 1 | 0 | 0 |
| stage3 | `effect_aware_dag` | True | 21 | 19 | 13 | 13 | 8 | 0.004857 | 0.678167 | 29.08 | 0 | 0 | 3 | 0 | 1 |

## Delta: effect_aware_dag - cost_aware_dag

| stage | action delta | edge delta | phase delta | critical path delta | capacity gate edge delta | transition elapsed sec delta | planning call wall sec delta | estimated hardware sec delta |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| stage1 | 0 | 0 | 0 | 0 | 0 | -0.000115 | 0.065742 | 0.0 |
| stage2 | 13 | 12 | 0 | 0 | 0 | 0.000822 | 0.023769 | 13.077 |
| stage3 | -18 | -21 | -5 | -5 | 0 | -0.002646 | 0.078463 | -16.557 |

## Action Counts

- **stage1 `cost_aware_dag`**: `{"activate_serving_route": 3, "allocate_gpu": 3, "bind_target_gpu": 3, "configure_full_template": 3, "delete_pods": 1, "deploy_target_workloads": 3, "mark_draining_instance": 1, "register_mig_devices": 3, "stop_accepting_new": 1}`
- **stage2 `cost_aware_dag`**: `{"activate_serving_route": 2, "clear_gpu_binding": 3, "clear_template": 3, "configure_partial_profile": 1, "delete_pods": 6, "deploy_target_workloads": 1, "mark_draining_instance": 10, "place_instance": 1, "register_mig_devices": 1, "return_gpu": 3, "stop_accepting_new": 2, "stop_gpu_traffic": 4}`
- **stage3 `cost_aware_dag`**: `{"activate_serving_route": 6, "allocate_gpu": 3, "bind_target_gpu": 3, "clear_gpu_binding": 1, "clear_template": 1, "configure_full_template": 3, "delete_pods": 3, "deploy_target_workloads": 3, "mark_draining_instance": 6, "place_instance": 3, "register_mig_devices": 3, "return_gpu": 1, "stop_accepting_new": 2, "stop_gpu_traffic": 1}`
- **stage1 `effect_aware_dag`**: `{"activate_serving_route": 3, "allocate_gpu": 3, "bind_target_gpu": 3, "configure_full_template": 3, "delete_pods": 1, "deploy_target_workloads": 3, "mark_draining_instance": 1, "register_mig_devices": 3, "stop_accepting_new": 1}`
- **stage2 `effect_aware_dag`**: `{"activate_serving_route": 4, "clear_gpu_binding": 3, "clear_template": 3, "configure_partial_profile": 1, "delete_pods": 9, "deploy_target_workloads": 1, "mark_draining_instance": 13, "place_instance": 1, "register_mig_devices": 1, "return_gpu": 3, "stop_accepting_new": 5, "stop_gpu_traffic": 4, "workload_change": 2}`
- **stage3 `effect_aware_dag`**: `{"activate_serving_route": 4, "allocate_gpu": 2, "bind_target_gpu": 2, "configure_full_template": 2, "configure_partial_profile": 1, "delete_pods": 1, "deploy_target_workloads": 3, "mark_draining_instance": 1, "place_instance": 1, "register_mig_devices": 3, "stop_gpu_traffic": 1}`

Notes:
- `transition_elapsed_sec` is the transition planner runtime reported by the planner result.
- `planning_call_wall_sec` is the wall time for this report script to call the full scenario planning adapter for that stage/planner pair.
- `estimated_hardware_sec` is a coarse action-cost sum, not measured hardware time.
