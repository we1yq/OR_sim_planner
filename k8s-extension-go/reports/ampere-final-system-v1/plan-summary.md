# Ampere Final System Experiment v1

## Planner Pipeline

- Planner: `original-gurobi-milp-greedy-repair-effect-aware-dag`
- Pipeline: `source -> feasible-options -> milp -> target-build -> effect_aware_dag -> canonical-next-state`
- MILP status: `OPTIMAL`
- Chosen template: `3+1+1+1+1`
- Materialized physical template: `1+1+1+1+3`

## Target Runtimes

| Model | Node | GPU | Profile | Exact resource | Host port |
|---|---|---|---|---|---:|
| gpt2 | ampere | ampere-gpu0 | 1g | `or-sim.io/ampere-gpu0-s0-1-1g` | 10682 |
| llama | ampere | ampere-gpu0 | 3g | `or-sim.io/ampere-gpu0-s4-8-3g` | 10681 |

## Action DAG

| Phase | Action ID | Type | Physical GPU | Duration s |
|---:|---|---|---|---:|
| 0 | `a0000_allocate_gpu_CREATE-gpu1` | `allocate_gpu` | ampere-gpu0 | 0.000 |
| 1 | `a0001_configure_full_template_CREATE-gpu1` | `configure_full_template` | ampere-gpu0 | 2.244 |
| 2 | `a0002_bind_target_gpu_CREATE-gpu1` | `bind_target_gpu` | ampere-gpu0 | 0.000 |
| 3 | `a0003_register_mig_devices_CREATE-gpu1` | `register_mig_devices` | ampere-gpu0 | 1.543 |
| 4 | `a0004_deploy_target_workloads_CREATE-gpu1` | `deploy_target_workloads` | ampere-gpu0 | 0.008 |
| 5 | `a0005_activate_serving_route_CREATE-gpu1` | `activate_serving_route` | ampere-gpu0 | 64.567 |
