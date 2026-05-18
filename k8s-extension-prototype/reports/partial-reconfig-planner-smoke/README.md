# Partial Reconfiguration Planner Smoke

planner: `transition.cost_aware_dag`
planner elapsed: `0.001450s`
wall elapsed: `0.001597s`
reached target: `True`
peak active GPU: `1`

## Actions
1. `stop_gpu_traffic`: slots=[(0, 1, '1g'), (1, 2, '1g'), (2, 3, '1g'), (3, 4, '1g')]
2. `delete_pods`: slots=[(0, 1, '1g'), (1, 2, '1g'), (2, 3, '1g'), (3, 4, '1g')]
3. `configure_partial_profile`: deleteSpec=0:1:1g,1:1:1g,2:1:1g,3:1:1g; createSpec=0:2:2g,2:2:2g; preserveSpec=4:2:2g,6:1:1g
4. `observe_mig_devices`: slots=[(0, 2, '2g'), (2, 4, '2g')]
5. `deploy_target_workloads`: slots=[(0, 2, '2g'), (2, 4, '2g')]
6. `activate_serving_route`: slots=[(0, 2, '2g'), (2, 4, '2g')]

## Final Slots
- `[0,2) 2g` workload=`x` mu=`10`
- `[2,4) 2g` workload=`y` mu=`10`
- `[4,6) 2g` workload=`keep2` mu=`10`
- `[6,7) 1g` workload=`keep1` mu=`5`

## DAG JSON
See `partial-safe-stage-dag.json`.

## DAG SVG
See `partial-safe-stage-dag.svg`.
