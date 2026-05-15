# Planner SLO/Time Comparison (Per Stage)

Generated: 2026-05-14

## Evaluation Model

This report compares `basic_dag` and `cost_aware_dag` per stage. It intentionally does not aggregate stage results.

Static SLO is computed from the target layout and profile-catalog `fitSlo`.

Transition SLO is a proxy, not a measured request-level violation rate. The transition-arrival model is proactive/controlled:

- During transition execution, incoming requests use `sourceArrival`, because the system is still serving the previous/current traffic while moving toward the target layout.
- The target layout itself is still built to satisfy `targetArrival`.
- At each DAG time interval, serving capacity is the sum of active source/target instance `mu` for each workload.
- New-arrival violation proxy is the integral of `max(0, sourceArrivalRate - activeCapacity)` over the plan makespan.
- For `basic_dag`, target/replacement capacity is treated as available only after the plan completes, matching the baseline interpretation: stopped traffic waits until completion and redistribution.
- For `cost_aware_dag`, target/replacement capacity becomes available when the DAG activates a route; stable source capacity remains available until its stop action.

## Per-Stage Results

| Stage | Planner | Makespan (s) | Source arrival rps | Target arrival rps | Action time (s) | Reconfig time (s) | Peak GPUs | Actions | New-arrival violation proxy | Existing queued proxy | Total transition violation proxy | Rerouted queued | Unrerouted queued | Drain rounds | Static SLO |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| stage0 | `basic_dag` | 128.7 | 0.0 | 3623.0 | 755.4 | 708.0 | 6 | 36 | 0.00 | 0.00 | 0.00 (0.0000%) | 0 | 0 | 0 | 0/26 (0.0000%) |
| stage0 | `cost_aware_dag` | 128.7 | 0.0 | 3623.0 | 755.4 | 708.0 | 6 | 36 | 0.00 | 0.00 | 0.00 (0.0000%) | 0 | 0 | 0 | 0/26 (0.0000%) |
| stage1 | `basic_dag` | 128.6 | 3623.0 | 9673.9 | 379.7 | 354.0 | 9 | 20 | 0.00 | 0.00 | 0.00 (0.0000%) | 0 | 0 | 0 | 0/34 (0.0000%) |
| stage1 | `cost_aware_dag` | 128.6 | 3623.0 | 9673.9 | 409.7 | 354.0 | 9 | 21 | 0.00 | 2.00 | 2.00 (0.0004%) | 0 | 2 | 3 | 0/34 (0.0000%) |
| stage2 | `basic_dag` | 163.1 | 9673.9 | 2914.4 | 295.5 | 273.5 | 9 | 31 | 1521907.99 | 0.00 | 1521907.99 (96.4331%) | 0 | 0 | 0 | 0/25 (0.0000%) |
| stage2 | `cost_aware_dag` | 193.1 | 9673.9 | 2914.4 | 695.5 | 273.5 | 9 | 54 | 1767407.94 | 26.00 | 1767433.94 (94.5953%) | 10 | 16 | 29 | 0/25 (0.0000%) |
| stage3 | `basic_dag` | 171.2 | 2914.4 | 8332.4 | 421.8 | 394.2 | 8 | 25 | 86.63 | 0.00 | 86.63 (0.0174%) | 0 | 0 | 0 | 0/31 (0.0000%) |
| stage3 | `cost_aware_dag` | 201.2 | 2914.4 | 8332.4 | 511.8 | 394.2 | 8 | 28 | 0.76 | 6.00 | 6.76 (0.0012%) | 0 | 6 | 9 | 0/31 (0.0000%) |

## New-Arrival Deficit by Workload

| Stage | Planner | Deficit requests by workload |
|---|---|---|
| stage0 | `basic_dag` | - |
| stage0 | `cost_aware_dag` | - |
| stage1 | `basic_dag` | - |
| stage1 | `cost_aware_dag` | - |
| stage2 | `basic_dag` | gpt2:702.0, llama:79.3, vgg16:52857.7, vit_base:1468269.0 |
| stage2 | `cost_aware_dag` | gpt2:831.1, llama:93.9, vgg16:28214.0, vit_base:1738269.0 |
| stage3 | `basic_dag` | gpt2:86.6 |
| stage3 | `cost_aware_dag` | gpt2:0.8 |

## Interpretation

- This version fixes the transition-arrival semantics: execution-time incoming traffic is `sourceArrival`, not `targetArrival`.
- This matters especially for demand-down stages: the system still has to survive the old higher rate while it transitions, unless the experiment explicitly models an instantaneous demand drop.
- A true SLO violation rate still requires request-level router/runtime timestamps. This report is a deterministic planning-time proxy using source arrival rates and profile service capacity.

## Raw JSON

```json
[
  {
    "action_counts": {
      "activate_serving_route": 6,
      "allocate_gpu": 6,
      "bind_target_gpu": 6,
      "configure_full_template": 6,
      "deploy_target_workloads": 6,
      "observe_mig_devices": 6
    },
    "actions": 36,
    "drain_rounds": 0,
    "expected_transition_requests": 0.0,
    "makespan_s": 128.711,
    "new_arrival_deficit_by_workload": {},
    "new_arrival_violation_proxy": 0,
    "peak_active_gpu": 6,
    "planner": "basic_dag",
    "queued_total": 0,
    "queued_violation_proxy": 0,
    "reconfig_s": 707.99,
    "rerouted_queued": 0,
    "source_arrival_rps": 0.0,
    "stage": "stage0",
    "static_instances": 26,
    "static_slo_violation_rate": 0.0,
    "static_slo_violations": 0,
    "sum_action_s": 755.39,
    "target_arrival_rps": 3623.0,
    "transition_violation_proxy": 0,
    "transition_violation_proxy_rate": 0.0,
    "unrerouted_queued": 0
  },
  {
    "action_counts": {
      "activate_serving_route": 6,
      "allocate_gpu": 6,
      "bind_target_gpu": 6,
      "configure_full_template": 6,
      "deploy_target_workloads": 6,
      "observe_mig_devices": 6
    },
    "actions": 36,
    "drain_rounds": 0,
    "expected_transition_requests": 0.0,
    "makespan_s": 128.711,
    "new_arrival_deficit_by_workload": {},
    "new_arrival_violation_proxy": 0,
    "peak_active_gpu": 6,
    "planner": "cost_aware_dag",
    "queued_total": 0,
    "queued_violation_proxy": 0,
    "reconfig_s": 707.99,
    "rerouted_queued": 0,
    "source_arrival_rps": 0.0,
    "stage": "stage0",
    "static_instances": 26,
    "static_slo_violation_rate": 0.0,
    "static_slo_violations": 0,
    "sum_action_s": 755.39,
    "target_arrival_rps": 3623.0,
    "transition_violation_proxy": 0,
    "transition_violation_proxy_rate": 0.0,
    "unrerouted_queued": 0
  },
  {
    "action_counts": {
      "activate_serving_route": 3,
      "allocate_gpu": 3,
      "bind_target_gpu": 3,
      "configure_full_template": 3,
      "delete_pods": 1,
      "deploy_target_workloads": 3,
      "observe_mig_devices": 3,
      "stop_accepting_new": 1
    },
    "actions": 20,
    "drain_rounds": 0,
    "expected_transition_requests": 465740.2729999999,
    "makespan_s": 128.551,
    "new_arrival_deficit_by_workload": {},
    "new_arrival_violation_proxy": 0,
    "peak_active_gpu": 9,
    "planner": "basic_dag",
    "queued_total": 0,
    "queued_violation_proxy": 0,
    "reconfig_s": 353.954,
    "rerouted_queued": 0,
    "source_arrival_rps": 3623.0,
    "stage": "stage1",
    "static_instances": 34,
    "static_slo_violation_rate": 0.0,
    "static_slo_violations": 0,
    "sum_action_s": 379.654,
    "target_arrival_rps": 9673.85,
    "transition_violation_proxy": 0,
    "transition_violation_proxy_rate": 0.0,
    "unrerouted_queued": 0
  },
  {
    "action_counts": {
      "activate_serving_route": 3,
      "allocate_gpu": 3,
      "bind_target_gpu": 3,
      "configure_full_template": 3,
      "delete_pods": 1,
      "deploy_target_workloads": 3,
      "mark_draining_instance": 1,
      "observe_mig_devices": 3,
      "stop_accepting_new": 1
    },
    "actions": 21,
    "drain_rounds": 3,
    "expected_transition_requests": 465740.2729999999,
    "makespan_s": 128.551,
    "new_arrival_deficit_by_workload": {},
    "new_arrival_violation_proxy": 0,
    "peak_active_gpu": 9,
    "planner": "cost_aware_dag",
    "queued_total": 2,
    "queued_violation_proxy": 2,
    "reconfig_s": 353.954,
    "rerouted_queued": 0,
    "source_arrival_rps": 3623.0,
    "stage": "stage1",
    "static_instances": 34,
    "static_slo_violation_rate": 0.0,
    "static_slo_violations": 0,
    "sum_action_s": 409.654,
    "target_arrival_rps": 9673.85,
    "transition_violation_proxy": 2,
    "transition_violation_proxy_rate": 4.294238905124703e-06,
    "unrerouted_queued": 2
  },
  {
    "action_counts": {
      "activate_serving_route": 2,
      "allocate_gpu": 1,
      "bind_target_gpu": 1,
      "clear_gpu_binding": 4,
      "clear_template": 4,
      "configure_full_template": 1,
      "delete_pods": 5,
      "deploy_target_workloads": 1,
      "observe_mig_devices": 1,
      "place_instance": 1,
      "return_gpu": 4,
      "stop_accepting_new": 2,
      "stop_gpu_traffic": 4
    },
    "actions": 31,
    "drain_rounds": 0,
    "expected_transition_requests": 1578201.5628499999,
    "makespan_s": 163.141,
    "new_arrival_deficit_by_workload": {
      "gpt2": 701.9891973600004,
      "llama": 79.31197599600004,
      "vgg16": 52857.683999999994,
      "vit_base": 1468269.0
    },
    "new_arrival_violation_proxy": 1521907.985173356,
    "peak_active_gpu": 9,
    "planner": "basic_dag",
    "queued_total": 0,
    "queued_violation_proxy": 0,
    "reconfig_s": 273.536,
    "rerouted_queued": 0,
    "source_arrival_rps": 9673.85,
    "stage": "stage2",
    "static_instances": 25,
    "static_slo_violation_rate": 0.0,
    "static_slo_violations": 0,
    "sum_action_s": 295.536,
    "target_arrival_rps": 2914.356,
    "transition_violation_proxy": 1521907.985173356,
    "transition_violation_proxy_rate": 0.9643305525721404,
    "unrerouted_queued": 0
  },
  {
    "action_counts": {
      "accept_queued_requests": 5,
      "activate_serving_route": 2,
      "allocate_gpu": 1,
      "bind_target_gpu": 1,
      "clear_gpu_binding": 4,
      "clear_template": 4,
      "configure_full_template": 1,
      "delete_pods": 5,
      "deploy_target_workloads": 1,
      "mark_draining_instance": 13,
      "observe_mig_devices": 1,
      "place_instance": 1,
      "reroute_queued_tasks": 5,
      "return_gpu": 4,
      "stop_accepting_new": 2,
      "stop_gpu_traffic": 4
    },
    "actions": 54,
    "drain_rounds": 29,
    "expected_transition_requests": 1868417.0628499999,
    "makespan_s": 193.141,
    "new_arrival_deficit_by_workload": {
      "gpt2": 831.0779973600004,
      "llama": 93.89665599600005,
      "vgg16": 28213.961440185,
      "vit_base": 1738269.0
    },
    "new_arrival_violation_proxy": 1767407.936093541,
    "peak_active_gpu": 9,
    "planner": "cost_aware_dag",
    "queued_total": 26,
    "queued_violation_proxy": 26,
    "reconfig_s": 273.536,
    "rerouted_queued": 10,
    "source_arrival_rps": 9673.85,
    "stage": "stage2",
    "static_instances": 25,
    "static_slo_violation_rate": 0.0,
    "static_slo_violations": 0,
    "sum_action_s": 695.536,
    "target_arrival_rps": 2914.356,
    "transition_violation_proxy": 1767433.936093541,
    "transition_violation_proxy_rate": 0.945952577310323,
    "unrerouted_queued": 16
  },
  {
    "action_counts": {
      "activate_serving_route": 4,
      "allocate_gpu": 3,
      "bind_target_gpu": 3,
      "clear_gpu_binding": 1,
      "clear_template": 1,
      "configure_full_template": 3,
      "delete_pods": 1,
      "deploy_target_workloads": 3,
      "observe_mig_devices": 3,
      "place_instance": 1,
      "return_gpu": 1,
      "stop_gpu_traffic": 1
    },
    "actions": 25,
    "drain_rounds": 0,
    "expected_transition_requests": 498923.17542000004,
    "makespan_s": 171.195,
    "new_arrival_deficit_by_workload": {
      "gpt2": 86.62894987500079
    },
    "new_arrival_violation_proxy": 86.62894987500079,
    "peak_active_gpu": 8,
    "planner": "basic_dag",
    "queued_total": 0,
    "queued_violation_proxy": 0,
    "reconfig_s": 394.198,
    "rerouted_queued": 0,
    "source_arrival_rps": 2914.356,
    "stage": "stage3",
    "static_instances": 31,
    "static_slo_violation_rate": 0.0,
    "static_slo_violations": 0,
    "sum_action_s": 421.798,
    "target_arrival_rps": 8332.37724,
    "transition_violation_proxy": 86.62894987500079,
    "transition_violation_proxy_rate": 0.00017363184182028708,
    "unrerouted_queued": 0
  },
  {
    "action_counts": {
      "activate_serving_route": 4,
      "allocate_gpu": 3,
      "bind_target_gpu": 3,
      "clear_gpu_binding": 1,
      "clear_template": 1,
      "configure_full_template": 3,
      "delete_pods": 1,
      "deploy_target_workloads": 3,
      "mark_draining_instance": 3,
      "observe_mig_devices": 3,
      "place_instance": 1,
      "return_gpu": 1,
      "stop_gpu_traffic": 1
    },
    "actions": 28,
    "drain_rounds": 9,
    "expected_transition_requests": 586353.85542,
    "makespan_s": 201.195,
    "new_arrival_deficit_by_workload": {
      "gpt2": 0.7590375000000069
    },
    "new_arrival_violation_proxy": 0.7590375000000069,
    "peak_active_gpu": 8,
    "planner": "cost_aware_dag",
    "queued_total": 6,
    "queued_violation_proxy": 6,
    "reconfig_s": 394.198,
    "rerouted_queued": 0,
    "source_arrival_rps": 2914.356,
    "stage": "stage3",
    "static_instances": 31,
    "static_slo_violation_rate": 0.0,
    "static_slo_violations": 0,
    "sum_action_s": 511.798,
    "target_arrival_rps": 8332.37724,
    "transition_violation_proxy": 6.759037500000007,
    "transition_violation_proxy_rate": 1.1527232979748327e-05,
    "unrerouted_queued": 6
  }
]
```
