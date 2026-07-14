# 24h Target Allocation Throughput Replay Data

This folder contains the data needed to validate execution throughput for the
24-hour online provisioning experiment without requiring an 8-GPU testbed.

Source trace:

`/Users/weiyiqin/Downloads/OR_sim_planner/eval/results/online_24h/20260714-231227-scale0925-llama`

Target allocation entries:

`48` method/epoch allocations (`48` epochs for each exported method).

Unique target allocations:

`20`

Unique per-GPU templates:

`27`

## Files

- `request_rate_30min.csv`: demand trace used by the replay.
- `target_allocations.json`: full target allocations by method and epoch.
- `unique_target_allocations.json`: deduplicated target allocations that can be deployed directly.
- `unique_target_allocations.csv`: compact summary of deduplicated target allocations.
- `epoch_target_allocation_map.csv`: maps each epoch to a unique target allocation id.
- `target_allocation_summary.csv`: per-allocation GPU count, demand, and expected catalog capacity.
- `allocation_template_map.csv`: maps each method/epoch/logical GPU to a unique template id.
- `unique_gpu_templates.json`: full definition of each deduplicated per-GPU template.
- `unique_gpu_templates.csv`: compact template summary.
- `template_instances.csv`: one row per MIG instance in each unique template.
- `template_usage.csv`: how often each template appears across all target allocations.
- `two_gpu_sanity_pairs.csv`: representative adjacent template pairs for optional concurrent 2-GPU sanity checks.
- `aggregate_throughput_measurements.py`: post-processing helper that converts
  template-level measurements into epoch/workload measured capacity.

## What To Measure

There are two deployment granularities in this folder:

1. `unique_target_allocations.json` gives deduplicated full target allocations.
   Use this if you want to deploy a whole target allocation directly.
2. `unique_gpu_templates.json` gives deduplicated per-GPU templates. Use this
   if you want to measure each GPU template once and reconstruct allocation
   capacity by summing template throughput.

For the per-GPU template replay, for each row in `unique_gpu_templates.json`:

1. Configure one physical A100 GPU with the listed MIG slice layout.
2. Start all serving instances listed in the template.
3. Use the planner-selected request class and batch size directly.
4. Run all instances in the same template concurrently.
5. Measure completed requests per second for each instance.
6. Repeat each template 3-5 times and report mean/median throughput.

Do not search for the maximum SLO-safe rate in this measurement. SLO feasibility
has already been used to filter the serving catalog. This replay measures whether
the selected serving option produces the expected execution throughput.

Suggested output:

`template_throughput_measurements.csv`

Columns:

```text
template_id,trial,instance_idx,mig_profile,workload,request_class,batch_size,
measured_throughput_rps,measurement_seconds,physical_gpu_id,server_id
```

## How To Reconstruct Allocation-Level Throughput

For each method/epoch:

1. Read `allocation_template_map.csv`.
2. Look up the measured throughput for each template.
3. Sum measured throughput by workload across all logical GPUs in that allocation.
4. Compare the summed measured throughput against `request_rate_30min.csv`.

This gives:

```text
measured_capacity_rps[method, epoch, workload]
= sum(measured_template_throughput_rps[template, workload])
```

If the server has enough GPUs for a unique target allocation, it can instead
deploy the allocation directly from `unique_target_allocations.json` and measure
aggregate throughput for that allocation.

## Post-Processing After Measurement

After measuring all unique templates, aggregate the server output into
epoch-level workload throughput.

Input produced by the server:

`template_throughput_measurements.csv`

Required columns:

```text
template_id,trial,instance_idx,workload,measured_throughput_rps
```

Recommended aggregation:

1. Average repeated trials for each template instance:

```text
template_instance_mean[template_id, instance_idx, workload]
  = mean(measured_throughput_rps over trials)
```

2. Sum instances inside the same template by workload:

```text
template_workload_capacity[template_id, workload]
  = sum(template_instance_mean for that template and workload)
```

3. Expand each epoch allocation through `allocation_template_map.csv`:

```text
epoch_workload_capacity[epoch, workload]
  = sum(template_workload_capacity[template_id, workload]
        for all logical GPUs in that epoch)
```

4. Join with `request_rate_30min.csv` and compute:

```text
capacity_ratio[epoch, workload]
  = epoch_workload_capacity[epoch, workload] / demand_rate[epoch, workload]
```

Final output requested from the server:

`epoch_workload_measured_capacity.csv`

Columns:

```text
method,epoch,workload,demand_rate_rps,measured_capacity_rps,capacity_ratio
```

Also useful:

```text
epoch,min_capacity_ratio
```

where `min_capacity_ratio` is the minimum `capacity_ratio` across workloads with
nonzero demand in that epoch.

## Optional 2-GPU Sanity Check

Use `two_gpu_sanity_pairs.csv` to check server-level interference:

1. Pick pairs from peak or representative epochs.
2. Deploy template A on one GPU and template B on another GPU in the same server.
3. Run both templates concurrently.
4. Compare concurrent throughput with the sum of the individually measured
   template throughput.

Use only GPUs from the same server for this sanity check. Do not mix the third
GPU if it is on a different server, unless the server id is explicitly reported.

## Notes

- The same unique template may appear many times across epochs; measure it once
  and reuse the result.
- If a template has multiple instances, measure all instances concurrently.
- For CNN workloads, use the listed `batch_size`.
- For LLM workloads, use the listed prompt/output request class.
