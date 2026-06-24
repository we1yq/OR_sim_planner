# MIG Profile Data

This directory separates retired local-node profile data from the new
Kubernetes-based profiling campaign.

## Layout

- `old/`: retired raw CSVs from earlier local-node profiling. Keep these for
  comparison only; do not use them for new planner catalogs.
- `current/`: raw runtime-side measurements from the new Kubernetes pod-based
  profiling campaign. These are the source of truth for planner profile
  catalogs.
- `router-e2e/`: router-path measurements used to quantify system overhead.
  These measurements are not the base planner catalog.

## New Profiling Design

Planner catalog data should come from runtime-side measurements taken after the
workload pod is deployed on a specific MIG profile. The benchmark client should
run in the same pod or on the same node when possible, so the measured latency
captures the model service behavior on the MIG partition rather than router
placement, cross-node network time, or queueing.

Router-path measurements should be collected separately through the runtime
router. They are useful for evaluating system overhead, same-node versus
cross-node routing, route changes, and drain behavior. Treat these as overhead
or conservative calibration data, not as the base service capacity.

## Workload Identity

Vision workloads keep the model name as the workload identity because image size
is fixed at 224x224 and batch size remains a planner decision:

- `resnet50`
- `vgg16`
- `vit_base`

Language request shapes are separate workloads because prompt and output lengths
arrive with user requests and are not planner decisions:

- `gpt2_p64_o64`
- `gpt2_p64_o128`
- `gpt2_p512_o64`
- `gpt2_p512_o128`
- `gpt2_p1024_o64`
- `gpt2_p1024_o128`
- `llama32_3b_p64_o64`
- `llama32_3b_p64_o128`
- `llama32_3b_p512_o64`
- `llama32_3b_p512_o128`
- `llama32_3b_p1024_o64`
- `llama32_3b_p1024_o128`

## Runtime-Side Matrix

Run each workload shape on each supported MIG profile:

- MIG profiles: `1g`, `2g`, `3g`, `4g`, `7g`
- Vision batches: `1`, `4`, `16`, `32`, `64`
- Language batch: `1`
- Language prompt tokens: `64`, `512`, `1024`
- Language output tokens: `64`, `128`

Record OOM and SLO failures explicitly instead of dropping those rows.

## Required Raw Fields

Common fields:

- `run_id`
- `timestamp`
- `node`
- `gpu`
- `mig_profile`
- `mig_uuid`
- `workload`
- `model`
- `family`
- `batch`
- `repeat`
- `status`
- `error`
- `peak_alloc_mb`
- `peak_reserved_mb`

Vision fields:

- `image_h`
- `image_w`
- `latency_ms_mean`
- `latency_ms_p50`
- `latency_ms_p95`
- `latency_ms_p99`
- `throughput_rps`

Language fields:

- `prompt_len`
- `output_tokens`
- `ttft_ms_mean`
- `ttft_ms_p50`
- `ttft_ms_p95`
- `tpot_ms_mean`
- `decode_tps`
- `service_time_ms`
- `throughput_rps`

## Catalog Generation

For planner catalogs, aggregate `current/` runtime-side rows by workload,
profile, and batch. The catalog should keep the existing planner-facing shape:

- `workload`
- `family`
- `batch`
- `profile`
- `mu`
- `fit`
- latency metrics
- memory metrics
- `fitMem`
- `fitSlo`
- `sourceCsv`

Do not add prompt/output dimensions to the planner schema. They are already
encoded in the language workload name.

