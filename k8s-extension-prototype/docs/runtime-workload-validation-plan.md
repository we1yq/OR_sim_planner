# Runtime Workload Validation Plan

This note records what we should measure and update after real serving runtime
workloads are deployed on the MIGRANT Kubernetes cluster.

## Current Position

Today, the planner mainly uses hardware-level profile catalogs generated from
direct benchmark runs on the GPU host. This is enough for simulation and early
transition planning, but it does not yet capture the full serving path:

```text
router / serving layer
-> Kubernetes Pod
-> container runtime + CDI / NVIDIA_VISIBLE_DEVICES
-> model server
-> model execution on a MIG device
```

Until runtime workloads are deployed, keep using the existing hardware profile
catalogs as the planner input.

## Main Goal

After the runtime workload stack is deployed, calibrate the transition planner
with real end-to-end measurements so it can reason about:

- steady-state throughput under Kubernetes;
- p50/p95 request latency;
- cold start and warm start time;
- batch-size reload time;
- queue drain time during reroute;
- whether temporary batch-size increase is useful during transition.

## Measurements To Run

### 1. Hardware Baseline

Keep the existing direct-on-host benchmarks as the clean hardware baseline.
These numbers describe model capacity without Kubernetes or serving overhead.

Measure or keep:

- workload name;
- MIG profile: `1g`, `2g`, `3g`, `4g`, `7g`;
- batch size or runtime batching setting;
- throughput `mu`;
- latency metrics such as `e2eMs`, `ttftMs`, `tpotMs`, `serviceTimeMs`;
- peak memory;
- fit/fail result for memory and SLO.

### 2. Pod-Inside Steady-State Benchmark

Run the same workload inside the real serving Pod bound to a MIG UUID.

Measure:

- achieved throughput;
- p50/p95/p99 latency;
- request error rate;
- GPU memory usage;
- CPU and memory pressure inside the Pod;
- profile/batch combinations that satisfy SLO.

This should become the main profile source for final paper numbers.

### 3. Cold Start And Warm Start

Measure the time from Pod creation to serving-ready.

Break down if possible:

- Kubernetes scheduling and container start;
- Python/import/runtime initialization;
- model loading;
- CUDA initialization;
- first successful request;
- readiness signal propagation.

These numbers should update the planner costs for `deploy_target_workloads` and
related pod lifecycle actions.

### 4. Batch Reload

For workloads that support runtime batch changes, measure:

- config patch time;
- runtime reload/apply time;
- verification time;
- latency spike during reload;
- whether in-flight requests are affected.

This should update the costs for:

```text
Patch Config
-> Apply Batch
-> Verify Batch
```

### 5. Reroute And Queue Drain

During transition, measure what happens when queued requests are moved from one
slot to another stable slot.

Current implementation status:

- reroute is only allowed to a stable destination slot with the same workload
  and batch size;
- reroute actions now carry a hardware-profile-based pressure estimate:
  `targetMu`, `workloadRequiredMu`, `workloadProvidedAfterSourceRemoval`,
  `estimatedRerouteSpareMu`, and `estimatedBacklogDrainSeconds`;
- the cost-aware transition planner includes estimated reroute backlog time in
  candidate scoring;
- this estimate is conservative and should be replaced or calibrated once
  Pod-inside runtime measurements exist.

Measure:

- queued request count;
- source workload and batch;
- destination workload and batch;
- destination spare throughput;
- drain completion time;
- latency distribution of rerouted requests;
- whether the destination violates SLO during the burst.

This should replace the current constant-cost reroute/drain model with a
workload-aware model.

After runtime deployment, update reroute planning to account for:

- per-destination current load, not only workload-level spare capacity;
- multiple source queues rerouting into the same destination slot;
- runtime p95/p99 latency under bursty queue handoff;
- whether a destination slot should temporarily increase batch size before
  accepting rerouted requests;
- whether the temporary batch increase should be restored after the old slot is
  drained.

### 6. Temporary Batch Increase

For candidate reroute destinations, test whether increasing batch size before
reroute helps absorb backlog.

Candidate sequence:

```text
Patch Batch Config
-> Apply Batch
-> Verify Batch
-> Accept Queued Requests
-> Reroute Queued Requests
-> Wait Drain
-> Restore Batch Config   # optional
```

Only use this when:

- the runtime supports batch reload without Pod recreation;
- the larger batch still satisfies SLO;
- increased throughput reduces backlog drain time enough to justify reload cost.

## Missing Profile Data

The current LLM catalogs are thin:

- `llama` currently has mostly `batch=1`;
- `gpt2` currently has mostly `batch=1`;
- CV workloads such as `vgg16` already have richer multi-batch catalogs.

After runtime deployment, add multi-batch entries for:

```text
llama:
  profiles: 3g, 4g, 7g
  batch/runtime settings: 1, 2, 4, maybe 8

gpt2:
  profiles: 1g, 2g, 3g, 4g, 7g
  batch/runtime settings: 1, 2, 4, 8, maybe 16
```

For LLMs, the field named `batch` may represent runtime batching controls such
as `max_batch_size`, `max_num_seqs`, prefill batch, decode batch, or continuous
batching capacity. Record the exact runtime setting in catalog metadata.

## Catalog Schema Direction

Keep hardware and runtime measurements separate.

Suggested shape:

```yaml
options:
- workload: llama
  family: llm
  profile: 3g
  batch: 4
  hardwareProfile:
    mu: ...
    ttftMs: ...
    tpotMs: ...
    serviceTimeMs: ...
    peakMemMb: ...
  runtimeProfile:
    mu: ...
    p50Ms: ...
    p95Ms: ...
    p99Ms: ...
    coldStartMs: ...
    warmStartMs: ...
    batchReloadMs: ...
    fitSlo: true
```

The planner should prefer `runtimeProfile` when present and fall back to
hardware-level fields otherwise.

## Planner Updates After Measurement

Once runtime measurements exist, update the planner in this order:

1. Replace constant pod deployment costs with measured cold/warm start costs.
2. Replace constant drain time with workload-specific drain time.
3. Add destination spare-capacity checks for reroute:

```text
spare_throughput =
  target_runtime_mu
  - existing_arrival_rate_on_target
```

4. Estimate backlog delay:

```text
backlog_delay =
  queued_requests / max(spare_throughput, epsilon)
```

5. Gate reroute by SLO:

```text
backlog_delay + runtime_latency_p95 <= SLO
```

6. Add a candidate transition action for temporary batch increase when it
improves backlog drain time without violating SLO.
7. Compare candidates:

```text
reroute only
reroute + temporary batch increase
wait drain without reroute
partial reconfiguration
full reconfiguration with extra GPU
```

The transition planner objective remains:

```text
minimize extra GPU usage
subject to SLO violation <= epsilon
then minimize transition time and disruption
```

## Router Table And Adapter Updates

The current router adapter can produce preview CRs and can apply simple
annotation/HTTP router actions, but it does not yet own a full serving router
table. The intended split is:

```text
WorkloadRoutePlan   = transition command for this epoch
WorkloadRouteState  = observed/current routing table
```

Current implementation status:

- `WorkloadRoutePlan` records stop/reroute commands.
- `ServingInstanceDrain` records drain commands.
- reroute preview and real executor records now include `targetInstanceRef` and
  `reroutePressure`.
- drain records now include queued/inflight approximations and an estimated
  drain time when the planner provides one.

Future `WorkloadRouteState` should track one row per serving instance:

```yaml
workload: gpt2
instances:
- logicalGpuId: 3
  physicalGpuId: D
  slot: [4, 5, 1g]
  migUuid: MIG-...
  podName: gpt2-d-4-5
  endpoint: http://gpt2-d-4-5:8080
  batch: 1
  capacityMu: 1.131
  currentLoad: 0.7
  queued: 0
  inflight: 1
  acceptingNew: true
  draining: false
  routeWeight: 1.0
  p95Ms: ...
```

After runtime deployment, update adapters as follows:

1. Router adapter reads `WorkloadRouteState` before planning and writes
   `WorkloadRoutePlan` during transition.
2. Router executor applies route-table changes to the real serving router.
3. Router observer refreshes `WorkloadRouteState` from live router/runtime
   metrics.
4. Register/observer joins:

```text
slot -> MIG UUID -> Pod -> endpoint -> route state
```

5. The planner uses `WorkloadRouteState` for reroute destination selection,
   destination load, queue length, inflight count, and p95 latency.

The active/transitioning GPU queues should eventually use route readiness too:

```text
active =
  MIG UUID ready
  + CDI ready
  + Pod ready
  + route active

transitioning =
  any of MIG patching, CDI refresh, Pod deployment, drain/reroute,
  route activation, or observer verification is unfinished
```

## Expected Experiment Outputs

Produce these reports after runtime deployment:

- hardware vs Pod-inside profile comparison;
- multi-batch profile catalog for LLM workloads;
- cold start and warm start benchmark table;
- batch reload benchmark table;
- reroute/backlog drain benchmark table;
- planner ablation before and after runtime-aware costs;
- case study showing when temporary batch increase is selected.

## Reminder

Before claiming final paper numbers, rerun the planner with runtime-calibrated
profile catalogs. Hardware-only catalogs are acceptable for early simulation,
but final transition latency and SLO results should come from Pod-inside
measurements.
