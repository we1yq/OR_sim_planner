# Ampere batch/state and slot-concurrency smoke results

Date: 2026-09-28 UTC

Scope: `or-sim-exp`, node `ampere`, physical GPU `ampere-gpu0`. Candidate
images used immutable smoke tags during the run and the three deployments were
restored to their original `:go` tags afterwards.

## Results

- Local Go regression suites passed for cluster-state-manager,
  runtime-router, transition-executor, and planner-controller.
- Four independent `place_instance` actions on one physical GPU began within
  27 microseconds of one another. The recorded overlap for slots `[0,1,1g]`
  and `[4,7,3g]` was `3.493447s`.
- Independent VGG16 and ViT batch chains changed batch `1 -> 32`. Their
  `apply_batch`, `verify_batch`, and `activate_instance_route` intervals
  overlapped. Strict final validation returned `ok=true` with no missing,
  extra, or live-batch failures.
- After that update, registry bindings reported `batchSize=32`,
  `configuredBatchSize=1`, and `batchObservationSource=runtime-metrics` for
  both runtimes. Router route, runtime metrics, and driver batch all reported
  `32`.
- The immediately following planner invocation read `old_batch=32` for both
  runtimes and generated exactly two complete `32 -> 1` chains, with no
  placement or MIG actions. Execution and strict final validation passed.
- Cleanup returned the registry to `active=0, available=3, transitioning=0`,
  with zero runtime Pods and zero routes. Candidate deployments were rolled
  back and smoke CR/build resources were deleted.

Raw evidence from this run is in:

- `/tmp/batch32-plan-result.json`
- `/tmp/ampere-smoke-return1-20260928/`
- `/tmp/ampere-smoke-return1-executed-20260928/`
- `/tmp/ampere-smoke-concurrency-final2-executed-clean-20260928/`
- `/tmp/ampere-smoke-final-cleanup-executed-20260928/`
- `/tmp/ampere-smoke-final-registry.json`
- `/tmp/ampere-smoke-final-routes.json`
- `/tmp/ampere-smoke-final-runtime-pods.json`

The first setup attempt exposed a harness polling bug: after approval it
treated the still-`Planned` object as terminal and its failure cleanup could
delete an executing plan. The harness now waits only for `Executed`/`Failed`
after approval and never deletes an approved plan from an error handler.
