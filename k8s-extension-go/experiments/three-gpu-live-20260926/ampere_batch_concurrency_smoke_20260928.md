# Ampere batch/state and slot-concurrency smoke

This is a small, controlled validation of the fixes for runtime batch
observation, strict final validation, and executor resource locking.  It is not
a replacement for the 12-round experiment and it must not run in a shared
`or-sim` namespace or against an Ampere GPU with somebody else's workload.

## Scope

Use exactly one physical GPU on node `ampere` and two disjoint existing MIG
slots (for example `s0-1-1g` and `s1-2-1g`).  The smoke has three assertions:

1. A runtime changed from batch 1 to 32 is observed as 32 by the registry and
   is the source batch for the immediately following 32 to 1 plan.
2. Final verification rejects an otherwise-identical target when its expected
   batch differs from runtime `/metrics`.
3. Two independent slot chains on the same physical GPU overlap.  Geometry
   actions remain outside this overlap.

The second assertion is a deterministic Go test, rather than a deliberate
live misconfiguration.  A live negative test would either leave a bad route
in the shared router or require a fault-injection hook, neither of which is
safe for this smoke.

## Required preflight (read-only)

Run these from the repository root before building or applying anything.  Save
the output under a new, untracked run directory; do not overwrite a prior
experiment directory.

```sh
(cd k8s-extension-go && go test ./cmd/cluster-state-manager ./cmd/transition-executor ./cmd/runtime-router ./cmd/planner-controller)

python3 k8s-extension-go/experiments/three-gpu-live-20260926/live_runner_20260926.py \
  --preflight-only \
  --placement-node ampere \
  --output-root k8s-extension-go/experiments/three-gpu-live-20260926/cluster_results/ampere-smoke-preflight-$(date -u +%Y%m%dT%H%M%SZ)

kubectl -n or-sim get physicalgpuregistries default -o yaml
kubectl -n or-sim get pods -o wide
kubectl -n or-sim get deploy -o yaml
kubectl get node ampere -o json
```

Stop unless all controllers are Ready, the router reports runtime metrics, the
chosen GPU and two exact slot resources are reserved for this smoke, and the
saved snapshot proves that no unrelated workload will be changed.  Also record
the immutable image digest and source commit for the candidate build.  The
current live runner is intentionally unsuitable for this small test: its
`--start-round/--end-round` mode still uses the frozen 12-round demand series.

## Executable harness

`run_ampere_smoke.py` is the fail-closed harness. It is permanently scoped to
`or-sim-exp` and `ampere`; other values are rejected. It requires a dedicated
control plane already running in `or-sim-exp` (including a separate router URL)
and will not create that control plane or change images. The namespace and
minimal RBAC/ServiceAccount prerequisite manifests are
`manifests/base/namespace-or-sim-exp.yaml` and
`manifests/control-plane/serviceaccount-rbac-or-sim-exp.yaml`.

Give it two reviewed JSON objects containing source and target workload rates.
They must be selected from the deployed catalog so the plan has the prescribed
topology; unexpected actions are rejected. For a two-runtime batch step, first
run it read-only:

```sh
python3 k8s-extension-go/experiments/three-gpu-live-20260926/run_ampere_smoke.py \
  --router-url http://SMOKE_ROUTER:8080 \
  --source-arrival /secure/smoke/r1.json --target-arrival /secure/smoke/r2.json \
  --mode batch --expected-chains 2 \
  --allow-runtime RUNTIME_A --allow-runtime RUNTIME_B
```

The read-only invocation only captures preflight. To obtain a reviewable,
manual-gated `plan-planned.json`, repeat it with `--stage --run-name RUN`; this
creates CRs but does not approve any action. Only after that file proves two
complete batch chains should the reviewed invocation use
`--approve-staged RUN`, plus `--expect-batch RUNTIME_A=32 --expect-batch
RUNTIME_B=32`. A second invocation uses R2 as source and R3 (batch 1) as
target. The harness writes pre/post
registry, routes, plan and input artifacts into a fresh `/tmp/ampere-smoke-*`
directory. Its failure trap deletes only this run's control CRs; it never
guesses at workload/route/GPU rollback, which remains a separately reviewed
normal controller plan.

## Live procedure (requires an exclusive maintenance window)

The maintainer must first deploy the candidate images and wait for their
rollouts.  Do not use a mutable tag as the evidence of what ran.

Prepare an isolated smoke input with these properties:

- It pins all runtime Pods to `ampere` and one named physical GPU.
- It selects two non-overlapping, already registered slot resources.
- Its first target creates one runtime per slot at batch 1, with no traffic.
- Its second target changes both existing runtimes to batch 32 without changing
  model, physical GPU, profile, slot, or route identity.
- Its third target changes both back to batch 1, again without changing their
  placement identity.

Use workloads and profile/batch pairs that are present in the deployed catalog.
Before approving each plan, inspect it.  R2 and R3 must each contain exactly
two independent chains:

```text
patch_batch_config -> apply_batch -> verify_batch -> activate_instance_route
```

There must be no `configure_*`, `clear_*`, `allocate_gpu`, `return_gpu`,
`place_instance`, or `delete_instance` action in R2/R3.  If the planner chooses
a different layout, do not approve it; adjust the dedicated smoke demand/input
instead of accepting a different test.

Capture, for each plan, its full CR YAML, `status.transitionExecution`, router
`/routes`, registry YAML, and each runtime's `/metrics` before and after the
transition.  The positive batch assertions are:

```text
after R2: runtime.metrics.batchSize == registry.observedBatchSize == 32
before R3: planner source batch == 32
after R3: runtime.metrics.batchSize == registry.observedBatchSize == 1
finalValidation.ok == true only after the equality above holds
```

The exact JSON field names may differ while the implementation is landing; the
saved evidence must always include both the raw runtime `/metrics` response and
the registry row used by the planner.  Do not treat a Pod `BATCH_SIZE`
environment variable as the observed value.

For the concurrency assertion, run a fourth, no-traffic plan that creates two
new runtime instances in two already registered disjoint slots on the same GPU.
It may be combined with the R1 setup only if that plan has no geometry actions
after the first `register_mig_devices` barrier.  Preserve the executor action
timestamps and calculate the interval intersection for the two `place_instance`
actions (or their readiness wait intervals):

```text
overlap = min(end_A, end_B) - max(start_A, start_B)
```

Success requires `overlap > 0`, different slot keys, and the same physical GPU.
The two actions must each still depend on their own slot's readiness/route
chain; action overlap is never evidence that same-slot ordering was removed.

## Deterministic negative verification check

Run the added transition-executor test that builds a target with the same GPU,
slot, profile and workload but an expected batch different from the mocked
runtime `/metrics` batch.  It must fail final verification and report batch as
the difference.  This is the release gate for the historical false-positive:

```sh
(cd k8s-extension-go && go test ./cmd/transition-executor -run 'Test.*(Batch|Final).*' -count=1)
```

Also require the cluster-state-manager test that prefers runtime metrics to the
Pod environment value and the planner test that consumes that observed value.

## Cleanup and acceptance

After evidence is copied out, delete only resources created by the smoke's
unique run label/name, wait for their Pods to disappear, and return the GPU to
the exact saved baseline using the normal controller plan.  Never delete a
whole namespace, all routes, all deployments, or all Ampere resources as
cleanup.

The smoke passes only when all of the following are present in the run bundle:

- candidate commit and immutable image digests;
- preflight and baseline/final registry, route and Pod snapshots;
- R2/R3 action lists with two complete batch chains each;
- runtime-metrics-to-registry-to-next-plan batch evidence for 1 -> 32 -> 1;
- the deterministic final-validation negative test result;
- timestamp evidence of same-GPU, distinct-slot overlap; and
- a final baseline comparison with no smoke-labelled resources left behind.

Any missing runtime metrics, stale/unknown observation, unexpected geometry or
placement action, absent overlap, or failed cleanup makes the smoke
inconclusive/failing rather than passed.
