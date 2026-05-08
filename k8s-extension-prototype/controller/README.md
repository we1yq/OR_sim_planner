# Controller

This directory contains the minimal mock-based controller prototype.

Phase 5 starts with a local CLI controller that:

- reads scenario YAML and workload request references,
- reads mock GPU/MIG state,
- validates mock MIG rules,
- adapts inputs into the notebook-derived real MILP target builder and V3
  transition planner.

The controller must not execute real Kubernetes scheduling changes or MIG
reconfiguration commands in the first prototype.

Current modules:

```text
main.py
models.py
io_utils.py
state_adapter.py
feasible_options.py
mig_rules.py
scenario_loader.py
k8s_adapter.py
reconciler.py
k8s_api.py
current_state_feasibility.py
executor_preview.py
actuator.py
adapters/
```

## Setup

Check whether `PyYAML` is installed:

```bash
python3 -c 'import yaml; print(yaml.__version__)'
```

If it is missing, install the controller dependencies:

```bash
python3 -m pip install -r k8s-extension-prototype/controller/requirements.txt
```

## Parse a Scenario

The multi-workload path currently loads a scenario and prints a parsed summary.
The real target planner will be migrated from the notebook MILP, not replaced by
a simplified planner.

```bash
python3 k8s-extension-prototype/controller/main.py \
  --scenario k8s-extension-prototype/mock/scenarios/stage0.yaml
```

## Run Real MILP + V3 Dry-Run Planning

Single stage:

```bash
python3 k8s-extension-prototype/controller/main.py \
  --plan-scenario k8s-extension-prototype/mock/scenarios/stage0.yaml
```

Notebook stage chain with canonical next-stage state:

```bash
python3 k8s-extension-prototype/controller/main.py \
  --plan-scenario-chain \
  k8s-extension-prototype/mock/scenarios/stage0.yaml \
  k8s-extension-prototype/mock/scenarios/stage1.yaml \
  k8s-extension-prototype/mock/scenarios/stage2.yaml \
  k8s-extension-prototype/mock/scenarios/stage3.yaml
```

This path intentionally calls:

```text
solve_milp_gurobi_batch_unified
  -> build_target_state_from_milp
  -> run_v3_stage_iterative
  -> canonicalize_state_for_next_round
```

It does not include an approximate planner or the old V2 planner.

## Reconcile One MigPlan CR

The first Kubernetes adapter path is a one-shot dry-run reconciler. It reads a
`MigPlan`, resolves `spec.scenario` under `mock/scenarios`, runs the same real
MILP plus V3 flow, and patches only `MigPlan.status`.

```bash
kubectl apply -f k8s-extension-prototype/manifests/crds/migplan-crd.yaml
kubectl apply -f k8s-extension-prototype/manifests/examples/migplans/stage0.yaml

python3 k8s-extension-prototype/controller/main.py \
  --reconcile-migplan stage0 \
  --namespace or-sim

kubectl get migplan stage0 -n or-sim -o yaml
```

For staged transitions, the reconciler can persist the canonical next state to a
ConfigMap and use that ConfigMap as the next plan's source:

```text
stage0 -> status.canonicalNextState + ConfigMap/target0-state
stage1 spec.sourceStateConfigMap: target0-state -> ConfigMap/target1-state
stage2 spec.sourceStateConfigMap: target1-state -> ConfigMap/target2-state
stage3 spec.sourceStateConfigMap: target2-state -> ConfigMap/target3-state
```

In the current dry-run prototype, `canonicalNextState` is derived from the V3
transition simulator's executed state. In a real actuator-backed controller, this
must change: actions must be applied to the cluster first, the controller must
observe the actual post-action GPU/MIG state, and only that observed executed
state should be passed through `canonicalize_state_for_next_round`. The
canonical state is the next planning round's stable input, not proof that a
planned action succeeded.

Apply all stage examples:

```bash
kubectl apply -k k8s-extension-prototype/manifests/examples/profile-catalogs/
kubectl apply -f k8s-extension-prototype/manifests/examples/autoapprovalpolicies/
kubectl apply -f k8s-extension-prototype/manifests/examples/scenarios/
kubectl apply -f k8s-extension-prototype/manifests/examples/migplans/

python3 k8s-extension-prototype/controller/main.py \
  --run-controller \
  --namespace or-sim \
  --controller-max-cycles 4 \
  --poll-interval-s 1

kubectl get migplans -n or-sim
kubectl get configmaps -n or-sim -l mig.or-sim.io/state-kind=canonical-next-state
```

Each reconciled `MigPlan.status` is intentionally compact for day-to-day
inspection. It includes `planningSummary`, which keeps the intermediate planner
outputs to a short stage-by-stage summary:

```bash
kubectl get migplan stage0 -n or-sim -o yaml | yq '.status.planningSummary'
```

The summary includes feasible option count, MILP status/GPU count, target-build
GPU count, V3 transition iterations/action counts, action counts by type, chosen
templates, and the canonical physical GPU id mapping.

Verbose debug output is stored separately:

```bash
kubectl get configmap stage0-full-plan -n or-sim -o yaml
kubectl get configmap target0-state -n or-sim -o yaml
```

`*-full-plan` contains the full `planningTrace`, ordered actions, and
target/executed/canonical state dumps. `target*-state` contains only the
canonical next state used as the next stage's source.

Each reconcile also writes a `MigActionPlan` object such as
`stage0-action-plan`. This is the execution boundary for future actuator work:
it records the action count, action type summary, chosen templates, full-plan
ConfigMap, canonical next-state ConfigMap, and preview-only execution drafts, but
it remains `dryRun: true` and is only automatically approved when an
`AutoApprovalPolicy` allows it.

```bash
kubectl get migactionplans -n or-sim
kubectl get migactionplan stage0-action-plan -n or-sim -o yaml
kubectl get autoapprovalpolicy default -n or-sim -o yaml
```

The default example policy is dry-run-only, allows only the
`nvidia-gpu-operator` executor, requires a full-plan ConfigMap, and caps action
plans at 40 actions. Plans that pass become `status.phase: ApprovedDryRun`; plans
that fail become `ApprovalBlocked`; missing or disabled policy leaves them
`PendingApproval`.

The dry-run actuator is a separate Deployment. It consumes `ApprovedDryRun`
action plans, validates that the full-plan and canonical-next-state ConfigMaps
exist, that the action count matches, and that all execution previews are
explicitly preview-only, then marks the plan `SucceededDryRun`. It still does not
execute MIG, Pod, scheduler, or Node changes.

As part of that validation, it runs the dry-run adapter contracts for MIG,
router, Pod lifecycle, and observer previews. The result is written under
`MigActionPlan.status.validated.adapterContracts` as counts of `would*`
operations. This gives kind something concrete to validate even without real GPU
nodes: preview inputs are well formed, each adapter can consume them, and the
actuator still refuses to perform non-preview execution.

The router adapter now writes preview-only `WorkloadRoutePlan` and
`ServingInstanceDrain` custom resources derived from `trafficAndDrainPreview`.
It also keeps a mock router plan ConfigMap named like `<action-plan>-router-plan`
as a debug dump.

The Pod lifecycle adapter likewise writes preview-only `PodLifecyclePlan` custom
resources for `CreateOrReuse`, `Drain`, `DeleteOrRecycle`, and `ReloadInPlace`,
and keeps `<action-plan>-pod-lifecycle-plan` as a debug ConfigMap. These CRs are
still dry-run contract artifacts: no Pods, Deployments, or serving runtimes are
changed. The actuator patches each child CR status to
`SucceededDryRunPreview`, which proves the `/status` boundary and RBAC path work
without handing execution to a real router or Pod adapter yet. `MigActionPlan`
only reaches `SucceededDryRun` after these child resources have preview-only
status acknowledgements, and it records the aggregate under
`status.validated.childResources`.

The actuator also writes `<action-plan>-execution-log`, a preview-only execution
observation log. This log deliberately calls the final step
`SimulatedCanonicalization`: in kind, the canonical next input is derived from
the simulator, while real execution must wait until MIG/router/Pod actions
finish and the observer reports actual post-action cluster state.

The same dry-run path now writes an `ObservedClusterState` custom resource. In
kind it is marked `previewOnly: true` and `readyForCanonicalization: false`
because node/GPU/MIG/Pod/router inputs are missing. With real hardware, the
observer adapter should write the actual post-action state to this CR shape and
only then mark it ready for canonicalization.

Before MIG-specific observer integration is available, a read-only node/pod
smoke observer can verify that the controller service account can see real
Kubernetes nodes and Pods:

```bash
python3 k8s-extension-prototype/controller/main.py \
  --observe-cluster-state \
  --namespace or-sim \
  --apply-observed-state
```

This smoke object is still `readyForCanonicalization: false` because it lacks
physical GPU UUIDs, actual MIG instances, Pod-to-MIG assignment, and router
metrics. It is only the first real-node visibility check.

```bash
kubectl get deployment mig-dry-run-actuator -n or-sim
kubectl get migactionplans -n or-sim
kubectl get workloadrouteplans,servinginstancedrains,podlifecycleplans -n or-sim
kubectl get observedclusterstates -n or-sim
kubectl get configmaps -n or-sim -l mig.or-sim.io/state-kind=dry-run-execution-log
```

The intended production executor is `nvidia-gpu-operator`: a future actuator
should translate approved action plans into NVIDIA GPU Operator/MIG Manager
inputs, observe the real post-action GPU state, and only then canonicalize the
observed state for the next planning round. The controller should not jump
straight to privileged `nvidia-smi` commands unless the operator path is
insufficient and the safety model is explicit.

`MigActionPlan.spec.executorPreview` remains a compatibility summary for the
NVIDIA GPU Operator path. The more explicit adapter previews are:

```text
spec.migGeometryPreview
spec.trafficAndDrainPreview
spec.podLifecyclePreview
spec.abstractActionPreview
spec.adapterDryRunPreview
spec.observerPreview
```

`migGeometryPreview` maps the canonical target layout into per-physical-GPU
target MIG geometry and, when a real observer supplies
`physicalGpuId -> nodeName/deviceIndex` bindings, shows the node labels a GPU
Operator/MIG Manager actuator would patch:

```text
spec.migGeometryPreview.gpuTargets[]
  logicalGpuId
  physicalGpuId
  nodeName
  deviceIndex
  targetTemplate
  targetInstances

spec.migGeometryPreview.migManagerTargetConfigs[]
spec.migGeometryPreview.wouldPatchNodeLabels:
  <nodeName>:
    nvidia.com/mig.config: <generated-config-name>
```

`trafficAndDrainPreview` exposes the V3 abstract-action rules that are not MIG
geometry: stop accepting new requests, reroute queued work, wait for inflight
work to drain, and defer actions when takeover capacity or drain completion is
missing. It is intended for a future router/drain adapter.

`podLifecyclePreview` is intentionally separate from both MIG geometry and
traffic. It identifies which actions would need target serving capacity
(`create-or-reuse`), which old serving instances must drain, which Pods may be
deleted or recycled only after drain, and which changes should prefer in-place
runtime reload.

`abstractActionPreview` is a human-readable report over V3 coarse actions. It
shows why an action is `create_gpu`, `remove_gpu`, `reconfiguration`, or
`instance_diff`, which gates must pass, and the expected MIG/Pod impact. This is
the object to inspect when reviewing planner rules rather than individual fine
actions.

`adapterDryRunPreview` is the dry-run skeleton for future adapters. It groups the
same plan into what a MIG adapter would patch, what a router adapter would
stop/reroute/drain, and what a Pod adapter would create/reuse/delete/reload. It
does not execute any of those operations.

`observerPreview` lists the observations required before real execution can be
trusted: physical GPU node/device bindings, observed MIG instances, router queue
state, Pod readiness, Pod-to-MIG assignment, and inflight counts. Without these
observations, a future actuator must not assume that planned actions succeeded.

The dry-run actuator also builds `status.validated.observedStatePreview` from an
observer skeleton. In kind this preview intentionally contains empty or unknown
runtime values plus `missingRealClusterInputs`, because there are no NVIDIA GPU
nodes, router metrics, or Pod-to-MIG assignment observer. A real observer adapter
must replace those placeholders with live cluster observations before any
non-dry-run actuator can safely canonicalize the next source state.

In kind, `nodeName` and `deviceIndex` are normally unresolved because there is no
NVIDIA GPU node inventory. That is expected: kind validates the CRD/controller/
status/ConfigMap/action-plan/preview flow, not real MIG hardware. A real actuator
must first observe physical GPU bindings and current MIG layouts, then convert
the approved preview into GPU Operator/MIG Manager config, execute it, observe
the actual result, and canonicalize that observed state.

## Arrival Snapshots

The `stage0`, `stage1`, ... files are offline planning epochs. In production the
same role is played by an external arrival-rate snapshot or forecast. A
`MigPlan` can optionally set `spec.arrivalSnapshotConfigMap`; when present, the
controller loads `data.arrival-snapshot.yaml` and uses its `targetArrival` as
the next planning epoch's target arrival while keeping the scenario's workload
refs, profile catalogs, and source state.

An arrival snapshot is a demand update, not an execution command. Before running
MILP, the planner evaluates whether the current observed A100 state already
satisfies the new target arrival. If the existing MIG layout is a valid A100
7-slice layout and the current serving instances provide enough positive `mu`
capacity for every workload, the planner returns `SucceededNoOp`, skips MILP,
and records the decision in `status.currentStateFeasibility`. This avoids
unnecessary MIG reconfiguration when a new epoch can be absorbed by the current
state.

Example snapshot shape:

```yaml
name: arrival-stage0-epoch1
epoch: 1
source: external-forecast
previewOnly: true
targetStateRef: target0-epoch1
targetArrival:
  llama: 3
  gpt2: 20
```

This keeps the current stage-chain examples working while making the production
interpretation explicit: each new external arrival snapshot can drive a new
planning round.

kind validation is therefore limited but still useful. It can validate CRD schema,
RBAC, controller reconciliation, ConfigMap writes, preview generation, actuator
gating, and status transitions. It cannot validate real Node labels, GPU Operator
behavior, Pod placement on MIG devices, or live request draining; those require a
cluster with NVIDIA GPU nodes and the relevant runtime/router integrations.

## Run The Polling Controller Loop

The watch controller receives `MigPlan` API events and reconciles any object whose
`metadata.generation` is not reflected in `status.observedGeneration`.

```bash
python3 k8s-extension-prototype/controller/main.py \
  --run-watch-controller \
  --namespace or-sim
```

For a bounded smoke test:

```bash
python3 k8s-extension-prototype/controller/main.py \
  --run-watch-controller \
  --namespace or-sim \
  --watch-max-events 4
```

The polling loop remains available for debugging with `--run-controller`. Both
loops still only patch `MigPlan.status`; they do not modify pods, workload
requests, scheduler configuration, or MIG state.

Kubernetes I/O is isolated behind `k8s_api.py` and uses the Kubernetes Python
client. In-cluster config is used inside the controller Pod; local kubeconfig is
used when the controller is run from a development shell.

`MigPlan.spec.scenarioConfigMap` is preferred over the older `spec.scenario`
file lookup. Scenario ConfigMaps can use `profileCatalogConfigMaps` to point at
profile catalog ConfigMaps whose `data.catalog.yaml` contains the catalog YAML.
The older `profileCatalogRefs` file lookup remains available as a fallback for
local mock scenarios.

Smoke-test the reconciler helpers without a cluster:

```bash
python3 k8s-extension-prototype/controller/reconciler_test.py
```

## Validate MIG Rules

```bash
python3 k8s-extension-prototype/controller/main.py \
  --validate-mig-rules k8s-extension-prototype/mock/mig-rules/a100-40gb.yaml
```

To validate a mock GPU state against those rules:

```bash
python3 k8s-extension-prototype/controller/main.py \
  --validate-mig-rules k8s-extension-prototype/mock/mig-rules/a100-40gb.yaml \
  --gpu-state k8s-extension-prototype/mock/gpu-states/simulation-empty-9-a100.yaml \
  --validate-gpu-state
```
