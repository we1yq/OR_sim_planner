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
ConfigMap, canonical next-state ConfigMap, and an executor preview, but it
remains `dryRun: true` and is only automatically approved when an
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
exist, that the action count matches, and that the executor preview is explicitly
preview-only, then marks the plan `SucceededDryRun`. It still does not execute
MIG, Pod, scheduler, or Node changes.

```bash
kubectl get deployment mig-dry-run-actuator -n or-sim
kubectl get migactionplans -n or-sim
```

The intended production executor is `nvidia-gpu-operator`: a future actuator
should translate approved action plans into NVIDIA GPU Operator/MIG Manager
inputs, observe the real post-action GPU state, and only then canonicalize the
observed state for the next planning round. The controller should not jump
straight to privileged `nvidia-smi` commands unless the operator path is
insufficient and the safety model is explicit.

`MigActionPlan.spec.executorPreview` is the current interface draft for that
translation. It maps the canonical target layout into per-physical-GPU target MIG
geometry and, when a real observer supplies `physicalGpuId -> nodeName/deviceIndex`
bindings, shows the node labels a GPU Operator/MIG Manager actuator would patch:

```text
spec.executorPreview.gpuTargets[]
  logicalGpuId
  physicalGpuId
  nodeName
  deviceIndex
  targetTemplate
  targetInstances

spec.executorPreview.migManagerTargetConfigs[]
spec.executorPreview.wouldPatchNodeLabels:
  <nodeName>:
    nvidia.com/mig.config: <generated-config-name>
```

In kind, `nodeName` and `deviceIndex` are normally unresolved because there is no
NVIDIA GPU node inventory. That is expected: kind validates the CRD/controller/
status/ConfigMap/action-plan/preview flow, not real MIG hardware. A real actuator
must first observe physical GPU bindings and current MIG layouts, then convert
the approved preview into GPU Operator/MIG Manager config, execute it, observe
the actual result, and canonicalize that observed state.

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
