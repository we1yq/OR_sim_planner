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

Apply all stage examples:

```bash
kubectl apply -f k8s-extension-prototype/manifests/examples/migplans/

python3 k8s-extension-prototype/controller/main.py \
  --run-controller \
  --namespace or-sim \
  --controller-max-cycles 4 \
  --poll-interval-s 1

kubectl get migplans -n or-sim
kubectl get configmaps -n or-sim -l mig.or-sim.io/state-kind=canonical-next-state
```

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
