# MIGRANT Kubernetes Prototype

This directory contains the new Kubernetes controller/operator prototype for the
MIG planning system.

The existing simulation notebook remains a read-only research reference. New
prototype code and Kubernetes manifests should live here.

## Current Phase

Phase 5: mock-based controller with file inputs.

The controller reads workload request YAML, mock GPU state, and normalized
profile catalogs. It prints a dry-run phase-greedy action plan. No real GPU or MIG
operations are performed.

## Safety Rules

- Dry-run first.
- Do not modify the existing simulation notebook.
- Do not change `kube-scheduler`.
- Do not execute real MIG reconfiguration commands.
- Do not delete or recreate workload Pods automatically.
- Use mock GPU/MIG state on the local kind cluster.

## Directory Layout

```text
k8s-extension-prototype/
  README.md
  docs/
    phase3-architecture.md
    add-gpu-server-runbook.md
  manifests/
    README.md
    namespace.yaml
    crds/
      workloadrequest-crd.yaml
      migplan-crd.yaml
    examples/
      workloadrequests/
        gpt2.yaml
        llama.yaml
        resnet50.yaml
        vgg16.yaml
        vit_base.yaml
      mock-gpu-states/
        one-a100-empty-configmap.yaml
  controller/
    README.md
    main.py
    models.py
    io_utils.py
    state_adapter.py
    feasible_options.py
    mig_rules.py
    scenario_loader.py
  migrant_core/
    README.md
    state.py
    physical_ids.py
    templates.py
    preserve.py
  tools/
    extract_profile_catalog.py
  mock/
    README.md
    mig-rules/
      a100-40gb.yaml
    policies/
      migrant-default.yaml
    profile-catalogs/
      gpt2.yaml
      llama.yaml
      resnet50.yaml
      vgg16.yaml
      vit_base.yaml
    gpu-states/
      empty-cluster.yaml
      one-a100-empty.yaml
      migrant-empty-9-a100.yaml
    scenarios/
      stage0.yaml
      stage1.yaml
      stage2.yaml
      stage3.yaml
  examples/
    README.md
    dry-run-plan.yaml
```

## Planned Flow

```text
WorkloadRequest YAML
        +
Mock GPU/MIG State YAML
        +
Profile Catalog YAML
        |
        v
MIG Planner Controller
        |
        v
Dry-run MigPlan
```

## Traffic Experiments And Arrival Input

MIGRANT's control-plane input is a per-window arrival-rate snapshot. Synthetic
traffic generators and trace replayers should write the same Kubernetes object:

```yaml
apiVersion: mig.or-sim.io/v1alpha1
kind: ArrivalSnapshot
metadata:
  name: arrival-t00030
  namespace: or-sim
spec:
  source: poisson-generator
  mode: poisson
  windowSeconds: 30
  unit: requests_per_second
  targetArrival:
    resnet50: 120.0
    bert: 60.0
```

A `MigPlan` should reference the snapshot and the actual observed source state:

```yaml
spec:
  observedStateRef: cluster-observed-state
  arrivalSnapshotRef: arrival-t00030
```

Recommended experiment modes:

1. `static`: fixed request rates for sanity and ablation.
2. `poisson`: time-varying synthetic arrivals for one-week SLO experiments.
3. `traceReplay`: public or internal trace windows converted to arrival rates.
4. `stress`: burst, overload, and workload-mix shift robustness.

All modes should emit:

```text
ArrivalSnapshot per control window
request event log when request-level simulation is enabled
latency/SLO report
```

Generate an example `ArrivalSnapshot`:

```bash
python3 k8s-extension-prototype/tools/traffic_experiment.py \
  --mode poisson \
  --name arrival-t00030 \
  --epoch 1 \
  --window-seconds 30 \
  --workload-rate resnet50=120 \
  --workload-rate bert=60
```

`arrivalSnapshotConfigMap` remains as a backward-compatible/test-only input.
The CRD path, `arrivalSnapshotRef`, is the preferred runtime API.

## Long-Running Hardware Monitor

Real A100 discovery is maintained by `PhysicalGpuRegistry/default`. The current
inventory provider is `GpuOperatorExecProvider`: it runs `nvidia-smi -L` inside
a GPU Operator pod to collect real GPU UUIDs and MIG device UUIDs. This is kept
behind the observer interface so a future node-agent/exporter can replace it
without changing planner logic.

For long-running target clusters:

```bash
kubectl apply -f k8s-extension-prototype/manifests/controller/registry-monitor-gpu-operator-rbac.yaml
kubectl apply -f k8s-extension-prototype/manifests/controller/registry-monitor-deployment.yaml
```

The RBAC is scoped to the `gpu-operator` namespace. The monitor avoids running
`nvidia-smi -L` while any node has `nvidia.com/mig.config.state=pending`; it
reuses cached GPU UUID bindings and refreshes MIG device UUIDs after
success/failed.

## Regenerate Profile Catalogs

```bash
python3 k8s-extension-prototype/tools/extract_profile_catalog.py
```

This reads the existing `profile/*.csv` files and writes one catalog per
workload under:

```text
k8s-extension-prototype/mock/profile-catalogs/
```

## Run Mock Controller

```bash
python3 k8s-extension-prototype/controller/main.py
```

The default run uses:

```text
manifests/examples/workloadrequests/gpt2.yaml
mock/gpu-states/one-a100-empty.yaml
mock/profile-catalogs/gpt2.yaml
```

## Local Kubernetes Namespace

The prototype uses the namespace:

```text
or-sim
```

Create it with:

```bash
kubectl apply -f manifests/namespace.yaml
```

Verify:

```bash
kubectl get namespace or-sim
```
