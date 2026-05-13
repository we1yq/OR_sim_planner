# MIGRANT Edge Architecture Smoke Test Report

Date: 2026-05-12  
Cluster: `desktap` control-plane + `rtx1-worker` GPU worker  
Kubeconfig: `~/.kube/or-sim-edge.yaml`

## Summary

Result: PASS

The migrated edge architecture is functional for the currently available hardware:

- MIGRANT control components run on `or-sim-control-plane`.
- GPU Operator node components run on `rtx1-worker`.
- `PhysicalGpuRegistry/default` discovers the single A100 as `rtx1-worker-gpu0`.
- Non-A100 GPUs are ignored.
- Real MIG reconfiguration through the MIGRANT action adapter and GPU Operator works in both directions:
  - `or-sim-empty -> all-3g.20gb`
  - `all-3g.20gb -> or-sim-empty`
- After restore, the A100 returns automatically from `transitioningQueue` to `availableQueue`.

## Cleanup

Removed two zero-byte accidental files from the repo root:

- `get`
- `--kubeconfig`

No backups, reports, manifests, source files, or generated experiment outputs were removed.

Temporary test CRs were removed after the run:

```text
kubectl get migactionplans -n or-sim
No resources found in or-sim namespace.
```

## Notes On Dry-Run Tests

Some tests below use Kubernetes server-side dry-run or tool dry-run modes. These are not substitutes for real hardware tests. They are non-destructive configuration checks used to catch schema, RBAC, manifest, and config-generation failures before changing cluster state.

The real hardware path was separately tested with confirmed MIG label apply operations.

## Local Tests

| Test | Time | Result | Key output |
| --- | ---: | --- | --- |
| Python compile check | 0.03s | PASS | `compileall` completed for `controller`, `migrant_core`, and `tools` |
| Controller test suite | 0.09s | PASS | `ok` |
| A100 MIG rules validation | 0.06s | PASS | `14 abstract templates, 19 physical realizations` |
| Scenario YAML parse | 0.05s | PASS | `scenario_yaml_ok 9` |
| Template core invariants | 0.03s | PASS | `templateCount=14`, `physicalRealizationCount=19`, `voidLikeRewriteCandidateCount=6` |
| Physical ID invariants | 0.03s | PASS | `physical_ids_ok` |

## Configuration Checks

| Test | Time | Result | Key output |
| --- | ---: | --- | --- |
| Controller manifests server dry-run | 0.31s | PASS | Deployments, RBAC, ServiceAccount accepted by API server |
| MIG config installer dry-run | 0.40s | PASS | Would ensure 12 MIGRANT configs in `or-sim-mig-parted-config`; no cluster mutation |
| GPU Operator exec RBAC | 0.04s | PASS | `can-i get pods --subresource=exec`: `yes` |
| Node patch RBAC | 0.05s | PASS | `can-i patch nodes`: `yes` |
| Running pod code check | 0.15s | PASS | empty template maps to `or-sim-empty` |

## Runtime Health

### MIGRANT Pods

Time: 0.05s  
Result: PASS

```text
mig-dry-run-actuator            1/1 Running   or-sim-control-plane
mig-planner-controller          1/1 Running   or-sim-control-plane
physical-gpu-registry-monitor   1/1 Running   or-sim-control-plane
```

### GPU Operator Pods

Time: 0.06s  
Result: PASS

All required GPU Operator pods were healthy. Node-level components were on `rtx1-worker`, while NFD also had a worker pod on the control-plane as expected.

Key pods:

```text
nvidia-mig-manager              1/1 Running   rtx1-worker
nvidia-device-plugin-daemonset  1/1 Running   rtx1-worker
gpu-feature-discovery           1/1 Running   rtx1-worker
nvidia-dcgm-exporter            1/1 Running   rtx1-worker
```

### Registry Discovery

Time: 0.05s  
Result: PASS

```text
queueCounts:
  discovered: 1
  available: 1
  active: 0
  transitioning: 0
  ignored: 3
availableQueue:
  - rtx1-worker-gpu0
```

The A100 binding was:

```text
physicalGpuId: rtx1-worker-gpu0
product: NVIDIA A100-PCIE-40GB
gpuUuid: GPU-03ca4983-f693-39d2-d7e0-25090fe07b2f
currentMigConfig: or-sim-empty
currentMigConfigState: success
cleanliness: empty
state: available
```

Ignored GPUs:

```text
rtx1-worker-gpu1  NVIDIA TITAN RTX
rtx1-worker-gpu2  NVIDIA TITAN RTX
rtx1-worker-gpu3  NVIDIA GeForce RTX 3090
```

### GPU Inventory

MIGRANT registry monitor inventory call:

Time: 1.17s  
Result: PASS

```text
inventory_count 2
GPU 0: NVIDIA A100-PCIE-40GB
GPU 1: NVIDIA TITAN RTX
GPU 2: NVIDIA TITAN RTX
GPU 3: NVIDIA GeForce RTX 3090
```

Direct GPU Operator `nvidia-smi -L`:

Time: 0.19s  
Result: PASS

```text
GPU 0: NVIDIA A100-PCIE-40GB
GPU 1: NVIDIA TITAN RTX
GPU 2: NVIDIA TITAN RTX
GPU 3: NVIDIA GeForce RTX 3090
```

## Real MIG Action Round Trip

### Step 1: Create `3g+3g` Smoke Action Plan

Time: 0.80s  
Result: PASS

Key output:

```text
chosenTemplates:
- 3g+3g
actions:
- clear_template
- configure_full_template
- place_target_layout
nodeName: rtx1-worker
deviceIndex: 0
```

### Step 2: Preflight `3g+3g`

Time: 0.75s  
Result: PASS

Key output:

```text
wouldPatchNodeLabels:
  rtx1-worker:
    nvidia.com/mig.config: all-3g.20gb
gpuOperatorPreflight:
  configMap: or-sim-mig-parted-config
  targetConfigs:
  - all-3g.20gb
  missingTargetConfigs: []
  errors: []
```

### Step 3: Apply `3g+3g`

Time: 45.88s  
Result: PASS

Key output:

```text
targetMigConfig: all-3g.20gb
before:
  migConfig: or-sim-empty
  migConfigState: success
observed:
  migConfig: all-3g.20gb
  migConfigState: success
```

Device-plugin readiness after apply:

```text
pod/nvidia-device-plugin-daemonset-c44k5 condition met
elapsed_s=19.98
```

Stable node resources after apply:

```text
nvidia.com/mig-3g.20gb capacity: 2
nvidia.com/mig-3g.20gb allocatable: 2
```

Actual hardware inventory after apply:

```text
GPU 0: NVIDIA A100-PCIE-40GB
  MIG 3g.20gb Device 0
  MIG 3g.20gb Device 1
```

Registry while configured:

```text
queueCounts:
  discovered: 1
  available: 0
  transitioning: 1
transitioningQueue:
  - rtx1-worker-gpu0
```

### Step 4: Create Restore-To-Empty Action Plan

Time: 0.82s  
Result: PASS

Key output:

```text
chosenTemplates:
- ''
actions:
- clear_template
- configure_full_template
- place_target_layout
```

### Step 5: Preflight Restore-To-Empty

Time: 0.81s  
Result: PASS

Key output:

```text
wouldPatchNodeLabels:
  rtx1-worker:
    nvidia.com/mig.config: or-sim-empty
gpuOperatorPreflight:
  targetConfigs:
  - or-sim-empty
  missingTargetConfigs: []
  errors: []
```

### Step 6: Apply Restore-To-Empty

Time: 40.89s  
Result: PASS

Key output:

```text
targetMigConfig: or-sim-empty
before:
  migConfig: all-3g.20gb
  migConfigState: success
observed:
  migConfig: or-sim-empty
  migConfigState: success
```

Device-plugin readiness after restore:

```text
pod/nvidia-device-plugin-daemonset-cmwtg condition met
elapsed_s=23.17
```

Actual hardware inventory after restore:

```text
GPU 0: NVIDIA A100-PCIE-40GB
GPU 1: NVIDIA TITAN RTX
GPU 2: NVIDIA TITAN RTX
GPU 3: NVIDIA GeForce RTX 3090
```

No MIG devices were present after restore.

### Step 7: Automatic Registry Return To Available

Result: PASS

The query was manual, but the state transition was automatic. The registry monitor observed the restored hardware state and moved the A100 back to available:

```text
queueCounts:
  discovered: 1
  available: 1
  active: 0
  transitioning: 0
  ignored: 3
availableQueue:
  - rtx1-worker-gpu0
transitioningQueue: []
binding:
  cleanliness: empty
  currentMigConfig: or-sim-empty
  currentMigConfigState: success
```

## Observations

1. `PhysicalGpuRegistry` correctly uses GPU Operator exec / `nvidia-smi -L` inventory, so it handles the mixed GPU node correctly.
2. Kubernetes node aggregate label `nvidia.com/gpu.product` is not reliable for mixed-GPU nodes. Registry discovery must continue using per-GPU inventory.
3. After restore-to-empty, the node still showed stale `capacity.nvidia.com/mig-3g.20gb: 2` for a while, while `allocatable.nvidia.com/mig-3g.20gb` was `0`. This did not affect MIGRANT registry availability because registry uses per-GPU MIG device inventory and `or-sim-empty/success`.
4. The final registry state is clean and usable for the next planner allocation.

## Final Cluster State

```text
MIGRANT:
  mig-dry-run-actuator            Running on or-sim-control-plane
  mig-planner-controller          Running on or-sim-control-plane
  physical-gpu-registry-monitor   Running on or-sim-control-plane

GPU:
  rtx1-worker mig.config          or-sim-empty
  rtx1-worker mig.config.state    success
  PhysicalGpuRegistry available   [rtx1-worker-gpu0]
```

## Files Changed For This Validation

Relevant implementation/config fixes already present in the workspace:

- `controller/executor_preview.py`
- `manifests/controller/deployment.yaml`
- `manifests/controller/actuator-deployment.yaml`
- `manifests/controller/registry-monitor-deployment.yaml`
- `manifests/controller/observer-rbac.yaml`
- `manifests/controller/registry-monitor-gpu-operator-rbac.yaml`

