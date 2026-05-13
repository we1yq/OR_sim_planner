# MIGRANT Interface Implementation And Single-A100 Test Report

Date: 2026-05-12  
Cluster: `desktap` RKE2 control-plane + `rtx1-worker` RKE2 GPU worker  
Kubeconfig: `~/.kube/or-sim-edge.yaml`  
GPU under test: one NVIDIA A100-PCIE-40GB on `rtx1-worker`

## Summary

Result: PASS for the interfaces that can be validated on a single A100.

Implemented in this round:

- Real Pod lifecycle execution entrypoints for `MigActionPlan.spec.podLifecyclePreview`.
- A `createOrReuse` workload Pod path using a requested MIG resource.
- A `reloadInPlace` batch-size update path using a projected ConfigMap.
- MigActionPlan status writeback for real Pod lifecycle execution.
- Controller RBAC for Pod create/patch/delete/watch and Pod log reads.
- A short CUDA profile workload runner for profile-backed single-A100 tests.

Tested on the edge cluster:

- A100 `or-sim-empty -> all-3g.20gb -> or-sim-empty`.
- Workload placement onto `nvidia.com/mig-3g.20gb`.
- Batch-size update without deleting or recreating the Pod.
- A profile-backed `resnet50` workload option from the catalog: `profile=3g`, `batch=4`.
- Cleanup and registry return to `availableQueue`.

Important limitation: the profile-backed `resnet50` run used the profile catalog workload/profile/batch and executed real CUDA work inside the assigned MIG partition. It was not a full framework ResNet50 inference server because the repository does not yet contain a real model-serving image or model repository.

## Code Changes

### Real Pod Lifecycle Executor

File:

```text
k8s-extension-prototype/controller/executors/pod_lifecycle_executor.py
```

New callable interfaces:

```text
apply_pod_lifecycle_from_action_plan(...)
```

Supported real actions:

| Preview section | Real behavior |
| --- | --- |
| `createOrReuse` | Creates or reuses a Kubernetes Pod on a selected node and requests a MIG resource such as `nvidia.com/mig-3g.20gb`. |
| `reloadInPlace` | Patches a ConfigMap read by the running Pod and verifies the Pod UID did not change. |
| `deleteOrRecycle` | Deletes the owned smoke Pod and ConfigMap. |
| `drain` | Implemented as a no-traffic stub only. A real router is not present yet. |

### CLI Entry Points

File:

```text
k8s-extension-prototype/controller/main.py
```

New flags:

```text
--create-workload-lifecycle-smoke-action-plan
--apply-pod-lifecycle-from-action-plan
--confirm-real-pod-apply
--workload-smoke-node
--workload-smoke-mig-resource
--workload-smoke-name
--workload-smoke-initial-batch-size
--workload-smoke-updated-batch-size
--workload-smoke-image
--cleanup-workload-smoke
```

### Controller RBAC

File:

```text
k8s-extension-prototype/manifests/controller/rbac.yaml
```

Added access for:

```text
pods: get, list, create, patch, update, delete, watch
pods/log: get
```

### Profile Workload Runner

File:

```text
k8s-extension-prototype/tools/test_workloads/cuda_profile_workload.cu
```

This is a short CUDA runner used to exercise a selected workload/profile/batch from the profile catalog on an assigned MIG device.

## Test 1: Compile Checks

Result: PASS

Command:

```text
python3 -m compileall \
  k8s-extension-prototype/controller/executors/pod_lifecycle_executor.py \
  k8s-extension-prototype/controller/main.py
```

Output:

```text
Compiling 'k8s-extension-prototype/controller/executors/pod_lifecycle_executor.py'...
Compiling 'k8s-extension-prototype/controller/main.py'...
```

CUDA runner compilation:

```text
nvcc release: 12.8, V12.8.93
target: sm_80
binary: /tmp/or-sim-profile-workload/cuda_profile_workload
size: 703K
```

The first default nvcc build failed in the Pod with an unsupported PTX toolchain error. Recompiling for `sm_80` fixed it.

## Test 2: Workload Lifecycle Executor Smoke

Result: PASS

Action plan:

```text
iface-test-workload
```

Tested actions:

```text
createOrReuse: smoke workload on nvidia.com/mig-3g.20gb
reloadInPlace: batch 4 -> 8
```

Key result:

```yaml
createdOrReused:
- podName: or-sim-iface-test-workload-smoke-0-3-3g
  nodeName: rtx1-worker
  requestedMigResource: nvidia.com/mig-3g.20gb
  batchSize: '4'
  podUid: 7d451759-3629-4387-a667-649e0cb3dc8c
  reused: false
  readySeconds: 1.666
  initialBatchObservedSeconds: 0.017
reloads:
- oldBatchSize: '4'
  newBatchSize: '8'
  podUidBeforeUpdate: 7d451759-3629-4387-a667-649e0cb3dc8c
  podUidAfterUpdate: 7d451759-3629-4387-a667-649e0cb3dc8c
  podRecreatedDuringBatchUpdate: false
  observedSecondsAfterPatch: 2.015
timingsSeconds:
  total: 3.717
success: true
```

The Pod saw:

```text
GPU 0: NVIDIA A100-PCIE-40GB
  MIG 3g.20gb Device 0
tick=0 batch_size=4
tick=2 batch_size=8
```

## Test 3: Profile-Backed Workload On MIG

Result: PASS

Profile catalog option:

```text
catalog: manifests/examples/profile-catalogs/catalogs/resnet50.yaml
workload: resnet50
family: cv
profile: 3g
batch: 4
mu: 788.767944
e2eMs: 5.0712
peakMemMb: 166.0
fit: true
```

MIG setup:

```text
targetMigConfig: all-3g.20gb
before: or-sim-empty/success
observed: all-3g.20gb/success
elapsed_s=40.47
```

Device-plugin readiness:

```text
pod/nvidia-device-plugin-daemonset-94dd6 condition met
elapsed_s=28.76
```

Node resources:

```text
nvidia.com/mig-3g.20gb capacity: 2
nvidia.com/mig-3g.20gb allocatable: 2
```

Pod:

```text
name: profile-resnet50-3g-b4
namespace: or-sim
node: rtx1-worker
image: nvidia/cuda:12.4.1-base-ubuntu22.04
limit: nvidia.com/mig-3g.20gb=1
```

Pod state:

```text
phase: Succeeded
pod startTime: 2026-05-12T04:40:06Z
container startedAt: 2026-05-12T04:40:07Z
container finishedAt: 2026-05-12T04:40:28Z
exitCode: 0
```

Workload output:

```text
profile_workload_start workload=resnet50 profile=3g batch=4 seconds=20
cuda_device name=NVIDIA A100-PCIE-40GB MIG 3g.20gb multiprocessors=42 global_mem_mib=19968
profile_workload_done workload=resnet50 profile=3g batch=4 iterations=6960 elapsed_s=20.006
```

`nvidia-smi` captured while the Pod was running:

```text
MIG devices:
GPU 0 GI 1 CI 0 MIG 0: 38MiB / 19968MiB
GPU 0 GI 2 CI 0 MIG 1: 317MiB / 19968MiB

Processes:
GPU 0 GI 2 CI 0 PID 2281505 C /opt/or-sim/cuda_profile_workload 270MiB
```

Interpretation:

- The workload ran on a real MIG `3g.20gb` partition.
- The selected workload/profile/batch came from the profile catalog.
- The GPU process was visible from `nvidia-smi` on `rtx1-worker`.
- This validates resource placement and GPU execution, but not full ResNet50 model serving.

## Cleanup And Restore

Deleted temporary workload resources:

```text
pod "profile-resnet50-3g-b4" deleted
configmap "profile-workload-runner" deleted
```

Restored the A100 to empty MIG mode:

```text
targetMigConfig: or-sim-empty
before: all-3g.20gb/success
observed: or-sim-empty/success
elapsed_s=45.48
```

Device-plugin readiness after restore:

```text
pod/nvidia-device-plugin-daemonset-qwhxr condition met
elapsed_s=20.83
```

Final GPU inventory:

```text
GPU 0: NVIDIA A100-PCIE-40GB
GPU 1: NVIDIA TITAN RTX
GPU 2: NVIDIA TITAN RTX
GPU 3: NVIDIA GeForce RTX 3090
```

No MIG child devices were present after restore.

Final registry status:

```text
queueCounts:
  active: 0
  available: 1
  discovered: 1
  ignored: 3
  missingActive: 0
  transitioning: 0
availableQueue:
- rtx1-worker-gpu0
transitioningQueue: []
currentMigConfig: or-sim-empty
currentMigConfigState: success
```

Deleted temporary action plans:

```text
profile-test-a100-3g3g
profile-test-a100-empty
iface-test-a100-3g3g
iface-test-a100-empty
iface-test-workload
```

Local cleanup:

```text
removed: k8s-extension-prototype/backups/kind-or-sim-dev-20260512-133033/
removed: Python __pycache__ directories
removed: /tmp/or-sim-profile-workload/
kept: k8s-extension-prototype/backups/kind-or-sim-dev-20260512-133033.tar.gz
```

## Interfaces Still Missing Or Not Fully Implemented

### Can Be Further Tested On One A100

| Interface | Current state | Next one-A100 test |
| --- | --- | --- |
| Full model-serving workload image | Not implemented. Current test uses a profile-backed CUDA runner. | Add a real ResNet50/GPT2 serving image and run one inference loop on `nvidia.com/mig-3g.20gb`. |
| Runtime batch API | Current implementation uses ConfigMap volume reload. | Add an HTTP/gRPC endpoint in the workload process and make `reloadInPlace` call it. |
| Pod deletion after no-traffic drain | Delete path exists for owned smoke Pods. | Create a plan with `deleteOrRecycle` and verify deletion after a no-traffic stub. |
| Observed Pod-to-MIG assignment | Partially inferred through Pod resource requests and container `nvidia-smi`. | Record assigned MIG UUID into `ObservedClusterState` or `MigActionPlan.status`. |
| Real action-plan status detail | Pod lifecycle summary is written under `status.validated.realPodLifecycle`. | Add per-step conditions and timestamps as first-class status fields. |

### Requires More Than One A100 For Real Validation

| Interface | Why multiple A100s are needed |
| --- | --- |
| `target_first` reconfiguration with target-side warm capacity | Needs an old physical GPU and a separate target physical GPU. |
| `activeQueue` multi-GPU allocation ordering | One A100 cannot validate ordering or contention across multiple A100s. |
| Cross-GPU workload takeover | Needs one serving instance on an old GPU and takeover capacity on another GPU. |
| Multi-GPU router cutover | Needs at least two active serving targets to verify traffic shift. |
| Multi-GPU drain and cleanup after takeover | Needs old and new serving locations at the same time. |
| Multi-A100 canonicalization after real execution | One A100 cannot validate physical ID reuse, free-pool policy, or cross-GPU binding preservation. |

### Requires External Runtime Components

| Interface | Missing component |
| --- | --- |
| Real router/drain adapter | No router, queue, or inflight request source exists yet. Current drain is a no-traffic stub. |
| Exact MIG UUID placement | Kubernetes device-plugin scheduling selects by resource type, not by exact MIG UUID. Needs scheduler/admission/CDI integration or a device assignment observer. |
| Production model server lifecycle | No model-serving image/repository is present in this repo. |
| Image distribution | Development still relies on local images/imports for controller images; an internal registry is the right long-term path. |

## Final State

The edge cluster was left in the clean baseline state:

```text
MIGRANT controllers: Running on or-sim-control-plane
A100 MIG config: or-sim-empty/success
PhysicalGpuRegistry/default: availableQueue=[rtx1-worker-gpu0], transitioningQueue=[]
Temporary workload Pods: none
Temporary workload ConfigMaps: none
Temporary MigActionPlans: none from this test
```
