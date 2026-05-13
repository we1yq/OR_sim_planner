# Abstract Transition Actions

This document is the source of truth for MIGRANT abstract transition actions.
The paper-oriented DAG figures in `reports/abstract-action-dags/` render these
actions, but this document owns the wording and queue/binding semantics.

## Queue and Binding Semantics

```text
pendingLogicalGpuId:
  The physical GPU is reserved for a logical GPU while it is being configured or cleaned.
  It belongs to transitionQueue, not activeQueue.

activeLogicalGpuId:
  The physical GPU is the current active binding for that logical GPU.
  It belongs to activeQueue.

availableQueue:
  No pendingLogicalGpuId and no activeLogicalGpuId; clean / or-sim-empty.

transitionQueue:
  Has pendingLogicalGpuId and no activeLogicalGpuId; configure/clear/prepare is in progress.

activeQueue:
  Has activeLogicalGpuId and no pendingLogicalGpuId; planner-owned active GPU.
```

## Unified Abstract Actions

The transition planner emits these rules as ordered fine-grained actions. The
planner-facing `type` values intentionally stay executable:

```text
Allocate GPU -> allocate_gpu
Configure Template -> configure_full_template
Bind GPU -> bind_target_gpu
Resolve UUIDs -> observe_mig_devices
Deploy Pods -> deploy_target_workloads
Activate Route -> activate_serving_route
Stop GPU/Slot Traffic -> stop_accepting_new
Accept Queued Requests -> accept_queued_requests
Reroute Queued Requests -> reroute_queued_tasks
Wait Drain -> mark_draining_instance
Delete Pods -> delete_pods
Delete Pod -> delete_pods with one target slot
Clear GPU Binding -> clear_gpu_binding
Clear Template -> clear_template
Return GPU -> return_gpu
Patch Config -> patch_batch_config
Apply Batch -> apply_batch
Verify Batch -> verify_batch
```

```text
1. create-target-gpu

-> Allocate GPU: reserve a physical GPU from availableQueue; assign pendingLogicalGpuId
-> Configure Template: move GPU to transitionQueue; apply target MIG template on the physical GPU
-> Bind GPU: bind activeLogicalGpuId to this physical GPU; remove pendingLogicalGpuId; move GPU to activeQueue
-> Resolve UUIDs: map each target slot to real MIG device UUID
-> Deploy Pods: deploy workload pods on resolved target slots
-> Activate Route: route new requests to deployed pods
```

```text
2. delete-gpu

Main source GPU line:
-> Stop GPU Traffic: stop new requests entering pods on the source GPU
-> [optional] Reroute Queued Requests: move queued requests to stable serving slots
-> [optional] Wait Drain: wait until queued requests == 0 and running work == 0
-> Delete Pods: delete workload pods on the source GPU; pass all source slots
-> Clear GPU Binding: remove activeLogicalGpuId from the physical GPU; assign pendingLogicalGpuId; move GPU to transitionQueue
-> Clear Template: reset MIG template / set or-sim-empty
-> Return GPU: remove pendingLogicalGpuId; move physical GPU back to availableQueue

Optional destination line:
-> [optional] Accept Queued Requests: stable serving slot accepts rerouted queued requests

Cross-line edge:
Accept Queued Requests -> -> Reroute Queued Requests
```

```text
3. in-place-reconfiguration

Main GPU line:
-> Stop GPU Traffic: stop new requests entering pods on this GPU
-> [optional] Reroute Queued Requests: move queued requests to stable serving slots
-> [optional] Wait Drain: wait until queued requests == 0 and running work == 0
-> Delete Pods: delete current workload pods; pass all current slots
-> Clear GPU Binding: remove activeLogicalGpuId; keep/assign pendingLogicalGpuId; move GPU to transitionQueue
-> Configure Template: keep GPU in transitionQueue; apply target MIG template on the same physical GPU
-> Bind GPU: bind activeLogicalGpuId back to this physical GPU; remove pendingLogicalGpuId; move GPU to activeQueue
-> Resolve UUIDs: map each target slot to real MIG device UUID
-> Deploy Pods: deploy target workload pods
-> Activate Route: route new requests to target pods

Optional destination line:
-> [optional] Accept Queued Requests: stable serving slot accepts rerouted queued requests

Cross-line edge:
Accept Queued Requests -> -> Reroute Queued Requests
```

```text
4. target-first-reconfiguration

Target GPU line:
-> Allocate GPU: reserve a physical GPU from availableQueue; assign pendingLogicalGpuId
-> Configure Template: move GPU to transitionQueue; apply target MIG template on the physical GPU
-> Bind GPU: bind activeLogicalGpuId to this physical GPU; remove pendingLogicalGpuId; move GPU to activeQueue
-> Resolve UUIDs: map each target slot to real MIG device UUID
-> Deploy Pods: deploy workload pods on resolved target slots
-> Activate Route: route new requests to deployed pods

Old GPU line:
-> Stop GPU Traffic: stop new requests entering pods on the source GPU
-> Wait Drain: wait until queued requests == 0 and running work == 0
-> Delete Pods: delete workload pods on the source GPU; pass all source slots
-> Clear GPU Binding: remove activeLogicalGpuId from the physical GPU; assign pendingLogicalGpuId; move old GPU to transitionQueue
-> Clear Template: reset MIG template / set or-sim-empty
-> Return GPU: remove pendingLogicalGpuId; move old physical GPU back to availableQueue

Cross-line edge:
Clear GPU Binding -> -> Bind GPU
```

```text
5. workload-replacement

Main source slot line:
-> Stop Slot Traffic: stop new requests entering this slot/pod
-> [optional] Reroute Queued Requests: move queued requests to stable serving slots
-> [optional] Wait Drain: wait until queued requests == 0 and running work == 0
-> Delete Pods: delete old workload pod on this slot
-> Deploy Pod: deploy replacement workload pod on the same slot
-> Activate Route: route new requests to replacement pod

Optional destination line:
-> [optional] Accept Queued Requests: stable serving slot accepts rerouted queued requests

Cross-line edge:
Accept Queued Requests -> -> Reroute Queued Requests
```

```text
6. bridge-workload-replacement

Bridge slot line:
-> Deploy Bridge Pod: deploy temporary old-workload pod on compatible free slot
-> Accept Queued Requests: bridge pod accepts rerouted queued requests
-> Drain Bridge: wait until bridge queued requests == 0 and running work == 0
-> Delete Bridge Pod: delete temporary bridge pod

Original slot line:
-> Stop Slot Traffic: stop new requests entering this slot/pod
-> Reroute Queued Requests: move queued requests to bridge pod
-> Wait Drain: wait until original pod running work == 0
-> Delete Pods: delete old workload pod on this slot
-> Deploy Pod: deploy replacement workload pod on the same slot
-> Activate Route: route new requests to replacement pod

Cross-line edge:
Reroute Queued Requests -> -> Accept Queued Requests
```

```text
7. batch-update

-> Patch Config: update batch size in workload/runtime config
-> Apply Batch: runtime reloads or applies new batch size without pod deletion
-> Verify Batch: confirm new batch size is active in serving/runtime metrics
-> Activate Route: keep or reactivate route to the updated pod
```
