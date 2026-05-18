# Fast MIG Node Agent

The fast MIG node agent is the hardware actuator that replaces the NVIDIA GPU
Operator MIG Manager on the fast path. It does not wait for Kubernetes extended
resources to be re-advertised. Instead, the controller observes the resulting
MIG UUIDs and binds workloads by `NVIDIA_VISIBLE_DEVICES=<MIG-UUID>`.

## Slot Patch Contract

`patch-slots` executes a concrete slot rewrite:

```bash
fast-mig-node-agent -gpu-index 0 patch-slots \
  <delete-spec> \
  <create-spec> \
  <preserve-spec>
```

Each spec is a comma-separated list of `start:size:profile[:migUuid]`.

For example, to transform this observed placement:

```text
[0,1] 1g
[1,2] 1g
[2,3] 1g
[3,4] 1g
[4,6] 2g
[6,7] 1g
```

into `2+2+2+1`, the planner must choose the exact 1g slots to merge:

```bash
fast-mig-node-agent -gpu-index 0 patch-slots \
  0:1:1g,1:1:1g,2:1:1g,3:1:1g \
  0:2:2g,2:2:2g \
  4:2:2g,6:1:1g
```

The agent verifies that:

- delete slots exist before the patch;
- create slots are covered by deleted space;
- preserve slots do not overlap deleted slots;
- preserve slots still exist after the patch;
- preserve MIG UUIDs, when provided, remain visible after the patch;
- delete MIG UUIDs, when provided, have no active compute processes.

If any compute process exists on the node and a delete slot does not include a
MIG UUID, the agent refuses the patch. This keeps the execution path
conservative until the registry can provide UUID-level ownership for every slot.

## Control-Plane Role

The intended control loop is:

```text
observer -> registry(slot, placement, MIG UUID, workload/busy)
planner  -> concrete slot patch
agent    -> nvidia-smi MIG mutation
observer -> registry refresh
pod adapter -> NVIDIA_VISIBLE_DEVICES=<MIG-UUID>
```

The agent is not an observer. It only mutates hardware and returns enough output
for the controller to verify the patch. The registry remains the source of truth
for slot-to-UUID and slot-to-workload ownership.
