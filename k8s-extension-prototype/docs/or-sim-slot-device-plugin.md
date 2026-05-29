# OR-SIM Slot Device Plugin

The production workload binding contract is a stable logical MIG slot:

```text
physicalGpuId + slotStart + slotEnd + profile
```

The workload Pod requests that exact slot as a Kubernetes extended resource:

```yaml
resources:
  limits:
    or-sim.io/ampere-gpu0-s4-5-1g: 1
```

The `fast-mig-node-agent --slot-device-plugin` process runs as a kubelet device
plugin on each MIG-capable node. It scans local MIG geometry with `nvidia-smi`,
registers one `or-sim.io/...` resource per observed logical slot, and resolves
the slot to the current MIG UUID in `Allocate`.

The allocation response injects the NVIDIA CDI device:

```text
k8s.device-plugin.nvidia.com/gpu=<current MIG UUID>
```

The MIG UUID is deliberately not the long-term identity. It is only the current
runtime handle for the stable slot. After each MIG reconfiguration, the plugin
rescans and re-registers the resources with the new UUIDs.

The Pod lifecycle executor still verifies the invariant from inside the Pod:

```text
nvidia-smi -L == [registry slot currentMigDeviceUuid]
```

That verification is a safety assertion, not the primary placement mechanism.
