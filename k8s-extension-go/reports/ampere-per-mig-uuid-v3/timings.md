# Timings v3

All durations are seconds. This run includes active node-agent -> device-plugin refresh and node-agent transaction reporting.

| Event | Since ArrivalSnapshot | Timestamp / duration |
|---|---:|---|
| ArrivalSnapshot created | 0.000 | 2026-05-29T05:34:49Z |
| MigActionPlan created | 0.000 | 2026-05-29T05:34:49Z |
| executor started | 1.025 | 2026-05-29T05:34:50.025450404Z |
| MIG apply started | 1.043 | 2026-05-29T05:34:50.043340853Z |
| MIG apply finished | 4.434 | 2026-05-29T05:34:53.43356794Z |
| CDI refresh started | 4.434 | 2026-05-29T05:34:53.433596324Z |
| CDI refresh finished | 6.894 | 2026-05-29T05:34:55.893857602Z |
| MIG UUID resolve started | 6.894 | 2026-05-29T05:34:55.893857952Z |
| MIG UUID resolve finished | 7.266 | 2026-05-29T05:34:56.265514078Z |
| UUID resource allocatable wait started | 7.266 | 2026-05-29T05:34:56.265518256Z |
| UUID resource allocatable wait finished | 15.328 | 2026-05-29T05:35:04.327539185Z |
| runtime deploy started | 16.091 | 2026-05-29T05:35:05.090536043Z |
| runtime deployments created | 16.100 | 2026-05-29T05:35:05.100319643Z |
| runtime ready and CUDA UUID verified | 20.332 | 2026-05-29T05:35:09.332310322Z |
| route synced | 20.335 | 2026-05-29T05:35:09.334719256Z |
| executor finished | 20.335 | 2026-05-29T05:35:09.334724305Z |

| Executor sub-step | Duration |
|---|---:|
| phase 0: `allocate_gpu` | 0.000 |
| phase 1: `configure_full_template` | 3.390 |
| phase 2: `bind_target_gpu` | 0.000 |
| phase 3: `register_mig_devices` | 10.894 |
| phase 4: `deploy_target_workloads` | 0.773 |
| phase 5: `activate_serving_route` | 4.234 |
| executor metric `migApply` | 3.390 |
| executor metric `refreshCDI` | 2.460 |
| executor metric `resolveMIGUUIDs` | 0.372 |
| executor metric `uuidResourcePropagationAndStableWait` | 8.062 |
| executor metric `runtimeDeploymentCreate` | 0.010 |
| executor metric `runtimeReadyAndCUDAVerify` | 4.232 |
| executor metric `routeSync` | 0.002 |
| executor metric `total` | 19.309 |

Active refresh / transaction metrics:

| Metric | Value |
|---|---|
| `uuidResourceTargetCount` | `2` |
| `allocatablePolls` | `17` |
| `allocatableStablePollsRequired` | `3` |
| `allocatableFinalStablePolls` | `3` |
| `allocatableLastMissing` | `[]` |
| `applySlotsNodeAgent.migSlotCount` | `5` |
| `applySlotsNodeAgent.expectedResources` | `5` |
| `applySlotsNodeAgent.devicePluginRefresh.success` | `True` |
| `applySlotsNodeAgent.devicePluginRefresh.seconds` | `0.82718166` |
| `applySlotsNodeAgent.devicePluginRefresh.registeredResources` | `5` |
| `refreshCDINodeAgent.migSlotCount` | `5` |
| `refreshCDINodeAgent.expectedResources` | `5` |
| `refreshCDINodeAgent.devicePluginRefresh.success` | `True` |
| `refreshCDINodeAgent.devicePluginRefresh.seconds` | `0.379372266` |
| `refreshCDINodeAgent.devicePluginRefresh.registeredResources` | `5` |

Runtime readiness:

| Model | Health ready after deploy | CUDA process after deploy | Device resource | Expected UUID |
|---|---:|---:|---|---|
| gpt2 | 1.642 | 3.164 | `or-sim.io/mig-a9aaa9b9-3415-5b83-baab-d52b391db3ac` | `MIG-a9aaa9b9-3415-5b83-baab-d52b391db3ac` |
| llama | 1.642 | 4.232 | `or-sim.io/mig-f61eae53-d4d7-5315-a0c1-03a5baa8fb05` | `MIG-f61eae53-d4d7-5315-a0c1-03a5baa8fb05` |