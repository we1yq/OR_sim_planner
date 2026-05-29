# Timings v4

All durations are seconds. This run uses node-agent transaction confirmation plus a short Node allocatable wait.

| Event | Since ArrivalSnapshot | Timestamp / duration |
|---|---:|---|
| ArrivalSnapshot created | 0.000 | 2026-05-29T05:46:49Z |
| MigActionPlan created | 0.000 | 2026-05-29T05:46:49Z |
| executor started | 0.916 | 2026-05-29T05:46:49.91600481Z |
| MIG apply started | 0.930 | 2026-05-29T05:46:49.930336265Z |
| MIG apply finished | 4.469 | 2026-05-29T05:46:53.468533163Z |
| CDI refresh started | 4.469 | 2026-05-29T05:46:53.468548622Z |
| CDI refresh finished | 6.842 | 2026-05-29T05:46:55.842444789Z |
| MIG UUID resolve started | 6.842 | 2026-05-29T05:46:55.842445239Z |
| MIG UUID resolve finished | 7.172 | 2026-05-29T05:46:56.171835347Z |
| UUID resource allocatable wait started | 7.172 | 2026-05-29T05:46:56.17184251Z |
| UUID resource allocatable wait finished | 8.182 | 2026-05-29T05:46:57.182429287Z |
| runtime deploy started | 8.795 | 2026-05-29T05:46:57.795431521Z |
| runtime deployments created | 8.804 | 2026-05-29T05:46:57.804183328Z |
| runtime ready and CUDA UUID verified | 14.020 | 2026-05-29T05:47:03.020348584Z |
| route synced | 14.023 | 2026-05-29T05:47:03.022828688Z |
| executor finished | 14.023 | 2026-05-29T05:47:03.022834299Z |

| Executor sub-step | Duration |
|---|---:|
| phase 0: `allocate_gpu` | 0.000 |
| phase 1: `configure_full_template` | 3.538 |
| phase 2: `bind_target_gpu` | 0.000 |
| phase 3: `register_mig_devices` | 3.714 |
| phase 4: `deploy_target_workloads` | 0.622 |
| phase 5: `activate_serving_route` | 5.219 |
| executor metric `migApply` | 3.538 |
| executor metric `refreshCDI` | 2.374 |
| executor metric `resolveMIGUUIDs` | 0.329 |
| executor metric `uuidResourcePropagationAndStableWait` | 1.011 |
| executor metric `runtimeDeploymentCreate` | 0.009 |
| executor metric `runtimeReadyAndCUDAVerify` | 5.216 |
| executor metric `routeSync` | 0.002 |
| executor metric `total` | 13.107 |

Two-stage UUID resource confirmation:

| Metric | Value |
|---|---|
| `nodeAgentRegisteredTargetResources` | `True` |
| `nodeAgentRegisteredTargetMissing` | `[]` |
| `allocatableTimeoutSeconds` | `2` |
| `allocatablePolls` | `3` |
| `allocatableStablePollsRequired` | `1` |
| `allocatableFinalStablePolls` | `1` |
| `allocatableLastMissing` | `[]` |
| `applySlotsNodeAgent.devicePluginRefresh.success` | `True` |
| `applySlotsNodeAgent.devicePluginRefresh.seconds` | `0.832699996` |
| `applySlotsNodeAgent.devicePluginRefresh.registeredResources` | `5` |
| `refreshCDINodeAgent.devicePluginRefresh.success` | `True` |
| `refreshCDINodeAgent.devicePluginRefresh.seconds` | `0.39753827` |
| `refreshCDINodeAgent.devicePluginRefresh.registeredResources` | `5` |

Runtime readiness:

| Model | Health ready after deploy | CUDA process after deploy | Device resource | Expected UUID |
|---|---:|---:|---|---|
| gpt2 | 1.721 | 3.474 | `or-sim.io/mig-a9aaa9b9-3415-5b83-baab-d52b391db3ac` | `MIG-a9aaa9b9-3415-5b83-baab-d52b391db3ac` |
| llama | 1.720 | 5.216 | `or-sim.io/mig-f61eae53-d4d7-5315-a0c1-03a5baa8fb05` | `MIG-f61eae53-d4d7-5315-a0c1-03a5baa8fb05` |