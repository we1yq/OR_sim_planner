# Timings v2

All durations are seconds.

| Event | Since ArrivalSnapshot | Timestamp / duration |
|---|---:|---|
| ArrivalSnapshot created | 0.000 | 2026-05-29T05:15:13Z |
| MigActionPlan created | 0.000 | 2026-05-29T05:15:13Z |
| executor started | 0.635 | 2026-05-29T05:15:13.635017892Z |
| MIG apply started | 0.656 | 2026-05-29T05:15:13.655526562Z |
| MIG apply finished | 2.910 | 2026-05-29T05:15:15.909700962Z |
| CDI refresh started | 2.910 | 2026-05-29T05:15:15.909718094Z |
| CDI refresh finished | 4.545 | 2026-05-29T05:15:17.545433518Z |
| MIG UUID resolve started | 4.545 | 2026-05-29T05:15:17.545434479Z |
| MIG UUID resolve finished | 5.698 | 2026-05-29T05:15:18.698436124Z |
| UUID resource allocatable wait started | 5.698 | 2026-05-29T05:15:18.698441113Z |
| UUID resource allocatable wait finished | 8.220 | 2026-05-29T05:15:21.220327051Z |
| runtime deploy started | 8.634 | 2026-05-29T05:15:21.633919473Z |
| runtime deployments created | 8.643 | 2026-05-29T05:15:21.642752491Z |
| runtime ready and CUDA UUID verified | 13.074 | 2026-05-29T05:15:26.074445158Z |
| route synced | 13.076 | 2026-05-29T05:15:26.076431049Z |
| executor finished | 13.076 | 2026-05-29T05:15:26.076434325Z |

| Executor sub-step | Duration |
|---|---:|
| phase 0: `allocate_gpu` | 0.000 |
| phase 1: `configure_full_template` | 2.254 |
| phase 2: `bind_target_gpu` | 0.000 |
| phase 3: `register_mig_devices` | 5.311 |
| phase 4: `deploy_target_workloads` | 0.422 |
| phase 5: `activate_serving_route` | 4.434 |
| executor metric `migApply` | 2.254 |
| executor metric `refreshCDI` | 1.636 |
| executor metric `resolveMIGUUIDs` | 1.153 |
| executor metric `uuidResourcePropagationAndStableWait` | 2.522 |
| executor metric `runtimeDeploymentCreate` | 0.009 |
| executor metric `runtimeReadyAndCUDAVerify` | 4.432 |
| executor metric `routeSync` | 0.002 |
| executor metric `total` | 12.441 |

Allocatable wait metrics:

| Metric | Value |
|---|---|
| `allocatableFinalStablePolls` | `3` |
| `allocatableLastMissing` | `[]` |
| `allocatablePollIntervalMs` | `500` |
| `allocatablePolls` | `6` |
| `allocatableStablePollsRequired` | `3` |
| `allocatableTargets` | `['ampere/or-sim.io/mig-a9aaa9b9-3415-5b83-baab-d52b391db3ac', 'ampere/or-sim.io/mig-f61eae53-d4d7-5315-a0c1-03a5baa8fb05']` |
| `uuidResourceTargetCount` | `2` |

Runtime readiness:

| Model | Health ready after deploy | CUDA process after deploy | Device resource | Expected UUID |
|---|---:|---:|---|---|
| gpt2 | 2.434 | 4.428 | `or-sim.io/mig-a9aaa9b9-3415-5b83-baab-d52b391db3ac` | `MIG-a9aaa9b9-3415-5b83-baab-d52b391db3ac` |
| llama | 2.433 | 4.432 | `or-sim.io/mig-f61eae53-d4d7-5315-a0c1-03a5baa8fb05` | `MIG-f61eae53-d4d7-5315-a0c1-03a5baa8fb05` |