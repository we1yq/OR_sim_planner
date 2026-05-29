# Timings

All durations are seconds.

| Event | Since ArrivalSnapshot | Timestamp / duration |
|---|---:|---|
| ArrivalSnapshot created | 0.000 | 2026-05-29T03:58:56Z |
| MigActionPlan created | 0.000 | 2026-05-29T03:58:56Z |
| executor started | 1.002 | 2026-05-29T03:58:57.001660082Z |
| MIG apply started | 1.033 | 2026-05-29T03:58:57.033305701Z |
| MIG apply finished | 3.817 | 2026-05-29T03:58:59.816781244Z |
| runtime deploy started | 16.008 | 2026-05-29T03:59:12.007509883Z |
| runtime deployments created | 16.018 | 2026-05-29T03:59:12.017926117Z |
| runtime ready and CUDA UUID verified | 20.192 | 2026-05-29T03:59:16.191711551Z |
| route synced | 20.194 | 2026-05-29T03:59:16.19380251Z |
| executor finished | 20.194 | 2026-05-29T03:59:16.19380799Z |

| Executor sub-step | Duration |
|---|---:|
| phase 0: `allocate_gpu` | 0.000 |
| phase 1: `configure_full_template` | 2.783 |
| phase 2: `bind_target_gpu` | 0.000 |
| phase 3: `register_mig_devices` | 11.168 |
| phase 4: `deploy_target_workloads` | 1.033 |
| phase 5: `activate_serving_route` | 4.176 |
| executor metric `routeSync` | 0.002 |
| executor metric `runtimeDeploymentCreate` | 0.010 |
| executor metric `runtimeReadyAndCUDAVerify` | 4.174 |
| executor metric `slotResourcePropagation` | 2.783 |
| executor metric `total` | 19.192 |

Runtime readiness:

| Model | Health ready after deploy | CUDA process after deploy | Device resource | Expected UUID |
|---|---:|---:|---|---|
| gpt2 | 1.869 | 4.174 | `or-sim.io/mig-a9aaa9b9-3415-5b83-baab-d52b391db3ac` | `MIG-a9aaa9b9-3415-5b83-baab-d52b391db3ac` |
| llama | 1.869 | 2.770 | `or-sim.io/mig-f61eae53-d4d7-5315-a0c1-03a5baa8fb05` | `MIG-f61eae53-d4d7-5315-a0c1-03a5baa8fb05` |