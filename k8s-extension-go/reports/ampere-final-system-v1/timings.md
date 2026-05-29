# Timings

| Step | Time since arrival submit start (s) | Detail |
|---|---:|---|
| arrival apply returned | 0.170 | kubectl apply returned |
| MigActionPlan observed created | 1.281 | 2026-05-28T10:46:49Z |
| executor started | 0.336 | 2026-05-28T10:46:49.896416502Z |
| slots applied and slot resources propagated | 2.887 | 2026-05-28T10:46:52.447191424Z |
| runtime deployments created | 4.604 | 2026-05-28T10:46:54.164286448Z |
| runtime ready and CUDA UUID verified | 75.908 | 2026-05-28T10:48:05.469086796Z |
| route synced / executor finished | 75.911 | 2026-05-28T10:48:05.471319488Z |
| Executed observed by kubectl wait | 75.948 | local wait return timestamp |

## Internal Durations

| Metric | Seconds |
|---|---:|
| plannerEngineElapsedSec | 0.118 |
| MILP elapsed | 0.040 |
| feasible option build | 0.068 |
| executor routeSync | 0.002 |
| executor runtimeDeploymentCreate | 0.009 |
| executor runtimeReadyAndCUDAVerify | 71.305 |
| executor slotResourcePropagation | 2.516 |
| executor total | 75.575 |

## Runtime Readiness Breakdown

| Model | Pod | podCreated | podScheduled | podStart | containerStarted | healthReady | cudaProcessFound | MIG UUID |
|---|---|---:|---:|---:|---:|---:|---:|---|
| gpt2 | `gpt2-runtime-78cd7b4b56-hfs2z` | -0.164 | -0.164 | 33.836 | 34.836 | 69.080 | 70.784 | `MIG-a9aaa9b9-3415-5b83-baab-d52b391db3ac` |
| llama | `llama-runtime-fc6f5dd9-cv2l5` | -0.164 | -0.164 | 67.836 | 68.836 | 70.094 | 71.305 | `MIG-f61eae53-d4d7-5315-a0c1-03a5baa8fb05` |

## Diagnosis

The model runtime image was already present on ampere. CUDA workers started immediately after containers started. The dominant delay is Kubernetes admission/scheduling around dynamically registered exact slot device-plugin endpoints. Events from this run are saved in `events.txt`.
