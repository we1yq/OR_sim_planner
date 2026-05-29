| Step | Elapsed (s) | Evidence |
|---|---:|---|
| arrival snapshot accepted | 0.000 | ArrivalSnapshot exp-ampere-real-go-v1 created |
| plan object created | 1.000 | Kubernetes creationTimestamp: plan at 2026-05-28T08:13:13Z, arrival at 08:13:12Z |
| plan execution observed complete | ~69.000 | kubectl wait for status.phase=Executed returned at local 17:14:21 KST |
| runtime pods created | 61.000 | runtime Pod creationTimestamp 2026-05-28T08:14:13Z |
| runtime deployments ready | ~134.000 | kubectl wait for deployments available returned at local 17:15:26 KST |
| CUDA process verified | 265.000 | nvidia-smi timestamp 2026-05-28T08:17:37Z |
