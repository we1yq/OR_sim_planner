# Ampere Exact Slot Timing v6

Experiment start condition: ampere GPU0/GPU1 were cleared and `nvidia-smi -L` showed no MIG devices. The timer starts immediately before applying `exp-ampere-exact-slot-v6`.

| Step | Unix ms | Delta from start | Note |
|---|---:|---:|---|
| start | 1779950394544 | 0 ms | ampere empty, before applying ArrivalSnapshot |
| arrival_applied | 1779950415159 | 20615 ms | ArrivalSnapshot accepted by Kubernetes |
| plan_executed_observed | 1779950509223 | 114679 ms | MigActionPlan had already reached `Executed` when observed |
| llama_runtime_available | 1779950550916 | 156372 ms | `llama-runtime` Deployment Available |
| gpt2_runtime_available | 1779950571435 | 176891 ms | `gpt2-runtime` Deployment Available |
| router_smoke_complete | 1779950627836 | 233292 ms | routes listed and both inference calls returned HTTP 200 |

Notes:

- The plan moved from creation to `Executed` faster than the `kubectl wait` for the intermediate `Planned` phase could observe, so the table records the first observed executed timestamp.
- Runtime pods eventually became available, but earlier ReplicaSet attempts show `UnexpectedAdmissionError` from kubelet while the exact-slot device-plugin endpoint was being refreshed. The final running pods are the source of truth for this run.
