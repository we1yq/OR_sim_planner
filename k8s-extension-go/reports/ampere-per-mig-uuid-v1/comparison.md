# Planned vs Actual

| Check | Result | Evidence |
|---|---|---|
| Planner pipeline | PASS | Gurobi MILP -> greedy target materialization -> effect-aware DAG |
| Planned layout | PASS | `ampere-gpu0 = 1g + 1g + 1g + 1g + 3g`, `ampere-gpu1 = empty` |
| Actual GPU0 MIG slots | PASS | 5 slots: 1g[0,1), 1g[1,2), 1g[2,3), 1g[3,4), 3g[4,8) |
| Runtime binding mechanism | PASS | Deployment requests/limits use `or-sim.io/mig-<uuid>` resources; logical slot is annotation only |
| Kubernetes allocation accounting | PASS | `kubectl describe node ampere` shows only the two selected `or-sim.io/mig-<uuid>` resources allocated: gpt2 UUID and llama UUID |
| gpt2 UUID binding | PASS | requested `or-sim.io/mig-a9aaa9b9-3415-5b83-baab-d52b391db3ac`, health reported `MIG-a9aaa9b9-3415-5b83-baab-d52b391db3ac`, CUDA process: `2447567 /cuda-spin                               76MiB` |
| llama UUID binding | PASS | requested `or-sim.io/mig-f61eae53-d4d7-5315-a0c1-03a5baa8fb05`, health reported `MIG-f61eae53-d4d7-5315-a0c1-03a5baa8fb05`, CUDA process: `2447569 /cuda-spin                              176MiB` |
| Router routes | PASS | `{'routes': [{'model': 'gpt2', 'endpoint': 'http://115.145.135.205:10682'}, {'model': 'llama', 'endpoint': 'http://115.145.135.205:10681'}]}` |
| gpt2 routed request | PASS | latency `10.368` ms |
| llama routed request | PASS | latency `47.356` ms |

Note: `slotResource` remains the planner/registry logical slot identity. The Kubernetes scheduling resource is now the resolved per-MIG UUID resource.
