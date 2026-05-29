# Planned vs Actual v4

| Check | Result | Evidence |
|---|---|---|
| Two-stage UUID confirmation | PASS | `nodeAgentRegisteredTargetResources=true`; Node allocatable wait capped at 2s, completed in `1.011s` |
| Active device-plugin refresh | PASS | apply-slots and refresh-cdi both report `devicePluginRefresh.success=true` |
| Planned layout | PASS | `ampere-gpu0 = 1g + 1g + 1g + 1g + 3g`, `ampere-gpu1 = empty` |
| Actual GPU0 MIG slots | PASS | 5 slots: 1g[0,1), 1g[1,2), 1g[2,3), 1g[3,4), 3g[4,8) |
| gpt2 UUID binding | PASS | requested `or-sim.io/mig-a9aaa9b9-3415-5b83-baab-d52b391db3ac`, health reported `MIG-a9aaa9b9-3415-5b83-baab-d52b391db3ac`, CUDA process: `2572790 /cuda-spin                               76MiB` |
| llama UUID binding | PASS | requested `or-sim.io/mig-f61eae53-d4d7-5315-a0c1-03a5baa8fb05`, health reported `MIG-f61eae53-d4d7-5315-a0c1-03a5baa8fb05`, CUDA process: `2572796 /cuda-spin                              176MiB` |
| Router routes | PASS | `{'routes': [{'model': 'gpt2', 'endpoint': 'http://115.145.135.205:10682'}, {'model': 'llama', 'endpoint': 'http://115.145.135.205:10681'}]}` |
| Routed requests | PASS | gpt2 `12.335` ms; llama `53.31` ms |
| Timing result | PASS | executor total `13.107s`; register_mig_devices `3.714s`; UUID resource wait `1.011s` |

Interpretation: the two-stage path removes most of the kubelet Node-status wait from the executor critical path while still validating the final CUDA UUID after Pod startup.