# Planned vs Actual v3

| Check | Result | Evidence |
|---|---|---|
| Active device-plugin refresh | PASS | node-agent responses include `devicePluginRefresh.success=true` for apply-slots and refresh-cdi |
| Lightweight node-side transaction | PASS | node-agent returns MIG slots, expected per-MIG UUID resources, and registeredResources |
| Planned layout | PASS | `ampere-gpu0 = 1g + 1g + 1g + 1g + 3g`, `ampere-gpu1 = empty` |
| Actual GPU0 MIG slots | PASS | 5 slots: 1g[0,1), 1g[1,2), 1g[2,3), 1g[3,4), 3g[4,8) |
| gpt2 UUID binding | PASS | requested `or-sim.io/mig-a9aaa9b9-3415-5b83-baab-d52b391db3ac`, health reported `MIG-a9aaa9b9-3415-5b83-baab-d52b391db3ac`, CUDA process: `2559208 /cuda-spin                               76MiB` |
| llama UUID binding | PASS | requested `or-sim.io/mig-f61eae53-d4d7-5315-a0c1-03a5baa8fb05`, health reported `MIG-f61eae53-d4d7-5315-a0c1-03a5baa8fb05`, CUDA process: `2559206 /cuda-spin                              176MiB` |
| Router routes | PASS | `{'routes': [{'model': 'gpt2', 'endpoint': 'http://115.145.135.205:10682'}, {'model': 'llama', 'endpoint': 'http://115.145.135.205:10681'}]}` |
| Routed requests | PASS | gpt2 `12.338` ms; llama `36.142` ms |
| Timing result | MIXED | active refresh succeeded, but kubelet allocatable propagation was slower in this run: `uuidResourcePropagationAndStableWait=8.062s`; total `19.309s` |

Interpretation: device-plugin scan interval is no longer the main bottleneck. The remaining variability is kubelet/device-plugin resource propagation to Node allocatable.