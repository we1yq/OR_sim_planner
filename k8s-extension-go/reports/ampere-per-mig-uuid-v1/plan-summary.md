# Ampere Per-MIG UUID Binding Plan

Plan: `plan-exp-ampere-per-mig-uuid-v1`

| Model | Node | GPU | Profile | Logical slot identity | Kubernetes device resource | Expected MIG UUID | Host port | Runtime pod |
|---|---|---|---|---|---|---|---:|---|
| gpt2 | ampere | ampere-gpu0 | 1g | `or-sim.io/ampere-gpu0-s0-1-1g` | `or-sim.io/mig-a9aaa9b9-3415-5b83-baab-d52b391db3ac` | `MIG-a9aaa9b9-3415-5b83-baab-d52b391db3ac` | 10682 | `gpt2-runtime-7d7dc5dc48-wqrpk` |
| llama | ampere | ampere-gpu0 | 3g | `or-sim.io/ampere-gpu0-s4-8-3g` | `or-sim.io/mig-f61eae53-d4d7-5315-a0c1-03a5baa8fb05` | `MIG-f61eae53-d4d7-5315-a0c1-03a5baa8fb05` | 10681 | `llama-runtime-c768cf9dc-2vdrp` |

Planner pipeline:

`source -> feasible-options -> milp -> target-build -> effect_aware_dag -> canonical-next-state`

Planner: `original-gurobi-milp-greedy-repair-effect-aware-dag`
