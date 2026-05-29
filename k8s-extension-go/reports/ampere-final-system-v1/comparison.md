# Planned vs Observed

| Check | Planned | Observed | Result |
|---|---|---|---|
| Active GPU | ampere-gpu0 | ampere-gpu0 | pass |
| Free GPU | ampere-gpu1 available | available / empty | pass |
| gpt2 exact resource | `or-sim.io/ampere-gpu0-s0-1-1g` | `or-sim.io/ampere-gpu0-s0-1-1g` | pass |
| gpt2 MIG UUID | runtime-bound UUID | `MIG-a9aaa9b9-3415-5b83-baab-d52b391db3ac` | pass |
| gpt2 CUDA process | `/cuda-spin` on bound MIG | `1314869 /cuda-spin                               76MiB` | pass |
| llama exact resource | `or-sim.io/ampere-gpu0-s4-8-3g` | `or-sim.io/ampere-gpu0-s4-8-3g` | pass |
| llama MIG UUID | runtime-bound UUID | `MIG-f61eae53-d4d7-5315-a0c1-03a5baa8fb05` | pass |
| llama CUDA process | `/cuda-spin` on bound MIG | `1315694 /cuda-spin                              176MiB` | pass |

## Runtime Bindings In Registry

```yaml
- model: gpt2
  phase: Running
  pod: gpt2-runtime-78cd7b4b56-hfs2z
  slotResource: or-sim.io/ampere-gpu0-s0-1-1g
- model: llama
  phase: Running
  pod: llama-runtime-fc6f5dd9-cv2l5
  slotResource: or-sim.io/ampere-gpu0-s4-8-3g
```
