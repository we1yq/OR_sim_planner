# ampere-real-go-v1 Comparison

| Model | Planned GPU/profile/resource | Actual pod/node/resource | Assigned MIG UUID | Ready |
|---|---|---|---|---|
| gpt2 | ampere-gpu1 / 1g / `or-sim.io/ampere-gpu1-s0-1-1g` | gpt2-runtime-67bdbbdf58-65m2l on ampere / `or-sim.io/ampere-gpu1-s0-1-1g` | `MIG-183fa215-464e-5359-9394-6bca2fef6406` | True |
| llama | ampere-gpu0 / 7g / `or-sim.io/ampere-gpu0-s0-8-7g` | llama-runtime-f595b4d67-n2c4c on ampere / `or-sim.io/ampere-gpu0-s0-8-7g` | `MIG-457656ac-5439-5b56-8fcc-1ff4336bacf5` | True |

## Router routes
```json
{
  "routes": [
    {
      "model": "gpt2",
      "endpoint": "http://115.145.135.205:10682"
    },
    {
      "model": "llama",
      "endpoint": "http://115.145.135.205:10681"
    }
  ]
}
```

## Registry queues
```yaml
active: 2
available: 1
transitioning: 0

```

## CUDA process check
`nvidia-smi.txt` shows two `/cuda-spin` processes: one on GPU0 GI0 CI0 and one on GPU1 GI7 CI0.
