# Ampere Exact Slot v6 Comparison

## Plan

The planner generated `plan-exp-ampere-exact-slot-v6` from `exp-ampere-exact-slot-v6`:

| Model | Planned node | Planned GPU | Planned profile | Planned exact resource | Host port |
|---|---|---|---|---|---:|
| llama | ampere | ampere-gpu0 | 1g | `or-sim.io/ampere-gpu0-s4-5-1g` | 10681 |
| gpt2 | ampere | ampere-gpu1 | 1g | `or-sim.io/ampere-gpu1-s4-5-1g` | 10682 |

## Actual

| Check | Result |
|---|---|
| Runtime pod placement | `llama-runtime` and `gpt2-runtime` both Running on `ampere` |
| Exact resource requests | Deployments request and limit the planned `or-sim.io/...` resources |
| RuntimeClass | Model runtime pods use `runtimeClassName: nvidia` |
| Node resources | `ampere` advertises `or-sim.io/ampere-gpu0-s4-5-1g: 1` and `or-sim.io/ampere-gpu1-s4-5-1g: 1` |
| Real MIG state | `nvidia-smi -L` shows one `1g.5gb` MIG device on GPU0 and one on GPU1 |
| Router routes | `llama -> http://115.145.135.205:10681`, `gpt2 -> http://115.145.135.205:10682` |
| Smoke inference | Both `/infer/llama` and `/infer/gpt2` returned HTTP 200 through `runtime-router` |

## Observed MIG UUIDs

| GPU | Runtime | MIG UUID observed after execution |
|---|---|---|
| ampere-gpu0 | llama-runtime | `MIG-4556347f-ddcc-5e25-bef2-c9bdabee1e7d` |
| ampere-gpu1 | gpt2-runtime | `MIG-5b65e826-352e-52a9-bb2b-428b64d53b41` |

The exact UUID binding is enforced by the slot device-plugin allocation path: kubelet requests the exact `or-sim.io/<gpu-slot>` resource, and the plugin injects `NVIDIA_VISIBLE_DEVICES=<current MIG UUID>` plus `OR_SIM_MIG_UUID=<current MIG UUID>` for that exact slot.

## Residual Issue

Several earlier runtime pod attempts entered `UnexpectedAdmissionError` while kubelet saw the exact-slot device-plugin endpoint restart during slot refresh. The final ReplicaSet attempts succeeded and the running pods match the plan, but this admission retry noise should be cleaned up before repeated benchmark runs.
