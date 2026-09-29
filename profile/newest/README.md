# Newest profile: all catalog serving options on the worker-thread runtime — 2026-09-29

This directory is the current profile. It measures all 84 serving options on the three A100s with the fixed runtime: 67 vision options and 17 LLM options. Every catalog value comes from here. Earlier profiles (`profile/20260929`, `profile/current`) used older runtimes.

## Why this re-profile

The torchvision runtime serves HTTP with `ThreadingHTTPServer`, which handles every request on a new thread. Until this fix, inference ran on that new thread. The first CUDA call on a new thread sets up PyTorch's per-thread CUDA/cuBLAS/cuDNN state. That set-up fell between the two CUDA timing events, so `runtimeLatencyMs` included several milliseconds of idle GPU time per call.

Measured in a vgg16 2g pod (same MIG slice, same pinned core):

| vgg16, 2g | fixed thread | new thread per call |
|---|---|---|
| b1 | 4.18 ms | 10.4 ms |
| b16 | 33.6 ms | 34.5 ms |

The runtime now runs every CUDA call — model load, warm-up, input tensors, inference — on one long-lived thread (`CudaWorker` in `cmd/model-runtime/torchvision_runtime.py`). Request threads hand their work to it and wait. `profile/20260929` was measured with the old runtime, so its small-batch vision values are too low.

The LLM runtime changed in the same way. It also no longer runs a separate prefill forward per request: TTFT is marked inside the single `generate()` call.

## Protocol

Same as `profile/20260929` (see its README). Only the images differ:
- Pods: `executor_deployments.json` equals `profile/20260929/executor_deployments.json`, except for the images: `torchvision-worker-20260929`, `gpt2-medium-worker-20260929` and `llama32-3b-worker-20260929`. Each is the corresponding `*-batchctl-20260926` base with only the runtime `.py` replaced.
- Runs: rtx1 and ampere ran in parallel; ampere GPU0 then GPU1. Vision ran first. The LLM options followed (`PROFILE_WORKLOADS=...`, logs `logs/*-llm.log`).
  - Vision: 10 warm-up and 50 measured requests per batch.
  - LLM: 1 warm-up and 10 measured requests.
  - Result: 3520 samples per GPU, 0 errors.

## Catalog

`build_catalog_newest.py` builds the catalog with the same rule as before: per GPU, 1000 × batch / median `runtimeLatencyMs`, then the minimum over the three GPUs, rounded to 3 decimals. It writes:
- all 7 `k8s-extension-go/planner-engine/app/mock/profile-catalogs/<workload>.yaml`. It updates `mu` and `serviceTimeMs` of the measured options in place.
- `k8s-extension-go/experiments/three-gpu-live-20260926/catalog_newest.csv`, with the 84 rows of `catalog.csv`.

`catalog_20260929_worker.csv` is kept as the catalog of the E1 runs `20260929T023306` and `20260929T025230`. It has the same vision rows as this catalog, but its LLM rows come from `profile/20260929`. The LLM rows here are 0–5% lower than there: gpt2 −1.6% to −5.0%, llama −3.1% to +0.3%.

Change against `catalog_20260929_median_min.csv`:
- Small batches rise a lot. For example, resnet50 1g b1 goes from 42.1 to 187.2, resnet50 3g b4 from 182.0 to 815.8, and vgg16 2g b1 from 101.7 to 239.0.
- b32 and b64 change by 0–5%.
- vit_base changes by at most 8%, because its batches are compute-bound.

## Known limitation: ampere small batches

On rtx1, small batches are stable, with a coefficient of variation (CV) of 0.1–2.5%. They also increase monotonically with profile size; for example, vgg16 b1 takes 4.16, 2.57 and 2.17 ms on 2g, 3g and 4g.

On ampere, the same options have a CV of 10–27%. Their minimum latency matches rtx1, but most calls are delayed by 0.5–2 ms. The delay is on the CPU side. It is under investigation: the CPU frequency ramps up slowly on wake-up, and the Dell BIOS power profile may be the cause.

Because the catalog takes the minimum over the three GPUs, a few ampere-noise medians make a larger profile look slower than a smaller one:
- vision: resnet50 b1 (2g→3g, 3g→4g), resnet50 b4 3g→4g, vgg16 b1 3g→4g;
- LLM: gpt2_p64 (2g→3g, 3g→4g), gpt2_p512 3g→4g.

The drops are 0.3–3.8%. The values are kept as measured.
