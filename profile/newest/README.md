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

The LLM rows here are 0–5% lower than the `profile/20260929` values (gpt2 −1.6% to −5.0%, llama −3.1% to +0.3%). The intermediate catalogs `catalog_20260929_median_min.csv` and `catalog_20260929_worker.csv`, and the E1 runs that used them, were removed. See `k8s-extension-go/experiments/three-gpu-live-20260926/E1_EXPERIMENT_LOG.md`.

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

## Options marked unfit (`unmeasured_options/`)

These are the nine options that `fit: false` excludes from the catalog. They were measured with the same protocol so that the yaml carries a measured `mu` for them too. Each exceeds its SLO, so they stay excluded (worst GPU p95 against the SLO):

| option | p95 | SLO |
|---|---|---|
| resnet50 1g b64 | 165.4 ms | 100 ms |
| vgg16 1g b32 | 128.6 ms | 100 ms |
| vgg16 1g b64 | 256.1 ms | 100 ms |
| vgg16 2g b64 | 127.6 ms | 100 ms |
| vit_base 1g b32 | 500.4 ms | 300 ms |
| vit_base 1g b64 | 982.6 ms | 300 ms |
| vit_base 2g b64 | 499.4 ms | 300 ms |
| vit_base 3g b64 | 329.5 ms | 300 ms |
| llama_p2048_o64 2g | TTFT 291 ms (TPOT 33.5 ms) | TTFT 250 ms / TPOT 35 ms |

There were no out-of-memory errors.

## vgg16 b1 3g/4g repeat (`vgg16_b1_3g4g_repeat/`)

This is a second measurement of vgg16 b1 on 3g and 4g. It is a check only and does not feed the catalog.

| throughput (rps) | rtx1 | ampere0 | ampere1 | min over GPUs |
|---|---|---|---|---|
| 3g, first run | 389.7 | 339.5 | 321.5 | 321.5 |
| 3g, repeat | 389.2 | 319.5 | 350.2 | 319.5 |
| 4g, first run | 461.5 | 320.0 | 317.2 | 317.2 |
| 4g, repeat | 459.7 | 313.8 | 323.5 | 313.8 |

rtx1 is stable and gets faster from 3g to 4g. On both ampere GPUs, 4g is no faster than 3g in either run. The ampere host caps this launch-bound option at about 3.1 ms per call, while rtx1 reaches 2.17 ms on 4g.
