# Vision re-profiling on the worker-thread runtime — 2026-09-29

This directory re-measures all 67 vision serving options (resnet50, vgg16, vit_base) on the three A100s, after a runtime fix. The LLM options are not re-measured here; they keep their values from `profile/20260929`.

## Why this re-profile

The torchvision runtime serves HTTP with `ThreadingHTTPServer`, which handles every request on a new thread. Until this fix, inference ran on that new thread. The first CUDA call on a new thread sets up PyTorch's per-thread CUDA/cuBLAS/cuDNN state. That set-up fell between the two CUDA timing events, so `runtimeLatencyMs` included several milliseconds of idle GPU time per call.

Measured in a vgg16 2g pod (same MIG slice, same pinned core):

| vgg16, 2g | fixed thread | new thread per call |
|---|---|---|
| b1 | 4.18 ms | 10.4 ms |
| b16 | 33.6 ms | 34.5 ms |

The runtime now runs every CUDA call — model load, warm-up, input tensors, inference — on one long-lived thread (`CudaWorker` in `cmd/model-runtime/torchvision_runtime.py`). Request threads hand their work to it and wait. `profile/20260929` was measured with the old runtime, so its small-batch vision values are too low.

## Protocol

Same as `profile/20260929` (see its README). Only two things differ:
- Pods: `executor_deployments.json` is the vision subset of `profile/20260929/executor_deployments.json`, with the image changed to `torchvision-worker-20260929`. That image is the `torchvision-batchctl-20260926` base with only the runtime `.py` replaced.
- Scope: vision options only (`runtimes.json`), 201 (GPU, option, batch) combinations. rtx1 and ampere ran in parallel; ampere GPU0 then GPU1. Each batch had 10 warm-up and 50 measured requests. The result: 3350 samples per GPU and 0 errors.

## Catalog

`build_catalog_worker.py` builds the catalog with the same rule as before: per GPU, 1000 × batch / median `runtimeLatencyMs`, then the minimum over the three GPUs, rounded to 3 decimals. It writes:
- `k8s-extension-go/planner-engine/app/mock/profile-catalogs/{resnet50,vgg16,vit_base}_image.yaml`. It updates `mu` and `serviceTimeMs` of the measured options in place. The planner-engine image `worker-20260929` bakes these files.
- `k8s-extension-go/experiments/three-gpu-live-20260926/catalog_20260929_worker.csv`. It has the same 84 rows as `catalog_20260929_median_min.csv`: the 67 vision rows are replaced, and the 17 LLM rows are unchanged.

Change against `catalog_20260929_median_min.csv`:
- Small batches rise a lot. For example, resnet50 1g b1 goes from 42.1 to 187.2, resnet50 3g b4 from 182.0 to 815.8, and vgg16 2g b1 from 101.7 to 239.0.
- b32 and b64 change by 0–5%.
- vit_base changes by at most 8%, because its batches are compute-bound.
