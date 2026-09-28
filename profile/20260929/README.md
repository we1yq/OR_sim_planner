# Isolated re-profiling of all catalog serving options — 2026-09-29

This directory re-measures all 84 serving options in `k8s-extension-go/experiments/three-gpu-live-20260926/catalog.csv` on the three A100s: rtx1-worker GPU0, ampere GPU0 and ampere GPU1. Every value here is a measured value. Nothing has been adjusted or smoothed.

## Files

| File | Content |
|---|---|
| `catalog_min_measured.csv` | Same 84 rows and columns as `catalog.csv`. `mu` is the minimum over the three GPUs of the per-GPU mean throughput, with 3 decimals. |
| `per_gpu_throughput.csv` | Per-option mean throughput on each GPU, the minimum and which GPU it came from, and the July `catalog.csv` mu for comparison. |
| `<gpu>/raw-samples.csv` | Every request: warm-up and sample phases, `runtimeLatencyMs`, TTFT/TPOT, MIG UUID and the runtime's reported CPU affinity. |
| `run_catalog_profile.py` | The runner. Usage: `run_catalog_profile.py <node> <gpu_index>...`. |
| `executor_deployments.json`, `runtimes.json`, `executor_env.txt` | The pod specs used, and the inputs that produced them (see below). |
| `logs/` | Runner logs, one line per runtime: CPU affinity and load time. |

## Protocol

- **Pods match the live system.** `executor_deployments.json` was produced by transition-executor's own `deployment()` function, called with the env of the live `or-sim-exp/transition-executor`. Images, env vars, `MODEL_ID=/opt/models/...`, mounts, runtimeClass and hostNetwork are therefore the same as in the live experiment. Images:
  - `llama32-3b-cpuset-20260929`
  - `gpt2-medium-cpuset-20260929`
  - `torchvision-cpuset-20260929`

  Each is the `*-batchctl-20260926` image with only the runtime `.py` replaced.
- **Isolated.** One runtime at a time on one MIG slice at slot 0, with the rest of the GPU empty. rtx1 and ampere ran in parallel (different hosts). ampere GPU0 and GPU1 ran one after the other.
- **Dedicated CPU core.** Each runtime is pinned from process start to one physical core. The runtime reads `OR_SIM_CPU_SET` before importing torch. transition-executor picks the core from node annotation `mig.or-sim.io/runtime-cpu-pool`, entry `gpu*8 + slotStart`.
  - rtx1: CPUs `2,34`. `OR_SIM_CPU_EXCLUDE=1,33` also stays set.
  - ampere GPU0: CPUs `2,58`.
  - ampere GPU1: CPUs `18,74`.

  Every raw row records the affinity that was applied.
- **Vision.** Batch is switched with `/control/batch`. Each batch gets 10 warm-up requests, then 50 measured requests of `{"benchmark": true, "batch": B}`.
- **LLM.** Request body as in `live_traffic_20260926.py`: batch 1, explicit `prompt_len` and `output_tokens`. The runtime's built-in warm-up runs first. After that come 1 warm-up request at the target shape and 10 measured requests.
- **Throughput per GPU** = `1000 × batch / mean(runtimeLatencyMs)`. This is the same definition as `throughput_rps` in `profile/current` (see `k8s-extension-go/tools/run_k8s_profile_matrix.py`).
- **Catalog mu** = the minimum over the three GPUs, so every GPU can reach it, rounded to 3 decimals. The July `catalog.csv` used the median over the three GPUs.

## Why a dedicated core

Without pinning, runtime threads spread over all CPUs. Latency for launch-bound options then becomes bimodal. On ampere, gpt2_p64_o64 took either about 640 ms or about 920 ms per request, and the coefficient of variation (CV) was 6–19%.

Diagnostics ruled out several causes:
- a single slow core (a per-CPU launch scan found all cores uniform),
- NUMA placement,
- C6,
- the OpenMP thread count.

Only confining the runtime to one core removed the bimodality. On rtx1 there is also one slow physical core, CPUs 1 and 33, which is excluded.

With pinning, the median per-option CV is 0.4% on rtx1, 0.6% on ampere GPU0 and 0.5% on ampere GPU1. Pinning also raised ampere throughput by a median of ×1.09 and up to ×1.8, compared with an unpinned run of the same protocol.

## Results

- 84/84 options were measured on every GPU, with 0 errors.
- The minimum came from rtx1 41 times, from ampere GPU1 37 times and from ampere GPU0 6 times.
- Compared with the July `catalog.csv`: median ×1.089, range ×0.984–×1.915.

### Measured cases where a larger profile is not faster (2g → 3g → 4g, minimum over GPUs, 3 decimals)

| workload | batch | step | mu | change | per-GPU change (rtx1 / ampere0 / ampere1) |
|---|---:|---|---|---:|---|
| gpt2_p64_o64 | 1 | 3g → 4g | 1.535 → 1.474 | −4.0% | −4.0% / −2.5% / −0.8% |
| resnet50_image | 16 | 2g → 3g | 681.036 → 652.816 | −4.1% | +4.4% / +7.3% / −4.9% |
| llama_p1024_o128 | 1 | 3g → 4g | 0.358 → 0.349 | −2.5% | −2.5% / +0.3% / +0.2% |
| resnet50_image | 4 | 3g → 4g | 181.282 → 178.987 | −1.3% | −1.3% / −3.2% / −0.1% |
| gpt2_p512_o512 | 1 | 2g → 3g | 0.188 → 0.186 | −1.1% | −0.7% / +0.1% / −1.3% |
| gpt2_p512_o512 | 1 | 3g → 4g | 0.186 → 0.184 | −1.1% | −1.4% / −0.8% / −2.2% |
| resnet50_image | 1 | 3g → 4g | 42.546 → 42.181 | −0.9% | −0.9% / +1.0% / 0.0% |

- **Same direction on all three GPUs:** gpt2 (both shapes) and resnet50 b4. On A100, 3g.20gb and 4g.20gb have the same memory slices. Launch- or bandwidth-bound options gain nothing from the extra SMs.
- **Arise only because the minimum mixes GPUs:** llama_p1024_o128 and resnet50 b1. The drop appears on rtx1 only.
- **Possibly a single-run outlier:** resnet50 b16 2g → 3g. It drops on ampere GPU1 only, while the other two GPUs rise 4–7%.

Llama p2048/o64 is monotone on every GPU. Minimum over GPUs: 3g 0.655 → 4g 0.660 → 7g 0.705. Per GPU:
- rtx1: 0.655 → 0.660
- ampere GPU0: 0.670 → 0.707
- ampere GPU1: 0.672 → 0.710

## Not covered

These are isolated measurements. In the live deployment several runtimes share a host. MIG isolates SMs and memory, but not host CPU or PCIe. Co-located throughput can therefore be lower, especially for LLM decode.
