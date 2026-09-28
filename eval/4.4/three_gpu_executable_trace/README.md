# Section 4.4 Three-GPU Executable Trace

This folder contains the short trace for the Section 4.4 real three-GPU executable replay.

## Files

- `stages.json`: input for `eval/3gpu_test/real_3gpu_k8s_experiment.py --stages-json`.
- `request_rate_30min.csv`: full workload-schema demand trace.
- `target_probe.csv`: local SliceWise target probe for each planning round.
- `metadata.json`: trace metadata.

## Intended Use

Run the real executor with:

```bash
python eval/3gpu_test/real_3gpu_k8s_experiment.py \
  --run-id real3gpu-section44-<date> \
  --stages-json "$(cat eval/4.4/three_gpu_executable_trace/stages.json)"
```

The replay has 5 planning rounds. Round 0 provisions from an empty cluster,
rounds 1--4 exercise structural changes, and round 5 shuts the system down.
