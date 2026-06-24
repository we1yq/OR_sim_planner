# Current Profile Data

This directory contains the current Kubernetes pod-based runtime-side profile data used for planner catalogs.

## Consolidated CSVs

- `cnn_bench.csv`: ResNet50 and VGG16 at 224x224 with batch sizes 1/4/16/32/64.
- `vit_base_bench.csv`: ViT-base patch16 at 224x224 with batch sizes 1/4/16/32/64.
- `gpt2m_streaming_bench.csv`: GPT-2 medium language request shapes. Only rows with `prompt_len + output_tokens <= 1024` are retained.
- `llama32_3b_streaming_bench.csv`: Llama-3.2-3B language request shapes, including long-context stress rows at 8192 and 16384 prompt tokens. `1g` rows are retained as `status=error` because Llama failed to load on 1g during smoke.
- `llm_capacity_summary.md`: Markdown table of request/second capacity by request shape and MIG profile.

The CSV headers follow the older profile files where possible, but use `time_ms_p95` and `time_ms_p99` for the new run because the Kubernetes runner records p95/p99 rather than p90.

## Source Runs

Raw runner outputs and markdown summaries for the current campaign are in `run-20260623/`.

## Notes

- Node: `ampere`
- GPU: `ampere-gpu0`
- Vision warmup/sample count: 10/50
- GPT-2 warmup/sample count: 1/5
- Llama-3.2-3B cache was preloaded on rtx1-worker, synchronized to ampere, and profiled on 2g/3g/4g/7g; 1g failed model load during smoke.
- GPT-2 invalid 1024-token prompt rows from the initial run were removed from the consolidated CSV because GPT-2 medium's context limit is 1024 total tokens.
- Llama-3.2-3B 16384-token prompt rows failed on 2g and succeeded on 3g/4g/7g, giving a concrete long-context capacity boundary.
