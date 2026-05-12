# Round 2 Ablation Scenarios

This suite expands the original `stage0`-`stage3` chain with workload-shape
changes that stress different modules:

- `round2_stage0_bootstrap_balanced.yaml`: bootstrap from empty to a moderate mixed workload.
- `round2_stage1_llm_spike.yaml`: placement-pressure scenario with a heavy LLM/text spike while vision remains stable.
- `round2_stage2_vision_shift.yaml`: preserve-benefit scenario where demand shifts toward vision workloads.
- `round2_stage3_scale_down.yaml`: drain/reroute scenario with broad contraction that should reward cleanup and preservation.
- `round2_stage4_mixed_rebound.yaml`: transition-ordering scenario with renewed mixed demand after scale-down.

The ablation runner chains the executed state from one scenario into the next,
so `sourceStateRef` values after the first scenario are symbolic placeholders.
