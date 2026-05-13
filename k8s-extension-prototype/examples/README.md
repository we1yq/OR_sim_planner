# Examples

`expected/stage-chain-summary.yaml` is the compact regression fixture for the
notebook-derived stage0 to stage3 path. It records stable plan outputs only:
stage phase, GPU count, phase-greedy iteration/action metrics, MILP chosen templates, and
canonical physical GPU IDs.

Regenerate the source output with:

```bash
python3 k8s-extension-prototype/controller/main.py \
  --plan-scenario-chain \
  k8s-extension-prototype/mock/scenarios/stage0.yaml \
  k8s-extension-prototype/mock/scenarios/stage1.yaml \
  k8s-extension-prototype/mock/scenarios/stage2.yaml \
  k8s-extension-prototype/mock/scenarios/stage3.yaml
```

Human-readable example outputs for the prototype.

These examples are not produced by a real controller yet. They define the shape
we want Phase 5 to produce.
