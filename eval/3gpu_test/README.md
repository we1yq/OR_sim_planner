# 3GPU Online Test

This folder contains the 3GPU short online replay experiment.

## Entry Points

Generate the 3GPU demand trace:

```bash
/Users/weiyiqin/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 eval/3gpu_test/online_3gpu_trace.py
```

Run the replay and regenerate all copied 24h-style plots:

```bash
/Users/weiyiqin/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 eval/3gpu_test/online_3gpu_replay.py
```

## Layout

- `online_3gpu_trace.py`: builds the 3GPU trace and request-rate figures.
- `online_3gpu_replay.py`: copied/adapted from `eval/online_24h_replay.py`; runs transitions and generates 24h-style plots.
- `docs/online_3gpu_plot_spec.md`: plot list and rules.
- `results/online_3gpu/`: generated demand traces.
- `results/online_3gpu_replay/`: replay CSV outputs.
- `figures/`: generated figures.
