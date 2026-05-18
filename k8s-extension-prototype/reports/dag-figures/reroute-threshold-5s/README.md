# Reroute Threshold 5s DAG Figures

Generated with `cost_aware_dag` after enabling the local-completion reroute
gate:

```text
if estimated_local_completion_seconds <= 5s:
  skip reroute and drain queued/inflight locally
else:
  reroute queued requests when a stable destination exists
```

| stage | planner | actions | phases | SVG |
| --- | --- | ---: | ---: | --- |
| stage1 | cost_aware_dag | 21 | 6 | [stage1-cost-aware-dag/final-execution-dag.svg](stage1-cost-aware-dag/final-execution-dag.svg) |
| stage2 | cost_aware_dag | 37 | 7 | [stage2-cost-aware-dag/final-execution-dag.svg](stage2-cost-aware-dag/final-execution-dag.svg) |
| stage3 | cost_aware_dag | 39 | 12 | [stage3-cost-aware-dag/final-execution-dag.svg](stage3-cost-aware-dag/final-execution-dag.svg) |
