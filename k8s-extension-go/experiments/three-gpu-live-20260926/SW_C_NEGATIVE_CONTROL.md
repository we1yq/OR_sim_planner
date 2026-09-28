# SW-C live negative control

`SW-C` is an intentionally unsafe experimental Stage 3 variant. It uses the
same SliceWise source, Stage 2 target, and primitive action multiset, then
removes only dependency keys listed by Stage 3 capacity gates. Resource reuse,
drain, physical-GPU ordering, and target validation remain enabled.

The production default remains `slicewise`. An `ArrivalSnapshot` selects the
negative control explicitly:

```yaml
spec:
  planner: ours
  stage3Variant: sw-c
```

The live runner requires both flags before it will mutate the cluster:

```sh
python3 live_runner_20260926.py \
  --execute \
  --stage3-variant sw-c \
  --allow-unsafe-sw-c
```

Omitting `--allow-unsafe-sw-c` stops before preflight or cluster mutation. The
selected variant is saved in `environment.json`, every submitted
`ArrivalSnapshot`, and the planner's Stage 3 trace. Apply the updated CRD and
redeploy both `planner-controller` and `planner-engine` before using the flag.

Run SW-C only on an isolated testbed: it deliberately removes the ordering
that enforces the transition-capacity commitment. It does not remove route
drain or resource-safety dependencies, so it is a capacity negative control,
not a request-loss or resource-corruption test.
