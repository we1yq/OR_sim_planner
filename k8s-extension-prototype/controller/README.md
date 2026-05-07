# Controller

This directory contains the minimal mock-based controller prototype.

Phase 5 starts with a local CLI controller that:

- reads scenario YAML and workload request references,
- reads mock GPU/MIG state,
- validates mock MIG rules,
- adapts inputs into the notebook-derived real MILP target builder and V3
  transition planner.

The controller must not execute real Kubernetes scheduling changes or MIG
reconfiguration commands in the first prototype.

Current modules:

```text
main.py
models.py
io_utils.py
state_adapter.py
feasible_options.py
mig_rules.py
scenario_loader.py
k8s_adapter.py
```

## Setup

Check whether `PyYAML` is installed:

```bash
python3 -c 'import yaml; print(yaml.__version__)'
```

If it is missing, install the controller dependencies:

```bash
python3 -m pip install -r k8s-extension-prototype/controller/requirements.txt
```

## Parse a Scenario

The multi-workload path currently loads a scenario and prints a parsed summary.
The real target planner will be migrated from the notebook MILP, not replaced by
a simplified planner.

```bash
python3 k8s-extension-prototype/controller/main.py \
  --scenario k8s-extension-prototype/mock/scenarios/stage0.yaml
```

## Run Real MILP + V3 Dry-Run Planning

Single stage:

```bash
python3 k8s-extension-prototype/controller/main.py \
  --plan-scenario k8s-extension-prototype/mock/scenarios/stage0.yaml
```

Notebook stage chain with canonical next-stage state:

```bash
python3 k8s-extension-prototype/controller/main.py \
  --plan-scenario-chain \
  k8s-extension-prototype/mock/scenarios/stage0.yaml \
  k8s-extension-prototype/mock/scenarios/stage1.yaml \
  k8s-extension-prototype/mock/scenarios/stage2.yaml \
  k8s-extension-prototype/mock/scenarios/stage3.yaml
```

This path intentionally calls:

```text
solve_milp_gurobi_batch_unified
  -> build_target_state_from_milp
  -> run_v3_stage_iterative
  -> canonicalize_state_for_next_round
```

It does not include an approximate planner or the old V2 planner.

## Validate MIG Rules

```bash
python3 k8s-extension-prototype/controller/main.py \
  --validate-mig-rules k8s-extension-prototype/mock/mig-rules/a100-40gb.yaml
```

To validate a mock GPU state against those rules:

```bash
python3 k8s-extension-prototype/controller/main.py \
  --validate-mig-rules k8s-extension-prototype/mock/mig-rules/a100-40gb.yaml \
  --gpu-state k8s-extension-prototype/mock/gpu-states/simulation-empty-9-a100.yaml \
  --validate-gpu-state
```
