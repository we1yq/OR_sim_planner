# Mock Data

Mock data for local development without a GPU.

These files are inputs for the controller prototype and represent external facts
or requests. The planner can read them, but should not mutate them.

## Directories

- `gpu-states/`: mock current MIG layouts.
- `mig-rules/`: A100 MIG profile/template rules copied from the simulation model.
- `policies/`: mock planning policies such as max GPU count.
- `profile-catalogs/`: one normalized profile catalog per workload.
- `scenarios/`: multi-workload stage fixtures with source and target arrivals.

## GPU State vs MIG Rules

`gpu-states/*.yaml` should describe observed current state only:

```text
which GPUs exist
which MIG instances currently exist
which workload, if any, occupies each instance
```

The legal A100 MIG templates should stay in `mig-rules/a100-40gb.yaml`, because
they are part of planner knowledge and validation rules. They are not something
kind can natively enforce.

For a simulation-like local capacity ceiling, use:

```text
policies/simulation-default.yaml
gpu-states/simulation-empty-9-a100.yaml
scenarios/stage0.yaml
scenarios/stage1.yaml
scenarios/stage2.yaml
scenarios/stage3.yaml
```

The notebook minimizes GPU count and does not appear to use a fixed hard cap in
the MILP. The main simulation stages observed 6, 9, 6, and 8 target GPUs, so this
prototype uses 9 as the default mock capacity ceiling.

## Regenerate Profile Catalog

From the repo root:

```bash
python3 k8s-extension-prototype/tools/extract_profile_catalog.py
```

This reads the existing simulation-stage profiling CSV files and writes:

```text
k8s-extension-prototype/mock/profile-catalogs/*.yaml
```
