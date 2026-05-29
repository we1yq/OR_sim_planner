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

For the default local fixture, use:

```text
policies/migrant-default.yaml
gpu-states/migrant-empty-9-a100.yaml
scenarios/stage0.yaml
```

The deployed system normally receives ArrivalSnapshot and PhysicalGpuRegistry
objects from Kubernetes. `stage0.yaml` is only a minimal local fixture for the
same final planner path.

## Regenerate Profile Catalog

From the repo root:

```bash
python3 k8s-extension-prototype/tools/extract_profile_catalog.py
```

This reads profiling CSV files and writes:

```text
k8s-extension-go/planner-engine/app/mock/profile-catalogs/*.yaml
```
