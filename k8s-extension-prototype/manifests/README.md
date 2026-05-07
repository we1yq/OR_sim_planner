# Manifests

Kubernetes YAML files for the prototype.

For now these files are design skeletons and safe examples. They do not trigger
real GPU or MIG operations.

## Files

- `namespace.yaml`: namespace for local experiments.
- `crds/workloadrequest-crd.yaml`: future CRD for workload demand.
- `crds/migplan-crd.yaml`: future CRD for dry-run plans.
- `examples/workloadrequests/*.yaml`: workload requests generated from the
  simulation workload specs.
- `examples/mock-gpu-states/*.yaml`: sample mock GPU/MIG states as ConfigMaps.

## Apply Namespace

```bash
kubectl apply -f manifests/namespace.yaml
kubectl get namespace or-sim
```

Do not apply CRDs until Phase 5, when the controller prototype starts reading
these objects.
