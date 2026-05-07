# Manifests

Kubernetes YAML files for the prototype.

For now these files are design skeletons and safe examples. They do not trigger
real GPU or MIG operations.

## Files

- `namespace.yaml`: namespace for local experiments.
- `crds/workloadrequest-crd.yaml`: future CRD for workload demand.
- `crds/migplan-crd.yaml`: future CRD for dry-run plans.
- `controller/*.yaml`: service account, namespaced RBAC, and Deployment for the
  dry-run controller loop.
- `examples/workloadrequests/*.yaml`: workload requests generated from the
  simulation workload specs.
- `examples/mock-gpu-states/*.yaml`: sample mock GPU/MIG states as ConfigMaps.

## Apply Namespace

```bash
kubectl apply -f manifests/namespace.yaml
kubectl get namespace or-sim
```

## Deploy The Dry-Run Controller To Kind

Build and load the local image:

```bash
docker build -t or-sim-mig-planner:dev k8s-extension-prototype
kind load docker-image or-sim-mig-planner:dev --name or-sim-dev
```

Apply the controller resources:

```bash
kubectl apply -f k8s-extension-prototype/manifests/namespace.yaml
kubectl apply -f k8s-extension-prototype/manifests/crds/
kubectl apply -f k8s-extension-prototype/manifests/controller/
kubectl apply -f k8s-extension-prototype/manifests/examples/migplans/
```

Watch dry-run status:

```bash
kubectl get migplans -n or-sim
kubectl get configmaps -n or-sim -l mig.or-sim.io/state-kind=canonical-next-state
```

The controller image uses the Kubernetes Python client directly and watches
`MigPlan` API events; it does not carry `kubectl` inside the container. The Deployment still only patches
`MigPlan.status` and writes canonical state ConfigMaps. It does not modify pods,
scheduler configuration, or real MIG state.
