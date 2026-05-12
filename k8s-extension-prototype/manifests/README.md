# Manifests

Kubernetes YAML files for the prototype.

For now these files are design skeletons and safe examples. They do not trigger
real GPU or MIG operations.

## Files

- `namespace.yaml`: namespace for local experiments.
- `crds/workloadrequest-crd.yaml`: future CRD for workload demand.
- `crds/migplan-crd.yaml`: future CRD for dry-run plans.
- `crds/migactionplan-crd.yaml`: dry-run execution-boundary CRD generated from
  planner actions.
- `crds/workloadrouteplan-crd.yaml`: dry-run router action CRD for stop/reroute
  previews.
- `crds/servinginstancedrain-crd.yaml`: dry-run serving drain CRD for inflight
  drain previews.
- `crds/podlifecycleplan-crd.yaml`: dry-run pod lifecycle CRD for
  create/reuse/drain/delete/reload previews.
- `crds/observedclusterstate-crd.yaml`: observer handoff CRD for preview or
  actual post-action cluster state.
- `crds/autoapprovalpolicy-crd.yaml`: policy gate for automatically approving
  safe dry-run action plans.
- `controller/*.yaml`: service account, namespaced RBAC, optional observer
  read-only ClusterRole, optional GPU Operator exec RBAC, and Deployments for
  the dry-run controller loops and physical GPU registry monitor.
- `examples/workloadrequests/*.yaml`: workload requests generated from the
  simulation workload specs.
- `examples/profile-catalogs/kustomization.yaml`: profile catalog ConfigMaps
  generated with `data.catalog.yaml` from the catalog YAMLs in that directory.
- `examples/autoapprovalpolicies/default.yaml`: conservative dry-run-only
  approval policy.
- `examples/scenarios/*-configmap.yaml`: PlanningScenario inputs as ConfigMaps.
- `examples/arrival-snapshots/*-configmap.yaml`: optional external arrival
  snapshots that can override a scenario's target arrival for a planning epoch.
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
kubectl apply -k k8s-extension-prototype/manifests/examples/profile-catalogs/
kubectl apply -f k8s-extension-prototype/manifests/examples/autoapprovalpolicies/
kubectl apply -f k8s-extension-prototype/manifests/examples/scenarios/
# Optional: use when a MigPlan references spec.arrivalSnapshotConfigMap.
kubectl apply -f k8s-extension-prototype/manifests/examples/arrival-snapshots/
kubectl apply -f k8s-extension-prototype/manifests/examples/migplans/
```

Watch dry-run status:

```bash
kubectl get migplans -n or-sim
kubectl get migactionplans -n or-sim
kubectl get workloadrouteplans,servinginstancedrains,podlifecycleplans -n or-sim
kubectl get observedclusterstates -n or-sim
kubectl get autoapprovalpolicies -n or-sim
kubectl get configmaps -n or-sim -l mig.or-sim.io/input-kind=profile-catalog
kubectl get configmaps -n or-sim -l mig.or-sim.io/state-kind=canonical-next-state
```

The controller image uses the Kubernetes Python client directly and watches
`MigPlan` API events; it does not carry `kubectl` inside the container. The Deployment still only patches
`MigPlan.status` and writes canonical state ConfigMaps. It does not modify pods,
scheduler configuration, or real MIG state.

## Deploy The Physical GPU Registry Monitor

The long-running monitor keeps `PhysicalGpuRegistry/default` up to date. It
uses GPU Operator pod exec to run `nvidia-smi -L` for real GPU UUID and MIG UUID
inventory, so apply the narrowly-scoped `gpu-operator` namespace RBAC first:

```bash
kubectl apply -f k8s-extension-prototype/manifests/controller/registry-monitor-gpu-operator-rbac.yaml
kubectl apply -f k8s-extension-prototype/manifests/controller/registry-monitor-deployment.yaml
```

Check it:

```bash
kubectl logs -n or-sim deployment/physical-gpu-registry-monitor
kubectl get physicalgpuregistry -n or-sim default -o yaml
```

The monitor skips GPU Operator exec while any node has
`nvidia.com/mig.config.state=pending`; it reuses cached GPU UUID bindings and
waits for a success/failed state before refreshing MIG device UUIDs.
