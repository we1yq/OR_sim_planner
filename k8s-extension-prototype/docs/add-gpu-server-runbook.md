# Add a GPU Server Runbook

This runbook describes how to add a new GPU server or GPU Kubernetes cluster to
the OR-SIM MIG planning experiments. It assumes the controller and experiment
driver can run from a management host, while the GPU server runs Kubernetes,
NVIDIA GPU Operator, and the actual GPUs.

The current reference setup is:

```text
management host: desktap
target server:   rtx1
ssh endpoint:    we1yq@xxx.xxx.xxx.xxx -p 10533
target node:     rtx1
experiment ns:   or-sim
GPU operator ns: gpu-operator
```

## Architecture Choice

The recommended research setup is an external management-plane controller:

```text
management host
  repo, Python controller, planner, experiment scripts
  kubeconfig for one or more target clusters
  no local GPU required

target GPU cluster
  Kubernetes API server
  NVIDIA GPU Operator and MIG Manager
  or-sim CRDs and experiment objects
  real GPU nodes and MIG resources
```

The controller does not have to run inside every GPU cluster. Keeping it on the
management host makes multi-server experiments easier and avoids rebuilding and
pushing controller images during early development.

For the intended architecture where the management host runs the Kubernetes
control-plane and `rtx1`, `rtx2`, ... join as GPU workers, see
[Migrate To A Management Control Plane](migrate-to-management-control-plane.md).

Before adding multiple GPU servers, keep the planner/action boundary in
[MIG Planner Interface Contract](mig-action-interface.md) stable. In
particular, planner-local GPU ids must not be treated as Kubernetes node/device
identity; real execution should go through observed physical GPU bindings.

## 1. Confirm the Server

SSH to the server using the real lab endpoint:

```bash
ssh we1yq@xxx.xxx.xxx.xxx -p 10533
```

On the server, confirm Kubernetes and GPUs:

```bash
which kubectl
kubectl get nodes -o wide
kubectl get pods -A

which nvidia-smi
nvidia-smi -L
nvidia-smi
```

For a MIG-capable A100 node, confirm GPU Operator state:

```bash
kubectl get pods -n gpu-operator
kubectl describe node <node-name> | grep -i "nvidia.com/mig" -A 25
kubectl get runtimeclass
```

Success criteria:

```text
Node is Ready.
GPU Operator pods are Running or Completed.
nvidia.com/mig.config.state=success after MIG configuration.
Capacity and Allocatable include the expected MIG resource.
```

For the current `rtx1` setup, the expected MIG resource is:

```text
nvidia.com/mig-2g.10gb: 3
```

## 2. Open an SSH Tunnel

On the management host, open a tunnel to the target cluster API server.

Use the SSH port required by the lab server:

```bash
ssh -p 10533 -L 6443:127.0.0.1:6443 we1yq@xxx.xxx.xxx.xxx
```

Keep this terminal open. It forwards:

```text
management host https://127.0.0.1:6443
  -> target server 127.0.0.1:6443
  -> target Kubernetes API server
```

If local port `6443` is already used, use a different local port:

```bash
ssh -p 10533 -L 16443:127.0.0.1:6443 we1yq@xxx.xxx.xxx.xxx
```

Then use `https://127.0.0.1:16443` in the kubeconfig step below.

## 3. Copy and Name the Kubeconfig

On the management host:

```bash
mkdir -p ~/.kube
scp -P 10533 we1yq@xxx.xxx.xxx.xxx:~/.kube/config ~/.kube/rtx1-rke2.yaml
```

Point the copied kubeconfig at the local tunnel:

```bash
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml \
  kubectl config set-cluster default --server=https://127.0.0.1:6443
```

Rename only the context. This is enough to avoid confusing it with local kind
clusters:

```bash
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml \
  kubectl config rename-context default rtx1-rke2
```

If the context was already renamed, verify it:

```bash
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml kubectl config get-contexts
```

Expected:

```text
CURRENT   NAME        CLUSTER   AUTHINFO
*         rtx1-rke2   default   default
```

Do not use `kubectl config rename-cluster` or `rename-user`; those commands do
not exist in kubectl. Keeping `cluster: default` and `user: default` inside the
dedicated file is acceptable. The context name is the human-facing name.

If the context is accidentally changed to a non-existent cluster or user, repair
it:

```bash
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml \
  kubectl config set-context rtx1-rke2 --cluster=default --user=default
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml \
  kubectl config use-context rtx1-rke2
```

## 4. Verify Which Cluster You Are Operating

Always use an explicit kubeconfig when switching between the target server and
the local kind cluster.

Target server:

```bash
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml kubectl get nodes
```

Expected for the current server:

```text
rtx1
```

Local kind/dev cluster:

```bash
KUBECONFIG=$HOME/.kube/config kubectl get nodes
```

Expected for the current local dev setup:

```text
or-sim-dev-control-plane
```

If `kubectl` reports `localhost:8080 refused`, the current context probably
points at a missing cluster. Inspect and repair the kubeconfig:

```bash
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml kubectl config view
```

The copied target kubeconfig should contain:

```yaml
contexts:
- context:
    cluster: default
    user: default
  name: rtx1-rke2
current-context: rtx1-rke2
```

and:

```yaml
clusters:
- cluster:
    server: https://127.0.0.1:6443
  name: default
```

## 5. Create the Experiment Namespace

Create a dedicated namespace for OR-SIM experiment objects in the target
cluster:

```bash
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml \
  kubectl create namespace or-sim --dry-run=client -o yaml | \
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml \
  kubectl apply -f -
```

This does not affect NVIDIA GPU Operator. The intended namespace split is:

```text
gpu-operator
  NVIDIA GPU Operator, MIG Manager, device plugin, DCGM exporter

or-sim
  MigPlan, MigActionPlan, ObservedClusterState, action-plan CRs,
  experiment inputs, and experiment results
```

Safe operations:

```bash
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml kubectl get ns or-sim
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml kubectl get ns gpu-operator
```

Operations that can affect GPU Operator and should be done deliberately:

```text
helm upgrade gpu-operator ...
kubectl label node <node> nvidia.com/mig.config=...
kubectl delete pod -n gpu-operator ...
kubectl delete ns gpu-operator
```

## 6. Install OR-SIM CRDs and RBAC

From the repository on the management host:

```bash
cd ~/OR_sim_planner
```

Install CRDs into the target cluster:

```bash
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml \
  kubectl apply -f k8s-extension-prototype/manifests/crds/
```

Install service account and RBAC:

```bash
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml \
  kubectl apply -f k8s-extension-prototype/manifests/controller/
```

The controller Deployments in `manifests/controller/` use the development image:

```text
or-sim-mig-planner:dev
```

If this image is not present in the target cluster, the pods may stay Pending or
fail to start. For the external-controller workflow, scale them to zero:

```bash
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml \
  kubectl scale deployment -n or-sim mig-planner-controller --replicas=0
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml \
  kubectl scale deployment -n or-sim mig-dry-run-actuator --replicas=0
```

Keep the CRDs and RBAC. The management host controller still uses those CRDs as
the target cluster API surface.

Verify CRDs:

```bash
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml \
  kubectl get crd | grep -E 'mig|workload|serving|podlifecycle|observed'
```

## 7. Validate MIG Pod Scheduling

Run this on the target cluster through the management host kubeconfig:

```bash
cat <<'EOF' | KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: mig-test
spec:
  restartPolicy: Never
  containers:
  - name: cuda
    image: nvidia/cuda:12.4.1-base-ubuntu22.04
    command: ["nvidia-smi"]
    resources:
      limits:
        nvidia.com/mig-2g.10gb: 1
EOF
```

Watch and inspect:

```bash
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml kubectl get pod mig-test -w
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml kubectl logs mig-test
```

Success criteria:

```text
Pod reaches Completed.
Container nvidia-smi shows one MIG device.
```

Clean up:

```bash
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml kubectl delete pod mig-test
```

If the pod fails with:

```text
No help topic for 'disable-device-node-modification'
```

then the NVIDIA container toolkit/CDI hook is inconsistent with the generated
CDI spec. On `rtx1`, this was fixed by letting GPU Operator manage the toolkit:

```bash
helm upgrade gpu-operator nvidia/gpu-operator \
  -n gpu-operator \
  --reuse-values \
  --set toolkit.enabled=true \
  --wait \
  --timeout 15m
```

Then wait for:

```text
nvidia-container-toolkit-daemonset
nvidia-device-plugin-daemonset
gpu-feature-discovery
nvidia-operator-validator
```

to be Running.

## 8. Run the Physical GPU Registry Monitor

The long-running hardware monitor maintains `PhysicalGpuRegistry/default`.
It uses the current provider:

```text
GpuInventoryProvider = GPU Operator exec provider
```

That provider runs `nvidia-smi -L` inside a GPU Operator pod to collect real GPU
UUIDs and MIG device UUIDs. This is intentionally isolated behind the observer
interface so a future production deployment can replace it with a node
agent/exporter without changing planner logic.

Install the narrowly-scoped RBAC that allows `or-sim/mig-planner-controller` to
exec only in the `gpu-operator` namespace:

```bash
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml \
  kubectl apply -f k8s-extension-prototype/manifests/controller/registry-monitor-gpu-operator-rbac.yaml
```

Deploy the monitor:

```bash
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml \
  kubectl apply -f k8s-extension-prototype/manifests/controller/registry-monitor-deployment.yaml
```

Verify:

```bash
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml kubectl get pods -n or-sim
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml kubectl logs -n or-sim deployment/physical-gpu-registry-monitor
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml kubectl get physicalgpuregistry -n or-sim default -o yaml
```

Expected queue behavior:

```text
new A100 with existing MIG template -> transitioningQueue
A100 with nvidia.com/mig.config=or-sim-empty and state=success -> availableQueue
planner-claimed A100 -> activeQueue
non-A100 devices -> ignoredGpuDevices
```

Safety behavior:

```text
If any node has nvidia.com/mig.config.state=pending, the monitor does not exec
nvidia-smi -L in that cycle. It preserves stable GPU UUID bindings from the
previous PhysicalGpuRegistry and keeps affected GPUs in transitioningQueue.
After success/failed, it can exec again to refresh MIG device UUIDs.
```

For local debugging without a Deployment:

```bash
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml python3 k8s-extension-prototype/controller/main.py \
  --namespace or-sim \
  --run-physical-gpu-registry-monitor \
  --controller-max-cycles 1
```

## 9. Run the Real Cluster Observer

From the management host:

```bash
export KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml
python3 k8s-extension-prototype/controller/main.py \
  --namespace or-sim \
  --observe-cluster-state \
  --apply-observed-state
```

Verify:

```bash
kubectl get observedclusterstate -n or-sim
kubectl get observedclusterstate -n or-sim -o yaml
```

Current success criteria:

```text
ObservedClusterState exists.
status.phase=NodePodInventoryObserved.
nodeInventory includes the target node.
node labels include nvidia.com/mig.config.state=success.
capacity/allocatable include the expected nvidia.com/mig-* resources.
```

This is a smoke observer. It proves Kubernetes node/pod visibility and hardware
MIG resource visibility through the Kubernetes API, but it may still mark:

```text
readyForCanonicalization=false
missingRealClusterInputs includes MIG instance inventory, pod-to-MIG assignment,
and router metrics.
```

The registry monitor is now the preferred long-running observer path. Use this
one-shot command mainly for debugging the raw `ObservedClusterState`.

## 10. What to Record for a New Server

For each target server, record:

```text
server name:
ssh command:
kubeconfig file:
context name:
Kubernetes version:
node names:
GPU models:
GPU Operator version:
driver version:
CUDA runtime version:
MIG config:
MIG resource capacity:
MIG resource allocatable:
test pod result:
observer result:
physical registry result:
known caveats:
```

For the current `rtx1` server:

```text
server name: rtx1
ssh command: ssh we1yq@xxx.xxx.xxx.xxx -p 10533
kubeconfig file: ~/.kube/rtx1-rke2.yaml
context name: rtx1-rke2
Kubernetes version: v1.32.7+rke2r1
GPU Operator namespace: gpu-operator
experiment namespace: or-sim
MIG config: all-2g.10gb
MIG config state: success
MIG resource: nvidia.com/mig-2g.10gb
capacity: 3
allocatable: 3
test pod: nvidia/cuda:12.4.1-base-ubuntu22.04, Completed
physical registry: rtx1-gpu0 available, non-A100 GPUs ignored
```

## 11. When to Deploy In-Cluster Controllers

Use the external controller while the research system is changing quickly:

```text
management host runs Python controller and experiment scripts
target cluster stores CRDs, CRs, observations, and action plans
```

Deploy in-cluster controllers only after the image and interfaces are stable:

```text
or-sim namespace
  mig-planner-controller Deployment
  mig-dry-run-actuator Deployment
  physical-gpu-registry-monitor Deployment
```

In-cluster deployment is useful for long-running autonomous experiments, but it
requires a real image registry or image import flow for every target cluster.
