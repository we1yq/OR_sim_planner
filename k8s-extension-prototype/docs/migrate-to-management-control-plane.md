# Migrate To A Management Control Plane

This runbook moves the MIGRANT hardware experiment from the temporary
`rtx1` single-node RKE2 cluster to the intended edge-cluster architecture:

```text
management host
  RKE2 server / Kubernetes control-plane
  MIGRANT CRDs, controller, registry monitor
  NVIDIA GPU Operator cluster-level controller

rtx1, rtx2, ...
  RKE2 agent / Kubernetes worker nodes
  NVIDIA GPU Operator node daemons
  A100 GPUs, MIG Manager, device-plugin
```

The management host does not need a GPU. GPU Operator is installed once per
cluster and then deploys node-level components only on GPU nodes.

## Current Temporary State

The current validation setup is:

```text
rtx1
  RKE2 server/control-plane/etcd/master
  GPU Operator
  MIGRANT CRDs and registry monitor
  one A100 admitted by PhysicalGpuRegistry
```

This was useful for proving that GPU Operator, MIG reconfiguration,
`or-sim-empty`, and `PhysicalGpuRegistry` work on real hardware. It is not the
final multi-node research architecture.

## Migration Safety Rule

Do not stop the old `rtx1` RKE2 server until all of these are true:

- the current cluster resources are backed up,
- the management host IP is stable and reachable from `rtx1`,
- the RKE2 server on the management host is running,
- the management host server token is available,
- you accept that the old single-node cluster will be replaced.

Stopping `rke2-server` on `rtx1` removes access to the old Kubernetes API until
rollback or migration is complete.

## 1. Backup Current rtx1 Cluster Resources

From the repo root on the management host:

```bash
KUBECONFIG=$HOME/.kube/rtx1-rke2.yaml \
  k8s-extension-prototype/tools/backup_cluster_state.sh
```

This writes an archive under:

```text
k8s-extension-prototype/backups/
```

Copy the archive to `rtx1` local storage:

```bash
scp -P 10533 k8s-extension-prototype/backups/<archive>.tar.gz \
  we1yq@115.145.179.130:~/or-sim-migration-backups/
```

On `rtx1`, also save host-level RKE2/GPU state before any destructive change:

```bash
TS=$(date +%Y%m%d-%H%M%S)
mkdir -p "$HOME/or-sim-migration-backups/rtx1-host-$TS"

nvidia-smi -L > "$HOME/or-sim-migration-backups/rtx1-host-$TS/nvidia-smi-L.txt"
nvidia-smi > "$HOME/or-sim-migration-backups/rtx1-host-$TS/nvidia-smi.txt"
kubectl get nodes -o wide > "$HOME/or-sim-migration-backups/rtx1-host-$TS/nodes.txt"
kubectl get pods -A -o wide > "$HOME/or-sim-migration-backups/rtx1-host-$TS/pods.txt"

sudo tar -czf "$HOME/or-sim-migration-backups/rtx1-rke2-host-files-$TS.tar.gz" \
  /etc/rancher/rke2 \
  /var/lib/rancher/rke2/server/manifests \
  /var/lib/rancher/rke2/server/cred \
  2>/tmp/or-sim-rke2-backup-warnings.txt
```

The `sudo tar` command may warn about files changing while being read. Keep
`/tmp/or-sim-rke2-backup-warnings.txt` with the backup.

## 2. Prepare The Management Host

Choose the management host IP that `rtx1` can reach. In the commands below,
replace:

```text
<MGMT_HOST_IP>
```

with that stable IP address.

Install RKE2 server on the management host:

```bash
curl -sfL https://get.rke2.io | sudo sh -
sudo mkdir -p /etc/rancher/rke2
sudo tee /etc/rancher/rke2/config.yaml >/dev/null <<EOF
node-name: or-sim-control-plane
tls-san:
  - <MGMT_HOST_IP>
EOF
sudo systemctl enable --now rke2-server
```

Configure local kubectl on the management host:

```bash
mkdir -p "$HOME/.kube"
sudo cp /etc/rancher/rke2/rke2.yaml "$HOME/.kube/or-sim-edge.yaml"
sudo chown "$USER:$USER" "$HOME/.kube/or-sim-edge.yaml"
sed -i "s/127.0.0.1/<MGMT_HOST_IP>/" "$HOME/.kube/or-sim-edge.yaml"
KUBECONFIG=$HOME/.kube/or-sim-edge.yaml kubectl get nodes -o wide
```

Get the token needed by worker nodes:

```bash
sudo cat /var/lib/rancher/rke2/server/node-token
```

## 3. Convert rtx1 From Old Server To New Worker

This is the disruptive step. Run it only after confirming the backup.

On `rtx1`, stop the old single-node server:

```bash
sudo systemctl stop rke2-server
sudo systemctl disable rke2-server
```

Install or reconfigure RKE2 agent:

```bash
curl -sfL https://get.rke2.io | INSTALL_RKE2_TYPE="agent" sudo sh -
sudo mkdir -p /etc/rancher/rke2
sudo tee /etc/rancher/rke2/config.yaml >/dev/null <<EOF
server: https://<MGMT_HOST_IP>:9345
token: <NODE_TOKEN_FROM_MANAGEMENT_HOST>
node-name: rtx1
EOF
sudo systemctl enable --now rke2-agent
```

Back on the management host:

```bash
KUBECONFIG=$HOME/.kube/or-sim-edge.yaml kubectl get nodes -o wide
```

Expected:

```text
or-sim-control-plane   Ready   control-plane,etcd,master
rtx1                   Ready   <none>
```

## 4. Install GPU Operator In The New Cluster

Install GPU Operator once into the new cluster. The operator controller may run
on the management host node, but node-level GPU pods should schedule on `rtx1`
and future GPU nodes.

After installation, confirm:

```bash
KUBECONFIG=$HOME/.kube/or-sim-edge.yaml kubectl get pods -n gpu-operator -o wide
KUBECONFIG=$HOME/.kube/or-sim-edge.yaml kubectl describe node rtx1 | grep -i "nvidia.com" -A 80
```

Install the MIGRANT MIG parted config and reset the A100 to the strict empty
state:

```bash
KUBECONFIG=$HOME/.kube/or-sim-edge.yaml \
  python3 k8s-extension-prototype/tools/install_migrant_mig_configs.py

KUBECONFIG=$HOME/.kube/or-sim-edge.yaml \
  kubectl label node rtx1 nvidia.com/mig.config=or-sim-empty --overwrite
KUBECONFIG=$HOME/.kube/or-sim-edge.yaml \
  kubectl label node rtx1 nvidia.com/mig.config.state- --overwrite
```

Wait for:

```text
nvidia.com/mig.config=or-sim-empty
nvidia.com/mig.config.state=success
no MIG devices in nvidia-smi
```

## 5. Deploy MIGRANT To The New Cluster

```bash
KUBECONFIG=$HOME/.kube/or-sim-edge.yaml kubectl apply -f k8s-extension-prototype/manifests/namespace.yaml
KUBECONFIG=$HOME/.kube/or-sim-edge.yaml kubectl apply -f k8s-extension-prototype/manifests/crds/
KUBECONFIG=$HOME/.kube/or-sim-edge.yaml kubectl apply -f k8s-extension-prototype/manifests/controller/
```

Start or verify the registry monitor:

```bash
KUBECONFIG=$HOME/.kube/or-sim-edge.yaml \
  kubectl get deployment -n or-sim physical-gpu-registry-monitor

KUBECONFIG=$HOME/.kube/or-sim-edge.yaml \
  kubectl get physicalgpuregistry -n or-sim default -o yaml
```

Expected for a clean migrated `rtx1`:

```text
discoveredA100: [rtx1-gpu0]
activeQueue: []
availableQueue: [rtx1-gpu0]
transitioningQueue: []
ignoredGpuDevices: non-A100 devices only
```

## 6. Add Future GPU Servers

Future servers such as `rtx2` should join the same management-host cluster as
RKE2 agents. Do not copy the old `rtx1` kubeconfig to them as a separate
cluster identity. Each new server uses the management host URL and token:

```yaml
server: https://<MGMT_HOST_IP>:9345
token: <NODE_TOKEN_FROM_MANAGEMENT_HOST>
node-name: rtx2
```

The global queues live in one `PhysicalGpuRegistry/default` object in the
management cluster:

```text
discoveredA100: all observed A100 physical IDs
availableQueue: clean A100 GPUs ready for planner allocation
activeQueue: planner-owned active physical IDs
transitioningQueue: A100 GPUs being cleaned or reconfigured
```

That is what makes multi-server planning global rather than per-server.

## Rollback Notes

If the migration fails before `rtx1` joins the new cluster:

1. keep the backup archives,
2. stop `rke2-agent` on `rtx1`,
3. re-enable the old `rke2-server`,
4. use the backed-up kubeconfig to inspect the old cluster.

```bash
sudo systemctl stop rke2-agent
sudo systemctl disable rke2-agent
sudo systemctl enable --now rke2-server
```

Rollback after deleting `/var/lib/rancher/rke2` is much harder. Do not delete
that directory during the first migration attempt.
