#!/usr/bin/env bash
set -euo pipefail

KUBECONFIG_PATH="${KUBECONFIG:-$HOME/.kube/rtx1-rke2.yaml}"
STAMP="$(date +%Y%m%d-%H%M%S)"
BACKUP_ROOT="${BACKUP_ROOT:-k8s-extension-prototype/backups}"
BACKUP_DIR="${BACKUP_DIR:-$BACKUP_ROOT/rtx1-cluster-$STAMP}"

mkdir -p "$BACKUP_DIR"

run_kubectl() {
  KUBECONFIG="$KUBECONFIG_PATH" kubectl "$@"
}

capture_yaml() {
  local name="$1"
  shift
  if run_kubectl "$@" -o yaml >"$BACKUP_DIR/$name.yaml" 2>"$BACKUP_DIR/$name.err"; then
    rm -f "$BACKUP_DIR/$name.err"
  else
    mv "$BACKUP_DIR/$name.err" "$BACKUP_DIR/$name.error.txt"
    rm -f "$BACKUP_DIR/$name.yaml"
  fi
}

capture_text() {
  local name="$1"
  shift
  if run_kubectl "$@" >"$BACKUP_DIR/$name.txt" 2>"$BACKUP_DIR/$name.err"; then
    rm -f "$BACKUP_DIR/$name.err"
  else
    mv "$BACKUP_DIR/$name.err" "$BACKUP_DIR/$name.error.txt"
    rm -f "$BACKUP_DIR/$name.txt"
  fi
}

{
  echo "createdAt: $(date -Iseconds)"
  echo "kubeconfig: $KUBECONFIG_PATH"
  echo "backupDir: $BACKUP_DIR"
} >"$BACKUP_DIR/manifest.txt"

capture_yaml nodes get nodes
capture_yaml node-rtx1 get node rtx1
capture_text node-rtx1-describe describe node rtx1
capture_yaml namespaces get namespaces
capture_yaml crds get crds
capture_yaml runtimeclasses get runtimeclass
capture_yaml storageclasses get storageclass

capture_yaml or-sim-all get all,cm,secret,sa,role,rolebinding -n or-sim
capture_yaml or-sim-crs get physicalgpuregistry,observedclusterstate,migplans,migactionplans,workloadrouteplans,servinginstancedrains,podlifecycleplans,workloadrequests,autoapprovalpolicies -n or-sim

capture_yaml gpu-operator-all get all,cm,secret,sa,role,rolebinding -n gpu-operator
capture_yaml clusterpolicy get clusterpolicy cluster-policy

capture_text pods-all-wide get pods -A -o wide
capture_text nodes-wide get nodes -o wide
capture_text contexts config get-contexts

tar -C "$BACKUP_ROOT" -czf "$BACKUP_DIR.tar.gz" "$(basename "$BACKUP_DIR")"

echo "Backup directory: $BACKUP_DIR"
echo "Backup archive:   $BACKUP_DIR.tar.gz"
