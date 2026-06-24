#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-gpu-operator}"
CONTROL_PLANE_HOSTNAME="${CONTROL_PLANE_HOSTNAME:-or-sim-control-plane}"

DEPLOYMENTS=(
  gpu-operator
  gpu-operator-node-feature-discovery-master
  gpu-operator-node-feature-discovery-gc
)

PATCH=$(
  printf '{"spec":{"template":{"spec":{"nodeSelector":{"kubernetes.io/hostname":"%s"}}}}}' \
    "${CONTROL_PLANE_HOSTNAME}"
)

kubectl patch deployment -n "${NAMESPACE}" "${DEPLOYMENTS[@]}" \
  --type='merge' \
  -p "${PATCH}"

for deployment in "${DEPLOYMENTS[@]}"; do
  kubectl rollout status -n "${NAMESPACE}" "deployment/${deployment}" --timeout=180s
done

kubectl get pods -n "${NAMESPACE}" -o wide
