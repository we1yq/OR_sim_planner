#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-/tmp/migrant-go-build-context.yaml}"
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

tar -C "${ROOT}" \
  --exclude='./reports' \
  --exclude='./.git' \
  -czf "${TMP}/context.tar.gz" \
  .

kubectl create configmap migrant-go-build-context \
  --namespace or-sim \
  --from-file=context.tar.gz="${TMP}/context.tar.gz" \
  --dry-run=client \
  -o yaml > "${OUT}"

printf '%s\n' "${OUT}"
