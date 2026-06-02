#!/bin/sh
set -eu

mode="${RUNTIME_MODE:-synthetic}"
case "$mode" in
  torchvision|real|torch)
    exec python /torchvision_runtime.py "$@"
    ;;
  synthetic|"")
    exec /model-runtime "$@"
    ;;
  *)
    echo "unknown RUNTIME_MODE=$mode" >&2
    exit 2
    ;;
esac
