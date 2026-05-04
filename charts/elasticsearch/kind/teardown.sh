#!/usr/bin/env bash
# Tears down the kind cluster created by setup.sh.
set -euo pipefail
CLUSTER_NAME="chat-eck"

if kind get clusters | grep -qx "${CLUSTER_NAME}"; then
  echo "Deleting kind cluster '${CLUSTER_NAME}'"
  kind delete cluster --name "${CLUSTER_NAME}"
else
  echo "kind cluster '${CLUSTER_NAME}' not present, nothing to do"
fi
