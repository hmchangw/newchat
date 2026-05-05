#!/usr/bin/env bash
# Tears down both kind clusters created by setup-multi.sh.
set -euo pipefail
for site in site1 site2; do
  cn="chat-eck-${site}"
  if kind get clusters | grep -qx "${cn}"; then
    echo "Deleting kind cluster '${cn}'"
    kind delete cluster --name "${cn}"
  else
    echo "kind cluster '${cn}' not present, skipping"
  fi
done
