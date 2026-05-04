#!/usr/bin/env bash
# Cert-based CCS bootstrap for the two same-namespace kind clusters (Basic tier).
#
# Both clusters are signed by the SAME shared transport CA (synced from Vault
# by the chart's vault-secret-transport-ca template) so they mutually trust
# at the transport layer. They share the same `elastic` password so CCS can
# forward the calling user's credentials. All this script does is:
#
#   PUT /_cluster/settings on each cluster, registering the peer with
#   cluster.remote.<peer>.proxy_address pointing at <peer>-es-transport:9300.
#
# Re-runnable: PUT /_cluster/settings is idempotent.
set -euo pipefail

NS="chat"
SITE1="es-chat-site1"
SITE2="es-chat-site2"
PW="chat-elastic-pw"           # shared elastic password (kind/setup.sh seeded this in Vault)

log() { printf "\n\033[1;36m▶ %s\033[0m\n" "$*"; }

# curl from inside the gateway pod (no sidecar, can reach both ES Services).
ec() {
  local site="$1" method="$2" path="$3" body="${4:-}"
  local args=(-k -sS -u "elastic:${PW}" -X "${method}"
              "https://${site}-es-http.${NS}.svc.cluster.local:9200${path}"
              -H 'content-type: application/json')
  [[ -n "${body}" ]] && args+=(-d "${body}")
  kubectl -n "${NS}" exec deploy/chat-ingressgateway -- curl "${args[@]}"
}

register() {
  local local_site="$1" peer="$2"
  log "Registering remote ${peer} on ${local_site}"
  ec "${local_site}" PUT /_cluster/settings "$(cat <<EOF
{
  "persistent": {
    "cluster.remote.${peer}.mode": "proxy",
    "cluster.remote.${peer}.proxy_address": "${peer}-es-transport.${NS}.svc.cluster.local:9300",
    "cluster.remote.${peer}.skip_unavailable": true
  }
}
EOF
)"
  echo
}
register "${SITE1}" "${SITE2}"
register "${SITE2}" "${SITE1}"

log "Verifying _remote/info on both sides"
echo "${SITE1} → ${SITE2}:"
ec "${SITE1}" GET /_remote/info
echo
echo "${SITE2} → ${SITE1}:"
ec "${SITE2}" GET /_remote/info
echo
