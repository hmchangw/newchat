#!/usr/bin/env bash
# Cert-based CCS registration script — works in two modes:
#
#   MODE=internal  (default, used by kind/setup.sh)
#     For peers in the SAME K8s cluster + namespace. proxy_address points
#     at the peer's in-cluster transport Service:
#       <peer-cluster-name>-es-transport.<ns>.svc.cluster.local:9300
#
#   MODE=public
#     For peers in OTHER K8s clusters. proxy_address points at the peer's
#     public Istio passthrough endpoint (resolved via DNS to that K8s
#     cluster's Istio LB IP):
#       es-remote-<peer-site>.<public-domain>:443
#     Also sets server_name so Istio's SNI route on 443 hits the right VS.
#
# Both modes share the same trust + auth substrate:
#   - shared transport CA (Secret name in .Values.ccs.transport.caSecretName,
#     same content in every cluster's chat namespace, synced from Vault)
#   - shared elastic password (Vault path elasticsearch/elastic-user)
#
# Run once per cluster, with kubectl context pointing at THAT cluster.
# Re-runnable — PUT /_cluster/settings is idempotent.
#
# Usage examples:
#   # Phase 1 (kind, same namespace):
#   MODE=internal LOCAL_SITE=site1 PEERS=site2 ./register-remotes.sh
#   MODE=internal LOCAL_SITE=site2 PEERS=site1 ./register-remotes.sh
#
#   # Phase 2 (12 K8s clusters, run on each):
#   MODE=public LOCAL_SITE=site1 PEERS=site2,site3,...,site12 \
#     PUBLIC_DOMAIN=chat.com ./register-remotes.sh
#
# Required:
#   LOCAL_SITE    site identifier of THIS cluster (e.g. site1)
#   PEERS         comma-separated peer site identifiers (e.g. site2,site3)
#
# Optional:
#   MODE          internal | public                    (default: internal)
#   PUBLIC_DOMAIN public domain for es-remote-<site>.X (default: chat.com)
#   PUBLIC_PORT   port on the peer's public endpoint     (default: 443)
#                 Use 30443 when multi-kind testing where both clusters'
#                 NodePorts can't both bind host port 443.
#   NAMESPACE     K8s namespace                         (default: chat)
#   ES_PREFIX     ES cluster name prefix (matches chart's cluster.name +
#                 properties.division convention)       (default: es-chat)
#   ELASTIC_PW    shared elastic password               (default: chat-elastic-pw)
#   GATEWAY       deploy/<name> we exec curl from       (default: deploy/chat-ingressgateway)

set -euo pipefail

MODE="${MODE:-internal}"
LOCAL_SITE="${LOCAL_SITE:?LOCAL_SITE is required (e.g. site1)}"
PEERS="${PEERS:?PEERS is required (comma-separated, e.g. site2,site3)}"
PUBLIC_DOMAIN="${PUBLIC_DOMAIN:-chat.com}"
PUBLIC_PORT="${PUBLIC_PORT:-443}"
NAMESPACE="${NAMESPACE:-chat}"
ES_PREFIX="${ES_PREFIX:-es-chat}"
ELASTIC_PW="${ELASTIC_PW:-chat-elastic-pw}"
GATEWAY="${GATEWAY:-deploy/chat-ingressgateway}"

case "${MODE}" in
  internal|public) ;;
  *) echo "ERROR: MODE must be 'internal' or 'public', got '${MODE}'" >&2; exit 2 ;;
esac

LOCAL_ES="${ES_PREFIX}-${LOCAL_SITE}"

log() { printf "\n\033[1;36m▶ %s\033[0m\n" "$*"; }

# curl from inside the local cluster's ingressgateway pod (no sidecar in front
# of the Envoy gateway, can reach the local ES HTTP Service).
ec() {
  local method="$1" path="$2" body="${3:-}"
  local args=(-k -sS -u "elastic:${ELASTIC_PW}" -X "${method}"
              "https://${LOCAL_ES}-es-http.${NAMESPACE}.svc.cluster.local:9200${path}"
              -H 'content-type: application/json')
  [[ -n "${body}" ]] && args+=(-d "${body}")
  kubectl -n "${NAMESPACE}" exec "${GATEWAY}" -- curl "${args[@]}"
}

# Build cluster.remote.<peer>.* settings for one peer, in the appropriate mode.
build_remote_settings() {
  local peer_site="$1"
  local peer_es="${ES_PREFIX}-${peer_site}"
  local proxy_address server_name_line

  if [[ "${MODE}" == "internal" ]]; then
    proxy_address="${peer_es}-es-transport.${NAMESPACE}.svc.cluster.local:9300"
    server_name_line=""
  else
    proxy_address="es-remote-${peer_site}.${PUBLIC_DOMAIN}:${PUBLIC_PORT}"
    # In public mode, server_name MUST match the SNI host the Istio Gateway
    # is configured for, otherwise the VirtualService SNI route doesn't fire
    # and Istio drops the connection.
    server_name_line=$',\n    "cluster.remote.'"${peer_es}"'.server_name": "es-remote-'"${peer_site}.${PUBLIC_DOMAIN}"'"'
  fi

  cat <<EOF
{
  "persistent": {
    "cluster.remote.${peer_es}.mode": "proxy",
    "cluster.remote.${peer_es}.proxy_address": "${proxy_address}"${server_name_line},
    "cluster.remote.${peer_es}.skip_unavailable": true
  }
}
EOF
}

log "Registering peers on ${LOCAL_ES} (mode=${MODE})"
IFS=',' read -ra peer_list <<< "${PEERS}"
for peer in "${peer_list[@]}"; do
  peer="${peer// /}"   # trim spaces
  [[ -z "${peer}" ]] && continue
  printf "  → %s\n" "${peer}"
  ec PUT /_cluster/settings "$(build_remote_settings "${peer}")"
  echo
done

log "Verifying _remote/info on ${LOCAL_ES}"
ec GET /_remote/info
echo
