#!/usr/bin/env bash
# Two-kind-cluster cross-K8s harness — verifies the chart's MODE=public CCS
# path end-to-end, mirroring the user's prod topology (each K8s cluster has
# its own Vault, shared CA placed manually in every cluster's Vault, public
# Istio passthrough for cross-cluster transport).
#
# Layout:
#   chat-eck-site1 (kind cluster) — Istio + ECK + Vault + ES (es-chat-site1)
#   chat-eck-site2 (kind cluster) — Istio + ECK + Vault + ES (es-chat-site2)
#
# Cross-cluster reachability:
#   site1 ES dials es-remote-site2.chat.com:30443 → cluster site2's container IP
#                  ↓ (resolved via patched CoreDNS in cluster site1)
#                  hits cluster site2's Istio gateway NodePort
#                  ↓ (SNI=es-remote-site2.chat.com matches Gateway+VS)
#                  PASSTHROUGH to ES transport port 9300 in cluster site2
#                  ↓ (TLS via shared CA in both clusters → mutual trust)
#
# Run from the repo root: ./charts/elasticsearch/kind-multi/setup-multi.sh
# Re-runnable. Uses ./charts/elasticsearch/kind/charts/*.tgz (vendored).

set -euo pipefail

ECK_VERSION="2.16.1"
ISTIO_VERSION="1.24.2"
APP_NS="chat"
SITES=(site1 site2)

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
KIND_MULTI="${ROOT}/charts/elasticsearch/kind-multi"
KIND_VENDOR="${ROOT}/charts/elasticsearch/kind"        # reuse vendored charts + manifests
VCHARTS="${KIND_VENDOR}/charts"
MANIFESTS="${KIND_VENDOR}/manifests"

ISTIO_BASE="${VCHARTS}/base-1.24.2.tgz"
ISTIOD="${VCHARTS}/istiod-1.24.2.tgz"
ISTIO_GATEWAY="${VCHARTS}/gateway-1.24.2.tgz"
ECK_OPERATOR="${VCHARTS}/eck-operator-2.16.1.tgz"
VAULT="${VCHARTS}/vault-0.32.0.tgz"
VSO="${VCHARTS}/vault-secrets-operator-0.10.0.tgz"

log()  { printf "\n\033[1;32m▶ %s\033[0m\n" "$*"; }
sub()  { printf "\033[1;36m  • %s\033[0m\n" "$*"; }

cluster_name() { echo "chat-eck-$1"; }
ctx_name()     { echo "kind-chat-eck-$1"; }
container()    { echo "$(cluster_name "$1")-control-plane"; }

# ─────────────────────────────────────────────────────────────────────────────
# 0. Generate the SHARED transport CA + shared elastic password ONCE.
#    These are what gets placed manually into every cluster's Vault below.
# ─────────────────────────────────────────────────────────────────────────────
log "Generating shared transport CA + shared elastic password (once, on host)"
TMP=$(mktemp -d)
trap "rm -rf '${TMP}'" EXIT
openssl req -x509 -nodes -newkey rsa:2048 -days 36500 \
  -subj "/CN=chat-transport-ca" \
  -keyout "${TMP}/ca.key" -out "${TMP}/ca.crt" 2>/dev/null
SHARED_CA_CRT=$(cat "${TMP}/ca.crt")
SHARED_CA_KEY=$(cat "${TMP}/ca.key")
SHARED_ELASTIC_PW="chat-elastic-pw"

# ─────────────────────────────────────────────────────────────────────────────
# 1. Create both kind clusters
# ─────────────────────────────────────────────────────────────────────────────
for site in "${SITES[@]}"; do
  cn=$(cluster_name "${site}")
  if kind get clusters | grep -qx "${cn}"; then
    log "kind cluster '${cn}' already exists — reusing"
  else
    log "Creating kind cluster '${cn}'"
    kind create cluster --name "${cn}" --config "${KIND_MULTI}/kind-config.yaml"
  fi
done

# Discover each cluster's container IP on the kind Docker network.
# These are the addresses peers use as the public LB IP target.
# (Plain variables instead of an associative array — macOS bash 3.2 lacks `declare -A`.)
IP_site1=$(docker inspect "$(container site1)" \
  -f '{{(index .NetworkSettings.Networks "kind").IPAddress}}')
IP_site2=$(docker inspect "$(container site2)" \
  -f '{{(index .NetworkSettings.Networks "kind").IPAddress}}')
log "kind container IP for site1: ${IP_site1}"
log "kind container IP for site2: ${IP_site2}"
peer_ip_for() {
  case "$1" in
    site1) echo "${IP_site2}" ;;
    site2) echo "${IP_site1}" ;;
    *) echo "ERROR: unknown site '$1'" >&2; exit 2 ;;
  esac
}

# ─────────────────────────────────────────────────────────────────────────────
# 2. For each cluster: install Istio + ingressgateway + ECK + Vault + VSO
# ─────────────────────────────────────────────────────────────────────────────
install_infra() {
  local site="$1"
  local ctx="$(ctx_name "${site}")"

  log "[${site}] Installing istio-base + istiod"
  kubectl --context "${ctx}" create ns istio-system --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f -
  helm --kube-context "${ctx}" upgrade --install istio-base "${ISTIO_BASE}" \
    -n istio-system --wait --force-conflicts \
    -f "${MANIFESTS}/istio-base-values.yaml"
  helm --kube-context "${ctx}" upgrade --install istiod "${ISTIOD}" \
    -n istio-system --wait --force-conflicts \
    -f "${MANIFESTS}/istiod-values.yaml"

  log "[${site}] Creating chat namespace + per-namespace ingressgateway"
  kubectl --context "${ctx}" apply -f "${MANIFESTS}/namespace.yaml"
  helm --kube-context "${ctx}" upgrade --install chat-ingressgateway "${ISTIO_GATEWAY}" \
    -n "${APP_NS}" --wait --skip-schema-validation \
    -f "${MANIFESTS}/istio-gateway-values.yaml"

  log "[${site}] Installing ECK operator"
  helm --kube-context "${ctx}" upgrade --install eck-operator "${ECK_OPERATOR}" \
    -n elastic-system --create-namespace --wait \
    -f "${MANIFESTS}/eck-operator-values.yaml"

  log "[${site}] Installing Vault (dev mode)"
  helm --kube-context "${ctx}" upgrade --install vault "${VAULT}" \
    -n vault --create-namespace --wait \
    -f "${MANIFESTS}/vault-values.yaml"
  kubectl --context "${ctx}" -n vault wait --for=condition=Ready pod/vault-0 --timeout=180s

  log "[${site}] Configuring Vault: kubernetes auth + KV v2 + chat-app role"
  kubectl --context "${ctx}" -n vault exec -i vault-0 -- env \
    VAULT_ADDR=http://127.0.0.1:8200 VAULT_TOKEN=root sh -c '
    set -e
    vault auth enable -path=kubernetes kubernetes 2>/dev/null || true
    vault write auth/kubernetes/config \
      kubernetes_host="https://kubernetes.default.svc.cluster.local" \
      disable_iss_validation=true
    vault policy write chat-app - <<EOF
path "secret/data/elasticsearch/*"     { capabilities = ["read"] }
path "secret/metadata/elasticsearch/*" { capabilities = ["read"] }
EOF
    vault write auth/kubernetes/role/chat-app \
      bound_service_account_names=default \
      bound_service_account_namespaces=chat \
      policies=chat-app audience=vault ttl=24h
    vault secrets enable -path=secret -version=2 kv 2>/dev/null || true
  '

  log "[${site}] Seeding Vault with the SHARED CA + SHARED elastic password (manual sync)"
  sub "shared CA → secret/elasticsearch/transport-ca (same on every cluster)"
  kubectl --context "${ctx}" -n vault exec -i vault-0 -- env \
    VAULT_ADDR=http://127.0.0.1:8200 VAULT_TOKEN=root \
    vault kv put secret/elasticsearch/transport-ca \
    "tls.crt=${SHARED_CA_CRT}" "tls.key=${SHARED_CA_KEY}" >/dev/null
  sub "shared elastic password → secret/elasticsearch/elastic-user"
  kubectl --context "${ctx}" -n vault exec -i vault-0 -- env \
    VAULT_ADDR=http://127.0.0.1:8200 VAULT_TOKEN=root \
    vault kv put secret/elasticsearch/elastic-user elastic="${SHARED_ELASTIC_PW}" >/dev/null
  sub "per-site MinIO dummy creds → secret/elasticsearch/${site}/minio"
  kubectl --context "${ctx}" -n vault exec -i vault-0 -- env \
    VAULT_ADDR=http://127.0.0.1:8200 VAULT_TOKEN=root \
    vault kv put "secret/elasticsearch/${site}/minio" \
    MINIO_BUCKET_ACCESS_KEY=dummy MINIO_BUCKET_SECRET_KEY=dummy >/dev/null

  log "[${site}] Installing VSO + applying VaultConnection/VaultAuth"
  helm --kube-context "${ctx}" upgrade --install vault-secrets-operator "${VSO}" \
    -n vault-secrets-operator-system --create-namespace --wait \
    -f "${MANIFESTS}/vault-secrets-operator-values.yaml"
  kubectl --context "${ctx}" apply -f "${MANIFESTS}/vault-auth.yaml"
}

for site in "${SITES[@]}"; do install_infra "${site}"; done

# ─────────────────────────────────────────────────────────────────────────────
# 3. Patch CoreDNS BEFORE the chart install, so ES resolves its peers on the
#    very first connection attempt. If ES starts before DNS is ready, the JVM
#    caches the negative lookup and the remote stays disconnected until a manual
#    reconnect. No public DNS in this rig → map each peer's public hostname to
#    its kind container IP via the hosts plugin.
# ─────────────────────────────────────────────────────────────────────────────
patch_coredns() {
  local site="$1" peer="$2" peer_ip="$3"
  local ctx="$(ctx_name "${site}")"
  log "[${site}] patching CoreDNS so es-remote-${peer}.chat.com resolves to ${peer_ip}"
  local corefile
  corefile=".:53 {
    errors
    health {
       lameduck 5s
    }
    ready
    hosts {
       ${peer_ip} es-remote-${peer}.chat.com
       fallthrough
    }
    kubernetes cluster.local in-addr.arpa ip6.arpa {
       pods insecure
       fallthrough in-addr.arpa ip6.arpa
       ttl 30
    }
    prometheus :9153
    forward . /etc/resolv.conf {
       max_concurrent 1000
    }
    cache 30
    loop
    reload
    loadbalance
}"
  kubectl --context "${ctx}" -n kube-system create cm coredns \
    --from-literal=Corefile="${corefile}" --dry-run=client -o yaml | \
    kubectl --context "${ctx}" -n kube-system apply -f -
  kubectl --context "${ctx}" -n kube-system rollout restart deployment/coredns
  kubectl --context "${ctx}" -n kube-system rollout status deployment/coredns --timeout=120s
}
patch_coredns site1 site2 "${IP_site2}"
patch_coredns site2 site1 "${IP_site1}"

# ─────────────────────────────────────────────────────────────────────────────
# 4. Helm install the chart on each cluster
# ─────────────────────────────────────────────────────────────────────────────
for site in "${SITES[@]}"; do
  ctx="$(ctx_name "${site}")"
  log "[${site}] helm install es-chat-${site}"
  helm --kube-context "${ctx}" upgrade --install "es-chat-${site}" \
    "${ROOT}/charts/elasticsearch" \
    -n "${APP_NS}" --force-conflicts \
    -f "${KIND_MULTI}/values/${site}-multi.yaml"
done

# ─────────────────────────────────────────────────────────────────────────────
# 5. Wait for both clusters green
# ─────────────────────────────────────────────────────────────────────────────
for site in "${SITES[@]}"; do
  ctx="$(ctx_name "${site}")"
  log "[${site}] waiting for es-chat-${site} → green"
  kubectl --context "${ctx}" -n "${APP_NS}" \
    wait --for=jsonpath='{.status.health}'=green \
    "elasticsearch/es-chat-${site}" --timeout=600s
done

# ─────────────────────────────────────────────────────────────────────────────
# 6. CCS is wired automatically by the chart's post-install Job
#    (templates/job-register-remotes.yaml). Each site's values file declares
#    ccs.peers + ccs.mode=public + registrationJob.publicPort=30443, so the
#    Job runs as part of `helm install` and PUTs cluster.remote.* settings
#    pointing at the peer's es-remote-<peer>.chat.com:30443 NodePort endpoint.
#
#    The Job's PUT succeeds even if the peer isn't reachable yet (just persists
#    config); the actual transport connection establishes once both clusters
#    have CoreDNS hosts entries set up + are healthy.
#
#    For ad-hoc surgery, register-remotes.sh stays in kind/ — same logic.
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# 7. Settle + verify CCS. The chart installs site1 before site2, so site1's
#    first connection attempt fails (peer not up yet) and ES backs off. Now that
#    BOTH clusters are healthy, bounce each remote once to force an immediate
#    fresh connect, then assert connected:true. In real prod (peers already
#    running, real DNS) this settle step is unnecessary — ES connects on its own.
# ─────────────────────────────────────────────────────────────────────────────
settle_remote() {
  local site="$1" peer="$2"
  local ctx="$(ctx_name "${site}")"
  local es="https://es-chat-${site}-es-http.${APP_NS}.svc.cluster.local:9200"
  local rk="es-chat-${peer}"
  local addr="es-remote-${peer}.chat.com:30443"
  kubectl --context "${ctx}" -n "${APP_NS}" exec deploy/chat-ingressgateway -- \
    curl -k -sS -u "elastic:${SHARED_ELASTIC_PW}" -X PUT "${es}/_cluster/settings" \
    -H 'content-type: application/json' \
    -d "{\"persistent\":{\"cluster.remote.${rk}.mode\":null,\"cluster.remote.${rk}.proxy_address\":null,\"cluster.remote.${rk}.server_name\":null}}" >/dev/null
  sleep 2
  kubectl --context "${ctx}" -n "${APP_NS}" exec deploy/chat-ingressgateway -- \
    curl -k -sS -u "elastic:${SHARED_ELASTIC_PW}" -X PUT "${es}/_cluster/settings" \
    -H 'content-type: application/json' \
    -d "{\"persistent\":{\"cluster.remote.${rk}.mode\":\"proxy\",\"cluster.remote.${rk}.proxy_address\":\"${addr}\",\"cluster.remote.${rk}.server_name\":\"es-remote-${peer}.chat.com\",\"cluster.remote.${rk}.skip_unavailable\":true}}" >/dev/null
}
log "Settling CCS connections (bounce each remote now that both clusters are up)"
settle_remote site1 site2
settle_remote site2 site1

assert_connected() {
  local site="$1" peer="$2"
  local ctx="$(ctx_name "${site}")"
  local es="https://es-chat-${site}-es-http.${APP_NS}.svc.cluster.local:9200"
  local i out
  for i in $(seq 1 24); do
    out=$(kubectl --context "${ctx}" -n "${APP_NS}" exec deploy/chat-ingressgateway -- \
      curl -k -sS -u "elastic:${SHARED_ELASTIC_PW}" "${es}/_resolve/cluster/es-chat-${peer}:*" 2>/dev/null)
    case "${out}" in *'"connected":true'*) sub "[${site}] → ${peer}: connected:true"; return 0;; esac
    sleep 5
  done
  echo "ERROR: [${site}] → ${peer} did not reach connected:true" >&2
  echo "  last: ${out}" >&2
  return 1
}
log "Verifying CCS reaches connected:true both directions"
ccs_ok=0
assert_connected site1 site2 || ccs_ok=1
assert_connected site2 site1 || ccs_ok=1

# ─────────────────────────────────────────────────────────────────────────────
# Done
# ─────────────────────────────────────────────────────────────────────────────
cat <<EOF

─────────────────────────────────────────────────────────────────────
✓ multi-kind setup complete

Verify cross-K8s CCS:

  kubectl --context kind-chat-eck-site1 -n chat \\
    exec deploy/chat-ingressgateway -- \\
    curl -k -sS -u elastic:${SHARED_ELASTIC_PW} \\
    https://es-chat-site1-es-http.chat.svc.cluster.local:9200/_remote/info

  kubectl --context kind-chat-eck-site2 -n chat \\
    exec deploy/chat-ingressgateway -- \\
    curl -k -sS -u elastic:${SHARED_ELASTIC_PW} \\
    https://es-chat-site2-es-http.chat.svc.cluster.local:9200/_remote/info

Both should report connected: true for the peer.

To tear down both clusters:
  ${KIND_MULTI}/teardown-multi.sh
─────────────────────────────────────────────────────────────────────
EOF

if [ "${ccs_ok}" -ne 0 ]; then
  echo "✗ CCS verification FAILED — see errors above" >&2
  exit 1
fi
echo "✓ CCS verified: connected:true both directions"
