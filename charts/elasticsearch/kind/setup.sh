#!/usr/bin/env bash
# One-shot bring-up for a local kind cluster running:
#   * Istio (default profile, istiod only — no built-in ingressgateway)
#   * A per-namespace ingressgateway in the `chat` namespace
#   * ECK operator 2.16 (supports ES up to 8.x)
#   * HashiCorp Vault (dev mode, root token = "root") + Vault Secrets Operator
#   * Two minimal ECK Elasticsearch clusters (es-chat-site1 / es-chat-site2)
#     in the same namespace, with Phase 1 same-namespace CCS via apiKey: {}
#
# Every component is installed from a vendored chart .tgz checked into ./charts/
# (no `helm repo add`, no internet required for chart pulls). Chart values are
# stored as YAML files in ./manifests/.
#
# Run from the repo root: ./charts/elasticsearch/kind/setup.sh
# Re-runnable: every step is idempotent.
set -euo pipefail

CLUSTER_NAME="chat-eck"
APP_NS="chat"
VAULT_NS="vault"
VSO_NS="vault-secrets-operator-system"

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
KIND_DIR="${ROOT}/charts/elasticsearch/kind"
MANIFESTS="${KIND_DIR}/manifests"
VCHARTS="${KIND_DIR}/charts"

# Pinned chart versions — files vendored under ./charts/.
ISTIO_BASE="${VCHARTS}/base-1.24.2.tgz"
ISTIOD="${VCHARTS}/istiod-1.24.2.tgz"
ISTIO_GATEWAY="${VCHARTS}/gateway-1.24.2.tgz"
ECK_OPERATOR="${VCHARTS}/eck-operator-2.16.1.tgz"
VAULT="${VCHARTS}/vault-0.32.0.tgz"
VSO="${VCHARTS}/vault-secrets-operator-0.10.0.tgz"

log()  { printf "\n\033[1;32m▶ %s\033[0m\n" "$*"; }

# ─────────────────────────────────────────────────────────────────────────────
# 1. kind cluster
# ─────────────────────────────────────────────────────────────────────────────
if kind get clusters | grep -qx "${CLUSTER_NAME}"; then
  log "kind cluster '${CLUSTER_NAME}' already exists, reusing"
else
  log "Creating kind cluster '${CLUSTER_NAME}'"
  kind create cluster --name "${CLUSTER_NAME}" --config "${KIND_DIR}/kind-config.yaml"
fi
kubectl config use-context "kind-${CLUSTER_NAME}"

# ─────────────────────────────────────────────────────────────────────────────
# 2. Istio control plane (base + istiod) from vendored .tgz
# ─────────────────────────────────────────────────────────────────────────────
log "Installing istio-base (chart: ${ISTIO_BASE##*/})"
kubectl create namespace istio-system --dry-run=client -o yaml | kubectl apply -f -
helm upgrade --install istio-base "${ISTIO_BASE}" \
  -n istio-system --wait \
  -f "${MANIFESTS}/istio-base-values.yaml"

log "Installing istiod (chart: ${ISTIOD##*/})"
helm upgrade --install istiod "${ISTIOD}" \
  -n istio-system --wait \
  -f "${MANIFESTS}/istiod-values.yaml"

# ─────────────────────────────────────────────────────────────────────────────
# 3. chat namespace + per-namespace Istio ingressgateway
# ─────────────────────────────────────────────────────────────────────────────
log "Creating chat namespace (with istio-injection=enabled)"
kubectl apply -f "${MANIFESTS}/namespace.yaml"

# istio/gateway has a strict values schema that rejects unknown root keys
# (https://github.com/istio/istio/issues/47892). --skip-schema-validation
# (Helm 3.13+) lets us drive the chart entirely from a values file.
log "Installing chat ingressgateway (chart: ${ISTIO_GATEWAY##*/})"
helm upgrade --install chat-ingressgateway "${ISTIO_GATEWAY}" \
  -n "${APP_NS}" --wait --skip-schema-validation \
  -f "${MANIFESTS}/istio-gateway-values.yaml"

# ─────────────────────────────────────────────────────────────────────────────
# 4. ECK operator 2.x from vendored .tgz
# ─────────────────────────────────────────────────────────────────────────────
log "Installing ECK operator (chart: ${ECK_OPERATOR##*/})"
helm upgrade --install eck-operator "${ECK_OPERATOR}" \
  -n elastic-system --create-namespace --wait \
  -f "${MANIFESTS}/eck-operator-values.yaml"

# ─────────────────────────────────────────────────────────────────────────────
# 5. Vault (dev mode) + Vault Secrets Operator from vendored .tgz
# ─────────────────────────────────────────────────────────────────────────────
log "Installing Vault (chart: ${VAULT##*/}, dev mode)"
helm upgrade --install vault "${VAULT}" \
  -n "${VAULT_NS}" --create-namespace --wait \
  -f "${MANIFESTS}/vault-values.yaml"

log "Waiting for vault-0 to be ready"
kubectl -n "${VAULT_NS}" wait --for=condition=Ready pod/vault-0 --timeout=180s

log "Generating shared transport CA cert+key (locally, only for kind)"
TRANSPORT_CA_DIR=$(mktemp -d)
trap "rm -rf '${TRANSPORT_CA_DIR}'" EXIT
# 100-year validity — effectively indefinite. Cert rotation is intentionally
# out of scope for this design (see docs/superpowers/specs/2026-05-04-
# elasticsearch-ccs-mesh-design.md §5.3).
openssl req -x509 -nodes -newkey rsa:2048 -days 36500 \
  -subj "/CN=chat-transport-ca" \
  -keyout "${TRANSPORT_CA_DIR}/tls.key" \
  -out "${TRANSPORT_CA_DIR}/tls.crt" 2>/dev/null
TRANSPORT_CA_CRT=$(cat "${TRANSPORT_CA_DIR}/tls.crt")
TRANSPORT_CA_KEY=$(cat "${TRANSPORT_CA_DIR}/tls.key")

log "Configuring Vault: kubernetes auth + KV v2 + chat-app role"
VAULT_EXEC="kubectl -n ${VAULT_NS} exec -i vault-0 -- env VAULT_ADDR=http://127.0.0.1:8200 VAULT_TOKEN=root"
$VAULT_EXEC sh -c '
  set -e
  vault auth enable -path=kubernetes kubernetes 2>/dev/null || true
  vault write auth/kubernetes/config \
    kubernetes_host="https://kubernetes.default.svc.cluster.local" \
    disable_iss_validation=true
  vault policy write chat-app - <<EOF
path "secret/data/elasticsearch/*" { capabilities = ["read"] }
path "secret/metadata/elasticsearch/*" { capabilities = ["read"] }
EOF
  vault write auth/kubernetes/role/chat-app \
    bound_service_account_names=default \
    bound_service_account_namespaces=chat \
    policies=chat-app \
    audience=vault \
    ttl=24h
  vault secrets enable -path=secret -version=2 kv 2>/dev/null || true
'

log "Seeding Vault paths: shared elastic password, shared transport CA, dummy MinIO creds"
# Shared elastic password — cert-based CCS forwards the calling user's
# credentials, so site1 and site2 must agree on the elastic password.
$VAULT_EXEC vault kv put secret/elasticsearch/elastic-user elastic="chat-elastic-pw"

# Shared transport CA — both clusters point at the same Secret so ECK signs
# node transport certs from one root and they mutually trust.
$VAULT_EXEC vault kv put secret/elasticsearch/transport-ca \
  "tls.crt=${TRANSPORT_CA_CRT}" "tls.key=${TRANSPORT_CA_KEY}"

# Dummy MinIO creds — the chart references these in spec.secureSettings.
# No MinIO actually runs in kind; ECK loads them into the keystore and ES
# never reads them unless you configure a snapshot repository.
$VAULT_EXEC vault kv put secret/elasticsearch/site1/minio \
  MINIO_BUCKET_ACCESS_KEY=dummy MINIO_BUCKET_SECRET_KEY=dummy
$VAULT_EXEC vault kv put secret/elasticsearch/site2/minio \
  MINIO_BUCKET_ACCESS_KEY=dummy MINIO_BUCKET_SECRET_KEY=dummy

log "Installing Vault Secrets Operator (chart: ${VSO##*/})"
helm upgrade --install vault-secrets-operator "${VSO}" \
  -n "${VSO_NS}" --create-namespace --wait \
  -f "${MANIFESTS}/vault-secrets-operator-values.yaml"

log "Applying VaultConnection + VaultAuth in '${APP_NS}'"
kubectl apply -f "${MANIFESTS}/vault-auth.yaml"

# ─────────────────────────────────────────────────────────────────────────────
# 6. Two ES clusters in the chat namespace via the local elasticsearch chart
# ─────────────────────────────────────────────────────────────────────────────
log "Installing es-chat-site1 (chart: ./charts/elasticsearch)"
helm upgrade --install es-chat-site1 "${ROOT}/charts/elasticsearch" \
  -n "${APP_NS}" --force-conflicts \
  -f "${KIND_DIR}/values/site1-kind.yaml"

log "Installing es-chat-site2 (chart: ./charts/elasticsearch)"
helm upgrade --install es-chat-site2 "${ROOT}/charts/elasticsearch" \
  -n "${APP_NS}" --force-conflicts \
  -f "${KIND_DIR}/values/site2-kind.yaml"

log "Waiting for both Elasticsearch clusters to be green"
for site in es-chat-site1 es-chat-site2; do
  kubectl -n "${APP_NS}" wait --for=jsonpath='{.status.health}'=green elasticsearch/${site} --timeout=600s
done

# ─────────────────────────────────────────────────────────────────────────────
# 7. Wire up CCS the Basic-tier way (mint + register manually).
#    See register-remotes.sh for the full rationale on why we don't use ECK's
#    spec.remoteClusters auto-keying.
# ─────────────────────────────────────────────────────────────────────────────
log "Wiring cert-based CCS (Basic tier) — internal mode (same K8s, same ns)"
MODE=internal LOCAL_SITE=site1 PEERS=site2 "${KIND_DIR}/register-remotes.sh"
MODE=internal LOCAL_SITE=site2 PEERS=site1 "${KIND_DIR}/register-remotes.sh"

cat <<EOF

─────────────────────────────────────────────────────────────
✓ kind setup complete

Add to /etc/hosts (kind exposes the chat-ingressgateway on host 80/443):
  127.0.0.1  es-site1.chat.com kibana-site1.chat.com es-site2.chat.com kibana-site2.chat.com

Watch ES clusters come up:
  kubectl -n ${APP_NS} get elasticsearch,kibana,pods -w

Phase 1 CCS is wired automatically by register-remotes.sh. To verify via the
Istio VirtualService → Gateway → ES (no port-forward needed):

  curl -k -u elastic:chat-elastic-pw https://es-site1.chat.com/_remote/info | jq .
  # Expect: { "es-chat-site2": { "connected": true, "mode": "proxy", ... } }

  curl -k -u elastic:chat-elastic-pw https://es-site2.chat.com/_remote/info | jq .
  # Expect: { "es-chat-site1": { "connected": true, "mode": "proxy", ... } }

Kibana (browser, accept self-signed cert):
  https://kibana-site1.chat.com  → login elastic / chat-elastic-pw
  https://kibana-site2.chat.com  → login elastic / chat-elastic-pw

In Kibana Dev Tools, run:
  GET _remote/info
  GET messages-*,es-chat-site2:messages-*/_search

To tear everything down:
  ${KIND_DIR}/teardown.sh
─────────────────────────────────────────────────────────────
EOF
