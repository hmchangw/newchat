# Multi-kind cross-K8s CCS harness

Two-kind-cluster verification of the chart's `MODE=public` CCS path —
mirrors the production topology where each Kubernetes cluster has its own
Vault and CCS traffic crosses the public Istio passthrough endpoint
(`es-remote-<site>.<domain>:443`).

This is the harness that verifies **cross-K8s connectivity actually works**.
The single-cluster `kind/` harness verifies the same chart in same-namespace
mode (`MODE=internal`). Pick the one matching your test goal.

## What it stands up

```
chat-eck-site1 (kind cluster)              chat-eck-site2 (kind cluster)
─────────────────────────────────          ─────────────────────────────────
Istio (istio-system)                       Istio (istio-system)
ECK operator (elastic-system)              ECK operator (elastic-system)
Vault dev (vault)                          Vault dev (vault)
   └── secret/elasticsearch/transport-ca       └── secret/elasticsearch/transport-ca
       (SAME content as cluster site2's)           (SAME content as cluster site1's)
   └── secret/elasticsearch/elastic-user        └── secret/elasticsearch/elastic-user
       (SAME password)                              (SAME password)
VSO + chat-vault-auth (chat ns)            VSO + chat-vault-auth (chat ns)
es-chat-site1 (chat ns, 1 all-roles pod)   es-chat-site2 (chat ns, 1 all-roles pod)
chat-ingressgateway (chat ns, NodePort 30443)
                                           chat-ingressgateway (chat ns, NodePort 30443)
CoreDNS:                                   CoreDNS:
  es-remote-site2.chat.com → site2 IP        es-remote-site1.chat.com → site1 IP
```

Both kind containers are on the Docker `kind` network — site1's pod can
reach site2's container IP directly. The CoreDNS hosts entries in each
cluster make the public `es-remote-<peer>.chat.com` hostname resolvable
**from within** each cluster, so ES coordinator queries against
`cluster.remote.es-chat-site2.proxy_address` actually land on site2's
gateway.

The cross-cluster query path:

```
es-chat-site1 ES coordinator
  ↓ CCS query "es-chat-site2:messages-*"
  ↓ resolve es-remote-site2.chat.com (CoreDNS hosts → 172.x.x.x site2 container IP)
  ↓ TCP connect to <site2 IP>:30443
  ↓ ── Docker `kind` network ─────────────────────
  ↓ arrives at site2's chat-ingressgateway pod (NodePort 30443 → port 443)
  ↓ Istio Gateway matches SNI=es-remote-site2.chat.com (TLS PASSTHROUGH)
  ↓ VirtualService routes to es-chat-site2-es-transport:9300
  ↓ DestinationRule disables sidecar mTLS originate
  ↓ traffic.sidecar.istio.io/excludeInboundPorts:9300 bypasses sidecar
  ↓ arrives at es-chat-site2 ES pod's transport listener (port 9300)
  ↓ TLS handshake — site2's transport cert signed by shared CA
  ↓ site1 trusts because it has the SAME CA in its Vault → its truststore
  ↓ user credentials forwarded; same elastic password in both clusters → auth OK
  ↓ query executes; results stream back
```

## Prerequisites

- Docker Desktop with ≥6Gi memory (two kind clusters + their workloads)
- `kind`, `kubectl`, `helm` (Helm 3.13+ for `--skip-schema-validation`),
  `openssl` on PATH

## Run

```bash
./charts/elasticsearch/kind-multi/setup-multi.sh
```

Idempotent — re-running picks up where it left off (skips kind cluster
creation if both already exist, etc.).

## Verify

```bash
# Cross-K8s _remote/info from site1
kubectl --context kind-chat-eck-site1 -n chat exec deploy/chat-ingressgateway -- \
  curl -k -sS -u elastic:chat-elastic-pw \
  https://es-chat-site1-es-http.chat.svc.cluster.local:9200/_remote/info | jq .
# → {"es-chat-site2": {"connected": true, "mode":"proxy",
#                      "proxy_address":"es-remote-site2.chat.com:30443",
#                      "server_name":"es-remote-site2.chat.com", ...}}

# Same from site2 (bidirectional)
kubectl --context kind-chat-eck-site2 -n chat exec deploy/chat-ingressgateway -- \
  curl -k -sS -u elastic:chat-elastic-pw \
  https://es-chat-site2-es-http.chat.svc.cluster.local:9200/_remote/info | jq .

# End-to-end CCS query — index a doc on each side, search across
kubectl --context kind-chat-eck-site1 -n chat exec deploy/chat-ingressgateway -- \
  curl -k -sS -u elastic:chat-elastic-pw -XPOST \
  https://es-chat-site1-es-http.chat.svc.cluster.local:9200/messages-test/_doc?refresh \
  -H 'content-type: application/json' \
  -d '{"text":"hello site1"}'

kubectl --context kind-chat-eck-site2 -n chat exec deploy/chat-ingressgateway -- \
  curl -k -sS -u elastic:chat-elastic-pw -XPOST \
  https://es-chat-site2-es-http.chat.svc.cluster.local:9200/messages-test/_doc?refresh \
  -H 'content-type: application/json' \
  -d '{"text":"hello site2"}'

kubectl --context kind-chat-eck-site1 -n chat exec deploy/chat-ingressgateway -- \
  curl -k -sS -u elastic:chat-elastic-pw \
  'https://es-chat-site1-es-http.chat.svc.cluster.local:9200/messages-test,es-chat-site2:messages-test/_search'
# → 2 hits, one local, one prefixed `es-chat-site2:`
```

## Tear down

```bash
./charts/elasticsearch/kind-multi/teardown-multi.sh
```

## How this maps to YOUR prod topology

| Element | This harness (kind-multi) | YOUR prod (12 clusters) |
|---|---|---|
| K8s cluster count | 2 | 12 |
| Vault per cluster | yes (dev mode) | yes (your existing per-cluster Vault) |
| Shared CA distribution | setup-multi.sh writes the same CA into both Vaults | YOU put the same CA into all 12 Vaults manually (same `vault kv put` content) |
| Shared elastic password | setup-multi.sh writes same value into both Vaults | YOU put same value into all 12 Vaults manually |
| `es-remote-<site>.chat.com` resolution | CoreDNS hosts plugin (per-cluster, points at peer container IP) | Public DNS A records (per cluster, points at that cluster's Istio LB IP) |
| `proxy_address` port | `:30443` (kind NodePort) | `:443` (real LB on standard port) |
| `MODE=public` registration | setup-multi.sh runs `register-remotes.sh MODE=public PEERS=<peer> PUBLIC_PORT=30443` per cluster | YOU run `register-remotes.sh MODE=public PEERS=<11-peers> PUBLIC_PORT=443` per cluster |
| Trust + auth substrate | identical | identical |
| Wire format on cross-cluster traffic | identical (Istio passthrough, SNI route, transport TLS via shared CA) | identical |

The substrate (cert-based, shared CA, forward-the-user auth, Istio
passthrough) is byte-for-byte identical. Only the count of clusters,
the choice of port, and where DNS lives change between this harness and
real prod.

## Files

```
kind-multi/
├── README.md                  # this file
├── kind-config.yaml           # bare kind config (no host port mappings)
├── setup-multi.sh             # bring-up + cross-cluster wiring + registration
├── teardown-multi.sh          # delete both kind clusters
└── values/
    ├── site1-multi.yaml       # site1 chart values (publicEndpoint.enabled)
    └── site2-multi.yaml       # site2 chart values
```

Vendored upstream charts (Istio, ECK, Vault, VSO) are reused from
`../kind/charts/` — no duplication.
