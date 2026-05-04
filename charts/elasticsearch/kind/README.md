# Local kind setup — ECK + Istio + Vault + 2-cluster CCS (Basic tier)

A self-contained MacBook-friendly bring-up of the full architecture covered by
[`docs/superpowers/specs/2026-05-04-elasticsearch-ccs-mesh-design.md`][design],
restricted to **Phase 1**: two ECK Elasticsearch clusters in the **same
namespace, same K8s cluster**, federated via **TLS-cert-based CCS** (the only
CCS path that works on Basic tier — see the design doc Revision history and
§10 for why API-key CCS isn't an option).

The two clusters share a single transport CA (loaded from Vault) so node
transport certs across both chain to the same root and mutually trust. They
also share the `elastic` superuser password so cert-based CCS auth-forwarding
works. After both clusters are green, `register-remotes.sh` runs
`PUT /_cluster/settings` on each to register the peer via the in-cluster
transport Service on port 9300. No API keys are minted (that needs a paid
license); no port 9443 is involved.

[design]: ../../../docs/superpowers/specs/2026-05-04-elasticsearch-ccs-mesh-design.md

## What gets installed

| Component | Version | Where |
|---|---|---|
| kind cluster (single node) | latest | host (Docker Desktop) |
| Istio (istiod) | 1.24.2 | `istio-system` ns |
| Per-namespace Istio ingressgateway (`istio: chat-ingressgateway`) | 1.24.2 | `chat` ns |
| ECK operator | **2.16.1** (2.x line, supports ES up to 8.x) | `elastic-system` ns |
| HashiCorp Vault (dev mode, root token `root`) | latest chart | `vault` ns |
| Vault Secrets Operator | latest chart | `vault-secrets-operator-system` ns |
| `es-chat-site1` (1 master + 1 data + 1 coord, ES 8.19.8) | chart | `chat` ns |
| `es-chat-site2` (mirror) | chart | `chat` ns |
| Kibana for each site | chart | `chat` ns |

Resources are minimised — each ES pod requests 100m CPU / 1Gi memory,
JVM heap 512m, 1–2Gi `standard`-class PVC. Total footprint sits comfortably
under 8Gi RAM with Docker Desktop set to ~6Gi.

## Prerequisites

```bash
brew install kind kubectl helm istioctl
```

Docker Desktop running, with at least 6Gi RAM allocated (Settings → Resources).

## Bring everything up

```bash
./charts/elasticsearch/kind/setup.sh
```

The script is idempotent — re-running picks up where it left off. Watch ES
roll out:

```bash
kubectl -n chat get elasticsearch,kibana,pods -w
```

## Verify Phase 1 CCS works

Add hostnames to `/etc/hosts` (kind maps host:80/443 → the per-namespace
ingressgateway):

```bash
echo "127.0.0.1 es-site1.chat.com kibana-site1.chat.com es-site2.chat.com kibana-site2.chat.com" \
  | sudo tee -a /etc/hosts
```

Hit ES through the chart's `Gateway` + `VirtualService` (TLS PASSTHROUGH,
SNI-routed to `<es>-es-http:9200`):

```bash
curl -k -u elastic:chat-elastic-pw https://es-site1.chat.com/_remote/info | jq .
# Expected:
# {
#   "es-chat-site2": {
#     "connected": true,
#     "mode": "proxy",
#     "proxy_address": "es-chat-site2-es-remote-cluster-service.chat.svc:9443",
#     ...
#   }
# }

curl -k -u elastic:chat-elastic-pw https://es-site2.chat.com/_remote/info | jq .
# Symmetric — site2 sees site1 connected.
```

Cross-cluster search via the same ingress URL:

```bash
curl -k -u elastic:chat-elastic-pw -XPOST \
  "https://es-site1.chat.com/messages-test/_doc?refresh" \
  -H 'content-type: application/json' \
  -d '{"text":"hello from site1"}'

curl -k -u elastic:chat-elastic-pw -XPOST \
  "https://es-site2.chat.com/messages-test/_doc?refresh" \
  -H 'content-type: application/json' \
  -d '{"text":"hello from site2"}'

curl -k -u elastic:chat-elastic-pw \
  "https://es-site1.chat.com/messages-test,es-chat-site2:messages-test/_search?pretty"
```

The `elastic` passwords (`chat-elastic-pw` / `chat-elastic-pw`) come from
the seeded Vault paths and are pulled into the `<es>-es-elastic-user`
Secrets by VSO, which ECK then consumes — same Vault flow as Phase 2 prod.

## Kibana

Browse to either URL (accept self-signed cert):

- https://kibana-site1.chat.com → login elastic / chat-elastic-pw
- https://kibana-site2.chat.com → login elastic / chat-elastic-pw

In **Dev Tools** run:

```
GET _remote/info
GET messages-*,es-chat-site2:messages-*/_search
```

## Tear down

```bash
./charts/elasticsearch/kind/teardown.sh
```

Drops the entire kind cluster (everything inside it goes with it).

## How this maps to the prod design

| Concern | Phase 1 / kind (this) | Phase 2 / prod |
|---|---|---|
| CCS model | Cert-based (Basic), via transport port 9300 | Same — cert-based, Basic |
| Trust | Shared transport CA from Vault, both clusters reference it via `spec.transport.tls.certificate` | Same shared CA, distributed to all 12 sites via Vault → ESO |
| Auth | Shared `elastic` password from Vault path `elasticsearch/elastic-user`; CCS forwards calling user's credentials | Same |
| Remote registration | `PUT /_cluster/settings` with `proxy_address: <peer>-es-transport.chat.svc.cluster.local:9300` | `PUT /_cluster/settings` with `proxy_address: es-remote-<peer>.chat.com:443` (Istio passthrough → 9300) |
| Istio Gateway | Only `es-<site>` / `kibana-<site>` (HTTP/UI access). `ccs.publicEndpoint.enabled=false` — no public transport endpoint needed | All three per site (`ccs.publicEndpoint.enabled=true`); `es-remote-<site>` Gateway routes 443 → transport 9300 |
| AuthorizationPolicy | Workload-selector-scoped to ES + Kibana pods only — does NOT cover the gateway pod (port 443) | Same scoping |
| DestinationRule | `chat-es-passthrough` disables sidecar mTLS originate to `*.chat.svc.cluster.local` so gateway PASSTHROUGH lands on ES's own TLS | Same |
| Vault secrets | `elastic-user` (shared), `transport-ca` (shared), `<site>/minio` (dummy — chart references it but kind has no MinIO) | `elastic-user`, `transport-ca`, `<site>/minio` (real) |
| Sidecar exclusions | 9200, 9300 excluded inbound; 9300 outbound via `cluster.istioConfig` annotations | Same |
| Storage | `standard` (kind default), 2Gi | Region-specific SSD class, 50–500Gi |
| Node count | 1 all-roles node per cluster | 3 master + 3 data + 3 coord per cluster across 3 zones |

## Files

Every component is installed from a vendored chart `.tgz` checked into
`charts/`. No `helm repo add`, no runtime chart pulls. Chart values are
stored as YAML files in `manifests/`. No `--set` flags inline.

```
kind/
├── README.md                              # this file
├── kind-config.yaml                       # kind cluster (port mappings 80/443)
├── setup.sh                               # one-shot bring-up (helm install <local.tgz>)
├── teardown.sh                            # `kind delete cluster`
├── charts/                                # ↓ vendored upstream chart bundles
│   ├── base-1.24.2.tgz                    # istio/base
│   ├── istiod-1.24.2.tgz                  # istio/istiod
│   ├── gateway-1.24.2.tgz                 # istio/gateway
│   ├── eck-operator-2.16.1.tgz            # elastic/eck-operator (2.x → ES 8.x)
│   ├── vault-0.32.0.tgz                   # hashicorp/vault
│   └── vault-secrets-operator-0.10.0.tgz  # hashicorp/vault-secrets-operator
├── manifests/
│   ├── namespace.yaml                     # chat ns w/ istio-injection=enabled
│   ├── istio-base-values.yaml             # values: istio/base
│   ├── istiod-values.yaml                 # values: istio/istiod
│   ├── istio-gateway-values.yaml          # values: istio/gateway (chat ns)
│   ├── eck-operator-values.yaml           # values: elastic/eck-operator
│   ├── vault-values.yaml                  # values: hashicorp/vault (dev mode)
│   ├── vault-secrets-operator-values.yaml # values: hashicorp/vault-secrets-operator
│   └── vault-auth.yaml                    # VaultConnection + VaultAuth (k8s auth)
└── values/
    ├── site1-kind.yaml                    # values: ./charts/elasticsearch site1
    └── site2-kind.yaml                    # values: ./charts/elasticsearch site2
```

To refresh a vendored chart later:
```bash
helm repo add istio https://istio-release.storage.googleapis.com/charts
helm pull istio/gateway --version <new-version> -d charts/elasticsearch/kind/charts/
```
