# Setting up CCS between two same-namespace ES clusters with the existing Helm chart

**Date:** 2026-05-04
**Audience:** Anyone bringing up two ECK Elasticsearch clusters in the same
namespace with Cross-Cluster Search wired between them, using the chart at
`charts/elasticsearch/`.
**License:** Basic only — uses TLS-cert-based CCS over transport port 9300.
See `2026-05-04-elasticsearch-ccs-mesh-design.md` for the why.

---

## TL;DR — is the whole setup Helm-automated?

**Mostly, yes — with one explicit non-Helm step at the end.**

What `helm install` covers per cluster (i.e. what comes out of
`charts/elasticsearch/templates/`):

| Resource | Template | Notes |
|---|---|---|
| `Elasticsearch` CR | `es-cluster.yaml` | 3 master + 1 data + 3 coord nodes (default), shared transport CA via `spec.transport.tls.certificate` |
| `Kibana` CR | `kibana.yaml` | Single instance, references the ES cluster |
| Istio `Gateway` (ES HTTP, Kibana, optional ES-remote) | `gateway.yaml` | TLS PASSTHROUGH on port 443, hostnames `es-<site>.<domain>`, `kibana-<site>.<domain>`, `es-remote-<site>.<domain>` |
| Istio `VirtualService` (ES HTTP, Kibana, optional ES-remote) | `virtualservice.yaml` | SNI-routes 443 to ES (9200) / Kibana (5601) / transport (9300) |
| `AuthorizationPolicy` (ES + Kibana) | `authorization-policy.yaml` | Workload-selector-scoped, allows source CIDRs to reach 9200/9300/5601 — does NOT cover the gateway pod |
| `DestinationRule` (`chat-es-passthrough`) | `destinationrule.yaml` | Disables sidecar mTLS originate to `*.<ns>.svc` so gateway PASSTHROUGH lands on ES's own TLS |
| `VaultStaticSecret` (`<es>-es-elastic-user`) | `vault-secret-elastic-user.yaml` | Pulls the shared `elastic` password from Vault |
| `VaultStaticSecret` (`chat-transport-ca`) | `vault-secret-transport-ca.yaml` | Pulls the shared transport CA cert+key from Vault (rendered only by the release with `manageCASecret: true`) |
| `VaultStaticSecret` (`<es>-es-minio`) | `vault-secret-es-minio.yaml` | MinIO snapshot creds (chart-required even when MinIO is unused — load dummy creds) |

What `helm install` does NOT do:

| Step | Why not |
|---|---|
| Pre-seed Vault with `elastic-user`, `transport-ca`, `<site>/minio` paths | Vault is a separate component; seeding is bootstrap-time work outside the chart |
| `PUT /_cluster/settings` to register each cluster as a remote of the other | Has to run AFTER both clusters are green and the shared CA has rolled — Helm has no equivalent of "wait for both releases to be healthy". This is the `register-remotes.sh` step. |

You can wrap the registration in a Helm post-install hook Job per release if
you want pure `helm install` — see §6 below — but the script-based path is
what the kind setup uses today and is what's verified working end-to-end on
Basic tier.

---

## Prerequisites

A running Kubernetes cluster (kind for local, anything for prod) with:

- ECK operator (any 2.x, including older versions — chart only uses
  `spec.transport.tls.certificate` which has been there since ECK 1.x; we
  do NOT use `spec.remoteClusters[]`, which is Enterprise-gated)
- Istio (any recent version) with a `chat-ingressgateway` Gateway pod in
  the target namespace using the label `istio: chat-ingressgateway`
- HashiCorp Vault + Vault Secrets Operator, with a `VaultAuth` resource
  named `chat-vault-auth` in the target namespace
- Namespace `chat` with `istio-injection=enabled`

The kind setup at `charts/elasticsearch/kind/setup.sh` brings all of these
up for you. For prod, your platform team owns these.

---

## Step 1 — Pre-seed Vault

Three Vault paths, populated once per environment:

```bash
# 1. Shared `elastic` superuser password — same value for every cluster, so
#    cert-based CCS auth-forwarding works.
vault kv put secret/elasticsearch/elastic-user elastic="<your-password>"

# 2. Shared transport CA cert+key — every cluster signs node transport certs
#    from this CA, giving them automatic mutual trust.
openssl req -x509 -nodes -newkey rsa:2048 -days 3650 \
  -subj "/CN=chat-transport-ca" \
  -keyout ca.key -out ca.crt
vault kv put secret/elasticsearch/transport-ca \
  "tls.crt=$(cat ca.crt)" "tls.key=$(cat ca.key)"

# 3. Per-cluster MinIO snapshot creds (use real creds in prod, dummies in dev).
vault kv put secret/elasticsearch/site1/minio \
  MINIO_BUCKET_ACCESS_KEY=<key> MINIO_BUCKET_SECRET_KEY=<secret>
vault kv put secret/elasticsearch/site2/minio \
  MINIO_BUCKET_ACCESS_KEY=<key> MINIO_BUCKET_SECRET_KEY=<secret>
```

Vault must have:
- `kubernetes` auth enabled with a role `chat-app` bound to ServiceAccount
  `default` in the `chat` namespace
- A policy granting read on `secret/data/elasticsearch/*` and
  `secret/metadata/elasticsearch/*`

(In `kind/setup.sh` this is all done inline against dev-mode Vault.)

---

## Step 2 — Helm install both clusters

The chart ships per-site values files at `charts/elasticsearch/values/`.
For local kind testing use the simpler `kind/values/`.

```bash
# Install site1 — owns the shared CA Secret
helm upgrade --install es-chat-site1 ./charts/elasticsearch \
  -n chat \
  -f charts/elasticsearch/kind/values/site1-kind.yaml \
  --force-conflicts

# Install site2 — references the same CA, doesn't manage it
helm upgrade --install es-chat-site2 ./charts/elasticsearch \
  -n chat \
  -f charts/elasticsearch/kind/values/site2-kind.yaml \
  --force-conflicts
```

What's special in the values files:

```yaml
# site1-kind.yaml
ccs:
  enabled: true
  transport:
    enabled: true
    caSecretName: chat-transport-ca
    manageCASecret: true       # ← site1 ONLY: this release renders the CA VaultStaticSecret + DestinationRule

# site2-kind.yaml
ccs:
  transport:
    manageCASecret: false      # ← every other site
```

`manageCASecret` gates two namespace-singleton resources:

1. The `VaultStaticSecret` for `chat-transport-ca` — every cluster's
   `Elasticsearch` CR references the resulting K8s Secret, but only one
   release should render the VaultStaticSecret (otherwise Helm refuses the
   second install with "exists and cannot be imported").
2. The `DestinationRule` `chat-es-passthrough` — same reason, namespace-wide.

`--force-conflicts` is needed because ECK takes server-side-apply ownership
of `spec.nodeSets` after the first reconciliation. Without it, `helm upgrade`
fails with "conflict with elastic-operator using elasticsearch.k8s.elastic.co".

---

## Step 3 — Wait for both clusters green

```bash
kubectl -n chat wait --for=jsonpath='{.status.health}'=green elasticsearch/es-chat-site1 --timeout=600s
kubectl -n chat wait --for=jsonpath='{.status.health}'=green elasticsearch/es-chat-site2 --timeout=600s
```

ECK handles transport cert rotation when the shared CA Secret first appears
— if you install the Vault-CA path BEFORE deploying the ES clusters,
they come up directly with shared-CA-signed transport certs. If you flip
`spec.transport.tls.certificate` on a running cluster, ECK will roll all
nodes once.

---

## Step 4 — Register the peer (the one non-Helm step)

```bash
./charts/elasticsearch/kind/register-remotes.sh
```

This does exactly two `PUT /_cluster/settings` calls:

```
# On site1:
{
  "persistent": {
    "cluster.remote.es-chat-site2.mode": "proxy",
    "cluster.remote.es-chat-site2.proxy_address": "es-chat-site2-es-transport.chat.svc.cluster.local:9300",
    "cluster.remote.es-chat-site2.skip_unavailable": true
  }
}
# And the inverse on site2.
```

Both calls are idempotent — re-running the script is a no-op.

In Phase 2 (cross-K8s) the only thing that changes is `proxy_address`, which
becomes `es-remote-<peer>.chat.com:443` (Istio passthrough → transport 9300).

---

## Step 5 — Verify

In Kibana Dev Tools (`https://kibana-site1.chat.com` after adding to
`/etc/hosts`), or via `curl`:

```bash
curl -k -u elastic:<pw> https://es-site1.chat.com/_remote/info | jq .
# → { "es-chat-site2": { "connected": true, "mode": "proxy", ... } }

curl -k -u elastic:<pw> 'https://es-site1.chat.com/messages-*,es-chat-site2:messages-*/_search'
# → hits from both clusters, prefixed with `es-chat-site2:` for remote ones
```

The `_clusters` block in the search response shows per-cluster status:

```json
"_clusters": {
  "total": 2, "successful": 2, "skipped": 0,
  "details": {
    "(local)":         { "status": "successful", ... },
    "es-chat-site2":   { "status": "successful", ... }
  }
}
```

That's the wire-level success criterion — search-service code already issues
queries in this `messages-*,*:messages-*` shape, so once `_remote/info` is
green, search-service-driven CCS works without any service-side change.

---

## What's automated vs not — exact list

### Helm renders, ECK / Istio / VSO reconcile (zero manual work)
- ES + Kibana CRs (with shared transport CA so both clusters mutually trust)
- Per-site Istio Gateway, VirtualService, AuthorizationPolicy, DestinationRule
- Vault → K8s Secret syncing for elastic password, transport CA, minio creds
- ECK rolling restarts on Secret change (transport cert rotation)

### One-time bootstrap (outside Helm)
1. Seed Vault paths (Step 1)
2. After both clusters green, run `register-remotes.sh` (Step 4)

### Optional — make Step 4 part of `helm install`

Convert `register-remotes.sh` to a Kubernetes Job rendered by the chart with
`helm.sh/hook: post-install,post-upgrade`. The Job:

1. Waits for both ES clusters' `<es>-es-http` Service to return 200
   on `/_cluster/health?wait_for_status=green`.
2. Reads the shared `elastic` password from
   `<es>-es-elastic-user` Secret.
3. Issues the two `PUT /_cluster/settings` calls.
4. Verifies via `GET /_remote/info`.

Caveat: a Job rendered by site2's Helm release can't depend on site1 being
green — Helm has no cross-release dependencies. Pragmatic options:

- **Single-release deploy of both clusters** — wrap site1 and site2 into a
  parent chart that depends on the elasticsearch chart twice. Then a single
  post-install hook can run after both clusters' Pods are Ready. This is
  the cleanest fully-Helm path for Phase 1 same-namespace.
- **Two-round helm install** (the prior design's approach) — install both
  clusters with `jobs.registerRemotes.enabled=false`, then run a second
  pass with it enabled. Works without a parent chart.

The kind setup keeps the script-based path because it's transparent and
short. Either Job-based path is fine to add later — the chart is already
structured for it (the registration logic is small and idempotent).

---

## Common failures and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Helm install fails with `"exists and cannot be imported"` on `chat-transport-ca` or `chat-es-passthrough` | Both releases trying to manage the same namespace-singleton resource | Set `ccs.transport.manageCASecret: true` on exactly one release, `false` on all others |
| `helm upgrade` fails with `conflict with elastic-operator using elasticsearch.k8s.elastic.co: .spec.nodeSets` | ECK took server-side-apply ownership of nodeSets | Add `--force-conflicts` to the `helm upgrade` command |
| `_remote/info` returns `connected: false` | Both clusters not signed by the same CA | Verify both reference the same `chat-transport-ca` Secret; ECK rolls on Secret change |
| `_remote/info` returns `connected: true` but `_search` errors with `unable to authenticate user` | Calling user has different password on remote | Both clusters must share the same `elastic` password — verify the Vault `elasticsearch/elastic-user` path is consumed by both sites |
| Curl through `https://es-<site>.chat.com` returns RST after TLS handshake | `DestinationRule` missing — gateway is mTLS-originating to ES whose 9200 is sidecar-excluded | Confirm `ccs.transport.manageCASecret: true` is set on at least one release; the chart's DestinationRule template is gated on it |
| Curl through `https://es-<site>.chat.com` returns immediate connection close | `AuthorizationPolicy` blocking the gateway pod (port 443) | Verify the chart's policy has `selector.matchLabels: { common.k8s.elastic.co/type: elasticsearch }` (it does by default; only an issue if you manually edited the policy) |

---

## File reference

```
charts/elasticsearch/
├── values.yaml                          # Defaults — sized for kind (3 master + 1 data + 3 coord, 256Mi/512Mi per pod)
├── values/
│   ├── site1.yaml                       # Phase 2 prod overrides for site1 (manageCASecret=true)
│   └── site2.yaml                       # Phase 2 prod overrides for site2 (manageCASecret=false)
├── templates/
│   ├── es-cluster.yaml                  # Elasticsearch CR
│   ├── kibana.yaml                      # Kibana CR
│   ├── gateway.yaml                     # Istio Gateways
│   ├── virtualservice.yaml              # Istio VirtualServices (443 → 9200/5601/9300)
│   ├── authorization-policy.yaml        # Workload-selector-scoped to ES + Kibana
│   ├── destinationrule.yaml             # tls.mode: DISABLE for backend hop
│   ├── vault-secret-elastic-user.yaml   # Shared elastic password from Vault
│   ├── vault-secret-transport-ca.yaml   # Shared transport CA from Vault (gated on manageCASecret)
│   └── vault-secret-es-minio.yaml       # MinIO snapshot creds
└── kind/                                # Local kind setup that exercises this chart
    ├── README.md                        # End-to-end bring-up instructions
    ├── setup.sh                         # Vendored-chart Helm installs of Istio/ECK/Vault/VSO + this chart
    ├── register-remotes.sh              # Step 4 — the one non-Helm bit
    ├── kind-config.yaml
    ├── teardown.sh
    ├── charts/                          # Vendored upstream chart .tgz files
    ├── manifests/                       # Helm values for each upstream chart + namespace + VaultAuth
    └── values/
        ├── site1-kind.yaml              # Phase 1 same-namespace overrides for site1
        └── site2-kind.yaml              # Phase 1 same-namespace overrides for site2
```

---

## References

- `2026-05-04-elasticsearch-ccs-mesh-design.md` — full architecture and
  licensing rationale (why cert-based, not API-key)
- https://www.elastic.co/guide/en/elasticsearch/reference/current/remote-clusters-cert.html
  — TLS-cert-based CCS reference
- `charts/elasticsearch/kind/README.md` — running the local kind end-to-end
