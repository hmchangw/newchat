# Elasticsearch Cross-Cluster Search (CCS) Mesh — Design

**Date:** 2026-05-04 (revised)
**Status:** Design — revised after Basic-tier licensing audit
**Scope:** Multi-site Elasticsearch federation via Cross-Cluster Search across 12 production sites, with a same-namespace test setup as a stepping stone.
**License constraint:** Everything in this design runs on Elasticsearch **Basic** tier. No paid features.

---

## Revision history

The first cut of this design (same date) used the **API-key based** remote-cluster
model — `POST /_security/cross_cluster/api_key`, ECK's
`spec.remoteClusters[].apiKey` field, and a dedicated `remote_cluster_server`
listener on port 9443. That was wrong on licensing in two places:

1. Cross-cluster API keys (`/_security/cross_cluster/api_key`) require the
   `advanced-remote-cluster-security` feature, which is **NOT Basic** —
   verified empirically against ES 8.19.8: minting a cross-cluster API key
   returns `security_exception: current license is non-compliant for
   [advanced-remote-cluster-security]`.
2. ECK's `spec.remoteClusters` automation explicitly logs
   `"Remote cluster is an enterprise feature. Enterprise features are disabled"`
   on Basic — verified against ECK 2.16.1.

This revision drops the API-key path entirely and uses **TLS-certificate-based
CCS** (the legacy model), which is fully Basic-tier and compatible with all
ECK 2.x versions (and even older).

---

## 1. Problem statement

The chat platform federates across 12 sites, each running an independent
ECK-managed Elasticsearch 8.19.8 cluster (3 master + 3 data + 3 coordinating,
distributed across 3 datacenters per site). The `search-service` already
issues `messages-*,*:messages-*` queries that depend on remote clusters
being registered and reachable.

Today, only the application-layer CCS query path is built. The cross-cluster
transport — distributing trust, exposing the right ports through Istio,
registering remotes — is not yet wired. This design covers exactly that.

The constraints making this non-trivial:

- Sites run in **separate Kubernetes clusters** (one per region/DC group).
- Cross-site network reachability is **only via the public Istio ingress**
  (no VPC peering, no mesh federation, no Submariner).
- The shared Istio ingressgateway only listens on **ports 80 and 443**.
- The namespace default Gateway resource is owned by the platform team,
  terminates TLS for `*.chat.com` with a shared `ingress-cert`, and cannot
  be modified.
- Default sidecar injection is enabled on the `chat` namespace.
- An `AuthorizationPolicy` restricts traffic to source IPs in `172.x.x.x/8`
  and currently allows only port 9200.
- **Basic tier only** — no API-key CCS, no `remote_cluster_server`, no ECK
  Enterprise features.

The design uses **TLS-certificate-based CCS over the standard transport
listener (port 9300)**, with all 12 clusters sharing a single transport CA so
they mutually trust each other's node certificates. Cross-K8s connectivity
goes through a per-site SNI-passthrough Istio Gateway on port 443 that routes
to the cluster's internal transport Service.

---

## 2. Architecture overview

### 2.1 Two phases

| | Phase 1 (test) | Phase 2 (prod) |
|---|---|---|
| Topology | 2 ES clusters in same namespace, same K8s cluster | 12 ES clusters, separate K8s clusters, separate regions |
| Connectivity | Pod network — direct hit on `<es>-es-transport.<ns>.svc.cluster.local:9300` | Public internet via Istio ingress only |
| Trust | Both ES CRs reference the same shared-transport-CA Secret in `spec.transport.tls.certificate` so ECK signs node certs from it; clusters mutually trust automatically | Same shared CA, distributed via Vault → ESO → K8s Secret in every site's chat namespace |
| Auth | Both clusters share the same `elastic` superuser password (from Vault) — CCS forwards the local user's credentials to the remote, which authenticates against its native realm | Same — shared `elastic` password across all 12 sites in Vault |
| Istio changes | None — traffic stays in pod network | New SNI-passthrough Gateway + VirtualService per site for `es-remote-<site>.chat.com:443 → <es>-es-transport:9300` |
| Cluster registration | `PUT /_cluster/settings` with `cluster.remote.<peer>.proxy_address` pointing at the in-cluster transport Service | Same, but `proxy_address` is the public hostname `es-remote-<peer>.chat.com:443` |
| ECK version requirement | Any 2.x (1.x also works) — no `spec.remoteClusters`, just `spec.transport.tls.certificate` | Same |
| ES license | Basic ✓ | Basic ✓ |

The search-service code is unchanged across both phases — the wire format
(`PUT /_cluster/settings`) is identical to what the API-key model would have
used; only the auth model and the destination port differ.

### 2.2 Per-cluster node layout (unchanged from existing)

| Node set | Count | Roles |
|---|---|---|
| `master-{a,b,c}` | 1 each (3 total) | `[master, remote_cluster_client]` |
| `data-{a,b,c}` | 1 each (3 total) | `[data, remote_cluster_client]` |
| `coords-{a,b,c}` | 1 each (3 total) | `[remote_cluster_client]` (coord-only) |

`remote_cluster_client` is on every node so any node can coordinate a CCS
query. Cert-based CCS uses the standard transport listener on port 9300 —
there is no separate `remote_cluster_server` listener and no port 9443.

### 2.3 Topology: full bidirectional mesh

Every site searches every other site. Each cluster registers all 11 peers as
remotes. No per-link credentials — every cluster trusts every other via the
shared transport CA, and forwards the local user's credentials.

### 2.4 Hostname / DNS layout per site

Three public hostnames per site, all under `*.chat.com`, all pointing at the
same Istio LB IP:

| Hostname | Port | Backend Service | Purpose |
|---|---|---|---|
| `es-<site>.chat.com` | 443 → 9200 | `es-chat-<site>-es-http` | Existing ES HTTP API |
| `kibana-<site>.chat.com` | 443 → 5601 | `kibana-<site>-kb-http` | Existing Kibana UI |
| **`es-remote-<site>.chat.com`** | **443 → 9300** | **`es-chat-<site>-es-transport`** | **NEW — CCS inbound transport from peers** |

DNS additions: one A record per site (`es-remote-<site>.chat.com`).

---

## 3. Phase 1: same-namespace test setup

### 3.1 Goal

Validate the application-layer CCS path (search-service `messages-*,*:messages-*`
query) and Kibana cross-cluster Dev Tools queries with **zero Istio gateway
changes**, **zero certificate distribution work** (single shared-CA Secret in
the namespace), and **zero API-key minting**.

### 3.2 Prerequisites

- Any ECK 2.x operator — the only ECK feature used is `spec.transport.tls.certificate`,
  which has been available since ECK 1.0. ECK 2.13's `spec.remoteClusters`
  field is intentionally NOT used (Enterprise gated).
- Elasticsearch ≥ 7.x (existing 8.19.8 satisfies this trivially).
- Both `Elasticsearch` resources in the **same namespace** (`chat`).
- A Secret containing a shared transport CA cert+key (e.g. `chat-transport-ca`).

### 3.3 Manifest changes vs. baseline single-cluster setup

For each of the two test clusters (`es-chat-site1`, `es-chat-site2`):

1. Reference the shared transport CA so both clusters' node transport certs
   are signed by it:
   ```yaml
   spec:
     transport:
       tls:
         certificate:
           secretName: chat-transport-ca
   ```
2. Make sure both clusters' `elastic` superuser shares the same password —
   in Phase 1, write the same value into both Vault paths
   (`elasticsearch/site1/elastic-user`, `elasticsearch/site2/elastic-user`)
   or use a single Vault path consumed by both clusters' VaultStaticSecret
   resources.

### 3.4 Cluster registration

After both clusters are green, run `PUT /_cluster/settings` on each:

```json
PUT /_cluster/settings
{
  "persistent": {
    "cluster.remote.es-chat-site2.mode": "proxy",
    "cluster.remote.es-chat-site2.proxy_address": "es-chat-site2-es-transport.chat.svc.cluster.local:9300",
    "cluster.remote.es-chat-site2.skip_unavailable": true
  }
}
```

And the inverse on `es-chat-site2`. No keystore entries are needed — auth
forwards the calling user's credentials, which both clusters share.

### 3.5 AuthorizationPolicy

The chart's namespace `AuthorizationPolicy` keeps allowing 9200 (ES HTTP)
and 5601 (Kibana). It must NOT be applied to the per-namespace
ingressgateway pod (which listens on 443) — gate it with a workload selector
or skip it on the gateway. Port 9300 (transport) does not need to be in the
allowed list because transport traffic between ES pods in the same K8s
cluster is intra-mesh and the namespace's PeerAuthentication PERMISSIVE
allows it.

### 3.6 Verification

```
GET _remote/info                                        # site2 connected: true, mode: proxy
GET messages-*/_search                                  # local-only sanity check
GET es-chat-site2:messages-*/_search                    # remote-only
GET messages-*,es-chat-site2:messages-*/_search         # exactly what search-service runs
```

Symmetry check: same queries from `es-chat-site2`'s Kibana.

### 3.7 Common Phase 1 failures

- `_remote/info` shows `connected: false` → the two clusters are using
  different transport CAs. Verify both `spec.transport.tls.certificate.secretName`
  point to the same Secret and that ECK has rolled the pods after the change.
- `_search` returns `security_exception: unable to authenticate user` →
  the calling user doesn't exist on the remote, or the password differs.
  In Phase 1 just keep the elastic password identical on both sides.
- `connect_transport_exception` → the peer's transport Service is unreachable.
  Verify `<peer>-es-transport.chat.svc.cluster.local:9300` resolves and
  responds.

---

## 4. Phase 2: cross-K8s prod setup

### 4.1 Per-cluster manifest changes

Starting from the existing 9-node manifest, the deltas are:

**Δ1 — `spec.transport.tls.certificate` references the shared-CA Secret.**
Same as Phase 1. The Secret's content is the same across all 12 sites,
distributed via Vault → ESO (Section 5).

**Δ2 — A custom transport SAN that includes the public hostname**, so
calling clusters can validate the cert against `cluster.remote.<peer>.server_name`:

```yaml
spec:
  transport:
    tls:
      certificate:
        secretName: chat-transport-ca
      subjectAltNames:
      - dns: es-remote-site1.chat.com
      - dns: es-chat-site1-es-transport.chat.svc.cluster.local
```

**Δ3 — `secureSettings` does NOT need a per-peer credentials entry.** Because
auth is forward-the-user, the source cluster's keystore is unchanged from
the single-cluster baseline (only MinIO snapshot creds, if used).

### 4.2 Istio Gateway and VirtualService

The namespace default Gateway (owned by platform team, `*.chat.com`, port
443 mode SIMPLE with `ingress-cert`) cannot be modified. New per-site
resources for the transport endpoint mirror the existing pattern used for
ES HTTP and Kibana — separate Gateway resource, attached to the same
`chat-ingressgateway` selector, with TLS PASSTHROUGH on a specific hostname.

**Gateway** (per site):

```yaml
apiVersion: networking.istio.io/v1
kind: Gateway
metadata:
  name: chat-es-remote-site1-gateway
  namespace: chat
spec:
  selector:
    istio: chat-ingressgateway
  servers:
  - port: { number: 443, name: https-es-remote, protocol: HTTPS }
    hosts: [es-remote-site1.chat.com]
    tls: { mode: PASSTHROUGH }
```

**VirtualService:**

```yaml
apiVersion: networking.istio.io/v1
kind: VirtualService
metadata:
  name: chat-es-remote-site1-vs
  namespace: chat
spec:
  hosts: [es-remote-site1.chat.com]
  gateways: [chat-es-remote-site1-gateway]
  tls:
  - match:
    - port: 443
      sniHosts: [es-remote-site1.chat.com]
    route:
    - destination:
        host: es-chat-site1-es-transport.chat.svc.cluster.local
        port: { number: 9300 }
```

### 4.3 DestinationRule for sidecar coexistence

The Istio sidecar must not double-wrap the ES transport TLS. Two options
that both work:

- Annotation on ES pods: `traffic.sidecar.istio.io/excludeInboundPorts: "9300"`
  + `traffic.sidecar.istio.io/excludeOutboundPorts: "9300"`. Cleanest —
  9300 traffic bypasses the sidecar entirely, end-to-end TLS with no
  re-wrapping. (Used in the kind setup.)
- Or a `DestinationRule` with `tls.mode: DISABLE` for the transport Service.

Either one is required; without it the gateway → pod hop gets RST during
TLS handshake.

### 4.4 AuthorizationPolicy

Same as Phase 1 — the policy must have a workload selector so it only
applies to ES + Kibana pods, NOT the ingressgateway pod (which listens on
443 and would be blocked).

---

## 5. Shared-CA lifecycle

### 5.1 What lives where

| Where | Name | Holds |
|---|---|---|
| Vault path | `secret/elasticsearch/transport-ca` | `tls.crt` + `tls.key` of the shared transport CA |
| Per-namespace K8s Secret | `chat-transport-ca` | Same — populated by VSO from Vault |
| ES CRD reference | `spec.transport.tls.certificate.secretName: chat-transport-ca` | Each cluster references the Secret directly |

ECK reads the Secret, treats `tls.crt` + `tls.key` as a CA, and signs each
node's transport cert from it. All 12 clusters end up with node transport
certs chaining to the same root → mutual trust automatically.

### 5.2 Shared `elastic` user password

Stored at `secret/elasticsearch/elastic-user` in Vault as `elastic: <password>`.
A single VaultStaticSecret per site (the existing `<es>-es-elastic-user`
Secret pattern) syncs it. All 12 clusters end up with the same `elastic`
password → CCS auth-forwarding works without per-link credentials.

### 5.3 No rotation — indefinite validity by design choice

The shared transport CA is generated **once** with a 100-year validity
(`openssl req -x509 -days 36500`) and **never rotated**. This is a
deliberate operational choice mirroring the prior API-key design's
"indefinite key validity" decision: rotation is operationally expensive
(every cluster has to roll, in lockstep, with a CA-bundle dance), and the
threat model — self-managed CA, key never leaves Vault — doesn't require
it. The cert outlives the infrastructure it's deployed on.

If a future security review demands rotation, the runbook would be a
two-phase CA-bundle swap:

1. Generate a new CA. Issue a CA bundle that contains both old and new
   certs. Push to Vault.
2. ESO syncs to every site. ECK rolls every cluster, each now serving
   transport certs signed by NEW CA but still trusting OLD (because of the
   bundle).
3. Once all clusters are rolled, push a CA-only bundle without OLD. ECK
   rolls again. OLD-signed certs are rejected.

Until then: no rotation, no expiry surprise.

### 5.4 Why not per-cluster CAs cross-trusted

Mathematically equivalent, operationally worse: every cluster needs every
peer's CA in `spec.transport.tls.certificateAuthorities` (a list field). On
add/remove, every cluster rolls. With one shared CA, add/remove a site has
zero impact on existing clusters' certs.

---

## 6. Cluster registration

### 6.1 Settings payload

For each cluster, register the 11 peers as persistent cluster settings:

```json
PUT /_cluster/settings
{
  "persistent": {
    "cluster": {
      "remote": {
        "site2": {
          "mode": "proxy",
          "proxy_address": "es-remote-site2.chat.com:443",
          "server_name": "es-remote-site2.chat.com",
          "skip_unavailable": true
        }
        /* … 10 more, one per peer */
      }
    }
  }
}
```

In Phase 1 (same-namespace), substitute the in-cluster Service hostname:

```
"proxy_address": "es-chat-site2-es-transport.chat.svc.cluster.local:9300"
```

### 6.2 Field rationale

| Field | Value | Why |
|---|---|---|
| `mode` | `proxy` | Required — `sniff` mode needs direct transport reachability to every peer node, impossible through SNI passthrough. Proxy multiplexes N TCP sockets to one endpoint. |
| `proxy_address` | `es-remote-<peer>.chat.com:443` (Phase 2) or `<peer>-es-transport:9300` (Phase 1) | Port 443 in prod (the Istio LB port) — Istio passthrough hides the backend port from the client. |
| `server_name` | `es-remote-<peer>.chat.com` (Phase 2) | Both the SNI value (Istio routes by SNI) and the hostname checked against the cert SAN. Must exactly match. |
| `skip_unavailable` | `true` | If a peer is down, CCS queries return partial results from healthy peers instead of failing. |

No credential setting — auth is forward-the-user, so the keystore stays
clean of CCS-specific entries.

### 6.3 Registration Job

A K8s Job runs on each cluster after the shared CA has rolled and the
shared `elastic` password has rolled. The Job:

1. Reads the shared elastic password from the local
   `<cluster>-es-elastic-user` Secret.
2. Builds the settings payload with the cluster's peer list (excluding self).
3. Calls `PUT /_cluster/settings` against the cluster's local
   `<name>-es-http` Service.
4. Verifies via `GET /_remote/info` that all 11 peers report
   `connected: true`.

`PUT /_cluster/settings` is idempotent — re-applying the same settings is a
no-op. Safe to re-run on every Helm upgrade.

### 6.4 Order of operations per cluster

1. `Elasticsearch` resource applied → cluster up with
   `spec.transport.tls.certificate.secretName: chat-transport-ca`.
2. ESO has synced `chat-transport-ca` from Vault → ECK consumes it →
   node transport certs signed by shared CA.
3. ESO has synced `<cluster>-es-elastic-user` → ECK uses it as the
   `elastic` password.
4. Istio `Gateway` + `VirtualService` for `es-remote-<site>.chat.com` →
   public transport endpoint reachable.
5. AuthorizationPolicy with workload selector covers ES + Kibana pods only.
6. Register Job runs → `PUT /_cluster/settings` with the 11 peer entries.
7. Verify with `GET /_remote/info`.

There is **no inter-cluster dependency** — every cluster bootstraps
independently. The two-round Helm rollout from the prior design is no
longer necessary because there are no cross-cluster keys to mint and
distribute before registration can run.

---

## 7. Helm chart structure

### 7.1 Chart layout

A single chart `charts/elasticsearch/` parametrized per site:

```
charts/elasticsearch/
├── Chart.yaml
├── values.yaml                          # defaults
├── values/
│   ├── site1.yaml                       # site-specific overrides
│   ├── site2.yaml
│   └── … site12.yaml
└── templates/
    ├── es-cluster.yaml                  # Elasticsearch CR (Sections 3.3, 4.1)
    ├── kibana.yaml                      # Kibana CR
    ├── gateway.yaml                     # Istio Gateways (ES, Kibana, ES-remote when ccs.publicEndpoint.enabled)
    ├── virtualservice.yaml              # Matching VirtualServices
    ├── authorization-policy.yaml        # AuthZ scoped to ES + Kibana via workload selector
    ├── vault-secret-elastic-user.yaml   # VaultStaticSecret for shared elastic password
    ├── vault-secret-transport-ca.yaml   # VaultStaticSecret for shared transport CA (NEW)
    └── vault-secret-es-minio.yaml       # VaultStaticSecret for MinIO snapshot creds
```

Per-site values:

```yaml
# values/site1.yaml
properties:
  site: site1
  publicDomain: chat.com
ccs:
  enabled: true
  publicEndpoint:
    enabled: true                        # render the es-remote-<site> Gateway+VS
peers: [site2, site3, …, site12]
```

### 7.2 What's NOT in the chart anymore

Removed compared to the original API-key design:
- No `spec.remoteClusters[].apiKey` (Enterprise-gated)
- No `remote_cluster_server.enabled` config (paid feature)
- No `xpack.security.remote_cluster_server.ssl.*` (no separate listener)
- No `<es>-es-remote-cluster-service` (port 9443 — not used in cert-based)
- No `cc-bootstrap` user, no `cc-api-keys` Secrets, no key-mint Job
- No two-round Helm rollout (no cross-cluster pre-conditions)

Phase 2 still needs the per-site `chat-es-remote-<site>-gateway` /
`chat-es-remote-<site>-vs`, but they now route to port **9300** (transport)
instead of 9443.

---

## 8. Verification

### 8.1 Per-cluster checks

```
# 1. Transport cert chains to the shared CA
kubectl -n chat exec <pod> -- openssl x509 -in /usr/share/elasticsearch/config/transport-certs/<pod>.tls.crt -text \
  | grep -A1 'Issuer:'

# 2. _remote/info from inside Kibana Dev Tools or via curl
GET /_remote/info
# Expected: 11 entries (Phase 2) or 1 entry (Phase 1), each connected: true, mode: proxy
```

### 8.2 End-to-end mesh check

```
GET messages-*,*:messages-*/_search
```

Expected: hits from every site, with `_index` showing `<site>:` prefix on
remote hits.

### 8.3 search-service integration check

Run a NATS search request via search-service against any site. Confirm the
response merges hits from all sites.

---

## 9. Common failure modes

| Symptom | Root cause | Fix |
|---|---|---|
| `_remote/info` shows `connected: false` | Both clusters not signed by the same CA | Verify both reference the same `chat-transport-ca` Secret; ECK rolls on Secret change |
| TLS handshake errors during CCS | Sidecar mTLS double-wrapping 9300 | Add `traffic.sidecar.istio.io/excludeInboundPorts: "9300"` to ES pods |
| `security_exception: unable to authenticate user` on CCS query | Calling user has different password (or doesn't exist) on remote | Sync `elastic` password across all sites via Vault |
| Gateway returns RST instead of forwarding | AuthorizationPolicy with no workload selector blocks the gateway pod (port 443) | Add `selector.matchLabels: { common.k8s.elastic.co/type: elasticsearch }` (and one for kibana) |
| `connect_transport_exception` in Phase 2 | DNS / Istio Gateway misrouted | Confirm the per-site Gateway selects `istio: chat-ingressgateway` and the VS routes to the transport Service on 9300 |
| `_remote/info` connected: true but search returns 0 hits | `skip_unavailable: true` masking a remote failure | Run with explicit per-cluster `_search` to surface the error |

---

## 10. License compliance audit (revised)

| Component | License | Source of truth |
|---|---|---|
| Core CCS query path (`_search` against `cluster:index`) | **Basic** ✓ | https://www.elastic.co/subscriptions |
| TLS-cert-based CCS via transport (port 9300) | **Basic** ✓ | https://www.elastic.co/guide/en/elasticsearch/reference/current/remote-clusters-cert.html |
| ES Security (TLS, RBAC, native realm) | **Basic** ✓ (free since 6.8) | |
| ECK `spec.transport.tls.certificate` | **Basic** ✓ (operator is freeware, this field has been there since ECK 1.x) | |
| cert-manager + Let's Encrypt (used elsewhere) | Infrastructure (no ES license) | |
| ExternalSecrets / VSO + Vault | Infrastructure (no ES license) | |
| Istio Gateway / VirtualService / AuthorizationPolicy | Infrastructure (no ES license) | |

**Explicitly NOT used (paid):**

- ❌ Cross-cluster API keys — `advanced-remote-cluster-security`, paid (verified empirically — see Revision history)
- ❌ ECK `spec.remoteClusters[].apiKey` — Enterprise (verified empirically — operator logs)
- ❌ `remote_cluster_server` listener / port 9443 — paired with the API-key path, also paid
- ❌ CCR (Cross-Cluster Replication) — Platinum
- ❌ Field-level / document-level security — Platinum
- ❌ SAML / OIDC / Kerberos / LDAP / AD realms — Gold/Platinum
- ❌ Watcher / cluster alerts — Gold+
- ❌ ML / anomaly detection — Platinum

**Operational rule:** never click "Start a 30-day trial" in Kibana. Verify
license at any time with `GET /_license` — must show `"type": "basic"` and
`"status": "active"`.

---

## 11. Out of scope (deferred)

- **Fine-grained per-link auth** — cert-based CCS forwards the calling
  user, so granularity = whatever role that user has. If/when paid tier is
  acceptable, switch to API-key CCS for per-key access scoping.
- **Automated CA rotation** — design supports it (Section 5.3); deferred
  per "indefinite validity by design choice".
- **CCR for HA replication** — Platinum, never in scope.
- **Cross-cluster Kibana Spaces / dashboards beyond Dev Tools queries** —
  usable but not designed here.
- **Capacity planning for coord nodes under CCS load** — operational.

---

## 12. Resource inventory per site (Phase 2)

For each of the 12 sites, the Helm chart deploys:

1. `Elasticsearch` (with shared transport CA + `subjectAltNames`)
2. Istio `Gateway` `chat-es-remote-<site>-gateway`
3. Istio `VirtualService` `chat-es-remote-<site>-vs` (routes to port 9300)
4. `AuthorizationPolicy` with workload selector covering ES + Kibana pods
5. VaultStaticSecret `chat-transport-ca` (synced from
   `secret/elasticsearch/transport-ca`)
6. VaultStaticSecret `<es>-es-elastic-user` (synced from
   `secret/elasticsearch/elastic-user`)
7. Job `register-remotes` (post-install/upgrade hook — runs `PUT /_cluster/settings`)

Plus, outside the chart:

- DNS A record `es-remote-<site>.chat.com` → site's Istio LB IP
- Vault paths: `secret/elasticsearch/transport-ca`,
  `secret/elasticsearch/elastic-user`, `secret/elasticsearch/<site>/minio`

---

## 13. References

- `docs/superpowers/specs/2026-04-21-search-service-design.md` — defines the
  `messages-*,*:messages-*` query pattern this design enables.
- https://www.elastic.co/guide/en/elasticsearch/reference/current/remote-clusters-cert.html
  — TLS-cert-based CCS reference (the model used here).
- `charts/elasticsearch/kind/` — working same-namespace Phase 1
  implementation. Run `kind/setup.sh` then `kind/register-remotes.sh`.
