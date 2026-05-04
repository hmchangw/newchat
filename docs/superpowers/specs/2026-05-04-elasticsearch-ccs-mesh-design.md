# Elasticsearch Cross-Cluster Search (CCS) Mesh — Design

**Date:** 2026-05-04
**Status:** Design — pending implementation plan
**Scope:** Multi-site Elasticsearch federation via Cross-Cluster Search across 12 production sites, with a same-namespace test setup as a stepping stone.
**License constraint:** Everything in this design runs on Elasticsearch **Basic** tier. No paid features.

---

## 1. Problem statement

The chat platform federates across 12 sites, each running an independent ECK-managed Elasticsearch 8.19.8 cluster (3 master + 3 data + 3 coordinating, distributed across 3 datacenters per site). The `search-service` already issues `messages-*,*:messages-*` queries that depend on remote clusters being registered and reachable.

Today, only the application-layer CCS query path is built. The cross-cluster transport — registering remotes, exchanging trust, exposing the right ports through Istio, distributing API keys — is not yet wired. This design covers exactly that.

The constraint making this non-trivial:

- Sites run in **separate Kubernetes clusters** (one per region/DC group).
- Cross-site network reachability is **only via the public Istio ingress** (no VPC peering, no mesh federation, no Submariner).
- The shared Istio ingressgateway only listens on **ports 80 and 443**.
- The namespace default Gateway resource is owned by the platform team, terminates TLS for `*.chat.com` with a shared `ingress-cert`, and cannot be modified.
- Default sidecar injection is enabled on the `chat` namespace.
- An `AuthorizationPolicy` restricts traffic to source IPs in `172.x.x.x/8` and currently allows only port 9200.

The design uses Elasticsearch's **API-key based remote cluster model** (introduced in ES 8.10) with a **dedicated `remote_cluster_server` listener on port 9443**, exposed externally via SNI-based TLS passthrough through a per-site custom Istio Gateway, with TLS certificates issued by Let's Encrypt via cert-manager DNS-01 challenges.

---

## 2. Architecture overview

### 2.1 Two phases

| | Phase 1 (test) | Phase 2 (prod) |
|---|---|---|
| Topology | 2 ES clusters in same namespace, same K8s cluster | 12 ES clusters, separate K8s clusters, separate regions |
| Connectivity | Pod network (cluster-local DNS) | Public internet via Istio ingress only |
| Remote-cluster wiring | `spec.remoteClusters` on each ES with `apiKey: {}` (ECK-native) | Manual `PUT /_cluster/settings` per cluster, per peer |
| Trust | ECK auto-issues + auto-trusts API keys between operator-managed clusters | API keys generated explicitly per target cluster (12 total), distributed via Vault → ESO → K8s Secret → ES keystore |
| Istio changes | None — traffic stays in pod network | New SNI-passthrough Gateway + VirtualService per site for `es-remote-<site>.chat.com` |
| Transport TLS customization | None — ECK handles it | Let's Encrypt cert per site via cert-manager (DNS-01), consumed by ECK via `spec.transport.tls.certificate` |

Both phases use the **same underlying ES feature** (`cluster.remote.<name>.{mode, proxy_address, server_name}` with API-key auth via keystore). Only the *delivery* of those settings differs. The search-service code is unchanged across both phases.

### 2.2 Per-cluster node layout (unchanged from existing)

| Node set | Count | Roles | `remote_cluster_server.enabled` |
|---|---|---|---|
| `master-{a,b,c}` | 1 each (3 total) | `[master, remote_cluster_client]` | `false` (default) |
| `data-{a,b,c}` | 1 each (3 total) | `[data, remote_cluster_client]` | `false` (default) |
| `coords-{a,b,c}` | 1 each (3 total) | `[remote_cluster_client]` (coord-only) | **`true`** |

`remote_cluster_client` (a node role) and `remote_cluster_server` (a node setting) are independent and serve different sides of CCS:

- **`remote_cluster_client` role** → outbound: this node can initiate CCS requests to remotes. Kept on every node to avoid request-routing dead ends (e.g., Kibana queries that land on a master or data node).
- **`remote_cluster_server` setting** → inbound: this node opens the 9443 listener for incoming CCS calls. Restricted to coordinating nodes to keep the public attack surface concentrated on the cluster's natural front-door role.

### 2.3 Topology: full bidirectional mesh

Every site searches every other site. Each cluster registers all 11 peers as remotes; total system-wide: 132 directional remote-cluster registrations, but only **12 API keys** (per-target-shared simplification — each cluster issues one key that all 11 peers use).

### 2.4 Hostname / DNS layout per site

Three public hostnames per site, all under `*.chat.com`, all pointing at the same Istio LB IP:

| Hostname | Port | Backend Service | Purpose |
|---|---|---|---|
| `es-<site>.chat.com` | 443 → 9200 | `es-chat-<site>-es-http` | Existing ES HTTP API |
| `kibana-<site>.chat.com` | 443 → 5601 | `kibana-<site>-kb-http` | Existing Kibana UI |
| **`es-remote-<site>.chat.com`** | **443 → 9443** | **`es-chat-<site>-es-remote-cluster`** | **NEW — CCS inbound from peers** |

DNS additions: one A record per site (`es-remote-<site>.chat.com`).

---

## 3. Phase 1: same-namespace test setup

### 3.1 Goal

Validate the application-layer CCS path (search-service `messages-*,*:messages-*` query) and Kibana cross-cluster Dev Tools queries with **zero Istio changes**, **zero manual key management**, and **zero certificate work**.

### 3.2 Prerequisites

- ECK operator version **≥ 2.12** — the `spec.remoteClusters[].apiKey` field is required and was added in 2.12. Verify with `kubectl get statefulset -n elastic-system elastic-operator -o jsonpath='{.spec.template.spec.containers[0].image}'`.
- Elasticsearch version **≥ 8.10** — required for the API-key remote cluster model. Existing 8.19.8 satisfies this.
- Both `Elasticsearch` resources in the **same namespace** (`chat`) and managed by the **same ECK operator instance**.

### 3.3 Manifest changes vs. existing setup

For each of the two test clusters (`es-chat-site1`, `es-chat-site2`):

1. On each `coords-*` nodeSet, add to `config`:
   ```yaml
   remote_cluster_server.enabled: true
   ```
2. Add a top-level `spec.remoteClusters` block referencing the peer:
   ```yaml
   # On es-chat-site1
   spec:
     remoteClusters:
     - name: site2
       elasticsearchRef:
         name: es-chat-site2
         namespace: chat
       apiKey: {}      # forces API-key auth, not legacy cert-based mode

   # On es-chat-site2
   spec:
     remoteClusters:
     - name: site1
       elasticsearchRef:
         name: es-chat-site1
         namespace: chat
       apiKey: {}
   ```

### 3.4 What ECK does automatically when it sees `apiKey: {}`

1. Calls `POST /_security/cross_cluster/api_key` on the target cluster to mint a key with default `search` permissions on all indices.
2. Stores the resulting key in a K8s Secret in the namespace, owned by the source ES resource.
3. Adds that Secret as an entry in the source's `secureSettings`, mounting it into the keystore as `cluster.remote.<name>.credentials`.
4. Calls `PUT /_cluster/settings` on the source to register the remote with `mode: proxy`, `proxy_address` pointing at the target's internal `<es>-es-remote-cluster-service:9443`, and `server_name` set for TLS verification.
5. Re-mints and re-distributes keys on cert/key rotation events.

### 3.5 AuthorizationPolicy update (also required for Phase 1)

Add `9443` to the existing policy's allowed ports (option a — extend the existing rule's port list):

```yaml
spec:
  action: ALLOW
  rules:
  - from:
    - source: { ipBlocks: ["172.x.x.x/8"] }
    to:
    - operation: { ports: ["9200", "9443"] }
```

Without this, ECK's auto-wiring will appear to succeed but `_remote/info` will report `connected: false` because the sidecar AuthZ check on 9443 denies the inter-pod call.

### 3.6 Verification

In Kibana Dev Tools (single Kibana instance pointed at site1):

```
GET _remote/info                                        # site2 connected: true, mode: proxy
GET messages-*/_search                                  # local-only sanity check
GET site2:messages-*/_search                            # remote-only
GET messages-*,site2:messages-*/_search                 # exactly what search-service runs
```

Symmetry check: point Kibana at site2 (or curl) and run the inverse queries.

End-to-end: deploy `search-service` against site1 with seeded data on both clusters; confirm the NATS reply contains hits from both sites.

### 3.7 Common Phase 1 failures

- `_remote/info` shows `connected: false` → ECK operator < 2.12, or `remote_cluster_server.enabled` not actually applied to coord nodes, or AuthorizationPolicy missing 9443.
- `spec.remoteClusters` ignored, no Secret appears → operator version too old; check operator logs.
- `security_exception` on cross-cluster query → only if FLS/DLS or restricted indices are configured; out of scope for Phase 1, leave defaults.

---

## 4. Phase 2: cross-K8s prod setup

### 4.1 Per-cluster manifest changes

Starting from the existing 9-node manifest, the deltas are:

**Δ1 — `spec.remoteClusters` removed entirely.** ECK cannot wire across K8s clusters. Registration moves to a manual `PUT /_cluster/settings` Job (Section 7).

**Δ2 — `secureSettings` gains an API-key entry per remote.** The cluster's keystore needs the 11 API keys minted by its peers, mounted as `cluster.remote.<peer>.credentials`:

```yaml
spec:
  secureSettings:
  - secretName: es-chat-site1-es-minio                 # existing
    entries:
    - key: minio_access_key
      path: s3.client.default.access_key
    - key: minio_secret_key
      path: s3.client.default.secret_key
  - secretName: es-chat-site1-cc-api-keys              # NEW
    entries:
    - key: site2-api-key
      path: cluster.remote.site2.credentials
    - key: site3-api-key
      path: cluster.remote.site3.credentials
    # … 9 more, one per peer
```

Changes to this Secret trigger an ECK rolling restart to reload the keystore.

**Δ3 — Custom transport TLS certificate.** The remote_cluster_server listener uses transport TLS, and the cert must include `es-remote-<site>.chat.com` as a SAN (so calling clusters can validate the cert against `server_name`). Approach: Let's Encrypt via cert-manager DNS-01.

```yaml
spec:
  transport:
    tls:
      certificate:
        secretName: es-chat-site1-transport-cert       # cert-manager output
      subjectAltNames:
      - dns: es-remote-site1.chat.com
      - dns: es-chat-site1-es-coordinating.chat.svc.cluster.local
```

The JVM truststore in the ES image already trusts Let's Encrypt's root, so no per-cluster CA bundle distribution is required. cert-manager handles the 90-day rotation; ECK rolling-restarts on Secret change.

**Δ4 — `remote_cluster_server.enabled: true` on coord nodeSets.** Same as Phase 1.

### 4.2 Custom Service for port 9443

ECK does **not** automatically create a Service for the remote_cluster_server listener (verified — ECK creates only `<name>-es-http`, `<name>-es-transport`, and per-nodeSet headless services). Create manually:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: es-chat-site1-es-remote-cluster
  namespace: chat
  labels:
    elasticsearch.k8s.elastic.co/cluster-name: es-chat-site1
spec:
  type: ClusterIP
  ports:
  - name: remote-cluster
    port: 9443
    targetPort: 9443
    protocol: TCP
  selector:
    elasticsearch.k8s.elastic.co/cluster-name: es-chat-site1
    elasticsearch.k8s.elastic.co/node-master: "false"
    elasticsearch.k8s.elastic.co/node-data: "false"
```

Selector matches coord-only pods (where `remote_cluster_server.enabled: true` actually takes effect).

### 4.3 cert-manager Certificate

One Certificate resource per cluster, issued by a `letsencrypt-prod` `ClusterIssuer` configured for **DNS-01** challenge:

```yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: es-chat-site1-transport-cert
  namespace: chat
spec:
  secretName: es-chat-site1-transport-cert
  issuerRef:
    name: letsencrypt-prod
    kind: ClusterIssuer
  dnsNames:
  - es-remote-site1.chat.com
  - es-chat-site1-es-coordinating.chat.svc.cluster.local
```

DNS-01 is required because the ingressgateway terminates port 80 with `httpsRedirect: true` (HTTP-01 won't work).

### 4.4 Istio Gateway and VirtualService

The namespace default Gateway (owned by platform team, `*.chat.com`, port 443 mode SIMPLE with `ingress-cert`) cannot be modified. New per-site resources for the remote-cluster endpoint mirror the existing pattern used for ES HTTP and Kibana — separate Gateway resource, attached to the same `chat-ingressgateway` selector, with TLS PASSTHROUGH on a specific hostname. Specific hostnames win SNI matching against `*.chat.com`, so coexistence with the namespace default is safe.

**Gateway** (per site, separate resource for blast-radius isolation):

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
  - port: { number: 80, name: http, protocol: HTTP }
    hosts: [es-remote-site1.chat.com]
    tls: { httpsRedirect: true }
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
  hosts:
  - es-remote-site1.chat.com
  gateways:
  - chat-es-remote-site1-gateway
  tls:
  - match:
    - port: 443
      sniHosts: [es-remote-site1.chat.com]
    route:
    - destination:
        host: es-chat-site1-es-remote-cluster.chat.svc.cluster.local
        port: { number: 9443 }
```

### 4.5 Sidecar / PeerAuthentication compatibility

Whatever pattern is currently in use to make port 9200 work through the sidecar (PeerAuthentication PERMISSIVE, `traffic.sidecar.istio.io/excludeInboundPorts` annotation, or a DestinationRule with `tls.mode: DISABLE`) must be extended to also cover port **9443**. End-to-end TLS passthrough requires Istio mTLS to NOT double-wrap the connection.

### 4.6 AuthorizationPolicy

Same change as Phase 1: add `9443` to the existing policy's allowed ports.

---

## 5. API key lifecycle

### 5.1 Naming convention

| Where | Name | Holds |
|---|---|---|
| Vault path | `secret/elasticsearch/cc-api-keys/<site>` | The single key minted by `<site>`, used by all peers to connect into `<site>` |
| Issuing cluster's local Secret | `es-chat-<site>-cc-issued-key` | Same key, source of truth pre-Vault sync |
| Consuming cluster's local Secret | `es-chat-<site>-cc-api-keys` | All 11 keys this cluster needs to call its peers |
| Keystore path inside ES | `cluster.remote.<peer>.credentials` | What ECK mounts via `secureSettings` |

### 5.2 Cross-cluster API key

Minted via:

```json
POST /_security/cross_cluster/api_key
{
  "name": "ccs-mesh-shared",
  "access": {
    "search": [
      { "names": ["messages-*"] }
    ]
  },
  "metadata": {
    "purpose": "CCS mesh — minted by <site>, consumed by all peers",
    "created_at": "<ISO timestamp>"
  }
}
```

**No `expiration` field** — the key has indefinite validity per design choice. Revocation is via `DELETE /_security/cross_cluster/api_key/<id>` if compromise is suspected.

The `encoded` value from the response is what goes into the keystore (do not re-encode).

### 5.3 Bootstrap user

A dedicated native-realm user `cc-bootstrap` with cluster privileges `["manage_security", "manage"]` is used by all CCS automation Jobs (key minting, cluster registration, revocation). Created once during cluster commissioning by an init Job that authenticates as `elastic` (from the existing `<cluster>-es-elastic-user` Vault-backed Secret). After creation, `elastic` is never used by automation again.

`cc-bootstrap` credentials are stored at `secret/elasticsearch/bootstrap-user/<site>` in Vault.

### 5.4 Distribution via Vault + ExternalSecrets Operator

One `ExternalSecret` per cluster pulls the 11 peer keys into a single K8s Secret:

```yaml
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata:
  name: es-chat-site2-cc-api-keys
  namespace: chat
spec:
  refreshInterval: 1h
  secretStoreRef: { name: vault-backend, kind: ClusterSecretStore }
  target:
    name: es-chat-site2-cc-api-keys
    creationPolicy: Owner
  data:
  - secretKey: site1-api-key
    remoteRef: { key: secret/elasticsearch/cc-api-keys/site1, property: encoded }
  - secretKey: site3-api-key
    remoteRef: { key: secret/elasticsearch/cc-api-keys/site3, property: encoded }
  # … 9 more entries, one per peer
```

ECK consumes the resulting Secret via `secureSettings` (Section 4.1, Δ2) and mounts entries into the keystore.

### 5.5 Mesh-wide key inventory

- 12 keys total (one per cluster, shared across the cluster's 11 inbound consumers).
- Each cluster issues 1, consumes 11.
- No rotation. Revoke individually on incident.

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
        },
        /* … 10 more, one per peer */
      }
    }
  }
}
```

### 6.2 Field rationale

| Field | Value | Why |
|---|---|---|
| `mode` | `proxy` | Required — `sniff` mode needs direct transport reachability to every peer node, impossible through SNI passthrough. Proxy multiplexes N TCP sockets to one endpoint. |
| `proxy_address` | `es-remote-<peer>.chat.com:443` | Port 443 (the Istio LB port), not 9443 — Istio passthrough hides the backend port from the client. |
| `server_name` | `es-remote-<peer>.chat.com` | Both the SNI value (Istio routes by SNI) and the hostname checked against the cert SAN. Must exactly match. |
| `skip_unavailable` | `true` | If a peer is down, CCS queries return partial results from healthy peers instead of failing. Critical for production resilience. |

API-key credentials are NOT in the settings document — they're in the keystore at `cluster.remote.<peer>.credentials` (Section 5).

### 6.3 Registration Job

A K8s Job runs on each cluster after `secureSettings` are populated and ECK's rolling restart has loaded the keystore. The Job:

1. Reads `cc-bootstrap` credentials from the local Secret synced from Vault.
2. Builds the settings payload with the cluster's peer list (excluding self).
3. Calls `PUT /_cluster/settings` against the cluster's local `<name>-es-http` Service.
4. Verifies via `GET /_remote/info` that all 11 peers report `connected: true`.

`PUT /_cluster/settings` is idempotent — re-applying the same settings is a no-op. Safe to re-run on every Helm upgrade.

### 6.4 Order of operations per cluster

1. `Elasticsearch` resource applied → cluster up with `remote_cluster_server.enabled: true` on coords.
2. `Service es-chat-<site>-es-remote-cluster` applied.
3. cert-manager `Certificate` issued → Secret populated → ECK consumes via `spec.transport.tls.certificate`.
4. Istio `Gateway` + `VirtualService` applied → public endpoint reachable.
5. AuthorizationPolicy includes 9443.
6. Init Job creates `cc-bootstrap` user (one-time, using `elastic`).
7. Mint Job runs → key created → stored in Vault.
8. ESO syncs the 11 peers' keys into the local Secret (depends on each peer having completed step 7).
9. ECK reconciles `secureSettings` → rolling restart → keystore loaded.
10. Register Job runs → `PUT /_cluster/settings` with the 11 peer entries.
11. Verify with `GET /_remote/info`.

Step 8 implies a soft mesh-wide dependency: every cluster must finish step 7 before any cluster can finish step 8. Solution: two-phase Helm rollout (Section 7.2).

---

## 7. Helm chart structure

### 7.1 Chart layout

A single chart `chat-elasticsearch-site/` parametrized per site:

```
chat-elasticsearch-site/
├── Chart.yaml
├── values.yaml                          # defaults
├── values/
│   ├── site1.yaml                       # site-specific overrides
│   ├── site2.yaml
│   └── … site12.yaml
└── templates/
    ├── elasticsearch.yaml               # Elasticsearch CR (Section 4.1)
    ├── service-remote-cluster.yaml      # 9443 Service (Section 4.2)
    ├── certificate.yaml                 # cert-manager Certificate (Section 4.3)
    ├── gateway.yaml                     # Istio Gateway (Section 4.4)
    ├── virtualservice.yaml              # Istio VS (Section 4.4)
    ├── authorization-policy.yaml        # AuthZ with 9443 (Section 4.6)
    ├── externalsecret-cc-keys.yaml      # ESO ExternalSecret (Section 5.4)
    ├── job-init-bootstrap-user.yaml     # init Job (Section 5.3)
    ├── job-mint-key.yaml                # mint Job (Section 5.2)
    └── job-register-remotes.yaml        # registration Job (Section 6.3)
```

Per-site values:

```yaml
# values/site1.yaml
site:
  name: site1
  publicDomain: chat.com
  zones: [zone1-dc1, zone1-dc2, zone1-dc3]
peers: [site2, site3, site4, site5, site6, site7, site8, site9, site10, site11, site12]
elasticsearch:
  version: 8.19.8
  storageClass: fast-ssd-region-a
```

Deploy: `helm upgrade --install es-chat-site1 ./chat-elasticsearch-site -f values/site1.yaml`.

### 7.2 Job lifecycle and cross-release dependency

Use `helm.sh/hook: post-install,post-upgrade` annotations with weights:

- `init-bootstrap-user` — weight 5
- `mint-key` — weight 10
- `register-remotes` — weight 20

Hook delete policy: `before-hook-creation` (so re-runs are clean).

**Cross-release dependency** (registration depends on all 12 mints): Helm has no awareness across releases. Solution — **two-round rollout**:

1. **Round 1:** Deploy all 12 sites with `--set jobs.registerRemotes.enabled=false`. Brings up clusters, mints all 12 keys to Vault.
2. **Round 2:** Re-deploy all 12 with `--set jobs.registerRemotes.enabled=true`. Each cluster now has all peer keys synced via ESO, and the register Job successfully runs.

Manual but transparent. ArgoCD / CI orchestrates the two rounds.

### 7.3 Adding or removing a site

- **Add site13:** Add `values/site13.yaml`, deploy site13 through round 1 + round 2. Then bump every existing `peers` list to include `site13`, run `helm upgrade` across all 12 — register Job re-runs and adds site13 to each cluster's `_cluster/settings`. Mint Job is a no-op on existing sites.
- **Remove a site:** Remove from peer lists, run `helm upgrade` — register Job sets `cluster.remote.<site>: null`. Then decommission the ES cluster.

---

## 8. Verification

### 8.1 Per-cluster checks

After each cluster's commissioning:

```
# 1. Cert is LE-signed and SAN-valid
openssl s_client -connect es-remote-<site>.chat.com:443 \
                 -servername es-remote-<site>.chat.com \
                 -showcerts

# 2. Coord pods have remote_cluster_server enabled
kubectl exec -n chat <coord-pod> -- curl -s -u <user>:<pass> \
  http://localhost:9200/_nodes/settings?filter_path=**.remote_cluster_server

# 3. AuthZ allows 9443
kubectl exec -n chat <some-pod> -- curl -k https://es-chat-<site>-es-remote-cluster:9443
# Expected: TLS error (binary protocol mismatch), NOT "RBAC: access denied"

# 4. All peers connected
GET /_remote/info
# Expected: 11 entries, each with connected: true, mode: proxy
```

### 8.2 End-to-end mesh check

From any site, in Kibana Dev Tools:

```
GET messages-*,*:messages-*/_search
```

Expected: hits from all 12 sites, with `_index` showing `<site>:` prefix on remote hits.

### 8.3 search-service integration check

Run a NATS search request via search-service against any site. Confirm the response merges hits from all 12 clusters.

---

## 9. Common failure modes

| Symptom | Root cause | Fix |
|---|---|---|
| `_remote/info` shows `connected: false` (Phase 2) | TLS hostname mismatch — `server_name` doesn't match cert SAN | Verify Certificate's `dnsNames` includes the exact hostname used in `server_name` |
| `_remote/info` shows `connected: false` (Phase 1) | ECK operator < 2.12, or `remote_cluster_server.enabled` not applied | Check operator version; `GET _nodes/settings` for the setting |
| 401 on CCS query, but `_remote/info` connected | Keystore entry missing or empty for that peer | Verify local `cc-api-keys` Secret has the entry; verify ESO sync; ECK rolling restart should auto-load |
| `RBAC: access denied` from Istio | AuthorizationPolicy doesn't allow 9443 | Apply Section 4.6 update |
| TLS handshake succeeds but ES rejects at protocol level | Sidecar mTLS double-wrapping | Mirror the existing 9200 sidecar exclusion to 9443 |
| `_search` returns local hits but no remote hits, no error | `skip_unavailable: true` masking a remote failure | Run `GET _remote/info`; verify `_cluster/settings` registration |
| Helm `register-remotes` Job fails on first round | Peer keys not yet in Vault | Use two-round rollout (Section 7.2) |
| Unexpected rolling restart | Any `secureSettings` Secret change triggers it | Expected; schedule mints during low-traffic windows |

---

## 10. License compliance audit

Every component used:

| Component | License |
|---|---|
| CCS query path | Basic |
| `remote_cluster_server` listener (port 9443) | Basic |
| Cross-cluster API keys (`POST /_security/cross_cluster/api_key`) | Basic |
| Indefinite API key validity (no `expiration`) | Basic |
| Native-realm `cc-bootstrap` user with `manage_security` + `manage` | Basic |
| ECK `secureSettings`, `spec.transport.tls.certificate`, `spec.remoteClusters` | Basic (operator is freeware) |
| ES Security (TLS, RBAC, native realm) | Basic (free since 6.8) |
| cert-manager + Let's Encrypt | Infrastructure (no ES license) |
| ExternalSecrets Operator + Vault | Infrastructure (no ES license) |
| Istio Gateway / VirtualService / AuthorizationPolicy | Infrastructure (no ES license) |

**Explicitly NOT used (would require paid tiers):**

- ❌ CCR (Cross-Cluster Replication) — Platinum
- ❌ Field-level / document-level security on cross-cluster keys — Platinum
- ❌ SAML / OIDC / Kerberos / LDAP / AD realms — Gold/Platinum
- ❌ Watcher / cluster alerts — Gold+
- ❌ ML / anomaly detection — Platinum
- ❌ Cross-cluster searchable snapshots — Enterprise

**Operational rule:** never click "Start a 30-day trial" in Kibana. Verify license at any time with `GET /_license` — must show `"type": "basic"` and `"status": "active"`.

---

## 11. Out of scope (deferred)

- **Per-link API keys (132 total)** — design extends to it; deferred until security review demands finer granularity.
- **Automated key rotation** — design extends to it; deferred per "indefinite validity" decision.
- **CCR for HA replication** — Platinum, never in scope.
- **Cross-cluster Kibana Spaces / dashboards beyond Dev Tools queries** — usable but not designed here.
- **Capacity planning for coord nodes under CCS load** — operational concern, not architectural.
- **Pattern B in-Job retry for mesh registration race** — refinement if rollouts become frequent; Pattern A two-round Helm rollout is the initial approach.

---

## 12. Resource inventory per site (Phase 2)

For each of the 12 sites, the Helm chart deploys:

1. `Elasticsearch` (with Phase 2 deltas)
2. `Service` `es-chat-<site>-es-remote-cluster` (port 9443)
3. cert-manager `Certificate` `es-chat-<site>-transport-cert`
4. Istio `Gateway` `chat-es-remote-<site>-gateway`
5. Istio `VirtualService` `chat-es-remote-<site>-vs`
6. `AuthorizationPolicy` patch (9443 added) — shared across the namespace, applied once
7. ESO `ExternalSecret` `es-chat-<site>-cc-api-keys`
8. Job `init-bootstrap-user` (post-install/upgrade hook, weight 5)
9. Job `mint-key` (post-install/upgrade hook, weight 10)
10. Job `register-remotes` (post-install/upgrade hook, weight 20, gated by `--set jobs.registerRemotes.enabled`)

Plus, outside the chart:

- DNS A record `es-remote-<site>.chat.com` → site's Istio LB IP
- Vault paths: `secret/elasticsearch/cc-api-keys/<site>`, `secret/elasticsearch/bootstrap-user/<site>`

---

## 13. References

- Existing ES manifest pattern: prior `Elasticsearch` resource for `es-chat-site1` with MinIO secureSettings, 3-DC zone awareness, securityContext.
- Existing Istio pattern: per-app passthrough Gateway resources (`chat-es-site1-gateway`, `chat-kibana-site1-gateway`) coexisting with namespace default `*.chat.com` SIMPLE-mode gateway.
- Existing Vault pattern: `<cluster>-es-elastic-user` Secret pre-created so ECK consumes the password instead of generating one.
- Existing AuthorizationPolicy: source `172.x.x.x/8`, currently allows port 9200 only.
- search-service spec: `docs/superpowers/specs/2026-04-21-search-service-design.md` — defines the `messages-*,*:messages-*` query pattern this design enables.
