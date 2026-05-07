# Cross-Cluster Search (CCS) setup with this Helm chart

End-to-end guide to bringing up two ECK Elasticsearch clusters in the same
Kubernetes namespace with TLS-cert-based CCS wired between them, using only
this chart at `charts/elasticsearch/` plus one post-install registration step.

**License:** Basic only. See
`docs/superpowers/specs/2026-05-04-elasticsearch-ccs-mesh-design.md` for why
API-key CCS isn't an option (`advanced-remote-cluster-security` is paid;
ECK's `spec.remoteClusters` automation is Enterprise-gated).

---

## 1. Architecture at a glance

**FigJam diagram:** https://www.figma.com/board/deECG6NycF9v24uJwob8EM
([open in editor](https://www.figma.com/board/deECG6NycF9v24uJwob8EM?utm_source=claude&utm_content=edit_in_figjam))

### Request lifecycle

1. **DNS resolves** `es-site1.chat.com` to the Istio ingress LB IP (in kind:
   `127.0.0.1` via `extraPortMappings 30443→443`).
2. **HTTPS request hits the per-namespace ingressgateway pod** on port 443.
   - The Istio `Gateway` matches by SNI (`es-site1.chat.com`) and is configured
     for **TLS PASSTHROUGH** — the gateway does NOT terminate TLS.
   - The `VirtualService` routes the TLS-encrypted bytes to the backend
     Service `es-chat-site1-es-http:9200`.
   - The `DestinationRule` `chat-es-passthrough` sets `tls.mode: DISABLE`,
     telling the gateway's Envoy NOT to mTLS-originate to the backend.
   - The pod-level `traffic.sidecar.istio.io/excludeInboundPorts: "9200,9300"`
     annotation makes the receiving sidecar transparent — bytes go directly
     to the ES container, which terminates the client's TLS.
   - The `AuthorizationPolicy` is workload-selector-scoped to ES + Kibana
     pods only, so it doesn't accidentally block port 443 on the gateway.
3. **Cross-cluster query**: when a user runs a search like
   `messages-*,es-chat-site2:messages-*/_search`, the local ES coordinator
   opens a **transport-protocol** connection to
   `es-chat-site2-es-transport:9300`.
   - Mutual TLS using the **shared transport CA** (every cluster's node certs
     chain to the same root → automatic mutual trust).
   - Authentication: the calling user's credentials are forwarded; both
     clusters have the SAME `elastic` password (from the shared Vault path)
     so the remote authenticates successfully.
   - No API keys, no port 9443. Pure cert-based, Basic-tier.

### TLS layering at every hop

| Hop | Carries | Terminated by | Cert |
|---|---|---|---|
| User → Istio LB (443) | Client TLS (HTTPS) | The destination ES pod (PASSTHROUGH) | ECK's auto-generated `<es>-es-http-certs-public` self-signed cert |
| Istio LB → Backend Service (9200) | Same client TLS bytes, untouched | Same — sidecar bypassed via inbound exclusion | Same |
| ES coordinator → remote transport Service (9300) | Transport-protocol TLS | The remote ES pod's transport listener | Node cert signed by shared `chat-transport-ca` |
| ES pod ↔ ES pod intra-cluster (9300) | Transport TLS | Each pod | Same shared-CA-signed node cert |

---

## 2. Exposed URLs (after `/etc/hosts` mapping in kind)

Add to `/etc/hosts`:

```
127.0.0.1  es-site1.chat.com kibana-site1.chat.com es-site2.chat.com kibana-site2.chat.com
```

| URL | Backed by | Use |
|---|---|---|
| `https://es-site1.chat.com/` | Service `es-chat-site1-es-http:9200` | site1 Elasticsearch HTTP API |
| `https://es-site2.chat.com/` | Service `es-chat-site2-es-http:9200` | site2 Elasticsearch HTTP API |
| `https://kibana-site1.chat.com/` | Service `kibana-chat-site1-kb-http:5601` | site1 Kibana UI (Dev Tools) |
| `https://kibana-site2.chat.com/` | Service `kibana-chat-site2-kb-http:5601` | site2 Kibana UI |

In **Phase 2 prod**, with `ccs.publicEndpoint.enabled: true`, you also get
`es-remote-<site>.chat.com:443` per site, which Istio passthrough routes to
the cluster's `<es>-es-transport:9300`. That endpoint is what cross-K8s
peers use as `cluster.remote.<site>.proxy_address`. In Phase 1 same-namespace
it's not needed — peers use the in-cluster transport Service directly.

All HTTPS endpoints serve **ECK's auto-generated self-signed cert**; clients
need `-k` / browser "accept risk" / a trusted bundle.

Login: `elastic / chat-elastic-pw` (the seeded shared password). Same on
both clusters.

---

## 3. Setup steps

### Prerequisites

- Kubernetes cluster (kind, EKS, GKE, anything)
- ECK operator installed (any 2.x version, or 1.x — chart only uses
  `spec.transport.tls.certificate`, supported since ECK 1.0)
- Istio installed with a per-namespace ingressgateway pod labeled
  `istio: chat-ingressgateway` in the target namespace
- HashiCorp Vault + Vault Secrets Operator
- A `VaultAuth` resource named `chat-vault-auth` in the target namespace,
  bound to a Vault role that can read `secret/data/elasticsearch/*`
- Namespace `chat` labeled `istio-injection=enabled`

For local kind, `charts/elasticsearch/kind/setup.sh` brings all of the
above up from vendored chart `.tgz` files.

### Step A — Pre-seed Vault

```bash
# 1. Shared elastic superuser password (CCS auth-forwarding requires this).
vault kv put secret/elasticsearch/elastic-user \
  elastic="<password>"

# 2. Shared transport CA — generated once, valid for 100 years (effectively
#    indefinite — see design doc §5.3 for the no-rotation rationale).
openssl req -x509 -nodes -newkey rsa:2048 -days 36500 \
  -subj "/CN=chat-transport-ca" \
  -keyout ca.key -out ca.crt
vault kv put secret/elasticsearch/transport-ca \
  "tls.crt=$(cat ca.crt)" "tls.key=$(cat ca.key)"

# 3. MinIO snapshot creds (real in prod, dummies in dev — chart references them).
vault kv put secret/elasticsearch/site1/minio \
  MINIO_BUCKET_ACCESS_KEY=<key> MINIO_BUCKET_SECRET_KEY=<secret>
vault kv put secret/elasticsearch/site2/minio \
  MINIO_BUCKET_ACCESS_KEY=<key> MINIO_BUCKET_SECRET_KEY=<secret>
```

### Step B — Helm install both clusters

> **Kind footprint note**
> The chart's default values render **3 master + 1 data + 3 coord** = 7 pods
> per cluster (14 total). Combined with the Istio sidecars + Istio control
> plane + Vault + VSO + ECK + control plane, that needs ~12Gi Docker Desktop
> memory; the default 8Gi MacBook Docker allocation thrashes the apiserver.
> The kind values files at `charts/elasticsearch/kind/values/site*-kind.yaml`
> therefore **override `cluster.nodeSets` to a single 1+1+1 layout** for kind
> only. Prod values (`charts/elasticsearch/values/site*.yaml`) keep the full
> 3+1+3 layout for HA. This override has no effect on the wire-level CCS
> behaviour — the cert-based plumbing is identical regardless of node count.



```bash
# site1 owns the namespace-singleton resources (transport-CA Secret + DestinationRule)
helm upgrade --install es-chat-site1 ./charts/elasticsearch \
  -n chat --force-conflicts \
  -f charts/elasticsearch/kind/values/site1-kind.yaml

# site2 references them but doesn't manage them
helm upgrade --install es-chat-site2 ./charts/elasticsearch \
  -n chat --force-conflicts \
  -f charts/elasticsearch/kind/values/site2-kind.yaml
```

The key bit in the values files:

```yaml
# site1-kind.yaml
ccs:
  enabled: true
  transport:
    enabled: true
    caSecretName: chat-transport-ca
    manageCASecret: true       # site1 ONLY — creates the namespace's shared CA Secret + DestinationRule

# site2-kind.yaml
ccs:
  transport:
    manageCASecret: false      # every other site references but doesn't manage
```

`--force-conflicts` is necessary because ECK takes server-side-apply
ownership of `spec.nodeSets` after first reconciliation.

### Step C — Wait for both clusters green

```bash
kubectl -n chat wait --for=jsonpath='{.status.health}'=green elasticsearch/es-chat-site1 --timeout=600s
kubectl -n chat wait --for=jsonpath='{.status.health}'=green elasticsearch/es-chat-site2 --timeout=600s
```

### Step D — Register peers

Two equivalent options. Both call `PUT /_cluster/settings` on each cluster
and produce identical state in Elasticsearch — pick by ergonomic profile.

#### Option D1 — Helm post-install Job (default for prod, declarative)

Set `ccs.peers` and `ccs.mode` in each cluster's values file. The chart
renders a `Job` (template: `templates/job-register-remotes.yaml`) gated on
`helm.sh/hook: post-install,post-upgrade`, so registration runs
automatically as part of `helm install` / `helm upgrade`. Re-runnable;
adding/removing peers is purely a values-file change.

```yaml
# site1 values
ccs:
  enabled: true
  peers: [site2]                      # Phase 1: just the one peer
  mode: internal                      # in-cluster transport Service
  registrationJob:
    enabled: true                     # default — set false to suppress

# site2 values mirror it: peers: [site1]
```

For Phase 2 cross-K8s, use `mode: public` and list all 11 peers:
```yaml
ccs:
  enabled: true
  peers: [site2, site3, site4, site5, site6, site7, site8, site9, site10, site11, site12]
  mode: public
  registrationJob:
    enabled: true
```

What the Job does: waits for local ES `/_cluster/health` to be yellow/green,
builds the `cluster.remote.<peer>.{mode,proxy_address,server_name,skip_unavailable}`
JSON for every peer, PUTs it, then verifies via `_remote/info`. Idempotent —
re-runs on every `helm upgrade`. Logs visible via:

```bash
kubectl -n chat logs job/register-remotes-site1
```

#### Option D2 — `register-remotes.sh` script (ad-hoc / emergency / kind harness)

```bash
./charts/elasticsearch/kind/register-remotes.sh
```

Same logic, run from your laptop. Useful when:
- The Job's container image isn't available in your registry
- You need to re-register one cluster without `helm upgrade`-ing
- You're debugging connectivity and want interactive iteration

The default kind harnesses (`kind/setup.sh`, `kind-multi/setup-multi.sh`)
use this path because the values files there leave `ccs.peers` empty.

#### Pick one (you don't need both at the same time)

| Use case | Pick |
|---|---|
| Production rollout (12 K8s clusters), where ops wants `helm install` to be the entire workflow | **D1 (Job)** |
| Local kind harness, fast iteration, debugging | **D2 (script)** — or set `ccs.registrationJob.enabled: false` if you want the chart to leave it alone |
| Emergency surgery on a single cluster | **D2 (script)** |
| Rolling out a 13th site later | **D1 (Job)** — bump `ccs.peers` everywhere, `helm upgrade`, done |

---

## 4. Verification

```bash
# 4a. Both peers see each other connected
curl -k -u elastic:chat-elastic-pw https://es-site1.chat.com/_remote/info | jq .
# → { "es-chat-site2": { "connected": true, "mode": "proxy", ... } }

curl -k -u elastic:chat-elastic-pw https://es-site2.chat.com/_remote/info | jq .
# → { "es-chat-site1": { "connected": true, "mode": "proxy", ... } }

# 4b. End-to-end cross-cluster search
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
# → 2 hits, one local + one prefixed `es-chat-site2:`
```

Or use Kibana Dev Tools at `https://kibana-site1.chat.com/`:

```
GET _remote/info
GET messages-*,es-chat-site2:messages-*/_search
```

---

## 5. What's automated vs not

### Helm renders, ECK / VSO / Istio reconcile (zero manual work)
- `Elasticsearch` + `Kibana` CRs (3 master + 1 data + 3 coord per cluster by default)
- Per-site Istio `Gateway`, `VirtualService`, `AuthorizationPolicy`, `DestinationRule`
- `VaultStaticSecret` resources for the elastic password, transport CA, MinIO creds
- ECK rolling restarts whenever a referenced Secret changes (cert rotation handled automatically)

### Outside Helm
1. **Vault path seeding** (Step A) — bootstrap-time, once per environment
2. **Cross-cluster registration** (Step D) — both options live inside the
   chart now. The Helm post-install Job (`templates/job-register-remotes.yaml`)
   handles it declaratively when `ccs.peers` is set; `register-remotes.sh`
   remains under `kind/` for ad-hoc invocations. Helm has no cross-release
   ordering primitive, but `cluster.remote.*.skip_unavailable: true` makes
   the order independent — each cluster registers its peers when it's ready,
   and the actual transport connections establish whenever both ends come up.

---

## 6. Common failure modes

| Symptom | Cause | Fix |
|---|---|---|
| Helm install fails: `chat-transport-ca exists and cannot be imported into the current release` | Both releases trying to manage the same namespace-singleton Secret | Set `ccs.transport.manageCASecret: true` on exactly one release, `false` on all others |
| `helm upgrade` fails: `conflict with elastic-operator using elasticsearch.k8s.elastic.co: .spec.nodeSets` | ECK has SSA ownership of nodeSets after first reconcile | Add `--force-conflicts` |
| `_remote/info` returns `connected: false` | Both clusters not signed by the same CA | Verify `kubectl -n chat get secret chat-transport-ca` exists and both clusters reference it via `spec.transport.tls.certificate` |
| `_remote/info` shows connected, but `_search` returns `unable to authenticate user` | Calling user has different password on remote | Confirm the Vault path `elasticsearch/elastic-user` is consumed by both releases (check `vault.paths.elasticUser` in both values files) |
| `https://es-site1.chat.com` returns RST after TLS handshake | Gateway is mTLS-originating to ES whose 9200 is sidecar-excluded | Verify the `chat-es-passthrough` DestinationRule exists (created when at least one release has `manageCASecret: true`) |
| `https://es-site1.chat.com` returns immediate connection close | AuthorizationPolicy missing the workload selector and blocking the gateway pod (port 443) | Verify `kubectl -n chat get authorizationpolicy chat-es-site1-authz -o yaml` has `spec.selector.matchLabels.common.k8s.elastic.co/type: elasticsearch` (default) |
| ES pods crash-loop with `xpack.security.remote_cluster_server.ssl - server ssl configuration requires a key and certificate` | Stale state — the chart used to inject this; latest chart removed it | `helm upgrade --force-conflicts` to re-render with the current template |
| `helm install` fails with `Job register-remotes-<site> failed` | Job timed out waiting for ES health, or PUT failed with non-200 | `kubectl -n chat logs job/register-remotes-<site>` to see why; common causes: ES not green within `ccs.registrationJob.healthTimeoutSeconds` (bump it), wrong elastic password (check Vault), peer hostname unresolvable when `mode: public` (verify DNS) |
| Job succeeds but `_remote/info` shows `connected: false` for some peers | Peer cluster not up yet, or peer's transport listener unreachable | Expected during initial mesh bring-up — `skip_unavailable: true` keeps queries working with partial mesh. Re-check `_remote/info` after all peers are healthy. |

---

## 7. File reference

```
charts/elasticsearch/
├── values.yaml                          # Defaults — sized for kind (3+1+3 nodes, 256Mi/512Mi)
├── values/
│   ├── site1.yaml                       # Phase 2 prod values (manageCASecret=true)
│   └── site2.yaml                       # Phase 2 prod values (manageCASecret=false)
├── templates/
│   ├── es-cluster.yaml                  # Elasticsearch CR (shared transport CA reference)
│   ├── kibana.yaml                      # Kibana CR
│   ├── gateway.yaml                     # Istio Gateways — 443 TLS PASSTHROUGH per hostname
│   ├── virtualservice.yaml              # SNI-routed VirtualServices (443 → 9200 / 5601 / 9300)
│   ├── authorization-policy.yaml        # Workload-selector-scoped to ES + Kibana
│   ├── destinationrule.yaml             # chat-es-passthrough — gated on manageCASecret
│   ├── job-register-remotes.yaml       # Post-install Job — registers ccs.peers via PUT /_cluster/settings
│   ├── vault-secret-elastic-user.yaml   # Per-site VaultStaticSecret for elastic password
│   ├── vault-secret-transport-ca.yaml   # Shared CA VaultStaticSecret — gated on manageCASecret
│   └── vault-secret-es-minio.yaml       # MinIO snapshot creds VaultStaticSecret
└── kind/                                # Local kind setup that exercises this chart
    ├── README.md                        # End-to-end bring-up instructions
    ├── setup.sh                         # Self-contained installer
    ├── register-remotes.sh              # The one non-Helm step (Step D above)
    ├── kind-config.yaml
    ├── teardown.sh
    ├── charts/                          # Vendored upstream chart .tgz files
    ├── manifests/                       # Helm values for upstream charts + namespace + VaultAuth
    └── values/
        ├── site1-kind.yaml              # Phase 1 same-namespace values for site1
        └── site2-kind.yaml              # Phase 1 same-namespace values for site2
```

---

## 8. References

- `docs/superpowers/specs/2026-05-04-elasticsearch-ccs-mesh-design.md` —
  full architecture, licensing audit, and Phase 2 prod design
- `docs/superpowers/specs/2026-05-04-ccs-helm-setup-guide.md` — narrative
  walkthrough that mirrors this README (different angle / less diagrams)
- `charts/elasticsearch/kind/README.md` — local-kind-specific bring-up
- https://www.elastic.co/guide/en/elasticsearch/reference/current/remote-clusters-cert.html
  — TLS-cert-based CCS reference docs
