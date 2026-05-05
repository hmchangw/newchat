# Multi-cluster CCS — novice guide

**Date:** 2026-05-05
**Audience:** Engineers new to Kubernetes / Istio / Vault who need to deploy
the chat platform's Elasticsearch CCS mesh across many K8s clusters (Phase
2 prod). Walks through every concept and decision so nothing is "magic".
**Companion docs:** the design (`2026-05-04-elasticsearch-ccs-mesh-design.md`),
the same-namespace setup guide (`2026-05-04-ccs-helm-setup-guide.md`), and
the verified kind harness (`charts/elasticsearch/kind/`).

---

## 1. The mental model — what each piece IS

Before any setup, a single-line analogy for every term that shows up:

| Term | One-line analogy | What it actually is |
|---|---|---|
| **Kubernetes cluster** | An "island" running container workloads | One control plane + worker nodes. Pods can talk freely INSIDE the island; outside has to go through a gate. |
| **Namespace** | A "folder" inside an island | Just a label scoping resources. We use `chat`. |
| **Pod** | A running program | One or more containers sharing a network identity. ES is a pod, Kibana is a pod, the Istio gateway is a pod. |
| **Service** | An internal phone book entry | Stable DNS name + virtual IP that load-balances to one or more pods. `es-chat-site1-es-http:9200` resolves to the ES pods. |
| **Istio gateway pod** | The "front door" of the island | An Envoy proxy listening on public ports (80/443), accepting external traffic. |
| **Gateway CR** | The doorman's "rules sheet" | YAML: "open port 443, mode = TLS PASSTHROUGH, accept traffic for these hostnames". |
| **VirtualService CR** | "Once inside, walk to room X" | YAML: "for SNI=es-site1.chat.com, route to Service `es-chat-site1-es-http:9200`". |
| **DestinationRule CR** | "Outbound TLS rules at the door" | YAML: "when the gateway forwards traffic to backend X, do/don't wrap it in mTLS". |
| **AuthorizationPolicy CR** | "Security guard checking IDs" | YAML allow/deny based on source IP, dest port, etc. Enforced by Envoy sidecars on the receiving pods. |
| **CA cert + key** | A "rubber stamp + ink pad" | The shared secret that lets each cluster stamp valid certs. Anyone holding the public CA cert can verify those stamps. |
| **TLS** | A "sealed envelope" | Only the sender and the named recipient can read the contents. Authentication happens via the cert chain. |
| **TLS PASSTHROUGH at a gateway** | "Forward sealed envelope without opening it" | The gateway is L4 / TCP-only — it forwards bytes based on the SNI hostname (visible in TLS handshake) but doesn't open the envelope. |
| **SNI** | "Address on the outside of the envelope" | Server Name Indication — the hostname the client says it wants, sent in cleartext during TLS handshake before encryption begins. |

---

## 2. The shape of the multi-cluster CCS mesh

You have **12 separate Kubernetes clusters**. Each:

- Lives in its own region / data centre
- Has its own Istio install, its own Vault, its own ECK operator
- Runs ONE Elasticsearch cluster (3 master + 1 data + 3 coord nodes)

Goal: a search request issued on any cluster's ES can ALSO query indices on
the other 11 clusters' ES — `messages-*,*:messages-*/_search` returns merged
results from all 12 sites.

Two things must be true for that to work:

1. **Trust** — when ES on cluster 1 opens a TCP connection to ES on cluster
   2's transport port, ES-1 has to validate ES-2's TLS cert. This requires
   that ES-1's truststore contains the CA that signed ES-2's cert. For 12
   mutually-trusting clusters, the simplest answer is: **all 12 use the
   same CA**. (One shared rubber stamp.)
2. **Auth** — once trust is established, ES-2 still needs to authenticate
   the user the search is being run as. Cert-based CCS forwards the calling
   user's credentials; both sides must have a user with the same
   credentials. We use `elastic` with the same password everywhere.

That's the whole substrate. Everything below is plumbing to deliver those
two ingredients to all 12 clusters.

---

## 3. Per-cluster Vault — your sync mechanism

You said each K8s cluster has its OWN Vault. Good — that means no central
Vault, no replication, no etcd-cross-region weirdness. Your job is just to
put the SAME content into every Vault at the SAME path.

### The shared secrets every Vault must have

Three Vault paths, identical content across all 12 clusters:

| Vault path | Content | Why |
|---|---|---|
| `secret/elasticsearch/transport-ca` | `tls.crt` (PEM) + `tls.key` (PEM) | The shared CA. ECK will sign each node's transport cert from it. |
| `secret/elasticsearch/elastic-user` | `elastic` (the password) | The shared `elastic` password. Cert-based CCS forwards this to remotes. |
| `secret/elasticsearch/site<N>/minio` | `MINIO_BUCKET_ACCESS_KEY` + `_SECRET_KEY` | Per-site (different per cluster) MinIO snapshot creds. Chart references them; ECK loads into keystore but ES never reads them unless you wire snapshots. |

### How you actually do the placement

```bash
# ─── ONE TIME, ON YOUR LAPTOP ─────────────────────────────────────────────
# Generate the shared CA. Keep these PEM files safe.
openssl req -x509 -nodes -newkey rsa:2048 -days 36500 \
  -subj "/CN=chat-transport-ca" \
  -keyout shared-transport-ca.key \
  -out shared-transport-ca.crt
# 100-year validity = effectively never expires (see design doc §5.3)

# Pick a single shared elastic password
ELASTIC_PW='<choose-one-strong-password>'
```

```bash
# ─── PER CLUSTER (REPEAT FOR ALL 12) ──────────────────────────────────────
# Point the vault CLI at THIS cluster's Vault:
export VAULT_ADDR=https://vault.cluster1.internal
export VAULT_TOKEN=<your-admin-token>

# Push the shared CA — same exact content for every cluster
vault kv put secret/elasticsearch/transport-ca \
  "tls.crt=$(cat shared-transport-ca.crt)" \
  "tls.key=$(cat shared-transport-ca.key)"

# Push the shared elastic password — same value for every cluster
vault kv put secret/elasticsearch/elastic-user \
  elastic="${ELASTIC_PW}"

# Per-site MinIO creds (these CAN differ per cluster — typically real per-region creds)
vault kv put secret/elasticsearch/site1/minio \
  MINIO_BUCKET_ACCESS_KEY=<key1> MINIO_BUCKET_SECRET_KEY=<secret1>
```

Then `export VAULT_ADDR=...cluster2...` and run the same three commands
with the right `siteN` for the minio path. Twelve times total.

**You ARE the sync mechanism. That's it.**

### Each cluster also needs VSO + a VaultAuth resource

The chart's `VaultStaticSecret` resources reference a `VaultAuth` named
`chat-vault-auth` in the `chat` namespace. That object tells the Vault
Secrets Operator how to authenticate to Vault. It's installed once per
cluster, manually — and must use that exact name (or override
`vault.vaultAuthRef` in the chart values).

A typical `VaultAuth` for kubernetes auth method:

```yaml
apiVersion: secrets.hashicorp.com/v1beta1
kind: VaultAuth
metadata:
  name: chat-vault-auth
  namespace: chat
spec:
  method: kubernetes
  mount: kubernetes              # the auth path in YOUR cluster's Vault
  kubernetes:
    role: chat-app               # a Vault role that can read secret/data/elasticsearch/*
    serviceAccount: default      # the K8s SA the chat ns pods run as
    audiences: ["vault"]
```

The Vault role `chat-app` must exist in the cluster's Vault and have a
policy granting:
```hcl
path "secret/data/elasticsearch/*"     { capabilities = ["read"] }
path "secret/metadata/elasticsearch/*" { capabilities = ["read"] }
```

Your platform team probably already has VSO + a sample VaultAuth pattern.
If not, the kind setup at `charts/elasticsearch/kind/manifests/vault-auth.yaml`
is a working template.

---

## 4. DNS A records — what they are and how to set them

### What an A record is, in one sentence

A DNS "A record" is a public phone book entry: **"name X → IP address Y"**.
When anyone resolves `es-remote-site1.chat.com`, their machine asks DNS,
gets back an IP, then connects to that IP.

### Where the IP for each cluster comes from

Each K8s cluster's Istio ingress has a publicly reachable IP. How you find
it depends on your environment:

**Cloud (EKS / GKE / AKS):**
```bash
# Switch kubectl context to the target cluster, then:
kubectl -n chat get svc chat-ingressgateway \
  -o jsonpath='{.status.loadBalancer.ingress[0].ip}'

# AWS NLBs return a hostname instead of an IP:
kubectl -n chat get svc chat-ingressgateway \
  -o jsonpath='{.status.loadBalancer.ingress[0].hostname}'
```

You'll get something like `34.123.45.67` or `aabbcc.elb.amazonaws.com`.

**On-prem / bare metal:** the IP comes from your load balancer (MetalLB,
kube-vip, F5, hardware LB — same workflow). Same `kubectl get svc` shows
it.

### Setting the records

In your DNS provider (Route53, Cloudflare, GoDaddy, internal DNS — same
shape):

```
Type    Name                          Value                    TTL
A       es-remote-site1.chat.com      34.10.20.30              300
A       es-remote-site2.chat.com      34.10.20.31              300
...
A       es-remote-site12.chat.com     34.10.20.41              300

A       es-site1.chat.com             34.10.20.30              300   (same IP)
A       es-site2.chat.com             34.10.20.31              300
...

A       kibana-site1.chat.com         34.10.20.30              300   (same IP)
A       kibana-site2.chat.com         34.10.20.31              300
...
```

All three hostnames per site point at the same Istio LB IP — the gateway
distinguishes them by SNI (the hostname in the TLS handshake) and routes
each to a different backend Service via VirtualService.

### Verify before testing CCS

```bash
dig +short es-remote-site2.chat.com   # should print cluster 2's LB IP
nslookup es-remote-site2.chat.com     # alternative
```

**DNS is configured outside K8s, once. No automation needed beyond your
existing DNS workflow.**

---

## 5. Public TLS — do you need Let's Encrypt? **No.**

Two TLS layers exist and they're independent. CCS uses one of them; the
other is purely cosmetic.

### Layer A — the layer CCS uses (and why LE is irrelevant)

When ES on cluster 1 connects to `es-remote-site2.chat.com:443`:

1. ES-1 opens a TCP connection. TLS handshake begins.
2. The Istio gateway in cluster 2 sees the TLS handshake's SNI =
   `es-remote-site2.chat.com`. It matches the Gateway+VirtualService rule.
3. **TLS PASSTHROUGH means the gateway forwards the TLS handshake bytes
   verbatim** to the backend ES pod. It never decrypts.
4. The backend ES pod completes the handshake — presenting its node
   transport cert, signed by **the shared CA**.
5. ES-1 validates that cert against ITS truststore — which contains the
   same shared CA. Validation succeeds. Encrypted channel established.

Notice: at no point does this care about Let's Encrypt or any "public" CA.
The cert is signed by your self-managed shared CA. The CA is delivered to
every cluster through Vault. That's all.

**LE certs would not help here** — adding an LE cert would only matter if
the gateway were terminating TLS, which it isn't.

### Layer B — purely cosmetic for browsers / curl on Kibana / ES HTTP

When you, a human, hit `https://kibana-site1.chat.com/` in a browser:

- Same gateway, same TLS PASSTHROUGH
- The cert presented is ECK's auto-generated `<es>-es-http-certs-public`,
  which is self-signed
- Browsers warn "not secure"
- Workarounds:
  - `curl -k` to ignore the warning
  - In a browser, click "advanced → accept risk"
  - **OR** put a Let's Encrypt cert on the gateway by switching from
    PASSTHROUGH to SIMPLE/TERMINATE mode and adding cert-manager — purely
    a UX improvement for browser users

**For the CCS mesh itself, you don't need LE. Skip cert-manager.** The
chart works as-is.

---

## 6. AuthorizationPolicy and source IPs in cross-cluster traffic

This is the bit that confused you. Short answer: **the existing policy
needs no special configuration for cross-cluster CCS** because of how Istio
TLS PASSTHROUGH works.

### Trace a cross-cluster CCS request through cluster 2's policy enforcement

Step by step, when ES on cluster 1 queries ES on cluster 2:

1. ES-1 dials `es-remote-site2.chat.com:443` on the public internet.
2. Hits cluster 2's Istio LB IP. The Istio gateway pod accepts the TCP
   connection, reads the SNI, checks Gateway + VirtualService rules,
   matches them.
3. The gateway pod opens **a NEW TCP connection within cluster 2's pod
   network** to the backend Service `es-chat-site2-es-transport:9300`.
   The new connection's source IP is **the gateway pod's own IP**.
4. Backend Service routes to an ES pod. ES pod's sidecar / AuthorizationPolicy
   sees the source IP = the gateway pod's IP, which is in cluster 2's pod
   CIDR (e.g. `10.244.0.x/16`).
5. The chart's AuthorizationPolicy allows source CIDRs `10.0.0.0/8`,
   `172.16.0.0/12`, `192.168.0.0/16` — gateway pod IP falls inside one of
   these → ALLOW.
6. ES pod accepts the TCP connection. The TLS handshake (which has been
   passing through transparently) completes between ES-1 and the ES-2 pod.
   Mutual transport TLS authenticated via shared CA.

**Key insight: the original requester's external IP is HIDDEN from the
receiving ES pod.** The pod only sees the local gateway pod as the source.
So the AuthorizationPolicy doesn't need to know about peer clusters' egress
IPs at all — it was always going to see local-pod-CIDR sources.

### What the policy is actually buying you

It does NOT distinguish "request came from a peer ES cluster" vs "request
came from a random pod in cluster 2". The real security boundary for cross-
cluster CCS is:

- **The Istio gateway only accepts connections matching a Gateway+VirtualService
  rule** — i.e. the right SNI hostname. Random scanners hitting the LB
  without the right SNI get dropped at the gateway with no further traversal.
- **The TLS handshake requires a cert signed by the shared CA** — which
  only your 12 clusters have.

The pod-level AuthorizationPolicy is defense-in-depth, blocking unintended
in-cluster pods from speaking to ES. It needs no cross-cluster awareness.

---

## 7. The full step-by-step proceed plan

You've now seen every concept. Here's the actual ordered to-do list to
bring up the 12-cluster mesh.

### Once on your laptop (5 minutes)

```bash
openssl req -x509 -nodes -newkey rsa:2048 -days 36500 \
  -subj "/CN=chat-transport-ca" \
  -keyout shared-transport-ca.key \
  -out shared-transport-ca.crt

ELASTIC_PW='<choose-one-strong-password>'
```

Save those two files somewhere safe. They're sensitive.

### Per cluster (×12, do each one independently)

For each cluster, switch your `vault` CLI + `kubectl` context to that
cluster, then:

#### Step A — Seed Vault (3 commands)

```bash
vault kv put secret/elasticsearch/transport-ca \
  "tls.crt=$(cat shared-transport-ca.crt)" \
  "tls.key=$(cat shared-transport-ca.key)"

vault kv put secret/elasticsearch/elastic-user \
  elastic="${ELASTIC_PW}"

vault kv put secret/elasticsearch/site<N>/minio \
  MINIO_BUCKET_ACCESS_KEY=<key> MINIO_BUCKET_SECRET_KEY=<secret>
```

#### Step B — Confirm the cluster has VSO + VaultAuth ready

```bash
kubectl -n chat get vaultauth chat-vault-auth
# → should exist
kubectl -n chat get pods -l app.kubernetes.io/name=vault-secrets-operator -A
# → at least one Running pod somewhere in the cluster
```

If not, work with your platform team to install VSO and create
`chat-vault-auth`. The kind setup script's `manifests/vault-auth.yaml` is a
working template.

#### Step C — Helm install the chart

A site values file per cluster, e.g. `values/site<N>.yaml`:

```yaml
properties:
  site: site<N>
  stage: PROD
  division: site<N>
  publicDomain: chat.com

ccs:
  enabled: true
  transport:
    enabled: true
    caSecretName: chat-transport-ca
    manageCASecret: true        # ← always true in multi-K8s mode (each cluster owns its own copy)
  publicEndpoint:
    enabled: true               # ← exposes es-remote-site<N>.chat.com via Istio passthrough

cluster:
  istioConfig:
    sidecar.istio.io/inject: "true"
    traffic.sidecar.istio.io/excludeInboundPorts: "9200,9300"
    traffic.sidecar.istio.io/excludeOutboundPorts: "9300"
```

```bash
helm upgrade --install es-chat-site<N> ./charts/elasticsearch \
  -n chat --force-conflicts \
  -f values/site<N>.yaml
```

#### Step D — Configure DNS (outside K8s, in your DNS provider)

Get the Istio LB IP of THIS cluster:

```bash
kubectl -n chat get svc chat-ingressgateway -o jsonpath='{.status.loadBalancer.ingress[0].ip}'
```

Add to DNS:

```
A   es-remote-site<N>.chat.com   <that IP>
A   es-site<N>.chat.com          <that IP>
A   kibana-site<N>.chat.com      <that IP>
```

#### Step E — Wait for ES to reach green

```bash
kubectl -n chat wait --for=jsonpath='{.status.health}'=green elasticsearch/es-chat-site<N> --timeout=600s
```

#### Step F — Register all 11 peers

The generalized script (`charts/elasticsearch/kind/register-remotes.sh`)
supports both modes via env vars. For multi-K8s use `MODE=public`:

```bash
MODE=public \
LOCAL_SITE=site<N> \
PEERS=site1,site2,...,site12   # all peers EXCEPT site<N> itself \
PUBLIC_DOMAIN=chat.com \
ELASTIC_PW="${ELASTIC_PW}" \
./charts/elasticsearch/kind/register-remotes.sh
```

This issues 11 `PUT /_cluster/settings` calls on this cluster, one per
peer, with `proxy_address: es-remote-<peer>.chat.com:443` and matching
`server_name`. Idempotent — safe to re-run.

### After all 12 clusters are done

Verify on any one cluster:

```bash
curl -k -u elastic:"${ELASTIC_PW}" https://es-site1.chat.com/_remote/info | jq .
# → 11 entries, every one connected: true, mode: proxy

curl -k -u elastic:"${ELASTIC_PW}" \
  'https://es-site1.chat.com/messages-*,*:messages-*/_search?pretty'
# → hits from every site in the mesh
```

---

## 8. Common questions and traps

### "Do all 12 clusters need to be brought up at the same time?"

No. Each cluster is independent. You can bring up site1 today, site2 next
week, etc. After each new cluster goes live and gets its DNS record, you
re-run the register-remotes script on every existing cluster (with the
expanded peer list) so they pick up the new peer.

### "What happens if I change the elastic password later?"

Update the Vault path on every cluster (same new value). VSO will sync to
the K8s Secret. ECK rolls every cluster. CCS keeps working as long as the
new password is identical everywhere.

### "What if one cluster is down?"

`skip_unavailable: true` is set on every remote — a CCS query against `*:`
returns partial results from healthy peers and skips the down one.

### "How do I add a 13th site?"

1. Generate Vault paths in cluster 13's Vault with the SAME CA + elastic
   password (the shared CA from your laptop).
2. Helm install on cluster 13 (steps B + C + D + E above).
3. Re-run register-remotes on every existing cluster, adding `site13` to
   the PEERS env var (`PEERS=site1,...,site12,site13` minus self).
4. Done. The new peer shows up in `_remote/info` on all sites.

### "How do I remove a site?"

On every other cluster, run `PUT /_cluster/settings` with
`cluster.remote.es-chat-siteN: null` to deregister. Then decommission the
cluster.

### "I don't see the cross-cluster query working but `_remote/info` says connected. Why?"

Most likely: elastic passwords differ between clusters. Run on the
TARGET cluster:
```bash
curl -k -u elastic:<the-password-you-think-target-uses> \
  https://es-site<target>.chat.com/_security/_authenticate
```
If it returns 401, the target's password is different. Reset to the shared
value via Vault.

### "I get RST after TLS handshake when curling the gateway URL"

Verify the chart's `DestinationRule chat-es-passthrough` exists in the
namespace (`kubectl -n chat get destinationrule`). Without `tls.mode:
DISABLE`, the gateway tries to mTLS-wrap the upstream connection and the
ES pod (with port 9200 sidecar-excluded) can't speak mTLS. The chart's DR
is gated on `ccs.transport.manageCASecret: true` — confirm that's set.

### "The ECK operator logs say 'Remote cluster is an enterprise feature. Enterprise features are disabled'"

You can ignore this. The chart no longer uses `spec.remoteClusters[]` —
that's the path that needs Enterprise. Cert-based CCS works through
`PUT /_cluster/settings` and ECK doesn't gate that.

### "Do I need to mount any peer CA bundles into ES?"

No. Because every cluster signs from the SAME CA, `xpack.security.transport.ssl.certificate_authorities`
already trusts every peer (since the local CA == the peer's signing CA).
ECK auto-configures this when you set `spec.transport.tls.certificate.secretName`
in the chart.

---

## 9. Reference summary

| Question | Answer |
|---|---|
| How is trust distributed? | Generate one CA on your laptop. `vault kv put` the same `tls.crt` + `tls.key` into every cluster's Vault. VSO syncs to K8s Secret. ECK signs node certs from it. |
| How is auth distributed? | Same shared `elastic` password in every cluster's Vault path `secret/elasticsearch/elastic-user`. CCS forwards the calling user. |
| How does cross-cluster traffic reach the right pod? | DNS → Istio LB → Gateway pod (matches SNI) → VirtualService (TLS PASSTHROUGH route to internal Service:9300) → ES pod. Sidecar excluded for 9300. |
| Why no Let's Encrypt? | TLS PASSTHROUGH means the gateway never terminates. The cert seen by ES is the shared-CA-signed transport cert. LE only matters for browser UX on Kibana, which you can choose to add later. |
| What's the AuthorizationPolicy doing? | Allowing local pod CIDR sources to reach ES on 9200/9300. Cross-cluster traffic looks like local-gateway-pod traffic from the receiver's perspective (because of L4 passthrough). No special cross-cluster config needed. |
| What's the registration command? | `MODE=public LOCAL_SITE=site<N> PEERS=site1,...,site12 ./register-remotes.sh` — once per cluster, idempotent. |
| What's automated vs manual? | Helm chart installs ES + Kibana + Istio CRs + AuthZ + DR + VaultStaticSecrets. You manually: seed Vault paths (per-cluster), set DNS A records, run register-remotes. |

---

## 10. References

- `docs/superpowers/specs/2026-05-04-elasticsearch-ccs-mesh-design.md` —
  the full architectural design with licensing audit
- `docs/superpowers/specs/2026-05-04-ccs-helm-setup-guide.md` — narrative
  walkthrough of the same-namespace flow
- `charts/elasticsearch/CCS-SETUP-README.md` — the chart's top-level README
  with the FigJam architecture diagram
- `charts/elasticsearch/kind/` — the verified end-to-end same-namespace
  test harness (a reduced scale-model of what Phase 2 does)
- `charts/elasticsearch/kind/register-remotes.sh` — the generalized
  registration script (supports `internal` and `public` modes)
