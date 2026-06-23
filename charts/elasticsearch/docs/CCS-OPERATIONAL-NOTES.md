# Elasticsearch CCS — operational notes & design decisions

Field notes from end-to-end verification of cross-cluster search (CCS) over the
Basic-tier shared-CA model, on the two-kind-cluster harness
(`kind-multi/setup-multi.sh`). Captures *why* the chart is shaped the way it is,
the failure modes we hit, and how to debug them on a real cluster (e.g. f18).

---

## 1. How transport trust actually works (the mental model)

CCS node-to-node traffic is mutual-TLS on the transport port (9300). For two
separate clusters to trust each other on **Basic tier**, there is exactly one
supported mechanism: **both clusters issue their node certs from the *same* CA.**

- You provide that CA to ECK via `spec.transport.tls.certificate.secretName`
  → a `kubernetes.io/tls` Secret containing **`tls.crt` AND `tls.key`**.
- **The ECK *operator* (not the pod) consumes it.** It uses your CA to:
  1. **sign** every node's leaf transport cert (→ `issuer=CN=<your CA>`), and
  2. **publish** that CA into the remote-peer truststore at
     `…/config/transport-remote-certs/ca.crt`.
- Both happen automatically from that one field. There is **no** separate
  "mount the CA chain for peer trust" step, and ECK has **no** field to add
  extra trusted transport CAs while keeping self-signed issuance.

Because every cluster pulls the **same CA material** from the **same Vault path**
(`vault.paths.transportCA`, default `elasticsearch/transport-ca`), all node certs
chain to one root → mutual trust is automatic.

### The #1 misconception
> "Each cluster signs with its own self-signed ECK CA; the Vault CA is only
> trusted for verification, not used for signing — so they can't verify each
> other."

**False when the CA is wired correctly.** Verified cryptographically on the
harness: both clusters' leaf certs had `issuer=CN=chat-transport-ca`, all four CA
fingerprints (each cluster's Vault secret + each cluster's remote-truststore)
were byte-identical, and `openssl verify` of each peer's leaf against the other's
trusted CA returned `OK`. Cross-cluster search returned documents both ways.

That misconception **does** describe the *broken* state — but the cause is "ECK
never received the CA, so it fell back to self-signing
(`CN=elastic-<cluster>-transport`)", and the fix is "wire the CA", **not**
"cross-trust two CAs."

### One-command diagnosis on any cluster
```bash
kubectl get es <name> -n <ns> -o jsonpath='{.spec.transport.tls.certificate.secretName}{"\n"}'
kubectl get secret <name>-es-transport-certs-public -n <ns> \
  -o jsonpath='{.data.ca\.crt}' | base64 -d | openssl x509 -noout -issuer
```
- `issuer=CN=<your Vault CA>` → shared CA in use; trust is fine, look downstream.
- `issuer=CN=elastic-<cluster>-transport` → ECK self-signed; the CA isn't wired.

Self-signed fallback happens when any of these is true: the CA Secret is missing
on that cluster, it lacks `tls.key` (cert-only → ECK can't sign), or the ES CR
doesn't reference it.

---

## 2. Design decisions (and why we removed `manageCASecret`)

The original chart shared **one fixed Secret name** (`chat-transport-ca`) across
releases and used `ccs.transport.manageCASecret` to pick which release created
it (to avoid a Helm "already exists" collision in the same-namespace topology).

That was a footgun: in cross-K8s prod (one cluster per namespace), every cluster
**must** own its own copy (K8s Secrets don't cross clusters), so the field had to
be `true` everywhere — and a single stray `false` silently starved a cluster of
its CA → self-signed fallback → no trust.

**Changes made:**

| Knob | Before | After |
|---|---|---|
| CA Secret name | hand-set `caSecretName`, shared | auto: `<cluster.name>-<division>-transport-ca` (unique per cluster) |
| `manageCASecret` | required, easy to get wrong | **removed** — the Secret is always created |
| DestinationRule | wildcard `*.<ns>.svc`, namespace singleton, gated on `manageCASecret` | per-release rules scoped to the `es-http`, `es-transport` (+ kibana) Service hosts |

The Secret **name** is a purely local pointer; only the **content** (the CA
bytes, from the shared Vault path) must match across clusters. Unique per-cluster
names mean two releases in one namespace never collide, so the
"who-owns-the-singleton" gate is unnecessary. Likewise the DestinationRule is now
host-scoped and per-release, removing the last namespace singleton.

---

## 3. What the DestinationRule is for (TWO hosts — es-http AND es-transport)

The `*-passthrough` DestinationRules set `trafficPolicy.tls.mode: DISABLE` so the
ingress gateway does **not** originate Istio mTLS to the ES backend Services.
Because 9200/9300 are excluded from the sidecar, ES speaks its own native TLS; if
the gateway wrapped a connection in mTLS the handshake aborts with a TCP RST
(ECONNRESET / errno 104).

Two hosts need a rule, for two distinct gateway paths:

| DR host | Path it protects |
|---|---|
| `<es>-es-http`      | client access: `es-<site>.<domain>` → gateway → es-http:9200 |
| `<es>-es-transport` | **inbound CCS**: `es-remote-<site>.<domain>` → gateway → es-transport:9300 |

**The transport rule IS required for CCS** — verified the hard way: with only the
es-http rule, a peer's CCS handshake to this cluster RSTs (`write:errno=104`, no
server cert returned) and the remote stays disconnected. Adding the es-transport
DISABLE rule made the probe return the peer node cert (`issuer=CN=chat-transport-ca`)
and CCS go `connected:true` immediately.

> Earlier testing *seemed* to show the DR wasn't needed for CCS — that was a
> false-negative from an already-established connection being reused. A truly
> fresh handshake (post-teardown) exposes the RST. The original chart's wildcard
> `*.<ns>.svc` host happened to cover both services; when splitting to per-host
> rules, you must render **both** es-http and es-transport (the chart now does,
> gated on `ccs.transport.enabled` for the transport one).

---

## 4. The sidecar exclude annotations — needed or not?

```yaml
traffic.sidecar.istio.io/excludeInboundPorts: "9200,9300"
traffic.sidecar.istio.io/excludeOutboundPorts: "9300"
```

**Empirically, CCS works WITHOUT them when the mesh is PERMISSIVE** (no
`PeerAuthentication`, no mesh-wide mTLS). We stripped both off a cluster, let the
sidecar intercept 9200/9300, and es-http access + inbound CCS + outbound CCS +
cross-cluster search all still worked. This is why a default Istio install "just
works" without them.

**Keep them anyway.** Reasons, in priority order:

1. **STRICT mTLS is a one-line outage.** The moment anyone applies
   `PeerAuthentication{mode: STRICT}` (a standard hardening step), the sidecar
   *requires* Istio mTLS inbound on 9200/9300. ES speaks native TLS, not Istio
   mTLS → rejected → normal access **and** CCS break cluster-wide. The excludes
   make ES immune to mesh policy.
2. **Multi-node intra-cluster transport.** Without excluding 9300, node-to-node
   transport becomes a candidate for Istio auto-mTLS, which can wrap/terminate
   the connection in a way ES's transport layer doesn't expect — a failure mode
   that won't show on a single-node test but can bite a real multi-node cluster,
   especially during rolling restarts/scale-up.
3. **Performance.** Transport (9300) is high-throughput binary node-to-node
   traffic; routing it through Envoy adds a hop + protocol-sniffing on the
   hottest path.

> If your cluster runs fine today without the excludes, it's because the mesh
> happens to be PERMISSIVE — a fragile dependency, not a safe default.

---

## 5. Registration Job — password race (fixed)

The post-install Job (`job-register-remotes.yaml`) waits for ES health then PUTs
`cluster.remote.*`. It previously read `ELASTIC_PASSWORD` via env `secretKeyRef`,
which **freezes the value at pod start**. ECK first generates a random password
for `<es>-es-elastic-user`, then VSO overwrites it with the Vault password. If
the Job started inside that window it captured the stale random value → `401`
forever → health check hangs → Helm post-install hook times out → `set -e`
aborts the whole install (and, in the harness, skips the CoreDNS step). The race
made runs nondeterministic.

**Fix:** the Job now mounts `<es>-es-elastic-user` as a file and re-reads the
password on every call (`es_curl`). A mounted Secret volume updates in place, so
even if the password rotates mid-wait the Job converges instead of hanging.

---

## 6. Why a remote can show `connected: false` even when everything is correct

Proxy-mode remote connections are **lazy** (sockets opened on demand) and the
JVM **caches negative DNS lookups**. If ES tries to resolve a peer before DNS /
the gateway exists, it caches the failure and backs off, so the remote stays
disconnected after the path becomes healthy. (The harness now patches CoreDNS
*before* the chart install to avoid this; in prod, real DNS should exist first.)

**Force a reconnect (softest → hardest):**
```bash
# 1. trigger + show real status/error
curl -k -u elastic:<pw> "$ES/_resolve/cluster/<peer>:*?pretty"

# 2. bounce the remote setting (re-resolves DNS, bypasses backoff) — most reliable
curl -k -u elastic:<pw> -X PUT "$ES/_cluster/settings" -H content-type:application/json \
  -d '{"persistent":{"cluster.remote.<peer>.mode":null,"cluster.remote.<peer>.proxy_address":null,"cluster.remote.<peer>.server_name":null}}'
# ...then re-PUT the proxy/proxy_address/server_name (same body the Job sends)

# 3. restart the ES pod — ONLY guaranteed JVM-DNS-cache clear
kubectl delete pod <es>-es-all-a-0 -n <ns>     # multi-node: bump a podTemplate
                                               # annotation so ECK rolls it safely
```

**Important:** a pod restart re-mounts whatever certs the *operator already
produced* — it does **not** re-sign. If the CA is wrong (self-signed), restarting
returns the same wrong cert. Fix the Secret/CR first so the operator re-signs,
*then* restart. Restart fixes "won't reconnect", never "wrong CA".

For multi-node, prefer the ECK-native rolling restart (a changing annotation in
`spec.nodeSets[].podTemplate.metadata.annotations`, e.g. a timestamp) over
`kubectl rollout restart statefulset` — ECK owns the STS and reverts the latter.
