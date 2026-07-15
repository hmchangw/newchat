# Elasticsearch CCS — hands-on debug runbook

A field runbook for diagnosing a **cross-cluster search (CCS) that won't connect**
(the classic symptom: `GET /_cluster/health` works in Kibana, but anything that
touches a remote — `GET /_remote/info` — hangs and Kibana returns
`502 client request timeout`).

Every error string and log line below was **captured live** from the two-kind
harness (`kind-multi/setup-multi.sh`) by deliberately breaking one thing at a
time. This is the companion to `CCS-OPERATIONAL-NOTES.md` (which explains *why*
the chart is shaped this way); this doc is the *what-do-I-run-when-it's-down*.

---

## 0. TL;DR — the 3 commands that localize any CCS failure

Run these in order. Stop at the first that fails.

```bash
NS=chat                       # your namespace (staging: wsp)
ES=es-chat-site1              # your Elasticsearch resource name (before "-es-http")
PEER=es-chat-site2            # the remote cluster key (from _cluster/settings)
PW=<elastic-password>

# a curl-capable pod that can reach the es-http Service (the ingressgateway has curl)
GW="kubectl -n $NS exec deploy/chat-ingressgateway --"
ESURL="https://$ES-es-http.$NS.svc.cluster.local:9200"

# 1) THE diagnostic — reports connected + the real error, does NOT hang like _remote/info
$GW curl -k -sS -u elastic:$PW "$ESURL/_resolve/cluster/$PEER:*"

# 2) If connected:false and you need the exception text, force a search (surfaces the reason)
$GW curl -k -sS -u elastic:$PW "$ESURL/$PEER:any-index/_search"

# 3) The trust proof — run on BOTH clusters, issuer + fingerprint MUST match
kubectl -n $NS get secret $ES-es-transport-certs-public \
  -o jsonpath='{.data.ca\.crt}' | base64 -d | openssl x509 -noout -issuer -fingerprint -sha256
```

> **Never poll `GET /_remote/info` to debug** — it blocks on the broken remote
> and is what makes Kibana 502. `_resolve/cluster` returns a verdict instead.

---

## 1. Why CCS needs all these moving parts (one paragraph)

Two Elasticsearch clusters in **separate Kubernetes clusters** must do
**mutual-TLS on the transport port 9300** to search each other. But k8s Services
don't cross clusters, and ES's transport TLS must survive Istio untouched. That
single requirement forces every piece:

| Piece | Why it exists | Breaks as… |
|---|---|---|
| **Gateway** (`chat-es-remote-<site>-gateway`) | opens 443 on the shared ingressgateway for SNI `es-remote-<site>`, **PASSTHROUGH** (don't decrypt ES's own TLS) | handshake closed |
| **VirtualService** (`chat-es-remote-<site>-vs`) | routes that SNI's bytes to `<es>-es-transport:9300` | handshake closed |
| **DestinationRule** (`<es>-es-transport-passthrough`, `tls.mode: DISABLE`) | stops the gateway Envoy from **mTLS-wrapping** the backend (ES already speaks native TLS) | handshake closed (RST) |
| **sidecar excludeInbound/Outbound 9200,9300** | makes the ES pod's own sidecar transparent so ES terminates its own TLS; also makes ES **immune to STRICT mTLS / authz** | (see §4 — it's a *defense*, not a failure) |
| **AuthorizationPolicy** | L4 allow-list on ES pods; workload-scoped so it doesn't block the gateway | (neutralized on 9300 by the excludes) |
| **Shared transport CA** (Vault `elasticsearch/transport-ca`) | every cluster's node certs chain to one root → mutual trust; ECK signs + publishes it automatically | PKIX / cert not trusted |
| **Shared `elastic` password** | CCS forwards the caller's creds; the remote must authenticate them | 401 |
| **register-remotes Job** | PUTs `cluster.remote.<peer>.{mode:proxy, proxy_address, server_name}` | wrong address / SNI |

Packet path for one cross-cluster query:
```
your ES coord → (proxy_address) es-remote-<peer>.chat.com:443, SNI=es-remote-<peer>
  → peer ingressgateway:443  [Gateway: SNI match, PASSTHROUGH]
  → [VirtualService: route to <es>-es-transport:9300]
  → [DestinationRule: tls DISABLE, no mTLS wrap]
  → peer ES :9300  [sidecar excluded → ES terminates native transport TLS, shared CA]
  → mutual trust → search runs
```

---

## 2. Failure signature table (all rows captured live)

Match the **`_resolve` / search reason** you get in §0 step 1–2 against this table.

| Real error you see | Real ES log signature (`log.level: WARN`) | Root cause | Fix / where to look |
|---|---|---|---|
| `unknown host [es-remote-<peer>...]` | `RemoteClusterService: failed to update remote cluster connection` → `IllegalArgumentException: unknown host` | **DNS** — the peer hostname doesn't resolve *inside this cluster* | CoreDNS / external-DNS must resolve `es-remote-<peer>` to the peer's ingressgateway LB. (JVM caches negative DNS — see §5.) |
| `Unable to open any proxy connections to cluster [X] at address [IP:port]` + `[…][IP:port] connect_exception` | `ProxyConnectionStrategy: failed to open any proxy connections` → `ConnectTransportException: connect_exception`; then `NoSeedNodeLeftException` | **TCP can't reach the gateway** — wrong port, LB not reachable across clusters, firewall/NetworkPolicy dropping 443; **or** `server_name`/SNI matches no Gateway so the passthrough refuses | `nc -zv <host> <port>` from an ES pod; verify the peer LB EXTERNAL-IP + cross-cluster firewall; verify `server_name` matches a real Gateway host |
| `Connection closed while SSL/TLS handshake was in progress` | client: handshake closed mid-flight | **TCP reached the gateway but the handshake was killed.** THREE causes, indistinguishable from the client (see §3): missing **es-transport DestinationRule** (Envoy mTLS-wraps → RST), missing **Gateway**, or missing **VirtualService** | check the peer: `kubectl -n <ns> get gateway,virtualservice,destinationrule \| grep remote` — whichever is missing is your cause |
| `PKIX path building failed: … unable to find valid certification path` / `(certificate_unknown)` **plus** `DiagnosticTrustManager: … certificate is issued by [CN=…] … is not trusted in this ssl context` | full stack incl. `SSLHandshakeException`, `ValidatorException` | **Trust** — the peer's transport cert is **not signed by the shared CA** (CA mismatch, or ECK self-signed fallback because it never got the CA) | the §0 step-3 issuer/fingerprint proof on both sites; `issuer=CN=elastic-<cluster>-transport` means self-signed = CA not wired |
| `security_exception` / `401` / `unable to authenticate` | `authentication` failures | **Password mismatch** — sites don't share the `elastic` password | both sites must pull the same Vault `elasticsearch/elastic-user` |
| `connected:true` but `no such index [idx]` | none | **Not a CCS problem** — the link is healthy, the index just doesn't exist | you're done; CCS works |

### The `DiagnosticTrustManager` line is a gift
On a trust failure ES logs the *exact* cert the peer served and the *exact*
truststores it checked against, e.g. (captured live):
```
failed to establish trust with server …; the server provided a certificate with
subject name [CN=es-chat-site2-es-http.chat.es.local] … issued by
[CN=es-chat-site2-http] … which is self-issued; the [CN=es-chat-site2-http]
certificate is not trusted in this ssl context ([xpack.security.transport.ssl
(with trust configuration: PEM-trust{…/transport-certs/ca.crt,
…/transport-remote-certs/ca.crt})])
```
Read it literally: *"peer served a cert issued by X; X is not in my transport
truststore."* If issuer is `es-chat-<peer>-http` you accidentally pointed the
remote at the **HTTP** endpoint (9200), not transport. If issuer is
`elastic-<peer>-transport` (self-signed), the peer never got the shared CA.

---

## 3. The look-alike trap: "Connection closed while SSL/TLS handshake was in progress"

Missing **DestinationRule**, missing **Gateway**, and missing **VirtualService**
ALL produce this identical client-side error. Do **not** guess — disambiguate on
the peer cluster:

```bash
kubectl -n <ns> get gateway        | grep es-remote     # missing → Gateway cause
kubectl -n <ns> get virtualservice | grep es-remote     # missing → VS cause
kubectl -n <ns> get destinationrule | grep es-transport # missing → DR cause (mTLS-wrap RST)
```
All three present but still failing → suspect the CA/trust row instead (§2), or a
STRICT mTLS + missing-excludes combo (§4).

---

## 4. Why STRICT PeerAuthentication / AuthorizationPolicy did NOT break CCS

Captured live: applying `PeerAuthentication{mode: STRICT}` to the namespace left
CCS **`connected:true`**. That's the chart's **defense working**: the ES pods set
```yaml
traffic.sidecar.istio.io/excludeInboundPorts: "9200,9300"
traffic.sidecar.istio.io/excludeOutboundPorts: "9300"
```
so the sidecar never intercepts 9200/9300 — mesh mTLS policy and L4 authz simply
don't apply to those ports. **Corollary:** if someone *removes* those excludes
and STRICT mTLS is on, CCS breaks cluster-wide (the sidecar then demands Istio
mTLS on 9300, which ES doesn't speak). So when debugging a STRICT-mTLS cluster,
verify the excludes are still present:
```bash
kubectl -n <ns> get pod <es>-es-<nodeset>-0 -o jsonpath='{.metadata.annotations}' | tr ',' '\n' | grep sidecar
kubectl get peerauthentication -A
```

---

## 5. Gotcha: proxy remotes are lazy + JVM caches negative DNS

Proxy-mode remote sockets open **on demand**, and the JVM **caches failed DNS
lookups**. So a remote can stay `connected:false` even after you've fixed the
path, and a settings "bounce" won't always force a fresh TCP handshake (a warm
socket gets reused — this is the "false-negative" that hides a missing DR).

Force a genuinely fresh connect (softest → hardest):
```bash
# 1. bounce the remote setting (null then re-PUT) — re-resolves DNS, clears backoff
#    (same body the register Job sends; see §6)
# 2. register under a THROWAWAY alias — gets its own fresh socket pool (see §6)
# 3. restart the ES pod — the ONLY guaranteed JVM-DNS-cache clear
#    kubectl delete pod <es>-es-<nodeset>-0 -n <ns>   (multi-node: bump a podTemplate annotation)
```
**A pod restart never fixes a wrong CA** — it re-mounts the operator's existing
cert. Fix the Secret/CR so the operator *re-signs*, then restart.

---

## 6. How to reproduce any of these on the kind-multi harness (safe)

The harness (`kind-multi/setup-multi.sh`) runs two kind clusters, `site1`/`site2`,
namespace `chat`, shared password `chat-elastic-pw`. **Key trick:** use a
**throwaway remote alias** so you test a fresh handshake against the currently
broken state *without disturbing the real `es-chat-site2` connection* — the
harness stays green throughout.

```bash
PW=chat-elastic-pw
ES1="https://es-chat-site1-es-http.chat.svc.cluster.local:9200"
X() { kubectl --context kind-chat-eck-site1 -n chat exec deploy/chat-ingressgateway -- curl -k -sS -u elastic:$PW "$@"; }

# set a throwaway remote (skip_unavailable:false so failures surface)
setrmt() { X -X PUT "$ES1/_cluster/settings" -H 'content-type: application/json' \
  -d "{\"persistent\":{\"cluster.remote.$1.mode\":\"proxy\",\"cluster.remote.$1.proxy_address\":\"$2\",\"cluster.remote.$1.server_name\":\"$3\",\"cluster.remote.$1.skip_unavailable\":false}}" >/dev/null; }
clr()   { X -X PUT "$ES1/_cluster/settings" -H 'content-type: application/json' \
  -d "{\"persistent\":{\"cluster.remote.$1.mode\":null,\"cluster.remote.$1.proxy_address\":null,\"cluster.remote.$1.server_name\":null,\"cluster.remote.$1.skip_unavailable\":null}}" >/dev/null; }
probe() { X "$ES1/_resolve/cluster/$1:*"; echo; X "$ES1/$1:idx/_search" 2>/dev/null | grep -oE '"reason":"[^"]{0,140}"' | sort -u; }
```

| To reproduce… | Break | Probe |
|---|---|---|
| DNS `unknown host` | (none) | `setrmt t es-remote-nope.chat.com:30443 es-remote-nope.chat.com; probe t; clr t` |
| `connect_exception` (unreachable) | (none) | `setrmt t es-remote-site2.chat.com:39999 es-remote-site2.chat.com; probe t; clr t` |
| wrong SNI (no gateway) | (none) | `setrmt t es-remote-site2.chat.com:30443 wrong.chat.com; probe t; clr t` |
| **CA mismatch** (PKIX) | (none — point at the HTTP cert via gateway IP) | `IP2=$(docker inspect chat-eck-site2-control-plane -f '{{(index .NetworkSettings.Networks "kind").IPAddress}}'); setrmt t $IP2:30443 es-site2.chat.com; probe t; clr t` |
| **missing DestinationRule** | `kubectl --context kind-chat-eck-site2 -n chat delete destinationrule es-chat-site2-es-transport-passthrough` (snapshot with `-o yaml` first!) | `setrmt t es-remote-site2.chat.com:30443 es-remote-site2.chat.com; probe t; clr t` → restore with `kubectl apply -f` the snapshot |
| **missing Gateway** | delete `chat-es-remote-site2-gateway` (snapshot first) | same probe; restore |
| **missing VirtualService** | delete `chat-es-remote-site2-vs` (snapshot first) | same probe; restore |
| STRICT mTLS (proves defense) | apply `PeerAuthentication{mtls.mode: STRICT}` in `chat` | probe → stays `connected:true`; delete the PeerAuthentication |

Read WARN/ERROR ES logs cleanly (skip the noisy `updating [cluster.remote…]`
INFO lines) with a tiny extractor:
```bash
kubectl --context kind-chat-eck-site1 -n chat logs es-chat-site1-es-all-a-0 -c elasticsearch --since=40s \
 | python3 -c 'import sys,json
for l in sys.stdin:
 l=l.strip()
 if not l.startswith("{"):continue
 try:e=json.loads(l)
 except:continue
 if e.get("log.level") not in("WARN","ERROR"):continue
 st=e.get("error.stack_trace","").splitlines()
 print(e["log.level"],"|",e.get("log.logger","").split(".")[-1],"|",e.get("message",""))
 if st:print("   ",st[0])' | grep -v 'updating \[cluster'
```

**Always snapshot before deleting** (`kubectl get <res> -o yaml > /tmp/x.yaml`)
and restore (`kubectl apply -f /tmp/x.yaml`). After a round, confirm green:
```bash
X "$ES1/_resolve/cluster/es-chat-site2:*" | grep -oE '"connected":(true|false)'
```

---

## 7. Real-staging checklist (public mode, separate k8s per site)

When staging hangs but dev/test worked, walk these in order:

1. `_resolve/cluster/<peer>:*` → get the verdict + error (not `_remote/info`).
2. **CA proof on both sites** (§0.3) — identical issuer + fingerprint? Most
   likely real-staging fault after "I copied the same cert" (copying to Vault ≠
   ECK consuming it — check `-es-transport-certs-public`, the operator's output).
3. From an ES pod: `getent hosts es-remote-<peer>.<domain>` (DNS) then
   `nc -zv -w5 es-remote-<peer>.<domain> 443` (network/firewall — the #1
   real-cluster cause a kind harness never shows).
4. If `handshake was in progress`: check peer `gateway,virtualservice,destinationrule | grep remote` (§3).
5. Confirm `ccs.mode: public` and `ccs.publicEndpoint.enabled: true` in the
   site's values (the shared-transport-CA wiring is always rendered when
   `ccs.enabled: true`), and that `server_name` is set on the remote (SNI +
   cert SAN both depend on it).

---

## 8. Layer-by-layer: what you see if it WORKS vs if it's BROKEN

The cross-cluster hop is four layers on port **443** (DNS → LoadBalancer → Istio
ingress → internal 9300), then TLS trust. There is **no nginx/k8s Ingress** — the
Istio Gateway+VirtualService on the ingressgateway pod *is* the ingress; a
Service of `type: LoadBalancer` (cloud LB, or NodePort on kind) exposes it.

Test top-down; stop at the first that doesn't match the ✅ column. All outputs
below are real (harness where healthy, live-break captures where failing).

### Layer 1 — DNS: `es-remote-<peer>.<domain>` → peer LB IP
```bash
kubectl -n wsp exec <es>-es-<nodeset>-0 -c elasticsearch -- getent hosts es-remote-<peer>.<domain>
```
- ✅ **works:** `172.18.0.3      es-remote-site2.chat.com`  (any IP printed)
- ❌ **broken:** *(no output, exit code 2)* → in ES logs: `IllegalArgumentException: unknown host [es-remote-<peer>...]`
- fix: publish the record to this cluster's resolver (CoreDNS/external-DNS). Then force a fresh reconnect — JVM caches the negative lookup (§5).

### Layer 2 — LoadBalancer / NodePort: TCP 443 reachable
```bash
kubectl -n wsp exec <es>-es-<nodeset>-0 -c elasticsearch -- sh -c 'nc -zv -w5 es-remote-<peer>.<domain> 443'
# also: does the LB even have an address?
kubectl -n wsp get svc | grep -i ingressgateway    # look for EXTERNAL-IP (not <pending>)
```
- ✅ **works:** `Connection to es-remote-site2.chat.com (172.18.0.3) 30443 port [tcp/*] succeeded!`
- ❌ **broken (hang):** `nc: connect ... Connection timed out` → firewall / NetworkPolicy between clusters, or LB `EXTERNAL-IP: <pending>`. **This is the #1 real-staging cause and matches a *hang*.**
- ❌ **broken (fast):** `Connection refused` → nothing listening on 443 (wrong IP, ingressgateway down).

### Layer 3 — Istio ingress: Gateway (SNI, PASSTHROUGH) + VirtualService route
```bash
# on the PEER cluster:
kubectl -n wsp get gateway,virtualservice | grep es-remote
kubectl -n wsp logs deploy/chat-ingressgateway --since=2m | grep es-remote
```
- ✅ **works:** both rows present —
  `gateway.../chat-es-remote-<peer>-gateway` and
  `virtualservice.../chat-es-remote-<peer>-vs  ["es-remote-<peer>.chat.com"]`
- ❌ **broken:** a row missing → client error `Connection closed while SSL/TLS handshake was in progress`
- ⚠️ this error is **identical** for missing Gateway, missing VirtualService, *and* missing DestinationRule (§3) — that's why you check *which resource is absent* rather than trusting the client message.

### Layer 4 — internal 9300 hop: ingressgateway → `<es>-es-transport:9300`
```bash
# on the PEER cluster, from the ingressgateway pod (the hop AFTER SNI routing):
kubectl -n wsp exec deploy/chat-ingressgateway -- sh -c \
  'curl -v -s -m3 telnet://<es>-es-transport.wsp.svc.cluster.local:9300 2>&1 | grep -iE "connected|refused|timed"'
```
- ✅ **works:** `* Connected to <es>-es-transport.wsp.svc.cluster.local (10.244.0.10) port 9300`
- ❌ **broken:** `Connection refused`/`timed out` → NetworkPolicy denying gateway→es-transport, or es pods not Ready. (Note: cross-cluster traffic does **not** use 9300 — only this internal hop and node↔node do; see §on 9300 below.)

### The DestinationRule sub-check (part of layer 3/4)
```bash
kubectl -n wsp get destinationrule | grep es-transport
```
- ✅ **works:** `es-chat-<peer>-es-transport-passthrough  ...-es-transport.wsp.svc...` present with `tls.mode: DISABLE`
- ❌ **broken:** missing → Istio mTLS-wraps 9300 → `Connection closed while SSL/TLS handshake was in progress`

### Layer 5 — TLS trust: shared transport CA (run on BOTH sites)
```bash
kubectl -n wsp get secret <es>-es-transport-certs-public \
  -o jsonpath='{.data.ca\.crt}' | base64 -d | openssl x509 -noout -issuer -fingerprint -sha256
```
- ✅ **works:** *both* sites print `issuer= /CN=chat-transport-ca` **and the same** `SHA256 Fingerprint=02:D1:5C:0B:…`
- ❌ **broken (self-signed):** `issuer= /CN=elastic-<cluster>-transport` → ECK never got the shared CA on that site
- ❌ **broken (mismatch):** issuers match but **fingerprints differ** → different CA bytes per site
- either → client error `PKIX path building failed … unable to find valid certification path (certificate_unknown)`, with a `DiagnosticTrustManager` line naming the served cert's issuer + your truststore paths.

### End-to-end verdict (the two that tell you healthy-vs-not directly)
```bash
$GW curl -k -sS -u elastic:$PW "$ESURL/_resolve/cluster/<peer>:*"
```
- ✅ **works:** `{"<peer>":{"connected":true,...,"version":{...}}}`
- ❌ **broken:** `{"<peer>":{"connected":false,...}}` → then run the `_search` in §0 step 2 to get the reason and match §2.

### Where port 9300 does / doesn't matter (recap)
| Hop | 9300? | Broken symptom |
|---|---|---|
| node ↔ node inside a cluster | **yes** | cluster won't form — `_cat/nodes` missing nodes / health red |
| peer ingressgateway → es-transport | **yes** (layer 4) | 443 connects then `handshake was in progress` |
| **your ES → peer (cross-cluster)** | **NO — it's 443** | irrelevant; if your `proxy_address` ends in `:9300` you're wrongly in `internal` mode |

`_cat/nodes` healthy example (intra-cluster 9300 fine):
`es-chat-site2-es-all-a-0   10.244.0.10   dmr   *`  (node present, `*` = master elected)

---

## 9. When the network itself is blocked (LB/firewall on 443) — workarounds

If layer 2 (`nc -zv <peer> 443`) **hangs**, no ES/Istio config fixes it — the bytes
never leave your cluster. You need a *different network path*. Two constraints
shape every option:

- **CCS is bidirectional** — each site registers the other, so the path must work
  **both directions**.
- The transport is **end-to-end mutual-TLS with the shared CA** — any hop may only
  move **opaque L4 bytes**. A tunnel/proxy that *terminates* TLS breaks trust.

| # | Approach | How | Change | When |
|---|---|---|---|---|
| 1 | **Change the public port** | firewall blocks 443 but allows another port → set `ccs.registrationJob.publicPort` + the Gateway server port to it (the kind harness runs on **30443** exactly this way) | values only | some port is open |
| 2 | **NodePort instead of LoadBalancer** | cloud LB is the blocker (EXTERNAL-IP `<pending>`/filtered) but node IPs reachable → expose ingressgateway as `type: NodePort`, point `proxy_address` at `nodeIP:nodePort` | infra + values | LB layer broken, nodes reachable |
| 3 | **Network underlay** — VPC peering / VPN / Submariner / Cilium ClusterMesh | give the two clusters L3 connectivity; the existing 443 path then just works | infra (networking) | you control the network; want a permanent fix |
| 4 | **Outbound-only reverse tunnel** — Skupper / frp / inlets | firewalls usually allow *outbound*; a relay dials out and stitches an L4 tunnel to the peer's `es-transport:9300`. ES talks to a **local** Service that tunnels across. Keep it L4 (no TLS termination). | infra (tunnel pods) + `proxy_address` → local tunnel Service | inbound firewalled, outbound open, firewall can't be changed |
| 5 | **Drop live CCS → shared object store** | you already wire MinIO/S3 (`secureSettings`). Each site snapshots to a shared bucket; a central cluster restores/searches. No cross-cluster transport at all. | architectural | firewall immovable; non-live search acceptable |
| 6 | **Open the firewall** (the real fix) | allowlist the chosen port between the two clusters' gateway ingress+egress CIDRs, **both directions** | infra (firewall rule) | always the first ask |

**Recommended order:** (1) probe a few ports — if any is open, Option 1 is a 2-line
values change; (6) push to open one port in parallel (correct long-term state);
(4) Skupper if only outbound works; (5) object-store snapshots if live CCS isn't
a hard requirement.

**Cautions for tunnel/underlay routes (3, 4):**
- A direct tunnel to `es-transport:9300` **bypasses the Gateway's SNI routing**, so
  you lose `server_name` multiplexing (fine for a dedicated per-peer tunnel) — but
  the peer's transport cert **still needs the matching SAN** the client validates
  against. Keep `ccs.publicEndpoint.enabled: true` (adds the SAN) or add the tunnel
  hostname to `spec.transport.tls.subjectAltNames`.
- The **shared CA** and the **`es-transport` DestinationRule `tls: DISABLE`** are
  still required — a tunnel only replaces the *inter-cluster* transport, not the
  trust or the in-cluster mTLS-unwrap.

---

## 10. Capstone: test L4 reachability FIRST (the trap that wastes hours)

A real staging investigation chased SNI, DestinationRules, CA trust, passthrough
config, and NetworkPolicy for a long time — and the actual root cause was:

> **Pods in site1's cluster could not reach site2's ingress LB on 443 at all.**
> The LB was reachable from a **bastion/jump host** (external network), so es-http
> "worked" when tested by hand — but **not from inside pods**, and CCS is *always*
> pod→LB. A firewall/security-group allowed corp/VPN ranges but **not the peer
> cluster's pod-egress CIDR.**

### Why it masqueraded as everything else
| Symptom | Why it misleads |
|---|---|
| `Connection closed while SSL/TLS handshake in progress` (ES coord) | looks like DR/CA/SNI — actually the ClientHello never got a reply |
| **no PKIX anywhere** | never reached the cert stage — it's pre-TLS |
| **symmetric across all sites** | every site's pod-net is blocked to every peer LB |
| **health fine** | in-cluster, no cross-cluster hop |
| curl from ES pod says **`Connected to …:443`** then hangs | that "connected" is to the pod's **own sidecar** (localhost interception), *not* the peer — the sidecar's upstream (`PassthroughCluster`) then times out → `UF` |
| es-http "works from site1" | it was tested from a **bastion**, not a pod |

### The 2-minute check that would have caught it immediately
Before touching SNI/DR/CA/gateways, prove raw L4 reachability **from a no-sidecar
pod** (the sidecar's fake "connected" hides the block):
```bash
# raw TCP to the PEER's ingress LB IP — no TLS, no sidecar
kubectl -n <ns> run nc1 --rm -it --restart=Never \
  --annotations="sidecar.istio.io/inject=false" --image=busybox -- \
  nc -zv -w5 <peer-rigw-LB-ip> 443
```
- **timeout** → cross-cluster network block. **Stop — it's a firewall/routing/LB-allowlist problem, not ES/Istio.** Everything above this line is downstream noise.
- **succeeds** → *then* it's worth investigating SNI/DR/CA/passthrough.

**And never trust a curl from an ES pod for reachability** — 443 goes through the
sidecar, whose "Connected to …" means "connected to my own sidecar." Use a
`sidecar.istio.io/inject: false` pod (or the ES pod's `istio-proxy` access log /
`PassthroughCluster` + `UF` flag) to see the *real* upstream result.

### Interpreting the Envoy access-log flags on the ES pod's istio-proxy
| Flag | Meaning |
|---|---|
| `UF` + cluster `PassthroughCluster` | sidecar passed through to the real peer IP, and the **upstream connect failed** → network to the peer (this case) |
| `UF` + cluster `BlackHoleCluster` | sidecar **blocked** egress (`REGISTRY_ONLY` + no `ServiceEntry`) → add a ServiceEntry, different problem |
| `NR` / `no_filter_chain_match` on the **peer's** rigw | reached the peer, SNI matched nothing → genuine SNI mismatch |

### The fix (network, not chart)
Allow each cluster's **pod-egress CIDR → the peer cluster's ingress LB on 443**,
both directions (CCS is bidirectional). Or VPC peering / inter-cluster route. Or,
if inbound 443 truly can't be opened, the §9 workarounds. The exact platform ask:
> "Pods in cluster A time out on TCP to cluster B's ingress LB `<ip>:443` (a bastion
> reaches it fine). Need pod-egress→peer-ingress-LB:443 open, both directions, per
> site pair, for Elasticsearch CCS."

### Revised triage order (do L4 before L7)
1. `nc -zv <peer-rigw-ip> 443` from a **no-sidecar pod** — reachable at all? (this section)
2. `_resolve/cluster/<peer>:*` → the error string (§0/§2)
3. shared-CA issuer proof both sites (§0.3)
4. SNI byte-match on the peer's rigw (§3, §8) + `no_filter_chain_match` counter
5. everything else
```
