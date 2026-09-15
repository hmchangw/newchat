# NATS Site Failover: Full Chat Continuity via a Buddy-Hosted Standby Lane

A site's NATS cluster is lost while the rest of the site — pods, MongoDB,
Cassandra, Valkey, Elasticsearch — stays healthy. Today this takes every user of
that site offline, because NATS is the only ingress the chat plane has. This
spec makes site-A users keep chatting through the outage, served by site-A's own
processes against site-A's own databases.

## Scenario, precisely

| Component at site A | State during the outage |
|---------------------|-------------------------|
| NATS cluster | **down** (bad deploy, quorum loss, config error) |
| Service pods | up |
| MongoDB / Cassandra / Valkey / Elasticsearch | up |
| `auth-service`, `portal-service`, `upload-service` (HTTP) | up |
| Peer sites B, C, D | up |

A plain-language walkthrough of the scenarios and reasoning below lives in
[`docs/nats-failover-scenarios.md`](../../nats-failover-scenarios.md) — start
there if you want the shape before the mechanism.

Whole-site loss is explicitly **not** this design. Everything here depends on
site-A's databases being reachable, because the correctness property that makes
the design worth building is that site-A's data keeps landing in site-A's
stores.

## Scope

| Change | Where |
|--------|-------|
| Failover subject builders | `pkg/subject` |
| Standby stream configs | `pkg/stream` |
| Inbound redirect fallback helper | `pkg/outbox`, used by `outbox-worker` + direct publishers |
| Buddy connection + failover-lane consumers | 7 pipeline services |
| Buddy connection + queue subscriptions | 5 RPC services |
| Forced global room routing on the failover lane | `broadcast-worker`, `room-service` |
| Client failover: shuffle, walk, stick, revert | `chat-frontend` |
| Two-server test harness | `pkg/testutil` |
| Wire docs | `docs/client-api.md` + derived views |

### Out of scope

- **Whole-site loss.** Different problem, different design (it needs replicated
  databases, not a replicated bus).
- **`ROOMS-FAILOVER`.** Room creation, invites, and the `.event.member` roster
  events `room-worker` emits from them all stall for the outage duration.
  Continuity here means send and receive messages in existing rooms. Listed so
  the omission reads as a decision.
- **Capacity weighting in client site selection.** Uniform shuffle to start.
  Weights are a portal field addition whenever real distribution data says they
  are needed; guessing them now would age badly.
- **Latency-ranked peer selection (rejected, see §F).** Measuring per-peer
  latency and connecting to the nearest would defeat spreading, because a site's
  users are geographically co-located and would converge on one peer.
- **Sharding the lane across peers.** Rejected below.

## What actually breaks

The message hot path is **two JetStream streams**, not the five that
`docs/architecture.md` §3–4 implies:

```
client ──JS pub──▶ MESSAGES-{site} ──▶ message-gatekeeper
                                            │
                                       JS pub│
                                            ▼
                                  MESSAGES-CANONICAL-{site}
                                            │
              ┌─────────────┬───────────────┼──────────────────┐
              ▼             ▼               ▼                  ▼
        message-worker  broadcast-    notification-      search-sync-
              │           worker         worker             worker
              ▼             │               │                  │
          Cassandra    core NATS      PUSH-NOTIF-{site}       ES
                       chat.room.>
```

There is **no FANOUT stream** — `pkg/stream/stream.go` has no `Fanout()`, and
`message-worker`, `broadcast-worker` and `notification-worker` all bind
`MESSAGES-CANONICAL` directly through `stream.Resolve`. The architecture diagram
is stale and is corrected as part of this work.

Delivery to clients is **core NATS**, not JetStream (`broadcast-worker/handler.go:35`
— a plain `Publish` abstraction). Core subjects are gateway-routed across the
supercluster, so that plane is already globally reachable. Subject to §E below.

So the durable state that dies with site-A's NATS is two streams on the message
path plus `PUSH-NOTIFICATION-{site}` and `OUTBOX-{site}`. Everything else either
writes to a database that is up, or publishes on a plane that still routes.

**Inbound federation is redirected, not parked.** Peers that cannot reach a down
site's INBOX publish to its buddy-hosted `INBOX-FAILOVER` instead, where the
site's own `inbox-worker` consumes it and writes to the site's own MongoDB. See
§H.

## Design

### A. Topology — spread the connections, pin the lane

Two loads are easy to conflate and must not be:

- **Connection load** — WebSockets, subscription state, per-client fanout. This
  is what overwhelms a cluster: an entire site's user population arriving at
  once.
- **Pipeline load** — one canonical message per send, plus a handful of durable
  consumers. An order of magnitude smaller.

They are independent, because a JetStream publish from one cluster lands in a
stream hosted on another, gateway-routed by subject interest. This repo already
depends on that: cross-site federation is a direct JetStream publish into
`chat.inbox.{destSiteID}.external.{eventType}` with no sourcing or
SubjectTransform. Where a client connects therefore does not constrain where the
lane lives.

| | Placement | Rationale |
|---|---|---|
| Displaced clients | **Every surviving peer**, uniform shuffle | No hotspot, no coordination, no health tracking — depends on §E's forced global routing, without which only the buddy is reachable |
| Standby lane | **One designated buddy** | 5 streams per site, not 5×(N−1); workers hold 2 connections, not N−1 |

Buddy assignment is a ring (`a→b, b→c, c→a`) so each cluster absorbs exactly one
peer's pipeline.

**Each site knows only its own buddy — no service holds the full pairing.**

| Who | Knows | Why |
|---|---|---|
| A site's own services | Its own buddy | To open the buddy connection (§D) |
| Peer sites | Nothing | They publish `chat.failover.inbox.{dest}.external.*`; interest routing finds the stream wherever it is hosted (§H) |
| Clients | Nothing | They shuffle the portal registry (§F); the buddy concept never reaches the frontend |
| A site acting *as* a buddy | Nothing at runtime | The standby streams it hosts are consumed by the **origin** site's services over their buddy connection, not by its own |
| Ops/IaC | The full ring | Provisioning, placement, and capacity sizing |

No N×N matrix exists in any service's config, so adding a site or rotating the
ring touches only the affected sites. The one thing a buddy must absorb without
being told is **capacity** — its own load plus one peer's pipeline — which is a
sizing fact, not runtime config.

Config per site is two values: `BUDDY_SITE_ID` and `BUDDY_NATS_URL`. **NATS
cluster names match site IDs in this deployment** (confirmed), so
`BUDDY_SITE_ID` doubles as the expected `StreamInfo.Cluster.Name` for the
placement assertion (§C) and no separate `BUDDY_CLUSTER_NAME` is needed. Treat
that correspondence as an ops invariant: were a cluster ever renamed away from
its site ID, the assertion would fail at startup — loudly, which is the right
direction, but it would need the third value at that point.

That assertion is also what catches a **ring disagreement**, not merely an
accidental placement. If ops provisions a site's standby streams on the wrong
cluster, `js.Stream()` still finds them — the JetStream API is supercluster-wide,
so lookup succeeds regardless of where an asset is hosted. Only the
`Cluster.Name` comparison detects that ops config and service config disagree
about the ring, which is exactly the split-brain that would otherwise stay
invisible until an incident.

**Why services pin to the buddy while clients spread.** The asymmetry is
principled, not incidental, and it follows from the same load split above.

The decisive reason: **load follows the streams, not the connections.** The
standby lane is pinned to the buddy, so the JetStream work — append, replicate,
consumer state, acks — lands there no matter which cluster a service connects
from. Spreading service connections cannot reduce the buddy's load by one byte;
it would only add gateway hops to reach the same streams. Clients are the
opposite case: their connection *is* the load (WebSockets, subscription state,
per-client fanout), so spreading them moves real work.

Three supporting reasons:

- **Consuming is chatty.** A pull consumer continuously exchanges pull requests,
  deliveries and acks. On the buddy that traffic is intra-cluster; from a random
  peer every ack is a WAN round trip to the stream leader. At `MAX_WORKERS=100`
  that is a large avoidable cost.
- **Scale does not warrant it.** A site is a dozen services times a few replicas
  — tens of connections, no fanout. The capacity pressure that drives client
  spreading does not exist here.
- **Determinism.** "Which cluster is `inbox-worker` on today?" is a bad question
  to face mid-incident. Pinning makes capacity planning arithmetic.

Random placement also buys nothing in the double-failure case where it looks
most attractive: if the buddy is down too, the standby streams are unavailable
regardless of what a service is connected to. A live connection elsewhere does
not resurrect them.

The RPC services (`room-service`, `user-service`, `history-service`,
`search-service`, `user-presence-service`) are the one arguable case — they
consume no streams, so their location is nearly free. But spreading them would
not help either: a request from a displaced client crosses one gateway hop to
reach its home site's service wherever that service sits, and removing the hop
would require N−1 connections per service, the fan-out this design rejects. One
rule for all services is simpler and no worse.

**Rejected: sharding the lane across peers** (e.g. by `roomID`). It fragments a
single site's canonical stream across clusters, which breaks per-room ordering
and splits `CanonicalDedupID`'s dedup window across independent stream instances.
The pipeline load does not earn that.

### B. Subject design

Two constraints, both supercluster-wide rather than per-cluster, because all
sites share one account (see "Ops invariants") and no JetStream domains are
configured.

**Names are globally unique.** The meta group spans every JetStream server in
the domain and tracks all stream assignments, so a name identifies exactly one
asset across the whole supercluster. This is what the `{siteID}` suffix in
`pkg/stream/stream.go` is for — and it is load-bearing rather than cosmetic,
because the bootstrap path uses `js.CreateOrUpdateStream`: two sites
bootstrapping the same name would not collide with an error, the second would
silently **update the first site's stream**, overwriting its subjects and
placement. It is also why `js.Stream(ctx, name)` resolves from any cluster, and
therefore why a successful lookup says nothing about placement (§C).

The standby streams are accordingly named for the **origin** site
(`INBOX-FAILOVER-site-a`), never the hosting buddy.

**Subject filters cannot overlap.** Failover subjects therefore cannot simply
append a token to a live filter. But they also cannot go
anywhere convenient, because client-facing subjects must stay inside
`chat.user.{account}.>` or the JWT minted by `auth-service` will not permit
publishing them.

That yields two rules:

**Client-facing — stay in JWT scope, place the token where it cannot overlap.**

```
live:     chat.user.*.room.*.{siteID}.msg.>              ← MESSAGES-{siteID}
failover: chat.user.{acct}.room.{roomID}.{siteID}.failover.msg.send
```

`.failover.msg.send` does not match `.msg.>` at that position, so the filters are
disjoint. No `auth-service` change, no JWT permission change.

**Internal — separate root**, since services publish these under service creds
and are not constrained by the user JWT:

```
chat.failover.msg.canonical.{siteID}.>
chat.failover.push.{siteID}.>
chat.failover.outbox.{siteID}.{destSiteID}.{eventType}
chat.failover.inbox.{siteID}.external.{eventType}
```

A shared `chat.failover.>` root guarantees no filter can overlap a live stream in
the same account, now or as new streams are added.

**Both roots clear the leaf-node filter.** That filter is scoped to
`chat.local.>` specifically (confirmed with the platform team), so neither
`chat.user.{acct}.…failover.msg.send` nor `chat.failover.>` is denied interest
advertisement across the supercluster. This is load-bearing in two places: the
client-facing subject must cross the gateway for a displaced client to reach
its home site's `MESSAGES-FAILOVER` stream, and the internal subjects must cross
it for the buddy-hosted lane to be publishable from a site-A process. Choosing
either root under `chat.local.` would have silently disabled the whole design.

New builders in `pkg/subject` mirroring the live ones, so no call site builds a
failover subject with `fmt.Sprintf`.

### C. Standby streams

Five per site, hosted on that site's buddy cluster, named for the **origin**
site. Ops/IaC-owned in production; `BOOTSTRAP_STREAMS=true` stands them up in
dev, per the existing convention.

| Stream (on buddy's cluster) | Subjects |
|---|---|
| `MESSAGES-FAILOVER-{site}` | `chat.user.*.room.*.{site}.failover.msg.>` |
| `MESSAGES-CANONICAL-FAILOVER-{site}` | `chat.failover.msg.canonical.{site}.>` |
| `PUSH-NOTIFICATION-FAILOVER-{site}` | `chat.failover.push.{site}.>` |
| `OUTBOX-FAILOVER-{site}` | `chat.failover.outbox.{site}.>` |
| `INBOX-FAILOVER-{site}` | `chat.failover.inbox.{site}.external.>` |

`INBOX-FAILOVER-{site}` carries only the `external.>` lane. The `internal.>`
lane is a same-site search feed published by services that are idle during the
outage, so it has nothing to carry.

#### Placement is load-bearing and must be explicit

A stream created without `StreamConfig.Placement` is placed wherever the
supercluster's meta-leader chooses. For these five that is fatal: if
`INBOX-FAILOVER-site-a` lands on cluster A, it dies with site A and every
failover silently fails with the lane looking correctly provisioned. Each
standby stream MUST carry an explicit `Placement.Cluster` naming the **buddy's**
cluster.

**Relocating a live stream is not an alternative.** Placement is changeable at
runtime — update the stream config, or `nats stream move` — but the migration
scales the stream's Raft group onto the target cluster's peers and catches them
up from the existing group, so it requires the **source cluster healthy**. A
stream cannot be moved off a cluster that is down: quorum is lost and the data
is on that cluster's disks. Standby streams are therefore not a cost-saving
choice over relocating the live ones; relocation is simply unavailable in the
failure mode this design targets.

**Nothing is ever moved at runtime.** Placement is set once at creation and never
updated; a failover is simply traffic arriving at streams that already exist in
the right place. Even buddy reassignment does not need a migration — because
standby streams sit empty in steady state, the operation is delete-and-recreate
on the new buddy, with no catch-up and no partially-moved state to reason about.
The one precondition is that consumers have drained, which only matters when
reassigning shortly after a real failover, while the stream still holds that
incident's residue.

**Ownership and verification.** Placement is topology, so it stays with ops/IaC
per the existing rule that a service's `bootstrap.go` sets only `Name +
Subjects`. But the production path in each `bootstrapStreams` already calls
`js.Stream(ctx, name)` to fail fast on a missing stream; for the standby streams
it additionally asserts `info.Cluster.Name` equals the configured buddy cluster.
That is the same fail-fast check one field deeper, and it converts the one
misconfiguration that would otherwise be silent and catastrophic into a startup
error.

Note the test-fidelity limit this implies: `testutil.NATSPair` gives two
standalone JetStream servers, not a supercluster, so placement has no meaning
there. The harness proves the application logic; **placement correctness is
verifiable only against a real supercluster in staging**, and belongs in the
rollout checklist rather than the test suite.

### D. Dual connections, no mode flag

Each affected service opens **two** NATS connections — home and buddy — and
binds each lane's consumers on its own connection.

A `nats.Conn` attaches to exactly one server at a time, so a comma-separated
`NATS_URL` cannot express this: a worker that reconnected to the buddy could not
also drain its home backlog, which would force an explicit failover-mode flag
and with it the possibility of half the workers flipping and half not.

Two connections removes the question entirely:

| Home NATS | Behavior |
|---|---|
| Down | Home consumers idle; buddy consumers run |
| Up | Both run; the pre-outage backlog drains alongside the failover lane |

There is no detection, no flag, and no cross-service agreement to reach.
Recovery stops being a runbook and becomes a property.

One qualification: this holds fully for **consumer binding**, which is what §D
covers. Room-event *routing* (§E) additionally needs to know how recently the
home connection was restored, because clients revert on a slower clock than
servers do. That is a timestamp each process reads from its own connection, not
a mode flag and not shared state — the no-coordination property survives.

Connect semantics differ by role. **Home stays fail-fast** per CLAUDE.md — if the
home bus is unreachable at startup, log and exit. **Buddy connect failure at
startup logs a warning and the service runs without a failover lane.** A buddy
that is already down when we start is a double fault; a retry loop for it is not
worth the goroutine.

### E. Room routing must go global on the failover lane

**Implemented** by `docs/superpowers/plans/2026-08-15-nats-failover-room-routing.md`:
`pkg/subject.EffectiveRouteMode` / `LaneRouter` resolve the mode per publish,
`pkg/natsutil.RestoreTracker` supplies the restoration timestamp, and
`broadcast-worker` and `room-service` each run one handler per lane.

`pkg/subject/subject.go:154-159` routes a room's events to one of two roots:

```go
func roomBase(roomID string, global bool) string {
	if global {
		return "chat.room." + roomID          // gateway-propagated
	}
	return "chat.local.room." + roomID        // leaf-filtered, site-local
}
```

`roomRouteGlobals` (`:415`) sends a **confirmed same-site room**
(`crossSite=false`) to the local root under `RouteLocal` / `RouteDual`. The
local prefix is filtered at the leaf node — denied from interest advertisement
across the supercluster — per the infrastructure boundary in
`2026-07-28-local-global-room-subjects-design.md`. A publish on it never crosses
a gateway.

**This is not a stale-data problem.** `crossSite` lives on the `Room` document in
MongoDB, which is up, and the classification stays correct throughout the outage.
The problem is what the flag means:

> A room is global iff at least one member's **home site** differs from
> `room.SiteID`.

`crossSite` encodes members' home site, not their connection location. Those
normally coincide, which is what makes the whole scheme sound. Failover is
exactly the case where they diverge: a displaced site-A user is still homed at
site A, so the room remains correctly classified `crossSite=false`, while that
user now sits behind a gateway the local prefix is filtered from. Mongo
faithfully reports "all members are at site A" — still true, and no longer
sufficient to conclude that site-local delivery reaches them.

The failure is also partial rather than clean. Displaced users who land on the
same cluster as the relocated `broadcast-worker` do receive the publish (same
cluster, no gateway crossing); those on other peers do not. Which users go
silent depends on which peer each client's shuffle happened to pick.

**Fix, both sides flipping on the same condition:**

- **Server** — routing is decided per message, by which connection the triggering
  work arrived on (a failover-lane JetStream message for a worker, a
  buddy-connection request for an RPC service) and, on the home connection, by
  how recently the home connection was restored:

  | Work arrived on | Routing |
  |---|---|
  | Buddy connection | Global only |
  | Home connection, within `FAILOVER_REVERT_GRACE` of home restoration | **Dual — local then global** |
  | Home connection, outside the window | Configured `ROOM_SUBJECT_MODE` |

- **Client** — while in failover mode, ignore `crossSite` and subscribe to the
  global root for every room.

The dual-publish row is not decoration; without it the design breaks on
**recovery**. Clients revert on their own backoff (§F), up to five minutes after
home NATS returns, while a server keyed only on connection identity would revert
the instant the home lane delivers its first message. In that window it would
publish local while displaced clients are still subscribed global on another
cluster — the same silent gap as §E, but during recovery, when every health
check reads green.

This is the codebase's own established remedy for a publisher/subscriber flip
mismatch: `roomLocalityGrace` (`pkg/subject/subject.go:10-15`) dual-publishes
after a room flips local→global "so members not yet re-subscribed keep
receiving." Same shape, same fix. It needs a **separate** constant, though —
`roomLocalityGrace` defaults to 7 days because room reclassification is
permanent and clients learn it from bootstrap, whereas this window only has to
outlast the client revert backoff. Default `FAILOVER_REVERT_GRACE` = 30m against
a 5m backoff cap.

**This is a locally observed timestamp, not a mode flag.** Each service reads its
own home-connection restoration time from its own connection state — no
coordination, no shared state, no agreement to reach between services. Services
whose windows close at different moments cost a little redundant gateway
interest and never a missed message, which is the same fail-safe direction the
local/global design already runs in. (Contrast `roomLocalityGrace`, which *must*
be identical across publishers because it is keyed on a shared `crossSiteAt`
timestamp; this one is keyed on each process's own reconnect.)

The justification is exact rather than approximate: if home NATS is down, every
site-A client is on a remote cluster by definition, so `chat.local.room.>` at
site A has zero legitimate subscribers for the duration. Forcing global costs
only the gateway interest the local namespace was built to save, and only while
a site is down — which is the trade that design already prescribes, since its
first stated principle is that global is the fail-safe and over-routing to
global is "no correctness bug." Treating a displaced member as cross-site is not
a workaround for that scheme; it is the scheme's own rule applied to the case
where home site and connection location differ.

It also runs with the grain of the existing fail-safes: `roomRouteGlobals`
already routes local only on an explicit `crossSite=false`, and
`docs/client-api.md` already requires clients to treat a missing `crossSite` as
global.

#### Forced global routing is what makes client spreading possible

This section and §A's client spreading are one mechanism, not two concerns.

Without forced global routing, a displaced client would receive a same-site
room's events only by connecting to the **buddy** — the single cluster where
`broadcast-worker`'s publish reaches it without crossing a gateway. Every other
peer would be silent for most of that user's rooms. The uniform shuffle would be
unusable, the buddy would become the only viable destination, and it would
absorb the entire displaced client population: precisely the hotspot that
spreading exists to prevent.

So §E is not only about not losing events during an outage. It is the
precondition for a client being able to connect anywhere, and therefore for the
connection load being spread at all. Removing it does not degrade the design
gracefully — it collapses §A's topology back onto one cluster.

#### The underlying assumption, and the other thing that violates it

The local/global optimization assumes **a user's connection location equals
their home site**. Exactly two things break that assumption:

- **Failover**, handled here by forcing global routing on the buddy connection.
- **Travel.** A US-homed user in Tokyo who connected to the Japan cluster would
  miss every same-site room's events, for the identical reason. Today this
  cannot happen: portal resolves `natsUrl` from the directory entry's `siteId`
  (`portal-service/handler.go:189`), so a client always dials home wherever its
  user is. That is correct and should stay that way — and it costs nothing,
  because a traveler's data and services are at home regardless, so connecting
  locally would add a gateway hop in front of the same unavoidable long haul
  rather than removing it.

The consequence worth recording: **"connect to the nearest site for latency"
cannot become a feature without revisiting local/global first.** It would be a
harder change than this one, because §E forces global only for the duration of
an incident, whereas a roaming feature would mean permanently global routing for
anyone who might roam — surrendering exactly the gateway interest savings the
local namespace was built to capture.

Three services call the `RoomEventTargets` / `RoomMemberEventTargets` family.
Two are in scope: `broadcast-worker` (`handler.go:845` — `.stream.msg`,
`.event.metadata.update`, `.event`) and `room-service` (`handler.go:1446` —
`.event`). The third, `room-worker` (`handler.go:2362`, `:2375` — `.event`,
`.event.member`), is driven entirely by the `ROOMS` stream, which has no
failover lane; its member events defer with that deliberate omission and it
needs no change here.

### F. Client behavior

1. **Detect.** Home NATS unreachable past the normal reconnect window (~15s).
2. **Select.** Shuffle the peer list from portal and walk it. First peer that
   accepts the connection wins. A peer that is also down is simply the first
   failed attempt; no liveness tracking is needed anywhere.

   **This list is not served today.** Portal holds the full registry server-side
   (`h.sites`, parsed from `PORTAL_SITE_URLS`) but every response exposes only
   the caller's own site: `GET /api/userInfo` and `POST /api/v1/login` return a
   single `natsUrl`/`siteId` pair from `h.sites[e.SiteID]`
   (`portal-service/handler.go:189-220`), and `GET /api/settings` carries no
   sites at all. A displaced client currently has no way to learn any peer's
   NATS URL, so **portal must expose one — this is a required change, not a
   reuse of existing data.**

   Add it to `GET /api/settings`: that endpoint is already the deployment config
   served to the frontend, is identical for every caller, and the client already
   fetches it, so no new endpoint and no extra round trip. Per-user shapes
   (`/api/userInfo`) would re-send an identical list for every user.

   Decide deliberately: `/api/settings` is token-free, so the peer list becomes
   publicly readable and reveals the federation topology. In practice these are
   public WSS endpoints clients connect to anyway, and `/api/userInfo` is also
   token-free discovery, so the exposure is comparable wherever it lands — but
   it should be a decision on the record rather than a side effect.
3. **Stick.** Hold that peer until it too fails, then re-shuffle excluding it.
   Re-randomizing on every reconnect attempt would churn connections and discard
   warm subscription state for no benefit.
4. **Switch.** Publish sends on the `.failover.` subject variant; subscribe to
   the global room root for every room (§E).
5. **Revert.** Probe home on its own exponential backoff (5s → 5min cap). On
   success, reconnect home and return to normal subjects and normal
   `crossSite`-driven routing.

The same JWT is reused throughout — see the ops invariants below.

**Rejected: latency-ranked selection.** Maintaining a live per-peer latency list
so clients connect to the nearest peer defeats the purpose of spreading. A
site's users are geographically co-located — that is why they are homed there —
so ranking by latency makes them overwhelmingly agree on one answer and
converge on a single peer, recreating the hotspot uniform shuffle exists to
prevent. It would amount to an implicitly-chosen buddy, selected by network
topology rather than deliberately with capacity in mind. Two lesser objections
reinforce it: a dynamic list means probing peers continuously during normal
operation to optimize an event that should almost never happen, and measuring at
failover time means every displaced client probes every peer simultaneously,
precisely when the federation is already degraded.

If geographic spread turns out to matter, the answer is **static region tags,
not measurement**: a `region` alongside each site in the portal registry (one
more field on the peer list §F step 2 already adds), with clients shuffling
uniformly *within* the nearest region and falling back to the next only if none
accepts. No probing, no background traffic, and spreading preserved among the
peers that matter. Worth doing only if sites actually span regions — and note
that a region holding a single peer collapses back to one destination, at which
point sizing that peer is the real answer rather than the selection algorithm.

No `buddySiteId` field — an earlier draft proposed one, and uniform client
spreading makes it unnecessary, since the client picks from the peer list rather
than from a designated buddy. But a peer-list field **is** required, per step 2:
portal serves only the caller's own site today.

### G. Services affected

| Plane | Services | Change |
|---|---|---|
| RPC | `room-service`, `user-service`, `history-service`, `search-service`, `user-presence-service` | Buddy connection + the same `QueueSubscribe` calls on it. RPC subjects already carry `{siteID}`, so site-B's copy of a service cannot answer site-A's requests. `room-service` also §E. |
| Pipeline | `message-gatekeeper`, `message-worker`, `broadcast-worker`, `notification-worker`, `search-sync-worker`, `push-notification-service`, `outbox-worker` | Buddy connection + failover-lane consumers. `broadcast-worker` also §E. |
| Client | `chat-frontend` | §F. |
| Portal | `portal-service` | Expose the peer list on `GET /api/settings` (§F step 2) — required; not served today. |

`room-worker` needs no change: its only input is the `ROOMS` stream, which is
out of scope, so it is idle for the duration of the outage.

### H. Federation

Cross-site traffic splits three ways, and only one of them needs anything built.

**Real-time cross-site messages — unaffected.** `broadcast-worker` contains no
inbox/outbox code; cross-site delivery to remote members is core NATS on
`chat.room.{roomID}.>`, gateway-routed. A displaced site-A user receives a
remote site's messages directly, with no federation machinery in the path. (The
`outbox.{site}.to.{dest}.*` edge in `docs/architecture.md` §3 is stale, like the
FANOUT stream, and is corrected with it.)

**Direct-publish federation — free.** `message-worker`
(`thread_subscription_upserted`) and `user-service` (`user_status_updated`,
settings) publish straight to `chat.inbox.{dest}.external.>`. That target is a
**remote** stream, which is up, and a publish from the buddy connection is
gateway-routed exactly as from home. No failover lane, no change.

**OUTBOX-buffered federation — needs the lane.** `room-service`'s
subscription-state events go through the local `OUTBOX-{site}` buffer, which is
down with the site. `OUTBOX-FAILOVER-{site}` (§C) carries them, with the same
`ConcurrentEventTypes` / `OrderedEventTypes` consumer partition. `room-worker`'s
membership events need nothing, since `ROOMS` is deferred and it is idle.

The rule underneath: **federation that targets a remote stream is already
failover-safe; federation that buffers in a local stream is not.**

#### Inbound to the failed site — redirect to the buddy INBOX

Rather than letting peers park their forwards until the site returns, a peer that
cannot reach a down site's INBOX republishes to `INBOX-FAILOVER-{site}` on that
site's buddy. The site's own `inbox-worker`, already holding a buddy connection
(§D), consumes it and writes to the site's own MongoDB — which is up. Inbound
federation keeps flowing instead of going one-directional for the outage.

**Peers need no knowledge of the buddy topology.** A peer publishes
`chat.failover.inbox.{site}.external.{eventType}` and the supercluster routes it
by interest to whichever cluster hosts that stream. No peer ever learns which
site is whose buddy, so a buddy reassignment stays pure ops config, invisible to
every other site's code and config alike.

**Fall back only on `ErrNoResponders`, never on timeout.** This is the rule that
makes the redirect correct rather than merely useful:

| Primary publish result | Action |
|---|---|
| Success | Ack |
| `ErrNoResponders` — unambiguously not delivered | Republish to `INBOX-FAILOVER`; Ack on success |
| Timeout or any other error — **may** have landed | Nak and park, exactly as today |
| Failover publish also fails | Nak and park |

`INBOX-{site}` and `INBOX-FAILOVER-{site}` are separate streams with independent
dedup windows, so a shared `Nats-Msg-Id` does **not** collapse a duplicate across
them. Falling back on an ambiguous error would therefore risk applying an event
twice. NoResponders carries no such ambiguity — there was no interest, so nothing
was delivered — and `pkg/natsutil/request_failure.go:47` already distinguishes it
from a timeout. Restricting the fallback to that one error class guarantees each
event lands in exactly one stream.

**The ordered lane stays ordered.** Ordering for `member_added` / `member_removed`
/ `room_renamed` is enforced at the *sender*, by the peer's per-destination FIFO
consumer at `MaxAckPending=1`. Event N falls back and Acks before N+1 is
released, so a membership sequence flows through `INBOX-FAILOVER` in the same
order it would have taken through `INBOX`.

This also removes the peer-OUTBOX-retention risk that parking created: forwards
are delivered promptly rather than accumulating unacked for the outage duration,
so a long outage no longer approaches any peer's `MaxAge` or `MaxBytes`.

**Applies to the direct publishers too.** `message-worker` and `user-service`
publish to a remote INBOX with no OUTBOX buffer behind them, so today a failed
publish has nowhere to retry from. The fallback therefore belongs in a shared
helper in `pkg/outbox` used by `outbox-worker`'s forward and both direct
publishers, not inlined in `outbox-worker/handler.go`.

**Residual: in-flight backlog at the moment of failure.** Whatever sat
unprocessed in `INBOX-{site}` when its NATS died drains at recovery, after the
failover-lane events that followed it. That is seconds of backlog, not the whole
outage, because an event only reaches `INBOX-{site}` if its publish succeeded —
which means the site was up and `inbox-worker` was consuming. Every
concurrent-lane handler absorbs it regardless: `inbox-worker` guards room
upserts, subscription read/mute/favorite/section, user status, settings and
chatlist on the event `Timestamp` specifically so "an out-of-order or duplicate
delivery can't regress" the stored value, and thread subscriptions are
documented idempotent and order-safe. Member events are idempotent through their
unique index.

## Recovery

When home NATS returns, both lanes run concurrently: the pre-outage backlog in
`MESSAGES-CANONICAL-{site}` replays while failover-lane traffic winds down.

`CanonicalDedupID` (`pkg/natsutil/canonical_dedup.go`) makes this idempotent —
duplicates collapse on the stream's dedup window. It does **not** order the two
lanes against each other: a message sent at T−1 and stuck in the backlog can
persist after one sent at T+5 through the failover lane.

**Decision: accept the reorder.** History is unaffected, because
`messages_by_room` clusters by `created_at` — a read returns correct order
regardless of write order. Only live delivery order is affected, only for
clients connected during the drain, and only for its duration. The alternatives
were rejected: pausing the failover lane until the backlog empties trades a
brief cosmetic reorder for a hard stall plus cross-lane coordination; buffering
and re-emitting in `created_at` order puts latency and real complexity on the
hot path permanently to fix a transient.

Clients revert to home independently on their own backoff, so the client
population drains back gradually rather than stampeding. That gradual return is
also why room-event routing dual-publishes through `FAILOVER_REVERT_GRACE`
(§E): for as long as any client may still be on a peer cluster, both roots have
to carry the traffic.

## Testing

TDD, red first, per CLAUDE.md §4.

`pkg/testutil` gains `NATSPair(t) (home, buddy string)` — two JetStream-enabled
servers following the established `Xxx` / `EnsureXxx` / `TerminateXxx` shape,
with `TerminateNATSPair` wired into `TerminateAll`. Seven-plus packages need it,
which is exactly the threshold CLAUDE.md sets for a shared testutil container.

**Unit**

| Case | Assertion |
|------|-----------|
| Failover subject builders | Exact strings; disjoint from live filters at every token position |
| Filter overlap guard | Table over every live/failover stream pair — no pair overlaps |
| Dedup key stability | `CanonicalDedupID` identical for the same event on either lane |
| Lane-forced routing | Buddy connection yields `chat.room.>` for `crossSite=false` |
| Revert grace — inside | Home connection within `FAILOVER_REVERT_GRACE` yields both roots, local first |
| Revert grace — outside | Home connection past the window yields the configured `ROOM_SUBJECT_MODE` routing |
| Revert grace — clock | Window measured from each process's own home restoration; a second failover restarts it |
| Buddy connect failure | Service starts, logs a warning, runs with home lane only |
| Placement assertion | Production path fails startup when a standby stream's `Cluster.Name` is not the configured buddy |
| Home connect failure | Service exits non-zero (unchanged) |
| Outbox partition parity | `OUTBOX-FAILOVER` consumers cover exactly `ConcurrentEventTypes ∪ OrderedEventTypes` |

**Integration** (`//go:build integration`, `testutil.RunTests`)

| Case | Assertion |
|------|-----------|
| Failover send | Home NATS stopped; publish to the failover lane; message reaches Cassandra and a subscriber sees the broadcast |
| Correct store | Message persists to the **origin site's** keyspace, not the buddy's |
| Global routing | Same-site room under `RouteLocal`; failover-lane publish observed on `chat.room.>` |
| Recovery overlap | Backlog seeded pre-outage; both lanes drain; no duplicate rows, all messages present |
| Outbound federation | `OUTBOX-FAILOVER` forwards reach a peer INBOX during the outage |
| Direct-publish federation | `user-service` status publish from the buddy connection lands in a peer's INBOX with no failover lane |
| Shared fallback helper | `message-worker` and `user-service` direct publishes redirect through the same `pkg/outbox` helper |
| Inbound redirect | Primary publish returning `ErrNoResponders` republishes to `INBOX-FAILOVER` and reaches the down site's Mongo |
| Timeout does **not** redirect | An ambiguous timeout Naks and parks; the event never lands in both streams |
| Ordered lane preserved | A member add/remove/rename sequence arrives through `INBOX-FAILOVER` in send order |

Coverage: the 80% floor repo-wide, 90% target for `pkg/`.

## Rollout

Ops provisions the five standby streams per site — **each with an explicit
`Placement.Cluster` naming the buddy's cluster** (§C) — and sets `BUDDY_SITE_ID` /
`BUDDY_NATS_URL` before any service rolls.

The failover subject scheme is shared code (`pkg/subject`) rather than
configuration, so within a build every site agrees on it with nothing to
distribute. The rollout consequence is one-sided: a peer still running an older
build has no fallback builder and parks instead of redirecting (§H), which is
exactly today's behavior. A partial rollout is therefore safe — inbound redirect
simply works only for peers that have shipped it — but inbound coverage is not
complete until every peer has. Placement cannot be checked by the
test suite, so verifying it in staging is a rollout step, not a test:
`nats stream info` each standby stream and confirm the hosting cluster. Until the frontend ships its failover
logic the lane is inert — no client publishes a `.failover.` subject — so the
backend can land first and sit dormant, which is also how it should be verified
in staging: stop a site's NATS and drive the lane with a test client.

**Capacity.** Each cluster must have headroom for its own load plus one peer's
pipeline, and each must tolerate an arbitrary share of a failed peer's client
connections — uniform in expectation, but with real variance at small N.

**Wire docs.** `docs/client-api.md` gains the failover subject variant and the
failover-mode routing rule, with the derived views
(`docs/client-api/request-reply.md`, `docs/client-api/events.md`) updated in the
same PR. `docs/architecture.md` §3–4 gets the stale FANOUT stream corrected, and
`docs/nats-subject-naming.md` gains the `chat.failover.>` tree.

## Ops invariants this design rests on

Two facts live in infrastructure config rather than in the repo. Both are
confirmed; both must stay true, because a change to either would disable
failover **silently** — the lane would look correctly provisioned and simply
never carry anything.

**All sites share one NATS account.** A JWT minted by site-A's `auth-service` is
therefore accepted by site-B's NATS, which is what lets a displaced client
relocate at all without re-authenticating against a foreign site.

This is not only an ops fact — it is the stated premise of
`2026-07-28-local-global-room-subjects-design.md`, whose problem statement opens
with "all chat clients connect under a single shared NATS account" and whose
entire rationale (per-account gateway interest propagation replicating every
site's subscriptions) only holds if that account spans sites. The failover
design and the local/global design rest on the same foundation.

**The leaf-node interest filter is scoped to `chat.local.>` specifically.** No
other prefix is denied advertisement across the supercluster, so both failover
subject roots chosen in §B propagate. See §B for why that is load-bearing in
both directions.

Should the shared account ever be split per site, this design does not degrade —
it stops working entirely, and the only remaining option is stretching JetStream
across sites (R3/R5), rejected here because it puts a cross-WAN quorum
round-trip on every message publish in normal operation and dissolves the
independent-site premise the whole OUTBOX/INBOX federation design rests on.
