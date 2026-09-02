# clientsim — real-WSS connection soak tool

Status: approved design, revised after external review (subscription model,
latency semantics, JWT lifecycle, pool contract). Pre-implementation.
Owner: load-testing tooling. Companion to `tools/loadgen`.

## 1. Summary

`tools/clientsim` is a standalone load tool that holds tens of thousands of
**real client connections** against a site: per simulated user it performs the
full production client edge path — NKey generation → `POST /api/v1/auth` →
NATS **WebSocket** connect with the minted user JWT → the real client
subscription walk (user event lane + `subscription.list` + per-room channel
subscriptions) — then sits there counting deliveries. It exists so soak and
failure tests run with a realistic connection plane (NATS websocket listener,
JWT refresh/reconnect churn, fan-out to live subscriptions) instead of the
near-zero connection count of a loadgen-only run.

loadgen deliberately connects with shared `backend.creds` over core NATS TCP
("Not an auth benchmark"); clientsim is the missing other half.

## 2. Goals

- Hold N live WSS connections (target: tens of thousands to 100k+ across
  replicas) through the real client auth + transport path.
- Subscribe like the production frontend — user event lane plus per-room
  channel subscriptions resolved via `subscription.list` — so
  `broadcast-worker` fan-out (DM **and** channel) actually lands on these
  connections; count deliveries, discard payloads.
- Measure at the true client edge: auth latency, connect latency, disconnect
  reasons, JWT refresh behavior, broadcast→client and canonical→client
  delivery latency, and delivery-loss visibility (drops, decode failures).
- Run in docker-local, and deploy to the real k8s test and staging clusters
  (local container runs are explicitly not representative for this tool).
- Optional connection churn to model mobile clients dropping/rejoining.

## 3. Non-goals

- Sending messages, read receipts, presence heartbeats — traffic generation
  stays in loadgen (it already has presence workloads).
- Auth-service capacity testing: the auth hop goes through a dedicated
  side issuer (§6), so the main auth-service is intentionally not loaded.
- Decrypting message content. Only cleartext envelope fields are read.
- **Delivery verdicts.** clientsim makes loss *visible* (§8) but does not
  decide `eligible`/`missing`/`INCONCLUSIVE` — that requires the global
  view of room membership × traffic that only loadgen's soak
  ledger/verdict tooling has. True end-to-end correlation by `LastMsgID`
  likewise stays in loadgen, which already implements it; duplicating it
  here would create exactly the runtime coupling this design avoids.
- CI regression gating. Manual/ops-invoked, like loadgen.

## 4. Relationship to loadgen

Zero runtime coupling: no RPC, no shared process, no shared state. Exactly
two contracts:

1. **Pool artifact.** clientsim connects as the accounts loadgen's fixtures
   reference, so fan-out from loadgen-driven rooms reaches clientsim-held
   connections. The seeding step (local `loadgen seed` and the staging soak
   seeder) **emits a versioned pool artifact** — a JSON file with
   `schemaVersion`, `runID`, `siteID`, `configDigest`, and the ordered
   list of `accounts` — and clientsim consumes only that file, failing
   fast at startup on an unknown `schemaVersion`, a `siteID` that does
   not match `CLIENTSIM_SITE_ID`, or an empty account list (`runID` and
   `configDigest` are echoed into the run summary for correlation). One
   mode everywhere:
   - Local: seed writes the fabricated `user-%d` accounts (including any
     `--users` override, which pure preset+seed re-derivation would miss).
   - Staging: the soak manifest's `ActiveUserIDs` are Mongo `_id`s, not
     accounts, so the seeder resolves and writes the borrowed users'
     **accounts** into the artifact at seed time (it is the only component
     with the Mongo view to do so).
   The side issuer's allowlist (§6.3) is generated from the same artifact,
   so pool and allowlist cannot drift.
2. **Observability plane.** clientsim exposes Prometheus metrics scraped by
   the same Prometheus/Grafana overlay loadgen runs use. Note the units:
   loadgen counts **logical sends**, clientsim counts **per-connection
   fan-out copies** (one channel send can be hundreds of deliveries, a DM
   one or two), so the two counters are diagnostic trend evidence side by
   side, never a division — the same rule
   `docs/load-testing/failure/nats-metrics-contract.md` already states for
   `fanout_recipients` vs `recipient_deliveries_total`.

The subscription plan (which rooms, which namespace) is deliberately **not**
a loadgen contract: clientsim asks the system itself via the real client
RPC (§5.2), the same way the production frontend does.

Why a separate tool (decided): the two press different failure domains —
loadgen is throughput-bound (open-loop rate into the pipeline), clientsim is
FD/goroutine/memory-bound (long-lived idle-ish connections). Separating them
keeps a connection-plane collapse from killing the traffic generator, lets
each scale independently, and keeps lifecycles distinct (bounded `run
--duration` vs long-running soak pool).

## 5. Architecture

New flat tool directory `tools/clientsim` (single root `go.mod`, repo
conventions apply — env config via `caarlos0/env`, slog JSON, graceful
shutdown via `pkg/shutdown.Wait`).

### 5.1 User pool + sharding

The pool is always a file (`CLIENTSIM_POOL_FILE`, the §4 artifact). Let
`P = len(pool.accounts)` and `T = min(CLIENTSIM_TARGET_CONNS, P)` (default
`T = P`). Shard `i` of `n` owns accounts
`pool[floor(T·i/n) : floor(T·(i+1)/n)]` — shards partition exactly `T`
accounts with no overshoot and sizes differing by at most one.

On k8s a **StatefulSet** supplies `SHARD_INDEX` for free: pod ordinal from
the pod name via the downward API. No coordination service. (StatefulSets
give stable *identity*, not stable pod IPs; that is fine — the ordinal is
what sharding needs, and each pod having its own IP is what spreads the
source-port tuples in §5.3 regardless of stability across restarts.)

### 5.2 Simulated client lifecycle (one goroutine set per user)

1. Generate an NKey user pair (in memory only).
2. `TokenProvider` supplies auth material; `POST {AUTH_URL}/api/v1/auth`
   with `natsPublicKey` (+ provider fields) → NATS user JWT.
3. `nats.Connect` to `NATS_WS_URL` with the JWT + NKey signature callback.
   Each client holds a **cached JWT**; the UserJWT callback only returns
   the cache — it never mints. Minting happens in exactly one place per
   mode (step 6), so a refresh costs one auth call, not two. This mirrors
   the frontend's `jwtRef` pattern (`useJwtRefresh.js`: the authenticator
   reads a ref that the refresher updates).
4. Subscribe the way the production frontend does
   (`chat-frontend/src/context/RoomEventsContext/useRoomSubscriptions.js`),
   in this order:
   - the user event lane — `subject.UserRoomEvent(account)`
     (`chat.user.{account}.event.room`, DM messages + per-user mutations),
   - the live subscription-update lane —
     `subject.SubscriptionUpdate(account)` — **before** the bootstrap
     walk, so a membership change landing mid-bootstrap is not lost,
   - then bootstrap via the real client RPC
     `subject.UserSubscriptionList(account, siteID)`. This is paginated,
     not one call: request `{"type": "rooms", "offset": N, "limit": 40}`
     (`type` is required — empty is `bad_request`; 40 is the server
     default page), advance `offset` by the sent `limit` while
     `hasMore=true`, and **dedupe rows by `roomId`** (the client-api doc
     states multi-page drains are best-effort ordered; a row can repeat
     across pages). The NATS reply is capped at 128 KB; fixture users sit
     well under the 100+-rooms threshold where the doc redirects to the
     HTTP form, and a `response_too_large` error is treated as a bootstrap
     failure, not retried. Other request fields mirror what the frontend
     actually sends (verified in M1).
   - Open one room subscription per **`roomType == "channel"`** row (the
     frontend opens channel subs only for channels — DM traffic rides the
     user lane) on `chat.room.{roomID}.event` or
     `chat.local.room.{roomID}.event`, selected by the room's `crossSite`
     flag with the frontend's tri-state rule: missing ⇒ `true` (global
     fail-safe, never assume same-site).
   Channel fan-out is published room-scoped, so without this walk a
   simulated client receives no channel traffic at all. Never subscribe
   `chat.room.>`: every client would receive every channel's events and
   the fan-out model would be meaningless. The login-time RPC burst
   during ramp is part of the realism, not an artifact to avoid.

   The subscription set then stays **live**, mirroring the frontend's
   `subscription.update` handling — this is what keeps a long soak honest
   under loadgen membership churn:
   - `added` with `roomType == "channel"` → open the room subscription
     (crossSite from the embedded room view, same tri-state rule);
   - `removed` → close the room subscription immediately (a stale sub on
     a departed room would keep counting deliveries the real user no
     longer gets);
   - an update whose `crossSite` differs from the open sub's namespace →
     close the old namespace's sub, open the new one (never
     double-subscribed);
   - after any reconnect, re-run the bootstrap walk and reconcile
     (unsubscribe rooms that vanished, add rooms that appeared) — events
     during the disconnect window are gone, the walk is the resync.
5. On delivery: increment counters; if the payload parses as a `RoomEvent`
   envelope, record the two latency observations of §8. Payload is then
   dropped — never stored, never logged.
6. JWT lifecycle **defaults to the production frontend's behavior**
   (`useJwtRefresh.js`). Who mints, per mode (always exactly once per
   refresh — the connect callback of step 3 never does):
   - `proactive` (default): a per-client timer at ~80% of the JWT's
     remaining life ±5% jitter mints once, updates the cached JWT, then
     forces a reconnect; the callback presents the already-fresh cache.
   - `expiry` (resilience A/B mode): no timer. The server's expiry
     disconnect triggers reconnect, and the reconnect path re-mints into
     the cache before the callback presents it (the one case where
     reconnect handling mints).
   Runs report which mode was active and the two are never mixed in one
   run.
7. Optional churn: with `CHURN_RATE` > 0, randomly selected clients
   disconnect and rejoin (through the full auth + subscription walk) at
   that rate.

Ramp-up is paced per shard (`RAMP_RATE` connects/sec **per replica**;
cluster-wide rate = `RAMP_RATE × replicas`, stated in run docs) so starting
10k clients measures the steady state, not a self-inflicted thundering herd.

### 5.3 Resource controls (required, not tuning niceties)

- nats.go per-connection defaults do not survive 10k+ connections per
  process: subscription pending limits and the reconnect buffer are config
  knobs with small defaults (512 msgs / 128 KiB pending, reconnect
  buffer ≤ 64 KiB). Flush/ping intervals tuned for many idle conns.
- Room subscriptions use `ChanSubscribe` into ONE shared per-client channel
  drained by a single pump goroutine — async `Subscribe` would cost one
  nats.go dispatcher goroutine per subscription (~35/client, ~350k at 10k
  clients), which alone breaks the per-process target. Only the two
  per-user lanes keep callback subscriptions.
- Memory budget per connection: the two lanes' pending limits plus the
  shared room channel (`SubPendingMsgs` slots) — room count no longer
  multiplies buffer budget.
- Deploy docs must state: `ulimit -n` sizing, and the ~60k ephemeral-port
  ceiling per (srcIP → dstIP:port) tuple — beyond that, add replicas
  (each pod brings its own source IP) or additional NATS endpoint IPs.

## 6. Auth: TokenProvider + load-test side issuer

### 6.1 TokenProvider

```
TokenProvider interface { Material(account string) (authRequestFields, error) }
  devProvider   — dev-mode mint: sends {account, natsPublicKey} only (default)
  fileProvider  — future: account → ssoToken/authToken map, when real tokens
                  become obtainable. Interface point exists; not built now.
```

Key architectural fact making this sound: auth-service is **not a
per-connection callout**. It mints user JWTs signed with
`AUTH_SCOPED_SIGNING_KEY`; the NATS server verifies signatures only. A JWT
minted via the dev branch is indistinguishable on the NATS side from one
minted via SSO — connection-plane fidelity is 100% regardless of token
source. (Botplatform `authToken` was evaluated and rejected: upstream
validation dependency and role-scoped bot JWTs with the wrong permission
shape for simulating humans.)

### 6.2 Side issuer deployment

Every environment points clientsim's `AUTH_URL` at a **dedicated
load-test auth-service instance** with `DEV_MODE=true` and the same
`AUTH_SCOPED_SIGNING_KEY` secret:

- docker-local: one extra compose service in the clientsim overlay.
- k8s test/staging: separate Deployment, ClusterIP only, no ingress,
  NetworkPolicy admitting only the clientsim namespace/pods.

The main auth-service is untouched and never runs dev mode in staging.

### 6.3 Dev-mint guard (small auth-service change) — DEFERRED to a follow-up PR

> **Status:** NOT in the clientsim landing PR. This touches a production
> service (auth-service), so it ships separately. Until it lands, the side
> issuer's dev mode signs **any** account, so the overlay and any k8s
> deployment are safe **only** on throwaway local stacks — a shared
> test/staging cluster MUST NOT run the side issuer without this guard. The
> allowlist references in §4 and §9 assume this PR has landed.

Dev mode today mints a JWT for **any** account — a network-isolation
failure would turn the side issuer into an impersonation oracle for real
staging users. Add two mutually-exclusive optional envs to auth-service
(unset = current behavior, so existing deployments are unaffected):

- `DEV_MODE_ACCOUNT_PREFIX` — mint only accounts with this prefix.
  Local: `user-` matches existing loadgen fixtures unchanged.
- `DEV_MODE_ACCOUNT_ALLOWLIST_FILE` — mint only accounts listed in the
  file. Staging: the file is **generated from the run's pool artifact**
  (§4) and mounted as a ConfigMap/Secret, so a compromised issuer can at
  worst mint the few thousand accounts enrolled in the run — never the
  full user base.

Operational semantics, kept deliberately simple: allowlist changes are
applied by restarting the issuer Deployment with the regenerated mount (no
hot reload); end-of-run revocation is tearing the side issuer down. The
already-minted JWTs then age out on their normal ≤2h expiry.

Defense in depth: NetworkPolicy keeps attackers away from the issuer; the
guard bounds the blast radius if they get there anyway.

`POST /api/v1/auth` is a client-facing auth-service route, so this change
follows the repo's client-facing-handler rule: the same PR updates
`docs/client-api.md` (new rejection case + its `errcode` reason for a
guard miss) alongside the handler tests.

### 6.4 Accepted risk (staging)

On staging, clientsim connects **as borrowed real accounts** and therefore
receives whatever is delivered to their subjects, including any real
traffic those users generate during the run. Mitigations: payloads are
counted and discarded in-process, never persisted or logged (CLAUDE.md
logging rules already forbid message bodies); the borrowed set is the
manifest-curated selection. This extends the existing borrowed-users
decision from "send as them" to "receive as them" — recorded here as an
explicitly accepted part of the staging blast radius.

## 7. Configuration (env, prefix `CLIENTSIM_`)

| Env | Default | Meaning |
|---|---|---|
| `CLIENTSIM_NATS_WS_URL` | required | `ws(s)://` endpoint clients dial |
| `CLIENTSIM_AUTH_URL` | required | side issuer base URL |
| `CLIENTSIM_POOL_FILE` | required | pool artifact path (§4) |
| `CLIENTSIM_SITE_ID` | required | site for `subscription.list` + subjects |
| `CLIENTSIM_TARGET_CONNS` | pool size | `T = min(target, pool)`; partitioned per §5.1 |
| `CLIENTSIM_SHARD_INDEX` / `CLIENTSIM_SHARD_COUNT` | `0` / `1` | replica slice |
| `CLIENTSIM_RAMP_RATE` | `50` | connects/sec **per replica** during ramp |
| `CLIENTSIM_CHURN_RATE` | `0` | reconnect cycles/sec across the shard |
| `CLIENTSIM_JWT_MODE` | `proactive` | `proactive` (frontend-like 80% ±5%) or `expiry` (resilience A/B) |
| `CLIENTSIM_SUB_PENDING_MSGS` / `_BYTES` | `512` / `128KiB` | lane pending limits; msgs also sizes the shared room-delivery channel |
| `CLIENTSIM_RECONNECT_BUF_BYTES` | `64KiB` | nats.go reconnect buffer per conn |
| `CLIENTSIM_PING_INTERVAL` | `2m` | client ping interval (idle-conn keepalive) |
| `CLIENTSIM_METRICS_ADDR` | `:2112` | Prometheus endpoint |

Fail fast on missing required config, per repo convention.

## 8. Observability

Prometheus (primary interface — soak runs are long; scraped by the loadgen
overlay Prometheus and the k8s stack):

- `clientsim_conns_active`, `clientsim_conns_connecting`
- `clientsim_auth_duration_seconds`, `clientsim_connect_duration_seconds` (histograms)
- `clientsim_disconnects_total{reason}`, `clientsim_reconnects_total`
- `clientsim_jwt_refreshes_total{mode}`
- `clientsim_msgs_delivered_total{lane}` (lane: `user` / `channel`)
- `clientsim_broadcast_to_client_latency_seconds` — receive −
  `RoomEvent.Timestamp`. `Timestamp` is stamped `time.Now()` by
  broadcast-worker at client-event build (`buildRoomEvent`), so this is
  **broadcast publish → client edge**, named accordingly.
- `clientsim_canonical_to_client_latency_seconds` — receive −
  `RoomEvent.EventTimestamp` (the upstream canonical event's publish
  time), the longest span measurable from the envelope alone.
- Loss visibility (mandatory, so received-only histograms can't
  silently survive a drop storm with a *better* p99):
  - slow-consumer **episodes** via the existing
    `pkg/natsutil` helper (`clientsim_slow_consumer_events_total`) — the
    nats.go async error callback fires once per Active→SlowConsumer
    transition, not per dropped message, and `Subscription.Dropped()` is
    a lifetime cumulative (the helper's comment documents the
    double-count trap); exact dropped-message counts, if ever needed, are
    a separate cumulative-delta sampling of `Dropped()`, never
    callback-additions.
  - `clientsim_decode_failures_total`,
    `clientsim_invalid_timestamp_total` (zero/negative observed age).
  - Any increment on these during a measurement window marks that
    window's latency/delivery numbers **degraded** in the run summary —
    they are not silently averaged in.

Both latency histograms span hosts; runs must note that they carry
inter-host clock skew and are for trend/regression comparison, not
absolute truth. `clientsim_msgs_delivered_total` counts per-connection
fan-out copies — a different unit from loadgen's logical send counters —
so the two sit side by side as diagnostics and are never divided into a
"loss ratio" (§4). True end-to-end per-message accounting (eligible /
received / missing verdicts, `LastMsgID` correlation against the send
ledger) remains loadgen's soak-verdict domain (§3).

On shutdown, print a loadgen-style summary (counts, p50/p95/p99,
disconnect reasons, drop totals).

## 9. Deployment

- `tools/clientsim/deploy/`: `Dockerfile` (repo multi-stage convention,
  context = repo root), `docker-compose.yml` overlay joining `chat-local`
  (clientsim + the dev-mode side issuer), `azure-pipelines.yml`.
- k8s (test + staging): clientsim **StatefulSet** (ordinal → shard index),
  side issuer Deployment + ClusterIP + NetworkPolicy, ConfigMap/Secret for
  the pool artifact and allowlist. Manifests live wherever the clusters'
  existing service manifests live (ops repo or `deploy/` — follow current
  practice at implementation time).

## 10. Milestones

**M1 — walking skeleton, deployed to the test cluster early.** Auth exchange
→ WSS connect → user event lane + `subscription.list` + channel subs →
`conns_active` + per-lane delivery counters; a few hundred connections;
side issuer with the prefix guard; seed emits the pool artifact; run
overnight on the test cluster. M1 deliberately front-loads the only real
unknowns, none of which are answerable locally:

- ingress/LB idle-timeout behavior on long-lived mostly-idle WSS conns,
- LB per-backend/per-IP connection ceilings,
- source-port exhaustion shape,
- whether the minted JWT's permissions cover the full client subscription
  walk (`subscription.list` request + per-room `chat.room.{roomID}.event`
  / `chat.local.room.{roomID}.event` subjects) exactly as the frontend
  exercises it.

**M2 — full tool.** Sharding, ramp pacer, churn, JWT expiry A/B mode,
live `subscription.update` maintenance + reconnect resync (§5.2.4),
allowlist guard + staging pool artifact, latency histograms + loss
counters, resource-limit knobs, summary report, staging deployment.

## 11. Testing

TDD throughout (repo rule).

- Unit: `TokenProvider` + auth exchange against `httptest` servers shaped
  like auth-service; pool parsing + shard partition math (property: shards
  partition exactly `T`, no overlap, no overshoot); lifecycle state
  machine with an injected connect function; subscription-walk planning
  from canned `subscription.list` pages (pagination loop, `hasMore`
  advance, cross-page `roomId` dedupe, channel-only filter, crossSite
  tri-state); live-update reconciliation (added/removed/namespace flip,
  post-reconnect resync); JWT cache single-mint invariant per mode;
  envelope timestamp extraction (zero/negative cases).
- auth-service guard change: table-driven handler tests (prefix hit/miss,
  allowlist hit/miss, both-set config error, unset = unchanged behavior).
- Pool artifact: seeder-side tests that local seed (incl. `--users`
  override) and the staging ID→account resolution write the same schema.
- Integration (`//go:build integration`): clientsim is the **second**
  package needing a WebSocket-enabled NATS container (after
  `pkg/roomkeysender`), so per the testutil rule the shared helper moves
  to `pkg/testutil` (`Xxx(t)` + `EnsureXxx` + `TerminateXxx` shape) as
  part of this work, with `roomkeysender` migrated onto it. Covers:
  connect over ws with a JWT minted by a locally-signed test key,
  subscription walk against stubbed replies, receive, count, drop
  accounting.

## 12. Future work (explicit non-blockers)

- `fileProvider` for real SSO/botplatform tokens if they become obtainable.
- A loadgen-ledger export of **expected recipient deliveries for the
  clientsim pool** (per lane, per window), giving the delivered counters a
  compatible denominator for true loss accounting — until then the
  cross-tool comparison stays diagnostic-only (§8).
- Read-receipt / presence emission if a future workload needs closed-loop
  client behavior (would need a fresh scope discussion — it blurs the
  loadgen boundary on purpose today).
- Main-path auth-service capacity workload (separate tool/workload; out of
  scope here by design).
