# notification-worker — Downstream Contracts

This document specifies the contracts the `notification-worker` overhaul
([PR #237](https://github.com/hmchangw/chat/pull/237)) establishes for two
**internal-codebase** services (the push-notification service and the presence
service) plus the ops/IaC provisioning required to run the worker in
production.

`notification-worker` is the **producer** for both contracts. It does not
implement either consumer. Until the consumers land, the worker runs in a
safe degraded mode (see each section).

---

## 1. Push-notification service (mobile push delivery)

**Status:** required for any mobile push to be delivered. Until the push
service consumes the stream, push events accumulate / are dropped per the
stream's retention policy — the worker publishes and moves on.

### Transport

| Property | Value |
|---|---|
| Stream | `PUSH-NOTIFICATION-{siteID}` |
| Bound subject filter | `chat.server.notification.push.{siteID}.>` |
| Publish subject (current leaf) | `chat.server.notification.push.{siteID}.send` |
| Namespace | `chat.server.*` — server-only; client JWTs have no subscribe permission |
| Delivery model | fire-and-forget async publish; durability via JetStream PubAck |
| Granularity | one event per **batch of up to `PUSH_RECIPIENT_BATCH_SIZE`** recipients (default `100`, configurable per deploy) |
| Payload encoding | JSON, **uncompressed** (raw `application/json`); consumers `json.Unmarshal(msg.Data, …)` directly — **no `Content-Encoding` header is set** |
| Stream storage compression | `S2` — transparent server-side; the only compression layer (publisher sends raw JSON) |

The `.send` leaf is the only current event type; the `.>` filter leaves room
for future siblings (`.silent`, `.priority`) without restructuring the stream.

### Event schema

`PushNotificationEvent` (JSON; `pkg/model/push.go`). The wire payload is raw JSON
(no compression); the shape is:

```json
{
  "id": "{messageId}-b{batchIndex}",
  "accounts": ["alice", "bob", "carol"],
  "title": "",
  "body": "Alice Wang 愛麗絲 please review",
  "data": {
    "roomId": "r123",
    "messageId": "m456",
    "type": "c",
    "sender": { "account": "bob", "userId": "u-bob", "displayName": "Bob Chen 陳大寶" },
    "threadMessageId": "",
    "fileName": "",
    "fileType": "",
    "parentRoomId": "",
    "pushTime": "2026-05-28T00:00:00Z",
    "alsoSendToChannel": false
  },
  "roomId": "r123",
  "timestamp": 1700000000000,
  "unreadCounts": { "alice": 3, "carol": 9 }
}
```

Field notes:

- **`id`** = `{messageId}-b{batchIndex}` (zero-based). Also set as the `Nats-Msg-Id` header — see Dedup. Sorting survivors lexicographically before chunking makes batch *ordering* deterministic across redeliveries; it does not by itself guarantee the same accounts land in the same `batchIndex` on every redelivery — see the caveat under Dedup.
- **`accounts`** = recipient accounts in this batch, lexicographically sorted, capped by `PUSH_RECIPIENT_BATCH_SIZE` (default 100). The push service iterates this list, resolves device tokens per account, and is expected to use the provider's native multicast (e.g. FCM `send_each_for_multicast` — up to 500 tokens per call) so one batch becomes one outbound HTTP request.
- **`title`** is resolved by the worker so push-service needs no DB lookup. The rule mirrors the legacy implementation: `room.Name` if present, otherwise the sender's account (DM rooms have no name). Room metadata is served from an LRU+TTL cache (`pkg/roommetacache`) sized via `ROOM_META_CACHE_SIZE` / `ROOM_META_CACHE_TTL`.
- **`body`** is the message content with `@mentions` rendered for humans, **untruncated**. The push service should truncate to the APNs/FCM payload limit (~4 KB total) before delivery. (Truncation/PII-scrubbing on the worker side is tracked as follow-up.) Mention substitution (`pkg/mention.ReplaceAccounts`) drops the `@` marker and rewrites each token:
  - a known account → its display name, composed with the same `pkg/displayfmt.CombineWithFallback(engName, chineseName, …)` rule as `data.sender.displayName` (`@alice` → `Alice Wang 愛麗絲`);
  - `@all` → `All`, `@here` → `here` — fixed words, matched case-insensitively, never a DB lookup;
  - anything unresolved — unknown account, user with neither name, failed lookup — keeps its literal `@token`, so push-service may still see raw `@account` text and must not assume substitution succeeded.

  At most **50 distinct accounts** per message are resolved (`maxMentionLookups`); tokens past that cap keep their raw `@token`, bounding a user-controlled fan-out.

  Names come from the `users` collection through an LRU+TTL cache (`pkg/userstore`, `USER_CACHE_SIZE` / `USER_CACHE_TTL`), bounded by `MENTION_NAMES_TIMEOUT` and gated by `MENTION_NAMES_ENABLED`. A renamed user can therefore show a stale name for up to `USER_CACHE_TTL`. The lookup runs once per message, only when the content has mentions **and** at least one recipient survived the filters, and it **fails open**: a Mongo error or timeout sends the unsubstituted body rather than dropping the push. Substitution is not idempotent or reversible — consumers must treat `body` as display text, not as a parseable mention source.
- **`data.type`** is the short room type: `"c"` channel, `"d"` DM/botDM, `"p"` discussion.
- **`data.sender`** is a `Participant` carrying `account`, `userId`, and `displayName`. **`displayName` is pre-composed by `message-gatekeeper`** at canonical-message write time via `pkg/displayfmt.CombineWithFallback(engName, chineseName, account)` (same helper already used by `room-worker/sysmsg.go`, `room-service/store_mongo.go`, and reaction rendering — one source of truth for display formatting across the system). The composition happens once per message regardless of downstream consumer count and never on the push hot path; push-service renders `sender.displayName` verbatim. Empty `displayName` (legacy in-flight canonical messages predating the field) falls back to `sender.account` in `notification-worker`. `engName` / `chineseName` are deliberately not propagated on the push event since the composed string is the only render-time input.
- **`timestamp`** is event publish time (UnixMilli); **`data.pushTime`** is the RFC3339 domain send time. They are distinct fields.
- **`unreadCounts`** (optional) is per-recipient badge counts stamped at notify time — see § Badge counts below. Omitted entirely (not an empty object) when the badge phase is disabled or produced no counts for this batch.

Mention resolution emits no metric of its own. A lookup that errors or times out is reported by the `mention name lookup failed, body keeps raw mentions` warn line (trace-correlated, carries both the requested and the resolved count); the message itself still records as `notification_worker_outcomes_total{result="sent"}`, since the push is delivered either way. A failed lookup does **not** imply the whole body is unsubstituted: `Resolve` returns the names it did collect alongside the error and those are still applied, so an affected body may contain raw `@tokens` for some, all, or none of its mentions — compare the two counts on the warn line to tell which.

### Badge counts (`unreadCounts`)

`PushNotificationEvent.UnreadCounts` (`map[string]int`, `json:"unreadCounts,omitempty"`, `pkg/model/push.go`)
rides along with the push so the push-service/client can render the app badge
without a separate round trip.

- **Shape:** `account → count`, restricted per event to the accounts in that
  event's `accounts` batch (each batch carries only its own recipients' counts).
- **Cap:** count is the number of distinct unread rooms for that account,
  capped at **10** by default (`BADGE_COUNT_CAP` on user-service; the client
  renders the capped value as `"9+"`).
- **Absence semantics:** an account **missing** from the map means its count
  could not be computed this time (its home-site RPC failed, timed out, or the
  phase is disabled) — the push is still delivered. Clients should not treat
  absence as "zero unread"; fall back to the locally-cached badge value and
  let it refresh on next app open.
- **Gate:** populated by default (notification-worker's `BADGE_COUNT_RPC_ENABLED`, default `true`); set `false` to disable
  (see §3). When disabled, `BadgeClient` is nil and the badge
  phase is skipped entirely — `unreadCounts` never appears on the wire.
- **Fetch path:** per push, the worker groups the badge audience by home site and issues
  one `badge.count.batch` RPC per site concurrently, merging replies into one
  map before per-batch filtering (`notification-worker/handler.go`,
  `fetchUnreadCounts`).
- **Home-site resolution:** a member's home site is read from the **users**
  collection (`users.siteId`) by the member-cache loader at cache-fill time —
  one batch `$in` query per room per `ROOMSUBCACHE_TTL`, cached on
  `roomsubcache.Member.HomeSiteID`. `Subscription.siteId` is deliberately not
  used: it is the **room's** home site, identical for every member at the
  room's own site, and would route every RPC locally. A member missing from
  the users collection degrades (no RPC on its behalf, push still delivered).

#### `badge.count.batch` RPC (user-service)

| Property | Value |
|---|---|
| Subject | `chat.server.request.user.{siteID}.badge.count.batch` |
| Pattern | NATS request/reply, server-to-server (`chat.server.*` — no client JWT can subscribe) |
| Caller | `notification-worker`, one request per survivor home site per push (§ above) |
| Callee | `user-service` (`BadgeCountBatch`, backed by `pkg/badgecache`) |

Request / reply (`pkg/model/subscription.go`):

```json
// request
{ "roomId": "r123", "accounts": ["alice", "bob"] }

// reply
{ "counts": { "alice": 3, "bob": 9 } }
```

- **`roomId`** is the room whose message triggered the notification —
  `user-service` SADDs it into the account's unread-room set atomically with
  reading the set's size, so the triggering room is always reflected even on
  a cache miss.
- **`counts`** maps account → unread-room count, capped (`BADGE_COUNT_CAP`,
  default 10). An account
  absent from `counts` means its count could not be computed (see the
  degrade path below); the caller logs and drops it rather than failing.
- Per-account degrade path, in order: cache hit (`BumpBatch`, one pipelined
  Valkey `SADD`+`SCARD` round per cluster node for the whole batch)
  → cache miss (`Seed` from Mongo + cross-site room RPCs, unioned with the
  trigger room) → cache down entirely (`cappedUnion`, computed without Valkey).
  Only a hard per-account error (e.g. Mongo failure while reseeding) drops the
  account from the response.
- On timeout / no responder / a remote `errcode` envelope, `notification-worker`
  treats that whole site's batch as failed; that site's accounts are simply
  absent from `UnreadCounts` — see § Badge counts absence semantics above. A
  badge-count failure never NAKs or delays the push.

**Accuracy model:** the triggering room is exact at notify time — the RPC's
`SADD` is atomic with the size read. The set is maintained on both edges:
every message bumps the **full badge audience** (all members past the
sender/muted/restricted/thread-scope filters — including members who won't be
pushed), and a read removes exactly the room read (`ClearRoom`) — a room with
unread followed threads stays counted, so the cache is left untouched in that
case; a mute-on transition and a member-removal are the same exact `ClearRoom`. Thread-read,
`thread.read.all`, and unmute still drop the account's whole set (`ClearAll`,
plus its `badge:fresh` marker), since their post-state is genuinely ambiguous.
Drift is now bounded by the freshness marker's own TTL
(`BADGE_MARKER_TTL`, user-service only) rather than the set's TTL
(`BADGE_CACHE_TTL`): the marker is stamped only by the Mongo-verifying
seed/reseed paths and is never refreshed by a bump, so its expiry is the
upper bound on how long the cached set can go unverified before the next
count or push recomputes from Mongo and re-stamps it. A degraded computation
— a cross-site `GetRoomsMeta` RPC failed and that site's rooms
were dropped — is never cached: the best-effort count is still returned, but
the marker is not stamped, so a knowingly-incomplete set is never blessed as
verified. Failure direction is undercount-until-recompute, never a stuck
overcount. `subscription.count` (unread=true) may itself be served from this
set when user-service's `BADGE_COUNT_CACHE_FIRST` is enabled — flip it to true
only after all badge writers run the marker-aware `pkg/badgecache`, or an old
writer's set-only clear can leave a marker reading as a stale "fresh zero".

### Payload decoding

The publisher sets only `Content-Type: application/json`; **no `Content-Encoding`
header is set** and payloads are never compressed. Consumers `json.Unmarshal(msg.Data, …)`
directly — no gzip-aware decoder is needed.

> **Push-notification service — required update.** Earlier revisions of this
> contract had the worker publish gzip-compressed payloads, and the push service
> decoded via `natsutil.DecodePayload(msg)`. That helper has been removed.
> Consumers MUST switch to `json.Unmarshal(msg.Data, &evt)` directly and drop any
> `Content-Encoding`/gzip branching. Until this update lands, the push service
> will fail to decode events published by the new worker.

### Payload size cap

The wire payload is bounded by the broker's `max_payload`. The worker reads the
cap from the broker's own INFO on connect (`nc.NatsConn().MaxPayload()`) and
**rejects any batch whose JSON wire size exceeds it before publishing** — the
emitter surfaces a clear `exceeds NATS max_payload` error instead of letting the
broker NACK with a less informative one. The `PUSH_RECIPIENT_BATCH_SIZE=100`
default leaves a wide margin under the broker's typical 256 KiB `max_payload`
for typical recipient/metadata sizes; the cap exists as a last-resort guard
against pathological events (huge bodies, oversized metadata). No env var
configures this — it always tracks the connected broker.

### Routing predicate notes

- **`@here` is not a push trigger.** The worker treats `@all` as the broad-mention
  signal; `@here` is parsed but not acted upon, because the current frontend does
  not render `@here` mentions. A large-room message containing only `@here` will
  result in zero push events.
- **`@all`** still bypasses the large-room throttle and the thread-follower gate.

### Schema departures from the legacy push payload

The push service must read the new tag names (one coordinated cutover — there
is no dual old/new support, since `PUSH-NOTIFICATION-{siteID}` is a new stream
with no prior consumer):

| Legacy | New |
|---|---|
| `rid` | `roomId` |
| `tmid` | `threadMessageId` |
| `prid` | `parentRoomId` |
| flat `chineseName` / `engName` | nested `sender` object (`Participant`) — push-service reads `sender.displayName` (pre-composed at message-gatekeeper via `pkg/displayfmt.CombineWithFallback`) and renders it verbatim. `sender.account` remains as the final fallback when `displayName` is empty (only possible for legacy in-flight messages predating the field). |
| (none) | `timestamp` (event-level UnixMilli) added |

### Dedup

Dedup here protects against **upstream re-emit only** — push-service uses
`MaxDeliver=1` and ack-first (see § Consumer guidance), so it never causes
redelivery itself. The case dedup covers: `notification-worker` NAKs a
canonical message (emit error after retries), JetStream redelivers the
canonical event, the worker re-runs fan-out and re-publishes push events
with the same content.

The worker sets the JetStream `Nats-Msg-Id` header to `{messageId}-b{batchIndex}`.
Sorting survivors before chunking makes the batch *order* deterministic across
redeliveries, and for an unchanged batch the same `Nats-Msg-Id` still causes
JetStream to drop the duplicate at the stream. For this to suppress duplicate
pushes, the **stream's dedup window must be ≥ the canonical consumer's
redelivery horizon**:

```text
dedup_window  ≥  AckWait × MaxDeliver  =  30s × 5  =  150s   (defaults)
```

Set the `PUSH-NOTIFICATION-{siteID}` `Duplicates` window to a safe margin
above 150s (e.g. 5 min). If the window is shorter, a canonical-message
redelivery (after a worker NAK) can produce a duplicate push.

> **Known accepted risk — survivor set is not guaranteed stable across
> redeliveries.** `batchIndex` assignment depends on the sorted survivor list,
> and survivorship now depends on a live user-settings read (`usersettings.go`)
> that fails open and can change between a NAK and its redelivery (a setting
> read that failed the first time can succeed the second, or the user can
> change a setting in between). If the survivor count shifts between
> attempts, later batches shift with it: e.g. 250 survivors at batch size 100
> — attempt 1 publishes `b0`/`b1` and fails on `b2`, so the message is NAKed;
> attempt 2 resolves one more account (now 249 survivors), shifting what was
> `b2`'s first account into `b1`. Re-publishing `b1` carries the same
> `Nats-Msg-Id` as before, so JetStream drops it as a duplicate — and the
> shifted-in account never receives its push. A content-derived `Nats-Msg-Id`
> would close this gap; that is a contract change tracked separately, not
> addressed here.

### Consumer guidance

**Delivery semantics: at-most-once.** Push-service MUST ack the JetStream
message **on receipt**, before any provider HTTP call, and MUST NOT NAK or
trigger redelivery on provider failure. Rationale: a duplicate push is
user-visible spam; a missed push on transient provider failure is invisible
and bounded by the per-recipient HTTP retry below.

- Use a durable consumer named after the push service.
- **Ack first.** Call `msg.Ack()` immediately after the payload decodes
  cleanly — before fanning out to FCM/APNs. Provider outcomes do not affect
  ack.
- **Set `MaxDeliver=1`** on the durable consumer. There is no upstream retry
  semantics worth preserving here; the stable `Nats-Msg-Id` already protects
  against `notification-worker` re-emit on canonical redelivery (see § Dedup).
- **`AckWait` can be tight** (e.g. `5s`). Ack happens within milliseconds of
  receipt because no I/O blocks it; the wider default just causes slow
  shutdowns on stuck pods.
- **HTTP retry per recipient: up to 2 attempts** with exponential backoff
  (e.g. `100ms`, `400ms`). On terminal failure, **log and drop** — no
  bookkeeping, no DLQ, no provider-side state machine. A structured log line
  with `account`, `provider`, `status_code`, `error`, `messageId`, `batchId`
  is enough for ops triage; aggregate alarming should fire on **error rate**,
  not individual misses.
- Treat each event as a fan-out unit: iterate `accounts`, resolve device
  tokens, and prefer a single multicast HTTP request per batch over
  per-recipient calls (FCM `send_each_for_multicast` accepts up to 500 tokens;
  one batch = one HTTP).
- A push for a bot account never arrives (the worker filters bots), so no
  bot-device handling is needed.
- Decode the payload via `json.Unmarshal(msg.Data, &evt)` directly — payloads
  are raw JSON with no `Content-Encoding`.

**Why no NAK / no MaxDeliver > 1**: the only failure modes that would benefit
from JetStream redelivery are (a) the push-service pod crashing before ack —
solved by acking immediately, and (b) provider being down — best handled by
a per-recipient HTTP retry that's bounded in wall time, not by re-running the
entire push fan-out which would duplicate pushes for recipients that did
succeed on the first pass.

---

## 2. Presence service (DND gating)

**Status:** optional but recommended. The worker ships with
`PRESENCE_RPC_ENABLED=false` and a no-op snapshotter, so **every push-eligible
recipient is pushed regardless of presence** (fail-open). Implementing this RPC
enables busy/in-call (DND) suppression. Flip `PRESENCE_RPC_ENABLED=true` once
it's live.

### Transport

| Property | Value |
|---|---|
| Subject | `chat.presence.{siteID}.request.snapshot` |
| Pattern | NATS request/reply |
| Cardinality | one request per canonical message (the worker chunks large account sets — see below) |

### Request / reply schema

`PresenceSnapshotRequest` → `PresenceSnapshotReply` (`pkg/model/presence.go`):

```json
// request
{ "accounts": ["alice", "bob", "carol"] }

// reply
{
  "presences": {
    "alice": { "aggregatedStatus": "online" },
    "bob":   { "aggregatedStatus": "busy" }
  }
}
```

- **`aggregatedStatus`** is the single field the worker reads. The presence
  service must **fold manual user overrides into this field** (the worker does
  no override logic). One of: `online`, `offline`, `away`, `busy`, `in-call`.
- An account **absent from the reply map** is treated fail-open (pushed).
- On error, reply with the repo-standard `model.ErrorResponse`
  (`{"error": "...", "code": "..."}`) via `natsutil.ReplyError`. The worker
  detects this envelope, logs it, and fails open for that chunk.

### Status → push decision — presence alone (worker-side, for reference)

Presence is one of two independent suppressors the worker evaluates, not the
whole push decision. The table below is what presence alone implies; the
worker also consults the recipient's stored notification settings
(`notification-worker/usersettings.go`), which can move the outcome either
way: `muteAllNotifications` can suppress a push at **any** status (including
`online`), and `showNotificationsInCall` can allow one through at `busy` /
`in-call`. See `shouldPush` (`notification-worker/presence.go`) for the
combined gate.

| `aggregatedStatus` | Push? (presence alone) | Rationale |
|---|:--:|---|
| `online`  | yes | multi-device — push fires alongside the client desktop banner |
| `offline` | yes | not connected — reach by push |
| `away`    | yes | idle, not DND — fail-open |
| `busy`    | **no** | Do-Not-Disturb |
| `in-call` | **no** | treated as DND (mirrors Teams in-meeting muting) |
| absent / RPC error | yes | fail-open — never drop on a presence gap |

### Chunking / sizing

For an `@all` to a very large room the survivor set can be thousands of
accounts. The worker splits the request across several concurrent RPCs at
`PRESENCE_BATCH_SIZE` (default 512) so each request/reply stays under the NATS
max message size, then merges replies. The presence service should size its
handler to answer a single ~512-account request comfortably; it does **not**
need to handle one giant request.

The worker does **not** read the presence service's storage directly — the RPC
is the only coupling, so the presence service's Valkey/storage migration is
invisible to the worker.

---

## 3. Ops / IaC provisioning

Required before a production rollout:

1. **Provision `PUSH-NOTIFICATION-{siteID}`** (the worker only bootstraps it
   in dev via `BOOTSTRAP_STREAMS=true`; in prod `BOOTSTRAP_STREAMS=false` and
   the worker only publishes). Set:
   - Subjects: `chat.server.notification.push.{siteID}.>`
   - `Duplicates` (dedup) window ≥ ~5 min (see §1 Dedup)
   - Retention/limits per the push service's drain rate
2. **`subscriptions.roomType`** — already populated by `room-service`; the
   worker reads it for routing. No action unless a site predates the field.
3. **`thread_subscriptions` `(parentMessageId, userAccount)` index** — the
   worker ensures it idempotently at startup (bounded by
   `INDEX_ENSURE_TIMEOUT`, default 2 min). On a large existing collection,
   pre-create it so the first boot isn't slowed; otherwise no action.
4. **New env vars** (see `notification-worker/deploy/docker-compose.yml` for
   dev values):
   - `VALKEY_ADDRS` (**required**, comma-separated cluster seeds), `VALKEY_PASSWORD`
   - `ROOMSUBCACHE_TTL` (default `5m`) — TTL for the Valkey room-member cache; no in-process L1 (per-pod memory bounded against very large rooms)
   - `LARGE_ROOM_THRESHOLD` (default `500` — same knob as message-gatekeeper)
   - `PUSH_RECIPIENT_BATCH_SIZE` (default `100` — recipients per push event; tune toward provider multicast caps)
   - `ROOM_META_CACHE_SIZE` (default `10000`), `ROOM_META_CACHE_TTL` (default `2m`) — fronts `rooms` collection lookups for title resolution
   - `USER_CACHE_SIZE` (default `10000`), `USER_CACHE_TTL` (default `5m`) — LRU+TTL cache fronting the `users` collection for mention display names
   - `MENTION_NAMES_ENABLED` (default `true`) — kill switch for mention display-name resolution; `false` skips the users lookup entirely and only `@all`/`@here` are substituted
   - `MENTION_NAMES_TIMEOUT` (default `2s`) — bounds the mention display-name lookup; on expiry the body ships with raw `@tokens`
   - `PUSH_ASYNC_MAX_PENDING` (default `1024`)

   `message-gatekeeper` owns the **sender** display-name resolution
   (`data.sender.displayName`), composed once at canonical-message write time;
   configure its `USER_CACHE_SIZE` / `USER_CACHE_TTL` there.
   `notification-worker` makes three users-collection reads of its own, which
   consciously revises the original "no users-collection lookups" contract:
   - the member-cache loader's batched **home-site** lookup (one `$in` per room
     per `ROOMSUBCACHE_TTL` — see §1 Badge counts, Home-site resolution), since
     badge RPCs must route to each recipient's home site and only the users
     collection knows it;
   - **notification settings**, primary-pinned so a just-muted user is not
     notified (see `USER_SETTINGS_*`);
   - **mention** display names for `body`, at the default read preference (a
     renamed user tolerates replica lag), cached per `USER_CACHE_SIZE` /
     `USER_CACHE_TTL` and skipped entirely for messages without mentions.

   None of the three is a per-recipient read: each is batched or cached.
   - `INDEX_ENSURE_TIMEOUT` (default `2m`)
   - `PRESENCE_RPC_ENABLED` (default `false`), `PRESENCE_BATCH_SIZE` (`512`), `PRESENCE_RPC_TIMEOUT` (`2s`)
   - `BADGE_COUNT_RPC_ENABLED` (default `true`) — gates the `badge.count.batch` RPC to each recipient's home-site `user-service`; set `false` to disable badge stamping (see §1 Badge counts)

---

## 4. Optional — veto hook

The worker exposes an in-process `Vetoer` (Stage 2, suppress-only) that ships
as `noopVetoer` (allows all). If the team has notification-suppression rules,
implement a real `Vetoer`:

- Signature: `Allow(ctx, *model.Message, roomsubcache.Member) (bool, error)`
- It runs **once per recipient in-process** — any external data it needs must
  be **batch-loaded once per message** before the per-recipient loop, never
  fetched per recipient.
- On error the worker logs and fails open (allows).

---

## Rollout sequencing (suggested)

1. Land this PR; deploy the worker with `PRESENCE_RPC_ENABLED=false`. No pushes
   are delivered yet (push service not consuming) — safe.
2. Provision the `PUSH-NOTIFICATION-{siteID}` stream (§3.1).
3. Ship the push service consumer (§1). Mobile push now flows; presence gating
   is still fail-open (everyone eligible is pushed).
4. Ship the presence RPC handler (§2), then flip `PRESENCE_RPC_ENABLED=true`.
   DND suppression now active.
