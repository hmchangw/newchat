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
| Stream | `PUSH_NOTIFICATIONS_{siteID}` |
| Bound subject filter | `chat.server.notification.push.{siteID}.>` |
| Publish subject (current leaf) | `chat.server.notification.push.{siteID}.send` |
| Namespace | `chat.server.*` — server-only; client JWTs have no subscribe permission |
| Delivery model | fire-and-forget async publish; durability via JetStream PubAck |
| Granularity | one event per **batch of up to `PUSH_RECIPIENT_BATCH_SIZE`** recipients (default `100`, configurable per deploy) |
| Payload encoding | JSON, **gzip-compressed**; consumers must read `Content-Encoding: gzip` header and decompress before `json.Unmarshal` |
| Stream storage compression | `S2` — transparent server-side, layered with gzip on top for inter-replica + on-disk savings |

The `.send` leaf is the only current event type; the `.>` filter leaves room
for future siblings (`.silent`, `.priority`) without restructuring the stream.

### Event schema

`PushNotificationEvent` (JSON; `pkg/model/push.go`). The wire payload is gzip-compressed
(see § Payload decoding); the shape after decompression is:

```json
{
  "id": "{messageId}-b{batchIndex}",
  "accounts": ["alice", "bob", "carol"],
  "title": "",
  "body": "the message content",
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
  "timestamp": 1700000000000
}
```

Field notes:

- **`id`** = `{messageId}-b{batchIndex}` (zero-based). Also set as the `Nats-Msg-Id` header — see Dedup. `batchIndex` is stable across redeliveries because the worker sorts survivors lexicographically before chunking.
- **`accounts`** = recipient accounts in this batch, lexicographically sorted, capped by `PUSH_RECIPIENT_BATCH_SIZE` (default 100). The push service iterates this list, resolves device tokens per account, and is expected to use the provider's native multicast (e.g. FCM `send_each_for_multicast` — up to 500 tokens per call) so one batch becomes one outbound HTTP request.
- **`title`** is resolved by the worker so push-service needs no DB lookup. The rule mirrors the legacy implementation: `room.Name` if present, otherwise the sender's account (DM rooms have no name). Room metadata is served from an LRU+TTL cache (`pkg/roommetacache`) sized via `ROOM_META_CACHE_SIZE` / `ROOM_META_CACHE_TTL`.
- **`body`** is the raw message content, **untruncated**. The push service should truncate to the APNs/FCM payload limit (~4 KB total) before delivery. (Truncation/PII-scrubbing on the worker side is tracked as follow-up.)
- **`data.type`** is the short room type: `"c"` channel, `"d"` DM/botDM, `"p"` discussion.
- **`data.sender`** is a `Participant` carrying `account`, `userId`, and `displayName`. **`displayName` is pre-composed by `message-gatekeeper`** at canonical-message write time via `pkg/displayfmt.CombineWithFallback(engName, chineseName, account)` (same helper already used by `room-worker/sysmsg.go`, `room-service/store_mongo.go`, and reaction rendering — one source of truth for display formatting across the system). The composition happens once per message regardless of downstream consumer count and never on the push hot path; push-service renders `sender.displayName` verbatim. Empty `displayName` (legacy in-flight canonical messages predating the field) falls back to `sender.account` in `notification-worker`. `engName` / `chineseName` are deliberately not propagated on the push event since the composed string is the only render-time input.
- **`timestamp`** is event publish time (UnixMilli); **`data.pushTime`** is the RFC3339 domain send time. They are distinct fields.

### Payload decoding

The publisher sets `Content-Encoding: gzip` and `Content-Type: application/json` on
every event. Consumers must read the header and gunzip before `json.Unmarshal`. The
shared helper `pkg/natsutil.DecodePayload(*nats.Msg)` implements this — it returns
`msg.Data` verbatim for absent/`identity` encoding, gunzips for `gzip`, and errors
loudly on any other encoding to keep silent mis-parses out.

### Payload size cap

The wire payload (after gzip) is bounded by the broker's `max_payload`. The worker
reads `NATS_MAX_PAYLOAD_BYTES` (default `262144` = 256 KiB) and **rejects any
gzipped batch larger than the cap before publishing** — the emitter surfaces a
clear `exceeds NATS max_payload` error instead of letting the broker NACK with a
less informative one. The `PUSH_RECIPIENT_BATCH_SIZE=100` default leaves a wide
margin under 256 KiB for typical recipient/metadata sizes; the cap exists as a
last-resort guard against pathological events (huge bodies, oversized metadata).

Set `NATS_MAX_PAYLOAD_BYTES` to match your broker's configured `max_payload`. The
push service should decode with `natsutil.DecodePayloadWithLimit(msg, <same value>)`
(or rely on the default 256 KiB) so the gzip-bomb defense matches the producer's
commitment.

### Routing predicate notes

- **`@here` is not a push trigger.** The worker treats `@all` as the broad-mention
  signal; `@here` is parsed but not acted upon, because the current frontend does
  not render `@here` mentions. A large-room message containing only `@here` will
  result in zero push events.
- **`@all`** still bypasses the large-room throttle and the thread-follower gate.

### Schema departures from the legacy push payload

The push service must read the new tag names (one coordinated cutover — there
is no dual old/new support, since `PUSH_NOTIFICATIONS_{siteID}` is a new stream
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
Batches are content-stable across redeliveries because the worker sorts
survivors before chunking, so the same canonical message always produces the
same `Nats-Msg-Id` set and JetStream drops the duplicates at the stream. For
this to suppress duplicate pushes, the **stream's dedup window must be ≥ the
canonical consumer's redelivery horizon**:

```text
dedup_window  ≥  AckWait × MaxDeliver  =  30s × 5  =  150s   (defaults)
```

Set the `PUSH_NOTIFICATIONS_{siteID}` `Duplicates` window to a safe margin
above 150s (e.g. 5 min). If the window is shorter, a canonical-message
redelivery (after a worker NAK) can produce a duplicate push.

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
- Decode the payload via `natsutil.DecodePayload(msg)` (or equivalent
  gzip-aware decoder); never `json.Unmarshal(msg.Data, …)` directly.

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

### Status → push decision (worker-side, for reference)

| `aggregatedStatus` | Push? | Rationale |
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

1. **Provision `PUSH_NOTIFICATIONS_{siteID}`** (the worker only bootstraps it
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
   - `NATS_MAX_PAYLOAD_BYTES` (default `262144` = 256 KiB — must match broker `max_payload`; see §1 Payload size cap)
   - `ROOM_META_CACHE_SIZE` (default `10000`), `ROOM_META_CACHE_TTL` (default `2m`) — fronts `rooms` collection lookups for title resolution
   - `PUSH_ASYNC_MAX_PENDING` (default `1024`)

   `message-gatekeeper` owns the sender display-name resolution; configure its
   `USER_CACHE_SIZE` / `USER_CACHE_TTL` (defaults 10000 / 5m) there.
   `notification-worker` does **no** users-collection lookups under this design.
   - `INDEX_ENSURE_TIMEOUT` (default `2m`)
   - `PRESENCE_RPC_ENABLED` (default `false`), `PRESENCE_BATCH_SIZE` (`512`), `PRESENCE_RPC_TIMEOUT` (`2s`)

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
2. Provision the `PUSH_NOTIFICATIONS_{siteID}` stream (§3.1).
3. Ship the push service consumer (§1). Mobile push now flows; presence gating
   is still fail-open (everyone eligible is pushed).
4. Ship the presence RPC handler (§2), then flip `PRESENCE_RPC_ENABLED=true`.
   DND suppression now active.
