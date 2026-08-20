# Room key cache staleness — design

## Problem

Removing several room members in quick succession can produce broadcast messages
that no client can ever decrypt. Two independent defects combine.

### 1. Cached key outlives the store's recovery window

`broadcast-worker` caches the room key in a bounded LRU with a TTL
(`broadcast-worker/keycache.go`, `ROOM_KEY_CACHE_TTL` default `10m`) and stamps
the cached version into every envelope. The cache has no invalidation path.

`roomkeystore` retains exactly one previous version (`encKey.prevPriv` /
`prevVer` / `prevExpiresAt`), so `GetByVersion` — the store side of the client's
`key.get` repair RPC — resolves only the current key or the single prior one.

Each individual member removal rotates the key (`room-worker/handler.go`
`rotateAndFanOut`). Two removals inside one cache TTL take the store from `v` to
`v+2`. The cache still serves `v`; the previous slot now holds `v+1`. A message
stamped `v` is undecryptable: `key.get?version=v` returns `errRoomKeyAbsent` and
the client renders the `[encrypted message]` placeholder permanently.

Retention depth has been one since the original design
(`2026-03-30-room-key-rotation-design.md`: `Rotate` "replaces any existing
previous key"), and the Mongo migration preserved that faithfully. It was
correct under the original precondition that every encrypt reads the store.
`broadcast-worker`'s cache removed that precondition without revisiting the
retention rule; `keyCacheTTLSafe` then guards the wrong dimension — time
(TTL < grace) rather than version distance.

Impact is partial, which is why it reads as intermittent: a client that held `v`
before the rotations keeps it in `keyBytesRef` (an untrimmed per-session map,
`chat-frontend/src/context/RoomKeysContext/RoomKeysContext.tsx`) and still
decrypts. Clients that joined later or reloaded seed only the current version and
cannot recover. Live broadcast only — Cassandra history is not encrypted with
this key.

### 2. Fan-out labels keys with a predicted version

`rotateAndFanOut` computes `predictedVersion = currentPair.Version + 1` and fans
the new key out to survivors *before* calling `Rotate`. `room-worker` dispatches
ROOMS messages to 100 concurrent goroutines with no per-room serialization
(`room-worker/main.go`); `bot-room-service/handler.go` follows the same pattern.

Two concurrent removals on one room at `v5` both predict `6` and both fan out.
The store assigns `6` and `7`. Clients receive two different keys labelled `6`,
last write wins, and the store's `6` is the other one.

That state is unrecoverable: `ensureKey` short-circuits on any version already in
`knownKeysRef`, so the client never refetches. Decrypt fails for the session.

The two defects compound, and neither fix substitutes for the other: retention
cannot repair mislabelled bytes, and correct labels do not make an evicted
version resolvable.

## Goal

A version stamped into an envelope stays resolvable for as long as a cached copy
could still produce it, and the bytes clients hold for a version match the bytes
the store assigned to it.

**Non-goals.**

- **The removed-member read window is unchanged at ~10 minutes.** The L1 cache is
  deliberately left alone, so a removed member can still decrypt broadcasts until
  the cached pre-rotation key expires. Closing that needs rotation debouncing or
  push invalidation — separate work.
- Rotation debouncing, cache invalidation, and any change to the 24h grace
  period.

## Approach

1. **Retired-key collection.** A new MongoDB collection holds each retired key
   version as its own document, expired by a TTL index. `Rotate` writes the key
   it demoted; `GetByVersion` falls back to the collection when neither the
   current nor the previous slot matches.
2. **Rotate first, then fan out.** `room-worker` and `bot-room-service` fan out
   the version the store returned instead of a predicted one.

The room document's `encKey` sub-document, the `Rotate` pipeline's existing
semantics, the 24h grace period, and `broadcast-worker`'s L1 cache are all
unchanged.

Alternatives considered and rejected:

- **Expiry-pruned previous-key ring inside `encKey`.** Keeps one datastore and
  one atomic write, but grows a document that many services read for unrelated
  reasons, needs a count cap as a memory guard, and overloads the grace period
  with a second meaning. A separate collection makes the document the unit of
  expiry, which is what a TTL index needs.
- **Count-capped ring (`$slice` to N).** Retention bounded by rotation *count*
  while the cache is bounded by *time*; N is a guess.
- **Valkey archive keyed by `(roomID, version)`.** Same shape, but splits key
  material across two systems, puts retention at the mercy of the eviction
  policy, and reverses `2026-06-09-room-keys-mongo-migration-design.md`. A second
  *collection* in the same database concedes far less.
- **Rotation debounce with a minimum inter-rotation gap ≥ cache TTL.** Prevents
  the situation rather than repairing it and closes the removed-member window
  too, but needs a distributed atomic guard (in-process timers do not bind 100
  goroutines × N replicas × two services) plus a collapsing mechanism — a naive
  per-removal rotate-request with `NakWithDelay` serialises rather than merges,
  turning 10 removals into 10 rotations spread over 18 minutes. Deferred.

## Architecture

### New collection: `retired_room_keys`

| Field | Value |
|---|---|
| `_id` | `{roomID}:{version}` |
| `priv` | the retired 32-byte secret |
| `expiresAt` | write time + retention |

`_id` is a deterministic composite rather than an `idgen` value — the natural key
is what callers look up by, so no secondary index is needed and a re-archive of
the same version overwrites its own document rather than adding a duplicate. DM
room IDs set the precedent for deterministic composite `_id`s.

Documents are written with `$set`, not `$setOnInsert`: re-archiving a version is
idempotent when the bytes are identical (it only refreshes `expiresAt`), and if a
version number is reused — `Delete` unsets `encKey` and the next `Set` restarts
numbering at 0 — the newer bytes correctly win instead of the stale incarnation's
being served.

**TTL index** on `expiresAt` with `expireAfterSeconds: 0`, so retention is set
per document at write time and is tunable by config without an index rebuild.
Created by `EnsureIndexes` at startup (precedent: `room-service/store_mongo.go`,
`broadcast-worker/store_mongo.go`). This is the repo's first TTL index.

Unlike `prevExpiresAt` — a read gate — `expiresAt` here is purely a deletion
mechanism. MongoDB's TTL monitor runs about every 60s and deletes lazily, so
documents outlive their nominal expiry; serving one is harmless, because a client
asking for that version legitimately needs it. Reads therefore do **not**
re-check `expiresAt`.

### `pkg/roomkeystore`

The collection is entirely encapsulated in the store. No service handler changes.

**`Rotate`** keeps its single atomic `FindOneAndUpdate` and pipeline. The
After-projection widens from `{encKey.ver: 1}` to also return
`encKey.prevPriv` / `encKey.prevVer` — after the update those hold exactly what
*this* call demoted, so the archive write is precise even when rotations race.
`Rotate` then upserts the retired document.

Ordering is deliberate: rotate, then archive. Archiving first would need a
separate pre-read, and two concurrent rotations would both read `v5`, both
archive `v5`, and demote `v6` unarchived. Rotating first has no such gap, costs
no extra round trip, and the demoted key is durable in `prevPriv` regardless —
the archive is a copy, never the only one.

**Archive-write failure does not fail the rotation.** The rotation is committed
and must not be re-run; returning an error would redeliver the removal and rotate
again. Log it and count it (`roomkeymetrics.StoreErrors`, `op="ArchiveRetired"`).
The exposure is one unarchived version, still resolvable from `prevPriv` under
the grace period unless a second rotation follows inside the cache window.

**`GetByVersion`** keeps its current order — current slot, then previous slot
under `prevExpiresAt` — and falls back to `retired_room_keys` by `_id` only when
neither matches. The hot path is unchanged; the fallback is a point read on a
miss that today returns `(nil, nil)`.

**Constructor.** The collection is optional, via a functional option so
consumers that never touch it stay unchanged:

```go
func NewMongoStore(rooms *mongo.Collection, gracePeriod time.Duration, opts ...Option) RoomKeyStore
func WithRetiredKeys(col *mongo.Collection, ttl time.Duration) Option
```

The collection name is exported as `roomkeystore.RetiredKeysCollection` so the
three wiring sites cannot drift on a typo.

**Interface change.** `RoomKeyStore` gains `EnsureIndexes(ctx) error`, which
creates the indexes the store owns (today only the `expiresAt` TTL index) and
no-ops when the archive is not configured. It is idempotent, so every service
calls it at startup. Adding it to the interface — rather than type-asserting on
the concrete store — keeps index ownership with the store and forces every
implementation and generated mock to account for it.

| Service | Uses it for |
|---|---|
| `room-worker`, `bot-room-service` | write (rotation) |
| `room-service` | read (`key.get` fallback) |
| `broadcast-worker`, `tools/*` | omitted — only calls `Get` |

### `room-worker` / `bot-room-service` — rotate before fan-out

`rotateAndFanOut` becomes:

1. Generate the new pair.
2. `currentPair == nil` → `Set` (version 0). Otherwise `Rotate`; on
   `ErrNoCurrentKey` fall back to `Set`.
3. Fan out to survivors the pair the store confirmed — the version `Rotate`
   returned on the rotate leg, and the version *and* bytes read back after `Set`
   on the fallback leg (`Set` is last-write-wins at v0, so its own return value
   is not trustworthy until re-read).

`predictedVersion` disappears, and with it the `SetWithVersion` fallback: nothing
has been fanned out at the point of the fallback, so plain `Set` is correct.
`SetWithVersion` has no other callers, so it was removed outright — from
`room-worker/store.go`, `bot-room-service/store.go` *and* from
`roomkeystore.RoomKeyStore` and `mongoStore`. Keeping it as an unused store
primitive would only invite the predict-then-patch pattern back.

The three-step commit is shared, not duplicated per service: `Rotate` → on
`ErrNoCurrentKey` (or no current key at all) `Set` → re-`Get` lives in
`roomkeystore.CommitRotation`, which both handlers call and which emits
`roomkeymetrics.StoreErrors` on each store failure.

**Accepted trade.** The current ordering is deliberate — survivors hold `v+1`
before `broadcast-worker` can switch to it. Inverting it opens a brief window
where `broadcast-worker` encrypts at `v+1` before a survivor has received it.
That window is self-healing: the client hits an unknown version and `ensureKey`
fetches it (`2026-06-02-room-key-fetch-on-missing-design.md`). The failure it
replaces — wrong bytes under a correct-looking version — is permanent.

### Configuration

`ROOM_KEY_RETIRED_TTL`, default `20m`, in `room-worker`, `bot-room-service`, and
`room-service`.

**It must be at least twice `broadcast-worker`'s `ROOM_KEY_CACHE_TTL`.** A
version can be stamped into a message at the very end of a cache entry's life, so
retention has to outlast the cache entry plus the client's fetch and retry. This
is a cross-service constraint of the same kind as `MESSAGE_BUCKET_HOURS`.

`broadcast-worker` also reads `ROOM_KEY_RETIRED_TTL` — purely to fail fast at
startup when `retiredTTL < 2 × cacheTTL`, mirroring the existing
`keyCacheTTLSafe` check. It is the only service that knows both numbers.

### Observability

`room-service`'s `key.get` increments the existing
`roomkeymetrics.KeyAbsentErrors` (`room_key_absent_errors_total`) when the
fallback also misses. That is the only signal that retention was insufficient;
without it the failure is silent.

## Testing

TDD per `CLAUDE.md` — tests first, confirmed failing, then implementation.

**`pkg/roomkeystore` unit** (mocked/injected clock): `Rotate` archives the
version it demoted; `GetByVersion` resolves current and previous without touching
the collection, and falls back only on a miss; a failed archive write leaves the
rotation successful and counts the metric; `Rotate` with no retired-keys option
configured behaves exactly as today.

**`pkg/roomkeystore` integration** (`//go:build integration`,
`testutil.MongoDB`): the TTL index is created with the expected spec; rotate
three times and resolve the oldest version through the fallback; concurrent
`Rotate` calls yield consecutive versions with an archive document for every
demoted version.

**`room-worker` / `bot-room-service` handler unit** (mocked store): fan-out
carries the store-returned version, not `current + 1`; two concurrent removals
fan out distinct versions matching what the store assigned; `ErrNoCurrentKey`
falls back to `Set` and fans out version 0; a store failure fans out nothing.

**`broadcast-worker`**: startup rejects `retiredTTL < 2 × cacheTTL`.

Coverage floor 80% per package, 90% target for the store and handlers.

## Rollout

No migration and no change to existing documents. The collection is created on
first write; `EnsureIndexes` installs the TTL index at startup.

Old and new binaries coexist safely. An old binary does not archive, so versions
it retires are recoverable only from `prevPriv` — the pre-existing behaviour, and
the gap closes as instances roll. A new binary's fallback simply misses for those
versions.

`docs/client-api.md` needs no change: no client-facing request/response struct
changes, and `key.get`'s schema and error cases are unchanged — only the range of
versions it can resolve.

## Risks

- **Archive write lost** (Mongo error, or an old binary mid-rollout) leaves one
  version recoverable only from `prevPriv`, i.e. today's behaviour, surfaced by
  `room_key_absent_errors_total` rather than silent.
- **`ROOM_KEY_RETIRED_TTL` misconfigured below `2 × ROOM_KEY_CACHE_TTL`** would
  reopen the bug; `broadcast-worker`'s startup check makes that unbootable rather
  than subtly broken.
- **Retired keys outlive their room.** `Delete` unsets `encKey` but leaves
  retired documents until TTL (≤20m). Accepted: `key.get` is membership-gated, so
  they are unreadable, and the window is short.
- **Removed-member read window remains** at ~10 minutes, unchanged by this work
  and stated as a non-goal above.
