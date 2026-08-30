# Error Handling Guide

How to produce client-facing errors in this codebase. The canonical source is
`pkg/errcode` (and its adapters `errnats` for NATS, `errhttp` for Gin); this
guide is a developer-facing walkthrough.

For the client-side view of the wire envelope (what callers see and how to
branch), see `docs/client-api.md` §6.

---

## 1. The contract

Every client-facing error is an `*errcode.Error` that marshals to:

```json
{
  "error":    "<human-readable, user-safe message>",
  "code":     "<one of 8 generic categories>",
  "reason":   "<optional, domain-specific machine code>",
  "metadata": { "<key>": "<value>" }
}
```

- `code` is **always present** and drives HTTP status.
- `reason` is **optional**; declare it only when the frontend must distinguish
  cases that the generic `code` cannot.
- `metadata` is **client-visible** structured detail (`map[string]string`).
- The cause attached via `WithCause` is **never serialized** — it is logged
  server-side once by `Classify` and reachable via `Unwrap()`/`errors.Is`/`As`.

The eight generic categories and HTTP statuses:

| Constant                       | Wire `code`         | HTTP |
|--------------------------------|---------------------|------|
| `errcode.CodeBadRequest`       | `bad_request`       | 400  |
| `errcode.CodeUnauthenticated`  | `unauthenticated`   | 401  |
| `errcode.CodeForbidden`        | `forbidden`         | 403  |
| `errcode.CodeNotFound`         | `not_found`         | 404  |
| `errcode.CodeConflict`         | `conflict`          | 409  |
| `errcode.CodeTooManyRequests`  | `too_many_requests` | 429  |
| `errcode.CodeUnavailable`      | `unavailable`       | 503  |
| `errcode.CodeInternal`         | `internal`          | 500  |

`503 vs 429`: `unavailable` is server-wide saturation (admission control,
expand-timeout); `too_many_requests` is per-caller rate limiting / quota.

---

## 2. Producing errors

### The common case — a typed client error

```go
return nil, errcode.BadRequest("name is required")
return nil, errcode.NotFound("room not found")
return nil, errcode.Forbidden("only owners can update roles")
return nil, errcode.Conflict("room is at maximum capacity",
    errcode.WithReason(errcode.RoomMaxSizeReached))
```

Use the **named constructor** (`BadRequest`, `Unauthenticated`, `Forbidden`,
`NotFound`, `Conflict`, `TooManyRequests`, `Unavailable`, `Internal`). There
are no `*f` variants on purpose — they would silently swallow trailing
`Option` args. For dynamic text, format the message at the call site:

```go
return nil, errcode.BadRequest(
    fmt.Sprintf("batch size %d exceeds limit %d", n, max))
```

`errcode.New(code, msg, opts...)` is the escape hatch for a dynamically chosen
category; semgrep warns when you pass a literal `errcode.CodeX` to it
(prefer the named constructor in that case).

### Infra / DB / third-party errors

Don't manually classify them — return the wrapped raw error and let `Classify`
collapse it to `internal`/"internal error" at the boundary (the real cause is
logged once, never sent):

```go
if err := h.store.Find(ctx, id); err != nil {
    return nil, fmt.Errorf("loading room: %w", err) // → client sees "internal error"
}
```

**One exception, at the NATS request/reply boundary.** `natsutil.RequestFailure`
classifies two transport errors — `nats.ErrNoResponders` and
`nats.ErrTimeout`/`context.DeadlineExceeded` — as `errcode.Unavailable` rather
than letting them collapse to `internal`. They mean an upstream is down or
unresponsive, not that this service is faulty, and a client can act on that
difference. Every other transport error still returns a raw wrap. Call it in
place of `fmt.Errorf` on the error returned by `nc.Request`/`RequestMsg`.

### Attaching a cause for server-side debugging

```go
return nil, errcode.BadRequest("invalid ensure-room-key request",
    errcode.WithCause(err))
```

`WithCause` panics if `err` already contains an `*errcode.Error` — the
invariant is **one `*errcode.Error` per chain**, propagated via a single `%w`.
Never wrap a message body, token, or any secret into a cause; the cause is
included in the server log line.

### Client-visible metadata

```go
return nil, errcode.Conflict("room is at maximum capacity",
    errcode.WithReason(errcode.RoomMaxSizeReached),
    errcode.WithMetadata("limit", strconv.Itoa(max)))
```

`WithMetadata` is **client-visible** (ships in the envelope). For server-only
attributes — request_id, account, roomID — use `WithLogValues` (next section).
Mixing them up is a leak risk.

---

## 3. Replying

You never marshal the envelope yourself; the adapter does it (and logs once):

| Transport            | Adapter                                            |
|----------------------|----------------------------------------------------|
| NATS sync reply      | `errnats.Reply(ctx, msg, err)`                     |
| NATS already-logged  | `errnats.ReplyQuiet(msg, err)` (panic backstop / `replyBusy`) |
| Gin HTTP             | `errhttp.Write(ctx, c, err)`                       |

Handlers registered via `pkg/natsrouter` are automatic: returning a typed
errcode error from the handler routes through `errnats.Reply`. JetStream
consumers / raw NATS handlers call `errnats.Reply` directly.

---

## 3a. Request-ID policy: mint by default, reject on dedup-critical paths

Every NATS and HTTP entry point in this repo enforces a rule on the inbound
`X-Request-ID` header. The repo runs **two** policies side by side:

### Default — mint on missing/malformed

Used by every entry point whose request ID is logging/tracing only — most
read paths, auth-service, gatekeeper validation reply, etc.

- **Valid hyphenated UUID** (`idgen.IsValidUUID`) → pass through unchanged.
- **Missing** (header absent or empty) → silently mint a fresh UUIDv7 via
  `idgen.GenerateRequestID`. No log line — this is the benign common case.
- **Malformed** (present but not a valid UUID) → mint a fresh UUIDv7 AND emit
  a single `slog.Warn("minted request_id (inbound invalid)", ...)` carrying
  the original inbound value, so a buggy client stays traceable.

Chokepoint: `idgen.ResolveRequestID(inbound) (id, replaced bool)`. NATS
wrapper: `natsutil.StampRequestID(ctx, headers, subject) (ctx, id)`. HTTP:
auth-service `requestIDMiddleware` calls `idgen.ResolveRequestID` directly.
The `pkg/natsrouter` `RequestID()` middleware applies the default policy
automatically.

### Request-ID minting and dedup safety (dedup-critical paths)

Some handlers in **room-service** and **room-worker** fan out to JetStream
publishes whose `Nats-Msg-Id` (via `natsutil.InboxDedupID`,
`natsutil.CanonicalDedupID`, and the in-package `messageDedupSeed` helper) and
whose canonical message IDs (via `idgen.MessageIDFromRequestID`) are derived
from the request ID. A server-side mint there weakens client-retry
deduplication: a client retrying without `X-Request-ID` (or with a malformed
value) gets a fresh server-minted ID each attempt, produces a different dedup
key each time, and can silently duplicate cross-site inbox events and system messages.

**Both services mint at the boundary** (`natsrouter.RequestID()`), so every
handler always has a request ID for logging and no server-to-server call is
rejected for a missing header. Dedup safety is preserved two ways:

- **Payload-derived dedup** (preferred): `room-worker.serverCreateDM`
  (`chat.server.request.room.{siteID}.create.dm`) derives its cross-site inbox
  dedup key from a deterministic payload seed (`room.ID` + `requester.Account` +
  `room.CreatedAt` in ms, suffixed with the destination site), independent of
  the request ID. Retries dedup correctly even with a minted/absent header.
- **Caller-supplied stable ID** (contract): handlers that still derive dedup or
  canonical IDs from the request ID — notably `room-service.roomRestricted`
  (`idgen.MessageIDFromRequestID` + `InboxDedupID`) and the async ROOMS-stream
  paths reached via `room-service` member RPCs — rely on the caller sending a
  **stable** `X-Request-ID` across retries. This is a contract expectation, no
  longer enforced at the boundary; a caller that omits it forfeits retry dedup.
- **The room-worker JetStream consume loop** keeps the default mint policy
  defensively. room-service stamps a request ID at publish time (minting one
  when the client omits it), so ROOMS-stream messages should always carry a
  valid UUID; the loop logs an `slog.Error` if it ever has to mint, because that
  means room-service failed to stamp one.

The strict `natsutil.RequireRequestID` / `natsrouter.RequireRequestID` helpers
remain available for any future path that wants to *enforce* a caller-supplied
ID, but no service installs them today.

**Client contract**: any client calling room-service or room-worker SHOULD
send a stable `X-Request-ID` header (a valid hyphenated UUIDv4 or v7) and
reuse the same value across retries of the same logical operation, to keep
dedup-critical operations idempotent. See `docs/client-api.md` for the
wire-level contract.

Once stamped, `errcode.Classify(ctx, err)` and every `slog.…Context(ctx, ...)`
call automatically carries `request_id` — handlers never need to pass it
explicitly.

## 4. Logging contract

`errcode.Classify(ctx, err)` emits **exactly one** `slog` line per failed
request, at a **category-aware level**:

- `internal`, `unavailable` → `ERROR`
- all expected client errors (`bad_request`, `unauthenticated`, `forbidden`,
  `not_found`, `conflict`, `too_many_requests`) → `INFO`

This keeps routine 4xx validation failures out of the ERROR stream so
error-rate alerting stays meaningful. **Handlers must not log-then-reply** —
the reply path logs.

Attach domain context once at handler entry. The seam differs by handler style:

- **natsrouter handler** (`*natsrouter.Context`): use the cycle-safe method
  `c.WithLogValues("account", a, "roomID", r)`.
- **Gin or raw NATS** (plain `context.Context`): use the package func
  `ctx = errcode.WithLogValues(ctx, "request_id", id, "account", a, ...)`.

The `request_id`/`account`/`roomID` then appear in the centralized Classify
log line and any downstream slog usage in the chain.

> **Why two APIs?** `*natsrouter.Context` implements `context.Context` and
> delegates `Value(key)` lookups to an inner `ctx` field. Calling
> `errcode.WithLogValues(c, …)` would derive a new ctx whose parent is `c` —
> any subsequent `c.Value(otherKey)` would loop. The method (`c.WithLogValues`)
> derives from the inner field, avoiding the cycle.

---

## 5. Adding a new `reason`

Reasons are **per-service catalogs** in `pkg/errcode/codes_<service>.go`
(declared as `Reason` constants — never `errcode.Reason("...")` inline; semgrep
will reject it).

1. Pick a `flat_snake_case` machine code (e.g. `bot_rate_limited`).
2. Add it to the right catalog:
   ```go
   // pkg/errcode/codes_room.go
   RoomBotRateLimited Reason = "bot_rate_limited"
   ```
3. Add the constant to `allReasons` in `pkg/errcode/codes_test.go` (the
   snake-case + uniqueness tests pick it up automatically).
4. Use it: `errcode.TooManyRequests("bot quota exceeded",
   errcode.WithReason(errcode.RoomBotRateLimited))`.
5. Update `docs/client-api.md` §6 reason catalog AND the relevant endpoint
   error table in the SAME PR (CLAUDE.md client-API rule).

Only add a reason when the frontend genuinely needs to distinguish it from
other errors of the same category. Most cases are generic.

---

## 6. Wrapping invariant — allowed vs forbidden

**Invariant:** at most one `*errcode.Error` per error chain, propagated via a
single `%w`.

**Allowed:**

```go
return errcode.BadRequest("name is required")
return errcode.NotFound("x", errcode.WithReason(RoomNotMember))
return errcode.Internal("x", errcode.WithCause(rawDBErr))      // RAW cause only
return fmt.Errorf("checking room: %w", typedErr)               // typed survives
return typedErr                                                 // bare propagation
```

**Forbidden (semgrep-flagged + panics at runtime):**

```go
return errcode.Internal("x", errcode.WithCause(anotherErrcodeErr)) // PANIC
return fmt.Errorf("%w and %w", errcodeA, errcodeB)                 // Classify picks one
```

---

## 7. Lint enforcement

`.semgrep/errcode.yml` (wired into `make sast`) enforces:

| Rule                                       | Severity | What it catches |
|--------------------------------------------|----------|-----------------|
| `errcode-no-reason-literal-outside-catalog`| ERROR    | Inline `errcode.Reason("...")` outside `codes_*.go` |
| `errcode-withcause-must-not-wrap-errcode`  | ERROR    | `errcode.WithCause(errcode.X(...))` literal |
| `errcode-no-multi-wrap-errcode`            | ERROR    | `fmt.Errorf("%w … %w")` mixing typed errors |
| `errcode-prefer-named-constructor`         | WARNING  | `errcode.New(errcode.CodeX, msg)` literal |

`.semgrep/jsnak.yml` enforces the JetStream settle rules (see §9a):

| Rule                                          | Severity | What it catches |
|-----------------------------------------------|----------|-----------------|
| `jsretry-no-bare-nak`                         | ERROR    | `msg.Nak()` — instant redelivery, ignores `BackOff` |
| `jsretry-no-zero-nak-delay`                   | ERROR    | `NakWithDelay(0)` — the same thing on the wire |
| `jsretry-no-hardcoded-consumer-backoff`       | ERROR    | Assigning `ConsumerConfig.BackOff` directly |
| `jsretry-marshal-failure-must-be-permanent`   | ERROR    | A marshal error returned as a bare wrapped error on a settle path |

CI runs `make sast` on every PR.

---

## 8. Testing

Use `pkg/errcode/errtest` to assert on a decoded reply payload:

```go
import "github.com/hmchangw/chat/pkg/errcode/errtest"

errtest.AssertCode(t, replyBytes, errcode.CodeNotFound)
errtest.AssertReason(t, replyBytes, errcode.RoomNotMember)
e := errtest.Decode(t, replyBytes) // for ad-hoc checks
```

For in-process matching on chained errors:

```go
if errcode.HasReason(err, errcode.RoomNotMember) { /* … */ }
r := errcode.ReasonOf(err) // "" if no errcode error in chain
```

---

## 9. JetStream consumers — `errcode.Permanent`

JetStream handlers face a different question than request/reply handlers: on
failure, do we **Ack** (drop the message) or **Nak** (let JetStream redeliver)?
The category alone can't answer it — an `Internal` from a deterministic bug
should drop, while a transient infra `Internal` should retry. The marker is
**explicit**:

```go
if err := json.Unmarshal(data, &req); err != nil {
    // Malformed payload: redelivery won't help. Ack via Permanent.
    return errcode.Permanent(errcode.BadRequest("unmarshal X"))
}
// Transient infra failure: bare error → consumer Naks for redelivery.
if err := h.store.Save(ctx, &row); err != nil {
    return fmt.Errorf("save row: %w", err)
}
```

The consume loop hands the error to `pkg/jsretry`, which reads the marker:

```go
// Ack on nil, Ack-drop on Permanent, NakWithDelay(backoff) otherwise.
jsretry.Settle(ctx, msg, jsretry.DefaultBackoff, h.HandleMessage(ctx, msg.Data()))
```

Never call `msg.Nak()` or `NakWithDelay(0)` directly: a bare Nak redelivers
instantly and ignores the consumer's `BackOff`, so a sub-second blip burns
`MaxDeliver` in milliseconds. `.semgrep/jsnak.yml` fails CI on both.

`Permanent` wraps an `*errcode.Error` so `fillAsyncError` can still extract
`Code` / `Reason` for the `AsyncJobResult` envelope; the wrapper is invisible
to clients (it isn't serialized). `errors.Is(err, errcode.ErrPermanent)` is
the sentinel-style match if you don't need the wrapped `*Error`.

**Don't** infer permanence from `Code`: an `Internal` can be either a poison-
pill (bad payload classified to internal by Classify) or a retryable
infra-down condition. Wrap explicitly at the call site.

---

## 9a. Which errors are permanent — classification reference

### The test

An error is **permanent** if its outcome depends only on the **message bytes
and the code**. It is **transient** if it depends on the **state of the world**,
because that state can differ on the next delivery.

Everything below follows from that one question. When a case is not listed,
ask it rather than pattern-matching on a similar-looking entry.

### Permanent — Ack-drop

| Class | Examples | Construct with |
|---|---|---|
| Serialization out | `Marshal` of a fixed struct | `errcode.MarshalFailed("<what>", err)` |
| Serialization in | Unmarshal/decode of the inbound envelope or payload | `Permanent(BadRequest("unmarshal X", WithCause(err)))` |
| Round-trip of our own bytes | `bson.Unmarshal` of this function's own `bson.Marshal` output | `Permanent(Internal(…, WithCause(err)))` |
| Payload validation | Missing required field, unknown event type, malformed subject, invalid ID format | `Permanent(BadRequest("…"))` |
| Oversized publish | `nats.ErrMaxPayload` — rejected client-side before the wire (`nats.go:4345`) | `Permanent(Internal("… exceeds broker max_payload"))` |
| Remote terminal reply | `not_found`, `forbidden`, `bad_request`, `conflict` from an RPC — a fact about *this* message | `errcode.Terminal` → `Permanent(ee)` |
| Backend rejected the document | Elasticsearch bulk item `400` | `searchengine.IsBulkItemPermanent` |

Attaching the decoder error via `WithCause` is right for `encoding/json`, whose
errors name the offending character and Go type/field names. It is **not** safe
for `sonic`, which embeds a window of the input — see §2's cause rules.

### Transient — Nak with backoff

| Class | Examples |
|---|---|
| Datastore unavailability | Timeouts, no reachable hosts, primary election/failover, write-concern failures |
| Publish failures other than oversize | `ErrConnectionClosed`, `ErrConnectionDraining` (normal at shutdown), no stream response |
| Remote non-terminal reply | `unavailable`, `internal`, `too_many_requests`, `unauthenticated` — `errcode.Terminal` excludes these deliberately |
| Ordering races | A thread parent not yet persisted; `member_added` before the user replicates; subscription-state before `member_added` |
| Backpressure | ES `429`, `es_rejected_execution`, `circuit_breaking` — pair with `jsretry.BackpressureBackoff` |
| Key and secret fetches | Room-key store reads |

`unauthenticated` is transient on purpose: a credential problem hits every
message at once, so dropping is mass data loss rather than poison rejection.

Bound a race that has a known short window rather than spending the whole
budget on it — see `parentResolveExhausted` in broadcast-worker and
notification-worker, which retries twice and then drops with a terminal metric.

### Do not classify these

| Case | Why not |
|---|---|
| Mongo duplicate key (`E11000`) | A domain signal, not a failure. Every site has its own right answer — swallow as idempotent, retry without upsert, or translate to a sentinel. A blanket "permanent" is wrong at all of them. |
| Mongo cursor decode (`cursor.All`) | Fails from either a BSON decode error (deterministic) or a network error mid-cursor (transient), and the error does not distinguish them. Splitting needs `errors.As` on the driver type. |
| `jetstream.ErrNoStreamResponse` | A destination stream that does not exist and a peer that is briefly unreachable produce the identical error. |
| Best-effort paths | A handler that logs and continues has no ack decision to get wrong. Leave the bare error and say so in a comment. |

### The asymmetry — why transient is the default

The two mistakes do not cost the same:

- **Wrong-transient** burns `MaxDeliver`, holds an ack-pending slot, then drops
  silently. Wasteful, but bounded.
- **Wrong-permanent** drops a message immediately that a retry would have
  delivered. Unbounded data loss during any real outage.

So a bare `fmt.Errorf` — transient — is the correct default to fail toward.
Reach for `Permanent` when you can name the reason the outcome cannot change,
not because a retry looks unlikely to help.

### Where the decision lives

At the **leaf**, where the error is first constructed. `%w` wrappers propagate
permanence untouched, so a wrapper must not re-decide:

```go
// leaf — decides
return errcode.Permanent(errcode.BadRequest("unmarshal member_added payload", errcode.WithCause(err)))

// wrapper — propagates, decides nothing
return fmt.Errorf("handle thread room and subscriptions: %w", err)
```

Before classifying a site, read its caller. An error that the caller discards
is not a classification decision at all.

### Lanes without a retry budget change the calculus

`hr-sync-worker` and `outbox-worker`'s per-peer lanes run `MaxDeliver=-1` with
`MaxAckPending=1`. There a non-resolving error never drops — it blocks the lane
forever, and every later message behind it.

Classification cannot rescue an unclassifiable error on such a lane. The policy
is to keep blocking (losing an HR row or a federated event is worse than a
stalled lane) and to **alert on the stall**, so a parked lane is distinguishable
from a healthy idle one.

---

## 10. Migration history

This package replaced four legacy patterns (all removed in `pkg/natsrouter`
cleanup):

- `pkg/natsrouter`'s `RouteError` + `Err*` constructors + `Code*` consts
- `pkg/natsutil`'s `MarshalError` / `MarshalErrorWithCode` / `ReplyError` /
  `TryParseError`
- `pkg/model.ErrorResponse`
- `auth-service`'s ad-hoc `gin.H{"error": ...}`

See `docs/superpowers/specs/2026-05-28-centralized-error-codes-design.md` for
the design rationale and the per-service error contract.
