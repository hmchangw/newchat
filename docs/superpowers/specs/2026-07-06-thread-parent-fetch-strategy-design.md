# Thread-Parent CreatedAt: Gatekeeper-First Resolution with Worker Fallback

**Date:** 2026-07-06
**Status:** Approved — partially superseded (see below)

> **Scope note (post-rebase):** Section 4 (broadcast-worker restricted-history filter),
> its store additions (`GetThreadRoomInfo`, `GetSubscriptionsHistorySince`), and the
> related testing bullets are **superseded by #435** ("Gate thread-reply visibility by
> history window"), which merged to main first and already implements thread-reply
> history gating in broadcast-worker. This PR was rebased onto that work and is scoped to
> Sections 1–3 + docs: the gatekeeper→worker `ThreadParentMessageCreatedAt` propagation,
> which now *feeds* #435's gate (message-worker's `markThreadMentions` reads the resolved
> value). The broadcast-worker design below is retained as a record of the original plan.

## Context

PR #399 removed the thread-parent `createdAt` resolution from message-gatekeeper so the
send path stays available during a Cassandra outage. The cost: every downstream consumer
now re-resolves the parent per thread reply — message-worker from Cassandra
(`messages_by_id`, NAK on miss), search-sync-worker from Elasticsearch, and
notification-worker from `thread_rooms` (fused with its followers query).

This design restores the gatekeeper fetch as a **best-effort** resolution carried on the
canonical event, demotes the per-worker fetches to **fallbacks** that run only when the
event lacks the value, and adds a new **restricted-history filter** to broadcast-worker's
thread fan-out.

## Decisions (settled with the user)

1. Gatekeeper resolves the parent `createdAt` best-effort: on fetch failure it publishes
   the canonical event **without** the value (soft-fail, no NAK, no client error).
2. message-worker and search-sync-worker use the event value when present and fall back
   to their own resolution only when it is absent.
3. notification-worker is unchanged (its lookup rides the followers query for free and is
   already fail-closed).
4. broadcast-worker gains restricted-history filtering on **all** thread fan-out events
   (created, updated, deleted). When the parent `createdAt` cannot be resolved, restricted
   users are **suppressed** (fail-closed), matching notification-worker.
5. The channel-path subscription lookup uses a new targeted store method (option (a)
   below), not `ListSubscriptions`.

## Design

### 1. message-gatekeeper (`message-gatekeeper/handler.go`)

Restore `resolveThreadParentCreatedAt` (shape of the pre-#399 helper) with soft-fail
semantics:

- Returns `nil, nil` for non-thread messages.
- Reuses the quote snapshot's `CreatedAt` when the thread parent is also the quoted
  message and the snapshot is verified (not the unverified placeholder).
- Otherwise fetches via the existing `historyParentFetcher.FetchQuotedParent` (existing
  2s request timeout).
- **Any error or nil snapshot: log at WARN with `parent_message_id` and `request_id`,
  return `nil` — the message ships without the value.** The helper no longer returns an
  error to the caller.

`msg.ThreadParentMessageCreatedAt` is populated from the helper's result.
`pkg/model.Message.ThreadParentMessageCreatedAt` already exists; no model change. The
client-facing `SendMessageRequest` field removed in #399 is NOT re-added — resolution
stays server-side only.

Worst case under Cassandra outage: thread replies pay up to the 2s fetch timeout in the
gatekeeper, then proceed. Non-thread traffic is unaffected.

### 2. message-worker (`message-worker/handler.go`)

In `processMessage`, when `evt.Message.ThreadParentMessageID != ""`:

- If `evt.Message.ThreadParentMessageCreatedAt != nil`: trust it, skip the
  `GetMessageCreatedAt` Cassandra lookup.
- If nil: current behavior unchanged — resolve from `messages_by_id`; error or miss
  returns a bare wrapped error → NAK for redelivery. The hard guarantee on persisted
  partition coordinates is preserved.

### 3. search-sync-worker (`search-sync-worker/messages.go`)

`resolveThreadParentCreatedAt` becomes a no-op when
`evt.Message.ThreadParentMessageCreatedAt` is already set; the ES resolver runs only when
it is absent. Existing nil-resolver / non-thread / deleted guards unchanged.

### 4. broadcast-worker — restricted-history thread filter

New behavior in `handleThreadCreated`, `handleThreadUpdated`, `handleThreadDeleted`
(channel and DM branches).

**Parent `createdAt` resolution order:**

1. `evt.Message.ThreadParentMessageCreatedAt` (present when the gatekeeper resolved it —
   send path only; edit/delete canonical events do not pass through the gatekeeper and
   will rely on step 2).
2. `thread_rooms.threadParentCreatedAt`:
   - Channel path: `GetThreadFollowers` is extended to also project
     `threadParentCreatedAt` and return both (a `ThreadRoomInfo`-style struct mirroring
     notification-worker's fused `Lookup`) — zero extra queries.
   - DM path: a lazy `thread_rooms` lookup by `parentMessageId`, issued only when at
     least one member has `HistorySharedSince` set AND the event lacks the value.
3. Still unresolved → treated as nil (fail-closed below).

**Filter rule** (mirrors notification-worker's `isRestricted`):

- Recipient has `HistorySharedSince == nil` → always delivered (common case; zero
  behavior change).
- Recipient has `HistorySharedSince != nil`:
  - `parentCreatedAt == nil` → **suppressed** (fail-closed).
  - `parentCreatedAt < HistorySharedSince` → suppressed.
  - Otherwise delivered.
- **The message sender is exempt** — always delivered, for multi-device echo of their own
  reply.

**Channel-path subscription lookup (chosen option (a)):** new store method

```go
GetSubscriptionsHistorySince(ctx context.Context, roomID string, accounts []string) (map[string]*time.Time, error)
```

One `subscriptions` query filtered by `roomId` + `u.account $in accounts`, projecting
only `{u.account, historySharedSince}` (precise projection per CLAUDE.md). Issued once
per thread event after the fan-out list is built; accounts absent from the result map are
treated as `HistorySharedSince == nil`. A query failure returns an error → NAK, so the
whole fan-out is redelivered rather than delivering past a filter we could not evaluate
(same semantics as the existing `GetThreadFollowers` error path).

**DM path:** `publishDMEvents` already holds full subscriptions (which include
`HistorySharedSince`); apply the same rule inline. The lazy `thread_rooms` fallback fires
only in the rare restricted-member case. The DM filter applies only on the thread-reply
paths (`handleThreadCreated`/`Updated`/`Deleted` DM branches), not on main-room DM
fan-out.

**Thread badge/metadata events** (`publishThreadBadge`, tcount updates) are out of scope
— they carry counts, not message content, and today's fan-out for them is unchanged.

### 5. Docs

- `docs/client-api.md` and `docs/client-api/events.md`: document
  `threadParentMessageCreatedAt` on the event-carried message as optional and
  server-resolved (absent when the gatekeeper could not resolve it). No request-schema
  change (`SendMessageRequest` unchanged).

## Known trade-off: first-reply race

A reply sent immediately after its parent may race the parent's Cassandra persistence:
the gatekeeper fetch misses (soft-fail), and broadcast-worker's `thread_rooms` fallback
may also miss (the doc is created by message-worker on an unordered consumer). For that
one event, restricted users are suppressed — the chosen fail-closed behavior, identical
to notification-worker today. Unrestricted users are unaffected.

## Testing (TDD, red-green-refactor per task)

- **gatekeeper** (`handler_test.go`): table tests — resolved via fetch; resolved via
  verified quote-snapshot reuse; unverified snapshot forces fetch; fetch error → message
  still published with nil field + WARN logged; non-thread message skips resolution.
- **message-worker** (`handler_test.go`): event value present → no `GetMessageCreatedAt`
  call, value persisted as-is; absent → fallback fetch; fallback miss → error (NAK path).
- **search-sync-worker** (`messages_reresolve_test.go`): value present → resolver not
  invoked; absent → resolver invoked (existing tests adjusted).
- **broadcast-worker** (`handler_test.go`): filter table — restricted + parent older than
  share point → suppressed; restricted + parent newer → delivered; restricted +
  unresolved → suppressed; unrestricted → delivered; sender exempt; DM branch same rules;
  subscription-lookup error → handler error (NAK).
- **broadcast-worker integration** (`integration_test.go`): `GetThreadFollowers` returns
  `threadParentCreatedAt`; `GetSubscriptionsHistorySince` projection against real Mongo
  (via `testutil.MongoDB`).
- Coverage: keep every touched package ≥80%, handlers target 90%+.

## Not In Scope

- notification-worker changes.
- Re-adding the client `SendMessageRequest.ThreadParentMessageCreatedAt` field.
- Thread badge / tcount metadata fan-out filtering.
- Any caching of subscriptions or thread-room lookups in broadcast-worker.
