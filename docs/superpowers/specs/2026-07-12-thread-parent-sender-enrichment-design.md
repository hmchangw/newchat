# Thread-Parent Sender Enrichment: Gatekeeper-Carried Account, Worker Fetch Fallback

**Date:** 2026-07-12
**Status:** Approved

## Context

message-gatekeeper already resolves the thread parent's `createdAt` best-effort and
ships it on the canonical `MessageEvent` (`#458`). Consumers that need only the
timestamp (message-worker, search-sync-worker) already skip their own lookup when the
event carries the value.

Two consumers — **broadcast-worker** and **notification-worker** — need more than the
timestamp: they also need the parent message's **sender account** (the thread owner is a
race-free fan-out / notification recipient, added directly rather than via the
eventually-consistent `thread_rooms.replyAccounts`). Because the account is not carried
on the event, both consumers issue an unconditional NATS request to history-service
(`FetchParent`) for every channel thread reply — even when the gatekeeper already
resolved the parent on the send path.

The gatekeeper's existing parent fetch (`FetchQuotedParent`) **already reads the parent's
`Sender`** in the same request it uses for `createdAt`; it just discards the account. So
carrying the account onto the event costs **zero extra calls** at the gatekeeper and lets
both consumers drop their round-trip on the hot create path, race-free.

## Decisions (settled with the user)

1. The parent sender account rides the `MessageEvent` **envelope**, not the persisted
   `Message` — it is a server-only routing hint, mirroring `QuotedParentUnverified`.
   Never persisted (`bson:"-"`), never reaches clients, so no `client-api.md` change.
2. The gatekeeper resolves `createdAt` **and** the sender account together, best-effort:
   on any soft-fail both are absent and consumers fall back to their own resolution.
3. broadcast-worker and notification-worker use the event-carried pair when **both**
   values are present, and fall back to `FetchParent` when **either** is missing (covers
   edit/delete canonical events, which bypass the gatekeeper, and gatekeeper soft-fails).

## Design

### 1. `pkg/model` (`event.go`)

Add to `MessageEvent`:

```go
// ThreadParentSenderAccount is the account of the thread parent's author,
// resolved best-effort by the gatekeeper on the send path (empty when it could
// not resolve, and on edit/delete events which bypass the gatekeeper). Envelope-only
// (never persisted, never reaches clients), like QuotedParentUnverified — consumers
// that need the parent author (broadcast-worker, notification-worker) use it to skip
// their own history fetch, falling back when it is absent.
ThreadParentSenderAccount string `json:"threadParentSenderAccount,omitempty" bson:"-"`
```

No change to `model.Message` or `model.ClientMessage`.

### 2. `message-gatekeeper` (`handler.go`)

`resolveThreadParentCreatedAt` becomes `resolveThreadParent`, returning both the
`createdAt` pointer and the sender account (both zero for non-thread / soft-fail):

- Non-thread reply → `(nil, "")`.
- Verified quote-snapshot reuse (parent == verified quoted message) →
  `(&snapshot.CreatedAt, snapshot.Sender.Account)`.
- Otherwise fetch via `FetchQuotedParent` → `(&snap.CreatedAt, snap.Sender.Account)`.
- Any error / nil snapshot → WARN (unchanged) → `(nil, "")`.

`evt.ThreadParentSenderAccount` is set from the returned account;
`msg.ThreadParentMessageCreatedAt` from the returned timestamp (unchanged).

### 3. `broadcast-worker` (`handler.go`)

`channelThreadFanOut` gains two params carried from the event —
`parentCreatedAt *time.Time` and `parentSenderAccount string`:

- If **both** present (`parentCreatedAt != nil && parentSenderAccount != ""`): use them
  directly, skip `FetchParent`.
- Else: `FetchParent` as today (returns both), used for the gate and the recipient.

Its three callers (`handleThreadCreated`, `handleThreadUpdated`, `handleThreadDeleted`)
pass `evt.Message.ThreadParentMessageCreatedAt` and `evt.ThreadParentSenderAccount`. On
create these are populated on the send path; on update/delete they are absent (gatekeeper
bypass) so the fetch fallback runs — no behavior change there.

### 4. `notification-worker` (`handler.go`)

In the `isThreadOnlyReply` branch, when `msg.ThreadParentMessageCreatedAt != nil &&
evt.ThreadParentSenderAccount != ""`, use them directly and skip
`h.deps.Parent.FetchParent`. Otherwise fetch as today. The followers `Lookup` is
unchanged (it is a separate, always-needed query).

### 5. Docs

None. `MessageEvent` is the internal canonical event (not client-facing); no
`client-api.md` / events-view change. `SendMessageRequest` unchanged.

## Testing (TDD, red-green-refactor)

- **gatekeeper** (`handler_test.go`): thread reply resolved via fetch sets both
  `ThreadParentMessageCreatedAt` and `ThreadParentSenderAccount`; resolved via verified
  quote-snapshot reuse sets both from the snapshot; fetch error → both absent + WARN;
  non-thread → both absent.
- **broadcast-worker** (`handler_test.go`): create with both event values → `FetchParent`
  NOT called, recipient set + mention gate use the event values; create with missing
  account (or missing createdAt) → `FetchParent` called; update/delete → `FetchParent`
  called (fallback). Assert via the existing `mock_parentfetcher_test.go` call count.
- **notification-worker** (`handler_test.go`): both present → `FetchParent` not called,
  parent author still notified + gate uses event createdAt; missing → fetch fallback.
- Coverage: keep every touched package ≥80%, handlers target 90%+.

## Not In Scope

- message-worker / search-sync-worker (already event-first for createdAt; neither needs
  the parent account).
- Re-adding any client `SendMessageRequest` field.
- Enriching edit/delete canonical events (they bypass the gatekeeper by design).
