# Add-Member historySharedSince Inheritance — Design

**Date:** 2026-08-14
**Status:** Approved (user-directed behavior)

## Problem

`historySharedSince` (HSS) on a subscription caps how far back a member can read
room history: `nil` = unrestricted, non-nil = restricted to messages at/after
that instant. Today, when members are added via the add-member RPC,
room-worker derives HSS purely from the request's `history.mode`:

- `mode: "none"` → HSS = request accept timestamp (restricted from now)
- any other mode (`"all"`, or empty) → HSS = `nil` (full history)

This ignores the **adder's own** HSS. A member who was themselves added with
restricted history (HSS = T) can add a new member with `history.mode: "all"`,
and the new member gets `nil` — seeing history the adder cannot see. That is a
history-visibility escalation: any restricted member can launder full history
access through a second account.

## Required behavior

When the requester (adder) has a non-empty HSS on their subscription to the
room and the add request would grant full history (`mode` ≠ `"none"`), the
newly added members must inherit the adder's HSS instead of `nil`. Members can
never be granted history older than their adder can see.

- Adder unrestricted (HSS nil) + `mode: "all"` → new member HSS `nil` (unchanged).
- Adder restricted (HSS = T) + `mode: "all"` → new member HSS = **T** (new).
- Any adder + `mode: "none"` → new member HSS = the accept timestamp or the
  adder's HSS, **whichever is later** (amended post-review: the accept
  timestamp alone could predate the adder's boundary when stamped by a skewed
  clock, which would leak the skew window's history — so the cap is stamped
  for every mode and the worker takes the max).

The rule is transitive by construction: if B inherited HSS = T from A, anyone B
adds with share-all inherits T likewise. It applies uniformly to every account
materialized by the request (direct users, org expansion, channel-ref
expansion) because all of them share the request-level history decision.

## Approach

**Decide in room-service at accept time; carry the cap on the canonical event.**
room-service's `addMembers` already loads the requester's subscription (the
membership gate), so the adder's HSS is in hand with zero extra reads. It
computes the effective cap and stamps it onto the published
`AddMembersRequest`; room-worker applies it wherever it resolves HSS today.

Alternative considered and rejected: clamping inside room-worker by fetching
the requester's subscription there. Costs an extra Mongo read per add, and adds
a failure mode (requester left the room between accept and processing). HSS
never changes after join, so the accept-time read is not racy.

### Rollout / mixed versions

Deploy **room-worker before room-service**. An old room-worker ignores the new
wire field (unknown JSON key, degrades gracefully to today's behavior), so a
new room-service stamping the cap has no effect until the worker is upgraded —
the escalation this design closes persists during a service-first window.
Worker-first is safe in the other direction: a new worker sees nil from an old
room-service and behaves exactly as today.

### Changes

1. **`pkg/model/member.go` — `AddMembersRequest`** gains
   `HistorySharedSince *int64` (`json:"historySharedSince,omitempty"
   bson:"historySharedSince,omitempty"`), epoch ms UTC. Server-set only:
   room-service **always overwrites** it (like `RequesterID` /
   `RequesterAccount` / `Timestamp`), so a client-supplied value can never
   leak through. `nil` = no inherited cap.

2. **room-service `handler.go` `addMembers`** — after the room-type guard,
   compute:
   - `req.HistorySharedSince = nil` (unconditional reset of client input)
   - if the requester's subscription has a non-nil, non-zero, positive
     `HistorySharedSince`: `req.HistorySharedSince =
     ptr(sub.HistorySharedSince.UnixMilli())` — stamped for **every** mode
     (amended post-review): share-all inherits it directly; mode `"none"`
     uses it as the clock-skew floor.

3. **room-service `store_mongo.go`** — add `historySharedSince` to
   `subscriptionReadProjection` (it is not currently projected, so the
   handler would otherwise always see nil). Update the projection drift-guard
   integration test accordingly.

4. **room-worker `handler.go` `historySharedSincePtr`** — honor the inherited
   cap on the share-all branch. New shape (signature takes the request or an
   extra param; exact form is an implementation detail):
   - `mode == "none"` → the later of `&req.Timestamp` and the inherited cap
     (clock-skew guard, amended post-review); a missing/non-positive
     timestamp fails **closed** to the inherited cap when one is present
     (logged), nil only when no cap is available
   - otherwise → `req.HistorySharedSince` if non-nil and > 0, else `nil`
   Both call sites in `processAddMembers` (per-sub resolution and the
   `MemberAddEvent` value) go through this helper, so local subscriptions,
   the room-scoped event, the internal inbox copy, and the cross-site
   federated copies all carry the inherited value with no further changes.

5. **No change** to inbox-worker, broadcast-worker, history-service,
   search-sync-worker: they consume `HistorySharedSince` from the
   subscription/event and are agnostic to how it was derived. Create-room and
   Teams-room-create paths are out of scope (creator/creation members are
   unrestricted or handled by their own flow).

### Docs

- `docs/client-api.md` Add Members section: document the inheritance rule on
  `history.mode` ("all" is capped at the adder's own `historySharedSince`),
  and note it on the `member_added` event's `historySharedSince` field.
- Mirror the same edits in the derived views
  (`docs/client-api/request-reply.md`, `docs/client-api/events.md`) where
  those fields appear.
- The new `AddMembersRequest.historySharedSince` field is server-internal; it
  is documented as server-set (not a client-settable request field), matching
  how `requesterId`/`timestamp` are treated.

## Edge cases

| Case | Result |
|---|---|
| Adder HSS nil, mode all | HSS nil (unchanged) |
| Adder HSS = T, mode all | HSS = T |
| Adder HSS = T, mode none | HSS = accept ts (unchanged; ts ≥ T always) |
| Adder HSS = T, mode empty/unknown | Treated as share-all today (helper's `!= none` branch) → HSS = T |
| Client sends `historySharedSince` in request body | Overwritten unconditionally by room-service |
| Adder HSS non-nil but zero time / ≤0 ms | Defensive: not inherited (emit nil), preserving the "never emit &0" invariant on events |
| Target already subscribed (re-add) | No subscription write happens; existing HSS untouched (unchanged behavior) |
| Org / channel-ref expansion | All materialized accounts inherit the same cap (request-level decision) |
| Cross-site member | Federated `MemberAddEvent` carries inherited HSS; inbox-worker replica sub gets it automatically |
| Redelivery of the canonical event | Value is on the event, so reprocessing is deterministic |

## Testing

TDD (red-green-refactor) per repo rules:

- **`pkg/model`**: round-trip test for the new field (nil and set).
- **room-service `handler_test.go`**: table-driven addMembers cases capturing
  the published payload via the injected `publishToStream`: restricted adder +
  mode all → inherited ms; unrestricted adder + mode all → nil; restricted
  adder + mode none → nil; client-supplied field overwritten; zero-time HSS
  not inherited.
- **room-worker `handler_test.go`**: `historySharedSincePtr` table extended
  for the inherited-cap branch; `processAddMembers` test asserting created
  subscriptions and `MemberAddEvent` (local + federated) carry the inherited
  value.
- **room-service integration test**: projection now returns
  `historySharedSince` (extend the projection drift-guard test).

## Coverage / gates

`make lint`, `make test` (race), coverage ≥80% on touched packages,
`make sast` before push. Client-facing handler touched → `docs/client-api.md`
updated in the same PR (plus derived views).
