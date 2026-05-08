# Large-Room Post Restriction — Design

**Date:** 2026-05-07
**Branch:** `claude/validate-message-sending-5HTd9`
**Status:** Draft, awaiting team-lead confirmation on Q3 (edits/deletes)

## Summary

In rooms with more than 500 members, only owners, admins, and bots may send
top-level messages. Thread replies are exempt regardless of room size. Edits
and deletes are unaffected (author-only rule in `history-service` stands).
The check is added to `message-gatekeeper` as an admission gate before
publishing to `MESSAGES_CANONICAL`.

## Rule

A new top-level message is rejected when **all** of the following hold:

1. The send is **not** a thread reply (`req.ThreadParentMessageID == ""`).
2. The room's `userCount` is **strictly greater** than the configured
   threshold (default `500`).
3. The sender does **not** qualify for bypass — none of:
   - subscription carries `model.RoleOwner`, OR
   - subscription carries `model.RoleAdmin`, OR
   - sender's account matches the bot naming pattern (`\.bot$|^p_`).

When rejected, the user receives a coded error and no message is published to
`MESSAGES_CANONICAL`.

## Architecture & data flow

Single change point: `message-gatekeeper`. No other service is modified.

The new check sits between `GetSubscription` and `resolveQuoteSnapshot` in
`processMessage`:

```
Subject parse → SiteID match → unmarshal → ID/content validation
    ↓
GetSubscription (existing)
    ↓
[NEW] LARGE-ROOM CHECK
    ↓
resolveQuoteSnapshot (existing)
    ↓
Build Message + publish to MESSAGES_CANONICAL
```

Why here:

- The subscription is the data source for the role bypass — must come after
  `GetSubscription`.
- Quote-resolution is a downstream Mongo/RPC cost we should avoid paying for
  messages that will be rejected.
- Canonical publish must come after — gatekeeper is the admission gate.

Producers that already bypass gatekeeper bypass this rule too (by design):
`room-worker` writes system messages (`Message.Type != ""`) directly to
`MESSAGES_CANONICAL` — they remain unaffected.

## Predicate logic (approach: owner fast-path)

```go
isThreadReply := req.ThreadParentMessageID != ""

if !isThreadReply && !canBypassLargeRoomCap(sub) {
    userCount, err := h.store.GetRoomUserCount(ctx, roomID)
    if err != nil {
        return nil, &infraError{cause: fmt.Errorf("get user count for room %s: %w", roomID, err)}
    }
    if userCount > h.largeRoomThreshold {
        slog.Info("send blocked",
            "reason",    codeLargeRoomPostRestricted,
            "account",   account,
            "roomID",    roomID,
            "userCount", userCount,
            "threshold", h.largeRoomThreshold,
        )
        return nil, errLargeRoomPostRestricted
    }
}
```

The bypass predicate is a small named function — the single edit point for
the bypass policy:

```go
// canBypassLargeRoomCap reports whether the subscriber is exempt from the
// large-room post restriction. Owners, admins, and bots bypass.
func canBypassLargeRoomCap(sub *model.Subscription) bool {
    for _, r := range sub.Roles {
        if r == model.RoleOwner || r == model.RoleAdmin {
            return true
        }
    }
    return isBot(sub.User.Account)
}
```

### Bot detection — inline regex (drift acknowledged)

Bot identification reuses the convention defined by `room-service/helper.go:32`:

```go
var botPattern = regexp.MustCompile(`\.bot$|^p_`)

func isBot(account string) bool { return botPattern.MatchString(account) }
```

This is **duplicated inline** in `message-gatekeeper/helper.go` rather than
imported from `room-service` (which is internal to that service) or extracted
to a shared `pkg/botid` package. The duplication is intentional and
short-term:

- `room-service` is owned by another developer and we cannot refactor it on
  this PR's timeline.
- Any drift between the two regexes would be a real bug — if the bot-naming
  convention changes, both copies must be updated. **Mitigation:** the test
  suite `TestIsBot` in `message-gatekeeper` mirrors the cases in
  `room-service/helper_test.go:72-99`, so a divergence should manifest as a
  test difference even before reaching production.
- **Future cleanup:** when the third caller appears (or when ownership lines
  permit), promote `isBot` to `pkg/botid`. Both services would then import
  `botid.IsBot(account)`. This is a single-PR refactor and is explicitly out
  of scope for the current spec.

### Admin role — constant + bypass only

`pkg/model/subscription.go` gains a new `RoleAdmin` constant:

```go
const (
    RoleOwner  Role = "owner"
    RoleAdmin  Role = "admin"  // NEW
    RoleMember Role = "member"
)
```

What this PR does **not** do:
- Wire up admin support in `room-service`'s role-update RPC validation
  (currently `errInvalidRole = "must be owner or member"`).
- Add admin-aware logic to `errCannotDemoteLast`, `errPromoteRequiresIndividual`,
  or any other role-management invariants.
- Update frontend role-display logic.

Those changes are owned by other developers and tracked separately. The
gatekeeper bypass clause for admin is therefore live as soon as
`Subscription.Roles` contains `"admin"` — whether that arrives via a future
role-update RPC change, a direct DB write, an admin tooling path, or
otherwise. Until then, the admin clause is dormant.

The role enum's existing consumers iterate via range or use defaults in their
switches, so adding a new value is safe.

### Cost matrix

| Sender / message | Outcome | Added Mongo cost |
|---|---|---|
| Thread reply (any role, any room size) | Allowed | none |
| Top-level send by an owner | Allowed | none |
| Top-level send by an admin | Allowed | none |
| Top-level send by a bot account (any role) | Allowed | none |
| Top-level send by a regular member, room ≤ threshold | Allowed | +1 `findOne` |
| Top-level send by a regular member, room > threshold | Rejected | +1 `findOne` |

Four of the six cases pay zero new cost — and they cover the common paths
(thread replies, owners, admins, and any bot account).

### Error classification

`*infraError` (NACK + JetStream redelivery) for `GetRoom` failures. Plain
validation error (`errLargeRoomPostRestricted`, ACK + reply) for
non-owner-in-big-room. Mirrors how `GetSubscription` errors are classified
today.

## Store interface changes

`message-gatekeeper/store.go` — one new method:

```go
type Store interface {
    GetSubscription(ctx context.Context, account, roomID string) (*model.Subscription, error)
    GetRoomUserCount(ctx context.Context, roomID string) (int, error) // NEW
}
```

`message-gatekeeper/store_mongo.go` — `MongoStore` gains a `rooms` collection
field and a `GetRoomUserCount` method that uses a Mongo
`SetProjection({"userCount": 1})` to pull only the field the rule consumes
instead of the full `Room` document. No new indexes — `rooms._id` is
already the primary key.

Adding a method to `Store` invalidates the generated mock; regenerate via
`make generate SERVICE=message-gatekeeper`.

## Configuration

One new field on `message-gatekeeper`'s `Config` struct in `main.go`:

```go
LargeRoomThreshold int `env:"LARGE_ROOM_THRESHOLD" envDefault:"500"`
```

- Type `int` (matches `Room.UserCount`).
- Default `500` — the product rule of record.
- Not `required` — non-critical config with a sensible default, matching
  `CLAUDE.md` Section 6.
- Threaded into `Handler` via constructor injection alongside `siteID`:
  ```go
  type Handler struct {
      ...
      largeRoomThreshold int  // NEW
  }
  ```

Operational note: setting `LARGE_ROOM_THRESHOLD` to a very large value
(e.g. `999999999`) effectively disables the rule without a code change. This
is the rollback knob.

## Error contract & wire format

### `pkg/model/error.go` — backward-compatible field addition

```go
type ErrorResponse struct {
    Error string `json:"error"`
    Code  string `json:"code,omitempty"`  // NEW
}
```

`omitempty` is load-bearing: every existing `MarshalError(string)` call site
keeps producing identical bytes (`{"error": "..."}`).

### `pkg/natsutil/reply.go` — new helper

```go
// MarshalErrorWithCode encodes an error message and code as a JSON ErrorResponse.
func MarshalErrorWithCode(errMsg, code string) []byte {
    data, _ := json.Marshal(model.ErrorResponse{Error: errMsg, Code: code})
    return data
}
```

### `message-gatekeeper` — typed error sentinel

A small unexported type in `store.go` (alongside `errNotSubscribed`):

```go
type codedError struct {
    Code    string
    Message string
}

func (e *codedError) Error() string { return e.Message }

// codeLargeRoomPostRestricted is shared between the sentinel and the slog
// "reason" field so the wire code and log-query vocabulary stay aligned.
const codeLargeRoomPostRestricted = "large_room_post_restricted"

var errLargeRoomPostRestricted = &codedError{
    Code:    codeLargeRoomPostRestricted,
    Message: "posting is restricted to owners and admins in this room",
}
```

The handler's validation-error branch dispatches via `errors.As`:

```go
var ce *codedError
var replyData []byte
if errors.As(err, &ce) {
    replyData = natsutil.MarshalErrorWithCode(ce.Message, ce.Code)
} else {
    replyData = natsutil.MarshalError(err.Error())
}
h.sendReply(ctx, account, msg.Data(), replyData)
```

All other validation errors (subscription missing, content empty, etc.) flow
through the existing `MarshalError(err.Error())` path unchanged.

### Wire examples

Reject (new rule fires):
```json
{"error": "posting is restricted to owners and admins in this room", "code": "large_room_post_restricted"}
```

Existing reject (unchanged):
```json
{"error": "user alice is not subscribed to room R"}
```

## Logging

A single new log line on the rejection branch:

```go
slog.Info("send blocked",
    "reason",    codeLargeRoomPostRestricted,
    "account",   account,
    "roomID",    roomID,
    "userCount", userCount,
    "threshold", h.largeRoomThreshold,
)
```

- Level `Info`: this is expected behavior, not an anomaly.
- `reason` is the same `codeLargeRoomPostRestricted` constant emitted as the
  wire `code`, so log queries and frontend dispatch share the same
  vocabulary; promotion to a Prometheus counter label is mechanical.
- No log on bypass paths (would be enormous volume in busy 500+ rooms for zero
  diagnostic value).
- No log of `req.Content` — `CLAUDE.md` Section 3 forbids logging full message
  bodies.
- `requestID` is already on the structured log scope from request-id
  middleware; no manual threading needed.
- Existing log paths cover `GetRoomUserCount` infrastructure failures and
  ACK/NACK failures — no new lines added there.

## Testing strategy

TDD per `CLAUDE.md` Section 4. Three layers:

### `pkg/model`
- Round-trip tests for `ErrorResponse` covering both shapes (with and without
  `Code`), asserting `omitempty` keeps the wire format identical for the
  no-code case.
- Extend `TestRoleValues` to assert `RoleAdmin == "admin"` alongside the
  existing `RoleOwner` / `RoleMember` checks.

### `pkg/natsutil`
Unit test for `MarshalErrorWithCode` mirroring the existing `MarshalError`
test style.

### `message-gatekeeper`
Extend the existing table-driven `TestHandler_ProcessMessage` with the cases
below. Regenerate mocks first (`make generate SERVICE=message-gatekeeper`).

| Case | Roles | Account | userCount | ThreadParent | `GetRoomUserCount` expected? | Outcome |
|---|---|---|---|---|---|---|
| owner sends in big room | `[owner]` | `alice` | 600 | "" | no (fast-path) | success, publishes to canonical |
| **admin sends in big room** | `[admin]` | `alice` | 600 | "" | no (fast-path) | success, publishes to canonical |
| **bot account in big room (any role)** | `[member]` | `helper.bot` | 600 | "" | no (fast-path) | success, publishes to canonical |
| owner sends in small room | `[owner]` | `alice` | 50 | "" | no | success |
| member sends in small room | `[member]` | `alice` | 50 | "" | yes | success |
| member sends in big room | `[member]` | `alice` | 600 | "" | yes | reject `large_room_post_restricted`, no canonical publish |
| boundary: count == threshold | `[member]` | `alice` | 500 | "" | yes | success (strict `>`) |
| boundary: count == threshold+1 | `[member]` | `alice` | 501 | "" | yes | reject |
| member thread reply in big room | `[member]` | `alice` | 600 | non-empty | no (fast-path) | success, publishes to canonical |
| `GetRoomUserCount` returns Mongo error | `[member]` | `alice` | n/a | "" | yes (returns err) | infraError → NACK |
| `GetRoomUserCount` returns ErrNoDocuments | `[member]` | `alice` | n/a | "" | yes | infraError → NACK |
| Custom threshold (env=2), 3-person room | `[member]` | `alice` | 3 | "" | yes | reject |

Reject-case assertions check the wire payload:
```go
assert.Equal(t, `{"error":"posting is restricted to owners and admins in this room","code":"large_room_post_restricted"}`, string(replyData))
```

Per-case assertion fields supported by the table runner:
- `wantNoPublish bool` — set on every reject case so `assert.Empty(t, *publishedPtr)` runs against the captured `published` slice. Proves no canonical publish happens when the rule rejects.
- `checkResult` on bypass-success cases asserts `assert.Len(t, published, 1)`.
  Combined with the absence of a `GetRoomUserCount` mock expectation,
  this proves the fast-path both skipped the Mongo call and still
  published.

### Predicate unit test

`TestCanBypassLargeRoomCap` tables over a `Subscription` (roles + account)
and asserts the boolean. Cases must cover:
- `[owner]` → `true`
- `[admin]` → `true`
- `[member]`, plain account → `false`
- `[member]`, bot account (`helper.bot`, `p_scheduler`) → `true`
- `[]` (empty roles), bot account → `true`
- `[]` (empty roles), plain account → `false`
- combinations: `[owner, member]`, `[admin, member]` → `true`
- unknown role string (e.g. `"superuser"`), plain account → `false`

### `TestIsBot`

Mirrors `room-service/helper_test.go:72-99` so divergence in the
duplicated regex is detectable. Cases: `helper.bot` true, `p_scheduler`
true, `alice` false, `botmaster` (contains "bot" but not as suffix) false,
empty string false.

### Integration tests
Not added. The rule has no real Mongo/NATS dependency that requires
integration coverage; unit tests with mocked store cover all branches.

### Coverage target
≥80% required, target 90%+ for handlers (per `CLAUDE.md` Section 4).

## Out of scope

| Item | Why |
|---|---|
| Edit & delete authorization | Q3 — author-only rule in `history-service` stands. |
| System-message admission | Q4 — `room-worker` writes directly to `MESSAGES_CANONICAL`. |
| Promoting `isBot` to a shared `pkg/botid` package | Other-team-owned refactor; duplicated inline in gatekeeper for now. |
| Admin role-update RPC support in `room-service` | Other-team-owned; this PR adds only the `RoleAdmin` constant and the bypass clause. Role assignment via the RPC remains rejected by `errInvalidRole`. |
| Admin-aware role-promotion / demotion invariants (`errCannotDemoteLast` etc.) | Same — owned separately. |
| Frontend role-display for admin | Owned by other developers. |
| `Room.UserCount` lifecycle | Maintained by `room-worker.ReconcileUserCount`; this spec only reads. |
| Per-room threshold override | YAGNI; single env-var threshold. |
| Federation outbox/inbox flow | Q6 — gate runs on the room's home site by subject routing. |
| Cross-service refactor of error handling | New `Code` field on `model.ErrorResponse` is the only shared change. |
| Prometheus metrics | Q8 — logs only with structured `reason` label. |
| Frontend UX changes | Backward-compatible; existing clients keep reading `error`. |
| Schema migrations | None — uses existing `Subscription.Roles` and `Room.UserCount`. The new `RoleAdmin` constant is an enum value addition, not a schema change. |
| Stream / subject / event schema | Unchanged. |
| Production rollout audit | Q10 — pre-production. |

## Future follow-ups this spec sets up for

- **`pkg/botid` consolidation**: when ownership lines permit, lift `isBot` and
  the `\.bot$|^p_` regex into a shared `pkg/botid` package and switch both
  `room-service` and `message-gatekeeper` to import it. Single source of truth
  for the bot-naming convention.
- **Admin role wiring in `room-service`**: when the other team is ready, add
  `"admin"` to the role-update RPC's accepted set, decide promotion/demotion
  semantics, and update the failing tests that currently treat `"admin"` as
  the negative case. The gatekeeper bypass clause already handles admin once
  it can be assigned.
- **Edit/delete extension** (re-opening Q3): the `codedError` shape and
  `large_room_post_restricted` code are reusable from `history-service`. The
  check would live in `EditMessage` / `DeleteMessage`, not gatekeeper.
- **Metrics promotion**: same `reason` label key; one new
  `prometheus.CounterVec` registration plus one `.Inc()` call.

## Open question

**Q3 — edits and deletes scope** is parked awaiting team-lead confirmation.
This spec assumes the answer is "rule does not apply to edits/deletes"
(author-only stands). If the team later decides edits/deletes should also be
gated, that becomes a separate spec against `history-service` that reuses the
predicate vocabulary defined here.
