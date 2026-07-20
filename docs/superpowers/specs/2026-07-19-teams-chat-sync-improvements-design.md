# teams-chat-sync improvements — Design

**Date:** 2026-07-19
**Status:** Approved
**Amends:** `2026-07-14-teams-chat-sync-design.md`, `2026-07-15-teams-chat-member-sync-design.md`

Focused changes to the Teams chat sync pipeline, each independent: (1) finalize
small chats inline, (2) hand the run deadline to Kubernetes (remove
`RUN_TIMEOUT`), (3) throttle logging, (4) ensure teams_chat indexes at startup,
(5) drop the OTel SDK from teams-room-creation, (6) drop publish-side dedup from
teams-room-creation, (7) carry the member user id in the room-creation event.

## 1. Finalize small chats inline (skip member-sync)

**Problem.** `teams-chat-sync` lists chats with `$expand=members`, so it already
holds the full roster for small chats — yet it always defers group/meeting room
creation to `teams-chat-member-sync`, which re-fetches the roster via
`GET /chats/{id}/members`. That round trip is redundant work (and extra Graph
calls) for the common case of a small chat.

**Rule.** For a **non-oneOnOne** chat whose inline `$expand=members` roster has
**fewer than 25** members (`inlineMemberThreshold`), `teams-chat-sync` finalizes
the chat itself: it writes `members`, sets `needCreateRoom=true`, and sets
`needMemberSync=false` — so member-sync skips it. A chat with **25 or more**
members keeps the existing behavior (defer to member-sync), because Graph may
truncate a large inline expansion; the dedicated paginated fetch is authoritative.

The threshold must stay at or below Graph's inline-expansion cap for the
list-chats endpoint — that is what makes "fewer than threshold ⇒ complete" safe.
It is a named constant (`inlineMemberThreshold = 25`) in `teams-chat-sync/syncer.go`.

**Where the decision lives.** `buildChat` sets
`NeedMemberSync = chatType != oneOnOne && len(members) >= inlineMemberThreshold`.
`chatUpsertModel` then has three branches keyed on `chatType` + `NeedMemberSync`:

| Chat | `$setOnInsert` | `$set` |
|---|---|---|
| oneOnOne | everything incl. `needCreateRoom:true` | — (never modified) |
| small non-oneOnOne (`!NeedMemberSync`) | `createdDateTime, siteId` | `name, chatType, lastUpdatedDateTime, updatedAt, members, needMemberSync:false, needCreateRoom:true` |
| large non-oneOnOne (`NeedMemberSync`) | `createdDateTime, siteId, needCreateRoom:false` | `name, chatType, lastUpdatedDateTime, updatedAt, needMemberSync:true` |

**Why `needCreateRoom` is `$set:true` on the inline path (not `$setOnInsert`).**
This mirrors what member-sync's `SetMembersSynced` already writes: every re-sync
re-writes the fresh roster and re-flags `needCreateRoom`, so each chat change
yields exactly one create-or-sync event downstream. `teams-room-creation`'s
compare-and-set on `updatedAt` clears the flag and is safe against a re-sync that
lands between its read and its clear (the CAS just misses, leaving the chat for
the next run). A chat that crosses the 25 boundary between runs converges: the
`$setOnInsert`/`$set` split for `needCreateRoom` differs per branch but they are
mutually exclusive, so the same field is never written on both sides of one
update, and `errSuperseded` (member-sync's optimistic write) already guards the
concurrent-write race.

## 2. Run deadline owned by Kubernetes (RUN_TIMEOUT removed)

Backfills over the whole federation, paced by Graph throttling, can run far
longer than any sensible app-level default, and the deadline is an operational
concern better expressed once in the Job spec. So `RUN_TIMEOUT` is **removed**
from all teams-* jobs (`teams-chat-sync`, `teams-chat-member-sync`,
`teams-room-creation`); the Kubernetes CronJob owns the deadline via
`activeDeadlineSeconds`.

Each `run()` replaces `context.WithTimeout(…, RUN_TIMEOUT)` with
`signal.NotifyContext(context.Background(), SIGINT, SIGTERM)`, so the SIGTERM
Kubernetes sends when the deadline is exceeded (or the pod is deleted) cancels
the run context — the job aborts cleanly between operations (Graph calls,
throttle waits, and Mongo all honor the context) instead of being SIGKILLed
mid-batch. This matches the sibling `teams-user-sync`, which already worked this
way. Config fields, validation, `deploy/docker-compose.yml` entries, and the
spec config-table rows for `RUN_TIMEOUT` are all dropped.

## 3. Log Graph throttling / 429 Retry-After

The shared `getThrottled` in `pkg/msgraph` handled 429/503 (per-request retry +
tenant-wide gate) **silently** — rate-limiting was invisible in the logs. It now
emits a `WARN` on every throttle response:

```
msgraph: graph throttled request, backing off
  operation, status, retryAfter, backoff, attempt, maxAttempts, willRetry
```

`getThrottled` takes an `operation` label (`"list user chats"` /
`"list chat members"`) so the log identifies the caller; `noteThrottle` returns
the capped backoff it armed so the log reports the actual wait. The token and
endpoint are never logged. Because `getThrottled` is shared, this covers both
`teams-chat-sync` (`ListUserChats`) and `teams-chat-member-sync`
(`ListChatMembers`).

## 4. Ensure teams_chat indexes at startup

`mongoStore.EnsureIndexes` (idempotent, write/primary client) is called from
`run()` right after the store is built. teams-chat-sync owns `teams_chat`, so it
owns these indexes:

| Collection | Index | Type | Serves |
|---|---|---|---|
| `teams_chat` | `{needMemberSync: 1}` | partial `{needMemberSync: true}` | member-sync's pending scan |
| `teams_chat` | `{needCreateRoom: 1, _id: 1}` | partial `{needCreateRoom: true}` | room-creation's pending scan + `_id` sort |

Partial-on-`true` keeps the two flag indexes to the small actionable working set
as `teams_chat` grows. `_id`-keyed writes and `{_id: $in}` reads use the default
`_id` index.

## 5. Drop the OTel SDK from teams-room-creation

`teams-room-creation` was the only teams-* job wiring `obs.Init` (OpenTelemetry
traces/metrics); the others use plain `log/slog`. It is removed for consistency:
`run()` no longer calls `obs.Init`/`obsShutdown`. It still connects to NATS, which
requires a tracer/propagator, so it passes no-ops (`noop.NewTracerProvider()`,
`propagation.TraceContext{}`) — the same call this service's integration test and
`tools/loadgen` already use. `o11y/nats` gates header work on `O11Y_ENABLED`, so
tracing stays off the hot path. slog is unchanged (set up in `main`, not `obs`).

## 6. Drop publish-side dedup from teams-room-creation

The batch publisher set a deterministic `Nats-Msg-Id`
(`teamroom:{siteID}:{sha256 of sorted chat ids}`) for JetStream server-side
dedup. But this is a CronJob that re-runs minutes-to-hours later — far outside
any `Duplicates` window — so cross-run dedup never fired, and the design already
called it "best-effort, not load-bearing". The real guarantee against duplicate
room creation is the downstream room-worker being idempotent on chat id. So the
`dedupID` helper and the `Nats-Msg-Id` are removed: `publishFunc` drops its
`dedupID` parameter and `PublishMsg` is called with no `WithMsgID`. The
`dedupID`-only `publisher_test.go` is deleted (the publish path stays covered by
`runner_test.go` and the integration test).

## 7. Carry the member user id in the room-creation event

`TeamsRoomCreateMember` originally dropped the Graph member id, carrying only
`account` + `visibleHistoryStartDateTime`. It now also carries the member's user
id (`id`, the AAD object id / `teams_user` `_id`) so the downstream room-worker
can correlate members by id, not just account. `buildEvent` maps it through (the
struct is now field-identical to the stored `TeamsChatMember`, so a direct
conversion carries all three fields), and the `pkg/model` round-trip test covers
the new field. This is an internal canonical event (`chat.room.canonical.…`), not
a client contract, so `docs/client-api.md` is unaffected.

## Testing

TDD, per repo convention (≥ 80% coverage):

- **Item 1** — `buildChat` threshold table (oneOnOne / below / at / above the
  threshold); `chatUpsertModel` inline-finalize `$set`/`$setOnInsert` split; a
  MongoDB integration test that a small chat is finalized with members +
  `needCreateRoom`, and that a re-sync refreshes the roster and re-flags
  `needCreateRoom` while `siteId` stays immutable.
- **Item 2** — config tests drop the `RUN_TIMEOUT` default assertion and the
  timeout validation cases (the field no longer exists).
- **Item 3** — a slog-capturing test asserting a `WARN` with the operation,
  status, and `Retry-After` on a throttled chats request and a throttled members
  request, and no throttle log on success.
- **Item 4** — a MongoDB integration test asserting `EnsureIndexes` creates the
  two `teams_chat` indexes (names, key specs, partial filters) and is idempotent.

## Out of scope

- Making the threshold configurable (kept a constant; a follow-up could add
  `GRAPH_INLINE_MEMBER_THRESHOLD` if ops need to tune or disable it).
- Kubernetes CronJob manifests / `activeDeadlineSeconds` (ops/IaC).
- Throttle handling for the non-`getThrottled` Graph endpoints
  (directory/user-list/meetings), which are unused by these two jobs.
