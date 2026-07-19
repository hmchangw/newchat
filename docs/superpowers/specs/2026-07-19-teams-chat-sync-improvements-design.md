# teams-chat-sync improvements — Design

**Date:** 2026-07-19
**Status:** Approved
**Amends:** `2026-07-14-teams-chat-sync-design.md`, `2026-07-15-teams-chat-member-sync-design.md`

Focused changes to the Teams chat sync pipeline, each independent: (1) finalize
small chats inline, (2) longer run timeouts, (3) throttle logging, (4) fetch the
least-caught-up users first, (5) ensure indexes at startup.

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

## 2. Longer run timeouts

`RUN_TIMEOUT` is the whole-job context deadline for these run-to-completion
CronJobs. Backfills over the whole federation, paced by Graph throttling, can run
far longer than the old 30m default.

- `teams-chat-sync`: **`240h`** (10 days)
- `teams-chat-member-sync`: **`48h`** (2 days)

Go's `time.Duration` (and `caarlos0/env`) has no `d` unit, so the values are
expressed in hours. Both stay overridable via the `RUN_TIMEOUT` env var; the
`deploy/docker-compose.yml` defaults track the code defaults.

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

## 4. Fetch the least-caught-up users first

`ListUsers` now reads `teams_user` **ordered by `from` ascending**. In Mongo's
ascending order a missing/null `from` sorts before any date, so users that have
never synced (no watermark, `from` is `null` on the first run) are returned —
and dispatched to workers — first, then the rest oldest-watermark to newest.
The full-collection cache load is unchanged (all users still populate the vote
map); only the order changes, so the most-behind users get Graph time first.

## 5. Ensure indexes at startup

`mongoStore.EnsureIndexes` (idempotent, write/primary client) is called from
`run()` right after the store is built. teams-chat-sync owns `teams_chat` and
the `teams_user.from` watermark, so it owns these indexes:

| Collection | Index | Type | Serves |
|---|---|---|---|
| `teams_user` | `{from: 1}` | full | the new watermark-ordered `ListUsers` (non-sparse so null sorts first) |
| `teams_chat` | `{needMemberSync: 1}` | partial `{needMemberSync: true}` | member-sync's pending scan |
| `teams_chat` | `{needCreateRoom: 1, _id: 1}` | partial `{needCreateRoom: true}` | room-creation's pending scan + `_id` sort |

Partial-on-`true` keeps the two flag indexes to the small actionable working set
as `teams_chat` grows. `_id`-keyed writes and `{_id: $in}` reads use the default
`_id` index.

## Testing

TDD, per repo convention (≥ 80% coverage):

- **Item 1** — `buildChat` threshold table (oneOnOne / below / at / above the
  threshold); `chatUpsertModel` inline-finalize `$set`/`$setOnInsert` split; a
  MongoDB integration test that a small chat is finalized with members +
  `needCreateRoom`, and that a re-sync refreshes the roster and re-flags
  `needCreateRoom` while `siteId` stays immutable.
- **Item 2** — `TestConfig_Defaults` asserts the new `RUN_TIMEOUT` defaults parse.
- **Item 3** — a slog-capturing test asserting a `WARN` with the operation,
  status, and `Retry-After` on a throttled chats request and a throttled members
  request, and no throttle log on success.
- **Item 4** — a MongoDB integration test seeding users with null / old / new
  watermarks out of order and asserting `ListUsers` returns null-first, then
  oldest to newest.
- **Item 5** — a MongoDB integration test asserting `EnsureIndexes` creates the
  three indexes (names, key specs, partial filters) and is idempotent.

## Out of scope

- Making the threshold configurable (kept a constant; a follow-up could add
  `GRAPH_INLINE_MEMBER_THRESHOLD` if ops need to tune or disable it).
- Kubernetes CronJob manifests / `activeDeadlineSeconds` (ops/IaC).
- Throttle handling for the non-`getThrottled` Graph endpoints
  (directory/user-list/meetings), which are unused by these two jobs.
