# Batched Member Fetch and Write in `teams-chat-member-sync`

## Summary

`teams-chat-member-sync` currently issues one Graph `GET /chats/{id}/members`
per chat and one conditional Mongo `UpdateOne` per chat. This design replaces
both with batches of 15:

1. **Graph side.** A new `pkg/msgraph` surface posts up to 20 sub-requests to
   the Graph JSON batching endpoint (`POST /$batch`), so 15 chats' member lists
   are fetched in one HTTP round-trip instead of 15.
2. **Mongo side.** The 15 resulting conditional updates are issued as one
   unordered `BulkWrite`.

The worker pool is retained: `jobs` carries `[]ChatToSync` of 15 rather than a
single `ChatToSync`, so `MAX_WORKERS` (default 8) workers each own one in-flight
`$batch` POST and one `BulkWrite`.

**What this buys.** Fewer HTTP round-trips and TLS handshakes — material for
this job, which runs on-prem behind a TLS-intercepting proxy. It does **not**
reduce Graph throttle consumption: `$batch` sub-requests bill individually
against the per-app+tenant budget, so 15 chats still cost 15 requests. In-flight
chats rise from 8 to 120; the existing tenant-wide throttle gate absorbs the
resulting pressure by serialising all workers on any 429, and `MAX_WORKERS`
remains the lever for a throttle-tight tenant.

Optimistic concurrency (`errSuperseded`) is preserved. Batching the write costs
per-chat visibility, which is bought back with a follow-up read — see
[Identifying superseded chats](#identifying-superseded-chats).

## Graph batching surface (`pkg/msgraph/batch.go`)

### Request and response shape

```json
POST /$batch
{"requests":[{"id":"0","method":"GET","url":"/chats/{chatID}/members"}, ...]}
```

Sub-request `url` is relative to the API version root — no
`https://graph.microsoft.com/v1.0` prefix. `GET` sub-requests carry no headers.

```json
{"responses":[{"id":"0","status":200,"body":{"value":[...]}}, ...]}
```

Four properties of `$batch` drive the implementation:

| Property | Handling |
|----------|----------|
| Responses return **unordered** | Match sub-responses to chats by `id`, never by position. `id` is the decimal index into the caller's `chatIDs` slice. |
| Each sub-response has its own `status` | A per-chat failure is recorded against that chat only; the other 14 still succeed. |
| A sub-response can be **429 with its own `Retry-After`** | Arm the shared throttle gate via `noteThrottle`, then re-batch **only the throttled ids**, up to `chatsMaxAttempts`. |
| A sub-response body can carry **`@odata.nextLink`** | Drain that chat's remaining pages via the shared `drainMemberPages` helper (see below). The nextLink is absolute, so it is passed through unchanged. |

### Public API

```go
// ChatMembersBatchResult is one chat's outcome within a batch. Err is set when
// that chat alone failed; Members is then nil.
type ChatMembersBatchResult struct {
    ChatID  string
    Members []ChatMemberDetail
    Err     error
}

// ChatMembersBatchReader fetches several chats' members in one Graph $batch
// request. App-only (Chat.Read.All / ChatMember.Read.All).
type ChatMembersBatchReader interface {
    // ListChatMembersBatch returns one result per requested chat, in the
    // order requested. The returned error is non-nil only when the whole
    // batch failed; per-chat failures are carried in each result's Err.
    ListChatMembersBatch(ctx context.Context, chatIDs []string) ([]ChatMembersBatchResult, error)
}
```

The split between the outer `error` and per-result `Err` is the point of the
signature: the outer error covers whole-batch failures (token acquisition,
transport, outer non-200 after retries, malformed envelope), while a single
chat's 403 or 404 never denies the other 14 their write.

Constructor `NewChatMembersBatchClient(cfg Config, opts ...Option)` mirrors
`NewChatMembersClient` — same `*graphClient`, same `applyProxyURL` handling.

### Input guards

- `len(chatIDs) == 0` → `(nil, nil)`.
- `len(chatIDs) > 20` → error. Graph's hard ceiling is enforced here rather
  than trusted from config, so a misconfigured `GRAPH_BATCH_SIZE` fails loudly
  instead of producing a rejected batch.
- An empty string in `chatIDs` → that entry's `Err` is set; the rest proceed.

### Throttle handling and the `doThrottled` refactor

`getThrottled` currently welds the gate-wait/429-retry/body-read loop to
`http.MethodGet`. The loop is extracted into a shared helper:

```go
func (g *graphClient) doThrottled(ctx context.Context, operation string,
    build func() (*http.Request, error)) ([]byte, error)
```

`getThrottled` becomes a thin wrapper over it, so `chats.go` behaviour is
unchanged, and the new POST path inherits identical semantics: waits out the
tenant-wide gate before every attempt, arms/extends the gate on 429/503 (even
on the final failed attempt), retries to `chatsMaxAttempts`, bounds the body
read at 64 MiB, and logs status/`Retry-After`/backoff without ever logging the
token or endpoint.

### Page-drain reuse

Both paths need "follow `@odata.nextLink` until exhausted", so that loop is
extracted from `ListChatMembers` into:

```go
func (g *graphClient) drainMemberPages(ctx context.Context, token, next string) ([]ChatMemberDetail, error)
```

`ListChatMembers` becomes "build the first URL, then `drainMemberPages`". The
batch path decodes the first page from the sub-response body and calls
`drainMemberPages` with the sub-response's `@odata.nextLink` (empty for the
common single-page case, which therefore costs no extra request).

Together with `doThrottled`, these two extractions are the only changes to
existing `pkg/msgraph` code; both are behaviour-preserving.

Two throttle layers therefore apply, and both feed the same gate:

- **Outer POST 429/503** — handled by `doThrottled`, retries the whole batch.
- **Sub-response 429** — handled in `ListChatMembersBatch`, re-batches only the
  throttled ids. Chats that already succeeded are not re-requested.

A chat whose sub-request is still 429 after `chatsMaxAttempts` gets its `Err`
set; it is not a whole-batch failure.

## Syncer changes

### Interfaces (`store.go`)

```go
// membersFetcher is the Graph surface the sync consumes.
type membersFetcher interface {
    ListChatMembersBatch(ctx context.Context, chatIDs []string) ([]msgraph.ChatMembersBatchResult, error)
}

// MemberSyncUpdate is one chat's pending conditional write.
type MemberSyncUpdate struct {
    ChatID        string
    SeenUpdatedAt time.Time
    Members       []model.TeamsChatMember
}
```

`TeamsChatStore.SetMembersSynced` is replaced by:

```go
// SetMembersSyncedBatch applies every update as one unordered BulkWrite of
// conditional UpdateOnes, each matching on {_id, updatedAt}. It returns the
// ids of chats whose updatedAt no longer matched (superseded by a concurrent
// teams-chat-sync rewrite); those keep needMemberSync=true for retry.
SetMembersSyncedBatch(ctx context.Context, updates []MemberSyncUpdate, now time.Time) ([]string, error)
```

`errSuperseded` as a returned sentinel is no longer needed — supersession is
now reported as a list of ids rather than an error — so it is removed along
with the `errors.Is` branch in `run`.

`TeamsUserStore` and `accountCache` are unchanged.

### `syncBatch` (`syncer.go`)

`run` chunks `chats` into slices of `cfg.BatchSize` (a ragged final batch is
expected and fine) and feeds `jobs chan []ChatToSync`. Worker count, shutdown,
`sync.WaitGroup`, and the summary log line are otherwise unchanged.

`syncChat` becomes `syncBatch(ctx, batch []ChatToSync, sum *summary)`:

1. `graph.ListChatMembersBatch(ctx, ids)`. A non-nil error fails every chat in
   the batch: `Failed += len(batch)`, one error log carrying the batch size and
   error (not 15 identical lines).
2. Partition results. Each result with `Err != nil` → `Failed++` and an error
   log with that `chatID`; excluded from the write set.
3. `buildMembers` over **all surviving chats at once**, so `accountCache.resolve`
   issues one `AccountsByIDs` per batch instead of per chat. A `resolve` failure
   fails the whole batch's remaining chats (it is an infra failure, not a
   per-chat one).
4. `chats.SetMembersSyncedBatch(ctx, updates, s.cfg.Now())`. Returned superseded
   ids → `Superseded++` each, with today's WARN per `chatID`. Every other chat
   in the write set → `Succeeded++` and `MembersWritten += len(members)`.

`MembersWritten` keeps its current meaning: superseded chats are not counted,
because nothing was written for them.

### Counter invariant

`Succeeded + Failed + Superseded == Total` must hold across every path,
including a whole-batch Graph error and a `buildMembers` failure. This is
asserted in tests.

## Batched conditional write (`store_mongo.go`)

```go
models[i] = mongo.NewUpdateOneModel().
    SetFilter(bson.M{"_id": u.ChatID, "updatedAt": u.SeenUpdatedAt}).
    SetUpdate(setMembersSyncedUpdate(u.Members, now))
```

issued through the existing unordered `writeChats.BulkWrite`.
`setMembersSyncedUpdate` is reused verbatim — the `$set` document per chat is
identical to today's.

`BulkWrite` returns `(nil, nil)` on empty input, so an all-failed batch
short-circuits without a round-trip.

### Identifying superseded chats

`BulkResult` reports only an aggregate `Matched`, so a mismatch says *how many*
chats were superseded but not *which*. Today each superseded chat logs its own
`chatID`, and that fidelity is preserved:

- `res.Matched == len(updates)` → return `nil` immediately. The common path
  costs no extra query.
- Otherwise → one read of `{_id: {$in: batchIDs}, needMemberSync: true}`
  projected to `_id`. A chat still carrying `needMemberSync: true` after the
  bulk write is exactly one whose conditional filter did not match.

**This follow-up read uses the write (primary) client, not `readChats`.**
`readChats` is secondary-preferred; a secondary lagging the bulk write we just
issued would report updated chats as still-flagged, converting successes into
phantom "superseded" entries. `mongoStore` therefore gains a `writeChats`-based
read path for this query alone. `readChats` keeps serving `ListChatsToSync`,
and `readUsers` keeps serving `AccountsByIDs`.

## Configuration

`main.go` `Config` gains:

```go
GraphBatchSize int `env:"GRAPH_BATCH_SIZE" envDefault:"15"`
```

`validateConfig` rejects `< 1` and `> 20` with a message naming the Graph
ceiling. The Graph layer enforces 20 independently (defence in depth), so
neither layer relies on the other.

`WithMaxIdleConns(cfg.MaxWorkers)` is unchanged and still correct: each worker
still issues one sequential request at a time, just a larger one, so one warm
idle connection per worker remains the right pool size. The comment above it is
updated to say "one `$batch` request at a time".

`deploy/docker-compose.yml` gains `GRAPH_BATCH_SIZE=15` for parity with the
default.

## Error handling

This is a run-to-completion CronJob with no client-facing NATS or HTTP
boundary, so `pkg/errcode` does not apply. All errors are wrapped with
`fmt.Errorf("<what this function was doing>: %w", err)`, matching the existing
files.

The job's exit contract is unchanged: `run` returns an error when any chat
failed, so `main` exits non-zero and the CronJob records the failure.
Superseded chats remain benign — `needMemberSync` stays `true` and they retry
next run.

Graph error bodies are never logged raw (they can carry upstream payload);
only status and the Graph error `code` are surfaced, as `getThrottled` already
does.

## Testing

TDD per `CLAUDE.md` — tests written and failing first, minimum implementation
second. Mocks regenerated with `make generate SERVICE=teams-chat-member-sync`.

### `pkg/msgraph/batch_test.go` (`httptest` server)

| Case | Assertion |
|------|-----------|
| Happy path, 15 chats | All 15 results populated, one POST issued |
| Responses returned out of order | Each chat gets its own members, matched by `id` |
| Sub-response 404 / 403 | That chat's `Err` set; other 14 unaffected |
| Sub-response 429 with `Retry-After` | Gate armed; **only** throttled ids re-batched; succeeds on retry |
| Sub-response 429 past `chatsMaxAttempts` | That chat's `Err` set; batch error is nil |
| Outer POST 429 | Whole batch retried through the gate |
| Outer POST 500 | Whole-batch error; no per-chat results |
| Sub-response with `@odata.nextLink` | Follow-up pages fetched and merged in order |
| Malformed envelope | Whole-batch error |
| Empty `chatIDs` | `(nil, nil)`, no HTTP call |
| 21 `chatIDs` | Error, no HTTP call |
| Sub-request URL shape | Asserted relative (`/chats/{id}/members`), token in header, token never logged |

### `syncer_test.go`

Chunking into 15s including a ragged tail; per-chat Graph error isolated (the
other 14 still written); whole-batch Graph error counts all as failed; superseded
ids counted and logged per `chatID`; `MembersWritten` excludes superseded chats;
one `AccountsByIDs` call per batch; the counter invariant above; context
cancellation mid-run.

### Store tests

`store_mongo_test.go` for the model-construction logic; integration test
(`//go:build integration`, `testutil.MongoDB`) for the bulk conditional write:
all-match path issues no follow-up read; partial-match path returns exactly the
superseded ids; empty input short-circuits.

Coverage floor of 80% applies; `pkg/msgraph/batch.go` and the syncer target 90%+.

## Out of scope

- `docs/client-api.md` and its derived views are **not** affected: this service
  registers no client-facing NATS handler or HTTP route, and no `pkg/model`
  struct changes.
- `ListChatMembers`, `ChatMembersReader`, and `NewChatMembersClient` are
  **retained**, even though `teams-chat-member-sync` — their only production
  caller today — stops using them. `ListChatMembers` is not dead internally
  (it shares `drainMemberPages` with the batch path), but
  `NewChatMembersClient` will have no caller. Deleting public surface from a
  shared package is unrelated cleanup, so it is left in place; say the word if
  you would rather not carry an uncalled constructor and it can be removed in
  the same change.
- No change to Graph throttle budget, permissions, or the CronJob schedule.
