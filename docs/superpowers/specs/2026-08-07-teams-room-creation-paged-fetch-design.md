# teams-room-creation: paged Mongo fetch (page size 2000)

**Date:** 2026-08-07
**Status:** Approved for implementation (autonomous session — recommended options selected)

## Problem

`runner.run` currently loads **every** `teams_chat` with `needCreateRoom=true` into
memory in a single `ListChatsNeedingRoom` query, then plans and publishes all
batches. A large backlog (e.g. initial Teams migration with hundreds of
thousands of flagged chats) makes the job's memory footprint unbounded and the
single query slow.

## Change

Fetch flagged chats from MongoDB in **pages of 2000** (configurable), dispatch
each page to the existing worker pool, wait for the whole page to finish, then
fetch the next page — until a page comes back empty.

### Pagination strategy: keyset cursor on `_id`

Each page queries:

```
needCreateRoom == true AND _id > <lastSeenID>   (no _id bound on the first page)
sort: _id ascending
limit: PageSize
```

The cursor advances to the last `_id` of each page regardless of per-batch
publish outcomes.

**Why keyset and not re-query-first-page:** successfully published chats have
their flag cleared, but a chat whose publish *fails* stays flagged. With a
naive "re-fetch the first 2000" loop, a persistently failing site would return
the same page forever and the run would never terminate. Keyset pagination
strictly advances through `_id` space, so the run always terminates; failed
chats are retried on the **next CronJob run** — exactly today's semantics.

**Why not a single server-side Mongo cursor:** it holds a cursor open for the
entire run (idle-timeout risk while workers drain a page) and doesn't fit the
`mongoutil.Collection.FindMany` abstraction. Repeated limited queries on the
indexed `needCreateRoom`/`_id` path are cheap and restartable.

## Component changes

### `store.go` / `store_mongo.go`

```go
ListChatsNeedingRoom(ctx context.Context, afterID string, limit int) ([]model.TeamsChat, error)
```

- `afterID == ""` → no lower bound (first page); otherwise `_id: {$gt: afterID}`.
- Adds `mongoutil.WithLimit(int64(limit))`; existing projection and `_id` sort
  are unchanged (the sort is now load-bearing for the cursor).
- Mocks regenerated via `make generate SERVICE=teams-room-creation`.

### `config.go`

- New knob: `PageSize int env:"MONGO_PAGE_SIZE" envDefault:"2000"` — the number
  of flagged chats fetched per Mongo query.
- `validateConfig` rejects non-positive values.
- Existing `ROOM_CREATE_BATCH_SIZE` (chats per published event, default 100)
  and `MAX_WORKERS` are unchanged and orthogonal: a 2000-chat page still fans
  out as ~20 batches of 100.

### `runner.go`

`run` becomes a page loop:

1. Fetch a page (`afterID`, `PageSize`).
2. If the page is empty: stop (log "no chats need room creation" only if the
   *first* page is empty).
3. Group the page by site, chunk into `BatchSize` event batches, publish under
   the existing `MaxWorkers` semaphore, `wg.Wait()` for the page.
4. Advance `afterID` to the page's last `_id`; go to 1.

- A list error on *any* page aborts the run with an error (pages already
  processed keep their cleared flags — publishes are independently durable).
- Context cancellation (SIGTERM) is honored between pages via the list query's
  ctx, same as today.
- `runConfig` gains `PageSize`.

## Not changing

- Per-batch publish/mark semantics (compare-and-set on `updatedAt`) — untouched.
- Event schema, subjects, streams — untouched; no `docs/client-api.md` impact
  (no client-facing handler involved).
- Site grouping stays **per page**: a site spanning a page boundary simply
  produces batches from each page. Batches were already independent events, so
  this changes nothing for the consumer (`room-worker` is idempotent on chat id).

## Testing

- `runner_test.go` (unit, mocked store): multi-page happy path (page fully
  processed before next fetch is asserted via `gomock.InOrder` + capture
  ordering), empty first page, list error on a later page, cursor advances past
  a failed batch, existing batch/flag tests updated to the paged signature.
- `config_test.go`: PageSize validation cases.
- `store_mongo_test.go` (integration): pagination — seed >limit flagged chats,
  assert page contents, `afterID` bound, limit, and empty-page termination;
  existing tests updated.
- `integration_test.go` e2e: updated to the new runConfig (single-page pass);
  multi-page behavior is covered by the runner unit tests plus the store
  pagination integration test.
