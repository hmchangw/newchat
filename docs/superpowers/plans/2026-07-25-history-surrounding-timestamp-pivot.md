# Plan: Timestamp pivot for Load Surrounding Messages

Spec: `docs/superpowers/specs/2026-07-25-history-surrounding-timestamp-pivot-design.md`

TDD throughout: write the failing test (Red), implement to green (Green), tidy (Refactor).

## Step 1 — Repo inclusivity via `+1ms` (Red → Green)

No new repository methods. The timestamp before-read reuses the strict
`GetMessagesBefore` / `GetMessagesBetweenDesc` with `beforeUpper = pivot + 1ms`.

1. **Red:** add integration tests in `internal/cassrepo/messages_by_room_integration_test.go`:
   - `GetMessagesBefore(pivot+1ms)` includes the exact-pivot row and same-ms siblings; excludes `> pivot`.
   - bucket-boundary case (`pivot` = last ms of its window): next-bucket message must not leak.
2. **Green:** no repo code change — the existing strict methods already provide this.

## Step 2 — (removed) no interface/mocks change

The `MessageReader` interface is unchanged, so no `make generate` is required for the reads.

## Step 3 — Models

1. Add `Timestamp *int64` to `LoadSurroundingMessagesRequest`; mark `messageId` `omitempty`.

## Step 4 — Handler (Red → Green)

1. **Red:** add unit tests in `internal/service/messages_test.go` (see spec Testing list) — timestamp
   happy path (no HSS / with HSS), 403 outside window, exactly-one-of + positivity validation,
   `limit == 1` skips after-read, more-before/after propagation, before/after error wrapping.
2. **Green:**
   - Extract `assembleSurrounding(c, roomID, accessSince, central, beforeFn, afterFn)` running the
     errgroup + assembly + redaction + response; route the existing messageId body through it
     (`central = centralMsg`) with no behavior change.
   - Add `loadSurroundingByTimestamp` using `checkAccessAndRoomTimes`, before-inclusive reads,
     strict after, `central = nil`, zero-count guard on the after read.
   - Make `LoadSurroundingMessages` validate exactly-one-of / positivity and dispatch.
3. Run existing surrounding tests — all must stay green.

## Step 5 — Docs

1. `docs/client-api.md` §Load Surrounding Messages — request table (exactly-one-of), no-central
   timestamp semantics, error cases, example.
2. `docs/client-api/request-reply.md` §Load Surrounding Messages — mirror request-field change.

## Step 6 — Verify & commit

1. `make generate SERVICE=history-service`, `make fmt`, `make lint`.
2. `make test SERVICE=history-service` (race); `make test-integration SERVICE=history-service`.
3. `make sast`.
4. Commit; push to `claude/history-service-timestamp-rpc-ehtbsb`.
