# Plan: Timestamp pivot for Load Surrounding Messages

Spec: `docs/superpowers/specs/2026-07-25-history-surrounding-timestamp-pivot-design.md`

TDD throughout: write the failing test (Red), implement to green (Green), tidy (Refactor).

## Step 1 — Repo inclusive reads (Red → Green)

1. **Red:** add integration tests in `internal/cassrepo/messages_by_room_integration_test.go`:
   - `GetMessagesAtOrBefore` includes the row whose `created_at == at` and same-ms siblings;
     excludes `created_at > at`; respects `floor`.
   - `GetMessagesBetweenDescInclusive` includes `<= at`, excludes `<= since`.
2. **Green:** in `messages_by_room.go`, extract the shared before/between walk into internal helpers
   taking `inclusiveUpper bool`; select between constant `< ?` / `<= ?` query strings. Expose:
   - `GetMessagesBefore` (strict) / `GetMessagesAtOrBefore` (inclusive).
   - `GetMessagesBetweenDesc` (strict) / `GetMessagesBetweenDescInclusive` (inclusive).
   Keep the strict methods' behavior byte-identical.

## Step 2 — Reader interface + mocks

1. Add `GetMessagesAtOrBefore` and `GetMessagesBetweenDescInclusive` to `MessageReader`
   (`internal/service/service.go`).
2. `make generate SERVICE=history-service` to regenerate `internal/service/mocks/mock_repository.go`.
3. Confirm `var _ MessageRepository = (*cassrepo.Repository)(nil)` still compiles.

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
