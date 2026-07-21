# `rooms.get` — room last-message endpoint (chat#393)

**Closes:** hmchangw/chat#393 (A2 read-only, E2 `rooms.get`). Design locked in the
#393 design-lock comment (2026-06-29, maintainer-approved by mliu33).

**What:** a new history-service RPC `rooms.get` — the customer's `/api/v1/rooms.get`
(mobile's room-last-message fetch). Per-site, account-scoped, **batch** `{roomIds[]}` →
`{roomId → lastMessage}`, resolved at read time. Modeled on the existing
`ThreadSubscriptionList` per-site history RPC.

## Design (as built)
- **Subject:** `chat.user.{account}.request.history.{siteID}.rooms.get` (account+site
  from the subject; roomIds in the body). One batch RPC per site — the caller groups a
  user's rooms by site and fans out, exactly like the thread-inbox.
- **Resolution (history-service `RoomsGet` → `roomLastMessage`):** per room, run the
  existing access check (`checkAccessAndRoomTimes`) then read the latest message via the
  existing `messages_by_room … DESC` primitives (`GetMessagesBefore` /
  `GetMessagesBetweenDesc`, limit 1) — respects the per-room access window + history
  floor (mirrors LoadHistory). No new repo method.
- **A2 read-only:** no denormalized write, no event, no edit/delete hooks, **no
  walk-back** — a soft-deleted latest message is returned as-is with `deleted=true`.
- **Best-effort batch:** bounded-concurrency (16) per-room resolve; a room that's
  not-accessible / empty / errored is **omitted**, never failing the batch. Batch ≤ 100.
- **Content** preview-trimmed to 256 runes.

## Files
- `pkg/subject/subject.go` — `RoomsGet` + `RoomsGetPattern` builders (+ test).
- `history-service/internal/models/message.go` — `RoomsGetRequest`, `RoomsGetResponse`,
  `LastMessage` (+ test).
- `history-service/internal/service/rooms.go` — `RoomsGet` handler + `roomLastMessage`
  resolver + `previewContent`/`dedupRoomIDs` helpers (+ test).
- `history-service/internal/service/service.go` — register the handler.
- `docs/client-api.md` — §3.2 "Get Rooms Last Message" (doc-ratchet).

## Out of scope (follow-ups)
- user-service room-list integration (calling `rooms.get` per-site during
  `ListSubscriptions` for the web sidebar) — separate PR; this delivers the endpoint
  the mobile calls.
- `roomLastMsg` live event / edit-delete hooks (A2 read-only).

## Verification
- `go build ./pkg/subject/... ./history-service/...` — clean.
- `go test -p 1 ./pkg/subject/... ./history-service/internal/models/... ./history-service/internal/service/...` — green.
- Dev-stack NATS-wire e2e: pending (per the PR-dev-stack workflow).

## PR / merge
- Draft-first on `hmchangw/chat`; pr-self-audit (internal gate) + `/simplify` +
  Jacob review + dev-stack e2e before ready; maintainer (mliu33/hmchangw) merges.
