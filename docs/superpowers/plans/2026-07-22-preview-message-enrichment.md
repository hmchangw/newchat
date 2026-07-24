# Room-list preview enrichment (`PreviewMessage`) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Enrich the room-list preview (issue #110) so it carries `attachments`,
`mentions` (with display names), `visibleTo`, and a sender that shows `chineseName` /
bot app name — on top of the #104 system/quoted skip. Design: `../specs/2026-07-22-preview-message-enrichment-design.md`.

**Tech Stack:** Go. `history-service` (read path), `pkg/model` (wire type), `user-service` (consumer).

---

## File Structure

- Modify: `pkg/model/message.go` — `LastMessage` → `PreviewMessage` (enriched); `RoomsGetResponse`.
- Modify: `pkg/model/subscription.go` — `SubscriptionRoom.LastMessage` type.
- Modify: `history-service/internal/models/message.go` — type alias.
- Modify: `history-service/internal/service/rooms.go` — `toPreviewMessage` mapper.
- Modify: `history-service/internal/service/reactions.go` — extract `botAwareDisplayName`.
- Modify: `user-service/{service/service.go,historyclient/client.go,service/subscriptions.go,service/mocks/mock_repository.go}` — rename ripple.
- Modify: `docs/client-api.md` — preview schema (doc-ratchet).
- Tests: `history-service/internal/service/{rooms_test.go,integration_test.go}`, `history-service/internal/models/roomsget_test.go`.

---

### Task 1: Wire type + rename

- [ ] Add `attachments`/`mentions`/`visibleTo` to a renamed `PreviewMessage` (sender = wire `Participant`); `// TODO(#106): forwardSource`.
- [ ] Ripple `LastMessage` → `PreviewMessage` across the alias + `user-service` consumers; keep the `SubscriptionRoom.LastMessage` field name.
- [ ] `go build ./...` clean.

### Task 2: Enrichment mapper

- [ ] Extract `HistoryService.botAwareDisplayName(ctx, engName, chineseName, account)` from `ReactMessage`; call it from both sites.
- [ ] Add `toPreviewMessage`: decode attachments (`decodeMessageAttachments`), map sender + mentions via `toWireParticipant`, set sender `displayName`, pass through `visibleTo`.
- [ ] `roomLastMessage` returns `s.toPreviewMessage(...)`; keep the #104 skip.

### Task 3: Docs + tests

- [ ] `docs/client-api.md`: `LastMessage` → `PreviewMessage` schema + example (sender wire shape, new fields).
- [ ] Unit (`rooms_test.go`): attachments/mentions/visibleTo mapped; chineseName from `company_name`; bot → app name; normal sender unaffected; empty collections omit.
- [ ] Integration (`integration_test.go`): seed a message with attachments + mentions, assert the preview carries them.
- [ ] Keep #104's filter tests green.

**Verify:** `chat-lint.sh ./...` clean; `go test ./history-service/... ./user-service/service/...`; `go vet -tags integration ./history-service/internal/service/`.

### Wire-level verification (dev stack)

Beyond the in-repo unit + integration tests, the preview was verified end-to-end at
the NATS request/reply layer on our dev stack: drove the real `rooms.get` (via
`subscription.getChannels`) against a room whose latest message is a quoted reply, and
confirmed the preview resolves to the non-quoted survivor (system + quoted skipped) with
the enriched fields present on the wire. The harness lives in our tooling, not this repo.
