# Forwarded messages — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** a message can be sent as a forward; the canonical/persisted message and the delivered event carry a `Forwarded` snapshot (mirroring `QuotedParentMessage`), and an empty-content forward previews as `"Forwarded a message"` in the room list.

**Architecture:** A nullable `ForwardedMessage` snapshot on `model.Message`, resolved by `message-gatekeeper` from a new `ForwardedFromMessageID` request field, persisted on `messages_by_room` (new UDT + column), and read back by `history-service` for the room-list preview. Mirrors the quote path throughout; a forward degrades (never hard-fails) on an unresolvable source.

**Tech Stack:** Go 1.25, gocql (Cassandra), `pkg/atrest` (envelope encryption), `pkg/errcode`, `pkg/idgen`, NATS.

**Design spec:** `docs/superpowers/specs/2026-07-22-forward-message-design.md`
**Branch:** `feat/forward-message`

---

## File map

**New files**
- `docker-local/cassandra/init/09-udt-forwarded_message.cql` — the `chat."ForwardedMessage"` UDT.
- `docker-local/cassandra/migrations/2026-07-forwarded-messages-by-room.cql` — additive UDT + `ALTER TABLE … ADD forwarded`.

**Modified files**
- `pkg/model/cassandra/message.go` — `ForwardedMessage` struct + `Message.Forwarded` field + cql tag.
- `pkg/model/cassandra/message_test.go` — round-trip coverage + `ForwardedMessage` JSON test.
- `pkg/model/message.go` — `Message.Forwarded` + `SendMessageRequest.ForwardedFromMessageID`.
- `message-gatekeeper/handler.go` — validate the id, `resolveForwardSnapshot`, set `msg.Forwarded`.
- `message-gatekeeper/handler_test.go` — resolve table test + invalid-id reject test.
- `pkg/atrest/atrest.go` + `split.go` — strip/restore the forward body in `enc_payload`.
- `message-worker/store_cassandra.go` — bind `forwarded` on the `messages_by_room` INSERTs (plaintext + encrypted) + `buildCassandraMessage`.
- `message-worker/integration_test.go` — schema UDT + column; persist/read a forward.
- `history-service/internal/models/message.go` — `ForwardedMessage` type alias.
- `history-service/internal/cassrepo/messages_by_room.go` — read the new column on the room query.
- `history-service/internal/cassrepo/integration_test.go` + `messages_by_room_integration_test.go` — schema + read round-trip.
- `history-service/internal/service/rooms.go` — empty-content-forward preview label.
- `history-service/internal/service/rooms_test.go` — preview label + content-forward cases.
- `docs/client-api.md`, `docs/client-api/events.md`, `docs/client-api/request-reply.md`, `docs/cassandra_message_model.md` — request field, message/event schema, UDT + column.

---

## Tasks

### Phase 1 — model + storage schema
- [ ] Add `ForwardedMessage` struct + `Message.Forwarded` (cql `forwarded`) to `pkg/model/cassandra`; update the round-trip test + add a `ForwardedMessage` JSON test.
- [ ] Add `Forwarded` + `ForwardedFromMessageID` to `pkg/model/message.go`.
- [ ] Add the init UDT + `messages_by_room` column and the additive migration (keyspace-scoped, `messages_by_room` only).

### Phase 2 — gatekeeper resolve
- [ ] Validate `ForwardedFromMessageID` → `errcode.BadRequest`; add `resolveForwardSnapshot` (degrade on any fetch failure, never hard-fail); set `msg.Forwarded`.
- [ ] Tests: resolve → snapshot, unresolvable → placeholder, invalid id → reject.

### Phase 3 — persist + at-rest
- [ ] Bind `forwarded` on the `messages_by_room` INSERTs; project it in `buildCassandraMessage`.
- [ ] Mirror the quote body strip/restore for the forward body in `pkg/atrest`.
- [ ] Integration: persist a forward + read the column back.

### Phase 4 — read + preview
- [ ] Read the column on the room query; render `"Forwarded a message"` for an empty-content forward in `roomLastMessage`.
- [ ] Tests: preview label + content-forward; cassrepo read round-trip.

### Phase 5 — docs + e2e
- [ ] Update client-api / events / request-reply / cassandra_message_model docs in this change.
- [ ] Author the dev-stack e2e (forward send → reply + persisted message carry `forwarded`; empty-content preview label).

🤖 Generated with Claude Code
