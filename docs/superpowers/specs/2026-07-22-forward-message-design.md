# Forwarded messages — mark, persist, preview

**Date:** 2026-07-22
**Branch:** `feat/forward-message`

## Overview

Add first-class forwarded-message support. Today `model.Message` has no forward concept — the system can't tell a forward from a normal message. This adds a forward marker that mirrors the existing quote mechanism (`QuotedParentMessage`) almost 1:1: a nullable `Forwarded` snapshot on the canonical message. Non-nil marks the message as a forward and carries the source's sender + content so the client can render it. An empty-content forward renders `"Forwarded a message"` in the room-list preview.

## Goals

- A message can be sent as a forward; the canonical/persisted message and the delivered event carry a `Forwarded` snapshot.
- An empty-content forward previews as `"Forwarded a message"` in the room list; a forward that carries its own content previews that content.
- Additive, non-breaking storage: a new UDT + one column on `messages_by_room`. Existing rows read back as non-forwards.
- client-api / events / cassandra_message_model docs updated in the same change.

## Non-goals

- Thread-list preview rendering of a forward. The `"Forwarded a message"` preview applies to the main-room room-list preview only (`roomLastMessage`); the thread list resolves its preview via a stored pointer, a separate follow-up. The forward marker itself works inside threads with no special handling.
- The preview/quoted filter system (separate ticket).
- Cross-room source resolution plumbing. A same-room source resolves fully; a source the sender's room can't fetch degrades to a placeholder (see below) rather than failing the send.

## Design

### Field shape — mirror `QuotedParentMessage`

Not a bare boolean — a nullable `ForwardedMessage` snapshot, so attribution/content can grow without a breaking change and the client gets the forwarded content to render. A dedicated struct (not reusing `QuotedParentMessage`) keeps the two concepts independent; it drops the quote's thread-context fields (`thread_parent_id`, `thread_parent_created_at`, `tshow`), which are meaningless for a forward.

- `pkg/model/cassandra.ForwardedMessage` — `message_id`, `room_id`, `sender`, `created_at`, `msg`, `mentions`, `attachments`, `message_link`. `cql:` tags mirror the quote UDT.
- `model.Message.Forwarded *cassandra.ForwardedMessage` (mirrors `QuotedParentMessage`).
- `SendMessageRequest.ForwardedFromMessageID string` (mirrors `QuotedParentMessageID`).

### Gatekeeper resolve + degraded mode

`message-gatekeeper.resolveForwardSnapshot` reuses the by-id parent fetch and projects the result into a `ForwardedMessage`, then sets it on the canonical message (mirrors `resolveQuoteSnapshot` + `msg.QuotedParentMessage = …`).

- Invalid `ForwardedFromMessageID` → `errcode.BadRequest` at the boundary (mirrors the quoted-id check).
- Unresolvable source → degrades to a placeholder snapshot (`"Content temporarily unavailable"`), the send still ships. Unlike a quote, a forward never hard-fails on the source lookup — the marker is what matters.

No re-projection plumbing (no `Forwarded`-unverified envelope flag): a degraded placeholder persists as-is. The client-facing `MessageEvent` carries `Forwarded` automatically (it embeds `Message`).

### Storage (additive)

- New UDT `chat."ForwardedMessage"`.
- `forwarded FROZEN<"ForwardedMessage">` on `messages_by_room` — the one table the room-list preview reads. `messages_by_id` / thread tables are not touched (the preview never reads them).
- `init/*.cql` for fresh clusters + a keyspace-scoped `migrations/*.cql` `ALTER` for prod (additive → non-breaking).
- `message-worker` binds the column on the `messages_by_room` INSERTs (plaintext + encrypted). At-rest: the forward body (`msg`, `attachments`) is stripped into `enc_payload` like the quote body (`pkg/atrest`), so encrypted rows don't leak the forwarded content plaintext.

### Read / preview

`history-service.roomLastMessage`: if the resolved message has empty content and a non-nil `Forwarded`, the preview text is `"Forwarded a message"`; otherwise the message's own content previews as normal. The `messages_by_room` projection reads the new column (appended to the room query, not the shared `baseColumns` — `messages_by_id` has no such column).

## Testing

- Unit (table-driven): gatekeeper resolves a forward → snapshot, invalid-id reject, unresolvable-source degrade; model round-trip; `roomLastMessage` renders the label for an empty-content forward and the source content for a non-empty one.
- Integration (real Cassandra): persist a forward + read the `forwarded` column back; projection round-trip.
- e2e (dev stack): a forward send's reply + the persisted message carry `forwarded`; empty-content forward previews as `"Forwarded a message"`.

🤖 Generated with Claude Code
