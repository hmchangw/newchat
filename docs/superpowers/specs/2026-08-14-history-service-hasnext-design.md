# history-service: `hasNext` in Load History response

**Date:** 2026-08-14
**Status:** Approved

## Problem

The Load History RPC (`chat.user.{account}.request.room.{roomID}.{siteID}.msg.history`,
handled by `HistoryService.LoadHistory`) returns a page of messages but gives the
frontend no signal whether older messages exist. The frontend needs a `hasNext`
field to drive lazy loading when the user scrolls up in a room — today it can only
guess (e.g. "page came back full") and must issue a wasted fetch to discover the
end of history.

## Decision

Expose the pagination signal the Cassandra layer already computes. Every backward
page read (`GetMessagesBefore` / `GetMessagesBetweenDesc`) returns
`cassrepo.Page[models.Message]` with a populated `HasNext`; `LoadHistory`
currently discards it. Plumb it through to the wire.

Scope is deliberately `hasNext` only. `nextCursor`/`cursor` (full cursor
pagination as in `msg.next`) is out of scope: the frontend paginates Load History
by `before` = oldest received `createdAt`, and that contract stays unchanged.
A cursor can be added later without breaking this change.

## Semantics

`hasNext=true` means the page walk did not reach its terminal boundary. The
boundary is whichever applies to the caller:

- the room's effective start (room `createdAt`, clamped by the configured
  history floor), for callers with full history access, or
- the caller's access window (`accessSince`), for windowed access — so a
  limited-history user sees `hasNext=false` at *their* boundary, not the room's
  true beginning.

The signal is conservative: `true` can rarely occur when the remaining buckets
happen to be empty (e.g. the page filled exactly at the last message and buckets
below are empty). The frontend's next fetch then returns an empty page with
`hasNext=false`. This is the identical contract already shipped by
`msg.next` (Load Next Messages), thread replies, and pinned messages.

## Changes

1. **Model** — `history-service/internal/models/message.go`:
   `LoadHistoryResponse` gains `HasNext bool` with tag `json:"hasNext"`
   (always present, no `omitempty` — matching `LoadNextMessagesResponse`).
   Request body unchanged.

2. **Handler** — `history-service/internal/service/messages.go`,
   `LoadHistory`: set `HasNext: page.HasNext` in the returned
   `LoadHistoryResponse`. Both read paths (open walk and access-windowed walk)
   already produce it.

3. **Docs** — client-facing RPC, so in the same PR:
   - `docs/client-api.md` § Load History: add the `hasNext` row to the success
     response table and to the JSON example, worded like the Load Next
     Messages section.
   - `docs/client-api/request-reply.md` § Load History: mirror the same change
     (derived view must not drift).
   - `docs/client-api/events.md`: untouched (no event change).

## Testing (TDD)

Extend the `LoadHistory` unit tests in
`history-service/internal/service/messages_test.go`, following the existing
`Test<Type>_<Method>_<Scenario>` per-scenario pattern, with mocked reader pages:

- reader returns `Page{HasNext: true}` → response `hasNext=true`;
- reader returns a terminal page (`HasNext: false`) → response `hasNext=false`;
- both on the open-walk path and the access-windowed path.

Red first (assert the new field before implementing), then the one-line
implementation. No `cassrepo` integration-test changes: the walker's `HasNext`
behavior is already covered there.

## Error handling

Unchanged. No new error paths; failures still surface via the existing
`errcode`/`errnats` boundary.

## Out of scope

- `nextCursor` in the Load History response / `cursor` in its request.
- Any frontend implementation.
- Changes to `LoadNextMessages`, surrounding-messages, threads, or pins.
