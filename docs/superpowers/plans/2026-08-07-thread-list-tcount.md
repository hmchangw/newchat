# Plan: surface `tcount` on ThreadListItem (thread.list RPC)

**Spec:** `docs/superpowers/specs/2026-08-07-thread-list-reply-count-design.md` — read it for the full rationale; this plan is self-contained for execution.

**Goal:** the cross-site thread inbox (`chat.user.{account}.request.user.{siteID}.thread.list`) returns each thread's reply count as an explicit, always-present item field `tcount`, lifted from the parent message that `buildThreadItems` already hydrates. No new reads, no new writes, no schema change.

## Global Constraints

- TDD Red-Green-Refactor per task: write the tests first, run them, confirm they FAIL for the right reason, then implement, then confirm green. Commit only after green.
- Run everything through `make` targets — never raw `go` commands: `make test SERVICE=<name>`, `make lint`, `make fmt`.
- The new field is `TCount int` with tags `json:"tcount" bson:"tcount"` — plain `int`, NO `omitempty`, NO pointer. A zero must serialize. Exact name `tcount` (not `replyCount`).
- The cap is `pkg/threadcount.Cap` (= 99) and passes through unchanged — no re-capping, no arithmetic anywhere in this change.
- `ParentMessage`/`LastMessage` stay opaque (`json.RawMessage`) — nothing in this change may decode them.
- No store interface changes → `make generate` not needed; never edit generated mocks.
- user-service production code must NOT change (verify by diff) — the aggregator copies items verbatim and that is the point of Task 3's tests.
- Match surrounding comment style and density; wrap errors per CLAUDE.md; test files stay in their existing packages (`package model_test`, `package service_test`, `package service` respectively — follow whatever the file being edited already declares).

## Task 1: `pkg/model` — add `TCount` to `ThreadListItem`

Files: `pkg/model/threadlist.go`, `pkg/model/threadlist_test.go`.

**Red:** in `pkg/model/threadlist_test.go` (existing file, `package model_test`):
1. Extend the existing `TestThreadListItemJSON` round-trip source struct with `TCount: 7` (the file's `roundTrip` helper does the rest).
2. New test `TestThreadListItemJSON_ZeroTCountSerialized`: marshal a `ThreadListItem` with `TCount: 0`, unmarshal into `map[string]any`, assert the `"tcount"` key IS present and equals `float64(0)`. Mirror the style of the existing `TestThreadListItemJSON_OmitsNilHRInfo` test in the same file (inverted assertion).

Run `make test SERVICE=pkg/model` (if the Makefile target doesn't take pkg paths, use the closest make target that runs `pkg/model` tests — check `Makefile` first; do not fall back to raw `go test`). Confirm compile failure / test failure.

**Green:** in `pkg/model/threadlist.go`, inside `ThreadListItem`, directly after the `LastMsgAt` field, add:

```go
// TCount is the thread's non-deleted reply count, capped at
// pkg/threadcount.Cap — 99 means "99 or more". Lifted from the hydrated
// parent's tcount; 0 when that column was never written (migrated threads).
// Always present on the wire so clients never branch on undefined.
TCount int `json:"tcount" bson:"tcount"`
```

And update the `LastMsgAt` doc comment: replace the sentence `Reply count rides on ParentMessage.TCount.` with `Reply count is surfaced as TCount below.`

Run the same make target; all green. `make lint`. Commit: `feat(model): surface thread reply count as ThreadListItem.tcount`.

## Task 2: history-service — populate `TCount` in `buildThreadItems`

Files: `history-service/internal/service/threads.go`, `history-service/internal/service/threadlist_test.go`.

**Red:** in `history-service/internal/service/threadlist_test.go` (existing file, `package service_test` — reuse its `newThreadListService`, `decodeThreadMsg`, `intPtr`, `ptrTime`, `testContext` helpers):
1. In the existing `TestHistoryService_ListThreadSubscriptions_Success`, after the current `assert.Equal(t, 4, *parent.TCount)` line, assert `assert.Equal(t, 4, first.ParentMessage != nil ... )` — concretely: `assert.Equal(t, 4, resp.Items[0].TCount)` and `assert.Equal(t, 0, resp.Items[1].TCount)` (row 2's parent `p2` has nil TCount).
2. New table-driven test `TestHistoryService_ListThreadSubscriptions_TCountFromParent` with cases over the parent's Cassandra `TCount` → item `TCount`:
   - `intPtr(4)` → `4` ("count passes through")
   - `nil` → `0` ("never written — migrated thread")
   - `intPtr(0)` → `0` ("all replies deleted")
   - `intPtr(99)` → `99` ("cap passes through unchanged")
   Each case: one `ThreadSubRow` (parent `p1`, last `m1`), mock `ListUserThreadSubscriptions` returns the row, mock `GetMessagesByIDs` returns parent with the case's TCount plus last message, assert `resp.Items[0].TCount`. Use `t.Run` with the descriptive names above.

`make test SERVICE=history-service` — confirm the new assertions FAIL (field exists after Task 1 but is never populated, so it is 0 where 4/99 expected).

**Green:** in `history-service/internal/service/threads.go`, in `buildThreadItems`, extend the `item := pkgmodel.ThreadListItem{...}` literal with nothing — instead, directly after the literal (before the `row.LastSeenAt` block), add:

```go
// Lift the reply count off the already-hydrated parent; nil = column never
// written (e.g. migrated threads) and stays 0.
if parent.TCount != nil {
    item.TCount = *parent.TCount
}
```

`make test SERVICE=history-service` green. `make lint`. Commit: `feat(history-service): populate ThreadListItem.tcount from the hydrated parent`.

## Task 3: user-service — aggregator pass-through tests (test-only)

Files: `user-service/service/threads_test.go` ONLY. Production code must not change; the aggregator already copies items verbatim — these tests pin that.

**Red-that-passes caveat:** these tests assert existing pass-through behavior plus the Task 1 field, so they go green immediately once written correctly. That is acceptable here (regression pin, not new behavior) — but first deliberately verify they CAN fail: run them once with a wrong expected value (e.g. expect `TCount: 999`), see the failure, restore the right value. State in the report that this mutation check was done and what failed.

Reuse the file's existing helpers: `newThreadSvc`, `expectThreadList`, `ctx`, `ids`, and the `item(...)` constructor (extend rows inline with struct literals where the helper is too narrow).

New test `TestUserService_ListUserThreads_TCountSurvivesAggregation`:
- site-a returns two rows, site-b returns one, `lastMsgAt` interleaved so the global `(lastMsgAt, threadRoomId) DESC` sort reorders across sites:
  - Row A (site-a): `RoomType: model.RoomTypeChannel`, `ThreadRoomID: "ta1"`, `LastMsgAt: 50`, `TCount: 4`
  - Row B (site-b): `RoomType: model.RoomTypeDM`, `RoomName: "bob"`, `ThreadRoomID: "tb1"`, `LastMsgAt: 40`, `TCount: 99`
  - Row C (site-a): `RoomType: model.RoomTypeBotDM`, `RoomName: "helper-bot"`, `ThreadRoomID: "ta2"`, `LastMsgAt: 30`, `TCount: 1`
- Stub the enrichment lookups the dm/botDM rows trigger: `users.EXPECT().GetHRInfoByAccounts(gomock.Any(), []string{"bob"})` returning a non-nil `map[string]*model.SubscriptionHRInfo{"bob": {...}}`, and `apps.EXPECT().GetAppsByAssistants(gomock.Any(), []string{"helper-bot"})` returning `map[string]*model.App{"helper-bot": {Name: "Helper"}}` (match the expectation style of the existing `TestUserService_ListUserThreads_DM_CarriesHRInfo` and `..._BotDM_ReplacesRoomNameWithAppName` tests).
- Assert on post-sort `resp.Items` order `[ta1, tb1, ta2]`, and per row:
  - Items[0]: `TCount == 4` (channel — untouched by enrichment)
  - Items[1]: `TCount == 99` AND `HRInfo != nil` (dm — enriched AND count intact)
  - Items[2]: `TCount == 1` AND `RoomName == "Helper"` (botDM — rewritten AND count intact)

New test `TestUserService_ListUserThreads_TCountSurvivesDegradedEnrichment`:
- One dm row (`TCount: 3`) and one botDM row (`TCount: 5`); HR lookup mock returns `(nil, errors.New("hr down"))`, app lookup mock returns `(nil, errors.New("apps down"))` — mirror the existing `..._HRInfoDegrades` / `..._AppLookupDegrades` tests.
- Assert both rows keep their `TCount` values and the request still succeeds.

`make test SERVICE=user-service` green (after the mutation check). `make lint`. Verify `git diff --stat` for this task touches ONLY `user-service/service/threads_test.go`. Commit: `test(user-service): pin tcount pass-through across thread-list aggregation`.

## Task 4: docs — client-api + design-doc supersede

Files: `docs/client-api.md`, `docs/design/user-thread-list.md`, `docs/superpowers/specs/2026-08-07-thread-list-reply-count-design.md`.

1. `docs/client-api.md`, **ThreadListItem** table (§ List User Threads, around line 5304): add a row directly after `lastMsgAt`:

   ```markdown
   | `tcount` | number | Non-deleted reply count, capped at 99 — `99` means "99 or more". Always present; `0` also covers threads whose count was never written (e.g. migrated threads). |
   ```

2. Same section: change the `parentMessage` row note from

   ```markdown
   Optional. The hydrated parent message; reply count rides on its `tcount`.
   ```

   to

   ```markdown
   Optional. The hydrated parent message.
   ```

3. Same section, JSON example: add `"tcount": 3` to the first item (it matches the embedded `parentMessage.tcount: 3` — place it after `"lastMsgAt"`), and `"tcount": 1` to the second item (after `"lastMsgAt"`).
4. Verify the derived views need no edit: `docs/client-api/request-reply.md` links to the canonical ThreadListItem schema without restating fields (check around its line 1813) and `docs/client-api/events.md` has no thread.list entry. State the verification result in the report; edit only if a restated field table is actually found.
5. `docs/design/user-thread-list.md` (around lines 328-329, "Reply count is **not** a separate field..."): replace that bullet with:

   ```markdown
   - Reply count: originally not a separate field (clients read `parentMessage.TCount`); superseded 2026-08-07 — `tcount` is now lifted onto the item itself (see `docs/superpowers/specs/2026-08-07-thread-list-reply-count-design.md`). The parent's embedded `tcount` remains for backward compatibility.
   ```

   Also update the §5 enrichment bullet around line 251, which reads

   ```markdown
   reply count rides on `parentMessage.TCount` (no separate item field)
   ```

   with a matching parenthetical: `(since 2026-08-07, also lifted to the item's tcount)`.
6. Spec status: in `docs/superpowers/specs/2026-08-07-thread-list-reply-count-design.md` change `**Status:** Sketch — for review, not implemented` to `**Status:** Approved — implemented` and resolve open question 2 in place by appending **Resolved: plain `int`.** to that question.

No tests. `make lint` still runs (markdown untouched by it, but keeps the gate honest). Commit: `docs(client-api): document ThreadListItem.tcount`.
