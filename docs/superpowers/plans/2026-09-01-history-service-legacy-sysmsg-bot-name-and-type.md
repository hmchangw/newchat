# Legacy System-Message Bot Names + Type Normalization — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Two follow-ups to PR #404 / #413, both on `history-service`'s read path:

1. **Bots.** The display-name substitution in legacy `members_removed` rows currently resolves through the `users` collection only. A bot account (`.bot` suffix) frequently has no user document at all, so those rows still render the raw account. Resolve a bot's name from the `apps` collection instead (`apps.assistant.name == account` → `apps.name`).
2. **Type normalization.** The migrated Cassandra rows carry legacy plural types the frontend does not know. Rewrite them on the wire: `members_removed` → `member_removed`, `members_left` → `member_left`. `members_left` rows keep their stored `msg` text unchanged.

**Architecture:** Both are in-place passes over the message slice at the same six `messages.go` return points that already run `redactUnavailableQuotes` / `setDecodedAttachments` / `resolveRemovedMemberNames`. Nothing is written back to Cassandra — this is read-time rendering, so a bot or user renamed today shows its current name on the very next history load. The bot lookup reuses `HistoryService.appName`, the `preview.CachedAppNameLookup` already built in `New` for reaction actors (bounded LRU, 5-minute TTL, singleflight), so a page of rows from one bot costs one Mongo read per TTL rather than one per row.

**Tech Stack:** Go 1.25, MongoDB (`go.mongodb.org/mongo-driver/v2`), `go.uber.org/mock` (mockgen), `stretchr/testify`.

**Spec:** No separate design doc — this is a bounded change agreed in chat. The design is restated in full under "Design Decisions" below; executors read this file alone.

## Global Constraints

- Go 1.25. Single `go.mod` at repo root.
- Never run raw `go` commands — always the root `Makefile` targets (`make test SERVICE=history-service`, `make lint`, `make sast`).
- TDD is mandatory: write the failing test, run it, confirm it FAILS for the right reason, then implement. Never write implementation before its test.
- Minimum 80% coverage; target 90%+ for this service-layer logic.
- All tests use `-race` (the Makefile handles it).
- Logging is `log/slog` with structured key-value fields only. Never log full message bodies.
- Unit tests never connect to a real database, NATS, or any external service.
- Commit after each task's tests pass. Pre-commit hook runs lint + tests; fix failures before retrying.
- Branch: `claude/system-message-member-removal-w1w6xk`. Never push elsewhere.
- No new third-party dependencies. Everything this plan needs already exists in the repo.

---

## Design Decisions (agreed, do not re-litigate)

These were settled during brainstorming. They are requirements, not suggestions.

1. **Bot accounts join the SAME batch, they do not get a second query.** `resolveRemovedMemberNames` already issues exactly one `FindUsersByAccounts` per response. Bot accounts stay in that batch — a bot *may* have a user document, and including it costs nothing because the query is issued either way. What changes is only how a resolved account becomes a name:
   - `model.IsBot(account)` → `s.botAwareDisplayName(ctx, engName, chineseName, account)`, which prefers the app name and degrades to the composed name.
   - otherwise → `displayfmt.CombineWithFallback(engName, chineseName, account)`, exactly as today.

   This is byte-for-byte the semantics room-worker adopted for the live path in PR #446 (`room-worker/handler.go:memberDisplayName`), so a legacy row and a modern one render a bot identically.

2. **A `users` failure must no longer abandon the bot half.** Today a `FindUsersByAccounts` error returns early, leaving every row raw. After this change it logs at WARN and continues with an empty user map, so a bot account can still resolve through `apps`. A non-bot account still ends up untouched, so the existing degradation guarantee is unchanged. Same for a nil `s.users`: skip the batch, do not skip the pass.

3. **The app lookup is the one already wired.** `s.appName` is built ONCE in `New` as `preview.CachedAppNameLookup(apps.AppNameByAccount)` (`service.go:254`). Do NOT construct a new `CachedAppNameLookup` — a per-call wrapper mints a fresh empty cache and never hits (#366). `s.appName` is nil when no app store was wired; `preview.BotAwareDisplayName` already skips a nil lookup, so no nil check is needed at the call site.

4. **An unresolvable name never changes the row.** A bot with no `apps` row, an `apps` read error, an account absent from `users`, a user document with no names — every one of these leaves the sentence reading exactly as it does today. Errors are logged, never returned. A display name is not worth failing a history load over.

5. **Type normalization is unconditional and independent of the text rewrite.** A `members_removed` row whose `msg` does NOT match the suffix (so its text is left alone) still gets its `Type` rewritten. The two passes share no gate.

6. **Ordering: text rewrite first, type rewrite second.** `extractRemovedAccount` gates on `m.Type == "members_removed"`. If the type were normalized first, the text pass would no longer recognize the row and every legacy sentence would keep its raw account. Both passes live behind one entry point so this ordering cannot drift apart at a call site.

7. **`members_left` keeps its stored text.** Only its `Type` is rewritten, to `member_left` (NOT `member_removed`). No suffix matching, no name resolution, no new suffix constant.

8. **Wire-only.** Nothing is written back to Cassandra and no migration job is introduced. The rows on disk keep their legacy types forever; every reader that needs the modern form normalizes at its own boundary.

9. **Scope: `messages.go` only.** `pin.go` and `threads.go` stay out, unchanged from the #404 decision — a legacy member row can never be pinned or become a thread parent, and `pin.go:250` blanks `Type` on oversize placeholders anyway.

10. **Ordering relative to `fitPage` / `fitWindow` is unchanged.** Both passes stay where `resolveRemovedMemberNames` sits today: after `setDecodedAttachments`, before the budget trim. Type normalization only ever shortens the encoded row (`members_removed` 15 → `member_removed` 14 chars; `members_left` 12 → `member_left` 11), so trimming after it stays conservative.

### Explicit non-goals

- **Do NOT add `members_removed` / `members_left` to `pkg/model.systemMessageTypes`.** The #404 plan ruled this out and it stays ruled out. That map drives `IsSystemMessageType`, which gates preview eligibility (`preview.Eligible`), unread/ordering (`NewMessageEvent.SystemMsg`) and search indexing across many services; changing it would alter behavior for legacy rows repo-wide, far past what was asked.
- **Known pre-existing gap, out of scope:** because of the above, a legacy `members_removed` row is not recognized as a system message by `preview.Eligible`, so it can still be selected as a room-list preview. Normalizing at history-service's read boundary does not fix that — the preview is sealed by `broadcast-worker` / `roomlist-worker` from the stored type. Worth a follow-up ticket; do not fix it here.
- No change to `room-worker`, `broadcast-worker`, `search-service`, or any live-path producer. New rows are already correct via PR #446.

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `history-service/internal/service/sysmsgname.go` | Modify | Add the legacy-type map + `normalizeLegacySysMsgTypes`; make name resolution bot-aware; add the combined `normalizeLegacySysMsgs` / `normalizeLegacySysMsg` entry points. |
| `history-service/internal/service/sysmsgname_test.go` | Modify | New table-driven cases for both passes; existing cases must stay green untouched. |
| `history-service/internal/service/messages.go` | Modify (6 call sites) | Swap `resolveRemovedMemberNames`/`Name` for the new entry points. No other change. |
| `history-service/internal/service/messages_test.go` | Modify (`:3122`) | Extend the end-to-end assertion to cover the normalized `Type`. |
| `docs/client-api.md` | Modify (`:2936` + the `type` row) | Document bot-name substitution and legacy-type normalization. |
| `docs/client-api/request-reply.md` | Check + Modify if the Message schema is mirrored there | Derived view must not drift. |

No new files. No interface changes, so **no `make generate` run is needed** — `UserStore` and `AppStore` are both already declared and mocked.

---

## Task 1 — Legacy type normalization

The pure half, done first because it has no dependencies.

- [ ] **RED.** In `sysmsgname_test.go`, add `TestNormalizeLegacySysMsgTypes` as a table-driven test over a `[]models.Message` slice, asserting `Type` after the call and that `Msg` is never touched:

  | case | in `Type` | in `Msg` | want `Type` | want `Msg` |
  |---|---|---|---|---|
  | legacy removed | `members_removed` | `"bob" has been removed from the channel.` | `member_removed` | unchanged |
  | legacy removed, unmatched text | `members_removed` | `something else` | `member_removed` | unchanged |
  | legacy left | `members_left` | `whatever the migration wrote` | `member_left` | unchanged |
  | modern removed | `member_removed` | any | `member_removed` | unchanged |
  | modern left | `member_left` | any | `member_left` | unchanged |
  | other system type | `members_added` | any | `members_added` | unchanged |
  | ordinary message | `""` | any | `""` | unchanged |

  Add a separate assertion that nil and empty slices are no-ops (`require.NotPanics`).

- [ ] Run `make test SERVICE=history-service` and **confirm it fails to compile** (the function does not exist). That is the Red phase.

- [ ] **GREEN.** In `sysmsgname.go`:
  - add `legacyMembersLeftType = "members_left"` beside the existing `legacyMembersRemovedType`, with a comment noting these are migrated Cassandra types that exist nowhere else in the repo;
  - add a package-level `legacySysMsgTypes` map from each legacy type to its `pkg/model` constant (`model.MessageTypeMemberRemoved`, `model.MessageTypeMemberLeft`) — use the constants, never string literals, so a rename in `pkg/model` reaches here;
  - add `func normalizeLegacySysMsgTypes(msgs []models.Message)` — a plain function, no receiver, since it touches no dependency. Loop by index (`for i := range msgs`) so the rewrite lands on the slice, not a copy.

- [ ] Run `make test SERVICE=history-service`. Confirm green.
- [ ] `make lint`. Commit: `feat(history-service): normalize legacy members_removed/members_left types on read`

---

## Task 2 — Bot-aware name resolution

- [ ] **RED.** In `sysmsgname_test.go`:
  - Replace `newSysMsgNameService(t, users)` with a form that takes both stores — e.g. `newSysMsgNameServiceWith(t, users, apps)` — keeping the current one-arg helper as a thin wrapper over `mocks.NewMockAppStore(ctrl)` so **every existing test stays byte-identical**. This matters: those tests are the regression net for decisions 1, 2 and 4.
  - Add `legacyRemovedBot(account)` beside `legacyRemoved`, or just reuse `legacyRemoved("helper.bot")` — the sentence shape is the same.
  - New cases:

  | test | setup | expectation |
  |---|---|---|
  | `_BotResolvesAppName` | `FindUsersByAccounts(["helper.bot"])` → `nil`; `AppNameByAccount("helper.bot")` → `("Helper Bot", nil)` | `"Helper Bot" has been removed from the channel.` |
  | `_BotWithUserDocPrefersAppName` | users returns `{Account:"helper.bot", EngName:"Helper"}`; apps returns `"Helper Bot"` | app name wins → `"Helper Bot"` |
  | `_BotWithNoAppRowFallsBackToUserDoc` | users returns `{Account:"helper.bot", EngName:"Helper"}`; apps returns `("", nil)` | `"Helper"` |
  | `_BotWithNoAppRowAndNoUserDocKeepsRawAccount` | users returns `nil`; apps returns `("", nil)` | `"helper.bot"`, unchanged |
  | `_BotAppLookupErrorKeepsRawAccount` | users returns `nil`; apps returns `("", errors.New("mongo unavailable"))` | `"helper.bot"`, unchanged |
  | `_UserStoreErrorStillResolvesBots` | `FindUsersByAccounts` → error; apps returns `"Helper Bot"` | bot row resolves; a non-bot row in the same page stays raw |
  | `_MixedPageOneBatchOnePerBot` | page of `bob`, `helper.bot`, `bob`, `helper.bot`, one ordinary message | `FindUsersByAccounts` called `Times(1)` with both accounts; `AppNameByAccount` called `Times(1)` |
  | `_NilUserStoreStillResolvesBots` | `newSysMsgNameServiceWith(t, nil, apps)` | bot row resolves, no panic |

  The `Times(1)` assertions are the point of the whole pass — they are what stops a page of 50 bot rows from becoming 50 Mongo reads. Do not weaken them to `AnyTimes()`.

- [ ] Run `make test SERVICE=history-service` and confirm the new cases FAIL (bot rows come back raw) while every pre-existing case still passes.

- [ ] **GREEN.** In `sysmsgname.go`, rework `resolveRemovedMemberNames`:
  - Keep the collect-and-dedupe loop exactly as it is. Bots are NOT filtered out of `accounts` (decision 1).
  - Change the guard from `if len(msgs) == 0 || s.users == nil { return }` to `if len(msgs) == 0 { return }`, and guard the batch itself with `if s.users != nil` (decision 2).
  - On a `FindUsersByAccounts` error: keep the existing WARN log, but fall through with a nil/empty user map instead of returning (decision 2). Keep the log message and its `accounts` / `error` fields.
  - Build a `map[string]*model.User` (or keep the existing `map[string]string` and add a parallel one) — the rewrite loop needs the user's raw `EngName`/`ChineseName`, not a pre-composed string, because the bot branch feeds them to `BotAwareDisplayName`.
  - Add a small helper that turns `(account, *model.User)` into the rendered name:
    - `model.IsBot(account)` → `s.botAwareDisplayName(ctx, engName, chineseName, account)` (the existing method on `reactions.go:169`; do not inline `preview.BotAwareDisplayName`);
    - `u == nil` → return `""`, meaning "leave this row alone";
    - otherwise → `displayfmt.CombineWithFallback(u.EngName, u.ChineseName, account)`.
  - In the rewrite loop, skip when the rendered name is empty. Rewriting to a name equal to the account is harmless (identity), so no extra comparison is needed.
  - The `strings` / `displayfmt` imports stay; add `pkg/model` if `sysmsgname.go` does not already import it.

- [ ] Run `make test SERVICE=history-service`. Confirm green, including every pre-existing case.
- [ ] `make lint`. Commit: `feat(history-service): resolve bot app names in legacy members_removed rows`

---

## Task 3 — Combined entry point + call-site wiring

- [ ] **RED.** In `sysmsgname_test.go`, add `TestNormalizeLegacySysMsgs_TextThenType`: a page holding one `members_removed` row for a user, one for a bot, and one `members_left` row. Assert that after ONE call to `normalizeLegacySysMsgs` all three carry their modern `Type` **and** the two removed rows carry resolved names, while the `members_left` row's `Msg` is untouched. This test is what pins decision 6 — if an implementer reorders the two passes, the names stop resolving and this test goes red.
- [ ] Add `TestNormalizeLegacySysMsg_SingleMessage` mirroring the existing single-message wrapper test, plus its nil no-op case.
- [ ] In `messages_test.go:3122`, extend `TestHistoryService_LoadHistory_ResolvesRemovedMemberNames` with `assert.Equal(t, "member_removed", resp.Messages[0].Type)`. Confirm this fails today.

- [ ] **GREEN.** In `sysmsgname.go`:
  - Add `func (s *HistoryService) normalizeLegacySysMsgs(ctx context.Context, msgs []models.Message)` calling `s.resolveRemovedMemberNames` then `normalizeLegacySysMsgTypes`, with a comment stating why the order is load-bearing (decision 6).
  - Add the single-message form `normalizeLegacySysMsg(ctx, *models.Message)`, built the same way `resolveRemovedMemberName` is today (wrap in a one-element slice, write back). Delete `resolveRemovedMemberName` if nothing else calls it, or keep `resolveRemovedMemberNames` unexported-but-direct-tested and route only the call sites through the new pair.

- [ ] In `messages.go`, replace all six calls:
  - `:93` `LoadHistory` — `resolveRemovedMemberNames` → `normalizeLegacySysMsgs`
  - `:164` — same
  - `:231` `LoadSurroundingMessages` single-row early return — `resolveRemovedMemberName` → `normalizeLegacySysMsg`
  - `:376` — `resolveRemovedMemberNames` → `normalizeLegacySysMsgs`
  - `:442` `GetMessageByID` — `resolveRemovedMemberName` → `normalizeLegacySysMsg`
  - `:487` `GetMessagesByIDs` — `resolveRemovedMemberNames` → `normalizeLegacySysMsgs`

  Position at each site is unchanged (decision 10). Verify with `grep -rn "resolveRemovedMemberName" history-service/` that no production call site is left behind.

- [ ] Run `make test SERVICE=history-service`. Confirm green.
- [ ] `make lint`. Commit: `feat(history-service): normalize legacy sys-msg type and name at every read path`

---

## Task 4 — Client API documentation

Required by CLAUDE.md §5: these are client-facing `chat.user.` handlers, so `docs/client-api.md` moves in the same PR, and any derived view with it.

- [ ] In `docs/client-api.md`, update the Message schema `msg` row (`:2936`) so it covers bots: history-service substitutes the removed member's display name for the quoted account, resolving a `.bot` account through the `apps` collection (`assistant.name` → `name`) and any other account through `users`; an account that resolves to neither is returned unchanged.
- [ ] In the same table, extend the `type` row (or add one if the schema has none) noting that history-service returns modern types for migrated rows: stored `members_removed` is returned as `member_removed` and stored `members_left` as `member_left`. `members_left` keeps its stored `msg` text.
- [ ] `grep -n "members_removed" docs/client-api/request-reply.md docs/client-api/events.md` — if the Message schema is mirrored in either derived view, apply the same edit there. If neither mentions it, no change is needed; record that in the commit body so a reviewer does not go looking.
- [ ] Keep the edits terse — field-table style, no prose essays (CLAUDE.md §5).
- [ ] Commit: `docs(client-api): legacy sys-msg bot names and type normalization`

---

## Task 5 — Verification before completion

Do not claim done until every one of these has been run and its output read.

- [ ] `make test SERVICE=history-service` — green, with `-race`.
- [ ] Coverage on the touched package: confirm `sysmsgname.go` is at or above the 80% floor and the new branches (bot hit, bot miss, bot error, users error) are each covered. `go tool cover -func` via the Makefile's coverage path.
- [ ] `make lint` — clean.
- [ ] `make sast` — clean at medium+. This change adds no new I/O and no user-controlled formatting, so a finding here is a signal something unintended crept in.
- [ ] `make test` (full unit suite) — nothing outside history-service should move, but `pkg/model` constants are now referenced from a new place, so confirm nothing else broke.
- [ ] `grep -rn "resolveRemovedMemberName" history-service/` — only the intended definitions and their direct unit tests remain.
- [ ] Re-read the final diff adversarially before pushing: is the pass ordering still text-then-type? Is `s.appName` still built once in `New` and never re-wrapped? Does any path return early and skip the bot half?
- [ ] Push to `claude/system-message-member-removal-w1w6xk` with `git push -u origin claude/system-message-member-removal-w1w6xk`. Do NOT open a PR unless asked.

---

## Risk Notes

| Risk | Mitigation |
|---|---|
| Reordering the two passes silently disables name resolution | Task 3's `TextThenType` test fails loudly; decision 6 documents why. |
| A page of many bot rows becomes many Mongo reads | Accounts are deduped before lookup, and `s.appName` is the shared cached lookup. `Times(1)` assertions in Task 2 lock it in. |
| A `users` outage now reaches the `apps` collection too | Same Mongo client, so it degrades identically; both sides fail open and the row keeps its raw text. |
| `members_left` text turns out to have a resolvable account after all | Out of scope by explicit instruction — the row's `msg` is preserved verbatim. Revisit as a separate change if the FE asks. |
| Legacy rows still eligible as room previews | Pre-existing, documented as a non-goal above, needs a fix in the preview writers rather than here. |
