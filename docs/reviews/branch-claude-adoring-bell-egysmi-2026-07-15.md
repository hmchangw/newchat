# Branch review — claude/adoring-bell-egysmi

- **Date:** 2026-07-15
- **Base:** `main` (a6fa309)
- **Head:** e6fa16d — feat: room last-message preview on `message_deleted` + `rooms.lastMsg` upkeep (encryption-ready)
- **Diff size:** 36 files, +2,881 / −259
- **Services touched (2):** broadcast-worker, history-service (plus `pkg/model`, `pkg/subject`, docs)
- **Reviewers:** 2 per-service generalists + 5 global lenses (Go expert, test-automation, bug & security, performance, observability)

## Finding counts (as reported per reviewer; cross-reviewer overlaps noted below)

| Severity | Count |
|----------|-------|
| critical | 0 |
| high | 7 |
| medium | 18 |
| low | 15 |
| nitpick | 13 |

The 7 highs collapse to **6 distinct issues** after dedup: the legacy-room edit-guard defect and the delete-path availability coupling were each independently flagged by three reviewers at different severities; the lane-duplication issue was rated high by both service generalists.

## Top-line risk assessment

No criticals. Test discipline, errcode tiering, docs sync (client-api.md + both derived views), and project-pattern adherence are consistently strong — several reviewers called the test suite exemplary. Hold merge for:

1. **Coalescer flush race (high, bug & security):** the unguarded 250ms bulk flush can *resurrect a deleted message's full content* into `rooms.lastMsg` (indefinitely in a quiet room) or persist a pre-edit body after clients saw the edit. Fix is local: purge/patch the pending buffer in the guarded-store overrides.
2. **System-message previews (high, broadcast-worker):** `handleCreated` has no `msg.Type` gate, so every membership event overwrites `rooms.lastMsg` with a system preview — violating the field's own documented invariant.
3. **Convergence with main's parallel `rooms.get` lane (high, both generalists):** two walkers, two wire shapes, two skip rules, two trim rules — the same room preview flip-flops depending on which path refreshed it. Needs a team decision; recommended target: this branch's skip semantics + `LastMessagePreview` shape, reimplementing `roomLastMessage` atop `GetLastRoomMessage`.
4. **Two cheap ops bounds (high, performance):** add a total-row cap to the bucket walk (`rooms.get` precedent: 250 rows) and gate the delete-path RPC on `room.LastMsgID == msg.ID` to shrink head-of-line blocking on the shared MaxWorkers semaphore.
5. **Security-model sign-off (medium, bug & security):** plaintext preview content newly at rest in Mongo for non-encrypted rooms; survivor previews bypass per-member history windows.

SAST: gosec **pass** (no medium+); govulncheck and semgrep environment-blocked in this sandbox (proxy 403) — must run in CI.

---

# Service: broadcast-worker

## (a) Diff correctness
- **high** — Create path stores system-message previews. `handleCreated` has no `msg.Type` gate before `buildLastMessagePreview` (broadcast-worker/handler.go:188-199), and room-worker publishes `members_added`/`room_created` system messages onto MESSAGES_CANONICAL (room-worker/handler.go:1132, 1682, 1737). Every membership change overwrites `rooms.lastMsg` with a system preview, violating the field's own invariant ("non-system", pkg/model/lastmsg.go:8-12) and diverging from the delete-path survivor, which skips system types (history-service/internal/cassrepo/last_message.go:24-33). Wire contract on `message_deleted` stays correct; the stored doc self-contradicts and will surface as a bug when a read path consumes `rooms.lastMsg`.
- **medium** — Edit/coalescer race, unacknowledged. `SetRoomLastMessageEdited` bypasses the ~250ms buffer (coalescer.go:65-79); if the create is still buffered, the guard misses, and the later **unguarded** flush (`$set` on `_id` only, store_mongo.go:113-127) persists the pre-edit preview with no `editedAt`. The documented delete race (store_mongo.go:140-142) is also understated: the flush actively writes the *deleted* message's preview back, not merely "leaves the old pointer".
- **medium** — Legacy rooms (lastMsgId set pre-feature, no `lastMsg` subdoc): `SetRoomLastMessageEdited` (store_mongo.go:173-186) creates a partial `lastMsg {msg, editedAt}` missing required `messageId`/`senderAccount`/`createdAt`.
- **low** — `handleDeleted` pays a history RPC even for non-latest deletes (handler.go:543); deliberate per comment. Ordering is safe: history-service soft-deletes in Cassandra before publishing canonical (history-service/internal/service/messages.go:497-516).

## (b) Scope drift
Incremental extension of an owned responsibility (worker already maintained `lastMsgId/lastMsgAt`); no split needed. Note (**low**): `rooms.lastMsg` is write-only in this branch — no reader exists — so this is groundwork carrying real upkeep cost.

## (c) Abstractions
`LastMessageFetcher` mirrors `ParentFetcher` exactly (interface + mockgen + fetcher file, lastmsg_fetcher.go, store.go:16) — justified. `encryptPreviewContent` is a clean generalization of `encryptEditedContent` (one ciphertext, two destinations); `roomEncrypted` (handler.go:88-90) deduplicates a repeated condition; guarded store methods are warranted by the concurrent consumer. **low**: `previewSenderName` (handler.go:1013-1021, EngName→UserDisplayName→Account) and history's cascade (last_message.go:57-63, EngName→AppName→Account) can yield different `senderName` for the same message on create vs delete paths.

## (d) Design coherence / duplication with main's parallel feature
- **high** — Three coexisting "room last message" mechanisms with divergent semantics: main's read-time `rooms.get` batch (model.LastMessage, history-service/internal/service/rooms.go:70-115) skips *only* deleted rows and trims to 256 runes (rooms.go:18); this branch adds a second walker (cassrepo/last_message.go) that *also* skips system types and returns **untrimmed** content, plus a second wire shape (`LastMessagePreview`) and a write-time denormalization. The same room-list preview can differ depending on which path refreshed it. Should converge on one shape and one skip rule (extend `rooms.get` instead of a parallel single-room RPC).
- **medium** — Full untrimmed bodies (DMs in plaintext) denormalized into the hot `rooms` collection (handler.go:1027-1037), which `GetRoom` reads unprojected (store_mongo.go:52-59); `rooms.get` deliberately trims.

## (e) Project-pattern adherence
Compliant: subject via `pkg/subject.MsgRoomLast` + test; no raw sprintf; no stream/consumer changes; same-site request/reply (mirrors `RoomsInfoBatch`) so outbox rule not implicated; sonic pretouch updated (pretouch.go:19-20); `errcode.Parse` matches the existing parent_fetcher.go:60 precedent; `DeleteRoomEvent.Timestamp` still set at publish site. No violations found.

## (f) Client-API doc rule
Satisfied. `DeleteRoomEvent`/`Room` (server→client structs) changed and `docs/client-api.md`, `docs/client-api/events.md`, `docs/client-api/request-reply.md` are all updated in-diff (LastMessagePreview schema, `lastMessage` on `message_deleted`, encrypted example). **nitpick**: docs promise "system messages never appear as previews" — currently false for the stored `rooms.lastMsg` (see (a) high).

---

# Service: history-service

## (a) Diff correctness
- `SoftDeleteMessage`'s 5-tuple widening is mechanically consistent across all error returns (`history-service/internal/cassrepo/write.go:288-368`), the interface (`history-service/internal/service/service.go:45`), `migration.go:89`, and regenerated mocks. The tlm write already existed (`setParentTcountAndTlm`); only the return plumb is new. **low**: on count-set failure (`write.go:366`) the event carries `NewTCount=nil, NewThreadLastMsgAt=nil` with applied=true — follows pre-existing tcount semantics; fine.
- Repo walk correct: strict `created_at < before` only on the first bucket mirrors `GetMessagesBefore` (`cassrepo/last_message.go:51-56` vs `messages_by_room.go:84-95`); handler's `+1ms` cap (`service/last_message.go:41`) matches LoadHistory. Filtering on plaintext deleted/type columns so only the winner pays decrypt (`last_message.go:89-99`) improves on `scanMessagesUpTo`, which decrypts every row. Builds clean.
- **medium** — `scanFirstQualifying` has no row-scan cap — gocql transparently drains an entire bucket's deleted tail, while the parallel `rooms.go` walk deliberately caps at 250 rows. Worst case per delete-fanout: unbounded scan of a 72h bucket.
- **nitpick** — comment "within [floor, before)" (`service.go:29-31`) overstates precision — floor bounds buckets, not rows (same as sibling readers).

## (b) Scope drift
Tight; every touched file serves the feature, `pkg/model/model_test.go` roundtrips added per the Model Tests rule, no drive-by refactors. **nitpick**: `service/last_message.go:35` double-wraps ("resolving room times for %s" around resolveRoomTimes' own "resolve room times for %s").

## (c) Abstraction — the two walks
The repo-level walk is justified as a primitive (single decrypt, repo-side filter, no cursor machinery), but two last-message resolvers now diverge:
- **high** (coherence): `roomLastMessage` (`service/rooms.go:71`) skips only `Deleted`; `GetLastRoomMessage` also skips 8 system types (`cassrepo/last_message.go:24`). Delete the newest user message above a system message and `message_deleted.lastMessage` shows an older user message (or nothing) while a rooms.get refresh shows "alice added bob" — the same room-list preview flip-flops by refresh path. The skip-set (matching the new client docs) is the right semantic; rooms.get should adopt it.
- **medium**: `rooms.go:119` trims to 256 runes; the preview ships full content — up to 20KB fanned out per member per visible delete. Trim at the service edge.
- **medium**: three shapes coexist (`model.LastMessage`, `model.LastMessagePreview`, `Room.LastMsgID/At`). Converge: reimplement `roomLastMessage` atop `GetLastRoomMessage` (adding a row-scan cap), make `LastMessagePreview` the target shape, deprecate `model.LastMessage`.
- **medium** (drift risk): `lastMessageSkipTypes` hand-enumerates constants from `pkg/model/event.go:577-593`; a future system type silently leaks into previews. Export a canonical `model.SystemMessageTypes` set.

## (d) Design coherence
Good: the RPC returns plaintext; broadcast-worker seals `EncMsg` (`broadcast-worker/handler.go:826`) — encryption stays at the fanout boundary. Missing-room → empty reply (not NotFound) is intentional, commented, tested (`last_message_test.go:151`). **low**: a stale Mongo `lastMsgAt` ceiling can hide a newer survivor in the first bucket — shared, pre-existing trade-off with rooms.go. **nitpick**: preview `Type` can never be non-empty today (skip-set covers all defined types); `last_message.go:47-56` builds the query twice instead of a `firstBucket`-style branch.

## (e) Pattern adherence
Clean: `pkg/subject.MsgRoomLast` builder + test; Tier-1 errcode constructors, raw `%w` for infra, no log-and-return; bucket math stays inside cassrepo (`r.bucket`) so MESSAGE_BUCKET_HOURS never leaks; `package service_test` matches this package's existing unit-test convention; integration tests use shared testutil containers. **nitpick**: subject namespace `chat.server.request.msg.{siteID}…` vs sibling `chat.server.request.history.{siteID}.rooms.get` (`pkg/subject/subject.go:80`) — two namespaces for one service's internal RPCs.

## (f) Client-API docs
Compliant. The RPC is `chat.server.*`, correctly undocumented (precedent: rooms.get; `docs/client-api.md:88` scopes server subjects out). `DeleteRoomEvent.lastMessage` + the `LastMessagePreview` shared-schema table landed in `docs/client-api.md` with plaintext and `encMsg` JSON examples; `events.md` and `request-reply.md` updated in the same PR; `newThreadLastMsgAt`'s reply_deleted population reflected in both `client-api.md:5429` and `events.md:547`. No drift among the three views.

---

# Go expert

**Overall**: high-quality, TDD-shaped work. Tier discipline, nil-safety guards, and doc sync are all correct. Findings below.

**[high] Partial `$set` fabricates a corrupt preview on legacy rooms — `broadcast-worker/store_mongo.go:173-186`**
`SetRoomLastMessageEdited` filters only on `{_id, lastMsgId: editedMsgID}` with no guard that `lastMsg` exists. Rooms written before this feature (or rooms whose coalesced create-flush hasn't landed) have `lastMsgId` set but no `lastMsg` document. The dotted `$set {"lastMsg.msg", "lastMsg.editedAt"}` then creates a fragment missing `messageId`/`senderAccount`/`createdAt`, which BSON-decodes as a non-nil `*LastMessagePreview` with zero values (`CreatedAt` = 0001-01-01) served to clients in room snapshots. Fix: add `"lastMsg.messageId": editedMsgID` to the filter — guards existence and identity in one clause.

**[medium] Divergent sender-name cascades for the same denormalized field**
`broadcast-worker/handler.go:1013-1021` (`previewSenderName`: EngName → `UserDisplayName` → `UserAccount`) vs `history-service/internal/service/last_message.go:56-63` (`lastMessagePreview`: EngName → `AppName` → `Account`). The create path and the delete-rewind path write the same `rooms.lastMsg.senderName`, so a bot's preview name flips shape depending on which path wrote last. Align the cascades (or document why they must differ).

**[medium] Delete fan-out now hard-depends on history-service — `broadcast-worker/handler.go:540-546`**
Every visible delete blocks on a synchronous NATS RPC (Mongo read + Cassandra bucket walk behind it) and Naks on any failure, so a history-service outage halts all delete delivery with unbounded redelivery. This is deliberate and commented, and degrading is not free — the wire contract defines *absent* `lastMessage` as "clear the preview" (docs/client-api.md), so "unknown" is unrepresentable. Flagging the blast radius as a conscious trade-off the team should sign off on; a future `lastMessageUnknown` sentinel would decouple it.

**[low] Documented resurrect race — `broadcast-worker/store_mongo.go:108-133, 140-142`**
`BulkUpdateRoomLastMessage`'s unguarded `$set` can re-point the room at a just-deleted message when the delete lands before the create's ~250ms coalesced flush. Acknowledged in the comment as self-healing on the next room event; acceptable, but note a room can display a deleted preview indefinitely in an otherwise-quiet room. *(Escalated to high by the bug & security lens — see that chapter for the active-overwrite variants.)*

**[low] Interface placement inconsistency — `broadcast-worker/lastmsg_fetcher.go:21-23`**
`LastMessageFetcher` is declared beside its implementation, while sibling `ParentFetcher` sits with its consumer in `handler.go:52-57`. Same package, so the consumer-defined rule isn't breached, but move it next to `ParentFetcher` for symmetry.

**[nitpick] Discarded query allocation — `history-service/internal/cassrepo/last_message.go:52-62`**
The unbounded query is built unconditionally, then overwritten when `walked == 0`. Invert to if/else.

**[nitpick] Decrypt error mislabeled by caller wrap — `history-service/internal/cassrepo/last_message.go:104-106`**
`scanFirstQualifying` returns the `decryptIfNeeded` error bare; the sole caller (`last_message.go:65-67`) wraps everything as `"scan bucket %d"`, mislabeling a decrypt failure as a scan failure. Wrap locally (`"decrypt last-message row: %w"`). The bare `structScan` return at :90 is fine — the caller's wrap is accurate there.

**Verified clean**: errcode tiering is textbook (`errcode.BadRequest` + raw infra wraps + `errors.Is(mongo.ErrNoDocuments)`, `last_message.go:19-33`; no log-and-return anywhere); nil-deref guards on `EditedAt`/`UpdatedAt` precede every deref (`handler.go:311-313, 527-529`); sonic boundary rules honored — new wire types have no map fields and are pretouch-registered (`pretouch.go:19-20`); `errcode.Parse` in the fetcher follows the existing `parent_fetcher.go:60` precedent; struct tags are camelCase json+bson with correct pointer/`omitempty`/`json.RawMessage` semantics and BSON round-trip tests (`pkg/model/lastmsg.go`); guarded rewind filter is race-sound; the double-`%w` iterator-close wrap (`last_message.go:88`) is correct Go 1.20+ usage; `docs/client-api.md` + both derived views updated per CLAUDE.md.

---

# Test-automation

**Verdict: strong test discipline.** Every new exported/significant function in the diff has same-diff tests: `historyLastMessageFetcher.FetchLastMessage` (5 subtests: happy, nil-survivor, errcode envelope, no-responder, malformed JSON — broadcast-worker/lastmsg_fetcher_test.go), coalescer preview carry/latest-wins, `RewindRoomLastMessage`/`SetRoomLastMessageEdited` guarded-update no-ops incl. stale-encMsg `$unset` (with the bson.D decode pitfall correctly handled, broadcast-worker/integration_test.go:2288), `GetLastRoomMessage` repo (9 integration tests: tombstone-dense page crossing, older-bucket walk, at-rest decrypt, thread-only exclusion, system-skip), service RPC (bad request, repo error, room-not-found short-circuit asserted via *absent* repo expectation), `SoftDeleteMessage` 5-tuple with nil-vs-value tlm, model/BSON round-trips + omitempty, subject builder. Nak-ordering invariants ("no publish before failed write/encrypt") are asserted via absent mock expectations throughout — exemplary. Table-driven where variations exist (`previewSenderName`, sender fallbacks). Integration tests use shared `setupMongo`/`setupCassandra` testutil helpers, per-test room IDs, `t.Cleanup`; no inline GenericContainer. `-race` via Makefile, no deviations.

Findings:

- **medium** — broadcast-worker/handler_lastmsg_test.go: no test for the **TShow=true (visible) thread-reply delete** lane. docs/client-api.md:3167 explicitly documents it as carrying `lastMessage`, and handler.go:531/571 routes it through the room path (preview fetch + rewind + badge). Only the hidden `TShow=false` lane is tested (handler_lastmsg_test.go:677). Per Section 4 "all documented scenarios," add a test asserting preview fetch, rewind, `lastMessage` on the delete event, and the badge publish.
- **medium** — mock staleness **not byte-verifiable in this environment**: `make generate` (full and `SERVICE=broadcast-worker`/`history-service`) fails — pinned mockgen was built with go1.24 vs the go1.25 toolchain ("Loading input failed… go1.25 (application built with go1.24)", Makefile:78). Signature-level freshness *is* proven (mocks are consumed as the interfaces; unit suite compiles green). The failed run also dirty-touched two unrelated generated files (pkg/emoji/standard_emoji_gen.go, portal-service/mock_store_test.go — non-semantic reordering); reverted via `git checkout --`, tree verified clean. Fix `make tools`/mockgen pinning so the pre-commit "run make generate" guardrail actually works.
- **low** — broadcast-worker/lastmsg_fetcher_test.go:1: untagged unit test spins up an in-process nats-server per subtest (`startTestNATS`, parent_fetcher_test.go:22). Section 4 says no real NATS in unit tests; this follows the pre-existing parent-fetcher precedent with proper `t.Cleanup` isolation, so noting rather than blocking — but the pattern deserves an explicit sanction in CLAUDE.md.
- **low** — broadcast-worker/handler.go:355-363: encrypted-room **edit with empty content** (attachment-only edit → `encryptPreviewContent` returns nil,nil → store patch `("", nil)` `$unset`s encMsg) is untested; empty-content is only covered on the create path (handler_lastmsg_test.go:1153).
- **low** — history-service/internal/service/last_message_test.go:82: the historyFloor clamp branch of `walkBounds` is deliberately dodged ("so the historyFloor clamp never kicks in") — the clamped bound for this RPC is never asserted.
- **low** — history-service/internal/cassrepo/last_message.go:68: `maxBuckets` exhaustion exit untested (tests cover floor and bucket-walk but not the walked-budget bound).
- **nitpick** — broadcast-worker/mock_lastmsgfetcher_test.go is generated (store.go:16 directive) but unused; all tests use hand-rolled `stubLastMsgFetcher`/`nopLastMsgFetcher` (handler_lastmsg_test.go:23,34). Use the mock or drop the directive.
- **nitpick** — cassrepo/last_message.go:97-128 `scanFirstQualifying` scan/Close error paths uncovered; acceptable without gocql fault injection.
- **nitpick** — single squashed commit: Red-phase ordering unverifiable from history; test content itself is TDD-shaped (behavior-first, exhaustive error paths).

No shared mutable state, no ordering dependencies, `TestMain`/testutil cleanup conventions intact.
