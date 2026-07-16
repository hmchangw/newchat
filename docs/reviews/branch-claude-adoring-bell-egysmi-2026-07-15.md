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
