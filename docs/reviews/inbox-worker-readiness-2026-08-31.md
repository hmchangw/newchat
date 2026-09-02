# inbox-worker — Production Readiness Review

**Service:** `inbox-worker` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 2.8 / 5

**The lowest-scoring service in the audit — and the most consequential position in the fleet**, since it is the destination side of *all* federation and the sole owner of the INBOX stream. It gets ownership exactly right: `bootstrap.go` sets only `Name + Subjects`, contains no gateway config, fail-fast-verifies in production, and every event type any producer emits is dispatched here. But the ordering guarantee the origin pays for is **thrown away at the destination**: `room_renamed` rides the origin's FIFO lane yet is routed to the concurrent fan-out pool here, so a rename can be applied before the subscription it renames exists — permanently. `subscription_opened` is applied with **no high-water-mark guard** despite its concurrent lane being justified on the claim that one exists. Coverage is **44.1%**, the worst in the fleet, with the entire store and all of `main()` at 0%.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 3 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 11 | 21 | 13 | 5 | **51** |

---

## 2. Go code quality — 4 / 5

Error wrapping, logging discipline and guard documentation are consistently above average for this repo; deductions are a stringly-typed dispatch that silently drops events, one dead store method, a bare `return err`, and two log-and-return sites.

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | **`HandleEvent`'s dispatch uses raw string literals for 10 of 21 event types** while the other 11 use `model.Inbox*` constants that exist for every one of them. **The `default` branch returns `nil`, so `jsretry.Settle` Acks**: any drift between a publisher's constant and a literal here **silently discards a federated event** with only a `Warn`. There is no compile-time link — and `"room_sync"` has no constant at all, at either end | `handler.go:226-246`; `pkg/model/event.go:167-187` |
| medium | `CreateSubscription` is **dead**: declared on `InboxStore`, implemented against Mongo, and mocked, but no handler ever calls it. §3 requires the consumer-side store interface to carry "only the methods it needs" | `handler.go:23`; `main.go:132`; `mock_store_test.go:144` |
| medium | `CreateSubscription` returns a bare `err` from `InsertOne` with no context — the **only unwrapped store error in the file**, where every other method wraps precisely | `main.go:134` |
| medium | **Malformed-payload handling is inconsistent**: 19 of 22 `json.Unmarshal` failures return a plain wrapped error (transient → NAK), while 3 return `errcode.Permanent`. A payload that fails to parse **will never parse on redelivery**, so the transient sites burn the full `DefaultBackoff` budget (~12.6 min over `MaxDeliver`) before being dropped anyway | `handler.go:611`, `:633`, `:644` vs `:277`, `:377`, `:398`, `:455`, `:503` |
| low | Two handlers log **and** return the same failure, and `main.go` settles with `jsretry.Settle` (which logs the business error) — each poison event produces two log lines | `handler.go:463-465`, `:618-622` |
| low | `slog.Warn` (no `Context`) on the **unknown-event-type drop path** — the one log line emitted when an event is silently discarded is also the one line that loses trace correlation and `request_id` | `handler.go:271` |
| low | `main.go` is 1,046 lines and holds the entire Mongo store (~35 methods); the `InboxStore` interface lives in `handler.go`. Owned by D2/D4; noted here because it is why `main.go` mixes wiring with query construction | `main.go:68` |
| low | Audit-coverage gap (environmental): `govulncheck` + registry packs blocked | — |
| nitpick | `badge` and `valkey` are assigned post-construction even though `NewHandler` already accepts `HandlerOption`s; `fmt.Errorf` with no format verb where `errors.New` is idiomatic | `main.go:893-894`; `handler.go:186-192`, `:203` |

### Recommendations
- `medium` — Replace all 10 string literals in `HandleEvent` with the `model.Inbox*` constants; add `InboxRoomSync` to `pkg/model/event.go` and use it in **both** `inbox-worker` and the migration publisher, so the two sides cannot drift.
- `medium` — Delete `CreateSubscription` from `InboxStore`, the Mongo store and the test double, then `make generate SERVICE=inbox-worker`. If it survives instead, wrap its error.
- `medium` — Make every `json.Unmarshal` failure return `errcode.Permanent(errcode.BadRequest("unmarshal <type> payload"))`, matching the three sites that already do and the `broadcast-worker` pattern.
- `low` — Drop the two `slog.WarnContext` calls before a returned `Permanent`; change `handler.go:271` to `WarnContext` and include `evt.SiteID`, since that line is the **only trace of a dropped federated event**.
- `nitpick` — Split the store out of `main.go` into `store.go` + `store_mongo.go` (see Chapter 3); use the existing `HandlerOption`s for `badge`/`valkey`.

---

## 3. Architecture — 3 / 5

INBOX ownership, bootstrap scoping and the two-lane consumer are exemplary and well-reasoned, but the service violates the mandated file organization and re-declares a shared cross-service knob.

### Verified correct — the ownership rules
`bootstrap.go` sets **only `Name + Subjects`** from `stream.Inbox`, contains **no sourcing/mirror/SubjectTransform/gateway config**, no-ops creation when `Enabled=false`, and fail-fast-verifies in production. **Sole INBOX creator confirmed** — `search-sync-worker` explicitly skips INBOX bootstrap. The consumer is the correct high-throughput pattern (`cons.Messages()` + `PullMaxMessages(2*MaxWorkers)` + semaphore), never mixed with `Consume()`; `buildConsumerConfig` uses `stream.DurableConsumerDefaults` with no hardcoded `BackOff`; `FilterSubjects` correctly scopes to `external.>`, leaving `internal.>` to search-sync-worker. `shutdown.Wait` order matches the documented sequence.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | **No `store.go` / `store_mongo.go`.** The `InboxStore` interface lives in `handler.go` and ~730 lines of MongoDB implementation (33 methods) live in `main.go`. This is a flat worker, so the sanctioned sub-package exception does not apply — `main.go` should be config + wiring + shutdown, and is instead the largest file in the service | `main.go:68-793`; `handler.go:22` |
| high | **Shared knob re-declared per service**: `BADGE_CACHE_TTL` duplicated verbatim in `room-service` and `user-service`, while `pkg/badgecache` declares no `TTLConfig`. **The in-code comment ("Keep identical across all badge-cache writers") documents exactly the drift the rule exists to prevent** — and this service *writes* badge state that user-service *reads*. Contrast the correct handling of `Pool mongoutil.PoolConfig` and `Valkey valkeyutil.Config` in the same struct | `main.go:64`; `room-service/main.go:115`; `user-service/config/config.go:125` |
| medium | Constructor DI bypassed — `handler.badge`/`handler.valkey` assigned post-construction, though a `HandlerOption` mechanism already exists and `NewHandler` accepts variadic options | `main.go:889-890`; `handler.go:172-186` |
| medium | Event-type dispatch is **half-typed**: 11 raw literals, 11 constants — while `isMembershipSubject` builds the *same* two subjects from the constants. **A rename of `model.InboxMemberAdded` would silently desynchronise the router from the lane classifier** | `handler.go:226-243`; `main.go:1035-1036` |
| medium | **`remote_rooms` is a write-only collection.** inbox-worker upserts and deletes ordering rows and `pkg/model.RemoteRoom` documents it as the writer, but a repo-wide grep finds **no reader**. The whole activity-refresh lane — core-NATS subscriber, `roomsubcache.go` (110 lines), three store methods, `HasRoomSubscription`, plus `ROOM_SUB_CACHE_*` config — currently feeds nothing | `main.go:81`, `:101`, `:111`, `:902` |
| medium | **The two lanes are coupled by back-pressure.** The single dispatcher does a **blocking** `membershipCh <- m`; when the sequential membership worker is slow, the buffer fills and the dispatcher stops calling `iter.Next()`, **stalling the fan-out lane too**. The comment claims serialising membership "costs negligible throughput" — that holds for steady state, not for a membership stall | `main.go:962`, buffer at `:942`, comment at `:917-931` |
| low | No `New<Type>` constructor for the store; it is a bare struct literal with five collection handles in `main()`, so the collection-name wiring is untestable and unshared | `main.go:840-846` |
| low | `pkg/stream.Inbox` builds INBOX subjects with raw `fmt.Sprintf` rather than `subject.InboxInternal`/`InboxExternalAll`, unlike sibling `Outbox`. **inbox-worker itself is clean** — zero `fmt.Sprintf` in service code | `pkg/stream/stream.go:69-70` vs `:78` |
| nitpick | Service-local `roomSubCache` shadows the unrelated `pkg/roomsubcache` — different data, same name | `roomsubcache.go:23` |

### Recommendations
- `high` — Split `main.go`: move `InboxStore` + `//go:generate` to `store.go`, the 33 Mongo methods to `store_mongo.go`, add `newMongoInboxStore(db)`, and move the two-lane pull loop + `isMembershipSubject`/`buildConsumerConfig` to `consumer.go`. Leaves `main.go` at ~150–250 lines. **Pure code motion.**
- `high` — Add `badgecache.TTLConfig{ TTL … }` in `pkg/badgecache` and mount it as a named field in all three services; delete the per-service declarations.
- `medium` — Add `WithBadgeCache` / `WithSubauthValkey` options and drop the post-construction writes.
- `medium` — Convert every `HandleEvent` case to a constant, and change `InboxEventType` from a **type alias** (`= string`) to a defined type so a missing constant is a compile error.
- `medium` — Decide `remote_rooms`' fate: land the chat-list reader, or delete the activity lane, `roomsubcache.go`, the three store methods and the `ROOM_SUB_CACHE_*` config.
- `medium` — Decouple the lanes: give the membership lane its own consumer, or make the dispatcher's send non-blocking-with-overflow so a membership stall cannot starve read receipts.

---

## 4. Test coverage — 1 / 5

**44.1% (669 statements) — the lowest in the fleet**, far under the §4 60% line. `handler.go` is genuinely well tested (~90%, with real Permanent/badge/guard assertions), but **the whole `mongoInboxStore` and all of `main()` sit at 0% in the default profile, so the destination side of federation has no signal from `make test`.**

| Sev | Finding | Evidence |
|-----|---------|----------|
| critical | 44.1% (669 stmts) — under 60% and far under the 80% floor | `main.go:86` |
| high | **The entire store implementation (~50 methods) is 0% in the unit profile**; every guarded-write semantic — high-water `$lt` guards, `$max`, `MatchedCount==0` disambiguation — is verified only behind `//go:build integration`. **Pre-commit/CI unit runs and the coverage gate therefore protect none of the write semantics this service exists to enforce** | `main.go:194` |
| high | **`main()` is 0% and structurally untestable**: the membership-FIFO vs `MaxWorkers` fan-out dispatcher, the `jsretry.Settle` wiring and the `jobguard` panic→Ack drop are all inline. **Nothing proves a `member_added` actually lands on the sequential lane** — the add/remove resurrection race that the 20-line comment at `main.go:914` defends against is asserted by comment only | `main.go:948` |
| high | Three store methods have **zero coverage at any level** (absent from integration too): `UpdateSubscriptionSection` (including the `sectionID == nil` `$unset` + favorite-reset branch and its NAK decision), `UpdateUserChatlist`'s watermark, and `BulkRefreshJoinedAt`'s unordered `BulkWrite` | `main.go:473` |
| medium | **`mock_store_test.go` (454 generated lines) has zero references repo-wide**; all 100 handler tests use a hand-rolled 100-field `stubInboxStore`. The mandated mockgen artifact is dead code `make generate` keeps regenerating | `handler_test.go:104` |
| medium | Permanent-vs-transient classification is **untested for the two order-sensitive federated events** — the `errcode.Permanent` branches in `handleRoomRenamed` and `handleRoomVisibilityChanged`, and both store-error branches beside them. A mis-tag is either infinite redelivery or a **silent poison-drop of a federated rename** | `handler.go:632` |
| low | Federation fallback/error branches uncovered: `HandleRoomActivity`'s subscription-check and upsert failures, `handleMemberRemoved`'s warn-only delete path, and the room-sub cache's fill/read error paths | `handler.go:206`, `:212`, `:418`; `roomsubcache.go:80`, `:105` |
| low | 100 top-level `Test*` funcs but only 10 `t.Run` tables — e.g. seven `MemberRemoved_*` and six `SubscriptionRead_BadgeCache_*` near-clones sharing one input shape | `handler_test.go:1481` |
| nitpick | Integration hygiene is correct and worth preserving: `TestMain` → `testutil.RunTests`, containers from `pkg/testutil` with no inline `GenericContainer`, no package-level mutable state | `main_test.go:11` |

### Recommendations
- `high` — Extract the dispatch loop from `main()` into a testable `dispatch(iter, sem, membershipCh)` (or a `laneRouter` type) and unit-test with a fake `jetstream.Msg` that **membership subjects serialize FIFO while others fan out**, and that a panicking handler Acks rather than crash-loops.
- `high` — Add integration coverage for `UpdateSubscriptionSection` (both `sectionID` nil and non-nil, guard-rejected, missing-sub NAK), `UpdateUserChatlist` (out-of-order skipped / newer applies), and `BulkRefreshJoinedAt` — mirroring the existing out-of-order test pattern.
- `medium` — Add a coverage target that runs `-tags=integration` so the store's 0% is not the reported number; **the current 44.1% understates real verification and hides which paths are genuinely unverified.**
- `medium` — Either delete `mock_store_test.go` and its `//go:generate`, or migrate `stubInboxStore`'s call sites to `NewMockInboxStore`. Keeping both is a standing drift risk.
- `medium` — Extend the `handleSubscriptionMention` `wantPermanent` table pattern to `room_renamed` and `room_visibility_changed`.
- `low` — Collapse the `MemberRemoved_*` and `SubscriptionRead_BadgeCache_*` clusters into two tables; cover `HandleRoomActivity`'s two error branches, since it is the only fire-and-forget entry point where a swallowed failure is invisible in production.

---

## 5. Maintainability — 3 / 5

Excellent WHY-comment discipline and well-factored small units (`roomsubcache.go`, `bootstrap.go`), undermined by a 1,046-line `main.go` that swallows the whole store layer, a dead generated mock shadowed by a 600-line hand-written double, and a dispatch switch that mixes literals with constants.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | `main.go` is 1,046 lines carrying **three unrelated responsibilities**: `config`, the entire 30-method / ~715-line Mongo store, and `main()` + two-lane dispatch. `CLAUDE.md` mandates `store_mongo.go`; **20 peer services comply.** Adding one federated event means editing a file where wiring, lane routing and 30 Mongo queries already compete for attention | `main.go:68` |
| high | **The generated mock is dead.** `mock_store_test.go` (454 lines) has **zero** references; all 100 unit tests drive a hand-written 46-method `stubInboxStore` spanning 600+ lines. **Two doubles for one interface**: every `InboxStore` change requires hand-editing ~620 lines that mockgen already regenerates for free | `handler_test.go:104-715` |
| medium | `HandleEvent`'s switch mixes 9 bare string literals with 12 `model.Inbox*` constants — for keys that **all exist as constants**. `InboxEventType` is a **type alias for `string`**, so there is no compile-time check: a typo or a constant-value change routes to `default:` and **silently Acks**. `isMembershipSubject` already uses the constants, so the two dispatch points can drift apart | `handler.go:224`; `pkg/model/event.go:164`, `:167-187` |
| medium | Dead interface method: `CreateSubscription` is declared, implemented (the only method in the file returning a bare unwrapped `err`), and stubbed in tests — but **no handler calls it** | `handler.go:23`; `main.go:132` |
| medium | `InboxStore` has **30 methods over five collections** and `HandleEvent` fans to 22 event types. The five `user_*` appliers replicate identity/settings/permissions/chatlist and **share nothing with room membership** — clear responsibility creep past the service's original remit | `handler.go:22`, `:671-741` |
| medium | The watermark filter `$or: [{x: {$exists: false}}, {x: {$lt\|$lte: t}}]` is **hand-written 13 times**. A mistyped field name is a **permanently silent no-op with no test that would notice.** The already-extracted `threadReadGuard`/`threadReadUpdate` prove the pattern is extractable — it was just applied once | `main.go:146`, `:175`, `:265`, `:267`, `:286`, `:313`, `:331`, `:349`, `:421`, … |
| low | **Orphaned doc comment**: `handleMemberRemoved`'s six-line doc block sits directly above `handleMemberJoinedAtRefreshed`'s own comment and function, so godoc **describes a different function**, and the real `handleMemberRemoved` is undocumented | `handler.go:366-375`, `:394` |
| low | `handler_test.go` is 3,426 lines / 100 top-level tests in one file, mirroring `handler.go`'s breadth | `handler_test.go:1` |
| nitpick | `handleMemberRemoved` performs six actions with **three different failure policies** (return-error, log-and-continue, nil-checked no-op) interleaved; the ordering constraints live only in comments | `handler.go:394-435` |

### Recommendations
- `high` — Split `main.go` per `CLAUDE.md`: move `mongoInboxStore` and its 30 methods verbatim to `store_mongo.go`, the interface + `//go:generate` to `store.go`, and the two-lane pull loop + `isMembershipSubject`/`buildConsumerConfig` to `consumer.go`. **Leaves `main.go` at ~150 lines. Pure code motion, no behaviour change.**
- `high` — Delete `stubInboxStore` and migrate the unit tests to the already-generated `MockInboxStore`; or, if the stub's recorded-call accessors are genuinely needed, delete `mock_store_test.go` and the directive instead. **Keeping both is the worst option.**
- `medium` — Replace every string literal in the switch with its constant, add the missing `InboxRoomSync` constant, and change `InboxEventType` from a type alias to a **defined type** so the compiler catches drift.
- `medium` — Drop `CreateSubscription` from the interface, the store and the double.
- `medium` — Extract `watermarkGuard(field string, at any) bson.A` and use it at all 13 sites; add one table test asserting the produced filter per field name.
- `low` — Move the misplaced doc block; extract the five `user_*` handlers into `handler_user.go`/`store_mongo_user.go` as the low-risk first step toward splitting user replication out of inbox-worker entirely.

---

## 6. Integration — 3 / 5

Every produced INBOX event type is consumed and the lane/ownership rules are implemented exactly as `CLAUDE.md` specifies — but **the destination-side lane split silently discards the FIFO ordering guarantee the origin OUTBOX pays for**, and one concurrent-lane handler lacks the high-water-mark guard its lane assignment assumes.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | **`room_renamed` is in `pkg/outbox.OrderedEventTypes`** — it shares the per-destination FIFO lane precisely "so a `room_renamed` can't overtake the `member_added` that creates the subscription it renames" — **but at the destination `isMembershipSubject` routes only `member_added`/`member_removed` to the sequential lane.** `room_renamed` goes to the `MaxWorkers` fan-out pool and is processed concurrently with an in-flight `member_added`. **The stranding is permanent, not transient**: `UpdateSubscriptionNamesForRoom` is an `UpdateMany` over *existing* subs, so a rename applied before the subscription exists matches zero documents, and `handleMemberAdded` then writes the stale `event.RoomName` with no later event to correct it. **The origin FIFO lane (`MaxAckPending=1`) buys an ordering the destination throws away** | `main.go:1032-1035`; `main.go:633-646`; `handler.go:313` |
| high | **`subscription_opened` is applied with no high-water-mark guard, contradicting the stated precondition for its lane.** `UpdateSubscriptionOpen` is a bare `$set{open}`, while `model.SubscriptionOpenedEvent.Timestamp` **exists and is simply ignored** — and `pkg/outbox.ConcurrentEventTypes` justifies concurrent forwarding on the claim that "inbox-worker applies them under high-water-mark / idempotent-upsert guards". A hide→reopen pair reordered across the fan-out pool leaves the room **permanently in the wrong state**. Every sibling handler (mute, favorite, section_moved, role, rename, restrict) *does* carry the `$lt` guard — this one is the outlier | `main.go:511-523`; `handler.go:532-541`; `pkg/model/event.go:673-678` |
| medium | **`room_sync` has no `model.Inbox*` constant** — the producer builds it as `model.InboxEventType("room_sync")` and the consumer matches a bare literal. The one cross-site contract with **no compile-time anchor at either end** | `data-migration/oplog-collections-transformer/rooms.go:205`; `handler.go:231` |
| medium | 12 of the 22 dispatch cases match raw string literals. Because `InboxEventType` is a **type alias** for `string`, changing a constant's value compiles clean everywhere and silently routes the new subject to `default:` → `Warn` + **Ack — events dropped, not redelivered**. The file already uses constants for the newer 10 cases, so this is half-migrated drift | `handler.go:226-246`, `:270-272` |
| medium | `BADGE_CACHE_TTL` re-declared in three services against the declare-once rule. `pkg/badgecache` is the owner, and **inbox-worker writes badge state that user-service reads**, so a divergent TTL is a real coherence bug the tag-level default cannot prevent | `main.go:64`; `room-service/main.go:115`; `user-service/config/config.go:125` |
| low | `ApplyUserPermissions`'s watermark uses `$lte` on `updatedAt`, so two events sharing a millisecond resolve last-write-wins rather than being rejected; **every other guard in the file uses strict `$lt`** | `main.go:314` |
| low | `handleRoomSync` upserts the whole `model.Room` with **no timestamp guard**, unlike every other room-scoped handler, and rides the fan-out pool alongside the guarded `room_renamed`/`room_restricted` events the transformer emits with it | `handler.go:436-447` |

### Verified clean
**All 21 `model.Inbox*` constants plus `room_sync` are dispatched** — cross-checked against `ConcurrentEventTypes` (13), `OrderedEventTypes` (3), and every direct external-lane publisher (`user-service` ×3, `admin-service` ×2, `outbox-worker`, the oplog transformer). **No produced type is unconsumed.** Consumer filters `external.>` only, leaving `internal.>` to search-sync-worker. Sole INBOX owner with fail-fast verification when disabled, setting only `Name + Subjects`. Subjects built exclusively via `pkg/subject`. `idgen.GenerateUUIDv7()` for federated subscriptions. `jsretry.Settle` throughout; no bare `Nak()`. No `chat.user.*` handler, so `docs/client-api.md` is not implicated.

### Recommendations
- `high` — **Extend `isMembershipSubject` to cover every `pkg/outbox.OrderedEventTypes` member — derive the set from that slice rather than hardcoding two subjects** — so the destination lane split mirrors the origin FIFO partition; add a test asserting the two sets agree.
- `high` — Add a `nameUpdatedAt`-style watermark to `member_added`'s subscription insert, or have `UpdateSubscriptionNamesForRoom` record the rename so a later-created subscription adopts it. **The ordering fix alone still loses a rename that races a redelivered `member_added`.**
- `high` — Guard `UpdateSubscriptionOpen` with an `openUpdatedAt` `$lt` from `SubscriptionOpenedEvent.Timestamp`, matching the mute/favorite handlers — **the event already carries the field.**
- `medium` — Replace all remaining literal cases with constants and add a `room_sync` constant used by both the transformer and this switch.
- `medium` — Move `BadgeCacheTTL` into a `badgecache.TTLConfig` mounted in all three services.
- `low` — Tighten the permissions watermark to `$lt`; add a timestamp guard to `handleRoomSync`.

---

## 7. Performance — 3 / 5

Sound hot-path design (two-lane dispatch, LRU membership cache, `jsretry.Settle` everywhere, guarded idempotent writes), undercut by one unprojected `Find` on the member_added path, cross-lane head-of-line blocking, and an uncoalesced serialized activity lane.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | **`FindUsersByAccounts` issues a `Find` with no projection**, decoding whole `model.User` docs — which carry `Services` (**credential material**), `Settings`, `Permissions` and `Chatlist` — while `handleMemberAdded` reads only `user.ID` and `user.Account`. A large `member_added` (team import) drags full documents over the wire **on the sequential membership lane** | `main.go:245-253`; `handler.go:306-315`; `pkg/model/user.go:66-79` |
| high | **The puller blocks on the membership channel, stalling the fan-out lane.** `membershipCh <- m` is an unguarded blocking send from the single `iter.Next()` loop, and the membership lane is one goroutine costing 3–4 sequential Mongo RTTs per event. A membership burst fills the 100-slot buffer and then **no further messages are pulled at all** — the high-volume `subscription_read`/`thread_read` lane stops even though `MaxWorkers` is idle. **Worse, buffered messages sit un-acked past `AckWait` (30 s), so the server redelivers them and duplicate work is amplified exactly when the lane is already behind** | `main.go:970-972`, buffer at `:937` |
| medium | Room-activity refresh is **fully serialized with an un-coalesced Mongo write per event** — a core-NATS async subscription runs its callback on one goroutine, so every refresh's upsert (and every cache-miss `FindOne`) is serialized at ~1/RTT per pod. Volume grows with active-room count, and **overflow here is a silent slow-consumer drop, not backpressure.** The `$max` guard makes the write coalescible in memory; nothing coalesces it | `main.go:902-908`; `handler.go:212` |
| medium | Badge invalidation **loops per account** instead of batching — one `SREM` round trip per removed account, on the sequential membership lane, while the adjacent `subauthcache.BustSubs` for the same account list is a **single batched call** | `handler.go:429-431`; `pkg/badgecache/badgecache.go:204-208` vs `handler.go:408` |
| medium | `InboxEvent.Payload` is `[]byte`, so **every federated payload is base64-encoded on the wire** — ~33% inflation plus an encode and a decode-and-copy per event, on this service's entire input. `OutboxEvent.Envelope` uses `json.RawMessage` and **its comment names precisely this problem.** (Repo-wide change, not inbox-worker-local) | `pkg/model/event.go:289` vs `:294-297` |
| medium | **The dispatcher goroutine is untracked and can `wg.Add` after `wg.Wait` returned** — the loop has no `wg.Add`, and calls `wg.Add(1)` concurrently with the drain step's `wg.Wait()`. A message already returned by `iter.Next()` can start processing **after the drain declares success**, then run against a Mongo client being disconnected | `main.go:962-984` vs `:996-1010`, `:1014` |
| low | Every guard-rejected write pays an extra `FindOne` — duplicate/out-of-order deliveries (the **normal** federated case) cost two round trips instead of one. Acceptable on cold paths; `UpdateSubscriptionRead` is not one | `main.go:186`, `:430`, `:462`, `:502`, `:520`, `:545` → `:195-220` |
| nitpick | `encoding/json` is correct here (not on the sonic list); `jsretry` discipline is clean — single settle point, no bare `Nak()`/`NakWithDelay(0)`, `cc.BackOff` derived | `main.go:948`, `:1041-1045` |

### Recommendations
- `high` — Project `FindUsersByAccounts` to `{_id:1, account:1}` and decode into a local two-field struct; the handler needs nothing else.
- `high` — **Make the membership hand-off non-blocking-safe**: size `membershipCh` against `MaxAckPending`, and on a full buffer `NakWithDelay` (via `jsretry.Nak`) rather than blocking the puller, so backpressure lands on **one message** instead of the whole consumer.
- `medium` — Coalesce room-activity: keep an in-memory per-room `$max` map flushed on a ticker via `BulkWrite`, and/or run the callback on the existing bounded worker pool.
- `medium` — Batch the badge clears: add `ClearRooms(ctx, accounts, roomID)` to `pkg/badgecache` that pipelines per-slot, mirroring `subauthcache.BustSubs`.
- `medium` — Track the dispatcher goroutine in `wg` and stop it before the drain step, so no handler can begin after `wg.Wait()` returns.
- `medium` — Change `InboxEvent.Payload` to `json.RawMessage` in a coordinated PR with the publishers, matching `OutboxEvent.Envelope`.
- `low` — Fold the existence disambiguation into the guarded write to drop the second round trip on `UpdateSubscriptionRead`.

---

## 8. Prioritized action list

| # | Sev | Action | Dimension | Evidence | Why |
|---|-----|--------|-----------|----------|-----|
| 1 | `high` | Derive `isMembershipSubject` from `pkg/outbox.OrderedEventTypes` so `room_renamed` rides the sequential lane, and add a test asserting the two sets agree | Integration | `main.go:1032-1035` | **The destination throws away the ordering the origin pays `MaxAckPending=1` to preserve.** A rename processed before its `member_added` matches zero subscriptions, and `handleMemberAdded` then writes the stale name — **permanently, with no correcting event.** |
| 2 | `high` | Guard `UpdateSubscriptionOpen` with an `openUpdatedAt` `$lt` from the event's own `Timestamp` | Integration | `main.go:511-523`; `pkg/model/event.go:673` | Its concurrent lane is justified on the claim that inbox-worker applies it under a watermark. **It does not.** A reordered hide→reopen leaves the room permanently wrong, and the field it needs is already on the wire. |
| 3 | `high` | Project `FindUsersByAccounts` to `{_id:1, account:1}` | Performance | `main.go:245-253` | Whole `model.User` documents — including credential material in `Services` — pulled over the wire on the sequential membership lane, to read two fields. |
| 4 | `high` | Make the membership hand-off non-blocking (Nak on full buffer instead of blocking the puller) | Performance | `main.go:970-972` | **Head-of-line blocking across lanes**: a membership burst stops `iter.Next()` entirely, stalling read receipts while workers idle — and the buffered messages then exceed `AckWait` and get redelivered, amplifying the backlog. |
| 5 | `critical` | Split `main.go` into `store.go` / `store_mongo.go` / `consumer.go`, then unit-test the dispatcher and the three uncovered store methods | Test coverage / Maintainability | `main.go:68-793`, `:948` | 44.1% is the fleet's worst, and **the whole store and all of `main()` are 0%** — so `make test` verifies none of the guarded-write semantics this service exists to enforce. The split is pure code motion and is what makes items 1–4 testable. |
| 6 | `medium` | Replace every literal dispatch case with a `model.Inbox*` constant; make `InboxEventType` a defined type, not an alias | Code quality / Integration | `handler.go:226-246`; `pkg/model/event.go:164` | Today a constant rename compiles clean and routes the event to `default:` — which **Acks**. Federated events are dropped, not retried, with one `Warn` as the only trace. |
| 7 | `high` | Delete either `stubInboxStore` or `mock_store_test.go` | Maintainability | `handler_test.go:104-715` | Two doubles for one 30-method interface; the generated one is dead, so every interface change means hand-editing ~620 lines mockgen would regenerate free. |
| 8 | `medium` | Move `BADGE_CACHE_TTL` into `badgecache.TTLConfig`; extract `watermarkGuard(field, at)` and use it at all 13 sites | Architecture / Maintainability | `main.go:64`; 13 sites from `:146` | The badge TTL comment already says "keep identical across all writers" — which is the rule, hand-enforced. A mistyped watermark field is a **permanently silent no-op** no test would catch. |
| 9 | `medium` | Decide `remote_rooms`' fate — land the reader or delete the lane | Architecture | `main.go:81`, `:101`, `:111`, `:902` | An entire write-only collection plus a core-NATS subscriber, a 110-line cache, three store methods and its own config, **feeding nothing**. |
| 10 | `medium` | Track the dispatcher goroutine in `wg` and stop it before the drain | Performance | `main.go:962-984` | A handler can currently begin **after** the drain declares success and then run against a Mongo client mid-disconnect. |

### Verdict

**Fix items 1–2 before shipping.** This service holds the most important position in the fleet — the destination side of all federation — and it gets stream ownership and event coverage exactly right. But two ordering guarantees that the rest of the system pays real throughput for are silently dropped here, and both cause **permanent** state divergence rather than transient lag. Item 5 is why nobody would notice: at 44.1%, with the store and `main()` at zero, the unit suite verifies none of it.
