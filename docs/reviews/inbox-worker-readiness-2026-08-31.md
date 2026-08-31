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
| Count | 1 | 13 | 20 | 12 | 6 | **52** |

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

