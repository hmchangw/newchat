# bot-message-handler — Production Readiness Review

**Service:** `bot-message-handler` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

Textbook CLAUDE.md layout — consumer-defined store, constructor DI, `pkg/subject` builders, opt-in bootstrap, correct shutdown order — in 1080 readable lines. The gaps cluster in two places, and they compound.

**Half the client-facing surface is untested.** `handleSendDM` is at **0.0%** coverage: every DM-specific behaviour — the missing-`userID` branch, the `idgen.BuildDMRoomID` derivation, the DM-specific `Forbidden` reply — has no test, because all thirteen handler tests exercise `handleSendRoom`. There are no integration tests at all, so the whole Mongo store is 0% and the `ErrNotFound` translation **every handler branch keys on** is entirely unverified. `Register` is 0% too, so a copy-paste swap of the two route patterns would ship green.

**And the mention path is an N+1 on an unbounded scan, with nothing in front of Mongo.** `canonicalizeMentions` fetches every member of the room and then issues one `FindUser` per mention inside the loop — 11 round trips for a 10-mention message, on top of the 2 the handler already makes — while `ListMemberIDs` streams every subscription document of the room just to answer "is this one user a member". By explicit design there is no cache, and no breaker was mounted alongside that decision.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 2 | 4 | 9 | 15 | 2 | **32** |

---

## 2. Go code quality — 4 / 5

Idiomatic, well-wrapped, no logging or `errors.Is` violations; one genuine `pkg/errcode` tiering breach and an unvalidated numeric header keep it off 5.

### Findings
- `medium` — infra publish failure is dressed up as an errcode instead of a raw wrapped error: `errcode.Internal("publish canonical", errcode.WithCause(err))` — `bot-message-handler/handler.go:200`. CLAUDE.md Tier 1 is explicit: "For an infra failure, `return fmt.Errorf("…: %w", err)` … do NOT dress it up as an errcode." Every other error site in the file gets this right (`handler.go:65,99,147,267,291`).
- `medium` — `parseHeaderIDs` accepts any `int64` unix-ms from `X-Bot-Created-At` with no range sanity check — `bot-message-handler/handler.go:237-242`. That value becomes `Message.CreatedAt` and is the Cassandra partition/clustering input downstream (`bot-message-worker/store_cassandra.go:70,111`), so a negative or year-3000 value writes a message into an unreachable bucket.
- `low` — `Subscription.SiteID`, `Room.Type/Name/SiteID` are decoded and projected but never read; all three call sites discard the value (`_, err :=`) — `bot-message-handler/store.go:28-39`, `handler.go:59,94,142`. Either enforce `sub.SiteID == h.siteID` (real defence-in-depth, the comment at `handler.go:57` implies it) or shrink the types to an existence check.
- `low` — SAST audit-coverage gap: gosec and repo-owned semgrep are clean repo-wide, but `govulncheck` and the semgrep registry packs could not run (blocked egress, per GLOBAL_PREP). Environmental, not a service defect.
- `nitpick` — `publishTimeout` is a hardcoded 2s const while every other timing knob is env-driven — `bot-message-handler/handler.go:28`.

### Recommendations
- `medium` — Replace `handler.go:200` with `fmt.Errorf("publish canonical message: %w", err)`; the boundary already collapses it to `internal`.
- `medium` — Bound `createdAt` in `parseHeaderIDs` (e.g. reject > ±24h from `time.Now()`), returning `BotInvalidHeader`.
- `low` — Assert `sub.SiteID == h.siteID` in both handlers, or delete the unused fields and their projections.
- `nitpick` — Move `publishTimeout` into `config` with an `envDefault:"2s"`.

---

---

## 3. Architecture — 4 / 5

Textbook CLAUDE.md layout — consumer-defined store, constructor DI, `pkg/subject` builders, opt-in bootstrap, correct shutdown order — but the mandated `deploy/azure-pipelines.yml` is missing and the Mongo hot path has no breaker.

### Findings
- `high` — no `deploy/azure-pipelines.yml`; the directory holds only `Dockerfile` and `docker-compose.yml` — `bot-message-handler/deploy/`. CLAUDE.md §5 "When Creating Services" requires it; 29 of 37 services have one, so this is a real gap, not a fleet-wide convention change.
- `medium` — `config` mounts `mongoutil.PoolConfig` (`main.go:36`) but no `mongoutil.BreakerConfig`, even though `main.go:29` explicitly positions this service as "same authz shape as message-gatekeeper, with no cache in front". `message-gatekeeper/main.go:57,149-153` mounts the breaker with per-collection instances; here a Mongo stall parks all 200 guarded slots for the full 10s `REQUEST_TIMEOUT`.
- `low` — `bootstrapStreams` sets only `Name + Subjects` and, when disabled, *verifies* the stream exists so a missing stream fails at boot rather than first publish — `bot-message-handler/bootstrap.go:24-38`. This exceeds the CLAUDE.md contract in a good way; noted as a pattern other services should copy.
- `low` — the service is req/reply only (no JetStream consumer), so the `MAX_WORKERS`/`Consume` pattern rules don't apply; admission is bounded by `natsrouter.GuardConfig` (`main.go:61-64`) — correctly validated before use.

### Recommendations
- `high` — Add `bot-message-handler/deploy/azure-pipelines.yml`, copying `bot`-adjacent shape from `botplatform-service/deploy/azure-pipelines.yml`.
- `medium` — Mount `Breaker mongoutil.BreakerConfig` with an env prefix and wrap the subscription/user lookups, mirroring `message-gatekeeper/main.go:149-169`.
- `low` — Add a Mongo readiness check alongside `natsutil.HealthCheck(nc)` at `main.go:104-106`; every request needs Mongo, but readiness only reflects NATS.

---

---

## 4. Test coverage — 1 / 5

Coverage is **40.9% (198 stmts)** — below the 60% critical threshold — and the gap is not vanity padding: an entire client-facing handler and the whole Mongo store are at 0%.

### Findings
- `critical` — 40.9% statement coverage, far under the CLAUDE.md §4 80% floor (per `coverage_by_service.txt`).
- `critical` — `handleSendDM` is **0.0% covered** — `bot-message-handler/handler.go:78`. Every DM-specific behaviour is untested: the `userID`-missing branch (`:80`), the `idgen.BuildDMRoomID` derivation (`:92`), and the DM-specific `Forbidden`/`BotNotARoomMember` reply (`:96`). `handler_test.go` exercises only `handleSendRoom` (13 of 13 tests).
- `high` — zero integration tests and no `TestMain`: no `integration_test.go` in the directory, so all of `store_mongo.go` is 0% (`newStoreMongo`, `FindSubscription`, `FindRoom`, `ListMemberIDs`, `FindUser` — `store_mongo.go:21,29,50,70,93`). CLAUDE.md §4 states store implementations are covered by testcontainer integration tests; 29 services have `integration_test.go`, this one does not. The `ErrNotFound` translation at `store_mongo.go:42,62,102` — the exact contract every handler branch keys on — is completely unverified.
- `medium` — `Register` is 0% (`handler.go:70`), so nothing asserts the two routes are bound to `subject.ServerBotMsgRoomSendPattern` / `ServerBotDMSendPattern`; a copy-paste swap of the two patterns would ship green.
- `medium` — hand-rolled `fakeStore` (`handler_test.go:26-47`) instead of the mandated `go.uber.org/mock` mock in `mock_store_test.go`; `store.go` carries no `//go:generate mockgen` directive. 25 services follow the mandated pattern.
- `low` — existing tests are otherwise good quality: table-driven with descriptive subtests (`handler_test.go:169,196`), independent state per test, publisher injected as an interface field, and a real security assertion that client-supplied mention fields are overwritten (`handler_test.go:280`).

### Recommendations
- `critical` — Add a `handleSendDM` test set mirroring the room suite: missing `userID` param, missing DM subscription → `forbidden/not_a_room_member`, DM room ID equals `idgen.BuildDMRoomID(bot, target)`, happy path publish subject/MsgID.
- `high` — Add `integration_test.go` (build tag `integration`, `func TestMain(m *testing.M) { testutil.RunTests(m) }`, `testutil.MongoDB(t, "botmsghandler")`) covering the four store methods, both hit and `ErrNotFound` paths, plus `ListMemberIDs` on an empty room.
- `medium` — Replace `fakeStore` with a mockgen mock: add `//go:generate mockgen` to `store.go` and regenerate into `mock_store_test.go`.
- `medium` — Test `Register` against a fake router to pin both subject patterns.
- `low` — Cover the `canonicalizeMentions` store-error branch (`handler.go:267,291`), currently the only untested error path in a 76.2%-covered function.

---

---

## 5. Maintainability — 4 / 5

At 1080 lines across 8 files with small, single-purpose functions it is easy to hold in the head; the one real smell is a 25-line message-construction block duplicated verbatim between the two handlers.

### Findings
- `medium` — `handleSendDM` (`handler.go:110-124`) and `handleSendRoom` (`handler.go:163-183`) build an identical 13-field `model.Message` and repeat the same identity/header/validate/canonicalize/publish sequence. A new `Message` field must be added in two places, and the `TShow && ThreadParentMessageID != ""` rule is written twice (`:118`, `:175`).
- `low` — asymmetric defence: `handleSendRoom` calls `verifyRoomExists` (`:150`) but `handleSendDM` does not, and neither rejects a self-DM (`ident.ID == targetUserID`) even though the contract names `cannot_dm_self` (`docs/client-api.md:8543`). The asymmetry is undocumented at the DM site.
- `low` — dead surface: `Subscription`/`Room` carry five fields no caller reads (`store.go:28-39`), and the Mongo projections fetch them (`store_mongo.go:39,59`).
- `nitpick` — the `roomID == ""` / `targetUserID == ""` guards (`handler.go:80,129`) are unreachable through the router: NATS subject tokens cannot be empty for a `{param}` match.
- `low` — comment discipline is good throughout: comments explain *why* (e.g. `handler.go:57`, `:126`, `:225`, `bootstrap.go:23`), not what.

### Recommendations
- `medium` — Extract `func (h *handler) buildAndPublish(c *natsrouter.Context, roomID string, ident *BotIdentity, messageID string, createdAt time.Time, req BotSendRoomRequest) (*BotSendResponse, error)` holding the validate → canonicalize → construct → publish tail; both handlers shrink to their auth-specific prologue.
- `low` — Add an explicit self-DM rejection in `handleSendDM` with `errcode.BadRequest(..., WithReason(...))`, or a one-line comment stating BP owns it.
- `low` — Delete the unused struct fields and narrow the projections to `_id`-only existence checks.

---

---

## 6. Integration — 3 / 5

Subjects, stream config and IDs all come from the shared packages and the contract is documented, but the canonical event's `Timestamp` violates the CLAUDE.md publish-site rule and an unbounded header value reaches a Cassandra partition key.

### Findings
- `medium` — `MessageEvent.Timestamp` is set from the BP-supplied `msg.CreatedAt`, not publish time — `bot-message-handler/handler.go:191`. CLAUDE.md §6 "Event Timestamps": the event timestamp is set at the publish site via `time.Now().UTC().UnixMilli()` and is "distinct from any domain-level timestamps in embedded structs (e.g. `Message.CreatedAt`)". `message-gatekeeper/handler.go:504` does it correctly with `now.UnixMilli()` (`now := time.Now().UTC()`, `:440`). Here the two are collapsed, so event-lag observability on BOT-MESSAGES-CANONICAL measures the bot's clock, not the broker's.
- `medium` — the unvalidated `createdAt` (`handler.go:237-242`) flows through the canonical event into `bot-message-worker`'s `s.bucket.Of(msg.CreatedAt)` (`bot-message-worker/store_cassandra.go:70,111`), so a bad header picks the partition. See D1.
- `low` — no OUTBOX/INBOX involvement: this service publishes only to `BOT-MESSAGES-CANONICAL-{siteID}` via `subject.BotCanonicalCreated` (`handler.go:199`), matching `stream.BotMessagesCanonical` (`pkg/stream/stream.go:94`). No `fmt.Sprintf` subject construction anywhere; `pkg/outbox` partition rules do not apply.
- `low` — IDs are correct per entity: DM rooms via `idgen.BuildDMRoomID` (`handler.go:92`, always exactly two participants), message IDs validated by `idgen.IsValidMessageID` (`handler.go:228`), never ad-hoc.
- `low` — the client contract *is* documented: subjects are `chat.server.bot.request.…`, not `chat.user.…`, so the §5 client-api rule does not bind, yet `docs/client-api.md:8451-8552` documents both endpoints and every reason code this handler emits (`content_invalid`, `mention_invalid`, `invalid_header`, `not_a_room_member`, `room_not_found`) — no drift found.
- `low` — JetStream dedup rides `PublishWithMsgID(..., msg.ID)` (`handler.go:44,199`), but the effective window is the ops-owned stream `Duplicates` setting, which no code or doc pins; `handler.go:225` acknowledges the Cassandra-PK fallback.

### Recommendations
- `medium` — Set `Timestamp: time.Now().UTC().UnixMilli()` in `publishCanonical` and leave `Message.CreatedAt` as the domain time.
- `medium` — Reject out-of-range `createdAt` at the header boundary before it reaches the bucket math.
- `low` — Record the assumed `Duplicates` window for `BOT-MESSAGES-CANONICAL` in a comment beside `handler.go:199` so ops and code agree.

---

---

## 7. Performance — 3 / 5

No goroutines, precise projections and bounded admission, but the mention path is a textbook N+1 sitting on an unbounded member scan, with no cache or breaker in front of Mongo.

### Findings
- `high` — N+1 on mentions: `canonicalizeMentions` fetches every member of the room (`handler.go:265`) and then issues one `FindUser` per mention inside the loop (`handler.go:284`). A 10-mention message costs 11 Mongo round trips on top of the 2 the handler already makes (`FindSubscription` `:142`, `FindRoom` `:150`), all inside the synchronous 10s request budget.
- `high` — `ListMemberIDs` has no `limit` and no projection-side filter — `store_mongo.go:70-90`. It streams every subscription document of the room into a slice just to answer "is this one user a member". In a 5000-member room that is 5000 docs decoded per mention-bearing send.
- `medium` — no Mongo circuit breaker and, by explicit design (`main.go:29`), no cache. With `MAX_CONCURRENCY=200` and `REQUEST_TIMEOUT=10s` (`main.go:37-38`), a Mongo brownout converts directly into 200 parked handlers and `replyBusy` for everyone else (`pkg/natsrouter/router.go:153`).
- `low` — no `jsretry` concerns: the service has no JetStream consumer, so no `Nak()`/`NakWithDelay(0)` risk exists; the single publish is bounded by a 2s context (`handler.go:197`).
- `low` — no goroutine leaks: the only concurrency is the router's, with `Shutdown` wired first in the 25s `shutdown.Wait` chain (`main.go:113-119`); no `time.Sleep` anywhere.
- `low` — projections are explicit and precise on all four reads (`store_mongo.go:39,59,73,97`) and `mongo.ErrNoDocuments` is handled at every site (`:42,62,102`); no `$lookup`.

### Recommendations
- `high` — Replace the member scan + per-mention lookup with two targeted queries: `FindMemberIDs(ctx, roomID, requestedIDs)` using `{"roomId": roomID, "u._id": {"$in": ids}}`, and a single `FindUsers(ctx, ids)` with `$in` — turning N+1 into 2 round trips independent of room size.
- `high` — Do **not** cap `ListMemberIDs` with a bare `options.Find().SetLimit(...)` as a stopgap: the caller uses the result to decide membership and to resolve mentions, so a silent truncation answers those questions wrongly — a member past the cap stops being mentionable, with no error. The targeted query above is the fix; there is no safe interim limit on a call whose correctness depends on completeness.
- `medium` — Mount `mongoutil.BreakerConfig` and wrap the subscription and user lookups so a Mongo stall fails fast instead of consuming the concurrency budget.
- `low` — Consider a short-TTL L2 for the `(roomID, botID)` subscription check, the single most repeated read on this path; if the "no cache" stance at `main.go:29` is deliberate, restate the reason there so the next reader does not re-litigate it.

---

## 8. Prioritized action list

| # | Sev | Action | Dim | Evidence | Why |
|---|-----|--------|-----|----------|-----|
| 1 | `critical` | **Test `handleSendDM`** | Test coverage | `handler.go:78`, **0.0% covered**; branches at `:80`, `:92`, `:96`; all 13 tests target `handleSendRoom` | An entire client-facing handler with **no test at all.** The DM room-ID derivation via `idgen.BuildDMRoomID` is a correctness-critical, deterministic-key path — get it wrong and two users get two different DM rooms — and nothing checks it. |
| 2 | `critical` | **Add `integration_test.go` covering the Mongo store** | Test coverage | no integration file; `store_mongo.go:21`, `:29`, `:50`, `:70`, `:93` all 0%; `ErrNotFound` translation at `:42`, `:62`, `:102` | 29 services have one; this one does not, and coverage is **40.9%**, under the critical line. The `ErrNotFound` translation is **the exact contract every handler branch keys on**, and it is completely unexercised. |
| 3 | `high` | **Fix the N+1 on mentions and bound `ListMemberIDs`** | Performance | fetch-all at `handler.go:265`, per-mention `FindUser` at `:284`; unbounded scan at `store_mongo.go:70-90` | A 10-mention message costs **11 Mongo round trips** on top of the handler's existing 2, inside a synchronous 10s budget. And `ListMemberIDs` streams **every subscription document of the room** — 5000 docs decoded per mention-bearing send in a large room — to answer a single-membership question. Batch the user lookups, and answer membership with a targeted query. |
| 4 | `high` | **Add `deploy/azure-pipelines.yml`** | Architecture | `bot-message-handler/deploy/` holds only `Dockerfile` and `docker-compose.yml` | CLAUDE.md §5 requires it, and **29 of 37 services have one** — so this is a real gap, not a fleet-wide convention change. Without it the service has no CI gate at all, which is also why items 1 and 2 went unnoticed. |
| 5 | `medium` | **Mount `mongoutil.BreakerConfig`** | Architecture / Perf | `main.go:36` mounts `PoolConfig` only; `main.go:29` positions the service as "same authz shape as message-gatekeeper, with **no cache in front**"; `message-gatekeeper/main.go:57`, `:149-153` mounts per-collection breakers | The no-cache decision is deliberate and documented — but it was made without the breaker that makes it survivable. With `MAX_CONCURRENCY=200` and `REQUEST_TIMEOUT=10s`, **a Mongo brownout parks all 200 guarded slots** and everyone else gets `replyBusy`. |
| 6 | `medium` | **Validate `X-Bot-Created-At`** | Quality / Integration | `parseHeaderIDs` accepts any `int64` at `handler.go:237-242`; flows to `bot-message-worker/store_cassandra.go:70`, `:111` | The value becomes `Message.CreatedAt` and is **the Cassandra partition and clustering input downstream**, so a negative or year-3000 header **writes a message into an unreachable bucket** — invisible to the sender, unreadable afterwards. A range check at the boundary is the whole fix. |
| 7 | `medium` | **Set the canonical event's `Timestamp` at the publish site** | Integration | `handler.go:191` uses the BP-supplied `msg.CreatedAt`; the correct form at `message-gatekeeper/handler.go:440`, `:504` | CLAUDE.md §6 is explicit that the event timestamp is publish-time and **"distinct from any domain-level timestamps in embedded structs"**. Collapsing the two destroys event-lag observability — and, given item 6, lets a client-supplied value masquerade as a server timestamp. |
| 8 | `medium` | **Return raw wrapped errors for infra failures** | Code quality | `errcode.Internal("publish canonical", errcode.WithCause(err))` at `handler.go:200` | CLAUDE.md Tier 1 is explicit: an infra failure returns `fmt.Errorf("…: %w", err)` and **must not be dressed up as an errcode** — it collapses to `internal` at the boundary anyway. **Every other error site in the file gets this right** (`:65`, `:99`, `:147`, `:267`, `:291`), so this is a one-line outlier. |
| 9 | `medium` | **Switch to a generated mock and add the `//go:generate` directive** | Test coverage | hand-rolled `fakeStore` at `handler_test.go:26-47`; no directive in `store.go`; 25 services follow the mandated pattern | CLAUDE.md §4 mandates `go.uber.org/mock` into `mock_store_test.go`. The hand-rolled fake is also why the store interface can drift from what the tests believe it is. |
| 10 | `medium` | **Extract the duplicated message construction** | Maintainability | identical 13-field `model.Message` at `handler.go:110-124` and `:163-183`; the `TShow && ThreadParentMessageID != ""` rule written twice at `:118`, `:175` | A new `Message` field must be added in two places, and the thread-show rule already exists twice. Fixing this before item 1 makes the DM tests cheaper to write. |

**Also worth doing.** Add a test for `Register` (`handler.go:70`) asserting the two routes bind to `subject.ServerBotMsgRoomSendPattern` and `ServerBotDMSendPattern` respectively — today a swap of the two would compile, pass, and mis-route every bot message. And note the interaction between items 3 and 5: the N+1 is survivable today only because bot traffic is low; adding the breaker without fixing the N+1 converts a Mongo slowdown into a bot outage, and fixing the N+1 without the breaker leaves the 200-slot parking hazard in place. They are one change.
