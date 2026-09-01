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
