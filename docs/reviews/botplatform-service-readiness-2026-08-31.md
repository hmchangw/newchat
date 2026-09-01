# botplatform-service — Production Readiness Review

**Service:** `botplatform-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 2.8 / 5

Genuinely well-written Go with narrow consumer-side interfaces, precise Mongo projections, a shared breaker, an L2 session tier, no goroutine leaks and no N+1 — and comments that say WHY. But it is also the service where the most CLAUDE.md rules are breached at once, and where three unbounded-work paths meet on an unauthenticated endpoint.

**The bot rate limiter runs *after* the auth middleware**, so an invalid token is never rate-limited — and because the session cache is positive-only by design, every bogus-token request is one uncapped MongoDB `FindOne`. Token spraying is an unmetered load generator against the same Mongo the breaker exists to protect. Alongside it, **`/api/v1/login` is unauthenticated, has no rate limiter, and runs a full bcrypt verify** (~50–100 ms CPU) per request, and **the idempotency middleware buffers the entire request body with an uncapped `io.ReadAll`.**

Structurally: three of the handler's five dependencies are **poked in after construction**, so every bot route nil-derefs if a wiring line is dropped; `BcryptCost` is parsed and range-validated but **never used by anything**; and the 15 s room-management budget is **unreachable** — the request deadline is 10 s and cuts it first. Coverage is 56.5%, below the critical line, with both federation forwarders and the cross-site routing decision at zero.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 3 / 5 |
| 2 | Architecture | 3 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 14 | 18 | 20 | 8 | **61** |

---

## 2. Go code quality — 3 / 5

Genuinely well-written Go — narrow consumer-side interfaces, near-universal error wrapping, and a clean secret-handling record — but six distinct CLAUDE.md Section 3 rules are breached, including string-matching on error text and log-AND-return.

### Findings
- `high` — `bindStrict` classifies a decode failure by **matching error text**: `msg := err.Error()` then `strings.Contains(msg, "unknown field")` — `botplatform-service/bot_handlers.go:210-212`
  Direct breach of "Never compare errors by string." A stdlib message tweak silently reclassifies every unknown-field 400 as a generic `bot_content_invalid`, changing the wire `reason` clients branch on. `encoding/json` gives no typed error here, so the fix is to detect the unknown field before decode (decode to `map[string]json.RawMessage` and diff keys) or to accept the coupling explicitly with a test pinning the literal.
- `medium` — the idempotency middleware reads the request body with **no size cap**: `io.ReadAll(c.Request.Body)` — `botplatform-service/middleware_idempotency.go:60`
  It runs *before* the handler (`routes.go:39-47`), so the deliberate `botRequestBodyMaxBytes` cap in `bot_handlers.go:190-193` never applies on any of the five bot routes. Every request is fully buffered and SHA-256'd first. Wrap in `http.MaxBytesReader(c.Writer, c.Request.Body, botRequestBodyMaxBytes)` and map the overflow to the same 400.
- `medium` — log-AND-return: `denied` calls `slog.WarnContext` then `errhttp.Write` on the same error — `botplatform-service/handler.go:180-181`
  `errhttp.Write` → `errcode.Classify` already logs once (`pkg/errcode/errhttp/errhttp.go:13-16`), so every failed bot login emits two lines. Move `reason` into `errcode.WithLogValues(ctx, "reason", reason)` and drop the `slog` call.
- `medium` — dependencies injected by post-construction field poke, not by constructor: `h.subs`, `h.forwarder`, `h.dmEnsurer` — `botplatform-service/main.go:106,114,116`
  `newHandler` (`handler.go:46`) returns a handler that is *not usable*; three of five deps are nil until later statements run. Nothing compiles-checks the wiring, and a reordering ships a nil-panic on the first bot request. Widen `newHandler` to take all five.
- `medium` — Tier-1 drift: infra failures are dressed up as `errcode.Internal(..., WithCause(err))` at ~12 sites instead of raw `fmt.Errorf` — `botplatform-service/middleware.go:70,128,136,151`, `middleware_idempotency.go:55,62,74`, `bot_forwarder.go:84,141,153`, `dm_ensurer.go:63,72`, `bot_handlers.go:48`
  CLAUDE.md: "a raw wrapped error collapses to `internal` at the boundary; do NOT dress it up as an errcode." The same file knows the rule — `bot_handlers.go:185` and `:83` use raw `fmt.Errorf` correctly — so the codebase is internally inconsistent.
- `medium` — bare `err` returned from a function that did more than delegate — `botplatform-service/store_mongo.go:99`
  `DeleteSessionsBeyondCap` loses "which store operation" on the way up. (`InsertSession`:81 / `FindSessionByHash`:85 are pure pass-throughs; defensible, but the same rule reads on them.)
- `low` — sentinel compared with `!=` rather than `errors.Is`: `if err := dec.Decode(&struct{}{}); err != io.EOF` — `botplatform-service/bot_handlers.go:219`
- `low` — exported identifiers in `package main` that nothing can import: `BotplatformStore` (`store.go:14`), `BotSub` (`subscription_store.go:21`), `HandleLogin`/`HandleValidate`/`HandleHealth` (`handler.go:55,89,193`)
- `low` — dead config: `DevMode` is declared and never read anywhere in the service — `botplatform-service/config.go:57`
- `low` — SAST audit-coverage gap (environmental, per GLOBAL_PREP): gosec + repo-owned semgrep are clean repo-wide; `govulncheck` and the semgrep registry packs could not run (blocked egress). No dependency-CVE signal for this service.
- `nitpick` — `context.Context` is the second parameter, not the first: `func (h *handler) denied(c *gin.Context, ctx context.Context, ...)` — `botplatform-service/handler.go:178`
- `nitpick` — misleading variable name: `var unknownErr *json.SyntaxError` holds a *syntax* error, and the unknown-field case is handled ten lines below — `botplatform-service/bot_handlers.go:205`
- `nitpick` — typo in a doc comment: "Reuses model.model.ErrSubscriptionNotFound" — `botplatform-service/subscription_store.go:17`

**Credential handling — verified clean.** Every `slog` call site in the service was read (`handler.go:57,154,159,180`, `main.go:29,72,77,87,93,95,138,147,150`, `middleware.go:27`, `middleware_idempotency.go:91`). No password, `x-auth-token`, raw session token, or request body is logged; the login path logs only `account` + `userId` (`handler.go:159`), the access log logs `bot_account` (`middleware.go:36`), and the idempotency warning logs a SHA-256 `opID`, not the body (`middleware_idempotency.go:91-92`). No `fmt.Println`/`log.Println` anywhere; no token is ever placed in an `errcode` cause.

### Recommendations
- `high` — Replace the `strings.Contains(err.Error(), "unknown field")` classification in `bot_handlers.go:211` with a structural check, and add a test asserting the `bot_unknown_field` reason so a stdlib message change fails CI instead of silently degrading.
- `medium` — Apply `http.MaxBytesReader(c.Writer, c.Request.Body, botRequestBodyMaxBytes)` in `middleware_idempotency.go:60` so the documented cap holds at the first reader, and keep `bindStrict`'s cap as defence-in-depth.
- `medium` — Delete the `slog.WarnContext` in `handler.go:180`; carry `reason` via `errcode.WithLogValues` so `Classify` emits the single line.
- `medium` — Change `newHandler` to accept `subs`, `forwarder`, and `dmEnsurer`, removing the three field assignments in `main.go:106,114,116`.
- `medium` — Convert the ~12 `errcode.Internal(..., WithCause(err))` infra sites to raw `fmt.Errorf("<what this function was doing>: %w", err)`, matching the correct usage already present at `bot_handlers.go:83,185`.
- `low` — Wrap `store_mongo.go:99` as `fmt.Errorf("delete sessions beyond cap: %w", err)`; switch `bot_handlers.go:219` to `!errors.Is(err, io.EOF)`.
- `low` — Unexport `BotplatformStore`→`botplatformStore`, `BotSub`→`botSub`, and the three `Handle*` methods; drop the unused `DevMode` field.

---

## 3. Architecture — 3 / 5

Clean boundaries, narrow consumer-defined interfaces and correct `pkg/subject`/`pkg/shutdown` usage, undercut by an explicit shared-knob violation, post-construction dependency injection, and a timeout budget that config makes unreachable.

### Findings
- `high` — `SESSIONS_MAX_PER_ACCOUNT` and `BCRYPT_COST` are re-declared with their own `env` tag + `envDefault` in this service *and* in admin-service, while both operate on the same `sessions` collection and the same `pkg/pwhash` recipe — `botplatform-service/config.go:34`, `botplatform-service/config.go:37`, `admin-service/config.go:38`, `admin-service/config.go:44`. CLAUDE.md §6 Configuration: a knob shared by more than one service is declared **once**, in the package that owns the thing it configures, and mounted as a named field (the `mongoutil.PoolConfig` / `sessioncache.TTLConfig` shape this service already follows at `config.go:28`). The two services drifting on the FIFO cap means one silently evicts sessions the other still considers valid; drifting on cost means hashes written by admin-service verify at a cost this service never agreed to.
- `high` — `BcryptCost` is parsed and range-validated but never used: nothing in the service hashes a password — `verifyPassword` calls `pwhash.Verify`, which takes no cost — `botplatform-service/main.go:44`, `botplatform-service/handler.go:227`. A validated-but-unread knob is worse than an absent one: it fails startup on an operator value that has no effect, and it is the *duplicate* half of the shared knob above.
- `high` — Three of the handler's five dependencies are assigned by field mutation after construction, not through the constructor — `botplatform-service/main.go:107` (`h.subs`), `:114` (`h.forwarder`), `:116` (`h.dmEnsurer`); `newHandler` takes only `store` and `cfg` (`botplatform-service/handler.go:46`). CLAUDE.md §3: "Handler structs hold dependencies injected via constructor." Every bot route nil-derefs if a wiring line is dropped, with no compile-time or startup check — and `handler_test.go:65` / `integration_test.go:55` already construct partially-wired handlers, so the incomplete object is a normal, exercised state.
- `medium` — `forwardRoomMgmt` hardcodes `15*time.Second` and ignores the `timeout` field the constructor set on the same struct — `botplatform-service/bot_forwarder.go:75` vs `:34`, `:132`. One forwarder now has two timeout sources, one configurable-by-construction and one a literal.
- `medium` — The 15s room-mgmt and DM-ensure budgets are unreachable under default config: `ginutil.Timeout` derives the deadline on `c.Request.Context()` (`pkg/ginutil/timeout.go:20`) with `REQUEST_TIMEOUT` defaulting to 10s (`pkg/ginutil/timeoutconfig.go:18`), and both RPCs derive `reqCtx` from that same request context (`bot_forwarder.go:75`, `dm_ensurer.go:54`). The request is cancelled at 10s and mapped to 503 `handler_timeout` before the stated budget elapses. `deploy/docker-compose.yml` sets no `REQUEST_TIMEOUT`, so dev runs on the 10s default. The `BOT_IDEMPOTENCY_ROOM_MGMT_TTL` comment ("exceeds the 15s NATS timeout", `config.go:54`) is reasoning from a number the config cannot deliver.
- `medium` — A second store interface, `subscriptionStore`, lives in `subscription_store.go:28` rather than `store.go`, and the sole `//go:generate mockgen -source=store.go` directive (`store.go:10`) therefore does not cover it — tests hand-roll `fakeSubStore` (`subscription_store_test.go:15`). CLAUDE.md §3: "Each service defines its own store interface in `store.go`." Two store surfaces, one mocked mechanically and one by hand, is exactly the split that lets a signature change land untested.
- `low` — `DevMode` is declared (`config.go:57`) and wired in compose (`deploy/docker-compose.yml:29`) but read nowhere in the service. Dead config that reads to an operator as a live switch.
- `low` — `session.NewMongoStore(db)` is constructed twice for the same database — once in `main.go:75` for `EnsureIndexes`, again inside `newStoreMongo` (`store_mongo.go:33`). The store's own index bootstrap leaks into `main`.
- `nitpick` — `BotplatformStore` (`store.go:14`) and `BotSub` (`subscription_store.go:21`) are exported from `package main`, where nothing outside can consume them; CLAUDE.md §3 says export only what other packages consume.

Correctly clean: no `os.Getenv`, no raw `fmt.Sprintf` subject building (all via `pkg/subject/bot.go`), no JetStream so `BOOTSTRAP_STREAMS` is legitimately absent, shutdown via `pkg/shutdown.Wait` with the documented HTTP order (`main.go:145-157`), and the breaker/cache layering in `newStoreMongo` is well-reasoned.

### Recommendations
- `high` — Move `SessionsMaxPerAccount` into a `session.CapConfig` (or equivalent) in `pkg/session`, and `BcryptCost` into a `pwhash.CostConfig` in `pkg/pwhash`; mount both as named fields in admin-service and botplatform-service. Drop `BcryptCost` from botplatform-service entirely — it verifies, it never hashes.
- `high` — Change `newHandler` to take `store, cfg, forwarder, subs, dmEnsurer` and return an error (or panic) on any nil, deleting the three field assignments in `main.go`.
- `medium` — Promote the two RPC budgets to config (`BOT_MSG_RPC_TIMEOUT`, `BOT_ROOM_MGMT_RPC_TIMEOUT`), give `botForwarder` a second `roomMgmtTimeout` field instead of the literal, and validate at startup that `REQUEST_TIMEOUT` exceeds the larger of them — the same fail-fast admin-service applies to `RoomRPCTimeout` vs `WriteTimeout`.
- `medium` — Move `subscriptionStore` and `BotSub` into `store.go` under the existing mockgen directive; delete `fakeSubStore`.
- `low` — Delete `DevMode` and its compose entry, or wire it; collapse the duplicate `session.NewMongoStore` by exposing `EnsureIndexes` on `storeMongo`.
- `low` — Unexport `BotplatformStore` → `botplatformStore` and `BotSub` → `botSub`.

---

## 4. Test coverage — 1 / 5

At **56.5% (545 stmts)** botplatform-service is below CLAUDE.md Section 4's 60% floor, and the shortfall is concentrated exactly where it hurts — the bot auth/authorization error branches, the cross-site routing decision, and both NATS federation forwarders are 0% or asserted-not-at-all.

### Findings
- `critical` — Coverage 56.5% (545 stmts), under the 60% floor; 80% is the merge requirement — `/home/user/newchat/botplatform-service/` (per `coverage_by_service.txt`)
- `high` — `dm_ensurer.go` is 0% end-to-end (`newNATSDMEnsurer`, `Ensure`) — `botplatform-service/dm_ensurer.go:31-74`
  The first-time-DM federation fallback: missing-session 500, `marshal identity`, timeout→`Unavailable`, and the `errcode.Parse` decode of a remote error reply are never executed. `bot_forwarder_test.go:20` already has a `fakeRequester` seam that makes this directly testable — this is an omission, not a hard case.
- `high` — All four room-management forwarders are 0% — `botplatform-service/bot_forwarder.go:57,94,98,102`
  `forwardRoomMgmt`'s 15s-timeout branch, `BotHandlerTimeout` mapping, and pass-through of bot-room-service's error envelope (`errcode.Parse`, line 86) are untested, while the structurally identical `forward` sits at 88.9%. Nothing verifies `createRoom`/`addMembers`/`removeMembers` build the right `subject.ServerBotRoom*` subjects.
- `high` — `requireBot`'s store-failure branch is uncovered — `botplatform-service/middleware.go:69-72`
  The bot-auth table at `middleware_bot_test.go:42-110` covers missing headers, unknown hash, userID mismatch, non-bot role and success, but has no `findByHashFn` returning a generic error, so the `errcode.Internal("find session")` path — the one that must NOT leak a 401-vs-500 distinction wrongly — is unverified.
- `high` — Cross-site routing, the service's reason to exist, is asserted nowhere — `botplatform-service/routes_bot_test.go:25-45`
  `alwaysLocalSub` always returns `SiteID: "site-a"` and `successForwarder`'s methods discard the `siteID` argument entirely, so no test proves `sub.SiteID` from `FindForBot`/`FindDMForBot` reaches the forwarder. The statement coverage on `botSendRoomMessage` (70%) / `handleMembersBatch` (73%) is vanity for the routing decision.
- `high` — `mongoSubscriptionStore` is 0% with no integration test at all — `botplatform-service/subscription_store.go:40-76`
  `integration_test.go` covers only login/validate/sessions/indexes. The `mongo.ErrNoDocuments → model.ErrSubscriptionNotFound` mapping at line 70 is what decides 403-not-a-member vs 500, and the `BuildDMRoomID` filter's agreement with bot-room-service is untested.
- `medium` — `notMemberOrInternal` is 0% — `botplatform-service/bot_handlers.go:180`
  The authorization verdict (403 `bot_not_a_room_member` vs infra 500) for every room-scoped bot endpoint has no test; `bot_handlers_test.go` mounts handlers with `h.subs` nil and only reaches pre-routing validation.
- `medium` — Login credential-path branches uncovered: malformed JSON body (`handler.go:93`), `tokenGen` failure (`handler.go:132`), best-effort `DeleteSessionsBeyondCap` failure (`handler.go:152`) — `botplatform-service/handler.go`
  The eviction branch in particular must not fail the login; nothing pins that.
- `medium` — Rate-limit failure branches uncovered: missing principal → 500 (`middleware.go:127`), global-counter `IncrEx` error (`middleware.go:150`), and `botPrincipalFrom`'s not-present branch (`middleware.go:96`) — `botplatform-service/middleware.go`
- `low` — Idempotency guard rails uncovered: missing principal (`middleware_idempotency.go:54`), body-read error (`:61`), sentinel `Del` failure (`:89`) — `botplatform-service/middleware_idempotency.go`
- `low` — `accessLogMiddleware` 0%; no test asserts `request_id`/`bot_account`/`login_outcome` are emitted — `botplatform-service/middleware.go:23`
- `low` — `store_mongo.go`'s 0% is partly an artifact: it is exercised only by `//go:build integration` tests (`integration_test.go:53`), which the profile excludes. Don't double-count it; `subscription_store.go` has no such excuse.
- `nitpick` — Structure is otherwise compliant and good: per-test `gomock.NewController` (`handler_test.go:63`), no shared mutable state, injected `tokenGen`/`now` seams, no real NATS/Mongo in unit tests, and `TestMain` uses `testutil.RunTestsWithPrewarm(m, testutil.EnsureMongo)` (`integration_test.go:27-29`).

### Recommendations
- `critical` — Add a `dm_ensurer_test.go` table over the existing `fakeRequester`: nil session, marshal path, `nats.ErrTimeout`→`Unavailable`/`BotHandlerTimeout`, `errcode.Parse` remote-error pass-through, happy path asserting the `subject.ServerBotDMEnsure` subject and `HeaderBotIdentity`. Highest coverage-per-line in the service.
- `high` — Mirror `bot_forwarder_test.go`'s existing cases onto `forwardRoomMgmt`: assert each of `createRoom`/`addMembers`/`removeMembers` builds its exact subject, sets the identity header, omits `X-Bot-Message-ID`, and maps timeout and errcode replies.
- `high` — Extend the `requireBot` table with one `findByHashFn` returning a generic store error; assert 500 and that no `botPrincipal` is set.
- `high` — Make routing assertable: have the fake forwarder record `siteID`, and add cases where `FindForBot`/`FindDMForBot` return `SiteID: "site-b"` (remote), `model.ErrSubscriptionNotFound` (→403 for room, →`Ensure` then local site for DM), and a generic error (→500).
- `high` — Add integration tests for `mongoSubscriptionStore` using `testutil.MongoDB(t, …)`: hit, miss→`ErrSubscriptionNotFound`, DM lookup via `BuildDMRoomID`, and projection-only fields.
- `medium` — Fill the login/rate-limit/idempotency error branches listed above; each is a two-line table case against mocks already in place.

---

## 5. Maintainability — 4 / 5

Small, well-organized, unusually well-commented service (comments genuinely say WHY), let down by a staged handler assembly, a duplicated-and-diverged forwarder, and two config knobs that do nothing.

### Findings
- `medium` — `handler` is assembled in three stages: `newHandler(st, &cfg)` sets only store/cfg, then `h.subs`, `h.forwarder`, `h.dmEnsurer` are poked in afterwards — `/home/user/newchat/botplatform-service/main.go:105`, `:114`, `:116`. Nothing (compiler or constructor) forces those three to be set, and every bot handler dereferences them unguarded (`bot_handlers.go:41`, `:51`, `:78`). Adding a sixth dependency means remembering a fourth assignment site; a missed one is a nil-panic on the first bot request, not a startup failure. CLAUDE.md ("Handler structs hold dependencies injected via constructor") wants all five in `newHandler`.

- `medium` — `forwardRoomMgmt` hardcodes `15*time.Second` and ignores the `f.timeout` field the constructor was given — `/home/user/newchat/botplatform-service/bot_forwarder.go:75` vs the field set at `:35`. So `newBotForwarder(nc, 3*time.Second)` (`main.go:114`) silently configures only half the forwarder, and the room-mgmt budget is unreachable from config while its twin — the DM-ensure 15s — *is* a constructor argument (`main.go:116`). Two identical magic numbers, one injectable, one not.

- `medium` — `forward` (`bot_forwarder.go:106-156`) and `forwardRoomMgmt` (`:57-92`) are ~85% the same code: nil-session guard, `BotIdentity` marshal, `natsutil.NewMsg` + nil-header dance, header set, `WithTimeout`, the identical timeout/`nats.ErrTimeout` → `errcode.Unavailable` branch, and the identical `errcode.Parse` reconstruction. They differ only in three headers, the timeout, one error string, and whether the reply is decoded. `dm_ensurer.go:35-74` is a *third* copy of the same skeleton. Any change to bot RPC framing (a new header, a trace attribute, a retry) must be made in three places.

- `low` — `BCRYPT_COST` is validated at startup (`main.go:44`) and never read again; `verifyPassword` delegates to `pwhash.Verify(stored, plaintext)` with no cost parameter (`handler.go:227`). The knob is documented in `config.go:36` as load-bearing but is inert — an operator tuning it gets nothing.

- `low` — `DevMode` (`config.go:57`) has exactly one reference in the whole service: its own declaration. Dead config.

- `low` — the generic access-log middleware hardcodes three handler-specific keys — `login_outcome`, `validate_outcome`, `bot_account` (`middleware.go:34-36`) — always emitted, usually empty. Every new endpoint with an outcome dimension edits shared middleware; the list only grows.

- `low` — request bodies are read and re-encoded three times per bot call: `botIdempotency` `io.ReadAll` + restore (`middleware_idempotency.go:60-67`), `bindStrict` `io.ReadAll` + decode (`bot_handlers.go:193`), then each handler re-marshals the decoded struct back to bytes (`bot_handlers.go:46`, `:87`, `:112`, `:160`) — four copies of the same five-line `json.Marshal` + `errcode.Internal("re-marshal bot request", …)` block. `bindStrict` already holds the validated raw bytes and could return them.

- `low` — unknown-field detection is a substring match on `encoding/json`'s error text: `strings.Contains(msg, "unknown field")` (`bot_handlers.go:211`). Brittle against a stdlib message change, and it silently downgrades to `BotContentInvalid` rather than `BotUnknownField` if it ever shifts. (CLAUDE.md's "never compare errors by string" is aimed at `errors.Is`, but this is the same fragility.)

- `nitpick` — the variable holding a `*json.SyntaxError` is named `unknownErr` (`bot_handlers.go:205`), which reads as the unknown-field case it is not.

- `nitpick` — stale comment: `// ValkeyAddrs seeds …` describes a field named `Valkey` (`config.go:42`); and `// Reuses model.model.ErrSubscriptionNotFound` (`subscription_store.go:17`) has a duplicated package qualifier.

No file exceeds 260 production lines, no function exceeds ~50, and the file layout matches CLAUDE.md's per-service organization exactly — size and complexity are not a problem here.

### Recommendations
- `medium` — Make `newHandler(store, cfg, subs, forwarder, dmEnsurer)` take all five dependencies and delete the three post-construction assignments in `main.go:105-116`; move the NATS connect above handler construction to allow it.
- `medium` — Extract the shared RPC skeleton into one unexported `botForwarder.request(ctx, sess, subj, timeout, headers, body) ([]byte, error)` covering nil-session, identity header, timeout classification and `errcode.Parse`; have `forward`, `forwardRoomMgmt` and `natsDMEnsurer.Ensure` call it. Give `botForwarder` a second `roomMgmtTimeout` field fed from config so `15s` stops being a literal in two files.
- `low` — Either wire `BcryptCost` into the hash path or delete it plus its startup validation (`main.go:44`, `config.go:37`); same for `DevMode`.
- `low` — Have `bindStrict` return the validated `[]byte` alongside the decoded struct and forward those bytes, removing all four re-marshal blocks.
- `low` — Replace the three named outcome keys in `accessLogMiddleware` with a single `c.Set("outcome", …)` (plus `bot_account` kept as identity), so new endpoints don't edit the middleware.
- `nitpick` — Fix the `unknownErr` name and the two stale comments; add a fixture-backed test around `bindStrict`'s unknown-field branch so a stdlib text change fails a test rather than degrading an error code in production.

---

## 6. Integration — 3 / 5

Subject, ID and downstream type contracts are clean and builder-driven, but the documented HTTP contract has drifted from the registered routes, a derived view omits the bot surface entirely, and the `BotIdentity` enrichment contract is dead at its only producer.

**`docs/client-api.md` obligation:** CLAUDE.md's literal trigger is a handler on `chat.user.{account}.…` or an `auth-service` HTTP route. `botplatform-service` has neither — its outbound subjects are `chat.server.bot.request.…` (`pkg/subject/bot.go:8-41`) and its routes are its own (`routes.go:10-73`). So the rule does **not** bind by its literal text. It binds in practice anyway: §10 of `docs/client-api.md` already adopts BP's entire HTTP surface as client API, and that adopted contract is currently wrong (finding 1).

### Findings
- `high` — Documented member endpoints do not exist. Code registers `POST /api/v1/rooms/:roomID/members/add` and `POST /api/v1/rooms/:roomID/members/remove` — `botplatform-service/routes.go:69-73` — while `docs/client-api.md:8584-8585` (and the routing table at `:8466-8467`) publish `POST /api/v1/rooms/:roomID/members` and `DELETE /api/v1/rooms/:roomID/members`. Any SDK written against the doc gets a 404 on both add and remove.
- `high` — Derived view drift: `docs/client-api/request-reply.md:256-261` lists only `/api/v1/login` and `/api/v1/auth/validate` for botplatform and states `**Emits:** None — HTTP-only.`, silently dropping the five bot endpoints that canonical §10.3–10.7 documents. CLAUDE.md requires the derived views never drift from the canonical file.
- `medium` — The `BotIdentity` enrichment contract is unfillable at its only producer. BP constructs the header with `{ID, Account, SiteID}` only — `botplatform-service/bot_forwarder.go:62`, `:114-118`, `dm_ensurer.go:43` — but `model.BotIdentity` carries `EngName/ChineseName/AppID/AppName` (`pkg/model/bot.go:23-31`) and downstream *reads* them: `bot-room-service/handler.go:199-200` and `:281` persist the room owner's `appId`/`appName`, and `bot-message-handler/handler.go:113,168` derives `UserDisplayName`. `session.Session` (`pkg/session/session.go:23-29`) holds none of these, so every bot room is stored with an empty owner app identity and every bot message's display name falls back to the raw account.
- `medium` — `docs/client-api.md:8542` advertises `503 unavailable / dm_ensure_unavailable`, but no such `Reason` exists in `pkg/errcode/codes_botplatform.go` and no path emits it; BP's DM-ensure failure surfaces as `handler_timeout` (`dm_ensurer.go:59-61`) or `internal` (`:63`).
- `low` — Retry semantics leak a duplicate message. The messageID that downstream uses verbatim is minted per HTTP attempt inside the forwarder (`bot_forwarder.go:111`), and the idempotency sentinel is a 30s time-bucketed hash of the body (`middleware_idempotency.go:100-113`). A client retrying a `503 handler_timeout` after the window creates a second distinct canonical message; the API accepts no client-supplied message/op ID.
- `low` — Room-management timeout is a magic literal: `forwardRoomMgmt` hardcodes `15*time.Second` (`bot_forwarder.go:75`) rather than a field, ignoring the injected `f.timeout`, and duplicates the 15s the DM-ensurer is wired with at `main.go:116`. Neither is configurable, and the two can drift.
- `low` — The routing projection re-declares the subscription schema by hand (`subscription_store.go:59-63`) instead of reusing `model.Subscription`'s tags (`pkg/model/subscription.go:78-83`), and the integration suite covers only login/session (`integration_test.go:74-190`) — a `roomId`/`siteId`/`roomType` rename would silently route every bot RPC to the empty string.

**Verified clean:** every outbound subject goes through `pkg/subject` builders, no raw `fmt.Sprintf` in the service (`bot_forwarder.go:45,51,95,99,103`, `dm_ensurer.go:48`); BP's publish subjects pair exactly with the `natsrouter` patterns registered by `bot-message-handler/handler.go:71-74` and `bot-room-service/handler.go:88-97`; request/response types are `pkg/model` aliases on both sides (`bot-message-handler/handler.go:23-24`); `idgen.GenerateMessageID` output is accepted by the downstream `idgen.IsValidMessageID` gate (`bot-message-handler/handler.go:228`) and `BuildDMRoomID` is used identically by BP (`subscription_store.go:53`) and the handler (`:92`). BP publishes no JetStream events, so the `Timestamp`-at-publish-site, OUTBOX partition, INBOX lane, `msgbucket` and `ROOM_KEY_RETIRED_TTL` rules have no surface here; correspondingly there is no `bootstrap.go`, which is correct.

### Recommendations
- `high` — Reconcile §10.7 and the §10.3 routing table with `routes.go`: either rename the routes to `POST`/`DELETE /api/v1/rooms/:roomID/members` or fix the doc to `.../members/add` and `.../members/remove`. Pick the code side only if no SDK has shipped against the doc.
- `high` — Add the five bot endpoints to `docs/client-api/request-reply.md` §"HTTP — Botplatform Service" and correct its `Emits: None` line.
- `medium` — Either populate `EngName/ChineseName/AppID/AppName` on `BotIdentity` (resolve the bot's user doc at login and cache it on the session, or have downstream look them up by `ident.ID`), or delete the four fields from `pkg/model/bot.go` and drop their downstream reads, so the contract stops promising data nobody sends.
- `medium` — Add `BotDMEnsureUnavailable Reason = "dm_ensure_unavailable"` and emit it from the DM-ensure failure path, or remove the row from `docs/client-api.md:8542`.
- `low` — Accept an optional client-supplied idempotency key (or message ID) on the two send endpoints and thread it into `X-Bot-Message-ID`, so a post-window retry converges instead of duplicating.
- `low` — Move the 15s room-management timeout into `config` and inject it into both `botForwarder` and `natsDMEnsurer` so the two cannot drift.
- `low` — Add one integration test that writes a `model.Subscription` and reads it back through `FindForBot`/`FindDMForBot`, pinning the projection's bson field names against the shared model.

---

## 7. Performance — 3 / 5

Hot paths are genuinely well-built — precise Mongo projections everywhere, a shared breaker, an L2 session tier, no goroutine leaks, no `time.Sleep`, no `$lookup`, no N+1 — but three unbounded-work paths (full-body buffering before any cap, unrate-limited bcrypt, uncached negative auth) and a timeout budget that the request deadline silently truncates keep it off a 4.

### Findings
- `high` — The idempotency middleware reads the entire request body with an **uncapped** `io.ReadAll(c.Request.Body)` and buffers it in RAM before the handler runs — `botplatform-service/middleware_idempotency.go:60`.
  It is wired ahead of the handler on all five bot routes (`routes.go:39-47`), so `bindStrict`'s `http.MaxBytesReader` cap (`bot_handlers.go:193`) is applied to an already-buffered `NopCloser` and can never reject during read. The comment at `bot_handlers.go:189` ("Body is capped so oversized requests fail during read") is false whenever Valkey is configured, i.e. in production. A single authenticated bot posting a multi-GB body OOMs the pod; the body is then also SHA-256'd (`:101`) and re-read into a second copy by `bindStrict`.

- `high` — `/api/v1/login` is unauthenticated, has **no rate limiter**, and performs a full bcrypt verify (default cost 10, ~50-100 ms CPU) per request — `handler.go:120`, `pwhash.Verify` at cost baked into the stored hash; the engine's global middleware chain (`main.go:120-125`) contains no limiter, and `botRateLimit` is attached only to bot routes (`routes.go:29,40-42`).
  A few dozen concurrent wrong-password requests against one known bot account saturate every CPU and starve `/auth/validate`, which every bot request depends on.

- `high` — `botRateLimit` runs **after** `requireBot`, so an invalid token is never rate-limited — `routes.go:40` (`out := []gin.HandlerFunc{auth}` before `rateLimit`). Combined with the session tier being positive-only by design (`pkg/sessioncache/sessioncache.go:11-16`, `loadEntry` returns `ErrNotFound` as an uncached miss at `:90-95`), every bogus-token request is one uncapped MongoDB `FindOne`. Token spraying is an unmetered load generator against the same Mongo the breaker is meant to protect.

- `medium` — The 15 s room-management budget is unreachable: `forwardRoomMgmt` derives `context.WithTimeout(ctx, 15*time.Second)` from the request context (`bot_forwarder.go:75`), and `dmEnsurer` likewise (`main.go:116`), but `cfg.HTTP.Middleware()` already stamped a 10 s deadline on that context (`main.go:124`; `ginutil.TimeoutConfig.RequestTimeout` `envDefault:"10s"`). Multi-member fan-out is cut at 10 s, and the idempotency TTLs sized against 15 s (`config.go:54-55`) are reasoning from a number that never applies.

- `medium` — `forwardRoomMgmt` hardcodes `15*time.Second` instead of using the forwarder's own `timeout` field — `bot_forwarder.go:75` vs `bot_forwarder.go:132`. The room-mgmt budget is the one that scales with batch size (up to 100 userIDs, `bot_handlers.go:26`) and is the only unconfigurable timeout in the service.

- `low` — Each bot request costs ~5 sequential round trips before any work: session L2 read, `IncrEx` per-caller, `IncrEx` global, `SetNX`, then `Del` after — `middleware.go:134`, `middleware.go:149`, `middleware_idempotency.go:72`, `:89`. The two counters are independent and could be pipelined into one round trip.

- `low` — `sessions` has no TTL/expiry index and `Session` carries no expiry field; rows are trimmed only by the per-account FIFO cap of 100 (`pkg/session/session.go:23-30`, `:208-230`, `handler.go:151`). The collection grows monotonically with account count and nothing ever ages an idle token out.

- `low` — `/healthz` issues a raw `client.Ping` outside the breaker — `store_mongo.go:106`. During a Mongo outage every probe pays the 2 s `MONGO_SERVER_SELECTION_TIMEOUT` and holds a pooled connection, exactly when the breaker is trying to stop paying that cost.

- `nitpick` — `DeleteBeyondCap` sorts `{issuedAt:-1, _id:-1}` but the index is `{account:1, issuedAt:1}` (`pkg/session/session.go:110`, `:219`), so the `_id` tie-break forces a blocking in-memory sort stage. Harmless at cap 100; would not be at a larger cap.

### Recommendations
- `high` — Wrap the idempotency read in `http.MaxBytesReader(c.Writer, c.Request.Body, botRequestBodyMaxBytes)` at `middleware_idempotency.go:60`, return the same 400 as `bindStrict`, and stash the buffered bytes in the gin context so `bindStrict` reuses them instead of re-reading.
- `high` — Put an IP/account-keyed limiter in front of `/api/v1/login` (the existing `botRateLimit` shape with a caller key of `req.Username`+`c.ClientIP()` works), so bcrypt cost is bounded per source.
- `high` — Move `rateLimit` ahead of `auth` in `routes.go:40` with an IP-derived key when no principal exists, so invalid tokens are throttled before they reach MongoDB.
- `medium` — Either raise `REQUEST_TIMEOUT` above the room-mgmt budget or derive the room-mgmt timeout from config and assert `RequestTimeout > roomMgmtTimeout` in `run()` alongside the other `Validate()` calls (`main.go:47-55`).
- `medium` — Give `botForwarder` a second `roomMgmtTimeout` field set from config and delete the literal at `bot_forwarder.go:75`.
- `low` — Pipeline the two `IncrEx` calls, and add a short-TTL negative cache for repeatedly-failing token hashes (keyed by hash, seconds not minutes) to cap the Mongo cost of token spraying without weakening the positive-only security posture.
- `low` — Add a TTL index on a new `expiresAt` field in `pkg/session`, or document why sessions are retained indefinitely.
