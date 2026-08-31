# room-service — Production Readiness Review

**Service:** `room-service`
**Date:** 2026-08-31
**Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents (code quality, architecture, test coverage, maintainability, integration, performance), each judging against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

The most RPC-dense service in the fleet: 31 client-facing handlers across ~10 domains in a single 2,677-line `handler.go`. The engineering inside is good — error wrapping and `errcode` tiering are exemplary, the federation boundary holds cleanly through OUTBOX, all nine federated event types are correctly partitioned, and the Mongo layer shows deliberate performance work (precise projections, over-fetch+1 pagination, batched Go-side rollups replacing correlated `$lookup`s). What drags the score down is *shape and safety net*: a 47-method god store interface, a constructor that takes 13–14 positional args and is then finished by 11 post-construction field pokes, and **57.2% coverage** with `store_mongo.go` at 3.4%. The single most consequential defect is a **request-ID-derived OUTBOX dedup key that collides across the multi-row `moveChat` rebalance**, silently dropping all but one federated event.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 3 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 4 / 5 |

### Findings by severity

| Severity | Count |
|----------|-------|
| critical | 1 |
| high | 9 |
| medium | 21 |
| low | 14 |
| nitpick | 6 |
| **Total** | **51** |

> **Audit-coverage caveat.** `gosec` and the 18 repo-owned `semgrep` rules ran clean repo-wide. `govulncheck` and the `semgrep` registry packs could **not** run (egress policy blocks `vuln.go.dev` / `semgrep.dev`), so dependency-CVE coverage is unverified and must be re-run before shipping.

---

## 2. Go code quality — 4 / 5

Error handling, `errcode` tiering and wrapping discipline are exemplary across ~2,700 lines of handler and ~2,100 lines of store. The deductions are logging-context drift and a constructor that has outgrown positional DI.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | **17 `slog.*` sites inside request handlers pass no `context.Context` and no `request_id`**, so they emit with no `trace_id`/`span_id`/`request_id` | `handler.go:1185`, `:1427`, `:1516`, `:1529`, `:1542`, `:1548`, `:1554`, `:1729`, `:1871`, `:1905`, `:1916`, `:1921`, `:1927`, `:2243` |
| medium | Two `slog.Debug` calls are **dead code — they can never emit**. `logctx.Handler.Enabled` gates sub-INFO records on `honoredThreshold(ctx)`, which defaults to `LevelInfo` when the ctx carries no admission; `slog.Debug` uses `context.Background()`, so DEBUG is dropped even for a request the operator explicitly admitted via `DEBUG_LOG_*` | `handler.go:1246`, `:1981`; `pkg/logctx/handler.go:30-35`, `:60-65` |
| medium | `NewHandler` takes 14 positional parameters, then `main.go` mutates 11 more dependencies onto the struct after construction | `handler.go:102`; `main.go:373-383` |
| medium | `handler.go` is 2,677 lines / 104 KB in one file; `addMembers` 165 lines, `roomRestricted` 161, `messageRead` 135 | `handler.go:887`, `:2033`, `:1381` |
| low | Exported constructor returns an unexported type — `NewNATSMemberListClient` returns `*natsMemberListClient`, though `MemberListClient` exists at `:27` | `memberlist_client.go:55` |
| low | Broad export surface in a `package main` service where nothing external can consume it | `store.go:13-17`, `:61`; `store_mongo.go:26`; `handler.go:46` |
| low | Audit-coverage gap (environmental): `govulncheck` + registry packs blocked | — |
| nitpick | `ReadReceiptRow` and `RoomBotAppEntry` carry `bson` tags only | `store.go:47-52`, `:56-59` |

The 17 uncorrelated log sites are **drift, not ignorance** — `handler.go:869`, `:1751`, `:2193` already use `slog.ErrorContext(ctx, …, "request_id", natsutil.RequestIDFromContext(ctx))`. That matters because the affected lines are the read-receipt and thread-read fan-out failures: the highest-value error logs in the service, and currently the ones an operator cannot join to a request.

### Recommendations
- `high` — Convert all 17 sites to the `*Context` variants with `ctx` + `request_id`, matching `handler.go:869`. Then add a repo-owned semgrep rule under `.semgrep/` (with its `.go` fixture, per §2) banning a bare `slog.Error|Warn|Info|Debug(` inside any function holding a `context.Context` — otherwise this drifts straight back.
- `medium` — Rewrite the two dead `slog.Debug` calls as `slog.Log(ctx, logctx.LevelFlow, …)`, the pattern already used at `handler.go:407`, `:1040`. As written the `DEBUG_LOG_*` knob buys nothing on these paths.
- `medium` — Replace the 14-arg `NewHandler` + 11 trailing assignments with a required-deps struct plus functional options, mirroring `StoreOption`/`memberListClientOption` already in this service.
- `medium` — Split `handler.go` along its existing seams: `handler_read.go` (~1381–1935), `handler_members.go` (~654–1110), `handler_chatlist.go` (~2340+).
- `low` — Return `MemberListClient` from `NewNATSMemberListClient`; unexport the store implementation.
- `low` — Re-run `make sast-vuln` with `vuln.go.dev` reachable before shipping.

---

## 3. Architecture — 3 / 5

The federation, subject, bootstrap and shutdown boundaries are all correct and carefully reasoned. The service has, however, clearly outgrown the flat layout.

### Verified clean (and load-bearing)

**The federation boundary holds.** Every cross-site relay goes through `outbox.Publish` on the local OUTBOX; there is no direct remote-INBOX publish anywhere in the service (`handler.go:882-885`). All nine event types it emits — `InboxRoleUpdated`, `InboxSubscriptionRead`, `InboxThreadRead`, `InboxThreadReadAll`, `InboxSubscriptionMuteToggled`/`FavoriteToggled`/`SectionMoved`/`Opened`, `InboxRoomRestricted` — are present in `pkg/outbox.ConcurrentEventTypes` (`pkg/outbox/outbox.go:20-46`). Also verified: no `os.Getenv`; zero raw `fmt.Sprintf` subject building across all 41 sites; stream configs from `pkg/stream`; typed `caarlos0/env` config with fail-fast validation (`main.go:170-205`); `pkg/shutdown.Wait` in the documented order (`main.go:395-420`); the two goroutines in `requireMembershipAndGetRoom` are `WaitGroup`-bounded (`handler.go:538-547`).

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | Mongo driver semantics decided in the **handler** layer: `handler.go` imports the mongo driver and branches on `mongo.ErrNoDocuments`; `handler_teams.go:192` branches on `mongo.IsDuplicateKeyError`. A non-Mongo `RoomStore` cannot satisfy the handler's contract | `handler.go:18`, `:780`, `:1999`, `:2011`, `:2071`, `:2526` |
| medium | Root cause of that leak: `GetSubscriptionWithMembership` returns a wrapped `mongo.ErrNoDocuments` while **every sibling method** maps to `model.ErrSubscriptionNotFound` — which is why `handler.go:780` must test both | `store_mongo.go:356` vs `:257`, `:274`, `:969`, `:1089`, `:1110`, `:1174` |
| medium | `RoomStore` is a **47-method interface** spanning rooms, subscriptions, thread rooms, members, orgs, users, apps and bot menus, implemented by a 65-method `MongoStore` holding 15 collection handles | `store.go:61-284`; `store_mongo.go:26-44` |
| medium | Split constructor: 14 positional args, then ten more dependencies set by direct field assignment. Correctness rests on scattered nil checks, not the compiler | `handler.go:87`; `main.go:355-368` |
| medium | **Shared knobs re-declared per service**, against §6's "declared once, in the package that owns the thing it configures": `BADGE_CACHE_TTL` duplicated in `inbox-worker` and `user-service`; `ROOM_KEY_RETIRED_TTL`/`ROOM_KEY_GRACE_PERIOD` duplicated in `room-worker`, `bot-room-service`, `broadcast-worker`. Neither `pkg/badgecache` nor `pkg/roomkeystore` exports a config type | `main.go:65`, `:67`, `:115` |
| low | The sanctioned sub-package layout exists precisely for request/reply services this large; only Teams was ever split out | `handler.go` (2,677 lines); `handler_test.go` (8,171 lines) |
| low | `MongoStore`/`NewMongoStore` exported though nothing outside `package main` consumes them; 11 of 15 sibling services use the unexported form | `store_mongo.go:26`, `:59` |
| nitpick | `bootstrapStreams` verifies only ROOMS, though the service also publishes to MESSAGES-CANONICAL and OUTBOX | `bootstrap.go:44-59` |
| nitpick | `bootstrapStreams` does not "no-op when disabled" as `CLAUDE.md` words it — it verifies via `js.Stream` and fails startup. This is the repo-wide pattern (12/12 `bootstrap.go` files), so **the convention text is stale, not the code** | `bootstrap.go:55` |

On the shared-knob finding: `CLAUDE.md` names this divergence as producing exactly the failure it warns about — a short retired-TTL permanently breaking `key.get` for messages already on the wire. Today all four services agree at 30m; nothing but coincidence keeps them agreeing.

### Recommendations
- `medium` — Map `GetSubscriptionWithMembership`'s miss to `model.ErrSubscriptionNotFound`, then delete the `mongo` import from `handler.go`/`handler_teams.go`, adding a store-level `ErrDuplicate` sentinel for the Teams idempotency path.
- `medium` — Move `BadgeCacheTTL` into a `badgecache.Config` and `RoomKeyRetiredTTL`/`GracePeriod` into a `roomkeystore.TTLConfig`, mounted as named fields in all four services.
- `medium` — Fold the ten post-construction assignments into an options-style constructor.
- `low` — Split `RoomStore` along its natural seams (`RoomReader`, `SubscriptionStore`, `MemberStore`, `ThreadStore`, `DirectoryStore`), keeping one `MongoStore` implementing all of them; adopt the sanctioned sub-package layout; unexport the store.
- `nitpick` — Extend `bootstrapStreams` to verify MESSAGES-CANONICAL and OUTBOX (verify only — never create).

