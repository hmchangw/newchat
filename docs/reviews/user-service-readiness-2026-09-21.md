# user-service — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `c63a2fa` (base `main`)  
**Overall score:** 3.2 / 5 (baseline 2026-09-01: 3.3, Δ -0.1)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

The sub-package layout is genuine rather than cosmetic — clean dependency direction, compile-time interface pins, mocks where CLAUDE.md puts them — and four dimensions score 4. One root cause surfaces in three of them: `HTTPConfig` re-declares `mongoutil.PoolConfig`'s fields instead of mounting the struct under `envPrefix:"HTTP_"`, on the strength of a comment that is wrong, so the HTTP Mongo client is built without `WithPool` and silently runs on the driver's 30-second server-selection timeout while the NATS path fails in 2 seconds; a quiet Mongo pins every HTTP request for its whole budget. Integration found a live subject bug: `subject.SettingsUpdate`/`ChatlistUpdate` interpolate the raw account where the sibling builder encodes it, so a bot's own settings and chatlist sync land on a subject outside its JWT scope. Cross-site status/settings/chatlist replication is a sequential PubAck loop inside the request path with no retry — a lost `muteAllNotifications` leaves the remote pushing until the user touches settings again. Coverage is 54.0%: all six chatlist RPCs and their store methods are untested at every layer, and one unit test boots a real `nats-server`.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 1 |
| Maintainability | 3 |
| Integration | 3 |
| Performance | 4 |

**Findings by severity:** 1 critical, 6 high, 13 medium, 21 low, 7 nitpick (48 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 4

### Evidence

- [high] HTTP Mongo pool knobs re-declared in the service instead of mounting the owner's `mongoutil.PoolConfig` with `envPrefix:"HTTP_"` — `user-service/config/config.go:51-58`, `:180-194`, `:246` — CLAUDE.md: "Never re-declare the env tag and envDefault in a service". `PoolConfig`'s tags already carry `MONGO_` (`pkg/mongoutil/poolconfig.go:25-32`), so `Pool mongoutil.PoolConfig \`envPrefix:"HTTP_"\`` would read `HTTP_MONGO_MAX_POOL_SIZE` exactly as intended; the comment at `:180-182` claiming the tags "cannot be reused under HTTP_MONGO_" is wrong. Concrete consequences: the HTTP pool defaults to 128 vs the owner's 150, a hand-rolled `validateMongoPool` duplicates `PoolConfig.Validate`, and the HTTP Mongo client (`user-service/main.go:145-151`, built with `WithMaxPoolSize/WithMinPoolSize/WithMaxIdleTime`, never `WithPool`) silently keeps the driver's 30s `ServerSelectionTimeout` instead of the shared 2s bound (`pkg/mongoutil/poolconfig.go:49,67-68`) — a quiet MongoDB hangs HTTP handlers for their whole 30s budget.
- [medium] Whole-document reads with no explicit projection — `user-service/mongorepo/apps.go:84` (`GetApp` → `FindByID`), `:139-140` (`GetAppsByAssistants` → bare `FindMany`), `user-service/mongorepo/subscriptions.go:886` (`GetAppSubscription` → bare `FindOne`) — CLAUDE.md "Always project precisely: every find/aggregation MUST specify an explicit projection". Every other read in the repo layer projects; these three fetch the full `apps`/`subscriptions` docs (the app doc carries channel-tab/URL sub-documents the callers never read).
- [low] Unbounded fan-out in `GetThreadUnreadSummary` — `user-service/service/threadunread.go:58-79` — one goroutine per distinct site with a `WaitGroup` but no semaphore, while every sibling fan-out in the service (`subscriptions.go:496`, `badge.go`, `threads.go:224`, `threadunread.go:147-148`) bounds itself with `s.fanout()`. Sites come from row data, not config, so the bound is only the 500-row read cap.
- [low] `service.New` takes 14 positional dependencies including two adjacent same-typed `EventPublisher` values (`pub, clientPub`) — `user-service/service/service.go:202`; wired twice in `user-service/main.go:231` and `:239-244`. A swap of `publisher.New(js)`/`publisher.NewCore(nc)` compiles and silently sends federation events over core NATS.
- [low] `badgeCache` interface duplicated in `main` to work around the unexported one in `service` — `user-service/main.go:62-71` mirrors `user-service/service/service.go:92-101`, with `noopBadgeCache` (`main.go:77-84`) living in `main` rather than beside the consumer. Two copies of one contract that must be kept in step by hand.
- [low] Hand-rolled `chunkStrings` duplicates stdlib `slices.Chunk` already used in the same package — `user-service/service/threadunread.go:179-195` vs `user-service/service/subscriptions.go:323`.
- [low] Stringly-typed list type crosses the service/repo boundary with no shared constant — `user-service/service/subscriptions.go:24` (`validListTypes`) vs literals `"current"/"rooms"/"apps"` re-spelled in `user-service/mongorepo/subscriptions.go:280-290` and `:359`. A drift on either side returns an empty page rather than failing.
- [low] Four `$lookup` sites lack the inline `// $lookup justification:` comment CLAUDE.md requires — `user-service/mongorepo/subscriptions.go:101`, `:156`, `:682`, `:732` — while the sibling joins in the same package carry one (`subscriptions.go:854`, `apps.go`, `threadsubscriptions.go`). All four blame to one 2026-08-25 commit, so grandfathering is unverifiable.
- [nitpick] Parameter named `cap` shadows the builtin — `user-service/service/badge.go:111` (`cappedUnion(ids, trigger, cap int)`).
- [nitpick] `UserStatusView` mapping copy-pasted four times — `user-service/service/me.go:26-32`, `service/status.go:31-37`, `:56-62`, `:81-87`; `GetStatusByName`/`GetProfileByName` (`status.go:19`, `:44`) are byte-identical apart from the wrap string.
- [nitpick] Exported handler methods without doc comments — `user-service/service/status.go:19`, `:65`; `service/apps.go:17`, `:134`; `service/subscriptions.go:732`. Also `user-service/store.go:13` puts a service-slice interface (`subscriptionLister`) in a file the repo layout reserves for the Store interface.

### Recommendations

- [high] Replace `HTTPConfig.MongoMaxPoolSize/MinPoolSize/MaxIdleTime` with `HTTPPool mongoutil.PoolConfig \`envPrefix:"HTTP_"\``, validate via `cfg.HTTPPool.Validate()`, and build the HTTP client with `mongoutil.WithPool(cfg.HTTPPool)` — `config/config.go:51-58,183-194,246`, `main.go:145-151` — deletes `validateMongoPool`, aligns defaults with the owner, and gives the HTTP client the 2s server-selection bound it currently lacks. Env names are unchanged, so no deploy edits.
- [medium] Add explicit projections to `GetApp`, `GetAppsByAssistants`, `GetAppSubscription` — `mongorepo/apps.go:84,140`, `mongorepo/subscriptions.go:886` — listing the fields `AppSubscriptionFromApp` and the reactivation event consume; add a tag-vs-projection guard test like `TestSubscriptionFieldsProjection_MatchesModelTags`.
- [low] Bound `GetThreadUnreadSummary`'s per-site goroutines with the same `sem := make(chan struct{}, s.fanout())` pattern used at `threadunread.go:147-148`.
- [low] Introduce a `service.Deps` struct (or named options) for `service.New` and export `BadgeCache` + `NoopBadgeCache` from `service` — `service/service.go:92-101,202`, `main.go:62-84` — removes the duplicated interface and the swap-prone positional publishers.
- [low] Replace `chunkStrings` with `slices.Chunk` (`threadunread.go:63,179-195`) and move the list-type vocabulary into a typed constant set shared by `service` and `mongorepo` (`service/subscriptions.go:24`, `mongorepo/subscriptions.go:280-290,359`).
- [nitpick] Rename `cap` → `limit` in `cappedUnion` (`badge.go:111`), factor a `statusView(u *model.User)` helper for the four copies, and add doc comments to the five exported handlers listed above.

## 3. Architecture — score 4

### Evidence

- [medium] Cross-site user_status/settings/chatlist replication is fire-and-forget direct JetStream publish, not OUTBOX — `user-service/service/settings.go:138-158`, `user-service/service/status.go:101-121`, `user-service/service/chatlist.go:202-223`, `user-service/publisher/publisher.go:26-31` — Each handler loops `allSiteIDs` sequentially, awaits PubAck under the request ctx, and on failure only `slog.Warn`s ("the next set re-broadcasts"). A lost publish leaves the remote replica stale indefinitely (remote `notification-worker` reads the settings replica for push decisions per the comment at settings.go:129-131). Because the handler ctx carries `HANDLER_TIMEOUT` (main.go:263), nats.go uses that deadline instead of its 5s default (`nats.go@v1.50.0/jetstream/jetstream.go:1234-1238`), so one unreachable peer can consume the whole 15s budget after the Mongo write already committed. `pkg/outbox` exists precisely to make request/reply federation durable (`pkg/outbox/outbox.go:1-4, 22-47`); the user_* types are in neither partition. CLAUDE.md names user-service as a direct publisher, so this is a robustness gap, not a MUST violation.
- [low] `service.badgeCache` is unexported, so `main` re-declares it structurally and hosts the no-op implementation — `user-service/main.go:62-84` vs `user-service/service/service.go:92-101` — Two copies of one consumer interface; a method added to the service copy only breaks the `service.New` call site, and the null object lives outside the package that owns the contract.
- [low] `service.New` takes 14 positional dependencies and is built twice with duplicated client/publisher wiring; the HTTP instance shares NATS-pool repos — `user-service/service/service.go:202`, `user-service/main.go:231-232, 239-244` — `httpSvc` receives `threadSubRepo` and `ssoTokenRepo` bound to the NATS-path Mongo client (main.go:175-176, 242, 244), so the "HTTP-only pool" property (main.go:141-144) holds only because `subscriptionLister` (`user-service/store.go:13-16`) exposes two methods; a future HTTP route would silently cross pools.
- [low] `service/` couples business logic to the NATS transport — `user-service/service/status.go:19`, `user-service/service/settings.go:55`, `user-service/service/chatlist.go:45` (every handler takes `*natsrouter.Context`) — Only `ListSubscriptionsFor`/`CountSubscriptionsFor` (`user-service/service/subscriptions.go:65, 741`) are transport-neutral; the rest cannot be reused by the HTTP surface without the same split, so `service/` is in practice the NATS handler layer rather than a service layer.
- [low] `GetThreadUnreadSummary` fans out without the shared semaphore — `user-service/service/threadunread.go:58-80` — Every sibling fan-out bounds goroutines with `s.fanout()`/`badgeSeedFanout()` (`badge.go:47`, `subscriptions.go:813`, `threadunread.go:148`, `threads.go:224`); this one spawns one goroutine per site unbounded. Practically capped by `len(ALL_SITE_IDS)`, but it is the one path that ignores `MAX_SITE_FANOUT`.
- [nitpick] `$lookup` justification comments are applied inconsistently — `user-service/mongorepo/subscriptions.go:101, 156, 682, 732` carry none while `subscriptions.go:854`, `apps.go:92`, `threadsubscriptions.go:48` do — The file has been touched repeatedly (#382, #461, #491 per `git log`), so the grandfather clause is thin for the unannotated sites.

### Recommendations

- [medium] Route `user_status_updated`/`user_settings_updated`/`user_chatlist_updated` through `outbox.Publish` and add the three types to `outbox.ConcurrentEventTypes` (they are last-write-wins / HWM-guarded per the handler comments) — `user-service/service/settings.go:132-159`, `status.go:93-122`, `chatlist.go:200-223`, `pkg/outbox/outbox.go:22` — Makes the replica durable against a down peer, drops the per-peer PubAck wait out of the request path, and lets `publisher.Publisher` (JetStream) be retired in favour of the local OUTBOX publish. Update CLAUDE.md's "cross-site publishers" sentence in the same PR.
- [medium] Until the outbox migration lands, run the per-peer INBOX publishes concurrently under a short detached-with-deadline ctx (or `s.fanout()` semaphore) so one stalled gateway cannot spend the whole handler budget after commit — `user-service/service/settings.go:138`, `status.go:101`, `chatlist.go:202`.
- [low] Export `service.BadgeCache` (or add `service.NoopBadgeCache`) and delete the mirror in `main` — `user-service/main.go:62-84`, `user-service/service/service.go:92`.
- [low] Replace the 14-arg constructor with a `service.Deps` struct and build the HTTP-path repos from `httpDB` for every repo (or make `httpSvc` a narrower type that only holds `subs/users/apps/rooms/history/badge`) — `user-service/service/service.go:202`, `user-service/main.go:239-244` — Removes the latent pool-crossing and makes the two instances' differences explicit.
- [low] Bound `GetThreadUnreadSummary` with the same `sem := make(chan struct{}, s.fanout())` pattern its siblings use — `user-service/service/threadunread.go:58-80`.
- [nitpick] Add `// $lookup justification:` lines to the four unannotated joins in `user-service/mongorepo/subscriptions.go:101, 156, 682, 732`.
