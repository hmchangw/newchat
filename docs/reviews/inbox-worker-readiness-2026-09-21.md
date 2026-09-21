# inbox-worker — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `1209ff9` (base `main`)  
**Overall score:** 2.7 / 5 (baseline 2026-09-01: 2.8, Δ -0.1)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

inbox-worker is the sole owner of the INBOX stream and the destination end of cross-site federation; its handler logic, settle/retry discipline and subject usage are clean, but three reviewers independently converged on the same two structural defects. First, `room_renamed` is routed to the concurrent pool rather than the sequential membership lane, so the FIFO ordering that the origin-side OUTBOX lane exists to preserve is discarded at the destination: a rename that lands before the `member_added` it depends on matches zero documents and is silently lost, and the new subscription is created with the stale name. Second, the membership lane channel is sized from the raw `CONSUMER_MAX_ACK_PENDING` env value (1024 floor) while the consumer itself is silently promoted to 10000 in the same default case, so the documented "dispatcher never blocks" invariant is false and a membership backlog of more than 1024 events stalls every other event type behind the single sequential Mongo writer. Around those, `FindUsersByAccounts` fetches whole user documents with no projection on the membership hot path, `BADGE_CACHE_TTL` is re-declared per service instead of owned by `pkg/badgecache`, the store implementation lives in `main.go` rather than `store_mongo.go`, the generated mock is dead in favour of a hand-written stub that has no error hooks for 14 store-error branches, and the unit profile sits at 48.1% because every store method is integration-only and CI never runs the integration tier. The per-process-only sequential lane also means the add/remove resurrection race returns at more than one replica, which nothing enforces.

| Dimension | Score |
|---|---|
| Code quality | 3 |
| Architecture | 3 |
| Test coverage | 1 |
| Maintainability | 3 |
| Integration | 3 |
| Performance | 3 |

**Findings by severity:** 1 critical, 10 high, 15 medium, 18 low, 5 nitpick (49 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 3

### Evidence

- [high] `FindUsersByAccounts` fetches whole `users` documents with no projection — `inbox-worker/main.go:246` — CLAUDE.md "Always project precisely: every find MUST specify an explicit projection". The only consumer (`handler.go:315`) reads `user.ID` and `user.Account`; every `member_added` pulls full user docs (settings, chatlist, permissions sub-docs) off the wire. Every other find in the file projects (`main.go:89`, `:212`, `:540`, `:694`), so this one is an outlier, not a convention.
- [medium] Per-service layout not followed: the `InboxStore` interface + `//go:generate` live in `handler.go:19-144` and the Mongo implementation `mongoInboxStore` (~730 lines, `main.go:68-795`) lives in `main.go` — no `store.go` / `store_mongo.go` exist. CLAUDE.md §1 says all services follow the `store.go` / `store_mongo.go` split; here `main.go` is 1,121 lines of which wiring is only `806-1019`.
- [medium] Log-AND-return on permanent errors — `inbox-worker/handler.go:463-465` and `:619-622` — both sites `slog.WarnContext(...)` then `return errcode.Permanent(errcode.BadRequest(...))`. The settle path `jsretry.Settle` (`pkg/jsretry/jsretry.go:129`) logs the same permanent error again at WARN with `request_id`, so each poison event emits two lines, the handler's one without a request ID. CLAUDE.md: "Never log AND return."
- [medium] Handler-level log lines carry no `request_id` — `inbox-worker/handler.go:356`, `:415`, `:419`, `:463`, `:619`, `:705` — `logctx.Handler.Handle` (`pkg/logctx/handler.go:68-77`) only forwards the record; it does not inject the request ID from ctx, which is why `pkg/jsretry` and `main.go:1090` attach `"request_id", natsutil.RequestIDFromContext(ctx)` by hand. Peers do the same in their handlers (`broadcast-worker` 32 refs, `room-worker` 17); `inbox-worker/handler.go` has 0, so these lines cannot be joined to the settle/ack log for the same delivery.
- [low] Dead interface method returning a bare error — `inbox-worker/handler.go:23` / `main.go:133-136` — `CreateSubscription` is declared on `InboxStore`, implemented, mocked, and never called (only `BulkCreateSubscriptions` is, `handler.go:333`). Its body `_, err := s.subCol.InsertOne(ctx, sub); return err` is the one un-wrapped error return in the service (CLAUDE.md: never return bare `err`). 0% covered in the profile.
- [low] Two further bare `return err` passthroughs — `inbox-worker/main.go:218` (`subscriptionExists`) and `inbox-worker/roomsubcache.go:106` (`hasRoomSubscription`) — both callers re-wrap (`main.go:199`, `handler.go:207`) so nothing is lost at the boundary, but the rule is "always wrap".
- [low] `HandleEvent` switch mixes raw strings and typed constants — `inbox-worker/handler.go:226-246` — ten cases use literals (`"member_added"`, `"room_sync"`, `"role_updated"`, …) while the remaining twelve use `model.Inbox*` constants that already exist for every literal (`pkg/model/event.go:167-187`); `main.go:1027` uses `model.InboxMemberAdded` for the same string. A constant rename or retype would silently desync the two sides.
- [low] Context-less `slog.Warn` on the unknown-event path — `inbox-worker/handler.go:271` — the only ctx-less log in the handler; drops trace correlation for exactly the case an operator would need to find (a new event type arriving at an old site).
- [low] Connection strings defaulted rather than `required` — `inbox-worker/main.go:36`, `:39` — `NATS_URL` and `MONGO_URI` default to `localhost`; CLAUDE.md: "never default secrets or connection strings — mark them required". Five of six `MONGO_URI` declarations repo-wide do this, so it is inherited, not local.
- [nitpick] Interface/impl comment drift: `InboxStore` says a missing user is a "logged no-op" (`handler.go:111`, `:115`, `:122`); the implementations are silent no-ops with unchecked `MatchedCount` (`main.go:260`, `:272`, `:292`, `:337`). Store method `naksIfSubscriptionMissing` (`main.go:196`) names a transport action the store does not perform — it returns an error.

### Recommendations

- [high] Add `options.Find().SetProjection(bson.M{"_id": 1, "account": 1})` to `FindUsersByAccounts` — `inbox-worker/main.go:246` — restores the MUST and stops shipping full user docs per `member_added`.
- [medium] Move `InboxStore` + `//go:generate` to `store.go` and `mongoInboxStore` + helpers (`threadReadGuard`/`threadReadUpdate`) to `store_mongo.go`; regenerate mocks (destination unchanged) — `inbox-worker/handler.go:19-144`, `main.go:68-795` — brings the service to the documented layout and leaves `main.go` as wiring only.
- [medium] Drop the two `slog.WarnContext` calls before `return errcode.Permanent(...)` (fold `account`/`room_id` into the errcode message or `WithLogValues`) — `inbox-worker/handler.go:463`, `:619` — `jsretry.Settle` already logs once with `request_id`.
- [medium] Attach `"request_id", natsutil.RequestIDFromContext(ctx)` to every handler-level log line and switch `handler.go:271` to `slog.WarnContext(ctx, …)` — matches `broadcast-worker`/`room-worker` and `main.go:1090`.
- [low] Delete `CreateSubscription` from `InboxStore` and `mongoInboxStore` (`handler.go:23`, `main.go:133`) and wrap the two bare passthroughs (`main.go:218`, `roomsubcache.go:106`) — removes the only unwrapped returns and 0%-covered dead code.
- [low] Replace the ten string-literal cases with `model.Inbox*` constants — `inbox-worker/handler.go:226-246` — one source of truth shared with `isMembershipSubject`.
