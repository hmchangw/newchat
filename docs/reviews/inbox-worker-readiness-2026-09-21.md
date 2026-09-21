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

## 3. Architecture — score 3

### Evidence

- [high] `room_renamed` bypasses the sequential lane at the destination, discarding the FIFO ordering the OUTBOX ordered lane exists to preserve — `inbox-worker/main.go:1026-1029` — `isMembershipSubject` routes only `member_added`/`member_removed`; `room_renamed` goes to the concurrent pool. `pkg/outbox/outbox.go:353-363` puts rename in `OrderedEventTypes` precisely so "a room_renamed must not overtake the member_added that creates the subscription it renames", and `room-worker/handler.go:2421-2424` states a rename applied before the add "would apply to zero docs and be lost". At the destination `handleRoomRenamed` (`handler.go:630-639`) is an `UpdateMany` that returns nil on zero matches (Ack), and `handleMemberAdded` then inserts the sub with the event's stale `RoomName` and no `NameUpdatedAt` (`handler.go:311-329`). Whenever the membership lane is backed up (post-outage replay, exactly when FIFO matters) this loss is deterministic.
- [medium] Membership lane sized from the raw env setting, not the effective consumer budget — `inbox-worker/main.go:951` — `membershipLaneCap(cfg.Consumer.MaxAckPending)` yields 1024 at the default, but `buildConsumerConfig` (`main.go:1054-1056`) raises the server-side budget to `federationMaxAckPending` = 10000 in the same default case. The invariant in `lanes.go:19-25` ("the send is non-blocking in every reachable state") is therefore false: >1024 in-flight membership events park the dispatcher and starve the concurrent lane. `consInfo.Config.MaxAckPending` is already fetched at `main.go:875`. `lanes_test.go:88-107` tests the cap with the same number on both sides, so it cannot catch the mismatch.
- [medium] Sequential lane is per-process only; the shared durable delivers to every replica — `inbox-worker/main.go:927-932`, `main.go:1052` — one durable `inbox-worker` with ack-pending 10000 hands add/remove for the same `(room, account)` to different pods concurrently, so the resurrection race the lane is documented to prevent returns at `replicas > 1`. The comment admits "within this instance" but nothing (config, deploy doc, watermark on `DeleteSubscriptionsByAccounts` at `main.go:223-229`) enforces or compensates.
- [medium] Federation is durable at origin, lossy at destination — `inbox-worker/main.go:1079-1096` — OUTBOX retries forever (`MaxDeliver=-1`), but here `WithOutageRetryBudget` caps at 17 deliveries (~1h minimum window) and `settleFederated` then `TermWithReason`s the event. A `member_added` whose user has not materialised on this site within that window (`handler.go:344-346` NAKs until the user lands) is dropped with only a log line and manual `nats stream get` as recovery. The trade-off is reasoned (`main.go:1044-1049`) but there is no dead-letter/replay path.
- [low] Per-service layout not followed: no `store.go`/`store_mongo.go` — `inbox-worker/handler.go:19-144` holds `InboxStore` and the mockgen directive; `mongoInboxStore` and ~730 lines of Mongo code live in `main.go:68-795`, leaving a 1121-line `main.go` that CLAUDE.md reserves for config/wiring/shutdown.
- [low] Config comment contradicts code on Valkey — `inbox-worker/main.go:59-61` vs `main.go:886-890` — the field doc says a connect failure "logs and continues rather than exiting"; the code `os.Exit(1)`s. `valkeyutil.ConnectOptional` (`pkg/valkeyutil/config.go:91-98`) implements the documented behaviour for an optional tier.
- [low] Dependencies injected by field poke after construction — `inbox-worker/main.go:904-905` — `handler.badge`/`handler.valkey` are set directly although `NewHandler` already has a functional-option surface (`handler.go:172-187`, `WithRoomSubCache`).
- [nitpick] Dispatch switch mixes string literals and `model.Inbox*` constants — `inbox-worker/handler.go:226-269` — `"member_added"`, `"room_sync"`, `"role_updated"`, … alongside `model.InboxRoomRenamed`; `main.go:1027` uses `model.InboxMemberAdded` for the same type. `room_sync` has no constant at all (`handler_test.go:3437`).

### Recommendations

- [high] Route `room_renamed` onto the sequential lane and derive the predicate from `outbox.OrderedEventTypes` — `inbox-worker/main.go:1026-1029` — one source of truth for "order-sensitive", so the origin and destination partitions cannot drift again; rename volume is negligible so throughput is unaffected. Add a handler/integration test that delivers rename before add and asserts the final name.
- [medium] Size the lane from `consInfo.Config.MaxAckPending` — `inbox-worker/main.go:951` — restores the non-blocking invariant; make `TestDispatchLanes_MembershipBacklogDoesNotStallConcurrentLane` size the channel from `buildConsumerConfig(...).MaxAckPending` so a future divergence fails.
- [medium] Make membership order-safe instead of order-dependent — `inbox-worker/main.go:223-229`, `handler.go:394-434` — e.g. a `membershipUpdatedAt` watermark on the subscription plus a tombstone/`removedAt` guard so a stale add cannot resurrect; until then, state `replicas: 1` for this deployment explicitly (deploy manifests / README).
- [medium] Give the give-up a recovery path — `inbox-worker/main.go:1087-1095` — publish the Termed envelope to a dead-letter subject/stream or ship an advisory consumer on `MSG_TERMINATED`, and consider `WithUnlimitedRedelivery` for the membership types only (their ack-pending share is small), as `roomlist-worker` does.
- [low] Move `InboxStore` + mockgen to `store.go` and `mongoInboxStore` to `store_mongo.go` — `inbox-worker/handler.go:19-144`, `main.go:68-795` — pure file move, no behaviour change.
- [low] Reconcile Valkey startup semantics — `inbox-worker/main.go:59-61`, `main.go:886-890` — either switch to `ConnectOptional` or fix the comment; and add `WithBadgeCache`/`WithValkey` options so `NewHandler` fully wires the handler.
