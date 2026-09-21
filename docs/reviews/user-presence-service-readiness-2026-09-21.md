# user-presence-service — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `94fd295` (base `main`)  
**Overall score:** 2.8 / 5 (baseline 2026-09-01: 2.8, Δ +0.0)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

user-presence-service confirms, across three independent reviewers, a cross-service contract that has now survived two audit cycles unfixed: notification-worker requests a presence snapshot on a subject no service in the repository registers, and the two sides do not even share a payload shape or a batch limit. Enabling the flag that turns it on does not degrade gracefully into something useful — every chunk times out and the call fails open, so do-not-disturb and in-call push suppression can never engage, while the ops document still presents it as a live contract waiting to be switched on. The service's own contract, by contrast, is clean and matches its documentation exactly. Its second structural problem is a second deployable binary nested inside the service directory, which re-declares the Valkey connection knobs and both presence timing knobs with their own defaults and dials Valkey raw — bypassing the shared helper that the store's own comment says was introduced to end exactly this duplication. Both binaries drive the same Lua scripts over the same keys and the stale threshold controls connection pruning, so an operator override on one and not the other makes the two disagree about who is online. On scale, the sweep index is a single un-hash-tagged key, so every presence write in the site serializes onto one Valkey shard and the store cannot be scaled out by adding shards; expiry drains at about a hundred accounts per second, so recovering from a gateway restart that drops fifty thousand connections takes minutes. A mid-sweep error discards status changes already committed, leaving those accounts permanently stale. Coverage is 44.8% and the presence state machine itself — a hundred and twenty lines of Lua inside Go strings — has no unit coverage at all and no CI job that runs its integration tests.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 3 |
| Test coverage | 1 |
| Maintainability | 3 |
| Integration | 3 |
| Performance | 3 |

**Findings by severity:** 1 critical, 17 high, 20 medium, 12 low, 6 nitpick (56 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 4

### Evidence

- [high] `sync/` re-declares shared Valkey and presence knobs with its own `envDefault` — `user-presence-service/sync/main.go:24-31` — `PRESENCE_STALE_THRESHOLD` (45s), `PRESENCE_CONNS_TTL` (5m), `VALKEY_ADDRS`, `VALKEY_PASSWORD` are declared flat instead of mounting `valkeyutil.Config` (`pkg/valkeyutil/config.go:20-23`) and the owning `PresenceConfig`. CLAUDE.md forbids re-declaring a shared knob's tag+default. This is not cosmetic: the sync feeds its own `StaleThreshold` into `presencestore.NewValkeyStoreFromClient` (`sync/main.go:109`), and `computeLua` prunes connections against that value (`presencestore/store.go:77`), so a drifted default makes the two binaries materialize different effective statuses for the same account.
- [medium] `sync` dials Valkey raw, bypassing `valkeyutil` — `user-presence-service/sync/main.go:101-103` — `redis.NewClusterClient` skips `valkeyutil.ConnectRaw`'s startup Ping (fail-fast) and o11y instrumentation (`pkg/valkeyutil/valkey.go:110-131`), so the sync's Valkey commands emit no traces/metrics and an unreachable cluster surfaces mid-run instead of at startup. `presencestore/store.go:185-190` documents this exact duplication as having been deliberately removed from the main service; the sync reintroduces it.
- [medium] Sentinel error compared with `==`/`!=` instead of `errors.Is` — `user-presence-service/presencestore/store.go:300` and `:305` — `err != redis.Nil` / `err == redis.Nil`. CLAUDE.md §3 mandates `errors.Is`, and every other `redis.Nil` site in the repo uses it (`pkg/valkeyutil/valkey.go:164,189,232`). A wrapped `redis.Nil` from a cluster pipeline would turn an expected miss into a hard `batch get` failure for the whole batch.
- [medium] Script reply is decoded with discarded type assertions — `user-presence-service/presencestore/store.go:209-211` — `changed, _ := res[0].(int64)` etc. Arity is checked (`:206`) but types are not, so a Lua reply shape change degrades silently to `changed=false, effective=""`, which the handler then treats as "no change, publish nothing" — a presence stall with no error and no log.
- [low] Bare `return err` — `user-presence-service/sweeper.go:46` — `tick` returns `s.store.Sweep`'s error unwrapped; CLAUDE.md §3 says never return bare `err`. Impact is limited because the sole caller logs it with context (`sweeper.go:36`).
- [low] Request-scoped logs use the non-context `slog` API — `user-presence-service/handler.go:209`, `presencestore/store.go:40,44` — these three sites have a live request/trace context in hand (the `errgroup` child of `*natsrouter.Context`, and `PublishState`'s `ctx`) but call `slog.Error`, dropping the trace/request-ID correlation the router middleware sets up (`pkg/natsrouter/router.go:142`). The service has zero `slog.ErrorContext` calls. Repo-wide this is the majority pattern (621 vs 82), so it is a repo debt rather than a service-local deviation.
- [low] Peer error envelope is flattened to a string — `user-presence-service/peer_client.go:56-58` — `fmt.Errorf("remote presence query: %s", errResp.Message)` discards the parsed code/reason, so the caller cannot distinguish a peer-side `BadRequest` (a real client bug, e.g. batch over max) from `Unavailable`; `handler.go:205-211` degrades both to offline identically.
- [nitpick] Redundant loop-variable copy — `user-presence-service/handler.go:189` — `site, accounts := site, accounts` is a no-op under Go 1.22+ (`go.mod`: `go 1.25.13`); only one other site in the repo still does this.
- [nitpick] Hardcoded sweep page size — `user-presence-service/presencestore/store.go:320` — `Count: 500` is the one magic number in an otherwise fully env-tunable service.

### Recommendations

- [high] Mount `valkeyutil.Config` and the presence tunables as named fields in the sync's `Config` — `sync/main.go:24-31` — removes the divergent-threshold hazard and the second copy of the Valkey env contract.
- [medium] Replace the raw `redis.NewClusterClient` with `valkeyutil.ConnectRaw(ctx, cfg.Valkey, valkeyutil.Instrumented(sdk))` — `sync/main.go:101` — restores fail-fast connectivity and Valkey tracing/metrics for the Teams sync.
- [medium] Switch both `redis.Nil` comparisons to `errors.Is` — `presencestore/store.go:300,305`.
- [medium] Fail loudly on an unexpected script reply: check the `ok` of each assertion in `run` and return a wrapped error — `presencestore/store.go:209-211`.
- [low] Wrap the sweep error with context (`fmt.Errorf("sweep: %w", err)`) — `sweeper.go:46`.
- [low] Use `slog.ErrorContext` on the three request-scoped sites — `handler.go:209`, `presencestore/store.go:40,44`.
- [low] Collapse the near-identical Hello/Ping/Activity/Bye cases into table-driven subtests — `handler_test.go:64-154` — CLAUDE.md §4 prefers table-driven for repeated variations of the same logic; these eight tests differ only in the store method and payload.

## 3. Architecture — score 3

### Evidence

- [high] `notification-worker`'s presence RPC targets a subject NO service registers — `pkg/subject/subject.go:1768` / `notification-worker/presence.go:61` — `PresenceSnapshot` builds `chat.presence.{siteID}.request.snapshot`; the only non-test caller of `PresenceSnapshot(` repo-wide is that line. This service registers only `chat.user.presence.{siteID}.query.batch` and `chat.server.request.presence.{siteID}.query.batch` (`main.go:212-220`). Schemas differ too (`PresenceSnapshotRequest/Reply` + `Presence{aggregatedStatus}` vs `PresenceQuery/PresenceQueryResponse` + `PresenceState`), so flipping `PRESENCE_RPC_ENABLED=true` (`notification-worker/main.go:65`) times out per batch and fails open — in-call push suppression would never engage.
- [high] `sync/` is a second `package main` binary nested inside a service dir — `user-presence-service/sync/main.go:1` plus its own `deploy/Dockerfile`, `docker-compose.yml`, `azure-pipelines.yml`. It is a deployable service, and CLAUDE.md requires those at the repo root; the sanctioned sub-package exception covers `config/ models/ service/` splits, not a nested main. `presencestore/` (shared by both binaries) is fine; `sync/` also shadows stdlib `sync`.
- [high] `sync` re-declares Valkey config and dials raw — `sync/main.go:30-31,101-103` declares `VALKEY_ADDRS`/`VALKEY_PASSWORD` itself and calls `redis.NewClusterClient` directly, while the main binary mounts `valkeyutil.Config` and uses `valkeyutil.ConnectRaw` (`main.go:53,120`). `presencestore/store.go:185-190` explicitly documents removing exactly this duplication; `sync` reintroduced it, so its Valkey traffic is uninstrumented and on a different dial policy.
- [high] `PRESENCE_STALE_THRESHOLD` / `PRESENCE_CONNS_TTL` declared twice with independent defaults — `main.go:42,44` vs `sync/main.go:24-25`. Both feed `presencestore.NewValkeyStoreFromClient`, and `staleMs` drives the Lua prune/recompute (`presencestore/store.go:77,198-199`). `sync/deploy/docker-compose.yml` passes neither through, so an operator override on the main service silently diverges pruning semantics against the same keys — the precise failure the CLAUDE.md "declared once in the owning package" rule prevents.
- [medium] Peer client bypasses the instrumented conn, dropping trace + request-ID across the site hop — `main.go:161` (`nc.NatsConn()`), `peer_client.go:47-52` hand-builds `&nats.Msg{… Header: nats.Header{}}` instead of `natsutil.NewMsg(ctx, …)`. No `X-Request-ID`, no propagator headers, no client span. The sibling doing the identical RPC (`user-service/presenceclient/client.go:36`) uses the wrapped `o11ynats.Conn.Request`.
- [medium] `errcode.Parse` result flattened, losing the remote code — `peer_client.go:56-58` returns `fmt.Errorf("remote presence query: %s", errResp.Message)` rather than the typed `*errcode.Error`, so callers cannot `errors.Is/As`. Blast radius is small only because `handler.go:206-211` degrades to offline anyway.
- [medium] Sweep index is one un-tagged cluster key with a hard 500/tick cap — `presencestore/store.go:18,317-321`. Per-account keys are hash-tagged `{account}` and spread, but `presence:sweep` holds every connected account in the site, so every replica's 5s sweep hits one slot and drains ≤500 accounts at 2 sequential round trips each. Correctness is fine — the Lua CAS on `statusKey` makes concurrent replica sweeps idempotent, so no leader election is needed — but expiry throughput caps at ~100 accounts/s/replica on a fleet-wide hotspot.
- [low] Stale comment asserts a guarantee the interface does not make — `main.go:65-67` claims the compile-time check covers `SetExternal`, but `PresenceStore` (`store.go:23-49`) has no such method; it is constrained by `sync/store.go:16`.
- [nitpick] Mock layout deviates from the documented single `mock_store_test.go` — `mock_peer_client_test.go`, `sync/mock_test.go`; a second `//go:generate` sits above the package clause at `peer_client.go:1`.

### Recommendations

- [high] Resolve the presence contract: register a `subject.PresenceSnapshot(siteID)` route in `registerRoutes` (`main.go:212-220`) returning `model.PresenceSnapshotReply`, or delete the builder and repoint `notification-worker/presence.go:61` at `PresenceQueryBatchPeer`. Until then `PRESENCE_RPC_ENABLED` must stay `false` and say so in the runbook — otherwise it is dead config.
- [high] Promote `sync/` to a repo-root service (e.g. `user-presence-sync/`) importing `presencestore`, or move `presencestore` to `pkg/`. Restores the flat-service convention and drops the `sync` name shadow.
- [high] Mount `valkeyutil.Config` in `sync/main.go:30-31`, dial via `valkeyutil.ConnectRaw`, and source the stale/conns TTLs from one shared struct with `main.go:39-46`. Closes both config-drift vectors.
- [medium] Build peer requests with `natsutil.NewMsg(ctx, …)` on the wrapped conn (`peer_client.go:47-52`, `main.go:161`) and return the typed error from `errcode.Parse` (`:56`) — matches `user-service/presenceclient` and restores cross-site tracing.
- [medium] Add a per-peer failure short-circuit around `h.peer.QueryPeer` (`handler.go:205-218`): a hung peer costs the full `PEER_TIMEOUT` on every query with no admission cap in front (`main.go:164-174`) — a missing peer is fast via NATS no-responders, a sick one is not. Downgrade the per-failure `slog.Error` (`handler.go:209`) to Warn.
- [medium] Make the sweep index shardable (`presencestore/store.go:18`, e.g. N hash-tagged buckets swept round-robin) and the 500 cap (`:320`) configurable, so expiry scales with the site rather than one Valkey slot.
- [low] Fix the `main.go:65-67` comment and consolidate the generate directives / mock filenames onto the documented layout.

## 4. Test coverage — score 1

### Evidence

- [critical] coverage below repo minimum 80%, currently 44.8% — `user-presence-service/` tree — Below the 60% floor in CLAUDE.md §4, so this dimension caps at 1. No package clears 80%.
- [high] `presencestore` has 0% unit coverage; its only tests are integration-tagged and this service's CI never runs them — `presencestore/integration_test.go:1`, `deploy/azure-pipelines.yml:44` — The pipeline runs `go test ./$(SERVICE_NAME)/... -race`, no `-tags=integration`. So the whole presence state machine (`computeLua` precedence ladder, `store.go:68-118`; sweep decay; CAS status write) is verified by nothing that runs automatically. The package has no untagged `_test.go`, so `go test ./...` reports "no test files".
- [high] `Store.ActiveAccounts` and `Store.Close` are exercised nowhere — `presencestore/store.go:279`, `:341` — `ActiveAccounts` is the sole input to the Teams reconcile (`sync/reconcile.go:53`) yet is only ever mocked (`sync/reconcile_test.go:53`); no test calls the real method, so a wrong ZSET key ships silently. `integration_test.go:20` states outright that `Close` is never called.
- [medium] `registerRoutes` is 0% covered despite being extracted to be the single registration table — `main.go:212-220` — It takes a `*natsrouter.Router` and does no I/O; asserting the seven subjects is ~15 lines. A wrong `subject.*` builder or mis-wired handler is caught by nothing.
- [medium] `Sweeper.Run` is 0%, including its `ctx.Done()` exit — `sweeper.go:27-40` — The service's only long-lived goroutine, and `main.go:187-198` blocks shutdown on `sweepDone`; a regression that stops `Run` returning turns graceful shutdown into a 25s timeout. `tick` is already 100% — only the loop is untested.
- [medium] Integration tests start a per-test Valkey cluster 15 times instead of the documented shared default — `integration_test.go:24`, `presencestore/integration_test.go:21`, `sync/valkey_integration_test.go:16,33` — §4 makes `SharedValkeyCluster`+`FlushValkey` the default and reserves `StartValkeyCluster` for routing assertions or a wrapper that calls `Close()`. The justification at `integration_test.go:18-21` refutes itself ("the test never calls store.Close()").
- [medium] `user-presence-service/integration_test.go` is `package main` but tests only `presencestore.Store` — `integration_test.go:22-174` — It references no `package main` symbol and duplicates the manual-override/precedence cases already in `presencestore/integration_test.go:47-92`. CLAUDE.md requires integration tests in the package under test.
- [medium] `sync/valkey.go` is 0%, including `natsPublisher.Publish` — `sync/valkey.go:94-96` — The only place sync-originated presence events get their siteID and timestamp; it takes an injectable `PublishFunc`, so it needs no container. The other two types in that file are at least integration-covered.
- [low] Every uncovered statement in the otherwise-strong handler tests is a store-error or invalid-input return — `handler.go:49`, `:67`, `:83`, `:95`, `:99`, `:111`, `:135` — No store-error test for `Hello`/`Ping`/`Activity`/`Bye`, no missing-`connId` test for `Bye`, no missing-`account` test for `SetManual`, no empty-accounts test for `QueryBatchPeer`.
- [low] One `t.Run` in the whole service — `sync/reconcile_test.go:30` — `handler_test.go:75/105/135` are three copies of one assertion and `reconcile_test.go:140-248` is nine near-identical error-path functions; both are table-driven candidates under §4.
- [nitpick] Wall-clock `time.Sleep(40ms)` gates a staleness assertion — `integration_test.go:115` — Timing-dependent, though `Sweep(ctx, now)` takes the clock as a parameter.

### Recommendations

- [critical] Add untagged tests to `presencestore` until it clears 80%: start with `PublishState` (`store.go:36`, pure given an injected `PublishFunc`) and `run`'s arity/parse branches (`:206-212`), then extract the Lua-reply parsing so it is testable without Valkey. Largest lever: 86 uncovered statements.
- [high] Make the Lua state machine verifiable in CI: add `-tags=integration` plus a Valkey container to `deploy/azure-pipelines.yml:44` and `sync/deploy/azure-pipelines.yml:45`, and `-coverpkg=./user-presence-service/...` so those tests count. Today the ladder can regress on a green build.
- [high] Integration-test `ActiveAccounts` and `Close` (`presencestore/store.go:279`, `:341`): assert the sweep index is filled by `SetActivity` and emptied by `RemoveConnection` — the contract the Teams sync depends on, currently asserted only against a mock.
- [medium] Unit-test `registerRoutes` (`main.go:212`) and `Sweeper.Run`'s cancellation exit (`sweeper.go:27`), and split `main.go:69-205` into a `run() error` as `sync/main.go:77` already does, making the validation ladder at `main.go:77-106` (~30 statements, 0%) testable. The main package has no `config_test.go`.
- [medium] Move `integration_test.go` into `presencestore/`, merge the duplicated precedence cases, and switch all Valkey integration tests to `SharedValkeyCluster`+`FlushValkey` — restores the package rule and cuts ~15 container starts to one per package.
- [low] Close the handler error branches with one table-driven test per handler over {missing account, missing connId, store error, happy path} (`handler.go:42-127`) — lifts the main package toward the gate and satisfies §4's per-handler requirement.
