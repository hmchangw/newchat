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
