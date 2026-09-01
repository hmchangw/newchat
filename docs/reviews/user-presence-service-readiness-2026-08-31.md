# user-presence-service — Production Readiness Review

**Service:** `user-presence-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 2.8 / 5

Idiomatic, densely WHY-commented Go with correct `errcode` Tier-1 usage, exemplary DI in the core binary, and a genuinely careful hot path — a single-round-trip Lua recompute, deduped pipelined batch reads, a precise Mongo projection via `pkg/userstore`, and clean sweeper termination.

Three findings dominate. **The bulk-presence RPC that `notification-worker`'s push gate calls has no responder anywhere in the repo** — and the two sides also speak different payload shapes and disagree on chunk size by 5×, so the gate cannot be enabled by configuration alone. It is dead-by-flag today; flipping that flag would fail three ways. **The sweep index is a single untagged cluster key** while every per-account key is hash-tagged, so 100% of hello, heartbeat, activity and bye traffic for the entire site funnels into one Valkey master's single-threaded loop — the service's scaling ceiling, invisible until it is hit. And **`Sweep` drains 100 stale accounts per second**, so a gateway restart dropping 50k connections leaves those users shown as online for about eight minutes, long past the 45-second staleness threshold.

Underneath: the `sync/` sub-binary re-declares the shared Valkey and presence knobs and hand-dials the cluster, against a rule the store's own comments state explicitly. Coverage is 45.1%.

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
| Count | 1 | 14 | 21 | 15 | 10 | **61** |

---

## 2. Go code quality — 4 / 5

Idiomatic, densely WHY-commented Go with correct `errcode` Tier-1 usage, structured `slog` throughout and zero forbidden patterns — undercut by a total absence of `errors.Is`, silently-swallowed type assertions in the Lua reply decoder, and the `sync/` sub-binary re-hand-rolling config and dial that `pkg/valkeyutil` exists to own.

### Findings
- `high` — `sync/main.go` re-declares the shared Valkey knobs (`VALKEY_ADDRS`, `VALKEY_PASSWORD`) as its own fields and hand-dials `redis.NewClusterClient` instead of mounting `valkeyutil.Config` + `valkeyutil.ConnectRaw` — `user-presence-service/sync/main.go:30-31`, `:91-93`
  CLAUDE.md §6 Configuration: "A knob shared by more than one service is declared once, in the package that owns the thing it configures… Never re-declare the env tag and `envDefault` in a service." The sibling binary in the same directory does it correctly (`main.go:53`, `:120`). Consequences are concrete: the sync's client carries no o11y instrumentation and is never pinged at startup, and `presencestore/store.go:185-190`'s own doc comment states this exact duplication was already removed once. `PRESENCE_STALE_THRESHOLD` / `PRESENCE_CONNS_TTL` (`sync/main.go:24-25`) are the same defect against `presencestore` — the two binaries feed the same Lua scripts and their defaults agree only by luck.

- `medium` — Redis sentinel errors compared with `==`/`!=` instead of `errors.Is` — `user-presence-service/presencestore/store.go:300`, `:305`
  ```go
  if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
  ```
  These are the only two such sites in the repo; `pkg/valkeyutil/valkey.go:164,189,232` uses `errors.Is(err, redis.Nil)` everywhere. go-redis wraps pipeline errors in some paths, so a wrapped `redis.Nil` would escape both guards and turn an ordinary cache miss into a failed `BatchGet` (and a 500-equivalent on `QueryBatch`). There is no `errors.Is`/`errors.As` anywhere in the 3121-line service.

- `medium` — Lua reply type assertions silently discard failure — `user-presence-service/presencestore/store.go:209-211`
  ```go
  changed, _ := res[0].(int64)
  effective, _ := res[1].(string)
  nextDeadline, _ := res[2].(int64)
  ```
  The arity check on `:206` is careful, but a wrong element *type* (e.g. a Valkey/Redis version returning a different numeric encoding) yields `changed=false, effective="", nextDeadline=0` with no error — presence silently stops publishing transitions and `reschedule` ZADDs every account to score 0, permanently hot in the sweep index. CLAUDE.md §3: "Never ignore errors silently — comment if intentionally discarded."

- `medium` — A parsed `*errcode.Error` from a peer is downgraded to an opaque string error — `user-presence-service/peer_client.go:56-58`
  ```go
  if errResp, ok := errcode.Parse(reply.Data); ok {
      return nil, fmt.Errorf("remote presence query: %s", errResp.Message)
  }
  ```
  `errcode.Parse` is the sanctioned Tier-3 cross-site decode, but formatting with `%s` discards the code/reason, so no caller can `errors.As` it — and the test is forced into string matching to compensate (`peer_client_test.go:89-90`), the very pattern CLAUDE.md §3 bans. Wrap with `%w` or return the `*errcode.Error`.

- `low` — Bare `return err` in five places, each relying on the callee having wrapped — `user-presence-service/sweeper.go:46`, `handler.go:221`, `presencestore/store.go:234`, `:237`, `sync/reconcile.go:61`
  CLAUDE.md §3 is unconditional ("Never return bare `err`"); the caller loses which of the two branches in `mutate` failed.

- `low` — `QueryBatch` and `QueryBatchPeer` assemble near-identical response loops with divergent normalization — `handler.go:144-153` vs `handler.go:224-237`
  Only the client-facing path guards `status == model.StatusNone`; the peer leaf guards only `!ok`. The divergence is currently masked by `BatchGet` mapping `""` to offline (`presencestore/store.go:305`), so this is a latent inconsistency, not a live bug.

- `nitpick` — Single-method interfaces not `-er`-suffixed: `UserDirectory` (`store.go:16`), `PeerPresenceClient` (`peer_client.go:20`). CLAUDE.md §3 Naming. The `sync/` package gets this right (`activeLister`, `userResolver`, `presenceReader`, `externalApplier`).

- `nitpick` — Redundant `slog.SetDefault(slog.New(slog.NewJSONHandler(...)))` — `sync/main.go:58`; `pkg/obs/bootstrap.go:14` already installs a JSON default in `init()` before any `main()` runs.

- `nitpick` — Go 1.22+ makes the per-iteration copy `site, accounts := site, accounts` dead — `handler.go:189` (repo is on Go 1.25).

- `low` — SAST audit-coverage gap (environmental, not a service defect): gosec and the 18 repo-owned semgrep rules are clean repo-wide; `govulncheck` and the semgrep registry packs could not run (egress blocked), so dependency-CVE and generic-pattern coverage is unverified for this service.

### Recommendations
- `high` — Replace `sync/main.go`'s Valkey block with `Valkey valkeyutil.Config` + `Validate()` + `valkeyutil.ConnectRaw(ctx, cfg.Valkey, valkeyutil.Instrumented(sdk))`, matching `main.go:53,77,120`; move the two `PRESENCE_*` duration knobs into a single `presencestore`-owned config struct both binaries mount.
- `medium` — Switch both `redis.Nil` comparisons to `errors.Is` (`presencestore/store.go:300,305`); add a repo-owned semgrep rule for `== redis.Nil` since this pattern recurred once already.
- `medium` — Make the three assertions in `Store.run` checked, returning `fmt.Errorf("presence script %q: reply element %d has type %T", account, i, res[i])` on mismatch.
- `medium` — Propagate the peer `*errcode.Error` with `%w` (or return it directly) in `peer_client.go:57`, then rewrite `peer_client_test.go:89-90` to assert via `errors.As` instead of substring matching.
- `low` — Wrap the five bare `return err` sites with what the calling function was doing (e.g. `fmt.Errorf("sweep tick: %w", err)` in `sweeper.go:46`).
- `low` — Extract the shared response-assembly loop from `QueryBatch`/`QueryBatchPeer` into one helper so the `StatusNone` normalization cannot drift again.
- `nitpick` — Rename `UserDirectory` → `UserResolver` (or `SiteResolver`) and `PeerPresenceClient` → `PeerQuerier`; delete `handler.go:189` and `sync/main.go:58`.
