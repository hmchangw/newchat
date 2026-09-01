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

---

## 3. Architecture — 3 / 5

The core binary is exemplary DI (consumer-owned interfaces, constructor injection, compile-time store assertion, correct `shutdown.Wait` ordering), but the contract with notification-worker's push gate does not exist, and the nested `sync/` binary re-declares shared knobs and bypasses `valkeyutil`.

### Findings
- `high` — notification-worker's push gate requests `chat.presence.{siteID}.request.snapshot` (`pkg/subject/subject.go:1729-1732`, used at `notification-worker/presence.go:61`), but user-presence-service registers only `PresenceQueryBatch`/`PresenceQueryBatchPeer`/the four `chat.user.…presence` patterns (`user-presence-service/main.go:172-178`). No handler for `PresenceSnapshot` exists anywhere in the repo. The gate is dead-by-flag (`PRESENCE_RPC_ENABLED` default `false`, `notification-worker/main.go:65`); flipping it yields a 2s timeout per chunk and fail-open on every message.
- `high` — the payload contract is also incompatible, not just the subject: the gate marshals `model.PresenceSnapshotRequest` and expects `PresenceSnapshotReply{Presences map[string]Presence{aggregatedStatus}}` (`pkg/model/presence.go:3-16`), while the service speaks `PresenceQuery` → `PresenceQueryResponse{States []PresenceState{status}}` (`pkg/model/presence.go:78-95`). One side must be adapted; no adapter exists. The working consumer, `user-service/presenceclient/client.go:36`, uses the real `PresenceQueryBatchPeer` subject — the presence service's contract is fine, notification-worker's is invented.
- `high` — batch limits disagree across the same boundary: `PRESENCE_BATCH_SIZE` defaults to 512 (`notification-worker/main.go:63`) while `PRESENCE_BATCH_MAX` defaults to 100 (`user-presence-service/main.go:40`), and the handler hard-rejects over-limit batches (`user-presence-service/handler.go:167-169`). Even after fixing subject and payload, the default wiring would `BadRequest` every full chunk.
- `high` — `sync/` dials Valkey with a raw `redis.NewClusterClient` and re-declares `VALKEY_ADDRS`/`VALKEY_PASSWORD` itself (`user-presence-service/sync/main.go:31-32,89-91`) instead of `valkeyutil.Config` + `ConnectRaw` as the parent does (`main.go:53,120`). This is the exact duplication `presencestore/store.go:185-190` documents as ended, and it violates the CLAUDE.md shared-knob rule; the sync's Valkey traffic is also uninstrumented and carries a different dial policy against the same keyspace.
- `medium` — `sync/` re-declares `PRESENCE_STALE_THRESHOLD` and `PRESENCE_CONNS_TTL` with its own `envDefault`s (`sync/main.go:23-24`), duplicating `PresenceConfig` (`main.go:42,44`). Both values are fed into the *same* `computeLua` stale-pruning path via `NewValkeyStoreFromClient`; a drift between the two deployments makes `SetExternal` prune connections on a different threshold than the owning service.
- `medium` — a second, independently deployed binary lives inside another service's directory (`user-presence-service/sync/main.go` with its own `deploy/Dockerfile`, `azure-pipelines.yml`, `docker-compose.yml`), contrary to "new service at repo root, not under `cmd/` or `internal/`". Correspondingly `presencestore/` is code shared by two binaries but sits outside `pkg/`.
- `medium` — the sweeper is an unpartitioned global scan: `sweepKey` is a single un-hash-tagged ZSET read with `Count: 500` per tick (`presencestore/store.go:18,319-321`) and every replica runs its own ticker (`main.go:180-186`). There is no leader election or sharding, so N replicas repeat identical work and expiry throughput is capped at ~100 accounts/s at the default 5s interval; a burst of disconnects backlogs rather than shedding.
- `low` — ownership of the Valkey client is split: `main.go:120` dials it, `store.Close()` (`presencestore/store.go:341`) closes it at `main.go:204`. A failure between dial and store construction leaks the client.
- `low` — `PRESENCE_HEARTBEAT_INTERVAL` is parsed and validated (`main.go:41,92`) but never read by any code in the service — a dead operator knob that implies a server-side cadence the service does not enforce.
- `nitpick` — `NewNATSPeerPresenceClient` is exported but returns the unexported `*natsPeerPresenceClient` (`peer_client.go:32`); callers outside `package main` cannot name the type. Either unexport the constructor or export the struct.

### Recommendations
- `high` — Pick one presence RPC contract. Preferred: delete `subject.PresenceSnapshot`, `model.PresenceSnapshotRequest/Reply/Presence` and point `bulkPresenceSource` at `subject.PresenceQueryBatchPeer` with `PresenceQuery`/`PresenceQueryResponse`, mapping `PresenceState.Status` into the gate. Add a cross-service test that asserts the subject the worker requests equals a subject the presence service registers.
- `high` — Align the batch limits after the contract fix: default `PRESENCE_BATCH_SIZE` to the presence service's `BATCH_MAX`, or have `newBulkPresenceSource` clamp; a silent `BadRequest` per chunk is invisible fail-open.
- `high` — Replace `sync/`'s raw cluster dial with `valkeyutil.ConnectRaw(ctx, cfg.Valkey, valkeyutil.Instrumented(sdk))` and mount `valkeyutil.Config` as a named field; delete the two re-declared `VALKEY_*` tags.
- `medium` — Move the stale/conns-TTL knobs into `presencestore` as a single `TTLConfig`-style struct owned there, mounted by both binaries with `envPrefix:"PRESENCE_"`, so the value feeding `computeLua` has one declaration.
- `medium` — Promote the sync to a repo-root service (`user-presence-sync/`) and move `presencestore` to `pkg/presencestore`, restoring the flat-service + shared-code-in-`pkg` convention.
- `medium` — Shard or lease the sweep: hash-tag per-shard sweep ZSETs (`presence:sweep:{n}`) with replicas claiming shards, or run the sweeper as a singleton; and make the `500` cap a config knob with a "sweep backlog" metric.
- `low` — Either wire `PRESENCE_HEARTBEAT_INTERVAL` into a server-side expectation or delete it, and have `main` own the Valkey client's `Close` rather than `store.Close()`.
