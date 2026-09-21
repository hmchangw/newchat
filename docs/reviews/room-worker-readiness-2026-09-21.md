# room-worker — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `421c933` (base `main`)  
**Overall score:** 3.2 / 5 (baseline 2026-09-01: 3.3, Δ -0.1)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

The ROOMS consumer pattern, OUTBOX ordered-lane federation, `pkg/subject` usage, dedup-id helpers and shutdown order all match CLAUDE.md, and code quality is strong. The high finding is a client contract break: `publishAsyncJobResult` emits `status:"error"` for any non-nil error before `HandleJetStreamMsg` NAKs a transient one for retry, so a Mongo blip tells the client the job failed and minutes later that the same `requestId` succeeded — `docs/client-api.md` describes the result as terminal. Performance carries the real cost: `GetRoom`, `ListByRoom` and `GetSubscription` fetch whole documents (the first drags the room's private key on every DM redelivery), the Teams reconcile does a per-member `GetUser` and a per-member synchronous PubAck where a batch call already exists, no per-job deadline exists so a wedged Mongo call leaks a `MAX_WORKERS` slot, and the writer's own L1 room-meta cache is never invalidated after a rename. The ROOMS durable has no `FilterSubjects`, so every mute toggle from room-service is a wasted delivery plus a WARN. Coverage is 62.7%, and the `HandleJetStreamMsg` dispatcher — the only production entry point — is 0% at every layer.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 3 |
| Test coverage | 2 |
| Maintainability | 3 |
| Integration | 4 |
| Performance | 3 |

**Findings by severity:** 0 critical, 5 high, 22 medium, 19 low, 6 nitpick (52 total).  
**Highest-risk dimension:** Test coverage (2).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 4

### Evidence

- [medium] Poison messages on the unmarshal-failure paths are Acked with zero log lines — `room-worker/handler.go:284` — `HandleJetStreamMsg` settles with `jsretry.SettleQuiet` on the premise that `fillAsyncError → errcode.Classify` already logged, but `publishAsyncJobResult` returns before `Classify` when `requesterAccount == ""` (`handler.go:177`), which is exactly the state after a failed `json.Unmarshal` (`handler.go:304`, `:870`, `:1556`). `SettleQuiet` itself logs nothing for a permanent error (`pkg/jsretry/jsretry.go:126-135`). The Teams lane has no async-result publish at all, so `teamsroomcreate.go:29` and `handler.go:273` drop a malformed batch silently. Operators cannot see a corrupt producer.
- [high] Bare `err` returned from the store — `room-worker/store_mongo.go:767`, `:771` — `FindDMSubscriptionPair` returns the raw driver error from `Find`/`cursor.All` with no wrapping; CLAUDE.md §3 "never return bare err". Also `handler.go:386` (`appNameLookup`) and `store_mongo.go:194` (`GetRoomMeta`) pass through unwrapped. Every other store method wraps correctly.
- [medium] Client-facing error message interpolates the JSON decoder error — `room-worker/handler.go:2330` — `errcode.BadRequest(fmt.Sprintf("unmarshal rename request: %s", err.Error()))` ships `json.SyntaxError`/`UnmarshalTypeError` text (which embeds payload fragments/field names) into `AsyncJobResult.Error` to the requester. The sibling path at `handler.go:1556-1559` explicitly documents why this must not be done; the two are inconsistent.
- [medium] Request-ID/trace correlation lost on seven log lines — `room-worker/handler.go:118`, `:2333`, `:2449`, `:2457`, `:2472`, `:2485`, `:2607` — ctx-less `slog.Error`/`slog.Info` while every other line in the file uses `*Context`. The base handler is the o11y trace-correlated handler (`pkg/logctx/handler.go:68-76` delegates to it), so these lines carry neither `trace_id` nor `request_id`. `:2333` additionally uses camelCase keys (`roomID`, `requestID`) against the file's `room_id`/`request_id` convention. CLAUDE.md §3: "include [request ID] in all log lines".
- [low] Log-and-return at the sync-DM collision — `room-worker/handler.go:2126-2130` — `slog.ErrorContext(... reconcileErr)` then returns `errRoomIDCollision`, which the router classifies and logs again; double-logs one event (ERROR + INFO). The comment acknowledges the choice; a `WithCause`/`WithLogValues` on the returned error would keep the detail in a single Classify line.
- [low] 22 silently discarded `json.Marshal` errors — `room-worker/handler.go:193`, `:477`, `:492`, `:505`, `:557`, `:693`, `:1213`, `:1220`, `:1893`, `:1901`, `:1923`, `:1373` (`mustMarshal`, whose name promises a panic it does not deliver) — CLAUDE.md §3 "never ignore errors silently — comment if intentionally discarded". Same file uses `errcode.MarshalFailed` correctly at `:1944`, `:2007`, `:2355`, and `teamsroomcreate.go` does so throughout; the two styles coexist with no comment explaining why.
- [low] Post-constructor dependency injection — `room-worker/main.go:268-283` — `publishUsers`, `dekProvisioner`, `valkey`, `reconcileTTL` are assigned onto the handler after `NewHandler`, so the constructor does not describe the handler's real dependency set (`handler.go:88-100`); CLAUDE.md §3 "dependencies injected via constructor".
- [nitpick] Dead code / dead alias — `room-worker/handler.go:2321-2326` re-validates the request ID that `runJobWithRecovery` already guarantees valid (`main.go:455`, `pkg/idgen/idgen.go:179-184`), so both branches are unreachable; `handler.go:43` `errPermanent` has zero production references (20 test-only uses).
- [nitpick] Exported service-internal types — `room-worker/handler.go:59`, `store_mongo.go:23`, `store.go:64,169,182` — `Handler`, `MongoStore`, `SubscriptionStore`, etc. are exported in `package main`; CLAUDE.md §3 "keep handler/store implementations unexported within services". Repo-wide pattern, not service-specific.

### Recommendations

- [medium] In `HandleJetStreamMsg`, use `jsretry.Settle` (not `SettleQuiet`) for the Teams lane and whenever `publishAsyncJobResult` will short-circuit — or have `publishAsyncJobResult` run `Classify` before its early return — `handler.go:177`, `:284` — so every dropped poison message produces exactly one log line.
- [high] Wrap the three bare returns: `fmt.Errorf("find DM subscription pair for room %q: %w", roomID, err)` — `store_mongo.go:767,771`; wrap `handler.go:386` and `store_mongo.go:194` likewise.
- [medium] Replace `handler.go:2330` with the constant message used at `:1559` (`errcode.BadRequest("unmarshal rename request")`) so decoder text never reaches the async reply.
- [medium] Convert the seven ctx-less `slog.*` calls to `*Context` variants with `room_id`/`request_id` keys — `handler.go:118,2333,2449,2457,2472,2485,2607`.
- [low] Standardise on `errcode.MarshalFailed` for outbound event marshals (or annotate `// marshal of a fixed struct cannot fail` once, at `mustMarshal`) and route the `_ :=` sites through it — `handler.go:1372-1375`.
- [low] Fold `publishUsers`/`dekProvisioner`/`valkey`/`reconcileTTL` into `NewHandler` (functional options if churn is the concern) — `main.go:268-283`.
- [nitpick] Delete the unreachable request-ID checks at `handler.go:2321-2326` and move `errPermanent` into a `_test.go` file.
