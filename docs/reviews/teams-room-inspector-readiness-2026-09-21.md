# teams-room-inspector — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `1f1d314` (base `main`)  
**Overall score:** 3.5 / 5 (baseline 2026-09-01: 3.3, Δ +0.2)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

`teams-room-inspector` is the smallest and cleanest service in this audit: nine files, 774 lines, one read-only HTTP endpoint that answers `teams-room-verify` with "does this room exist and how many subscriptions does it have". It scores 4 on five of six dimensions — every one but coverage — with zero TODOs, no function over 51 lines, correct `pkg/errcode` Tier-1 usage behind a single `errhttp.Write` adapter, explicit projections, no `$lookup`, a request-body cap, and exactly two Mongo queries for any batch size up to the 500-id cap. `handler.go`, `routes.go` and `newServer` are at 100% coverage. Its 3.2 is almost entirely a coverage artefact plus a set of shared-knob omissions.

The one finding every reviewer raised independently is the **room-id derivation, duplicated by hand across two binaries**. Both `handler.go:68` and `room-worker/teamsroomcreate.go:62` inline `idgen.DeterministicID([]byte(chatID))`, and both carry a comment saying the two must be kept in step by hand. The repo already has the right pattern next door — `pkg/teamsmigrate.EmployeeIDFromGraphID` wraps exactly this call so HR-sync and migration cannot drift. Drift here is silent and self-sustaining: every chat comes back `missing_room`, `teams-room-verify` logs mismatches forever, `needVerify` never clears, and the flagged set grows without bound while both jobs report healthy runs. The existing test cannot catch it, because it recomputes the expected id with the same call rather than pinning a literal.

Three shared knobs the rest of the fleet mounts are missing here, and they compound into the same failure. `mongoutil.PoolConfig` is not mounted, so the client keeps the driver's 30-second server-selection default instead of the repo's 2 seconds — a quiet Mongo becomes a 30-second hang rather than a fast, reportable failure. `ginutil.TimeoutConfig` is not wired either, so the handler context has no deadline at all: `http.Server`'s Read/WriteTimeout bound the socket, not the query. Together, an unreachable secondary pins every handler goroutine and its pooled connection for the caller's whole 30-second budget, and since the caller runs 8 concurrent batches, one slow site consumes an entire verify pass. Five sibling Gin services mount both.

Two contract gaps worth noting. The store filters on `_id`/`roomId` only, although the service is configured with `SITE_ID` and rooms are replicated to non-owning sites carrying the origin site's id — so a chat asked at the wrong site is answered `roomExists: true` with a partial subscription count rather than "not here", and the misroute guard is advisory only. And neither end of the only service-to-service hop in the pipeline propagates `X-Request-ID` or `traceparent`, so `ginutil.RequestID` mints a fresh id per call and a mismatch logged by the verifier can never be joined to the inspector's access log for the same batch. The inspector's extraction side is correct; what is missing is the caller's half of the contract.

Coverage is 47.7%, the lowest of the Teams family, and all 46 uncovered statements sit in `main.go` (29) and `store_mongo.go` (17). The store's tests exist, are fully compliant, and run in no pipeline. `run()` is 10.3% because the dial/serve/shutdown wiring was never given the seam that `newServer` was — extracting it closes roughly 26 of the 46 and lifts the package over the floor on its own.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 1 |
| Maintainability | 4 |
| Integration | 4 |
| Performance | 4 |

**Findings by severity:** 1 critical, 2 high, 14 medium, 13 low, 7 nitpick (37 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 4

### Evidence

- [medium] The chat-id → room-id derivation is copy-pasted across two services with no shared helper — `teams-room-inspector/handler.go:68` and `room-worker/teamsroomcreate.go:62` — both call `idgen.DeterministicID([]byte(chatID))` inline, and the comments on both sides (`handler.go:44-46`, `teamsroomcreate.go:56-59`) state the two "must be changed together" by hand. `pkg/teamsmigrate` already hosts exactly this kind of derivation for the sibling case (`EmployeeIDFromGraphID`, `pkg/teamsmigrate/teamsmigrate.go:62-67`), so the right home exists and was not used. A divergence is silent: every chat reports `roomExists:false` and the verifier just logs mismatches forever.
- [medium] No request deadline on the Mongo path — `teams-room-inspector/handler.go:71` passes `c.Request.Context()` (no timeout) into a find plus an aggregate over up to 500 ids (`store_mongo.go:47,56`). `newServer` wires no `ginutil.Timeout`/`TimeoutConfig` (`main.go:50-57`) although that shared knob exists and peers mount it (`tcard-service/main.go:120`, `botplatform-service/main.go:124`), and `mongoutil.ConnectRead` sets no client operation timeout (`pkg/mongoutil/mongo.go:271-284`). `http.Server.WriteTimeout` closes the connection but does not cancel the query, so a stalled secondary parks goroutines and connections for the whole 25s drain.
- [low] The `errcode` log line carries no request id — `handler.go:53,57,61,73` call `errhttp.Write`, which runs `errcode.Classify`; that logs through `loggerFrom(ctx)` = `slog.Default()` (`pkg/errcode/classify.go:40`) and nothing put `request_id` on that logger. Peers call `errcode.WithLogValues(ctx, "request_id", c.GetString("request_id"))` at handler entry (`auth-service/handler.go:136`, `botplatform-service/handler.go:89`). CLAUDE.md §3 asks for the correlation id in all log lines; here `request failed` and the access log line (`pkg/ginutil/middleware.go:67-74`) must be joined by trace id instead.
- [low] Sentinel compared with `!=` instead of `errors.Is` — `main.go:115` (`err != http.ErrServerClosed`). Not a live bug (`ListenAndServe` returns it unwrapped) and the repo is split, but the newer peers this file was modelled on use `errors.Is` (`admin-service/main.go:174`, `media-service/main.go:143`); `errorlint` is not enabled in `.golangci.yml`, so nothing catches a future wrapping change.
- [nitpick] The bind error is discarded and every rejection collapses to one message — `handler.go:52-55` — `err` from `ShouldBindJSON` is never used and no `errcode.WithCause(err)` is attached, so a malformed body, a wrong type and a `MaxBytesReader` cutoff all log and return the identical `decode verify request`. Discarding it matches house style (`admin-service/permissions.go:143`), but here it is the only diagnosis the caller's CronJob gets.
- [nitpick] An oversized body answers 400, not 413 — `handler.go:50-55` — `http.MaxBytesReader` is correctly installed, but its error is indistinguishable from a parse failure at the `ShouldBindJSON` boundary, and `handler_test.go:120` pins 400. A `*http.MaxBytesError` check would let the caller tell "batch too big" from "bad JSON".

### Recommendations

- [medium] Move the derivation into `pkg/teamsmigrate` as `RoomIDFromChatID(chatID string) string` and call it from both `handler.go:68` and `room-worker/teamsroomcreate.go:62` — turns a hand-maintained cross-service invariant into a single function with one test, beside the sibling `EmployeeIDFromGraphID`.
- [medium] Mount the shared HTTP timeout knob (`ginutil.TimeoutConfig` field + `cfg.HTTP.Middleware()`) in `newServer` — `main.go:50-57` — bounds the verify handler so a slow secondary sheds load instead of accumulating goroutines; keep it under the 30s `WriteTimeout`.
- [low] Add `ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))` at the top of `HandleVerify` — `handler.go:48` — one line, matches auth/botplatform, and makes the `request failed` record self-correlating.
- [low] Attach the bind error as a server-only cause: `errcode.BadRequest("decode verify request", errcode.WithCause(err))` — `handler.go:53` — the cause is never serialized (`pkg/errcode/error.go`, `classify.go:19-21` logs it as `underlying`), so the operator learns why a batch 400'd at no client-exposure cost.
- [low] Use `errors.Is(err, http.ErrServerClosed)` — `main.go:115` — aligns with the peer mains and survives any future wrapping.
- [nitpick] Distinguish the oversized-body case with `errors.As(err, &maxErr)` (`*http.MaxBytesError`) and return 413 — `handler.go:52` — lets `teams-room-verify` detect a misconfigured `VERIFY_BATCH_SIZE` rather than reading it as malformed JSON.
