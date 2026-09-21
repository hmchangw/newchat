# admin-service — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `8eaf4a7` (base `main`)  
**Overall score:** 3.3 / 5 (baseline 2026-09-01: 3.2, Δ +0.1)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

A well-structured admin HTTP surface — routes in `routes.go`, consumer-defined store, explicit projections on every read, no `$lookup`, streamed uploads with body caps, the documented HTTP shutdown order — with one high-severity security gap that two reviewers reached independently: password reset, self-service password change and account deactivation revoke sessions through a hand-rolled `DeleteMany` inside the transaction that returns no ids, so unlike the explicit revoke endpoints they never call `sessioncache.Bust*`, and a reset or deactivated user's token keeps authenticating from the consumer-side cache for the refresh window (~67 min at defaults, indefinitely during a Mongo outage). Two operational gaps follow it: `authenticate` collapses every `FindByHash` error to 401, so a Mongo outage logs the whole console out and pages as auth failures, and `user_account_updated`/`user_permissions_updated` publish straight to remote INBOX lanes with no OUTBOX retry, so a permission revoke that misses a down peer stays effective there until an operator runs resync. `SearchUsers` executes two unanchored case-insensitive regex scans over every site's users per console keystroke, with no sort, so pages repeat rows under HR-sync writes. Coverage is 68.2% — 93% outside the integration-only store — but five handlers lack the malformed-body test CLAUDE.md requires and nothing enforces the floor in any pipeline.

| Dimension | Score |
|---|---|
| Code quality | 3 |
| Architecture | 3 |
| Test coverage | 2 |
| Maintainability | 4 |
| Integration | 4 |
| Performance | 4 |

**Findings by severity:** 0 critical, 4 high, 12 medium, 21 low, 9 nitpick (46 total).  
**Highest-risk dimension:** Test coverage (2).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 3

### Evidence

- [high] Password reset / self change-password / deactivate revoke sessions in Mongo but never bust the session cache — `admin-service/handler.go:701`, `admin-service/login.go:201`, `admin-service/handler.go:483` — `UpdateUserPasswordAndRevoke` and `DeactivateAndRevoke` (`store_mongo.go:376`, `:402`) `DeleteMany` the shared sessions collection inside the transaction and return no ids, so unlike `revokeAllSessions`/`revokeSession` (`handler.go:599`, `:620`) no `sessioncache.Bust*` follows. `pkg/sessioncache/sessioncache.go:119-121` and the service's own `revokebust_test.go:27-32` state the consequence: a revoked token keeps authenticating from cache (~67 min at the default refresh window, indefinitely during a Mongo outage) at the consumer that caches sessions (`botplatform-service/store_mongo.go`). A forced reset or account deactivation is exactly the case where that matters.
- [medium] Any `FindByHash` failure collapses to 401 `invalid session token` — `admin-service/middleware.go:424-430` — `session.ErrNotFound` exists (`pkg/session/session.go:49`) precisely so "a caller cannot mistake an outage for an invalid token" (`pkg/sessioncache/sessioncache.go:135-137`), but the middleware never `errors.Is`-checks it. A Mongo outage therefore logs every admin request at INFO as an auth failure (`Classify` logs `Unauthenticated` at Info, `pkg/errcode/classify.go:44-50`) and the console logs everyone out. `middleware_test.go:87-91` only pins the `ErrNotFound` path, so the outage behaviour is not a deliberate contract.
- [low] Request ID reaches the `Classify` boundary log in only two handler files — `admin-service/login.go:59`, `admin-service/room_onduty.go:349` seed `errcode.WithLogValues(ctx, "request_id", …)`; every handler in `handler.go` (e.g. `:297`, `:379`, `:670`), `permissions.go:139`, `rooms.go:261`, `client_update.go:296` use the bare request ctx. `ginutil.RequestID` stores the id in ctx (`pkg/ginutil/middleware.go:24-25`) but nothing in `pkg/obs` copies it onto slog lines, so "request failed" lines from most endpoints carry only the trace id, contrary to CLAUDE.md "include in all log lines".
- [low] Bare `err` returns — `admin-service/config.go:278`, `admin-service/store_mongo.go:346`, `admin-service/store_mongo.go:409` — `return Config{}, err` / `return err` / `return nil, err` with no context, against the CLAUDE.md "never return bare err" rule (callers wrap, so no information is lost, but the pattern is inconsistent with the rest of the file).
- [low] `run()` calls `slog.Error` + `os.Exit(1)` instead of returning — `admin-service/main.go:79-81` — the only exit inside `run()`; it bypasses the `run() error` contract and skips `obsShutdown`, whereas the neighbouring config/connect failures return wrapped errors.
- [low] Outbound HTTP drives `net/http` directly — `admin-service/client_update.go:39-43`, `:81`, `:90` — CLAUDE.md says "never `net/http` client directly". The deviation is justified inline (resty v2 buffers non-replayable bodies) and the transport still comes from `restyutil`, but the exception lives only in a comment; nothing in `CLAUDE.md` or `pkg/restyutil` sanctions it, so a future lint/semgrep rule on `http.NewRequestWithContext` would flag it.
- [nitpick] `nowMillis` is a mutable package-level var "so tests can stub" — `admin-service/handler.go:77-79` — no test references it (grep across `admin-service/*_test.go`: zero hits), and sibling code uses `time.Now().UTC()` directly (`permissions.go:176`, `handler.go:266`). Dead indirection plus a global that would race if tests ever went parallel.
- [nitpick] Same failure class logged at different levels — audit append failure: Warn at `login.go:216`, Error at `handler.go:267` and `permissions.go:286`; INBOX publish failure: Warn at `handler.go:215`, Error at `permissions.go:384`. Alert rules keyed on level will see half the events.
- [nitpick] `fmt.Errorf` with constant strings — `admin-service/room_onduty.go:411`, `:428` — `errors.New` is the idiom; harmless but inconsistent with `permissions.go:86`.

### Recommendations

- [high] Return the deleted session `_id`s from `UpdateUserPasswordAndRevoke`/`DeactivateAndRevoke` (find-then-DeleteMany inside the same transaction, as `session.MongoStore.DeleteBeyondCap` does) and call `sessioncache.BustMany` at `handler.go:701`, `handler.go:483`, `login.go:201` — closes the revoked-token window the service already closes for explicit revokes; add a `revokebust_test.go` case per path.
- [medium] In `authenticate` (`middleware.go:424`) branch on `errors.Is(err, session.ErrNotFound)` → 401; anything else → `fmt.Errorf("find session: %w", err)` so it classifies as `internal` (ERROR log, 500) — an outage then looks like an outage; add a table row with a non-sentinel error to `middleware_test.go`.
- [low] Seed `request_id` once, either in `applyBaseMiddleware` (`main.go:42`) by wrapping the request ctx with `errcode.WithLogValues` after `ginutil.RequestID()`, or as a `ginutil` middleware shared by all Gin services — removes the per-handler drift and makes every `Classify` line correlatable.
- [low] Wrap the three bare returns (`config.go:278`, `store_mongo.go:346`, `:409`) with `%w` context, and make `main.go:79` `return fmt.Errorf("parse mongo read preference: %w", err)`.
- [low] Either promote the streaming-upload exception into `pkg/restyutil` (e.g. a `StreamingClient` helper) or record it in CLAUDE.md next to the Resty rule so the `net/http` usage in `client_update.go` is a sanctioned pattern rather than a per-file comment.
- [nitpick] Drop `nowMillis` in favour of `time.Now().UTC().UnixMilli()` (or actually stub it in tests), and align the audit/publish failure log levels to one choice per class.
