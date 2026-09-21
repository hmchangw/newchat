# botplatform-service — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `5439895` (base `main`)  
**Overall score:** 2.8 / 5 (baseline 2026-09-01: 2.8, Δ +0.0)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

botplatform-service is competently built — clean logging, projected reads, a correct breaker predicate, tested middleware — but it sits on a revocation hole that spans two services. Admin-service's deactivate and password-reset paths delete sessions directly inside their transaction and return only an error, so they never hand any session IDs to the cache-busting helper, while this service authenticates every bot request from that same cache with a 90-minute TTL that slides on load error. A deactivated or password-reset bot therefore keeps authenticating for roughly an hour, and indefinitely while MongoDB is down. Three contract problems follow. Two documented member-management endpoints do not exist at the paths the client API publishes, so an SDK written from the doc gets a 404 on both — flagged in the previous audit and still unfixed. Remote error envelopes are re-emitted without validating the code field, which the error package explicitly warns will panic. And `SESSIONS_MAX_PER_ACCOUNT` and `BCRYPT_COST` are re-declared per service, with two independently-defaulted session caps enforcing limits on one shared collection; `BCRYPT_COST` is dead here but still a hard startup gate. Operationally, downstream-unavailable is misclassified as a 500 rather than a retryable 503 because the transport switch is hand-rolled instead of using the shared helper; `/healthz` pings Mongo with no separate readiness probe, so an outage the session cache is designed to survive instead restarts pods; the request deadline sits below the NATS budgets it wraps, making the documented 15-second room-management timeout unreachable; and the idempotency middleware buffers request bodies before the size cap applies. Coverage is 56.5%: the entire NATS forwarding layer, the first-time-DM flow and the routing store are untested at every tier, and the login handler is a drifted clone of admin-service's that has already lost its timing-attack guard.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 3 |
| Test coverage | 1 |
| Maintainability | 3 |
| Integration | 3 |
| Performance | 3 |

**Findings by severity:** 3 critical, 14 high, 23 medium, 16 low, 6 nitpick (62 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 4

### Evidence

- [medium] Bot-route error logs carry no request ID — `botplatform-service/middleware.go:51` — `requireBot` never calls `errcode.WithLogValues` nor rewrites `c.Request` context, so every `Classify` line from the 5 bot endpoints plus the rate-limit/idempotency middlewares (`middleware.go:70,128,136,151`, `middleware_idempotency.go:55,62,74`) logs `request failed` without `request_id`. `HandleLogin`/`HandleValidate` do it (`handler.go:90,194`), as do sibling services in their auth middleware (`user-service/middleware.go:46`, `media-service/middleware_auth.go:33`, `upload-service/middleware.go:140`). CLAUDE.md §3 requires the ID "in all log lines".
- [medium] Idempotency middleware reads the request body unbounded — `botplatform-service/middleware_idempotency.go:60` — `io.ReadAll(c.Request.Body)` runs *before* the handler, and the only size cap (`http.MaxBytesReader`, `bot_handlers.go:193`) lives downstream in `bindStrict`. No global limit is wired (`main.go:118-127`; `ginutil.TimeoutConfig` only sets a deadline). An authenticated bot can make the process buffer an arbitrary body per request.
- [medium] The 15s room-management budget is unreachable — `botplatform-service/bot_forwarder.go:75` — `forwardRoomMgmt` derives `context.WithTimeout(ctx, 15*time.Second)` and `newNATSDMEnsurer` gets 15s (`main.go:116`), but the parent request context is already capped by `ginutil.TimeoutConfig.RequestTimeout`, `envDefault:"10s"` (`pkg/ginutil/timeoutconfig.go:18`, not overridden in `deploy/docker-compose.yml`). Those calls always die at 10s; the comments at `bot_forwarder.go:55` / `main.go:115` and the TTL sizing rationale at `config.go:54` all assert 15s.
- [low] Idempotency sentinel release uses the (possibly cancelled) request context — `botplatform-service/middleware_idempotency.go:89` — `ctx` is captured at `:51` and reused after `c.Next()`. On a 2xx whose client disconnected, `Del` fails and the key is held for the full TTL, so honest retries get 409 `bot_in_flight` for up to 60s.
- [low] Error text is string-matched and a raw decoder message is returned to the client — `botplatform-service/bot_handlers.go:211` — `strings.Contains(msg, "unknown field")` is exactly the string-comparison CLAUDE.md §3 forbids, and `:212` writes the stdlib decoder's own text onto the wire. A test pins the behaviour (`bot_handlers_test.go:76`), which contains the drift risk; `encoding/json` exposes no typed error here. Also `:219` compares `err != io.EOF` instead of `errors.Is`, and the `*json.SyntaxError` variable at `:206` is misnamed `unknownErr`.
- [low] Bare `err` returns — `botplatform-service/store_mongo.go:99` and `main.go:154` — both return the callee's error unwrapped; CLAUDE.md §3 requires `fmt.Errorf("<what this fn was doing>: %w", err)`.
- [low] Infra failures dressed as `errcode.Internal` instead of a raw wrapped error — `botplatform-service/middleware.go:70` — CLAUDE.md's Tier-1 rule says an infra failure returns `fmt.Errorf(...: %w)` and collapses to `internal` at the boundary. The same service does it the documented way for the identical operation at `handler.go:210` (`fmt.Errorf("find session: %w", err)`). Same pattern at `middleware.go:128,136,151`, `middleware_idempotency.go:55,62,74`, `bot_forwarder.go:84,141`.
- [low] Dependencies poked in after construction rather than injected — `botplatform-service/main.go:106,114,116` — `newHandler` (`handler.go:46`) takes only the store; `subs`, `forwarder` and `dmEnsurer` are assigned as fields, so a wiring omission is a nil-deref at request time instead of a compile/startup error. CLAUDE.md: "Handler structs hold dependencies injected via constructor".
- [nitpick] Log-and-return on the denial path — `botplatform-service/handler.go:180` — `slog.WarnContext` then `errhttp.Write`, which runs `Classify` and logs again (`pkg/errcode/classify.go:40`). CLAUDE.md says never log AND return, but `admin-service/login.go:145` does the same, so this is a repo-wide idiom, not a local slip.
- [nitpick] `accessLogMiddleware` (`botplatform-service/middleware.go:23`) is a copy of `ginutil.AccessLog` (`pkg/ginutil/middleware.go:62`) plus three fields; `subscription_store.go:17` has a `model.model.` typo.

### Recommendations

- [medium] Set `errcode.WithLogValues(ctx, "request_id", c.GetString("request_id"))` and push it onto `c.Request` inside `requireBot` — `middleware.go:51` — one line restores per-log correlation across the whole bot surface, matching `user-service/middleware.go:46`.
- [medium] Wrap the idempotency read in `http.MaxBytesReader(c.Writer, c.Request.Body, botRequestBodyMaxBytes)` — `middleware_idempotency.go:60` — reuses the constant already defined at `bot_handlers.go:190` and closes the unbounded buffer.
- [medium] Reconcile the HTTP request deadline with the room-mgmt budget — either raise `REQUEST_TIMEOUT` above 15s in `deploy/docker-compose.yml` + the prod manifest, or make `forwardRoomMgmt`'s timeout a config field like `f.timeout` — `bot_forwarder.go:75` — today the documented 15s never applies.
- [low] Release the sentinel with `context.WithoutCancel(ctx)` — `middleware_idempotency.go:89` — prevents a disconnected client from locking its own opID for the TTL.
- [low] Wrap the two bare returns (`store_mongo.go:99`, `main.go:154`) and switch `bot_handlers.go:219` to `errors.Is(err, io.EOF)`; return a fixed message at `:212` rather than the decoder's text.
- [low] Move `subs`/`forwarder`/`dmEnsurer` into `newHandler`'s signature — `handler.go:46` — makes incomplete wiring a compile error.
- [nitpick] Factor the three extra fields into `ginutil.AccessLog` (optional attrs) and drop the local copy — `middleware.go:23`.
