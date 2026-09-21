# auth-service — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `f2b606d` (base `main`)  
**Overall score:** 3.3 / 5 (baseline 2026-09-01: 3.2, Δ +0.1)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

auth-service is a small, stateless JWT-minting front door whose code is idiomatic and SAST-clean, but its client contract has drifted from its behaviour in ways that overstate the security model. `docs/client-api.md` §2.2 says the JWT scope is "derived server-side from the principal's roles (admin > bot > user)"; the code never reads roles and every path mints the same `scoped_user` template, which `pkg/principal` openly documents as a not-yet-done follow-up. The same section documents only the SSO response shape, while the session-token path returns a NATS-encoded account with every directory field empty, and the dev-mode tokenless body that the frontend actually sends is absent from the request table. Three reviewers independently flagged that `BOTPLATFORM_URL` is re-declared with three different contracts across auth/upload/portal and is missing from auth-service's own compose and `.env.example`, so the README's documented local admin login ends in a 503. Two composition defects follow: dev-mode wiring hands the handler a nil `TokenValidator` that any request carrying `ssoToken` still dereferences (a panic caught by `gin.Recovery`, which itself bypasses the JSON log pipeline), and the request body is bound with no size cap on the one public unauthenticated endpoint. Session revocation by admin-service never reaches NATS, so a minted JWT stays valid up to 2h×1.1. Coverage is 61.9% because the 101-line `run()` inlines every config guard (signing-key type, account key validity, OIDC-required-unless-dev) with no test seam, while the handler half sits at 93%; the three auth branches triplicate the same mint tail, and `integration_test.go` is a unit test in disguise that proves nothing about the real NATS resolver.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 2 |
| Maintainability | 3 |
| Integration | 3 |
| Performance | 4 |

**Findings by severity:** 0 critical, 5 high, 16 medium, 18 low, 10 nitpick (49 total).  
**Highest-risk dimension:** Test coverage (2).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 4

### Evidence

- [medium] Success-path and jitter-fallback logs are ctx-less, so they carry no request_id or trace correlation — `auth-service/handler.go:203`, `:262`, `:293`, `:125` — All four use `slog.Debug`/`slog.Error` instead of the `*Context` variants. The request ID lives only in the ctx (`pkg/ginutil/middleware.go:25`) and in the errcode logger (`handler.go:136`), so a successful mint logs an `account` with no `request_id`/`trace_id`, contrary to CLAUDE.md "include in all log lines"; `ginutil.AccessLog` (`middleware.go:67`) shows the correct `InfoContext` form.
- [medium] Panic recovery bypasses the JSON/OTLP log pipeline — `auth-service/main.go:115` — `gin.Recovery()` writes stack traces through gin's ANSI text logger to stderr, not `log/slog`. Same in all 9 Gin services (repo-wide convention), but for an auth front door an unparseable panic trace is an incident-time gap.
- [low] Key-derivation error discarded — `auth-service/main.go:63-65` — `signingKP.PublicKey()` error is folded into a generic message with no `%w`, so a genuine nkeys failure is reported as "not an account-type signing key". Violates "never ignore errors silently / always wrap".
- [low] All JSON-bind failures collapse to `missing_fields` / "natsPublicKey is required" — `auth-service/handler.go:139-142` — malformed JSON or wrong content-type yields the same envelope, which misleads client debugging; `ShouldBindJSON` distinguishes syntax errors from validation errors.
- [low] Session-branch account gate has a divergent, undocumented message and no reason — `auth-service/handler.go:249` vs `:190`/`:280` — "account contains invalid characters" differs from the sibling gates and from the documented row in `docs/client-api.md` (§2.2 error table lists only the "single NATS subject token" wording); none of the three carries a `WithReason`, so a client cannot branch on it.
- [low] Exported surface in `package main` — `auth-service/handler.go:27`, `:57`, `:63`, `:75-104`, `:135`, `:354` — `AuthHandler`, `NewAuthHandler`, `TokenValidator`, `BotplatformValidator`, `Option`, `With*`, `HandleAuth`, `HandleHealth` are exported though nothing imports a main package; CLAUDE.md says keep handler implementations unexported. Same drift in `tcard-service`/`portal-service`, so treat as repo-wide.
- [nitpick] Magic context key duplicated — `auth-service/handler.go:136` — `c.GetString("request_id")` re-spells ginutil's private key; `natsutil.RequestIDFromContext(c.Request.Context())` already holds the same value (`middleware.go:25`).
- [nitpick] Sentinel compared with `!=` rather than `errors.Is` — `auth-service/main.go:147` — correct today (ListenAndServe returns the bare sentinel) but off the repo's stated idiom.

### Recommendations

- [medium] Switch the four ctx-less log calls to `slog.DebugContext(ctx, …)` / `slog.ErrorContext` — `handler.go:125,203,262,293` — restores trace/identity correlation via the o11y handler; optionally add `"request_id", natsutil.RequestIDFromContext(ctx)` explicitly.
- [medium] Add a `ginutil.Recovery()` that uses `gin.CustomRecoveryWithWriter(io.Discard, fn)` and logs the panic with `slog.ErrorContext` + stack — `main.go:115` — one shared helper fixes all 9 Gin services; adopt here first.
- [low] Wrap the `PublicKey()` error: `return fmt.Errorf("derive signing key public key: %w", err)` and keep the account-type check as a separate branch — `main.go:63-65`.
- [low] Distinguish bind failures: on `*json.SyntaxError`/`*json.UnmarshalTypeError` return `BadRequest("malformed JSON body")`, keep `missing_fields` for validation — `handler.go:139`.
- [low] Extract the three account-format rejections into one package-level sentinel with a `WithReason(errcode.AuthInvalidAccount)` (add to `codes_auth.go`), use it at `:190`, `:249`, `:280`, and document the reason in `docs/client-api.md` §2.2.
- [low] Log a startup `slog.Warn` when `TLS_SKIP_VERIFY=true` in non-dev mode — `main.go:96-100` — mirrors the DevMode warning at `:88` so an insecure OIDC transport is visible in production logs.
- [nitpick] Unexport the handler type/ctor/options (`authHandler`, `newAuthHandler`, …) or leave as a repo-wide decision — `handler.go:63`.
