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

## 3. Architecture — score 4

### Evidence

- [medium] Dev-mode wiring hands the handler a nil `TokenValidator`, but any request carrying `ssoToken` is still routed to it — `auth-service/main.go:89` + `auth-service/handler.go:323-338` — `NewAuthHandler(nil, …, true)` then `case req.SSOToken != "": h.handleSSO` calls `h.validator.Validate` on a nil interface: panic, caught by `gin.Recovery()` (`main.go:115`) as a 500 with no access-log line. The doc comment at `handler.go:299-302` promises "a dev-mode request carrying a token still validates normally", and the only test of that path (`handler_test.go:364-383`) injects a non-nil validator, so the real `main.go` composition is untested. Dev-only (`DEV_MODE=false` in prod), hence medium not high.
- [medium] The OIDC/botplatform config trio is re-declared per service instead of owned by `pkg/oidc`/`pkg/botauth` — `auth-service/main.go:34-36,43`, `upload-service/main.go:84-90`, `user-service/config/config.go:122-126,174` — and has already drifted: user-service reads `OIDC_TLS_SKIP_VERIFY`, the others `TLS_SKIP_VERIFY`; `BOTPLATFORM_URL` is optional here, `required,notEmpty` in upload/media, defaulted in portal. The "required when DEV_MODE is false" check is copy-pasted (`auth-service/main.go:91`, `upload-service/main.go:161`). `pkg/oidc.Config` (`pkg/oidc/oidc.go:147-154`) is the natural owner but carries no env tags — the CLAUDE.md shared-knob rule applied to a non-TTL knob.
- [low] `/readyz` is registered with zero checks, so it is byte-for-byte `/healthz` — `auth-service/routes.go:14` — while `docs/health-probes.md:11` defines readiness as NATS connectivity, which this service has none of. Process-only readiness is defensible for a stateless minting service, but it is undocumented and gives ops a probe that can never flip.
- [low] No per-request context deadline: sibling Gin services mount `ginutil.TimeoutConfig` (`portal-service/main.go:65,157`, `botplatform-service/main.go:124`, `tcard-service/main.go:43`); auth-service sets only socket timeouts (`auth-service/main.go:124-125`). The handler blocks on outbound OIDC JWKS refresh (10s client timeout, `pkg/oidc/oidc.go:165`) and botplatform validate; a stalled upstream outlives the 10s `WriteTimeout` with nothing cancelling the handler goroutine.
- [low] `handleSession` re-implements `botauth`'s error mapping — `auth-service/handler.go:396-407` duplicates `pkg/botauth/botauth.go:167-183` (`errors.As` → forward, else `Unavailable`+`WithCause`). Two places to change when the upstream contract shifts; `Authenticate` can't be reused because auth-service has no `x-user-id` to match.
- [nitpick] Handwritten `HandleHealth` sits beside the shared `health.ReadinessHandler` — `auth-service/routes.go:12-14`, `handler.go:522-524` — while `health.LivenessHandler()` (`pkg/health/health.go:43`) exists for exactly this.
- [nitpick] Doc drift: `CLAUDE.md:15` and `docs/architecture.md:300` describe auth as a "NATS callout service"; the service mints scoped user JWTs signed by an account signing key (`handler.go:490-497`) and nothing in the tree implements a callout.
- [nitpick] `auth-service/deploy/.env.example` omits `DEV_MODE`, `BOTPLATFORM_URL` and `NATS_JWT_EXPIRY_JITTER`, so the session-token branch is invisible to someone standing the service up from the template.

### Recommendations

- [medium] Close the nil-validator hole — `auth-service/handler.go:337-338` — either guard `if h.validator == nil { errhttp.Write(ctx, c, errcode.Unavailable("sso auth not configured", …)); return }` (mirroring the `bpValidator == nil` guard at `:390-394`) or wire a dev validator in `main.go`; add a unit test that constructs the handler exactly as `main.go:89` does (nil validator, devMode, `ssoToken` set) so the composition root is covered.
- [medium] Give the shared knobs one owner — add an env-tagged `oidc.EnvConfig{IssuerURL, Audiences, TLSSkipVerify}` with `Validate(required bool)` in `pkg/oidc` and a `botauth.Config{URL}` in `pkg/botauth`; mount them as named fields in auth/upload/user-service. Removes the name drift and the three copies of the DEV_MODE gate.
- [low] Mount `ginutil.TimeoutConfig` (`HTTP ginutil.TimeoutConfig` in config, `r.Use(cfg.HTTP.Middleware())` after `AccessLog`) — `auth-service/main.go:117` — so upstream stalls cancel the handler's context like every other Gin service.
- [low] Make readiness honest — `auth-service/routes.go:14` — either add a `health.Check` that the OIDC verifier's keyset loaded (or botplatform is reachable when configured), or state in `docs/health-probes.md` that auth-service readiness is process-only by design.
- [low] Extract `botauth.MapValidateError(err) error` and call it from both `Authenticate` and `handleSession` — `pkg/botauth/botauth.go:167-183`, `auth-service/handler.go:396-407`.
- [nitpick] Replace `HandleHealth` with `gin.WrapF(health.LivenessHandler())`, fix the "NATS callout" wording in `CLAUDE.md`/`docs/architecture.md`, and complete `deploy/.env.example`.

## 4. Test coverage — score 2

### Evidence

- [high] coverage below repo minimum 80%, currently 61.9% (109/176 statements) — `auth-service/main.go:53` — Every one of the 67 uncovered statements but 8 is in `run()` (59 statements, 0%). The handler/route half of the service is at 93% (109/117); the gap is structural, not a missing test case: `run()` inlines all config validation, handler construction, router/server wiring and shutdown, so none of it is reachable from a unit test without a live OIDC issuer, `obs.Init`, and a bound port.
- [high] Startup config guards are untested and untestable in place — `auth-service/main.go:59-68`, `:91-93` — The signing-key-type check, `AUTH_ACCOUNT_PUB_KEY` validity check and the "OIDC required when DEV_MODE=false" gate are the only things standing between a misconfigured deploy and minting unverifiable JWTs. All three are 0% and have no test seam (no `validateConfig`/`newHandler` function to call).
- [medium] `handleSession` error paths are unexercised (77.8%) — `auth-service/handler.go:240-244`, `:248-251`, `:257-260` — Three of the four error branches on the bot/admin lane have no test: principal with empty `Account` (must 401 `AuthInvalidToken`), an account that survives `subject.EncodeAccount` but still fails `IsValidAccountToken` (e.g. `*`/`>`/whitespace, must 400), and `signNATSJWT` failure (500, cause must not leak). The SSO lane has a dedicated test for each of the analogous cases (`handler_test.go:553`, `:571`, `:826`), so the session lane is objectively behind the standard the file already sets.
- [medium] `integration_test.go` is a unit test in disguise — `auth-service/integration_test.go:26-74` — It uses the in-memory `fakeValidator` and `httptest`, starts no container, and is a byte-for-byte subset of `TestHandleAuth_ValidToken` (`handler_test.go:93`). It proves nothing the unit suite does not. The one property only an integration test can establish — that a NATS server running the operator/account resolver with the scoped signing-key template actually accepts the minted `account:<x>`-tagged JWT and applies the `chat.user.{account}.>` grant — is untested anywhere in the repo (`SetScoped`/`IssuerAccount` appear only in `auth-service/*_test.go`, which only decode the JWT). `deploy/keycloak/realm-export.json` exists but no test consumes it either.
- [low] Unit router omits the production middleware chain — `auth-service/handler_test.go:85-90` — `setupRouter` registers routes on a bare `gin.New()`, so `c.GetString("request_id")` at `handler.go:136` is always `""` in tests and the request-ID → `errcode.WithLogValues` → error-log correlation path is never verified for this service. (`pkg/ginutil` tests the middleware in isolation; the composition is what is unverified.)
- [low] Hand-rolled fakes instead of `mockgen` — `auth-service/handler_test.go:32`, `:633` — CLAUDE.md §4 says mock with `go.uber.org/mock`. Both interfaces are single-method and the fakes are small, so this is defensible, but `fakeBPValidator` already carries call-count/last-arg bookkeeping that `gomock` would provide with `Times()`/argument matchers, and there is no `//go:generate` directive to keep it honest if `TokenValidator`/`BotplatformValidator` grow.
- [nitpick] Table-driven structure is used only where inputs vary (`handler_test.go:199`, `:490`, `:511`, `:571`) — 7 `t.Run` sites across 29 top-level tests. The many `TestHandleAuth_SessionToken_*` / `TestHandleAuth_DevMode_*` functions repeat the same 12-line request/recorder boilerplate; a per-lane table would halve the file and make missing cases (finding 3) visible by omission.

### Recommendations

- [high] Extract `validateConfig(cfg config) error` (main.go:59-68, :91-93) and `newHandler(ctx, cfg, kp nkeys.KeyPair) (*AuthHandler, error)` (main.go:77-106) from `run()`, keep `run()` as pure wiring, and add table-driven tests for each guard (user-key seed, non-account pubkey, prod mode without issuer/audiences, `BOTPLATFORM_URL` set/unset) — brings the tree to roughly 85% and puts the deploy-safety guards under test for the first time.
- [high] Add the three missing `handleSession` cases (`handler.go:240`, `:248`, `:257`) mirroring the existing SSO-lane tests; assert reason code and that the 500 body does not contain "generating NATS token".
- [medium] Make `integration_test.go` earn its build tag: run `testutil.NATS`-style container in operator mode with the scoped signing-key template from `docker-local/setup.sh`, mint a JWT through the handler, and assert `nats.Connect` with that JWT succeeds and can `Publish` on `chat.user.<account>.x` but is denied on `chat.user.other.x`. If a JetStream/operator-mode helper is out of scope, delete the file rather than keep a duplicate that inflates the "integration" signal.
- [medium] Have `setupRouter` apply `ginutil.RequestID()` (and optionally `AccessLog`) and add one test that sends `X-Request-ID` and asserts it is echoed on the response and appears in the captured error log line for a 401 — verifies `handler.go:136` end-to-end.
- [low] Collapse the per-lane `TestHandleAuth_SessionToken_*` and `_DevMode_*` functions into two table-driven tests with a shared `doAuth(t, router, body) (*httptest.ResponseRecorder)` helper — reduces boilerplate and makes lane parity reviewable.
- [nitpick] Add `//go:generate mockgen` for `TokenValidator`/`BotplatformValidator` into a `mock_validator_test.go`, or leave a one-line comment on each fake stating the mockgen exemption is deliberate.
