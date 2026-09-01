# auth-service — Production Readiness Review

**Service:** `auth-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

The front door to the whole system, and the service where the gap between what the documentation promises and what the code does is widest. The hot path itself is cheap and allocation-light — no database, no JetStream, an Ed25519 sign — and the JWT expiry jitter correctly de-synchronises fleet-wide re-auth. Handler tests are genuinely strong: table-driven, error-path-first, with reason-code assertions.

Four findings stand out. **`docs/client-api.md` tells clients a security property the server does not provide** — that JWT scope is derived from the principal's roles; it is not, and `pkg/principal` says so outright: bots, admins and SSO users all receive the identical `scoped_user` template. **The auth-bypass dev branch ships in the production binary**, gated only by a boolean env var. **A dev-mode request carrying `ssoToken` nil-panics** — `main.go` wires a nil validator, `HandleAuth` routes token-carrying requests into `handleSSO` regardless, and `handleSSO` has no nil guard although its sibling `handleSession` does. And **the most connection-critical service in the fleet has no admission control at all** on its public unauthenticated endpoint, compounded by a JWKS refetch amplification an attacker can drive at zero cost.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 3 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 12 | 17 | 15 | 8 | **52** |

---

## 2. Go code quality — 4 / 5

Idiomatic, tightly-commented Go with correct `errcode` tiering and verified-clean credential hygiene, marred by one reachable nil-deref panic on the dev-mode path and a handful of dropped/unwrapped errors.

### Findings
- `high` — Dev mode wires a `nil` `TokenValidator` (`auth-service/main.go:89`), but `HandleAuth` routes any request carrying `ssoToken` into `handleSSO`, which calls `h.validator.Validate` unguarded — `auth-service/handler.go:170`
  Confirmed empirically: `NewAuthHandler(nil, kp, pub, 2h, true)` + `{"ssoToken":"anything",...}` panics with `invalid memory address or nil pointer dereference` (in prod `gin.Recovery` converts it to a 500). The doc comment at `handler.go:133-134` asserts the opposite ("A dev-mode request carrying a token still validates normally"), and the only test covering that claim, `TestHandleAuth_DevMode_WithSSOToken_UsesSSO` (`handler_test.go:363-369`), passes a **non-nil** validator with `devMode=true` — a configuration `main.go` can never produce, so it tests a path that does not exist and misses the one that does.

- `medium` — Error silently discarded and not wrapped in the signing-key check — `auth-service/main.go:63-65`
  `if skPub, err := signingKP.PublicKey(); err != nil || !nkeys.IsValidPublicAccountKey(skPub)` collapses two distinct failures into one message and drops `err` entirely (no `%w`), against CLAUDE.md §3 "always wrap with context" / "never ignore errors silently". A keypair that fails to yield a public key is reported as a wrong *key type*.

- `medium` — Handler success logs use the non-context `slog` variants, so they carry neither `request_id` nor trace correlation — `auth-service/handler.go:203`, `:262`, `:293` (also `:125`)
  Each site has a `ctx` in hand that `HandleAuth` already enriched (`handler.go:136`). CLAUDE.md requires the correlation ID "in all log lines" and o11y-correlated logs; `slog.Debug(...)` bypasses both. `slog.DebugContext(ctx, ...)` is a one-word fix.

- `low` — Sentinel compared with `!=` rather than `errors.Is` — `auth-service/main.go:147`
  `err != http.ErrServerClosed` works today only because `ListenAndServe` returns the sentinel unwrapped; any future middleware/listener wrapper silently turns a clean shutdown into a non-zero exit.

- `low` — The bind error is discarded and every malformed body is reported as one specific cause — `auth-service/handler.go:139-142`
  A truncated/invalid-JSON body, a wrong content type, and a genuinely missing `natsPublicKey` all return `"natsPublicKey is required"` with reason `missing_fields`. The `err` is dropped rather than attached via `errcode.WithCause`, so nothing server-side records what actually failed to parse.

- `low` — Three 400s are emitted with no `reason`, while `errcode.AuthInvalidRequest` is declared and unused repo-wide — `auth-service/handler.go:190`, `:249`, `:280` (constant at `pkg/errcode/codes_auth.go:11`)
  Every sibling error in the same handler carries a reason; these three collapse to `""`, so a client cannot distinguish "your account token is malformed" from any other 400. Either use the existing constant or delete it.

- `low` — Failure to start the HTTP server returns before `obsShutdown` runs and strands the `shutdown.Wait` goroutine — `auth-service/main.go:146-150`
  `<-shutdownDone` is skipped on the error return, so buffered spans/metrics are lost and the goroutine blocks until `os.Exit`. Harmless today, but it is a goroutine with no termination path on that branch (CLAUDE.md §3 Concurrency).

- `low` — SAST audit-coverage gap: gosec and the repo-owned semgrep rules are clean repo-wide, but `govulncheck` and the semgrep registry packs could not run (blocked egress). Not a service defect; this service's dependency surface (`go-oidc`, `nkeys`, `jwt/v2`, `resty`) is exactly where a CVE feed matters most.

- `nitpick` — Single-method interfaces named `TokenValidator` / `BotplatformValidator` rather than the `-er` form CLAUDE.md §3 prescribes — `auth-service/handler.go:27`, `:57`. Also, `handler.go:190` and `:249` return `BadRequest` for values sourced from the OIDC claim / botplatform principal, not from the client — `Unauthenticated` (or a 5xx) fits the actual fault.

**Verified clean (credential handling):** no token, seed, password, or raw body is logged anywhere — `slog` sites are `main.go:48,82,88,104,130,139` and `handler.go:125,203,262,293`, all identity/config values only. `errcode.WithCause(err)` at `handler.go:179` and `:237` is safe: go-oidc's `Verify` errors never echo `rawIDToken` (`verify.go:225-251`), `botauth` reports only status + body *length* (`pkg/botauth/botauth.go:149-150`), and `nkeys.FromSeed` returns sentinels only. The `errors.As` guard at `handler.go:230-238` correctly preserves the one-errcode-per-chain invariant that `WithCause` panics on. No `log AND return` double-logging: every error path goes to `errhttp.Write` exactly once.

### Recommendations
- `high` — Guard `handleSSO` with `if h.validator == nil { errhttp.Write(ctx, c, errcode.Unavailable("SSO auth not configured", errcode.WithReason(errcode.BotplatformUpstreamUnavailable))) }`, mirroring `handleSession`'s existing nil check (`handler.go:222-226`), and rewrite `TestHandleAuth_DevMode_WithSSOToken_UsesSSO` to construct the handler exactly as `main.go:89` does.
- `medium` — Split the `main.go:63-65` check into two statements, wrapping the `PublicKey()` error with `fmt.Errorf("derive signing key public key: %w", err)`.
- `medium` — Switch the four `slog.*` calls in `handler.go` to `slog.*Context(ctx, ...)`.
- `low` — Use `errors.Is(err, http.ErrServerClosed)` at `main.go:147`, and move `<-shutdownDone` / `obsShutdown` onto both exit paths.
- `low` — Attach the bind error via `errcode.WithCause(err)` at `handler.go:140` and give the three reason-less 400s `errcode.AuthInvalidRequest`.
- `low` — Re-run `make sast` (govulncheck included) from a network-permitted runner before shipping; this service's OIDC/JWT dependency set is the highest-value CVE surface in the repo.

---

## 3. Architecture — 3 / 5

Clean DI (constructor + functional options), consumer-side interfaces, correct file layout and fail-fast on NKey secrets — undercut by a guaranteed panic path in the dev branch, an unguarded auth-bypass mode compiled into the production binary, and a shared timeout knob the rest of the fleet mounts but this service does not.

### Findings
- `high` — Dev mode wires a `nil` `TokenValidator`, but `HandleAuth` still routes a token-carrying dev request into `handleSSO`, which dereferences it — `auth-service/main.go:89` / `auth-service/handler.go:338`.
  The doc comment at `handler.go:301-302` asserts the opposite ("A dev-mode request carrying a token still validates normally"). Any `DEV_MODE=true` deployment receiving an `ssoToken` panics; `gin.Recovery()` converts it to a 500. No test covers it — `handler_test.go:369` is the only dev-mode+validator case and it injects a real fake.

- `high` — The auth-bypass branch ships in the production binary, gated only by a boolean env var — `auth-service/handler.go:446-455`, selected at `auth-service/main.go:87`.
  `handleDevAuth` mints a signed, scoped NATS user JWT from a fully client-supplied `req.Account` with no token, no issuer, no validation. The single `slog.Warn` at `main.go:88` is the only signal; there is no build tag, no separate binary, and no startup refusal when the real signing key/account key look production-shaped. `envDefault:"false"` is the sole thing between a config typo and fleet-wide impersonation.

- `medium` — Service boundary contradicts its documented identity: repo docs describe `auth-service` as the NATS `auth_callout` service (`docs/superpowers/spec.md:307`, `docs/superpowers/plans/2026-03-19-plan-02-auth-service.md:5`), but the shipped service is HTTP-only (`auth-service/routes.go:14-17`) and mints JWTs out-of-band.
  Only `docs/superpowers/specs/2026-06-05-seamless-nats-jwt-refresh-design.md:18` records the correction; the primary spec still misdescribes the boundary.

- `medium` — The shared per-request timeout knob `ginutil.TimeoutConfig` is not mounted, unlike its sibling Gin services — `auth-service/main.go:111-117` vs `tcard-service/main.go:116` and `botplatform-service/config.go:31`.
  Handlers therefore run with no context deadline, and the layering has zero headroom: `pkg/oidc/oidc.go:64` uses a 10s client timeout against a 10s `WriteTimeout` (`auth-service/main.go:125`), so a slow JWKS refresh trips the socket timeout at the same instant the upstream call would have returned.

- `medium` — `BOTPLATFORM_URL` has no `envDefault` and no `required`, so a missing value degrades silently at request time into a 503 rather than failing fast at startup — `auth-service/main.go:43,78` / `auth-service/handler.go:391-393`.
  CLAUDE.md requires `envDefault` for non-critical config and fail-fast for the rest; this is neither. An operator who forgets it gets a working `/healthz` and a permanently broken bot/admin login path.

- `low` — Handler surface is exported from `package main`, against "export only what other packages consume" and the repo's own convention (`media-service/handler.go:16,24`, `botplatform-service/handler.go:23,46` use unexported `handler`/`newHandler`) — `auth-service/handler.go:231,272,195,225,243`.

- `low` — No `//go:generate mockgen` anywhere in the service; both consumer interfaces are doubled by hand-written fakes in `handler_test.go`, against CLAUDE.md Section 4's `go.uber.org/mock` mandate — `auth-service/handler.go:195,225`.

- `low` — Botplatform client timeout is a bare literal with no env knob, and disagrees with the layer beneath it (`pkg/botauth/botauth.go:36` sets a 10s ceiling the 5s resty timeout always pre-empts) — `auth-service/main.go:79`.

- `nitpick` — `err != http.ErrServerClosed` compares by value instead of `errors.Is`, and the early return skips `<-shutdownDone`, stranding the shutdown goroutine on a bind failure — `auth-service/main.go:147-150`.

Not findings: middleware order matches every peer Gin service; `/healthz` present per CLAUDE.md; `pkg/subject` builders used rather than `fmt.Sprintf` (`handler.go:357,415-416`); no JetStream usage, so `BOOTSTRAP_STREAMS`/`bootstrap.go` are correctly absent; `pkg/shutdown.Wait` used with the documented HTTP ordering.

### Recommendations
- `high` — Guard `handleSSO` on `h.validator == nil` (or make `NewAuthHandler` reject `nil` validator when `devMode` is false and short-circuit dev-mode SSO to a 400), and add the `DEV_MODE=true` + `ssoToken` regression test that currently does not exist.
- `high` — Move `handleDevAuth` behind a `//go:build dev` file so the branch is not linked into release images; failing that, refuse startup when `DEV_MODE=true` and the signing key/account key are not the local-dev pair.
- `medium` — Mount `HTTP ginutil.TimeoutConfig` on `config` and register `cfg.HTTP.Middleware()`, matching `tcard-service`/`botplatform-service`; raise `WriteTimeout` above the 10s OIDC client timeout.
- `medium` — Make `BOTPLATFORM_URL` an explicit deployment decision: either `required`, or a `SESSION_AUTH_ENABLED` flag validated at startup, so the 503 branch is unreachable by misconfiguration.
- `medium` — Correct `docs/superpowers/spec.md:307` and the plan doc to describe an HTTP token-exchange service, so no future reader wires `$SYS.REQ.USER.AUTH` against it.
- `low` — Unexport `AuthHandler`/`NewAuthHandler`/`TokenValidator`/`Option`/`With*` to `handler`/`newHandler`/`tokenValidator`, and add `//go:generate mockgen` for the two consumer interfaces to replace the hand-written fakes.
- `low` — Promote the botplatform client timeout to an env-tagged field so it can be tuned against `pkg/botauth`'s 10s ceiling.

---

## 4. Test coverage — 2 / 5

Handler-level tests are genuinely strong (table-driven, error-path-first, reason-code assertions, no-leak checks), but 61.9% statement coverage sits below the CLAUDE.md Section 4 floor and the uncovered remainder is exactly the wrong half: all of `run()`'s credential/key-material validation plus the botplatform account-guard branches — so the dimension is floored at 2.

### Findings
- `high` — Coverage is **61.9% (176 stmts)**, below the CLAUDE.md Section 4 80% floor — `auth-service/handler.go`, `auth-service/main.go`
  Not vanity-percentage failure: `handler.go` is ~93% (107/115); the shortfall is `main.go` `run()` at **0.0% (61 stmts)**.
- `high` — `run()` is untested and contains the service's only key-material guards: signing key must be account-type (`auth-service/main.go:63`), `AUTH_ACCOUNT_PUB_KEY` must be a valid account key (`auth-service/main.go:66`), and OIDC issuer/audiences required when `DEV_MODE=false` (`auth-service/main.go:91`).
  A regression here mints JWTs under the wrong issuer, or silently boots prod in dev mode where `handleDevAuth` mints an unauthenticated JWT for any `account` (`auth-service/handler.go:278`). None of this is reachable from a test because config parse, key parse, OIDC dial and HTTP serve are one unsplittable function.
- `high` — The botplatform branch's two account guards are uncovered: empty `p.Account` (`auth-service/handler.go:240`) and `EncodeAccount` output failing `IsValidAccountToken` (`auth-service/handler.go:248`).
  These are the privilege-escalation guards — a principal with an empty or `.`/`*`/`>`-bearing account would otherwise be stamped into `account:<x>` and substituted into the scoped signing-key subject template. The SSO twins of both *are* covered (`handler_test.go:553` missing-account, `handler_test.go:571` invalid-format), so the omission is asymmetric, not systematic.
- `medium` — `integration_test.go` starts no container and touches no real dependency: it wires `fakeValidator` (`auth-service/integration_test.go:205`) into `httptest`, duplicating `TestHandleAuth_ValidToken`. Consequently the real `pkg/oidc` validator — JWKS fetch, signature verification, `aud`/`iss` checks, expiry → `ErrTokenExpired` — is exercised **nowhere** in this service, despite `auth-service/deploy/keycloak/realm-export.json` already providing a realm to run against.
  `TestMain` correctly calls `testutil.RunTests(m)` (`integration_test.go:244`), so the harness is in place; only the dependency is missing.
- `medium` — `handleSession`'s JWT-signing failure path (`auth-service/handler.go:257`) has no test, though its SSO and dev-mode twins do (`handler_test.go:826`, `handler_test.go:449`). The bot path is the one that returns a *different* account string (`natsAccount`) to the signer, so it is not covered by transitivity.
- `low` — `GET /readyz` is registered (`auth-service/routes.go:14`) but no test hits it; `TestHandleHealth` (`handler_test.go:477`) covers only `/healthz`. `registerRoutes` reads 100% covered purely because the route table is executed, not the route.
- `low` — `TLS_SKIP_VERIFY` (`auth-service/main.go:36`, wired at `:99`) disables issuer TLS verification and has zero coverage — it is only reachable through the untested `run()`.
- `nitpick` — Doubles are hand-written (`fakeValidator` `handler_test.go:31`, `fakeBPValidator` `handler_test.go:633`) rather than `go.uber.org/mock`, which CLAUDE.md Section 4 mandates. Defensible here — there is no `store.go` and both interfaces are single-method — but there is no `//go:generate mockgen` directive anywhere in the service, so the deviation is undeclared.
- `nitpick` — `cryptoRandFloat`'s `rand.Reader` failure branch (`auth-service/handler.go:124`) is uncovered and unreachable without a reader seam; the 66.7% on that function is noise, not risk.

### Recommendations
- `high` — Extract the pure, testable parts of `run()` into functions unit-testable in `package main`: `validateKeys(cfg) (nkeys.KeyPair, error)` covering `main.go:59-68`, and `buildHandler(cfg, ...) (*AuthHandler, error)` covering the dev-mode/OIDC-required fork at `main.go:87-106`. Table-drive with a bad seed, a user-type seed, a non-account `AUTH_ACCOUNT_PUB_KEY`, and `DEV_MODE=false` with empty issuer/audiences. This alone lifts the service over 80% and puts the tests where the credential risk is.
- `high` — Add the three missing `handleSession` cases with `errtest.AssertReason`: principal with `Account: ""` → 401 `AuthInvalidToken`; principal with `Account: "a*b"` (and one with a bare `>`), asserting 400 **and** that no JWT is returned; and a non-account signing key → 500 asserting the body omits `"generating NATS token"`, mirroring `handler_test.go:826`.
- `medium` — Make `integration_test.go` earn its build tag: stand up the existing Keycloak realm, construct a real `pkgoidc.NewValidator`, and assert the four rejection paths (bad signature, wrong `aud`, wrong `iss`, expired → `ErrTokenExpired` → 401 `AuthTokenExpired`). Follow the CLAUDE.md rule — if Keycloak is used by ≥2 packages it belongs in `pkg/testutil` as `Keycloak(t)`/`EnsureKeycloak()`/`TerminateKeycloak()`; otherwise inline `testcontainers.GenericContainer` with a stored ref and `t.Cleanup(c.Terminate)`.
- `medium` — Delete the fake-validator happy path from `integration_test.go` once the real one lands; it is a byte-for-byte duplicate of the covered `TestHandleAuth_ValidToken` and inflates apparent integration coverage.
- `low` — Add a `/readyz` request test asserting 200 and the response shape, so the route is covered rather than merely registered.
- `low` — Either add `//go:generate mockgen` for `TokenValidator`/`BotplatformValidator`, or add a one-line comment in `handler_test.go` recording why hand-written fakes are used, so the Section 4 deviation is explicit.
- `nitpick` — Inject the randomness source into `cryptoRandFloat` (or accept the 66.7%) rather than leaving an untestable error branch; `WithRandFloat` already establishes the seam pattern at `handler.go:92`.

---

## 5. Maintainability — 4 / 5

A small, cleanly-layered service whose production code reads well and comments mostly say *why*; the maintainability debt is concentrated in one asymmetric nil-guard, a three-way-duplicated mint tail, and an 846-line test file built from 28 copy-pasted request blocks.

### Findings
- `high` — `handleSSO` dereferences `h.validator` with no nil guard, while its sibling `handleSession` has exactly that guard for `h.bpValidator` — `auth-service/handler.go:170` vs `auth-service/handler.go:222`. `main.go:89` wires `NewAuthHandler(nil, …)` in dev mode, so a dev-mode request carrying `ssoToken` nil-panics into `gin.Recovery()` → 500. `HandleAuth`'s own doc comment (`handler.go:132`) promises "a dev-mode request carrying a token still validates normally". No test catches it: `TestHandleAuth_DevMode_WithSSOToken_UsesSSO` injects a **non-nil** fake (`handler_test.go:368-369`), so it exercises a wiring that production never produces.
  Two optional dependencies, two different nil policies, is the exact shape a future edit trips over.

- `medium` — the identity-stamp + sign + error-write tail is copy-pasted verbatim across all three branch handlers — `auth-service/handler.go:193-200`, `:252-258`, `:283-289`. Adding a fourth auth branch (or one new stamped attribute, e.g. `siteID` into `obs.ContextWithIdentity`, currently `""` in all three) means three synchronized edits.

- `medium` — 28 near-identical 5-line HTTP request blocks (`NewRecorder` / `NewRequest` / `Header.Set` / `ServeHTTP`) across `auth-service/handler_test.go` — e.g. `:266-272`, `:372-376`, `:663-667`, `:701-705`. The file already has three helpers (`mustAccountKP`, `mustUserNKey`, `setupRouter`, `handler_test.go:65,75,85`); the request block is the obvious fourth that was never extracted. `mustAccountKP(t)` alone appears 28 times.
  The boilerplate is why the file is 846 lines and still only 61.9% covered — the cost per new case is high enough to discourage adding one.

- `medium` — near-duplicate single-scenario tests that want to be one table: `TestHandleAuth_ExpiredToken` / `_InvalidToken` / `_MissingAccountClaim` (`handler_test.go:147,165,553`) differ only in fake state and expected reason; `_SessionToken_Bot_HappyPath` / `_Admin` (`:649,688`) differ only in the principal. CLAUDE.md §4 prefers table-driven for input/output variations of the same logic, and the file already does this correctly at `:199` and `:571`.

- `low` — `WithRandFloat` (`auth-service/handler.go:92`) exists in production code purely as a test seam; its sole caller is `handler_test.go:528`. CLAUDE.md §4 bars test helpers in production code. Not a real violation (it is a functional option, not a helper), but it is unexported-by-intent leakage of test needs into the public option set.

- `low` — `run()` (`auth-service/main.go:53-153`) is 100 lines doing config parse, nkey validation, o11y init, two-branch handler construction, gin wiring, server start, and shutdown orchestration. Below the pain threshold today; the dev/prod branch at `:87-106` is the seam that will split first.

- `nitpick` — `signNATSJWT`'s doc comment (`handler.go:309-315`) restates the NATS grant list as a third copy alongside `docker-local/setup.sh:72-74` and `docs/client-api.md:170-174`, self-declaring "a platform-team template change must mirror both". Verified consistent today; nothing enforces it.

- `nitpick` — `main.go:63-64` discards the `PublicKey()` error into a generic message, so a malformed seed and a wrong key *type* produce the same startup line.

### Recommendations
- `high` — Give `handleSSO` the symmetric guard `if h.validator == nil { errhttp.Write(…, errcode.Unavailable(…)) }` at `handler.go:170`, and add a test constructing the handler exactly as `main.go:89` does (nil validator, `devMode=true`, body carrying `ssoToken`) asserting a non-500.
- `medium` — Extract `func (h *AuthHandler) mintAndRespond(ctx, c, account, resp userInfoResp)` covering `handler.go:193-200`/`:252-258`/`:283-289`; the three branch handlers then reduce to validate-then-delegate.
- `medium` — Add `postAuth(t *testing.T, r *gin.Engine, body string) *httptest.ResponseRecorder` and `newTestHandler(t, opts...)` to `handler_test.go`; mechanically collapses ~140 lines of boilerplate and makes new cases one line.
- `medium` — Fold `_ExpiredToken`/`_InvalidToken`/`_MissingAccountClaim` into one table keyed by fake-validator state → expected status + `errcode` reason; likewise the two session-token happy paths into a principal table.
- `low` — Split `run()` into `newHandler(ctx, cfg) (*AuthHandler, error)` and `newServer(cfg, h) *http.Server`, leaving `run()` as wiring; this also makes the dev/prod branch unit-testable.
- `nitpick` — Replace the grant list in `handler.go:309-315` with a pointer to `docs/client-api.md §2.1` rather than a third transcription.
- `nitpick` — Wrap the key-type error at `main.go:63` so the seed-parse and key-type failures are distinguishable at startup.
