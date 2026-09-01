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

---

## 6. Integration — 3 / 5

Wire and subject-scoping mechanics are correct (`EncodeAccount` matches `pkg/subject`, tag-based scoped JWT matches the live template), but `docs/client-api.md` §2.2 misstates how scope is derived, omits the entire session-token response shape, and the derived request/reply view has drifted — all `high` under CLAUDE.md's explicit auth-service clause.

### Findings
- `high` — `docs/client-api.md:200` claims "The scope of the returned JWT is derived server-side from the principal's roles (admin > bot > user)". It is not: `signNATSJWT` stamps only `account:<name>` (`auth-service/handler.go:321`) and `pkg/principal/principal.go` states role-derived scoping is an unimplemented follow-up — bots, admins and SSO users all get the identical `scoped_user` template. Clients are told a security property the server does not provide.
- `high` — the `authToken` (session) branch's success response is undocumented. `handler.go:262-272` returns `user.account` = the **encoded** account and leaves `email`/`employeeId`/`engName`/`chineseName`/`deptName`/`deptId` empty, but the §2.2 response table (`docs/client-api.md:212-227`) documents only the SSO shape and says `user.account` is "Derived from the token's `preferred_username` claim". CLAUDE.md requires request/response schema per auth-service route.
- `high` — derived view drift: `docs/client-api/request-reply.md:58` still says "Exchanges an SSO token for a signed NATS user JWT", contradicting canonical §2.2 (`docs/client-api.md:190`) which documents both the SSO and botplatform-session branches. `docs/client-api/events.md` is correctly silent (HTTP-only), no drift there.
- `medium` — undocumented error variant: `handler.go:249` returns `400 "account contains invalid characters"` on the session branch, but the §2.2 error table (`docs/client-api.md:249`) only lists the SSO/dev wording `"account must be a single NATS subject token (no '.', '*', '>' or whitespace)"`. Two different bodies for the same 400, one of them unpublished.
- `medium` — `handler.go:312-313` documents the effective grants as including `_INBOX.>` on both pub and sub and claims to be "kept in sync with `docker-local/setup.sh` and `docs/client-api.md` §2.1". Both sources say the opposite: `docker-local/setup.sh:56-62` states "There is no _INBOX grant" and the template (`:68-75`) has none; the §2.1 table (`docs/client-api.md:166-173`) has no `_INBOX` row. A platform-team change made from this comment would over-grant.
- `medium` — `routes.go:12-14`: neither `/healthz` nor `/readyz` appears in `docs/client-api.md`, though CLAUDE.md names auth-service HTTP routes as binding (client-update-service's `/healthz` is documented at `docs/client-api.md:8872`, so the omission is inconsistent, not a category exemption).
- `medium` — `/readyz` is registered with **zero** checks (`routes.go:14`), so it is a constant 200 identical to `/healthz`. `docs/health-probes.md:11` says readiness "Reports whether this pod is connected to NATS" and lists auth-service at `:17` — auth-service holds no NATS connection, and nothing probes the OIDC JWKS or botplatform, so a pod that cannot validate a single token still reports Ready to the gateway.
- `medium` — the bot-account encoding contract is enforced only by scattered guards, not by the subject layer. `auth-service/handler.go:247` scopes a bot JWT to the encoded account, but only 2 of the ~10 per-user event builders encode (`pkg/subject/subject.go:260`, `:500`); `UserRoomEvent` (`:494`) does not. Today every publish site skips bots (`broadcast-worker/handler.go:701,918,1182,1339`; `room-service/handler.go:1490` excludes `RoomTypeBotDM`), so it is latent — but an unguarded future publisher emits `chat.user.weather.site-a.bot.event.room`, which the bot's JWT cannot match and which a human account named `weather` **can** (its grant is `chat.user.weather.>`).
- `low` — `handleSSO` (`handler.go:189`) accepts any `IsValidAccountToken` account, including one ending `_bot`. `pkg/subject/subject.go:49-56` documents `DecodeAccount`'s correctness as resting on "no non-bot account ends in `_bot`" — auth-service is the mint point and enforces nothing.
- `nitpick` — no NATS/JetStream/Mongo/Cassandra/idgen surface here, so the OUTBOX/INBOX, `Timestamp`-at-publish-site, `msgbucket` and ID-format checks are N/A for this service. `integration_test.go:76` correctly uses `testutil.RunTests(m)`.

### Recommendations
- `high` — rewrite `docs/client-api.md:200` to state that scope comes solely from the `account:` tag + `scoped_user` template and that roles are pass-through today.
- `high` — add a session-token response sub-table to §2.2: encoded `user.account` (dots→underscores, cross-referencing §5), and the empty directory fields.
- `high` — update `docs/client-api/request-reply.md:58` to name both token branches.
- `medium` — collapse `handler.go:249` onto the documented 400 wording, or add the second variant to the §2.2 error table.
- `medium` — delete `_INBOX.>` from `handler.go:312-313` and mirror `setup.sh`'s explicit "no `_INBOX` grant" rationale.
- `medium` — document `/healthz` + `/readyz` in §2, and give `/readyz` a real check (OIDC JWKS reachability; botplatform when `BOTPLATFORM_URL` is set) or drop it and fix `docs/health-probes.md:11,17`.
- `medium` — make `subject.UserRoomEvent` (and the other per-user event builders) call `EncodeAccount` so the grant contract is enforced in one place rather than by every caller's `isBot` guard.

---

## 7. Performance — 3 / 5

The hot path itself is cheap and allocation-light (no DB, no JetStream, Ed25519 sign only) and the JWT jitter correctly de-synchronises fleet-wide re-auth, but the single most connection-critical service in the fleet ships with zero admission control and an unpooled upstream client that sibling services already tune correctly.

### Findings
- `high` — No admission control on the public, unauthenticated `POST /api/v1/auth`: the middleware chain is CORS → o11y → Recovery → RequestID → AccessLog only — no `ginutil.MaxConcurrency`, no `ginutil.LimitListener`, no `ginutil.Timeout` — `auth-service/main.go:111-125`, `auth-service/routes.go:11`.
  `user-service` wires all three for the same Gin stack (`user-service/routes.go:25-27`, `user-service/main.go:387`); auth-service is the one endpoint every client connection must traverse, so it is the one that most needs a shed valve. `pkg/ginutil.MaxConcurrency` exists and returns 429 + Retry-After precisely for this.

- `high` — Unknown-`kid` JWKS refetch amplification. `Validate` calls `verifier.Verify` (`pkg/oidc/oidc.go:133`); go-oidc's `RemoteKeySet` has **no minimum refresh interval** — a cache miss goes straight to `keysFromRemote` and re-fetches the IdP's JWKS (`go-oidc@v3.17.0/oidc/jwks.go:196-235`), serialised only by `inflight`. A caller submitting syntactically valid JWTs with random `kid` values drives a continuous 10s-timeout fetch loop against the IdP, one after another, at zero cost to the attacker — and with finding 1 there is nothing throttling the submission rate.

- `medium` — The botplatform Resty client is built without `WithMaxIdleConns`, so it inherits `http.DefaultTransport`'s `MaxIdleConnsPerHost = 2` — `auth-service/main.go:79`. `pkg/botauth` permits 64 concurrent upstream validations (`pkg/botauth/botauth.go:41`), so every session-token request past the second pays a fresh TCP+TLS handshake. Both peer services that construct the identical validator do size the pool: `media-service/main.go:112` and `upload-service/main.go:191` pass `WithMaxIdleConns(32)`. This is a straight omission, not a deliberate difference.

- `medium` — No per-request timeout, so `WriteTimeout` abandons the response while the handler keeps working. `srv.WriteTimeout` is 10s (`auth-service/main.go:125`) and the OIDC client timeout is also 10s (`pkg/oidc/oidc.go:78`, `httpTimeout`); a slow JWKS fetch therefore outlives the response deadline, and without `ginutil.Timeout` nothing cancels the request context. The goroutine, its buffers and its upstream socket stay held on a connection nobody will read. Under an IdP slowdown this compounds with finding 1 into unbounded goroutine growth.

- `low` — Nothing is cached on either branch. Every client connect costs a full RSA signature verify (SSO) or a botplatform HTTP round trip (session); `pkg/botauth` coalesces only *concurrently* in-flight identical tokens and deliberately caches nothing between requests (`pkg/botauth/botauth.go:70-79`). The no-positive-cache choice is correctly justified for revocation, but *negative* results (`errInvalidToken`) carry no revocation risk and are exactly what a client retry storm generates.

- `low` — On a startup listen failure `run` returns at `auth-service/main.go:146-149` without reaching `<-shutdownDone`, so `obsShutdown` never flushes and the `shutdown.Wait` goroutine is abandoned. The process exits, so it is not a true leak — but the traces and metrics explaining the startup failure are dropped.

- `nitpick` — `big.NewInt(denom)` is re-allocated on every mint in `cryptoRandFloat` (`auth-service/handler.go:123`); the bound is a compile-time constant and belongs in a package-level `var`.

### Recommendations
- `high` — Add `ginutil.MaxConcurrency(cfg.MaxConcurrency, onShed)` and `ginutil.Timeout` on the `/api/v1/auth` route group, and wrap the listener with `ginutil.LimitListener(ln, cfg.HTTP.MaxConns)`, mirroring `user-service/routes.go:25-27` and `user-service/main.go:387`. Export the shed counter — it is the alert signal.
- `high` — Bound JWKS refetch: gate `Validate` behind a token-bucket keyed on verification failures, or pre-parse the JWT header and reject an unrecognised `kid` against the cached key set before calling `Verify`, so an unknown `kid` cannot force an IdP round trip per request.
- `medium` — Change `auth-service/main.go:79` to `restyutil.New("", restyutil.WithTimeout(5*time.Second), restyutil.WithMaxIdleConns(32))`, matching media-service and upload-service and covering `botauth`'s 64-slot ceiling.
- `medium` — Mount `ginutil.TimeoutConfig` as a named config field (`HTTP ginutil.TimeoutConfig`, as `tcard-service/main.go:43` and `portal-service/main.go:65` do), set it below `WriteTimeout`, and let it cancel the request context so an abandoned response releases its upstream call.
- `low` — Add a small TTL-bounded negative cache for rejected tokens on both branches (seconds, not minutes) so retry storms are absorbed without touching the IdP or botplatform; positive results stay uncached.
- `low` — Move `<-shutdownDone` (and the `obsShutdown` flush) ahead of the error return at `auth-service/main.go:146`, so startup-failure telemetry is exported.
- `nitpick` — Hoist the `big.Int` bound in `cryptoRandFloat` to a package-level `var`.

---

## 8. Prioritized action list

| # | Sev | Action | Dim | Evidence | Why |
|---|-----|--------|-----|----------|-----|
| 1 | `high` | **Correct the scope claim in `docs/client-api.md` §2.2 — or implement it** | Integration | `docs/client-api.md:200` claims scope "is derived server-side from the principal's roles (admin > bot > user)"; `signNATSJWT` stamps only `account:<name>` at `handler.go:321`; `pkg/principal/principal.go` records role-derived scoping as an unimplemented follow-up | **Clients are told a security property the server does not provide.** Bots, admins and SSO users all get the identical `scoped_user` template. Anyone reasoning about blast radius from this document reasons wrongly. Fixing the doc is minutes; implementing the claim is a design change — but leaving both is not an option. |
| 2 | `high` | **Guard the nil validator in `handleSSO`** | Quality / Arch / Maint | nil wired at `main.go:89`; unguarded deref at `handler.go:170`; **the sibling `handleSession` has exactly this guard** at `:222`; the doc comment at `:132` promises the opposite behaviour | Found by three of six experts. A dev-mode request carrying `ssoToken` **nil-panics into `gin.Recovery()` → 500**, and `HandleAuth`'s own doc comment promises "a dev-mode request carrying a token still validates normally". No test catches it. The asymmetry with the sibling handler is the tell. |
| 3 | `high` | **Move the auth-bypass branch behind a build tag** | Architecture | `handler.go:446-455`, selected at `main.go:87` | An **authentication bypass compiled into the production binary**, gated by one boolean env var. A `//go:build dev` file removes it from release images entirely, which is the difference between a misconfiguration and an impossibility. |
| 4 | `high` | **Install admission control and a per-request timeout on `POST /api/v1/auth`** | Performance / Arch | middleware chain at `main.go:111-125` is CORS → o11y → Recovery → RequestID → AccessLog only; route `routes.go:11`; `ginutil.TimeoutConfig` mounted by `tcard-service/main.go:116` and `botplatform-service/config.go:31`, not here | The **public, unauthenticated front door of the entire system** has no `MaxConcurrency`, no `LimitListener` and no `Timeout`. And with `WriteTimeout` (10s) equal to the OIDC client timeout (10s), a slow JWKS fetch **outlives the response deadline while nothing cancels the request context** — goroutine, buffers and upstream socket all held on a connection nobody will read. |
| 5 | `high` | **Bound unknown-`kid` JWKS refetches** | Performance | `Validate` → `verifier.Verify` at `pkg/oidc/oidc.go:133`; go-oidc's `RemoteKeySet` has **no minimum refresh interval** — a cache miss goes straight to `keysFromRemote` (`go-oidc@v3.17.0/oidc/jwks.go:196-235`), serialised only by `inflight` | A caller submitting syntactically valid JWTs with **random `kid` values drives a continuous 10s-timeout fetch loop against the IdP, at zero cost to the attacker.** With item 4 unfixed, each such request also holds a slot. Add a negative-`kid` cache or a minimum refresh interval. |
| 6 | `high` | **Test `run()`'s key-material guards** | Test coverage | 61.9% (176 stmts); `run()` untested and holds the **only** key guards: account-type signing key `main.go:63`, valid `AUTH_ACCOUNT_PUB_KEY` `:66`, OIDC issuer/audiences required when `DEV_MODE=false` `:91` | Under the 80% floor, and **the uncovered half is the wrong half.** These three checks are what stand between a misconfigured deploy and a service minting JWTs nobody can verify — or verifying tokens against nothing. |
| 7 | `high` | **Document the session-token response shape and fix the derived view** | Integration | undocumented branch at `handler.go:262-272` vs the SSO-only table at `docs/client-api.md:212-227`; drifted view at `docs/client-api/request-reply.md:58` | The `authToken` branch returns the **encoded** account and leaves six user fields empty, while §2.2 documents only the SSO shape and claims `user.account` is "derived from the token's `preferred_username` claim". The derived view still says the endpoint only "exchanges an SSO token". CLAUDE.md names auth-service routes explicitly as binding. |
| 8 | `high` | **Cover the two botplatform account guards** | Test coverage | empty `p.Account` at `handler.go:240`; `EncodeAccount` output failing `IsValidAccountToken` at `:248` | These decide whether a bot gets a JWT scoped to a **malformed subject token** — and neither is exercised. `handleSession`'s JWT-signing failure path (`:257`) is likewise untested, and it is not covered by transitivity from its SSO twin because the bot path passes a *different* account string to the signer. |
| 9 | `medium` | **Fix the `_INBOX` grant comment** | Integration | `handler.go:312-313` claims `_INBOX.>` on both pub and sub, "kept in sync with `docker-local/setup.sh` and §2.1"; **both sources say the opposite** — `docker-local/setup.sh:56-62` states "There is no _INBOX grant", the template at `:68-75` has none, and `docs/client-api.md:166-173` has no `_INBOX` row | **A platform-team change made from this comment would over-grant** on the service that defines what every client may publish and subscribe to. |
| 10 | `medium` | **Make `/readyz` mean something, and make `BOTPLATFORM_URL` fail fast** | Arch / Integration | `/readyz` registered with zero checks at `routes.go:14`; `docs/health-probes.md:11`, `:17` describes it as reporting NATS connectivity — which this service does not hold; `BOTPLATFORM_URL` neither `required` nor defaulted at `main.go:43`, `:78`, degrading to a 503 at `handler.go:391-393` | **A pod that cannot validate a single token still reports Ready to the gateway** — nothing probes the OIDC JWKS or botplatform. And a missing upstream URL surfaces as a runtime 503 rather than a startup failure, which is the opposite of CLAUDE.md's fail-fast rule. |

**A boundary question worth settling.** Repo documentation describes `auth-service` as the NATS `auth_callout` service (`docs/superpowers/spec.md:307`, and the original plan document), but the shipped service is **HTTP-only** and mints JWTs out of band. That is not a defect — it may well be the better design — but the service's documented identity and its actual boundary have diverged, and every reader of those documents inherits the confusion.

**Also worth doing.** Size the botplatform Resty client's idle pool: `pkg/botauth` permits 64 concurrent upstream validations while the client inherits `MaxIdleConnsPerHost = 2`, so **every session-token request past the second pays a fresh TCP+TLS handshake** — and both peers constructing the identical validator (`media-service/main.go:112`, `upload-service/main.go:191`) already pass `WithMaxIdleConns(32)`. Make `integration_test.go` actually integrate: it starts no container and wires a `fakeValidator`, duplicating a unit test, so the real `pkg/oidc` path — JWKS fetch, signature verification, `aud`/`iss`, expiry — is exercised **nowhere**, despite `deploy/keycloak/realm-export.json` already providing a realm to run against. Extract the three-way-duplicated identity-stamp-sign-write tail (`handler.go:193-200`, `:252-258`, `:283-289`), and the 28 copy-pasted request blocks in `handler_test.go`. Convert the per-request success logs to the `*Context` slog variants so they carry `request_id`. Publish the second 400 wording (`handler.go:249`) and the `/healthz`/`/readyz` routes in §2.2. And encode bot accounts in the subject layer rather than in scattered guards — only 2 of ~10 per-user event builders encode, and `UserRoomEvent` does not; every publisher currently skips bots, so it is latent, but the guard lives in the wrong place.
