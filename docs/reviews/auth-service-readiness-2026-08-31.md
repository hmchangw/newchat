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
