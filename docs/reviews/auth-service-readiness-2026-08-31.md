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
