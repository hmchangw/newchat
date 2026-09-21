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

