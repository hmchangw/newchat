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

