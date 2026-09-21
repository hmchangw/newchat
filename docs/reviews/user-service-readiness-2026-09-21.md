# user-service — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `c63a2fa` (base `main`)  
**Overall score:** 3.2 / 5 (baseline 2026-09-01: 3.3, Δ -0.1)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

The sub-package layout is genuine rather than cosmetic — clean dependency direction, compile-time interface pins, mocks where CLAUDE.md puts them — and four dimensions score 4. One root cause surfaces in three of them: `HTTPConfig` re-declares `mongoutil.PoolConfig`'s fields instead of mounting the struct under `envPrefix:"HTTP_"`, on the strength of a comment that is wrong, so the HTTP Mongo client is built without `WithPool` and silently runs on the driver's 30-second server-selection timeout while the NATS path fails in 2 seconds; a quiet Mongo pins every HTTP request for its whole budget. Integration found a live subject bug: `subject.SettingsUpdate`/`ChatlistUpdate` interpolate the raw account where the sibling builder encodes it, so a bot's own settings and chatlist sync land on a subject outside its JWT scope. Cross-site status/settings/chatlist replication is a sequential PubAck loop inside the request path with no retry — a lost `muteAllNotifications` leaves the remote pushing until the user touches settings again. Coverage is 54.0%: all six chatlist RPCs and their store methods are untested at every layer, and one unit test boots a real `nats-server`.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 1 |
| Maintainability | 3 |
| Integration | 3 |
| Performance | 4 |

**Findings by severity:** 1 critical, 6 high, 13 medium, 21 low, 7 nitpick (48 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

