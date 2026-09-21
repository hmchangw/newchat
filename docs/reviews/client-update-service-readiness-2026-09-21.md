# client-update-service — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `bf684cf` (base `main`)  
**Overall score:** 3.2 / 5 (baseline 2026-09-01: 3.3, Δ -0.1)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

client-update-service distributes the desktop client's binaries to the whole fleet, and it is the weakest link in that chain in ways that matter more than its size suggests. Nothing verifies the integrity of what it serves: no checksum, no signature, no digest header, and the download route is unauthenticated, so a client cannot tell a tampered executable from a good one and no record exists of which bytes were published or by whom. The read timeout is hardcoded at thirty seconds while the service advertises a two-gigabyte upload cap, and Go's read timeout bounds the whole request body — so admin-service's documented ten-minute relay budget is silently capped, and any artifact needing more than about a hundred megabytes of transfer is severed mid-body. This was flagged in the previous audit and is still unfixed. Publication is not atomic either: the two objects are written by independent uploads with no locking, so a mid-pair failure or two concurrent uploads leave a mismatched descriptor and executable, and both callers get a success. Cache invalidation is process-local with a twenty-four-hour default, so on more than one replica the documented "a re-upload busts the cache" claim holds only on the pod that received the upload, and a client can be handed a new descriptor with an old binary. On the download path there is no conditional-request support, no range support and no cache directives, so every startup poll re-transfers the whole body, no proxy can absorb a release herd, and an interrupted download restarts from zero. The singleflight fill runs on the first requester's context, so one client hanging up fails every concurrent waiter. Coverage is 76.8% and the entire gap is `run`, which is where the read timeout lives.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 3 |
| Test coverage | 2 |
| Maintainability | 4 |
| Integration | 3 |
| Performance | 3 |

**Findings by severity:** 0 critical, 10 high, 21 medium, 13 low, 3 nitpick (47 total).  
**Highest-risk dimension:** Test coverage (2).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

