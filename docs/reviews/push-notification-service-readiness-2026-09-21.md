# push-notification-service — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `a7241fa` (base `main`)  
**Overall score:** 2.7 / 5 (baseline 2026-09-01: 2.7, Δ -0.0)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

push-notification-service is the last hop of the push pipeline and it does not deliver anything. The only dispatcher wired into production is a log stub: it writes a line, returns nil, and the message is acked and removed from the stream. There is no APNs or FCM client anywhere in the repository, no credential or endpoint configuration, and no switch to select a real sink, so every push event the notification pipeline produces is silently consumed. Its retry semantics also directly contradict its own written contract: the ops document says this service must ack on receipt before any provider call, must never NAK, and must run at `MaxDeliver=1`, because a duplicate push is user-visible spam — the code instead acks after dispatch, NAKs transient errors, and inherits `MaxDeliver=6`. That same document tells operators to provision a five-minute duplicate window on a stream whose producer refuses to start under one hour. The consume loop is missing three guards every sibling worker has: it is not registered in the WaitGroup, so a message can slip past the drain and be acked on a drained connection; it has no panic guard, so a real dispatcher's first crash becomes a crash loop; and it stamps no request ID, so none of its log lines correlate with the notification-worker trace that produced them. Worst of all, when the loop dies it returns with no log, no metric and no exit, while the health check probes only the NATS connection — the pod stays Ready while the stream backs up. Coverage is 26.9% because `run` holds the wiring, the retry choice and all three missing guards, and nothing tests it.

| Dimension | Score |
|---|---|
| Code quality | 3 |
| Architecture | 3 |
| Test coverage | 1 |
| Maintainability | 3 |
| Integration | 3 |
| Performance | 3 |

**Findings by severity:** 1 critical, 11 high, 23 medium, 14 low, 4 nitpick (53 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

