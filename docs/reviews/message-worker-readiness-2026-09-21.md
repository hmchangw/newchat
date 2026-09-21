# message-worker — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `2369149` (base `main`)  
**Overall score:** 3.3 / 5 (baseline 2026-09-01: 3.2, Δ +0.1)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

The persistence hot path is correct where it matters most: plaintext creates pin `USING TIMESTAMP`, encrypted creates and the legacy-strip UPDATEs do not, `store_cassandra_writetime_test.go` pins both halves, `sonic` and `jsonwarm.Pretouch` are in place, the consumer opts into the outage retry budget, the #484 degraded start gates every document-creating thread write on the index gate, and Mongo is ordered before Cassandra so an outage NAKs before anything persists. Two design gaps carry the medium findings. The enrichment columns (`sender`, `mentions`) are re-resolved fail-open on every delivery yet bound under one pinned timestamp — the per-cell mixed-enrichment window CLAUDE.md itself names as unfixed — and a consume-loop exit is invisible to `/readyz`, so a pod that has silently stopped persisting history keeps reporting healthy; roomlist-worker already carries the readiness pattern to port. Performance is well-sized but leaves round-trips on the table: every subsequent thread reply pays two Paxos LWTs it can skip on non-redeliveries, thread-mention marking costs 2k+1 serial Mongo writes per reply, thread-store calls carry no deadline, and a 20 KB `tshow` reply's three-copy unlogged batch exceeds Cassandra's default fail threshold deterministically and then NAK-loops for the hour-long budget. `thread_reply_added` publishes the reply's own `CreatedAt` instead of the `TLM` the store computed, regressing thread freshness under out-of-order redelivery. Coverage is 56.3% with the parent `thread_room_id` CAS stamp untested at any layer; seven hand-aligned INSERT literals guard the pin rule only by comment and one hand-listed test; the README describes a service that no longer exists.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 1 |
| Maintainability | 3 |
| Integration | 4 |
| Performance | 4 |

**Findings by severity:** 1 critical, 1 high, 22 medium, 21 low, 7 nitpick (52 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

