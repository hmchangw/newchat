# outbox-worker — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `139f825` (base `main`)  
**Overall score:** 3.0 / 5 (baseline 2026-09-01: 3.2, Δ -0.2)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

outbox-worker is a small, cleanly written NATS-to-NATS relay (429 non-test lines, one responsibility, every checklist item in the rulebook honoured) whose score is held down by three advertised guarantees that the reviewers verified do not hold. The per-peer isolation stops at the consumer: every concurrent lane funnels into one shared `MaxWorkers` semaphore, so a down peer's parked forwards (up to 1000, each costing 0.5–3s per attempt) can occupy the whole pool and stall healthy peers for the first minutes of an outage. The FIFO lane's "a rename cannot overtake the add it renames" contract is origin-only: inbox-worker routes `room_renamed` to its concurrent pool, so the ordering this service pays `MaxAckPending=1` for is discarded at the destination and a new member is stranded on the old name. And the two-lane shutdown handshake covers only the concurrent lane: ordered-lane callbacks are outside the WaitGroup and `cc.Stop()` discards rather than drains, so `natsutil.Drain` can run under a live forward. Beyond those, a dead pump leaves the process healthy with one peer's lane silently stopped, destinations absent from `ALL_SITE_IDS` strand in the stream with no detection, forward idempotency relies on a 2-minute server-default dedup window nothing sets against a 10-minute retry tail, the service emits no consumer metrics, and three docs (`nats-subject-naming.md`, `architecture.md`, `nats-traffic-estimation.md`) still describe a sourcing-based topology that no longer exists. Coverage is 37.8% because `main()` holds every closure and the drain-pool message path is untested; the integration suite drives the concurrent lane through `Consume` rather than the production `Messages()`+`drainPool` path, so the pull-iterator mechanism is exercised nowhere.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 3 |
| Test coverage | 1 |
| Maintainability | 4 |
| Integration | 3 |
| Performance | 3 |

**Findings by severity:** 1 critical, 4 high, 19 medium, 17 low, 3 nitpick (44 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

