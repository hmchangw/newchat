# room-service — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `14987f4` (base `main`)  
**Overall score:** 3.0 / 5 (baseline 2026-09-01: 3.2, Δ -0.2)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

The largest request/reply surface in the fleet — 28 RPCs, a 2,695-line `handler.go`, a 47-method `RoomStore` — and the size shows in maintainability, but architecture and performance hold up: every subject comes from `pkg/subject`, all nine cross-site event types ride OUTBOX through `outbox.Publish`, Mongo projection discipline and downstream timeouts were verified end to end. Two correctness bugs need fixing before the next release. In integration, the `moveChat` rebalance federates one `section_moved` event per rewritten row with the same `Nats-Msg-Id` (`requestID:eventType:destSiteID`), so JetStream dedup silently drops every sibling after the first and remote replicas keep stale ordering; the covering test discards the msgID argument. In code quality, `removeMember`/`updateRole` wrap a non-member requester's or target's `ErrSubscriptionNotFound` unconditionally, so a documented `forbidden` reaches the client as `internal` and pages as ERROR. Coverage is 57.3%: the store layer is integration-only, and `ComputeSectionOrder`, `MoveSubscriptionSection` and the `InsertTeamsMeeting` idempotency gate are untested at any layer. The four ROOMS-stream publishes carry no dedup id, so a timeout-retried create yields two channels.

| Dimension | Score |
|---|---|
| Code quality | 3 |
| Architecture | 4 |
| Test coverage | 1 |
| Maintainability | 3 |
| Integration | 3 |
| Performance | 4 |

**Findings by severity:** 1 critical, 6 high, 21 medium, 19 low, 7 nitpick (54 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

