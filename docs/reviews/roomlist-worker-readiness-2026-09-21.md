# roomlist-worker — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `377040a` (base `main`)  
**Overall score:** 3.7 / 5 (baseline 2026-09-01: 3.7, Δ -0.0)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

roomlist-worker is one of the best-engineered workers in the fleet: the disjoint-halves contract with broadcast-worker's preview writer holds exactly as CLAUDE.md describes, `msgbucket.NewerRow` ordering is applied on both the coalescer and the BSON filter, settlement is exclusively `jsretry.Settle`, shutdown ordering is explicit and load-bearing, and every business-logic function sits at 80–100% coverage. The 65.9% figure is mechanical — `main()` holds 30% of the statements — and the remaining findings are ops-facing rather than correctness bugs. Three reviewers independently flagged that the only bound on the mention map is derived as `4×CONSUMER_MAX_ACK_PENDING` with no validation, so a legal `0` or `-1` (JetStream "unlimited") silently disables the early drain the worker's memory model depends on. The operator README contradicts the binary: it omits `FLUSH_TIMEOUT` and the `2×FLUSH_TIMEOUT + FLUSH_INTERVAL < ACK_WAIT` fail-fast rule, and says the health endpoint checks only NATS when `/readyz` now also fails on a dead consume loop. Subscription writes filter on `(roomId, u.account)` with no `WarnMissingIndexes` unlike every sibling on the shared collection; the 10-minute `DefaultBackoff` tail pins the whole ack-pending budget for 5–10 minutes after a MongoDB outage ends, which `MaxDeliver=-1` makes unnecessary; `DeliverNewPolicy` on a durable means a deleted-and-recreated consumer silently loses every pointer and badge in the gap; the edit path can badge a message's own sender; and `docs/client-api.md` still names broadcast-worker as the `hasMention` writer. The store is stubbed by hand with a reasoned but rule-deviating no-mockgen comment.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 2 |
| Maintainability | 4 |
| Integration | 4 |
| Performance | 4 |

**Findings by severity:** 0 critical, 1 high, 9 medium, 21 low, 11 nitpick (42 total).  
**Highest-risk dimension:** Test coverage (2).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

