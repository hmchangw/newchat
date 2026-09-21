# bot-message-worker — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `fe27c91` (base `main`)  
**Overall score:** 1.8 / 5 (baseline 2026-09-01: 2.5, Δ -0.7)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

bot-message-worker is the weakest service in the fleet on this audit. Every reviewer flagged the same thread-partition defect: bot thread replies bind `thread_room_id` to the parent *room* ID, while message-worker keys that table by a Mongo ThreadRoom UUID and stamps it onto the parent row, and history-service resolves replies only through that stamp. Bot replies therefore land in a partition no reader queries, and the shared thread-count scan then runs over that wrong partition and blind-writes the result onto the parent, clobbering the user pipeline's count. It also decodes a `MessageEvent` envelope that bot-room-service does not send, so system messages arrive as zero-valued rows that fail and NAK until dropped. Four structural protections its twin has are missing: no outage retry budget (a Cassandra outage past roughly six minutes permanently drops bot messages, versus the hour message-worker survives), no `jobguard` panic guard on the worker goroutine, the consume loop is outside the WaitGroup so the Cassandra session can close under an in-flight message, and no per-message deadline or heartbeat. On the storage rules, none of the ten create INSERTs pin `USING TIMESTAMP` as CLAUDE.md requires, and the encrypted paths bind literal nulls inline — the exact shape the rulebook forbids, which will silently stop clearing legacy rows the moment someone adds the pending pin. `buildCassandraMessage` aliases rather than copies the quoted parent, so the at-rest split mutates the caller's message, and it skips the attachment re-encode, silently dropping quoted attachments on encrypted messages. Coverage is 14.1%, the lowest in the fleet: the entire Cassandra store, bootstrap and wiring are at zero, and `SaveMessage` and both encrypted paths have never been executed by any test.

| Dimension | Score |
|---|---|
| Code quality | 2 |
| Architecture | 2 |
| Test coverage | 1 |
| Maintainability | 2 |
| Integration | 2 |
| Performance | 2 |

**Findings by severity:** 1 critical, 17 high, 18 medium, 9 low, 4 nitpick (49 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

