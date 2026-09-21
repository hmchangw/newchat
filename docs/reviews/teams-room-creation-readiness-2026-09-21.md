# teams-room-creation — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `b4126eb` (base `main`)  
**Overall score:** 3.2 / 5 (baseline 2026-09-01: 3.5, Δ -0.3)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

`teams-room-creation` reads chats flagged `needCreateRoom`, batches them per site, and publishes a room-create event for `room-worker`. It is well-built Go — 4s in code quality, architecture, maintainability and performance, a consumer-owned store interface, publish injected as a `publishFunc` so tests need no NATS, subjects exclusively from `pkg/subject`, and a compare-and-set on `updatedAt` so a concurrently re-synced chat is not silently cleared. It scores 3.2 on the strength of one critical integration finding that the sibling audits corroborate from the other end of the pipe.

**An empty roster is published unguarded, and the consumer treats it as authoritative.** `buildEvent` copies `c.Members` verbatim and the query filters on `needCreateRoom: true` alone, so a chat whose roster came back empty is published with `members: []`. `room-worker/teamsroomcreate.go:153-179` then computes `removed` as every existing subscription whose account is absent from the event and calls `DeleteSubscriptionsByAccounts` — a `DeleteMany`. This is not confined to first-time migration: both `teams-chat-sync` and `teams-chat-member-sync` re-set `needCreateRoom = true` on every re-sync of an already-materialised room, so the path is live in steady state. Three things make it worse. `integration_test.go:77` pins the empty-roster publish as *intended* behaviour. The audit lane cannot catch it — `teams-room-verify` compares `SubscriptionCount` against `accountsPresent(members)`, and 0 versus 0 verifies as converged. And the upstream reviewers found the two ways an empty roster is produced in the first place: `teams-chat-sync` writes a zero-member Graph response as a complete roster, and `teams-chat-member-sync` stores `Account: ""` for any member missing from `teams_user`, which this service forwards and `room-worker` then skips — evicting a live member on a transient lookup miss. The fix wants both halves: skip a chat with no resolvable accounts here, and refuse to delete when `len(wantAccounts) == 0` there, so neither side alone can empty a room.

Second, the job cannot fail. `publishBatch` logs a Warn and returns; `run` returns nil unconditionally; `main` then logs "teams-room-creation done". A site whose `ROOMS-TEAMS-{siteID}` stream is absent, or a total NATS outage, produces a green CronJob run forever with no counters in the completion line to contradict it. Its own sibling `teams-chat-member-sync` returns an error when any item failed.

Third, batches are bounded by chat count and never by bytes. `ROOM_CREATE_BATCH_SIZE` defaults to 100 chats, each carrying its full roster — a few hundred large group chats is megabytes before zstd. An oversized batch returns `nats.ErrMaxPayload`, gets a Warn, never clears its flag, and because the scan sorts by `_id` the identical batch is rebuilt and fails again on every run: a permanent poison batch. Four peers in this repo already read `nc.NatsConn().MaxPayload()` and clamp. The same scan is also unbounded and fully materialised, then copied again into the per-site map, so the initial migration decodes the whole flagged corpus into one pod's heap.

Coverage is 55.9%. As elsewhere in this family the shape is better than the number — `runner.go` is at 96.8% and `config.go` at 100%, while `publisher.go` and `store_mongo.go` are 0% because their only tests are integration-tagged and no pipeline runs them. Two gaps are real rather than measurement artefacts: no test ever has more batches than workers, so the semaphore's blocking path is never exercised under `-race`, and no test passes a cancelled context although `main.go` documents an abort-between-operations contract the runner does not implement.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 1 |
| Maintainability | 4 |
| Integration | 2 |
| Performance | 4 |

**Findings by severity:** 2 critical, 4 high, 17 medium, 18 low, 6 nitpick (47 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

