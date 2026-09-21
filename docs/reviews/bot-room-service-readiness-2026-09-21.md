# bot-room-service — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `1b686d0` (base `main`)  
**Overall score:** 2.5 / 5 (baseline 2026-09-01: 2.8, Δ -0.3)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

bot-room-service is the bot-platform counterpart of room-service, and its handler logic, error tiering and room-key rotation net read well — but three reviewers independently found the same broken wire contract. Its system messages publish a bare `model.Message` onto BOT-MESSAGES-CANONICAL while every consumer on that stream decodes a `MessageEvent` envelope, so each membership system message decodes to a zero-valued message: bot-message-worker tries to persist an empty partition key and NAK-loops it through the retry budget, and search-sync-worker drops it as poison. Every bot membership system message is lost from history, and the service's own tests pin the wrong shape. Two further contract breaks sit in MongoDB: rooms are written with a legacy `t:"c"/"d"` field instead of the canonical `type`, so room-service and the room-meta cache decode bot rooms as typeless, and subscription rows omit `joinedAt`, `open` and `roles`, which the member-list index and sidebar visibility depend on, so a locally-created membership and its federated copy diverge. The `member_added` dedup ID is keyed on the room/user/destination triple rather than the subscription ID that the remove path already uses, so a remove-then-re-add inside the duplicate window silently drops the re-add. Member batches are unbounded and processed as sequential N+1 round trips under one 10-second budget, and the deferred key-rotation safety net runs on the request context, so when the failure is the timeout the net cannot fire. Coverage is 49.4% with 14 of 30 functions at zero, no mockgen mocks, and no `deploy/azure-pipelines.yml` at all — a gap shared by all three bot services.

| Dimension | Score |
|---|---|
| Code quality | 3 |
| Architecture | 3 |
| Test coverage | 1 |
| Maintainability | 3 |
| Integration | 2 |
| Performance | 3 |

**Findings by severity:** 1 critical, 10 high, 18 medium, 19 low, 7 nitpick (55 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

