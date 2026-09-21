# bot-message-handler — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `6c855de` (base `main`)  
**Overall score:** 2.8 / 5 (baseline 2026-09-01: 3.2, Δ -0.4)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

bot-message-handler is small and idiomatic — error tiering is clean, subjects come only from the builder package, the stream bootstrap is correct, and no file or function exceeds the size thresholds — but its two send handlers are near-clones and only one of them is tested at all. `handleSendDM` is at zero percent coverage: the DM room-ID derivation, the empty-parameter rejection and the non-member Forbidden are unverified, and that is the branch most likely to drift from its twin. Four reviewers converged on the same stream-contract break: this service publishes a `MessageEvent` envelope onto BOT-MESSAGES-CANONICAL while bot-room-service publishes a bare `model.Message` on the identical subject, so the worker decoding both shapes silently writes empty rows for one of them. Three more contract defects sit alongside: the event `Timestamp` is taken from a client-supplied header rather than publish time as the rulebook requires, with no sanity bound on the value; the rooms projection reads a legacy `t` key that only bot-room-service writes, so room type comes back empty for any room the user-facing path created; and the guard knobs `MAX_CONCURRENCY` and `REQUEST_TIMEOUT` are re-declared locally at different defaults instead of mounting the owning package's config. On the hot path the mention flow fetches every subscriber ID in the room to test membership of a handful of mentions, then issues one user lookup per mention, with no cap on mention count anywhere — two bounded queries would replace work proportional to room size. None of the caching tiers the equivalent human path uses are wired, so every bot send pays at least two Mongo round trips against the primary. There is no integration test, no mockgen mocks and no CI pipeline definition — gaps shared by all three bot services.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 3 |
| Test coverage | 1 |
| Maintainability | 3 |
| Integration | 3 |
| Performance | 3 |

**Findings by severity:** 1 critical, 8 high, 17 medium, 17 low, 7 nitpick (50 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

