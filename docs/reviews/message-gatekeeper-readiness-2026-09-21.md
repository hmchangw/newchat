# message-gatekeeper — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `4e34d6b` (base `main`)  
**Overall score:** 3.3 / 5 (baseline 2026-09-01: 3.3, Δ +0.0)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

message-gatekeeper is the site's single MESSAGES ingress and is in good shape: error tiering is textbook, the consumer pattern, backoff, dedup and shutdown all match the rulebook, gosec and the repo semgrep rules are clean, and the handler logic outside `main` sits above 90% coverage. The findings that matter are contract and correctness gaps rather than structure. `docs/client-api.md`'s msg.send success schema has drifted from the wire: the reply is the full `model.Message` including `type` and raw base64 `attachments`, while the doc says `type` is not populated and lists neither field; the error table documents a not-found string history-service never emits and omits six rejections the gatekeeper does emit. A late redelivery or the client retry the docs recommend can leave two rows in `messages_by_room`, because `CreatedAt` is minted per delivery and the canonical dedup window is the 2-minute server default, so the doc's "a retry can never leave two copies in history" claim is false for the timeline table. The large-room rejection path logs and returns the same error (double-logged by `Classify`), the request-ID bridge from the payload is applied only inside `processMessage` so the client reply header and NAK logs carry a different ID, and the handler context has no per-message deadline while the per-leg timeouts sum past `AckWait`. Coverage is 66.4% only because `main` holds 28% of the tree's statements at 0%; the history `GetMessageByID` client and its wire mirror are hand-copied in three services, the L1 cache knobs are re-declared per service with a `60s` vs `2m` default drift, and `processMessage` is a 216-line function with a 1051-line test table.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 2 |
| Maintainability | 3 |
| Integration | 3 |
| Performance | 4 |

**Findings by severity:** 0 critical, 4 high, 19 medium, 20 low, 6 nitpick (49 total).  
**Highest-risk dimension:** Test coverage (2).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

