# Branch Review — `claude/new-session-439bi9`

| | |
|---|---|
| **Branch** | `claude/new-session-439bi9` |
| **Base** | `origin/main` (local `main` was 9 commits stale — reviewed against the real base) |
| **Date** | 2026-08-27 |
| **PR** | [#395](https://github.com/hmchangw/newchat/pull/395) |
| **Diff** | 2 files, +212 / −4 |
| **Services touched** | `room-service` (1) |
| **`pkg/` changed** | no |
| **Method** | 1 per-service generalist + 5 global lenses (Go, test-automation, bug & security, performance, observability) |

## Executive summary

The branch adds MongoDB projections to five previously-unprojected `room-service` store reads — `GetUser`, `GetApp`, `FindDMSubscription`, `GetThreadSubscriptionByParent` and `getRoomSubscriptions` — plus five `*_ProjectionFields_Integration` tests.

**Top-line risk: low, and the dominant hazard was checked directly.** A projection that omits a field a caller reads yields a zero value rather than an error, so the review's central question was whether each projected field set is a superset of what every caller actually reads. Three lenses traced that independently — including transitively, through `handleCreateSelfDM` / `handleCreateRoomDMOrBotDM` / `handleCreateRoomChannel` / `publishCreateRoom` and through `model.IsPlatformAdmin` — and all five projections came back clean. Critically, **no projected struct is ever written back to Mongo or marshaled into a NATS event**, so there is no data-destruction path; `publishCreateRoom` copies two scalars out before marshaling. Every projected bson key was verified against its struct tag, including the `u._id` nested path where `u.id` would have silently zeroed `Member.ID` in every member list.

The authorization path is fail-closed: `roles` is genuinely projected, and had it been dropped, `model.IsPlatformAdmin` returns `false` on nil/empty — denying, never granting. The claimed security win holds: `users.services`, the bcrypt credential block, no longer leaves Mongo on the `GetUser` path.

**What's actually worth acting on is verification, not correctness.** All five new tests are `//go:build integration`, and `room-service/deploy/azure-pipelines.yml:44` runs `go test` with no `-tags integration` — so the only guard against projection drift is a test CI never executes, and Docker was unavailable when the branch was written, so they have never been run at all. One of the five is additionally weak: `TestMongoStore_ListRoomMembers_SubscriptionProjection_Integration` asserts only included fields and would still pass with the projection deleted. The other four do pin exclusions and are load-bearing.

Beyond that: a comment in the new test contradicts the call it documents (four lenses caught it), the `store.go` interface docs still describe these methods as returning whole entities, and `active` is unprojected — harmless today, but a future `IsActive()` check would silently pass a deactivated account.

## Findings by severity

| Severity | Count |
|----------|:-----:|
| `critical` | 0 |
| `high` | 2 |
| `medium` | 8 |
| `low` | 5 |
| `nitpick` | 7 |
| **Total** | **22** |

Counts are deduplicated across lenses — four separate reviewers flagged the same stale test comment, three the redundant `_id: 1`, and two each the CI gap and the unprojected `active`.

## SAST status

`gosec` **PASS** (0 findings, repo-wide). Repo-local semgrep rules **PASS** (9 rules, 0 findings). `govulncheck` and the semgrep registry rulesets (`p/golang`, `p/security-audit`) **did not run** — the sandbox proxy returns 403 for `vuln.go.dev` and `semgrep.dev`. `make sast` exits 2 on those two network failures, not on any code finding. **Neither can be reported as passing**; re-run on a runner with egress before treating the gate as green.
