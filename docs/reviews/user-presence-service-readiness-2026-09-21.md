# user-presence-service — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `94fd295` (base `main`)  
**Overall score:** 2.8 / 5 (baseline 2026-09-01: 2.8, Δ +0.0)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

user-presence-service confirms, across three independent reviewers, a cross-service contract that has now survived two audit cycles unfixed: notification-worker requests a presence snapshot on a subject no service in the repository registers, and the two sides do not even share a payload shape or a batch limit. Enabling the flag that turns it on does not degrade gracefully into something useful — every chunk times out and the call fails open, so do-not-disturb and in-call push suppression can never engage, while the ops document still presents it as a live contract waiting to be switched on. The service's own contract, by contrast, is clean and matches its documentation exactly. Its second structural problem is a second deployable binary nested inside the service directory, which re-declares the Valkey connection knobs and both presence timing knobs with their own defaults and dials Valkey raw — bypassing the shared helper that the store's own comment says was introduced to end exactly this duplication. Both binaries drive the same Lua scripts over the same keys and the stale threshold controls connection pruning, so an operator override on one and not the other makes the two disagree about who is online. On scale, the sweep index is a single un-hash-tagged key, so every presence write in the site serializes onto one Valkey shard and the store cannot be scaled out by adding shards; expiry drains at about a hundred accounts per second, so recovering from a gateway restart that drops fifty thousand connections takes minutes. A mid-sweep error discards status changes already committed, leaving those accounts permanently stale. Coverage is 44.8% and the presence state machine itself — a hundred and twenty lines of Lua inside Go strings — has no unit coverage at all and no CI job that runs its integration tests.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 3 |
| Test coverage | 1 |
| Maintainability | 3 |
| Integration | 3 |
| Performance | 3 |

**Findings by severity:** 1 critical, 17 high, 20 medium, 12 low, 6 nitpick (56 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

