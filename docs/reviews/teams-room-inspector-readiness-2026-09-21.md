# teams-room-inspector — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `1f1d314` (base `main`)  
**Overall score:** 3.5 / 5 (baseline 2026-09-01: 3.3, Δ +0.2)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

`teams-room-inspector` is the smallest and cleanest service in this audit: nine files, 774 lines, one read-only HTTP endpoint that answers `teams-room-verify` with "does this room exist and how many subscriptions does it have". It scores 4 on five of six dimensions — every one but coverage — with zero TODOs, no function over 51 lines, correct `pkg/errcode` Tier-1 usage behind a single `errhttp.Write` adapter, explicit projections, no `$lookup`, a request-body cap, and exactly two Mongo queries for any batch size up to the 500-id cap. `handler.go`, `routes.go` and `newServer` are at 100% coverage. Its 3.2 is almost entirely a coverage artefact plus a set of shared-knob omissions.

The one finding every reviewer raised independently is the **room-id derivation, duplicated by hand across two binaries**. Both `handler.go:68` and `room-worker/teamsroomcreate.go:62` inline `idgen.DeterministicID([]byte(chatID))`, and both carry a comment saying the two must be kept in step by hand. The repo already has the right pattern next door — `pkg/teamsmigrate.EmployeeIDFromGraphID` wraps exactly this call so HR-sync and migration cannot drift. Drift here is silent and self-sustaining: every chat comes back `missing_room`, `teams-room-verify` logs mismatches forever, `needVerify` never clears, and the flagged set grows without bound while both jobs report healthy runs. The existing test cannot catch it, because it recomputes the expected id with the same call rather than pinning a literal.

Three shared knobs the rest of the fleet mounts are missing here, and they compound into the same failure. `mongoutil.PoolConfig` is not mounted, so the client keeps the driver's 30-second server-selection default instead of the repo's 2 seconds — a quiet Mongo becomes a 30-second hang rather than a fast, reportable failure. `ginutil.TimeoutConfig` is not wired either, so the handler context has no deadline at all: `http.Server`'s Read/WriteTimeout bound the socket, not the query. Together, an unreachable secondary pins every handler goroutine and its pooled connection for the caller's whole 30-second budget, and since the caller runs 8 concurrent batches, one slow site consumes an entire verify pass. Five sibling Gin services mount both.

Two contract gaps worth noting. The store filters on `_id`/`roomId` only, although the service is configured with `SITE_ID` and rooms are replicated to non-owning sites carrying the origin site's id — so a chat asked at the wrong site is answered `roomExists: true` with a partial subscription count rather than "not here", and the misroute guard is advisory only. And neither end of the only service-to-service hop in the pipeline propagates `X-Request-ID` or `traceparent`, so `ginutil.RequestID` mints a fresh id per call and a mismatch logged by the verifier can never be joined to the inspector's access log for the same batch. The inspector's extraction side is correct; what is missing is the caller's half of the contract.

Coverage is 47.7%, the lowest of the Teams family, and all 46 uncovered statements sit in `main.go` (29) and `store_mongo.go` (17). The store's tests exist, are fully compliant, and run in no pipeline. `run()` is 10.3% because the dial/serve/shutdown wiring was never given the seam that `newServer` was — extracting it closes roughly 26 of the 46 and lifts the package over the floor on its own.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 1 |
| Maintainability | 4 |
| Integration | 4 |
| Performance | 4 |

**Findings by severity:** 1 critical, 2 high, 14 medium, 13 low, 7 nitpick (37 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

