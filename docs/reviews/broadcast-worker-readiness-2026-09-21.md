# broadcast-worker — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `1a95503` (base `main`)  
**Overall score:** 3.3 / 5 (baseline 2026-09-01: 3.5, Δ -0.2)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

The CLAUDE.md hot-path contract holds under inspection: `sonic` on every per-message codec call with `jsonwarm.Pretouch` at boot, exactly one MongoDB write and it is buffered, coalesced and never returned to the handler, `jsretry.Settle` only, every subject from `pkg/subject`, member resolution batched with no N+1 — code quality and architecture both score 4. The high finding is a client contract regression: `buildRoomEvent` never sets `systemMsg`, a field #382 added to this exact function and #188 dropped without touching `docs/client-api.md` or `events.md`, so in encrypted rooms a rename or member-add now advances unread state and reorders every member's sidebar; no test asserts the field. Three reviewers flagged the same fleet pattern from here: the "must match across services" knobs (`ROOM_KEY_RETIRED_TTL`, `ROOM_LOCALITY_GRACE`, `USER_CACHE_*`) are re-declared per service because no owning config type exists. Performance is sound in sizing and batching but has no per-message deadline or heartbeat, so a slow Mongo read holds a `MAX_WORKERS` slot past `AckWait` and the redelivery fans the same message out twice; thread fan-out spawns an unbounded goroutine per follower; the preview seal is awaited before fan-out. DM mutations read membership from a different, uncached, differently-gated source than creates, so an unsubscribed botDM member gets edits but not messages. Coverage is 68.1%, every mutation handler's store-error branch is untested, the preview-writer shed path is asserted only by comment, and the documented dev harness cannot start.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 2 |
| Maintainability | 3 |
| Integration | 3 |
| Performance | 4 |

**Findings by severity:** 0 critical, 4 high, 16 medium, 19 low, 11 nitpick (50 total).  
**Highest-risk dimension:** Test coverage (2).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

