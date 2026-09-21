# room-service — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `14987f4` (base `main`)  
**Overall score:** 3.0 / 5 (baseline 2026-09-01: 3.2, Δ -0.2)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

The largest request/reply surface in the fleet — 28 RPCs, a 2,695-line `handler.go`, a 47-method `RoomStore` — and the size shows in maintainability, but architecture and performance hold up: every subject comes from `pkg/subject`, all nine cross-site event types ride OUTBOX through `outbox.Publish`, Mongo projection discipline and downstream timeouts were verified end to end. Two correctness bugs need fixing before the next release. In integration, the `moveChat` rebalance federates one `section_moved` event per rewritten row with the same `Nats-Msg-Id` (`requestID:eventType:destSiteID`), so JetStream dedup silently drops every sibling after the first and remote replicas keep stale ordering; the covering test discards the msgID argument. In code quality, `removeMember`/`updateRole` wrap a non-member requester's or target's `ErrSubscriptionNotFound` unconditionally, so a documented `forbidden` reaches the client as `internal` and pages as ERROR. Coverage is 57.3%: the store layer is integration-only, and `ComputeSectionOrder`, `MoveSubscriptionSection` and the `InsertTeamsMeeting` idempotency gate are untested at any layer. The four ROOMS-stream publishes carry no dedup id, so a timeout-retried create yields two channels.

| Dimension | Score |
|---|---|
| Code quality | 3 |
| Architecture | 4 |
| Test coverage | 1 |
| Maintainability | 3 |
| Integration | 3 |
| Performance | 4 |

**Findings by severity:** 1 critical, 6 high, 21 medium, 19 low, 7 nitpick (54 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 3

### Evidence

- [high] Membership-gate errors in `removeMember`/`updateRole` collapse to `internal` instead of the typed not-member sentinel — `room-service/handler.go:700-703`, `:727-730`, `:780-783`, `:692-695` — Each site wraps the requester's/target's `GetSubscription`/`GetSubscriptionWithMembership` error unconditionally (`fmt.Errorf("get requester subscription: %w", err)`; `:782` even says "requester not found"), so a non-member requester or a non-member target gets `code: internal` and an ERROR-level "request failed" log (`pkg/errcode/classify.go:22-23,44-47`) rather than `forbidden`/`RoomNotMember`. `addMembers` does it right (`handler.go:906-912`), and `docs/client-api.md` (Update Member Role, "Target account is not a member of the room") promises a client error. Tests only cover `"db down"` (`handler_test.go:596-609`, `:956-971`, `:990-1000`), so the gap is unpinned.
- [medium] Store leaks the driver sentinel `mongo.ErrNoDocuments` across the store boundary — `room-service/store_mongo.go:366` — `GetSubscriptionWithMembership` returns `mongo.ErrNoDocuments` while every sibling maps to `model.ErrSubscriptionNotFound` (`:267`, `:284`, `:1120`). The handler therefore imports the Mongo driver to check both (`handler.go:790`, `:2027`) and misses it entirely at `:692-695`. CLAUDE.md puts the `ErrNoDocuments` check in the store, not the consumer.
- [medium] 20 of 32 log calls in `handler.go` drop the context and lose trace/request correlation — `room-service/handler.go:1198,1259,1443,1481,1532,1545,1558,1564,1570,1745,1887,1921,1932,1937,1943,1997,2069,2259,2618,2625` — The o11y handler injects `trace_id` from `ctx` (`o11y@v0.12.0/internal/log/handler.go:71-83`), and CLAUDE.md requires the request ID in all log lines; `slog.Error(...)` (no ctx) gets neither. The same message is emitted with `WarnContext` at `:1825` and without at `:1443`/`:1745`, so this is drift, not policy.
- [medium] Silent error discard — `room-service/handler.go:2257` — `if canonData, err := json.Marshal(canonEvt); err == nil {` drops the marshal error with no comment, violating "Never ignore errors silently — comment if intentionally discarded".
- [low] Wire-tier `errcode` constructed inside the store — `room-service/store_mongo.go:1019` — `ListOrgMembers` returns `errcode.BadRequest("list org members for %q", WithReason(RoomInvalidOrg))`; the handler then rebuilds a fresh errcode (`handler.go:422-423`). Tier-1 errcodes belong in handlers; the store should return a domain sentinel like `ErrRoomNotFound`.
- [low] Constructor DI bypassed — `room-service/handler.go:103` (14 positional args incl. three ints and a duration) plus 11 post-construction field assignments `room-service/main.go:383-393` — `handler.dekProvisioner/badge/valkey/graphClient/...` are set after `NewHandler`, so the constructor no longer describes the dependency set; tests build `&Handler{...}` literals directly (`handler_test.go:601`).
- [low] Repeated reconstruction of identical errcodes instead of sentinels — `handler.go:173` & `:276` (`NotFound("user not found", RoomUserNotFound)`), `:446,:482,:577,:622` (`BadRequest("invalid request")`), `:1149,:1160,:1169` (`Unavailable("timeout listing members…")`) — CLAUDE.md prefers package-level sentinels so `errors.Is` works and messages cannot drift; `helper.go` already holds 60+ such sentinels.
- [low] Hard-coded per-handler `5*time.Second` timeouts — `handler.go:1227`, `:1279`, `:2590`, `:2641` — duplicated magic number while `natsrouter.GuardConfig` (`main.go:61`) already carries a configurable `REQUEST_TIMEOUT`.
- [nitpick] Log-and-return — `room-service/memberlist_client.go:358-364` — `WarnContext(...)` then `return errcode.Internal(msg)`, which the router's `Classify` logs again at ERROR; one of the two is redundant.
- [nitpick] Inconsistent bare vs wrapped sentinel returns in the store — `store_mongo.go:939`, `:957`, `:979`, `:1945`, `:1969` return bare sentinels; `:242`, `:267`, `:1120` wrap with context. Harmless for `errors.Is` but the log `cause` loses the id/account on the bare paths.
- [nitpick] Misnamed sentinel — `helper.go:103` / `handler.go:2062-2063` — `errInvalidRestrictedSubject` ("invalid restricted subject") guards body fields `roomId`/`account`, not a subject.

### Recommendations

- [high] In `removeMember`/`updateRole`, branch on `errors.Is(err, model.ErrSubscriptionNotFound)` for the requester (→ `errNotRoomMember`) and target (→ `errTargetNotMember`) exactly as `addMembers:906-912` does — `handler.go:700,727,780,692` — and add TDD cases returning `ErrSubscriptionNotFound` for each; restores the documented `forbidden`/`bad_request` contract and stops user errors paging as ERROR.
- [medium] Make `GetSubscriptionWithMembership` return `fmt.Errorf("…: %w", model.ErrSubscriptionNotFound)` and drop the `mongo` import/`ErrNoDocuments` checks from `handler.go:790,2027` — `store_mongo.go:366` — one sentinel per concept, checked in one layer.
- [medium] Convert every `slog.Error/Warn/Info/Debug` in `handler.go` to the `*Context` form (or `c.WithLogValues`), and delete the manual `"request_id"` attrs once the context carries it — lines listed above — so best-effort fan-out failures are traceable to the request.
- [medium] Handle or explicitly comment the discarded marshal error — `handler.go:2257`.
- [low] Replace the 14-arg constructor + post-hoc field sets with a `HandlerConfig`/functional options and construct fully in `NewHandler` — `handler.go:103`, `main.go:357-393`.
- [low] Promote the duplicated `errcode.NotFound("user not found")`, `BadRequest("invalid request")`, and timeout `Unavailable` literals to `helper.go` sentinels; move `ListOrgMembers`'s errcode to a store sentinel — `store_mongo.go:1019`.
- [low] Source the four `5*time.Second` handler timeouts from `cfg.Guard.RequestTimeout` (or one named const) — `handler.go:1227,1279,2590,2641`.
