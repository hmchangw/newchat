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


## 2. Code quality — score 4

### Evidence

- [high] Log-AND-return on the large-room rejection path — `message-gatekeeper/handler.go:447-454` — `slog.Info("send blocked", …)` is emitted and then `errLargeRoomPostRestricted` is returned; the caller at `handler.go:224` passes it to `errnats.Marshal`, whose `errcode.Classify` (`pkg/errcode/classify.go:40`) logs it again as "request failed". CLAUDE.md's "Never log AND return" rule is violated on a per-message hot path. Same family at `handler.go:160`+`164`: `slog.Warn("invalid subject")` plus the `Classify` log inside `errnats.Marshal` (which runs even when `accountFromSubject` returns "" and `sendReply` no-ops).
- [medium] Request-ID context bridge is local to `processMessage`, so the client reply and the NAK path see a different ID — `handler.go:355` vs `handler.go:259`, `307-318`, `254`. `main.go:234` builds ctx via `logctx.ConsumeContext`, which (`pkg/natsutil/request_id.go:112-116`) mints a fresh UUID when the inbound `X-Request-ID` header is absent; `processMessage` overrides that with the payload `requestId` on its own ctx copy only. `sendReply` builds the reply with `natsutil.NewMsg(ctx, …)` from the outer ctx, so the reply's `X-Request-ID` header carries the minted ID (≠ payload `requestId`), and `jsretry.Nak`'s failure log (`pkg/jsretry/jsretry.go:115-116`) reads `RequestIDFromContext` = minted ID while neighbouring lines log `req.RequestID` via `WithLogValues`. Tests at `handler_test.go:1223-1256` pin only the canonical-publish header, not the reply header.
- [medium] Bare error returns — `message-gatekeeper/metacache.go:21` (`return nil, err` from `roommetacache.WrapStore`) and `subcache.go:114` (`return nil, ctx.Err()`). CLAUDE.md §3 "never return bare err". `metacache.go:21` is startup-only (logged by `main.go:159`), so production impact is nil; `subcache.go:114` is on the hot path but the handler re-wraps at `handler.go:425`.
- [low] Context-less `slog` calls on the message path drop request-ID/trace correlation — `handler.go:160`, `:320`, `:447`, `:494`. All four use `slog.Warn/Error/Info` instead of the `*Context` variants, so the `request_id`/`room_id` values attached via `errcode.WithLogValues` (`handler.go:145`, `:173`) and the trace correlation never reach these lines. `:320` ("reply to client failed") is precisely the line an operator would search by request ID.
- [low] Redundant triple-wrapping of the subscription error chain — `store_mongo.go:44` ("get subscription for %s in %s"), `subcache.go:108` ("get cached subscription"), `handler.go:425` ("get subscription for user %s in room %s") repeat the same identifiers; the final string reads `get subscription for user a in room r: get cached subscription: get subscription for a in r: …`.
- [low] Client-controlled strings are reflected into errcode messages — `handler.go:350`, `:359`, `:363`, `:370`, `:406` embed `req.RequestID`/`req.ID`/`req.ThreadParentMessageID`/`req.QuotedParentMessageID`/`req.Type` via `%q`; the message reaches both the client envelope and the server log (`Classify` `cause`). The parse-error path (`handler.go:178-180`) deliberately avoids this; `:380-383` already shows the structured `WithMetadata` alternative. Bounded by NATS max_payload, `%q`-escaped and JSON-encoded, so low.
- [low] Test hygiene — `subcache_test.go:114` and `:146` use `time.Sleep` (the latter for goroutine scheduling in the singleflight test, which is timing-dependent under `-race`); `handler_test.go:1121`, `:1143`, `:1189`, `bootstrap_test.go:98`, `subcache_test.go:279` assert on `err.Error()` substrings rather than `errors.As` + `Code`/`Reason`.
- [nitpick] Mixed slog key styles in one file — `userCount`, `userId`, `messageId` (`handler.go:451`, `:495`) vs `room_id`, `request_id`, `stream_wait_ms` elsewhere (`:173`, `:295`); hampers log querying.

### Recommendations

- [high] Delete the `slog.Info` at `handler.go:447-453` and carry `userCount`/`threshold` via `errcode.WithLogValues(ctx, …)` (or `WithMetadata` on a fresh error) so `Classify` emits the single, enriched line; at `handler.go:160-164` drop the `slog.Warn` or switch that reply to `errnats.MarshalQuiet` — one log per rejection.
- [medium] Hoist `ctx = natsutil.WithRequestID(ctx, req.RequestID)` into `HandleJetStreamMsg` immediately after the `parseErr` check (`handler.go:187`), before `processMessage`, so the reply header, `jsretry.Nak` logs and the canonical publish share the payload ID; add a unit test asserting the reply `*nats.Msg` header equals `req.RequestID` when no inbound header is present.
- [medium] Wrap `metacache.go:21` (`fmt.Errorf("wrap room meta store: %w", err)`) and `subcache.go:114` (`fmt.Errorf("get cached subscription: %w", ctx.Err())`).
- [low] Convert `handler.go:160`, `:320`, `:447`, `:494` to `slog.*Context(ctx, …)`.
- [low] Collapse the subscription chain: keep identifiers only in the store wrap (`store_mongo.go:44`) and make `handler.go:425` a plain `fmt.Errorf("get subscription: %w", err)`.
- [low] Replace `assert.Contains(err.Error(), …)` with `errors.As`+`Code` assertions; replace the `time.Sleep` at `subcache_test.go:146` with a `started` channel counted inside the gated `DoAndReturn` (and inject a clock or use a sub-ms TTL with a retry loop at `:114`).
- [nitpick] Normalise slog keys to snake_case in `handler.go:451`, `:495`.
