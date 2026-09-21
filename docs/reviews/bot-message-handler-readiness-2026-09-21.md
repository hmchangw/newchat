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


## 2. Code quality — score 4

### Evidence

- [high] Unit coverage is 40.9% (81/198 stmts), below the CLAUDE.md 80% MUST — `bot-message-handler/handler.go:79`, `bot-message-handler/store_mongo.go:21-108` — `handleSendDM` (the only DM path) is 0% with no test at all; `Register`, `PublishWithMsgID`, `run` and every `store_mongo.go` method are 0%. There is no `integration_test.go`, so the Mongo projections (`u._id`, `t`, `siteId`) are never exercised against a real driver. Coverage is scored mainly under test-coverage; it is repeated here because it is a merge-blocking MUST.
- [medium] Store double is a hand-rolled fake, not a mockgen mock — `bot-message-handler/handler_test.go:26-47`, `bot-message-handler/store.go:11` — CLAUDE.md §4 says mock with `go.uber.org/mock`, generated into `mock_store_test.go`, with a `//go:generate mockgen` directive in `store.go`. Neither exists. All three `bot-*` services share this deviation (`bot-room-service/handler_test.go`, `bot-message-worker/handler_test.go`), so it is a family-level drift, not a one-off; the fake also silently returns `nil, nil` for an unset `ListMemberIDsFn` (`handler_test.go:40-42`), contradicting its own "unset fields panic loudly" comment.
- [low] Publish failure is dressed as an errcode instead of a raw wrapped error — `bot-message-handler/handler.go:201` — `errcode.Internal("publish canonical", errcode.WithCause(err))` where CLAUDE.md Tier 1 says infra failures return `fmt.Errorf("...: %w", err)` and collapse to `internal` at the boundary. Behaviourally equivalent on the wire, but diverges from the sibling idiom (`bot-room-service/sysmsg.go:28`, `room-service/handler.go:405`) and from the same file's own store-error handling (`handler.go:100,148`).
- [low] Canonical publish drops the request ID — `bot-message-handler/handler.go:44-46` — `JetStreamPublisher` calls bare `js.Publish(ctx, subj, data, ...)`, so the `X-Request-ID` resolved by `natsrouter` never reaches the `MessageEvent` header. `message-gatekeeper/handler.go:533` builds the canonical message with `natsutil.NewMsg(ctx, ...)` for exactly this reason. OTel trace context still propagates via the o11y wrapper, and `bot-message-worker` does not currently read the header, so impact is log-correlation only.
- [low] ~35 lines duplicated between the two send handlers — `bot-message-handler/handler.go:79-125` vs `:128-184` — identical header parsing, subscription check, content validation, mention canonicalisation and `model.Message` construction. The only intentional differences are the room-ID source and `verifyRoomExists` being skipped on the DM path. A future field added to `model.Message` must be added twice; the DM copy is the untested one.
- [low] `deploy/azure-pipelines.yml` is missing — `bot-message-handler/deploy/` — CLAUDE.md §1/§5 say every service ships one; 29 of 37 services do. Same gap in `bot-room-service` and `bot-message-worker`.
- [nitpick] `Subscription`/`Room` carry no `json`/`bson` tags — `bot-message-handler/store.go:28-39` — they are decoded via anonymous projection structs in `store_mongo.go`, so nothing breaks, but the rule "all model structs get both tags" is not met.
- [nitpick] Early-return paths in `run()` skip `obsShutdown`/`nc.Drain` — `bot-message-handler/main.go:71-92` — a NATS/JetStream/Mongo failure after `obs.Init` returns without flushing the exporter; the process exits anyway, so this only loses the final error span/metrics.
- [nitpick] `fakePublisher` mixes `atomic` on `calls` with unsynchronised writes to `lastSubj/lastData/lastMsgID` — `bot-message-handler/handler_test.go:57-63` — harmless today (single goroutine) but signals a concurrency expectation the fake does not honour.

### Recommendations

- [high] Add a table-driven `TestHandleSendDM_*` suite mirroring the room suite (happy path, missing `userID` param, no subscription, header/content/mention rejections, publish error) — `handler.go:79` — closes the largest 0% block on the request path.
- [high] Add `integration_test.go` (`//go:build integration`, `testutil.MongoDB`, `testutil.RunTests`) for `FindSubscription`/`FindRoom`/`ListMemberIDs`/`FindUser` including `ErrNotFound` mapping — `store_mongo.go` — the field projections are the only thing that can silently break against a real schema.
- [medium] Add `//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main` to `store.go:11` and migrate the fake; do the same across `bot-*` in one PR so the family matches the repo — removes the hand-rolled fake's inconsistent nil-behaviour.
- [low] Replace `errcode.Internal(...WithCause(err))` with `fmt.Errorf("publish canonical: %w", err)` — `handler.go:201` — aligns with Tier 1 and the sibling publishers.
- [low] Have `JetStreamPublisher.PublishWithMsgID` call `j.JS.PublishMsg(ctx, natsutil.NewMsg(ctx, subj, data), jetstream.WithMsgID(msgID))` — `handler.go:45` — carries `X-Request-ID` onto BOT-MESSAGES-CANONICAL like the gatekeeper does.
- [low] Extract the shared tail of both handlers into `func (h *handler) send(c *natsrouter.Context, roomID string, ident *BotIdentity, messageID string, createdAt time.Time, req BotSendRoomRequest) (*BotSendResponse, error)` — `handler.go:95-124,143-183` — one place to build `model.Message`, and DM tests then cover the shared body.
- [low] Add `deploy/azure-pipelines.yml` following an existing service's file — `bot-message-handler/deploy/`.
