# search-sync-worker — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `e73a723` (base `main`)  
**Overall score:** 3.0 / 5 (baseline 2026-09-01: 3.2, Δ -0.2)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

search-sync-worker is a well-structured Elasticsearch indexer whose collection abstraction, bulk-error matrix and backoff discipline are all solid, but this audit found one silent production defect that outranks everything else: the `bot-message-sync` consumer's filter subject (`chat.msg.canonical.{site}.*`) never matches the BOT-MESSAGES-CANONICAL stream it binds (`chat.bot.canonical.{site}.>`), and nats-server v2.12.6 no longer rejects a non-subset filter, so the durable is created and delivers nothing — no bot message has ever been indexed, with no log or metric to show it. The fix exists on `origin/fix/search-sync-bot-filter` but is not on this branch, and because the durable's cursor already sits at stream end, correcting the filter in place will not backfill unless the durable is versioned or deleted. Three further gaps recur across dimensions: ES bulk/update-by-query calls have no deadline (a hung ES node wedges a whole collection and then the consumer loop), `main()` is a 319-line untestable monolith that holds most of the 12.6-point coverage shortfall, and the `Collection` interface carries no `context.Context`, so the thread-parent ES lookup and Teams identity Mongo lookup run outside the consumer span and outside shutdown cancellation. The Mongo `ResolveIdentities` resolver has no test at any tier, `ES error.reason` is logged verbatim (and for `mapper_parsing_exception` carries message content), and stream bootstrap is inlined in `main` instead of the repo's `bootstrap.go` helper.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 3 |
| Test coverage | 2 |
| Maintainability | 3 |
| Integration | 3 |
| Performance | 3 |

**Findings by severity:** 0 critical, 7 high, 16 medium, 19 low, 7 nitpick (49 total).  
**Highest-risk dimension:** Test coverage (2).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 4

### Evidence

- [medium] Request/trace context is dropped at the Collection boundary — `search-sync-worker/collection.go:42` — `BuildAction(data []byte)` carries no `context.Context`, so the per-message consumer span delivered to `AddWithContext` (`handler.go:88`) never reaches the collections. `messages.go:220` and `messages.go:304` therefore call the ES parent lookup and the Mongo identity lookup with `context.Background()`, and the resulting `slog.Warn/Error` lines at `messages.go:240`, `:247`, `:306`, `:373` carry no correlation ID. CLAUDE.md §3 requires the ID to be propagated via context and present in all log lines; the 2s lookup timeout (`thread_parent_resolver.go:29`) is also detached from shutdown cancellation.
- [medium] ES `error.reason` is logged verbatim and can carry message content — `search-sync-worker/handler.go:295` — `results[i].Error` is the raw ES `reason` string (`pkg/searchengine/adapter.go:31`, `:183`). For `mapper_parsing_exception` (a 400, the exact case this branch drops as poison) Elasticsearch appends `Preview of field's value: '…'`, i.e. the indexed `content`. This contradicts the intent stated two functions up (`handler.go:156-157`: "the document body never belongs in an error that reaches the server log") and the CLAUDE.md rule never to log message bodies.
- [low] Batch-level failures logged without the span context in hand — `search-sync-worker/handler.go:242` and `:261` — `slog.Error(...)` is used while `bulkCtx` (the flush span with links to every source message) is available; `ErrorContext(bulkCtx, ...)` would keep the log trace-correlated like the per-item line at `:293`.
- [low] Bare `return nil, err` — `search-sync-worker/consumer_source.go:38` — `rawConsumerAdapter.Fetch` returns the JetStream error unwrapped. CLAUDE.md §3 forbids bare returns; the sibling `o11yConsumerAdapter.Fetch` is a pure pass-through, but this one branches on the error and should add the "fetch HR batch" context.
- [low] Discarded `json.Marshal` errors without the required justification comment — `search-sync-worker/messages.go:279`, `messages.go:376`, `spotlight_org.go:242` — the same pattern at `messages.go:160` and `spotlight.go:47` carries the "marshal cannot fail" comment; these three do not, violating "never ignore errors silently — comment if intentionally discarded". `messages.go:376` marshals a `MessageDoc` built from event data, so the assumption is weaker than for the static literal maps.
- [low] Dead production API — `search-sync-worker/handler.go:81` (`Add`) and `handler.go:223` (`Flush`) — neither is called outside `handler_test.go:91-199`; the consumer loop uses `AddWithContext` + `Take`/`FlushBatch`. `Add` also hardcodes `context.Background()`, so a future caller would silently lose tracing.
- [nitpick] Inconsistent template-name/doc-ID construction — `search-sync-worker/spotlight_org.go:85` builds `"%s_template"` with `fmt.Sprintf` while every other collection uses a `searchindex.XxxTemplateName` helper; `spotlight.go:83` uses `fmt.Sprintf("%s_%s")` on the fan-out path where `account + "_" + roomID` is cheaper and clearer.
- [nitpick] `Handler`, `NewHandler`, `Collection`, `Store`, `SpotlightOrgIndex` are exported inside `package main` — `search-sync-worker/handler.go:48`, `collection.go:14`, `store.go:12`, `spotlight_org.go:193` — CLAUDE.md asks for unexported handler/store implementations, but ten other `package main` services share this convention, so it is a repo-wide choice, not a service defect.

### Recommendations

- [medium] Add `ctx context.Context` to `Collection.BuildAction`/`BuildActionSeq`/`BuildByQuery` and thread it from `AddWithContext` through `resolveThreadParentCreatedAt` and `resolveTeamsIdentities`; switch the four `messages.go` log calls to `*Context` variants — `collection.go:42`, `handler.go:124`, `messages.go:220`, `:304` — restores request-ID/trace correlation for the ES and Mongo lookups and lets shutdown cancel an in-flight parent lookup.
- [medium] Stop logging `results[i].Error` raw; log `ErrorType` + status only, or pass the reason through a truncating/`Preview of field's value` stripping helper in `pkg/searchengine` — `handler.go:295` — closes the message-content leak on the 400/poison path without losing the diagnostic type.
- [low] Use `slog.ErrorContext(bulkCtx, ...)` at `handler.go:242` and `:261` — keeps whole-batch failures joined to the flush span and its source-message links.
- [low] Wrap the error at `consumer_source.go:38` (`fmt.Errorf("fetch HR consumer batch: %w", err)`) — brings the adapter in line with the repo error-wrapping rule at zero cost.
- [low] Add the "static literals, marshal cannot fail" comment at `messages.go:279`, `spotlight_org.go:242`, and at `messages.go:376` either handle the error (Ack-drop as poison via the existing `BuildAction` error path) or document why a `MessageDoc` can never fail to marshal.
- [low] Move `Handler.Add` and `Handler.Flush` into `handler_test.go` as test helpers (or make tests call `AddWithContext`/`Take`+`FlushBatch`) — `handler.go:81`, `:223` — removes two untraced entry points from production code.
- [nitpick] Add `searchindex.SpotlightOrgTemplateName` and use it at `spotlight_org.go:85`; replace the `fmt.Sprintf` doc-ID at `spotlight.go:83` with string concatenation.

## 3. Architecture — score 3

### Evidence

- [high] `bot-message-sync` consumer filter cannot match anything on its stream, so bot messages are silently never indexed — `search-sync-worker/messages.go:122-130` — `FilterSubjects` returns `subject.MsgCanonicalMessageWildcard` (`chat.msg.canonical.{site}.*`, `pkg/subject/subject.go:708-710`) for both the user and bot collections, but the bot collection binds `BOT-MESSAGES-CANONICAL` whose only subject is `chat.bot.canonical.{site}.>` (`messages.go:104-107`, `pkg/subject/bot.go:73-75`). nats-server v2.12.6 never raises `JSConsumerFilterNotSubsetErr` (defined in `jetstream_errors_generated.go:661`, no caller; `checkConsumerCfg` at `server/consumer.go:678-830` only checks filters against each other), so the durable is created and simply delivers nothing. `messages_test.go:621-622` pins the wrong filter. The audited tree has no `checkFilterSubjects` / `subjectoverlap.go`; the fix lives on `origin/fix/search-sync-bot-filter` (a699c11..8d483b5) and is not an ancestor of this branch.
- [high] Correcting the filter in place will not backfill: the durable is not versioned — `messages.go:79` — `getNextMsg` advances `o.sseq` to stream end on `ErrStoreEOF` (`server/consumer.go:4521-4528`), so the existing `bot-message-sync` cursor already sits past every bot message ever published. `DeliverPolicy=All` is honored only at creation (`pkg/stream/consumer.go:51-53`), and the fix branch keeps the same durable name, so merging it as-is leaves historical bot messages unindexed until someone deletes or renames the durable.
- [medium] Stream bootstrap is inlined in `main` instead of a `bootstrap.go` `bootstrapStreams(ctx, js, siteID, enabled)` helper — `search-sync-worker/main.go:290-309` — CLAUDE.md says "never inline"; the other ten JetStream services (`inbox-worker/bootstrap.go`, `bot-message-worker/bootstrap.go`, …) follow the helper convention. Behavior is otherwise correct (opt-in via `BOOTSTRAP_STREAMS`, `main.go:30-34`; INBOX and HR skipped, `main.go:295-301`; Name+Subjects only, `messages.go:99-112`, `inbox_stream.go:26-31`, `spotlight_org.go:71-74`), but the skip logic and the Name+Subjects invariant have no unit test (`main_test.go` covers only `pushMappings`).
- [low] `Collection.StreamConfig` returns a full `jetstream.StreamConfig` and its doc invites `Sources, SubjectTransforms` — `search-sync-worker/collection.go:14-17` — this contradicts the "schema = Name+Subjects, federation routing is ops/IaC" rule; returning `stream.Config` would make the rule structural instead of a comment.
- [low] `BuildAction(data []byte)` carries no context, so the ES parent lookup and the Mongo identity lookup run on `context.Background()` — `messages.go:220`, `messages.go:304` — the consumer span/request-id that `Handler.AddWithContext` (`handler.go:88`) already holds cannot reach them; the resolver's 2s timeout (`thread_parent_resolver.go:95`) is the only bound.
- [low] Dependencies are poked in after construction rather than injected — `main.go:232` (`teamsUsers`), `main.go:245-256` (`parentResolver`), `main.go:357` (`handler.metrics`) — and `NewHandler` takes a variadic `tracers ...trace.Tracer` (`handler.go:63`); a nil-safe field is workable but a mis-wired pod degrades silently instead of failing construction.
- [nitpick] The consumer loop is a third pattern (pull `Fetch` + batch flush + `flushPipeline`, `main.go:506-563`), not one of the two CLAUDE.md sanctions; it is the right shape for ES bulk and is well documented in code, but CLAUDE.md does not list it as an exception.

Verified clean: no bare `Nak`/`NakWithDelay(0)` (`handler.go:114`, `handler.go:317`, `handler.go:355` all via `pkg/jsretry`); backoff via `stream.DurableConsumerDefaults` with no hardcoded `cc.BackOff` (`main.go:611-618`, asserted by `consumer_config_test.go:62`); all subjects via `pkg/subject` builders (no raw `Sprintf` subjects); store interface in the consumer with only `Bulk`/`UpdateByQuery` (`store.go:14-19`); INBOX/HR consumed as non-owner; user/teams/spotlight/user-room/spotlight-org filters all overlap their streams (`chat.msg.canonical.{site}.*` ⊂ `.>`, `chat.teams…batch` ⊂ `chat.teams…>`, `InboxMemberEventSubjects` ⊂ `internal.>`/`external.>`, `chat.hr.{c}.employees.upsert` ⊂ `chat.hr.{c}.>`); shutdown order `close(stopCh)` → wait `doneChs` → `natsutil.Drain` → Mongo disconnect → health → obs (`main.go:400-419`) matches CLAUDE.md.

### Recommendations

- [high] Merge `origin/fix/search-sync-bot-filter` (bot filter = `BotCanonicalMessageWildcard`, startup `checkFilterSubjects` against the deployed stream, `subjectoverlap.go` + tests) — `messages.go:122-130` — restores bot-message indexing and makes a filter that selects nothing a startup failure instead of a silent no-op.
- [high] Version the bot durable when the filter lands (e.g. `bot-message-sync-v2`) or add a documented runbook step to delete `bot-message-sync` before rollout — `messages.go:79` — otherwise `DeliverPolicy=All` never fires and every bot message published so far stays unsearchable.
- [medium] Extract `main.go:290-309` into `bootstrap.go` with `bootstrapStreams(ctx, js, siteID, enabled)` behind a small `streamManager` interface (mirror `inbox-worker/bootstrap.go`), and unit-test the INBOX/HR skip and the disabled no-op — brings the service onto the repo convention and puts the ownership rule under test.
- [low] Change `Collection.StreamConfig` to return `stream.Config` and build the `jetstream.StreamConfig{Name, Subjects}` once in the bootstrap helper — `collection.go:14-17` — removes the per-collection copy and the invitation to add `Sources`/`SubjectTransforms`.
- [low] Thread `ctx` into `BuildAction`/`BuildActionSeq`/`BuildByQuery` (`collection.go:44`, `collection.go:59`) so the parent-createdAt and teams-identity lookups inherit the consumer span and request id — `messages.go:220`, `messages.go:304`.
- [low] Move `parentResolver`, `teamsUsers` and `metrics` into the constructors (`newMessageCollection`, `newTeamsMessageCollection`, `NewHandler`) — `main.go:232`, `main.go:245-256`, `main.go:357` — so a missing dependency is a compile/startup error, not a silently degraded index.
- [nitpick] Add the batch-flush pull loop to CLAUDE.md's consumer-pattern list as the sanctioned shape for bulk indexers, so the next reviewer does not flag `main.go:506-563` as a deviation.
