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
