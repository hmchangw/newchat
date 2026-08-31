# search-sync-worker — Production Readiness Review

**Service:** `search-sync-worker` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

Thoughtful abstractions (`Collection`, `msgFetcher`, `flushPipeline`) and a genuinely well-engineered bulk pipeline — slot-based backpressure, dual size/interval flush triggers, precomputed metric attributes, clean `jsretry` discipline. Three things are seriously wrong. **A `critical`: the bot-message collection binds a stream whose subjects its own consumer filter cannot match**, so in `MODE=default` the consumer is rejected and the pod exits 1 — and a unit test enshrines the wrong filter. **The shipped default tuning trips its own coupling check** (`BULK_BATCH_SIZE × PIPELINE_DEPTH` exceeds the default `MaxAckPending`), and the check only warns. And **no ES request on the flush path carries a deadline**, so a hung connection holds a pipeline slot forever and blocks shutdown.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 2 / 5 |
| 6 | Performance | 4 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 12 | 18 | 13 | 5 | **49** |

---

## 2. Go code quality — 4 / 5

Disciplined, heavily-reasoned Go with correct `jsretry`/`errcode` worker tiering and nil-safe metrics; the defects are localized.

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | **The failed-bulk-item log emits ES's raw `error.reason`, which routinely quotes the offending field value** (`mapper_parsing_exception … Preview of field's value: '…'`) — and for the messages collection that field is `content`, i.e. **the message body**. This contradicts §3 ("Never log … full message bodies") *and* the rule the same file states 130 lines above: "the document body never belongs in an error that reaches the server log". `ErrorType` + `Status` are already logged and carry the diagnosis | `handler.go:293-295`, rule at `:158-159`; `pkg/searchengine/adapter.go:184` |
| medium | `context.Background()` passed to two blocking network calls on the message path, discarding the consumer span context `AddWithContext` was built to carry. Both are untraced and **uncancellable at shutdown** — and the Mongo call has no timeout of its own, so a slow primary can outlast the 25 s drain. Root cause: `Collection.BuildAction(data []byte)` takes no `ctx`, though the ctx is already in hand at `handler.go:98` | `messages.go:220`, `:304`; `collection.go:45` |
| medium | Two flush-failure logs drop the context every other log in the file uses, **breaking trace correlation exactly on the failure path** — `bulkCtx` is in scope and passed to the next three calls; only the `slog` call is context-free | `handler.go:242`, `:261` |
| low | Four bare `return nil, err`. The worst is `consumer_source.go:38`: a raw `Fetch` error surfaces with no indication it came from the domain-scoped HR consumer, and that branch is silently swallowed | `spotlight_org.go:149`; `consumer_source.go:38`; `spotlight.go:69`; `user_room.go:58` |
| low | Three silently-discarded `json.Marshal` errors with no justification comment — the convention exists in this very service ("Error discarded: input is a static map of literals"). `messages.go:376` is **not** that case: it marshals a `MessageDoc` built from event data, and a failure would push an empty/invalid `Doc` into a bulk action rather than fail the message | `messages.go:279`, `:376`; `spotlight_org.go:242` |
| low | Three exported `Handler` methods exist **only for tests** — production uses `AddWithContext`/`Take`+`FlushBatch` exclusively. §4: "Test helpers belong in `_test.go` files only" | `handler.go:82`, `:223`, `:363` |
| low | Audit-coverage gap (environmental): `govulncheck` + registry packs blocked | — |
| nitpick | Five metric-constructor fallbacks discard the error with no comment | `metrics.go:69`, `:75`, `:80`, `:85`, `:90` |

### Recommendations
- `medium` — Drop `"error", results[i].Error` from `handler.go:293`; keep `status`/`errorType`/`docID`/`index`. If the reason is needed, gate it behind `DEV_MODE` or truncate to the exception class before the colon.
- `medium` — Add `ctx context.Context` as the first parameter of `Collection.BuildAction`/`BuildActionSeq`/`BuildByQuery` and thread `AddWithContext`'s ctx through both resolvers; give the Mongo resolver its own bounded timeout.
- `medium` — Change the two flush-failure logs to `slog.ErrorContext(bulkCtx, …)`.
- `low` — Wrap the four bare returns; handle or comment the three marshal discards — at `messages.go:376` return the error and let `BuildAction` Ack-drop it as poison.
- `low` — Move `Add`, `Flush` and `MessageCount` into `handler_test.go` as unexported helpers, or delete them.

