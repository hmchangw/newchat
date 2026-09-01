# teams-room-inspector — Production Readiness Review

**Service:** `teams-room-inspector` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.3 / 5

670 lines across nine files, every function short, file layout and DI exactly per CLAUDE.md, and comments that explain WHY — including the one at `pkg/model/teams.go:104-110` explaining why an exceeded batch cap looks like a healthy run rather than a failure. Two bounded queries, an explicit projection, no `$lookup`, secondary-preferred reads.

The service's exposure is that it is **the read side of a federated verification contract, with no deadline and no index guarantee**. `newServer` sets only `ReadTimeout`/`WriteTimeout`, which bound the socket and do **not** cancel the handler context — the repo's own helper says exactly this — so a stalled Mongo read pins the request goroutine and its pooled connection indefinitely, with no `MaxPoolSize` lever because `mongoutil.PoolConfig` is not mounted either. And the subscriptions aggregation depends on an index owned by a different service without calling `mongoutil.WarnMissingIndexes`, so a dropped index degrades a 500-id batch to a full collection scan **with no signal**.

Coverage is 47.7% — below the critical line, though concentrated in wiring rather than logic.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 1 | 7 | 10 | 2 | **21** |

---

## 2. Go code quality — 4 / 5

Idiomatic, well-commented, correct `errcode` Tier-1 usage and `slog`-only logging; the one real defect is a silently discarded bind error.

### Findings
- `medium` — the `ShouldBindJSON` error is dropped without a comment and without `WithCause`, so a malformed body and a `MaxBytesReader` trip are indistinguishable in server logs — `teams-room-inspector/handler.go:52-55`. CLAUDE.md §3: "Never ignore errors silently — comment if intentionally discarded"; `errcode.Classify` logs a cause once server-side (`pkg/errcode/errhttp/write.go:13-16`) and never serializes it.
- `low` — sentinel compared with `!=` instead of `errors.Is` — `teams-room-inspector/main.go:115`. Peers use `errors.Is` (`media-service/main.go:143`, `admin-service/main.go:174`); tcard/portal/upload share the older form, so this is drift, not a bug.
- `low` — audit-coverage gap, not a service defect: gosec and the repo-owned semgrep rules are clean repo-wide; `govulncheck` and the semgrep registry packs could not run (egress blocked, per GLOBAL_PREP).
- `nitpick` — `//nolint:gocritic // hugeParam` on `newServer` (`main.go:50`) exists only because the whole `Config` is passed for one field (`cfg.Port`, `main.go:60`).

Correct by inspection: raw `fmt.Errorf("read room states: %w", err)` for infra (`handler.go:73`), named constructors for client errors (`handler.go:53,57,61`), no log-and-return, no `fmt.Println`, store impl unexported (`store_mongo.go:26`), both `json` and `bson` camelCase tags on the wire structs (`pkg/model/teams.go:130-146`).

### Recommendations
- `medium` — attach the bind error: `errcode.BadRequest("decode verify request", errcode.WithCause(err))` (`handler.go:53`). It is not an `*errcode.Error`, so `WithCause` is legal, and a JSON syntax error carries no body content.
- `low` — switch `main.go:115` to `!errors.Is(err, http.ErrServerClosed)`.
- `nitpick` — pass `port string` to `newServer` and delete the `nolint`.
