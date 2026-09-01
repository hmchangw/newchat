# tcard-service — Production Readiness Review

**Service:** `tcard-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.5 / 5

Textbook conformance to the repo's Gin-service shape, and a read path that is genuinely excellent: a lock-free `atomic.Pointer` snapshot means a card fetch does zero Mongo reads, zero locks and zero re-marshalling — the cached bytes go straight to the wire.

The weakness is on the write and refresh side, and the two halves compound. **`POST /api/v1/cards/refresh` and `POST /api/v1/cards/validate` have no authentication or authorization** — `refresh` is an unauthenticated trigger for an unbounded full-collection scan. And **`Load` starts its 60-second budget before acquiring a non-context-aware mutex**, so N concurrent refresh calls serialize into N sequential full scans, each pinning a goroutine and a request context past its own HTTP deadline, with later waiters reaching Mongo with almost none of their budget left. Separately, `/validate` is advisory only: it checks "highest version" against an in-memory snapshot and then writes nothing, so two authors validating the same version concurrently both pass.

Coverage is 69.7%, and the service's own CI gate excludes two files before measuring — reporting ~97% and passing the same 80% threshold this audit fails.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 4 | 9 | 13 | 1 | **27** |

---

## 2. Go code quality — 4 / 5

Idiomatic, well-commented, `slog`-clean and errcode-correct throughout; the only real defects are a sentinel compared with `!=` instead of `errors.Is`, decorative struct tags, and an errcode surface with no `reason` on an endpoint whose whole purpose is client-side branching.

### Findings
- `medium` — `err != http.ErrServerClosed` compares a sentinel directly instead of `errors.Is` — `tcard-service/main.go:150`
  The repo's newer Gin services (`media-service/main.go:143`, `admin-service/main.go:174`, `botplatform-service/main.go:160`) use `errors.Is`; this is the older half of a split convention and breaks if the stdlib ever wraps.
- `medium` — `HandleValidate` returns eleven distinct `errcode.BadRequest` plus one `Conflict` with no `WithReason`, and there is no `pkg/errcode/codes_tcard.go` — `tcard-service/handler.go:144-169`, `handler.go:174-215`
  A card-authoring client must branch on prose strings to tell "path must have exactly 3 segments" from "version must be 1.5". CLAUDE.md §3 reserves `WithReason` for exactly this case.
- `low` — `card`'s `json`/`bson` tags are dead: the doc is decoded into `bson.D` and the struct is built field-by-field in `docToCard`, and `Template` is written to the wire raw via `c.Data` — `tcard-service/store.go:12-16`, `store_mongo.go:73-96`, `handler.go:100`
  Tags that no codec reads mislead the next reader into thinking the shape is wire-bearing.
- `low` — `cardDoc` carries `json` tags only, against CLAUDE.md's "all model structs get both `json` and `bson`" — `tcard-service/store.go:20-27`
  The inline comment ("never persisted") justifies it; flagged so the deviation is on record, not to be changed.
- `low` — the skip warning cannot name the offending document, because the projection removes `_id` and the branch fires precisely when `path` is absent — `tcard-service/store_mongo.go:41`, `store_mongo.go:60`
  An operator gets "a card doc is broken" with nothing to grep on.
- `low` — SAST audit coverage is incomplete: `gosec` and the 18 repo-owned semgrep rules are clean repo-wide, but `govulncheck` and the semgrep registry packs could not run (blocked egress). Environmental, not a service defect — per `GLOBAL_PREP.md`.

### Recommendations
- `medium` — Switch `main.go:150` to `!errors.Is(err, http.ErrServerClosed)`, matching `media-service`/`admin-service`.
- `medium` — Add `pkg/errcode/codes_tcard.go` with reasons for the validate failure classes (`CardPathShape`, `CardVersionNotSemver`, `CardSchemaPinned`, `CardVersionNotHighest`) and attach them via `WithReason`.
- `low` — Delete the tags on `card`, or add a round-trip test that makes them load-bearing.
- `low` — Keep `_id` in the projection and log it in the skip warning at `store_mongo.go:60` (it is already dropped from the payload in `docToCard`, so nothing leaks).

---
