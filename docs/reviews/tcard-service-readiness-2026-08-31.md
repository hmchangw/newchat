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

---

## 3. Architecture — 4 / 5

Textbook conformance to the repo's Gin-service shape — consumer-defined store, constructor DI, typed `caarlos0/env` config, `pkg/shutdown.Wait`, shared knobs mounted as named fields — undercut by two unauthenticated endpoints and a validate/write split that leaves the service authoritative for reads but with no write path.

### Findings
- `high` — `POST /api/v1/cards/refresh` and `POST /api/v1/cards/validate` have no authentication or authorization; `registerRoutes` attaches no guard and `main.go` installs no auth middleware — `tcard-service/routes.go:8-10`, `main.go:110-117`
  `refresh` is an unauthenticated trigger for an unbounded full-collection Mongo scan. The repo already has the pattern for this (`admin-service/routes.go:17` groups mutating routes behind `requireAdmin`).
- `medium` — `/validate` is advisory only: it checks "highest version" against an in-memory snapshot and then writes nothing, and the code itself documents that cards arrive out-of-band — `tcard-service/handler.go:163-168`
  Two authors validate the same version concurrently and both pass; the only real guard is the unique index at `store_mongo.go:26-29`, whose duplicate-key error the client never sees because this service does not do the insert. The endpoint's contract is weaker than it appears.
- `low` — `MONGO_READ_PREFERENCE` is re-declared per service (19 copies, with divergent `envDefault`s) rather than owned by `mongoutil` and mounted as a named field — `tcard-service/main.go:40`, e.g. `portal-service/main.go:62`, `upload-service/main.go:57`
  Fleet-wide, not a tcard defect, and the differing defaults are deliberate policy; noted because CLAUDE.md §6 names exactly this shape as the cause of divergent shared-key config.

Confirmed conformant, no finding: consumer-owned `CardStore` with only `ListCards` (`store.go:29-32`); `NewCardHandler` constructor DI (`handler.go:44`); file organization matches CLAUDE.md exactly; `MONGO_URI` marked `required`, everything else defaulted, `Pool mongoutil.PoolConfig` / `HTTP ginutil.TimeoutConfig` mounted as named fields (`main.go:27-44`); middleware order identical to `portal-service/main.go:150-157`; `pkg/shutdown.Wait` with a 25s budget under the 30s grace period (`main.go:136-147`). The service touches no NATS, so `BOOTSTRAP_STREAMS`, `pkg/stream`, and the INBOX/OUTBOX ownership rules do not apply.

### Recommendations
- `high` — Put `/refresh` and `/validate` behind an authorization guard modelled on `admin-service`'s `requireAdmin` group, or document the ingress ACL that fronts them.
- `medium` — Decide the ownership question: either give tcard-service the write path (`POST /register` was planned and removed — see `handler_test.go:491`) so validate-and-insert is atomic against the unique index, or demote `/validate` to a lint endpoint and say so in its response.
- `low` — Raise `MONGO_READ_PREFERENCE` into `mongoutil` as a named config field with per-service `envDefault` set at the mount point.

---
