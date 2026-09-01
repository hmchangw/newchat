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

---

## 3. Architecture — 4 / 5

File layout, consumer-side interface, constructor DI and `pkg/shutdown` usage are exactly per CLAUDE.md; two shared-knob configs that the rest of the fleet mounts are simply absent.

### Findings
- `medium` — no `Pool mongoutil.PoolConfig` on the config struct — `teams-room-inspector/main.go:29-39`. CLAUDE.md §6 names it as a knob declared once and mounted as a named field; siblings do (`teams-hr-sync/config.go:45`, `teams-chat-sync/main.go:30`, `media-service/config.go:68`). The inspector runs on driver defaults with no operator lever.
- `medium` — no `ginutil.TimeoutConfig` and no `ginutil.Timeout` on the engine — `main.go:50-65`, `routes.go:7-10`. That type exists precisely to stop per-service re-declaration (`pkg/ginutil/timeoutconfig.go:10-19`). See D6 for the runtime consequence.
- `low` — hand-rolled liveness handler and no `/readyz` — `handler.go:38-40`, `routes.go:8`. `docs/health-probes.md:3-11` says every service serves both via `pkg/health`; several Gin peers also omit `/readyz`, so this is fleet-wide drift the service inherits.
- `low` — the room-id derivation is duplicated against its producer, by hand, and the code says so — `handler.go:44-46,68` vs `room-worker/teamsroomcreate.go:62`. `pkg/teamsmigrate` already hosts the analogous `EmployeeIDFromGraphID` (`pkg/teamsmigrate/teamsmigrate.go:65-67`), so the shared-helper pattern exists and was not used.

N/A by design: no JetStream, so `BOOTSTRAP_STREAMS`/`pkg/stream`/consumer-pattern rules do not apply; `env.ParseAs` with `required,notEmpty` on `MONGO_URI`/`SITE_ID` and `envDefault` elsewhere is correct (`main.go:30-38,70-73`).

### Recommendations
- `medium` — mount `Pool mongoutil.PoolConfig` and pass it through `mongoutil.ConnectRead` (`main.go:86`).
- `medium` — mount `HTTP ginutil.TimeoutConfig`, `Validate()` at load, `r.Use(cfg.HTTP.Middleware())`.
- `low` — extract `RoomIDFromChatID(chatID string) string` into `pkg/teamsmigrate` and call it from both `room-worker/teamsroomcreate.go:62` and `handler.go:68`.
