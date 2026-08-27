# Production-Readiness Review — `history-service`

| | |
|---|---|
| **Service** | `history-service` |
| **Date** | 2026-08-27 |
| **Branch** | `claude/production-readiness-history-service-7vbzpz` |
| **Base commit** | `e7e94c9` |
| **Overall score** | **3.4 / 5** |
| **Method** | 6 independent expert audits (code quality, architecture, test coverage, maintainability, integration, performance), each reading `CLAUDE.md` + the full service tree + the `pkg/` packages it imports |

## TL;DR

`history-service` is a well-engineered read/mutate service — 19 NATS request/reply RPCs over Cassandra message history and MongoDB room metadata, ~6.1k lines of production code behind ~15.6k lines of tests. Five of six dimensions score 4-ish: error wrapping, `pkg/errcode` tier discipline, `pkg/subject` builder usage, bucket math, page clamping, graceful shutdown and `docs/client-api.md` sync are all clean, and gosec reports zero findings. Two things hold it back from production-ready. First, **measured unit coverage is 53.9%**, far below the repo's mandatory 80% floor, and nothing in CI enforces the floor — the gap is concentrated in `internal/cassrepo` (32.1%) and `internal/mongorepo` (3.5%), including the at-rest-encryption helper whose failure mode is writing plaintext to an unencrypted column. Second, **canonical mutation events are fire-and-forget**: edits, deletes, pins and reactions commit to Cassandra and then publish to MESSAGES-CANONICAL on the request context with failures swallowed by a `slog.Warn`, so a NATS blip permanently desynchronizes connected clients and the search index while the client sees success. Everything else is bounded, fixable debt: a service layout that invents a third convention the Makefile already special-cases, a missing `bootstrap.go`, three Mongo aggregation shapes that don't scale with room size, and double JSON encoding on every page.

## Dimension scores

| Dimension | Score | One-line verdict |
|---|---|---|
| Go code quality | 4 / 5 | Idiomatic and disciplined; gosec clean. One unprojected Mongo read, one log-and-return, five context-less `slog` calls. |
| Architecture | 3.5 / 5 | Clean DI and subject discipline, but `cmd/`+`internal/` is a third layout, no stream bootstrap, and a non-durable dual write. |
| Test coverage | 1 / 5 | Floored by the 80% rule: 53.9% measured. `internal/service` is excellent at 93%; the two repo packages are 32% and 3.5%. |
| Maintainability | 4 / 5 | No god-files, WHY-comments with issue refs. Canonical-event mapping duplicated 7× with observed drift. |
| Integration | 4 / 5 | Zero raw-`Sprintf` subjects, correct bucket math, client-API docs in sync. Event durability is the gap. |
| Performance | 4 / 5 | Pages clamped, classic N+1s already batched. Double JSON encode, `$facet` + uncapped `$skip`, correlated `$lookup`. |
| **Average** | **3.4 / 5** | |

## Findings by severity

| Severity | Count |
|---|---|
| `critical` | 2 |
| `high` | 14 |
| `medium` | 19 |
| `low` | 10 |
| `nitpick` | 8 |
| **Total** | **53** |

Counts are per-dimension as reported and therefore include cross-dimension duplicates — six issues were independently found by two or three experts, which is a signal of their weight rather than double-counting to inflate the total. The repeats are: the missing `bootstrap.go` / `BOOTSTRAP_STREAMS` (architecture, integration, maintainability), the `cmd/`+`internal/` layout (architecture, maintainability), the unprojected `GetSubscription` read (code quality, integration), `$facet` + uncapped `$skip` on thread-parent lists (code quality, performance), the duplicated limit clamp (code quality, maintainability), and the fire-and-forget canonical publish (architecture, integration).

### Synthesis note on the two `critical` tags

The two `critical` findings are not equivalent, and the report is explicit about that rather than letting the tag flatten them:

- **Coverage at 53.9%** is `critical` by the letter of `CLAUDE.md` §4, and the substance backs it: security-relevant code (`blankQuotedBody`, `decryptIfNeeded`) and 371 statements of `cassrepo` have no unit coverage, and no CI gate would catch further drift. Treat this one as genuinely blocking.
- **The `cmd/`+`internal/` layout** was tagged `critical` by the architecture expert as a guidelines violation. It carries no runtime risk — it is a convention and tooling-tax problem (`Makefile:169-180` special-cases this one service; four sibling services hand-copy wire structs they cannot import). It is real and worth fixing, but it is ranked below the availability and correctness items in the prioritized action list at the end of this report.

### Audit gaps

Two of the three blocking SAST scanners could not run in this environment: `govulncheck` was rejected by the agent proxy reaching `vuln.go.dev:443`, and `semgrep`'s registry ruleset (`p/golang`) was rejected reaching `semgrep.dev`. gosec ran clean repo-wide (0 medium+ findings), and the repo's nine **local** `.semgrep` rules ran offline over 33 files with 0 findings — including the `errcode` guardrails. No findings were invented from the two blocked scanners. Integration tests were not executed (Docker unavailable in the audit sandbox), so `cassrepo`/`mongorepo` coverage is measured without the `integration` tag; see the Test coverage chapter for why that measurement gap is itself a finding.

---

# 1. Go code quality — 4 / 5

Genuinely strong Go. Error wrapping in `cassrepo` is exemplary: every gocql call site names the table and entity it was working on (`internal/cassrepo/write.go:254`, `internal/cassrepo/pin.go:82`). Across the service there are zero `os.Getenv` calls, zero `fmt.Println`/`log.Println`, zero `time.Sleep`-as-synchronization, zero `map[string]interface{}` payloads, zero raw `fmt.Sprintf` subject construction, and zero string comparisons of errors — `errors.Is` is used correctly at `internal/service/messages.go:521` and `internal/service/reactions.go:57`. `pkg/errcode` tier discipline is clean: all 18 handlers return either a typed `errcode` value or a raw wrapped error and let `natsrouter` adapt at the boundary, with no Tier 3 escapes (`errnats.*`, `Classify`, `Permanent`, `Parse`) anywhere in the tree. Both `$lookup` sites carry the inline justification `CLAUDE.md` §6 requires (`internal/mongorepo/pipelines.go:39`).

The score reflects the findings below, not structural problems.

## SAST (`make sast`, repo root)

| Scanner | Result |
|---|---|
| gosec (medium+, medium-confidence) | **PASS** — 0 findings repo-wide, including `history-service/` |
| govulncheck | **BLOCKED** — agent proxy returned 403 `connect_rejected` for `vuln.go.dev:443` |
| semgrep (registry `p/golang`) | **BLOCKED** — agent proxy returned 403 for `semgrep.dev/c/p/golang` |
| semgrep (repo-local `.semgrep/*.yml`) | **PASS** — 9 rules over 33 files, 0 findings, including the `errcode` guardrails |

No SAST findings touch `history-service/`, so none are folded in as `high`. The three `#nosec G115` suppressions at `internal/cassrepo/walker.go:27,29,52` are in the correct gosec-native form, on the line directly above the statement, with substantive justifications.

**Audit gap, `medium`:** two of the three blocking CI scanners could not execute here. Nothing was inferred from them. Allowing `vuln.go.dev` and `semgrep.dev` through the agent proxy — or vendoring the Go rulesets locally — would make the full blocking gate verifiable pre-merge instead of only at CI time.

## Findings

### `high` — `GetSubscription` fetches the whole document with no projection
`internal/mongorepo/subscription.go:28` issues `FindOne(ctx, bson.M{"u.account": …, "roomId": …})` with no `WithProjection`. Its only consumer, `pinPreCheck` (`internal/service/pin.go:38`), reads exactly three fields: `sub.Roles`, `sub.User.ID`, `sub.User.Account`. This violates `CLAUDE.md` §6's "every find/aggregation MUST specify an explicit projection". The cost is real, not stylistic: `model.Subscription` carries `ThreadUnread []string` — an unbounded parent-ID list — plus roughly 25 other fields, all pulled over the wire on every pin and unpin. Every sibling read in the same package does it correctly (`subscription.go:35`, `room.go:41,54,246`, `app.go:29`, `threadroom.go:88`), so this is a single omission rather than a pattern. The integration expert flagged the same line independently.

### `medium` — Logs and returns the same error, guaranteeing a double log
`internal/service/reactions.go:58-59` calls `slog.WarnContext(c, "react: actor not found", …)` and then immediately returns `fmt.Errorf("react: actor not found for account %s: %w", …)`. `CLAUDE.md` §6 is explicit: "Never log AND return. `Reply`/`Write` run `Classify`, which logs once at a category-aware level." The router classifies the returned error at the boundary, so one failure emits two lines. The only correct-by-construction escape (`ReplyQuiet`) is Tier 3 and not applicable to a router handler.

### `medium` — Five `slog` call sites drop the context, losing trace correlation and request ID
`internal/service/messages.go:402`, `messages.go:732`, `messages.go:737`, `internal/service/threads.go:370`, and `internal/cassrepo/utils.go:162` use bare `slog.Warn` on request-serving paths where a `context.Context` is in scope. The service wires `obs.InitWithLoggerHandler` + `logctx.NewHandler` for trace-correlated logging at `cmd/main.go:85`, which the non-`Context` variants bypass entirely. This is an internal inconsistency rather than a blanket miss — `rooms.go:99,123`, `utils.go:265` and `migration.go:139,145` all use `WarnContext` *plus* an explicit `natsutil.RequestIDFromContext(ctx)`. `messages.go:732/737` is the sharpest case: it is a near-verbatim twin of `migration.go:139/145`, and the twin carries `request_id` while it does not.

### `medium` — `ReactMessage` is the one client handler without `WithLogValues`
`internal/service/reactions.go:25-27` reads `account` and `roomID` off the subject but never calls `c.WithLogValues("account", account, "room_id", roomID)`. All 15 other handler sites do (`messages.go:36,117,180,420,449,490,570`; `pin.go:90,148,190`; `threads.go:31,307`; `migration.go:25,93`). `WithLogValues` feeds `errcode.WithLogValues` (`pkg/natsrouter/context.go:188`), so every boundary `Classify` line for a failed reaction lands without the account or room — precisely the two fields needed to triage one.

### `medium` — Unbounded `$skip` offset plus `$facet`/`$count` on every thread-list page
`internal/service/threads.go:319` passes `req.Offset` straight into `mongoutil.NewOffsetPageRequest`, which clamps `limit` to 100 but only floors `offset` at 0 (`pkg/mongoutil/pagination.go:34-42`). `AggregatePaged` then builds a `$facet` with `$skip: req.Offset` and a `$count` over the full match set (`pkg/mongoutil/collection.go:94-102`). A client sending `offset: 50000000` buys an O(offset) server-side skip, and `$facet` blocks index-covered limit pushdown so the `$count` walks every matching `thread_rooms` document per request. The codebase already knows better: `pkg/mongoutil/pagination.go:20` documents `AggregatePagedHasMore` as the `$facet`-avoiding variant, and `ListThreadSubscriptions` uses keyset pagination. This handler is the outlier. The performance expert reached the same conclusion from the cost side.

### `low` — Unchecked type assertion in the shared cache hot path
`internal/readcache/readcache.go:95` returns `res.Val.(V)`. Every `singleflight` return path currently yields a `V`, so it cannot panic today; it is one edit away from a panic in code shared by three caches, and a comma-ok assertion costs nothing.

### `nitpick` — Duplicated limit-clamp boilerplate, five copies
Identical `if limit <= 0 {…}; if limit > maxPageSize {…}` blocks at `internal/service/messages.go:56-61`, `messages.go:132-137`, `messages.go:192-197`, `internal/service/threads.go:76-81`, `threads.go:171-176`.

### `nitpick` — Non-constant `slog` key
`cmd/main.go:51` calls `slog.Error("invalid config", c.name, c.value)`, making the env var name the attribute *key* and producing a different log schema per failure. `"setting", c.name, "value", c.value` keeps the schema stable for log-based alerting.

## Recommendations

1. **`high`** — Add `mongoutil.WithProjection(bson.M{"roles": 1, "u": 1, "_id": 0})` to `internal/mongorepo/subscription.go:28`. It is the only projection-free read in the service.
2. **`medium`** — Delete the `slog.WarnContext` at `internal/service/reactions.go:58`, keeping only the returned wrapped error. While there, consider returning `errcode.NotFound`/`BadRequest` instead of a raw wrap, since `ErrUserNotFound` currently collapses to an `internal` 500 at the boundary.
3. **`medium`** — Convert the five bare `slog.Warn` sites to `slog.WarnContext(ctx, …)` and add `"request_id", natsutil.RequestIDFromContext(ctx)`, matching the `internal/service/migration.go:139` pattern. Add a `golangci-lint` `forbidigo` rule banning non-`Context` `slog` calls in `internal/service` and `internal/cassrepo` so this cannot regress.
4. **`medium`** — Add `c.WithLogValues("account", account, "room_id", roomID)` at `internal/service/reactions.go:28`.
5. **`medium`** — Bound `offset` in `pkg/mongoutil/pagination.go:34` (a `maxOffset` returning `BadRequest`, or reject offsets past a page ceiling), and move `GetThreadParentMessages` to `AggregatePagedHasMore` or keyset pagination as `ListThreadSubscriptions` already does. If `Total` is a hard client requirement, document that at `internal/service/threads.go:319` so the `$facet` cost is a recorded trade rather than an accident.
6. **`low`** — Make `internal/readcache/readcache.go:95` a comma-ok assertion, and consider promoting the hardcoded `fetchTimeout = 10 * time.Second` (`readcache.go:27`) to a config knob — it is the only unconfigurable timeout in an otherwise fully env-driven service.
7. **`nitpick`** — Extract a `clampLimit(req.Limit)` helper for the five duplicated clamps, and fix the dynamic log key at `cmd/main.go:51`.

---

# 2. Architecture — 3.5 / 5

Internally this is one of the better-engineered services in the repo: constructor dependency injection, an options pattern, `pkg/subject` builders everywhere, cache decorators around the repo interfaces, and a shutdown sequence that matches the house pattern byte for byte. The deductions are for a layout that invents a third convention, a missing stream fail-fast, a non-durable dual write, and scope that has drifted past "history".

**Correct, for the record:** all subjects come from `pkg/subject` builders with zero raw `fmt.Sprintf` (`internal/service/service.go:226-262`); stream names come only from `pkg/stream`; the service registers no JetStream consumers, so there is no consumer-pattern violation and correctly no INBOX/OUTBOX involvement; `shutdown.Wait` ordering (`router.Shutdown` → `natsutil.Drain` → Mongo → Cassandra → Vault → health → obs, `cmd/main.go:242-256`) is identical to `room-service` and `user-service`; user handlers take `account` from the subject rather than the request body (`internal/service/messages.go:34`); both `$lookup` sites carry the required inline justification (`internal/mongorepo/pipelines.go:39`).

## Findings

### `critical` (as tagged; see the synthesis note in the executive summary) — `cmd/` + `internal/` matches neither the flat convention nor the sanctioned exception
`CLAUDE.md` §1 states services are flat `package main` directories with "no `cmd/` or `internal/`". The sanctioned exception for larger request/reply services is `main.go` at the service root plus `config/`, `models/`, `mongorepo/`, `service/`, `service/mocks/` — exactly what `user-service/main.go` + `user-service/{config,models,mongorepo,service}` does. `history-service/cmd/main.go` + `history-service/internal/*` is a third layout, and it is the **only** `cmd`/`internal` pair in the repo: a `find` across 40+ service directories returns just these two. The cost is already being paid in shared tooling — `Makefile:169-173` and `Makefile:176-180` carry an `ifeq ($(SERVICE),history-service)` branch in both the Windows and Unix `build` targets that no other service needs, and `deploy/Dockerfile:7` builds `./history-service/cmd/`. This carries no runtime risk; it is a convention and tooling-tax problem, and it is ranked accordingly in the prioritized action list.

### `high` — `internal/` forces four sibling services to hand-copy the wire contract
Because the request/response types live in an unimportable `internal/models`, callers duplicate them by hand: `room-service/reader_history.go:48` (whose comment says it "mirrors history-service's GetMessageByIDRequest wire"), `message-gatekeeper/fetcher_history.go:37`, `broadcast-worker/parent_fetcher.go:36`, and `tools/loadgen/history_generator.go:100`. Contrast `RoomsGetRequest`, which *is* in `pkg/model` (`internal/models/message.go:90`) and is shared compile-checked by `user-service/historyclient/client.go:60`. That is four silent-drift surfaces versus one typed contract, and it also conflicts with `CLAUDE.md` §6's "All NATS payloads are JSON with typed structs from `pkg/model`".

### `high` — No `bootstrap.go` and no `Bootstrap` config despite publishing to MESSAGES-CANONICAL
The service publishes to `MESSAGES-CANONICAL-{siteID}` from six sites (`internal/service/messages.go:556,636`, `pin.go:139,181`, `reactions.go:114`, `migration.go:80,129`), but `internal/config/config.go` has no `Bootstrap bootstrapConfig` field and there is no `bootstrapStreams` helper. `CLAUDE.md` §6 mandates the opt-in bootstrap convention for any service that consumes *or publishes to* a stream, and 11 sibling services have one. `room-service/bootstrap.go:44-59` is the exact precedent for a publish-only request/reply service, including the production `js.Stream()` verify-or-fail check. Two consequences: a misprovisioned deployment is not caught at startup — instead every edit, delete, pin and reaction silently fails at first publish, per the next finding — and `deploy/docker-compose.yml` sets no `BOOTSTRAP_STREAMS`, so the service cannot stand up against a fresh dev NATS. Independently flagged by the integration and maintainability experts.

### `high` — Cassandra-commit-then-best-effort-publish dual write
`internal/service/messages.go:729-740`: the canonical event is marshalled and published *after* the Cassandra mutation has committed, and both failure paths are a bare `slog.Warn`. On publish failure the edit, delete, pin or reaction is durably persisted but `broadcast-worker` never fans it out and `search-sync-worker` never reindexes — permanent divergence, with no retry, no reconciliation, and a success reply to the client. The repo already owns the durable-buffer pattern for exactly this (`pkg/outbox` + `outbox-worker`), used by `message-worker` and `room-worker`. Note also the global `slog.Warn` rather than `WarnContext(c, …)`, so the one signal that this happened is not trace-correlated.

### `medium` — Consumer-defined interfaces are expressed in implementer types
`internal/service/service.go:22-29` types six `MessageReader` methods in `cassrepo.PageRequest` / `cassrepo.Page[T]`; line 70 returns `map[string]mongorepo.RoomTimes`; line 119 returns `[]mongorepo.ThreadSubRow`. The interface nominally lives with its consumer as `CLAUDE.md` §3 requires, but `service` must import both repo packages, so no alternative store could satisfy it without depending on the Cassandra and Mongo packages. The dependency-inversion intent is inverted in practice.

### `medium` — Scope has drifted past "history"
The `service` package now holds message reads, message *mutations*, `RoomsGet` room-list previews **with write-back into the `rooms` collection** (`internal/service/rooms.go:171`, `messages.go:702,719,722` → `internal/mongorepo/room.go:193-241`), a cross-site thread inbox (`threads.go:164`), and migration RPCs (`migration.go`). The `rooms` collection is otherwise owned by `room-service` and `broadcast-worker`; `cmd/main.go:167-172` additionally calls `EnsureIndexes` on `thread_rooms`/`thread_subscriptions`, which `room-service/store_mongo.go:73` and `message-worker/store_mongo.go:33` also touch. Two corroborating symptoms: `RoomRepository`'s doc comment says "reads room metadata" (`service.go:64`) while the interface carries four write methods, and `readcache.RoomSource` (`internal/readcache/readcache.go:151-161`) must re-declare all four writes as pure passthroughs — a third place to edit per new method.

### `low` — `HistoryService` is documented "transport-agnostic" but has 53 `natsrouter` references
`internal/service/service.go:158`. The struct owns `RegisterHandlers` and every method takes a `*natsrouter.Context`. Harmless, but the comment misleads a reader about what could be reused.

### `nitpick` — Config validation split across two homes, plus one dead field
`cmd/main.go:37-56` `checkConfig` validates five integer knobs and calls `os.Exit`, while `internal/config/config.go:132-171` `validate()` validates everything else and returns errors. A new integer knob must be added in both places or it goes silently unvalidated. Separately, `config.go:48` `MetricsAddr` has zero readers — `obs` owns the endpoint.

## Recommendations

1. **`critical`** — Flatten to the sanctioned exception: `history-service/main.go` in `package main`, plus `config/`, `models/`, `mongorepo/`, `cassrepo/`, `service/`, `service/mocks/`, `publisher/`, `readcache/`. This deletes the `Makefile:169-180` special case and removes the `cmd`/`internal` precedent. Mechanical — no import cycles exist today.
2. **`high`** — Promote the client-facing request/response structs in `internal/models/message.go` into `pkg/model` (as `RoomsGetRequest` already is) and delete the four hand-mirrored copies in `room-service`, `message-gatekeeper`, `broadcast-worker` and `tools/loadgen`. Update `docs/client-api.md` and its derived views in the same PR per §5.
3. **`high`** — Add `history-service/bootstrap.go` plus a `Bootstrap bootstrapConfig` field, mirroring `room-service/bootstrap.go`: `CreateOrUpdateStream(Name + Subjects)` from `stream.MessagesCanonical(siteID)` when enabled, `js.Stream()` verify-or-fail otherwise. Set `BOOTSTRAP_STREAMS=true` in `deploy/docker-compose.yml`.
4. **`high`** — Make the canonical mutation event durable. The cheapest correct step is to propagate the publish error out of `publishCanonicalBestEffort` (`internal/service/messages.go:729`) so the mutation RPC fails and the client retries; better, buffer through a durable stream so persist-then-publish is at-least-once rather than fire-and-forget.
5. **`medium`** — Move `PageRequest`, `Page[T]`, `RoomTimes` and `ThreadSubRow` into `internal/models` (or `pkg/model`) so the interfaces in `service.go` no longer name `cassrepo`/`mongorepo` types.
6. **`medium`** — Split the preview write-back out of `RoomRepository` into a separate `PreviewWriter` interface, drop it from `readcache.RoomSource` by embedding rather than re-declaring, and then decide explicitly whether `history-service` or `broadcast-worker` owns `rooms` preview writes — today both do.
7. **`low`** — Make the room access gate structural rather than per-handler: `getAccessSince`/`checkAccessAndRoomTimes` are called by hand at eight-plus sites (`internal/service/messages.go:41,120,207,277,422,451`, `pin.go:192`), so a new user-facing handler that forgets one is an unauthorized read with no compile-time or lint signal.
8. **`nitpick`** — Fold `cmd/main.go:37-56` `checkConfig` into `config.validate()`, delete the dead `MetricsAddr` field, and replace the dynamic log key at `cmd/main.go:51` with fixed `"setting"`/`"value"` keys.
