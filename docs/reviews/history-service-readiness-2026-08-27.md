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

---

# 3. Test coverage — 1 / 5

**Score floored by the mandatory rule in `CLAUDE.md` §4:** measured total 53.9% is below 60%, which forces a `critical` finding and a score of 1. The floor is doing most of the work in this number, and the report says so plainly: the `internal/service` suite is genuinely excellent and the integration scaffolding is fully compliant. The problem is concentrated, measurable and fixable.

## Measured coverage

`go test -coverprofile=/tmp/cov.out ./history-service/...` followed by `go tool cover -func`. Run without the `integration` build tag, since Docker was unavailable in the audit sandbox.

| Package | Coverage | Covered / total statements |
|---|---|---|
| `internal/service` | **93.0%** | 982 / 1056 |
| `internal/config` | 92.6% | 25 / 27 |
| `internal/readcache` | 89.2% | 74 / 83 |
| `internal/publisher` | 83.3% | 20 / 24 |
| `internal/cassrepo` | **32.1%** | 175 / 546 |
| `internal/mongorepo` | **3.5%** | 5 / 144 |
| `cmd` | 0.0% | 0 / 120 |
| `internal/service/mocks` (generated) | 0.0% | 0 / 378 |
| **Total (verbatim tool output)** | **53.9%** | 1281 / 2378 |

`internal/models` reports `[no statements]` — pure type declarations. Three alternative framings, for honesty: **64.0%** excluding the generated mock package, **68.1%** excluding mocks *and* `cmd/main.go`. All three are below the 80% floor.

Other checks run:

- `make test SERVICE=history-service` → **all 8 packages pass**, no failures.
- `make generate SERVICE=history-service` → **no diff; mocks are current.** No stale-mock finding. Working tree verified clean afterwards (`git status --porcelain` empty).

## Findings

### `critical` — Total coverage 53.9%, below the repo minimum 80%
Coverage below repo minimum 80%, currently 53.9%. Driven almost entirely by two packages: `internal/cassrepo` at 32.1% and `internal/mongorepo` at 3.5%.

### `high` — The 80% floor is not enforced anywhere in CI
`history-service/deploy/azure-pipelines.yml:44` writes `-coverprofile=coverage-$(SERVICE_NAME).out` and then never reads it. The repo *has* a gate tool — `tools/coveragecheck`, wired at `Makefile:141-152` — but it is applied only to `tools/loadgen`. Nothing would have caught this drift, and nothing will catch the next one.

### `high` — `internal/mongorepo` has exactly one untagged test file, 49 LOC
`internal/mongorepo/threadsubscription_unit_test.go` is the only non-`integration` test in a 144-statement package. Every repository method sits at 0%: `room.go:38 GetMinUserLastSeenAt` through `room.go:251 GetRoomUserCount` (78 uncovered statements), `threadroom.go:32-85`, `threadsubscription.go:49-65`. Worse, `pipelines.go:10/18/25/136` are **pure `bson.M`/`bson.A` builders that need no database at all** and are 0% covered — including `unreadThreadsPipeline` (`pipelines.go:136`), whose `$lookup` shape (itself a `high` performance finding) is asserted only behind Docker.

### `high` — At-rest-encryption correctness paths have zero unit coverage
`internal/cassrepo/write.go:116 blankQuotedBody` is a pure function whose entire job is clearing *only* the quoted-parent body sub-fields before the plaintext column is written; a bug leaks plaintext into an unencrypted column. It is at 0%. The same applies to `internal/cassrepo/decrypt.go:21 decryptIfNeeded`, whose `ErrEncryptedRowCipherDisabled` branch (`decrypt.go:26`) is a security-visible error path at 0% — and trivially testable, since the `r.cipher` seam already exists for a fake.

### `medium` — `RegisterHandlers` is 0% covered
`internal/service/service.go:221` wires 18 NATS subjects. A swapped handler or a wrong `subject.*Pattern` would ship silently in a service that is reachable *only* over NATS request/reply. Cheap to cover with a fake router that records `(subject → handler)` pairs.

### `medium` — `PreviewCache.Invalidate` is 0% covered
`internal/readcache/readcache.go:285`, and its `ttlCache.remove` at `readcache.go:63`, is the eviction mechanism the file's own comment identifies as the protection against serving a just-deleted message as a room preview. Untested.

### `low` — Two timing-dependent tests will flake under load
`internal/readcache/readcache_test.go:111` sleeps 40ms against a 20ms TTL, and `internal/cassrepo/messages_by_id_test.go:97` sleeps 10ms to widen a concurrency window then asserts `assert.Greater(maxSeen, 1)`. Both are vulnerable on a loaded or single-CPU CI runner under `-race`.

### `nitpick` — Table-driven usage is thin relative to the guideline
544 test functions against 26 `[]struct{}` tables and 79 named cases. Many one-scenario-per-function tests where a table would read better — `walker_test.go` alone has 14 discrete `TestWalkBuckets_*` functions. A style deviation from §4's "prefer table-driven", not a correctness gap.

## What is genuinely good

The raw percentage understates this suite, and the recommendations are aimed at the gap rather than at the whole:

- **`internal/service` at 93.0% with real error-path depth.** All 30 partially-covered functions sit at 84–98%, and the only sub-80% items are three best-effort publish fallbacks (`messages.go:729`, `migration.go:136`, `rooms.go:207`) — themselves the subject of the `high` durability finding. Assertions check typed errors properly (`errcode.CodeNotFound/BadRequest/Forbidden/Internal`, `errcode.HasReason`, `errcode.MessageOutsideAccessWindow`), never error strings.
- **Integration scaffolding is fully compliant.** 125 integration test functions across 18 tagged files; all three packages have `TestMain(m) { testutil.RunTests(m) }` (`cassrepo/main_test.go`, `mongorepo/main_test.go`, `service/main_test.go`); containers come exclusively from `testutil.MongoDB` / `testutil.CassandraKeyspace`; **zero** inline `testcontainers.GenericContainer`. The depth is real rather than smoke — `write_integration_test.go` is 1773 LOC across 31 tests.
- **The mock-versus-real split is correct.** Business logic is mocked via `service/mocks` (10 interfaces, mockgen, up to date); store implementations are tested against real Cassandra and Mongo. No unit test touches a real dependency.
- **No test helpers in production code**; no `testing` import outside `_test.go`. No package-level *mutable* shared state — the six package variables are immutable `time.Time`/`preview.Key` constants — and no `t.Parallel`, so no ordering hazard.

## Recommendations

1. **`critical`** — Wire `tools/coveragecheck` into `history-service/deploy/azure-pipelines.yml` after line 44 with `-min 80`, plus a `-min 90` scoped run for `internal/service`. Without a gate the floor is advisory and will drift again.
2. **`high`** — Add an untagged `internal/mongorepo/pipelines_test.go` asserting the built `bson` documents for all four builders. Pure functions, no Docker, and it makes the `$lookup` shapes reviewable in CI.
3. **`high`** — Add untagged unit tests for the pure and seam-injectable `cassrepo` helpers: `blankQuotedBody` (`write.go:116`), `decryptIfNeeded` (`decrypt.go:21`) including the cipher-disabled branch, `startBucketFromCursor` (`messages_by_room.go:24`), and `toPage` (`walker.go:81`). Table-driven, fake cipher, no container.
4. **`high`** — Run the integration suite with `-coverprofile` in CI and merge the two profiles (`go tool covdata`, or `-coverpkg=./history-service/...`) so `cassrepo`/`mongorepo` coverage is *measured* rather than assumed. As it stands nobody can tell whether the Docker suite actually covers those 371 uncovered `cassrepo` statements.
5. **`medium`** — Cover `RegisterHandlers` (`service.go:221`) with a fake `natsrouter.Router` capturing registered subjects, asserted against the expected `subject.*Pattern` set.
6. **`medium`** — Cover `PreviewCache.Invalidate` / `ttlCache.remove` (`readcache.go:285`, `readcache.go:63`): populate, invalidate, assert the next `Get` re-loads.
7. **`low`** — De-flake the two timing tests: inject a clock into `ttlCache` (or assert only "at least one reload"), and replace the `Sleep(10ms)` in `messages_by_id_test.go:97` with a channel barrier that releases once `limit` fetches are in flight.

---

# 4. Maintainability — 4 / 5

Production code is only about 6.1k LOC — the 22.7k total is 15.6k of tests. There are no god-files and no god-functions, packages are cohesive, and comments are almost uniformly WHY-focused with issue references (`internal/service/messages.go:660-680` on #226, `rooms.go:122-129` on #291). The debts below are specific and fixable, not structural rot.

**Positive signals worth preserving:** `docs/client-api.md` is in sync — all 13 client-facing subjects are documented (`:2854-2866`), including the `meta`/`RoomMeta` hint (`:2875-2889`) and `sizeLimited` semantics. `internal/service/service.go:21-140` is a genuine consumer-defined interface set with compile-time assertions at `:256-259`, and `mongorepo` justifies its `$lookup` inline (`pipelines.go:39`) as §6 requires. Dead code is near-zero, and `previewWalk`'s state machine (`rooms.go:177-204`) correctly makes "degraded" the zero value.

## Findings

### `high` — The `cmd/` + `internal/` layout has already leaked into shared tooling
`CLAUDE.md` §1 permits `config/`, `models/`, `mongorepo/`, `service/`, `service/mocks/` *directly under the service directory*, as `user-service` does — never `cmd/` or `internal/`. `Makefile:169-179` now carries a hardcoded `ifeq ($(SERVICE),history-service)` branch in *both* the Windows and Unix `build` targets solely because this is the only service with a `cmd/`, and `deploy/Dockerfile:7` builds `./history-service/cmd/`. Every future repo-wide script pays this tax. Corroborates the architecture chapter's finding from the tooling side.

### `high` — Canonical-event mapping duplicated seven times, with drift already observed
A complete `cassandra.Message → pkgmodel.Message` mapper already exists at `internal/service/reactions.go:125` (`toWireMessage`) and is used by exactly **one** publish site. The other six hand-roll partial subsets: `messages.go:531`, `messages.go:613`, `migration.go:59`, `migration.go:117`, `pin.go:125`, `pin.go:168`. The drift is already visible — `messages.go:613` (user delete) carries `UserID`/`UserAccount`, while `migration.go:117` (migration delete) omits both, so downstream consumers see two different shapes for `EventDeleted`. `UserDisplayName` is populated only on the reaction path. Adding a field the canonical event must carry means auditing seven sites, and a miss is silent.

### `high` — Cassandra column lists are hand-maintained in four places with no coverage guard
`baseColumns` (`internal/cassrepo/messages_by_room.go:13`), `pinnedColumns` (`pin.go:33`), `threadMessageColumns` (`thread_messages.go:14`), plus an ad-hoc `SELECT` at `write.go:147`. `buildScanValues` (`utils.go:125`) errors only on an *extra* column — a **forgotten** column leaves the field zero-valued with no error at all. The only reference to `baseColumns` from tests is a benchmark (`utils_test.go:19`). Adding a message field is therefore a seven-file change (`pkg/model/cassandra`, three column constants, the init DDL, `docs/cassandra_message_model.md`) whose failure mode is silent data loss.

### `medium` — `internal/readcache` re-implements `pkg/roommetacache`'s L1
`internal/readcache/readcache.go:27` and `pkg/roommetacache/roommetacache.go:34` both declare `const fetchTimeout = 10 * time.Second` under the *same comment text*, both wrap `lru.NewLRU` + `singleflight` + `cachemetrics`, both re-check under singleflight, and both detach via `context.WithoutCancel`. The generic primitive belongs in `pkg/`; two copies means two places to fix a singleflight bug.

### `medium` — Reusable Cassandra plumbing is locked inside `internal/`
`Cursor`, `QueryBuilder`, `structScan` and `cqlFieldIndex` (`internal/cassrepo/utils.go:13-186`) are service-agnostic and solve exactly the problem `data-migration/es-index-migrator/messagesource_cassandra.go:81` still suffers — a six-plus-argument positional `iter.Scan`. `pkg/cassutil` exposes only `Connect`/`Close`.

### `medium` — Split config validation, plus one dead field
`config.validate()` (`internal/config/config.go:122`) returns errors; `cmd/main.go:38` `checkConfig` duplicates the job with `os.Exit`. A new integer knob must be added in both files or it silently goes unvalidated. Related: `MetricsAddr` (`config/config.go:48`) is dead — zero readers.

### `medium` — No `bootstrap.go` or `Bootstrap` config, unlike 11 sibling services
`history-service` publishes to MESSAGES-CANONICAL but has no `BOOTSTRAP_STREAMS` in config or in `deploy/docker-compose.yml`, so it cannot stand up against a fresh NATS in dev — the stated purpose of the convention in §6. Third independent sighting of this gap.

### `low` — Copy-pasted limit clamp, five times
`internal/service/messages.go:56-61`, `messages.go:132-137`, `messages.go:192-197`, `threads.go:76-81`, `threads.go:171-176`. Separately, `threads.go:94-102` re-derives the ceiling and floor inline instead of going through `walkBounds` (`room_times.go:42`).

### `low` — Production/test file mapping is broken, and one 3k-line test god-file
`internal/service/messages_test.go` holds **130** test functions in 2,999 lines, while `pagefit_test.go`, `preview_walk_test.go`, `preview_repair_test.go`, `appname_test.go` and `threadlist_test.go` test code that lives in `utils.go`/`rooms.go`/`messages.go`/`reactions.go`. "Where does a test for X go?" has no answer. Relatedly, `toWireMessage`/`toWireParticipant`/`botAwareDisplayName` — display-name concerns — live in `reactions.go`. And `cmd/` has zero test files, so `checkConfig` is untested despite §4's per-package 80% requirement.

### `nitpick` — Doc comment attached to the wrong function
`internal/readcache/readcache.go:56-62`: the `getOrLoad` doc block ends with `// remove drops key…` and is followed by `func remove` at `:63`, then `getOrLoad` at `:65`. godoc attributes the whole block to `remove`, leaving `getOrLoad` undocumented.

### `nitpick` — Cross-file invariants held by comment only
`maxContentBytes = 20 * 1024` is duplicated at `internal/service/messages.go:30` and `message-gatekeeper/handler.go:32` ("mirrors message-gatekeeper's content cap"). `rooms.go:21-22` says "mirrors `maxGetByIDsBatchSize`" and "mirrors `cassrepo.maxConcurrentIDReads`" — the latter is unexported and therefore unreferenceable. `rooms.go:127` `walkBoundsFromRow` is a pass-through returning two struct fields.

## Recommendations

1. **`high`** — Extract `canonicalMessage(msg *models.Message, updatedAt *time.Time) pkgmodel.Message` from `reactions.go:125` into a new `internal/service/canonical.go`, and route all seven publish sites through it. Reconcile the `migration.go:117` versus `messages.go:613` field divergence deliberately rather than inheriting it.
2. **`high`** — Add a table-driven test asserting each of the three column constants is a subset of `cqlFieldIndex(reflect.TypeOf(models.Message{}))` *and* covers every non-`cql:"-"` field the table persists. This is the cheapest available guard against silent field loss.
3. **`high`** — Flatten to the sanctioned layout (`main.go` at `history-service/`, packages as `config/ models/ mongorepo/ cassrepo/ service/ …`) and delete the `Makefile:169-179` special case.
4. **`medium`** — Promote `Cursor`/`QueryBuilder`/`structScan` to `pkg/cassutil`, and the LRU+TTL+singleflight+metrics primitive to a shared `pkg` package consumed by both `readcache` and `roommetacache`.
5. **`medium`** — Fold `cmd/main.go:38 checkConfig` into `config.validate()`, delete `MetricsAddr`, and add `config/` tests covering the merged positive-integer checks.
6. **`medium`** — Add `bootstrap.go` plus `Bootstrap` config plus `BOOTSTRAP_STREAMS=true` in `deploy/docker-compose.yml`, matching the 11 sibling services.
7. **`low`** — Add a `clampLimit(req, def int) int` helper to replace the five clamp copies; split `internal/service/messages_test.go` to mirror the production files; move `toWireMessage`/`toWireParticipant`/`botAwareDisplayName` out of `reactions.go` into a `wire.go`; and fix the comment placement at `readcache.go:56-65`.

---

# 5. Integration — 4 / 5

Strong on every mechanical rule: **zero** raw-`fmt.Sprintf` subjects, exemplary error-envelope discipline, correct bucket math throughout, and complete client-API documentation. Points come off for event-delivery durability, the missing stream-bootstrap convention, and one unbudgeted server-to-server reply.

## Verified clean

- **Subject usage.** All 17 registrations come from `pkg/subject` builders (`internal/service/service.go:226-262`), and so do all five publish subjects (`messages.go:556,636`, `pin.go:139,181`, `reactions.go:114`). The only `Sprintf` calls in the service are a log field (`cassrepo/utils.go:162`) and an error string (`service/threads.go:299`) — neither builds a subject.
- **JetStream consumer pattern — not applicable.** Pure request/reply: no `Consume()`, `Messages()`, `Nak()`, `jsretry`, or hardcoded `cc.BackOff` anywhere in the tree, so none of the redelivery-backoff rules can be violated. Admission control and per-request deadlines come from `natsrouter.GuardConfig` (`cmd/main.go:222-226`, defaults `MAX_CONCURRENCY=256` / `REQUEST_TIMEOUT=10s`), and `cassrepo` bounds its own fan-out (`walker.go:18` fanout 8 ≤ `CASSANDRA_NUM_CONNS`; `messages_by_id.go:66` `errgroup.SetLimit`).
- **Cassandra.** Every bucketed read and write derives the bucket from `r.bucket` (a `msgbucket.Sizer`) — `messages_by_room.go:94-201`, `write.go:211,226,393`, `pin.go:79,99`, `reactions.go:39,62`. `thread_messages_by_thread` correctly queries by `thread_room_id` alone with no bucket walk (`thread_messages.go:29`). The three-way alignment (`docs/cassandra_message_model.md` ↔ `pkg/model/cassandra/message.go` ↔ init DDL) holds for all four tables, and projections are explicit (`messages_by_room.go:13-18`, `thread_messages.go:14-18`) — no `SELECT *`.
- **`MESSAGE_BUCKET_HOURS` agrees fleet-wide.** `config.go:53` `envDefault:"360"` matches `message-worker/main.go:46` and `bot-message-worker/main.go:37`. No mismatch, so no `critical` finding here.
- **Outbox/inbox.** No INBOX or OUTBOX publish, consume, or stream creation, so `inbox-worker`'s sole ownership is trivially respected.
- **IDs.** The service generates no IDs (read and mutate only) — nothing to violate.
- **Client API docs.** All 14 `chat.user.…` RPCs appear in `docs/client-api.md` and `docs/client-api/request-reply.md`; field tables match `internal/models/message.go`, including `sizeLimited` (`client-api.md:3088,3242`) present exactly where the code sets it and absent where the code omits it. Every history `reason` in `pkg/errcode/codes_message.go:6-16` is catalogued (`client-api.md:6884-6889`). **No drift found** — no `high` documentation finding.
- **Errors.** 100% Tier 1: typed constructors returned to the router, zero `errnats.Reply`, zero log-and-return in this dimension's scope, and `WithCause` only over infra errors (`cassrepo/utils.go:74`, `service/threads.go:331`).
- **Shutdown.** `shutdown.Wait(ctx, 25s, …)` in the correct order — router → drain → Mongo → Cassandra → Vault → health → obs (`cmd/main.go:242-256`).

## Findings

### `high` — Canonical mutation events are fire-and-forget on the request context
`publisher.Publish` is called with the *request* context (`internal/service/messages.go:736`, reached from `messages.go:556,636`, `pin.go:139,181`, `reactions.go:114`) and every failure is swallowed by a `slog.Warn` (`messages.go:737-740`). Two consequences:

- `broadcast-worker/handler.go:170-172` and `search-sync-worker/messages.go:129,318` are the only consumers of `.updated`/`.deleted`/`.pinned`/`.reacted`. A NATS blip means the mutation is committed in Cassandra but **never** reaches connected clients and never reaches the search index — permanently, with no OUTBOX buffer and no retry, unlike `room-worker`, `message-worker` and `broadcast-worker`.
- Because the context is the request context, a request that has already burned its 10s `REQUEST_TIMEOUT` on Cassandra publishes into a cancelled context and fails immediately. Contrast the preview write, which correctly takes a fresh bounded budget (`messages.go:688`, `rooms.go:169`).

### `medium` — No `bootstrap.go` or `BOOTSTRAP_STREAMS` despite publishing to MESSAGES-CANONICAL
`CLAUDE.md` §6 requires every service that publishes to a stream to carry `Bootstrap bootstrapConfig` plus a `bootstrapStreams` helper. `internal/config/config.go` has no `Bootstrap` field, there is no `history-service/bootstrap.go` (11 peers have one), and `deploy/docker-compose.yml` never sets `BOOTSTRAP_STREAMS`. The service cannot stand up against a fresh NATS, and against a missing stream every canonical publish degrades to the silent `Warn` above — the two findings compound.

### `medium` — `ListThreadSubscriptions` reply is unbudgeted
`internal/service/threads.go:195` returns up to 100 items, each carrying two fully marshalled messages (`threads.go:261-262`), with no `pagefit` pass — even though `pageBudget` is wired (`service.go:180`) and used for `msg.history` (`messages.go:99`) and `msg.surrounding` (`messages.go:375`). `user-service/service/threads.go:120` trims the *merged* result, which cannot help: an oversize leaf reply never arrives, so the whole site drops into `unavailableSites` rather than returning a short page. The same gap exists with lower blast radius on `msg.next` (`messages.go:167`), `msg.get.ids` (`messages.go:482`), `msg.pinned.list` (`pin.go:212`) and `msg.thread` (`threads.go:141`) — but those are documented as intentional at `docs/client-api.md:6845`, so they are acceptable as-is.

### `low` — `GetSubscription` fetches whole documents
`internal/mongorepo/subscription.go:28` issues `FindOne` with no projection, violating §6's "always project precisely". Its sibling three lines down does it correctly (`subscription.go:33-35`). It is on the pin/unpin hot path (`service/pin.go:38`) and is deliberately *not* cached (`readcache/readcache.go:150-151`). Same line as the code-quality chapter's `high` finding; that chapter carries the higher tag and the recommendation.

### `low` — Event `Timestamp` reuses the domain timestamp
`internal/service/messages.go:549` (`editedAtMs`), `messages.go:628` (`deletedAtMs`), `reactions.go:100` and `pin.go:137` set the event-level `Timestamp` from the mutation's own time rather than at the publish site, as §6's "Event Timestamps" rule requires. `migration.go:80,124` do it correctly and comment on the distinction — so the codebase disagrees with itself. Semantically near-identical in practice; flagged for consistency.

### `low` — `MESSAGE_BUCKET_HOURS` not pinned in this service's compose file
The fleet-wide value agrees (all three services default to 360), but `history-service/deploy/docker-compose.yml` omits it while `bot-message-worker/deploy/docker-compose.yml:18` pins it. Pinning makes the cross-service invariant visible rather than implied by three independently-declared `envDefault`s.

### `nitpick` — Stale primary-key comments
`internal/service/migration.go:87,95` and `internal/cassrepo/write.go:48` assert the `messages_by_id` primary key is `(message_id, created_at)`. The source of truth says `PRIMARY KEY(message_id)` (`docs/cassandra_message_model.md:270`, `docker-local/cassandra/init/13-table-messages_by_id.cql:28`), and the queries agree (`messages_by_id.go:21`).

## Recommendations

1. **`high`** — Buffer the canonical mutation events durably. Publish an `OutboxEvent` for `.updated`/`.deleted`/`.pinned`/`.unpinned`/`.reacted` — adding the types to exactly one `pkg/outbox` partition set, as the convention requires — so `outbox-worker` retries them. At minimum, give `publishCanonicalBestEffort` its own `context.WithTimeout(context.WithoutCancel(c), …)` plus a bounded in-process retry, and emit a metric on final failure so the divergence is at least observable.
2. **`medium`** — Add `history-service/bootstrap.go` with a `bootstrapStreams(ctx, js, siteID, enabled)` helper creating only `stream.MessagesCanonical(siteID)`'s `Name + Subjects`, a `Bootstrap` field in `config.Config`, and `BOOTSTRAP_STREAMS=true` in `deploy/docker-compose.yml`.
3. **`medium`** — Apply `pagefit.Fit` to `ListThreadSubscriptions` (`threads.go:195`), blanking oversize rows with `Truncated: true` exactly as `user-service/service/threads.go:339-344` does, so a fat page degrades to a short page instead of taking the whole site offline.
4. **`low`** — Add `mongoutil.WithProjection` to `internal/mongorepo/subscription.go:28`, limited to the fields `pin.go` actually reads.
5. **`low`** — Compute the event `Timestamp` at the publish site (`messages.go:549,628`, `reactions.go:100`, `pin.go:137`), matching `migration.go:80`.
6. **`low`** — Pin `MESSAGE_BUCKET_HOURS=360` explicitly in `deploy/docker-compose.yml`.
7. **`nitpick`** — Correct the `messages_by_id` primary-key comments (`migration.go:87,95`, `write.go:48`), and dedupe `req.MessageIDs` before `GetMessagesByIDs` (`messages.go:463`) so duplicates don't each cost a Cassandra read.

---

# 6. Performance — 4 / 5

Genuinely strong for a read service. Every page size is clamped (`internal/cassrepo/utils.go:66-78`, `pkg/mongoutil/pagination.go:33-43`), the classic N+1s are already batched (`internal/service/threads.go:211`, `threads.go:352`, `internal/mongorepo/room.go:77`), independent Mongo reads run concurrently (`internal/service/utils.go:403-428`), projections are precise, and the Cassandra bucket walk is both bounded and fan-out-adaptive. The read cache is correct where it counts: singleflight, `WithoutCancel` plus `fetchTimeout`, positives-only, and `Invalidate` on mutation (`internal/service/messages.go:684-686`). Deductions are for CPU and lock overhead on the hot path plus two Mongo aggregation shapes that do not scale with room size.

## Findings

### `high` — Every page is JSON-encoded twice
`pkg/pagefit/pagefit.go:144-157` reflectively `json.Encode`s every row into a length counter, and then `natsrouter` marshals the same rows again for the reply. `fitPage` (`internal/service/utils.go:210-211`) runs unconditionally whenever a budget is set — which is the default (`cmd/main.go:213-219`) — so even a five-row page pays the sizing pass. `warnIfStillOversize` (`utils.go:260-261`) can make it three passes.

### `high` — `$facet` plus uncapped `$skip` on thread-parent lists
`pkg/mongoutil/collection.go:93-106` appends `$facet{$skip/$limit, $count}`. `$facet` is a blocking stage: it materializes the entire matched-and-sorted set before paging, defeating index sort-limit pushdown, and it runs without `allowDiskUse` so it hard-fails at the 100 MB stage limit. `Offset` is never capped (`pkg/mongoutil/pagination.go:33-43`), making deep pagination O(offset). Caller: `internal/service/threads.go:319-332`. The code-quality expert independently flagged the same shape from the input-validation side.

### `high` — Correlated `$lookup` per thread room, executed before pagination
`internal/mongorepo/pipelines.go:136-161`: `unreadThreadsPipeline` matches *all* thread rooms in the room, joins `thread_subscriptions` for **each** one, and only then sorts and pages. Cost is O(threads in room) sub-queries per request regardless of `limit=20`. `userThreadSubscriptionsPipeline` in the same file (`pipelines.go:56-133`) is the well-designed counterexample: filter first, `$limit`, then join.

### `medium` — Bucket-wave over-read multiplied by per-row decrypt
`internal/cassrepo/walker.go:320` and `:332` request `w.pageSize` from *every* bucket in a wave rather than the remaining shortfall, and the surplus rows are discarded at `walker.go:261-262`. Worst case is fanout 8 × pageSize 100 = 800 rows fetched, scanned and **decrypted** to return 100. Each decrypt opens an OTel span, an AEAD, and a `json.Unmarshal` (`pkg/atrest/cipher.go:81-108`), so the waste is CPU-heavy rather than just I/O.

### `medium` — Per-row reflection allocations in the scanner
`internal/cassrepo/utils.go:153-159` rebuilds a 28-element `colNames []string` per row, and `buildScanValues` (`utils.go:134`) a 28-element `[]any` per row, inside `scanMessagesUpTo`'s loop (`utils.go:57-72`). The columns are invariant for the whole iterator, so this is roughly 1,600 needless slice allocations per over-read page.

### `medium` — Read caches serialize on one exclusive mutex each
`hashicorp/golang-lru/v2@v2.0.7`'s `expirable.Get` takes `mu.Lock()` rather than `RLock` in order to move the LRU element, so **every hit** in `internal/readcache/readcache.go:66` is an exclusive acquisition on a process-global lock — with `MAX_CONCURRENCY=256` (`pkg/natsrouter/guard.go:21`) and three or more lookups per read RPC. Each cache also spawns a cleanup ticker at `ttl/100`, which is **100ms** for the 10s room and preview TTLs.

### `medium` — `GetAllPinnedMessages` is unbounded and over-projected, per pin request
`internal/cassrepo/pin.go:218-237` reads the whole `pinned_messages_by_room` partition with all 24 columns — message body, attachments, `enc_payload` — while its only caller reads just `MessageID` and `PinnedAt` (`internal/service/pin.go:56-70`). No `LIMIT`, no page size.

### `medium` — Fan-out amplification versus the Cassandra connection pool
`internal/service/rooms.go:94` (16 goroutines) × `internal/cassrepo/repository.go:17` (fanout 8) is up to 128 concurrent CQL queries per `RoomsGet`, multiplied by 256 in-flight handlers, against `NumConns=8` per host (`pkg/cassutil/cass.go:18`). Nothing bounds Cassandra concurrency process-wide, so a burst of `RoomsGet` calls queues on the driver rather than being shed at admission.

### `low` — Reply size unbounded on four RPCs
`pagefit` is applied only to `LoadHistory` and `LoadSurroundingMessages`; `internal/service/messages.go:167`, `threads.go:141`, `pin.go:212` and `messages.go:482` return their pages whole. See the integration chapter — four of these are documented as intentional, `ListThreadSubscriptions` is not.

### `low` — The hottest queries' indexes are never verified at startup
`subscriptions{u.account, roomId}` (`internal/mongorepo/subscription.go:125,130`) and `apps{assistant.name}` (`app.go:28`) get no `mongoutil.WarnMissingIndexes` call, unlike `thread_subscriptions` (`threadsubscription.go:51`) and unlike peers such as `room-service/store_mongo.go:130`.

### `nitpick` — Small hot-path waste
`internal/service/rooms.go:250` copies a roughly 400-byte `Message` per scanned row just to test eligibility; `threads.go:247,254` perform two reflective `json.Marshal` calls per thread-list row; `internal/readcache/readcache.go:73-75` records a singleflight-recheck hit as a `Miss`, skewing the cache hit-rate metric.

## Recommendations

1. **`high`** — Encode rows once: have `pagefit` return the per-row encoded bytes (or a `[]json.RawMessage`) and assemble the reply from them, eliminating the second marshal. A cheap interim win is to skip `rowSizes` entirely when `len(items) * knownMaxRowBytes < budget`.
2. **`high`** — Replace `AggregatePaged`'s `$facet` on thread lists with `AggregatePagedHasMore` (already available at `pkg/mongoutil/collection.go:142`), and cap `Offset` in `NewOffsetPageRequest` — or move to the keyset cursor that `userThreadSubscriptionsPipeline` already uses.
3. **`high`** — Restructure `unreadThreadsPipeline` to drive from `thread_subscriptions` (userAccount + roomId), `$sort`/`$limit`, and only then join `thread_rooms`, mirroring `userThreadSubscriptionsPipeline` in the same file.
4. **`medium`** — In `fetchWave`, size each bucket's limit to the remaining shortfall known at plan time, and hoist the `colNames`/`[]any` mapping out of `structScan` into a per-iterator prepared scanner.
5. **`medium`** — Shard the readcache LRUs (for example 16 shards keyed by an FNV hash of the key) or move to a striped map, and raise the 10s TTLs so the cleanup ticker is not firing every 100ms.
6. **`medium`** — Add a narrow `SELECT message_id, pinned_at FROM pinned_messages_by_room WHERE room_id = ? LIMIT maxPinnedPerRoom+N` for `enforcePinLimit`, instead of reading the full partition with all 24 columns.
7. **`low`** — Apply the page budget to `LoadNextMessages`, `GetThreadMessages`, `ListPinnedMessages` and `GetMessagesByIDs`, and add `WarnMissingIndexes` for `subscriptions` and `apps` at startup.

---

# 7. Prioritized action list

Ordered by severity first, then by impact ÷ effort. Items found by more than one expert are marked; that convergence is part of why they rank where they do.

### 1. `critical` — Enforce the 80% coverage floor in CI, then close the `cassrepo`/`mongorepo` gap
**Dimension:** test coverage · **Evidence:** `history-service/deploy/azure-pipelines.yml:44`, `Makefile:141-152`, `internal/mongorepo/pipelines.go:10,18,25,136`, `internal/cassrepo/write.go:116`, `internal/cassrepo/decrypt.go:21,26`
**Why:** Measured coverage is 53.9% against a mandatory 80% floor, and the profile CI already writes is never read — so nothing catches further drift. The uncovered code is not incidental: `blankQuotedBody` exists solely to stop plaintext reaching an unencrypted column, and `decryptIfNeeded`'s cipher-disabled branch is a security-visible error path. Both are pure or seam-injectable and testable with no container. Highest impact ÷ effort on the list: wiring `tools/coveragecheck -min 80` is a few lines, and `pipelines_test.go` unlocks 144 statements of pure `bson` builders with no Docker.

### 2. `high` — Make canonical mutation events durable
**Dimension:** architecture + integration (two experts) · **Evidence:** `internal/service/messages.go:729-740`, reached from `messages.go:556,636`, `pin.go:139,181`, `reactions.go:114`
**Why:** The only availability-affecting correctness bug in the service. Edits, deletes, pins and reactions commit to Cassandra and then publish on the *request* context with failures swallowed by a `slog.Warn`. A NATS blip — or simply a request that already spent its 10s budget in Cassandra — leaves the mutation persisted but never fanned out to clients and never reindexed by `search-sync-worker`, permanently, while the client is told it succeeded. The repo already owns the fix pattern (`pkg/outbox` + `outbox-worker`), and even the interim step (`context.WithoutCancel` + bounded retry + a failure metric) converts silent permanent divergence into an observable, retried one.

### 3. `high` — Add `bootstrap.go` + `Bootstrap` config + `BOOTSTRAP_STREAMS`
**Dimension:** architecture + integration + maintainability (three experts) · **Evidence:** `internal/config/config.go` (no `Bootstrap` field), no `history-service/bootstrap.go`, `deploy/docker-compose.yml` (no `BOOTSTRAP_STREAMS`); precedent at `room-service/bootstrap.go:44-59`
**Why:** Found independently by three of six experts. Small, well-precedented change (11 siblings have one) that buys a startup fail-fast on a misprovisioned MESSAGES-CANONICAL stream — which today manifests as item 2's silent warn on every mutation instead — and makes the service runnable against a fresh dev NATS.

### 4. `high` — Fix the two Mongo aggregation shapes that don't scale with room size
**Dimension:** performance + code quality (two experts) · **Evidence:** `internal/mongorepo/pipelines.go:136-161`, `internal/service/threads.go:319-332`, `pkg/mongoutil/collection.go:93-106`, `pkg/mongoutil/pagination.go:33-43`
**Why:** `unreadThreadsPipeline` issues one correlated `$lookup` per thread room in the room *before* paging, so a busy room's cost is O(threads) per request regardless of `limit=20`. `AggregatePaged`'s `$facet` blocks index sort-limit pushdown and hard-fails at the 100 MB stage limit with no `allowDiskUse`, and `Offset` is uncapped so a client can buy an O(offset) skip. All three fixes have in-repo counterexamples to copy: `userThreadSubscriptionsPipeline` in the same file, and `AggregatePagedHasMore` at `collection.go:142`.

### 5. `high` — Project `GetSubscription`
**Dimension:** code quality + integration (two experts) · **Evidence:** `internal/mongorepo/subscription.go:28`, consumer at `internal/service/pin.go:38`
**Why:** A one-line change with the best effort ratio on the list. The only projection-free read in the service, on the pin/unpin hot path, deliberately uncached, pulling `model.Subscription`'s unbounded `ThreadUnread []string` plus ~25 other fields to read three. Every sibling read in the same file already does it correctly.

### 6. `high` — Collapse the seven hand-rolled canonical-event mappings into one
**Dimension:** maintainability · **Evidence:** `internal/service/reactions.go:125` (the existing complete mapper), versus `messages.go:531,613`, `migration.go:59,117`, `pin.go:125,168`
**Why:** The drift is not hypothetical — it has already happened. `messages.go:613` emits `EventDeleted` with `UserID`/`UserAccount`; `migration.go:117` emits the same event type without them, so consumers see two shapes for one event. `UserDisplayName` is set only on the reaction path. A complete mapper already exists and is used by exactly one of seven sites; routing the other six through it is mechanical and removes a silent-failure class.

### 7. `high` — Guard the four hand-maintained Cassandra column lists with a reflection test
**Dimension:** maintainability · **Evidence:** `internal/cassrepo/messages_by_room.go:13`, `pin.go:33`, `thread_messages.go:14`, `write.go:147`, scanner at `utils.go:125`
**Why:** `buildScanValues` errors on an *extra* column but silently zero-values a *forgotten* one, so adding a message field is a seven-file change whose failure mode is silent data loss with no test coverage (the only reference from tests is a benchmark). A single table-driven test asserting each column constant against `cqlFieldIndex(reflect.TypeOf(models.Message{}))` closes it.

### 8. `high` — Stop encoding every page twice
**Dimension:** performance · **Evidence:** `pkg/pagefit/pagefit.go:144-157`, `internal/service/utils.go:210-211,260-261`, default enabled at `cmd/main.go:213-219`
**Why:** Pure CPU waste on the single hottest path in a read service — reflective `json.Encode` for sizing, then a second marshal for the reply, three passes when `warnIfStillOversize` fires, and it runs even for a five-row page. The interim guard (skip sizing when `len(items) * knownMaxRowBytes < budget`) is cheap; returning the encoded bytes from `pagefit` is the real fix.

### 9. `medium` — Restore log discipline: five context-less `slog` calls, one log-and-return, one missing `WithLogValues`
**Dimension:** code quality · **Evidence:** `internal/service/messages.go:402,732,737`, `threads.go:370`, `internal/cassrepo/utils.go:162`; `reactions.go:58-59`; `reactions.go:25-27`
**Why:** Grouped because they are one afternoon's work and they all degrade incident triage: bare `slog.Warn` bypasses the `logctx` handler wired at `cmd/main.go:85`, so those lines carry no trace or request ID — including `messages.go:732/737`, the near-verbatim twin of `migration.go:139/145` which *does* carry one. `reactions.go:58` double-logs every failure, and `ReactMessage` is the one handler of 16 whose boundary error lines land without account or room. A `forbidigo` rule banning non-`Context` `slog` in `internal/service` and `internal/cassrepo` prevents regression.

### 10. `medium` — Flatten `cmd/` + `internal/` to the sanctioned layout
**Dimension:** architecture + maintainability (two experts; the architecture expert tagged it `critical`) · **Evidence:** `history-service/cmd/main.go`, `history-service/internal/*`, `Makefile:169-180`, `deploy/Dockerfile:7`, hand-copied wire structs at `room-service/reader_history.go:48`, `message-gatekeeper/fetcher_history.go:37`, `broadcast-worker/parent_fetcher.go:36`, `tools/loadgen/history_generator.go:100`
**Why:** Ranked here rather than at the top deliberately, and the demotion is stated rather than quiet: this is the only `cmd`/`internal` pair among 40+ services and it violates `CLAUDE.md` §1, but it carries **no runtime risk**. The real costs are a Makefile special case in two targets and four sibling services hand-copying wire structs they cannot import — genuine drift surfaces, but bounded ones. Best done as its own mechanical PR (no import cycles exist today), paired with promoting the client-facing structs from `internal/models` into `pkg/model`, ideally *before* items 6 and 7 so those land in their final home.

---

## Verdict

`history-service` is close to production-ready and reads like code written by people who know the codebase's conventions. Item 2 is the one finding that can lose user-visible data delivery in normal operation, and item 1 is the one that would let the next such bug ship unnoticed; between them they account for both `critical`/`high` clusters. Items 3–8 are well-precedented changes with in-repo counterexamples to copy. Nothing here requires a redesign.
