# room-worker — Production Readiness Review

**Service:** `room-worker` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.3 / 5

The federation plumbing is correct and unusually well-reasoned — every OUTBOX type is correctly partitioned onto the FIFO lane, subjects all come from `pkg/subject`, `bootstrapStreams` is *stricter* than the spec, and the high-throughput consumer pattern is textbook. The problems are elsewhere. **A rename can permanently diverge `rooms.name` from `subscriptions.name`**: the room-name `$set` is unguarded and commits before a NAK-able federate, while the subscription write *is* high-water-mark guarded and refuses to follow it back. **The teams-mode deploy silently serves live client DM-create RPCs** on the shared queue group. And structurally the service is the hardest in the fleet to change safely: a 476-line function inside a 2,625-line `handler.go`, a 7,920-line test file, a 31-method store interface, and five copy-pasted federation blocks.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 2 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 4 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 12 | 22 | 15 | 6 | **55** |

> **Audit-coverage caveat.** `gosec` and the repo-owned `semgrep` rules are clean repo-wide; `govulncheck` and the registry packs could not run (blocked egress), so dependency-CVE coverage is unverified.

---

## 2. Go code quality — 4 / 5

Disciplined, idiomatic Go — correct `%w` wrapping, `errors.Is` never string comparison, clean `errcode` Tier-1 usage and `jsretry.SettleQuiet` — held back by 22 systematically-discarded marshal errors and context-less log calls.

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | **22 `json.Marshal` errors discarded to `_`** with no justifying comment (§3: "never ignore errors silently — comment if intentionally discarded"). A nil `data` is published as an **empty body**, so a marshal regression ships a malformed event instead of failing. The same file does it right at `handler.go:1977`, which makes this stylistic drift rather than policy | `handler.go:187`, `:452`, `:467`, `:480`, `:504`, `:506`, `:532`, `:668`, `:682`, `:693`, `:708`, `:731`, `:1188`, `:1195`, `:1210`, `:1234`, `:1259`, `:1296`, `:1348`, `:1868`, `:1876`, `:1898` |
| medium | `mustMarshal` violates the Go `must*` convention: it **swallows the error and returns `nil`** rather than panicking. The name promises a guarantee the body does not provide; every caller reads it as infallible | `handler.go:1347` |
| medium | Non-`Context` `slog` variants used inside functions holding a `ctx`, dropping the correlation ID §3 requires. These are the publish-failure and rename paths — precisely the lines an operator needs to join to a request — while 20+ sibling sites in the same file use `*Context` correctly | `handler.go:112`, `:2305`, `:2426`, `:2434`, `:2449`, `:2462`, `:2584` |
| medium | Raw decoder error text interpolated into a **client-facing** `errcode.BadRequest` — §3: "Never expose raw internal errors to clients". The other three unmarshal sites use static strings, so this is an outlier | `handler.go:2302` |
| low | `SubscriptionStore` declares 30 methods spanning rooms, users, apps, orgs, threads, room-members and cross-site flags; the `<Domain>Store` name no longer names a domain | `store.go:64` |
| low | 14 bare `return err`. Mostly benign (the callee wraps), but at `handler.go:417` the surviving text is only `pkg/outbox`'s "publish outbox event for {dest}" — **the caller's room operation is lost** | `handler.go:354`, `:417`, `:545`, `:626`, `:758`, `:861`, `:1014`, `:1300`, `:1566`, `:1643`, `:1901`, `:2408`, `:2546`; `teamsroomcreate.go:85` |
| low | `loadAddMemberInputs` uses a plain `errgroup.Group`, not `errgroup.WithContext`, so a failing branch leaves up to four sibling Mongo queries running to completion | `handler.go:780-783` |
| low | Audit-coverage gap (environmental): `govulncheck` + registry packs blocked | — |
| nitpick | Inconsistent log-key casing: `room_id` (24 sites) vs `roomId` vs `roomID`; `request_id` (22) vs `requestID` — breaks dashboard filters keyed on the dominant form | `handler.go:2308`, `:2309`, `:2584`, `:2610` |
| nitpick | Store result DTOs carry only `bson` tags, no `json` | `store.go:30`, `:37`, `:55` |
| nitpick | `errors.New("chat has no id")` states no operation, unlike every other error in the file | `teamsroomcreate.go:47` |

### Recommendations
- `medium` — Replace the 22 discarded marshals with the existing `publishCanonical` shape on error-returning paths, and a single logged-and-skipped branch on best-effort fan-outs. The payloads are all `pkg/model` structs, so the errors are unreachable — **that is the argument for a one-line comment, not for `_`**.
- `medium` — Either make `mustMarshal` actually panic (matching `text/template.Must`) or rename it `marshalOrEmpty` and document that callers may publish an empty body.
- `medium` — Convert the seven non-`Context` `slog` calls to `*Context`; add a `forbidigo`/semgrep rule so the plain variants cannot be reintroduced in `package main` handlers.
- `medium` — Drop `err.Error()` from `handler.go:2302`; use the static `errcode.BadRequest` form the other three sites use.
- `low` — Rename `SubscriptionStore` to `RoomWorkerStore` or split off the thread-cleanup and org-display groups; switch `loadAddMemberInputs` to `errgroup.WithContext`; normalize log keys to `snake_case`.

---

## 3. Architecture — 4 / 5

The federation boundary, stream-bootstrap opt-in, consumer pattern and shutdown ordering are all correct and unusually well-reasoned. The deductions are a mode leak, constructor DI bypassed by field pokes, and units that have outgrown the flat layout.

### Verified clean
All cross-site publishes route through `federate` → `outbox.Publish` (`handler.go:341-343`, call sites `:544`, `:757`, `:1299`, `:1900`, `:2406`, `:2515`, `teamsroomcreate.go:328`, `:392`) — **no direct remote-INBOX publish anywhere**; only same-site `subject.InboxInternal` search-feed writes. Every type used is in exactly one `pkg/outbox` partition set. `bootstrapStreams` sets only `Name + Subjects` and, when disabled, **verifies** the stream rather than no-op'ing — stricter than the spec. The consumer is `cons.Messages()` + `MAX_WORKERS` semaphore with `PullMaxMessages(2*MaxWorkers)`, never mixed with `Consume()`, and `BackOff` comes from `stream.DurableConsumerDefaults`. Shutdown follows `iter.Stop → wg.Wait → Drain → DB` at 25 s. Config is a typed `caarlos0/env` struct with fail-fast validation; no `os.Getenv`.

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | The **`teams`-mode deploy unconditionally registers the production sync-RPC** `chat.server.request.room.{site}.create.dm` on the shared `room-worker` queue group. `Mode` gates the stream, the durable and `bootstrapStreams` — but **not the router**. A Teams-migration pod sized for a batch job silently takes production DM-create traffic | `main.go:283-285` vs `:213`, `:449-452`; `bootstrap.go:43-46` |
| medium | Four dependencies assigned by **direct field poke after `NewHandler`** (`publishUsers`, `dekProvisioner`, `valkey`, `reconcileTTL`). The comment states the reason is avoiding churn — which is exactly what the functional-option pattern already used in-repo (`broadcast-worker/handler.go:102`, `inbox-worker/handler.go:172`) solves. A zero-value `reconcileTTL` silently means "recompute every add" | `main.go:266`, `:279-281`; `handler.go:64-67` |
| medium | `SubscriptionStore` is a **31-method interface** spanning subscriptions, rooms, users, apps, room_members, thread state, org-display rollups and room creation, with no seam for the Teams-only methods — so the default-mode deploy carries the migration surface | `store.go:64` |
| medium | `handler.go` is 2,625 lines / 111 KB with remove, add, create, rename, key-fan-out and sync-DM flows in one file (`processAddMembers` alone spans ~475 lines); `store_mongo.go` is 829 lines | `handler.go:834-1309` |
| low | Cross-service coherence knobs re-declared per service: `ROOM_KEY_RETIRED_TTL` duplicated verbatim in four services that `CLAUDE.md` requires to agree. `roomkeystore` owns the collection and should own a mounted config struct, as `roommetacache/ttlconfig.go:13` does | `main.go:70`; `room-service/main.go:67`; `bot-room-service/main.go:42`; `broadcast-worker/main.go:82` |
| low | `ROOM_META_CACHE_TTL` defaults to **60 s here vs 2 m** in broadcast-worker, message-gatekeeper and notification-worker. Per-process L1 caches, so not a shared-key bug — but exactly the drift the declare-once rule prevents | `main.go:58` |
| low | The single `PublishFunc` picks its transport **implicitly from whether `msgID` is empty** (core NATS vs JetStream), coupling durability choice to a parameter's emptiness | `main.go:239-262`; `handler.go:50` |
| nitpick | JetStream dispatch matches subjects with `strings.HasSuffix`/`Contains` rather than `pkg/subject` parsers; correctness depends on `.teams.create` being tested before `.create` | `handler.go:256-274` |
| nitpick | `MongoStore`/`NewMongoStore` exported from `package main` | `store_mongo.go:22`, `:42` |

### Recommendations
- `medium` — Gate `natsrouter.Register` (and the router construction) behind `cfg.Mode == "default"`, or give teams mode its own queue group.
- `medium` — Replace the four post-construction assignments with `NewHandler(..., opts ...handlerOption)`, mirroring broadcast-worker.
- `medium` — Split `SubscriptionStore` along the flows the handler already has; move the Teams surface behind its own interface; rename the residual to `RoomStore`.
- `medium` — Adopt the sanctioned sub-package layout: extract `processAddMembers`/`processRemoveMember` into `member.go`, create into `create.go`, key fan-out into `keyfanout.go`.
- `low` — Move `ROOM_KEY_RETIRED_TTL` + `ROOM_KEY_GRACE_PERIOD` into a `roomkeystore.Config` mounted in all four services; align `ROOM_META_CACHE_TTL` or document why 60 s.
- `nitpick` — Add `subject.ParseRoomCanonical`-style helpers so dispatch uses parsed tokens rather than suffix matching.

