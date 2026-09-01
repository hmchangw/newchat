# teams-room-creation — Production Readiness Review

**Service:** `teams-room-creation` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.5 / 5

The cleanest cross-service contract in the `teams-*` family, and it was verified end to end rather than assumed: subject from `pkg/subject` matching the sole binding of `ROOMS-TEAMS-{siteID}`; stream created by its owner `room-worker` and by nothing here; zstd framing round-tripped through `natsutil.DecodePayload`; the wire struct a **legal direct conversion** from the source type, so divergence becomes a compile error; and `Timestamp` stamped at the publish site with `Now` injected. Bounded concurrency, an index-backed precisely-projected read, one bulk write per batch.

The gaps are all about what happens when something goes wrong. **A batch too large for the NATS `max_payload` fails, logs at WARN, and is retried identically forever** — nothing splits, dead-letters or alerts, and with no metrics wired the stall is invisible. **`MarkRoomsCreated` discards its bulk result**, so a CAS that matches nothing is indistinguishable from a clean clear. And coverage at 55.9% is under the critical line, with the zstd publish contract — the actual cross-service wire format — proven only under Docker.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 4 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 1 | 12 | 12 | 2 | **28** |

---

## 2. Go code quality — 4 / 5

Idiomatic, tightly-scoped Go with correct `%w` wrapping and JSON `slog` throughout; the only real gap is a missing run-scoped correlation ID that both sibling jobs already have.

### Findings
- `medium` — No request/correlation ID is minted for the run, so every log line and every published message is uncorrelated — `teams-room-creation/main.go:36-48`
  CLAUDE.md §3 "Request Logging & Tracing" requires an ID generated at the entry point and propagated via `context.Context`. `teams-hr-sync/main.go:91` (`natsutil.WithRequestID`) and `teams-user-sync/main.go:74` both do this. Consequence: `natsutil.NewMsg` returns a nil `Header` (acknowledged at `teams-room-creation/publisher.go:33-35`), so room-worker mints a fresh ID and the CronJob run cannot be traced to the rooms it created.
- `low` — `publisher.go:31-37` hand-rolls the nil-header + `Nats-Encoding` guard that `natsutil.NewMsgEncoded` already owns — `pkg/natsutil/request_id.go:76-86`
  The pkg doc explicitly says "callers don't need to know the quirk"; this is the duplicate that drifts.
- `low` — `fmt.Errorf` with no format verb where `errors.New` is correct — `teams-room-creation/config.go:38`, `:41`
- `low` — SAST audit-coverage gap, environmental not a service defect: gosec and the 18 repo-owned semgrep rules are clean repo-wide; `govulncheck` and the semgrep registry packs could not run (egress blocked) — per `GLOBAL_PREP.md`.
- `nitpick` — Log key style is snake (`"site_id"`, `runner.go:79`) while sibling jobs use camel (`"requestId"`, `teams-user-sync/main.go:74`); the repo is genuinely mixed.

Positives verified: no `fmt.Println`/`log.Println`; no bare `err` returns; no string error comparison; no token/body logging; no `errcode` misuse (correctly absent — this service has no client boundary); `//nolint` directives carry reasons.

### Recommendations
- `medium` — Mint `idgen.GenerateRequestID()` in `run()`, stamp it via `natsutil.WithRequestID`, and use it as the base logger, matching `teams-hr-sync/main.go:91-93`.
- `low` — Replace `publisher.go:31-37` with `natsutil.NewMsgEncoded(ctx, subj, natsutil.EncodeZstd(data), natsutil.EncodingZstd)`.
- `low` — `errors.New` for the two verbless `fmt.Errorf` calls in `config.go`.
- `low` — Re-run `make sast-vuln` in a network-enabled CI leg before release; this environment cannot certify dependency CVEs.

---

---

## 3. Architecture — 4 / 5

Textbook service shape — consumer-owned store interface, constructor DI, no stream creation, typed env config — weakened by missing observability wiring and an unbounded shutdown path.

### Findings
- `medium` — No `pkg/obs.Init`, no metrics of any kind; the job emits nothing an operator can alert on (batches published/failed, chats stalled) — `teams-room-creation/main.go:63-66`
  CLAUDE.md §1 requires each service to wire the o11y SDK once via `pkg/obs.Init`; `teams-hr-sync` and `teams-room-inspector` do. The `noop.NewTracerProvider()` shortcut is documented but is still the deviation.
- `medium` — Both Mongo disconnects run on an unbounded `context.Background()`, so an unresponsive node holds the deferred cleanup past the pod's termination grace period — `main.go:55`, `main.go:61`
  `teams-room-verify/main.go:41-51` defines exactly this pattern correctly (`disconnectTimeout = 10s`, fresh non-cancelled context) and documents why; this service took the fresh-context half and dropped the deadline. The NATS drain, by contrast, is bounded correctly at `main.go:73`.
- `low` — `pkg/shutdown.Wait` is not used, contrary to CLAUDE.md §6 "in every service's `main.go`"; `signal.NotifyContext` (`main.go:48`) is the right primitive for a run-to-completion CronJob and matches all sibling `teams-*` jobs, so this reads as a rule that predates the job services rather than a defect.

Verified compliant: `TeamsChatStore` defined in the consumer with exactly two methods (`store.go:26-34`); `newRunner`/`newMongoStore` accept interfaces, return structs; file layout matches the per-service convention (no `routes.go`/`handler.go` — correct, no HTTP or subscribe surface); **no `bootstrap.go` and no stream creation** — ROOMS-TEAMS is created by its owner `room-worker` (`room-worker/bootstrap.go:43-47`), exactly as the `BOOTSTRAP_STREAMS` rule demands; config is a typed `caarlos0/env` struct with `required,notEmpty` on both connection strings and `envDefault` on the knobs (`config.go:14-28`); `Pool mongoutil.PoolConfig` is mounted as a named field, not re-declared (`config.go:19`).

### Recommendations
- `medium` — Wire `pkg/obs.Init` and emit at least three counters: chats listed, batches published, batches failed — a silent CronJob whose batches all fail looks identical to a healthy no-op run.
- `medium` — Copy `teams-room-verify`'s bounded `disconnect(client)` helper verbatim into `main.go`.
- `low` — Add a startup existence check for `ROOMS-TEAMS-{siteID}` per site (mirroring `room-worker/bootstrap.go:57-60`'s fail-fast) so a misprovisioned site surfaces at startup rather than as a warn-per-run forever.

---

---

## 4. Test coverage — 1 / 5

**55.9% (127 statements)** — below the 60% floor, so CLAUDE.md §4 forces a `critical` and a score of 1, though the shortfall is concentrated in wiring code that integration tests do exercise.

### Findings
- `critical` — Coverage is 55.9%, below both the 80% floor and the 60% critical threshold (CLAUDE.md §4 "Coverage") — `coverage_by_service.txt`
- `high` — The uncovered mass is entirely `main.run` 9.4%, `newJetStreamPublisher` 0.0%, and the three `store_mongo.go` funcs 0.0% (`covfunc.txt`); the store and publisher are covered only behind `//go:build integration`, which the profile excludes. So the number understates real quality — but it also means **no non-Docker CI leg proves the zstd publish contract**.
- `medium` — `publishBatch`'s only uncovered line is an unreachable branch: `json.Marshal` of `TeamsRoomCreateEvent` (all strings/times/slices) cannot fail, so 83.3% is the ceiling — `runner.go:77-81`
- `medium` — Genuinely untested behaviours that matter: context cancellation mid-run (no test that a SIGTERM stops publishing or that `MarkRoomsCreated` at `runner.go:89` behaves on a cancelled ctx); `MaxWorkers` actually bounding concurrency (`runner.go:58`); `planBatches` boundaries — a site with exactly `BatchSize` chats, and `BatchSize=1` (`runner.go:110-116`); env defaults (`ROOM_CREATE_BATCH_SIZE=100`) are never parsed in a test, only hand-built (`config_test.go:13`).
- `low` — `TestRunner_ListErrorReturned` asserts only `require.Error` (`runner_test.go:157`); it does not assert the wrap text or that no publish occurred.

Verified compliant: table-driven with named subtests (`config_test.go:20-28`); tests in `package main`; mocks generated by mockgen in `mock_store_test.go` and confirmed non-stale repo-wide; no real DB/NATS in unit tests; the publish function is injected as a field (`runner.go:29`) exactly as CLAUDE.md §4 prescribes; integration files carry the build tag with `TestMain` → `testutil.RunTests(m)` (`store_mongo_test.go:18`) and use `testutil.MongoDB`/`testutil.NATS`, no inline `GenericContainer`; the CAS-miss path has a dedicated integration test (`store_mongo_test.go:74-97`).

### Recommendations
- `critical` — Unit-test `newJetStreamPublisher` against a fake `o11ynats.JetStream` asserting the `Nats-Encoding: zstd` header and that the body round-trips through `natsutil.DecodePayload` — this is the cross-service wire contract and today only Docker proves it.
- `high` — Add a cancellation test: cancel ctx after the first batch and assert remaining batches neither publish nor mark.
- `medium` — Extend `runner_test.go` with `BatchSize=1` and exactly-`BatchSize` cases, plus a `MaxWorkers=1` case asserting serialized publishes.
- `medium` — Delete the unreachable marshal branch at `runner.go:77-81` (or assert it via an injected marshaller) so the function can reach 100%.
- `low` — Add an `env.ParseAs[Config]` test with `t.Setenv` covering the defaults.

---

---

## 5. Maintainability — 4 / 5

973 lines across 12 small files with no function over ~45 lines and comments that consistently explain WHY; the blemishes are three redundant passes over the same slice and two stale subject doc-comments.

### Findings
- `medium` — `chatIDs(b.chats)` allocates a full `[]string` per batch and is then used **only** for its length — `runner.go:82`, consumed at `:86` and `:91` as `len(ids)`, which equals `len(b.chats)`
  The whole helper (`runner.go:149-156`) plus its `//nolint` exists to serve two log lines.
- `medium` — Two doc-comments name a subject that does not exist: `chat.room.canonical.{siteId}.teams.create` — `teams-room-creation/main.go:4` and `pkg/model/teamsroom.go:6`. The real subject is `chat.teams.room.canonical.{siteID}.create` (`pkg/subject/subject.go:213`). The `pkg/model` one is worse: it misleads every future consumer of the shared type.
- `low` — `publishBatch` walks `b.chats` three times through three near-identical `//nolint:gocritic // rangeValCopy` helpers (`buildEvent` `:128`, `chatIDs` `:152`, `roomCreatedRefs` `:163`), copying a heavy struct each pass. One loop building both the event and the refs removes two helpers and two nolints.
- `low` — Dead error branch: `json.Marshal` on `TeamsRoomCreateEvent` cannot fail — `runner.go:77-81`.
- `nitpick` — `RoomCreatedRef` (`store.go:17`) is the only exported type in `package main` and nothing outside consumes it; CLAUDE.md §3 says export only what other packages consume. Harmless, but mockgen forced it.

Adding a feature here is easy: a new projected field is a one-line change in the projection (`store_mongo.go:36`), the model, and `buildEvent`. No leaky abstractions; the store hides both clients behind two methods.

### Recommendations
- `medium` — Fix both stale subject doc-comments to `chat.teams.room.canonical.{siteID}.create`; the `pkg/model` one is the shared contract.
- `medium` — Delete `chatIDs` and log `len(b.chats)` directly.
- `low` — Merge `buildEvent`/`roomCreatedRefs` into one pass over `b.chats`, dropping two `//nolint` directives.
- `low` — Drop the unreachable marshal-error branch.

---

---

## 6. Integration — 4 / 5

The room-worker contract verified end-to-end — subject, stream binding, zstd framing, struct identity and timestamp all line up — with one avoidable field omission that forces the consumer to re-derive room type from a string suffix.

Cross-service contract, traced and confirmed:
1. **Subject** `subject.RoomTeamsCanonicalCreate(siteID)` (`runner.go:83`) → `chat.teams.room.canonical.{siteID}.create` (`pkg/subject/subject.go:213`), matching `RoomTeamsCanonicalWildcard`, which is the sole binding of `ROOMS-TEAMS-{siteID}` (`pkg/stream/stream.go:49-54`). No raw `fmt.Sprintf` subject anywhere in the service.
2. **Stream ownership** `room-worker` in teams mode selects that stream (`room-worker/main.go:212-216`) and bootstraps/verifies it (`room-worker/bootstrap.go:43-47`); this service creates nothing — correct.
3. **Framing** publish sets `Nats-Encoding: zstd` (`publisher.go:31-37`); consumer decodes via `natsutil.DecodePayload` before dispatch (`room-worker/handler.go:264-265`). Round-trip proven in `integration_test.go:101-103`.
4. **Struct** `model.TeamsRoomCreateMember(m)` is a legal direct conversion — `TeamsChatMember` (`pkg/model/teams.go:72-76`) and `TeamsRoomCreateMember` (`pkg/model/teamsroom.go:28-33`) are field-identical, so divergence becomes a compile error (`runner.go:133`). All fields carry both `json` and `bson` camelCase tags.
5. **Timestamp** set at the publish site via `now.UTC().UnixMilli()` with `Now` injected (`runner.go:76`, `:144`); the consumer reads it as `acceptedAt` and it becomes the room's `UpdatedAt` and the ES external doc version (`room-worker/teamsroomcreate.go:32`, `:288`).

### Findings
- `medium` — The event omits `chatType`, which the source document carries authoritatively (`pkg/model/teams.go:85`, values `oneOnOne|group|meeting`), forcing room-worker to re-derive DM-ness from a chat-id string suffix — projection at `store_mongo.go:36`, re-derivation at `room-worker/teamsroomcreate.go:223-228`
  Any `oneOnOne` chat whose id does not end `@unq.gbl.spaces` silently becomes a channel room. Passing the field the producer already has removes a guess from the consumer.
- `medium` — No guard against an empty `SiteID` at the publish site: `subject.RoomTeamsCanonicalCreate("")` yields `chat.teams.room.canonical..create`, an invalid empty-token subject whose publish fails every run forever, leaving those chats flagged permanently — `runner.go:83`
  Upstream does guarantee non-empty (`teams-chat-sync/syncer.go:216-221`, `SYNC_DEFAULT_SITE_ID` is `required,notEmpty`), which is why this is medium and not high — but the sibling defends and this one does not.
- `low` — Cross-service ID convention: room-worker derives the DM room id via `idgen.DeterministicID([]byte(chat.ID))` (`room-worker/teamsroomcreate.go:62`) rather than `idgen.BuildDMRoomID(a,b)` as CLAUDE.md §6 mandates for DM rooms. Deliberate (the Teams chat id is the stable key) but it means migrated DMs will not collide-dedup with natively created DMs between the same two users.
- `low` — Doc drift on the shared model's subject comment (see D4).

Not applicable, verified: no `chat.user.…` subject and no HTTP route, so `docs/client-api.md` and its derived views are correctly untouched — this is a server-to-server contract, matching the explicit precedent documented at `pkg/model/teams.go:100-103`. No OUTBOX/INBOX participation, so the `pkg/outbox` partition rule does not apply. No Cassandra, no `msgbucket`, no room-key TTL surface.

### Recommendations
- `medium` — Add `chatType` to the projection (`store_mongo.go:36`), to `TeamsRoomCreateChat`, and to `buildEvent`; switch `roomTypeFromTeamsChatID` to prefer it, keeping the suffix as fallback for in-flight events.
- `medium` — Skip and WARN on a chat with an empty `SiteID` in `planBatches` rather than emitting an invalid subject.
- `low` — Correct the subject in `pkg/model/teamsroom.go:6` and `main.go:4`.
- `low` — Add a comment at `room-worker/teamsroomcreate.go:62` recording why migrated DMs deviate from `BuildDMRoomID`.

---

---

## 7. Performance — 4 / 5

Bounded concurrency, a precisely projected index-backed read, one bulk write, and zstd on the wire — the residual risks are an unbounded result set on the first migration run and a permanently-stalling oversize batch.

### Findings
- `medium` — `ListChatsNeedingRoom` loads **every** flagged chat into memory with no `WithLimit`/pagination — `store_mongo.go:34-38`
  On the first migration pass this is the entire Teams chat corpus, each with a `members` array, decoded into `[]model.TeamsChat` by `cursor.All` (`pkg/mongoutil/collection.go:65`). Fleet-consistent (every `teams-*` sibling does the same), but this is the one job whose cold-start set is unbounded by design.
- `medium` — A batch too large for the NATS `max_payload` fails, is logged at WARN, and is retried identically on every subsequent run forever — `runner.go:84-88` with a static `BatchSize` default of 100 (`config.go:26`)
  Nothing splits, dead-letters, or alerts; the chats simply never leave `needCreateRoom=true`. With no metrics (D2) this is invisible.
- `medium` — `MarkRoomsCreated` discards the `*BulkResult`, so a CAS that matches nothing is indistinguishable from a clean clear — `store_mongo.go:63`
  `pkg/mongoutil/collection.go:169` returns matched/modified counts. A chat whose `updatedAt` churns every run (member-sync racing) stays flagged and republished indefinitely with zero signal.
- `low` — `MarkRoomsCreated` is called with the same `ctx` that SIGTERM cancels (`runner.go:89`), so a shutdown arriving between a successful PubAck and the flag clear guarantees the mark fails and the whole batch republishes next run. Idempotent downstream (documented at `publisher.go:26-28`), but `context.WithoutCancel` here would avoid the redundant fan-out.
- `low` — The dispatch loop never checks `ctx.Err()` (`runner.go:60-68`), so after cancellation it still spawns a goroutine per remaining batch, each of which fails its publish.
- `low` — Per-batch waste: a `[]string` allocated only to be measured (`runner.go:82`) and three copying passes over the same heavy slice (see D4).

Verified sound: goroutines are bounded by a `chan struct{}` sized from `MaxWorkers` and every one is joined by `wg.Wait()` (`runner.go:58-69`) — no leak, no `time.Sleep`; the read is served by `teams-chat-sync`'s partial compound index `needCreateRoom:1,_id:1`, which also serves the `_id` sort without an in-memory pass (`teams-chat-sync/integration_test.go:99-103`); the projection is explicit and minimal (`store_mongo.go:35-37`); no `$lookup`; no N+1 — one read, one bulk write per batch; payloads are zstd-compressed (`publisher.go:31`); `mongo.ErrNoDocuments` is not applicable (list, not point-read). `jsretry`/`BackOff` rules do not apply: this service has no JetStream consumer, and the CronJob re-run is the retry.

### Recommendations
- `medium` — Add a configurable `LIST_LIMIT` via `mongoutil.WithLimit` and let the CronJob drain across runs; the `_id` sort already makes this deterministic.
- `medium` — Log the `BulkResult` matched/modified counts and emit a counter for `matched < len(refs)`; without it, a permanently stuck chat is silent.
- `medium` — On a publish failure, detect `nats.ErrMaxPayload` and split the batch (or refuse to build a batch whose marshalled size exceeds a configured cap) so an oversize batch cannot stall forever.
- `low` — Use `context.WithoutCancel(ctx)` for the `MarkRoomsCreated` call at `runner.go:89`, and `break` the dispatch loop on `ctx.Err()`.
- `low` — Drop the `chatIDs` allocation and merge the three slice passes.
