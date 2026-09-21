# teams-room-creation — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `b4126eb` (base `main`)  
**Overall score:** 3.2 / 5 (baseline 2026-09-01: 3.5, Δ -0.3)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

`teams-room-creation` reads chats flagged `needCreateRoom`, batches them per site, and publishes a room-create event for `room-worker`. It is well-built Go — 4s in code quality, architecture, maintainability and performance, a consumer-owned store interface, publish injected as a `publishFunc` so tests need no NATS, subjects exclusively from `pkg/subject`, and a compare-and-set on `updatedAt` so a concurrently re-synced chat is not silently cleared. It scores 3.2 on the strength of one critical integration finding that the sibling audits corroborate from the other end of the pipe.

**An empty roster is published unguarded, and the consumer treats it as authoritative.** `buildEvent` copies `c.Members` verbatim and the query filters on `needCreateRoom: true` alone, so a chat whose roster came back empty is published with `members: []`. `room-worker/teamsroomcreate.go:153-179` then computes `removed` as every existing subscription whose account is absent from the event and calls `DeleteSubscriptionsByAccounts` — a `DeleteMany`. This is not confined to first-time migration: both `teams-chat-sync` and `teams-chat-member-sync` re-set `needCreateRoom = true` on every re-sync of an already-materialised room, so the path is live in steady state. Three things make it worse. `integration_test.go:77` pins the empty-roster publish as *intended* behaviour. The audit lane cannot catch it — `teams-room-verify` compares `SubscriptionCount` against `accountsPresent(members)`, and 0 versus 0 verifies as converged. And the upstream reviewers found the two ways an empty roster is produced in the first place: `teams-chat-sync` writes a zero-member Graph response as a complete roster, and `teams-chat-member-sync` stores `Account: ""` for any member missing from `teams_user`, which this service forwards and `room-worker` then skips — evicting a live member on a transient lookup miss. The fix wants both halves: skip a chat with no resolvable accounts here, and refuse to delete when `len(wantAccounts) == 0` there, so neither side alone can empty a room.

Second, the job cannot fail. `publishBatch` logs a Warn and returns; `run` returns nil unconditionally; `main` then logs "teams-room-creation done". A site whose `ROOMS-TEAMS-{siteID}` stream is absent, or a total NATS outage, produces a green CronJob run forever with no counters in the completion line to contradict it. Its own sibling `teams-chat-member-sync` returns an error when any item failed.

Third, batches are bounded by chat count and never by bytes. `ROOM_CREATE_BATCH_SIZE` defaults to 100 chats, each carrying its full roster — a few hundred large group chats is megabytes before zstd. An oversized batch returns `nats.ErrMaxPayload`, gets a Warn, never clears its flag, and because the scan sorts by `_id` the identical batch is rebuilt and fails again on every run: a permanent poison batch. Four peers in this repo already read `nc.NatsConn().MaxPayload()` and clamp. The same scan is also unbounded and fully materialised, then copied again into the per-site map, so the initial migration decodes the whole flagged corpus into one pod's heap.

Coverage is 55.9%. As elsewhere in this family the shape is better than the number — `runner.go` is at 96.8% and `config.go` at 100%, while `publisher.go` and `store_mongo.go` are 0% because their only tests are integration-tagged and no pipeline runs them. Two gaps are real rather than measurement artefacts: no test ever has more batches than workers, so the semaphore's blocking path is never exercised under `-race`, and no test passes a cancelled context although `main.go` documents an abort-between-operations contract the runner does not implement.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 1 |
| Maintainability | 4 |
| Integration | 2 |
| Performance | 4 |

**Findings by severity:** 2 critical, 4 high, 17 medium, 18 low, 6 nitpick (47 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 4

### Evidence

- [medium] No run/request correlation ID is minted, so no log line and no published message carries one — `teams-room-creation/main.go:48` — `run()` builds ctx from `signal.NotifyContext` and never calls `natsutil.WithRequestID`; `publisher.go:32-35` even documents the consequence ("NewMsg returns a nil Header when ctx carries no request-id"). CLAUDE.md §3 requires a correlation ID minted at the entry point and present in all log lines; peer CronJobs do exactly that (`teams-hr-sync/main.go:97-100`, `teams-user-sync/main.go:76`). room-worker's per-chat WARNs (`room-worker/teamsroomcreate.go:35-37`) cannot be tied back to the run.
- [medium] A run in which every batch fails still exits 0 and logs success — `teams-room-creation/runner.go:70` — `run` returns nil unconditionally; publish and mark failures are only WARNed (`runner.go:84-92`) and `main.go:94` then logs "teams-room-creation done". A total-failure pass is indistinguishable from a healthy one in the Job status, and no published/failed counts are logged, unlike the sibling jobs' end-of-run stats lines.
- [medium] Batch loop ignores context cancellation — `teams-room-creation/runner.go:58-68` — the `sem <- struct{}{}` acquire and the `go`-spawn have no `select` on `ctx.Done()`, so on SIGTERM every remaining batch is still launched and each fails inside `publish` with `context.Canceled`, emitting a WARN per batch. A normal pod deletion becomes an error-log storm that masks real publish failures; a batch cancelled between publish and `MarkRoomsCreated` also leaves a clear un-applied.
- [low] Duplicated error-wrap text produces a stuttering chain — `teams-room-creation/runner.go:48` and `teams-room-creation/store_mongo.go:40` — both wrap with the identical string `"list chats needing room: %w"`, and `mongoutil.Collection.FindMany` already adds `"querying teams_chat: %w"` (`pkg/mongoutil/collection.go:63`). The operator sees `run: list chats needing room: list chats needing room: querying teams_chat: …`. CLAUDE.md asks each wrap to describe what *that* function was doing.
- [low] Publisher hand-rolls a helper that already exists in `pkg/natsutil` — `teams-room-creation/publisher.go:31-37` — `natsutil.NewMsgEncoded(ctx, subj, data, encoding)` (`pkg/natsutil/request_id.go:79-88`) does exactly this, and its doc comment explicitly says it owns the nil-Header guard "so callers don't need to know the quirk". This is the only production site in the repo setting `HeaderNatsEncoding` by hand; `admin-service/main.go:111` and `teams-hr-sync/main.go:186` use the helper.
- [low] `chatIDs` allocates a `[]string` per batch that is only ever used for its length — `teams-room-creation/runner.go:82`, used at `:86` and `:91` as `len(ids)` — `len(b.chats)` is identical, and `chatIDs` (`runner.go:149-156`) has no other caller, so the function is effectively dead weight on the publish path.
- [low] No `pkg/obs.Init` wiring — `teams-room-creation/main.go:63-66` — the job runs on plain slog with no-op tracer/propagator. CLAUDE.md's observability row says each service wires o11y once via `pkg/obs.Init`; the comment's "like the sibling teams-* jobs" is only partly true (`teams-hr-sync` and `teams-room-inspector` do wire it). Consequence: no metrics at all for a lane whose failures are otherwise silent (see the exit-0 finding).
- [nitpick] Four identical blanket `//nolint:gocritic // rangeValCopy` suppressions over the same heavy `model.TeamsChat` — `runner.go:100,127,151,162` — the stated reason ("index-range would be less idiomatic") is contradicted by the consumer of the same type: `room-worker/teamsroomcreate.go:33-34` uses `for i := range … { chat := &evt.Chats[i] }`.
- [nitpick] `fmt.Errorf` with a constant string and no verbs — `teams-room-creation/config.go:38,41` — should be `errors.New`. Also `main.go:42` returns `validateConfig`'s error bare, which CLAUDE.md forbids in general (harmless here only because the message self-describes).

### Recommendations

- [medium] Mint a run ID at the top of `run()` and stamp it on ctx + a `slog.With` logger — `main.go:48` — copy `teams-hr-sync/main.go:97-100` verbatim; this alone restores end-to-end correlation into room-worker at zero risk.
- [medium] Track per-batch outcomes, log a summary line with counts, and return an error from `runner.run` when no batch succeeded — `runner.go:45-71` — so the CronJob fails visibly instead of reporting success on a total outage.
- [medium] Guard the dispatch loop with `select { case sem <- struct{}{}: case <-ctx.Done(): }` and break out — `runner.go:60-68` — turns SIGTERM into a clean early stop instead of N cancelled-publish warnings.
- [low] Replace `publisher.go:31-37` with `natsutil.NewMsgEncoded(ctx, subj, natsutil.EncodeZstd(data), natsutil.EncodingZstd)` — removes the duplicated nil-Header quirk and keeps the encoding contract in one place.
- [low] Drop the duplicate wrap at `runner.go:48` (or reword it, e.g. `"load flagged chats: %w"`) — gives a readable, non-stuttering error chain.
- [low] Delete `chatIDs` and use `len(b.chats)` at `runner.go:86,91` — removes an allocation and a now-unused helper from the publish path.
- [nitpick] Wire `pkg/obs.Init` (or state the exemption once in the package doc rather than four sibling jobs each deciding separately) — `main.go:63-66`.

## 3. Architecture — score 4

### Evidence

- [medium] A run in which every publish fails still exits 0 with no counts — `teams-room-creation/runner.go:75-93`, `teams-room-creation/main.go:91-95` — `publishBatch` swallows both the publish and the mark error into a WARN; `run` returns nil unconditionally and `main` logs "teams-room-creation done". If one site's `ROOMS-TEAMS-{siteID}` stream is absent or unreachable, that site's migration stalls forever while every CronJob run looks green. The sibling job aggregates per-site outcomes and emits a summary line precisely so a site whose every batch fails cannot vanish silently (`teams-room-verify/runner.go:33-46,88,207`); this job has no counters at all.
- [medium] Batches are bounded by chat count, never by bytes — `teams-room-creation/runner.go:76-87`, `teams-room-creation/config.go:26` — `ROOM_CREATE_BATCH_SIZE=100` chats with large rosters can exceed the broker's `max_payload` even after zstd. The repo's convention is to read `nc.NatsConn().MaxPayload()` and clamp (`notification-worker/main.go:211-214`, `notification-worker/emit.go:52`; also `room-service/main.go:380`, `history-service/cmd/main.go:369`). Here an oversize batch fails identically every run: those chats never clear, with only a WARN and no size-aware split.
- [medium] The flagged-chat scan is unbounded and fully materialized — `teams-room-creation/store_mongo.go:31-43` → `pkg/mongoutil/collection.go:61-68` (`cursor.All` into one slice). `BatchSize` chunks only the publish, not the read, so the initial migration (the whole `teams_chat` corpus flagged at once) is decoded into the CronJob pod's heap in one shot. `mongoutil.WithLimit` (`pkg/mongoutil/options.go:48`) plus `_id` paging is already available and the compound partial index `{needCreateRoom:1,_id:1}` (`teams-chat-sync/store_mongo.go:56-59`) was built to support exactly that order.
- [medium] No correlation id is minted at the job entry point — `teams-room-creation/main.go:36-48` — so `natsutil.NewMsg` returns a nil header (`pkg/natsutil/request_id.go:68-74`) and every published batch reaches room-worker with no `X-Request-ID`; room-worker then logs a locally minted id (`room-worker/handler.go:255`) that cannot be traced back to the run. The job's own log lines also carry no run identifier, so two runs' logs are indistinguishable. Sibling `teams-hr-sync/main.go:97-99` mints one and threads it through ctx.
- [low] `publisher.go` re-implements an existing helper — `teams-room-creation/publisher.go:31-37` vs `pkg/natsutil/request_id.go:79-87` — `NewMsgEncoded` exists to own the nil-header quirk the comment describes; `teams-hr-sync/main.go:186` and `admin-service/main.go:111` use it.
- [low] Package doc names the pre-cutover subject — `teams-room-creation/main.go:4-5` (and `pkg/model/teamsroom.go:5-9`) say `chat.room.canonical.{siteId}.teams.create`, but the builder emits `chat.teams.room.canonical.{siteId}.create` (`pkg/subject/subject.go:210-214`) and room-worker matches the old form only transitionally (`room-worker/handler.go:265-268`). Misleading on the one contract this job owns.
- [low] No guard on an empty `SiteID` — `teams-room-creation/runner.go:101-105` groups by `c.SiteID` and `subject.RoomTeamsCanonicalCreate("")` yields `chat.teams.room.canonical..create`, an invalid empty-token subject that would fail publish on every run forever. Upstream `teams-chat-sync` skips empty-site votes (`teams-chat-sync/worker_test.go:202-217`), so this is defense-in-depth rather than a live bug.
- [nitpick] The dispatch loop ignores cancellation — `teams-room-creation/runner.go:60-68` — after SIGTERM it keeps launching every remaining batch; each fails fast on the cancelled ctx, producing a WARN flood instead of stopping.

### Recommendations

- [medium] Accumulate per-site published/failed/marked counters in `runner`, log one summary line, and return a non-nil error from `run` when no batch succeeded — `runner.go:45-93`, `main.go:91-95` — mirrors `teams-room-verify/runner.go:207` and makes a total stall a failed Job rather than a green one.
- [medium] Pass the broker's `MaxPayload()` into `runConfig` and split a batch whose encoded frame exceeds it (recursively halve, or refuse a single oversize chat loudly) — `runner.go:76-87` — removes the permanent poison-batch.
- [medium] Page `ListChatsNeedingRoom` with `mongoutil.WithLimit` + `_id > lastSeen`, draining until empty — `store_mongo.go:31-43` — bounds pod memory during the initial backfill at no index cost.
- [medium] Mint a request id once in `run` and stamp it on ctx (`idgen.GenerateRequestID` + `natsutil.WithRequestID`, as `teams-hr-sync/main.go:97-99`) — `main.go:36-48` — restores run-to-room-worker log correlation for the whole migration lane.
- [low] Replace the hand-rolled header guard with `natsutil.NewMsgEncoded(ctx, subj, natsutil.EncodeZstd(data), natsutil.EncodingZstd)` — `publisher.go:31-37`.
- [low] Fix the subject named in the package and model doc comments — `main.go:4-5`, `pkg/model/teamsroom.go:5-9`.
