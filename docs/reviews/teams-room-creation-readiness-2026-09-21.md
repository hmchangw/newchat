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
