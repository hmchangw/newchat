# notification-worker — Production Readiness Review

**Service:** `notification-worker` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

The hot path is well-engineered — sonic with pretouch, narrowed-candidate batching, L1/L2 room caches, a bounded worker semaphore, precise Mongo projections, clean `jsretry` discipline. Three things pull the score down, and all three were found independently by more than one expert. **A shutdown-time send-on-closed-channel race can panic the process during a routine rolling restart**: the member-event invalidation goroutine is tracked by no `WaitGroup`, yet shutdown stops the iterator and closes the channel it sends into two steps later — a message already returned by `Next()` sends after the close, and a `select` with a `default` does not save it. **The dev bootstrap narrows a shared stream**: it creates `MESSAGES-CANONICAL-{site}` with only the `.created` leaf instead of the declared `.>` wildcard, silently constraining a stream four services consume. And **the mute gate fails open at remote sites** — the user-settings replica this worker reads has no creation or backfill path, so a muted user at a site added after their last settings write gets pushed anyway. Coverage at 59.0% is under the 60% critical line, driven by a 196-statement `main()` at 2.0%.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 4 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 2 | 12 | 18 | 13 | 9 | **54** |

---

## 2. Go code quality — 4 / 5

Idiomatic, carefully-reasoned Go with disciplined error wrapping and correct `errcode`/`jsretry` tiering; marred by one shutdown-time send-on-closed-channel race and a cluster of logging-discipline drifts (double-log, non-context `slog`, missing request IDs).

**Message-body privacy: verified clean.** No log line in the service carries `msg.Content` or a payload; `handler.go:479` logs only counts. The single body-logging path is `logctx.CapturePayload` via `logctx.ConsumeContext` (`main.go:363`), which is inert here because the service never calls `logctx.Configure` (`pkg/logctx/limiter.go:27,87` — `capturePayloads` defaults false).

### Findings
- `high` — the canonical-member-event reader goroutine is untracked by any WaitGroup (`notification-worker/main.go:304-325`), yet shutdown step 4 closes the channel it sends on (`notification-worker/main.go:420`) after only `invalIter.Stop()` (`main.go:416`). `Stop()` does not wait for a message already returned by `Next()`, so a decode in flight can reach `invalCh <- evt.RoomID` (`main.go:318`) after the close and panic the process during graceful shutdown. Every other goroutine here has a tracked termination path; this one does not.
- `medium` — log-and-return double-logging on the emit path: `slog.Error("emit push batch failed", …)` (`notification-worker/handler.go:332`) is followed by returning the same error, which `jsretry.Settle` (`notification-worker/main.go:375`) logs again (`pkg/jsretry/jsretry.go:129`). `SettleQuiet` exists precisely for already-logged paths; CLAUDE.md's "never log AND return" is the same rule one transport over.
- `medium` — the per-message path uses non-context `slog` and so loses o11y trace correlation: `handler.go:247`, `handler.go:332`, `handler.go:387`, `handler.go:497`, `presence.go:76,83,87,95`, `usersettings.go:107`, `main.go:312,320`. Only `handler.go:479` uses `WarnContext`. Sibling hot-path worker `broadcast-worker/handler.go:205,219,228,234,255` is uniformly `*Context`.
- `medium` — four warn lines in the presence fan-out carry no request/correlation ID at all (`notification-worker/presence.go:76,83,87,95`), against "include in all log lines"; the handler and `usersettings.go:107` do attach it, so a presence failure is the one event that cannot be joined to its message.
- `medium` — `logctx.ConsumeContext` is called (`notification-worker/main.go:363`) but the service never declares `logctx.Config` nor calls `logctx.Configure`/`SetupDefault` (contrast `broadcast-worker/main.go:99,125,133`). The admitted `X-Debug` rung is therefore dead on arrival: the package limiter stays `denyAll` (`pkg/logctx/limiter.go:24`) and the default handler installed by `obs` (`pkg/obs/obs.go:229`) is not the logctx wrapper, so sub-INFO records never emit. Half the helper's contract is wired.
- `low` — bare `return err` from the JetStream publish adapter (`notification-worker/emit.go:91`), against "never return bare `err`". The caller re-wraps (`emit.go:63`), so impact is cosmetic, but it is the one unwrapped return in the service.
- `low` — sonic marshals `model.PushNotificationEvent`, which carries a `map[string]int` (`pkg/model/push.go:17`), on the emit hot path (`notification-worker/emit.go:41`) with no sonic wire-compat test in this service (only `pretouch_test.go` mentions sonic). CLAUDE.md names map fields as one of two triggers for confirming compat before adopting sonic, with `broadcast-worker`/`message-gatekeeper` as the pattern. Dedup keys off `evt.ID`, not a payload hash, so the actual risk is small — the missing verification is the gap.
- `low` — audit-coverage gap, not a service defect: gosec and the repo-owned semgrep rules are clean repo-wide; `govulncheck` and the semgrep registry packs could not run (blocked egress).
- `nitpick` — `ch := ch` loop-variable copy (`notification-worker/presence.go:70`) is obsolete under `go 1.25.13` (`go.mod:3`); the newer `errgroup` fan-out in `handler.go:379` already omits it.
- `nitpick` — 16 identifiers are exported from `package main` (e.g. `MemberCache`, `EligibleForPush`, `ParentMessageInfo`), which nothing can import; `badgeClient` (`badge_client.go:23`) and `notifSettings` (`usersettings.go:33`) show the intended unexported form.

### Recommendations
- `high` — track the invalidation reader with `invalWG` (or a dedicated one) and wait on it *before* `close(invalCh)`; alternatively drop the close entirely and let `invalCancel()` plus a `select` on `invalCtx.Done()` terminate the drain worker.
- `medium` — switch `main.go:375` to `jsretry.SettleQuiet` (handler already logs) or delete `handler.go:332` and let `Settle` own the log; pick one owner.
- `medium` — mechanically convert the per-message `slog.Warn/Error` calls listed above to `WarnContext`/`ErrorContext`, and add `"request_id", natsutil.RequestIDFromContext(ctx)` to the four `presence.go` lines (thread `ctx` into the goroutine closure, which it already captures).
- `medium` — add `DebugLog logctx.Config \`envPrefix:"DEBUG_LOG_"\`` to `config` and call `logctx.SetupDefault(os.Stdout)` + `logctx.Configure(cfg.DebugLog)` at startup, matching the other canonical-stream workers — or drop `ConsumeContext` for plain `natsutil.StampRequestID` so the code stops implying a capability it lacks.
- `low` — wrap the adapter error at `emit.go:91` (`fmt.Errorf("jetstream publish: %w", err)`), and add a sonic-vs-stdlib wire-compat test for `PushNotificationEvent` (including a populated `UnreadCounts`) modelled on `broadcast-worker`'s.

---

## 3. Architecture — 4 / 5

Strong, deliberate architecture — consumer-defined interfaces, constructor DI, correct high-throughput consumer pattern, and clean `pkg/subject`/`pkg/stream` discipline — held back by a bootstrap subject narrowing, an untracked shutdown goroutine, and a few config-convention slips.

### Findings
- `high` — `bootstrapStreams` creates `MESSAGES-CANONICAL-{site}` with the **`.created` leaf** as its subject list, not the stream's declared `chat.msg.canonical.{site}.>` — `notification-worker/main.go:186`, `notification-worker/bootstrap.go:27-31`
  `CreateOrUpdateStream` is an update: whichever pod boots last wins, and in dev (`BOOTSTRAP_STREAMS=true`, `deploy/user/docker-compose.yml:47`) this narrows the stream and silently rejects `.edited`/`.deleted` publishes. Sibling consumers pass the wildcard instead (`broadcast-worker/main.go:330`, `roomlist-worker/main.go:128` use `wiring.CanonicalWildcard`), and `pkg/stream.MessagesCanonical` is the declared schema. CLAUDE.md: bootstrap sets `Name + Subjects` **from `pkg/stream.<Stream>(siteID)`**.

- `high` — the member-event invalidation loop goroutine is in no `WaitGroup`, yet `close(invalCh)` runs in the next shutdown step — `notification-worker/main.go:279-303` (goroutine), `main.go:411-425` (shutdown steps 3 and 4)
  `pullSubscription.Stop()` only closes `done` (`nats.go@v1.50.0/jetstream/pull.go:769-781`); it establishes no happens-before with the loop, which may be between `iter.Next()` returning and `invalCh <- evt.RoomID` (`main.go:296`). A send on a closed channel panics during shutdown. The main fan-out loop is guarded exactly right (`main.go:319-323`, "The loop itself is counted") — the invalidation loop was not given the same treatment.

- `medium` — bootstrap sets `Compression: jetstream.S2Compression` on the PUSH stream — `notification-worker/bootstrap.go:36-37`
  CLAUDE.md: the helper "sets ONLY the stream's schema — `Name + Subjects`". Storage policy is ops/IaC's; here it silently differs between the dev-bootstrapped stream and the provisioned one. Every other bootstrap in the repo passes `cfg.Name`/`cfg.Subjects` and nothing else (`inbox-worker/bootstrap.go:48-51`, `outbox-worker/bootstrap.go:43-46`, `room-worker/bootstrap.go:48-51`).

- `medium` — the invalidation consumer's config is hand-rolled and never passes through `stream.DurableConsumerDefaults` — `notification-worker/main.go:288-293`
  It gets no `MaxDeliver`, `AckWait` or `BackOff`, unlike the main consumer (`main.go:190`, `buildConsumerConfig` at `main.go:443-447`). Server defaults apply by accident rather than by the repo's ConsumerSettings contract.

- `medium` — `MONGO_URI` and `NATS_URL` carry `envDefault` localhost values instead of `required` — `notification-worker/main.go:37,40`
  CLAUDE.md: "never default secrets or connection strings — mark them `required`". `MODE` is correctly `required` (`main.go:76`), and `message-worker/main.go:50` marks `MONGO_URI,required`, so the convention is live in the repo.

- `low` — no `store.go` / `store_mongo.go`; the two Mongo store implementations live in feature files — `notification-worker/threads.go:26`, `notification-worker/usersettings.go:74`
  Interfaces are correctly consumer-defined (`ThreadFollowerLister` `threads.go:22`, `UserSettingsSnapshotter` `usersettings.go:19`, `badgeClient` `badge_client.go:29`, `publisher` `emit.go:20`) and injected via `HandlerDeps` (`handler.go:45-61`), so the *principle* holds; only the file layout deviates. The sanctioned sub-package exception does not apply to a worker.

- `low` — `push-notification-service` has no `bootstrap.go` and no `BOOTSTRAP_STREAMS`, so `notification-worker` is the de-facto creator of a stream it only publishes to — `notification-worker/bootstrap.go:33-38` vs `push-notification-service/` (no bootstrap file)
  `pkg/stream.PushNotification` comments it as "ops-owned in prod", so nothing breaks; the ownership is just inverted from the INBOX/OUTBOX model.

- `nitpick` — `deploy/{user,bot}/` nested variant split departs from CLAUDE.md's flat `deploy/` layout — `notification-worker/deploy/user/`, `deploy/bot/`
  Justified: one binary, two pipelines selected by `MODE` (`main.go:76`); the Dockerfiles are byte-identical and only compose env differs. Worth codifying in CLAUDE.md rather than leaving as an undocumented deviation.

- `nitpick` — `chunkStrings` is defined in `presence.go:109` but consumed by `usersettings.go:104`.

**Boundary verified as sound.** The settings read is against the *local* `users` collection (`main.go:145`, `usersettings.go:118`), replicated cross-site by `inbox-worker` (`inbox-worker/handler.go:684-691`, `inbox-worker/main.go:280-292`) — no synchronous cross-site RPC on the push gate. Read preferences are split with documented intent (`main.go:127,133,141-146`). Consumer pattern is the correct high-throughput form: `cons.Messages()` + `MAX_WORKERS` semaphore + `PullMaxMessages(2*MaxWorkers)` (`main.go:305-315`), never mixed with `Consume()`. No raw `fmt.Sprintf` subject construction anywhere.

### Recommendations
- `high` — pass `wiring.CanonicalStream.Subjects` (not `CanonicalCreated`) to `bootstrapStreams`; keep `CanonicalCreated` for the consumer `FilterSubjects` only. Add a bootstrap test asserting the created subject list equals `stream.MessagesCanonical(siteID).Subjects`.
- `high` — add the invalidation loop to a `WaitGroup` (or reuse `invalWG`) and wait on it *before* `close(invalCh)`, mirroring the main loop's counted-goroutine comment at `main.go:319`.
- `medium` — drop `Compression` from `bootstrap.go`; move it to the IaC stream definition.
- `medium` — build the invalidation consumer through `stream.DurableConsumerDefaults(cfg.Consumer)` so it inherits `MaxDeliver`/`AckWait`/`BackOff`.
- `medium` — mark `MONGO_URI` and `NATS_URL` `required`, matching `message-worker`.
- `low` — consolidate `mongoThreadFollowers` and `mongoUserSettings` into `store_mongo.go` with their interfaces in `store.go`, or get the `deploy/{user,bot}` + per-concern-file layout written into CLAUDE.md as a sanctioned worker variant.
- `low` — give `push-notification-service` its own `bootstrap.go` verifying (not creating) `PUSH-NOTIFICATION-{siteID}`, so stream ownership follows the INBOX/OUTBOX single-owner rule.

---

## 4. Test coverage — 1 / 5

Coverage is 59.0% (373/632 statements) — below the CLAUDE.md 60% line, so the dimension is capped at 1 despite genuinely excellent `handler.go` test work (96.7%); the entire wiring layer, the migration ack-drop branch, and the Mongo settings/thread readers carry no unit coverage.

### Findings
- `critical` — Service coverage is **59.0% (373/632 stmts)**, below the CLAUDE.md Section 4 60% line and far below the 80% merge floor. Per-file: `handler.go` 96.7% (202/209), `main.go` **2.0% (4/196)**, `usersettings.go` 51.1% (23/45), `threads.go` **0% (0/15)**, `emit.go` 75%, `presence.go` 79.4% — `/tmp/.../scratchpad/pr/coverage_by_service.txt`
  One 196-statement `main()` is essentially the whole deficit; the business logic is well tested.
- `critical` — The migrated-event **Ack-drop branch is untested in-service**: `natsutil.IsMigrationLiveHeader` → `msg.Ack()` + `notifySuppressed` lives inside `main()` and never executes under test — `main.go:366-373`. `migration_test.go:14` only exercises the `pkg/natsutil` predicate, not the worker's reaction to it. An inverted condition or a dropped `Ack()` would re-notify every migrated message and no test would fail.
- `high` — `main.go` is 2% covered because all wiring is inline in `func main()`, including the three suppression kill switches that decide whether the mute gate runs at all (`PresenceEnabled`→noop, `UserSettingsEnabled`→`noopUserSettings`, `BadgeCountEnabled`→nil client) — `main.go:215-243`. Nothing proves `USER_SETTINGS_ENABLED=false` actually yields pre-enforcement behaviour end-to-end; `config_test.go:78` only asserts the parsed bool.
- `high` — The **member-cache invalidation goroutine is entirely uncovered**: decode-failure → `Ack()` + continue, and the `default:` queue-full drop path — `main.go:304-325`. A silently full `invalCh` means stale member lists (and therefore missed or leaked notifications) until TTL; the drop branch has no regression guard.
- `high` — `mongoThreadFollowers.Lookup` is 0% in unit tests — `threads.go:33`. Its `mongo.ErrNoDocuments` → empty-followers branch (`threads.go:43-45`) is the *first-reply race* semantics the handler depends on at `handler.go:165-169`; it is exercised only by `TestMongoThreadFollowers_Lookup` (`integration_test.go:144`), which is Docker-gated and excluded from the coverage gate.
- `medium` — `mongoUserSettings.appendChunk` is 0% and `Snapshot` 30% — `usersettings.go:91,115`. The fail-open contract (`Find` error / `Decode` error / `cur.Err()` → return partial map, never error) is the load-bearing "a Mongo blip must not silence the site" rule and is only covered behind the integration tag (`integration_test.go:457`). The documented mid-loop-timeout partial-chunk behaviour (`usersettings.go:100-104`) has no test at all.
- `medium` — A **unit test starts a real in-process NATS server**, contrary to "Never connect to real databases, NATS, or external services in unit tests" — `parent_fetcher_test.go:25-38` (`natsserver.NewServer` + `o11ynats.Connect`) in an untagged file. Every other collaborator here is stubbed through a narrow interface (`presenceRequester`, `publisher`); `ParentFetcher` should be too.
- `low` — No `go.uber.org/mock` mocks and no `//go:generate mockgen` anywhere in the service; all collaborators use hand-written fakes in `handler_test.go`. Acceptable given there is no `store.go`, but it is a documented-convention deviation.
- `nitpick` — Thin adapters uncovered: `natsPresenceRequester.Request` (`presence.go:175`), `jsPublisher.PublishMsg` (`emit.go:88`), `pretouchJSON` (`pretouch.go:19`). Low value individually, ~10 statements total.

Quality note (the part that counts): the covered portion is **not** vanity. `handler_test.go` has 69 tests with descriptive names, table-driven where it matters (`TestShouldPush`/`TestIsInCall` at `presence_test.go:52,115`), and it covers the suppression matrix properly — mute skip (`:243`), restricted-window (`:261`), priority-contact pierce over mute *and* over presence (`:1497,:1528`), DND/presenting (`:1559`), settings error fail-open (`:1457`), partial-map fail-open (`:1473`). NAK/permanent branches are covered too: malformed payload → `Permanent` (`:476`), parent-fetch NAK + terminal classification (`:509,:1894`), follower-lookup NAK (`:722`), oversized-batch permanent vs publish-failure transient (`emit_test.go:128,145`). Test independence is sound — the `isDND`/`isPresenting` package vars are swapped and restored via `t.Cleanup` (`presence_test.go:31-49`) and no test calls `t.Parallel()`. Integration tests are correctly shaped: `//go:build integration`, `TestMain` → `testutil.RunTests(m)` (`main_test.go:11`), containers from `pkg/testutil` with `FlushValkey` cleanup (`integration_test.go:29-31`), no inline `GenericContainer`.

### Recommendations
- `critical` — Extract `main()`'s message loop into a testable `handleDelivery(ctx, msg, handler)` (or `run(cfg, deps)`), then unit-test the migration Ack-drop (`main.go:366-373`) and the `jsretry.Settle` call site (`main.go:375`) with a fake `jetstream.Msg`. This alone moves the service past 60%.
- `critical` — Add a unit test for the invalidation goroutine's decode-failure Ack and queue-full `default:` drop (`main.go:304-325`) by extracting the loop body into a named function.
- `high` — Port `mongoThreadFollowers.Lookup`'s `ErrNoDocuments`, empty-`parentMessageID`, and error branches to unit tests against a stubbed collection surface, or narrow `ThreadFollowerLister`'s Mongo dependency so those three paths are reachable without Docker.
- `high` — Cover `mongoUserSettings.appendChunk`'s three error exits and the mid-loop shared-timeout partial-result case (`usersettings.go:100-113`) — this is the mute gate's fail-open guarantee.
- `medium` — Replace the embedded NATS server in `parent_fetcher_test.go` with a stub requester interface, matching `presenceRequester`'s pattern.
- `medium` — Add a wiring test asserting `UserSettingsEnabled=false` produces `noopUserSettings` and thus zero suppression (`main.go:231-233`).
- `low` — Add `//go:generate mockgen` for `MemberCache`/`ThreadFollowerLister`/`Emitter` or document the hand-fake choice in-service.

---

## 5. Maintainability — 3 / 5

Excellent WHY-oriented commenting and clean small-file decomposition per collaborator, undercut by a 252-line/CC≈30 `HandleMessage` and a 361-line `main()` that concentrate almost all of the service's decision logic in two functions.

### Findings
- `high` — `HandleMessage` is 252 lines with ~40 decision points (cyclomatic ≈30), spanning six unrelated stages: payload decode, cache invalidation, thread-parent resolution + retry-budget policy, member filtering, settings/presence gating, badge fan-out, push-event construction and batch emission — `notification-worker/handler.go:109-354`
  Adding one new suppression rule means editing the middle of a function whose local state (`followers`, `parentCreatedAt`, `parentSenderAccount`, `badgeAccounts`, `siteByAccount`, `candidates`, `accounts`, `survivors`, `emitErrs`) is eight live variables deep. `.golangci.yml` enables neither `funlen`, `gocyclo` nor `cyclop`, so nothing bounds this.

- `high` — `main()` is 361 lines and mixes config validation, Mongo/Valkey/NATS wiring, two independent JetStream consumers, an inline invalidation worker goroutine, and a nine-step shutdown — `notification-worker/main.go:81-438`
  The invalidation subsystem (consumer creation, decode loop, bounded channel, two of the shutdown steps) is a self-contained feature inlined into `main`; it has no unit test because it cannot be reached without a live NATS.

- `high` — the invalidation reader goroutine is untracked by any `WaitGroup`, unlike the main consumer loop which is deliberately counted (`main.go:337-342`) — `notification-worker/main.go:304-325`
  Shutdown step 3 calls `invalIter.Stop()` and step 4 immediately `close(invalCh)` (`main.go:417-420`). Nothing proves the reader has exited, so a goroutine sitting between `sonic.Unmarshal` and `invalCh <- evt.RoomID` sends on a closed channel and panics the process during drain.

- `medium` — five `HandlerDeps` fields carry nil-sentinel semantics, but `NewHandler` normalizes only two of them; the other three are nil-checked at scattered use sites — `notification-worker/handler.go:52-60`, `handler.go:100-105`, vs `handler.go:363`, `handler.go:465`, `handler.go:491`
  Two different conventions for "optional dependency" in one struct; a sixth optional dep will pick whichever the author noticed last.

- `medium` — every batching collaborator re-declares its own default, duplicating the `envDefault` in `config` — `handler.go:28` vs `main.go:56` (100), `handler.go:95` vs `main.go:55` (500), `presence.go:49,52` vs `main.go:63,64` (512/2s), `usersettings.go:79,82` vs `main.go:68,69` (512/2s)
  Two sources of truth per knob; CLAUDE.md's "declared once in the package that owns the thing it configures" principle is being violated locally.

- `medium` — `natsBadgeClient.Counts` and `historyParentFetcher.FetchParent` are the same five-step request/reply body (sonic marshal → `nc.Request` → `errcode.Parse` + `Code.Valid()` → sonic unmarshal → wrap) — `notification-worker/badge_client.go:42-60`, `notification-worker/parent_fetcher.go:69-89`
  `badge_client.go:29` admits it ("mirroring historyParentFetcher's shape"). A third RPC will copy it a third time.

- `low` — `bootstrapStreams` takes six positional parameters, four of them adjacent strings (`inputStream, inputSubject, outputStream, outputSubject`) — `notification-worker/bootstrap.go:25`
  Transposing any pair compiles cleanly and creates a stream on the wrong subject. CLAUDE.md's stated shape is `bootstrapStreams(ctx, js, siteID, enabled)`.

- `low` — `Vetoer` has exactly one production implementation, `noopVetoer`, wired unconditionally at `main.go:264`; the only real implementation is a test fake — `notification-worker/hook.go:12-19`, `handler_test.go:133-137`
  A per-recipient interface call inside the hot member loop (`handler.go:245`) that can never do anything.

- `nitpick` — `isDND`/`isPresenting` are package-level `var` function stubs that always return false, making rule 2 of `shouldPush` permanently inert — `notification-worker/presence.go:132-135`, `presence.go:160`

- `nitpick` — `handler_test.go` is 2083 lines / 69 top-level tests in one file, ~4× the file it tests — `notification-worker/handler_test.go`

### Recommendations
- `high` — Split `HandleMessage` into named stages in new files: `audience.go` (`selectAudience` → badge accounts, candidates, `siteByAccount`), `threadctx.go` (`resolveThreadContext` → followers + parent, owning `parentResolveExhausted`), `pushevent.go` (`buildPushEvent` + `emitBatches`). `HandleMessage` becomes a ~40-line orchestration that reads as the documented pipeline.
- `high` — Extract the invalidation subsystem from `main()` into `invalidator.go` with a `newRoomInvalidator(...)` returning a struct with `Start`/`Stop`; track its goroutine in a `WaitGroup` and join it *before* `close(invalCh)`, closing the send-on-closed-channel window.
- `medium` — Enable `funlen` (say 80) and `cyclop`/`gocyclo` (say 15) in `.golangci.yml` so the next 250-line handler is rejected at lint time rather than at audit time.
- `medium` — Make every optional `HandlerDeps` field normalized in `NewHandler` (nil `RoomMeta`/`MentionNames`/`BadgeClient` → explicit noop types, as `Settings` already does), then delete the three scattered nil checks.
- `medium` — Hoist the shared defaults into single constants consumed by both the `envDefault` tag site and the constructor, or drop the constructor fallbacks entirely and rely on config validation.
- `low` — Extract the shared RPC body into one `requestJSON[Req, Resp]` helper in the service; `badge_client.go` and `parent_fetcher.go` each shrink to a subject + type pair.
- `low` — Change `bootstrapStreams` to take a small struct (`streamSpec{name, subject}` pair) instead of four positional strings, and delete `Vetoer`/`noopVetoer` until a real veto exists.

---

## 6. Integration — 3 / 5

Subject/stream/event plumbing is disciplined (all subjects from `pkg/subject`/`stream.Resolve`, `Timestamp` correctly stamped at the publish site, typed `pkg/model` contracts on every RPC), but the federated user-settings replica it gates push on has no backfill path, and its dev bootstrap narrows a stream three other services share.

### Findings
- `high` — `bootstrapStreams` creates `MESSAGES-CANONICAL-{site}` with `Subjects: [chat.msg.canonical.{site}.created]` only, while `pkg/stream.MessagesCanonical` (and `message-gatekeeper`) declare the `.>` wildcard — `notification-worker/main.go:195`, `notification-worker/bootstrap.go:29-36`, `pkg/stream/stream.go:22-27`.
  `CreateOrUpdateStream` is last-writer-wins, and `deploy/{user,bot}/docker-compose.yml:45/49` default `BOOTSTRAP_STREAMS=true`, so booting this worker after the gatekeeper strips `.updated`/`.deleted` from the stream — subjects history-service actively publishes (`history-service/internal/service/messages.go:561,641`). CLAUDE.md requires the helper to set `Name + Subjects` **from `pkg/stream.<Stream>(siteID)`**; it passes a hand-picked subject instead.
- `high` — the settings replica this worker reads has no creation or backfill path, so a mute silently fails open at remote sites. `inbox-worker`'s apply is a non-upsert `UpdateOne` ("a missing user is a silent no-op") — `inbox-worker/main.go:280-294` — and `model.UserAccountUpdated` (`pkg/model/event.go:212-222`) carries no `settings`, so the doc that later materializes the user has none. A `user_settings_updated` that arrives before the account upsert, or a user replicated to a site added after their last settings change, leaves `settings` absent forever; `notification-worker/usersettings.go:127-131` then resolves the zero `notifSettings` and pushes. Only the user's *next* settings mutation repairs it.
- `medium` — shutdown can panic on a closed channel. The member-event reader goroutine at `notification-worker/main.go:304-330` is tracked by no `WaitGroup`; shutdown step 3 calls `invalIter.Stop()` (`main.go:416`) and step 4 immediately `close(invalCh)` (`main.go:420`), but a message already returned by `Next()` is still in flight and executes `invalCh <- evt.RoomID` at `main.go:318`. `Stop()` only fails *future* `Next()` calls, so the send-after-close window is real. (CLAUDE.md: goroutines need a clear termination path; the documented cleanup order assumes the drain is awaited.)
- `medium` — `showPreviewsInNotifications` is validated, stored, watermarked and federated to every site, yet no service consumes it: the only reference outside `pkg/model` is the write at `user-service/mongorepo/users.go:133`. `handler.go:297` puts the full message content into `PushNotificationEvent.Body` unconditionally. The body is rendered by the OS notification shade, so this cannot be honored client-side — a user-facing privacy setting is inert end-to-end. `docs/client-api.md:4873` describes it without an "unenforced" caveat, unlike the explicit enforcement note on `muteAllNotifications` at `:4871`.
- `low` — `active: {$ne: false}` (`notification-worker/usersettings.go:118`) drops a deactivated account from the snapshot, and absence means the zero value, i.e. **push**. The filter agrees with user-service on "active", but the fail-open default inverts its intent for exactly the accounts most likely to want silence.
- `low` — the invalidation consumer is built from a hand-written `jetstream.ConsumerConfig` (`main.go:289-297`) rather than `stream.DurableConsumerDefaults(cfg.Consumer)`, so it inherits none of the repo's `AckWait`/`MaxDeliver`/`BackOff` derivation; the main consumer at `main.go:200-204` does it correctly.
- `low` — `bootstrap.go:37` sets `Compression: S2Compression` on the PUSH stream, beyond the `Name + Subjects` schema the ownership rule allows a service bootstrap helper to set; the ops-owned `pkg/stream.PushNotification` config carries no such field, so dev and prod streams differ.
- `nitpick` — verified clean: `PushNotificationEvent.Timestamp` is set at the publish site via `time.Now().UTC()`/`UnixMilli()` (`handler.go:296,309`); all four outbound subjects come from `pkg/subject` (`badge_client.go:52`, `parent_fetcher.go:83`, `presence.go:64`, `main.go:291`); remote envelopes decoded with `errcode.Parse`; no `chat.user.` handler is registered, so no `docs/client-api.md` obligation is outstanding.

### Recommendations
- `high` — pass `stream.MessagesCanonical(siteID).Subjects` (and `PushNotification(siteID)`) into `bootstrapStreams` instead of the single `.created` filter subject; keep the filter subject for the consumer only.
- `high` — close the settings-replica hole: either make `inbox-worker`'s `UpdateUserSettings` upsert under the watermark, or add `Settings` to `UserAccountUpdated` so the account snapshot materializes them. Add an integration test for "settings event before account upsert".
- `medium` — add `invalWG.Add(1)` around the reader goroutine and `Wait()` it before `close(invalCh)`, or drop the close and rely on `invalCancel()`.
- `medium` — either enforce `showPreviewsInNotifications` (suppress `Body`/`FileName` for opted-out recipients, which requires per-recipient bodies or a per-batch split) or mark it client-default-only in `docs/client-api.md`.
- `low` — decide explicitly what a deactivated account means here: drop it from `accounts` before the snapshot rather than letting absence mean push.
- `low` — build the invalidation consumer through `stream.DurableConsumerDefaults` and drop the ad-hoc `Compression` from `bootstrap.go`.

---

## 7. Performance — 4 / 5

Genuinely strong hot-path engineering — sonic + pretouch, narrowed-candidate batching, L1/L2 room caches, bounded worker semaphore, precise Mongo projections, clean `jsretry` discipline — held back by a shutdown channel-close race and three avoidable serial round trips per message.

### Findings
- `high` — the invalidation reader goroutine is untracked by any `WaitGroup`, yet shutdown closes the channel it sends into two steps later: `invalIter.Stop()` at `notification-worker/main.go:416`, then `close(invalCh)` at `notification-worker/main.go:420`. A message already returned by `invalIter.Next()` (`notification-worker/main.go:306`) that reaches `invalCh <- evt.RoomID` (`notification-worker/main.go:317`) after the close panics — send-on-closed-channel panics even from a `select` with a `default`. The panic aborts the remaining shutdown steps (`nc.Drain`, Mongo/Valkey disconnect), so in-flight messages die un-acked. Every other goroutine here is `wg`-tracked; this one is the exception.
- `medium` — three independent blocking round trips run strictly sequentially on the fan-out path: user settings (Mongo, 2s budget) `notification-worker/handler.go:270`, presence (NATS RPC, 2s) `notification-worker/handler.go:271`, badge counts (NATS RPC, 5s) `notification-worker/handler.go:286`. All three inputs (`accounts`, `badgeAccounts`, `siteByAccount`) are fully known when the member loop ends at `notification-worker/handler.go:268`, so worst-case per-message latency is ~9s of dependency wait where ~5s would do — each second held is an ack-pending slot against `MAX_ACK_PENDING`.
- `medium` — mention-name lookups go L1 → Mongo with no L2 and no circuit breaker: `userstore.NewCache(userstore.NewMongoStore(...))` at `notification-worker/main.go:243`. Every peer that reads the same shared user cache key wires `userstore.Resilient(col, breaker, valkey, cfg.UserL2.TTL, ...)` (`broadcast-worker/main.go:295`), and this service already holds both a `valkeyClient` and a `mongoutil.BreakerConfig`. Result: an L1 miss pays a Mongo round trip on an entry a peer has warm in Valkey, and a sick users collection is fenced for `roomsubcache` (`notification-worker/main.go:172`) but not here.
- `medium` — `mongoUserSettings.Snapshot` runs its chunks sequentially under one shared 2s deadline (`notification-worker/usersettings.go:96`, loop at `:105`). This is the gate that enforces mute. On a large `@all` fan-out (chunks of 512), a slow secondary means the *tail* chunks never execute and those recipients fail open — muted users get pushed. The comment at `:100-103` acknowledges it; presence solves the same shape concurrently (`notification-worker/presence.go:72`).
- `medium` — push batches are emitted one at a time with a synchronous JetStream publish awaiting each `PubAck`: loop at `notification-worker/handler.go:316`, `Emit` at `:330`, sync publish at `notification-worker/emit.go:88`. At the 100-recipient default, a 5 000-member `@all` room serializes 50 broker round trips inside one worker slot. Batches are independent, deterministic, and already `Nats-Msg-Id`-deduped, so nothing forces serial ordering.
- `low` — `isRestricted` takes `msg model.Message` by value (`notification-worker/handler.go:431`) and is called once per member (`:224`). `model.Message` is a ~24-field struct (`pkg/model/message.go:9-42`) and the function reads only `CreatedAt`. The `//nolint:gocritic` justification ("pointer indirection adds no benefit") inverts the tradeoff — for a hugeParam the pointer is strictly cheaper, and the sibling `Hook.Allow` already passes `&msg` (`:245`).
- `low` — `candidates` is allocated at `len(members)` capacity as a `[]roomsubcache.Member` (`notification-worker/handler.go:210`) but only `c.Account` is ever read from it (`:275`). In a large room this holds a second full copy of the member list for the life of the handler; `[]string` would do.
- `nitpick` — `ch := ch` at `notification-worker/presence.go:73` is a pre-Go-1.22 loop-variable capture workaround, dead under this repo's Go 1.25.

### Recommendations
- `high` — Add `invalReaderWG.Add(1)` around the goroutine at `main.go:304` and wait on it in the shutdown step *between* `invalIter.Stop()` and `close(invalCh)`; only close the channel once the sole sender has provably exited.
- `medium` — Wrap the settings, presence and badge lookups in one `errgroup.Group` immediately after `handler.go:268`. All three are fail-open, so no error plumbing changes; this cuts the dependency wait to the slowest single leg.
- `medium` — Switch `main.go:243` to `userstore.Resilient(db.Collection("users"), <breaker>, valkeyClient, cfg.UserL2.TTL, cfg.UserCacheSize, cfg.UserCacheTTL)` and mount `UserL2 userstore.TTLConfig` on the config struct, matching `message-gatekeeper`/`broadcast-worker`/`message-worker`. CLAUDE.md requires the shared L2 knob be mounted, not omitted.
- `medium` — Parallelize the settings chunk loop (`usersettings.go:105`) the way `bulkPresenceSource` does, so a shared deadline degrades uniformly instead of silently un-muting whichever recipients land in the last chunks.
- `medium` — Emit batches through a bounded `errgroup` (limit ~4–8) at `handler.go:316`; keep the per-batch error aggregation and permanence check unchanged.
- `low` — Change `isRestricted` to take `msg *model.Message` and drop the `nolint`; change `candidates` to `[]string` of accounts.
- `low` — Add a unit test that stops the invalidation iterator while a message is mid-flight, to pin the close-ordering fix (service coverage is 59.0%; `main.go`'s shutdown path is entirely uncovered).
