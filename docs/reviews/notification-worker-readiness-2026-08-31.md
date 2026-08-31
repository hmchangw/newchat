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
