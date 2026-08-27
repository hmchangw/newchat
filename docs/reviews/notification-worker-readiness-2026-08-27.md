# Production Readiness Review — `notification-worker`

| | |
|---|---|
| **Service** | `notification-worker` |
| **Date** | 2026-08-27 |
| **Branch** | `claude/notification-worker-prod-ready-bupzgf` |
| **Overall score** | **2.7 / 5** (mean of six dimensions) |
| **Method** | Six independent expert passes (code quality, architecture, test coverage, maintainability, integration, performance), findings cross-verified against the source |

## Executive summary

`notification-worker` is a well-crafted service carrying a small number of genuinely
serious defects. The craftsmanship is visible and consistent: every NATS subject is
built through `pkg/subject` (no raw `fmt.Sprintf` anywhere), the primary consumer
follows the `Messages()` + semaphore pattern exactly, ack discipline runs through
`jsretry.Settle` with `errcode.Permanent` for poison messages, Mongo reads carry
precise projections and batch `$in` fetches, and `HandleMessage` — the actual business
logic — sits at 97.4% unit coverage with strong fail-open error-path tests. The
integration suite is textbook-compliant with CLAUDE.md Section 4.

What holds it back is concentrated in three places. **First, a shutdown race**: the
cache-invalidation reader goroutine (`main.go:378-399`) is tracked by no `WaitGroup`,
and shutdown closes the channel it sends on one step after stopping its iterator —
a send on a closed channel panics the pod. Four of the six experts found this
independently. **Second, a bootstrap defect**: `main.go:268` passes the `.created`
leaf subject as the *stream's* subject set, so a dev boot with `BOOTSTRAP_STREAMS=true`
narrows `MESSAGES-CANONICAL-{site}` and strips `.edited/.deleted/.reacted/.pinned`
from every other publisher, last-writer-wins. **Third, the hot path serializes work
that is independent**: settings, presence, and badge lookups run back-to-back
(`handler.go:243-259`) for ~13 s of worst-case serial wall time against a 30 s
`AckWait`, and the badge RPC is the one fan-out call that is never chunked, so a
5 000-member room becomes a single 5 000-account request per message.

Separately, the service deviates from the mandated per-service layout — there is no
`store.go` / `store_mongo.go` / `//go:generate mockgen`, and a full Mongo store
implementation lives inside `main.go` — which is both a convention violation and the
direct cause of the coverage number below.

**Coverage is 55.6%, against a repo floor of 80%**, which floors that dimension at 1
per CLAUDE.md Section 4. This number needs an honest caveat: `func main()` alone is
184 statements (27.5% of the package) of untested wiring, and Docker was unavailable
in the audit environment so the `//go:build integration` suite — which does exercise
the uncovered Mongo adapters — could not run. Excluding `main()`, coverage is 76.9%.
The floor is still missed, and the fix (extracting the store out of `main()`) is the
same fix the architecture and maintainability dimensions independently call for.

Nothing here is unshippable in the sense of "wrong notifications get delivered". The
shutdown race and the bootstrap narrowing are the two items that would bite in
production and both are small, local fixes.

## Dimension scores

| # | Dimension | Score |
|---|---|---|
| 1 | Code quality | 3 / 5 |
| 2 | Architecture | 3 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 3 / 5 |
| | **Overall** | **2.7 / 5** |

## Findings by severity

Counts are **deduplicated** across dimensions — the shutdown race was reported by four
experts and the missing `store.go` by three; each is counted once, at the highest
severity any expert assigned it.

| Severity | Count |
|---|---|
| `critical` | 1 |
| `high` | 10 |
| `medium` | 24 |
| `low` | 13 |
| `nitpick` | 9 |
| **Total unique** | **57** |

### Verification notes

- Coverage independently re-run and confirmed: **55.6%** total, `HandleMessage` 97.4%.
- `gosec` runs clean (exit 0, no medium+ findings). `govulncheck` and `semgrep` could
  **not** be executed in this environment (`vuln.go.dev` blocked by the proxy; `make tools`
  aborts on a `pipx`/`uv` version conflict). Two of three blocking SAST gates are
  therefore unverified here, not passed — see Chapter 2.
- `make test SERVICE=notification-worker` passes with `-race`.
- `make generate` produces no diff — mocks are not stale. This service uses hand-written
  stubs rather than mockgen, and has no `//go:generate` directives.

---

# Chapter 2 — Code quality

**Score: 3 / 5**

Precise error wrapping, correct `errcode` tiering (`Permanent` in a worker, `Parse` for
remote envelopes), consumer-defined interfaces, and thoughtfully documented fail-open
behaviour. Held back by one shutdown-crash defect, one latent correctness bug, a layout
deviation, and logging that drops trace correlation.

## Findings

### `high` — Send on a closed channel during shutdown

The cache-invalidation goroutine at `notification-worker/main.go:378-399` is tracked by
no `WaitGroup`. Shutdown step 3 (`main.go:490`) calls `invalIter.Stop()`; step 4
(`main.go:494`) immediately runs `close(invalCh)`. `Stop()` is asynchronous, so a
goroutine already past `invalIter.Next()` will execute `invalCh <- evt.RoomID`
(`main.go:392`) on a closed channel and panic the process. The main consumer loop is
deliberately `wg`-tracked for exactly this hazard (`main.go:411-416`); this loop is not.
Violates CLAUDE.md Section 3 ("never launch goroutines without a clear termination path").

*Verified directly against the source; independently reported by four of six experts.*

### `medium` — Two of three SAST gates unverifiable in this environment

`gosec` ran clean (exit 0, no medium+ findings). `govulncheck` failed with
`Get "https://vuln.go.dev/index/modules.json.gz": Forbidden` (agent proxy). `semgrep`
could not be installed — `make tools` aborts with `pipx needs uv>=0.9.17, but
/root/.local/bin/uv reports 0.8.17`. SAST is a blocking CI gate per CLAUDE.md Section 5,
so this must be re-run somewhere with network and working tooling before the gate can be
called green. **No medium+ SAST finding touching `notification-worker/` was detected by
the one scanner that did run.**

### `medium` — Mention matching is case-sensitive against lowercased input

`pkg/mention/mention.go:47` lowercases every parsed account. `handler.go:199` then indexes
`mentionedAccounts[m.Account]` using the raw `subscriptions.u.account` value loaded at
`main.go:107`, with no normalization. Any stored account containing an uppercase character
silently loses its mention — no push in large rooms (`handler.go:228`) and no delivery on a
thread-only reply (`handler.go:209`). `mentionnames.go:72` gets this right and lowercases
its map keys; the handler does not. Latent — it only fires if account data is not already
uniformly lowercase.

### `medium` — Log-and-return double-logs

`handler.go:302` logs the emit failure and `handler.go:309` returns it; `main.go:449` then
passes it to `jsretry.Settle`, which logs it again at `pkg/jsretry/jsretry.go:122`.
CLAUDE.md Section 6: "Never log AND return."

### `medium` — Mandated per-service layout not followed

No `store.go`, no `store_mongo.go`, no `//go:generate mockgen`, no `mock_store_test.go`.
Mongo access is scattered across `main.go:83-178` (`mongoMemberLoader`, `fillHomeSites`),
`threads.go:25-55`, and `usersettings.go:71-145`. Siblings comply
(`broadcast-worker/store.go`, `message-worker/store.go`).

### `medium` — Trace correlation lost in most log lines

Non-`Context` `slog` calls at `handler.go:220,302,346,456`; `members.go:46,62,80`;
`presence.go:76,83,87,94`; `main.go:386,394,441,443` — with `request_id` hand-threaded as a
field. Only `handler.go:438` uses `WarnContext`. This defeats the o11y SDK's
trace-correlated logging described in CLAUDE.md Section 1.

### `medium` — `HandleMessage` is a 220-line function

`handler.go:93-313` performs decode, cache invalidation, thread resolution, three filter
passes, settings/presence/badge fan-out, and batched emit in one body.

### `low` — Bare error returns

`emit.go:86` returns `err` unwrapped; `members.go:73` returns `ctx.Err()` unwrapped.
CLAUDE.md Section 3 requires `fmt.Errorf("...: %w", err)` with context.

### `low` — Inconsistent `errcode.Parse` guard

`badge_client.go:53` and `parent_fetcher.go:82` gate on `ee.Code.Valid()`; `presence.go:86`
does not, so any reply carrying an `error` field is misread as a failure.

### `low` — Secrets carry `envDefault` instead of `required`

`main.go:40` `NatsCredsFile`, `main.go:45` `MongoPassword`, `main.go:57` `ValkeyPassword`
all default to `""`. CLAUDE.md Section 6: "never default secrets or connection strings."

### `low` — Unchecked type assertion

`members.go:71` asserts `res.Val.([]roomsubcache.Member)` without the comma-ok form.

### `low` — Production test seams

`presence.go:132-135` declares `isDND` / `isPresenting` as package-level mutable func vars
that always return `false`, swapped by `presence_test.go:34-49`. Shared mutable global
state in production code; blocks `t.Parallel()`.

### `nitpick` — Mixed log-key casing

`messageId`/`roomId`/`siteId` alongside `request_id` in the same call
(`handler.go:303-304,347`; `main.go:465-466`).

### `nitpick` — Pre-Go-1.22 idioms

`ch := ch` loop-variable copy (`presence.go:70`), `sort.Strings` (`handler.go:8,255`),
manual clamps where `min()` fits (`handler.go:288`, `presence.go:116`). `main.go:420`
records `LoopFailed` on the normal shutdown-induced iterator-closed error.

## Recommendations

1. **`high`** — Track the invalidation goroutine with a `sync.WaitGroup` and join it *before*
   `close(invalCh)`; or drop the close and let `invalCancel()` terminate the drain.
2. **`medium`** — Normalize `Member.Account` to lowercase in `mongoMemberLoader.Load`, or
   lowercase at the `handler.go:199` lookup, and add a table case pinning mixed-case mentions.
3. **`medium`** — Switch `main.go:449` to `jsretry.SettleQuiet`, or remove the `slog.Error` at
   `handler.go:302`.
4. **`medium`** — Extract `store.go` (interface + `//go:generate mockgen`) and `store_mongo.go`;
   move `mongoMemberLoader` out of `main.go`.
5. **`medium`** — Convert request-path `slog.X` calls to `slog.XContext`, drop the hand-threaded
   `request_id`, and settle on one key casing.
6. **`medium`** — Split `HandleMessage` into `resolveThreadContext`, `selectRecipients`, and
   `emitBatches`.
7. **`low`** — Wrap the bare returns, add the comma-ok assertion, mark secrets `required`, and
   re-run `make sast` where `vuln.go.dev` and `pipx`/`uv` are reachable.

---

# Chapter 3 — Architecture

**Score: 3 / 5**

Solid interface hygiene and a correct primary consumer, undercut by a 521-line `main.go`
that absorbs store code and a whole second consumer, a missing `store.go`/mockgen layer,
and two concrete bootstrap/shutdown defects.

## What is right, and worth preserving

- Every subject flows through `pkg/subject` — no raw `fmt.Sprintf` of a subject anywhere
  in the service.
- Consumer-defined, minimal interfaces throughout (`handler.go:35-43`, `emit.go:19-26`,
  `badge_client.go:29`).
- Primary lane uses `Messages()` + `MAX_WORKERS` semaphore + `PullMaxMessages(2*MaxWorkers)`
  with no pattern mixing (`main.go:401-453`).
- `jsretry.Settle` rather than a bare `Nak()` (`main.go:449`).
- Correctly does not touch INBOX or OUTBOX — nothing this service emits is order-sensitive
  federation, so `inbox-worker`/`outbox-worker` ownership is respected.

## Findings

### `high` — `bootstrapStreams` narrows the input stream's subject set

`main.go:268` passes `wiring.CanonicalCreated` (`chat.msg.canonical.{site}.created`) as the
*stream's* subject list into `bootstrapStreams(ctx, js, inputStream, inputSubject, …)`
(`bootstrap.go:25-32`). The stream's real binding is `chat.msg.canonical.{site}.>`
(`pkg/stream/stream.go:22-27`), which `message-gatekeeper` provisions from
`stream.MessagesCanonical(siteID).Subjects` (`message-gatekeeper/bootstrap.go:46,54-59`).
Because `CreateOrUpdateStream` narrows an existing stream, with `BOOTSTRAP_STREAMS=true`
(the compose default) whichever service boots last wins: notification-worker silently strips
`.edited/.deleted/.reacted/.pinned/.unpinned` and every other canonical publisher then fails.
`wiring.CanonicalWildcard` exists for exactly this and is what `broadcast-worker/main.go:251`
passes.

*Verified directly: notification-worker is the only one of five canonical-stream bootstrappers
that passes a filter leaf rather than the full subject set or `.>` wildcard.*

### `high` — Shutdown can panic on send-to-closed-channel

The invalidation reader goroutine (`main.go:378-399`) sends on `invalCh` at `main.go:392` but
is tracked by no `WaitGroup`. Shutdown step 3 calls `invalIter.Stop()` (`main.go:490`) and
returns immediately; step 4 then runs `close(invalCh)` (`main.go:494`) with no happens-before
against a goroutine parked between `iter.Next()` returning and the select send. `invalWG`
exists but tracks only the *consumer* side (`main.go:353-358`). Contrast the main loop, which
is explicitly counted in `wg` for this reason (`main.go:411-416`).

### `high` — No `store.go`, no `//go:generate mockgen`, Mongo store implementation in `main.go`

`mongoMemberLoader` plus `fillHomeSites` is roughly 100 lines of bson/projection/cursor code
inside `main.go:77-178`. The rest of the data layer is scattered across `threads.go:22-73`
(`mongoThreadFollowers`) and `usersettings.go:67-145` (`mongoUserSettings`). Siblings
consolidate into `store.go` + `store_mongo.go` with generated mocks
(`broadcast-worker/store.go:11-14`, `message-worker/store.go:11-12`); this service has neither
file and hand-rolls every test double (`handler_test.go:21-99`). `integration_test.go:192,252`
exercises production store code that lives in `main.go`.

### `medium` — Second consumer bypasses `pkg/stream` consumer settings

`main.go:363-368` builds a raw `jetstream.ConsumerConfig` with only `Durable`, `FilterSubject`,
`AckPolicy`, and `DeliverPolicy` — none of `AckWait`, `MaxDeliver`, `MaxAckPending`, or the
derived `BackOff` from `stream.DurableConsumerDefaults` (`pkg/stream/consumer.go:51-61`), which
`buildConsumerConfig` correctly uses for the primary consumer (`main.go:516-521`). It inherits
server defaults (unlimited `MaxDeliver`, no backoff) and is invisible to `CONSUMER_*` tuning.

### `medium` — Message-handling logic in `main.go`

The invalidation lane's decode/filter/enqueue loop (`main.go:378-399`) is handler work,
unreachable from `handler_test.go`, and — unlike the primary path (`main.go:436`) — runs
outside `jobguard.Run`, so a panic there kills the pod rather than being contained.

### `medium` — Bootstrap helper sets more than `Name + Subjects`

`bootstrap.go:33-38` sets `Compression: jetstream.S2Compression`, the only such call in the
repo, against the CLAUDE.md rule that the helper sets ONLY the stream's schema. A dev boot
flips compression on a stream ops otherwise owns.

### `low` — `HandleMessage` mixes too many stages

`handler.go:93-313` — the extractable stages (`buildAudience`, `buildBatches`) would be
independently testable.

### `low` — Invalidation lane ignores `MODE` wiring

`stream.Rooms(cfg.SiteID)` is hardcoded (`main.go:362`) for both user and bot pipelines, while
every other binding flows through `stream.Resolve` (`main.go:266`). `ROOMS-{site}` is also never
verified in the production (bootstrap-disabled) path.

### `low` — Connection strings carry `envDefault`

`main.go:39,42` default `NATS_URL` and `MONGO_URI`; CLAUDE.md Section 6 says mark them
`required`, as `message-worker/main.go:37,47` does. `VALKEY_ADDRS` is validated imperatively at
`main.go:188` instead of via the tag.

### `nitpick` — Test seams in production code

`isDND`/`isPresenting` are mutable package vars (`presence.go:132-135`) existing only to be
swapped by tests (`presence_test.go:34-36`).

## Recommendations

1. **`high`** — Pass `wiring.CanonicalStream.Subjects` / `wiring.PushStream.Subjects` into
   `bootstrapStreams` instead of the filter leaves, and assert the exact `Subjects` (not just
   the name) in `bootstrap_test.go`. Better still: only *verify* the canonical stream, since
   `message-gatekeeper` owns it.
2. **`high`** — Add a `sync.WaitGroup` around the invalidation reader goroutine and wait on it
   in the step following `invalIter.Stop()`, before `close(invalCh)`.
3. **`high`** — Introduce `store.go` (`NotificationStore` with `ListMembers`, `FindHomeSites`,
   `LookupThreadFollowers`, `SnapshotSettings`) + `store_mongo.go`, move `mongoMemberLoader` out
   of `main.go`, and add a `//go:generate mockgen` directive.
4. **`medium`** — Move the invalidation consumer into its own unit, build its config from
   `stream.DurableConsumerDefaults`, and wrap the loop in `jobguard.Run`.
5. **`medium`** — Drop `Compression` from `bootstrap.go`, or hoist it into
   `pkg/stream.PushNotification` so app code stays schema-only.
6. **`low`** — Split `HandleMessage` into audience-building, gating, and batch-emitting helpers.
7. **`low`** — Mark `NATS_URL`/`MONGO_URI`/`VALKEY_ADDRS` `required` and route the ROOMS binding
   through `stream.Resolve`.

---

# Chapter 4 — Test coverage

**Score: 1 / 5**

Scored mechanically per CLAUDE.md Section 4: total coverage 55.6% is below 60%, which
mandates a `critical` finding and floors the dimension at 1.

**This score materially understates the suite's substantive quality**, and the report would
be misleading without saying so plainly. On qualitative merit — table-driven structure, test
independence, error-path depth, integration-suite compliance — this is a 4/5 suite. The
number is dragged down almost entirely by untested wiring in `func main()`, and the fix is
the same store-extraction that Chapters 3 and 5 call for on independent grounds.

## Measurements

`make test SERVICE=notification-worker` → **PASS** (`go test -race ./notification-worker/...`,
ok, 3.15s). No failures.

**Total: 55.6% of statements** (668 statements, 296 uncovered). Independently re-run and
confirmed.

`make generate` → **no diff**. Mocks are not stale. This service has no `//go:generate`
directives at all; it uses hand-written stubs rather than mockgen. Working tree verified
identical to its pre-audit state afterward.

### Worst-covered functions

| Function | Coverage | Statements |
|---|---|---|
| `main.go:180 main` | 0.0% | 184 |
| `main.go:88 (*mongoMemberLoader).Load` | 0.0% | ~26 |
| `main.go:145 fillHomeSites` | 0.0% | ~14 |
| `threads.go:33 (*mongoThreadFollowers).Lookup` | 0.0% | 14 |
| `usersettings.go:115 appendChunk` | 0.0% | ~14 |
| `threads.go:29 newMongoThreadFollowers` | 0.0% | 1 |
| `presence.go:175 (*natsPresenceRequester).Request` | 0.0% | 4 |
| `emit.go:82 (*jsPublisher).PublishMsg` | 0.0% | 3 |
| `pretouch.go:19 pretouchJSON` | 0.0% | 1 |
| `usersettings.go:91 (*mongoUserSettings).Snapshot` | 30.0% | — |
| `members.go:78 Invalidate` | 50.0% | — |
| `presence.go:47 newBulkPresenceSource` | 60.0% | — |
| `handler.go:403 shortRoomType` | 75.0% | — |

### Coverage decomposition

Context, not an excuse:

- `func main()` alone is 184 statements — **27.5% of the entire package** — and is untestable
  as written. Excluding it, coverage is **76.9%**.
- **Docker was unavailable in the audit environment**, so the `//go:build integration` suite
  could not run. It demonstrably covers `Lookup`, `appendChunk`, `Load`, and `fillHomeSites`
  (~77 further statements), which would put non-`main()` coverage near **93%**.
- `handler.go` — the actual business logic — is at **97.4%** on unit tests alone.

The 80% floor is still missed on the measured figure, and the remedy is structural rather
than a matter of writing more tests against `main()`.

## Findings

### `critical` — Coverage below repo minimum 80%, currently 55.6%

Driven by `notification-worker/main.go:180`, an untestable 184-statement `main()` that embeds
real logic: the invalidation goroutine's decode / drop-on-full / ack path, the migration-header
skip, the semaphore worker loop, and nine-step shutdown ordering.

### `high` — `GetMembers` error path has no test

`handler.go:126`: the `GetMembers` error → NAK branch is entirely uncovered.
`stubMembers.GetMembers` (`handler_test.go:39`) unconditionally returns `nil`, so no fixture
can drive the failure. This is the redeliver-vs-drop decision for the service's hottest
dependency.

### `medium` — Empty-member-list early return untested

`handler.go:129`. CLAUDE.md Section 4 names empty collections explicitly as a required edge case.

### `medium` — `isRestricted` nil-parent branch uncovered

`handler.go:395`: the `parentCreatedAt == nil` branch ("suppress, not leak") is the fail-closed
guard against leaking restricted history to a later-joining member, and nothing exercises it.

### `medium` — Several fail-open paths uncovered

`presence.go:94` (malformed presence reply / `sonic.Unmarshal` error) and `members.go:58/61/79`
(loader error inside singleflight, cache-`Set` failure, `Invalidate` failure) are all uncovered.

### `low` — Embedded NATS server inside the unit suite

`parent_fetcher_test.go:25` `startTestNATS` boots an in-process `nats-server`, also used by
`badge_client_test.go`. CLAUDE.md Section 4 says "Never connect to real … NATS … in unit tests."
Defensible (embedded, no container) but it binds a port and slows the default target.

### `low` — Test-only mutable globals

`presence.go:127-135`: `isDND`/`isPresenting` are package-level vars existing purely as a test
seam, mutated by `presence_test.go:33-49`. Restoration via `t.Cleanup` is correct and no test
calls `t.Parallel()`, so it is safe today — but it becomes a data race the moment anyone
parallelizes.

### `low` — Minor uncovered branches

`handler.go:407` (`RoomTypeDiscussion` → `"p"`) and `handler.go:109` (non-`created` event
backstop).

### `nitpick` — `pretouchJSON` left at 0%

`pretouch_test.go:14` iterates `pretouchTypes` directly instead of calling `pretouchJSON()`.

### `nitpick` — Wall-clock dependence in a test

`members_test.go:118` uses a 50 ms `fakeLoader.delay` to widen the singleflight race window.
The synchronization itself is correct (`sync.WaitGroup`), so this is not a Section 3 violation,
but it is timing-dependent.

## Strengths worth recording

122 test functions; heavy table-driven use with descriptive subtest names (`TestIsNotifiable`,
`TestHandle_SystemMessageProducesNoPush`, `TestBuildConsumerConfig`); every stub is
per-test-constructed with mutex-guarded recorders — no shared fixtures, no order dependence;
`-race` on both targets. The integration suite is textbook-compliant: `//go:build integration`,
`package main`, `TestMain(m) → testutil.RunTests`, **all** containers from `pkg/testutil` with
`FlushValkey` cleanup, and zero inline `testcontainers.GenericContainer`. Error-path and
fail-open coverage is strong (`TestHandle_SettingsErrorFailsOpen`,
`TestBulkPresence_ErrorResponseLoggedAndFailOpen`, `TestHandle_MentionPartialResolutionOnError`,
`TestCachedMemberLookup_LeaderCancelDoesNotPoisonWaiters`). TDD signals are genuine — production
comments cite the tests that pin their behaviour by name (`handler.go:229` references
`TestHandle_SettingsFetchedOnlyForSurvivingCandidates`).

## Recommendations

1. **`high`** — Extract `main()`'s logic into testable units: `runInvalidationLoop(ctx, iter, ch)`,
   `newInvalidationWorker`, and a `wireHandler(cfg, deps) *Handler`. Leaving `main()` as ~30 lines
   of `os.Exit` glue moves total coverage from 55.6% to roughly 90% *and* puts the drop-on-full
   invalidation queue and migration-skip under test.
2. **`high`** — Add an `err` field to `stubMembers` (`handler_test.go:32`) and a
   `TestHandle_MemberFetchError_NAKs` asserting `HandleMessage` returns a non-`Permanent` error,
   mirroring the existing `TestHandle_ThreadFollowersError_NAKs`.
3. **`medium`** — Add `TestHandle_EmptyMemberList_NoPush` and a direct table test for `isRestricted`
   covering `parentCreatedAt == nil`, equal, and after boundaries.
4. **`medium`** — Enforce the floor in CI with a per-package `go tool cover -func` gate, configured
   to exclude `main()` (or to track `handler.go`/`pkg` at 90%) so the metric measures logic rather
   than wiring.
5. **`low`** — Add `TestBulkPresence_MalformedReplyFailsOpen` via the existing `rawReply` hook, and
   a `newBulkPresenceSource` defaults test for `batchSize <= 0` / `timeout <= 0`.
6. **`low`** — Replace the `isDND`/`isPresenting` globals with a `presenceFlags` field on
   `HandlerDeps` (nil → inert), removing the seam from production code and unblocking `t.Parallel()`.
7. **`nitpick`** — Call `pretouchJSON()` from `TestPretouch_TypesCompile`, and pin
   `shortRoomType(RoomTypeDiscussion) == "p"`.

---

# Chapter 5 — Maintainability

**Score: 3 / 5**

Unusually well-commented code with strong tests and coherent deploy artifacts — but `main.go`
carries a Mongo store and two hand-rolled consume loops that the repo's own conventions and
`pkg/` helpers already solve, and `HandleMessage` is a 221-line pipeline with no seams.

## Findings

### `high` — `main.go` holds a full Mongo store; no `store.go`/`store_mongo.go`

`main.go:77-178` defines `mongoMemberLoader` + `fillHomeSites` (~100 lines of Find / projection /
decode). CLAUDE.md's per-service file organization mandates `store.go` (interface + mockgen) and
`store_mongo.go`. Both siblings comply (`broadcast-worker/store_mongo.go`,
`message-worker/store_mongo.go`); notification-worker is the only one of the three with neither
file. `integration_test.go:192,252` tests this store — production store code exercised out of
`main.go`.

### `high` — The consume loop reimplements `pkg/natsmetrics.Start`

`main.go:411-453` hand-rolls semaphore + `wg.Add` + `Track`/`Finish`/`LoopFailed`, and the comment
at `main.go:413-415` is a near-verbatim copy of the doc comment on `pkg/natsmetrics/loop.go:19-23`.
`message-gatekeeper/main.go:195` and `broadcast-worker/main.go:351` both call the shared helper.
Roughly 45 lines of drift-prone duplication.

*Verified: notification-worker builds `consumerMetrics` at `main.go:274` exactly as its siblings do,
then declines to use the shared loop.*

### `high` — `parent_fetcher.go` is a byte-identical copy of broadcast-worker's

`notification-worker/parent_fetcher.go:36-90` vs `broadcast-worker/parent_fetcher.go:21-76`. A
whitespace-and-comment-insensitive diff shows the two bodies are identical; they differ only in
where `ParentMessageInfo`/`ParentFetcher` are declared (broadcast-worker puts them in
`handler.go:60-72`). This is a history-service RPC client — it belongs in `pkg/`.

### `high` — `HandleMessage` is a 221-line function

`handler.go:93-313` performs decode, event filter, a cache-invalidation side effect, the
notifiability gate, member load, mention parse, thread/parent resolution, a four-stage 48-line
filter loop (`:187-234`), settings+presence snapshot, survivor sort, badge fan-out, payload build,
and batched emit with error aggregation. No complexity linter guards this: `.golangci.yml` enables
none of `gocyclo`, `cyclop`, `funlen`, or `gocognit`.

### `medium` — `main()` is 332 lines with three non-wiring subsystems

`main.go:180-512`. Beyond wiring it contains four feature-toggle branches with degradation policy
(`:289-330`) and an entire member-cache invalidation subsystem — bounded queue, drop policy, a second
JetStream consumer, and an inline decode/dispatch loop (`main.go:348-399`) — plus four of the nine
shutdown steps that exist only to drain it (`:489-505`). That loop is unmetered and un-`jobguard`ed,
unlike the primary one.

### `medium` — Decomposition is misfiled at the seam that matters most

`shouldPush` (`presence.go:153-167`) is the combined settings+presence gate, but `notifSettings` and
`isPriority` live in `usersettings.go:30-45`. Adding one user-setting touches `usersettings.go`
(struct, projection `:58-65`, `resolveNotifSettings:149`), `presence.go:153`, and `pkg/model` — the
decision logic sits in the file named after only one of its two inputs. A `gate.go` would make the
pipeline stage legible.

### `medium` — Three copies of every default, with no shared constant

`LargeRoomThreshold` defaults to `500` at `main.go:51` and again at `handler.go:79`; presence
batch/timeout `512`/`2s` at `main.go:59-60` and `presence.go:49-53`; user-settings `512`/`2s` at
`main.go:64-65` and `usersettings.go:79-83`. `RecipientBatchSize` does it correctly
(`handler.go:27` `defaultRecipientBatchSize`), so the inconsistency is internal to the service.

### `medium` — Test-only mutable globals in production code

`presence.go:132-135` declares `isDND`/`isPresenting` as package-level `var` closures that always
return `false`, reassigned by `presence_test.go:34-36,45-49`. `shouldPush:160` is therefore
permanently inert. This is shared mutable state across tests (CLAUDE.md Section 4) and a test helper
living in production code (Section 4 again).

### `low` — Magic numbers that should be config

`main.go:350` `make(chan string, 256)`; `main.go:373` `PullMaxMessages(64)`; `badge_client.go:19`
`badgeFetchTimeout = 5s` (a cross-site RPC, untunable); `members.go:18` `memberFetchTimeout = 10s`.
Sibling knobs (`PRESENCE_RPC_TIMEOUT`, `USER_SETTINGS_TIMEOUT`) *are* env-driven, so no rule
distinguishes them.

### `low` — Speculative generality

`Vetoer` (`hook.go:12`) has exactly one production implementation, `noopVetoer`, wired at
`main.go:338`; only tests supply a real one. `notifyKind` (`nats_metrics.go:13-20`) is a two-value
enum where `notifyKindUnknown` is only ever an unreachable map-miss fallback (`nats_metrics.go:73`).
`Handler` stores the same pointer twice (`handler.go:66` `metrics` and `deps.Metrics`).

### `nitpick` — Unresolvable comment references

`members.go:17` "See the design spec.", `usersettings.go:90` "See the spec", `presence.go:150` "the
issue's stated priority order", `emit.go:54` "contract doc § Dedup" — none name a path, while
`main.go:81` correctly cites `docs/notification-worker-downstream-contracts.md §3`.

### `nitpick` — Identical Dockerfiles

`deploy/user/Dockerfile` and `deploy/bot/Dockerfile` are byte-identical; only compose and pipeline
differ. Otherwise deploy is coherent and matches the dual-mode pattern of `broadcast-worker/deploy`
and `push-notification-service/deploy`.

## Recommendations

1. **`high`** — Extract `store.go` (interface + `//go:generate mockgen`) and `store_mongo.go` from
   `main.go:77-178`, folding in `threads.go` and `usersettings.go`'s Mongo readers. Restores the
   mandated layout and drops `main.go` by ~100 lines.
2. **`high`** — Replace `main.go:411-453` with `natsmetrics.Start(...)`, mirroring
   `broadcast-worker/main.go:351` including its `broadcastProcessor`/`guardedProcessor` split.
3. **`high`** — Promote `parent_fetcher.go` to `pkg/historyclient` and delete both copies.
4. **`medium`** — Split `HandleMessage` (`handler.go:93-313`) along its own comment boundaries:
   `resolveThreadContext`, `selectAudiences` (the `:187-234` loop), `emitBatches` (`:264-310`).
5. **`medium`** — Move the invalidation subsystem (`main.go:348-399` plus shutdown steps `:489-505`)
   into `invalidator.go` as a `newRoomInvalidator(...).Run/Stop` type, with the same
   `jobguard`/metrics treatment as the primary loop.
6. **`medium`** — Add `gocyclo`/`funlen` to `.golangci.yml` so this does not recur; hoist duplicated
   defaults into per-file constants referenced by both the `main.go` env tags and the constructors.
7. **`low`** — Delete `isDND`/`isPresenting` (`presence.go:132-135`) and the inert branch at `:160`
   until presence ships them; move `chunkStrings` (`presence.go:109`, duplicated at
   `user-service/service/threadunread.go:179`) into `pkg/`.
