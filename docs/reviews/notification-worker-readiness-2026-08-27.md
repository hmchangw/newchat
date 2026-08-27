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

---

# Chapter 6 — Integration

**Score: 3 / 5**

A thoughtful service at its contract boundaries: every subject comes from `pkg/subject`, the hot
loop follows the `Messages()` + semaphore pattern correctly, `jsretry.Settle` + `errcode.Permanent`
discipline is exact, sonic is pretouched, and the cross-site badge/presence/history RPCs are
fail-open by design. The integration defects are concentrated in bootstrap wiring and shutdown.

## Verified clean

- No raw `fmt.Sprintf` subjects anywhere.
- No OUTBOX/INBOX participation — correct, since nothing this service emits is order-sensitive
  federation. `inbox-worker`'s sole INBOX ownership is respected.
- No Cassandra / `messages_by_room` access, so `MESSAGE_BUCKET_HOURS` does not apply.
- No room-key access, so `ROOM_KEY_RETIRED_TTL` does not apply.
- No `pkg/idgen` entity IDs minted, so there is no ad-hoc ID generation to flag.
- No client-facing handler (`chat.user.*` or HTTP), so no `docs/client-api.md` §3 schema table is owed.
- `roomsubcache` schema is versioned (`pkg/roomsubcache/roomsubcache.go:52`), so the `HomeSiteID`
  addition cannot misroute badge RPCs.
- Env vars in both `deploy/{user,bot}/docker-compose.yml` are a strict subset of the `config` struct,
  with matching defaults.

## Findings

### `high` — Bootstrap narrows MESSAGES-CANONICAL to the `.created` leaf

`main.go:268` passes `wiring.CanonicalCreated` as the *stream subject* into `bootstrapStreams`
(`bootstrap.go:25-32`). The stream's real binding is `chat.msg.canonical.{site}.>`
(`pkg/stream/stream.go:22-27`), as `message-gatekeeper` creates it
(`message-gatekeeper/bootstrap.go:54-59`). Since `CreateOrUpdateStream` narrows an existing stream,
whichever service boots last wins: with `BOOTSTRAP_STREAMS=true` (the compose default,
`deploy/user/docker-compose.yml:44`) notification-worker silently strips
`.edited/.deleted/.reacted/.pinned/.unpinned` and breaks every other canonical publisher.

### `high` — Shutdown can panic on send to a closed channel

The invalidation reader goroutine (`main.go:378-399`) is untracked by any `WaitGroup`. Shutdown step
3 calls `invalIter.Stop()` (`main.go:490-492`) and step 4 immediately runs `close(invalCh)`
(`main.go:494`). `Stop()` is asynchronous; a goroutine sitting between `invalIter.Next()` returning
and `invalCh <- evt.RoomID` (`main.go:392`) sends on a closed channel and crashes the pod. The main
consumer loop is correctly counted (`main.go:411-416`) — this one is not.

### `medium` — Invalidation consumer bypasses `stream.DurableConsumerDefaults`

`main.go:363-368` builds a raw `jetstream.ConsumerConfig` with only
`Durable`/`FilterSubject`/`AckPolicy`/`DeliverPolicy`. It inherits none of `CONSUMER_*` (`AckWait`,
`MaxDeliver`, `MaxAckPending`, derived `BackOff`) — so `MaxDeliver` is the server default (unlimited)
and there is no backoff at all. Every sibling, including `push-notification-service/main.go:120-123`,
routes through `DurableConsumerDefaults`.

### `medium` — `X-Request-ID` is not propagated on any outbound NATS path

`emit.go:48-54` hand-builds `&nats.Msg{Header: nats.Header{}}` and sets only `Content-Type` and
`Nats-Msg-Id`; `badge_client.go:47` and `parent_fetcher.go:75` call `nc.Request` with bare bytes. The
repo convention is `natsutil.NewMsg(ctx, subj, data)` (`pkg/natsutil/request_id.go:67-77`) —
notification-worker is the only service constructing a publish `nats.Msg` by hand. user-service and
history-service use lenient `RequestID()` middleware so nothing breaks functionally, but each remote
mints a fresh ID and the correlation chain dies at this hop, contradicting CLAUDE.md Section 3
(Request Logging & Tracing).

### `medium` — `bootstrap.go` sets non-schema stream config

`bootstrap.go:37` sets `Compression: jetstream.S2Compression` on PUSH-NOTIFICATION. CLAUDE.md's
stream-bootstrap-ownership rule says the helper sets "ONLY the stream's schema — Name + Subjects". It
is the only `Compression` in any service, repo-wide. A dev boot flips compression on a stream ops
otherwise owns.

### `medium` — Production path never verifies the output stream

`bootstrap.go:43-46` skips verifying PUSH-NOTIFICATION, justified in-comment as "async publish
surfaces errors per-publish" — but `jsPublisher.PublishMsg` (`emit.go:82-86`) is synchronous. A
missing push stream in production becomes per-message naks until `MaxDeliver` drops them, instead of
failing fast at startup the way `message-gatekeeper/bootstrap.go:64-71` does.

### `medium` — `DefaultBackoff` on a user-visible delivery path

`main.go:449` uses `jsretry.DefaultBackoff` (1s/5s/30s/2m/10m). The sibling fan-out worker uses
`LowLatencyBackoff` (`broadcast-worker/main.go:446`), which `pkg/jsretry` documents as the choice
"where the first retry must be near-immediate so a sub-second hiccup isn't user-visible". A blip on
the member lookup delays a push by at least 1 s, then 5 s.

### `medium` — `docs/client-api.md` drift on `@here`

`docs/client-api.md:6566` states that large rooms push "only to mentioned recipients (`@user`, `@all`,
`@here`)". `handler.go:135-136` states and implements the opposite — `mentionsAll` is `MentionAll`
only, and `mention.Parse` never sets it for `@here` (`pkg/mention/mention.go:47-51`), with an explicit
comment: "`@here` is deliberately NOT a push trigger."

*Verified directly against all three files.*

### `medium` — No sonic wire-compat test despite a map field on the wire

`model.PushNotificationEvent.UnreadCounts` is `map[string]int` (`pkg/model/push.go:17`), marshaled by
sonic at `emit.go:41` and decoded by `encoding/json` at `push-notification-service/handler.go:30`.
CLAUDE.md requires confirming byte-compatibility when a path "marshals `map` fields — see the sonic
wire-compat tests in `broadcast-worker`/`message-gatekeeper`". `broadcast-worker/sonic_wire_test.go`
exists; notification-worker has only `pretouch_test.go`.

### `medium` — `bootstrap_test.go` masks the production call

`bootstrap_test.go:85` passes `"chat.msg.canonical.test.>"` where `main.go:268` passes the `.created`
leaf. That mismatch is exactly why the `high` bootstrap finding above is invisible to CI.

### `low` — Dedup key assumes a determinism the pipeline does not provide

`handler.go:246` claims the sort yields "a deterministic account set across redeliveries", but batch
*membership* depends on the fail-open `Settings`/`Presence` snapshots (`handler.go:243-244`). If a
presence RPC fails on attempt 1 and succeeds on attempt 2, batch `b0`'s account set differs while
`Nats-Msg-Id` (`handler.go:295`) is unchanged — so the corrected batch is deduped away inside the
window. Sorting fixes ordering only, not membership.

### `nitpick` — Minor contract hygiene

`msg.Ack()` errors are discarded without the CLAUDE.md-required comment at `main.go:387` and
`main.go:397`, and the decode-failure ack-drop there skips the `errcode.Permanent` idiom used on the
main path (`handler.go:105`). `badgeFetchTimeout` (`badge_client.go:19`) is the only cross-site
timeout that is not env-tunable, unlike `PRESENCE_RPC_TIMEOUT`/`USER_SETTINGS_TIMEOUT`.

## Recommendations

1. **`high`** — Pass `wiring.CanonicalStream.Subjects[0]` (not `wiring.CanonicalCreated`) at
   `main.go:268`, and update `bootstrap_test.go:85` to assert the exact subject the production call
   site supplies. Consider dropping the canonical-stream create entirely and only verifying it, since
   `message-gatekeeper` owns it.
2. **`high`** — Track the invalidation reader in `invalWG` (or a second `WaitGroup`) and wait on it
   *before* `close(invalCh)`; alternatively, close the channel from inside that goroutine on
   `Next()` error.
3. **`medium`** — Build the invalidation consumer with `stream.DurableConsumerDefaults(cfg.Consumer)`,
   overriding only `Durable`/`FilterSubject`/`DeliverPolicy`.
4. **`medium`** — Replace the hand-built `nats.Msg` in `emit.go` with
   `natsutil.NewMsg(ctx, e.sendSubject, data)`, and add `natsutil.RequestIDHeader` to the badge and
   history-parent requests.
5. **`medium`** — Drop `Compression` from `bootstrap.go:37`, and make the disabled path verify the
   push stream too, matching `message-gatekeeper`'s fail-fast.
6. **`medium`** — Switch `main.go:449` to `jsretry.LowLatencyBackoff` to match broadcast-worker's
   user-visible fan-out.
7. **`medium`** — Fix `docs/client-api.md:6566` to remove `@here` from the large-room push triggers,
   and add a sonic↔stdlib wire-compat test for `PushNotificationEvent` (map field, plus HTML
   metacharacters in `Title`/`Body`) mirroring `broadcast-worker/sonic_wire_test.go`.

---

# Chapter 7 — Performance

**Score: 3 / 5**

Strong fundamentals: precise projections everywhere, batch `$in` reads, singleflight on the member
cache, cache metrics, acquire-before-spawn semaphore, sonic with `Pretouch` warming, and a payload
cap. Loses points on serialized critical-path I/O, an unchunked badge RPC that becomes a cliff in
large rooms, a per-message stdlib JSON decode of the whole member list, and the shutdown race.

## Findings

### `high` — Untracked goroutine and send on closed channel at shutdown

`main.go:378-399` spawns the invalidation reader with no `WaitGroup`. Shutdown step 3
(`main.go:490-492`) calls `invalIter.Stop()`, step 4 (`main.go:494`) runs `close(invalCh)` — but
`shutdown.Wait` runs steps sequentially with no synchronization against that goroutine
(`pkg/shutdown/shutdown.go:26-31`). If it is past `Next()` and at `invalCh <- evt.RoomID`
(`main.go:392`) when the close lands, the process panics.

### `high` — Badge RPC is unchunked and unbounded by room size

`handler.go:259` → `fetchUnreadCounts` → `badge_client.go:42-47` sends *every* badge account for a
site in one request under a fixed 5 s budget. `badgeAccounts` is the whole membership minus
sender/muted/restricted — the large-room throttle (`routing.go:20`) narrows push candidates, not
badge accounts. A 5 000-member room means one 5 000-account RPC per message; server-side,
`BadgeCountBatch` loops per account with a Mongo `unreadRooms` recompute on every cache miss
(`user-service/service/badge.go:24-57`). Presence chunks at 512 and settings chunks at 512; this
does not chunk at all.

### `high` — Independent lookups serialized on the critical path

`handler.go:243-244` runs settings (2 s) then presence (2 s) sequentially; `handler.go:259` then runs
badge (5 s); `handler.go:268-269` then room-meta and mention-names (2 s). All are mutually
independent — `badgeAccounts` is computed before the survivor filter, so even the badge call does not
depend on the settings/presence result. Worst case is roughly 13 s of serial wall time per message
against `AckWait=30s`, where an `errgroup` would give about 5 s. At `MAX_WORKERS=100` that is roughly
an 8 msg/s ceiling under degradation.

*Verified directly at `handler.go:236-275`.*

### `high` — Settings snapshot fails open systematically for large rooms

`usersettings.go:97-111` runs all chunks sequentially under one shared 2 s timeout — the code itself
documents the consequence at `:100-104`. 5 000 candidates is 10 sequential primary-read `$in` queries
inside 2 s; on expiry the tail chunks return zero settings, and every one of those users is pushed
regardless of `muteAllNotifications`.

### `medium` — Full member list JSON-decoded per message, with the stdlib codec

`pkg/roomsubcache/roomsubcache.go:141` does `json.Unmarshal([]byte(raw), &members)` — a full
string→`[]byte` copy plus an `encoding/json` reflection decode, on every message, with
`members := []Member{}` (`:140`) growing unpreallocated. A 5 000-member room is roughly 350 KB copied
plus 5 000 structs allocated *per message*. Every other hot path in this worker uses sonic
(`handler.go:102`, `emit.go:41`). There is no L1 tier (`members.go:24-26`, a deliberate choice), so
this cost is paid on every single message.

### `medium` — Batch emits are serial

`handler.go:286-307` publishes batches one at a time with synchronous JetStream publishes. An `@all`
in a 50 000-member room is 500 sequential round-trips inside the ack budget.

### `medium` — Presence fan-out spawns unbounded goroutines

`presence.go:69-104` starts one goroutine per 512-account chunk with no semaphore. 50 000 candidates
across 100 in-flight messages is roughly 9 800 goroutines. Compare `broadcast-worker/handler.go:458-477`,
which bounds fan-out with `maxSiteFanout` plus a shared deadline.

### `medium` — Fan-out-sized reads pinned to the Mongo primary

`main.go:227` pins `usersCol` to `readpref.Primary()` for the settings gate, then `main.go:249` reuses
that same handle for `fillHomeSites` (`main.go:153`) — an unchunked `$in` over every member account on
every cache fill. Home site is near-immutable and does not need primary reads.

### `low` — `model.Message` copied by value per member

`handler.go:195` passes `msg` (a 24-field struct, ~300 B) by value inside the member loop; the
`//nolint` at `handler.go:390` justifies it backwards. Add a per-member `Member` copy at
`handler.go:188` and an interface dispatch at `handler.go:218` for a no-op vetoer.

### `low` — Wrong backoff class for a user-visible path

`main.go:449` uses `jsretry.DefaultBackoff` (1s/5s/30s…); sibling `broadcast-worker/main.go:446` uses
`LowLatencyBackoff` (200 ms first retry), which `pkg/jsretry` documents as the fan-out/delivery choice.

### `low` — Member loader slice unpreallocated

`main.go:103` `var out []roomsubcache.Member` appended per cursor row — roughly 13 reallocations for
5 000 members. `cursor.All` or a `userCount`-sized prealloc avoids it.

### `nitpick` — Pretouch list incomplete

`pretouch.go:11-17` omits `BadgeCountBatchRequest`/`Response`, `getMessageByIDRequest`, and
`parentMessageProjection`, all sonic-coded on the hot path.

## Recommendations

1. **`high`** — Track the invalidation goroutine in a `WaitGroup` and join it before `close(invalCh)`,
   or replace the close with `invalCancel()` plus a `select` on ctx.
2. **`high`** — Chunk `badgeAccounts` per site (reuse `chunkStrings`, ~512) and issue chunks under a
   bounded `errgroup.SetLimit`.
3. **`high`** — Wrap settings, presence, and badge (plus room-meta and mention-name resolution) in one
   `errgroup` so the critical path is `max(...)` rather than `sum(...)`; give each chunk its own
   timeout instead of one shared budget in `usersettings.go`.
4. **`medium`** — Switch `roomsubcache` to sonic and decode from the raw string without the `[]byte`
   copy; consider a small TTL-bounded L1 for hot rooms keyed off `userCount`.
5. **`medium`** — Emit batches through a bounded `errgroup` (limit ~8) instead of the serial loop.
6. **`medium`** — Give `fillHomeSites` a `secondaryPreferred` collection handle and chunk its `$in`.
7. **`low`** — Switch `main.go:449` to `jsretry.LowLatencyBackoff`, take `msg` by pointer in
   `isRestricted`, and preallocate the loader slice.

---

# Chapter 8 — Prioritized action list

Ordered by severity first, then by impact ÷ effort. Items reported by more than one expert are
marked with the count — cross-dimension agreement is a confidence signal, not extra severity.

### 1. `high` — Fix the shutdown send-on-closed-channel race

- **Dimension:** Architecture / Performance / Integration / Code quality (**found by 4 of 6**)
- **Where:** `notification-worker/main.go:378-399`, `main.go:490-494`
- **Why:** The only finding that crashes a running pod. `invalIter.Stop()` is asynchronous, so a
  reader goroutine parked between `Next()` returning and the channel send will send on a closed
  channel and panic during every rollout that hits the window. Untracked goroutine also violates
  CLAUDE.md Section 3.
- **Fix:** Add the goroutine to a `WaitGroup`, join it before `close(invalCh)`. Roughly ten lines.

### 2. `high` — Stop narrowing `MESSAGES-CANONICAL` at bootstrap

- **Dimension:** Architecture / Integration (**found by 2 of 6**)
- **Where:** `notification-worker/main.go:268`, `bootstrap.go:25-32`, `bootstrap_test.go:85`
- **Why:** Passing the `.created` filter leaf as the stream's *subject set* means a dev boot with
  `BOOTSTRAP_STREAMS=true` strips `.edited/.deleted/.reacted/.pinned` from a stream
  `message-gatekeeper` owns — last-writer-wins, breaking every other canonical publisher. The unit
  test passes the wildcard instead of the production value, which is why CI cannot see it.
- **Fix:** Pass `wiring.CanonicalStream.Subjects`, and assert the exact subjects in the test. Better:
  only *verify* the canonical stream, since another service owns it. One line plus a test.

### 3. `high` — Parallelize the serialized critical-path lookups

- **Dimension:** Performance
- **Where:** `notification-worker/handler.go:243-244`, `:259`, `:268-269`
- **Why:** Settings → presence → badge → room-meta/mention-names run back-to-back despite being
  mutually independent. Worst case ~13 s serial against a 30 s `AckWait`; an `errgroup` makes it
  ~5 s. Highest throughput return of any item here, at moderate effort.
- **Fix:** One `errgroup` around the four independent calls.

### 4. `high` — Chunk the badge RPC

- **Dimension:** Performance
- **Where:** `notification-worker/handler.go:259`, `badge_client.go:42-47`, `routing.go:20`
- **Why:** The only fan-out call that never chunks. A 5 000-member room sends one 5 000-account
  request per message under a fixed 5 s budget, and `user-service/service/badge.go:24-57` loops
  per account with a Mongo recompute on each cache miss. This is a cliff, not a gradient.
- **Fix:** Reuse `chunkStrings` at ~512 with a bounded `errgroup.SetLimit`, matching presence.

### 5. `high` — Extract `store.go` / `store_mongo.go` out of `main.go`

- **Dimension:** Architecture / Maintainability / Code quality (**found by 3 of 6**)
- **Where:** `notification-worker/main.go:77-178`, `threads.go:22-73`, `usersettings.go:67-145`
- **Why:** Highest impact-per-unit-effort item in the report: it simultaneously fixes the mandated
  layout violation, makes the Mongo adapters unit-testable, and is the single largest lever on the
  coverage number (`main()` is 27.5% of all statements). Also unblocks action 6.
- **Fix:** `store.go` with a `NotificationStore` interface + `//go:generate mockgen`, `store_mongo.go`
  with the implementations.

### 6. `high` — Close the coverage gap to the 80% floor

- **Dimension:** Test coverage
- **Where:** `notification-worker/main.go:180` (184 untested statements); `handler.go:126`
- **Why:** 55.6% against a repo minimum of 80% — a blocking merge criterion per CLAUDE.md Section 4.
  Note the figure is distorted: excluding `main()` it is 76.9%, `handler.go` is at 97.4%, and Docker
  was unavailable so the integration suite (which covers the Mongo adapters) could not contribute.
  The remedy is structural, not "write more tests".
- **Fix:** Do action 5 first, then add `TestHandle_MemberFetchError_NAKs` (add an `err` field to
  `stubMembers`), the empty-member-list case, and the `isRestricted` nil-parent branch.

### 7. `high` — Delete the two duplications of shared code

- **Dimension:** Maintainability
- **Where:** `notification-worker/main.go:411-453` vs `pkg/natsmetrics/loop.go:19-23`;
  `parent_fetcher.go:36-90` vs `broadcast-worker/parent_fetcher.go:21-76`
- **Why:** ~45 lines reimplementing `natsmetrics.Start` (which the service already builds
  `consumerMetrics` for at `main.go:274`), plus a byte-identical copy of a history-service RPC client.
  Both are pure deletions that reduce drift risk. Low effort, immediate payoff.
- **Fix:** Call `natsmetrics.Start` as `broadcast-worker/main.go:351` does; promote the fetcher to
  `pkg/historyclient`.

### 8. `medium` — Re-run the two SAST gates that could not execute here

- **Dimension:** Code quality
- **Where:** CI / build environment
- **Why:** SAST is a blocking CI gate. `gosec` is clean, but `govulncheck` was blocked by the proxy
  (`vuln.go.dev` Forbidden) and `semgrep` could not be installed (`pipx`/`uv` version conflict). Two
  of three gates are **unverified, not passed** — this audit cannot certify them.
- **Fix:** Run `make sast` in CI or any environment with network access and working tooling.

### 9. `medium` — Fix the `@here` documentation drift

- **Dimension:** Integration
- **Where:** `docs/client-api.md:6566` vs `handler.go:135-136`, `pkg/mention/mention.go:47-51`
- **Why:** The docs promise clients that `@here` triggers a push in large rooms; the code explicitly
  and deliberately does not. This is the client contract, and it is wrong in the direction that makes
  integrators build against behaviour that will never fire. A one-line doc fix.

### 10. `medium` — Give the invalidation consumer real consumer settings, and propagate request IDs

- **Dimension:** Integration / Architecture (**found by 3 of 6** for the consumer settings)
- **Where:** `main.go:363-368` (consumer config); `emit.go:48-54`, `badge_client.go:47`,
  `parent_fetcher.go:75` (request IDs)
- **Why:** The second consumer inherits unlimited `MaxDeliver` and no backoff, invisible to
  `CONSUMER_*` tuning. Separately, this is the only service in the repo hand-building a publish
  `nats.Msg`, so the `X-Request-ID` correlation chain dies at every outbound hop — contradicting
  CLAUDE.md Section 3.
- **Fix:** Build the config from `stream.DurableConsumerDefaults`; replace hand-built messages with
  `natsutil.NewMsg`.

---

## Closing note

Items 1 and 2 are the two that would actually bite in production, and both are small, local, and
well-understood. Item 5 is the highest-leverage structural change: it resolves a layout violation, a
maintainability complaint, and most of the coverage deficit in one move. Nothing in this audit
suggests the service delivers *wrong* notifications — the business logic is well-tested and the
fail-open behaviour is deliberate and documented. The gap is operational robustness at the edges
(shutdown, bootstrap) and headroom under large-room load.
