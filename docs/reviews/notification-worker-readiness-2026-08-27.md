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
