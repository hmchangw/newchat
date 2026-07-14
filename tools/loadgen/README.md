# loadgen

Capacity-baseline load generator for the single-site messaging pipeline
(`message-gatekeeper` → `MESSAGES_CANONICAL` → `message-worker` +
`broadcast-worker`). Single Go binary with three subcommands.

## Quick start

```
make -C tools/loadgen/deploy up
make -C tools/loadgen/deploy seed PRESET=medium
make -C tools/loadgen/deploy run  PRESET=medium RATE=500 DURATION=60s
```

`make up` brings up the shared `docker-local` stack (NATS, MongoDB,
Cassandra, Valkey, Elasticsearch, every microservice) and then the
load-test-only overlay (loadgen, Prometheus, Grafana). The overlay joins
the `chat-local` network so it can reach the same services any developer
sees with `make up` at the repo root.

For live dashboards:

```
make -C tools/loadgen/deploy run-dashboards PRESET=medium
# Grafana at http://localhost:3000 (anonymous admin)
```

Tear down:

```
make -C tools/loadgen/deploy teardown PRESET=medium  # drop Mongo fixtures
make -C tools/loadgen/deploy down                     # stop containers
```

## Encryption

`broadcast-worker` runs with `ENCRYPTION_ENABLED=true` by default in this
stack. `loadgen seed` provisions one AES-256-GCM key per fixture room into
the room's document in the MongoDB `rooms` collection (the same place
`broadcast-worker` reads from), derived from the RNG seed so runs stay
reproducible. To run an apples-to-apples plaintext comparison:

```
ENCRYPTION_ENABLED=false make -C tools/loadgen/deploy up
```

Loadgen's end-to-end broadcast correlation reads `RoomEvent.LastMsgID`,
which sits in the cleartext envelope regardless of encryption mode, so
the run binary itself never touches ciphertext.

## Presets

| preset      | users  | rooms | notes                                                  |
|-------------|--------|-------|--------------------------------------------------------|
| `small`     | 10     | 5     | uniform, 200-byte content                              |
| `medium`    | 1 000  | 100   | uniform, 200-byte content                              |
| `large`     | 10 000 | 1 000 | uniform, 200-byte content                              |
| `realistic` | 1 000  | 100   | Zipf senders, mixed room sizes, 50–2000 bytes, mentions|

## Subcommands

- `loadgen seed --preset=<name> [--seed=42]` — idempotently populate
  MongoDB with fixtures, including a per-room key in each room document.
  Indexes are owned by the services (`EnsureIndexes`), not the seeder: it
  preserves whatever indexes already exist, so bring the services up first
  (`make up`) for the seeded data to be indexed as in production.
- `loadgen run --preset=<name> [flags]` — open-loop publish at `--rate`
  msgs/sec for `--duration`, print a summary at the end. Flags:
  `--seed`, `--warmup`, `--inject=frontdoor|canonical`, `--csv=<path>`.
- `loadgen teardown --preset=<name> [--seed=42]` — clear the seeded
  Mongo data (the per-room keys go with the room documents), preserving
  the services' indexes so a following seed starts indexed.

## Reading the summary

- `final_pending == 0` on both durables, zero errors → the pipeline is
  sustaining your target rate.
- `final_pending` climbing, or error counts > 0 → over capacity or a
  regression upstream of the worker.

## Non-goals

- Not a CI regression gate. Invoked manually.
- Not an auth benchmark. Uses shared `backend.creds`.
- Not a cross-site benchmark. Single-site only.
- Not an absolute-number tool. Numbers vary by host — compare within one
  machine across changes, don't compare across machines.

## Members workload (add-member benchmark)

Benchmarks the add-member pipeline:
`room-service.handleAddMembers` → `chat.room.canonical.{siteID}.member.add`
(ROOMS stream) → `room-worker` → `chat.room.{roomID}.event.member` event. E2 is
the emission of that event, gated by room-worker's ROOMS-consumer throughput —
not a broadcast fan-out.

### Quick start

```
make -C tools/loadgen/deploy up
make -C tools/loadgen/deploy seed-members PRESET=members-medium
make -C tools/loadgen/deploy run-sustained PRESET=members-medium RATE=100 DURATION=60s
```

For capacity-mode growth curves:

```
make -C tools/loadgen/deploy seed-members PRESET=members-capacity
make -C tools/loadgen/deploy run-capacity  PRESET=members-capacity TARGET_SIZE=500
```

Between sustained runs, reset state so candidate pools refill:

```
make -C tools/loadgen/deploy reset-members PRESET=members-medium
```

### Presets

| preset             | rooms | baseline | candidate pool | use case                                |
|--------------------|-------|----------|----------------|-----------------------------------------|
| `members-small`    | 5     | 10       | 50             | smoke / dev                             |
| `members-medium`   | 100   | 100      | 900            | sustained-throughput default            |
| `members-heavy`    | 700   | 10       | 990            | high-rate sustained (≈1000 req/s)       |
| `members-capacity` | 5     | 1        | 990            | capacity-growth, fills up to ~MAX_ROOM_SIZE |

A candidate is single-use — once added it's a room member and can't be
re-added, and `baseline + candidate pool` is capped at `MAX_ROOM_SIZE` (1000).
So a sustained run can make at most `rooms × ⌊candidate pool ÷ users-per-add⌋`
add-member publishes total. `members-medium` (100 × ⌊900÷10⌋ = 9000 ops)
sustains the default `RATE=100 DURATION=60s` (6000 ops) with margin;
`members-small` is a smoke preset and cannot sustain that load.

For higher rates, add rooms rather than pool (pool is capped per room). To
sustain **1000 req/s for 60s** (60,000 ops) at the default `users-per-add=10`,
use `members-heavy` (700 × ⌊990÷10⌋ = 69,300 ops, ≈69s of headroom):

```
make -C tools/loadgen/deploy seed-members  PRESET=members-heavy
make -C tools/loadgen/deploy run-sustained PRESET=members-heavy RATE=1000 DURATION=60s
```

If instead each request need only add one member, `members-medium` at
`USERS_PER_ADD=1` already supplies 90,000 ops — no heavy preset required.

### Subcommands

- `loadgen seed --workload=members --preset=<name>` — populate Mongo
  for the members workload (including per-room keys in the room documents).
- `loadgen teardown --workload=members --preset=<name>` — drop the seeded data.
- `loadgen members-sustained --preset=<name> [flags]` — open-loop publish
  at `--rate` req/sec for `--duration`. Flags: `--users-per-add` (default 10),
  `--inject=frontdoor|canonical` (default frontdoor),
  `--shape=users` (v1; orgs/channels/mixed reserved for v2), `--warmup`,
  `--csv`.
- `loadgen members-capacity --preset=<name> --target-size=N [flags]` —
  per-room sequential growth until rooms reach `--target-size`. Flags:
  `--users-per-add`, `--inject`, `--shape`, `--max-rate` (per-room rate
  cap, default 0 = sequential pacing only), `--e2-timeout`, `--csv`.

### v1 scope

Only `--shape=users` is implemented. The flag accepts `orgs`, `channels`,
`mixed` for forward compat but rejects them at parse time. See
`docs/superpowers/specs/2026-05-19-load-test-room-members-design.md`
for the rationale and the v2 plan.

### Reading the summary

- **Sustained mode**: `final_pending == 0` on room-worker + zero errors →
  pipeline is sustaining the target rate. Climbing `final_pending` or
  non-zero errors → over capacity. If `rate × duration` would exceed the
  preset's pool budget (see the preset table above), the command now
  **refuses to start** and prints the achievable max `--rate`/`--duration`
  for the preset — lower one of them or pick a bigger preset. (The old
  behaviour ran for ~50s and then logged `aborted early — pools exhausted`.)
- **Capacity mode**: the size-bucket table shows latency at four
  size ranges; the `final sizes` block confirms each room hit
  `--target-size`. A row with `count > 0` whose `e2_p99` is much larger
  than smaller-size buckets indicates a per-room-size degradation. Like
  sustained mode, capacity mode **refuses to start** if `--target-size`
  is unreachable from the preset's per-room pool (`baseline +
  ⌊pool ÷ users-per-add⌋ × users-per-add`); it prints the reachable
  ceiling — lower `--target-size` or pick a larger preset.

## Room-read workload (mark-as-read benchmark)

Finds the maximum sustainable RPS for marking a room as read
(`room-service.handleMessageRead`, the `message.read` request/reply RPC). The
workload reuses the messages presets but seeds read-state so the room
read-floor recompute path stays exercised: every room's `lastMsgAt` is stamped
ahead of the run window and members' `lastSeenAt` are spread behind it, so each
read is "a user opening a room with unread content" — the floor scan fires on
every request and the floor write fires at a rate set by room size and the read
distribution.

### Quick start

```
make -C tools/loadgen/deploy up
make -C tools/loadgen/deploy seed-roomread PRESET=medium
make -C tools/loadgen/deploy run-max-rps WORKLOAD=room-read PRESET=medium
```

Override the ramp with `STEPS` (default `200,500,1000,2000,5000`):

```
make -C tools/loadgen/deploy run-max-rps WORKLOAD=room-read PRESET=medium STEPS=500,1k,2k,5k
```

Tear down the fixtures:

```
make -C tools/loadgen/deploy teardown-roomread PRESET=medium
```

### Notes

- Synchronous request/reply: gated on p95/p99 latency and error rate only
  (no consumer-pending signal). Defaults: `--slo-p95=100ms`, `--slo-p99=250ms`,
  `--slo-error-rate=0.001`; override via the shared `max-rps` flags.
- Single-site only: all seeded users are local, so no cross-site inbox event is
  published on the read path.
- Presets are the messages presets (`small`/`medium`/`large`/`realistic`); room
  size distribution drives floor-write contention.

## History workload (LoadHistory / GetThreadMessages benchmark)

Benchmarks the synchronous read path:
`history-service.LoadHistory` (Cassandra bucket walk on
`messages_by_room`) and `history-service.GetThreadMessages`
(single-partition slice on `thread_messages_by_thread`).

### Quick start

```bash
make -C tools/loadgen/deploy up
loadgen seed --workload=history --preset=history-medium
loadgen history-sustained --preset=history-medium --rate=200 --duration=60s
```

The history workload requires `CASSANDRA_HOSTS` (e.g. `cassandra:9042`)
in addition to the standard Mongo/NATS env. `MESSAGE_BUCKET_HOURS`
(default 72) must match what `history-service` is configured with so
seed-time and read-time bucket math agree.

### Presets

| preset           | rooms | msgs/room | span    | thread rate | use case             |
|------------------|-------|-----------|---------|-------------|----------------------|
| `history-small`  | 5     | 100       | 1 day   | 0           | smoke / dev          |
| `history-medium` | 100   | 5 000     | 7 days  | 5%          | sustained-throughput |
| `history-large`  | 1 000 | 50 000    | 30 days | 10%         | partition fan-out    |

Top-level messages are placed uniformly across the span with ±50% jitter
on the gap so they don't align to bucket boundaries. Thread replies land
1–10 min after their parent and share a bucket with it. Rooms are picked
via `rand.Zipf(s=1.1, v=1.0)` over the room list — a few hot rooms absorb
most reads.

### Subcommands

- `loadgen seed --workload=history --preset=<name>` — populate Mongo
  (users/rooms/subscriptions/thread\_rooms, plus per-room keys in the room
  documents — harmless for read workload), and Cassandra (messages\_by\_room,
  messages\_by\_id, thread\_messages\_by\_room).
- `loadgen teardown --workload=history --preset=<name>` — drop the
  seeded data.
- `loadgen history-sustained --preset=<name> [flags]` — open-loop
  request at `--rate` req/sec for `--duration`. Flags:
  `--mix=history:80,thread:20` (endpoint weighting),
  `--before-mode=open:70,scrollback:30` (cursor strategy),
  `--scrollback-pages=5` (pages per chain before reset),
  `--page-limit=20`, `--request-timeout=5s`, `--warmup`, `--csv`.

### Reading the summary

- Per-endpoint p50/p95/p99 + payload sizes split LoadHistory vs
  GetThreadMessages so a slow thread path doesn't get hidden by faster
  history reads. The `bucket-walk depth` block reports how many
  LoadHistory replies stayed within a single Cassandra bucket vs spanned
  multiple — climbing multi-bucket counts under `--before-mode=scrollback`
  indicate the walker is paying coordinator round-trips per page.
- Errors broken out by class (`timeout`, `reply`, `bad`); the
  `no-thread-parents` counter is informational (thread requests that
  landed on a room with no seeded parents and fell back to history).

## Thread-read workload (GetThreadMessages benchmark)

Finds the maximum sustainable RPS for **loading thread messages** —
`history-service.GetThreadMessages`, the single-partition slice read on the
Cassandra `thread_messages_by_thread` table. This isolates the thread-read
ceiling that the `history` workload only measures blended with `LoadHistory`
(via its `--mix`); read the focused number here and compare it against the
blended `history` run on the same box.

**First-page opens only.** Each request opens a thread cold — pick a seeded
parent and fetch the first page of replies (no cursor). Models the dominant
real case (a user clicking into a thread).

**Reuses the history fixtures and seed.** Like `read-receipt`, this workload
reads the history presets' rooms/subscriptions and the seeded thread parents +
replies; there is no dedicated seed. Requires `CASSANDRA_HOSTS` and the same
`MESSAGE_BUCKET_HOURS` as the running services.

### Quick start

```bash
make -C tools/loadgen/deploy up

# Seed rooms/subs/keys (Mongo) + parents/replies/thread_rooms (Cassandra+Mongo).
# Use a preset that seeds threads: history-medium or history-large
# (history-small has ThreadRate 0 and seeds no threads).
loadgen seed --workload=thread-read --preset=history-medium

# Ramp the thread-read path.
loadgen max-rps --workload=thread-read --preset=history-medium

# Clean up.
loadgen teardown --workload=thread-read --preset=history-medium
```

Via the deploy Makefile:

```bash
make -C tools/loadgen/deploy run-max-rps WORKLOAD=thread-read PRESET=history-medium
```

### Presets

Reuses the **history** presets. `history-medium` / `history-large` seed thread
parents in every room; `history-small` seeds none, so a thread-read ramp on it
issues no real reads (every request is counted as `no-thread-parents` and the
step reports no samples).

### Subcommands

- `loadgen seed --workload=thread-read --preset=<name> [--seed=42]` — delegates
  to the history seed (Mongo users/rooms/subscriptions/thread\_rooms + room keys;
  Cassandra `messages_by_room` / `messages_by_id` / `thread_messages_by_thread`).
- `loadgen max-rps --workload=thread-read --preset=<name> [flags]` — ramp the
  GetThreadMessages read path. Honors `--page-limit` (default 20),
  `--request-timeout` (default 5s), and the shared ramp flags (`--steps`
  defaults to `200,500,1000,2000,5000`, `--warmup`, `--hold`, `--cooldown`,
  `--slo-*`, `--csv`).
- `loadgen teardown --workload=thread-read --preset=<name>` — delegates to the
  history teardown.

### Reading the summary

Synchronous request/reply: gated on the single `thread-read` latency series'
p95/p99 and the error rate only (no consumer-pending signal, so
`--slo-pending-growth` is ignored). A non-zero error rate at low RPS usually
means a seeding/config problem — a `MESSAGE_BUCKET_HOURS` mismatch making the
seeded parents unreadable, or pointing the run at `history-small`. The verdict,
INCONCLUSIVE load-box guard, and CSV output behave exactly as for the other
read workloads.

## Thread-reply workload (thread-send benchmark)

Finds the maximum sustainable RPS for sending **thread replies**, directly
comparable to the `messages` workload on the same box. A thread reply costs
more than a plain message send because `message-gatekeeper` issues a
synchronous `GetMessageByID` RPC to `history-service` to resolve the parent
(extra E1 latency), and `message-worker` writes `thread_messages_by_thread`
plus thread-metadata fan-out (extra E2 latency).

**Frontdoor only.** The unique thread cost is on the gatekeeper path, so the
`thread` workload always uses frontdoor injection and ignores `--inject`.

**Parents must be pre-seeded.** The gatekeeper fetches the parent message, so
each reply must reference a real message. `seed --workload=thread` writes
`--parents-per-room` (default 8) parent messages per room into Cassandra
(`messages_by_room` + `messages_by_id`). Requires `CASSANDRA_HOSTS` and the
same `MESSAGE_BUCKET_HOURS` as the running services.

### Quick start

```bash
# 1. Seed rooms/subs/keys (Mongo) + parents (Cassandra). Use the same --seed
#    and --parents-per-room you will run with (defaults: seed 42, 8 parents).
loadgen seed --workload=thread --preset=medium --seed=42

# 2. Ramp the thread-reply send path.
loadgen max-rps --workload=thread --preset=medium --seed=42

# 3. (optional) Compare against plain sends on the same box.
loadgen max-rps --workload=messages --preset=medium --inject=frontdoor

# 4. Clean up (TRUNCATEs message tables + clears Mongo fixtures + room keys).
loadgen teardown --workload=thread --preset=medium --seed=42
```

Via the deploy Makefile:

```bash
make -C tools/loadgen/deploy run-max-rps WORKLOAD=thread PRESET=medium
```

### Presets

Reuses the messages presets (`small`/`medium`/`large`/`realistic`).

### Subcommands

- `loadgen seed --workload=thread --preset=<name> [--seed=42] [--parents-per-room=N]` —
  populate Mongo (users/rooms/subscriptions/room keys) and Cassandra
  (parent messages for each room). N defaults to 8 (the `0 → 8` fallback in `BuildThreadFixtures`).
- `loadgen max-rps --workload=thread --preset=<name> [--seed=42] [--parents-per-room=N] [flags]` —
  ramp thread-reply sends. `--parents-per-room` (default 8) must equal the value
  used at seed time. Shared ramp flags (`--steps`, `--warmup`, `--hold`,
  `--cooldown`, `--slo-*`, `--csv`) behave identically to the `messages`
  workload.
- `loadgen teardown --workload=thread --preset=<name> --seed=42` — drop the
  seeded Mongo fixtures and TRUNCATE Cassandra message tables. `--seed` is
  required because teardown rebuilds the room list to remove per-room keys.

### Seed-matching caveat

`--seed` and `--parents-per-room` **must match** between `seed` and `max-rps`.
The ramp rebuilds parent IDs from the seed to reference them; a mismatch
makes every reply target a non-existent parent and the gatekeeper rejects
the run. Both default to seed `42` / 8 parents — `max-rps --workload=thread`
now accepts `--parents-per-room` (default 8) so a non-default seed-time value
can be passed through. Leave both at the defaults for a straightforward
comparison against the `messages` workload.

## max-rps — auto-find Max RPS under SLO

Automatically finds the maximum RPS each workload can sustain while all
SLO signals hold. The subcommand ramps the target rate through an ordered
list of steps, holds at each step for a measurement window, evaluates SLO
signals, and reports the largest step at which every signal passed.

```bash
loadgen max-rps --workload=messages|history|read-receipt|room-read|thread-read --preset=<name> [flags]
```

### Quick start

```bash
# messages: ramp 500..10k rps, stop at first SLO breach
loadgen max-rps --workload=messages --preset=medium --steps=500,1k,2k,5k,10k

# history: per-endpoint SLO, custom p95
loadgen max-rps --workload=history --preset=history-medium --steps=200,500,1k,2k --slo-p95=80ms

# read-receipt: seed reader state first, then ramp
loadgen seed --workload=read-receipt --preset=history-medium --read-ratio=0.7
loadgen max-rps --workload=read-receipt --preset=history-medium --steps=200,500,1k,2k
```

Via the deploy Makefile:

```bash
make -C tools/loadgen/deploy run-max-rps PRESET=medium
make -C tools/loadgen/deploy run-max-rps WORKLOAD=history PRESET=history-medium STEPS=200,500,1k,2k
```

### Flags

| Flag | Default | Notes |
|------|---------|-------|
| `--workload` | `messages` | `messages`, `history`, `read-receipt`, `room-read`, or `thread-read` |
| `--preset` | (required) | an existing preset for the chosen workload (`read-receipt` reuses the history presets) |
| `--steps` | messages `500,1k,2k,5k,10k` / history+read-receipt `200,500,1k,2k,5k` | explicit ordered RPS list; `k` suffix = ×1000 |
| `--request-timeout` | `5s` | **history / read-receipt / room-read / thread-read**: per-request reply timeout |
| `--warmup` | `10s` | per-step warmup (samples discarded) |
| `--hold` | `30s` | per-step measurement window |
| `--cooldown` | `5s` | per-step settle gap before next step |
| `--slo-p95` | `100ms` | applied to **every** gated latency series |
| `--slo-p99` | `250ms` | applied to **every** gated latency series |
| `--slo-error-rate` | `0.001` | `failed / attempted` (0.1%) |
| `--slo-pending-growth` | `1000` | **messages only**: per-durable end−start `NumPending` delta |
| `--rate-tolerance` | `0.05` | achieved-vs-target shortfall band for the INCONCLUSIVE guard |
| `--stop-on-trip` | `true` | stop the ramp at the first TRIP (does **not** stop on INCONCLUSIVE) |
| `--seed` | `42` | RNG seed (parity with existing subcommands) |
| `--csv` | `""` | optional CSV output path |

### Reading the output

At the end of the run the tool prints a per-step table and a final
verdict line:

```text
ANSWER: max RPS = 2000 (workload=messages, preset=medium)
        Next limit: E2 p95=143ms > 100ms
```

This is the largest step at which **all** SLO signals passed; the
`Next limit:` line names why the first failing step tripped. If no step
passed, the output is `ANSWER: no step passed (workload=…, preset=…)`.

**INCONCLUSIVE rows** appear when the achieved throughput fell more than
`--rate-tolerance` below the target while the SLO signals still looked
healthy — i.e. the load generator itself, not the service under test, was
the limiting factor, so the step's result can't be trusted. An
INCONCLUSIVE step does **not** count as a pass and does **not** stop the
ramp, even with `--stop-on-trip`; only a hard TRIP stops the ramp.

The `reasons:` line names which load-box limit dominated so you know which
knob to turn — the two are distinct columns (`saturation`, `emit_underrun`)
in the CSV:

- **emit underrun** — the generator could not even *release* the load on
  schedule (its dispatch loop fell behind the target cadence). The load box
  is CPU/scheduler starved: give it more CPU, lower the per-box rate, or
  shard the load across more generator processes.
- **saturation** — the load *was* released on schedule but the in-flight
  pool was full when an event came due. The pool is too small for the
  rate×latency product: raise `MAX_IN_FLIGHT` (and/or reduce backend
  latency).

> **Rate pacing.** The generator paces an open-loop arrival rate with a
> batched emitter: it ticks on a coarse, reliably-schedulable interval and
> releases `rate × interval` events per tick. This replaces the old
> one-event-per-tick ticker, whose sub-millisecond intervals the Go runtime
> can't honor (it silently coalesces ticks), which capped achievable RPS at
> a few thousand regardless of `--steps`. Setting `MAX_IN_FLIGHT=0` selects
> the legacy serial-on-ticker path for bisection only — it will not ramp.

### Read-receipt workload (`--workload=read-receipt`)

Drives the room-service read-receipt RPC
(`chat.user.{account}.request.room.{roomID}.{siteID}.message.read-receipt`) — a
synchronous request/reply read ("who has read message X") — to find the maximum
sustainable RPS under the latency/error SLOs. Like `history`, it is a read with
no JetStream consumer, so `--slo-pending-growth` is ignored and the per-request
timeout is set with `--request-timeout`.

Read receipts reuse the **history** presets and seed: the requester for each
target is the message's sender (the RPC requires `msgSender == requesterAccount`),
and only top-level messages are used as targets. Reader state must be seeded so
the `ListReadReceipts` Mongo query exercises its real `$match`/`$lookup`/`$unwind`
path instead of short-circuiting on an empty `lastSeenAt` match.

Seed (stamps `lastSeenAt` on a `--read-ratio` fraction — default `0.7` — of each
room's subscribers; requires `CASSANDRA_HOSTS` like the history seed):

```bash
loadgen seed --workload=read-receipt --preset=history-medium --read-ratio=0.7
```

Then ramp:

```bash
loadgen max-rps --workload=read-receipt --preset=history-medium --steps=200,500,1k,2k,5k
```

The gated latency series is named `read-receipt`; the verdict, INCONCLUSIVE
guard, and CSV output behave exactly as for the other workloads.

To tear down, use the history teardown — read-receipt seeds the identical
history fixtures, so `loadgen teardown --workload=history --preset=<name>` drops
everything (dropping `subscriptions` removes the stamped `lastSeenAt` too):

```bash
loadgen teardown --workload=history --preset=history-medium
```

### Bottleneck attribution

When a `max-rps --workload=messages` ramp trips, loadgen appends a
`BOTTLENECK:` block naming the culprit component, the saturated resource,
and a confidence:

```text
ANSWER: max RPS = 2000 (workload=messages, preset=medium)
        Next limit: E2 p95=143ms > 100ms
BOTTLENECK: message-worker (Cassandra-bound)
        message-worker consumer backlog grew (first stage to back up)
        cassandra CPU plateaued between 1000 and 2000 rps while load rose
        confidence: high
```

It fuses loadgen's per-stage signals (E1/E2 latency, per-durable backlog)
with cAdvisor container CPU trends from Prometheus. `make run-max-rps`
starts cAdvisor + Prometheus for you (no need to run `make run-dashboards`
first). Tunables (env, `BOTTLENECK_` prefix):

| Var | Default | Notes |
|-----|---------|-------|
| `BOTTLENECK_ENABLED` | `true` | Set `false` to disable; run behaves as before. |
| `BOTTLENECK_PROM_URL` | (set in compose) | Prometheus that scrapes cAdvisor. Empty = disabled. |
| `BOTTLENECK_KNEE_TOLERANCE` | `0.10` | Max relative CPU rise still counted as a plateau. |
| `BOTTLENECK_QUERY_STEP` | `5s` | PromQL step; match the scrape interval. |
| `BOTTLENECK_CONTAINER_MAP` | (empty) | `shortid:name,…` fallback when cAdvisor omits the compose-service label. |

The verdict is best-effort: if Prometheus is unreachable or the data is too
thin (e.g. the breach was on the first step), the line reads
`BOTTLENECK: undetermined (<reason>)` and the run still reports normally.

## Daily-IM scenario (find N) — Operator Guide

Simulates N users using the chat system as their primary IM throughout
a workday, ramps N geometrically through a configured step list, holds
steady at each step while watching SLO signals, and reports the largest
N at which everything held. The output answers:

> *How many concurrent daily-IM users can a single-site deployment
> sustain before a real signal breaks, and what breaks first?*

Single-site only. Not a CI gate — invoked manually for capacity work.

### Table of contents

1. [Quick start](#quick-start)
2. [Prerequisites](#prerequisites)
3. [Presets](#presets)
4. [CLI flags](#cli-flags)
5. [Environment variables](#environment-variables)
6. [SLO signals and verdicts](#slo-signals-and-verdicts)
7. [Reading the output](#reading-the-output)
8. [Troubleshooting](#troubleshooting)
9. [Known limitations](#known-limitations)
10. [Design references](#design-references)

### Quick start

```bash
# 1. Bring up the docker-local stack (NATS, Mongo, Valkey, Cassandra, all services).
make -C tools/loadgen/deploy up

# 2. Seed Mongo with users/rooms/subscriptions (room keys live in the room docs) for your preset.
#    Must be re-run when you change preset (the fixture IDs differ per preset).
make -C tools/loadgen/deploy seed PRESET=daily-heavy

# 3. Ramp.
make -C tools/loadgen/deploy run-daily PRESET=daily-heavy
```

### Prerequisites

Before `loadgen daily` will produce a meaningful verdict, you need:

| Requirement | Why | How to get it |
|---|---|---|
| Docker-local stack running | Daily talks to message-gatekeeper, room-service, broadcast-worker, etc. | `make -C tools/loadgen/deploy up` |
| Mongo `users`/`rooms`/`subscriptions` seeded for the preset | Gatekeeper rejects every send with "user not subscribed" otherwise | `loadgen seed --workload=messages --preset=<your daily preset>` |
| Per-room AES-256-GCM keys (in the room documents) | broadcast-worker decrypts with these when `ENCRYPTION_ENABLED=true` (default) | Written by the same `loadgen seed` step |
| JetStream streams (`MESSAGES`, `MESSAGES_CANONICAL`, `ROOMS`, `INBOX`) | The whole pipeline | Auto-created by services at startup when `BOOTSTRAP_STREAMS=true` (docker-local default) |
| Cassandra tables | message-worker writes here; history-service reads here | Created by `docker-local/cassandra/init/*.cql` at first stack boot |
| `NATS_CREDS_FILE` pointing at credentials with `pub/sub` on `chat.>` | Loadgen otherwise dials anonymously and gets permission violations | docker-local writes `backend.creds` with full perms via `docker-local/setup.sh` |

A preflight runs at `runDaily` startup: it opens a short Mongo connection,
counts subscriptions for `cfg.SiteID`, and bails with an actionable error
if zero. So forgetting step 2 fails fast in seconds rather than burning
the whole ramp.

### Presets

All three daily presets seed 10000 users. They differ in the rooms-per-user
distribution (the "what a typical IM user's room list looks like" shape).

| preset       | DMs | small (5–20) | medium (50–200) | large (500–2000) | rooms/user | use case |
|--------------|-----|--------------|-----------------|------------------|------------|----------|
| daily-light  | 15  | 10           | 5               | 2                | ~32        | light daily-IM user |
| daily-heavy  | 25  | 20           | 8               | 3                | ~56        | heavy daily-IM user (default) |
| daily-power  | 40  | 30           | 10              | 3                | ~83        | power user (eng / manager) |

Room sizes within each band are drawn via Zipf-like sampling so the
long tail is realistic. Subscriptions are generated via stub-pairing
for the DM band and a slot-bag picker for the others — both
O(N × perUser), so fixture build at N=10000 finishes in ~1s.

### CLI flags

`loadgen daily -h` prints the same:

| Flag | Default | Notes |
|---|---|---|
| `--preset` | `daily-heavy` | `daily-light` \| `daily-heavy` \| `daily-power` |
| `--steps` | `1000,2000,5000,10000,20000,50000,100000` | Comma-separated N values per ramp step. `k` suffix = ×1000. Max cannot exceed the preset's `Users` (10000); excess is capped and the step INCONCLUSIVEs with `only X/Y users activated`. |
| `--warmup` | `60s` | Per-step warm-up before SLO measurement begins. Latency samples from this window are discarded by `Collector.Reset` at the start of hold. |
| `--hold` | `180s` | Steady-state window where SLO signals are evaluated. |
| `--cooldown` | `30s` | Drain time between steps to let consumers catch up. |
| `--stop-on-trip` | `true` | Stop the ramp on the first TRIP. Set `false` to keep ramping past the first failure (useful for understanding the slope of degradation). |
| `--max-direct-users` | `20000` | Cap on the direct-pool size (one `nats.Conn` per user). Above this, additional users are placed in the multiplex pool. |
| `--multiplex-pool-size` | `200` | Number of shared `nats.Conn` instances in the multiplex pool. Set `0` to disable multiplex (any user past `--max-direct-users` is then silently skipped). |
| `--max-conns-per-process` | `25000` | Safety ceiling on the total nats.Conn count to this process. Combined `direct + multiplex` must not exceed this. |
| `--csv` | `""` | Optional CSV output path (one row per step). |

Example:

```bash
loadgen daily \
  --preset=daily-heavy \
  --steps=1k,2k,5k,10k \
  --warmup=15s --hold=45s --cooldown=10s \
  --max-direct-users=2000 --multiplex-pool-size=200 \
  --csv=results.csv
```

**Optional presence load:** `--presence` makes each daily user also maintain
presence (a `hello` on activation, a `ping` every `--presence-heartbeat`, and an
activity flip on each active↔idle Markov transition). Presence latency/errors
are reported **observationally** — a `presence:` line under each step and
`presence_*` CSV columns — and never affect the daily PASS/TRIP/INCONCLUSIVE
verdict. Off by default; absent the flag, the daily run is unchanged.

```bash
loadgen daily --preset=daily-heavy --presence --presence-heartbeat=30s --csv=daily.csv
```

### Environment variables

Read by the base loadgen `config` struct (env vars, not flags):

| Var | Default | Notes |
|---|---|---|
| `NATS_URL` | (required) | `nats://...` |
| `NATS_CREDS_FILE` | `""` | Path to NATS creds (mandatory against operator-mode NATS — otherwise loadgen dials anonymous and gets "permissions violation"). |
| `NATS_MONITORING_URL` | `http://nats:8222/jsz` | Where the JetStream-pending poller queries. Override to `http://127.0.0.1:8222/jsz` if you're running loadgen on the host instead of inside the compose network. |
| `MONGO_URI`, `MONGO_DB`, `MONGO_USERNAME`, `MONGO_PASSWORD` | (uri required; db default `chat`) | Used by the seed step (including per-room keys, now stored in the room documents) and the daily preflight. |
| `SITE_ID` | `site-local` | Must match the gatekeeper's configured site or every send is rejected with `siteID mismatch`. Also used as the partition key for seeded fixtures. |

### SLO signals and verdicts

A step's verdict is one of `PASS`, `TRIP`, or `INCONCLUSIVE`.

**TRIP** if any of:

- `p95_latency_ms` > 500 — publish→broadcast latency, measured by correlating `RoomEvent.LastMsgID` with `RecordPublish` timestamps
- `p99_latency_ms` > 1000 — same source
- `error_rate` > 0.001 (0.1%) — failed publishes, request timeouts, gatekeeper 4xx/5xx; counted by the action emitter
- any JetStream consumer's `num_pending` grew by more than 1000 over the hold — polled via `/jsz?consumers=true` at hold start and end. The `notification-worker` durable is exempt: push-notification delivery delay is tolerated by design, so its backlog never fails the run (still shown in `worst-pending-delta` for observability)
- any service's `slog_errors_total` counter increased over the hold — currently a no-op since backend services don't expose `/metrics` HTTP endpoints; see known limitations
- any durable that existed at hold-start was *missing* at hold-end (consumer crashed or was deleted) — applies to `notification-worker` too, since a vanished consumer is an availability failure, not a tolerated delay

**INCONCLUSIVE** (overrides PASS/TRIP — means "verdict signals can't be trusted") when:

- Loadgen GC pause p99 > 50ms — the load box is under pressure, latency measurements may reflect loadgen-side GC rather than the system under test
- `AttemptedOps == 0` — publisher conn failed at startup, or no users were activated, or hold window was zero; a PASS here would be a silent lie
- `EffectiveN < 95% of N` — fewer than 95% of the nominal N users actually came online (pool caps too low, or `--steps` exceeded `preset.Users`)
- `pollPending` poll failed at start or end of hold even after retries — only when caused by ctx cancel; transient flakes are tolerated by dropping the pending-growth signal for that step alone
- `ctx.Done()` fires during warmup or hold — the run was interrupted

**PASS** otherwise.

The final ANSWER is the largest N where the verdict is PASS. If a step
TRIPped before any PASS, the answer is `no step passed`. INCONCLUSIVE steps
don't count as PASS and don't stop the ramp.

### Reading the output

Console table at end of run:

```
N        p50    p95    p99    err%    worst-pending-delta             verdict
1000     12     45     89     0.00%   broadcast-worker +12             PASS
2000     14     58     112    0.00%   broadcast-worker +34             PASS
5000     22     94     180    0.01%   broadcast-worker +180            PASS
10000    38     210    430    0.02%   broadcast-worker +890            PASS
20000(10000) 71  480  980    0.04%   broadcast-worker +1240           INCONCLUSIVE
    reasons: inconclusive: only 10000/20000 users activated (pool caps too low)

ANSWER: N = 10000 (last passing step)
        Next limit: broadcast-worker pending +1240 > +1000
```

The `N` column shows `N(EffectiveN)` when they differ — at `N=20000` above
only 10000 users came online (preset cap), so the step is marked
INCONCLUSIVE rather than overstating capacity. The `reasons:` line below
a TRIP/INCONCLUSIVE row says which signal fired.

CSV columns (`--csv=results.csv`):

```
n,effective_n,started_at,p50_ms,p95_ms,p99_ms,error_rate,attempted_ops,failed_ops,
worst_durable,worst_pending_delta,tripped,inconclusive,tripped_reasons
```

One row per step, sorted ascending by N. Use this for post-hoc plotting
or regression comparison across runs.

### Troubleshooting

Symptom → fix matrix for the failure modes that actually happen in real
runs:

| Symptom | Cause | Fix |
|---|---|---|
| Preflight errors with `no subscriptions found in mongo for siteID=...` | Mongo isn't seeded for the preset you're running, or `SITE_ID` differs between seed time and run time. | Run `loadgen seed --workload=messages --preset=<your preset>`. If `SITE_ID` changed, also re-seed (it's a per-site fixture). |
| Gatekeeper logs `user X is not subscribed to room Y` for every send | Preset mismatch between seed and run (fixture IDs differ per preset). | Teardown old preset + seed the new one: `loadgen teardown --workload=messages --preset=<old>` then seed the new one. |
| Gatekeeper logs `siteID mismatch: got X, want Y` | `SITE_ID` env differs between loadgen and gatekeeper. | Set both to the same value. Default is `site-local`. |
| Gatekeeper logs `posting is restricted to owners and admins` | Daily-band rooms have `UserCount` in [500, 2000]; gatekeeper rejects non-thread sends from member-role users when `UserCount > LargeRoomThreshold` (default 500). Documented known limitation. | Either raise `LARGE_ROOM_THRESHOLD` on the gatekeeper (operator-side, no re-seed), or wait for the planned admin-role fixture fix (loadgen-side, needs re-seed). |
| `nats: message does not have a reply` in room-service | Loadgen action handler used `Publish` instead of `Request` for a subject room-service responds on. | Use the latest loadgen — `markRead` was fixed in commit `0bde680` to use `Request`. |
| NATS `permissions violation` on subscribe | Loadgen's `NATS_CREDS_FILE` lacks subscribe rights on `chat.room.>` / `chat.user.>`. | Local dev: `./docker-local/setup.sh` regenerates `backend.creds` with full perms. Production-shaped: extend the chatapp account's `backend` user perms (`nsc edit user --account chatapp --name backend --allow-sub 'chat.room.>' --allow-sub 'chat.user.>'`). |
| All latency columns are 0 even though publishes succeed | No receivers configured (`--max-direct-users=0 --multiplex-pool-size=0`), or the broadcast subscriptions didn't survive the server registration race, or `RoomEvent.LastMsgID` isn't matching. | Set at least one of `--max-direct-users` or `--multiplex-pool-size` > 0. If still empty, check for `broadcast decode failed` warnings in the loadgen log — model drift between loadgen and broadcast-worker can break unmarshaling. |
| Step says `INCONCLUSIVE: only 10000/20000 users activated (pool caps too low)` | `max(--steps)` exceeded `preset.Users` (10000). | Trim `--steps` so its max is ≤ 10000, or change `preset.Users` in `preset.go` for that preset (and re-seed). |
| Loadgen process sits at 100% CPU for many minutes after startup, no output | Fixture build for very large `preset.Users`. Look for `INFO building fixtures preset=X users=Y` followed by `INFO fixtures built ... elapsed=Zs`. | At the default `preset.Users=10000` this is ~1s. If you've bumped it much higher, expect proportional time. |
| `start-of-hold pending poll failed` logged but the run continues | NATS `/jsz` endpoint is flaky. The step proceeds without the pending-growth signal; the other four signals still produce a verdict. | If persistent, set `NATS_MONITORING_URL` to a stable URL. |

### Known limitations

These are documented intentional shortcomings, not bugs to fix in a normal
run:

- **Large-band rooms are gatekeeper-blocked.** Daily fixtures have ~3 large rooms per user with `UserCount` in [500, 2000]; the gatekeeper rejects non-thread sends from member-role users to these. Roughly 3/56 = 5% of `sendMessage` calls land on a large room and fail. Workarounds: raise `LARGE_ROOM_THRESHOLD` (operator side) or change fixtures to seed users as RoleAdmin in large rooms (loadgen side, requires re-seed).
- **Auth-service JWT minting is a no-op stub.** `mintJWT` exists in `prodEnvFactory.Build` but doesn't call auth-service. All loadgen connections use the shared `backend.creds`. To exercise per-user auth, implement `mintJWT` and have `directPool.Add` open the user's conn with the minted JWT.
- **Service-error signal is dormant.** The verdict's `service_errors > 0 → trip` arm is wired but the URL map is empty because backend services don't expose `/metrics`. To enable: add a Prometheus endpoint per service and populate `svcURLs` in `prodEnvFactory.Build`.
- **CPU% in self-metrics is disabled.** The earlier goroutine-count-as-CPU proxy made the tool unusable at scale (every step INCONCLUSIVE above ~4000 users). Real CPU measurement (gopsutil) is a follow-up. The GC pause p99 signal still fires the loadgen-saturation INCONCLUSIVE branch.
- **Reconnect / presence storms are out of scope.** That's a separate scenario PR.
- **Cross-site federation (INBOX) is out of scope.** Single-site only.
- **Not a CI gate.** Invoked manually for capacity work; the deploy harness produces a CSV the operator interprets.

### Design references

- `docs/superpowers/specs/2026-05-27-daily-im-load-scenario-design.md` — full spec (goal, scope, behavior model, fixture topology, receiver architecture, ramp protocol, SLO definitions, risks).
- `docs/superpowers/plans/2026-05-27-daily-im-load-scenario.md` — implementation plan (file structure, task decomposition).
- `tools/loadgen/daily.go`, `daily_pool.go`, `daily_actions.go`, `daily_verdict.go`, `daily_report.go`, `preset.go` — implementation.

## Large-room bot scenario (max-room-size)

Finds the largest room a bot can blast at a fixed send rate before an SLO
signal breaks — gating on **notification-worker** consumer backlog as the
headline O(N)-per-message signal (notification-worker is NOT exempt here,
unlike the daily scenario).

### Quick start

```
make -C tools/loadgen/deploy up
make -C tools/loadgen/deploy seed-botroom PRESET=botroom-medium
make -C tools/loadgen/deploy run-max-room-size PRESET=botroom-medium RATE=200
```

### Presets

| preset          | sizes                      | rooms/size | users | use case          |
|-----------------|----------------------------|------------|-------|-------------------|
| `botroom-small` | 50, 100, 200               | 4          | 300   | smoke / dev       |
| `botroom-medium`| 100, 500, 1000, 2000, 5000 | 4          | 5500  | default capacity  |

### Flags

`--rate` (required, bot msgs/sec split across the step's rooms), `--sizes`
(default `100,500,1000,2000,5000`), `--rooms-per-size` (default 4), `--reads`
(room-service read rate, default 0 = off), `--warmup`/`--hold`/`--cooldown`,
`--stop-on-trip`, `--slo-p95`/`--slo-p99`/`--slo-error-rate`/`--slo-pending-growth`,
`--rate-tolerance`, `--seed`, `--csv`.

### Reading the output

A per-step table (size, rooms, rate, e2 p50/p95/p99, err%, worst-pending, verdict)
followed by `ANSWER: max room size = N` — the largest size where every SLO
signal held — and a `Next limit:` line naming the first signal that tripped.

### One room vs many

`--rooms-per-size=1` concentrates the rate on a single room — probes the
Cassandra hot-partition (`messages_by_room` key `(room_id, bucket)`) and the
Mongo room-doc write contention (`UpdateRoomLastMessage`). The default `4`
spreads the rate to measure aggregate fan-out plus member-list cache churn.

### Add-path past 1000 members

To test adding members to rooms larger than the old 1000 cap, the loadgen
deploy sets room-service `MAX_ROOM_SIZE=6000` and ships a `members-capacity-xl`
preset; run e.g. `make -C tools/loadgen/deploy run-capacity PRESET=members-capacity-xl TARGET_SIZE=5000`.

### v2 follow-ups (not yet built)

- Create-and-blast: bots create a ~100-member room and immediately send
  (cold-cache penalty).
- Live N-connection pool to measure NATS core delivery fan-out to real member
  connections.

## Presence workload

Two subcommands that benchmark `user-presence-service` over NATS. No
Mongo seeding is required: both use synthetic accounts (`u-NNNNNN`) that
the service accepts via the JWT self-token on `hello`/`ping`/`activity`/`bye`
without looking them up in any store.

**NATS credentials.** Both subcommands read the same `NATS_URL`,
`NATS_CREDS_FILE`, and `SITE_ID` env vars as every other loadgen subcommand.
The credentials must permit publishing on `chat.user.*` and subscribing to
`chat.user.presence.state.*`. The docker-local `backend.creds` covers both.

**In-repo tests** use an embedded NATS server with a fake presence responder,
so no Docker stack is needed for unit testing. Integration coverage against
the real `user-presence-service` (which needs Docker + Valkey) is a CI
concern.

### presence-sustained — find max sustainable population

Finds the maximum presence population N that the service can sustain
within SLO. It ramps N through `--steps`: at each step it activates
the delta of new users (each sends `hello`), warms up, then holds while
users heartbeat (`ping`, a no-op at the service) and churn (activity
flips and reconnects). Graded on:

- state-publish latency p95/p99 (`--p95-ms` / `--p99-ms`)
- error rate: missing observations + publish failures (`--error-rate`)
- loadgen self-saturation INCONCLUSIVE guard (GC pause)

Reports the largest N where every signal passed.

```
loadgen presence-sustained --steps=1k,2k,5k,10k --hold=120s --csv=presence.csv
```

### presence-storm — find largest survivable reconnect storm

At a fixed warmed population (`--users`), ramps the dropped-and-reconnected
fraction through `--storm-steps`. Two storm modes:

- `--storm-mode=graceful` — drops users via `bye` then re-`hello`s; pure
  thundering-herd.
- `--storm-mode=silent` — stops pinging until the sweeper marks users
  offline, then re-`hello`s; models a gateway blip and also exercises
  the offline sweeper.

Per fraction it grades recovery time vs `--recovery-slo`, spike p99
(`--p99-ms`), and error rate (`--error-rate`). Reports the largest fraction
that recovered within SLO.

```
loadgen presence-storm --users=20000 --storm-steps=0.1,0.25,0.5,1.0 --storm-mode=graceful
```

### presence-capacity — find max concurrent online users

Cumulatively ramps a synthetic population through `--steps`. Each step
activates the delta of new users (each `hello`, which measures connect-edge
latency), then holds with every user online and heartbeating, counting
**false offlines** (users the service wrongly swept offline) and **ping
sustainability**. Reports the largest N held without tripping.

- Connect-edge latency (`hello`→`online`) is measured during activation; the
  steady-state hold has no transitions to time.
- False offlines are the ceiling signal. A loadgen-induced ping shortfall
  reads INCONCLUSIVE, never TRIP, so the load box is never mistaken for a
  service limit.

Graded on connect p95/p99 (`--connect-p95-ms` / `--connect-p99-ms`), false-
offline rate (`--false-offline-rate`), connect error rate (`--error-rate`),
with a ping-sustainability + GC-pause INCONCLUSIVE guard.

```
loadgen presence-capacity --steps=10k,20k,50k,100k,200k --hold=120s --csv=cap.csv
```
