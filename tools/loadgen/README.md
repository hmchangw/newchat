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
make -C tools/loadgen/deploy teardown PRESET=medium  # drop Mongo + Valkey fixtures
make -C tools/loadgen/deploy down                     # stop containers
```

## Encryption

`broadcast-worker` runs with `ENCRYPTION_ENABLED=true` by default in this
stack. `loadgen seed` provisions one P-256 keypair per fixture room into
Valkey (the same Valkey `broadcast-worker` reads from), derived from the
RNG seed so runs stay reproducible. To run an apples-to-apples plaintext
comparison:

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
  MongoDB with fixtures and Valkey with per-room keypairs.
- `loadgen run --preset=<name> [flags]` — open-loop publish at `--rate`
  msgs/sec for `--duration`, print a summary at the end. Flags:
  `--seed`, `--warmup`, `--inject=frontdoor|canonical`, `--csv=<path>`.
- `loadgen teardown --preset=<name> [--seed=42]` — drop the seeded
  Mongo collections and delete the per-room Valkey keys for the preset.

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
(ROOMS stream) → `room-worker` → `chat.room.{roomID}.event.member` broadcast.

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
| `members-medium`   | 100   | 100      | 500            | sustained-throughput default            |
| `members-capacity` | 5     | 1        | 990            | capacity-growth, fills up to ~MAX_ROOM_SIZE |

### Subcommands

- `loadgen seed --workload=members --preset=<name>` — populate Mongo
  + Valkey for the members workload.
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
  non-zero errors → over capacity. If you see `aborted early — pools
  exhausted` in the logs, your `rate × duration × users-per-add` exceeded
  the preset's `CandidatePool` budget; pick a bigger preset or shorter
  duration.
- **Capacity mode**: the size-bucket table shows latency at four
  size ranges; the `final sizes` block confirms each room hit
  `--target-size`. A row with `count > 0` whose `e2_p99` is much larger
  than smaller-size buckets indicates a per-room-size degradation.

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
in addition to the standard Mongo/Valkey/NATS env. `MESSAGE_BUCKET_HOURS`
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
  (users/rooms/subscriptions/thread\_rooms), Valkey (room keys, harmless
  for read workload), and Cassandra (messages\_by\_room,
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

## max-rps — auto-find Max RPS under SLO

Automatically finds the maximum RPS each workload can sustain while all
SLO signals hold. The subcommand ramps the target rate through an ordered
list of steps, holds at each step for a measurement window, evaluates SLO
signals, and reports the largest step at which every signal passed.

```bash
loadgen max-rps --workload=messages|history --preset=<name> [flags]
```

### Quick start

```bash
# messages: ramp 500..10k rps, stop at first SLO breach
loadgen max-rps --workload=messages --preset=medium --steps=500,1k,2k,5k,10k

# history: per-endpoint SLO, custom p95
loadgen max-rps --workload=history --preset=history-medium --steps=200,500,1k,2k --slo-p95=80ms
```

Via the deploy Makefile:

```bash
make -C tools/loadgen/deploy run-max-rps PRESET=medium
make -C tools/loadgen/deploy run-max-rps WORKLOAD=history PRESET=history-medium STEPS=200,500,1k,2k
```

### Flags

| Flag | Default | Notes |
|------|---------|-------|
| `--workload` | `messages` | `messages` or `history` |
| `--preset` | (required) | an existing preset for the chosen workload |
| `--steps` | messages `500,1k,2k,5k,10k` / history `200,500,1k,2k,5k` | explicit ordered RPS list; `k` suffix = ×1000 |
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

```
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
