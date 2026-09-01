# NATS / JetStream Behavior and Loadgen Coverage

> Verified against the code on 2026-08-21. Scope: the streams and services that
> carry ordinary chat traffic. Federation, push, bot, Teams, HR and migration
> lanes are out of scope and are listed in §5.
>
> Loadgen generates traffic and records what it can observe. It does not inject
> faults and it does not decide whether the campaign passed.

## 1. Dependency behavior that changes how a result reads

| Behavior | In the code | Why it matters when reading a result |
|---|---|---|
| Core reconnect | `pkg/natsutil.Connect` sets `MaxReconnects(-1)` with a 2s interval | The connection recovers on its own, which is not the same as the request that was in flight succeeding |
| Core publish | No server PubAck; a publish during a disconnect rides the client reconnect buffer | A full buffer fails synchronously. Publish success means the write entered the client path, not that a subscriber processed it |
| Request/reply | One timeout, no application retry | Failover shows up as a timeout or as no-responders. A side-effecting RPC whose reply was lost may still have run |
| Readiness | `RECONNECTING` counts as ready; only `DISCONNECTED`/`CLOSED` do not | A pod stays ready for as long as it keeps reconnecting, while none of its NATS-backed functionality works |
| Consumer defaults | AckWait 30s, `MaxDeliver=6` (`pkg/stream/consumer.go:18`), `MaxAckPending=1000` | Retries are finite. An outage longer than the delivery budget ends in a terminal drop, not in a pending backlog |
| Retry backoff | `jsretry.DefaultBackoff` 1s/5s/30s/2m/10m; low-latency 200ms/1s/5s/30s | Against `MaxDeliver=6` the client-side budget is ~12.6 minutes — but `message-worker` (17) and `broadcast-worker` (18) opt into `stream.WithOutageRetryBudget` and span ~2 h. Handler-error exhaustion is counted by `chat_nats_terminal_failures_total{reason="max_deliver"}`; advisories add the un-acked paths and attribution |
| Consumer iterator | Ordinary reconnects and leader changes are survived. On a terminal error (e.g. the durable was deleted) most workers return from the goroutine | The process stays alive and ready while nobody consumes the durable |
| Panic guard | A per-message handler panic Acks and drops the message; a search batch panic leaves messages unacknowledged | A drop is an event-integrity outcome, not a uptime outcome. Service health will not show it |
| Startup | Services exit when the initial connection, JetStream context, stream lookup or consumer creation fails | A restart during an outage crash-loops. Read it as its own scenario, never as steady-state degradation |

## 2. Streams, services, and what loadgen can prove

| Stream / path | Producer → consumer | Retry and Ack behavior | Loadgen coverage |
|---|---|---|---|
| `MESSAGES-{site}` | client publish → message-gatekeeper | Transient gatekeeper errors Nak on `jsretry.DefaultBackoff` against `MaxDeliver=6` (~12.6 min), not immediately | **Reconciled**: admission is an observer on every message lane |
| `MESSAGES-CANONICAL-{site}` | gatekeeper, history-service mutations, room-worker system events → message-worker, broadcast-worker, notification-worker, search-sync-worker | Each consumer redelivers independently; downstream side effects must be idempotent | **Partial**: message persistence is reconciled in Cassandra and delivery per recipient when the recipient observer is on. notification and search-sync outcomes are not asserted |
| `ROOMS-{site}` | room-service → room-worker | JS PubAck on publish; room-worker at the repo default `MaxDeliver=6`; several side effects follow the Mongo write | **Reconciled**: member add/remove, rename, mute, room create and read receipt settle through `room_state` |

| Service | Loadgen coverage | What is not asserted |
|---|---|---|
| message-gatekeeper | **Reconciled** (admission) | Its durable is not sampled. Delivery-budget exhaustion **is** counted by `chat_nats_terminal_failures_total{reason="max_deliver"}` when the handler errors on the final delivery; the un-acked paths and reply loss are not enumerated |
| message-worker | **Partial** | Backlog sampling plus soak read-back exist; the Mongo-side thread writes are not read back |
| broadcast-worker | **Partial** | Per-recipient delivery is exact with the recipient observer on; DM and partial-fanout loss is not |
| notification-worker | **Traffic only** | Backlog and mute invalidation are not monitored |
| room-service / room-worker | **Reconciled** for the five mutation lanes | The rest of the RPC surface, and room-worker's post-write side effects |
| history-service | **Traffic only** | Post-timeout side-effect idempotency across the full RPC surface |
| user-service | **Traffic only** | 14 reads are driven uniformly; no write is reconciled |
| user-presence-service | **Traffic only** | The storm models silent application clients, not a transport failover |
| search-service / search-sync-worker | **Traffic only** | Query availability and index convergence; the index observer is refused at startup |

## 3. Paths that lose work quietly

Each of these ends with an Ack or a swallowed error, so nothing downstream ever
learns the work was dropped.

| Path | Behavior | What it looks like |
|---|---|---|
| message-gatekeeper transient failure | `jsretry.Nak` with `DefaultBackoff` against `MaxDeliver=6` (`handler.go:212`) — **not** a bare `Nak`, contrary to earlier revisions of this table | The budget spans ~12.6 minutes, so a brief blip cannot exhaust it. An outage *longer* than that still ends in a terminal drop while the stream looks healthy |
| broadcast-worker DM and partial thread fanout | Individual Core publish failures are logged, processing continues, the canonical event is Acked | Some recipients never receive the message and nothing retries. The send still reconciles `good` |
| room-worker post-write side effects | Some subscription/client/INBOX publish failures are logged and swallowed | Mongo is updated and the corresponding event never exists. Divergence with no error anywhere |
| Consumer-loop terminal error | The goroutine returns and is not recreated | Pending climbs on a durable nobody is reading, behind a green process and a passing readiness probe |
| Reply-loss on a side-effecting RPC | The request ran, the reply did not arrive | A client retry can duplicate the effect. Loadgen never retries a mutation for this reason; it reads state back instead |
| Loadgen's own `daily` connection pool | Raw default reconnect policy, no connection-state telemetry | A generator-side disconnect is indistinguishable from a system failure. The soak pool does emit `loadgen_nats_*`; `daily` does not |

## 4. Reading the results

Terminal results per operation, and what each one licenses you to say:

| Result | Meaning | Reading |
|---|---|---|
| `good` | Every required observer confirmed the effect | Accepted and delivered |
| `bad` | An observer saw a state that could not legally occur | A real violation — a duplicate delivery, an unexpected recipient, a mismatched identity |
| `missing_after_deadline` | Admission recorded `good`, the effect never appeared | The strongest loss claim available |
| `unverified` | The observer could not answer | Not evidence of loss |
| `not_sent` | The publish provably never left loadgen | No effect expected |

NATS-specific reading traps:

- **A terminal drop is invisible to the ledger unless advisories are
  collected.** `MaxDeliver` exhaustion produces an advisory, not an error the
  producer sees. Without it, an exhausted message and a message still being
  retried look the same from consumer pending alone.
- **Redelivery is normal.** At-least-once means duplicates are expected; only
  a duplicate *business effect* is a violation. That is what the recipient
  observer's cardinality check exists to separate.
- **Consumer pending is the primary backlog signal** for every asynchronous
  path, and it is per leader. Deduplicate by the current-leader label or a
  replicated consumer counts several times.

The query-time rules for turning these counters into `VALID` / `INCONCLUSIVE`,
impact and correctness live in
[`../loadgen/dashboard-contract.md`](../loadgen/dashboard-contract.md). Metric
names and labels are in
[`nats-metrics-contract.md`](nats-metrics-contract.md).

## 5. Out of scope this round

- **Federation**: `OUTBOX-{site}`, remote `INBOX-{site}`, outbox-worker per-peer
  isolation and ordered-lane FIFO, inbox-worker convergence — next round, and
  loadgen has no cross-site traffic to drive them with today.
- **Push delivery**: `PUSH-NOTIFICATION-{site}` and push-notification-service.
  There is no recipient-correlated push observer, and the current dispatcher is
  a log dispatcher.
- **Bot, Teams, HR and migration lanes**: `BOT-MESSAGES-CANONICAL`,
  `MESSAGES-TEAMS`, `ROOMS-TEAMS`, `HR`, `MIGRATION-OPLOG`. Not part of
  ordinary operation; `botroom` mode is synthetic and does not drive the real
  bot chain.
- **auth-service NATS callout**. Loadgen uses an auth stub, so per-user JWT
  connection behavior during a callout failure is untested.

## 6. Code evidence

- Connection, reconnect, health: `pkg/natsutil/`
- Retry backoff: `pkg/jsretry/`
- Stream and consumer configuration: `pkg/stream/stream.go`, each service's `main.go`
- Subject builders: `pkg/subject/`
- Loadgen lanes, observers and the ledger: `tools/loadgen/soak_*.go`, `tools/loadgen/failure_*.go`
