# Verifying Message Durability Across a Cassandra Outage

How to confirm, locally, that `message-worker` loses no message history while
Cassandra is down — and that clients are told the history is incomplete rather
than shown a gap as truth.

Design rationale lives in
`docs/superpowers/specs/2026-08-13-cassandra-outage-message-durability-design.md`.
This is the repeatable procedure, not the reasoning.

## What is actually being tested

Three claims, which fail in different ways and are worth checking separately:

1. **No loss.** A Cassandra outage is *infra-class*, so the write retries
   indefinitely. Nothing may be terminated by JetStream, at any delivery count.
2. **Clients are told.** A site-wide degraded marker drives `incompleteSince` on
   history reads while the marker is set.
3. **Recovery drains.** Every message written during the outage appears in
   Cassandra afterwards, and the marker clears once the backlog is empty.

Claim 1 is the one that used to fail silently: `broadcast-worker` and
`search-sync-worker` never touch Cassandra, so a message dropped by
`message-worker` stayed on screen and stayed searchable while being gone from
history permanently. Watching only the client is how this bug hid.

## Level 1 — the automated guard (~2 min)

```bash
make test-integration SERVICE=message-worker
```

`TestHandler_Integration_CassandraOutageDoesNotLoseMessages`
(`message-worker/integration_test.go`) runs against real Cassandra and Mongo via
testcontainers and covers the whole arc: every write fails for 20 messages, each
is asserted NAK'd and never acked even at `numDelivered` in the thousands; the
marker lands in Mongo; a second tracker that saw no failures of its own adopts it
via `Refresh`; recovery replays all 20 and every id persists; the marker clears
only once the backlog reads empty.

**Its limit:** it drives `processMessage` / `settle` directly with a fake
JetStream message. It proves the give-up *decision* is right. It does not prove
the consumer actually holds messages across an outage, because no broker is
involved. That is what Level 2 exists for.

## Level 2 — the real stack

### Bring it up

```bash
make deps-up          # NATS, Mongo, Cassandra, Valkey, Vault, …
make up-detached      # all services
make ui-up            # chat frontend on :3000
```

To read the durability metrics, message-worker needs observability switched on —
it is off by default (zero-impact) and `message-worker/deploy/docker-compose.yml`
does not set it. Either add `- O11Y_ENABLED=true` to that file's `environment:`
block, or run `make dev SERVICE=message-worker` against the shared deps, which
sets it for you. Without it every step below still works except the metric reads.

Send a few messages from the UI first and confirm they reach Cassandra, so the
happy path is known-good before anything is broken.

### Induce the outage

```bash
docker stop chat-local-cassandra
```

Now send more messages from the UI, and note the ids. They will still appear in
the room immediately — that is fan-out, which has no Cassandra dependency. Their
absence from history is the thing under test.

### Checks to run while it is down

**Nothing is being terminated.** The durable consumer must show `max_deliver: -1`
and a growing backlog, with no terminations:

```bash
curl -s 'localhost:8222/jsz?consumers=true' \
  | jq '.. | objects | select(.name? == "message-worker")
        | {num_pending, num_ack_pending, max_deliver: .config.max_deliver}'
```

`max_deliver` anything other than `-1` means the deployment overrode
`CONSUMER_MAX_DELIVER` and the old loss boundary is back. The service refuses to
start in that state (`validateConsumerConfig`), so a running worker with a finite
cap should be impossible — check the logs if you see one.

**The marker is set**, and is durable in Mongo rather than only in-process:

```bash
docker exec chat-local-mongodb mongosh chat --quiet \
  --eval 'db.history_degradations.find().pretty()'
```

One document per site, `_id` is the site id (`site-local` by default).

**Clients are told.** Scroll the room in the UI, or call the history RPC
directly, and the response carries `incompleteSince`. On a healthy site the field
is absent from the JSON entirely, so its presence is the signal.

**The failure class is visible** (needs `O11Y_ENABLED=true`):

```bash
docker compose -f message-worker/deploy/docker-compose.yml exec message-worker \
  wget -qO- localhost:9090/metrics | grep message_worker_
```

`message_worker_history_write_failures_total{class="infra"}` should be climbing
and `message_worker_history_dropped_total` should stay absent — a counter does
not appear in `/metrics` until its first increment, so an absent drop counter is
the pass condition, not a broken scrape.

### Recover

```bash
docker start chat-local-cassandra
```

Then confirm every id you sent during the outage is present:

```bash
docker exec chat-local-cassandra cqlsh -e \
  "SELECT id, msg FROM chat.messages_by_id WHERE id='<msg-id>';"
```

`messages_by_id` is the table to check by hand. `messages_by_room` is partitioned
by `(room_id, bucket)` where the bucket derives from `MESSAGE_BUCKET_HOURS`
(default 360), so a naive `WHERE room_id=` query there needs the bucket too.

## Timing — so you are not waiting an hour

The no-loss property needs **no waiting**. An infra-class failure retries
forever; there is no deadline to outlast. What you wait for is the redelivery
rung, because `jsretry.DefaultBackoff` (`1s, 5s, 30s, 2m, 10m`) is a compile-time
schedule, not an env knob:

| Outage length | Messages parked on | Backfill lands |
|---|---|---|
| under ~35s | the 1s / 5s / 30s rungs | seconds after restart |
| longer | the 10m tail | up to 10 min after restart |

**Keep the outage under ~35 seconds for a fast loop.** A ten-minute backfill
after a long outage is the schedule working, not a failure.

Two other clocks, both of which look like bugs if you do not know them:

- **The marker lingers ~20 minutes after the backlog drains.** `drainTailGrace`
  is `2 ×` the backoff tail (`message-worker/degrade.go`), so `incompleteSince`
  keeps appearing for that long post-recovery by design. The tail clock starts
  when `NumPending` first reads empty, and only a *write failure* restarts it —
  ordinary traffic does not.
- **`INVALID_RETRY_WINDOW` (default 1h) gates only the drop path**, which
  requires a *request*-class error. It never fires for a stopped Cassandra, so it
  has no bearing on this procedure.

## Exercising the drop path

Stopping Cassandra cannot produce a drop. To see destruction and the two brakes,
you need a request-class failure — a schema fault — and a short window:

```bash
# in message-worker/deploy/docker-compose.yml, or the shell running `make dev`
INVALID_RETRY_WINDOW=30s
```

```bash
docker exec chat-local-cassandra cqlsh -e "DROP TABLE chat.messages_by_id;"
```

Send messages and watch:

- `message_worker_history_write_failures_total{class="request"}` climbs
  immediately — this is the leading indicator, visible *before* the window
  elapses and anything is destroyed.
- After ~30s of accumulated retries, `message_worker_history_dropped_total{code="invalid"}`
  starts counting. Any non-zero value here is data destruction.
- Push past 10 drops in a minute and
  `message_worker_history_drop_suppressed_total{reason="rate_limited"}` appears —
  the unattended cap holding the rest of the feed together.
- Set `HISTORY_DROP_ENABLED=false` and restart: drops stop entirely and the
  reason becomes `disabled`.

Restore the schema afterwards:

```bash
docker compose -f docker-local/compose.deps.yaml --profile init run --rm cassandra-init
```

**Note for post-incident migrations:** set `HISTORY_DROP_ENABLED=false` before any
schema migration that follows an outage, until
[#383](https://github.com/hmchangw/newchat/issues/383) lands. The retry window is
measured from `NumDelivered`, which also counts deliveries burned on unrelated
failures, so a message pre-aged by a backlog replay can be dropped on its first
request-class error having never actually retried it.

## Reference

| Knob | Default | What it does |
|---|---|---|
| `CONSUMER_MAX_DELIVER` | `-1` | Unlimited redelivery. Startup fails if `>= 0`. |
| `INVALID_RETRY_WINDOW` | `1h` | How long a request-class failure retries before the drop. A floor, not a target — measured against the jittered minimum. |
| `HISTORY_DROP_ENABLED` | `true` | Operator kill switch for destruction. |
| `MAX_DROPS_PER_MINUTE` | `10` | Per-pod cap on destruction. |
| `DEGRADE_REFRESH_INTERVAL` | `5s` | How often a pod re-reads the site marker. |
| `MESSAGE_BUCKET_HOURS` | `360` | Must match across every service touching `messages_by_room`. |
| `METRICS_ADDR` | `:9090` | Prometheus endpoint, path `/metrics`. Needs `O11Y_ENABLED=true`. |

**Ops prerequisite this procedure cannot check:** with unlimited redelivery,
`MESSAGES-CANONICAL-{siteID}` retention is the only remaining durability
boundary. `MaxAge` must exceed the target outage and `MaxBytes` must hold the
backlog (~122 MB/hour at ~34 msg/s × 1 KB). A local run will not surface a
production retention that is too short.
