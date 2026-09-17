# Verifying Message Durability Across a Cassandra Outage

How to confirm, locally, that `message-worker` loses no message history while
Cassandra is down — and that clients are told the history is incomplete rather
than shown a gap as truth.

Start at Level 0 if you want to see the original loss happen before watching the
fix prevent it; a verification that only ever runs against fixed code cannot tell
you whether the fix did anything.

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

## Level 0 — prove the bug was real (~5 min)

Everything below verifies that the fix *holds*. None of it distinguishes "the fix
works" from "the outage was too short to have lost anything anyway". Running the
same outage against pre-fix code does, and it takes about five minutes.

**The lever is one env var, and the asymmetry is the fix in miniature.**
`CONSUMER_MAX_DELIVER=3`:

- On **pre-fix `main`** it takes effect. `stream.WithOutageRetryBudget` raises a cap
  only when it still equals `DefaultMaxDeliver` (6), so an explicit 3 survives.
- On **this branch** it is ignored. `buildConsumerConfig` applies
  `stream.WithUnlimitedRedelivery`, which pins `MaxDeliver = -1` before the backoff
  schedule is derived, and `validateConsumerConfig` refuses to start otherwise.

With a cap of 3 the message is destroyed about **six seconds** into the outage —
deliveries land at roughly t=0, t=1s and t=6s on `DefaultBackoff`'s first rungs —
so neither half of this needs the hour that a realistic cap would.

### A. Pre-fix: watch a message disappear

```bash
git worktree add ../newchat-prefix origin/main
cd ../newchat-prefix
# add to message-worker/deploy/docker-compose.yml under environment:
#   - CONSUMER_MAX_DELIVER=3
make deps-up && make up-detached && make ui-up
```

Send one message and confirm it reaches history, so the path is known-good. Then:

```bash
docker stop chat-local-cassandra
# send a second message from the UI; note its id
# wait ~15s — the third delivery fails at ~6s and JetStream terminates it
docker start chat-local-cassandra
# wait ~30s for the consumer to settle
docker exec chat-local-cassandra cqlsh -e \
  "SELECT id FROM chat.messages_by_id WHERE id='<second-id>';"
```

**Zero rows — and that is the whole bug.** The message is still on screen and still
returned by search, because `broadcast-worker` delivered it and
`search-sync-worker` indexed it, and neither touches Cassandra. Nothing logged an
error after the last NAK; the advisory JetStream emitted
(`$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES`) has no consumer in this repo. A
client cannot tell, and neither can an operator.

### B. This branch: watch it survive

Repeat exactly the same steps on this branch, `CONSUMER_MAX_DELIVER=3` included.
The env var is ignored, the message NAKs for as long as the outage lasts, and the
same query returns the row after recovery. The consumer also reports
`max_deliver: -1` rather than 3, which is the env var being overridden in the open.

```bash
cd /path/to/this/branch && make deps-up && make up-detached && make ui-up
# same outage, same query → one row
```

Tear the scratch tree down with `git worktree remove ../newchat-prefix`.

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

This check applies to the **default-mode** `message-worker` consumer only, where
`max_deliver` must be `-1`: anything else means the old loss boundary is back, and
`validateConsumerConfig` refuses to start in that state, so a running default-mode
worker with a finite cap should be impossible — check the logs if you see one.

Teams mode is the deliberate exception. It binds a separate durable
(`message-worker-teams`) and *requires* a finite positive cap, because it settles
through plain `jsretry.Settle` and has no give-up path of its own —
`validateConsumerConfig` rejects an unlimited cap there. Do not apply the `-1`
check to it.

**The marker is set** — after `DEGRADE_MARK_DELAY` (default 30s), not instantly. Writes
have to keep failing for that long before the site is declared degraded, so that a blip
the retry resolves does not raise a 20-minute site-wide notice. Give it a minute before
concluding the marker is broken. It is durable in Mongo, not just in-process:

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
| under ~36s | the 1s / 5s / 30s rungs | seconds after restart |
| ~36s to ~2m36s | the 2m rung | up to 2 min after restart |
| longer | the 10m tail | up to 10 min after restart |

Nominal figures. Equal jitter draws each wait from `[d/2, d]`, so a given message
can land anywhere from half these times to the full value.

**Keep the outage under ~36 seconds for a fast loop.** A two- or ten-minute
backfill after a longer outage is the schedule working, not a failure.

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
| `DEGRADE_MARK_DELAY` | `30s` | How long writes must keep failing before the site is marked degraded. Clears the 1s/5s/30s backoff rungs, so a blip the retry resolves never marks. `0` marks on the first failure. |
| `MESSAGE_BUCKET_HOURS` | `360` | Must match across every service touching `messages_by_room`. |
| `METRICS_ADDR` | `:9090` | Prometheus endpoint, path `/metrics`. Needs `O11Y_ENABLED=true`. |

**Ops prerequisite this procedure cannot check:** with unlimited redelivery,
`MESSAGES-CANONICAL-{siteID}` retention is the only remaining durability
boundary. `MaxAge` must exceed the target outage and `MaxBytes` must hold the
backlog (~122 MB/hour at ~34 msg/s × 1 KB). A local run will not surface a
production retention that is too short.
