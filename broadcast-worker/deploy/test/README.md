# Broadcast Worker Local Test Harness

Zero-code way to exercise the `broadcast-worker` against a locally running
`docker compose` stack. Covers four scenarios:

| Scenario              | What it tests                                                    |
|-----------------------|------------------------------------------------------------------|
| `group-plain`         | Group message, no mentions. Updates `rooms.lastMsgAt`.           |
| `group-mention-bob`   | Group message with `@bob`. Flips `subscriptions.hasMention`.     |
| `group-mention-all`   | Group message with `@all`. Updates `rooms.lastMentionAllAt`.     |
| `dm-cross-site`       | DM published on site1, received on site2 via supercluster gateway. |

## Prerequisites

- `docker` + `docker compose`
- `make`, `bash`
- Free host ports: `4222`, `4223`, `8222`, `8223`, `27017`, plus `8090` if
  running `tools/nats-debug` alongside.

## Topology

```
          +-------------+    gateway    +-------------+
          |  nats_site1 | <===========> |  nats_site2 |
          +------^------+               +-------------+
                 |
                 | publishes core NATS room/user events
                 |
          +------+------------+
          | broadcast-worker  |-- reads rooms/subs ---> +---------+
          +-------------------+                          | mongodb |
                                                         +---------+
          +-------+
          | tools |  (nats-box shell for publish.sh)
          +-------+
```

JetStream is **per-site** (not federated). Only core NATS pub/sub crosses
the gateway — which is exactly what the broadcast-worker emits.

## Make targets (run from repo root)

```
make -C broadcast-worker/deploy up                          # build + start stack
make -C broadcast-worker/deploy seed                        # reset + load fixtures
make -C broadcast-worker/deploy send SCENARIO=<name>        # publish scenario event
make -C broadcast-worker/deploy verify SCENARIO=<name>      # assert MongoDB state
make -C broadcast-worker/deploy logs                        # tail broadcast-worker logs
make -C broadcast-worker/deploy down                        # tear down + wipe volumes
```

Valid `SCENARIO` values: `group-plain`, `group-mention-bob`,
`group-mention-all`, `dm-cross-site`.

## Demo flow for managers (~5 minutes)

### 1. Bring up the stack

```
make -C broadcast-worker/deploy up
```

Verify services:

```
docker compose -f broadcast-worker/deploy/user/docker-compose.test.yml ps
```

Expected: `nats_site1`, `nats_site2`, `mongodb`, `broadcast-worker`, `tools`
all running.

### 2. Seed fixtures

```
make -C broadcast-worker/deploy seed
```

Prints `users: inserted 3`, `rooms: inserted 2`, `subscriptions: inserted 4`.

### 3. Start the observer UI (second terminal)

```
docker compose -f tools/nats-debug/deploy/docker-compose.yml up
```

Open http://localhost:8090.

### 4. Scenario 1 — group message

In nats-debug:

- **Source NATS:** `nats://localhost:4222`
- **Dest NATS:** `nats://localhost:4222`
- Subscribe to `chat.room.group-1.event`

```
make -C broadcast-worker/deploy send SCENARIO=group-plain
```

UI shows one event with `type: "new_message"`, `roomType: "group"`, sender
enriched (`engName: "Alice Wang"`).

```
make -C broadcast-worker/deploy verify SCENARIO=group-plain
```

Prints `OK: rooms.group-1 lastMsgId=m-group-plain lastMsgAt=2026-04-17T12:00:00.000Z`.

### 5. Scenario 2 — group message with `@bob`

Keep the same subscription.

```
make -C broadcast-worker/deploy send SCENARIO=group-mention-bob
make -C broadcast-worker/deploy verify SCENARIO=group-mention-bob
```

UI: the event includes a `mentions` array with bob's enriched record.
Verify: `OK: subscriptions(bob, group-1) hasMention=true`.

### 6. Scenario 3 — group message with `@all`

```
make -C broadcast-worker/deploy send SCENARIO=group-mention-all
make -C broadcast-worker/deploy verify SCENARIO=group-mention-all
```

UI: event has `mentionAll: true`.
Verify: `OK: rooms.group-1 lastMentionAllAt=2026-04-17T12:02:00.000Z`.

### 7. Scenario 4 — cross-site DM (the highlight)

In nats-debug, change **Dest NATS** to `nats://localhost:4223` (site2) and
subscribe to `chat.user.carol.event.room`.

Say to the audience: *the message is published on site1's JetStream and the
broadcast-worker lives on site1. Carol's client listens on a separate NATS
server (site2). The two servers are bridged by a NATS supercluster gateway.*

```
make -C broadcast-worker/deploy send SCENARIO=dm-cross-site
```

The event appears on the site2 subscription — broadcast crossed the gateway.

### 8. Tear down

```
make -C broadcast-worker/deploy down
```

## How re-running scenarios behaves

Every scenario file is fully static — same `message.id`, same `createdAt`,
same `timestamp` every time. Re-running a scenario republishes the identical
event; the broadcast-worker deterministically overwrites MongoDB state with
the same values. For observation in nats-debug that's fine: every publish
still produces a fresh NATS broadcast.

## FAQ

**Why do the publish/verify scripts call `docker compose exec` even though
`up` is already running?**

`up` started the compose stack; `exec` attaches a command to an
already-running service (the long-lived `tools` nats-box container, or the
`mongodb` container). This does **not** spin up a new container — it reuses
what `up` started. The alternative (`docker compose run --rm`) would create
a throwaway container per call, which is slower and noisier.

**Why is there a `tools` service?**

To avoid requiring the `nats` CLI on the host. The `tools` service runs
`natsio/nats-box` with `sleep infinity` so `publish.sh` can
`exec tools nats pub ...` into it.

**Why one MongoDB instead of one per site?**

For a local test harness, a shared MongoDB is enough — documents are
scoped by `siteId`. The supercluster scenario only needs cross-site NATS
routing, not cross-site storage.
