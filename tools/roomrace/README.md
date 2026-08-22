# roomrace

Reproduces and measures the two "new room → first message" races against a real
NATS server, using the production subject builders from `pkg/subject` so the
topology under test is the one the services actually publish on.

```sh
./tools/roomrace/run.sh
```

## What it models

| Actor | Modelled as |
|---|---|
| `room-worker` | publishes `subscription.update{action:"added"}` on `chat.user.{B}.event.subscription.update` |
| `broadcast-worker` | DM → `chat.user.{B}.event.room` (per member); channel → `chat.room.{id}.event` |
| `message-worker` | persists the message before the fan-out, so a backfill can recover it |
| `history-service` | request/reply stub on the real `…msg.history` subject |
| desktop client B | login-time subs to its own user subjects; per-room sub opened only when it handles `subscription.update`, after a configurable render delay |

`-render-ms` is how long client B takes to render the new chat and open the room
subscription. `-send-ms` sweeps the delay between `subscription.update` and the
first message — the axis the race turns on.

## Scenarios

| Scenario | What it shows |
|---|---|
| `dm / client drops unknown room` | Problem 1 as reported: the event is delivered, the client discards it |
| `dm / client buffers unknown room` | Problem 1 is fixable client-side, no backend change |
| `channel / subscribe on update` | Problem 2 as reported |
| `channel / subscribe + flush` | `Flush()` alone does not close it |
| `channel / subscribe + flush + backfill` | backfill over the existing `msg.history` RPC closes it |
| `channel / server join grace window` | server dual-publishes to `chat.user.{B}.event.room` for fresh members |
| `channel / grace window, client still drops` | the server fix needs the client fix; alone it does nothing |

## Interest window

Before the scenarios, the harness measures how long after `Subscribe()` returns a
publisher actually starts reaching the new subscription. That is the floor under
Problem 2 — no client can subscribe faster than this — and it is the number that
grows when the publisher sits on a different server (cluster) or a different site
(gateway).

## Topologies

`docker-compose.yml` starts one standalone server (`:4222`) and a 3-node full-mesh
cluster (`:4223`, `:4224`, `:4225`). Point `-sub-url` and `-pub-url` at different
cluster nodes to include route propagation, which is the production shape.
