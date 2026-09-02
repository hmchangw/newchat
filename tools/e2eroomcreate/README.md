# e2eroomcreate

Replays the room-creation preview race end to end against five real service
binaries and prints a per-action timeline.

## Scenario

1. Alice calls the create-room RPC naming Bob.
2. Alice's client waits for `subscription.update`.
3. Alice and Bob both receive `subscription.update` and subscribe to the room subject.
4. Alice sends `"new message"` right after her subscribe succeeds.
5. `broadcast-worker` fans out `new_message` on the room subject.
6. Bob receives it only if his `SUB` was registered before the publish.
7. Alice always receives it — she sends only after her own subscribe succeeded.

`--bob-delay` models how long Bob's client spends between handling
`subscription.update` and having its `SUB` registered (render, state update).
Both clients use independent NATS connections, as in production.

## Running

Needs NATS (JetStream), MongoDB and Cassandra on the default local ports, the
Cassandra schema from `docker-local/cassandra/init/*.cql` applied, and
`room-service`, `room-worker`, `message-gatekeeper`, `broadcast-worker` and
`message-worker` running against them (`BOOTSTRAP_STREAMS=true`,
`ATREST_ENABLED=false`, `ENCRYPTION_ENABLED=false`).

    for s in room-service room-worker message-gatekeeper broadcast-worker message-worker; do
      make build SERVICE=$s
    done
    go run ./tools/e2eroomcreate/ --bob-delay=10ms

Run with `ROOM_SUBJECT_MODE=dual` (the compose default) and the harness dedupes
the double delivery across the global and local namespaces.

## Measured result

Bob loses the message once his client takes more than ~5-8 ms (boundary is
jittery run to run) between `subscription.update` and his `SUB` being
registered:

| `--bob-delay` | Bob receives "new message" |
|---|---|
| 0-4 ms | yes |
| 5 ms | boundary — flips between runs |
| 8 ms and above | **no** |

Losing timeline at `--bob-delay=10ms`:

```
     0.04ms | alice-client | publish room.create RPC (users=[bob])
     4.82ms | room-service | sync reply status=accepted roomId=... type=channel
    11.35ms | alice-client | RECEIVED subscription.update (added)
    11.86ms | bob-client   | RECEIVED subscription.update (added)
    13.14ms | alice-client | SUBSCRIBED to room subject (flush confirmed)
    13.51ms | alice-client | PUBLISHED msg.send "new message"
    19.26ms | alice-client | <- room event new_message new message
    22.89ms | bob-client   | client processing done (10ms)
    23.24ms | bob-client   | SUBSCRIBED to room subject (flush confirmed)
```

Two things the timeline shows that the description does not predict:

- **Alice's send is not published to the room subject immediately.** It takes
  ~6 ms to travel `msg.send` -> `message-gatekeeper` -> MESSAGES-CANONICAL ->
  `broadcast-worker` -> room subject. That lag is Bob's head start, and it is
  the only reason a same-millisecond subscribe survives at all.
- **Bob also loses the room's system messages** (`room_created`,
  `members_added`), not just Alice's message — they ride the same subject
  through the same pipeline. His room opens completely blank.

See `tools/e2epreviewrace` for why a catch-up read issued immediately after
subscribing does not repair this.
