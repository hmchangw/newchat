# e2epreviewrace

Measures the window in which a newly-added member's room-list preview can be lost.

## The race

A creates a room with B; A waits for `subscription.update`, then sends the first
message. B receives the same `subscription.update` and subscribes to the room
subject. `chat.room.{roomID}.event` is **core NATS** (at-most-once), so a publish
that lands before B's `SUB` is registered is a silent no-op for B.

The obvious repair — B issues a catch-up read right after subscribing — is not
sound on its own, because `broadcast-worker` (fan-out) and `message-worker`
(Cassandra) are **parallel** consumers of MESSAGES-CANONICAL. At the instant the
fan-out event fires, the message is not yet readable anywhere. B can therefore
miss the live event *and* read nothing.

This tool measures that gap against the real binaries.

    t=0          A's message reaches MESSAGES-CANONICAL
    t=t_pub      broadcast-worker publishes new_message on chat.room.{id}.event
    t=t_sub      B's SUB is registered                                  [swept]
    t=t_sub+rpc  B's catch-up read executes                             [swept]
    t=t_write    message-worker's Cassandra INSERT becomes readable

B loses the live event when `t_sub > t_pub`, and the catch-up heals it only when
`t_sub + rpc > t_write`. The band where both fail is the bug.

## Running

Needs NATS (JetStream), MongoDB and Cassandra reachable on the default local
ports, with the Cassandra schema from `docker-local/cassandra/init/*.cql`
applied, plus `broadcast-worker` and `message-worker` running against them
(`BOOTSTRAP_STREAMS=true`, `ATREST_ENABLED=false`).

    make build SERVICE=broadcast-worker
    make build SERVICE=message-worker
    go run ./tools/e2epreviewrace/ --catchup-rpc=0s --reps=3

`--catchup-rpc` models the hops this harness skips (client → user-service →
history-service). A **larger** value is **safer** for B, so `0s` is the
adversarial case. `--reps` repeats each sweep point.

## Measured result

Single-node stack, RF=1, idle machine — production numbers will be larger and
more variable.

| | observed |
|---|---|
| `t_pub` — broadcast-worker publishes | 2.2–3.6 ms |
| `t_write` — row readable at LocalQuorum | 6.8–15.5 ms |
| gap where a read returns nothing | ~4–13 ms after fan-out |

Reproduced failure (`t_sub=2ms`): B missed the live event by ~1 ms, and its
catch-up completed at 6.6 ms — 1.8 ms before the write landed at 8.4 ms.

Losses by catch-up latency: `0s` → 1/42 trials, `5ms` → 0/42, `250ms` → 0/42.

The race is self-limiting (a later `t_sub` also delays the catch-up, buying it
margin), but the band's width is `t_write - t_pub`, which *grows* under load:
`message-worker` writes several tables at LocalQuorum with an outage retry
budget, while `broadcast-worker` publishes off cached room meta and stays fast.

Enabling preview persistence does not close it: `broadcast-worker`'s
`previewWriter` flushes on `PREVIEW_FLUSH_INTERVAL` (250 ms default), later than
the ~8 ms Cassandra visibility, so the walk measured here is the fastest path a
catch-up can take.

**Conclusion: a catch-up read must be deferred (~300 ms) and bounded-retried,
never issued immediately.**

## Scope

Runs the real `broadcast-worker` and `message-worker`. Does **not** run
`message-gatekeeper` (the canonical event is published directly), `room-worker`,
`user-service` or `history-service`; B's catch-up is a direct Cassandra query
plus synthetic latency for the missing hops. Faithful for the contested step —
`walkForPreview` walks Cassandra from `now`, so it needs nothing
`roomlist-worker` writes — but it is not the full stack.
