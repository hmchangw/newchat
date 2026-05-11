# JetStream Durable Consumer Defaults — Design

**Date:** 2026-05-08
**Branch:** `claude/jetstream-consumer-config-JTIKh`
**Status:** Draft, awaiting user review

## Summary

Standardize JetStream durable consumer configuration across all worker
services by introducing a shared default builder in `pkg/stream` and
applying per-service `MaxAckPending` recommendations. Today, services set
only `AckPolicy: Explicit` and rely on NATS defaults for everything else;
this design makes the relevant knobs explicit, uniform, and discoverable.

## Goals

1. A single source of truth for project-wide consumer defaults
   (`AckPolicy`, `AckWait`, `MaxDeliver`, `MaxWaiting`, `DeliverPolicy`).
2. Per-service `MaxAckPending` values sized to the service's pull
   concurrency and per-message cost.
3. No runtime impact on existing durable consumers — NATS honors
   `DeliverPolicy` only at consumer creation, so cursor positions are not
   reset.
4. Preserve existing per-service customizations that already serve a
   purpose (`search-sync-worker`'s progressive `BackOff`, `inbox-worker`'s
   `FilterSubjects`).

## Non-Goals

- Changing stream configurations or the existing `bootstrap.go` opt-in
  pattern.
- Touching non-JetStream NATS subscriptions (`nc.QueueSubscribe`,
  request/reply handlers).
- Making `MaxAckPending` env-driven; values stay as code constants per
  service to keep tuning visible in version control.
- Migrating or resetting any currently-running durable consumer.

## Standard Defaults

Every durable consumer in the repo gets the following baseline:

| Field           | Value                          | Rationale                                                                 |
|-----------------|--------------------------------|---------------------------------------------------------------------------|
| `AckPolicy`     | `AckExplicitPolicy`            | Already the project convention; required for at-least-once semantics.     |
| `AckWait`       | `30 * time.Second`             | Matches NATS default; long enough for Cassandra/Mongo writes + ES index.  |
| `MaxDeliver`    | `5`                            | Bounded retries before terminal failure; pairs with DLQ/log-and-drop.     |
| `MaxWaiting`    | `512`                          | NATS 2.10 default for max in-flight pull requests.                        |
| `DeliverPolicy` | `DeliverNewPolicy`             | New consumers start at the stream head; existing consumers unaffected.    |

## Per-Service `MaxAckPending`

Sizing rule: high-throughput services pull `2 × MAX_WORKERS` (default
`200`) into the iterator and process up to `MAX_WORKERS` (`100`)
concurrently. `MaxAckPending` must be `≥ 200` to avoid throttling the
iterator. Final values are tuned for per-message cost.

| Service                    | Pattern                  | `MaxAckPending` | Rationale                                                                                  |
|----------------------------|--------------------------|-----------------|--------------------------------------------------------------------------------------------|
| `message-gatekeeper`       | High-throughput pull     | `1000`          | Lightest per-msg work (validate + republish); allow large bursts on inbound.               |
| `broadcast-worker`         | High-throughput pull     | `1000`          | Fan-out to in-memory subscribers; fast, bursty.                                            |
| `message-worker`           | High-throughput pull     | `500`           | Cassandra writes are I/O bound; smaller cap limits unbounded backlog if Cassandra slows.   |
| `notification-worker`      | High-throughput pull     | `500`           | May call external push providers; bound exposure to provider latency.                      |
| `room-worker`              | High-throughput pull     | `200`           | Low-volume admin/membership stream; matches in-flight ceiling exactly.                     |
| `inbox-worker`             | Sequential `Consume()`   | `100`           | One-at-a-time callback; cap prefetch to avoid stale federated events.                      |
| `search-sync-worker` (×3)  | Batch `Fetch()`          | `500` each      | ES indexing is batched; supports existing batch flush thresholds with headroom.            |

## Architecture & Code Layout

### New file: `pkg/stream/consumer.go`

```go
package stream

import (
    "time"

    "github.com/nats-io/nats.go/jetstream"
)

const (
    DefaultAckWait    = 30 * time.Second
    DefaultMaxDeliver = 5
    DefaultMaxWaiting = 512 // NATS 2.10 default
)

// DurableConsumerDefaults returns the project-wide standard ConsumerConfig
// for durable JetStream consumers. Callers must set Durable, and should set
// MaxAckPending and FilterSubjects as appropriate for the service.
//
// DeliverPolicy is honored only at consumer creation; updating an existing
// durable consumer does not reset its cursor.
func DurableConsumerDefaults() jetstream.ConsumerConfig {
    return jetstream.ConsumerConfig{
        AckPolicy:     jetstream.AckExplicitPolicy,
        AckWait:       DefaultAckWait,
        MaxDeliver:    DefaultMaxDeliver,
        MaxWaiting:    DefaultMaxWaiting,
        DeliverPolicy: jetstream.DeliverNewPolicy,
    }
}
```

### Per-service `main.go` updates

Each consumer creation site changes from:

```go
cons, err := js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
    Durable:   "broadcast-worker",
    AckPolicy: jetstream.AckExplicitPolicy,
})
```

to:

```go
cc := stream.DurableConsumerDefaults()
cc.Durable = "broadcast-worker"
cc.MaxAckPending = 1000
cons, err := js.CreateOrUpdateConsumer(ctx, streamName, cc)
```

### Service-specific overrides retained

- **`message-worker`**: drop the existing `MaxDeliver: cfg.MaxRedeliver+1`
  override; the unified `MaxDeliver = 5` from defaults applies. The
  `MaxRedeliver` config field can be removed if it is not referenced
  elsewhere. Note: the prior code computed `MaxDeliver = MaxRedeliver + 1`
  with `MaxRedeliver` defaulting to `5`, yielding `MaxDeliver = 6` (1
  initial + 5 retries). The new project-wide default of `5` total
  deliveries (1 initial + 4 retries) is a deliberate 1-attempt
  reduction in `message-worker`'s retry budget — accepted as part of
  unifying the project standard.
- **`inbox-worker`**: keep `FilterSubjects:
  ["chat.inbox.{siteID}.aggregate.>"]`.
- **`search-sync-worker`** (all three consumers): keep `BackOff: [1s, 5s,
  30s]` and per-collection `FilterSubjects`. With `MaxDeliver = 5` and 3
  `BackOff` entries, the 4th and 5th retry intervals reuse the last entry
  (`30s`), which is the documented NATS behavior.

## Safety: Existing Consumers

`js.CreateOrUpdateConsumer` updates mutable fields on an existing durable
but does not reset its cursor. `DeliverPolicy` is a creation-only field;
NATS ignores changes to it on update. Therefore:

- Currently-running consumers retain their cursor positions.
- Pending/redelivered messages already queued for those consumers are not
  dropped.
- New consumers (e.g., a new `siteID` deployment) start from the stream
  head per `DeliverNewPolicy`.

No migration step or operator action is required.

## Testing

### Unit tests in `pkg/stream/consumer_test.go`

Table-driven assertions on `DurableConsumerDefaults()`:

- `AckPolicy == AckExplicitPolicy`
- `AckWait == 30 * time.Second`
- `MaxDeliver == 5`
- `MaxWaiting == 512`
- `DeliverPolicy == DeliverNewPolicy`
- Returned struct does not set `Durable`, `MaxAckPending`, or
  `FilterSubjects` (callers own these).

### Per-service tests

Extend each worker service's existing `*_test.go` to assert the constructed
`ConsumerConfig` carries the expected `MaxAckPending` for that service.
Where the consumer is built inline in `main.go`, extract the
config-construction into a small unexported helper (`buildConsumerConfig()
jetstream.ConsumerConfig`) so it can be unit-tested without standing up
NATS. This is a localized refactor consistent with the project's
testability conventions.

### Integration tests

No new integration tests required. Existing integration tests already
exercise the consumer end-to-end; they continue to pass with the new
defaults because:

- `AckPolicy` is unchanged.
- `AckWait = 30s` matches the prior NATS default.
- `MaxDeliver = 5` is permissive enough for any test that previously
  relied on default unlimited redeliveries (none exist in the codebase).
- `MaxAckPending` is set well above each service's in-flight ceiling.
- `DeliverPolicy = DeliverNewPolicy` only affects fresh consumer
  creation, which testcontainer setups already do.

## Rollout

1. Land the `pkg/stream/consumer.go` helper with tests.
2. Update services in this order (each in its own commit on the same
   branch): `message-gatekeeper`, `broadcast-worker`, `message-worker`,
   `notification-worker`, `room-worker`, `inbox-worker`,
   `search-sync-worker`.
3. Run `make lint` and `make test` after each commit; run
   `make test-integration` once at the end.
4. Open a single PR with the full set of changes.

## Risks and Open Questions

- **`message-worker.MaxRedeliver` config field**: if removed, ensure no
  deploy manifest still sets `MAX_REDELIVER`. The implementation step
  should grep deploy YAML/Helm before removal.
- **`MaxAckPending` ceilings under sustained backlog**: chosen values
  assume current load profiles. If a future load test reveals throttling
  on `message-worker` at `500`, raise to `1000`. No design change needed —
  it's a one-line constant.
- **`search-sync-worker` `BackOff` length vs. `MaxDeliver`**: NATS reuses
  the last `BackOff` entry for retries beyond the array length. This is
  the desired behavior here, but the implementation plan should add a
  comment in `search-sync-worker/main.go` documenting the interaction so
  future maintainers do not "fix" it by extending `BackOff` to length 5.
