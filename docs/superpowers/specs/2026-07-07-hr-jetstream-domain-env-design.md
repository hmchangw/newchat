# search-sync-worker: read HR JetStream domain from env

**Date:** 2026-07-07
**Status:** Approved for implementation

## Problem

`search-sync-worker` runs at every fab site and, among its collections, hosts
the `spotlight-org` collection which consumes HR org data from
`OrgSyncStream(HR_CENTRAL_SITE_ID)`. That HR stream is created by `hr-syncer`
and can live in a **different NATS JetStream domain** (a remote cluster in the
supercluster). A worker at site A must be able to create a durable consumer on,
and fetch from, the HR stream that lives in site B's domain.

Accessing a stream in another JetStream domain requires a JetStream context
whose API prefix targets that domain (`$JS.<domain>.API.*` instead of
`$JS.API.*`). The domain is fixed at context construction
(`jetstream.NewWithDomain`), not a per-call option.

Today `main.go` builds a single JetStream context — `oteljetstream.New(nc)` —
and uses it for **all** collections: the local ones (messages / spotlight /
user-room, reading INBOX & MESSAGES_CANONICAL in the local domain) and the HR
`spotlight-org` collection. A single context has a single domain, so it cannot
serve both the local streams and a remote-domain HR stream.

**Goal:** let the HR domain be supplied via an environment variable so the HR
consumer targets the remote domain, while the local collections keep using the
local (default-domain) context. When the variable is unset, behavior is
identical to today.

## Constraint: the otel wrapper cannot set a domain

The `oteljetstream` wrapper (`instrumentation-go v0.2.0`) exposes only
`New(conn)`, which hardcodes `jetstream.New(conn.NatsConn())` — there is no
domain variant. A domain-scoped context must therefore come from the raw
`nats.go/jetstream` package (`jetstream.NewWithDomain`), which is not
otel-wrapped.

This costs nothing that is currently used: `runConsumer` already unwraps each
message to its raw `jetstream.Msg` and `handler.Add(jetstream.Msg)` never reads
the per-message otel trace context (`oteljetstream.Msg.Ctx`). So using a raw
domain-scoped context for the single HR consumer loses only a per-message
consume span that this worker already discards.

## Decisions (confirmed)

1. **Raw `NewWithDomain` for the HR consumer only.** Local collections keep the
   otel-wrapped `js`. No dependency change.
2. **`HR_JETSTREAM_DOMAIN` optional, empty = current behavior.**
   `envDefault:""`. Empty means same-domain/same-cluster; the HR consumer uses
   the shared `js` exactly as today. Backward-compatible — local dev and
   single-cluster deploys need no new variable.

## Design

### Config (`main.go`)

Add to `config`:

```go
// HRJetStreamDomain, when set, is the JetStream domain of the remote NATS
// cluster that owns OrgSyncStream (hr-syncer's HR stream). The spotlight-org
// collection's durable consumer is created against a domain-scoped JetStream
// context so a worker at one site can consume the HR stream in another site's
// domain. Empty (default) means the HR stream is in this worker's local
// domain and the shared, otel-traced JetStream context is used.
HRJetStreamDomain string `env:"HR_JETSTREAM_DOMAIN" envDefault:""`
```

No fail-fast validation is needed: empty is a valid, meaningful value.

### Domain-scoped context (`main.go`)

After the shared `js` is built, construct a raw domain-scoped context only when
a domain is configured:

```go
var hrJS jetstream.JetStream
if cfg.HRJetStreamDomain != "" {
    hrJS, err = jetstream.NewWithDomain(nc.NatsConn(), cfg.HRJetStreamDomain)
    if err != nil {
        slog.Error("jetstream HR-domain init failed",
            "domain", cfg.HRJetStreamDomain, "error", err)
        os.Exit(1)
    }
}
```

`NewWithDomain` performs no network I/O — it only fixes the API prefix — so
this cannot fail on reachability here; remote-reachability failures surface at
`CreateOrUpdateConsumer`, which already exits non-zero.

### Consumer-source abstraction (new file `consumer_source.go`)

`runConsumer` currently takes `oteljetstream.Consumer`. Introduce a minimal
seam that both the otel-wrapped and raw consumers can satisfy, normalized to
yield raw `jetstream.Msg` (which `handler.Add` already wants):

```go
// msgFetcher is the subset of a JetStream pull consumer that runConsumer
// needs, normalized so the same loop drives both the otel-wrapped local
// consumers and the raw domain-scoped HR consumer.
type msgFetcher interface {
    Fetch(n int, opts ...jetstream.FetchOpt) (msgBatch, error)
}

// msgBatch yields already-unwrapped raw jetstream.Msg values.
type msgBatch interface {
    Messages() <-chan jetstream.Msg
}
```

Two adapters:

```go
// rawConsumerAdapter wraps a raw jetstream.Consumer. jetstream.MessageBatch
// already yields raw jetstream.Msg, so the batch passes through unchanged.
type rawConsumerAdapter struct{ c jetstream.Consumer }

func (a rawConsumerAdapter) Fetch(n int, opts ...jetstream.FetchOpt) (msgBatch, error) {
    b, err := a.c.Fetch(n, opts...) // jetstream.MessageBatch satisfies msgBatch
    return b, err
}
```

```go
// otelConsumerAdapter wraps an oteljetstream.Consumer, unwrapping each
// oteljetstream.Msg (which embeds jetstream.Msg) back to the raw interface.
type otelConsumerAdapter struct{ c oteljetstream.Consumer }

func (a otelConsumerAdapter) Fetch(n int, opts ...jetstream.FetchOpt) (msgBatch, error) {
    b, err := a.c.Fetch(n, opts...)
    if err != nil {
        return nil, err
    }
    return otelBatch{b}, nil
}

type otelBatch struct{ b oteljetstream.MessageBatch }

func (o otelBatch) Messages() <-chan jetstream.Msg {
    out := make(chan jetstream.Msg)
    go func() {
        defer close(out)
        for m := range o.b.Messages() {
            out <- m.Msg // embedded raw jetstream.Msg
        }
    }()
    return out
}
```

The re-channeling goroutine is leak-safe: `runConsumer` always drains
`batch.Messages()` to completion, so when the source channel closes the
goroutine closes `out` and exits.

`runConsumer` changes:
- signature `cons oteljetstream.Consumer` → `cons msgFetcher`
- loop body `add(msg.Msg)` → `add(msg)` (the channel now yields raw
  `jetstream.Msg` directly)

Everything else in `runConsumer` (batch/flush timing, jobguard) is unchanged.

### Wiring (`main.go` collection loop)

`hrName := stream.OrgSyncStream(cfg.HRCentralSiteID).Name` is already computed
(used to skip HR stream bootstrap). Reuse it to route consumer creation:

```go
consumerCfg := buildConsumerConfig(cfg.Consumer, coll, cfg.SiteID)

var fetcher msgFetcher
if streamCfg.Name == hrName && hrJS != nil {
    cons, err := hrJS.CreateOrUpdateConsumer(ctx, streamCfg.Name, consumerCfg)
    if err != nil {
        slog.Error("create consumer failed",
            "stream", streamCfg.Name, "consumer", coll.ConsumerName(),
            "domain", cfg.HRJetStreamDomain, "error", err)
        os.Exit(1)
    }
    fetcher = rawConsumerAdapter{cons}
    slog.Info("HR consumer bound to remote JetStream domain",
        "domain", cfg.HRJetStreamDomain, "stream", streamCfg.Name,
        "consumer", coll.ConsumerName())
} else {
    cons, err := js.CreateOrUpdateConsumer(ctx, streamCfg.Name, consumerCfg)
    if err != nil {
        slog.Error("create consumer failed",
            "stream", streamCfg.Name, "consumer", coll.ConsumerName(),
            "error", err)
        os.Exit(1)
    }
    fetcher = otelConsumerAdapter{cons}
}
// ...
go runConsumer(ctx, fetcher, handler, ...)
```

The stream-bootstrap block above this is untouched: it already skips `hrName`
(and `inboxName`), so no HR stream is ever created by this worker.

### Behavior when unset

`HR_JETSTREAM_DOMAIN=""` → `hrJS == nil` → the HR collection takes the `else`
branch → shared otel `js` via `otelConsumerAdapter`. The only difference from
today is that the local consumers now flow through `otelConsumerAdapter`
instead of being passed directly; the adapter is a pure unwrap of the same
messages, so observable behavior is identical.

## Testing (TDD)

Red → Green → Refactor. New unit tests in `consumer_source_test.go` and a
config-parse test (no existing config test in the service).

- **`otelConsumerAdapter` / `otelBatch` unwrap** — feed a fake
  `oteljetstream.MessageBatch` yielding `oteljetstream.Msg` values that embed
  sentinel fake `jetstream.Msg`s; assert `msgBatch.Messages()` delivers exactly
  those raw messages, in order, then closes.
- **`rawConsumerAdapter` pass-through** — a fake `jetstream.Consumer` whose
  `Fetch` returns a fake batch; assert the same raw messages are delivered.
- **Config parse** — `env.ParseAs[config]` picks up `HR_JETSTREAM_DOMAIN` when
  set, and defaults to `""` when absent.
- Optional happy-path `runConsumer` smoke test driven by a fake `msgFetcher`
  (single batch → single flush) — a bonus enabled by the new seam; include only
  if it stays small.

**Domain routing itself is not unit- or integration-tested.** A single-node
NATS testcontainer cannot model JetStream domains (routing needs a
supercluster/gateway topology), so cross-domain consumption is validated in a
real multi-cluster environment. The existing
`TestSearchSyncSpotlightOrg_Integration` exercises the empty-domain
(shared-`js`) path and stays green.

## Out of scope / no impact

- **Client API docs** — none. This is internal consumer wiring, not a
  `chat.user.` handler.
- **Stream bootstrap** — unchanged; HR stream is owned by `hr-syncer`/ops.
- **Other services** — none touched.

## Deploy

Add a commented `HR_JETSTREAM_DOMAIN` to
`search-sync-worker/deploy/docker-compose.yml` (mirroring the existing
`SYNC_MESSAGES_FROM` commented example), noting local single-cluster dev leaves
it unset.

## Files touched

| File | Change |
|------|--------|
| `search-sync-worker/main.go` | new config field; `hrJS` construction; route HR consumer creation; `runConsumer` signature + loop |
| `search-sync-worker/consumer_source.go` | **new** — `msgFetcher`/`msgBatch` interfaces + `rawConsumerAdapter` / `otelConsumerAdapter` |
| `search-sync-worker/consumer_source_test.go` | **new** — adapter unwrap/pass-through tests |
| `search-sync-worker/*_test.go` (config) | config-parse test for `HR_JETSTREAM_DOMAIN` |
| `search-sync-worker/deploy/docker-compose.yml` | commented `HR_JETSTREAM_DOMAIN` example |
