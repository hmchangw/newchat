# NATS / JetStream Failure Metrics Contract — moved

This contract now lives at
[`docs/specs/o11y/nats-metrics-contract.md`](../../specs/o11y/nats-metrics-contract.md).

It defines the application-side NATS metric families for normal operation, not
only for a failure campaign: instrument names, closed label vocabularies,
outcome semantics, and which platform exporter already answers a given
question. The failure campaign is one reader of it, alongside the dashboard and
alert guides and the SLO measurement roadmap.
