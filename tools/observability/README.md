# Local Observability — Container Metrics & JetStream Backlog

Run cAdvisor + Prometheus + Grafana locally to observe container-level CPU,
memory, and network trends for every service and dependency in the local dev
environment, plus a `prometheus-nats-exporter` that surfaces JetStream
stream/consumer state. Useful when load-testing the messaging pipeline, hunting
a hot service, or checking how many messages are pending on a stream/consumer.

## Prerequisites

- `make deps-up` must have run first — this creates the `chat-local` Docker
  network that the observability stack joins.

## Quick Start

```bash
make deps-up     # if not already running
make up          # if you want to see service containers in the dashboard
make obs-up
```

Then open Grafana: **http://localhost:3001**

Three dashboards are pre-loaded and refresh every 5 seconds (Anonymous Admin is
enabled, so no login is required):

- **Containers - CPU & Memory** — per-container CPU/memory from cAdvisor.
- **JetStream - Pending & Backlog** — per-consumer pending (`num_pending`),
  in-flight/unacked (`num_ack_pending`), and redelivered (`num_redelivered`),
  broken out by stream + durable, plus total messages per stream. This is where
  you answer "how many messages are pending?".
- **NATS - JetStream Overview** — a single-pane summary: publish rate and ack
  rate (msg/s, derived from `jetstream_stream_last_seq` /
  `jetstream_consumer_ack_floor_stream_seq`), delivery-vs-ack throughput per
  consumer, the same pending / ack-pending / redelivered backlog, plus stream
  sizing (messages + bytes retained) and pull-requests-waiting. A `Stream`
  template variable filters every panel to one or more streams. This is where
  you answer "what's the message rate?" and "is any consumer falling behind?".

  Note on "by subject": `prometheus-nats-exporter` (via jsz) does **not** emit
  per-subject message counts, so there is no true per-subject breakdown. The
  practical axes are **per-stream** (each stream owns a subject namespace, e.g.
  `MESSAGES_*` carries `chat.user.*...msg.>`) and **per-consumer** (durables are
  named after the consuming service) — both dashboards break out along these.

The JetStream dashboard only has data once a NATS server is running
(`make deps-up`) and streams/consumers exist (`make up` and send some traffic).

## Stop

```bash
make obs-down
```

Storage is ephemeral — Prometheus metrics are lost on teardown. UI-made
dashboard edits are blocked by the provisioning config (`allowUiUpdates: false`);
edit the JSON under `grafana/dashboards/` directly to change a dashboard, then
run `make obs-down && make obs-up` to reload.

## Ports

| Port  | Service       | Notes                                                       |
|-------|---------------|-------------------------------------------------------------|
| 3001  | Grafana UI    | `:3001` avoids collision with the frontend dev server :3000 |
| 9091  | Prometheus    | `:9091` avoids collision with `search-service`'s :9090      |
| 8088  | cAdvisor      | `:8088` avoids collision with `auth-service`'s :8080        |
| 7777  | NATS exporter | Raw `prometheus-nats-exporter` `/metrics` for debugging     |

## What's instrumented

cAdvisor reads Docker stats for **every container on the host**, so this works
uniformly for Go services and third-party dependencies (NATS, MongoDB,
Cassandra, Elasticsearch, Valkey, Keycloak, MinIO) — no per-service code
changes required.

Legends show the 12-character container short-ID (e.g. `0a1b2c3d4e5f`) rather
than the container name. cAdvisor populates the `name` label only when its
Docker daemon integration works, which is unreliable on older cAdvisor
versions and on cgroups v2 + systemd hosts. The short-ID is extracted directly
from the cgroup path via `label_replace`, which works on every cAdvisor build.
Cross-reference with `docker ps --format 'table {{.ID}}\t{{.Names}}'` to map
ID → name.

JetStream stream/consumer state is scraped from `prometheus-nats-exporter`,
which polls the deps NATS container's monitoring endpoint
(`http://chat-local-nats:8222`) over the shared `chat-local` network. The
exporter runs with `-jsz=all`, so Prometheus gets per-consumer
`jetstream_consumer_num_pending`, `..._num_ack_pending`, `..._num_redelivered`,
and per-stream `jetstream_stream_total_messages` (labelled by `stream_name` and
`consumer_name`). No per-service code changes are required.

Application-level Go metrics (goroutines, GC, heap) are out of scope here.
When a service exposes `/metrics`, add a scrape job to `prometheus/prometheus.yml`.

## Files

| File | Purpose |
|------|---------|
| `docker-compose.yml` | The four-container stack definition (cAdvisor, nats-exporter, Prometheus, Grafana). |
| `prometheus/prometheus.yml` | Prometheus scrape config. |
| `grafana/provisioning/datasources/prometheus.yml` | Auto-wires the Prometheus datasource. |
| `grafana/provisioning/dashboards/dashboards.yml` | Tells Grafana to load every JSON file in `./dashboards`. |
| `grafana/dashboards/containers-cpu.json` | Container CPU/memory dashboard. |
| `grafana/dashboards/jetstream-pending.json` | JetStream pending/backlog dashboard. |
| `grafana/dashboards/nats-jetstream-overview.json` | JetStream overview: message rate, throughput, backlog, and stream sizing. |

## Scope

**Local development only.** The cAdvisor mount posture (privileged container
with the Docker socket and `/`, `/sys`, `/var/lib/docker` bind-mounted read-only)
grants broad host visibility and is unsuitable for shared or production
environments. All three ports are bound to `127.0.0.1` so the anonymous-admin
Grafana and unauthenticated Prometheus/cAdvisor APIs stay off any LAN the host
is attached to; remove the prefix in `docker-compose.yml` only if you have a
reason to reach the stack from another machine.
