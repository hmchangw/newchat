# Health Probes

Every service exposes Kubernetes-style liveness and readiness probes over HTTP,
served by `pkg/health`.

## Endpoints

| Path       | Meaning   | Behavior |
|------------|-----------|----------|
| `/healthz` | Liveness  | Always `200 {"status":"ok"}` while the process runs. It never probes dependencies — a dependency outage must not restart the pod. |
| `/readyz`  | Readiness | Reports whether this pod is connected to NATS. `200` when `CONNECTED` or `RECONNECTING`; `503` once the connection is `DISCONNECTED`/`CLOSED`. |

## Where they listen

| Service | Probe port | Notes |
|---------|-----------|-------|
| `auth-service` | `PORT` (default `8080`) | On the main Gin server. |
| `user-service` | `HEALTH_ADDR` (default `:8081`) | A dedicated listener, deliberately separate from its client API on `HTTP_PORT`: the API group sheds overload with `429`, and a shed liveness probe would restart pods mid-burst. |
| `search-service` | `SEARCH_METRICS_ADDR` (default `:9090`) | Mounted on the existing metrics listener — no extra port. |
| all other (NATS) services | `HEALTH_ADDR` (default `:8081`) | A dedicated health-only listener. One port per pod, so the shared default does not collide. |

## What readiness checks — and why only NATS

Readiness probes **only the pod's own NATS connection**, not the shared
datastores (Mongo, Cassandra, Elasticsearch). This is deliberate:

- **Readiness should reflect per-pod serve-ability, not backend health.** A
  shared database is the same for every replica, so probing it in readiness means
  a brief DB blip flips *every* pod `NotReady` at once. For an HTTP service that
  removes all endpoints (clients get connection-refused instead of a clean
  `503`); for the NATS workers it gates nothing useful and risks correlated
  rollout/PDB churn. The application returns proper `errcode` errors when a
  datastore is down — that's the right failure mode, not yanking pods.
- **The NATS connection is genuinely per-pod.** If *this* pod loses its NATS
  connection while siblings are fine, `NotReady` correctly reflects that it can't
  do work. A brief reconnect is tolerated (`RECONNECTING` stays ready) so
  readiness doesn't flap; a sustained disconnect reports `NotReady`.

Most services receive work over NATS, not an HTTP Service, so readiness here is
primarily a rollout-gating and operator signal — and a safe one, since nothing is
routed off it.

The NATS readiness check is `natsutil.HealthCheck(nc)`.

## Liveness

Liveness is process-up only. (A consume-loop heartbeat — failing liveness when a
worker's pull loop wedges while the process stays alive — is the natural next
addition, since that is the failure neither current probe catches.)

`HEALTH_ADDR` is a standard `caarlos0/env` var; override per deployment if
`:8081` clashes with another container port.

## Valkey is never a startup gate

`valkeyutil` probes the cluster with `PING` at dial time but does not gate on
it: an unreachable Valkey is logged and a usable client returned. This mirrors
the readiness reasoning above. A shared datastore is the same for every replica,
so making it fatal at startup means a Valkey outage overlapping a rollout,
scale-up or node drain crashloops every pod at once — including the message
path — and the crashloop outlives the outage. go-redis dials lazily and
self-heals per call, so a pod that starts during an outage recovers on its own.

`valkeyutil.WithRequireReachable()` restores fail-fast, and only the one-shot
CLI `tools/seed-sample-data` uses it: it has no fallback and no next call to
self-heal into, so aborting the run beats seeding half the data.

The `Connect`/`ConnectRaw` error branches remain — they now cover construction
and instrumentation failures, not reachability.

### What degrades, and how

| Consumer | Behaviour while Valkey is down |
|---|---|
| The L2 tiers (`subauthcache`, `roommetacache`, `sessioncache`, `atrest`, `roomsubcache`, `userstore`, `roomtimescache`) and the search restricted-rooms cache | Fall through to the source of truth — MongoDB, Cassandra or Elasticsearch. Correct results, higher latency and load. |
| `botplatform-service` rate limit + idempotency | **Fail open** — bot requests are admitted unthrottled and without duplicate suppression. Bots are critical, so a lost ceiling beats a dead bot. |
| `user-presence-service` | No fallback exists — Valkey is the store of record, so presence RPCs error until it returns. `user-service` degrades `/me` to `presence: "offline"` rather than failing. |

Cache invalidation (`BustKeys`, the tier slides) is best-effort, so a write
during an outage can leave a stale entry until its TTL expires. The
authoritative write itself still lands in the source of truth.

### What to watch

- `bot_control_bypassed_total{control}` — non-zero means bot rate limiting or
  duplicate suppression is currently off. This is the only durable signal that
  those controls were skipped; alert on it.
- Valkey fallback logs are throttled: the first occurrence of a message is a
  `WARN`, repeats within 30s drop to `DEBUG`, and the window reopens so a
  sustained outage keeps a periodic heartbeat. Raise the level to `DEBUG` to see
  every occurrence.

## Optional pprof profiling surface

The ten message-pipeline NATS services (`broadcast-worker`, `history-service`,
`inbox-worker`, `message-gatekeeper`, `message-worker`, `notification-worker`,
`room-service`, `roomlist-worker`, `room-worker`, `search-sync-worker`) can mount the standard
`net/http/pprof` handlers (`/debug/pprof/*`) on the same health listener, gated
by `PPROF_ENABLED` (default `false`). It is wired via
`health.ServeWithPprof(addr, timeout, cfg.PProfEnabled, checks...)` — no extra
port and off unless explicitly enabled, so the operator-exposed health port
never leaks profiling by default.

This is a local-dev / load-test aid: bring the stack up with
`PPROF_ENABLED=true make up` and snapshot every service with `make profile`
(see `tools/profilecapture/`). Leave it `false` in production.

When pprof is enabled the listener's write timeout is disabled, because CPU and
trace profiles stream the response for a client-chosen duration
(`/debug/pprof/profile?seconds=30`) that exceeds the hardened 10s write timeout —
otherwise the profile is truncated mid-capture. The default (pprof off) path
keeps the hardened timeouts.
