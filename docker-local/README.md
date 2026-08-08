# Local dev stack

Five Compose stacks share one external Docker network, `chat-local`, created by
the deps stack. Each has its own `make` targets; each is independently
startable, and all five can run at once.

| Stack | File | Targets | What |
|---|---|---|---|
| deps | `compose.deps.yaml` | `deps-up` / `deps-down` | NATS, MongoDB, Cassandra, Elasticsearch, Valkey, Keycloak, Vault, MinIO |
| services | `compose.services.yaml` | `up` / `up-detached` / `down` | Every Go microservice + the Traefik `/api/v1` gateway |
| ui | `compose.ui.yaml` | `ui-up` / `ui-down` | chat-frontend, admin-frontend |
| o11y | `compose.o11y.yaml` | `o11y-up` / `o11y-down` | OTLP collector, Tempo, Loki, Prometheus, Grafana |
| obs | `../tools/observability/docker-compose.yml` | `obs-up` / `obs-down` | cAdvisor, NATS JetStream exporter, Prometheus, Grafana |

## First run

```sh
./docker-local/setup.sh   # once: NATS operator keys → nats.conf, backend.creds, .env
make deps-up              # third-party deps; waits for healthchecks, runs the init one-shots
make seed                 # sample users/rooms/messages into MongoDB + Valkey
make up                   # every microservice (foreground; Ctrl-C stops)
make ui-up                # chat-frontend :3000, admin-frontend :3001
```

`make deps-up` runs `setup.sh` for you if `nats.conf` / `backend.creds` are
missing. `make seed` is safe to re-run — it upserts by stable ID and never drops
a database or collection, so hand-added dev data survives. `make seed-reset`
deletes the seeded rows first.

Order matters in one place only: `make up` and `make ui-up` both refuse to start
until the deps stack is up, because every service mounts `backend.creds` and
resolves `nats`/`mongodb`/… by name on `chat-local`.

## Logging in

`make seed` writes `chat.users` and the `chat.hr_employee` enrichment rows that
portal-service left-joins onto them. Two login paths:

- **Human accounts** (`alice`, `bob`, `carol`, …) — no password. chat-frontend
  asks portal for `/api/userInfo?account=<name>`, then mints a NATS JWT at
  `POST {baseUrl}/api/v1/auth`. Works because the local Compose files default
  `DEV_MODE=true`, which puts auth-service on the tokenless dev path; set
  `DEV_MODE=false` on both auth-service and chat-frontend to exercise OIDC
  against the Keycloak realm on :8180 instead.
- **Password login** (`admin` / `AdminDev123!`) — chat-frontend posts to portal
  `/api/v1/login`, which forwards to botplatform-service in-network. Requires a
  `roles` entry on the user doc, which the seeder writes.

`ivan` and `judy` are homed on `site-remote` so cross-site data has somewhere to
live. Local dev runs one site, so `PORTAL_SITE_URLS` maps `site-remote` at the
same local endpoints — without that entry portal treats them as an ops
misconfiguration and answers 500.

Portal re-reads the directory every minute locally (`PORTAL_CACHE_REFRESH_INTERVAL=1m`,
against a 2h production default), so seeding after the stack is already up needs
no restart.

## Host ports

Everything below is bound by exactly one stack. Nothing is shared, and nothing
overlaps — if a `docker compose up` fails with "port is already allocated", it is
something outside this repo.

| Port | Owner | Notes |
|---|---|---|
| 3000 | chat-frontend | Same port `npm run dev` binds — run the container **or** Vite, not both |
| 3001 | admin-frontend | |
| 3002 | Grafana (obs) | localhost-only |
| 3003 | Grafana (o11y) | |
| 3200 | Tempo | |
| 4222 | NATS client | |
| 4318 | OTLP/HTTP collector | |
| 5601 | Kibana | `debug` profile, off by default |
| 6379 | Valkey | single-node cluster mode |
| 7777 | **Traefik `/api/v1` gateway** | This is `baseUrl`; portal hands it to every client |
| 7778 | NATS Prometheus exporter | localhost-only |
| 8080 | auth-service | also reachable as `{baseUrl}/api/v1/auth` |
| 8082 | admin-service | admin-frontend talks here directly |
| 8085 | portal-service | portal-direct, deliberately not behind the gateway |
| 8086 | upload-service | also `{baseUrl}/api/v1/file` |
| 8087 | tcard-service | |
| 8088 | cAdvisor | localhost-only |
| 8180 | Keycloak | admin/admin |
| 8200 | Vault | dev mode, in-memory |
| 8222 | NATS monitoring | |
| 9000/9001 | MinIO API / console | minioadmin/minioadmin |
| 9042 | Cassandra | |
| 9090 | Prometheus (o11y) | |
| 9091 | Prometheus (obs) | localhost-only |
| 9200 | Elasticsearch | |
| 9222 | NATS WebSocket | what browsers connect to |
| 19090 | search-service health | container listens on 9090 |
| 27017 | MongoDB | |

media-service and botplatform-service publish no host port on purpose: both
default to container port 8080, and only the gateway (media) or portal
(botplatform) needs to reach them.

## Gateway routing

`traefik/dynamic.yml` is a static file — no Docker labels, so services carry no
gateway-specific config. It forwards three prefixes:

| Prefix | Service |
|---|---|
| `/api/v1/auth` | auth-service |
| `/api/v1/file` | upload-service |
| `/api/v1/avatar` | media-service |

Anything media-service serves outside `/api/v1/avatar` (`/api/v1/emoji`,
`/api/v1/drive.members`) is reachable in-network but has no gateway route yet;
add one here when a browser client needs it.

Known gap: user avatars 307-redirect to `EMPLOYEE_PHOTO_BASE_URL`
(`http://localhost:8081/photos` by default) for any account with an
`employeeId`, and nothing local serves that host — so seeded users show broken
avatar images. Point `EMPLOYEE_PHOTO_BASE_URL` at a real photo host, or clear
`employeeId` on the user doc, to get the default-avatar path instead.

## Vault and encrypted rooms

Vault runs in dev mode, so its transit key is in-memory while MongoDB persists.
After a Vault restart, previously wrapped room DEKs can no longer be unwrapped.
Reset the derivative cache and let services re-provision:

```sh
docker compose -f docker-local/compose.deps.yaml --profile dek-reset run --rm vault-dek-reset
```
