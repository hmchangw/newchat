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

`make deps-up` runs `setup.sh` for you if `nats.conf`, `backend.creds` or `.env`
are missing. `make seed` is safe to re-run — it upserts by stable ID and never
drops a database or collection, so hand-added dev data survives.
`make seed-reset` deletes the seeded rows first.

Order matters in one place only: `make up` and `make ui-up` both refuse to start
until the deps stack is up, because every service mounts `backend.creds` and
resolves `nats`/`mongodb`/… by name on `chat-local`.

## Two federated sites

Federation is fully implemented in the services — `outbox-worker` fans out per
peer from `ALL_SITE_IDS`, `inbox-worker` owns the INBOX stream, all stream
names and subjects are site-scoped — but the stack above runs one site, so
none of it is exercised outside unit tests. This section stands up a
**second site** so cross-site behaviour can be QA'd in a browser: log in as
`alice` on site-local, `ivan` on site-remote, create a cross-site room, and
watch membership and messages federate live. The single-site stack above
remains the default for everyday work; reach for this only when the thing
you're testing is federation itself.

```sh
make deps-down            # the two stacks share host ports; only one at a time
./docker-local/setup.sh   # regenerate: adds the two NATS confs + per-site env files
make fed-deps-up          # 2× NATS (leafnode-linked), 2× Valkey, shared datastores
make fed-seed             # seed both sites' databases
make fed-up               # both sites' services (detached)
make fed-ui-up            # chat-frontend :3000 and :3100
make fed-logs             # streaming logs across both sites
make fed-o11y-up          # optional: traces/metrics/logs from both sites
```

`make fed-deps-up` runs `setup.sh` itself if any of the five generated files is
missing — `backend.creds`, both per-site NATS confs, both per-site env files —
so a half-generated tree is regenerated rather than half-used. (`make deps-up`
does the same for the single-site `nats.conf`/`backend.creds`/`.env`.) Bring it
down with `make fed-deps-down`, `make fed-down`, `make fed-ui-down` and
`make fed-o11y-down`.

**After changing anything `setup.sh` generates, run `make fed-regen`.** Two
things make a config change fail to take otherwise, and they compound: the
guard above only regenerates when a file is *missing*, so an edited template
never reaches a tree that already has the old output; and a bind-mounted file
changing on disk does not restart the NATS process that already read it. The
symptom is an error that survives a fix you can see applied on disk.
`fed-regen` takes the stack down, deletes the generated files, re-runs
`setup.sh` and brings everything back. It rotates the NATS keys, so
`docker-local/.env` is rewritten — the previous copy lands in `.env.bak` and
any local edits need re-applying.

Three Docker networks replace the single `chat-local`:

```
        ┌─────────── chat-federation ───────────┐
        │      (only the two NATS servers)      │
        │   nats-site-local ⟷ nats-site-remote  │   ← leafnode link :7422
        └───────────────────────────────────────┘
                 │                         │
   ┌─── chat-site-local ───┐   ┌─── chat-site-remote ───┐
   │ nats (alias)          │   │ nats (alias)           │
   │ valkey                │   │ valkey                 │
   │ 24 Go services        │   │ 24 Go services         │
   │ traefik :7777         │   │ traefik :7877          │
   │ chat-frontend :3000   │   │ chat-frontend :3100    │
   └───────────┬───────────┘   └───────────┬────────────┘
               └──── mongodb, cassandra, elasticsearch,
                     minio, keycloak, vault
                     (one container each, joined to BOTH networks)

          plus, only when `make fed-o11y-up` is running:
               └──── otel-collector, prometheus
                     (joined to BOTH networks; tempo, loki and grafana
                      sit on chat-site-local alone)
```

Each NATS container joins its site network under the alias `nats`, so every
service keeps `NATS_URL=nats://nats:4222` unchanged and still reaches its own
site's server; the shared datastores work the same way — one `mongodb`
container attached to both networks resolves as `mongodb` from either side.

### Logging in, two sites

Two browser **origins**, not two tabs on one origin, so the sessions don't
contend over localStorage:

- `alice` at `http://localhost:3000` — site-local
- `ivan` at `http://localhost:3100` — site-remote

Both are passwordless, same as single-site (see "Logging in" above).

### Site-remote port band

site-remote takes a +100 band across the nine `*_HOST_PORT` vars, with one
exception: `AUTH_SERVICE_HOST_PORT` goes `8080 → 8190`, not `8180`, because
8180 is Keycloak.

| | local | remote | | local | remote |
|---|---|---|---|---|---|
| chat-frontend | 3000 | 3100 | NATS client | 4222 | 4322 |
| admin-frontend | 3001 | 3101 | NATS monitor | 8222 | 8322 |
| gateway (baseUrl) | 7777 | 7877 | NATS WebSocket | 9222 | 9322 |
| portal | 8085 | 8185 | valkey | 6379 | 6479 |
| auth | 8080 | **8190** | admin-service | 8082 | 8182 |
| upload | 8086 | 8186 | tcard | 8087 | 8187 |
| search health | 19090 | 19190 | | | |

The shared datastores (Mongo, Cassandra, Elasticsearch, MinIO, Keycloak,
Vault) keep their single-site ports — that's *why* the federated and
single-site dep stacks can't run together. The NATS leafnode port (`:7422`)
stays container-internal on `chat-federation`; nothing is published to the
host for it. Full merged reference: the "Host ports" table below.

### Trimming the remote peer

Both sites run all 24 services by default — an asymmetric stack means every
"why doesn't this work cross-site?" has two candidate causes, a real
federation bug or a service that isn't running. `FED_REMOTE_SERVICES` (empty
= all) trims which services `fed-up` starts on site-remote; `make fed-up-lean`
sets it to the Tier 1 list below.

The constraint on trimming is **stream ownership**, not features: services
bootstrap the streams they own, and dropping an owner means the stream is
never created at that site. The failure is quiet — a JetStream publish to a
nonexistent stream gets no ack, so site-local's `outbox-worker` Naks and
retries forever (`MaxDeliver=-1`), parking events on its per-peer consumer
with nothing obviously wrong at the destination.

| Stream | Owner(s) |
|---|---|
| `INBOX-{site}` | inbox-worker |
| `OUTBOX-{site}` | outbox-worker |
| `MESSAGES`, `MESSAGES-CANONICAL` | message-gatekeeper (+ message-worker) |
| `ROOMS`, `ROOMS-TEAMS` | room-service, room-worker |
| `BOT-MESSAGES-CANONICAL` | bot-message-worker — not in the local stack at all |

**Tier 1 — must run (13 containers).** inbox-worker, outbox-worker,
room-service, room-worker, message-gatekeeper, message-worker,
broadcast-worker, user-service, history-service, auth-service,
portal-service, traefik, chat-frontend (the last one always starts — it's on
the UI compose, not trimmed by `FED_REMOTE_SERVICES`). Drop inbox-worker and
federation dies at the destination; drop message-gatekeeper and ivan cannot
send at all, because his client publishes straight into
`MESSAGES-site-remote`.

**Tier 2 — drop and lose a visible feature.** search-service +
search-sync-worker (no search as ivan), upload-service + media-service (no
upload; `/api/v1/avatar` 404s through site-remote's Traefik),
notification-worker + push-notification-service (no push; both are leaf
consumers), and user-presence-service (ivan reads as offline at both sites).

**Tier 3 — free drops, idle at both sites (6 containers).** The bot trio
(bot-broadcast-worker, bot-notification-worker,
bot-push-notification-service) consume `BOT-MESSAGES-CANONICAL-{site}`,
which nothing publishes because `bot-message-handler` is not in
`compose.services.yaml`. botplatform-service only serves portal's
password-login forward, and ivan/judy log in tokenless under
`DEV_MODE=true`. admin-service and tcard-service sit on no federation path.

Each idle Go service is ~20-40MB RSS, so Tier 3 saves ~200MB and Tier 2+3
~500MB against a stack whose RAM is dominated by the shared Cassandra, ES and
Keycloak — trimming is more a first-run build-time optimisation than a
memory one.

### Seeding one site vs both

`make seed` is unchanged: `--site` defaults to empty, and empty means
**unfiltered** — every fixture is written, exactly as before this work, into
whichever database `MONGO_DB` (or `--mongo-db`) points at. Passing `--site`
explicitly is what opts into per-site filtering:

| Flag | Default | Meaning |
|---|---|---|
| `--site` | empty (all sites, unfiltered) | Which site's rows to write; `site-local` or `site-remote` filters to that site |
| `--mongo-db` | unset (falls back to `MONGO_DB`) | Target database, when non-empty overrides `MONGO_DB` |

`make fed-seed` runs the seeder twice:

```sh
MONGO_DB=chat go run ./tools/seed-sample-data --site site-local --mongo-db chat
MONGO_DB=chat_remote VALKEY_ADDRS=localhost:6479 \
  go run ./tools/seed-sample-data --site site-remote --mongo-db chat_remote
```

What each pass writes, and why it is not simply "half the data each":

- **The full directory goes into both databases.** All users and their
  `hr_employee` rows are written to `chat` *and* `chat_remote`, unfiltered.
  Each portal must be able to resolve any account in order to tell a client
  where its home site is — that is how ivan's browser learns to connect to
  `:7877`. This mirrors what HR replication does in production.
- **Room-owned rows follow the room.** Rooms, room_members, messages,
  thread_rooms and room keys go to the database of the room's home site.
- **Subscriber-owned rows follow the subscriber.** Subscriptions,
  thread_subscriptions and the Valkey restricted-rooms cache go to the
  database of the *subscriber's* home site, while still carrying the *room's*
  `siteId`.

That last rule is the one to remember. ivan is a member of `r-general`, which
is homed at site-local: his subscription row lives in `chat_remote` but
records `siteId: site-local`. Services use that field to tell local rows from
cross-site rows within their own database
(`user-service/mongorepo/subscriptions.go:35`). Routing these by the room's
site instead puts ivan's rows in the wrong database and renders an empty chat
list for him — with no error anywhere.

`make fed-seed-reset` is `fed-seed` with `--reset` on both passes: it deletes
each site's seed records by stable ID from that site's database before
re-populating it, and never drops a database, so hand-added dev data survives.

The make targets pass fixed flags — `make` itself rejects `--site` as an
unknown flag, so per-site variations are run with `go run` directly:

```sh
# what a site-remote pass would write, without writing it
go run ./tools/seed-sample-data --dry-run --site site-remote
# reset + reseed one site only
MONGO_DB=chat_remote VALKEY_ADDRS=localhost:6479 \
  go run ./tools/seed-sample-data --site site-remote --mongo-db chat_remote --reset
```

The dry-run plan prints the site it is planning for and the filtered
per-collection counts, so `--dry-run --site site-remote` is the quickest check
that routing is sane. (`make seed-dry-run` is the unfiltered equivalent, and
reports `site all`.)

Three seeded rooms span both sites and carry `crossSite: true` —
`r-general` and `r-eng` (site-local, with ivan) and `r-remote-announce`
(site-remote, with alice). They exist so there is federated content to look
at before you create anything by hand.

### Observability across both sites

Every service in both site projects renders `O11Y_ENABLED=true` and
`OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318`, so that name has to
resolve on both site networks or half the stack exports into a void. `make
o11y-up` cannot supply it — `compose.o11y.yaml` joins the external network
literally named `chat-local`, which the federated deps stack never creates.
`compose.fed-o11y.yaml` is the overlay that fixes both:

```sh
make fed-o11y-up      # after make fed-deps-up
make fed-o11y-down
```

It repoints the inherited `chat-local` network key at the real
`chat-site-local` network (same trick as `compose.site.yaml`), keeps both site
networks `external` since `compose.fed-deps.yaml` owns them, and attaches
exactly two components to both networks:

| Component | Networks | Why |
|---|---|---|
| otel-collector | both | every service on either site resolves `otel-collector` by name to push OTLP traces and logs |
| prometheus | both | it scrapes each service container's `:2112` **directly** over Docker SD, so it needs a route to container IPs on both networks |
| tempo, loki, grafana | chat-site-local only | tempo and loki are written to by the collector; grafana reads tempo/loki/prometheus. None of them touches a service container |

Prometheus also needs different relabel rules, because the federated stack
renames both things `o11y/prometheus.yaml` filters on: the network
(`chat-local` → `chat-site-local`/`chat-site-remote`) and the Compose project
(`chat-local-services` → `chat-site-local`/`chat-site-remote`). The overlay
mounts `o11y/prometheus.fed.yaml` in its place, which keeps the same scrape
strategy, widens both filters, and adds a `site` label derived from the project
name so `room-service` from the two sites is distinguishable in a query.

Spans are separated the same way on the trace side: `setup.sh` writes
`OTEL_RESOURCE_ATTRIBUTES=site.id=<site>` into each per-site env file, so both
sites' spans land in the one Tempo with a `site.id` resource attribute. One
Grafana (`:3003`), one Tempo, one Prometheus, one Loki serve both sites.

chat-frontend traces from the browser too (`OTEL_ENABLED` defaults to true) and
posts straight to the host-published `http://localhost:4318/v1/traces`, so the
collector's CORS allowlist in `o11y/otel-collector.yaml` names both UI origins,
`:3000` and `:3100` — without the second one site-remote's browser spans are
rejected before they reach the collector.

### Known divergences from production

- **`chat.local.room.>` crosses the leafnode link.** Production filters that lane
  at a leaf node; there are no leaf nodes locally, so interest propagates.
  Harmless: `chat-frontend/src/api/subscribeToRoomEvents` subscribes to
  exactly one lane per room, selected from `room.crossSite`, so no client
  ever receives both copies. Documented, not fixed.
- **Shared Vault** means both sites wrap room DEKs under the same KEK.
  Per-site encryption isolation is not testable in this environment.
- **Shared Keycloak** is not site-scoped and `DEV_MODE=true` bypasses OIDC
  anyway.
- **Shared avatar bucket.** `media-service` hardcodes `MINIO_BUCKET=avatars`
  (not derived from `SITE_ID`, unlike upload-service's `chat-${SITE_ID}`), so
  both sites read and write the same MinIO bucket for avatars.
- **Two browser origins** (`:3000` and `:3100`) rather than two tabs on one
  origin, so the two logged-in sessions do not contend over localStorage.
- **Shared o11y backend.** One collector, Tempo, Prometheus, Loki and Grafana
  serve both sites; `site.id` on spans and the `site` label on metrics are what
  separate them, not separate backends.
- **JetStream runs standalone per site, not as a supercluster.** Neither conf
  has a `cluster{}` block, so each server owns its own JetStream and nothing
  spans a Raft meta-group. That is what this design needs — every stream is R1
  and site-scoped, and a cross-site event is a plain publish to
  `chat.inbox.{destSite}.external.>` that the leafnode link routes by subject
  interest, with the PubAck returning over the same link. A consequence worth
  knowing: there is no cross-site replication or failover of JetStream state,
  which is correct for a dev stack but is not how a production supercluster
  would be built.

  An earlier revision did emit `cluster { name: <site>, port: 6222 }` with no
  routes. That is a trap: a cluster name switches JetStream into clustered
  mode, whose Raft meta-group needs the system account carried over
  intra-cluster routes, and with one server per site there are none — the
  server refuses to start JetStream with *"JetStream cluster requires
  configured routes or solicited leafnode for the system account"*. Reaching a
  real supercluster from here means three servers per site with routes between
  them, not a cluster name on a lone server.
- **~8GB RAM.** Release valves are `fed-up-lean` and skipping the o11y stack.

### Verifying the two-site stack

Not yet run end to end — the branch was built without a Docker host. Whoever
brings it up first should check, and correct this README where reality differs:

1. `make fed-deps-up` from a clean tree generates all five files, and both NATS
   containers report healthy on `:8222`/`:8322`.
2. `make fed-seed` writes both databases; `make fed-seed-reset` re-runs cleanly.
3. alice (`:3000`) and ivan (`:3100`) log in, and a message in `r-general`
   reaches the other site live.
4. `make fed-o11y-up`, then in Grafana (`:3003`): one trace carrying spans with
   both `site.id` values, and Prometheus targets up for containers from both
   `chat-site-local` and `chat-site-remote`.
5. **Restart isolation.** `docker restart chat-fed-nats-site-remote`, then
   publish as alice: site-**local** JetStream should keep working throughout,
   since the two servers share no meta-group. Once the remote server is back,
   the parked forwards in `OUTBOX-site-local` should drain to ivan on their
   own — `outbox-worker` retries indefinitely (`MaxDeliver=-1`).

## Logging in

`make seed` writes `chat.users` and the `chat.hr_employee` enrichment rows that
portal-service left-joins onto them. Two login paths:

- **Human accounts** (`alice`, `bob`, `carol`, …) — no password. chat-frontend
  asks portal for `/api/userInfo?account=<name>`, then mints a NATS JWT at
  `POST {baseUrl}/api/v1/auth`. Works because `DEV_MODE` defaults to `true`,
  which puts auth-service on the tokenless dev path; set `DEV_MODE=false` once
  in `docker-local/.env` to move the whole stack onto the OIDC flow.
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

## Environment overrides

`docker-local/.env` is the one place to change shared settings. It reaches
containers two ways: services that declare `env_file` get every variable in it,
and `${VAR:-default}` references in any compose file resolve from it. The
Makefile passes `--env-file` explicitly, so `make up`, `make up SERVICE=<name>`
and `make ui-up` all resolve to the same values — without that flag, the
per-service path silently falls back to the in-file defaults.

The generated `.env` lists the common knobs commented out — datastore
endpoints, Vault, the OIDC settings, and the stack-wide toggles (`SITE_ID`,
`ALL_SITE_IDS`, `DEV_MODE`, `BOOTSTRAP_STREAMS`, `O11Y_ENABLED`,
`PPROF_ENABLED`). `SITE_ID` also drives everything derived from it —
Elasticsearch index names, the MinIO bucket, `PORTAL_SITE_URLS`,
`CLUSTER_DOMAINS` — so changing it moves the whole stack to a new site id
rather than half of it.

Published host ports are overridable too, as `<SERVICE>_HOST_PORT`, and the
URLs that reference them follow: moving `GATEWAY_HOST_PORT` rewrites portal's
`baseUrl` and media's `CLUSTER_DOMAINS`, and moving `CHAT_FRONTEND_HOST_PORT`
rewrites upload-service's CORS allowlist. The port table below lists defaults.

Only four settings stay literal, because a single shared value would break
them: the listen `PORT`, `MODE` (user/bot), `OTEL_SERVICE_NAME` (one name per
service) and `MINIO_BUCKET` (upload and media use different buckets). Everything
else — including each service's own cache sizes, TTLs, batch limits and
timeouts — reads `${VAR:-<default>}`; grep a compose file for `${` to see what
that service exposes.

## Host ports

Everything below is bound by exactly one stack. Nothing is shared, and nothing
overlaps — if a `docker compose up` fails with "port is already allocated", it is
something outside this repo.

The `site-remote` column only binds when the federated stack is running (see
"Two federated sites" above) — those ports are additional to, not instead of,
the `local` column, since both sites run at once.

| Port (local) | site-remote | Owner | Notes |
|---|---|---|---|
| 3000 | 3100 | chat-frontend | Same port `npm run dev` binds — run the container **or** Vite, not both |
| 3001 | 3101 | admin-frontend | |
| 3002 | | Grafana (obs) | localhost-only |
| 3003 | shared | Grafana (o11y) | one instance for both sites under `fed-o11y-up` |
| 3200 | shared | Tempo | |
| 4222 | 4322 | NATS client | |
| 4318 | shared | OTLP/HTTP collector | on both site networks under `fed-o11y-up` |
| 5601 | | Kibana | `debug` profile, off by default |
| 6379 | 6479 | Valkey | single-node cluster mode |
| 7777 | 7877 | **Traefik `/api/v1` gateway** | This is `baseUrl`; portal hands it to every client |
| 7778 | | NATS Prometheus exporter | localhost-only |
| 8080 | **8190** | auth-service | also reachable as `{baseUrl}/api/v1/auth`; site-remote is `+110`, not `+100`, because 8180 is Keycloak |
| 8082 | 8182 | admin-service | admin-frontend talks here directly |
| 8085 | 8185 | portal-service | portal-direct, deliberately not behind the gateway |
| 8086 | 8186 | upload-service | also `{baseUrl}/api/v1/file` |
| 8087 | 8187 | tcard-service | |
| 8088 | | cAdvisor | localhost-only |
| 8180 | shared | Keycloak | admin/admin |
| 8200 | shared | Vault | dev mode, in-memory |
| 8222 | 8322 | NATS monitoring | |
| 9000/9001 | shared | MinIO API / console | minioadmin/minioadmin |
| 9042 | shared | Cassandra | |
| 9090 | shared | Prometheus (o11y) | on both site networks under `fed-o11y-up` |
| 9091 | | Prometheus (obs) | localhost-only |
| 9200 | shared | Elasticsearch | |
| 9222 | 9322 | NATS WebSocket | what browsers connect to |
| 19090 | 19190 | search-service health | container listens on 9090 |
| 27017 | shared | MongoDB | |

"shared" rows are one container serving both sites at its single-site port:
the datastores `compose.fed-deps.yaml` pulls in with `extends:`, and the o11y
components `compose.fed-o11y.yaml` overlays. The datastore half is exactly why
the federated and single-site dep stacks can't run at the same time. The NATS
leafnode port (`:7422`) stays container-internal on `chat-federation`; nothing
is published to the host for it.

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
