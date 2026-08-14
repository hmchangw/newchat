# Two Federated Sites in Docker Compose

`docker-local/` runs one site. Federation is fully implemented in the services —
`outbox-worker` fans out per peer from `ALL_SITE_IDS`, `inbox-worker` owns the
INBOX stream, all stream names and subjects are site-scoped — but no local
environment has ever stood up a second site, so none of it is exercised outside
unit tests.

This spec adds a **second site to the local dev stack** so cross-site behaviour
can be QA'd in a browser: log in as `alice` on site-local in one tab, `ivan` on
site-remote in another, create a cross-site room, and watch membership and
messages federate live.

## Scope

| Change | Where |
|--------|-------|
| 4-line network-name override | new, `docker-local/compose.site.yaml` |
| Federated dep stack: 3 networks, 2× NATS, 2× Valkey, shared datastores | new, `docker-local/compose.fed-deps.yaml` |
| Per-site env files | `docker-local/setup.sh` emits `.env.site-local`, `.env.site-remote` |
| Two NATS confs instead of one | `docker-local/setup.sh` |
| Keyspace-templated Cassandra init | `docker-local/compose.fed-deps.yaml` (`cassandra-init` entrypoint) |
| Per-site seeding | `tools/seed-sample-data` gains `--site` and `--mongo-db` |
| `fed-*` targets | `Makefile` |
| Topology, ports, tiers, known limits | `docker-local/README.md` |

### Out of scope

- **Changing any of the 24 service compose files.** The design's main constraint
  is that `<service>/deploy/docker-compose.yml` stays untouched — see "The
  compose mechanism" for how.
- **The existing single-site flow.** `make up`, `make deps-up`, `make seed` and
  `docker-local/.env` behave exactly as they do today. Two-site is purely
  additive.
- **Leaf nodes.** Production filters the site-local room lane at a leaf node. We
  do not model leaf nodes; see "Known divergences" for why that is safe here.
- **Per-site Vault KEKs and per-site Keycloak.** Neither is site-scoped; both
  stay shared.

## Decisions taken

Recorded because each one closed off a plausible alternative:

- **Site IDs are `site-local` and `site-remote`**, not `site-a`/`site-b`. The
  seed fixtures already home alice…heidi on `site-local` and ivan+judy on
  `site-remote`, and portal/media compose defaults already carry `site-remote`
  placeholders. Renaming would churn fixtures and the single-site defaults for
  cosmetic gain.
- **Both sites run all 24 services by default.** An asymmetric stack means every
  "why doesn't this work cross-site?" has two candidate causes — a real
  federation bug, or a service that isn't running. Trimming is available as a
  knob (`FED_REMOTE_SERVICES`), not a fork.
- **Heavy datastores are shared containers with per-site logical namespaces.**
  One Mongo/Cassandra/ES/MinIO, isolated by db name / keyspace / index prefix /
  bucket. That is real isolation — a service pointed at `chat_remote` cannot see
  `chat` — at roughly a third of the RAM of twinning them.
- **NATS and Valkey are twinned.** NATS because federation requires two servers;
  Valkey because its keys (`presence:{account}:conns`, room key pairs) carry no
  site prefix, and a second Valkey costs ~10MB.

## Topology

Three Docker networks replace the single `chat-local`:

```
        ┌─────────── chat-federation ───────────┐
        │      (only the two NATS servers)      │
        │   nats-site-local ⟷ nats-site-remote  │   ← gateway link :7222
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
                     minio, keycloak, vault, otel-collector
                     (one container each, joined to BOTH networks)
```

Each NATS container joins its site network **under the network alias `nats`**,
plus `chat-federation` for the gateway link. That alias is what lets every
service keep `NATS_URL=nats://nats:4222` unchanged and still reach its own
site's server. The shared datastores work the same way: one `mongodb` container
attached to both networks resolves as `mongodb` from either side.

## The compose mechanism

Two Compose **projects** over the same files, differing only by project name,
env file, and one override:

```sh
docker compose -p chat-site-remote --env-file docker-local/.env.site-remote \
  -f docker-local/compose.services.yaml -f docker-local/compose.site.yaml up
```

`compose.site.yaml` in full:

```yaml
networks:
  chat-local:
    name: ${CHAT_NETWORK:-chat-local}
    external: true
```

The key stays `chat-local`, so all 24 included service files still match; `name:`
repoints it at the real network. Verified against Compose v5.1.1 that this
override merges correctly **through `include:`** — the rendered config shows
`name: chat-site-remote` on the external network with service definitions
untouched.

Three properties of the existing files make this work, all confirmed:

- **No `container_name`** in any of the 24 included service composes (only in
  `tools/`, which are not included), so per-project name prefixing does its job
  and nothing collides.
- **Every published port** in those files is already `${SERVICE_HOST_PORT:-…}`,
  so site-remote only needs a shifted band in its env file.
- **No `depends_on`** anywhere in them, so `docker compose up <subset>` starts
  exactly the named services with no hidden pull-ins.

Both projects build the same Dockerfiles from the same context, so the second
project's builds are layer-cache hits rather than a doubling of build work.

## The dep stack

`compose.fed-deps.yaml` is the federated counterpart of `compose.deps.yaml` and
owns everything that is not a Go service: all three networks, the two NATS
servers, the two Valkeys, and the shared datastores.

The shared datastores are **not** copied. They are pulled in with `extends:`,
which single-sources their image pins, healthchecks and volumes:

```yaml
services:
  mongodb:
    extends: { file: ./compose.deps.yaml, service: mongodb }
    container_name: chat-fed-mongodb
    networks: [chat-site-remote]

networks:
  chat-local:       { name: chat-site-local }
  chat-site-remote: { name: chat-site-remote }
  chat-federation:  { name: chat-federation }
```

`extends` **unions** the source service's networks rather than replacing them —
verified against Compose v5.1.1, where omitting the repoint fails with
`refers to undefined network chat-local`. So the same `name:` trick is reused: the
inherited `chat-local` key is repointed at `chat-site-local`, and
`chat-site-remote` is added. The rendered config puts `mongodb` on both site
networks with its `compose.deps.yaml` definition otherwise intact.

`container_name` is overridden to `chat-fed-*` for both clarity and safety —
the inherited name would otherwise collide with a running single-site stack.

NATS and Valkey are defined inline rather than extended, because they are
per-site rather than shared. Each attaches to its site network under the alias
its services expect:

```yaml
  nats-site-local:
    networks:
      chat-site-local: { aliases: [nats] }
      chat-federation: {}
```

`cassandra-init` is redefined here (not extended) because the federated version
takes a `KEYSPACE` and runs twice — see "Cassandra keyspace templating".

The federated and single-site dep stacks **cannot run simultaneously**: they
publish the same host ports for the shared datastores. `make fed-deps-up` fails
fast with a clear message if `chat-local-nats` is running, mirroring the existing
`require-deps` guard.

## Per-site configuration

| Var | site-local | site-remote |
|---|---|---|
| `SITE_ID` | `site-local` | `site-remote` |
| `ALL_SITE_IDS` | `site-local,site-remote` | `site-local,site-remote` |
| `CHAT_NETWORK` | `chat-site-local` | `chat-site-remote` |
| `MONGO_DB` | `chat` | `chat_remote` |
| `CASSANDRA_KEYSPACE` | `chat` | `chat_remote` |

`ALL_SITE_IDS` is identical on both sides — `outbox-worker`'s `federationPeers`
drops the local site itself.

Elasticsearch indexes and the MinIO bucket need **no entries**:
`search-service`, `search-sync-worker` and `upload-service` already default them
off `SITE_ID` (`spotlight-${SITE_ID}-v1`, `user-room-mv-${SITE_ID}`,
`chat-${SITE_ID}`). That isolation is free.

`OTEL_RESOURCE_ATTRIBUTES=site.id=${SITE_ID}` is set per site. Without it both
sites' `room-service` spans carry identical resource attributes and a federated
trace cannot be read in Tempo.

### Host ports

site-remote takes a +100 band across the nine `*_HOST_PORT` vars, with one
exception: `AUTH_SERVICE_HOST_PORT` goes `8080 → 8190`, because 8180 is Keycloak.

| | local | remote | | local | remote |
|---|---|---|---|---|---|
| chat-frontend | 3000 | 3100 | NATS client | 4222 | 4322 |
| admin-frontend | 3001 | 3101 | NATS monitor | 8222 | 8322 |
| gateway (baseUrl) | 7777 | 7877 | NATS WebSocket | 9222 | 9322 |
| portal | 8085 | 8185 | valkey | 6379 | 6479 |
| auth | 8080 | **8190** | admin-service | 8082 | 8182 |
| upload | 8086 | 8186 | tcard | 8087 | 8187 |
| search health | 19090 | 19190 | | | |

The NATS cluster port (`:6222`) and gateway port (`:7222`) stay
container-internal on `chat-federation`; nothing is published to the host.

### Site directory

Both sites get the same `PORTAL_SITE_URLS` and `CLUSTER_DOMAINS`, replacing
today's localhost placeholders with real endpoints:

```json
{"site-local":  {"baseUrl":"http://localhost:7777","natsUrl":"ws://localhost:9222"},
 "site-remote": {"baseUrl":"http://localhost:7877","natsUrl":"ws://localhost:9322"}}
```

That is the whole cross-site login story: ivan hits either portal, is told his
home is `site-remote`, and his browser connects to `:7877` and
`ws://localhost:9322`.

## NATS supercluster

`setup.sh` reuses the same operator, `chatapp` account and SYS keys it generates
today — gateway routing of JetStream publishes only works inside one account and
one JetStream domain — and writes two confs instead of one. Per-site deltas:

```
server_name: nats-site-local
cluster  { name: site-local,  port: 6222 }
gateway  { name: site-local,  port: 7222,
           gateways: [{ name: site-remote, url: nats://nats-site-remote:7222 }] }
```

No `jetstream{domain}` on either side. A shared domain is what lets site-local's
`outbox-worker` publish `chat.inbox.site-remote.external.*` on its **own local
connection** and have it land in `INBOX-site-remote`. Stream names are already
globally unique (`INBOX-site-local` vs `INBOX-site-remote`) and stream subjects
are site-scoped, so both sites' streams coexist in the one domain without
collision.

## Cassandra keyspace templating

Mongo, ES and MinIO namespace themselves from config. Cassandra does not:
`docker-local/cassandra/init/*.cql` hardcodes the keyspace. Every occurrence is
`chat.<object>` except a single bare `CREATE KEYSPACE IF NOT EXISTS chat`, so
one rule in the `cassandra-init` one-shot covers all of them:

```sh
sed "s/\bchat\b/${KEYSPACE:-chat}/g" "$f" | cqlsh cassandra
```

Run once with `KEYSPACE=chat` and once with `KEYSPACE=chat_remote`. The `.cql`
files stay byte-identical, which matters because CLAUDE.md makes them a mandated
mirror of `docs/cassandra_message_model.md`.

Fallback if the sed proves brittle: share one `chat` keyspace across both sites.
Message rows are partitioned by `room_id`, which is globally unique and whose
room has exactly one home site, so nothing collides — it only loses the "each
site owns its own history" property.

## Seeding both sites

`make fed-seed` runs the existing seeder twice with two new flags — `--site`
selects which fixtures count as home, `--mongo-db` selects the target database:

```sh
go run ./tools/seed-sample-data --site site-local  --mongo-db chat
go run ./tools/seed-sample-data --site site-remote --mongo-db chat_remote
```

Both default to leaving today's behaviour alone, so plain `make seed` is
unchanged: `--site` defaults to empty, which means *unfiltered* — every fixture
is written — and `--mongo-db` falls back to `MONGO_DB`. (This design originally
specified a `site-local` default for `--site`; it was changed during
implementation, on the human partner's ruling that a bare `make seed` must
behave exactly as it does today, since defaulting to `site-local` would have
silently dropped the site-remote fixtures from the single-site dataset.) What
each pass writes:

- **Directory into both DBs.** All 11 users plus their `hr_employee` rows go
  into `chat` *and* `chat_remote`. Each portal must resolve every account in
  order to tell a client where its home site is. This mirrors what HR
  replication does in production.
- **Rooms, subscriptions and messages by home site.** Only fixtures whose
  `siteId` matches the target DB.
- **One seeded cross-site room** (alice + ivan, `crossSite: true`): the room doc
  and alice's subscription into `chat`, ivan's subscription into `chat_remote`.
  This is the one fixture that deliberately straddles both DBs, and it gives a
  plumbing check that does not depend on room-service working correctly.

## Make targets

All additive; no existing target changes behaviour.

| Target | What |
|---|---|
| `fed-deps-up` / `fed-deps-down` | the federated dep stack; runs `setup.sh` on first use, waits on healthchecks, then the two keyspace-templated `cassandra-init` passes and `vault-init` |
| `fed-up` / `fed-down` | both site projects; **detached**, because two Compose projects cannot both hold the foreground |
| `fed-logs` | `docker compose logs -f` across both projects, replacing the log streaming `make up` gives you |
| `fed-seed` | the two seeder passes above |
| `fed-up-lean` | `fed-up` with `FED_REMOTE_SERVICES` set to the Tier 1 list |

`fed-up` is guarded the same way `up` is: it refuses to start unless the
federated deps are running and the per-site env files and NATS confs exist.

## Trimming the remote peer

`FED_REMOTE_SERVICES` (empty = all) names the services site-remote starts:

```make
FED_REMOTE_SERVICES ?=

fed-up-remote:
	docker compose -p chat-site-remote --env-file docker-local/.env.site-remote \
	  -f docker-local/compose.services.yaml -f docker-local/compose.site.yaml \
	  up -d --build $(FED_REMOTE_SERVICES)
```

The constraint on trimming is
**stream ownership**, not features: services bootstrap the streams they own, and
dropping an owner means the stream is never created at that site. The failure is
quiet — a JetStream publish to a nonexistent stream gets no ack, so site-local's
`outbox-worker` Naks and retries forever (`MaxDeliver=-1`), parking events on its
per-peer consumer with nothing obviously wrong at the destination.

| Stream | Owner(s) |
|---|---|
| `INBOX-{site}` | inbox-worker |
| `OUTBOX-{site}` | outbox-worker |
| `MESSAGES`, `MESSAGES-CANONICAL` | message-gatekeeper (+ message-worker) |
| `ROOMS`, `ROOMS-TEAMS` | room-service, room-worker |
| `BOT-MESSAGES-CANONICAL` | bot-message-worker — not in the local stack at all |

**Tier 1 — must run (13 containers).** inbox-worker, outbox-worker, room-service,
room-worker, message-gatekeeper, message-worker, broadcast-worker, user-service,
history-service, auth-service, portal-service, traefik, chat-frontend. Drop
inbox-worker and federation dies at the destination; drop message-gatekeeper and
ivan cannot send at all, because his client publishes straight into
`MESSAGES-site-remote`.

**Tier 2 — drop and lose a visible feature.** search-service +
search-sync-worker (no search as ivan), upload-service + media-service (no
upload; `/api/v1/avatar` 404s through site-remote's Traefik), notification-worker
+ push-notification-service (no push; both are leaf consumers), and
user-presence-service (ivan reads as offline at both sites).

**Tier 3 — free drops, idle at both sites (6 containers).** The bot trio
(bot-broadcast-worker, bot-notification-worker, bot-push-notification-service)
consume `BOT-MESSAGES-CANONICAL-{site}`, which nothing publishes because
`bot-message-handler` is not in `compose.services.yaml`. botplatform-service only
serves portal's password-login forward, and ivan/judy log in tokenless under
`DEV_MODE=true`. admin-service and tcard-service sit on no federation path.

Each idle Go service is ~20-40MB RSS, so Tier 3 saves ~200MB and Tier 2+3 ~500MB
against a stack whose RAM is dominated by the shared Cassandra, ES and Keycloak.
Trimming is a first-run build-time optimisation more than a memory one.

## Acceptance criteria

1. `curl localhost:8222/gatewayz` shows site-remote inbound and outbound
   connected; `:8322` shows the converse.
2. `nats stream ls` against either server lists both sites' streams (one JS
   domain), and `INBOX-site-remote` reports its leader in cluster `site-remote`.
3. alice (`:3000`) creates a room and invites ivan; it appears in ivan's chat
   list (`:3100`). Proves room-worker → `OUTBOX-site-local` → gateway →
   `INBOX-site-remote` → inbox-worker → Mongo `chat_remote`.
4. alice sends a message and ivan sees it live; ivan replies and alice sees it.
   Proves the global `chat.room.{id}` lane crosses the gateway both ways.
5. `docker stop` site-remote's inbox-worker, have alice fire several membership
   changes, then restart it: all of them land, in order. Proves the durable
   OUTBOX retry and the per-destination FIFO lane behave as CLAUDE.md describes.

## Known divergences from production

- **`chat.local.room.>` crosses the gateway.** Production filters that lane at a
  leaf node; there are no leaf nodes locally, so interest propagates. Verified
  harmless: `chat-frontend/src/api/subscribeToRoomEvents` subscribes to exactly
  one lane per room, selected from `room.crossSite`, so no client ever receives
  both copies. Documented, not fixed.
- **Shared Vault** means both sites wrap room DEKs under the same KEK. Per-site
  encryption isolation is not testable in this environment.
- **Shared Keycloak** is not site-scoped and `DEV_MODE=true` bypasses OIDC
  anyway.
- **Two browser origins** (`:3000` and `:3100`) rather than two tabs on one
  origin, so the two logged-in sessions do not contend over localStorage.
- **~8GB RAM.** Release valves are `fed-up-lean` and skipping the o11y stack.
