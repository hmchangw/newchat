# Two Federated Sites in Docker Compose — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up a second site in the local dev stack so cross-site federation can be QA'd in a browser — alice on site-local at `:3000`, ivan on site-remote at `:3100`, sharing federated rooms.

**Architecture:** Three Docker networks (two site networks plus a NATS-only `chat-federation`), two Compose *projects* over the existing service compose files, and a 4-line network-name override so none of the 24 service compose files change. NATS and Valkey are twinned; Mongo/Cassandra/Elasticsearch/MinIO are shared containers isolated per site by db name, keyspace, index prefix and bucket.

**Tech Stack:** Docker Compose v5.1.1 (`include:`, `extends:`, `name:` network repointing), NATS 2.11 supercluster (cluster + gateway blocks, one operator/account, one JetStream domain), Go 1.25 (seeder), GNU Make, bash.

**Spec:** `docs/superpowers/specs/2026-08-14-two-site-federated-docker-compose-design.md`

## Global Constraints

- **Never modify any `<service>/deploy/docker-compose.yml`.** All 24 stay byte-identical. If a task seems to need an edit there, the design is wrong — stop and escalate.
- **The single-site flow must keep working unchanged:** `make up`, `make deps-up`, `make seed`, `make ui-up`, and `docker-local/.env` behave exactly as they do today.
- **Use `make` targets, never raw `go` commands** (CLAUDE.md §2). Tests are `make test SERVICE=tools/seed-sample-data`; lint is `make lint`.
- **TDD is mandatory for all Go changes** (CLAUDE.md §4): write the failing test, run it and confirm it fails, implement minimally, confirm pass, commit. Tasks 5 and 6 are Go and follow this strictly. Tasks 1–4 and 7 are YAML/shell/Make/docs and have no Go test cycle — they are verified by rendering commands instead.
- **Never commit `.env`, `.env.site-local`, `.env.site-remote`, `nats*.conf`, or `backend.creds`.** Verify `docker-local/.gitignore` covers the new filenames before the first commit that could pick them up.
- **Site IDs are exactly `site-local` and `site-remote`.** Do not introduce `site-a`/`site-b`.
- **Lint and tests run in a pre-commit hook.** Fix failures before retrying a commit.
- **Docker daemon required for Tasks 3, 4 and 8's runtime verification.** `docker compose config` works without a daemon and is the verification tool for Tasks 1–4; the actual bring-up in Task 8 needs a Docker host with ~8GB available to Docker.

---

## File Structure

| File | Responsibility |
|---|---|
| `docker-local/compose.site.yaml` (new) | Repoints the `chat-local` network key at a per-site network. Nothing else. |
| `docker-local/compose.fed-deps.yaml` (new) | The whole federated dep topology: 3 networks, 2× NATS, 2× Valkey, shared datastores via `extends:`, keyspace-templated `cassandra-init`, `vault-init`. |
| `docker-local/setup.sh` (modify) | Additionally emits `nats-site-local.conf`, `nats-site-remote.conf`, `.env.site-local`, `.env.site-remote`. Existing single-site outputs unchanged. |
| `docker-local/README.md` (modify) | Two-site section: topology, ports, service tiers, seeding, known divergences. |
| `Makefile` (modify) | `fed-deps-up/down`, `fed-up/down`, `fed-ui-up/down`, `fed-logs`, `fed-seed`, `fed-up-lean`, `require-fed-deps`. |
| `tools/seed-sample-data/site.go` (new) | Site-routing helpers: which site's DB each fixture row belongs in. One responsibility, heavily commented because the rule is non-obvious. |
| `tools/seed-sample-data/site_test.go` (new) | Table-driven tests for the routing rules. |
| `tools/seed-sample-data/main.go` (modify) | `--site` / `--mongo-db` flags; thread site through `run`. |
| `tools/seed-sample-data/mongo.go` (modify) | `upsertAll`/`writeSideStores` take a site and filter. |
| `tools/seed-sample-data/fixtures.go` (modify) | Set `CrossSite` on the three rooms that already span sites. |

**The routing rule, stated once here because two tasks depend on it:** a subscription row lives in the DB of the **subscriber's** home site, while carrying the **room's** `siteId`. See `user-service/mongorepo/subscriptions.go:35` — that field is how the service tells local rows from cross-site rows *within its own database*. Room-owned rows (rooms, room_members, messages, thread_rooms, room keys) live in the DB of the room's home site. Getting these backwards produces a dataset that looks seeded but renders an empty chat list for ivan.

---

### Task 1: The network-name override

**Files:**
- Create: `docker-local/compose.site.yaml`

**Interfaces:**
- Consumes: nothing.
- Produces: a compose overlay usable as `-f docker-local/compose.services.yaml -f docker-local/compose.site.yaml`, driven by the env var `CHAT_NETWORK` (default `chat-local`). Tasks 4 and 8 use it.

- [ ] **Step 1: Create the override file**

```yaml
# Repoints the `chat-local` network key at a per-site network so the same 24
# service compose files can run twice, once per site, without editing any of
# them. The key stays `chat-local` (every included file references it by that
# name); `name:` selects the real Docker network.
#
# Used as an overlay: -f compose.services.yaml -f compose.site.yaml
# Defaults to chat-local so a stray invocation without CHAT_NETWORK set behaves
# like the single-site stack rather than creating a surprise network.
networks:
  chat-local:
    name: ${CHAT_NETWORK:-chat-local}
    external: true
```

- [ ] **Step 2: Verify it renders the site network**

Run:
```bash
CHAT_NETWORK=chat-site-remote docker compose -p chat-site-remote \
  -f docker-local/compose.services.yaml -f docker-local/compose.site.yaml \
  config 2>/dev/null | grep -A3 '^networks:'
```
Expected: `chat-local:` with `name: chat-site-remote` and `external: true`.

- [ ] **Step 3: Verify the default is unchanged**

Run:
```bash
docker compose -f docker-local/compose.services.yaml -f docker-local/compose.site.yaml \
  config 2>/dev/null | grep -A3 '^networks:'
```
Expected: `name: chat-local` — the single-site stack is unaffected when `CHAT_NETWORK` is unset.

- [ ] **Step 4: Verify no service compose file changed**

Run: `git status --short`
Expected: only `docker-local/compose.site.yaml` as untracked. If any `*/deploy/docker-compose.yml` appears, revert it — that violates a global constraint.

- [ ] **Step 5: Commit**

```bash
git add docker-local/compose.site.yaml
git commit -m "feat(docker-local): add per-site network override for federated stacks"
```

---

### Task 2: setup.sh emits two NATS confs and two env files

**Files:**
- Modify: `docker-local/setup.sh` (append after the existing `nats.conf` write, ~line 214)
- Modify: `docker-local/.gitignore`

**Interfaces:**
- Consumes: the operator/account/SYS JWTs and `$ACCOUNT_PUB_KEY` / `$SYS_PUB_KEY` / `$AUTH_SK_SEED` shell variables already computed earlier in the script.
- Produces: `docker-local/nats-site-local.conf`, `docker-local/nats-site-remote.conf`, `docker-local/.env.site-local`, `docker-local/.env.site-remote`. Tasks 3 and 4 mount and load these.

The same operator, `chatapp` account and SYS account are reused verbatim. Gateway routing of JetStream publishes only works inside one account and one JetStream domain — do **not** generate a second operator.

- [ ] **Step 1: Add the two NATS confs**

Append to `docker-local/setup.sh`, after the existing `cat > "$NATS_CONF"` block:

```bash
# --- Federated two-site NATS confs -------------------------------------------
# Same operator/account/SYS as the single-site conf above: gateway routing of
# JetStream publishes only works inside one account and one JS domain, so the
# two servers are one supercluster, not two independent deployments.
# No jetstream{domain} on either side — that shared domain is what lets
# site-local's outbox-worker publish chat.inbox.site-remote.external.* on its
# own local connection and have it land in INBOX-site-remote.
write_fed_nats_conf() {
  local site="$1" peer="$2" out="$3"
  cat > "$out" <<EOF
# Generated by docker-local/setup.sh — do not commit this file.
# Regenerate with: ./docker-local/setup.sh

server_name: nats-${site}
port: 4222
http_port: 8222

operator: ${OPERATOR_JWT}

resolver: MEMORY

resolver_preload {
  ${ACCOUNT_PUB_KEY}: ${ACCOUNT_JWT}
  ${SYS_PUB_KEY}: ${SYS_JWT}
}

jetstream {
  store_dir: /data/jetstream
  max_mem: 1G
  max_file: 10G
}

websocket {
  port: 9222
  no_tls: true
}

cluster {
  name: ${site}
  port: 6222
}

gateway {
  name: ${site}
  port: 7222
  gateways: [
    { name: ${peer}, url: "nats://nats-${peer}:7222" }
  ]
}
EOF
}

write_fed_nats_conf site-local  site-remote "$SCRIPT_DIR/nats-site-local.conf"
write_fed_nats_conf site-remote site-local  "$SCRIPT_DIR/nats-site-remote.conf"
echo "Wrote $SCRIPT_DIR/nats-site-local.conf"
echo "Wrote $SCRIPT_DIR/nats-site-remote.conf"
```

- [ ] **Step 2: Add the two per-site env files**

Append after the block from Step 1:

```bash
# --- Federated two-site env files --------------------------------------------
# One file per site, loaded via --env-file by the fed-* make targets. Only the
# values that must differ are set; everything else falls through to the
# ${VAR:-default} defaults in the service compose files.
#
# ALL_SITE_IDS is identical on both sides — outbox-worker's federationPeers()
# drops the local site itself.
#
# Elasticsearch indexes and the MinIO bucket need no entries: search-service,
# search-sync-worker and upload-service already derive them from SITE_ID.
write_fed_env() {
  local site="$1" network="$2" mongo_db="$3" keyspace="$4" out="$5"
  shift 5
  cat > "$out" <<EOF
# Generated by docker-local/setup.sh — do not commit this file.
# Regenerate with: ./docker-local/setup.sh

AUTH_SCOPED_SIGNING_KEY=${AUTH_SK_SEED}
AUTH_ACCOUNT_PUB_KEY=${ACCOUNT_PUB_KEY}

NATS_URL=nats://nats:4222
NATS_CREDS_FILE=/etc/nats/backend.creds
DEV_MODE=true

SITE_ID=${site}
ALL_SITE_IDS=site-local,site-remote
CHAT_NETWORK=${network}
MONGO_DB=${mongo_db}
CASSANDRA_KEYSPACE=${keyspace}

# Distinguishes the two sites' spans in Tempo. Without it both sites'
# room-service spans carry identical resource attributes and a federated trace
# cannot be read.
OTEL_RESOURCE_ATTRIBUTES=site.id=${site}

# Site directory, identical on both sites: a client asks either portal and is
# told where its home site actually lives.
PORTAL_SITE_URLS={"site-local":{"baseUrl":"http://localhost:7777","natsUrl":"ws://localhost:9222"},"site-remote":{"baseUrl":"http://localhost:7877","natsUrl":"ws://localhost:9322"}}
CLUSTER_DOMAINS=[{"siteID":"site-local","domain":"http://localhost:7777"},{"siteID":"site-remote","domain":"http://localhost:7877"}]
$(printf '%s\n' "$@")
EOF
  chmod 600 "$out"
}

write_fed_env site-local chat-site-local chat chat "$SCRIPT_DIR/.env.site-local"

# site-remote takes a +100 host-port band. AUTH_SERVICE_HOST_PORT is the one
# exception (8080 -> 8190): 8180 is Keycloak.
write_fed_env site-remote chat-site-remote chat_remote chat_remote "$SCRIPT_DIR/.env.site-remote" \
  "CHAT_FRONTEND_HOST_PORT=3100" \
  "ADMIN_FRONTEND_HOST_PORT=3101" \
  "GATEWAY_HOST_PORT=7877" \
  "PORTAL_SERVICE_HOST_PORT=8185" \
  "AUTH_SERVICE_HOST_PORT=8190" \
  "ADMIN_SERVICE_HOST_PORT=8182" \
  "UPLOAD_SERVICE_HOST_PORT=8186" \
  "TCARD_SERVICE_HOST_PORT=8187" \
  "SEARCH_SERVICE_HOST_PORT=19190" \
  "PORTAL_URL=http://localhost:8185"

echo "Wrote $SCRIPT_DIR/.env.site-local"
echo "Wrote $SCRIPT_DIR/.env.site-remote"
```

- [ ] **Step 3: Ignore the generated files**

Check `docker-local/.gitignore` and ensure it covers the new names. Read it first; if `nats.conf` and `.env` are listed individually rather than by glob, add:

```gitignore
nats-site-local.conf
nats-site-remote.conf
.env.site-local
.env.site-remote
```

- [ ] **Step 4: Run setup.sh and verify the outputs**

Run: `./docker-local/setup.sh`

Then:
```bash
grep -c 'gateway {' docker-local/nats-site-local.conf docker-local/nats-site-remote.conf
grep 'name: site-' docker-local/nats-site-local.conf
grep -E '^(SITE_ID|MONGO_DB|CHAT_NETWORK|GATEWAY_HOST_PORT)=' docker-local/.env.site-remote
```
Expected: each conf has exactly one `gateway {`; site-local's conf shows `cluster { name: site-local` and a gateway entry naming `site-remote`; the remote env file shows `SITE_ID=site-remote`, `MONGO_DB=chat_remote`, `CHAT_NETWORK=chat-site-remote`, `GATEWAY_HOST_PORT=7877`.

- [ ] **Step 5: Verify the single-site outputs are unchanged**

Run: `git status --short docker-local/`
Expected: `nats.conf`, `.env`, `backend.creds` and the new files are all ignored — nothing generated shows up as untracked or modified. Only `docker-local/setup.sh` and possibly `.gitignore` are modified.

- [ ] **Step 6: Commit**

```bash
git add docker-local/setup.sh docker-local/.gitignore
git commit -m "feat(docker-local): generate per-site NATS confs and env files"
```

---

### Task 3: The federated dep stack

**Files:**
- Create: `docker-local/compose.fed-deps.yaml`

**Interfaces:**
- Consumes: `docker-local/compose.deps.yaml` (via `extends:`), `nats-site-local.conf` / `nats-site-remote.conf` from Task 2.
- Produces: networks `chat-site-local`, `chat-site-remote`, `chat-federation`; containers `chat-fed-nats-site-local` / `chat-fed-nats-site-remote` (alias `nats` on their site network) and `chat-fed-valkey-site-local` / `chat-fed-valkey-site-remote` (alias `valkey`); shared datastores reachable as `mongodb`, `cassandra`, `elasticsearch`, `minio`, `keycloak`, `vault` from both site networks. Task 4 starts it.

Two mechanics matter here, both verified against Compose v5.1.1:
1. `extends:` **unions** the source service's networks rather than replacing them. Omitting the repoint fails with `service "mongodb" refers to undefined network chat-local`. So the inherited `chat-local` key is repointed at `chat-site-local`, and `chat-site-remote` is added alongside.
2. `container_name` is inherited by `extends:` and must be overridden to `chat-fed-*`, or it collides with a running single-site stack.

- [ ] **Step 1: Create the file**

```yaml
name: chat-fed-deps

# Federated counterpart of compose.deps.yaml. Owns everything that is not a Go
# service: the three networks, the two NATS servers (supercluster gateway pair),
# the two Valkeys, and the shared datastores.
#
# Shared datastores are pulled in with `extends:` rather than copied, so their
# image pins, healthchecks and volumes stay single-sourced in compose.deps.yaml.
# `extends` UNIONS the source's networks instead of replacing them, so the
# inherited `chat-local` key is repointed at chat-site-local below and
# chat-site-remote is added per service. container_name is overridden because
# the inherited chat-local-* name would collide with a single-site stack.
#
# Start: make fed-deps-up   Stop: make fed-deps-down
# Cannot run at the same time as the single-site deps stack — both publish the
# same host ports for the shared datastores.

services:
  # --- Per-site NATS: one supercluster, two clusters, gateway-linked ----------
  # Each joins its site network under the alias `nats` so every service keeps
  # NATS_URL=nats://nats:4222 unchanged, plus chat-federation for the gateway.
  nats-site-local:
    image: nats:2.11-alpine
    container_name: chat-fed-nats-site-local
    ports:
      - "4222:4222"
      - "8222:8222"
      - "9222:9222"
    volumes:
      - ./nats-site-local.conf:/etc/nats/nats.conf:ro
      - nats-site-local-data:/data/jetstream
    command: ["-c", "/etc/nats/nats.conf"]
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://localhost:8222/healthz || exit 1"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 5s
    networks:
      chat-local: { aliases: [nats] }
      chat-federation: {}

  nats-site-remote:
    image: nats:2.11-alpine
    container_name: chat-fed-nats-site-remote
    ports:
      - "4322:4222"
      - "8322:8222"
      - "9322:9222"
    volumes:
      - ./nats-site-remote.conf:/etc/nats/nats.conf:ro
      - nats-site-remote-data:/data/jetstream
    command: ["-c", "/etc/nats/nats.conf"]
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://localhost:8222/healthz || exit 1"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 5s
    networks:
      chat-site-remote: { aliases: [nats] }
      chat-federation: {}

  # --- Per-site Valkey -------------------------------------------------------
  # Twinned rather than shared: keys (presence:{account}:conns, room key pairs)
  # carry no site prefix. Same single-node cluster-mode entrypoint as
  # compose.deps.yaml; --cluster-announce-hostname valkey matches the alias.
  valkey-site-local:
    image: valkey/valkey:8.1.7-alpine
    container_name: chat-fed-valkey-site-local
    ports:
      - "6379:6379"
    entrypoint: &valkey-entrypoint
      - sh
      - -c
      - |
        valkey-server --cluster-enabled yes --cluster-config-file /tmp/nodes.conf --cluster-node-timeout 5000 --cluster-announce-hostname valkey --cluster-preferred-endpoint-type hostname --save '' --appendonly no &
        until valkey-cli ping > /dev/null 2>&1; do sleep 0.1; done
        if ! valkey-cli CLUSTER INFO | grep -q 'cluster_slots_assigned:16384'; then
          valkey-cli CLUSTER ADDSLOTSRANGE 0 16383
        fi
        wait
    healthcheck: &valkey-healthcheck
      test: ["CMD-SHELL", "valkey-cli CLUSTER INFO | grep 'cluster_state:ok'"]
      interval: 5s
      timeout: 3s
      retries: 10
    networks:
      chat-local: { aliases: [valkey] }

  valkey-site-remote:
    image: valkey/valkey:8.1.7-alpine
    container_name: chat-fed-valkey-site-remote
    ports:
      - "6479:6379"
    entrypoint: *valkey-entrypoint
    healthcheck: *valkey-healthcheck
    networks:
      chat-site-remote: { aliases: [valkey] }

  # --- Shared datastores, on both site networks ------------------------------
  mongodb:
    extends: { file: ./compose.deps.yaml, service: mongodb }
    container_name: chat-fed-mongodb
    networks: [chat-site-remote]

  cassandra:
    extends: { file: ./compose.deps.yaml, service: cassandra }
    container_name: chat-fed-cassandra
    networks: [chat-site-remote]

  elasticsearch:
    extends: { file: ./compose.deps.yaml, service: elasticsearch }
    container_name: chat-fed-elasticsearch
    networks: [chat-site-remote]

  minio:
    extends: { file: ./compose.deps.yaml, service: minio }
    container_name: chat-fed-minio
    networks: [chat-site-remote]

  keycloak:
    extends: { file: ./compose.deps.yaml, service: keycloak }
    container_name: chat-fed-keycloak
    networks: [chat-site-remote]

  vault:
    extends: { file: ./compose.deps.yaml, service: vault }
    container_name: chat-fed-vault
    networks: [chat-site-remote]

  # --- One-shots -------------------------------------------------------------
  # Keyspace-templated schema bootstrap. Every keyspace reference in the .cql
  # files is `chat.<object>` except a single bare `CREATE KEYSPACE IF NOT
  # EXISTS chat`, so one sed rule covers all of them and the .cql files stay
  # byte-identical (CLAUDE.md makes them a mandated mirror of
  # docs/cassandra_message_model.md). Run once per site with a different
  # KEYSPACE; the Makefile drives both passes.
  cassandra-init:
    image: cassandra:5
    container_name: chat-fed-cassandra-init
    profiles: ["init"]
    depends_on:
      cassandra:
        condition: service_healthy
    restart: "no"
    environment:
      - KEYSPACE=${KEYSPACE:-chat}
    volumes:
      - ./cassandra/init:/init:ro
    entrypoint: ["/bin/bash", "-c"]
    command:
      - |
        set -euo pipefail
        shopt -s nullglob
        for f in /init/*.cql; do
          echo "==> applying $$f as keyspace $${KEYSPACE}"
          sed "s/\bchat\b/$${KEYSPACE}/g" "$$f" | cqlsh cassandra
        done
        echo "Schema initialized for $${KEYSPACE}"
    networks:
      - chat-local

  vault-init:
    extends: { file: ./compose.deps.yaml, service: vault-init }
    container_name: chat-fed-vault-init
    networks: [chat-site-remote]

volumes:
  nats-site-local-data:
  nats-site-remote-data:
  mongo-data:
  cassandra-data:
  es-data:
  minio-data:

networks:
  # `chat-local` is the key every extended service inherits; it points at the
  # site-local network. chat-site-remote is added explicitly per service.
  chat-local:
    name: chat-site-local
  chat-site-remote:
    name: chat-site-remote
  chat-federation:
    name: chat-federation
```

- [ ] **Step 2: Verify it renders and the networks are right**

Run:
```bash
docker compose -f docker-local/compose.fed-deps.yaml config 2>&1 | head -5
```
Expected: valid YAML output starting `name: chat-fed-deps`, no `refers to undefined network` error.

- [ ] **Step 3: Verify mongodb is on both site networks and kept its inherited config**

Run:
```bash
docker compose -f docker-local/compose.fed-deps.yaml config 2>/dev/null \
  | awk '/^  mongodb:/,/^  [a-z]/' | head -25
```
Expected: `container_name: chat-fed-mongodb`, `image: mongo:8.2.9` (inherited), and both `chat-local` and `chat-site-remote` under `networks`.

- [ ] **Step 4: Verify the NATS aliases**

Run:
```bash
docker compose -f docker-local/compose.fed-deps.yaml config 2>/dev/null \
  | grep -B2 -A6 'nats-site-remote:'
```
Expected: `chat-site-remote` with `aliases: [nats]`, plus `chat-federation`. This alias is what keeps `NATS_URL=nats://nats:4222` working unmodified in all 24 service files.

- [ ] **Step 5: Commit**

```bash
git add docker-local/compose.fed-deps.yaml
git commit -m "feat(docker-local): add federated two-site dependency stack"
```

---

### Task 4: Makefile targets

**Files:**
- Modify: `Makefile` (add to `.PHONY` at line 1-3; add a new section after the existing local-dev docker targets, ~line 235)

**Interfaces:**
- Consumes: `docker-local/compose.fed-deps.yaml` (Task 3), `docker-local/compose.site.yaml` (Task 1), `.env.site-local` / `.env.site-remote` / the two NATS confs (Task 2).
- Produces: `fed-deps-up`, `fed-deps-down`, `fed-up`, `fed-down`, `fed-ui-up`, `fed-ui-down`, `fed-logs`, `fed-seed`, `fed-up-lean`. Tasks 7 and 8 reference these by name.

Note: `fed-ui-up` was not in the spec's target table — the two chat-frontends live in `compose.ui.yaml`, not `compose.services.yaml`, so they need their own target for the same reason `ui-up` exists today.

- [ ] **Step 1: Add variables**

Add near the existing compose variables (after line 21):

```make
FED_DEPS_COMPOSE := docker-local/compose.fed-deps.yaml
SITE_OVERRIDE    := docker-local/compose.site.yaml
FED_ENV_LOCAL    := docker-local/.env.site-local
FED_ENV_REMOTE   := docker-local/.env.site-remote
FED_NATS_LOCAL   := docker-local/nats-site-local.conf
FED_NATS_REMOTE  := docker-local/nats-site-remote.conf
FED_NATS_CONTAINER := chat-fed-nats-site-local

# Services site-remote starts. Empty = every service. Set to trim the remote
# peer; see the tier table in docker-local/README.md for what each drop costs.
FED_REMOTE_SERVICES ?=

# Tier 1: the minimum that keeps federation and a logged-in browser working.
# Dropping inbox-worker kills federation at the destination; dropping
# message-gatekeeper means ivan cannot send at all.
FED_TIER1 := inbox-worker outbox-worker room-service room-worker \
             message-gatekeeper message-worker broadcast-worker user-service \
             history-service auth-service portal-service traefik
```

- [ ] **Step 2: Add the targets**

```make
# --- Federated two-site local dev ---------------------------------------------
# A second site so cross-site federation can be QA'd in a browser: alice on
# site-local (:3000), ivan on site-remote (:3100). See docker-local/README.md.
#
# Cannot run alongside the single-site stack — both publish the same host ports
# for the shared datastores.
fed-deps-up:
	@docker container inspect -f '{{.State.Running}}' $(NATS_CONTAINER) 2>/dev/null | grep -q true && { \
	  echo "Single-site deps are running. Run 'make deps-down' first — the two stacks share host ports."; exit 1; \
	} || true
	@if [ ! -f $(NATS_CREDS) ] || [ ! -f $(FED_NATS_LOCAL) ] || [ ! -f $(FED_ENV_LOCAL) ]; then \
	  echo "First-time setup: generating NATS confs + env files..."; \
	  ./docker-local/setup.sh; \
	fi
	docker compose -f $(FED_DEPS_COMPOSE) up -d --wait
	KEYSPACE=chat docker compose -f $(FED_DEPS_COMPOSE) --profile init run --rm cassandra-init
	KEYSPACE=chat_remote docker compose -f $(FED_DEPS_COMPOSE) --profile init run --rm cassandra-init
	docker compose -f $(FED_DEPS_COMPOSE) --profile init run --rm vault-init

fed-deps-down:
	docker compose -f $(FED_DEPS_COMPOSE) down

# Guard: federated deps must be running and the per-site files present.
require-fed-deps:
	@docker container inspect -f '{{.State.Running}}' $(FED_NATS_CONTAINER) 2>/dev/null | grep -q true || { \
	  echo "Federated deps are not running. Run 'make fed-deps-up' first."; exit 1; \
	}
	@test -f $(FED_ENV_LOCAL) && test -f $(FED_ENV_REMOTE) || { \
	  echo "Missing per-site env files. Run './docker-local/setup.sh'."; exit 1; \
	}

# Both sites. Detached, because two Compose projects cannot both hold the
# foreground — use `make fed-logs` for the streaming view `make up` gives you.
fed-up: require-fed-deps
	docker compose -p chat-site-local --env-file $(FED_ENV_LOCAL) \
	  -f $(SERVICES_COMPOSE) -f $(SITE_OVERRIDE) up -d --build
	docker compose -p chat-site-remote --env-file $(FED_ENV_REMOTE) \
	  -f $(SERVICES_COMPOSE) -f $(SITE_OVERRIDE) up -d --build $(FED_REMOTE_SERVICES)

# fed-up with the remote peer trimmed to Tier 1.
fed-up-lean:
	$(MAKE) --no-print-directory fed-up FED_REMOTE_SERVICES="$(FED_TIER1)"

fed-down:
	docker compose -p chat-site-remote --env-file $(FED_ENV_REMOTE) \
	  -f $(SERVICES_COMPOSE) -f $(SITE_OVERRIDE) down
	docker compose -p chat-site-local --env-file $(FED_ENV_LOCAL) \
	  -f $(SERVICES_COMPOSE) -f $(SITE_OVERRIDE) down

# chat-frontend :3000/:3100, admin-frontend :3001/:3101.
fed-ui-up: require-fed-deps
	docker compose -p chat-site-local-ui --env-file $(FED_ENV_LOCAL) \
	  -f $(UI_COMPOSE) -f $(SITE_OVERRIDE) up -d --build
	docker compose -p chat-site-remote-ui --env-file $(FED_ENV_REMOTE) \
	  -f $(UI_COMPOSE) -f $(SITE_OVERRIDE) up -d --build

fed-ui-down:
	docker compose -p chat-site-remote-ui --env-file $(FED_ENV_REMOTE) \
	  -f $(UI_COMPOSE) -f $(SITE_OVERRIDE) down
	docker compose -p chat-site-local-ui --env-file $(FED_ENV_LOCAL) \
	  -f $(UI_COMPOSE) -f $(SITE_OVERRIDE) down

# Streaming logs across both site projects.
fed-logs:
	docker compose -p chat-site-local --env-file $(FED_ENV_LOCAL) \
	  -f $(SERVICES_COMPOSE) -f $(SITE_OVERRIDE) logs -f & \
	docker compose -p chat-site-remote --env-file $(FED_ENV_REMOTE) \
	  -f $(SERVICES_COMPOSE) -f $(SITE_OVERRIDE) logs -f; \
	kill %1 2>/dev/null || true

# Seed both sites. The directory (users + hr_employee) goes into both
# databases so either portal can resolve any account; room-owned and
# subscriber-owned rows are routed to their home site. See the seeding section
# of docker-local/README.md.
fed-seed:
	MONGO_DB=chat go run ./tools/seed-sample-data --site site-local --mongo-db chat
	MONGO_DB=chat_remote VALKEY_ADDRS=localhost:6479 \
	  go run ./tools/seed-sample-data --site site-remote --mongo-db chat_remote
```

- [ ] **Step 3: Add every new target to `.PHONY`**

Extend the `.PHONY` list at the top of the Makefile with:
`fed-deps-up fed-deps-down require-fed-deps fed-up fed-up-lean fed-down fed-ui-up fed-ui-down fed-logs fed-seed`

- [ ] **Step 4: Verify the recipes expand correctly**

Run: `make -n fed-up 2>&1 | head -20`
Expected: two `docker compose` invocations, one per project, each with `--env-file docker-local/.env.site-<id>` and both `-f` flags. (The guard runs first and may fail if deps aren't up — that's expected; `make -n` prints without executing.)

Run: `make -n fed-up-lean 2>&1 | grep -o 'FED_REMOTE_SERVICES=.*' | head -1`
Expected: the Tier 1 service list.

- [ ] **Step 5: Verify the single-site targets are untouched**

Run: `git diff Makefile | grep '^-' | grep -v '^---'`
Expected: only the `.PHONY` line is removed-and-readded. No existing recipe body appears as a deletion.

- [ ] **Step 6: Commit**

```bash
git add Makefile
git commit -m "feat(make): add fed-* targets for the two-site local stack"
```

---

### Task 5: Seeder site routing

**Files:**
- Create: `tools/seed-sample-data/site.go`
- Create: `tools/seed-sample-data/site_test.go`
- Modify: `tools/seed-sample-data/main.go`
- Modify: `tools/seed-sample-data/mongo.go`

**Interfaces:**
- Consumes: existing `BuildUsers()`, `BuildRooms()`, `BuildSubscriptions()`, `BuildRoomMembers()`, `BuildMessages()`, `BuildThreadRooms()`, `BuildThreadSubscriptions()`, `BuildRoomKeys()`, `BuildRestrictedCache()`.
- Produces:
  - `func filterBySite[T any](items []T, site string, homeSite func(T) string) []T`
  - `func userSiteByAccount() map[string]string`
  - `func roomSiteByID() map[string]string`
  - `upsertAll(ctx context.Context, db *mongo.Database, site string) (mongoCounts, error)`
  - `writeSideStores(ctx context.Context, keyStore *roomkeystore.MongoStore, vk valkeyutil.Client, site string) (sideCounts, error)`

  Task 6 consumes `roomSiteByID()`.

**The rule, restated because it is the whole point of this task:** subscriptions and thread_subscriptions live in the **subscriber's** site DB while carrying the **room's** `siteId`; everything else room-owned lives in the **room's** site DB. `user-service/mongorepo/subscriptions.go:35` documents why.

- [ ] **Step 1: Write the failing tests**

Create `tools/seed-sample-data/site_test.go`:

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestUserSiteByAccount_MapsSeededUsersToHomeSites(t *testing.T) {
	got := userSiteByAccount()

	assert.Equal(t, "site-local", got["alice"])
	assert.Equal(t, "site-remote", got["ivan"])
	assert.Equal(t, "site-remote", got["judy"])
	assert.Len(t, got, len(BuildUsers()))
}

func TestRoomSiteByID_MapsSeededRoomsToHomeSites(t *testing.T) {
	got := roomSiteByID()

	assert.Equal(t, "site-local", got["r-general"])
	assert.Equal(t, "site-remote", got["r-remote-announce"])
	assert.Len(t, got, len(BuildRooms()))
}

func TestFilterBySite(t *testing.T) {
	type row struct {
		id   string
		site string
	}
	homeSite := func(r row) string { return r.site }
	rows := []row{
		{id: "a", site: "site-local"},
		{id: "b", site: "site-remote"},
		{id: "c", site: "site-local"},
	}

	tests := []struct {
		name    string
		site    string
		wantIDs []string
	}{
		{name: "local site keeps only local rows", site: "site-local", wantIDs: []string{"a", "c"}},
		{name: "remote site keeps only remote rows", site: "site-remote", wantIDs: []string{"b"}},
		{name: "unknown site keeps nothing", site: "site-nope", wantIDs: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterBySite(rows, tt.site, homeSite)

			gotIDs := make([]string, 0, len(got))
			for _, r := range got {
				gotIDs = append(gotIDs, r.id)
			}
			assert.Equal(t, tt.wantIDs, gotIDs)
		})
	}
}

func TestFilterBySite_EmptyInput(t *testing.T) {
	got := filterBySite([]model.Room{}, "site-local", func(r model.Room) string { return r.SiteID })

	assert.Empty(t, got)
}

// The invariant that makes the two-site dataset usable: a subscription row
// lives in the DB of the SUBSCRIBER's home site, but keeps the ROOM's siteId.
// Routing these by sub.SiteID instead would put ivan's rows in site-local's
// database and render an empty chat list for him.
func TestFilterBySite_SubscriptionsRouteBySubscriberNotRoom(t *testing.T) {
	userSite := userSiteByAccount()
	subscriberSite := func(s model.Subscription) string { return userSite[s.User.Account] }

	all := BuildSubscriptions()
	remote := filterBySite(all, "site-remote", subscriberSite)

	require.NotEmpty(t, remote, "ivan and judy have subscriptions")
	for _, s := range remote {
		assert.Contains(t, []string{"ivan", "judy"}, s.User.Account)
	}

	// ivan is a member of r-general, which is homed at site-local. His row must
	// land in the remote set while still carrying the room's siteId.
	var ivanGeneral *model.Subscription
	for i := range remote {
		if remote[i].User.Account == "ivan" && remote[i].RoomID == "r-general" {
			ivanGeneral = &remote[i]
			break
		}
	}
	require.NotNil(t, ivanGeneral, "ivan's r-general subscription belongs in the remote set")
	assert.Equal(t, "site-local", ivanGeneral.SiteID,
		"row keeps the room's siteId even though it lives in the remote site's DB")
}

func TestFilterBySite_RoomsRouteByRoomHomeSite(t *testing.T) {
	roomSite := roomSiteByID()

	local := filterBySite(BuildRooms(), "site-local", func(r model.Room) string { return r.SiteID })
	remote := filterBySite(BuildRooms(), "site-remote", func(r model.Room) string { return r.SiteID })

	assert.Len(t, remote, 1, "only r-remote-announce is homed remotely")
	assert.Equal(t, "r-remote-announce", remote[0].ID)
	assert.Len(t, local, len(BuildRooms())-1)
	assert.Equal(t, "site-remote", roomSite["r-remote-announce"])
}

func TestFilterBySite_EverySubscriptionLandsAtExactlyOneSite(t *testing.T) {
	userSite := userSiteByAccount()
	subscriberSite := func(s model.Subscription) string { return userSite[s.User.Account] }

	all := BuildSubscriptions()
	local := filterBySite(all, "site-local", subscriberSite)
	remote := filterBySite(all, "site-remote", subscriberSite)

	assert.Len(t, append(local, remote...), len(all),
		"no subscription is dropped or duplicated across the two sites")
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=tools/seed-sample-data`
Expected: FAIL — `undefined: filterBySite`, `undefined: userSiteByAccount`, `undefined: roomSiteByID`.

- [ ] **Step 3: Write the minimal implementation**

Create `tools/seed-sample-data/site.go`:

```go
package main

import "github.com/hmchangw/chat/pkg/model"

// Site routing: which site's database each seeded row belongs in.
//
// Two rules apply, and mixing them up produces a dataset that looks seeded but
// renders an empty chat list for the remote user:
//
//   - Room-owned rows (rooms, room_members, messages, thread_rooms, room keys)
//     live in the database of the ROOM's home site.
//   - Subscriber-owned rows (subscriptions, thread_subscriptions, the Valkey
//     restricted-rooms cache) live in the database of the SUBSCRIBER's home
//     site, while still carrying the room's siteId. That field is how a service
//     tells local rows from cross-site rows within its own database — see
//     user-service/mongorepo/subscriptions.go:35.
//
// Users and hr_employee rows are deliberately not routed: the full directory is
// written to both databases so either portal can resolve any account and tell a
// client where its home site is.

// filterBySite keeps the items whose home site matches, using the caller's
// rule for deciding an item's home site.
func filterBySite[T any](items []T, site string, homeSite func(T) string) []T {
	out := make([]T, 0, len(items))
	for _, it := range items {
		if homeSite(it) == site {
			out = append(out, it)
		}
	}
	return out
}

// userSiteByAccount indexes the seeded users by account to their home site.
func userSiteByAccount() map[string]string {
	users := BuildUsers()
	out := make(map[string]string, len(users))
	for i := range users {
		out[users[i].Account] = users[i].SiteID
	}
	return out
}

// roomSiteByID indexes the seeded rooms by ID to their home site.
func roomSiteByID() map[string]string {
	rooms := BuildRooms()
	out := make(map[string]string, len(rooms))
	for i := range rooms {
		out[rooms[i].ID] = rooms[i].SiteID
	}
	return out
}

// Home-site accessors, one per fixture type, so call sites read as intent
// rather than as an inline closure. Each rebuilds roomSiteByID/userSiteByAccount
// per call; with a handful of seeded rooms that is not worth caching.
func roomHomeSite(r model.Room) string        { return r.SiteID }
func messageHomeSite(m model.Message) string  { return roomSiteByID()[m.RoomID] }
func memberHomeSite(m model.RoomMember) string { return roomSiteByID()[m.RoomID] }
func threadRoomHomeSite(tr model.ThreadRoom) string { return roomSiteByID()[tr.RoomID] }
func roomKeyHomeSite(e RoomKeyEntry) string   { return roomSiteByID()[e.RoomID] }

func subscriptionHomeSite(s model.Subscription) string {
	return userSiteByAccount()[s.User.Account]
}

func threadSubscriptionHomeSite(ts model.ThreadSubscription) string {
	return userSiteByAccount()[ts.UserAccount]
}

func restrictedCacheHomeSite(e RestrictedCacheEntry) string {
	return userSiteByAccount()[e.Account]
}
```

All field names above are verified against `pkg/model/`: `RoomMember.RoomID` (`member.go:44`), `ThreadRoom.RoomID` (`threadroom.go:9`), `ThreadSubscription.UserAccount` (`threadsubscription.go:16`), `Subscription.User.Account`. If something still does not compile, correct the accessor — never the routing rule.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make test SERVICE=tools/seed-sample-data`
Expected: PASS, all tests.

- [ ] **Step 5: Commit the routing helpers**

```bash
git add tools/seed-sample-data/site.go tools/seed-sample-data/site_test.go
git commit -m "feat(seed): add per-site routing helpers for the two-site dataset"
```

- [ ] **Step 6: Write the failing test for the CLI flags**

Append to `tools/seed-sample-data/site_test.go`:

```go
func TestResolveTarget(t *testing.T) {
	tests := []struct {
		name        string
		envDB       string
		flagDB      string
		flagSite    string
		wantDB      string
		wantSite    string
	}{
		{
			name:   "defaults match the single-site stack",
			envDB:  "chat",
			wantDB: "chat", wantSite: "site-local",
		},
		{
			name:   "flag overrides the env database",
			envDB:  "chat", flagDB: "chat_remote", flagSite: "site-remote",
			wantDB: "chat_remote", wantSite: "site-remote",
		},
		{
			name:   "empty flag falls back to env",
			envDB:  "chat_custom", flagDB: "", flagSite: "site-local",
			wantDB: "chat_custom", wantSite: "site-local",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, site := resolveTarget(tt.envDB, tt.flagDB, tt.flagSite)

			assert.Equal(t, tt.wantDB, db)
			assert.Equal(t, tt.wantSite, site)
		})
	}
}
```

- [ ] **Step 7: Run it to verify it fails**

Run: `make test SERVICE=tools/seed-sample-data`
Expected: FAIL — `undefined: resolveTarget`.

- [ ] **Step 8: Implement `resolveTarget` and wire the flags**

Add to `tools/seed-sample-data/site.go`:

> Corrected after implementation: this step originally defaulted an empty
> `--site` to `site-local`. The human partner ruled mid-implementation that "make
> seed without any parameter behaves exactly as it does today", and a `site-local`
> default does not — it filters the site-remote fixtures out of the single-site
> dataset. The shipped default is empty, meaning unfiltered; both the code and the
> flag default below are the shipped version.

```go
// resolveTarget picks the database and site to seed. The --mongo-db flag
// overrides MONGO_DB when non-empty. An empty --site is passed through
// unchanged: it means unfiltered, single-site seeding — a plain `make seed`
// behaves exactly as it did before per-site routing existed. Explicit
// per-site seeding (e.g. `make fed-seed`) always passes --site itself.
func resolveTarget(envDB, flagDB, flagSite string) (db, site string) {
	db = envDB
	if flagDB != "" {
		db = flagDB
	}
	return db, flagSite
}
```

In `tools/seed-sample-data/main.go`, update the doc comment's flag list and `main`:

```go
//	--site      home site to scope seeding to: site-local or site-remote
//	            (default: unfiltered, seeds every fixture — the single-site path)
//	--mongo-db  target database, overriding MONGO_DB
```

```go
	reset := flag.Bool("reset", false, "delete seed records before re-populating")
	dryRun := flag.Bool("dry-run", false, "print the plan and exit without writing")
	site := flag.String("site", "", "home site to scope seeding to: site-local or site-remote (default: unfiltered, single-site seeding)")
	mongoDB := flag.String("mongo-db", "", "target database, overriding MONGO_DB")
	flag.Parse()

	if *dryRun {
		slog.Info("seed dry-run summary", "site", *site, "plan", dryRunSummary(*site))
		return
	}

	if err := run(*reset, *site, *mongoDB); err != nil {
```

Change `run` to accept and apply them:

```go
func run(reset bool, site, mongoDBFlag string) error {
	cfg, err := parseConfig(envFromOS())
	if err != nil {
		return err
	}
	dbName, site := resolveTarget(cfg.MongoDB, mongoDBFlag, site)
	...
	db := mongoClient.Database(dbName)
```

and pass `site` into `upsertAll(ctx, db, site)` and `writeSideStores(ctx, keyStore, valkeyClient, site)`, adding `"site", site` to the final `slog.Info("seed complete", ...)` call.

- [ ] **Step 9: Run the tests to verify they pass**

Run: `make test SERVICE=tools/seed-sample-data`
Expected: PASS. Compilation will fail first on `upsertAll`/`writeSideStores`/`dryRunSummary` arity — Step 10 fixes those.

- [ ] **Step 10: Apply the filters in mongo.go and the dry-run summary**

In `tools/seed-sample-data/mongo.go`, change `upsertAll` to `func upsertAll(ctx context.Context, db *mongo.Database, site string) (mongoCounts, error)` and filter each fixture slice at its call site. Users and hr_employee stay **unfiltered**:

```go
	// The full directory goes into every site's database: each portal must
	// resolve any account to tell a client where its home site is. This mirrors
	// what HR replication does in production.
	users := mongoutil.NewCollection[model.User](db.Collection("users"))
	// ... unchanged, still BuildUsers()

	rooms := mongoutil.NewCollection[model.Room](db.Collection("rooms"))
	// ... use filterBySite(BuildRooms(), site, roomHomeSite)

	subs := mongoutil.NewCollection[model.Subscription](db.Collection("subscriptions"))
	// ... use filterBySite(BuildSubscriptions(), site, subscriptionHomeSite)
```

Apply the matching accessor to `room_members` (`memberHomeSite`), `messages` (`messageHomeSite`), `thread_rooms` (`threadRoomHomeSite`) and `thread_subscriptions` (`threadSubscriptionHomeSite`).

Do the same in `writeSideStores` with `roomKeyHomeSite` and `restrictedCacheHomeSite`.

Leave `deleteAll` unfiltered: it deletes by explicit ID list, and `DeleteMany` on IDs absent from that database is a no-op. Filtering it would add code that changes nothing.

Update `dryRunSummary` to take the site and report filtered counts:

```go
func dryRunSummary(site string) string {
	lines := []string{
		fmt.Sprintf("site %s", site),
		fmt.Sprintf("users %d", len(BuildUsers())),
		fmt.Sprintf("hr_employee %d", len(BuildHREmployees())),
		fmt.Sprintf("rooms %d", len(filterBySite(BuildRooms(), site, roomHomeSite))),
		fmt.Sprintf("subscriptions %d", len(filterBySite(BuildSubscriptions(), site, subscriptionHomeSite))),
		fmt.Sprintf("room_members %d", len(filterBySite(BuildRoomMembers(), site, memberHomeSite))),
		fmt.Sprintf("messages %d", len(filterBySite(BuildMessages(), site, messageHomeSite))),
		fmt.Sprintf("thread_rooms %d", len(filterBySite(BuildThreadRooms(), site, threadRoomHomeSite))),
		fmt.Sprintf("thread_subscriptions %d", len(filterBySite(BuildThreadSubscriptions(), site, threadSubscriptionHomeSite))),
		fmt.Sprintf("mongo:roomKeys %d", len(filterBySite(BuildRoomKeys(), site, roomKeyHomeSite))),
		fmt.Sprintf("valkey:restrictedCache %d", len(filterBySite(BuildRestrictedCache(), site, restrictedCacheHomeSite))),
	}
	return strings.Join(lines, "\n")
}
```

Fix any existing test in `main_test.go` / `mongo_test.go` that calls the changed signatures — pass `"site-local"` so their expectations hold.

- [ ] **Step 11: Run the full test suite**

Run: `make test SERVICE=tools/seed-sample-data`
Expected: PASS.

Run: `make lint`
Expected: clean.

- [ ] **Step 12: Verify both dry-run plans**

Run:
```bash
go run ./tools/seed-sample-data --dry-run --site site-local
go run ./tools/seed-sample-data --dry-run --site site-remote
```
Expected: both list the same full `users` and `hr_employee` counts; the remote plan shows exactly 1 room, and non-zero subscriptions (ivan and judy's, including their rows for site-local rooms).

- [ ] **Step 13: Commit**

```bash
git add tools/seed-sample-data/
git commit -m "feat(seed): add --site and --mongo-db for per-site seeding"
```

---

### Task 6: Mark the cross-site rooms

**Files:**
- Modify: `tools/seed-sample-data/fixtures.go`
- Modify: `tools/seed-sample-data/fixtures_test.go`

**Interfaces:**
- Consumes: `roomSiteByID()` and `userSiteByAccount()` from Task 5.
- Produces: nothing new — `BuildRooms()` keeps its signature; three of its rooms gain `CrossSite: &true`.

Three seeded rooms already span sites: `r-general` and `r-eng` (both site-local, both including ivan) and `r-remote-announce` (site-remote, including alice). `CrossSite` is currently unset on every seeded room. Nil is treated as global by the client (`crossSite === false ? local : global` in `chat-frontend/src/api/_transport/subjects.ts:34`), so delivery happens to work today — but leaving it nil means the fixture never exercises the sticky-flag path, and a room whose flag is nil is "unclassified" rather than "confirmed cross-site". Setting it makes the seeded dataset state what it means.

- [ ] **Step 1: Write the failing test**

Append to `tools/seed-sample-data/fixtures_test.go`:

```go
// A room with members homed at more than one site must carry CrossSite=true:
// that is what routes its events onto the global chat.room.{id} lane, which is
// the lane that crosses the NATS gateway.
func TestBuildRooms_CrossSiteFlagMatchesMembership(t *testing.T) {
	userSite := userSiteByAccount()

	for _, r := range BuildRooms() {
		sites := map[string]struct{}{}
		for _, account := range r.Accounts {
			sites[userSite[account]] = struct{}{}
		}
		spansSites := len(sites) > 1

		if spansSites {
			require.NotNil(t, r.CrossSite, "room %s spans sites and needs CrossSite set", r.ID)
			assert.True(t, *r.CrossSite, "room %s spans sites", r.ID)
			continue
		}
		assert.Nil(t, r.CrossSite, "room %s is single-site and should stay unclassified", r.ID)
	}
}

func TestBuildRooms_KnownCrossSiteRooms(t *testing.T) {
	byID := map[string]model.Room{}
	for _, r := range BuildRooms() {
		byID[r.ID] = r
	}

	for _, id := range []string{"r-general", "r-eng", "r-remote-announce"} {
		require.NotNil(t, byID[id].CrossSite, "%s", id)
		assert.True(t, *byID[id].CrossSite, "%s", id)
	}
	assert.Nil(t, byID["r-design"].CrossSite, "r-design is all site-local")
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=tools/seed-sample-data`
Expected: FAIL — `room r-general spans sites and needs CrossSite set`, because `CrossSite` is nil.

- [ ] **Step 3: Implement**

In `tools/seed-sample-data/fixtures.go`, add a helper next to `ptrTime`:

```go
func ptrBool(b bool) *bool { return &b }
```

Then set the flag on the three rooms that span sites. `channelRoom` builds the base and the flag is applied at the call site, so single-site rooms stay unclassified:

```go
// BuildRooms returns the seed room set: 3 local channels, 2 local DMs, 1 remote
// channel. r-general, r-eng and r-remote-announce each have members homed at
// both sites, so they carry CrossSite=true — that flag routes their events onto
// the global chat.room.{id} lane, the one that crosses the NATS gateway.
func BuildRooms() []model.Room {
	general := channelRoom("r-general", "general", siteLocal, false,
		[]string{"alice", "bob", "carol", "dave", "eve", "frank", "grace", "heidi", "ivan"})
	general.CrossSite = ptrBool(true)

	eng := channelRoom("r-eng", "engineering", siteLocal, true,
		[]string{"alice", "bob", "carol", "ivan"})
	eng.CrossSite = ptrBool(true)

	remoteAnnounce := channelRoom("r-remote-announce", "remote-announce", siteRemote, false,
		[]string{"ivan", "judy", "alice"})
	remoteAnnounce.CrossSite = ptrBool(true)

	return []model.Room{
		general,
		eng,
		channelRoom("r-design", "design", siteLocal, false,
			[]string{"frank", "grace", "dave"}),
		dmRoom("alice", "bob"),
		dmRoom("carol", "eve"),
		remoteAnnounce,
	}
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make test SERVICE=tools/seed-sample-data`
Expected: PASS. If a pre-existing test asserts on `BuildRooms()` ordering or a deep-equal room value, update it — the order above is unchanged from the original.

- [ ] **Step 5: Commit**

```bash
git add tools/seed-sample-data/fixtures.go tools/seed-sample-data/fixtures_test.go
git commit -m "feat(seed): mark the three cross-site seed rooms with CrossSite"
```

---

### Task 7: Documentation

**Files:**
- Modify: `docker-local/README.md`

**Interfaces:**
- Consumes: every target and file from Tasks 1–6.
- Produces: nothing consumed by later tasks.

The seeding section is a required deliverable, not a footnote: `make seed` gains flags and `make fed-seed` is new, and the routing rule is non-obvious enough that an undocumented version will be misused.

- [ ] **Step 1: Add the two-site section**

Add a `## Two federated sites` section after the existing "First run" section, covering:

- **What it is and when to use it** — a second site so cross-site federation can be QA'd in a browser; the single-site stack remains the default for everyday work.
- **Bring-up sequence**, as a fenced block:

```sh
make deps-down            # the two stacks share host ports; only one at a time
./docker-local/setup.sh   # regenerate: adds the two NATS confs + per-site env files
make fed-deps-up          # 2× NATS (gateway-linked), 2× Valkey, shared datastores
make fed-seed             # seed both sites' databases
make fed-up               # both sites' services (detached)
make fed-ui-up            # chat-frontend :3000 and :3100
make fed-logs             # streaming logs across both sites
```

- **The topology diagram** — copy the three-network diagram from the spec.
- **Who to log in as** — alice at `http://localhost:3000` (site-local), ivan at `http://localhost:3100` (site-remote), both passwordless. Two origins rather than two tabs so the sessions do not contend over localStorage.
- **The site-remote port band** — the full table from the spec, and the note that `AUTH_SERVICE_HOST_PORT` is the one non-`+100` entry because 8180 is Keycloak.
- **Service tiers** — the Tier 1/2/3 table from the spec, `FED_REMOTE_SERVICES`, `make fed-up-lean`, and the warning that dropping a stream owner fails quietly (site-local's outbox-worker Naks and retries forever with nothing obviously wrong at the destination).
- **Known divergences** — the `chat.local.room.>` gateway leak and why it is harmless, shared Vault KEK, shared Keycloak, ~8GB RAM.

- [ ] **Step 2: Add the seeding section**

Add `### Seeding one site vs both` under the two-site section. It must state all of:

> Two corrections after implementation, so this block matches what shipped: the
> `--site` default is empty/unfiltered rather than `site-local` (see Step 8), and
> the closing sentence originally claimed `make seed-reset` accepts `--site` —
> it does not, since the make targets forward no arguments.

```markdown
`make seed` is unchanged: `--site` defaults to empty, and empty means
**unfiltered** — every fixture is written, exactly as before this work. Passing
`--site` explicitly is what opts into per-site filtering:

| Flag | Default | Meaning |
|---|---|---|
| `--site` | empty (all sites, unfiltered) | Which site's rows to write; `site-local` or `site-remote` filters to that site |
| `--mongo-db` | unset (falls back to `MONGO_DB`) | Target database |

`make fed-seed` runs the seeder twice:

    MONGO_DB=chat        go run ./tools/seed-sample-data --site site-local  --mongo-db chat
    MONGO_DB=chat_remote VALKEY_ADDRS=localhost:6479 \
      go run ./tools/seed-sample-data --site site-remote --mongo-db chat_remote

What each pass writes, and why it is not simply "half the data each":

- **The full directory goes into both databases.** All users and their
  `hr_employee` rows are written to `chat` *and* `chat_remote`, unfiltered. Each
  portal must be able to resolve any account in order to tell a client where its
  home site is — that is how ivan's browser learns to connect to `:7877`. This
  mirrors what HR replication does in production.
- **Room-owned rows follow the room.** Rooms, room_members, messages,
  thread_rooms and room keys go to the database of the room's home site.
- **Subscriber-owned rows follow the subscriber.** Subscriptions,
  thread_subscriptions and the Valkey restricted-rooms cache go to the database
  of the *subscriber's* home site, while still carrying the *room's* `siteId`.

That last rule is the one to remember. ivan is a member of `r-general`, which is
homed at site-local: his subscription row lives in `chat_remote` but records
`siteId: site-local`. Services use that field to tell local rows from cross-site
rows within their own database (`user-service/mongorepo/subscriptions.go:35`).
Routing these by the room's site instead puts ivan's rows in the wrong database
and renders an empty chat list for him — with no error anywhere.

The make targets forward no arguments and `make` itself rejects `--site`, so
per-site variations run the binary directly:
`go run ./tools/seed-sample-data --dry-run --site site-remote`. The dry-run plan
prints the site it is planning for and the filtered per-collection counts, so
that is the quickest check that routing is sane.

Three seeded rooms span both sites and carry `crossSite: true` — `r-general`
and `r-eng` (site-local, with ivan) and `r-remote-announce` (site-remote, with
alice). They exist so there is federated content to look at before you create
anything by hand.
```

- [ ] **Step 3: Update the host ports table**

Add the site-remote column (or a second table) to the existing `## Host ports` section so the file has one authoritative port map, and note that those ports are only bound when the federated stack is running.

- [ ] **Step 4: Verify the docs match reality**

Run: `make -n fed-seed`
Expected: the two commands printed match the ones documented, character for character. Fix the doc, not the Makefile, if they differ.

Re-read the bring-up sequence against the actual target names in the Makefile.

- [ ] **Step 5: Commit**

```bash
git add docker-local/README.md
git commit -m "docs(docker-local): document the two-site stack and per-site seeding"
```

---

### Task 8: End-to-end verification

**Files:** none — this task changes no code. It executes the spec's acceptance criteria.

**Requires a Docker host** with ~8GB available to Docker. If the working environment has no Docker daemon, stop here and hand the checklist to someone who does; do not mark the criteria as met on inspection alone.

**Interfaces:**
- Consumes: everything from Tasks 1–7.
- Produces: a verification result.

- [ ] **Step 1: Bring the stack up**

```bash
make deps-down
./docker-local/setup.sh
make fed-deps-up
make fed-seed
make fed-up
make fed-ui-up
```
Expected: `fed-deps-up` completes with both `cassandra-init` passes reporting `Schema initialized for chat` and `... for chat_remote`.

- [ ] **Step 2: Criterion 1 — the gateway is connected**

```bash
curl -s localhost:8222/gatewayz | head -40
curl -s localhost:8322/gatewayz | head -40
```
Expected: `:8222` lists `site-remote` under both inbound and outbound gateways; `:8322` lists `site-local`. If outbound shows the peer but inbound is empty, the peer's conf or the `chat-federation` network attachment is wrong.

- [ ] **Step 3: Criterion 2 — streams exist and are placed correctly**

```bash
curl -s 'localhost:8222/jsz?streams=1' | grep -o '"name":"[A-Z-]*-site-[a-z]*"' | sort -u
```
Expected: both sites' streams are visible from either server (one JetStream domain), including `INBOX-site-local`, `INBOX-site-remote`, `OUTBOX-site-local`, `OUTBOX-site-remote`.

- [ ] **Step 4: Criterion 3 — membership federates**

In a browser: log in as `alice` at `http://localhost:3000`, create a channel, and invite `ivan`. In a second browser profile or private window, log in as `ivan` at `http://localhost:3100`.

Expected: the new room appears in ivan's chat list. This exercises room-worker → `OUTBOX-site-local` → gateway → `INBOX-site-remote` → inbox-worker → Mongo `chat_remote`.

If it does not appear, check in this order: `docker logs chat-site-local-outbox-worker-1` for parked forwards, then that `INBOX-site-remote` exists (Step 3), then `docker logs chat-site-remote-inbox-worker-1`.

- [ ] **Step 5: Criterion 4 — messages federate both ways**

Send a message as alice; confirm ivan sees it live without a refresh. Reply as ivan; confirm alice sees it.

Expected: both directions deliver. This exercises the global `chat.room.{id}` lane across the gateway.

- [ ] **Step 6: Criterion 5 — the outbox is durable**

```bash
docker stop chat-site-remote-inbox-worker-1
```
As alice, add and remove a member, and rename the room (three order-sensitive events). Then:
```bash
docker start chat-site-remote-inbox-worker-1
```
Expected: all three land at site-remote, in the order issued. This exercises the durable OUTBOX retry (`MaxDeliver=-1`) and the per-destination FIFO lane (`MaxAckPending=1`).

- [ ] **Step 7: Verify the single-site stack still works**

```bash
make fed-down && make fed-ui-down && make fed-deps-down
make deps-up && make seed && make up-detached && make ui-up
```
Expected: the single-site stack comes up and alice can log in at `http://localhost:3000` exactly as before. This is the regression check for the global constraint that nothing existing changed.

- [ ] **Step 8: Record the result**

If every criterion passed, note it in the PR description. If any failed, do not paper over it — capture the failing output and either fix it in the owning task or record it as a known limitation in `docker-local/README.md`.

---

## Self-Review

**Spec coverage:**

| Spec section | Task |
|---|---|
| `compose.site.yaml` network override | 1 |
| Per-site env files, two NATS confs | 2 |
| Federated dep stack, `extends:`, aliases | 3 |
| Cassandra keyspace templating | 3 (compose), 4 (both passes) |
| Host port band | 2 (values), 7 (documented) |
| Site directory (`PORTAL_SITE_URLS`, `CLUSTER_DOMAINS`) | 2 |
| NATS supercluster config | 2 |
| Make targets | 4 |
| Seeding both sites | 5, 6, and documented in 7 |
| Trimming / tiers | 4 (`FED_TIER1`), 7 (table) |
| Acceptance criteria | 8 |
| Known divergences | 7 |

**Two additions made during planning, both beyond the spec's text:**
1. `fed-ui-up` / `fed-ui-down` — the frontends live in `compose.ui.yaml`, not `compose.services.yaml`, so they need their own target for the same reason `ui-up` exists today. The spec's target table omitted them.
2. The `fed-deps-up` guard against a running single-site stack. The spec noted the two cannot coexist; without the guard the failure is a confusing port-binding error.

**One spec detail corrected:** the spec proposed adding a new cross-site room fixture. Three already exist (`r-general`, `r-eng`, `r-remote-announce`), so Task 6 sets `CrossSite` on those instead of adding a fourth — less churn, and it fixes an existing latent nil.
