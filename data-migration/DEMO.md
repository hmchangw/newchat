# Migration flow — local end-to-end demo

Run a message **insert → edit → soft-delete** through the full migration pipeline
and confirm each one reaches the new stack while users are **not** notified.

```
 source-mongo (legacy RS)   ──▶  oplog-connector  ──▶  MIGRATION_OPLOG_site-local
        [you write here]                                      │
                                                              ▼
                                                     oplog-transformer
                              insert ─▶ chat.msg.canonical.site-local.created (X-Migration: live)
                                          └─▶ message-worker ─▶ Cassandra
                              update/delete ─▶ history-service ─▶ Cassandra (+ canonical republish)
                                                              │
                              broadcast-worker / notification-worker ─▶ SKIP (migrated)
```

Everything runs on the shared `chat-local` docker network. Run all commands from
the repo root.

---

# A. SETUP  (3 steps, ~2–4 min)

### A1. Shared infra (NATS + Mongo + Cassandra + …)
**Run:**
```bash
make deps-up
```
**Expect (readiness):**
```bash
curl -s localhost:8222/healthz            # → {"status":"ok"}
```

### A2. Canonical-pipeline services (the downstream consumers)
**Run** — start only what the demo needs (the full aggregate also builds
`upload-service`, which can fail its build and block everything; and the full
stack is memory-heavy):
```bash
docker compose -f docker-local/compose.services.yaml up -d --build \
  message-gatekeeper message-worker history-service broadcast-worker notification-worker
```
> `message-worker` depends on `vault`, whose healthcheck probes `localhost`
> (IPv6) while vault binds IPv4 → it can show "unhealthy" though it's fine. If it
> blocks startup, bootstrap the transit key and start `--no-deps`:
> ```bash
> docker exec -e VAULT_ADDR=http://127.0.0.1:8200 -e VAULT_TOKEN=dev-only-token \
>   chat-local-services-vault-1 sh -c \
>   'vault secrets enable -path=transit transit; vault write -f transit/keys/chat-kek'
> docker compose -f docker-local/compose.services.yaml up -d --no-deps message-worker
> ```
**Expect (readiness):**
```bash
docker compose -f docker-local/compose.services.yaml ps \
  | grep -E 'message-worker|history-service|broadcast-worker|notification-worker'
# all four show "Up"
```

### A3. Migration trio (legacy source + connector + transformer)
**Run:**
```bash
docker compose -f docker-local/compose.migration-demo.yaml up -d --build
```
**Expect (readiness — all three lines should print):**
```bash
docker exec migration-source-mongo mongosh --quiet --eval 'rs.status().ok'      # → 1
docker exec migration-oplog-connector   wget -qO- http://localhost:9090/healthz # → ok
docker exec migration-oplog-transformer wget -qO- http://localhost:9090/healthz # → ok
```

> Optional — watch the flow live in a second terminal:
> ```bash
> docker logs -f migration-oplog-transformer
> ```

Setup done. The three inputs below are independent verifications of the same message.

---

# B. INPUTS → EXPECTED OUTPUT

## B1. INSERT  (legacy message → persisted in the new stack, users NOT notified)

**Input:**
```bash
docker exec migration-source-mongo mongosh rocketchat --quiet --eval '
db.rocketchat_message.insertOne({
  _id: "demoMsg0000000001",
  rid: "demoRoom01",
  msg: "hello from the legacy system",
  ts:  new Date(),
  u:   { _id: "u1", username: "alice", name: "Alice A" }
})'
```

**Expected output:**

1) Connector published 1 event, transformer processed 1 (no errors):
```bash
docker exec migration-oplog-connector   wget -qO- localhost:9090/metrics | grep oplog_events_published_total
#  oplog_events_published_total{collection="rocketchat_message"} 1

docker exec migration-oplog-transformer wget -qO- localhost:9090/metrics | grep -E 'processed|naks|terms'
#  oplog_transformer_events_processed_total 1
#  oplog_transformer_naks_total 0
#  oplog_transformer_terms_total 0
```

2) Persisted to Cassandra:
```bash
docker exec chat-local-cassandra cqlsh -e \
 "SELECT message_id,room_id,msg,deleted FROM chat.messages_by_id WHERE message_id='demoMsg0000000001';"
#  message_id          | room_id    | msg                          | deleted
#  demoMsg0000000001   | demoRoom01 | hello from the legacy system | False
#  (1 rows)
```

3) Users were NOT spammed — broadcast & notification skipped it:
```bash
docker compose -f docker-local/compose.services.yaml logs --tail 40 broadcast-worker notification-worker | grep -i skip
#  ...broadcast-worker... "skip migrated event" ...
#  ...notification-worker... "skip migrated event" ...
```

---

## B2. EDIT  (delta → source lookup → history-service edit RPC)

**Input:**
```bash
docker exec migration-source-mongo mongosh rocketchat --quiet --eval '
db.rocketchat_message.updateOne(
  { _id: "demoMsg0000000001" },
  { $set: { msg: "hello (edited from legacy)", editedAt: new Date() } })'
```

**Expected output:** Cassandra now shows the edited text (and `edited_at` set):
```bash
docker exec chat-local-cassandra cqlsh -e \
 "SELECT message_id,msg,edited_at FROM chat.messages_by_id WHERE message_id='demoMsg0000000001';"
#  message_id          | msg                        | edited_at
#  demoMsg0000000001   | hello (edited from legacy) | 2026-06-15 ...
```
(transformer counter `events_processed_total` is now `2`, `naks=0 terms=0`.)

---

## B3. SOFT-DELETE  (RocketChat `t:"rm"` → history-service delete RPC)

**Input:**
```bash
docker exec migration-source-mongo mongosh rocketchat --quiet --eval '
db.rocketchat_message.updateOne(
  { _id: "demoMsg0000000001" },
  { $set: { t: "rm", msg: "", editedAt: new Date() } })'
```

**Expected output:** Cassandra reflects the removal (content cleared / `deleted=true`):
```bash
docker exec chat-local-cassandra cqlsh -e \
 "SELECT message_id,msg,type,deleted FROM chat.messages_by_id WHERE message_id='demoMsg0000000001';"
#  message_id          | msg | type | deleted
#  demoMsg0000000001   |     | rm   | True
```
(transformer `events_processed_total` is now `3`, still `naks=0 terms=0`.)

---

## B4. FINAL TALLY (one snapshot)

**Run:**
```bash
docker exec migration-oplog-connector   wget -qO- localhost:9090/metrics | grep -E 'published|publish_errors|events_skipped'
docker exec migration-oplog-transformer wget -qO- localhost:9090/metrics | grep -E 'processed|naks|terms'
```
**Expect:**
```
oplog_events_published_total{collection="rocketchat_message"} 3
oplog_publish_errors_total ... 0
oplog_events_skipped_total ... 0
oplog_transformer_events_processed_total 3
oplog_transformer_naks_total 0
oplog_transformer_terms_total 0
```

---

# C. TEARDOWN

**Run:**
```bash
docker compose -f docker-local/compose.migration-demo.yaml down -v   # source + connector + transformer (+ source data)
docker compose -f docker-local/compose.services.yaml      down       # canonical services
make deps-down                                                       # shared infra (NATS/Mongo/Cassandra)
```
**Expect:** each command ends with `Removed` / `Network ... Removed`; `docker ps` shows no `chat-local-*` or `migration-*` containers.

---

## Troubleshooting

> ⚠️ **Do NOT `make deps-down` / recreate the `chat-local` network mid-run.** It's
> declared `external` by the service composes; recreating it leaves running
> containers pointing at a dead network ID and breaks cross-project DNS (services
> can't resolve `mongodb`/`cassandra`). If you must reset, `down` **every** compose
> project (deps + services + migration), then bring them all up again in order.
> Likewise the host needs enough RAM — under memory pressure Cassandra drops its
> native transport (`9042`) and message-worker fails to connect.

| Symptom | Fix |
|---|---|
| compose says deps missing / `nats connect failed` | run **A1** first; `docker-local/backend.creds` must exist (`./docker-local/setup.sh`) |
| NATS unhealthy: `JetStream account ... could not be resolved` | stale JetStream store from an old `setup.sh` → `make deps-down && docker volume rm chat-local-deps_nats-data && make deps-up` |
| `make deps-up` errors on `cassandra-init` (`column ... already exists`) | harmless on re-runs — `ALTER ADD` isn't idempotent but the tables already exist; verify with `docker exec chat-local-cassandra cqlsh -e "DESCRIBE chat.messages_by_id"` |
| `make up` fails building `upload-service` | unrelated build break — target only the services the demo needs (see **A2**) |
| vault "unhealthy" but logs show it unsealed | healthcheck probes `localhost` (IPv6); vault binds IPv4 → use the `--no-deps` workaround in **A2** |
| services can't resolve `mongodb`/`cassandra` (`no such host`) | the `chat-local` network was recreated mid-run — see the ⚠️ above; `down` all projects and restart |
| connector: `ChangeStreamHistoryLost` or no events | `source-mongo` RS not initiated → `docker exec migration-source-mongo mongosh --eval 'rs.status().ok'` must be `1` |
| transformer `naks_total` climbing | `MESSAGES_CANONICAL_site-local` not provisioned → start a canonical service (**A2**: message-gatekeeper/worker has `BOOTSTRAP_STREAMS=true`); the transformer Naks-and-retries (lossless) until it exists |
| transformer waits on startup: `waiting for stream to be created by the connector` | expected if the transformer started before the connector bootstrapped `MIGRATION_OPLOG`; it retries for up to 60s, no restart needed |
| nothing in Cassandra | message-worker down or keyspace missing → check `docker compose -f docker-local/compose.services.yaml logs message-worker`; ensure Cassandra `9042` is up (`docker exec chat-local-cassandra cqlsh -e 'SELECT now() FROM system.local'`) |
