# Testing NATS site failover locally, end to end

How to exercise the buddy-hosted standby lanes against two real NATS servers: a
site keeps working — send and receive — while its own NATS cluster is down.

Companion to [`docs/nats-failover-scenarios.md`](./nats-failover-scenarios.md),
which explains what survives an outage and why, and to
[the design spec](./superpowers/specs/2026-08-15-nats-site-failover-design.md),
which is the authority on mechanism. This page is only the procedure.

> **Status: unverified.** These steps were derived from the compose files,
> `docker-local/setup.sh` and the service configs, not from an executed run.
> Expect to shake out the runbook alongside the feature on the first pass, and
> correct this file when they disagree.

---

## 0. What the default dev stack does *not* test

Every service's compose file defaults the buddy to its own server:

```yaml
- BUDDY_SITE_ID=${BUDDY_SITE_ID:-site-local}
- BUDDY_NATS_URL=${BUDDY_NATS_URL:-nats://nats:4222}
```

That is deliberate — it exercises the full code path (stream create, consumer
bind, dual-lane drain) on one server — but it cannot fail over, because the
"buddy" dies with the home. **Everything interesting requires the buddy pointed at
a second server**, which is what §2–§3 set up.

Two consequences follow, and both are by design:

- `stream.CheckPlacement` demands a named cluster, and a single-server dev NATS
  reports none. So locally you must keep `BOOTSTRAP_STREAMS=true`, which takes
  `EnsureFailoverStream`'s create branch and skips placement entirely.
  **Placement assertion is not testable locally**; it needs clustered servers.
- The local link between the two sites is a **leafnode**, not a gateway
  supercluster. Direct buddy dials (which is what the standby lanes use) behave
  the same; cross-cluster *subject* routing does not, and the local leaf blocks
  carry no subject filters, so everything propagates. Production's
  `chat.local.>` interest filter is the thing that is *not* reproduced here.

---

## 1. Prerequisites

- Docker + Compose, ~8 GB free to the daemon.
- Ports free: 3000/3100, 4222/4322, 6379/6479, 7777/7877, 8222/8322, 9222/9322,
  plus the datastore ports from `compose.deps.yaml`.
- The single-site stack **down** — the two stacks publish the same host ports:

```sh
make deps-down
```

---

## 2. Stand up two federated sites

```sh
./docker-local/setup.sh      # regenerates: nats-site-*.conf, .env.site-*, creds
make fed-deps-up             # 2× NATS (leafnode-linked), 2× Valkey, shared datastores
make fed-seed                # seed both sites' databases
```

Leave `make fed-up` for §3 — the services need the buddy env first.

Sanity check before going on:

```sh
curl -s localhost:8222/varz | jq -r .server_name    # nats-site-local
curl -s localhost:8322/varz | jq -r .server_name    # nats-site-remote
curl -s localhost:8222/leafz | jq '.leafnodes | length'   # 1
```

Topology, from `docker-local/README.md`:

| | site-local | site-remote |
|---|---|---|
| NATS client / ws / monitor | 4222 / 9222 / 8222 | 4322 / 9322 / 8322 |
| Traefik gateway | 7777 | 7877 |
| chat-frontend | 3000 | 3100 |
| Mongo DB / Cassandra keyspace | `chat` | `chat_remote` |
| Login as | `alice` | `ivan` |

---

## 3. Cross-point the buddies

**3a. Make each NATS reachable from the other site's network.** The two servers
share `chat-federation`, but the services do not sit on it, so give each server a
second alias on the *other* site's network:

```sh
docker network connect --alias nats-buddy chat-site-local  chat-fed-nats-site-remote
docker network connect --alias nats-buddy chat-site-remote chat-fed-nats-site-local
```

Now `nats-buddy:4222` from any site-local service reaches site-remote's server,
and vice versa. (Re-run this after anything that recreates the NATS containers —
`fed-deps-down`, `fed-regen`.)

**3b. Point each site's services at it.** Append to the generated env files:

```sh
cat >> docker-local/.env.site-local <<'EOF'

# Failover: site-local's standby streams live on site-remote's cluster.
BUDDY_SITE_ID=site-remote
BUDDY_NATS_URL=nats://nats-buddy:4222
BOOTSTRAP_STREAMS=true
FAILOVER_REVERT_GRACE=2m
EOF

cat >> docker-local/.env.site-remote <<'EOF'

BUDDY_SITE_ID=site-local
BUDDY_NATS_URL=nats://nats-buddy:4222
BOOTSTRAP_STREAMS=true
FAILOVER_REVERT_GRACE=2m
EOF
```

`FAILOVER_REVERT_GRACE` is 30m by default — fine in production, tedious in a test.
2m keeps §7 observable. It is read by `room-service` and `broadcast-worker`.

> **`make fed-regen` rewrites these files**, exactly as the README warns for
> `.env`. Re-append after every regen, or add the four lines to `write_fed_env`'s
> trailing `printf` args in `setup.sh` so they survive.

**3c. Start the services:**

```sh
make fed-up        # detached
make fed-logs      # streaming, both sites
```

---

## 4. Verify the steady state (before breaking anything)

**Standby streams exist on the buddy, not at home.** Each site's five standby
streams must appear on the *other* server:

```sh
# site-remote's server hosts site-local's standby streams
curl -s 'localhost:8322/jsz?streams=1&accounts=true' | grep -o '"name":"[A-Z-]*-site-local"' | sort -u
# expect: INBOX-FAILOVER-site-local, MESSAGES-FAILOVER-site-local,
#         MESSAGES-CANONICAL-FAILOVER-site-local, PUSH-NOTIFICATION-FAILOVER-site-local,
#         OUTBOX-FAILOVER-site-local

# and the mirror image
curl -s 'localhost:8222/jsz?streams=1&accounts=true' | grep -o '"name":"[A-Z-]*-site-remote"' | sort -u
```

If a site's standby streams show up on its *own* server, 3a/3b did not take — the
service is still dialling itself as its buddy, and nothing below will prove
anything.

**Both lanes bound.** In `make fed-logs`, per service, expect a `buddy nats
connected` line and a failover-lane bind. A service that logs the buddy connect
warning instead (`buddy nats connect failed; running without the failover lane`)
is home-only by design — note which, because it will not participate.

**Baseline behaviour still works.** Log in as `alice` at `localhost:3000` and
`ivan` at `localhost:3100`, put them in a room, exchange messages both ways.
Failover tests are meaningless if the happy path is broken first.

---

## 5. Test A — the outage: displaced client, message path

**Break site-local's NATS:**

```sh
docker stop chat-fed-nats-site-local
```

*Expected within seconds:*

- `alice`'s tab (3000) drops its NATS connection, fetches the peer list from
  `/api/settings`, shuffles it, and dials `ws://localhost:9322` — site-remote.
  Confirm in devtools → Network → WS: the open socket is on **9322**.
- Site-local's services stay up. Their home lanes idle; their buddy lanes keep
  working. `/readyz` stays 200 because `LanesCheck` passes when *either* lane is
  serving.

**Now send a message as alice, in a site-local room.** It should be delivered to
ivan, and echo back to alice, with site-local's NATS still stopped.

*What just happened, and how to check each hop:*

| Hop | Where to look |
|---|---|
| alice publishes on the `failover` subject token | devtools WS frames: subject starts `chat.failover.` |
| lands in `MESSAGES-FAILOVER-site-local` on site-remote | `curl -s 'localhost:8322/jsz?streams=1&accounts=true'` — messages counter climbs |
| site-local's `message-gatekeeper` validates it over its buddy connection | `make fed-logs` — gatekeeper log lines from the **site-local** container |
| canonical event → `MESSAGES-CANONICAL-FAILOVER-site-local` | same `/jsz` |
| site-local's `message-worker` persists to **site-local's** Cassandra keyspace | see below |
| `broadcast-worker` fans out on the **global** room root | both clients receive it |

The persistence check is the one that matters — the whole correctness argument is
that the down site's own worker writes the down site's own store:

```sh
docker exec -it chat-fed-cassandra cqlsh -e \
  "SELECT room_id, msg_id, msg FROM chat.messages_by_room LIMIT 5;"
# keyspace `chat` = site-local. Nothing should have landed in `chat_remote`.
```

**Room routing.** Note that a same-site room's events now use the global root:
`chat.local.>` never crosses a gateway, so a displaced client would otherwise
receive nothing. If the message reaches alice on 9322, that rule is working.

---

## 6. Test B — inbound federation redirect

Still with site-local's NATS stopped, have **ivan** (3100, unaffected) do
something that federates *into* site-local — add alice to a room, rename a room,
or send into a shared room.

- Site-remote's `outbox-worker` tries the live `INBOX-site-local` first, gets
  **no responders** (not a timeout — the gate is deliberately no-responders only),
  and retries into `INBOX-FAILOVER-site-local`, which lives on site-remote's own
  server.
- Site-local's `inbox-worker` consumes it over its buddy connection and applies it
  to **site-local's** MongoDB.

```sh
curl -s 'localhost:8322/jsz?streams=1&accounts=true' | grep -A3 'INBOX-FAILOVER-site-local'

docker exec -it chat-fed-mongodb mongosh --quiet chat \
  --eval 'db.subscriptions.find({},{roomId:1,userAccount:1,_id:0}).sort({_id:-1}).limit(5)'
```

A redirect that never fires is the failure mode worth watching for: it is silent.
If the counter on `INBOX-FAILOVER-site-local` stays at zero while site-remote logs
publish failures, the no-responders classification is what to inspect first —
`outbox.IsNoResponders`.

---

## 7. Test C — recovery: revert grace and backlog drain

```sh
docker start chat-fed-nats-site-local
```

*Expected:*

1. Site-local's services reconnect home. The home lanes bind (or re-bind) and the
   **pre-outage backlog drains alongside** the failover lane — both run at once;
   there is no cutover.
2. Publishers open the revert grace window: for `FAILOVER_REVERT_GRACE` (2m, from
   §3b) the home lane **dual-publishes** room events to both roots, because
   clients revert on their own backoff up to 5 minutes later.
3. alice's tab keeps probing home on exponential backoff — `HOME_PROBE_BASE_MS`
   5s doubling to a 5-minute cap — and swaps back when home answers. Watch the WS
   connection in devtools return to **9222**.

The coupling is the thing to check: with the grace set *shorter* than the client
probe cap, you can observe the gap it exists to close. Set
`FAILOVER_REVERT_GRACE=10s`, restart the stack, repeat §5 → §7, and a client that
is slow to revert misses events published between grace expiry and its own
reconnect. That is the failure the 30m default prevents; it is worth seeing once.

---

## 8. Test D — a pod restarting *during* the outage

This is the case that used to remove a pod from the standby lane entirely.

```sh
docker stop chat-fed-nats-site-local
docker compose -p chat-fed-services restart broadcast-worker   # adjust to your project name
```

*Expected:* the service boots with home **down**. The home dial is lazy, so it
comes up in `RECONNECTING`, binds its buddy lane as usual, and `/readyz` reports
ready on the strength of that one lane. When site-local's NATS returns, the home
lane binds itself on the first `CONNECTED` (`natsutil.BindWhenConnected`, retried
with backoff), and the grace window opens on that connect.

Without a buddy configured the dial stays fail-fast, so this only holds on a
service that has one.

---

## 9. Teardown

```sh
make fed-down && make fed-ui-down && make fed-deps-down
```

The `docker network connect` aliases from §3a disappear with the containers.

---

## 10. Troubleshooting

| Symptom | Likely cause |
|---|---|
| Standby streams appear on the site's *own* server | §3a/§3b not applied, or env file regenerated by `fed-regen` |
| `buddy nats connect failed` in logs | `nats-buddy` unresolvable — re-run the `docker network connect` pair |
| `failover stream … placement: … no cluster info` | `BOOTSTRAP_STREAMS` got turned off; local single-server NATS has no cluster name |
| Client never leaves home after the stop | peer list empty — check `curl -s localhost:7777/api/settings \| jq .sites` |
| Client fails over but receives nothing | room routing stuck on the local root; check the subscribe subject in WS frames |
| Redirect never fires, events just park | publish error is not being classified as no-responders |
| Message sent while displaced lands in `chat_remote` | a lane is publishing over the wrong connection — the bug class the PR's per-lane `HandlerFor`/`RouterFor` builders exist to prevent |

## What this cannot cover

- **Stream placement assertion** (`CheckPlacement`) — needs clustered servers with
  names; locally `BOOTSTRAP_STREAMS=true` bypasses it. Verify in staging with
  `nats stream info` per standby stream.
- **Gateway subject routing and the `chat.local.>` interest filter** — the local
  link is a leafnode with no subject filters.
- **Capacity** — one peer absorbing another's full pipeline plus its clients.
