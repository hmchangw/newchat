# SP6 — Cross-Site Failover: Ops / IaC Runbook

> **What this is.** SP6 of the cross-site failover ("lifeboat") program is a
> **runbook coordinated with the platform/NATS/IaC team — not a TDD code plan**
> (roadmap §SP6). This document consolidates every operational requirement, and
> documents the operator-facing surface of the parts already shipped (SP3, SP4).
>
> - **Governing design:** `docs/superpowers/specs/2026-07-28-cross-site-failover-design.md` (§4, §6.5, §8, §9)
> - **Roadmap:** `docs/superpowers/plans/2026-07-28-cross-site-failover-roadmap.md` (SP6)
> - **Related shipped work:** SP4 (failover state + control surface, `portal-service`), SP3 (routing override), SP2 (identity — World 1).

## Readiness legend

Each item is tagged with what an operator can action **today** vs. what is gated:

- **[READY]** — actionable now against shipped code.
- **[GATED:SP1]** / **[GATED:SP1+SP2]** — needs the backup materialization / serving stack to exist first.
- **[INFRA]** — external platform/IaC work (Kubernetes, NATS supercluster, cloud), owned by the platform team; this repo carries no code for it.

---

## 1. The failover control surface  **[READY]**

Shipped in SP4 (`portal-service`). This is how an operator drives a failover and
failback. Portal is the **single routing authority and split-brain fence** — it
is the only place the decision is made.

### 1.1 Configuration

| Env | Where | Meaning | Ops action |
|---|---|---|---|
| `FAILOVER_OPS_TOKEN` | portal-service | Break-glass bearer token gating the control surface. **Empty disables the control surface entirely.** | Provision as a secret (below). Required to operate failover. |
| `FAILOVER_INTERNAL_ADDR` | portal-service | Listen address for the internal-only control listener (default `:8090`), kept **off** the public `:8085` discovery API. | Expose only to the ops network (below). |
| `PORTAL_BACKUP_SITE_ID` | portal-service | Reserved id in `PORTAL_SITE_URLS` whose `{baseUrl,natsUrl}` are served for a failed-over site (e.g. `_backup`). Empty in single-site deployments. | Set once the backup site exists; add its entry to `PORTAL_SITE_URLS`. |
| `FAILOVER_STATE_TTL` | portal-service | How long portal caches a site's serving target before re-reading (default `5s`). | Leave default unless routing-propagation latency needs tuning. |
| `PORTAL_SITE_URLS` | portal-service | Site registry; **must contain the `PORTAL_BACKUP_SITE_ID` entry** or a failover returns `500` (loud, by design). | Add the backup entry when the backup is deployed. |

### 1.2 Secret provisioning — `FAILOVER_OPS_TOKEN`

- Generate a high-entropy random token; store in the platform secret manager
  (the same mechanism that delivers other portal secrets).
- Deliver to portal-service as an env var / mounted secret. **Never** commit it;
  the local `deploy/docker-compose.yml` value (`dev-failover-token`) is dev-only.
- Rotation: replace the secret and restart/roll portal. There is no session
  state to migrate — the token gates each request independently.
- The token authenticates *possession*, not a person; the `operator` field in
  each request is self-asserted for the audit trail. A per-operator login is a
  planned follow-up (SP4 spec §8, "option 3") and is **not** in scope here.

### 1.3 Network policy — internal listener

- The control routes live on `FAILOVER_INTERNAL_ADDR` (a **separate** HTTP
  listener from the public discovery API on `:8085`). Restrict it to the ops
  network / an internal LB — it must **not** be internet-reachable.
- Portal's public `:8085` surface is unchanged and carries no privileged writes.

### 1.4 Operator procedures

Control endpoints (all require `Authorization: Bearer $FAILOVER_OPS_TOKEN`):

```
GET  /internal/v1/failover            # list every site's failover state
GET  /internal/v1/failover/{siteId}   # one site's state
POST /internal/v1/failover/{siteId}   # {"action": "...", "operator": "...", "reason": "..."}
```

State machine (operator-driven; every write is CAS-guarded, so concurrent portal
replicas cannot diverge):

```
healthy ──failover──► failed_over ──failback──► failing_back ──complete──► healthy
   ▲                       │
   └──────── resume ───────┘   (false alarm; skip the drain)
```

`servingTarget` is `backup` while `failed_over`/`failing_back`, else `home`.

**Declare a site down (fail over):**
```bash
curl -sS -H "Authorization: Bearer $FAILOVER_OPS_TOKEN" \
  -X POST https://portal-internal/internal/v1/failover/site-a \
  -d '{"action":"failover","operator":"jane","reason":"site-a NATS unreachable"}'
```
Effect: `site-a` accounts' `/api/userInfo` now return the backup's coordinates;
displaced clients reconnect there on their next connection failure.

**Begin failback (start draining home):** `{"action":"failback",...}` → moves to
`failing_back`. **`servingTarget` stays `backup`** — users keep writing to a
stable backup while the home site drains. *(The drain/replay itself is SP5.)*

**Complete failback (flip home):** `{"action":"complete",...}` → `healthy`;
`servingTarget` flips to `home`; the backup drains its impersonation and clients
reconnect home. **Gate this on SP5's lag≈0 signal** once SP5 exists; today it is a
manual operator action.

**False alarm (undo a failover):** `{"action":"resume",...}` → straight back to
`healthy`, skipping the drain.

**Inspect:** `GET /internal/v1/failover` for the whole fleet's state, including
`operator`/`reason`/`since` for the audit trail.

### 1.5 Split-brain guarantee

A site is `home` **xor** `backup` because the decision is a single, single-valued
`FailoverState` document that only portal writes (CAS-guarded). Do **not**
attempt to route around portal or edit the `failover_states` collection by hand —
that is the one thing that can break the fence.

---

## 2. Backup identity & auth-service deploy  **[READY (config) / GATED:SP1+SP2 (serving)]**

Identity is **resolved (World 1)** — one shared org NATS account, so the backup
mints with the *same* signing identity every site uses. No per-site keys, no KMS
scheme (see `specs/2026-08-11-sp2-backup-identity-jwt-minting.md`).

Backup `auth-service` deployment requirements:

- **[READY]** Configure with the **shared** `AUTH_SCOPED_SIGNING_KEY` +
  `AUTH_ACCOUNT_PUB_KEY` — identical to every site. No new key material.
- **[READY]** The `chat.local.room.>` subscribe grant is **already on `main`** in
  the shared scoped-signing template (SP0). Nothing to add; the backup inherits it.
- **[READY]** Point OIDC at the central Keycloak (reachable from the backup).
- **[READY]** Leave `BOTPLATFORM_URL` **unset** on the backup — bot/admin login is
  out of lifeboat scope, so the session-token branch need not work there.
- **[READY]** Size the backup `auth-service` (and Keycloak reachability) for **one
  largest site's reconnect peak** — a whole site re-mints at once on failover.
  Client reconnect backoff/jitter + proactive-refresh jitter already spread it
  (`docs/superpowers/specs/2026-06-05-seamless-nats-jwt-refresh-design.md`).
- **[INFRA]** The backup's NATS `resolver` must preload the same shared account
  JWT so it validates the minted user JWTs (it does, being a supercluster peer).

The serving *handlers* (send/receive/history against the materialized copy) are
**[GATED:SP1+SP2]** — they need the materialized data to exist.

---

## 3. Backup platform footprint  **[INFRA]**

Owned by the platform/NATS/IaC team; no code in this repo.

- **HA within itself (multi-AZ).** The backup is a failover SPOF — if it is down
  when a site fails, there is no lifeboat. It **must** be HA. Documented limit,
  not silently degraded (design §9).
- **Supercluster gateway peer.** The backup must be a full gateway peer so that,
  during a failover, **global-room** delivery reaches members still on healthy
  sites (design §7).
- **Leaf-node `chat.local.>` deny on the backup's leaf**, same as every site, so
  local-room subjects stay site-local. Displaced clients connect intra-cluster on
  the backup, so this does not affect their local-room delivery (design §7).
- **Per-`siteID` namespacing** of the backup's Mongo/Cassandra (the exact scheme —
  DB-per-site vs prefixed collections/keyspaces — is decided in SP1a/SP1b).

---

## 4. Streams & retention  **[GATED:SP1]**

- **Restore-log `MaxAge`.** The backup's canonical stream is the durable restore
  log for failback. Size its `MaxAge` **≥ the maximum tolerated outage**
  (days/weeks — it is 1× per message, cheap). Do **not** rely on the 72h
  Cassandra read window for restore; a longer outage would age its earliest
  messages out (design §6.5).
- **DR feed streams** (`DR_MESSAGES_{siteID}`, `DR_OPLOG_{siteID}`, per SP1a/SP1b)
  and the backup's INBOX — owned/bootstrapped per the `BOOTSTRAP_STREAMS`
  convention; ops/IaC owns creation in production.
- **Bucket-window parity.** The backup and every origin site **must** share
  `MESSAGE_BUCKET_HOURS` (default 72). A mismatch silently mis-partitions writes
  and reads (CLAUDE.md §Cassandra; design §6.4). Enforce this in the backup's
  deploy config.

---

## 5. Monitoring & alerting  **[GATED:SP1]**

- **Per-site replication lag — the RPO-decay signal (highest-value alert).** If
  the backup cannot keep up with the N-site aggregate ingest, its lag grows and
  the DR guarantee *quietly* weakens — **the lag is the live RPO** (design §8).
  Alert per origin site. NOTE: there is **no existing lag metric to reuse today**
  (the design's "reuse the membership-federation lag signal" is aspirational —
  verified: no such metric in the repo). This must be **built as part of SP1**
  (emit a per-site replication-lag gauge from the backup materializers) and wired
  to alerting here.
- **Backup capacity — two separate numbers.** Size/alert the **ingest** tier for
  the N-site aggregate (cheap 1× plane) and the **serving** tier for one largest
  site's peak with ×F fan-out (design §8). They are different capacity budgets.
- **Multi-site-outage ceiling.** The backup is sized for **one site down at a
  time**; alert on concurrent multi-site failure as a documented degradation
  ceiling (design §9), not a silent failure.

---

## 6. Failback operational must-dos  **[GATED:SP1+SP2, wired by SP5]**

When SP5 replays the outage log home, the platform config must ensure:

- **Suppress push notifications** for replayed messages — they are stale
  (`notification-worker` skips events flagged as replay; design §6.6).
- **Suppress / client-dedup re-broadcast** — restore is about durability in the
  home site, not re-delivering to users who have moved on (design §6.6).

These are behaviors SP5 implements; listed here so the ops handoff is complete.

---

## 7. Coordination checklist (platform/NATS team)

Ordered by when each unblocks:

1. **Now [READY]:** provision `FAILOVER_OPS_TOKEN`; restrict `FAILOVER_INTERNAL_ADDR`
   to the ops network; confirm portal's public surface is unchanged.
2. **Now [READY]:** confirm the World-1 shared-account model and that the backup
   `auth-service` can deploy with the shared creds (no per-site keys).
3. **With backup stand-up [INFRA]:** HA/multi-AZ deploy; gateway peering; leaf
   deny; add the backup entry to `PORTAL_SITE_URLS` + set `PORTAL_BACKUP_SITE_ID`.
4. **With SP1 [GATED]:** DR-feed stream creation; restore-log `MaxAge`; bucket-window
   parity; **build + wire the per-site replication-lag alert.**
5. **With SP5 [GATED]:** gate the `complete` transition on the lag≈0 signal;
   confirm replay push/broadcast suppression.

---

## 8. Status snapshot

| SP6 area | Readiness |
|---|---|
| Failover control surface ops (§1) | ✅ READY (shipped in SP4) |
| Backup identity config (§2) | ✅ READY (World 1; grant on `main`) |
| Backup serving handlers | ⛔ GATED on SP1+SP2 |
| Platform footprint (§3) | 📋 INFRA (platform team) |
| Streams/retention (§4), monitoring (§5) | ⛔ GATED on SP1 |
| Failback must-dos (§6) | ⛔ GATED on SP1+SP2, wired by SP5 |
