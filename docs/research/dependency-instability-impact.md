# Impact of Dependency Instability — NATS/JetStream, MongoDB, Cassandra, Valkey

**Scope:** Failure-mode impact, project/release stability, and operational-reliability data for the four core
infrastructure dependencies of this distributed multi-site Go chat system, with concrete implications tied to how
each is actually wired in this repo.

**Date:** 2026-06-27 · **Method:** multi-source web research (per-dependency, cross-verified, confidence-tagged) +
a codebase usage/config map. Several primary sources (jepsen.io, nvd.nist.gov, some vendor docs) returned HTTP 403
to the automated fetcher; claims resting on search-summary extracts are flagged. CVE/version specifics flagged
"verify" should be confirmed against NVD/JIRA before being used in a shipped security doc.

---

## TL;DR — risk ranking for *this* architecture

| Rank | Risk | Why it matters here | Action |
|------|------|--------------------|--------|
| 1 | **Federation durability is split: only consumer-originated events have an implicit outbox** | Cross-site events are a blocking JetStream publish into the *remote* site's INBOX over a gateway; streams never cross gateways. Events published from a **JetStream consumer** (messages, room membership) get a free outbox — a failed PubAck Naks the source message, which redelivers from the durable source stream. Events published from a **request/reply handler** (subscription read/mute/favorite, role_updated, room_restricted in room-service) have **no source stream to redeliver**: the local Mongo write commits, the inline publish fails, the client gets an error, and local/remote silently diverge. | Consumer paths: raise `MaxDeliver`+BackOff so a partition delays rather than drops (and pin source streams R3+file). Request/reply paths: route through a durable local stream + async federating consumer. |
| 2 | **Stream replica count (R1 vs R3) is not set in code** | `pkg/stream/stream.go` defines only `Name + Subjects`. `MESSAGES_CANONICAL` (single source of truth) and `INBOX` (federation ingress) **must be R3 + file storage** in prod, but nothing in the repo enforces it — dev bootstrap is minimal (effectively R1). | Verify production IaC pins R3 + file storage for these streams; consider `sync_interval` posture. |
| 3 | **MongoDB write/read concern uses driver defaults (untuned)** | Rooms/subscriptions are control-plane truth; a `w:1`-class write can be rolled back on primary stepdown. The code sets no explicit concern. | Pin `w:majority` + journaling as the connection-string default; use `majority` read concern on authz/membership reads. |
| 4 | **Security patch hygiene** | 2024–2025 brought a critical NATS authz-bypass (CVSS 9.6), a critical Redis/Valkey Lua RCE (CVE-2025-49844), and an exploited MongoDB info-leak (MongoBleed). | Pin patched builds (versions below). |
| 5 | **Cassandra read-side availability + repair discipline** | History-only and off the send path, so blast radius is bounded — but LocalQuorum hard-fails reads on quorum loss, and repair must run inside `gc_grace_seconds`. | Add a gocql retry/speculative policy; schedule repair; keep bucket sizing tight. |

**Licensing posture is mixed but low-risk here.** NATS, Cassandra, and Valkey are OSI-approved permissive licenses
(Apache 2.0, Apache 2.0, BSD-3-Clause); MongoDB is on the **SSPL** — source-available, *not* OSI-approved — which
constrains offering Mongo *as a managed service* but not embedding it as a dependency, as this repo does. The Valkey
fork *de-risked* the earlier Redis SSPL problem. Licensing is not a near-term operational risk (details per dependency).

---

## How resilience is wired today (codebase map)

Concrete, citable facts that ground the analysis below:

- **NATS reconnect:** `MaxReconnects(-1)` infinite, `ReconnectWait 2s` (`pkg/natsutil/connect.go`).
- **JetStream consumers** (`pkg/stream/consumer.go`): `AckExplicitPolicy`, `DeliverAllPolicy`, `AckWait` 30s
  (`CONSUMER_ACK_WAIT`), `MaxDeliver` 5 (`CONSUMER_MAX_DELIVER`), `MaxAckPending` 1000, `MaxWaiting` 512.
- **Stream schema** (`pkg/stream/stream.go`) defines **only `Name + Subjects`** — Replicas/Storage/Retention/MaxAge
  are **ops/IaC-controlled, not in code**. Dev bootstrap (`<service>/bootstrap.go`) calls `CreateOrUpdateStream`
  with minimal config.
- **Cross-site publish** is a synchronous JetStream publish to `chat.inbox.{destSiteID}.external.{eventType}` that
  **blocks on PubAck** (`message-worker/main.go:161`) and carries `jetstream.WithMsgID(...)` for server-side dedup.
  Dedup keys are built by `pkg/natsutil/canonical_dedup.go` (`CanonicalDedupID`) and `natsutil.InboxDedupID`.
  **The origin-side durable buffer is the source stream itself** for events published inside a JetStream consumer:
  a failed publish returns an error → the handler Naks → the message redelivers from the durable source stream and
  the publish retries. This holds for `message-worker` (consumes `MESSAGES_CANONICAL`) and `room-worker` (consumes
  `ROOMS`; external publish error returned at `room-worker/handler.go:514`). It does **not** hold for `room-service`
  cross-site publishes (`subscription_read`, `thread_read`, `mute_toggled`, `favorite_toggled`, `role_updated`,
  `room_restricted`), which are emitted inline from request/reply handlers (e.g. `room-service/handler.go:2035`):
  the local Mongo write commits first, then the publish; on failure the handler returns the error to the client
  (`...:2036`) with no durable source message to redeliver. `user-service` status publishes are best-effort by
  design (`status.go:112` — logged, not returned; last-write-wins self-heals).

### Federation publisher map (who has an outbox)

| Cross-site event | Publisher | Origin context | Implicit outbox? |
|---|---|---|---|
| message persist / thread-subscription | `message-worker` | consumes `MESSAGES_CANONICAL` (JS) | ✅ yes (Nak → redeliver) |
| `member_added` / `member_removed` | `room-worker` | consumes `ROOMS` (JS) | ✅ yes |
| `subscription_read`, `thread_read`, `mute_toggled`, `favorite_toggled` | `room-service` | request/reply handler | ❌ no — client-retry only |
| `role_updated`, `room_restricted` | `room-service` | request/reply handler | ❌ no — client-retry only |
| `user_status_updated` | `user-service` | request/reply, fire-and-forget | ❌ no — best-effort by design |

- **MongoDB** (`pkg/mongoutil`): connects with driver defaults — **no explicit write/read concern, read preference,
  or pool tuning** anywhere. Federation upserts are guard-gated by `updatedAt` so out-of-order events can't regress
  state (`inbox-worker/main.go`).
- **Cassandra** (`pkg/cassutil/cass.go`): `LocalQuorum`, 10s query timeout, `TokenAwareHostPolicy`, 8 conns/host,
  **no explicit retry policy** (gocql default = none). Message persistence is **async** via `message-worker`
  consuming `MESSAGES_CANONICAL` (idempotent `UnloggedBatch`), so **the send path does not block on Cassandra**.
  Bucketing via `pkg/msgbucket`, `MESSAGE_BUCKET_HOURS` default 72.
- **Valkey** (`pkg/valkeyutil`, `pkg/roommetacache`): **best-effort cache only**, cluster client, used by
  message-gatekeeper / broadcast-worker (L2 room-meta cache) and search-service (restricted-rooms cache). Every
  path degrades gracefully: L1→L2→Mongo `ReadThrough`; search-service logs "valkey read failed; falling through to
  ES." No connection-pool/timeout tuning beyond addrs+password.
- **Disposition:** `pkg/jobguard` **Acks on panic** (poison-pill drop); transient errors `Nak` for redelivery;
  `MaxDeliver=5` dead-letters. Readiness (`/readyz`) gates on dependency health; NATS health treats
  CONNECTED/RECONNECTING as healthy.

---

## 1. NATS + JetStream

### Failure-mode impact
- **At-least-once, not exactly-once.** AckWait expiry (default 30s here) and worker crashes cause legitimate
  redelivery; pull-consumer crash-before-ack double-delivers by design. → idempotency is mandatory, not optional.
  [docs.nats.io/consumers]
- **Poison messages redeliver forever unless `MaxDeliver` is set**; at the cap an advisory fires on
  `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.<STREAM>.<CONSUMER>`. The repo sets `MaxDeliver=5` (good) and
  `jobguard` Acks deterministic panics (good). [model_deep_dive]
- **Each stream is its own RAFT group**, separate from the meta group; an R3 stream can serve even with no meta
  leader, but **quorum loss stalls the stream** ("No Quorum has Stalled") and publishes during the stall are not
  stored. [nats-server#6236; Synadia insights]
- **Streams never span a supercluster** — a JetStream lives in exactly one cluster. A gateway partition doesn't
  corrupt a local stream but **blocks cross-cluster access** and, under a single shared JetStream domain, can strip
  meta leadership on the surviving side ("JetStream system temporarily unavailable, 10008"). Per-region **JetStream
  domains** are the documented fix. [jetstream_clustering; troubleshooting; nats-server#4502]
- **Request/reply during outages** returns "no responders" (503) or times out — clients must handle both; the
  room-member RPC here uses a 5s timeout. [core-nats/reqreply; nats-server#5738]

### Project/release stability
- **Apache-2.0 preserved; the Synadia/CNCF dispute resolved (May 1, 2025).** Synadia had moved to withdraw NATS
  from CNCF and relicense to BUSL; resolution assigned the NATS trademarks to the Linux Foundation, CNCF keeps the
  repos/domain, and **NATS stays Apache-2.0**. CNCF said Synadia was "free to fork"; no sustained fork emerged.
  Residual governance risk: low-moderate. [cncf.io; thenewstack; theregister]
- **Cadence:** ~6-month minors. 2.10 (26 patches) → 2.11 (Mar 2025) → 2.12 (Dec 2025). **2.12 ships a breaking
  change:** JetStream "strict mode" rejects invalid API requests by default; on-disk state format changed in 2.11
  and 2.12 (constrained downgrades). [nats.io/blog; whats_new_212]
- **CVEs:** **CVE-2025-30215** (CVSS 9.6) cross-account JetStream admin-API authz bypass enabling asset
  destruction — **fixed in 2.10.27 / 2.11.1**. **CVE-2026-27571** pre-auth WebSocket "compression bomb" DoS —
  relevant because `pkg/roomkeysender` uses WS — fixed in 2.11.12 / 2.12.3 *(details via search; verify)*.
  [GHSA-fhg8-qxh5-7q3w; openwall]

### Operational reliability
- **Jepsen NATS 2.12.1 (Dec 8, 2025): default config loses acknowledged writes on power failure.** Default
  `sync_interval` fsyncs only every ~2 min, so a simulated power loss dropped ~131,418 / 930,005 messages (≈14%,
  ≈30s of writes). `sync_interval: always` fsyncs before ack but costs throughput. *(A "49.7% loss" headline
  circulating is not a clean acked-write figure — treat as low-confidence; the verified number is ~14%.)*
  [jepsen.io/analyses/nats-2.12.1 (403; via search)]
- **Open consensus bugs as of that report:** even with `sync_interval: always`, partition + rapid
  membership-change lost 5/5,236 acked records (**#7545, open**); single-bit `.blk` corruption causes partial acked
  loss (**#7549**, medium-confidence). Streams have **permanently "vanished" after multiple `kill -9`** at R3/R5
  (**#6888, open**); rolling-restart **sequence desync** (#4875); consumer ack-floor outran stream seq after
  power-off, blocking delivery (#5412); R1 file consumers lose offsets on restart (#3260). 2.12 added empty-vote
  protection that changes quorum-recovery procedure. [nats-server issues]

### Implications here
- **Idempotency is already designed in** (`CanonicalDedupID` + `WithMsgID`, server dedup window ~2 min; idempotent
  Cassandra `UnloggedBatch` keyed on message ID). Keep every INBOX/canonical consumer idempotent — publish retries
  after an ambiguous PubAck timeout will double-deliver.
- **Pin `MESSAGES_CANONICAL` and `INBOX` to R3 + file storage in IaC** (the code won't do it for you), and decide a
  `sync_interval` posture for them — `always` trades throughput for durability on the streams that must not lose
  data. Treat rolling restarts/multi-node kills as real data-loss risk given the open RAFT issues; pin nats-server
  ≥ 2.10.27 / ≥ 2.11.1 and vet the 2.12.x open issues before adopting 2.12 in prod.
- **Federation durability is split, not uniformly absent (correction to an earlier draft of this report).** The
  source stream *is* a durable origin-side buffer for any cross-site event published inside a JetStream consumer:
  on a gateway partition the PubAck fails → the handler Naks → the durable source message redelivers and retries,
  with `WithMsgID` + idempotent destination consumers absorbing duplicates. This covers `message-worker` (messages,
  thread-subscriptions) and `room-worker` (room membership). The residual gap on these paths is narrow: (a) the
  `MaxDeliver=5` × `AckWait=30s` ≈ 2.5-minute poison cap drops the event if the partition outlasts the redeliveries
  — raise/unbound `MaxDeliver` and add a redelivery `BackOff` on these consumers, and alarm on the
  `MAX_DELIVERIES` advisory; and (b) the source stream must be **R3 + file storage** or the un-acked message can
  vanish (issue #2). A dedicated OUTBOX stream is **not** needed here.
- **The genuinely exposed paths are the request/reply-originated `room-service` events** (`subscription_read`,
  `thread_read`, `mute_toggled`, `favorite_toggled`, `role_updated`, `room_restricted`). These publish inline after
  the local Mongo write has already committed; a failed cross-site publish returns an error to the client with no
  durable source message to redeliver, so local and remote diverge and recovery depends on the client retrying.
  `role_updated` (permissions) is the most consequential to lose. Fix: route these through a durable local stream +
  async federating consumer (giving them the same implicit outbox), rather than publishing inline — a real refactor,
  not a config change. `user_status_updated` is also no-outbox but acceptably best-effort (last-write-wins).
- **Independently, run a per-site JetStream domain** so a remote-site outage can't strip the local meta leader (the
  #4502 mode), and keep destination INBOX consumers idempotent (already the case).

---

## 2. MongoDB

### Failure-mode impact
- **Single-primary failover window ≈12s median** by default (`electionTimeoutMillis`), during which writes to the
  absent primary fail/block. [replica-set-elections]
- **`w:1` writes can be silently rolled back on stepdown**; `w:majority` + journaling on voting members prevents it.
  A partitioned old primary keeps accepting "ghost writes" until it learns it was deposed, then rolls them back on
  rejoin. [replica-set-rollbacks; write-concern]
- **Defaults are not strongly consistent.** Causal consistency holds only with `majority` read **and** write
  concern; Jepsen found default configs unsafe, and 4.2.6 failed snapshot isolation and could lose acked writes in
  transactions under partition (2020 analysis; **not** re-verified on 8.0). Secondary reads expose replication lag.
  [jepsen 3.6.4 / 4.2.6 (403; via InfoQ + muratbuffalo); read-concern]
- **Go driver defaults** (`mongo-driver/v2`): `serverSelectionTimeoutMS` 30s, `heartbeatFrequencyMS` 10s,
  `retryWrites=true`, `maxPoolSize=100` (waits when exhausted). A stepdown can stall sends up to ~30s and exhaust
  the pool if traffic continues. [clientoptions.go; driver docs]

### Project/release stability
- **Lifecycles recently extended:** 6.0 EOL 2025-07-31; **7.0 and 8.0 supported through Oct 31, 2029.** Yearly
  majors; quarterly Rapid Releases are **Atlas-only** (self-host tracks the majors). [support-policy/lifecycles]
- **SSPL (since 2018, OSI-rejected):** Section 13 only bites if you **offer MongoDB to third parties as a service** —
  internal use / building apps on top is unaffected. Ecosystem trend is *toward* AGPL (Elastic 2024, Redis 2025);
  MongoDB staying on SSPL is a mild perception/outlier risk, no functional change for self-hosting here.
  [SSPL FAQ; redmonk]
- **CVEs:** **CVE-2025-14847 "MongoBleed"** (CVSS ~8.7, disclosed Dec 2025) — *unauthenticated* heap info-leak via
  crafted compression messages, **exploited in the wild**, ~87k exposed instances; self-host patch 2025-12-19.
  **CVE-2024-10921 / CVE-2024-6384** authenticated DoS/buffer over-read (fixed pre-5.0.30/6.0.19/7.0.15/8.0.3 —
  *verify ranges*). [mongodb.com security blog; bleepingcomputer; rapid7]

### Operational reliability
- **`w:1` + default read concern is the #1 silent-data-loss path** (rollback files are size-limited, not
  auto-reapplied). [replica-set-rollbacks]
- **8.0 parallel oplog apply** improves secondary throughput but is a **breaking metrics change**
  (`metrics.repl.buffer`), with a known lag regression on mixed binVersion-8.0/FCV-7.0 clusters and a TLS/OpenSSL
  high-CPU bug pre-8.0.5 *(SERVER ticket numbers via search — verify on jira.mongodb.org)*. Undersized oplog or
  lagging secondaries (e.g., during index builds) can force a full resync. [release-notes/8.0; oplog alerts]

### Implications here
- **Pin `w:majority` + journaling as the connection-string default.** Modern servers default to majority, but the
  repo sets **nothing explicitly** (`pkg/mongoutil` uses driver defaults) — make it explicit so no service can
  write at `w:1`. Rooms/subscriptions are control-plane truth whose loss corrupts membership/federation routing.
- **Use `majority` read concern for authz/membership reads** (reading a subscription to authorize a send, room
  membership for fan-out). **Do not read those from secondaries** — stale reads can authorize against a deleted sub
  or miss a just-added member. The `updatedAt`-guarded federation upserts are a good idempotency pattern; keep them.
- **Tame the failover blast radius on the send path:** set tight per-operation `context` deadlines (2–5s) on driver
  calls so a stepdown sheds load instead of saturating the 100-connection pool; `retryWrites=true` already lands the
  in-flight write on the new primary. Consider lowering `serverSelectionTimeoutMS` below 30s for interactive paths.
- **Per-site replica-set isolation is correct and must be preserved** — no service should hold a driver client
  pointed at a *remote* site's replica set; remote state arrives via INBOX events. Keep each site a standalone
  3-member majority-capable set (no cross-site arbiter/voting).
- **Patch MongoBleed now** and standardize on a supported 7.0.x or 8.0.x (≥8.0.5 to dodge the TLS/OpenSSL CPU bug).

---

## 3. Apache Cassandra

### Failure-mode impact
- **LocalQuorum trades availability for consistency.** RF=3 needs 2 local replicas; losing a 2nd in a range makes
  reads/writes unsatisfiable → `UnavailableException` (rejected before execution). [baeldung; datastax dml]
- **Hinted handoff only bridges short outages** — hints stop after `max_hint_window` (default 3h); a node down
  longer permanently misses those writes until repair. **`gc_grace_seconds` (default 10d) must exceed the hint
  window**, and repair must complete within it or deleted data resurrects ("zombies"). [thelastpickle; tombstones]
- **Tombstones/wide partitions degrade reads** in proportion to tombstones scanned; large partitions retain
  tombstones longer and can take nodes down. TWCS is right for append-only time-series but won't co-compact a
  tombstone with data in a different window. [instaclustr; thelastpickle TWCS; monday eng]
- **gocql defaults to NO retries**; an empty pool fails fast with "no hosts available in the pool," and there are
  reports of it not auto-recovering without a client restart (gocql#915). [pkg.go.dev/gocql; policies.go]

### Project/release stability
- **5.0 GA Sept 2024** — biggest change since 4.0: Storage-Attached Indexes, trie memtables/SSTables, Unified
  Compaction, native `vector` type/ANN, JDK17 support (but **JDK11 remains the default build**, and the two builds
  aren't cross-runnable). **3.x is EOL (2024-09-05).** Yearly cadence, latest-3 supported, no skipping majors on
  online upgrade. **Apache-2.0, ASF-governed → low fork/abandonment risk.** [cassandra blog; endoflife; java17 ref]
- **CVEs (all patched, none unauth-RCE):** CVE-2024-27137 (JMX/RMI deserialization → JMX cred capture; fixed
  4.0.15/4.1.8/5.0.3); CVE-2025-23015 & the misapplied-fix CVE-2025-26467 (priv-esc via `MODIFY ON ALL KEYSPACES`,
  CVSS 8.8; fixed 4.0.17/4.1.8/5.0.3); CVE-2025-24860 (`network_authorizer`/RBAC, specifics *low-confidence*).
  [apache lists; GHSA-5c4f-pxmx-xcm4]

### Operational reliability
- **Jepsen: Cassandra has lost acknowledged writes under partition** (one run: 285 acked writes lost,
  "not linearizable") — millisecond LWW timestamps raise conflict probability; counters are explicitly hazardous for
  exactly-once. (Older version; direction is well-established, exact figures medium-confidence.) [aphyr (403; via
  search); ably]
- **Repair is mandatory** (anti-entropy `nodetool repair` within `gc_grace_seconds`) — handoff/read-repair are
  best-effort. **Schema disagreement** ("schema version mismatch") recurs when DDL runs while nodes are down/
  partitioned (CASSANDRA-11142) — serialize DDL, never apply with nodes unreachable. No credible 5.0-specific
  durability-bug postmortem surfaced (*absence of evidence, not assurance* — 5.0's SAI/UCS/trie paths are newer
  code). [datavail; CASSANDRA-11142]

### Implications here
- **Blast radius is read-side, not write-side.** Persistence is async via `message-worker` (canonical → idempotent
  `UnloggedBatch`), so a Cassandra outage does **not** block message-send. What degrades: **history reads** (the
  history path reads at LocalQuorum; ≥2 of 3 local replicas down → `UnavailableException`, surfaced fast by gocql's
  no-retry default), and a **write-behind backlog** on `MESSAGES_CANONICAL` — *provided the worker Naks transient
  Cassandra errors (Unavailable/WriteTimeout) for redelivery and only Ack-poisons genuinely permanent ones* (the
  `errcode.Permanent` / `jobguard` convention is the right lever; confirm the timeout path Naks rather than panics).
- **Add a non-default gocql retry/speculative-execution policy** for history reads + a graceful "history temporarily
  unavailable" UX, since the current config relies on gocql defaults (no retry).
- **Repair discipline is a hard requirement** — schedule repair (e.g., Reaper) to complete every keyspace inside
  `gc_grace_seconds`. History is append-only, so a shorter `gc_grace_seconds` on `messages_by_room` is viable *only*
  after confirming it stays > `max_hint_window` (>3h) and repair reliably finishes inside it.
- **Bucket sizing is the top design lever.** `(room_id, bucket)` + `MESSAGE_BUCKET_HOURS` is exactly the
  wide-partition defense; re-tune the window *down* for the busiest rooms, prefer **TWCS**, and avoid in-bucket
  deletes that split tombstones across windows. Watch `thread_messages_by_thread` (one partition per thread, **not**
  bucketed) for unbounded growth on very long threads.
- **Pin ≥5.0.3 (or 4.0.17+/4.1.8+)**, lock down JMX/RMI, never grant broad `MODIFY ON ALL KEYSPACES`. Don't deploy
  new 3.x (EOL).

---

## 4. Valkey (Redis fork), cluster mode

### Failure-mode impact
- **Resharding produces MOVED/ASK redirects** and brief per-slot unavailability; go-redis `ClusterClient` follows
  them automatically up to `MaxRedirects` (commonly 8), after which an error surfaces — the app must catch it and
  degrade. [redis cluster-spec; oneuptime]
- **Async replication ⇒ acked writes can be lost on failover.** A write can be acked by a primary and lost on
  promotion before it replicated. Failover timing is governed by `cluster-node-timeout`; misconfig risks
  split-brain. `min-replicas-to-write` reduces but can't eliminate the loss window. [cluster-spec; oneuptime]
- **Persistence:** RDB (lose minutes) vs AOF `everysec` (lose ≤1s). For pure cache, RDB-only/none is fine — restart
  = cold cache, not corruption. [redis persistence]
- **Best-effort vs hard-dependency** changes everything: as a cache, an outage means cache misses → DB fallback
  (latency + DB load, no correctness loss). As coordination/locks, the async-loss window can drop a lock token and
  cause double-processing.

### Project/release stability — the licensing story (net positive)
- **Mar 2024:** Redis Inc. relicensed Redis BSD-3 → dual SSPLv1/RSALv2 (non-OSI). **Apr 2024:** the community forked
  **Valkey under the Linux Foundation** (BSD-3), backed by AWS, Google, Oracle, Ericsson, Snap; neutral multi-vendor
  governance. Within a year it became a distro/AWS default. [techcrunch; LF "a year of valkey"]
- **Cadence:** Valkey 8.0 (Sep 2024, large throughput/replication gains) → 8.1 GA (Apr 2025). **May 2025:** Redis
  *returned to open source* — AGPLv3 added as an option from Redis 8, antirez back. Both projects are OSI-open
  again but **have diverged** (different modules/features), so no longer drop-in identical at the edge. [valkey.io;
  redis.io/blog/agplv3; infoq]
- **CVE-2025-49844 "RediShell":** ~13-year-old Lua use-after-free → sandbox escape → potential RCE, **affects both
  Redis and Valkey** (post-auth). Patched **Valkey 7.2.11 / 8.0.6 / 8.1.4** (Redis 6.2.20/7.2.11/7.4.6/8.0.4/8.2.2).
  **CVSS discrepancy:** NVD/Redis/Wiz cite **10.0**, the Valkey GHSA lists **8.8** — same CVE, cite per-vendor.
  Workaround: restrict `EVAL`/`EVALSHA` via ACLs. [nvd; valkey GHSA-9rfg-jx7v-52p6; wiz]

### Operational reliability
- **Cross-slot multi-key ops fail** (`CROSSSLOT`) unless keys share a slot via `{hashtag}`; no cross-node
  transaction coordinator. [redis clustering best-practices]
- **Jepsen (Redis Sentinel):** up to ~56% writes lost during a partition, non-linearizable, historical permanent
  split-brain; **Redis-Raft (2020):** 21 issues incl. split-brain and data loss on failover. Async replication is
  unchanged, so the write-loss-under-partition property still holds. Mitigate (not eliminate) with
  `min-replicas-to-write` + tuned `cluster-node-timeout`. [aphyr; jepsen redis-raft]

### Implications here
- **Already correctly used as best-effort cache** — `roommetacache` L1→L2→Mongo `ReadThrough`, search-service
  "fall through to ES" on Valkey error. This bounds a full Valkey outage to elevated DB/ES load + latency, never
  correctness loss. **Keep it that way:** do not put correctness-critical coordination (exactly-once locks, dedup
  whose loss causes double-delivery) solely in Valkey — the durable stores already own dedup/idempotency, which
  aligns with the async-loss tolerance.
- **Realistic disruption is slot failover/resharding, not licensing.** Wrap `ClusterClient` calls with short
  timeouts + circuit-breaking so a resharding/failover storm (MOVED/ASK churn past `MaxRedirects`) doesn't stall
  request handlers — the current config sets no timeouts beyond addrs+password.
- **Cross-slot discipline:** co-locate related keys (e.g., a room's presence set + counter) under a `{roomID}`
  hashtag or hit `CROSSSLOT`.
- **Strategic upside:** choosing Valkey (LF/BSD, multi-vendor) **removes the Redis SSPL relicensing exposure** —
  there's no single-vendor lever to pull again. Lowest dependency-governance risk of the four.
- **Patch past CVE-2025-49844** (Valkey ≥ 8.1.4/8.0.6/7.2.11) and restrict Lua via ACLs if scripting isn't needed.

---

## Cross-cutting recommendations (prioritized)

1. **Close the federation durability gap — but only where it actually exists.** Consumer-originated cross-site
   events (messages via `message-worker`, membership via `room-worker`) already self-retry via Nak/redelivery from
   their durable source stream; for these, just raise/unbound `MaxDeliver` + add a redelivery `BackOff` and pin the
   source streams R3+file. The real silent-loss exposure is the **request/reply-originated `room-service` events**
   (`subscription_read`/`thread_read`/`mute`/`favorite`/`role_updated`/`room_restricted`), which publish inline
   after the local Mongo commit with no source message to redeliver — route those through a durable local stream +
   async federating consumer. Run a **per-site JetStream domain** regardless.
2. **Pin durability in IaC, not hope:** `MESSAGES_CANONICAL` + `INBOX` at **R3 + file storage**, chosen
   `sync_interval` posture; MongoDB **`w:majority` + journaling** as the default; gocql **retry/speculative policy**;
   Valkey **client timeouts**. Today these all rely on unstated defaults.
3. **Patch the 2024–2025 CVEs:** nats-server ≥ 2.10.27/2.11.1; MongoDB ≥ 8.0.5 (MongoBleed); Valkey ≥ 8.1.4/8.0.6/
   7.2.11 (RediShell); Cassandra ≥ 5.0.3 (or 4.0.17+/4.1.8+).
4. **Operational discipline:** Cassandra repair within `gc_grace_seconds`; MongoDB oplog sizing + lag alerts;
   serialize Cassandra DDL; avoid NATS rolling-restart/multi-kill choreography that the open RAFT issues punish;
   monitor `MESSAGES_CANONICAL` consumer lag as the Cassandra-outage pressure gauge.
5. **Verify the worker error taxonomy:** confirm the message-worker Naks transient Cassandra/Mongo errors
   (Unavailable/WriteTimeout/stepdown) for redelivery and only `errcode.Permanent`-Acks genuinely poison messages —
   this is what converts a dependency outage into a recoverable backlog instead of data loss.

---

## Confidence & caveats

- **High confidence:** at-least-once/redelivery semantics; streams-don't-cross-gateways; LocalQuorum availability
  cost; repair-within-gc_grace; async-replication loss window; all licensing history; the codebase config facts.
- **Medium / flagged:** exact Jepsen loss percentages (NATS ~14% verified vs ~49.7% headline unverified; Cassandra
  285-write figure from an old version); MongoDB transaction findings are 2020-era and **not** re-verified on 8.0;
  several CVE version ranges and SERVER ticket numbers came via search summaries (jepsen.io / nvd.nist.gov / some
  vendor docs returned 403) — **verify against NVD/JIRA before citing in a security doc**; CVE-2025-24860 specifics
  and CVE-2026-27571 fixed-dates are low/medium confidence; Valkey 9.x and Redis "atomic slot migration" parity in
  Valkey are unconfirmed.

### Primary sources (by dependency)
**NATS:** docs.nats.io (jetstream/consumers, clustering, reqreply, whats_new_211/212); nats.io/blog
(2.12-release, recover-quorum); nats-server issues #4502/#4875/#5412/#5738/#6236/#6888/#7545/#7549/#3260, PR #7038;
jepsen.io/analyses/nats-2.12.1; GHSA-fhg8-qxh5-7q3w; openwall 2025/04/08; cncf.io 2025/05/01; thenewstack;
theregister 2025/04/28 & 05/02.
**MongoDB:** docs.mongodb.com (replica-set-elections/rollbacks/write-concern/read-concern, drivers/go, lifecycles,
release-notes/8.0, oplog alerts); jepsen.io/analyses/mongodb-3-6-4 & 4.2.6; infoq 2020/05; muratbuffalo 2024/03;
mongodb.com security blog (MongoBleed); bleepingcomputer; rapid7; SSPL FAQ + wikipedia; scylladb; redmonk 2025/05/06.
**Cassandra:** cassandra.apache.org (5.0 SAI/announcement, java17, EOL, release-process, tombstones); thelastpickle
(hinted-handoff, TWCS); instaclustr; monday eng; pkg.go.dev/gocql + policies.go + gocql#915; apache lists
(CVE-2024-27137, CVE-2025-23015); GHSA-5c4f-pxmx-xcm4; aphyr/294; ably; datavail; CASSANDRA-11142; endoflife.date.
**Valkey/Redis:** valkey.io/blog (8.0/8.1); linuxfoundation.org; techcrunch 2024/03/31; redis.io (cluster-spec,
persistence, agplv3, atomic-slot-migration); antirez.com/news/151; lwn; infoq 2025/05; percona; oneuptime;
aphyr/283; jepsen.io/analyses/redis-raft; nvd CVE-2025-49844; valkey GHSA-9rfg-jx7v-52p6; redis GHSA-4789-qfc9-5f9q;
wiz.io.
