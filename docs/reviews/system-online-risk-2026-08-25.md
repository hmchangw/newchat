# System Online-Risk Analysis — 2026-08-25

**Scope:** whole system (all services + `pkg/`) — production risk of running the platform online: CPU-load hotspots, DB-load hotspots, speed/throughput ceilings, and stability problems.
**Branch:** `claude/system-risk-analysis-9osfrp`
**Method:** six parallel expert audits (message hot path, MongoDB, Cassandra, stability/resilience, cross-site federation, edge/config), each judging against CLAUDE.md conventions and industry practice for Go microservices at scale, cross-checked against `docs/nats-traffic-estimation.md` and `docs/load-testing/`.
**Assumed load (per site, from the traffic model):** ~20.8k users, ~4.5M messages/day (2.5M human + 2.0M bot), ~6,000 msg/s avg / ~24,100 msg/s peak on NATS, ~400M client deliveries/day.

## Executive summary

**Overall score: 3.7 / 5** (average of six dimensions).

The system is unusually well-engineered for its class: layered Valkey/LRU caches with singleflight shield MongoDB from per-message fan-out reads, room-doc writes are coalesced, `pkg/jsretry` makes bare `Nak()` impossible, panics are contained by `jobguard` in most workers, every Gin/Resty surface has timeouts, admission control (`natsrouter.GuardConfig`) covers the request/reply services, and shutdown ordering is correct everywhere. At the modeled *average* load nothing in this report bites.

The residual risk concentrates in four places, all of which surface under *peaks, hot entities, or outages* rather than average load:

1. **Cross-site federation can silently lose events** (the one **critical** finding): inbox-worker retries an unappliable event for only ~13 minutes (`MaxDeliver=6` × backoff) and then drops it with no DLQ and no reconciliation job — a destination-side Mongo outage longer than that permanently diverges membership state between sites.
2. **Thread hot-spots are quadratic**: every thread reply re-scans the entire `thread_messages_by_thread` Cassandra partition to recount replies — O(N²) cumulative cost focused on one partition, ending in timeout → NAK → redelivery storms on popular threads.
3. **Per-message fan-out amplification**: the badge-count RPC fans out to up to 500 members on *every* message, and `@all` in a large channel is a synchronous UpdateMany over every member's subscription doc — both scale cost linearly with room size × message rate.
4. **Config-drift traps with silent blast radius**: `MESSAGE_BUCKET_HOURS`, `ALL_SITE_IDS`, and `ROOM_KEY_RETIRED_TTL` are parsed independently per service with (almost) no runtime cross-check; each mismatch mode loses data or strands federation *silently*.

| # | Dimension | Score |
|---|-----------|:-----:|
| 1 | Message hot path (CPU & throughput) | 4 / 5 |
| 2 | MongoDB load hotspots | 4 / 5 |
| 3 | Cassandra / message-history load | 3 / 5 |
| 4 | Stability & resilience | 4 / 5 |
| 5 | Cross-site federation | 3 / 5 |
| 6 | Edge services & operational config | 4 / 5 |

**Findings by severity** (raw counts across dimensions; a handful of findings — the thread-count scan, `ALL_SITE_IDS` drift, `MESSAGE_BUCKET_HOURS` drift, inbox-worker replica ordering — were independently flagged by two dimensions and are counted in each):

| critical | high | medium | low | nitpick |
|:--------:|:----:|:------:|:---:|:-------:|
| 1 | 14 | 21 | 11 | 6 |

---

## 1. Message hot path — CPU & throughput (score 4/5)

Path audited: MESSAGES → `message-gatekeeper` → MESSAGES-CANONICAL → `message-worker` (Cassandra persist), `broadcast-worker` (delivery fan-out), `notification-worker` (push). The pipeline is well-engineered: L1+L2 caches with singleflight on the read path, coalesced `rooms.lastMsgAt` bulk writes, O(1) room-stream publish for channel fan-out, sonic + `jsonwarm.Pretouch`, jittered `NakWithDelay`, sane semaphore sizing (`MaxWorkers=100`, `PullMaxMessages=2×`, `MaxAckPending=1000`).

### Findings

- **high** — O(N²) thread-reply cost. `message-worker/store_cassandra.go:374-387` → `pkg/threadcount/count.go:34-60`: every reply re-scans the entire `thread_messages_by_thread` partition (page 5000) to derive `tcount`. At ~30k+ replies the 15s scan timeout trips → handler error → `jsretry` NAK-loop until `MaxDeliver=6` drops the reply; the thread stops accepting replies while hammering Cassandra with repeated full scans.
- **high** — per-message badge RPC over the full room audience. `notification-worker/handler.go:259,320-361`: `fetchUnreadCounts` runs on **every** created message, before the `survivors==0` early-return, over all non-muted members (up to `LARGE_ROOM_THRESHOLD=500`). Each message ⇒ one `badge.count.batch` RPC per home site computing unread counts for up to ~500 accounts; at N msg/s in busy 500-member rooms that pushes 500·N unread computations/s into user-service, and the 5s `badgeFetchTimeout` is spent inside the consumer's 30s AckWait budget.
- **medium** — uncoalesced per-message subscription writes. `broadcast-worker/handler.go:261` (`AdvanceSubscriptionLastSeen`, UpdateOne per message) and `:272` (`SetSubscriptionMentions`, UpdateMany per mention-carrying message): Mongo primary write IOPS scale 1:1 with site message rate even though the analogous rooms writes were coalesced (`coalescer.go`).
- **medium** — unbounded goroutine fan-out for thread followers. `broadcast-worker/handler.go:1254-1272`: one goroutine per follower, no semaphore (contrast `federateMentions`, capped at 8). 100 in-flight messages × 1k-follower threads = 100k concurrent goroutines contending on one NATS connection flush.
- **medium** — uncached, unprojected `GetRoom` on every edit/delete/pin/react. `broadcast-worker/store_mongo.go:54-61`: full room doc (including the `PreviewCiphertext` blob) per mutation event, bypassing the room-meta cache used on the create path; a reaction burst = one Mongo read per reaction. Also violates the "always project precisely" rule.
- **medium** — per-message member-list decode is O(room size). `notification-worker/handler.go:125` + `pkg/roomsubcache/roomsubcache.go:123-141`: every message fetches and stdlib-JSON-decodes the full member list from Valkey; dominant per-message CPU in this worker for large rooms.
- **low** — thread-reply Mongo chattiness: `message-worker/handler.go:335,434-536` pays ~8–10 round-trips per reply (dup-key probe insert, thread-room fetch, sender fetch, 2 subscription upserts, last-message + unread updates).
- **low** — negative subscription results not cached (`message-gatekeeper/subcache.go:88-89`): not-subscribed spam hits Mongo per message.
- **nitpick** — mention regex parse runs independently in three consumers per message; the parsed result could ride the canonical event.

Cross-check vs docs: at the modeled ~52/s average canonical rate none of this bites; the capacity ramp (docs/load-testing/performance/) will hit the two high findings first, and **neither hot-thread nor large-room-burst scenarios exist in the workload model**.

### Recommendations

1. **high** — replace the per-reply partition COUNT with a Cassandra counter or a Mongo `$inc` on `thread_rooms` (already written per reply), reconciling with an exact scan only on delete.
2. **high** — debounce/batch the badge phase per (room, site) over a short window (like the lastMsg coalescer), or compute counts client-side on push receipt.
3. **medium** — route `AdvanceSubscriptionLastSeen` through a coalescing buffer keyed (roomID, account) with `$max` semantics.
4. **medium** — add a semaphore to `publishToThreadAccounts`; give mutation paths a projected/cached room read.
5. **low** — cache negative subscription lookups (~10s TTL); add hot-thread and reaction-burst scenarios to `docs/load-testing/common/workload-model.md`.

---

## 2. MongoDB load hotspots (score 4/5)

The Mongo layer is disciplined: precise projections almost everywhere, indexes matched to filter shapes with cross-service ownership (`WarnMissingIndexes`), documented read-preference tiering (`docs/mongo-read-preference.md`), explicit pool config (`pkg/mongoutil/poolconfig.go`), coalesced room-doc writes, and four cache layers (roommetacache / roomsubcache / badgecache in Valkey, userstore LRU) shielding fan-out reads.

### Top 3 at-risk collections

1. **`subscriptions`** — the hottest collection: per-message writes (sender `lastSeenAt` `$max`, mention UpdateMany, `threadUnread` `$addToSet`), per-read FindOneAndUpdate, every chatlist/badge read, fan-out member loads, and 8+ indexes in room-service alone (every write updates all of them).
2. **`rooms`** — each busy room is one hot document receiving coalesced lastMsg/preview writes, read-floor writes, and member-count deltas while serving cache misses; single-document write serialization is the ceiling.
3. **`thread_subscriptions` / `thread_rooms`** — two UpdateOnes per reply per recipient, `replyAccounts` `$addToSet`, and a pre-limit double-`$lookup` inbox pipeline.

### Findings

- **high** — `message-gatekeeper/store_mongo.go:36-46`: `GetSubscription` on the per-send hot path has **no projection** — it decodes the full subscription doc including the unbounded `threadUnread` array; a member with thousands of unread threads inflates every send's read+decode.
- **high** — `message-worker/store_mongo.go:183-194` (`AddThreadUnread`): `$addToSet threadUnread` per thread reply with **no size cap**, cleared only on read (`room-service/store_mongo.go:1794,1814`). Inactive members of thread-heavy rooms accumulate ever-growing arrays → doc/index bloat + write amplification, and the array rides along in the gatekeeper hot-path read above.
- **medium** — `broadcast-worker/store_mongo.go:197-205` (`SetSubscriptionMentions`): `@all` in a 5k-member channel is an UpdateMany touching 5k subscription docs per message, synchronously in the canonical consumer; a burst of `@all` messages stalls the fan-out lane behind Mongo.
- **medium** — `user-service/mongorepo/subscriptions.go:295-302` (`AggregateSubscriptions`): phase 1 fetches **all** of an account's subscriptions, no limit, on the **primary**, per chatlist call; reconnect storms concentrate this on the primary.
- **medium** — `history-service/internal/mongorepo/pipelines.go:56-116`: thread-inbox pipeline runs two `$lookup`s per thread-subscription **before** `$limit` and sorts in-memory on an unindexable looked-up field; O(threads followed) per page.
- **medium** — `room-service/handler.go:1345-1476` (`messageRead`): 4–5 Mongo ops per read event; when the read floor moves, `UpdateRoomMinUserLastSeenAt` targets the same hot rooms doc as the coalescer flush — successive readers of a caught-up room serialize on that document (the `sameFloor` guard at `:1458` mitigates; watch p99 under read storms).
- **low** — `room-worker/store_mongo.go:89-90` (`ListByRoom`): unprojected, unbounded full sub docs for a whole room; rename-only, so occasional. `broadcast-worker/store_mongo.go:63-75` similar but DM-only (cosmetic).
- **nitpick** — `sso_tokens` has no TTL index; growth is capped by the unique `username` index, so acceptable.

### Recommendations

1. **high** — add a projection to gatekeeper `GetSubscription` (membership/mute/roomType only; exclude `threadUnread`).
2. **high** — bound `threadUnread` (`$push`+`$slice` cap, overflow ⇒ recompute from `thread_subscriptions`); alert on p99 array size now.
3. **medium** — gate `@all` by room size/role, or move the mention UpdateMany off the ack-critical path (best-effort goroutine like `federateMentions`).
4. **medium** — cap subscriptions per account (product limit) or page phase 1 of `AggregateSubscriptions`.
5. **medium** — denormalize `lastMsgAt` onto `thread_subscriptions` so the thread inbox sorts from an index and both `$lookup`s move after `$limit`.
6. **low** — project `room-worker.ListByRoom` to `u.account`; dashboard coalescer flush latency vs read-floor write rate per room to catch hot-doc serialization before users do.

---

## 3. Cassandra / message-history load (score 3/5)

The read path is genuinely well engineered — bucketed partitions with TWCS aligned to the bucket window, adaptive bounded bucket-walk (floor 730 days ≈ 49 buckets < `maxBuckets` 122, fanned 8-wide ⇒ worst case ~7 concurrent waves, not 49 serial reads), explicit projections, opaque page-state cursors, no ALLOW FILTERING outside tests, minimal LWT. The risk concentrates in thread hot-partitions and bucket sizing for busy rooms.

### Findings

- **high** — O(N) full-partition scan on every thread-reply write. `pkg/threadcount/count.go:33-66` scans the entire `thread_messages_by_thread` partition (no LIMIT) on every reply add (`message-worker/store_cassandra.go:374-387`) and delete (`history-service/internal/cassrepo/write.go:405-417`). A 100k-reply thread costs 20 round-trips of read per single write — cumulative O(N²) focused on one partition's three replicas; past the 15s backstop it becomes a timeout → NAK → redelivery storm aimed at exactly the hottest replicas. (Same root cause as hot-path finding #1.)
- **high** — `thread_messages_by_thread` is an unbounded single partition (`docs/cassandra_message_model.md:172-201`): no bucket, no reply cap. A long-lived support/bot thread at 2k replies/day × ~1KB ≈ 60MB/month crosses the ~100MB wide-partition guidance in ~2 months and grows forever, compounding the scan above.
- **high** — busy-room bucket can exceed healthy partition size. 360h = 15-day buckets (`history-service/internal/config/config.go:53`, `message-worker/main.go:46`); at ~1KB/row, 5k msg/day ≈ 75MB/bucket (borderline), a 20k msg/day firehose/bot room ≈ 300MB+. The window is table-global — one hot room can't be tuned without repartitioning everyone.
- **medium** — `MESSAGE_BUCKET_HOURS` drift has silent-data-loss blast radius and no runtime guard: five binaries parse it independently (`message-worker/main.go:46`, `bot-message-worker/main.go:37`, history-service, `tools/cdc-verify`, es-index-migrator — the latter `required`, no default). A writer/reader mismatch targets different partitions: messages persist but never render. `compaction_window_size` is also hardcoded `'360'` in the DDL, and changing the window live orphans all prior buckets. Contrast: `ROOM_KEY_RETIRED_TTL` got a startup cross-check; this more dangerous knob has none.
- **medium** — routine mutations of sealed TWCS windows: every reply UPDATEs the parent row's `tcount`/`thread_last_msg_at` in its original (often years-old) bucket (`write.go:384-401`, `store_cassandra.go:352-369`); edits/reactions/pins do the same. TWCS assumes immutable windows — these writes scatter cells across many windows' SSTables, inflating read SSTable-touch counts and blocking clean window drops.
- **low** — encrypted inserts write 4 explicit-null tombstone cells per row per table (`store_cassandra.go:157,167` + thread variants): ~400 tombstones per 100-row history page; under the 1000 warn threshold, deliberate, but accrues with edit/delete nulling.
- **low** — multi-partition UnloggedBatches (`store_cassandra.go:99-119`, `reactions.go:36`, `pin.go:69`): 2–3 statements spanning partitions force coordinator fan-out; documented and small.
- **nitpick** — `defaultWalkFanout` (8) equals per-host `NumConns` (8, `cassutil/cass.go:18`): one sparse-room walk can monopolize a host's pool; the 10s per-round-trip timeout is generous, so slow-node pileups hold worker goroutines long.

### Recommendations

1. **high** — replace the per-write full-partition COUNT with an incremental mechanism (counter column, or accept drift and recount only on delete); at minimum add a circuit breaker (stop recounting past N rows) so mega-threads can't NAK-loop.
2. **high** — bucket `thread_messages_by_thread` (or cap thread length product-side) before any thread reaches wide-partition territory; add max-partition-bytes metrics/alerts for both message tables now.
3. **high** — startup bucket-window guard: persist the canonical window in a Cassandra metadata row; every service verifies `MESSAGE_BUCKET_HOURS` against it and refuses to start on mismatch (same pattern as broadcast-worker's `ROOM_KEY_RETIRED_TTL` check).
4. **medium** — monitor TWCS efficacy under mutation churn (SSTablesPerRead p99 on `messages_by_room`); if old-window rewrites are material, serve `tcount`/`thread_last_msg_at` from `messages_by_id` only.
5. **medium** — write the per-room msgs/day ceiling the 360h window assumes into the schema doc; consider a smaller window at next major migration for known-hot rooms.
6. **low** — revisit explicit-null binds on the encrypted insert path (a read-side `IS NOT NULL` branch already exists).

---

## 4. Stability & resilience (score 4/5)

Foundations are disciplined: `pkg/jsretry` makes bare `Nak()` impossible (zero violations in production code; `time.Sleep` only in tests/tools), server-side BackOff is derived and clamped in one place (`pkg/stream/consumer.go`), `jobguard` gives panic containment with poison-drop semantics, NATS connects with `MaxReconnects(-1)`, request/reply services get admission control + handler timeouts, all consumers use queue groups / shared durables, caches are LRU+TTL or Valkey-TTL bounded, `memlimit` prevents OOMKill spirals, and every worker's shutdown ordering is correct (`iter.Stop` → `wg.Wait` → `Drain` → DBs, 25s < 30s grace).

### Findings

- **high** — message-gatekeeper has **no panic recovery**. `message-gatekeeper/main.go:195-202`: `natsmetrics.Consume` (`pkg/natsmetrics/loop.go:39`) has no `recover`, and unlike broadcast-worker (`guardedProcessor`), message-worker, notification-worker, room-worker and outbox-worker, the processor is not wrapped in `jobguard.Run`. A handler panic on a client-supplied poison message on the ingest path crashes the pod; JetStream redelivers → up to `MaxDeliver=6` crash-restarts per message; a burst becomes a crash storm.
- **medium** — notification-worker shutdown race. `notification-worker/main.go:378-399` vs `:489-505`: the invalidation pump goroutine is uncounted/unawaited; `invalIter.Stop()` is immediately followed by `close(invalCh)`, so an in-flight send at `:392` panics ("send on closed channel") during shutdown.
- **medium** — permanent head-of-line block on the ordered federation lane. `outbox-worker/handler.go:59` + `main.go:241-251`: on the `MaxDeliver=-1`, `MaxAckPending=1` lane only malformed subject/envelope is Permanent; a forward that can *never* succeed for other reasons (envelope over destination `max_msg_size`, deleted destination INBOX stream) is Nak'd forever and blocks all membership/rename federation to that peer, with no escape hatch or dedicated alert.
- **medium** — no Mongo deadlines in workers. `pkg/mongoutil` sets no client-level `Timeout`, and JetStream worker handlers (inbox-worker store methods `main.go:83-731`, message-worker) run Mongo ops on undeadlined consumer contexts; a TCP black hole pins up to `MaxWorkers` goroutines and the ack-pending budget until kernel timeouts.
- **medium** — inbox-worker ordering holds only within one instance. `inbox-worker/main.go:857-903`: the membership FIFO is a *shared* durable pull consumer; ≥2 replicas silently reintroduce the add/remove resurrection race the FIFO transport was built to prevent, and nothing enforces single-replica (one replica is also the per-site federation-apply SPOF, mitigated by JetStream buffering).
- **medium/low** — no DLQ anywhere: `MaxDeliver=6` × `jsretry.DefaultBackoff` ≈ 13-minute retry budget; an infra outage longer than that permanently drops messages (for message-worker that is lost Cassandra history) with only terminal metrics.
- **low** — `search-sync-worker/main.go:448-461`: `Fetch` errors are silent and unbackoffed; during a NATS outage each collection loop fails fast and spins hot.
- **low** — `push-notification-service/main.go:70-90`: pump goroutine not counted in the WaitGroup (message between `Next()` and `wg.Add(1)` can race `nc.Drain` — the exact race outbox-worker's `drainPool` comment fixes) and no `jobguard` around `HandleJetStreamMsg` (panic-safe today, fragile once real APNs/FCM replaces `LogDispatcher`).
- **nitpick** — `notification-worker/main.go:387,397`: `_ = msg.Ack()` discarded without the CLAUDE.md-required comment.

### Recommendations

1. **high** — wrap message-gatekeeper's processor in `jobguard.Run` (mirror broadcast-worker's `guardedProcessor`).
2. **medium** — count the notification-worker inval pump in a WaitGroup and await it between `invalIter.Stop()` and `close(invalCh)` (or let the pump own the close).
3. **medium** — in outbox-worker, classify never-succeeding publish errors (max-payload, stream-not-found) as Permanent-with-page; alert on ordered-lane oldest-ack-pending age.
4. **medium** — set a client-wide Mongo `SetTimeout` (or per-op deadlines in worker handlers).
5. **medium** — enforce inbox-worker single-replica at deploy level, or add the watermark guard before it is ever scaled out.
6. **low** — log + jitter-backoff the search-sync `Fetch` error path; add a park/DLQ stream for MaxDeliver-exhausted canonical messages so >13-minute outages degrade to replayable backlog instead of loss.

---

## 5. Cross-site federation (score 3/5)

The OUTBOX design (per-destination FIFO + concurrent lanes, `MaxDeliver=-1`, dedup IDs, watermark-guarded applies) is well-reasoned — `docs/design/2026-07-05-membership-federation-durability.md` shows the failure modes were understood. But the "no membership change is ever lost" guarantee rests on three things that don't exist yet: destination-side unbounded retry, the reconciliation backstop, and verified stream retention. Origin-side durability is real; destination-side it is a ~13-minute cliff.

### Findings

- **critical** — destination drops events after ~13 min of retries. `inbox-worker/main.go:983-988` builds its INBOX consumer from `DurableConsumerDefaults` with default `MaxDeliver=6` (`pkg/stream/consumer.go:18`); `jsretry.DefaultBackoff` totals ~12.6 min. Every Nak-until-dependency-lands path — `member_added` referencing not-yet-replicated users (`inbox-worker/handler.go:318-319`), and role/mute/favorite/section/read events awaiting their `member_added` (`main.go:192-199`) — silently exhausts and is server-dropped with no DLQ. A destination Mongo outage >13 min drops *every* in-flight INBOX event; the origin FIFO lane's `MaxDeliver=-1` durability ends at the destination stream, and the reconciliation backstop (design doc §3.4) is explicitly **not built**.
- **high** — OUTBOX retention is an unverified IaC precondition; both failure directions lose data. `outbox-worker/bootstrap.go:43-46` sets only Name+Subjects; nothing pins `MaxAge`/`MaxBytes`/`R3+file`. With Limits-policy `MaxAge`, expiry deletes *parked unacked* forwards during an outage longer than MaxAge → silent membership divergence with no repair path; without `MaxAge`, a multi-day peer outage grows the stream unboundedly. No OUTBOX oldest-message-age alert exists.
- **high** — cross-lane dependency race compounds the MaxDeliver cliff: concurrent-lane events (`role_updated`, `subscription_*`) routinely overtake the FIFO-lane `member_added` they depend on; the destination's Nak-retry absorbs this only within the ~13-min budget. After a peer outage the concurrent lane fans out at full pool speed while the ordered lane drains serially at ~1/RTT — if the ordered backlog takes >13 min to drain (≳15–45k events at 20–60 ev/s), dependent subscription-state events are dropped en masse.
- **high** — `ALL_SITE_IDS` drift silently strands events. Producers pick destinations from per-user/room `SiteID` data; `outbox-worker/main.go:38,122-126` builds lanes only for its own env list, warning only when the list is entirely empty. A peer present in data but absent from the env has *no consumer* — its events sit in OUTBOX unconsumed forever; `outbox.Publish` (`pkg/outbox/outbox.go:83`) validates event type but not destination, and each service carries its own copy of the list.
- **medium** — inbox-worker horizontal scaling breaks membership ordering (shared durable, ordering only within one instance; `member_added`/`member_removed` have no watermark guard and hard-delete) — same finding as stability §4; nothing caps replicas to 1.
- **medium** — FIFO-lane throughput ceiling under churn. `MaxAckPending=1` per destination (`outbox-worker/main.go:273-277`) ⇒ ceiling ≈ 1/(cross-gateway PubAck RTT): ~20 ev/s at 50ms RTT, ~6 ev/s at 150ms. Bulk adds are batched per room×destination (`room-worker/handler.go:1266-1292`), so 1000 members in one room is one event — but an HR/Teams sync touching 10k rooms produces ~10k ordered events per destination ≈ 8–30 min of lag, and `room_renamed` queues behind the entire churn backlog (SLO-9 says 99% forwarded within 30s).
- **medium** — direct-publish paths lose state on gateway outage. `user-service/service/status.go:117-120` (also settings/chatlist, `admin-service/permissions.go`) publish straight to remote INBOX, log-and-continue: status self-heals on the next set, but a lost chatlist/settings/permissions event stays divergent until the user next changes it. Real-time message fan-out to remote users is core-NATS fire-and-forget (`broadcast-worker/handler.go:1179`) — during a gateway outage remote users miss delivery *and* the activity refresh, so rooms show no new-message signal until the next post-recovery message; history is safe at origin.
- **low** — down-peer re-poke load is negligible (ordered lane ~1 probe/10 min; concurrent lane ≤1000 parked × ~10 min backoff ≈ 1.7 attempts/s).
- **nitpick** — design-doc drift: §3.2 says DefaultBackoff "capped at 2m"; `pkg/jsretry` has a 10m tail.

### Recommendations

1. **critical** — set `CONSUMER_MAX_DELIVER=-1` (or a large bound + DLQ) on inbox-worker's INBOX consumer; add a max-deliveries advisory/DLQ consumer with alerting.
2. **high** — codify OUTBOX/INBOX retention in code or a checked-in IaC manifest; have the bootstrap verify path assert `MaxAge`/replicas, not just existence; alert on OUTBOX oldest-message age.
3. **high** — build the §3.4 reconciliation backstop — the only unconditional repair for every finding above.
4. **high** — detect `ALL_SITE_IDS` drift: reject/alert on `outbox.Publish` to a destination outside the configured peer set, or derive producer and consumer peer sets from one source.
5. **medium** — enforce single-replica membership apply (or the Appendix-A watermark guard) before inbox-worker is scaled out.
6. **medium** — pre-plan hashed per-room FIFO lanes for churn bursts; alert on ordered-lane consumer lag while the peer is healthy.

---

## 6. Edge services & operational configuration (score 4/5)

Well-hardened edge: every Gin server sets Read/Write timeouts, Resty defaults to 30s, `natsrouter.GuardConfig` (MAX_CONCURRENCY=256 + 10s request timeout) is adopted by room/history/search/media/user services, presence is batch-capped (`BATCH_MAX=100`, 3s peer timeout, change-only publishes), pagination knobs exist (search `MAX_DOC_COUNTS`, history bucket caps, `ROOM_MEMBERS_LIMIT`), `BOOTSTRAP_STREAMS` defaults false, pool configs are validated, healthz/readyz everywhere. Residual risk concentrates in the auth front door and unvalidated cross-service invariants.

### Findings

- **high** — auth front door has no admission control. `auth-service/routes.go:12`, `main.go:108-126`: POST `/api/v1/auth` has no concurrency cap and no rate limit — `ginutil.MaxConcurrency` (429-shedding) exists and user-service HTTP uses it, auth-service does not. A fleet-wide reconnect storm (a NATS restart re-mints every JWT) against a slow OIDC IdP (10s timeout, `pkg/oidc/oidc.go:64`) piles unbounded in-flight goroutines/IdP calls on the one service every client needs to get online.
- **high** (mismatch #1) — `MESSAGE_BUCKET_HOURS` parsed independently in `message-worker/main.go:46`, `history-service/internal/config/config.go:53`, `bot-message-worker/main.go:37`: drift sends writers and readers to different Cassandra partitions — messages silently disappear from history (see §3).
- **high** (mismatch #2) — `ALL_SITE_IDS` divergence between producers (`user-service/config/config.go:66`, `broadcast-worker/main.go:49`, `bot-room-service/main.go:29`, `admin-service/config.go:48`) and `outbox-worker/main.go:38`: a peer in a producer's list but not outbox-worker's gets events published into a lane with no consumer — federation to that site silently stops (see §5).
- **medium** (mismatch #3) — `ROOM_KEY_RETIRED_TTL` in 4 services (`room-service/main.go:60`, `room-worker/main.go:70`, `bot-room-service/main.go:39`, `broadcast-worker/main.go:71`): only broadcast-worker enforces the 2×cache rule (`main.go:303`); drift among the three *writers* is unvalidated — a short-configured writer expires key versions peers still resolve, permanently failing `key.get` for messages already on the wire. Same convention-only class: `BADGE_CACHE_TTL`, `ROOM_LOCALITY_GRACE` (`room-service/main.go:103-110`), `ADMIN_ACCT_PREFIX`.
- **medium** — `translation-service/handler.go:26-45`: only an empty-text check before calling the external (paid/slow) backend; no max text length, no per-account rate limit — one client can monopolize the global `MAX_CONCURRENCY=100` budget with 1MB payloads.
- **medium** — observability gap: `pkg/natsmetrics/metrics.go:216` counts redeliveries/outcomes but no consumer-lag or stream-depth gauge exists anywhere; outbox-worker imports no natsmetrics at all — a down-peer backlog or a consumer-less OUTBOX lane is invisible unless ops independently scrapes NATS server metrics (assumed, not evidenced in-repo).
- **low** — `auth-service/handler.go:329`: JWT expiry jitter is ±, so a token can live 1.1×`NATS_JWT_EXPIRY` (~2.2h); with no revocation path an offboarded user keeps NATS access that long.
- **low** — `TLS_SKIP_VERIFY` / `TEAMS_TLS_INSECURE` single env vars (`auth-service/main.go:36`, `search-service/main.go:36`, `room-service/main.go:78`) disable TLS verification in prod with only a comment as a guard.
- **nitpick** — `upload-service/main.go:61,186`: `FILE_UPLOAD_MAX_FILE_SIZE=-1` (unlimited) is accepted; combined with a 5m WriteTimeout it's an operator footgun.

### Top 3 silent-incident config mismatches

1. `MESSAGE_BUCKET_HOURS` drift → cross-partition write/read split, silent history loss.
2. `ALL_SITE_IDS` producer/outbox-worker divergence → consumer-less OUTBOX lanes, silent federation stall.
3. `ROOM_KEY_RETIRED_TTL` writer drift → premature key-version expiry, permanent `key.get` failures.

### Recommendations

1. **high** — add `ginutil.MaxConcurrency` + a per-IP/account limiter to `/api/v1/auth`; consider a short-TTL validation cache to survive IdP brownouts.
2. **high** — export a per-destination `NumPending` gauge from outbox-worker and alert when OUTBOX contains destination subjects with no consumer lane (detects mismatch #2 at runtime).
3. **high** — Cassandra marker-row guard for the bucket window (see §3 rec 3).
4. **medium** — generalize broadcast-worker's fail-fast: writers publish their `ROOM_KEY_RETIRED_TTL` (and `BADGE_CACHE_TTL`, `ROOM_LOCALITY_GRACE`) to a well-known Mongo doc and cross-check at startup.
5. **medium** — enforce max text length + per-account rate limit in translation-service; adopt consumer-lag/pending gauges in workers or document the NATS exporter as a hard prod dependency.
6. **low** — make JWT jitter subtract-only, or document that effective max lifetime exceeds `NATS_JWT_EXPIRY`.
