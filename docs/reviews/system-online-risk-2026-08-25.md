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
