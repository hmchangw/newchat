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
