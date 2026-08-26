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
