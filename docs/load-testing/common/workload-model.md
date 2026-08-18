# Workload Model *(stub)*

> **Status: baseline seeded from the Cassandra plan; confirm the flagged items.**
> Shared production workload inputs for all system-level load tests. The
> authoritative per-input detail currently lives in
> [`../soak/cassandra-soak-plan.md`](../soak/cassandra-soak-plan.md) §B; this doc
> promotes it to a system-wide input and will absorb newer traffic data as it
> arrives.

## Baseline inputs (from Cassandra plan I1–I13)

| # | Input | Value | Note |
|---|---|---|---|
| I1 | Peak sustained send rate (busiest site, incl. federation-in) | **100 msg/s** | per site |
| I2 | Read : write ratio | **7 : 1** | |
| I3 | Thread-reply share of sends | **10%** | |
| I4 | Mutation rate (edit+delete+pin) of sends | **5%** | |
| I5 | Message size (median/p95/max), post-encryption | **1 / 2 / 10 KB** | |
| I6 | Messages per room per day (hot/cold) | **100 / 10** | Zipf room selection |
| I7 | Thread length (max/p99) | **500 / 50** | |
| I8 | Reactions per message (median/max) | **30 / 500** | ⚠️ **confirm meaning** — all msgs vs reacted-only; model as MAP width, not a rate multiplier |
| I9 | Soft-delete density | **0.1%** | inflates reply-count scan |
| I10 | Daily message volume | **4M/day** | ⚠️ **confirm scope** — global vs busiest site |
| I11 | Topology: rooms / group:DM / users-per-room | **1M / 3:7 / ~100** | |
| I12 | Messages per active user per day | **TBD** | ⚠️ needed to size active-user count |
| I13 | Total users on busiest site | **≤ 20,000** | ~2,000–5,000 concurrently active |

## Flagged for confirmation

- **I10 scope** (global vs busiest site) — 4M/day from ≤20k users ⇒ ~200 msg/user/day
  is very high, so I10 is likely global; a per-site cluster must use the per-site rate.
- **I8 meaning** — median-30-across-all-messages is not physically consistent at a
  realistic cadence; separate `reaction_rate` (active-users × per-user cadence)
  from `reactions_per_hot_message` (MAP width).
- **I12** — required to convert I13 into an active-user count.

*(Replace flagged values with confirmed production data when available.)*
