# SLO Measurement Map

> For each journey: the real path hop by hop, which instrument sits on which hop
> today, where the SLO's declared boundary actually falls, and which segments are
> dark. Read this before arguing about whether an SLO "passed" — most
> disagreements are really about which hop the number came from.
>
> Verified against the code on 2026-08-27. Rows marked **#337** land when that PR
> merges; everything else is on `main` today.

| | |
|---|---|
| **Status** | Draft — for review |
| **Companion** | [`p2-instrumentation-spec.md`](p2-instrumentation-spec.md) — what to build for the gaps this document finds |
| **Rule of thumb** | An instrument proves what happened **at its own hop**. Every hop between it and the user is an assumption |

---

## 0. The instrument inventory

Everything that exists, grouped by where it comes from. Nothing else is available
to an SLO.

### Shared, automatic (o11y SDK — every service gets these)

| Instrument | Covers |
|---|---|
| `http.server.request.duration` | Gin services (`o11ygin.Middleware`) — **auth-service** is the only one an SLO reads |
| `db.client.operation.duration` | Mongo + Cassandra client calls, wherever `WithObservability` is wired (all chat services; **not** the Teams sync or migration processes) |
| Go runtime / process metrics | Saturation context, not SLIs |

### `pkg/natsmetrics` — wired by 5 services only

Post-#337 the package exposes six instruments. **Consumer instrumentation is
wired in `broadcast-worker`, `message-gatekeeper`, `message-worker`,
`notification-worker`, `room-worker` — and nowhere else.**

| Instrument | Labels | What it proves |
|---|---|---|
| `chat.nats.consumer.messages` | `event_type`, `outcome` ∈ `ack \| nak \| term \| left_pending \| handler_cancelled` | Per-delivery disposition. `left_pending` is the crash/AckWait signature |
| `chat.nats.consumer.processing.duration` | `event_type` | **Handler start → disposition.** Not stream wait — see §1 |
| `chat.nats.terminal.failures` | `event_type`, `reason` ∈ `max_deliver \| permanent \| publish_exhausted \| consumer_deleted \| stream_unavailable \| invalid_payload \| internal` | Work that gets no further attempt. **This is the app-side terminal-drop counter** — see §6 |
| `chat.nats.publish.failures` | `destination`, `operation` | Publish failures only (the success half was deliberately removed) |
| `rpc.client.call.duration` **#337** | `rpc.method`, `error.type` | Outbound request/reply |
| `rpc.server.call.duration` **#337** | `rpc.method`, `error.type` | Inbound request/reply, on every `natsrouter` service |

`pkg/natsutil` adds `chat.nats.client.connected` and
`chat.nats.client.connection.events` (connection-risk backstop), plus
`nats_slow_consumer_events_total`.

### Per-service domain counters

| Instrument | Service | Labels |
|---|---|---|
| `message_gatekeeper_messages_total` | gatekeeper | `result` ∈ `accepted \| rejected \| retry \| failed`, `reason` |
| `message_worker_persistence_total` | message-worker | `message_kind` ∈ `user \| system \| thread_reply \| teams_migration \| unknown`, `result` ∈ `success \| error` |
| `broadcast_worker_fanout_recipients` | broadcast-worker | histogram, `room_kind`, `event_type` — **intended** audience |
| `broadcast_worker_recipient_deliveries_total` | broadcast-worker | `room_kind`, `event_type`, `result` — **per publish target** |
| `broadcast_worker_thread_view_publish_failures_total` | broadcast-worker | `event_type` |
| `notification_worker_outcomes_total` | notification-worker | `kind`, `result` ∈ `sent \| suppressed \| publish_failed \| failed` — **per message** |
| `search_service_requests_total` | search-service | `kind`, `status` |
| `search_service_request_duration_seconds` | search-service | `kind` only — **no status** |
| `search_service_es_duration_seconds` | search-service | — |
| `search_sync_worker_*` | search-sync-worker | bulk flush duration/actions, item failures, messages |
| `room_key_*_errors_total` | shared | absent / fanout / store |
| `cache_hits/misses/errors_total` | `pkg/cachemetrics` | cache name |

### Infra (not ours, not deployed on staging)

JetStream consumer `num_pending` / `num_ack_pending` / ack-floor — the **P3
exporter**. This is the outage backstop for every async SLO and it is the single
biggest hole in the list.

---

## 1. J1 — Send a message (SLO-1a, 1b, 2)

### The path

```text
client
 └─(1) publish → MESSAGES stream
      └─(2) message-gatekeeper consumes
            ├─ validate subject + payload
            ├─ GetSubscription           (Mongo, LRU+TTL cached)
            ├─ GetRoomMeta               (Mongo, cached — SKIPPED on thread/bypass paths)
            ├─ resolveQuoteSnapshot      (RPC → history-service, only when quoting)
            ├─ resolveThreadParent       (RPC → history-service, only for thread replies)
            ├─ FindUserByID              (Mongo)
            ├─(3) PublishMsg → MESSAGES-CANONICAL   ← JetStream PubAck
            ├─ reply to client
            └─ Ack MESSAGES
           (4) MESSAGES-CANONICAL fans to 4 independent consumers, in parallel
                ├─(5) message-worker      → Cassandra batch (+ thread scan + parent update)
                ├─(6) broadcast-worker    → Mongo writes, then route:
                │        channel → 1-2 core-NATS publishes to the room subject
                │        DM      → per-recipient publish
                │        thread  → per-account publish
                ├─(7) notification-worker → see J3
                └─(8) search-sync-worker  → see J4
                     (9) NATS fans the room subject out to connected clients
                          (10) client receives → decrypts → renders
```

### Hop-by-hop coverage

| Hop | Instrumented by | Proves | Blind to |
|---|---|---|---|
| 1 client publish | **nothing** | — | Whether the client's publish reached MESSAGES at all |
| 2 gatekeeper consume | `chat.nats.consumer.messages`, `.processing.duration`, `db.client.operation.duration`, `rpc.client.call.duration` **#337** | Disposition, handler time, its Mongo and history-RPC cost | Stream wait before the handler started |
| 3 canonical publish | `message_gatekeeper_messages_total{result="accepted"}` | **A canonical message was accepted.** This is the J1 denominator | Which fan-out route it will take — no `broadcast_path` label |
| 4 stream fan-out | **nothing in-app**; P3 exporter would give pending/ack-floor | — | Backlog on any of the four consumers |
| 5 persist | `message_worker_persistence_total{message_kind,result}` | **Cassandra write succeeded.** SLO-1a numerator | One outcome per *attempt*, so a redelivery double-counts |
| 6 broadcast | `broadcast_worker_fanout_recipients`, `broadcast_worker_recipient_deliveries_total`, `chat.nats.publish.failures` | Intended audience size; per-publish-target ok/failed | **Per logical message** outcome; and the age since canonical acceptance |
| 9 NATS core fan-out | **nothing** | — | Whether any subscriber received it |
| 10 client render | **nothing** | — | The entire last mile |

### Where each SLO actually lands

**SLO-1a — persisted / published. Computable today.** This is the correction
worth flagging: both halves already exist.

```promql
sum by (site) (rate(message_worker_persistence_total{
      message_kind=~"user|thread_reply", result="success"}[28d]))
/
sum by (site) (rate(message_gatekeeper_messages_total{result="accepted"}[28d]))
```

The `message_kind` filter matters. message-worker also persists `system` and
`teams_migration` messages, which enter MESSAGES-CANONICAL from history-service
mutations and room-worker system events — not through the gatekeeper — so without
the filter the numerator counts messages the denominator never saw and the ratio
drifts above the truth.

Still **approximate**, exactly as `sli-slo.md` §0.1 declares: both counters count
attempts, so a redelivery inflates both sides unevenly. Lag remains the primary
enforcement signal — and lag is the P3 hole.

**SLO-1b — channel enqueue accepted / published on the room-subject path. Not
computable.** Two independent reasons:

- The denominator cannot be sliced. `message_gatekeeper_messages_total` has no
  `broadcast_path`, so there is no way to count "canonical messages that will take
  the room-subject route".
- The nearest numerator counts the wrong unit.
  `broadcast_worker_recipient_deliveries_total{room_kind="channel"}` increments
  **once per publish target**, and `RoomEventTargets` returns two subjects for a
  room inside its cross-site flip window.

**SLO-2 — enqueued within 1 s of canonical acceptance. Not computable, and
nothing today is even close.** `chat.nats.consumer.processing.duration` on
broadcast-worker measures *handler start → disposition*. SLO-2 measures *canonical
acceptance → enqueue*, which includes the **stream wait before the handler
started** — precisely the interval that grows when broadcast-worker falls behind,
and precisely the interval the processing histogram excludes. A backlogged worker
can show a flat processing histogram while SLO-2 is failing badly.

### Dark segments

Hops 9 and 10 — core-NATS fan-out and client render — have no instrument
anywhere, by design. `sli-slo.md` §2 names this the "observational last mile" and
splits it into a protocol-receive prober (P6) and a render prober (P6b). Neither
exists. loadgen's L1 correlation observes hop 9 during a test run, which is why it
must never be reported as SLO-1b or SLO-2: it is a **different, later** boundary.

---

## 2. J2 — Login, enter channel, enter thread (SLO-3, 4, 5)

### The path

```text
LOGIN      client → POST /api/v1/auth (auth-service)  → NATS connect → subscription.list + rooms.get
ENTER CH   client → msg.history  → history-service LoadHistory  → 2 Mongo reads → Cassandra bucket walk
ENTER THR  client → msg.thread   → history-service GetThreadMessages → single partition slice
```

### Coverage

| Leg | Instrument | Status |
|---|---|---|
| auth POST | `http.server.request.duration` (`o11ygin.Middleware`, `auth-service/main.go:114`) | ✅ today |
| NATS connect | — | dark |
| initial data (`subscription.list`) | `rpc.server.call.duration` on user-service **#337** | ✅ post-merge, but as its own RPC — not joined to the login |
| `msg.history` | `rpc.server.call.duration{rpc_method="channel_history"}` **#337** | ✅ post-merge |
| `msg.thread` | `rpc.server.call.duration{rpc_method="thread_open"}` **#337** | ✅ post-merge |
| history-service → Cassandra | `db.client.operation.duration` | ✅ diagnostic |

**SLO-3 measures one leg of three.** The declared journey is auth → connect →
initial data; only the HTTP POST is measured, which `sli-slo.md` already labels a
proxy. The other two legs now have instruments (post-#337) but nothing joins them
into one journey — that needs a prober or spanmetrics, not another counter.

**SLO-4/5 are server-side proxies, and the bias runs the wrong way.** The
histogram's timer stops in a `defer` that runs after the handler returns —
after `Respond`, and `Respond` on core NATS returns once the reply is in the
client's write buffer. So the measured interval is *request received → reply
handed to the local NATS client*, not *caller got the answer*. If the connection
drops after the handler returns, the server records a sub-bound success and the
caller times out; the caller's timeout is not any server-side `error.type`, so it
lands in neither numerator nor denominator. **A server→client partition moves
these SLIs toward green during exactly the incident they should catch.** Pair
them with `chat.nats.client.connected` / `chat.nats.client.connection.events`,
which history-service emits.

Also note SLO-5's bound moved 300 ms → 250 ms and its target 99% → 95% in #337,
so the bound lands on a real histogram boundary.

---

## 3. J3 — Notifications (SLO-6)

### The path

```text
MESSAGES-CANONICAL
 └─ notification-worker
     ├─ GetMembers(roomID)          Valkey → Mongo on miss (no in-process tier)
     ├─ Presence.Snapshot(accounts) ⌈N/512⌉ request/reply to presence-service
     ├─ per-recipient filter        settings veto, eligibility, presence
     ├─ unread counts               per-site badge RPC
     └─ emit ⌈survivors/100⌉ batches → PUSH_NOTIFICATION stream
          └─ push-service (NOT IN THIS REPO) → provider → device
```

### Coverage

| Hop | Instrument | Gap |
|---|---|---|
| consume | `chat.nats.consumer.*` | — |
| member lookup | `cache_hits/misses/errors_total`, `db.client.operation.duration` | — |
| presence snapshot | `rpc.client.call.duration` **#337** | — |
| emit | `notification_worker_outcomes_total{kind,result}`, `chat.nats.publish.failures` | **Per message, not per recipient** |
| push-service onward | — | Different repo |

**SLO-6 is recipient-granular on both sides** — `good = recipients durably
accepted into PUSH_NOTIFICATION`, `valid = notifiable recipients`. The existing
counter records one outcome per *canonical message*
(`notification-worker/handler.go:99`, a deferred single `Record`), with
`result=sent` set once if any batch emitted. Counting events over recipients
mismatches units: one `sent` can stand for 1 or 4 000 recipients.

So SLO-6 needs the counter re-cut at recipient granularity — increment by
`len(batchAccounts)` on a successful emit, against a `push_recipients_total`
denominator. Everything past the stream boundary belongs to push-service and is
out of this repo's reach entirely.

---

## 4. J4 — Search (SLO-7, 8)

### The path

```text
QUERY   client → search.* → search-service → Elasticsearch
INDEX   MESSAGES-CANONICAL → search-sync-worker → ES bulk
```

### Coverage

| Hop | Instrument | Status |
|---|---|---|
| query outcome | `search_service_requests_total{kind,status}` | ✅ SLO-7 computable today |
| query latency | `search_service_request_duration_seconds{kind}` | ❌ **no `status`** — even after #337, which added status to the counter only |
| ES call | `search_service_es_duration_seconds` | diagnostic |
| RPC envelope | `rpc.server.call.duration` **#337** | ✅ |
| index path | `search_sync_worker_messages`, bulk flush duration/actions, item failures | traffic only |
| index convergence | — | dark |

**SLO-7 covers partial degradation only.** The denominator is search-service-local,
so a total outage reads as *no traffic* rather than as failures. That needs a
health-check or prober backstop, not a ratio.

**SLO-8 is not computable server-side.** `durationOptFor(kind)` returns a
`kind`-only attribute set, so successful requests cannot be isolated from failed
ones inside the histogram. loadgen's `--workload=search` scores it client-side,
which is a useful cross-check and not a substitute.

**Index convergence is invisible by construction.** search-sync-worker Acks and
drops a payload it cannot decode, so that loss leaves the consumer at zero
pending, no Nak, no redelivery. Only a query-side probe can see it — and loadgen's
`search_index` observer is refused at startup because soak payloads analyse to a
single token.

---

## 5. J5 — Federation (SLO-9)

### The path

```text
room-service / room-worker / message-worker / broadcast-worker
 └─ publish OutboxEvent → OUTBOX-{site}
     └─ outbox-worker, per remote peer:
          ├─ concurrent lane (order-insensitive types)
          └─ ordered lane, MaxAckPending=1 (member_added/removed, room_renamed)
               └─ direct JetStream publish → remote INBOX-{destSite}
                    └─ inbox-worker applies state
```

### Coverage

**This is the darkest lane in the system.** `outbox-worker` and `inbox-worker`
call `obs.Init` and nothing else: no `natsmetrics` consumer wiring, no domain
counters. They get Go runtime metrics and `db.client.operation.duration`, and
that is all.

| Needed for SLO-9 | Exists |
|---|---|
| Producer-side `outbox_events_published_total{origin_site,dest_site,event_type}` | ❌ |
| `outbox_forwarded_total` gated on age ≤ 30 s, same label set | ❌ |
| Consumer disposition / backlog on either worker | ❌ |
| Oldest-pending age per peer lane | ❌ (P3 + a custom monitor) |

And loadgen is single-site, so even with the counters there is no traffic to
drive them. SLO-9 is unmeasurable **and** unexercisable today.

---

## 6. The terminal-drop question, restated

Earlier notes in this program said a `MaxDeliver` exhaustion is invisible without
a JetStream advisory consumer. **That is only half true, and the half that is
false matters.**

`pkg/natsmetrics` already detects exhaustion from inside the application. When a
handler Naks on the delivery where `NumDelivered >= MaxDeliver`, `Message.finish`
calls `MarkTerminal(TerminalMaxDeliver)`, which increments
`chat_nats_terminal_failures_total{event_type, reason="max_deliver"}`
(`pkg/natsmetrics/metrics.go:432,488`). The gatekeeper uses the same
`IsFinalDeliveryFromContext` signal to classify its own outcome as `failed`
rather than `retry` (`message-gatekeeper/handler.go:197`).

So terminal drops **are** counted today, on the five consumers that wire the
package. What advisories would add:

| Case | App-side counter | Advisory |
|---|---|---|
| Handler returns an error on the final delivery | ✅ `reason="max_deliver"` | ✅ |
| Handler Terms a poison message | ✅ `reason="permanent"` | ✅ |
| Worker crashes / hangs / OOMs, message expires by AckWait | ❌ — the handler never runs `finish()` on the last attempt | ✅ |
| A consumer nobody instrumented (`outbox-worker`, `inbox-worker`, `search-sync-worker`) | ❌ | ✅ |

The crash path leaves a different fingerprint that *is* visible:
`chat.nats.consumer.messages{outcome="left_pending"}` plus a restart. So the
practical answer is that advisory capture buys **completeness and coverage of
un-instrumented consumers**, not basic detection — which is why the failure
program allows a campaign to report counts without it, as long as it does not
claim advisory-grade completeness.

---

## 7. Can loadgen produce a first SLO number?

Yes — for five of the nine, and the framing that makes it legitimate is:

> **loadgen is the traffic source, not the instrument.** The number comes from the
> *production counters* over the run window, using the *production predicates*.
> loadgen's own L1 measurements are used only where no production counter exists,
> and then only as a one-sided bound (see below).

This is exactly what `sli-slo.md` §10 requires before loadgen may be said to
*validate* anything: "the same production source counters and good/valid
predicates, evaluated over isolated run-window deltas — not the 28-day aggregate
rule, and not loadgen-local metrics."

**The local overlay already scrapes everything needed.**
`tools/loadgen/deploy/prometheus/prometheus.yml` collects the o11y SDK endpoint
(`:2112`) off every service by Docker SD — which is where all the domain counters
land — plus the per-service `:9090` counters, cAdvisor, **and a JetStream
exporter sidecar**. So the P3 backlog signal that staging lacks is present on
docker-local. Nothing new has to be built to run this.

### Per-SLO verdict

| SLO | Where loadgen sits vs the SLO's boundary | First number? |
|---|---|---|
| **1a** persist | Both counters exist and bracket the boundary exactly | ✅ **Real number.** `message_worker_persistence_total{message_kind=~"user\|thread_reply",result="success"}` ÷ `message_gatekeeper_messages_total{result="accepted"}`, as run-window deltas |
| **1b** channel enqueue | No enqueue counter; loadgen observes hop 9, downstream of the boundary | ⚠️ **One-sided bound only** — see below |
| **2** enqueue ≤ 1 s | Same | ⚠️ **One-sided bound only** — see below |
| **3** login | loadgen drives the real HTTP leg; `http.server.request.duration` is the production counter | ✅ **Real number.** `max-rps --workload=login` |
| **4** enter channel | Production counter lands **#337**; and loadgen is the caller, so it can measure the better boundary | ✅ **Real number, and better than production's** — see below |
| **5** enter thread | Same | ✅ Same |
| **6** push handoff | Counter is per-message, not per-recipient; no PUSH observer in loadgen | ❌ |
| **7** search ok | `search_service_requests_total{kind,status}` exists | ✅ **Real number.** `max-rps --workload=search` |
| **8** search ≤ 1 s | Histogram has no `status`; loadgen scores it client-side | ⚠️ **loadgen-side number only**, a different boundary from the eventual SLI |
| **9** federation | Single-site driver, no counters | ❌ |

### Why SLO-1b/2 still give you something worth having

loadgen's **E2** stage is `publish → RoomEvent received`: it starts at hop 1 and
ends at hop 9. SLO-2's interval is `canonical accepted → enqueue`: hop 3 to hop 6.

```text
hop 1 ────────── 3 ───────── 6 ───────── 9
      │          │           │           │
      │          └── SLO-2 ──┘           │
      └──────────── loadgen E2 ──────────┘
```

**SLO-2's interval is strictly inside loadgen's.** E2 starts earlier and ends
later, so `E2 ≥ SLO-2's age`, always. Therefore:

- **E2 ≤ 1 s ⟹ the enqueue age was ≤ 1 s.** A pass is conclusive.
- **E2 > 1 s does not mean SLO-2 failed** — the extra time could be entirely in
  fan-out or delivery, hops SLO-2 does not own.

The same logic gives SLO-1b a lower bound: observing the RoomEvent proves the
enqueue succeeded, so loadgen's received count is a floor under the
enqueue-accepted count.

So the honest report is:

> *At 100 msg/s, ≥ 99.4 % of accepted sends were received by a room member within
> 1 s. SLO-2's true good-ratio is at least this. If the target is missed, this run
> cannot say whether the miss is SLO-2's or the delivery lane's.*

That is a real, defensible statement, and it is enough to answer the question
calibration actually needs: **is the drafted target reachable at all?**

**Use the production denominator even here.** loadgen's own send count and
`message_gatekeeper_messages_total{result="accepted"}` are not the same set — a
rejected send leaves no canonical message. Pin the denominator to the accepted
counter so the ratio matches the SLO's definition.

### Where loadgen is a *better* instrument than production

SLO-4/5's known weakness is that the histogram's timer stops after `Respond`
returns, so a reply lost after the handler returned reads as a fast success and
lands in neither numerator nor denominator (§2).

**loadgen is the caller.** Its own request/reply timing measures the
caller-visible boundary — request sent → answer in hand — which is the boundary
the SLO actually wants and production cannot see. A run can therefore report
both, and the gap between them *is* the size of the blind spot:

```text
loadgen client-side p95  −  rpc_server_call_duration p95  =  the unmeasured tail
```

That comparison is worth doing once and writing down; it tells you how much to
trust the production SLI later.

### The run protocol that makes the numbers legitimate

Five things, all from `sli-slo.md` §10 and `end-to-end-plan.md` §0. Skip any one
and the number is not an SLI, it is a graph.

1. **Traffic isolation.** A dedicated test `SITE_ID` (or a separate Prometheus
   tenant). Counters are monotonic and shared — `increase()` over the window
   excludes *history*, not *concurrent* traffic.
2. **Warm-up → drain → baseline snapshot → send window → settle window.** Snapshot
   every counter at the warm-up boundary and measure the delta; otherwise warm-up
   completions inflate the numerator.
3. **Asymmetric window.** Denominator over the send window; numerator waited out to
   the max SLO deadline plus a scrape margin, because an async numerator lands
   after the sender stops.
4. **`O11Y_ENABLED=true`** with `OTEL_TRACES_SAMPLER=parentbased_traceidratio` and a
   **fixed, recorded** sampler arg. Unset means 100 % and distorts what you are
   measuring.
5. **Backlog must be flat.** Every durable's `num_pending` bounded and not
   monotonically growing over the window — the exporter is right there in the
   overlay. A latency number taken while a consumer is backing up is measuring a
   queue, not a service.

### What the number is, and what it is not

| It is | It is not |
|---|---|
| An **achievability check**: at load X, the system achieved Y | An SLO verdict — that is a 28-day window over production traffic |
| The **input to calibration** (Track 1.2/1.3): evidence that a drafted target is or is not reachable before anyone commits to it | Proof the target is right |
| A **regression baseline** for later runs on the same box | Comparable across machines |
| Box-relative on docker-local; production-representative only on staging | An absolute capacity number |

**Report it as a run-window SLI with the load and the window stated**, e.g.
*"SLO-1a run-window SLI = 99.94 % at 100 msg/s over 30 min, isolated site,
sampler 0.1, backlog flat"*. Never as "SLO-1a = 99.94 %".

### Suggested shape of the first run

1. Fixed load at the declared baseline (I1 = 100 msg/s) through
   `--inject=frontdoor`, `realistic` preset, encryption on, 30–60 min.
2. Compute the five available ratios from the production counters as run-window
   deltas, plus the two one-sided bounds.
3. Record the loadgen-vs-production gap for SLO-4/5.
4. Then ramp (`max-rps`, explicit `--steps=100,250,500,1k,2k,5k`) and repeat the
   ratios at each step — that turns the achievability check into "at what load
   does each SLI first miss its drafted target", which is Track 2's question and
   comes almost free once step 2 works.

Steps 1–3 need **no code change**. Step 4 needs none either.

---

## 8. Summary

| SLO | Numerator today | Denominator today | Verdict | Missing |
|---|---|---|---|---|
| **1a** persist | `message_worker_persistence_total{result="success"}` | `message_gatekeeper_messages_total{result="accepted"}` | ✅ **computable now** with a `message_kind` filter | Exactness only (attempts vs terminal outcomes) |
| **1b** channel enqueue | per-target, wrong unit | no `broadcast_path` slice | ❌ | `broadcast_path` label + a per-message enqueue counter |
| **2** enqueue ≤ 1 s | none — processing duration excludes stream wait | as 1b | ❌ | Age histogram from the JetStream metadata timestamp |
| **3** login | `http.server.request.duration` | same | ✅ one leg of three | Journey join (prober) |
| **4** enter channel | `rpc.server.call.duration{channel_history}` **#337** | same | ✅ as a server-side proxy | Caller-visible boundary |
| **5** enter thread | `rpc.server.call.duration{thread_open}` **#337** | same | ✅ as a server-side proxy | Caller-visible boundary |
| **6** push handoff | `notification_worker_outcomes_total` — per message | — | ❌ | Recipient granularity on both sides |
| **7** search ok | `search_service_requests_total{status}` | same | ✅ partial degradation only | Outage backstop |
| **8** search ≤ 1 s | duration histogram has no `status` | — | ❌ | `status` label on the histogram |
| **9** federation | none | none | ❌ | Both counters, consumer wiring, and a second site |

**Across every async SLO the same backstop is missing**: JetStream consumer
`num_pending` / `num_ack_pending` / ack-floor from the P3 exporter. `sli-slo.md`
makes lag the *primary* enforcement signal precisely because the v1 ratios are
approximate — so today the primary signal is absent and the secondary one is
partial.

---

## 9. Sibling documents

- [`common/sli-slo.md`](common/sli-slo.md) — the SLO definitions this maps
- [`p2-instrumentation-spec.md`](p2-instrumentation-spec.md) — what to build for J1's gaps
- [`execution-priority-plan.md`](execution-priority-plan.md) — where this sits in the schedule
- `docs/specs/o11y/nats-metrics-contract.md` — the instrument registry
