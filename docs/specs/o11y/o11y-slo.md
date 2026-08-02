# Service Level Objectives — chat platform

| | |
|---|---|
| **Status** | Draft — for review |
| **Author** | Michelle Leu |
| **Reviewers / Approvers** | *TBD* |
| **Approval date** | — |
| **Revisit date** | ~6 weeks after approval (end of calibration, §0.2) |

Companions: `o11y-metrics-inventory.md`, `o11y-trace-design.md`,
`o11y-performance-and-sampling.md`. Error budget policy: placeholder
(`o11y-error-budget-policy.md`, to be written).

---

## 0. How to read this

SLOs are organized by **critical user journey**, not by service — following
Google's CUJ methodology ([SRE Workbook](https://sre.google/workbook/implementing-slos/))
and how chat products measure themselves (Slack's
[Service Delivery Index](https://slack.engineering/service-delivery-index-a-driver-for-reliability/)
tracks *send a message* / *load a channel*, not services). Each journey
section: **SLI specification** (user-language intent, good/valid ratio) →
**SLO target** → **Measurement** (✅ computable today / 🔧 to build; the
**enforcement** implementation is marked) → caveats.

### 0.1 Conventions

- **Every SLI is an event-based ratio**: `good events / valid events` over the
  window. Availability SLIs gate on success; latency SLIs gate on *success **and**
  completion within a stated bound*. **Percentiles are diagnostic only, never SLO
  targets** — a raw percentile has no good/valid ratio, so error budgets and
  burn-rate alerts (§7) cannot be computed from it. A latency SLO is written as
  "X% of valid requests completed within the bound".
- **Window**: 28-day rolling.
- **Why event-based, not time-slice**: a minutes-below-threshold slice model
  either leaves the in-slice share undefined or, with a minimum-sample gate,
  hides sustained low-volume failures (a lane failing 19/19 every 5 min would
  read `insufficient_data` forever). Event ratios count every real failure. **Low
  sample size affects *alerting confidence*, not budget inclusion** — a sparse
  window still accrues its true failures; burn-rate multi-window alerting (§7)
  prevents a single 03:00 failure from paging. Synthetic probes (§8 P6) give
  sparse journeys a traffic floor for signal, not for hiding failures.
- **Outage-safe denominators.** A worker that emits its own denominator reports
  *nothing* when it is down, turning an outage into missing data instead of
  failures. So, wherever possible, the denominator is anchored **upstream** of
  the measured worker (e.g. gatekeeper's canonical-published count is the
  denominator for persistence/publication — it keeps climbing if a worker dies,
  dropping the ratio). Where a denominator is unavoidably self-emitted (e.g.
  notifications), **consumer lag (§8 P3) is the outage backstop**, not the ratio.
- **One terminal outcome per logical unit** *(target contract)*. Numerators aim
  for a single terminal outcome per logical message/notification/event — **not**
  per attempt — deduped at the emit site (never a message-ID label). Two distinct
  terminal **failures** exist and must be counted separately: an explicit
  **permanent poison drop** (`errcode.Permanent` → Ack-and-drop, no retry) and
  **`MaxDeliver` exhaustion** (Nak-retried until the delivery cap; delivery then
  stops and a max-delivery advisory fires — the message is not deleted, see below).
- **v1 accounting is approximate where the code can't yet emit that contract.**
  Today the pipeline lacks the hooks for exact one-outcome accounting: gatekeeper
  ignores `PubAck.Duplicate`, Cassandra's idempotent inserts don't flag the first
  write, `jsretry.Settle` has no exhaustion callback, and the outbox retries
  forever (`MaxDeliver=-1`). So under redelivery/recovery, v1 counters may
  double-count or split numerator/denominator across windows. Every SLI that
  depends on this is marked **approximate (lag-enforced)**: the primary
  enforcement signal is **consumer lag / oldest-pending age** (§8 P3), and the
  ratio is a secondary indicator until an exact **outcome ledger** lands (§8 P7).
- **Rounding**: shares ≥ 0.1% precision; latency bounds up to nearest 50 ms.
- **Scope: per site**; federation (J5) isolated by `{origin, dest}` site.
- **Error-budget eligibility** — a *failed valid event* is judged by whether the
  request was legitimate and *expected to succeed*, not by wire category alone.
  (`MaxDeliver` exhaustion does **not** delete the message — JetStream stops
  redelivering it to that consumer, emits a max-delivery advisory, and the
  message stays in the stream; exact terminal-failure accounting therefore needs
  an **advisory consumer**, §8 P7, not just a `jsretry` hook.)

  | `errcode` / HTTP | Burns budget? | Why |
  |---|---|---|
  | `internal`, `unavailable`, `too_many_requests` (429), server-side timeout | **Yes** | our fault: bug, overload, capacity |
  | `bad_request`, `unauthenticated`, `forbidden`, `not_found`, `conflict` (4xx) | No | legitimate user/client outcome |

  Excluded rows are removed from *valid events* entirely — never counted as good.

### 0.2 Calibration

All targets are **achievable-first starting values**. Run observationally for
4–6 weeks, then adjust and seek approval. No paging alerts before then.

### 0.3 SLO ≠ SLA

**Internal document.** Any external SLA must be *derived* (looser,
availability-focused, exclusions/remedies with business/legal, post-calibration).
Never publish these numbers as-is.

---

## 1. Journeys & SLOs at a glance

| Rank | Journey | One-liner |
|---|---|---|
| J1 | **Send a message** | Accepted, durably stored, published for real-time fan-out |
| J2 | **Named interactive workflows** | Login, enter channel, enter thread |
| J3 | **Notifications** | Offline users still learn about messages |
| J4 | **Search** | Find rooms and messages |
| J5 | **Federation** | Cross-site state converges quickly |

Targets are starting values (§0.2). Interactive RPCs not named here (room
create, member ops, uploads, …) are dashboard-monitored, **no SLO in v1**.
Ephemeral signals (typing, presence) are best-effort by design — never SLO
material; read receipts / unread badges → dashboard, v2 candidate (§8 P7).

| # | Journey | SLI = good / valid | Target | Today |
|---|---|---|---|---|
| SLO-1a | J1 | canonical message persisted to Cassandra / **canonical messages published** | 99.9% | 🔧 P2 · approximate (lag-enforced) |
| SLO-1b | J1 | **channel** room-subject broadcast published ok (single publish) / **canonical messages routed to the room-subject path** (`broadcast_path=room_subject`) | 99.9% | 🔧 P2 · approximate · core-NATS boundary |
| SLO-2 | J1 | room-subject broadcast published **within 1 s** of canonical acceptance (late = bad) / canonical messages on the room-subject path (`broadcast_path=room_subject`) | 99% | 🔧 P2 · approximate |
| SLO-3 | J2 | successful login **within 1 s** / eligible login attempts | 99% | ✅ (auth leg — proxy) |
| SLO-4 | J2 | channel load succeeds **within 500 ms** / eligible channel loads | 95% | 🔧 P1 |
| SLO-5 | J2 | thread open succeeds **within 300 ms** / eligible thread opens | 99% | 🔧 P1 |
| SLO-6 | J3 | **recipients** accepted into `PUSH_NOTIFICATION` / notifiable recipients | 99.9% | 🔧 P4 · approximate · handoff only (see §4) |
| SLO-7 | J4 | search returns ok / **eligible** search requests | 99.5% | ✅ partial-failure · outage needs backstop (§5) |
| SLO-8 | J4 | **successful** search returns **within 1 s** / successful searches | 95% | 🔧 P4 (needs status label) |
| SLO-9 | J5 | outbox event forwarded to remote INBOX **within 30 s** / outbox events published | 99% | 🔧 P4 · deadline-based · approximate |

**Declared (not a numbered SLO):** *last-mile client receive/render* — the true
user-perceived delivery, observational until a prober exists (§8 P6). See §2.

Percentiles behind the bounds stay as **diagnostic** dashboard signals (p99
publication age, p95 search, oldest-pending federation age), not SLO targets.

---

## 2. J1 — Send a message *(flagship)*

**Journey.** User sends; the message is durably stored and published so every
connected member can see it near-instantly. frontend → `MESSAGES` → gatekeeper
→ `MESSAGES_CANONICAL` → **message-worker** (persist) **and** **broadcast-worker**
(publish fan-out). The two workers consume `MESSAGES_CANONICAL` **independently
and in parallel** — no persisted-then-published ordering.

### Why J1 is split (not one "delivery success")

A "delivered ∧ persisted" SLI cannot be computed from independent counters (no
message-ID join; outcomes land separately). So J1 is **two success ratios plus a
latency ratio**, each its own budget, **not** combined — a combined number still
wouldn't prove the same message did both. A single correlated J1 outcome is
deferred to v2 (§8 P7) and, if built, means `persisted AND published within
bound` (never implying an ordering the architecture lacks).

### Delivery is layered — name each layer honestly

| Layer | Meaning | Status |
|---|---|---|
| **North-star / declared** | recipient received & **rendered** within bound | observational |
| **Enforced v1** | SLO-1a persistence + SLO-1b/SLO-2 channel broadcast publication | this doc |
| **Observational v1.5** | frontend receive-span telemetry | §8 P5 |
| **Protocol-receive prober v2** | loadgen NATS-subscribe prober — proves **protocol receipt**, not render | §8 P6 |
| **Render prober v2** | browser synthetic / RUM — proves decrypt/render/state-apply | §8 P6/P7 |

A NATS PubAck is **not** delivery, and a NATS protocol receive is **not** render:
dashboards can read green while a recipient's client shows nothing. v1 commits only
to persistence + channel publication and names them so; the last mile is split into
a protocol-receive prober (loadgen) and a render prober (browser) — neither is v1.

### Denominator & outcome contract

- **Denominators, `broadcast_path`-scoped:** `messages_canonical_published_total{broadcast_path}`,
  emitted by **message-gatekeeper** at the canonical publish site — upstream of
  both workers, so a dead worker drops the ratio instead of vanishing. The label
  is the **fan-out route**, not the room type — `room_subject` | `thread` | `dm`,
  classified to mirror broadcast-worker's dispatch exactly: `shouldUseThreadFanOut`
  (`ThreadParentMessageID != "" && !TShow`) → `thread` **first**, else channel →
  `room_subject`, else DM/BotDM → `dm`. This matters because a **channel thread
  reply** (`TShow=false`) is a channel room yet routes to per-account thread
  fan-out, **not** `publishChannelEvent` — a `room_type="channel"` denominator
  would count it while no `broadcast_channel_publish_total` fires, wrongly
  depressing SLO-1b/2. (gatekeeper resolves room type at the emit site to set the
  label; `thread`/`dm` slices are v1-unscored.)
  - **SLO-1a** uses the **all-`broadcast_path` total** (persistence covers every message).
  - **SLO-1b/2** use the **`broadcast_path="room_subject"` slice only** — v1
    excludes the `thread`/`dm` routes (see below).
- **Numerators**, one outcome per logical message (approximate — see §0.1):
  - `messages_persisted_total{outcome}` (message-worker) → SLO-1a.
  - `broadcast_channel_publish_total{outcome=ok|failed}` (broadcast-worker) →
    SLO-1b. **A channel broadcast is a single room-subject publish, not a
    per-recipient fan-out.** `publishChannelEvent` does one
    `nc.PublishMsg(subject.RoomEvent(roomID), …)` and NATS fans out to subscribers
    downstream (the code comment: *"one room-stream publish… reports the room
    audience, not per-recipient deliveries"*). So the only channel outcome is
    **`ok`/`failed` of that one publish** — there is **no `all/partial/failed`**.
    **Core-NATS boundary:** `nc.PublishMsg` is fire-and-forget — a nil return
    means *enqueued locally*, **not** server-acknowledged. Per-recipient
    `all/partial/failed` exists only on the **DM/thread** paths (`publishDMEvents`
    loops accounts with a `failed` count), which v1 does not score; those and the
    true per-recipient *delivery* outcome are the **observational last mile**
    (declared) / v2 (§8 P7).
  - `broadcast_channel_publish_age_seconds` = `now − evt.Timestamp` → SLO-2. **Good
    predicate: age ≤ 1 s**; a publication later than the bound is **bad** (stays
    in valid, not counted good). `evt.Timestamp` is the canonical event's
    server-set timestamp (`gatekeeper handler.go`, `now.UnixMilli()`) →
    "canonical acceptance → channel published".
- **v1 scope: the room-subject path only.** DM/thread per-recipient
  partial-failure semantics are deferred; SLO-1b/2 count only
  `broadcast_path="room_subject"` canonical messages in v1 (channel non-thread
  sends), so channel thread replies routed to thread fan-out are excluded from
  both numerator and denominator.
- **Backstop (the real enforcement):** message-worker / broadcast-worker consumer
  lag (§8 P3). Because the counters are core-NATS/approximate, lag is the
  primary stalled-worker signal; the ratios are secondary.

### Measurement

✅ nothing today (top roadmap item). 🔧 enforcement v1 = the counters above,
per-service `metrics.go` (`sdk.Meter()` exists, **no SDK change**; search-service
is the exemplar). 🔧 declared last-mile via P5/P6.

### Caveats

- A publication is **one publish to a room subject, not N recipient deliveries**;
  `UserCount` ≠ connected recipients. Denominators are messages/publications, not
  recipients.
- Ordering is not an SLO (JetStream orders within a stream only).
- `published` denominator excludes gatekeeper-rejected messages per §0.1.

---

## 3. J2 — Named interactive workflows

Three synchronous interactions users block on; each SLI = *completed
successfully within the bound* (§0.1).

| Workflow | Path (verified in code) |
|---|---|
| **Login** | `POST /api/v1/auth` (auth-service) → NATS connect → initial data (`subscription.list`, `rooms.get`) |
| **Enter channel** | `msg.history` → history-service `LoadHistory`: 2 concurrent Mongo reads → Cassandra `messages_by_room`, **bucket-walk** (`cassrepo/walker.go`) |
| **Enter thread** | `msg.thread` → `GetThreadMessages`: **single partition** `thread_messages_by_thread`, no walk |

Separate bounds because the cost models differ (open-ended bucket walk vs one
partition slice). Targets: §1.

### Measurement

- ✅ **Login — auth leg only (proxy).** `good = successful login (2xx) ∧ ≤ 1 s` /
  `valid = eligible login attempts` on auth-service `http.server.request.duration`.
  4xx (bad credentials, etc.) are **excluded from valid**, never counted as good.
  This is a **proxy** for the declared app-entry journey (auth → connect →
  initial data); the connect + initial-data legs are added via prober/spanmetrics
  (v2). Labelled proxy until then.
- 🔧 **Enter channel / thread** — the `natsrouter` metrics middleware (§8 P1):
  `rpc_server_duration_seconds{subject_pattern, errcode_category}`; the
  `subject_pattern` label slices both workflows from one middleware. Eligibility
  per the §0.1 errcode table.
- 🔧 Client-side v1.5: spanmetrics.

### Caveats

- **Bucket-walk tail is structural**: if calibration shows the SLO-4 miss rate
  dominated by walk depth, add a walk-depth metric / tune `MESSAGE_BUCKET_HOURS`
  — don't loosen the SLO.
- Encrypted-room `key.get` failure is user-visible — v2 SLI candidate (§8 P7).

---

## 4. J3 — Notifications

**Journey.** Message arrives for an offline/away member → notification-worker
enqueues a push event → **push-service** (separate service, not in this repo) →
provider → device.

**v1 measures the handoff, not provider delivery.** `notification-worker` only
publishes `PushNotificationEvent` into the `PUSH_NOTIFICATION` stream; the actual
provider retries and terminal delivery outcome live in the downstream
**push-service**, which this repo does not own. So:

- **SLO-6 (enforced v1) — push-stream handoff**: `good = recipient durably
  accepted into PUSH_NOTIFICATION` / `valid = notifiable recipients`
  (policy-suppressed — mutes, quiet hours — are **not** valid).
  **Recipient-granular, both sides.** notification-worker emits **batched** push
  events (one `PushNotificationEvent` carries N accounts, `handler.go`), so the
  numerator counts **recipients**, not events: `push_recipients_accepted_total`
  incremented by `len(batchAccounts)` on a successful emit, matched against
  `push_recipients_total` (denominator). Counting events over recipients would
  mismatch units. **"Accepted" = a JetStream `PubAck`** (the emitter's
  `js.PublishMsg` returns durably, `emit.go`) — a *durable stream ack*, not a
  local enqueue, so unlike SLO-1b there is no core-NATS caveat here.
  `notifications_suppressed_total{reason}` stays diagnostic. §8 P4.
  **Approximate:** the emitter discards the `PubAck` (`emit.go` ignores
  `PubAck.Duplicate`), so a batch redelivery can double-count already-accepted
  recipients. Exact once-per-recipient accounting waits on the outcome ledger
  (§8 P7); until then SLO-6 is lag-backstopped like the other async ratios.
- **Declared (not owned here) — notification delivery**: `delivered to provider
  within retry budget / recipients requested`, one terminal outcome per logical
  notification, retries not changing the unit. This must be emitted by
  **push-service** at recipient granularity — a **cross-repo dependency**, v2.

**Caveat.** v1 stops at the stream boundary; enqueue success does **not** mean the
user was notified. No latency SLO in v1.

---

## 5. J4 — Search

**Journey.** `chat.user.{account}.request.search.{siteId}.*` → search-service
→ Elasticsearch.

- **SLO-7 — availability**: `good = status=ok` / `valid = eligible search
  requests` (4xx — malformed query, unauthorized — excluded per §0.1, never
  counted good).
- **SLO-8 — latency**: `good = successful search returns ≤ 1 s` /
  `valid = successful searches`. **Gated on success** so a fast failure can't
  improve the number.

**Measurement.** ✅ SLO-7 for **partial/elevated failures**
(`search_service_requests_total{kind,status}` — note the endpoint label is
`kind`, not `type`; the ratio catches error responses while traffic flows).
🔧 **SLO-8 is not measurable yet**: the duration histogram
(`search_service_request_duration_seconds`) currently carries only `{kind}`, **no
`status`**, so successful requests can't be isolated — add `status` (giving
`{kind,status}`, §8 P4) before enforcing SLO-8. The unlabelled duration stays
diagnostic.

**Caveats.**
- **Full-outage blind spot.** The denominator is search-service-local, so a total
  outage reads as *no traffic*, not failures (the §0.1 self-emitted-denominator
  trap). SLO-7's ratio therefore covers partial degradation only; a complete
  outage is caught by the **health-check / uptime backstop** (and the synthetic
  prober, §8 P6), not the request ratio. Until the prober lands, pair SLO-7 with
  a request-rate-drop / probe alert.
- Index freshness (how fast a new message becomes searchable) is out of v1 —
  §8 P7.

---

## 6. J5 — Federation

**Journey.** Cross-site events converge site A → site B via `OUTBOX` →
outbox-worker → remote `INBOX` → inbox-worker.

**SLI 9 — Federation forward freshness**: `good = outbox event forwarded to the
destination INBOX within 30 s of publish` / `valid = outbox events published`. A
never-forwarded event (stuck peer) stays in the denominator and never reaches
the numerator → counted as a failure, not a missing sample.

### Outage-safe contract

- **Denominator: `outbox_events_published_total{origin_site, dest_site, event_type}`**,
  emitted **producer-side** (room-service / room-worker at OUTBOX publish) —
  upstream of outbox-worker, so worker downtime can't suppress it.
- **Numerator: forwarded within bound** — `outbox_forwarded_total{origin_site,
  dest_site, event_type}` gated on age ≤ bound. **Same label set as the
  denominator** so the ratio is
  `sum by(origin_site, dest_site)(forwarded_within_bound) / sum by(origin_site, dest_site)(published)`
  — `origin_site`+`dest_site` keep the budget isolated **per peer pair** (a busy
  or failing origin can't dilute another's), `event_type` is carried for slicing
  and aggregated away for the SLO. Age = `now − event.Timestamp`, both timestamps
  origin-side (no cross-site skew).
  **Approximate:** outbox consumers use
  `MaxDeliver=-1` (retry forever — there is no exhaustion terminal event), and
  exact one-time forwarded accounting needs explicit dedup/Ack semantics not yet
  in place. So the ratio is deadline-based and approximate.
- **Primary peer-down signal: a *stalled* backlog, not a growing one.** The
  standard JetStream Prometheus exporter exposes `num_pending` / `num_ack_pending`
  and sequence positions (ack-floor, delivered) — **not** a wall-clock age of the
  oldest pending message. Growth alone is insufficient: a peer that goes down with
  a **single** parked event shows `num_ack_pending=1` and then no further growth,
  yet the event is stuck forever. So v1 enforces on **sustained non-zero
  `num_pending + num_ack_pending` while the ack-floor does not advance** over the
  same window (observable even when outbox-worker is down, since it's
  server-side). A true **oldest-pending-age** gauge requires a **custom monitor**
  that resolves the timestamp of the earliest un-acked stream message (§8 P3) and,
  once it lands, becomes the primary signal — roadmap, not "free from the
  exporter". Either way this backlog/age signal is the enforced peer-down
  indicator; the forwarded-within-bound ratio is secondary — and note the ratio
  only holds a never-forwarded event as a failure **while it is inside the 28-day
  window**; past that it rolls off, so the backlog/age signal is what stays.

**Caveats.**
- **Budget isolated per `{origin_site, dest_site}`** — a dead peer burns only its
  own lane (FIFO parks with `MaxAckPending=1`).
- v1 covers the **OUTBOX relay only** (origin publish → forwarded), not
  direct-INBOX paths or remote *apply* by inbox-worker (v2; remote apply adds
  cross-site clock skew).
- `member_added` rides this pipeline — cross-site half of member-change
  convergence is covered; same-site half is v2 (§8 P7).

---

## 7. Alerting — burn rate, not thresholds

After calibration, alert on error-budget burn rate (multi-window, SRE Workbook
ch.5) — well-defined because every SLO is an event ratio: **page** at 14.4× over
1 h (+5 m), **page** at 6× over 6 h (+30 m), **ticket** at 1× over 3 d. Minimum
sample size gates *alert confidence* only (don't page on a 1/2-event window), never
budget inclusion. A **stalled JetStream backlog** (sustained non-zero
`num_pending`/`num_ack_pending` with a non-advancing ack-floor, §8 P3) — and,
once the custom monitor lands, **oldest-pending age** (§6) — are the leading /
outage-backstop indicators for the async SLOs (1a/1b/2/6/9).

---

## 8. Measurement roadmap

All items land in `newchat` (app code) or infra — **no flywindy/o11y SDK change
required** (`sdk.Meter()` is exposed; search-service is the exemplar).

| P | Work | Unlocks |
|---|---|---|
| P1 | `natsrouter` metrics middleware (`rpc_server_duration_seconds{subject_pattern, errcode_category}`) | SLO-4/5 + dashboards for all non-named RPCs |
| P2 | J1 counters — gatekeeper `messages_canonical_published_total` (upstream denominator), message-worker persisted, broadcast-worker publications + publication-age; terminal-outcome/dedup semantics, no message-ID labels | SLO-1a/1b/2 |
| P3 | NATS/JetStream Prometheus exporter (infra) — consumer `num_pending`/`num_ack_pending` + ack-floor (stalled-backlog signal); **plus a custom monitor** to derive oldest-pending **age** (exporter alone doesn't expose it) | outage backstop for 1a/1b/2/6/9 |
| P4 | notification-worker push-stream handoff (**recipient-granular** accepted/recipients) · **search duration `status` label** (→ `{kind,status}`) · outbox producer-side published + forwarded-within-bound (matching label sets) | SLO-6/8/9 |
| P5 | Collector `spanmetrics` on frontend spans | observational last-mile & J2 client view |
| P6 | **loadgen NATS-subscribe prober** (protocol receipt) + SLO assertion mode (§10) · login→connect→initial-data; sparse-journey floor; SLO-aware load asserts. **Render is out of scope here** — proves protocol receipt, not decrypt/render | protocol-receive last-mile SLI |
| P6b | **browser synthetic / RUM** prober — decrypt/render/state-apply | render-level declared last mile |
| P7 | v2: **exact outcome ledger** (dedup / first-write / exhaustion via a max-delivery **advisory consumer** — makes 1a/1b/6/9 exact instead of approximate) · **push-service** provider delivery metrics (cross-repo) · correlated single-J1 outcome · search index freshness · member-add convergence · encrypted `key.get` · read-receipt convergence | — |

---

## 9. Error budget policy

Placeholder — separate document (`o11y-error-budget-policy.md`) once team
consequences are agreed. Until then: breaches produce tickets and review
visibility only.

---

## 10. Load testing alignment

These SLOs are the **acceptance criteria** for load tests; *capacity* = the load
at which an SLO first breaks. The client surface is NATS (long-lived, stateful,
bidirectional), so the driver is `tools/loadgen` — not k6, whose VU/request model
can't express cross-connection delivery; k6 fits pure-HTTP workflows only.

**Current coverage is `ready / partial / missing`, not "validates"** — the
framework is reusable but does not yet assert against these SLOs:

| SLO | loadgen coverage today | Gap to `ready` |
|---|---|---|
| SLO-1a | **partial** — soak read-back | per-message outcome accounting |
| SLO-1b / 2 | **partial** — E2 stage correlation | channel-scoped `ok`/`failed` + publish-age accounting (protocol-receipt, not render) |
| SLO-4 / 5 | **near-ready** | per-endpoint `good within bound / eligible attempts` accounting |
| SLO-3 (auth) | **missing** — auth is a stub | HTTP login driver |
| SLO-7 / 8 (search) | **missing** | search workload |
| SLO-6 (push) | **missing** | `PUSH_NOTIFICATION` observer + recipient-level correlation/accounting (provider workload is push-service v2, not this boundary) |
| SLO-9 (federation) | **missing** — single-site only | multi-site + peer-down/recovery |

Today loadgen's message max-RPS mode **excludes missing replies/broadcasts from
failures**, its verdict uses successful-sample percentiles + a separate error
rate, and its Prometheus overlay scrapes only loadgen/cAdvisor — **not** the
service recording rules or a NATS exporter.

**Required before "validates":** an **SLO assertion mode** that counts
`eligible`, `good`, and `missing-after-deadline` events (so a dropped
reply/broadcast is a failure, not an excluded sample) and reads SLIs from the
**same production recording rules**, not loadgen-local metrics.

Two distinct thresholds, not one (the "50–70%" and "track this document" rules
were in tension):

- **Hard gate** — the run fails if it can't meet the **actual SLO predicate and
  target** from this document (e.g. SLO-4 = 95% within 500 ms). This is what
  "thresholds track this document" means, and it applies to **latency and
  availability** targets alike.
- **Engineering headroom** — a separately-named, *stricter* guardrail (e.g.
  assert the latency bound at ~50–70% of the prod value) to catch regression
  before it reaches the SLO. A headroom miss warns; only a hard-gate miss fails
  the release. Headroom is a lab-margin device for **latency bounds**, not a
  loosening of availability targets.

Run with `O11Y_ENABLED=true`; once per release, run master-switch on vs off to
re-verify overhead.

Known gaps: federation is single-site only (per-peer FIFO isolation never
load-verified); loadgen bypasses the WebSocket/browser leg (P6 prober should
share its client code).
