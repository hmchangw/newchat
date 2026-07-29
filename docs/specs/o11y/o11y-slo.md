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
  per attempt — deduped at the emit site (never a message-ID label);
  `MaxDeliver` exhaustion (poison) is a terminal **failure**.
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
  request was legitimate and *expected to succeed*, not by wire category alone:

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
| SLO-1b | J1 | **channel** broadcast fan-out published (**good = `outcome=all`**; partial/failed both burn) / canonical channel messages published | 99.9% | 🔧 P2 · approximate · core-NATS boundary |
| SLO-2 | J1 | channel fan-out published **within 1 s** of canonical acceptance (late = bad) / canonical channel messages published | 99% | 🔧 P2 · approximate |
| SLO-3 | J2 | successful login **within 1 s** / eligible login attempts | 99% | ✅ (auth leg — proxy) |
| SLO-4 | J2 | channel load succeeds **within 500 ms** / eligible channel loads | 95% | 🔧 P1 |
| SLO-5 | J2 | thread open succeeds **within 300 ms** / eligible thread opens | 99% | 🔧 P1 |
| SLO-6 | J3 | push event accepted into `PUSH_NOTIFICATION` / notifiable recipients | 99.9% | 🔧 P4 · handoff only (see §4) |
| SLO-7 | J4 | search returns ok / search requests | 99.5% | ✅ partial-failure · outage needs backstop (§5) |
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
| **North-star / declared** | recipient received & rendered within bound | observational |
| **Enforced v1** | SLO-1a persistence + SLO-1b/SLO-2 broadcast publication | this doc |
| **Observational v1.5** | frontend receive-span telemetry | §8 P5 |
| **Enforcement candidate v2** | synthetic prober — true last mile | §8 P6 |

A NATS PubAck is **not** delivery: dashboards can read green while a recipient
gets nothing. v1 commits only to persistence + publication and names them so.

### Denominator & outcome contract

- **Denominator (both 1a and 1b/2): `messages_canonical_published_total`**,
  emitted by **message-gatekeeper** at the canonical publish site — upstream of
  both workers, so a dead worker drops the ratio instead of vanishing.
- **Numerators**, one outcome per logical message (approximate — see §0.1):
  - `messages_persisted_total{outcome}` (message-worker) → SLO-1a
  - `broadcast_fanout_total{outcome=all|partial|failed}` (broadcast-worker) →
    SLO-1b. **Good predicate: `outcome=all` only** — `partial` and `failed` both
    burn budget (any recipient the message didn't reach is a miss). **Core-NATS
    boundary:** broadcast-worker publishes via `nc.PublishMsg` (core NATS,
    fire-and-forget) — a nil return means the publish call was *enqueued locally*,
    **not** server-acknowledged. And fan-out is **per recipient** (channel emits a
    room event + one user-room event per account; DM/thread emit per recipient),
    tolerating partial failure. So the outcome is a **logical all/partial/failed
    per message**, not a single ack.
  - `broadcast_fanout_age_seconds` = `now − evt.Timestamp` → SLO-2. **Good
    predicate: age ≤ 1 s**; a publication later than the bound is **bad** (stays
    in valid, not counted good). `evt.Timestamp` is the canonical event's
    server-set timestamp (`gatekeeper handler.go`, `now.UnixMilli()`) →
    "canonical acceptance → fan-out published".
- **v1 scope: channel messages only.** DM/thread per-recipient partial-failure
  semantics are deferred; SLO-1b/2 denominators count canonical **channel**
  messages in v1.
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

- **SLO-7 — availability**: `good = status=ok` / `valid = all search requests`.
- **SLO-8 — latency**: `good = successful search returns ≤ 1 s` /
  `valid = successful searches`. **Gated on success** so a fast failure can't
  improve the number.

**Measurement.** ✅ SLO-7 for **partial/elevated failures**
(`search_service_requests_total{type,status}` — the ratio catches error responses
while traffic flows). 🔧 **SLO-8 is not measurable yet**:
`search_service_request_duration_seconds` has no status/outcome label, so
successful requests can't be isolated — add that label (§8 P4) before enforcing
SLO-8. `…es_duration_seconds` stays diagnostic.

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

- **Denominator: `outbox_events_published_total{dest_site, event_type}`**, emitted
  **producer-side** (room-service / room-worker at OUTBOX publish) — upstream of
  outbox-worker, so worker downtime can't suppress it.
- **Numerator: forwarded within bound** — `outbox_forwarded_total{dest_site,
  event_type}` gated on age ≤ bound. **Same label set as the denominator** so the
  ratio is `sum by(dest_site)(forwarded_within_bound) / sum by(dest_site)(published)`
  — event_type is carried for slicing and aggregated away for the SLO. Age =
  `now − event.Timestamp`, both timestamps origin-side (no cross-site skew).
  **Approximate:** outbox consumers use
  `MaxDeliver=-1` (retry forever — there is no exhaustion terminal event), and
  exact one-time forwarded accounting needs explicit dedup/Ack semantics not yet
  in place. So the ratio is deadline-based and approximate.
- **Primary peer-down signal: oldest-pending age** — derived from **NATS
  consumer state** (`num_pending` / oldest un-acked) via the JetStream exporter
  (§8 P3), **not** a worker-emitted gauge, so it stays observable when
  outbox-worker itself is down. This gauge is the enforced peer-down signal;
  the forwarded-within-bound ratio is secondary.

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
budget inclusion. **NATS/JetStream consumer lag** (§8 P3) and **oldest-pending
age** (§6) are the leading / outage-backstop indicators for the async SLOs
(1a/1b/2/6/9).

---

## 8. Measurement roadmap

All items land in `newchat` (app code) or infra — **no flywindy/o11y SDK change
required** (`sdk.Meter()` is exposed; search-service is the exemplar).

| P | Work | Unlocks |
|---|---|---|
| P1 | `natsrouter` metrics middleware (`rpc_server_duration_seconds{subject_pattern, errcode_category}`) | SLO-4/5 + dashboards for all non-named RPCs |
| P2 | J1 counters — gatekeeper `messages_canonical_published_total` (upstream denominator), message-worker persisted, broadcast-worker publications + publication-age; terminal-outcome/dedup semantics, no message-ID labels | SLO-1a/1b/2 |
| P3 | NATS/JetStream Prometheus exporter (infra) — consumer lag **and** oldest-pending age from consumer state | outage backstop for 1a/1b/2/6/9 |
| P4 | notification-worker push-stream handoff (**recipient-granular** accepted/recipients) · **search duration `status` label** · outbox producer-side published + forwarded-within-bound (matching label sets) | SLO-6/8/9 |
| P5 | Collector `spanmetrics` on frontend spans | observational last-mile & J2 client view |
| P6 | Synthetic prober from `tools/loadgen` + SLO assertion mode (§10) | declared last-mile SLI; login→connect→initial-data; sparse-journey floor; SLO-aware load asserts |
| P7 | v2: **exact outcome ledger** (dedup / first-write / exhaustion — makes 1a/1b/6/9 exact instead of approximate) · **push-service** provider delivery metrics (cross-repo) · correlated single-J1 outcome · search index freshness · member-add convergence · encrypted `key.get` · read-receipt convergence | — |

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
| SLO-1b / 2 | **partial** — E2 stage correlation | per-endpoint all/partial/failed + channel scope |
| SLO-4 / 5 | **near-ready** | per-endpoint `good within bound / eligible attempts` accounting |
| SLO-3 (auth) | **missing** — auth is a stub | HTTP login driver |
| SLO-7 / 8 (search) | **missing** | search workload |
| SLO-6 (push) | **missing** | provider/push workload |
| SLO-9 (federation) | **missing** — single-site only | multi-site + peer-down/recovery |

Today loadgen's message max-RPS mode **excludes missing replies/broadcasts from
failures**, its verdict uses successful-sample percentiles + a separate error
rate, and its Prometheus overlay scrapes only loadgen/cAdvisor — **not** the
service recording rules or a NATS exporter.

**Required before "validates":** an **SLO assertion mode** that counts
`eligible`, `good`, and `missing-after-deadline` events (so a dropped
reply/broadcast is a failure, not an excluded sample) and reads SLIs from the
**same production recording rules**, not loadgen-local metrics. Rules: assert at
~50–70% of the prod bound (lab margin); thresholds track this document (drift =
bug); run with `O11Y_ENABLED=true`; once per release, run master-switch on vs off
to re-verify overhead.

Known gaps: federation is single-site only (per-peer FIFO isolation never
load-verified); loadgen bypasses the WebSocket/browser leg (P6 prober should
share its client code).
