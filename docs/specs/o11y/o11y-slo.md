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

- **Every SLI is a ratio**: `good events / valid events`. Availability SLIs
  gate on success; latency SLIs gate on *success **and** completion within a
  stated bound*. **Percentiles are diagnostic only, never SLO targets** — a raw
  percentile over a window has no good/valid ratio, so error budgets and
  burn-rate alerts (§7) cannot be computed from it. (A latency SLO is therefore
  written as "X% of valid requests completed within the bound", not "p99 < B".)
- **Window**: 28-day rolling, all SLOs.
- **Time-slice accounting**: the window is divided into **5-minute slices**.
  - A slice with **≥ `MIN_SAMPLES` valid events** is **good** if ≥ the SLO's
    stated share meet the SLI criterion, else **bad**.
  - A slice with **< `MIN_SAMPLES` valid events** is **`insufficient_data`** —
    **excluded from the denominator** (counts as neither good nor bad). It never
    inflates compliance.
  - **SLO = good_slices / (good_slices + bad_slices)** over the window.
  - `MIN_SAMPLES` starts at **20** per 5-min slice (calibratable). Low-traffic
    journeys (notifications, federation) will spend most slices in
    `insufficient_data`; a scheduled **synthetic probe** (§8 P6) provides a
    traffic floor so those journeys still accumulate signal.
- **Rounding**: shares ≥ 0.1% precision; latency bounds up to nearest 50 ms.
- **Scope: per site** — each site is its own failure domain and budget.
  Federation (J5) is measured separately and isolated by `{origin, dest}` site.
- **Error-budget eligibility** — what counts as a *failed* valid event is
  decided by whether the request was legitimate and *expected to succeed*, not
  by wire category alone:

  | `errcode` (server) / HTTP | Burns budget? | Why |
  |---|---|---|
  | `internal`, `unavailable`, `too_many_requests` (429), server-side timeout | **Yes** | our fault: bug, overload, capacity rejection |
  | `bad_request`, `unauthenticated`, `forbidden`, `not_found`, `conflict` (4xx) | No | legitimate user/client outcome, not a reliability failure |

  Excluded rows are removed from *valid events* entirely (not counted as
  failures).

### 0.2 Calibration

All targets are **achievable-first starting values**. Run observationally for
4–6 weeks, then adjust and seek approval. No paging alerts before then.

### 0.3 SLO ≠ SLA

**This document is internal.** If numbers are ever published externally they
become an SLA, which must be *derived*, not copied: one notch looser than the
internal SLO, availability-focused, with exclusions/remedies defined with
business/legal, and only after calibration data exists. Never publish these
numbers as-is.

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

| # | Journey | SLI (good / valid) | Target | Today |
|---|---|---|---|---|
| SLO-1a | J1 | canonical message persisted to Cassandra / eligible canonical messages | 99.9% good slices | 🔧 P2 |
| SLO-1b | J1 | broadcast publication accepted by NATS / eligible publications | 99.9% good slices | 🔧 P2 |
| SLO-2 | J1 | publication accepted **within 1 s** of canonical acceptance / eligible publications | 99% good slices | 🔧 P2 |
| SLO-3 | J2 | login succeeds within 1 s / login attempts | 99% in-slice · 99.9% slices | ✅ (auth leg — proxy) |
| SLO-4 | J2 | channel load succeeds within 500 ms / channel loads | 95% in-slice · 99.9% slices | 🔧 P1 |
| SLO-5 | J2 | thread open succeeds within 300 ms / thread opens | 99% in-slice · 99.9% slices | 🔧 P1 |
| SLO-6 | J3 | notification handed to provider / notifications attempted | 99.5% good slices | 🔧 P4 |
| SLO-7 | J4 | search returns ok / search requests | 99.5% good slices | ✅ |
| SLO-8 | J4 | **successful** search returns within 1 s / **successful** searches | 95% in-slice · 99.9% slices | ✅ |
| SLO-9 | J5 | outbox event forwarded to remote INBOX within 30 s / all outbox events | 99% good slices | 🔧 P4 |

**Declared (not a numbered SLO):** *last-mile client receive/render* — the
true user-perceived delivery. Observational only until a synthetic prober
exists; enforcement candidate v2 (§8 P6). See §2.

Percentiles behind the bounds are kept as **diagnostic** signals on dashboards
(e.g. p99 publication age, p95 search) but are not the SLO.

---

## 2. J1 — Send a message *(flagship)*

**Journey.** User sends; the message is durably stored and published so every
connected member can see it near-instantly. frontend → `MESSAGES` → gatekeeper
→ `MESSAGES_CANONICAL` → **message-worker** (persist to Cassandra) **and**
**broadcast-worker** (publish fan-out). The two workers consume
`MESSAGES_CANONICAL` **independently and in parallel** — there is no
persisted-then-published ordering.

### Why J1 is split (not one "delivery success")

A single "delivered ∧ persisted" SLI **cannot be computed** from independent
Prometheus counters: they can't be joined by message ID (and message ID must
never be a metric label — unbounded cardinality), and the two outcomes land in
different slices. So J1 is measured as **two independent ratios plus a latency
ratio**, each with its own denominator and budget. They are **not** combined
into an overall "delivery" number — that would still not prove the same message
did both. A single correlated J1 budget is deferred to v2 (§8 P7) and, if built,
must mean `persisted AND publication observed within bound` — never implying an
ordering the architecture doesn't guarantee.

### Delivery is layered — name each layer honestly

| Layer | What it means | Status |
|---|---|---|
| **North-star / declared** | recipient received & rendered within bound | declared SLI, observational |
| **Enforced v1** | SLO-1a persistence + SLO-1b/SLO-2 broadcast publication | this doc |
| **Observational v1.5** | frontend receive-span telemetry (spanmetrics) | §8 P5 |
| **Enforcement candidate v2** | synthetic prober — true last mile | §8 P6 |

A NATS PubAck is **not** delivery: dashboards can read green while a recipient
receives nothing. v1 therefore commits only to persistence + publication and
names them as such.

### SLIs & targets — see §1 (SLO-1a / 1b / 2)

- **SLO-1a — Message persistence success**: `good = canonical message written
  to Cassandra` / `valid = eligible canonical messages`.
- **SLO-1b — Broadcast publication success**: `good = fan-out publication
  accepted by NATS` / `valid = eligible broadcast publications`.
- **SLO-2 — Broadcast publication latency**: `good = publication accepted ≤ 1 s
  after canonical acceptance` / same denominator as 1b. Measured
  `now − canonicalEvent.Timestamp` at the publish site — the canonical event's
  server-set timestamp (`gatekeeper handler.go`, `now.UnixMilli()`), read by
  broadcast-worker as `evt.Timestamp`. Both ends are same-site server clocks.

### Measurement

- ✅ Today: nothing — specified but not enforceable (top roadmap item).
- 🔧 **Enforcement, v1** — per-service `metrics.go` (copy the search-service /
  data-migration pattern; `sdk.Meter()` already exposed, **no SDK change**):

| Instrument | Where |
|---|---|
| `messages_validated_total` / `messages_rejected_total{reason}` | message-gatekeeper |
| `messages_persisted_total{outcome}` | message-worker → SLO-1a |
| `broadcast_publications_total{outcome}` | broadcast-worker → SLO-1b |
| `broadcast_publication_age_seconds` (`now − evt.Timestamp`) | broadcast-worker → SLO-2 |

- 🔧 Observational v1.5 / v2: frontend spanmetrics (§8 P5), synthetic prober
  (§8 P6) — feed the *declared* last-mile SLI.

### Caveats

- **A publication is one publish to a room subject, not N recipient
  deliveries.** `UserCount` is not connected-recipient count. v1 denominators
  are *messages / publications*, not recipients — do not read SLO-1b as
  "everyone received it".
- Ordering is not an SLO (JetStream orders within a stream only).
- `eligible` excludes gatekeeper-rejected messages per §0.1.

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
partition slice). Targets: §1 (SLO-3/4/5).

### Measurement

- ✅ **Login — auth leg only (proxy).** auth-service
  `http.server.request.duration` gives `good = status<500 ∧ ≤ 1s`. This is a
  **proxy** for the declared app-entry journey (auth → connect → initial data);
  the connect + initial-data legs are client-side, added via prober/spanmetrics
  (v2). SLO-3 is labelled proxy until then.
- 🔧 **Enter channel / thread** — blocked on the `natsrouter` metrics
  middleware (§8 P1): `rpc_server_duration_seconds{subject_pattern,
  errcode_category}`; the `subject_pattern` label slices both workflows from one
  middleware. Budget eligibility per the §0.1 errcode table.
- 🔧 Client-side v1.5: spanmetrics.

### Caveats

- **Bucket-walk tail is structural**: a long-idle room's first load walks more
  buckets. If calibration shows the SLO-4 miss rate dominated by walk depth, add
  a walk-depth metric / tune `MESSAGE_BUCKET_HOURS` — don't loosen the SLO.
- Encrypted-room `key.get` failure is user-visible — v2 SLI candidate (§8 P7).

---

## 4. J3 — Notifications

**Journey.** Message arrives for an offline/away member → notification-worker →
push provider.

**SLI 6.** `good = notification successfully handed to the provider` /
`valid = notifications attempted`. Explicit instrument contract:

| Instrument | Meaning |
|---|---|
| `notifications_attempted_total` | **denominator** — one per provider hand-off attempt, **including retries** (each retry is a new attempt); **excludes** policy-suppressed |
| `notifications_sent_total` | successful provider hand-offs (the numerator) |
| `notifications_failed_total{reason}` | unsuccessful attempts, by reason |
| `notifications_suppressed_total{reason}` | policy-suppressed (mutes, quiet hours) — **not** valid events |

Target: §1 (SLO-6). §8 P4.

**Caveat.** Measures to provider hand-off; provider→device is out of scope. No
latency SLO in v1.

---

## 5. J4 — Search

**Journey.** `chat.user.{account}.request.search.{siteId}.*` → search-service
→ Elasticsearch.

- **SLO-7 — availability**: `good = status=ok` / `valid = all search requests`.
- **SLO-8 — latency**: `good = successful search returns ≤ 1 s` /
  `valid = **successful** searches`. **Latency is gated on success** so a fast
  failure cannot improve the number (an ungated p95 rewards fast-failing).

**Measurement.** ✅ Fully measurable today: `search_service_requests_total
{type,status}`, `search_service_request_duration_seconds`
(`…es_duration_seconds` is diagnostic). The duration histogram needs a
`status`/outcome label to gate SLO-8; add if absent.

**Caveat.** Index freshness (how fast a new message becomes searchable) is
deliberately out of v1 — §8 P7.

---

## 6. J5 — Federation

**Journey.** Cross-site events converge site A → site B via `OUTBOX` →
outbox-worker → remote `INBOX` → inbox-worker.

**SLO-9 — Federation forward freshness**: `good = outbox event forwarded to the
destination INBOX within 30 s of its publish time` / `valid = all outbox
events`. Framed as a **ratio, not a p99**, so an event that is **never**
forwarded (a stuck peer) counts as a failure in the denominator instead of
silently producing no sample.

**Measurement.** ✅ nothing today; 🔧 in outbox-worker (§8 P4):
`outbox_forwarded_total{dest_site, event_type}` gated on age ≤ bound,
`outbox_events_total{dest_site, event_type}` (denominator),
`outbox_retries_total{dest_site}`. Age = `now − event.Timestamp`, both
timestamps on the origin site (no cross-site clock skew). A companion
`outbox_forward_age_seconds` histogram is kept diagnostic.

**Caveats.**
- **Budget isolated per `{origin_site, dest_site}`** — a dead peer burns only
  its own lane's budget (its FIFO parks with `MaxAckPending=1`), and the
  per-destination forward-success ratio is the primary peer-down alert signal.
- v1 covers the **OUTBOX relay only** (origin publish → forwarded). It does not
  cover direct-INBOX publication paths or remote *apply* by inbox-worker; those
  are v2 refinements (remote apply adds cross-site clock skew).
- `member_added` rides this pipeline — the cross-site half of member-change
  convergence is covered; same-site half is v2 (§8 P7).

---

## 7. Alerting — burn rate, not thresholds

After calibration, alert on error-budget burn rate (multi-window, SRE Workbook
ch.5), now well-defined because every SLO is a good/valid ratio: **page** at
14.4× over 1 h (+5 m), **page** at 6× over 6 h (+30 m), **ticket** at 1× over
3 d. NATS/JetStream consumer lag (§8 P3) is the leading indicator for
SLO-1a/1b/2/6/9 — alert on lag growth before budget burns.

---

## 8. Measurement roadmap

All items land in `newchat` (app code) or infra — **no flywindy/o11y SDK change
required** (`sdk.Meter()` is exposed; search-service is the working exemplar).

| P | Work | Unlocks |
|---|---|---|
| P1 | `natsrouter` metrics middleware (`rpc_server_duration_seconds{subject_pattern, errcode_category}`) | SLO-4/5 + dashboards for all non-named RPCs |
| P2 | J1 counters: persisted / publications / publication-age (gatekeeper, message-worker, broadcast-worker) — no message-ID labels | SLO-1a/1b/2 |
| P3 | NATS/JetStream Prometheus exporter (infra) | consumer-lag leading indicator |
| P4 | notification-worker (attempted/sent/failed/suppressed) + outbox-worker (forwarded-within-bound / events / retries) | SLO-6/9 |
| P5 | Collector `spanmetrics` on frontend spans | observational last-mile & J2 (client view) |
| P6 | Synthetic prober from `tools/loadgen` | declared last-mile SLI; full login→connect→initial-data |
| P7 | v2 candidates: correlated single-J1 outcome (`persisted AND published within bound`) · search index freshness · member-add convergence · encrypted `key.get` success · unread-badge / read-receipt convergence | — |

---

## 9. Error budget policy

Placeholder — separate document (`o11y-error-budget-policy.md`) once team
consequences are agreed. Until then: breaches produce tickets and review
visibility only.

---

## 10. Load testing alignment

These SLOs are the **acceptance criteria** for load tests; *capacity* = the
load at which an SLO first breaks. The client surface is NATS (long-lived,
stateful, bidirectional), so the driver is `tools/loadgen` (already asserts
per-stage P95/P99 in `attribution.go`, tracks consumer lag) — not k6, whose
VU/request model can't express cross-connection delivery; k6 fits the pure-HTTP
workflows only.

| Scenario | Driver | Validates |
|---|---|---|
| Message pipeline, fan-out matrix (room sizes × rate ladder) | `loadgen` | SLO-1a/1b/2 |
| Channel-load storm incl. cold/idle-room fixtures | `loadgen history` | SLO-4 |
| Thread-open storm | `loadgen thread` | SLO-5 |
| Login surge | k6 or simple HTTP driver | SLO-3 |
| Federation under load + peer-down/recovery | *missing — to build* | SLO-9 |
| Soak: redelivery, lag drift, memory | `loadgen` + P3 exporter | leading indicators |

Rules: assert at ~50–70% of the prod bound (lab margin) · loadgen thresholds
must track this document (drift = bug) · run with `O11Y_ENABLED=true` and read
SLIs from the same recording rules as prod (loadgen itself uses a noop tracer) ·
once per release, run master-switch on vs off to re-verify the overhead claims.

Known gaps: federation scenario (per-peer FIFO isolation never load-verified);
loadgen bypasses the WebSocket/browser leg (P6 prober should share its client
code).
