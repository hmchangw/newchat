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
section: **SLI specification** (user-language intent, good/valid ratio —
written against real user experience even where not yet measurable) →
**SLO target** → **Measurement** (✅ computable today / 🔧 to build; the
**enforcement** implementation is marked) → caveats.

### 0.1 Conventions

- **Window**: 28-day rolling, all SLOs.
- **Ratio SLOs — time-slice accounting**: 5-minute slices; a slice is *good*
  if ≥ the stated share of its valid events meet the criterion, or it has no
  valid events. SLO = share of good slices. (Low off-peak traffic makes pure
  event ratios unfair; same minutes-below-threshold model as Slack SDI.)
- **Workflow SLIs combine success and speed** (*"completed successfully within
  the bound"*) — to the user, a failure and a 10-second success are the same.
- **Percentiles are chosen per SLI, not uniformly p99.** Criteria: how often a
  user performs the action (frequent → p99), event volume (sparse data → p95
  is statistically saner), and whether the tail *is* the signal (then p99 is
  mandatory — e.g. federation, where the tail means a stuck peer).
- **Rounding**: ratios ≥ 0.1% precision; latency bounds up to nearest 50 ms.
- **Scope: per site** — each site is its own failure domain and budget.
  Federation (J5) is measured separately.
- **Client errors never burn budget**: `errcode` categories other than
  `internal`, and HTTP 4xx, are excluded from valid events.

### 0.2 Calibration

All targets are **achievable-first starting values**. Run observationally for
4–6 weeks, then adjust and seek approval. No paging alerts before then.

### 0.3 SLO ≠ SLA

**This document is internal.** If numbers are ever published externally they
become an SLA, which must be *derived*, not copied: one notch looser than the
internal SLO (breach buffer — internal miss must not equal contractual
breach), availability-focused (external latency-percentile commitments are
rare industry-wide), with exclusions/remedies defined with business/legal, and
only after calibration data exists. Never publish these numbers as-is.

---

## 1. Journeys & SLOs at a glance

| Rank | Journey | One-liner |
|---|---|---|
| J1 | **Send a message** | The product: accepted, durably stored, delivered in real time |
| J2 | **Named interactive workflows** | Login, enter channel, enter thread — what users block on |
| J3 | **Notifications** | Offline users still learn about messages |
| J4 | **Search** | Find rooms and messages |
| J5 | **Federation** | Cross-site state converges quickly |

All nine SLOs (targets are starting values, §0.2). Deliberately **out of
scope**, in three tiers:

- **Dashboard-only interactive RPCs** (room create, member ops, uploads,
  pins/reactions, …): monitored via the same instrumentation, no SLO in v1.
- **Ephemeral signals — typing indicators, presence**: best-effort *by
  design*; loss is invisible or self-heals. Under load these are the **first
  traffic to shed** to protect J1. Never SLO material.
- **Read receipts / unread badges**: persisted state users notice when wrong —
  dashboard in v1, promoted to a v2 candidate (§8 P7: unread-badge
  convergence).

| # | Journey | SLI | Criterion | Target | Measurable today |
|---|---|---|---|---|---|
| SLO-1 | J1 | Delivery success | delivered ∧ persisted, per delivery | 99.9% of slices good | 🔧 P2 |
| SLO-2 | J1 | E2E delivery latency | send → broadcast delivered | **p99** < 1 s | 🔧 P2 |
| SLO-3 | J2 | Login (app entry) | success ≤ 1 s | 99% in-slice; 99.9% of slices | ✅ (auth leg) |
| SLO-4 | J2 | Enter channel | success ≤ 500 ms | **95%** in-slice; 99.9% of slices | 🔧 P1 |
| SLO-5 | J2 | Enter thread | success ≤ 300 ms | 99% in-slice; 99.9% of slices | 🔧 P1 |
| SLO-6 | J3 | Notification delivery | handed to provider | 99.5% of slices good | 🔧 P4 |
| SLO-7 | J4 | Search availability | status=ok | 99.5% of slices good | ✅ |
| SLO-8 | J4 | Search latency | — | **p95** < 1 s | ✅ |
| SLO-9 | J5 | Federation freshness | publish → forwarded to remote INBOX | **p99** < 30 s | 🔧 P4 |

Percentile choices: SLO-2 p99 (high volume, tail = group-chat pain); SLO-4
95% (bucket-walk tail is structural — start looser, tighten with data); SLO-5
99% (single partition, no excuse); SLO-8 p95 (tolerant journey); SLO-9 p99
(tail is the stuck-peer signal).

---

## 2. J1 — Send a message *(flagship)*

**Journey.** User sends; every connected member sees it near-instantly; the
message survives reloads. frontend → `MESSAGES` → gatekeeper →
`MESSAGES_CANONICAL` → message-worker (Cassandra) + broadcast-worker (fan-out).

**SLIs.**
- **SLI 1 — Delivery success**: `good = delivery published ∧ message
  persisted` / `valid = accepted messages × recipients at broadcast time`.
  Broadcast-but-not-persisted counts as failure — integrity is non-negotiable
  in chat (cf. [Ably's Four Pillars](https://ably.com/four-pillars-of-dependability)).
- **SLI 2 — E2E delivery latency**: spec is sender-hits-enter → recipient
  renders.

**Measurement.**
- ✅ Today: nothing — J1 is *specified but not enforceable* (top roadmap item).
- 🔧 **Server-side (enforcement, v1)** — per-service `metrics.go`, copying the
  data-migration counter pattern:

| Instrument | Where |
|---|---|
| `messages_validated_total` / `messages_rejected_total{reason}` | message-gatekeeper |
| `deliveries_total{outcome}` + `fanout_size` histogram | broadcast-worker |
| `messages_persisted_total{outcome}` | message-worker |
| `message_e2e_delivery_seconds` (= `now − event.Timestamp` at publish) | broadcast-worker |

- 🔧 Client-side (observational, v1.5): collector `spanmetrics` over existing
  frontend spans — zero frontend change; needs 100% sampling, untrusted
  browser clocks.
- 🔧 Synthetic prober (v2): resident canary from `tools/loadgen`; covers the
  true last mile; candidate enforcement source for SLO-2.

**Caveats.**
- Denominator is **per delivery**, not per message — a 500-member room failure
  weighs 500× (big-room incidents must not average away).
- v1 measures to broadcast publish, not client render; gap tracked (§8 P6).
- Clock skew (same-site NTP ≲10 ms) is negligible vs a 1 s bound.
- Ordering is not an SLO (JetStream orders within a stream only).

---

## 3. J2 — Named interactive workflows

The three synchronous interactions users block on, each a named workflow
(Slack-SDI style); SLI = *completed successfully within the bound* (§0.1).

| Workflow | Path (verified in code) |
|---|---|
| **Login (app entry)** | `POST /api/v1/auth` (auth-service) → NATS connect → initial data (`subscription.list`, `rooms.get`) |
| **Enter channel** | `msg.history` → history-service `LoadHistory`: 2 concurrent Mongo reads → Cassandra `messages_by_room`, **bucket-walk** (`cassrepo/walker.go`). `markRoomRead` / `key.get` follow, non-blocking |
| **Enter thread** | `msg.thread` → `GetThreadMessages`: **single partition** `thread_messages_by_thread`, no walk |

Separate bounds because the cost models differ: channel loads walk an
open-ended number of 72 h buckets (idle rooms walk further); thread loads are
one partition slice.

**SLIs.** Login attempts / channel loads / thread opens completing
successfully within the bound (bad credentials etc. are not valid events).
Targets: see §1 table (SLO-3/4/5).

**Measurement.**
- ✅ Login: auth-service `http.server.request.duration` — enforceable today
  (`status<500 ∧ duration ≤ 1s`; verify series/label names on a live `:2112`).
- 🔧 Channel/thread: blocked on the `natsrouter` metrics middleware (§8 P1) —
  `rpc_server_duration_seconds{subject_pattern, errcode_category}`; the
  `subject_pattern` label slices both workflows out of one middleware.
- 🔧 Client-side v1.5: same spanmetrics route as J1.

**Caveats.**
- Bucket-walk tail is structural: if calibration shows p99 dominated by walk
  depth, add a walk-depth metric / tune `MESSAGE_BUCKET_HOURS` — don't loosen
  the SLO.
- **App entry ≠ client cold start.** The SLO-3 spec covers the *backend legs*
  of entering the app (auth → connect → initial data). v1 measures the auth
  HTTP leg; the `subscription.list` / `rooms.get` legs join under the same SLO
  once P1 lands. Full cold start (bundle download, JS boot, render/TTI)
  depends on device and network the backend cannot fix — it is a **client-side
  product performance metric** (P5 RUM), not a backend SLO.
- Encrypted-room `key.get` failure is user-visible — v2 SLI candidate (§8 P7).

---

## 4. J3 — Notifications

**Journey.** Message arrives for an offline/away member → notification-worker
→ push provider.

**SLI 6**: `good = push handed to provider` / `valid = attempts`
(policy-suppressed — mutes, quiet hours — are not valid events). Target: §1.

**Measurement.** ✅ nothing today; 🔧 `notifications_sent_total` /
`_failed_total{reason}` / `_suppressed_total{reason}` (§8 P4).

**Caveat.** Measures to provider hand-off; provider→device is out of scope.
No latency SLO in v1.

---

## 5. J4 — Search

**Journey.** `chat.user.{account}.request.search.{siteId}.*` → search-service
→ Elasticsearch.

**SLI 7 / SLI 8**: ok-response ratio; fast-response ratio. Targets: §1.

**Measurement.** ✅ Fully measurable today:
`search_service_requests_total{type,status}`,
`search_service_request_duration_seconds` (`…es_duration_seconds` is
diagnostic, not an SLI). Availability and latency stay separate here (existing
counters/histogram don't share labels; merging isn't worth re-instrumenting).

**Caveat.** Index freshness (how fast a new message becomes searchable) is
deliberately out of v1 — §8 P7.

---

## 6. J5 — Federation

**Journey.** Cross-site events converge site A → site B via `OUTBOX` →
outbox-worker → remote `INBOX` → inbox-worker.

**SLI 9 — Federation freshness**: `good = forwarded to destination INBOX
within threshold of publish time` / `valid = all outbox events`. Target: §1.

**Measurement.** ✅ nothing today; 🔧 `outbox_forward_age_seconds{dest_site,
event_type}` (both timestamps on the origin site — no cross-site skew) +
`outbox_forwarded_total` / `outbox_retries_total{dest_site}` (§8 P4).

**Caveats.**
- v1 measures origin-side (publish → forwarded); remote apply is a v2
  refinement (cross-site clock skew).
- Per-destination: a dead peer burns only its own destination's budget (its
  FIFO lane parks with `MaxAckPending=1`); forward-age p99 doubles as the
  peer-down alert signal.
- `member_added` rides this pipeline — the cross-site half of member-change
  convergence is covered here; same-site half is v2 (§8 P7).

---

## 7. Alerting — burn rate, not thresholds

After calibration, alert on error-budget burn rate (multi-window, SRE
Workbook ch.5): **page** at 14.4× over 1 h (+5 m), **page** at 6× over 6 h
(+30 m), **ticket** at 1× over 3 d. NATS/JetStream consumer lag (§8 P3) is the
leading indicator for SLO-1/2/6/9 — alert on lag growth before budget burns.

---

## 8. Measurement roadmap

| P | Work | Unlocks |
|---|---|---|
| P1 | `natsrouter` metrics middleware (`rpc_server_duration_seconds{subject_pattern, errcode_category}`) | SLO-4/5 + dashboards for all non-named RPCs |
| P2 | J1 counters + `message_e2e_delivery_seconds` (gatekeeper / broadcast / message-worker) | SLO-1/2 |
| P3 | NATS/JetStream Prometheus exporter (infra) | consumer-lag leading indicator |
| P4 | notification-worker + outbox-worker instruments | SLO-6/9 |
| P5 | Collector `spanmetrics` on frontend spans | client-side observational SLIs |
| P6 | Synthetic prober from `tools/loadgen` | last-mile SLO-2; full login→connect |
| P7 | v2 candidates: search index freshness · member-add convergence (invite → subscription + room key; cross-site half already under SLO-9) · encrypted `key.get` success · unread-badge / read-receipt convergence | — |

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
VU/request model can't express cross-connection E2E delivery; k6 fits the
pure-HTTP workflows only.

| Scenario | Driver | Validates |
|---|---|---|
| Message pipeline, fan-out matrix (room sizes × rate ladder) | `loadgen` | SLO-1/2 |
| Channel-load storm incl. cold/idle-room fixtures | `loadgen history` | SLO-4 |
| Thread-open storm | `loadgen thread` | SLO-5 |
| Login surge | k6 or simple HTTP driver | SLO-3 |
| Federation under load + peer-down/recovery | *missing — to build* | SLO-9 |
| Soak: redelivery, lag drift, memory | `loadgen` + P3 exporter | leading indicators |

Rules: assert at ~50–70% of the prod bound (lab margin) · loadgen thresholds
must track this document (drift = bug) · run with `O11Y_ENABLED=true` and read
SLIs from the same recording rules as prod (loadgen itself uses a noop
tracer) · once per release, run master-switch on vs off to re-verify the
overhead claims.

Known gaps: federation scenario (per-peer FIFO isolation never
load-verified); loadgen bypasses the WebSocket/browser leg (P6 prober should
share its client code).
