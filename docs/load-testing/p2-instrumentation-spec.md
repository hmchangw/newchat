# P2 Instrumentation Spec — J1 (SLO-1a / 1b / 2)

> **This document is rationale only. The executable contract is
> [`p2-implementation-task.md`](p2-implementation-task.md), and above it
> [`common/sli-slo.md`](common/sli-slo.md) §"Denominator & outcome contract".**
>
> Everything below that names a *specific* instrument, label, PromQL expression,
> diff or ordering has been superseded and removed — it disagreed with the
> contract on where the `broadcast_path` denominator lives (gatekeeper, upstream
> of both workers, so a dead worker drops the ratio instead of vanishing) and on
> label names (`outcome=ok|failed`, not `result`/`accepted`). What remains is the
> reasoning: which instruments exist today, why each missing one is needed, and
> what was deliberately *not* added.

> What to build so the flagship journey becomes measurable, written against the
> code rather than against the roadmap line. Constraint: **the fewest instruments
> that make the three SLOs computable, and no regression on the send hot path.**
>
> Verified against the code on 2026-08-27. #337 is the baseline — its recorder
> discipline (precomputed attribute sets, nil-collapse when disabled, the semgrep
> cardinality gate, the `pkg/obs` registry guard) applies to everything here.

| | |
|---|---|
| **Status** | Rationale only — superseded on every implementation detail by [`p2-implementation-task.md`](p2-implementation-task.md) |
| **Net change** | **Four instrument families to add, one already exists, one to drop.** Two additions carry a measurable hot-path cost and both are benchmarked. SLO-1a has an *approximate, attempt-based* indicator today with no code change — it is **not** a hard gate, and P2 does not make it one (see the G4 note in [`execution-priority-plan.md`](execution-priority-plan.md)) |
| **Not in scope** | Anything that needs a JetStream advisory consumer — see §6 |

---

## 0. What P2 actually needs, after reading the code

The roadmap line (`common/sli-slo.md` §8 P2) reads as five pieces of work. Two of
them are already done and one should be dropped.

| Roadmap piece | Reality | Action |
|---|---|---|
| gatekeeper `messages_canonical_published_total{broadcast_path}` | `message_gatekeeper_messages_total{result="accepted"}` is close but is a *handler-outcome* counter recorded after the reply, not at the publish site, and it carries no fan-out label | **Add a dedicated counter at the publish site** — decided in [`p2-implementation-task.md`](p2-implementation-task.md) §4.1 |
| message-worker persisted numerator | **Already exists** as `message_worker_persistence_total{message_kind,result}` (`message-worker/nats_metrics.go:46`, recorded at `handler.go:190,193,204,207`) | **Nothing** — [task §8](p2-implementation-task.md) |
| broadcast-worker `broadcast_channel_enqueue_total` | #337's `broadcast_worker_recipient_deliveries_total` is close but counts per publish target | **Add**, one counter — [task §4.2](p2-implementation-task.md) |
| broadcast-worker `broadcast_channel_enqueue_age_seconds` | Absent, and the handler cannot see the message | **Add** + plumbing — [task §5.3](p2-implementation-task.md) |
| `broadcast_channel_enqueue_age_invalid_total{reason}` | Absent | **Add** — free — [task §5.2](p2-implementation-task.md) |
| gatekeeper build→publish diagnostic | Explicitly unscored; #337's `rpc.server.call.duration` already answers the question | **Drop** |

---

## 1–2. Instrument designs and per-instrument costs — removed

This document used to carry a per-instrument design (§1 A–F) and a per-message
cost table (§2). Both encoded the pre-contract design and are gone: §1A derived
the `broadcast_path` label from a `sub.RoomType` field that does not exist and
recommended widening the existing counter; §1D threaded the stream timestamp as
an explicit parameter; §2 scored SLO-1a as "computable today, no code change",
which is true only of an approximate attempt-based indicator and was being read
as a finished result.

**The live design is [`p2-implementation-task.md`](p2-implementation-task.md)**
(§3 placement and classification, §4 the counters, §5 the age histogram), under
[`common/sli-slo.md`](common/sli-slo.md) §"Denominator & outcome contract".

What survives from §2 and is worth keeping in mind while implementing:

- With #337's recorder discipline, a recording call costs ~3.27 ns / 0 allocs
  when `O11Y_ENABLED=false` collapses it, and a warm option lookup ~9.6 ns / 0
  allocs. Counter adds are noise next to what these paths already do.
- **The two costs that are not noise are the two the task brief benchmarks**: an
  unconditional room-meta lookup in the gatekeeper (cache-fronted, but not free)
  and an unconditional `msg.Metadata()` parse in broadcast-worker.
- **None of these instruments is per-recipient.** That is the property that keeps
  them safe in a 5 000-member room, and it is the one thing not to trade away.

---

## 3. What not to add

- **Per-recipient channel enqueue counters.** A channel broadcast is one
  room-subject publish; NATS fans out downstream. #337's comment already makes
  this point, and per-recipient counting on the channel path would be both wrong
  and expensive.
- **Any message, room, account, user or subject identifier as a label.** The
  `.semgrep/metrics.yml` taint rule from #337 blocks it, including through a
  local variable. Exact identifiers belong in the loadgen WAL, never in
  Prometheus.
- **Attribute construction on a recording path.** Same gate. Use the precomputed
  option map, or `optTable` if the label space is wide.
- **A redelivery counter.** #337 deleted `chat.nats.consumer.redeliveries`
  deliberately and the reasoning holds. Do not reintroduce it as a substitute for
  §6.
- **The build→publish diagnostic** — `rpc.server.call.duration` already answers it.
- **Sampling on the age histogram** — unless its benchmark says otherwise, in which case it is a decision for the owner, not a silent trade.

Every instrument added here must appear in `docs/specs/o11y/nats-metrics-contract.md`
next to *what reads it*, or the `pkg/obs` registry test fails. For these the reader
is named: the SLO-1a/1b/2 recording rules. **Not a hard gate** — see G4 in
[`execution-priority-plan.md`](execution-priority-plan.md) for why an
attempt-based numerator cannot be one.

---

## 4. Superseded sections

This document used to carry the resulting PromQL (§4), an order of work (§5), a
file-by-file diff (§5b) and a PR split (§5c). All four have been removed rather
than left to rot: they encoded the pre-contract design, and a stale executable
section beside a live one is worse than no section at all — a reader cannot tell
which is current.

| What it said | Where it lives now |
|---|---|
| PromQL for SLO-1a/1b/2 | [`first-slo-run-runbook.md`](first-slo-run-runbook.md) §5, against the shipped series |
| Order of work, PR split | [`p2-implementation-task.md`](p2-implementation-task.md) §1 |
| File-by-file diff, call sites, tests | [`p2-implementation-task.md`](p2-implementation-task.md) §3–§6 |
| Which label goes where, and its name | [`common/sli-slo.md`](common/sli-slo.md) §"Denominator & outcome contract" — the contract, above both |

---

## 5d. What happens if P2 is never built

Be precise about this — the cost is **not** "we cannot see problems". Several
signals survive.

**What you keep without P2:**

- `broadcast_worker_recipient_deliveries_total{room_kind="channel", result="failed"}`
  — publish errors on the channel lane are already counted.
- Consumer lag on broadcast-worker's MESSAGES-CANONICAL durable — which
  `sli-slo.md` §0.1 names the **primary** enforcement signal for every async SLO,
  precisely because the v1 ratios are approximate.
- `chat.nats.client.connected` / `_connection_events_total` — the connection-risk
  backstop for the core-NATS hop.
- loadgen's one-sided bound in a test window (`slo-measurement-map.md` §7).

**What you lose, specifically:**

1. **No error budget for J1's delivery half.** Burn-rate alerting (`sli-slo.md`
   §7) is defined on event ratios. An error counter with no denominator cannot
   produce one, so SLO-1b and SLO-2 can never be alerted on the way every other
   SLO is. They stay 🔧 permanently, and J1 ships with one of its three SLOs
   enforceable.
2. **SLO-2's failure mode has no proportional signal.** Consumer lag reports
   **queue depth, not age**. `sli-slo.md` §6 already makes this exact argument for
   federation: growth alone is insufficient, because a lane can park a small,
   constant backlog forever and look stable. The same trap applies here — a
   broadcast-worker that is *steadily* a minute behind shows a flat, non-zero
   pending and no alert, while every user sees late messages. Age is the signal
   that distinguishes "slow" from "backed up", and nothing else produces it.
3. **A delivery incident is not attributable from metrics.** When messages stop
   arriving, the question is "did broadcast-worker fail to enqueue, or did the
   delivery lane drop them?" Without the enqueue ratio both look identical from
   the dashboards, and the investigation falls back to logs.
4. **Calibration for 1b/2 is pushed out by a full window.** §0.2 asks for 4–6
   weeks of observation before targets are set. That clock cannot start until the
   counters exist, so deferring P2 by N weeks delays *enforceable* J1 SLOs by
   N + 6.
5. **Every load-test verdict on 1b/2 stays one-sided.** A pass is conclusive; a
   miss is unattributable. Good enough for the first achievability run
   (`slo-measurement-map.md` §7), not good enough for a release gate.

**The honest summary:** without P2 you can still tell that something is wrong on
the J1 delivery path. You cannot say *how often*, cannot set a budget, cannot
page on the burn rate, and cannot separate a slow worker from a backed-up one.

---

## 6. X9 without advisories — the premise changed

Capturing `$JS.EVENT.ADVISORY.>` needs a stream provisioned on the NATS cluster,
which is the platform team's territory and not available to us. That constraint
matters less than it looks, for three reasons.

**The severity was overstated, and the source doc is stale.**
`failure/nats-jetstream.md` §3 says message-gatekeeper does an "immediate `Nak()`
against `MaxDeliver=5`", so "a short fault can burn the whole delivery budget in
seconds". The code does not do that: `message-gatekeeper/handler.go:212` calls
`jsretry.Nak(ctx, msg, jsretry.DefaultBackoff, …)`, which is
`1s / 5s / 30s / 2m / 10m` (`pkg/jsretry/jsretry.go:51`), and `MaxDeliver`
defaults to **6**, not 5 (`pkg/stream/consumer.go:18`). The client-side budget is
therefore about **12.6 minutes**, and the server-side `BackOff` for an un-acked
message is `{30s, 1m, 2m, 4m, 8m}`. A two-second dependency blip cannot exhaust
either. X9 drops from "highest-severity silent loss path" to "confirm the budget
holds", and the two stale numbers in the failure doc should be corrected.

**What still needs watching, and how to see it without advisories.** An outage
*longer than the budget* does still end in terminal drops. `msg.Metadata()`
carries `NumDelivered`, and the age histogram's plumbing parses that metadata
anyway ([task §5.3](p2-implementation-task.md)) — so a
bounded counter of "settled at delivery attempt ≥ N" costs nothing extra and
shows how close messages are getting to the cap. It does not prove a drop, and it
must not be sold as proving one; it is a leading indicator, and it is entirely in
our own code. Treat it as **optional**: add it only if the budget check above
shows real headroom pressure, so it does not become another instrument nobody
reads.

**For a load test specifically, the ledger is already the ground truth.**
loadgen's `missing_after_deadline` means admission recorded `good` and the effect
never appeared — that *is* the drop, observed end to end, and it is the strongest
loss claim the evidence model defines. Advisories would add **attribution**
(which consumer dropped it), not detection. So a campaign can reach a verdict
without them; what it must not do is claim advisory-grade completeness. The
failure program already says exactly this: a campaign whose advisory capture was
absent or a plain subscription "may report advisory counts, but must not claim
advisory completeness".

---

## 7. Sibling documents

- [`slo-measurement-map.md`](slo-measurement-map.md) — every journey's path, hop by hop, and which instrument sits where
- [`common/sli-slo.md`](common/sli-slo.md) §2, §8 — the SLO definitions and the roadmap line this spec implements
- [`execution-priority-plan.md`](execution-priority-plan.md) — Track 1.1
- [`extreme-scenarios.md`](extreme-scenarios.md) X9 — the delivery-budget scenario
- `docs/specs/o11y/nats-metrics-contract.md` — where every instrument must be registered
