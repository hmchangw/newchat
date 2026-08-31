# First SLO Run — Report Template, with a Worked Example

> The output contract for the run described in
> [`first-slo-run-runbook.md`](first-slo-run-runbook.md). That document is the
> **SOP** — what to set, when to mark, what to read. **This** document is what
> the run has to hand back.
>
> **Every number below is invented.** They are internally consistent — the
> arithmetic checks out and the counter deltas agree — so that the *shape* of a
> real answer is visible. Do not quote any of them.

**Fill this in before the run, with blanks.** A blank you cannot see how to fill
is a missing step in the SOP, and it is much cheaper to find it now than at `t2`.

---

## Part 1 — The run ledger

Every run, including the discarded ones, with the reason. §7b: a ledger with
twelve rows and one reported result is healthy; a ledger with one row is the
suspicious one.

| Run | Date | Phase | Duration | Outcome | Reason |
|---|---|---|---|---|---|
| R01 | 09-02 | shakedown | 12 m | **discarded** | `site` selector returned empty — PRE-6 option A relabel not applied to the `history-service` job |
| R02 | 09-02 | shakedown | 12 m | **discarded** | Same, plus `search_service_requests_total` absent — search scrape target missing entirely (PRE-2) |
| R03 | 09-03 | shakedown | 15 m | **discarded** | `t0-sync` marked 40 s early; `get_thread_messages` rate was 71/s against the derived 105/s, catalog not refilled |
| R04 | 09-03 | shakedown | 15 m | **discarded** | Backlog observer not actually running — `nats consumer info` was being read from the wrong stream. `t2` unmarkable |
| R05 | 09-04 | shakedown | 20 m | **discarded** | Clean instrument, but sampler ratio was still `1.0`; kept as the sampler-overhead A/B, not as a result |
| R06 | 09-04 | shakedown | 20 m | **passed** | Instrument verified end to end. **Freeze here.** |
| **M1** | 09-05 | **measurement** | 34 m | **reported** | — |
| **M2** | 09-05 | **measurement** | 32 m | **reported** | — |
| M3 | 09-08 | measurement | 8 m | **discarded** | Mongo maintenance on the shared host at +6 m; neighbour activity recorded, run aborted |
| **M4** | 09-08 | **measurement** | 31 m | **reported** | Replaces M3 |

**Reported = median of M1, M2, M4.** Not the best of them.

---

## Part 2 — The run record

Attached to every number, per §7 step 8.

| | |
|---|---|
| `runId` | `slo-cal-20260905-a` |
| `siteId` | `slo-test-a` (isolated — G1 satisfied) |
| Image digest | `sha256:9f2c…41ab` (contains `bf0ea62`) |
| Workload shape | `realistic` preset, `i12=derived`, `sendRate=100`, `readRate=700`, `threadShare=0.10` |
| Encryption | `ENCRYPTION_ENABLED=true`, `ATREST_ENABLED=true` |
| Sampler ratio | `0.1` |
| Live consumer config | `MaxDeliver=6`, `AckWait=30s`, `BackOffFactor=2`, `BackOffMax=8m` |
| Derived `t2` operational cap | **1050 s (17.5 m)**, per §5 |
| Scrape interval | `30s` |
| Marks (M1) | `t0-async 09:14:00` · `t0-sync 09:21:30` · `t1 09:55:30` · `t2 09:58:00` |
| Background contamination | `n_b/n_c = 1.86%` (gatekeeper accepted: 3 417 over an equal background-only window vs 183 402 in-run) |
| `t2` floor band | `num_pending + num_ack_pending ∈ [0, 34]`, characterised over the same background window |
| `sli-slo.md` revision | `@bf0ea62` |

---

## Part 3 — The validity gate

Read this **before** the ratios (§6). All six must pass or the run is
INCONCLUSIVE regardless of how the numbers look.

| Check | M1 | M2 | M4 |
|---|---|---|---|
| Backlog flat (no monotonic climb on any durable) | ✅ max 34 | ✅ max 41 | ✅ max 29 |
| Dispatch ratio ≥ 95% | ✅ 99.2% | ✅ 99.4% | ✅ 98.9% |
| No loadgen NATS disconnect | ✅ | ✅ | ✅ |
| No emit underrun / GC pause invalidation | ✅ | ✅ | ✅ |
| `t2` reached inside the 1050 s cap | ✅ 150 s | ✅ 210 s | ✅ 135 s |
| Neighbour activity recorded | ✅ none | ✅ none | ✅ none |

---

## Part 4 — Artefact 1: the SLO-4 / SLO-5 curves

**This is the deliverable, not a single number.** One point per histogram
boundary, so Track 1.3 can pick a target off the curve instead of confirming the
one that was guessed.

### SLO-4 — `channel_history` (J2 channel load)

Counter deltas, M1, `@t1 − @t0-sync`:

```
denominator (valid)  = 1 247 903      # _count, error_type=~"|internal|unavailable|too_many_requests"
successes  (+Inf)    = 1 246 015      # _bucket{le="+Inf"}, error_type=""
errors               =     1 888      (0.151%)
```

| `le` | cumulative bucket | good-ratio | vs draft |
|---|---|---|---|
| 0.05 | 421 110 | 33.75% | |
| 0.1 | 812 447 | 65.10% | |
| **0.25** | 1 140 882 | **91.42%** | |
| **0.5** | 1 200 455 | **96.20%** | ← **drafted bound**, target 95% |
| **1** | 1 236 001 | **99.05%** | |
| 2.5 | 1 245 110 | 99.78% | |
| +Inf | 1 246 015 | 99.85% | (= availability ceiling) |

Across the three reported runs, at `le=0.5`: **96.20 / 95.88 / 96.41** → median
**96.20%**, spread **0.53 pt**.

### SLO-5 — `thread_open` (J2 thread open)

```
denominator (valid)  = 187 240
successes  (+Inf)    = 187 015
errors               =     225      (0.120%)
```

| `le` | cumulative bucket | good-ratio | vs draft |
|---|---|---|---|
| 0.05 | 121 440 | 64.86% | |
| 0.1 | 168 900 | 90.21% | |
| **0.25** | 179 102 | **95.65%** | ← **drafted bound**, target 95% |
| **0.5** | 185 443 | **99.04%** | |
| 1 | 186 880 | 99.81% | |
| +Inf | 187 015 | 99.88% | |

Across the three reported runs, at `le=0.25`: **95.65 / 94.81 / 95.98** → median
**95.65%**, spread **1.17 pt**.

> **Read this one carefully — it is the interesting result.** The median clears
> the drafted 95% by **0.65 pt**, but the run-to-run spread is **1.17 pt**, and
> **one of the three runs (94.81%) missed the draft outright.**
>
> The margin is smaller than the spread. That is not "met" — it is *"the drafted
> target sits inside the noise of this environment"*, which is a different and
> more useful finding. Note that execution rule 10's own bar (spread ≤ ~10% of
> the value) passes here at 1.2% — the rule that catches this is
> **margin > spread**, and it belongs beside the other one.

---

## Part 5 — Artefact 2: SLO-7

```
ok        = 402 118
eligible  = 403 455
ratio     = 99.669%
```

Three runs: **99.67 / 99.71 / 99.64** → median **99.67%**, spread **0.07 pt**.
Drafted target **99.5%** — margin **0.17 pt**, larger than the spread. **Met.**

Carry the standing caveat: SLO-7 covers partial degradation only. A complete
search outage empties the denominator too, and needs the prober
(`sli-slo.md` §5), which this run does not provide.

---

## Part 6 — Artefact 3: SLO-1a

Raw snapshots, so the arithmetic is auditable:

| Counter | `@t0-async` | `@t2` | Δ |
|---|---|---|---|
| `message_gatekeeper_messages_total{result="accepted"}` | 4 118 207 | 4 301 609 | **183 402** |
| `message_worker_persistence_total{message_kind=~"user\|thread_reply", result="success"}` | 5 002 144 | 5 185 539 | **183 395** |

```
approximate indicator = 183 395 / 183 402 = 99.9962%
```

Three runs: **99.9962 / 99.9938 / 99.9971** → median **99.9962%**.
Drafted target **99.9%** — met with roughly **26× headroom in the error budget**.

**Three caveats that must travel with this number, per §5:**

1. It is **per-attempt, not per-message**. The persistence counter increments on
   every delivery, so a redelivered message counts twice on the numerator. The
   ratio can legitimately exceed 100%; this one did not.
2. It is contaminated by **1.86%** background traffic on both sides (Part 2).
   The background-only window gave `r_b = 99.90%` over `n_b = 3 417`, so §1a's
   decomposition gives `r_run = (0.999962 × 183 402 − 0.9990 × 3 417) /
   (183 402 − 3 417)` = **99.998%**. The background is *worse* than the run, so
   the raw figure was slightly pessimistic. It changes no decision here — but
   report both, because "which direction did the contamination push it" is the
   question a reviewer will ask.
3. **A genuinely lost message is invisible to it.** Both counters would simply
   be smaller. Only P2a's `messages_canonical_published_total` closes that, which
   is the whole argument for building it.

---

## Part 7 — Artefact 4: the SLO-1b / SLO-2 one-sided bounds

From a separate short `run --preset=realistic --rate=100`, 60 000 messages,
using E2 stage correlation. These are **lower bounds**, because E2 measures
protocol receipt, which is strictly downstream of the enqueue boundary the SLOs
are defined at — anything E2 saw was certainly enqueued, and anything E2 saw
within 1 s was certainly enqueued within 1 s.

| | Bound | Draft | Reading |
|---|---|---|---|
| **SLO-1b** | **≥ 99.940%** (59 964 / 60 000 received) | 99.9% | Draft is **not contradicted**. It is also not confirmed — a bound is not a measurement |
| **SLO-2** | **≥ 99.31%** (59 586 received within 1 s) | 99% | Same |

The gap between "not contradicted" and "measured" is exactly what **P2b** buys.
Both bounds would tighten to real ratios the moment
`broadcast_channel_enqueue_total` and `broadcast_channel_enqueue_age_seconds`
exist.

---

## Part 8 — Artefact 5: the loadgen-vs-production measurement gap

Measured **once**, then reused. This sizes the permanent blind spot between what
loadgen sees (client-side, includes the NATS round trip) and what the SLO is
scored on (server-side handler duration).

| | loadgen p95 | server-side p95 | gap |
|---|---|---|---|
| `load_history` / `channel_history` | 312 ms | 268 ms | **+44 ms (16.4%)** |
| `get_thread_messages` / `thread_open` | 194 ms | 171 ms | **+23 ms (13.5%)** |

**What this means for Part 4:** the server-side curves are **optimistic by
roughly 40 ms at p95 for SLO-4**. Against a 500 ms bound that is minor. Against
SLO-5's 250 ms bound it is not — a 23 ms shift is meaningful when the margin is
0.65 pt. Say so in the recommendation.

---

## Part 9 — Artefact 6: the apparatus

The most reusable output, and the one easiest to forget to hand over.

| | Where |
|---|---|
| Verified query set (PRE-9), with returned shape and a sample value per query | `evidence/slo-cal-20260905/queries.md` |
| SLI/SLO dashboard, built from those queries | Grafana UID `slo-cal-v1` |
| Background baseline window + contamination share | `evidence/slo-cal-20260905/background.md` |
| `t2` floor band per durable | same |
| The run ledger (Part 1) | this document |

---

## Part 10 — The recommendation to Track 1.3

The point of the whole exercise. One row per SLO: what was drafted, what the
evidence says, what should happen to the draft.

| SLO | Drafted | Achieved (median) | Margin vs spread | Recommendation |
|---|---|---|---|---|
| **1a** | 99.9% | 99.9962% | 26× budget headroom | **Raise the draft to 99.95%** (headroom drops to 13×, still ample). At 99.9% the target would never alert on a real regression. Do this only once P2a makes the measurement exact — raising a target on an approximate indicator is how you get an unfalsifiable SLO |
| **1b** | 99.9% | ≥ 99.940% (bound) | n/a | **Hold the draft, build P2b.** A bound cannot confirm a target |
| **2** | 99% | ≥ 99.31% (bound) | n/a | **Hold the draft, build P2b** |
| **4** | 95% @ 500 ms | 96.20% | 1.20 pt vs 0.53 pt ✅ | **Confirm 95% @ 500 ms.** Also record that 99.05% @ 1 s is available if a two-tier objective is ever wanted |
| **5** | 95% @ 250 ms | 95.65% | **0.65 pt vs 1.17 pt ❌** | **Do not confirm.** Two workable rewrites: **98% @ 500 ms** (achieved 99.04%, margin 1.04 pt) or **93% @ 250 ms** (margin 2.65 pt). Both put the margin above the spread. **The draft as written would burn its budget on environment noise alone** |
| **7** | 99.5% | 99.67% | 0.17 pt vs 0.07 pt ✅ | **Confirm 99.5%**, with the outage-backstop caveat still open |
| 3, 6, 8, 9 | — | not measured | — | §8 — each needs a different run or missing instrument |

### Two caveats that qualify Part 4 and must be in the same paragraph as any target decision

1. **SLO-4's number is optimistic.** The soak has no historical backfill, so
   `LoadHistory` reads hit shallow, dense, run-generated data. Production walks
   aged buckets. The curve is an upper envelope, not a prediction.
2. **SLO-4/5 were measured under the soak's full mix** — mutations, presence,
   search and read receipts all concurrent. Closer to production than a
   single-journey test, but not the same number a focused
   `max-rps --workload=history` would give.

---

## Part 11 — What an INCONCLUSIVE report looks like

Worth writing out, because the failure mode is dressing it up as a weak result.

> **Run `slo-cal-20260907-b` — SLO-1a INCONCLUSIVE.**
>
> `phase: stopped → soak` was refused by the GitOps controller (self-heal put
> the Deployment back within 40 s), so `t0-async` could not be taken against a
> drained backlog. Per §7 there is no fallback: the interval bound is not an
> upper bound, and the contamination bound is inside the error budget of the
> target itself.
>
> **SLO-1a is not reported for this run.** SLO-4, 5 and 7 are unaffected — they
> are synchronous and depend on `t0-sync`, which was marked normally — and are
> reported above.
>
> Unblocking needs the Argo self-heal exclusion for the loadgen Deployment
> (raised with infra, `INFRA-2211`).

No number. No interval. No caveat-laden estimate. **The word, the reason, and
the thing that would unblock it.**

---

## Sibling documents

| | |
|---|---|
| The SOP this reports on | [`first-slo-run-runbook.md`](first-slo-run-runbook.md) |
| Why each SLO is or is not measurable | [`slo-measurement-map.md`](slo-measurement-map.md) |
| The acceptance contract being calibrated | [`common/sli-slo.md`](common/sli-slo.md) |
| Where this run sits in the programme | [`execution-priority-plan.md`](execution-priority-plan.md) Track 1.0b |
