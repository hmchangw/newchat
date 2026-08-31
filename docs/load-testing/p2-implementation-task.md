# Task Brief — P2 Instrumentation (SLO-1a / 1b / 2)

> **Hand this whole file to the implementer.** It is written to be executable
> without reading the rest of `docs/load-testing/`. Every file path and line
> number was verified against `main` at the time of writing; re-verify with
> `grep` before editing, as line numbers drift.
>
> Background, if wanted: [`p2-instrumentation-spec.md`](p2-instrumentation-spec.md)
> (why), [`slo-measurement-map.md`](slo-measurement-map.md) (what it unblocks),
> [`common/sli-slo.md`](common/sli-slo.md) (the SLO definitions being served).

---

## 1. Goal

Three SLOs on the message-send journey (J1) are currently **unmeasurable** from
production telemetry. This task adds the missing instruments — and **only**
those.

| SLO | Definition (`sli-slo.md`) | Missing today |
|---|---|---|
| **SLO-1a** | canonical message persisted to Cassandra / **canonical messages published** | A denominator that means "published". See §3.1 — the counter being used instead is definitionally wrong |
| **SLO-1b** | channel room-subject broadcast **enqueue-accepted** / canonical messages routed to the room-subject path | Both sides |
| **SLO-2** | room-subject broadcast enqueue-accepted **within 1 s** of canonical acceptance / same denominator as 1b | The age measurement |

**Ship as two PRs.** P2a (§4) delivers SLO-1a's denominator and all of SLO-1b,
costs one counter add per message, and needs no plumbing. P2b (§5) delivers
SLO-2 and is the one that needs a benchmark. P2a is independently valuable —
merge it without waiting for P2b.

---

## 2. Hard constraints

These are not preferences. A PR that violates one gets sent back.

1. **TDD, per `CLAUDE.md` §4.** Tests first, watch them fail, then implement.
   Every new instrument needs: recorded on the happy path, recorded on the
   error path, **not** recorded on paths that must not count, and the label
   values are the ones documented here.
2. **No per-call attribute construction.** `.semgrep/metrics.yml` enforces this
   in taint mode and it is a blocking CI gate. `attribute.NewSet` costs
   ~264 ns / 192 B per call against ~9.6 ns / 0 B for a map lookup. **Follow the
   existing pattern**: build the closed label combinations into a map in the
   constructor and look them up at the recording site — see
   `message-gatekeeper/nats_metrics.go:52-74` (`newGatekeeperMetrics`), which is
   the exact shape to copy.
3. **Instrument construction must never block startup.** Same file, lines 55-60:
   on `Int64Counter` error, fall back to a no-op instrument. Copy that too.
4. **Every recorder is nil-safe.** `if m == nil || m.<instrument> == nil { return }`.
5. **No unbounded label values.** No message IDs, room IDs, account names,
   subjects, or error strings as label values. Every label is a closed Go enum
   whose values are enumerated in a package-level slice, so a typo is a compile
   error. `.semgrep/metrics.yml` has a cardinality rule as well.
6. **No new store calls, no new network calls, no new allocations on the hot
   path** beyond what §5 explicitly budgets and benchmarks. This is a stated
   requirement from the owner: instrumentation that costs throughput defeats its
   own purpose.
7. **Update the docs in the same PR**: `docs/specs/o11y/nats-metrics-contract.md`
   (the contract) and `docs/specs/o11y/o11y-metrics-inventory.md` (the per-service
   inventory table — `message-gatekeeper` is row 119, `broadcast-worker` row 121).
8. **`docs/client-api.md` is NOT affected** — no client-facing handler
   registration, request struct, response struct or event struct changes. Say so
   in the PR body so a reviewer does not have to check.
9. `make lint`, `make test`, `make sast` all clean before pushing.

---

## 3. The design decision, and why it is not what the older spec says

`p2-instrumentation-spec.md` §5b proposes putting a `broadcast_path` label on
`message_gatekeeper_messages_total{result="accepted"}`, computed from
`sub.RoomType`. **Do not implement that.** Two problems, both found by reading
the current code:

- **There is no `sub.RoomType`.** `model.Subscription` does not carry the room
  type. The gatekeeper's only source is `h.store.GetRoomMeta`, and that is called
  **conditionally** — `message-gatekeeper/handler.go:416` guards it with
  `if !isThreadReply && !bypass`. Labelling the counter would force an
  unconditional `GetRoomMeta` on every message: a store call per message, which
  constraint 6 forbids.
- **It would be a prediction, not an observation.** broadcast-worker decides the
  fan-out route from `meta.Type` at `broadcast-worker/handler.go:290-299`. A
  gatekeeper-side label would be a second, independent guess at the same fact,
  and the two can disagree — giving SLO-1b a numerator and denominator counted
  under different definitions, which is the one defect that makes a ratio
  worthless.

**Instead: put each side of the ratio where the fact is already known.**

| Series | Service | Why there |
|---|---|---|
| `messages_canonical_published_total` (no path label) | message-gatekeeper | SLO-1a's denominator is all paths, so no label is needed at all |
| `broadcast_canonical_consumed_total{broadcast_path}` | broadcast-worker | `meta.Type` is already fetched at `handler.go:279` for routing. The label costs nothing |
| `broadcast_channel_enqueue_total{result}` | broadcast-worker | The enqueue is here |

**The trade-off, stated honestly.** SLO-1b's denominator becomes "canonical
messages **consumed** by broadcast-worker and routed to the room subject" rather
than "**published** and routed". A message lost between the canonical publish and
broadcast-worker consuming it is invisible to SLO-1b. That gap is exactly what
SLO-1a covers, so the pair still spans the journey — but write it into the
contract doc so nobody later reads SLO-1b as covering more than it does.

---

## 4. P2a — the two counters and the denominator

### 4.1 `messages_canonical_published_total` · message-gatekeeper

**Why a new counter rather than reusing the existing one.** `sli-slo.md` wants
SLO-1a's denominator to be "canonical messages published".
`message_gatekeeper_messages_total{result="accepted"}` is recorded at
`handler.go:238`, *after* `processMessage` returns. But `processMessage` publishes
to MESSAGES-CANONICAL at `handler.go:512` and then does
`return sonic.Marshal(msg)` at `handler.go:519`. **If that marshal fails, the
message is already on MESSAGES-CANONICAL, yet the handler takes the infra branch
— records `retry`/`failed` and Naks.** The redelivery publishes again (deduped by
`natsutil.CanonicalDedupID`) and *then* counts `accepted`.

So the existing counter is not "published": it can miss a published message and
it can count a republish. In practice `sonic.Marshal` of a built message
essentially never fails, so the *magnitude* is negligible — but an SLO
denominator has to be right by construction, not by luck, or the first person to
debug a 0.01% discrepancy will not be able to tell instrument error from data
loss.

**Change:**

- New file section in `message-gatekeeper/nats_metrics.go`: an
  `Int64Counter` named `messages_canonical_published_total`, description
  *"Messages successfully published to MESSAGES-CANONICAL."*, constructed in
  `newGatekeeperMetrics` with the same no-op fallback.
- **No labels.** Adding one would only invite the cardinality question later.
- New method `func (m *gatekeeperMetrics) RecordCanonicalPublished(ctx context.Context)`,
  nil-guarded, `Add(ctx, 1)`.
- Call it in `processMessage`, on the line **immediately after** the publish
  succeeds — i.e. between `handler.go:514` (the error return) and the flow log at
  `:516`. Not at the end of the function; the point of the change is that the
  counter fires when the publish lands.

**Tests** (`message-gatekeeper/handler_test.go`, or `nats_metrics_test.go` for
the recorder itself):

| Case | Expected |
|---|---|
| Publish succeeds, `sonic.Marshal` of the reply succeeds | `messages_canonical_published_total` = 1, `messages_total{accepted}` = 1 |
| Publish fails | `messages_canonical_published_total` = 0 |
| Validation rejects before the publish | `messages_canonical_published_total` = 0 |
| Nil metrics struct | No panic |

> The "publish succeeds but the reply marshal fails" case is the one that
> motivates the counter. It is hard to reach without an injection seam; **do not
> add production code to make it testable**. Assert the ordering instead — the
> publish-success test proves the counter fires before the marshal — and note the
> gap in the PR body.

### 4.2 `broadcast_canonical_consumed_total{broadcast_path}` · broadcast-worker

The denominator for SLO-1b/2. Recorded in `handleCreated`
(`broadcast-worker/handler.go:240`), where the routing decision is made.

**Label — a closed enum with exactly four values:**

| `broadcast_path` | Recorded where |
|---|---|
| `thread` | `handler.go:243` — the `msg.IsHiddenThreadReply()` early return |
| `room_subject` | `handler.go:291` — `case model.RoomTypeChannel` |
| `direct` | `handler.go:295` — `case model.RoomTypeDM, model.RoomTypeBotDM` |
| `unknown` | `handler.go:299` — the `default` branch that warns and skips fan-out |

Record **once per call**, on entry to the branch, **before** the publish attempt —
this is a denominator, so it must count the message regardless of whether the
publish then succeeds.

**One thing to get right:** the `thread` case returns before `GetRoomMeta`
(`handler.go:279`) has run, so the counter for it must be recorded at the top of
`handleCreated`, not after the meta fetch. Structure it so every path through
`handleCreated` records exactly one value — a table test that walks all four
branches and asserts a total of exactly 1 is the right shape.

**Tests:** one subtest per label value asserting the label, plus
`GetRoomMeta` returning an error → **no** counter increment (the function returns
before routing, so nothing was routed), plus nil-safety.

### 4.3 `broadcast_channel_enqueue_total{result}` · broadcast-worker

The numerator for SLO-1b.

`publishChannelEvent` (`broadcast-worker/handler.go:1048`) currently ends at
`:1067` with `return h.publishRoomEvent(...)`. Capture the error, record, return
it:

```go
err := h.publishRoomEvent(ctx, meta.ID, meta.CrossSite, meta.CrossSiteAt, payload, "channel event")
h.metrics.RecordChannelEnqueue(ctx, enqueueResultFor(err))
return err
```

**Label — a closed enum with two values:** `accepted` (nil error), `failed`
(non-nil). Do **not** classify the error further; that is a different metric and
a different conversation.

**The denominator match is exact and worth verifying rather than assuming.**
`publishChannelEvent` has exactly one caller — `handleCreated` at
`handler.go:292`. Mutations (edit, delete, pin, react) reach the room subject via
`publishMutation` → `publishRoomEvent` (`handler.go:913`), bypassing
`publishChannelEvent` entirely. So this counter counts exactly "created channel
messages on the room-subject path", which is 4.2's `room_subject` denominator.
**Add a test that fails if a second caller appears** — assert the ratio in an
existing handler test that exercises both creates and mutations.

**Redelivery caveat for the contract doc:** both 4.2 and 4.3 are consumer-side,
so a JetStream redelivery increments both again. The ratio stays meaningful
(numerator and denominator move together) but neither is a message count. Write
that down.

---

## 5. P2b — the age histogram (SLO-2)

Ship separately. Its review is about cost, not about the metric.

### 5.1 What to measure

`broadcast_channel_enqueue_age_seconds` — a histogram of
`time.Now() − <the JetStream stream store timestamp>`, recorded at the same point
as 4.3, i.e. when the room-subject enqueue is accepted.

**The origin is `msg.Metadata().Timestamp`**, not `evt.Timestamp`. `sli-slo.md`
§2 is explicit about this: `evt.Timestamp` is stamped by the gatekeeper's clock
and subtracting it in broadcast-worker measures clock skew as latency. The stream
store timestamp is assigned by the server that also feeds this consumer.

Buckets: reuse `o11y.DefaultLatencyBuckets()` the way
`pkg/natsmetrics/rpcsemconv.go:68` does, **after verifying that `1` is one of its
boundaries** — SLO-2's bound is 1 s and the good-ratio has to be readable at
exactly that `le`. If it is not a boundary, define an explicit bucket set for
this histogram that includes it, and say so in the contract doc rather than
silently diverging from the shared set.

### 5.2 The invalid-reading counter

`broadcast_channel_enqueue_age_invalid_total{reason}`, closed enum:

| `reason` | When |
|---|---|
| `missing_metadata` | `msg.Metadata()` returned an error or a nil meta |
| `negative_age` | The computed age is `< 0` — impossible physically, so it is a measurement fault (clock or plumbing), not a latency |

**Only these two are measurement-invalid.** A large *positive* age is a genuine
SLO-2 miss and must stay in the histogram and be scored as bad. Do not add an
upper cap. This distinction is the one thing that makes the metric certifiable;
`sli-slo.md` §2 Caveats says so directly.

### 5.3 The plumbing, and the trap in it

`HandleMessage` receives `msg.Data()`, not the `jetstream.Msg`
(`broadcast-worker/main.go:538`), so the timestamp has to travel by context from
`broadcastProcessor` (`main.go:522`).

**Do not add a new `context.WithValue`.** There is already one on this path —
`withBroadcastMetricLabels` (`nats_metrics.go:186`), holding the
`broadcastMetricLabels` struct. Add a `streamTS time.Time` field to that struct;
it is already a ctx value, so a wider struct costs no extra allocation.

**The trap:** `withBroadcastMetricLabels` constructs a *fresh* struct on every
call, and there are **seven** call sites — `handler.go:189, 677, 901, 1074, 1127,
1162, 1271`. A naively added field is silently zeroed at each of them, and
`streamTS` would be the zero time by the time `publishChannelEvent` reads it,
turning every reading into `missing_metadata`. (The older spec says three call
sites; it is wrong — check with `grep -n withBroadcastMetricLabels`.)

**Fix it inside the helper, not at the call sites:** have
`withBroadcastMetricLabels` read the existing value out of the incoming ctx and
carry `streamTS` forward. Then no call site changes and no future one can
regress. **Pin this with a test** that re-stamps labels and asserts `streamTS`
survives — it is the single most likely thing to break silently.

Also: `broadcastProcessor` currently calls `msg.Metadata()` **only inside the
flow-log gate** (`main.go:530-533`). P2b needs it unconditionally. Parse it once,
outside the gate, seed the ctx, and have the flow-log branch **reuse** the parsed
value instead of parsing again.

### 5.4 The benchmark — this is the deliverable, not an extra

`msg.Metadata()` parses the reply subject and allocates. Moving it out of the
flow-log gate makes that cost unconditional on the hottest path in the system.

Required in the PR:

- A Go benchmark over `broadcastProcessor`'s per-message work, **before and
  after**, with `-benchmem`, both numbers in the PR body.
- If the regression exceeds **1% of per-message wall time or one allocation per
  message**, stop and raise it rather than merging. The alternative design —
  sampling the age rather than measuring every message — is a legitimate answer
  and needs a decision, not a silent trade.

---

## 6. Acceptance criteria

Check every box. Each is verifiable, not a judgement call.

**P2a**

- [ ] `messages_canonical_published_total` increments on the line after a
      successful canonical publish, and does not increment on any failure before it
- [ ] `broadcast_canonical_consumed_total{broadcast_path}` records exactly one
      value per `handleCreated` call, across all four branches
- [ ] `broadcast_channel_enqueue_total{result}` records exactly one value per
      `publishChannelEvent` call, `accepted` / `failed`
- [ ] Every label is a closed Go enum enumerated in a package-level slice
- [ ] Attribute sets are precomputed in the constructor and looked up at the
      recording site — no `metric.WithAttributes` in any recording function
- [ ] Every instrument falls back to no-op on construction error; every recorder
      is nil-safe
- [ ] `make test SERVICE=message-gatekeeper` and `SERVICE=broadcast-worker` pass
      with `-race`; new code is ≥80% covered
- [ ] `make lint`, `make sast` clean
- [ ] `docs/specs/o11y/nats-metrics-contract.md` and
      `o11y-metrics-inventory.md` updated, **including the two caveats**: the
      consumer-side redelivery double-count (§4.3) and SLO-1b's denominator being
      *consumed* rather than *published* (§3)
- [ ] PR body states that `docs/client-api.md` is not affected, and why

**P2b**

- [ ] Age is computed from `msg.Metadata().Timestamp`, never `evt.Timestamp`
- [ ] `_age_invalid_total{reason}` covers `missing_metadata` and `negative_age`
      **only**; large positive ages stay scored in the histogram
- [ ] `msg.Metadata()` is parsed **once** per message and the flow-log branch
      reuses it
- [ ] A test asserts `streamTS` survives a `withBroadcastMetricLabels` re-stamp
- [ ] Before/after benchmark with `-benchmem` in the PR body, within the §5.4 budget

---

## 7. Verification

```bash
make generate SERVICE=message-gatekeeper   # only if a store interface changed (it should not)
make test SERVICE=message-gatekeeper
make test SERVICE=broadcast-worker
make lint
make sast
```

Then, against a local stack (`deploy/docker-compose.yml`, `BOOTSTRAP_STREAMS=true`),
send one channel message and confirm on the services' o11y endpoints that all
four series appear with the expected label values. **A metric that compiles but
never appears in `/metrics` is the normal failure here** — otelprom only exports
an instrument once a data point has been recorded.

---

## 8. Explicitly out of scope

Do not add these, even though they are adjacent:

- Any `site` label on any metric. Site scoping is a scrape-time relabel decision,
  not an application concern, and `pkg/obs` deliberately keeps `chat.site.id` as a
  baggage/span attribute rather than a metric label.
- Per-recipient notification counters (that is P4).
- A `status` label on the search duration histogram (also P4).
- The max-delivery advisory consumer (P7) — and note the owner has said the
  NATS/JetStream team's advisory surface is not available to us.
- Any change to what `message_gatekeeper_messages_total` already records. The new
  counter sits beside it; the existing one keeps its current meaning, which other
  dashboards depend on.
