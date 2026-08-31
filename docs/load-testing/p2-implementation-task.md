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

**Ship as two PRs.** P2a (§4) delivers SLO-1a's denominator and SLO-1b. P2b (§5)
delivers SLO-2. P2a is independently valuable — merge it without waiting for P2b.

**Both need a benchmark**, for different reasons: P2a makes a cache-fronted
room-meta lookup unconditional (§3.2), P2b makes a JetStream metadata parse
unconditional (§5.4). Neither is free, neither is obviously expensive, and
neither should be merged on an assertion.

**Two things P2a does not deliver, and they belong in the PR body**: SLO-1b's
ratio can exceed 1 under redelivery (§4.2), and a `broadcast_path="unknown"`
rate above a small fraction makes SLO-1b INCONCLUSIVE for that window (§3.3).

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

## 3. Where each side of the ratio lives

`common/sli-slo.md:169-185` fixes this, and the reason it gives is the one that
matters:

> **Denominators, `broadcast_path`-scoped:** `messages_canonical_published_total{broadcast_path}`,
> emitted by **message-gatekeeper** at the canonical publish site — **upstream of
> both workers, so a dead worker drops the ratio instead of vanishing.**

**Implement it that way.** An earlier revision of this brief moved the
`broadcast_path` denominator onto a broadcast-worker counter to avoid a store
call. That is wrong, and the way it is wrong is the failure mode SLOs exist to
catch: a consumer-side denominator is only incremented by a worker that is
running. If broadcast-worker stalls or exhausts its delivery budget, the
messages it never processed leave **both** sides of SLO-1b, and the ratio stays
at 100% through the outage. SLO-1a does not cover that gap either —
message-worker and broadcast-worker are independent consumers of
MESSAGES-CANONICAL, so message-worker persisting everything says nothing about
whether broadcast-worker fanned anything out.

### 3.1 The classification, and why it is not a second guess

`sli-slo.md` requires the label to mirror broadcast-worker's dispatch exactly.
It can, because the thread/non-thread half is a **shared predicate**, not a
re-derivation: `model.IsHiddenThreadReply(threadParentMessageID, tshow)`
(`pkg/model/message.go:107`) is what broadcast-worker's `handleCreated` calls at
`broadcast-worker/handler.go:242`. Call the same function.

| `broadcast_path` | Condition | Mirrors |
|---|---|---|
| `thread` | `model.IsHiddenThreadReply(req.ThreadParentMessageID, tshow)` | `handler.go:242` early return |
| `room_subject` | else, `meta.Type == model.RoomTypeChannel` | `handler.go:291` |
| `dm` | else, `meta.Type` is `RoomTypeDM` or `RoomTypeBotDM` | `handler.go:295` |
| `unknown` | else, **or the room-meta lookup failed** | `handler.go:299`, plus fail-open (§3.3) |

**Use the normalized `tshow`, not `req.TShow`.** `processMessage` computes
`tshow := req.TShow && req.ThreadParentMessageID != ""` at
`message-gatekeeper/handler.go:481`, and it is that value which lands on the
message broadcast-worker later classifies. Passing the raw request field
misclassifies a `tshow=true` send that carries no thread parent.

### 3.2 The cost, honestly stated

The `thread` test is free. Splitting `room_subject` from `dm` needs the room
type, and the gatekeeper's only source is `h.store.GetRoomMeta` — called
**conditionally** today, guarded by `if !isThreadReply && !bypass`
(`handler.go:416`). Making it unconditional is a real change, and an earlier
revision of this brief rejected the whole design on that basis. That rejection
overstated the cost:

- **`GetRoomMeta` is already cache-fronted in this service.**
  `message-gatekeeper/metacache.go` wraps the store in a
  `roommetacache.Wrapper` with a size and TTL, so the common case is an
  in-process lookup, not a Mongo read.
- The messages that would newly pay for it are the ones that skip the fetch
  today: bypass-eligible senders (owners, admins, bots) and thread replies —
  **and thread replies classify without it**, so only the bypass path is
  actually new.

**It still has to be measured, not assumed.** Requirements:

- A before/after benchmark of `processMessage`, `-benchmem`, both numbers in the
  PR body.
- The cold-miss path — a room not in the cache — timed separately, because the
  benchmark's steady state will be all hits and will flatter the change.
- If the regression exceeds **1% of per-message wall time**, stop and raise it
  rather than merging. The fallback is *not* to move the denominator downstream;
  it is to widen the room-meta cache, or to emit `unknown` for the bypass path
  and record the resulting blind spot. Bring the numbers and let the owner choose.

### 3.3 Fail open, and count the failures

A metric must never fail a message. If `GetRoomMeta` returns an error on a path
that would otherwise not have called it, **label the message `unknown` and carry
on** — the same fail-open stance `handler.go:422` already takes for the
large-room cap.

`unknown` is not a silent bucket. It is a validity signal: SLO-1b's denominator
is the `room_subject` slice, so a message misfiled as `unknown` is silently
removed from it. **Alert on it, and make it a run-validity check** — if
`unknown` exceeds a small fraction of the total during a measurement run, SLO-1b
is INCONCLUSIVE for that run, exactly as an unmarkable `t2` makes SLO-1a
INCONCLUSIVE. Put that in the runbook's §6 gate when this lands.

---

## 4. P2a — the denominator and the enqueue counter

### 4.1 `messages_canonical_published_total{broadcast_path}` · message-gatekeeper

**Why a new counter rather than a label on the existing one.**
`message_gatekeeper_messages_total{result,reason}` is a handler-outcome counter,
and `result="accepted"` is recorded at `handler.go:238` — after `sendReply`, not
at the publish. Widening it with `broadcast_path` would multiply its label space
across all four `result` values for the benefit of one, and would change the
meaning of a series other dashboards already read. Add a sibling.

> **A correction to an earlier revision of this brief.** It justified the new
> counter by claiming that `return sonic.Marshal(msg)` at `handler.go:519` can
> fail after the publish at `:512`, leaving a published message uncounted. **That
> is unreachable**: `evt` is built as `model.MessageEvent{… Message: msg …}`
> (`handler.go:504`), `sonic.Marshal(evt)` at `:505` has already succeeded on the
> same value, and `msg` is not touched in between. The reason for a separate
> counter is the label and the publish-site semantics, not that failure mode.

**Placement:** in `processMessage`, immediately after the publish succeeds —
between the error return at `:514` and the flow log at `:516`.

**It is an approximate count, and the caveat is not optional.** The publish is
deduplicated (`jetstream.WithMsgID(natsutil.CanonicalDedupID(&evt))`,
`handler.go:512`) and the `*jetstream.PubAck` is currently **discarded**. Two
consequences:

- Counting every successful PubAck counts a JetStream **redelivery of an
  already-published message a second time** — the stream deduplicates it, the
  counter does not.
- Excluding duplicates undercounts when a first publish succeeds but its ack is
  lost: the retry is then flagged duplicate and neither attempt is counted.

**No application counter can be an exactly-once logical-publish count.** Do this:

- Capture the PubAck (`ack, err := h.publish(...)`) and increment the main
  counter only when the ack is **not** flagged as a duplicate. *Verify the field
  name on the pinned nats.go version before writing the condition.*
- Add `messages_canonical_publish_duplicate_total` (no labels) for the excluded
  case, so the magnitude is visible rather than inferred.
- Document in `docs/specs/o11y/nats-metrics-contract.md` that this is an
  **approximate PubAck-based publish count**, that duplicates are excluded, and
  that a lost ack undercounts by one. An exact logical-publish denominator needs
  a server-side stream delta or a persisted ledger — out of scope here, and
  `sli-slo.md` §0.1 already labels these numerators approximate.

**Tests:**

| Case | Expected |
|---|---|
| Publish succeeds, ack not duplicate | `+1` on the path the classifier chose |
| Publish succeeds, ack flagged duplicate | main counter unchanged, `_duplicate_total` `+1` |
| Publish fails | neither counter moves |
| Validation rejects before the publish | neither counter moves |
| Hidden thread reply | `broadcast_path="thread"`, and `GetRoomMeta` is **not** called |
| `TShow=true` with no thread parent | **not** `thread` — the normalized-`tshow` case |
| Channel / DM / BotDM | `room_subject` / `dm` / `dm` |
| `GetRoomMeta` errors | `broadcast_path="unknown"`, message still published |
| Nil metrics struct | no panic |

**And one test that is the whole point of §3.1:** a shared table of
`(threadParentMessageID, tshow, roomType)` cases asserting that the gatekeeper's
label and broadcast-worker's dispatch branch agree, case for case. If the two
ever diverge, SLO-1b's numerator and denominator are counted under different
definitions and the ratio is worthless — this test is what stops that.

### 4.2 `broadcast_channel_enqueue_total{outcome}` · broadcast-worker

The numerator for SLO-1b.

**Label name:** `outcome`, values `ok` and `failed` — as
`common/sli-slo.md:188` specifies. Not `result`, and not `accepted`; earlier
drafts of this brief and of the spec used both, and the contract wins.

`publishChannelEvent` (`broadcast-worker/handler.go:1048`) currently ends at
`:1067` with `return h.publishRoomEvent(...)`. Capture the error, record, return:

```go
err := h.publishRoomEvent(ctx, meta.ID, meta.CrossSite, meta.CrossSiteAt, payload, "channel event")
h.metrics.RecordChannelEnqueue(ctx, enqueueOutcomeFor(err))
return err
```

Do **not** classify the error further; that is a different metric.

**The denominator match is exact, and worth verifying rather than assuming.**
`publishChannelEvent` has exactly one caller — `handleCreated` at
`handler.go:292`. Mutations (edit, delete, pin, react) reach the room subject via
`publishMutation` → `publishRoomEvent` (`handler.go:913`), bypassing
`publishChannelEvent` entirely. So this counter counts exactly "created channel
messages on the room-subject path", which is what
`messages_canonical_published_total{broadcast_path="room_subject"}` counts
upstream. **Add a test that fails if a second caller appears** — assert the
counts in an existing handler test that exercises both creates and mutations.

**Redelivery caveat for the contract doc:** the numerator is consumer-side, so a
JetStream redelivery increments it again while the upstream denominator does not
move. The ratio can therefore exceed 1. That is the correct trade — an
outage-safe denominator is worth an over-countable numerator — but it must be
written down, and the dashboard must not clamp it silently.

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

- [ ] `messages_canonical_published_total{broadcast_path}` increments on the line
      after a successful, **non-duplicate** canonical publish, and on no failure
      before it
- [ ] `messages_canonical_publish_duplicate_total` increments when the PubAck is
      flagged duplicate; the main counter does not
- [ ] `broadcast_path` is computed with `model.IsHiddenThreadReply` and the
      **normalized** `tshow`, never `req.TShow`
- [ ] A shared table test asserts the gatekeeper's `broadcast_path` and
      broadcast-worker's dispatch branch agree, case for case (§4.1)
- [ ] A `GetRoomMeta` failure yields `broadcast_path="unknown"` and **does not
      fail the message**
- [ ] `broadcast_channel_enqueue_total{outcome}` records exactly one value per
      `publishChannelEvent` call, `ok` / `failed` — the label name and values from
      `common/sli-slo.md:188`, not `result`/`accepted`
- [ ] A test fails if `publishChannelEvent` gains a second caller
- [ ] Every label is a closed Go enum enumerated in a package-level slice
- [ ] Attribute sets are precomputed in the constructor and looked up at the
      recording site — no `metric.WithAttributes` in any recording function
- [ ] Every instrument falls back to no-op on construction error; every recorder
      is nil-safe
- [ ] **Before/after `processMessage` benchmark with `-benchmem` in the PR body**,
      cold-miss path timed separately, within the §3.2 budget
- [ ] `make test SERVICE=message-gatekeeper` and `SERVICE=broadcast-worker` pass
      with `-race`; new code is ≥80% covered
- [ ] `make lint`, `make sast` clean
- [ ] `docs/specs/o11y/nats-metrics-contract.md` and
      `o11y-metrics-inventory.md` updated, **including the three caveats**: the
      approximate PubAck-based publish count with its dedup behaviour (§4.1), the
      consumer-side numerator that can push the ratio above 1 (§4.2), and
      `unknown` as a validity signal rather than a bucket (§3.3)
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
- **Renaming anything to match `common/sli-slo.md`'s draft names.** The contract
  writes SLO-1a's numerator as `messages_persisted_total{outcome}`; the shipped
  metric is `message_worker_persistence_total{message_kind,result}`
  (`message-worker/nats_metrics.go:46`). That drift is real and worth resolving,
  but it is the contract owner's call and it would break existing dashboards.
  **Flag it in the PR, change nothing.**
- Any change to what `message_gatekeeper_messages_total` already records. The new
  counter sits beside it; the existing one keeps its current meaning, which other
  dashboards depend on.
