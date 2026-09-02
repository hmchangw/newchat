# Branch review: `claude/max-ack-pending-retries-qsi7fb`

**Date:** 2026-09-02
**Base:** `origin/main`
**Head:** `02d1fd5` — feat(bot-lane): attribute bot-message-worker failures to the sending bot
**Services touched (2):** `bot-message-handler`, `bot-message-worker`
**Also touched:** `docs/specs/o11y/nats-metrics-contract.md`. No `pkg/` changes.

## Findings by severity

| Severity | Count |
|---|---|
| critical | 0 |
| high | 5 |
| medium | 10 |
| low | 9 |
| nitpick | 6 |

Counts are deduplicated across the seven lenses: four separate reviewers raised
the `bot-room-service` envelope mismatch, and it is counted once.

## Top-line risk assessment

**The code is correct; the feature does not reach an operator.** Nothing in the
diff is unsafe, the build is clean, both service suites and the full-repo
`go test -race ./...` pass, `gosec` reports zero findings and the 19 repo-owned
semgrep rules report zero findings over 1800 files. The cardinality exception
the change takes against the metrics contract was independently traced end to
end and holds: no external actor can drive distinct `bot_account` label values.

The risk is that the change's headline deliverable — an alertable per-bot
failure series — is not queryable as shipped. `bot_msg_worker_failure_total` is
registered on the `promauto` default registry, and
`docs/specs/o11y/o11y-metrics-inventory.md` §2.3 states that those families are
**not** exposed on the SDK `:2112` endpoint and carry no `service_name`/`site`
attributes. `pkg/health/health.go:121-122` serves only `/healthz` and `/readyz`;
there is no `promhttp` handler in any service path. The metric therefore has the
same reachability as its sibling `bot_msg_worker_permanent_error_total`, which
that inventory already lists as an orphan. Consistency with an existing gap is
not the same as working.

Second-order: the branch's stated fallback ("messages published before this
change still attribute via the payload") is only half true. `bot-room-service`
publishes system messages onto the same canonical subject as a bare
`model.Message` rather than the `model.MessageEvent` the worker decodes, so
those decode to a zero-valued message and land under `unknown`. That bug
predates this branch, but the new counter absorbs it rather than exposing it.

Third: test coverage of the new code is uneven. The worker's attribution paths
are well covered by behaviour-asserting tests, but the DM handler that the diff
edited has zero tests, and the one adapter that actually attaches the JetStream
dedup id is unexercised.

None of this is a reason to revert. Items 1-3 of the action list are small and
land the feature properly.

## Verification run for this review

| Check | Result |
|---|---|
| `make lint` | 0 issues |
| `make test` (full repo, `-race`) | pass |
| `make test SERVICE=bot-message-{worker,handler}` | pass, cache cleared |
| `make sast-gosec` | pass, 0 findings |
| `make sast-semgrep-test` (rule fixtures) | 2/2 pass |
| repo-owned semgrep rules over tree | 0 findings, 19 rules, 1800 files |
| `govulncheck` | could not run — `vuln.go.dev` 403 from the environment proxy |
| `p/golang` + `p/security-audit` semgrep | could not run — `semgrep.dev` 403 from the environment proxy |

The two blocked scanners are environment limits, not results. `go.mod` is
untouched by this branch, so govulncheck has no new dependency surface to see.

---

# Service: bot-message-handler

## (a) Diff correctness

Correct and consistent. `publishCanonical` now builds the outbound message with
`natsutil.NewMsg` (`bot-message-handler/handler.go:210`), matching the repo-wide
idiom (`message-gatekeeper/handler.go:524`, `outbox-worker/main.go:91`,
`broadcast-worker/main.go:404`). The nil-header guard at `handler.go:211-216` is
real, not cargo-culted: `natsutil.HeaderForContext` returns `nil` when ctx
carries nothing (`pkg/natsutil/request_id.go:43-49`), and it returns a freshly
allocated map, so mutating it cannot alias the inbound request header. The
request id genuinely reaches ctx in production — `pkg/natsrouter/middleware.go:42-45`
stamps it before handlers run — so the new assertion in
`TestHandleSendRoom_StampsRequestIDOnCanonicalPublish` reflects real behaviour
rather than a test-only path. Consumer side matches: `bot-message-worker` reads
`model.HeaderBotIdentity` from `msg.Headers()` before the unmarshal, which is
exactly the failure mode the change exists for.

**medium** — The DM call site (`handler.go:123`) is the one edited line with zero
test coverage: `handleSendDM` is at 0.0% statement coverage, and
`handler_test.go` contains no reference to it, on this branch or on
`origin/main`. Both new tests exercise only `handleSendRoom`. The two call sites
are textually identical, so the risk is low, but the diff extends an untested
handler.

**low** — Package coverage is 42.4%, under the 80% floor in CLAUDE.md §4.
Pre-existing (`store_mongo.go` and `handleSendDM` are wholly untested), not
introduced here, but the diff does not move it.

## (b) Scope drift

Responsibilities are cohesive: parse BP headers → authz against local Mongo →
canonicalize mentions → publish one canonical event. No creep.

**low** — Duplication between `handleSendDM` (`handler.go:113-126`) and
`handleSendRoom` (`handler.go:166-185`) grew by this diff: the near-identical
`model.Message` literal plus the now-identical
`h.publishCanonical(c, &msg, c.Msg.Header.Get(model.HeaderBotIdentity))` line. A
shared `buildAndPublish(c, roomID, ident, req)` would collapse both and remove
the chance of the two paths drifting.

## (c) Abstraction changes

The `Publisher` reshape is justified. Headers cannot ride a `Publish(subj, data)`
signature, so the change was forced by the requirement. The interface stays
narrow (one method) and is defined in the consumer, per CLAUDE.md §3.

**nitpick** — `jetStreamAPI` + `JetStreamPublisher` (`handler.go:39-48`) is a
second interface plus adapter serving one call site, and the adapter itself is at
0.0% coverage, so a regression in the one line that attaches `WithMsgID` would
not be caught. (See also the Go-expert finding that the comment's stated
justification for keeping `msgID` a named parameter is now stale.)

## (d) Design coherence

Fits. The service's job is to turn an authenticated BP request into one canonical
event; carrying the authenticated identity forward on that event is the same
class of concern as the `MsgID` dedup this file already handled. Forwarding the
raw header verbatim rather than re-deriving from `msg.UserAccount` is the right
call and is correctly explained at `handler.go:192-194` — a re-encoded value
would be unavailable in precisely the decode-failure case the change targets.

## (e) Project patterns

All green: `pkg/subject` builders throughout (`handler.go:75,77,210`), no raw
`fmt.Sprintf` on subjects; `pkg/stream.BotMessagesCanonical` in `bootstrap.go:25`
behind the opt-in `BOOTSTRAP_STREAMS` gate; `pkg/idgen` for message-ID validation
and DM room IDs (`handler.go:95,246`). No JetStream consumer here (natsrouter
request/reply), so that pattern is N/A; no cross-site publish, so the outbox
pattern is correctly not involved. No new `pkg/model` event structs.

**low** — `MessageEvent.Timestamp` is set from `msg.CreatedAt` (`handler.go:200`),
i.e. the BP-supplied `X-Bot-Created-At` header, not `time.Now().UTC().UnixMilli()`
as CLAUDE.md's event-timestamp rule requires. Pre-existing and untouched by this
diff, but it means a caller controls the event-level timestamp.

## (f) Client-API doc rule

**Does not fire — no finding.** This service registers only
`chat.server.bot.request.room.{siteID}.{roomID}.msg.send` and
`chat.server.bot.request.dm.{siteID}.{userID}.msg.send` (`handler.go:74-78` →
`pkg/subject/bot.go:44,49`); neither begins with `chat.user.`, and the service
owns no `auth-service` HTTP route. The changed publish subject
`chat.bot.canonical.{siteID}.created` is an internal JetStream hop. No
request/response schema, error code, or client-facing `pkg/model` struct changed,
so leaving `docs/client-api.md` untouched is correct.

---

# Service: bot-message-worker

## (a) Diff correctness vs. service conventions

**low** — `logctx.ConsumeContext` is called inside the handler
(`bot-message-worker/handler.go:72`); every other stream consumer in the repo
calls it in the `main.go` consume loop before dispatch
(`message-worker/main.go:380`, `roomlist-worker/main.go:365`,
`outbox-worker/main.go:105`, nine more). Defensible here — it is what makes the
request-id test possible without a NATS connection — but it is the only such site
in the repo and deserves a one-line comment saying why, as
`room-worker/main.go:425` does for its own deviation.

Otherwise correct: the explicit `"request_id", natsutil.RequestIDFromContext(ctx)`
on each line matches `message-worker/handler.go:81`; the settle paths themselves
are unchanged.

## (b) Scope drift

No drift. The service is still consume → decode → Cassandra write; the additions
are observability on paths that already existed. `store.go` and
`store_cassandra.go` are untouched. Nothing here argues for a split.

## (c) The `botSender` abstraction

**nitpick** — Justified but slightly over-shaped. `senderFromHeader` earns its
place: it must run before the unmarshal for a malformed body to be attributable,
which a payload-only read cannot do. `orElse` and `label` are each three lines
with a real test. The only excess is `unknownBot` being an untyped sentinel that
can collide with a real bot account literally named `unknown`; a reserved form
(`__unattributed__`) removes the ambiguity.

(The performance lens disputes the *placement* — see that chapter. The helper
must exist; it need not run on the success path.)

## (d) Design coherence

Fits. Counting only failures (`metrics.go:32-38`) is the right call — throughput
is already on `jetstream_consumer_*`, and it keeps the vector empty for a healthy
fleet.

## (e) Project patterns

Clean: `subject.BotCanonicalCreated` (`main.go:208`),
`stream.BotMessagesCanonical` (`bootstrap.go:24`),
`stream.DurableConsumerDefaults` (`main.go:204`), `cons.Messages()` + semaphore
(`main.go:143-160`), `jsretry.Nak` with `DefaultBackoff` (`handler.go:100`), no
bare `Nak()`. No raw `fmt.Sprintf` subjects, no inline stream config.

## (f) Client-API doc rule

**Not applicable.** This worker's only subject is
`chat.bot.canonical.{siteID}.created` (`main.go:208`). No `chat.user.` handler and
no client-facing `pkg/model` struct changed — the `Publisher` interface change is
internal. No violation.

## Cardinality exception — holds today, for a reason the diff does not state

Exactly two publishers reach `BotCanonicalCreated`:
`bot-message-handler/handler.go:210` and `bot-room-service/sysmsg.go:65`. Both
derive the account from `parseIdentity`, which rejects an empty `Account`
(`bot-message-handler/handler.go:224`, `bot-room-service/handler.go:705`) and is
reachable only over the NATS-gated `chat.server.bot.request.>`. On the handler
path the header and the payload `UserAccount` are the *same* `BotIdentity` value
(`handler.go:115`, `:170`), so `orElse` cannot widen the set. The exception holds.

**high (pre-existing, not introduced here, but it silently defeats the fallback)** —
`bot-room-service/sysmsg.go:55` marshals a bare `model.Message`, while this worker
decodes `model.MessageEvent`, whose message is a nested `"message"` key
(`pkg/model/event.go:31`). A system message therefore decodes to a **zero**
`evt.Message`: `orElse("","")` leaves the sender empty and every sysmsg failure
lands under `unknown` — and `h.write` (`handler.go:113`) persists a row with an
empty `ID`/`RoomID`. `search-sync-worker/messages.go:180` decodes the same shape,
so the mismatch is real rather than a misreading. Until the publisher's envelope
is fixed, the metrics-contract note's claim that the payload fallback attributes
pre-header messages is only half true.

*Confirmed independently against primary sources during synthesis: `sysmsg.go:55`
`json.Marshal(msg)` on a `model.Message`, versus `pkg/model/event.go:29-31`
`MessageEvent.Message Message \`json:"message"\``.*

**low** — `senderFromHeader` re-parses the forwarded header without validating
`Account`; the label value is whatever BP stamped. Bounded by trust, not by code.
The contract note already names the mitigation (known-accounts cap) — worth a
`TODO` referencing it.

---

# Go expert

Both packages build clean; error wrapping, struct tags, concurrency rules and
interface placement are compliant with CLAUDE.md §3 throughout.

**high — the other producer on this subject was not migrated, and it publishes a
different wire shape.** `bot-room-service/sysmsg.go:65` publishes to
`subject.BotCanonicalCreated(siteID)` — the exact subject `bot-message-worker`
filters on (`bot-message-worker/main.go:208`) — but marshals a raw
`model.Message` (`sysmsg.go:55`), while the worker decodes `model.MessageEvent`,
which nests it (`pkg/model/event.go:31`). `json.Unmarshal` succeeds and yields a
zero `evt.Message`. The mismatch predates this branch, but this branch is where
it should have surfaced: every sysmsg now lands with no `X-Bot-Identity`, an
empty payload `UserAccount`, and is billed to `unknown` — the new counter absorbs
the bug instead of exposing it. Either migrate `bot-room-service` to the same
envelope and headers, or the attribution story covers only half the stream.

**medium — the deliberately-swallowed error is documented but untested.**
`bot-message-worker/handler.go:44` discards the unmarshal error. The doc comment
satisfies CLAUDE.md §3 ("comment if intentionally discarded"). But no test drives
it: `TestHandleJetStreamMsg_UnattributableFailureCountsAsUnknown` covers an
*absent* header, never a *malformed* one — the error path CLAUDE.md §4 explicitly
requires. Add a case with `h.Set(model.HeaderBotIdentity, "{not-json")`. Consider
also distinguishing the two in the log: a malformed header is a BP wiring bug
(`bot-message-handler/handler.go:231` treats it as `BadRequest`) and currently
vanishes with zero signal.

**medium — the `Publisher` interface's stated justification is now stale.**
`bot-message-handler/handler.go:31-37`. `jetstream.WithMsgID` only sets the
`Nats-Msg-Id` header on the message you hand it (nats.go
`jetstream/publish.go:187`). Once the parameter is a `*nats.Msg`, a fake can
assert the dedup id off `msg.Header` — so the "pubOpts is unexported" rationale,
and with it the whole `Publisher`/`JetStreamPublisher` indirection over
`jetStreamAPI`, no longer earns its keep. The shape is fine; the comment argues
for something that stopped being true. Related: nats.go mutates the caller's
`Header`, and the fake stores it by reference (`handler_test.go:66`) while
defensively copying `Data` — copy both.

**low — the request id is returned, discarded, then re-derived four times.**
`bot-message-worker/handler.go:72` does `ctx, _ =`; lines 82, 94, 101 and 108
then each call `natsutil.RequestIDFromContext(ctx)`. Bind it once.

On the broader question: the repeated `"request_id", natsutil.RequestIDFromContext(ctx)`
key *is* house convention (`message-worker/handler.go:113`,
`broadcast-worker/handler.go:209`) — do **not** hoist a `slog.Default().With(...)`
logger, that would diverge from every sibling. The 4× `botID`/`botAccount` pair
is new to the repo and is the part actually worth hoisting, but only if bound
alongside the id.

**low — naming divergence is acceptable as-is.** `PublishMsgWithID` versus
`bot-room-service/handler.go:54`'s `PublishWithMsgID`. Each is a consumer-defined
interface per CLAUDE.md §3, and the new name correctly mirrors nats.go's
`Publish`/`PublishMsg`. Not worth churning; converging them falls out of fixing
the high finding above.

**nitpick — `orElse` reads as Scala.** `bot-message-worker/handler.go:52`. The
value receiver returning a copy is the right call (don't mutate), but name it
`fillFrom`/`withFallback`, and take `*model.Message` — two adjacent `string`
parameters invite a silent argument swap. Zero-value semantics are adequately
documented.

**nitpick — duplicated nil-header guard.** `bot-message-handler/handler.go:212-214`
reimplements the guard `natsutil.NewMsgEncoded` already owns
(`pkg/natsutil/request_id.go:82-85`). A `natsutil.NewMsgWithHeader` would keep the
"NewMsg returns nil Header" quirk in one place.

---

# Test automation

Both suites pass (`make test SERVICE=…`) and the full-repo `go test -race ./...`
is green. No `store.go` appears in the diff, so the mock-staleness check does not
apply — `make generate` was not run.

**high — `JetStreamPublisher.PublishMsgWithID` has no test at all**
(`bot-message-handler/handler.go:46`, 0.0% coverage). The diff changed both the
`Publisher` interface and this adapter from `Publish`/`PublishWithMsgID` to
`PublishMsg`/`PublishMsgWithID`. Every new test asserts against `fakePublisher`
(`handler_test.go:59`), so the only production code that actually converts a
`*nats.Msg` + msgID into a JetStream call is unexercised — dropping
`jetstream.WithMsgID(msgID)` there would kill JS-layer dedup and no test would
fail. `jetStreamAPI` (`handler.go:39`) is already an interface field, so a
15-line fake makes this testable. TDD violation per CLAUDE.md §4.

**high — the DM path's new identity stamping is untested**
(`bot-message-handler/handler.go:123`; `handleSendDM` at `:81` is 0.0% covered).
The diff added the `c.Msg.Header.Get(model.HeaderBotIdentity)` argument to both
call sites but only `handleSendRoom` got tests (`handler_test.go:382`, `:405`).
`handleSendDM` has zero tests in the whole file. A DM sent by a bot would lose
attribution silently.

**medium — malformed `X-Bot-Identity` is the one branch the feature exists for,
and it is uncovered** (`bot-message-worker/handler.go:44-46`, the
`json.Unmarshal` error return; confirmed uncovered by coverprofile). Tests cover
header-present-and-valid (`handler_test.go:221`) and header-absent (`:246`), never
present-but-undecodable. That path must still count under `unknown`/`malformed`
rather than panic or mislabel.

**medium — `orElse` partial fill is untested** (`bot-message-worker/handler.go:52-61`).
Both existing cases are all-header (`handler_test.go:233`) or all-payload
(`:195`). The mixed case — header carries `ID` but an empty `Account`, so
`label()` must fall through to the payload account — is exactly where the two
sources interleave and is never asserted. `orElse` shows 100% statement coverage
only because each `if` body is hit by a different test; the combination is not.

**medium — the six new worker tests are one logic shape and should be a table**
(`bot-message-worker/handler_test.go:195-269`). Each varies only
(payload, headers) → (expected account label, outcome, delta). CLAUDE.md §4
"Table-Driven Tests" prefers a table here; as written, adding a seventh outcome
means a seventh copy-pasted ten-line function.

**medium — the delta approach is order-independent but not parallel-safe, and two
tests share a label pair.** `TestHandleJetStreamMsg_TransientFailureCountsAgainstSender`
(`:199`) and `TestHandleJetStreamMsg_SuccessRecordsNoFailure` (`:262`) both key on
`("payload.bot","nak")`, one asserting delta 1 and the other delta 0.
Sequentially this is sound; `botFailureTotal` children are atomic, so adding
`t.Parallel()` would produce a silent flake rather than a `-race` report — the
worst failure mode. Fix by deriving the account label from `t.Name()` so each test
owns its own series; parallelism is then free and the before/after snapshot
becomes redundant.

**low — `f.lastCtx = ctx` is an unsynchronized write**
(`bot-message-worker/handler_test.go:35,45`) while sibling counters use `atomic`.
Harmless today (one handler call per test) but inconsistent, and it becomes a real
race the moment anyone parallelizes or fans out the fake.

**nitpick — `failureCount` materializes a zero series as a side effect**
(`handler_test.go:190-193`): `WithLabelValues` creates the child before reading
it, so snapshotting a never-hit label pair adds phantom series. Use
`testutil.ToFloat64` on a `GetMetricWithLabelValues` result, or accept it and note
why.

**Assertion quality (positive).** The worker tests assert real behaviour, not mock
behaviour: the Prometheus counter is the production `botFailureTotal`, and
`TestHandleJetStreamMsg_StampsRequestIDFromMessageHeader` (`:271`) proves
propagation through the real `logctx.ConsumeContext` by capturing the ctx the
store actually received. `TestHandleJetStreamMsg_HeaderIdentityWinsOverPayloadAccount`
(`:233`) pins the precedence rule the code comment claims. The handler-side tests
are weaker — they only prove the fake was handed the right header.

---

# Bug & security

Build clean, both services' unit tests pass, `make sast-gosec` = 0 findings,
repo-owned semgrep = 0 findings over 19 rules and 1800 files. `govulncheck` and
the remote semgrep rulesets could not run in this environment (proxy-blocked) and
are not reported as findings.

## Cardinality DoS — answered concretely: bounded today, unvalidated by construction

Traced end to end. `bot_account` resolves from `X-Bot-Identity`, which
`bot-message-handler/handler.go:181,215` forwards verbatim; its only origin is
`botplatform-service/bot_forwarder.go:62,116` and `dm_ensurer.go:43`, which build
it from `sess.Account` — a Mongo-backed session record
(`botplatform-service/handler.go:140`, `Account: u.Account`), never a client
field. Publish rights on `chat.server.bot.request.>` are unreachable from chat
clients: the scoped user template grants pub only on
`chat.user.{{tag(account)}}.>` and `chat.user.presence.*.query.batch`
(`docker-local/setup.sh:68-73`). Publish rights on
`chat.bot.canonical.{siteID}.created` are held by backend credentials only
(`bot-message-handler`, `bot-room-service/sysmsg.go:65`). **No external attacker
can drive distinct label values.** The exception documented in
`docs/specs/o11y/nats-metrics-contract.md` is materially correct.

**medium — nothing validates the label value itself.**
`bot-message-handler/handler.go:235` checks only that `Account` is non-empty;
`FindSubscription` validates `ident.ID`, never `Account`.
`bot-message-worker/handler.go:63-68` then uses that free-form string as a
Prometheus label with no length cap, charset check, or known-account bound. A BP
bug, or a compromise of any backend-credentialed publisher, yields unbounded and
unrecoverable series. Cheap fix: cap length and allowlist the charset, or bucket
unrecognised accounts to `other`.

**medium — the payload fallback is a strictly weaker source than the header.**
`bot-message-worker/handler.go:87`, `sender.orElse(m.UserID, m.UserAccount)`,
takes the label from an unauthenticated JSON body field. Stating the trust
question plainly: for the *header* the worker's trust is equal to the handler's
(re-stamped after `parseIdentity`), but the *fallback* is not — it is a payload
field on a stream that carries at least one publisher (`sysmsg.go`) that never
stamps the header at all. The metric's guarantee is therefore "bounded by BP's
session records **or** by whatever any canonical publisher wrote in the body".

## Other findings

**medium — `bot-room-service/sysmsg.go:55,65` publishes a bare `model.Message`
onto the same subject the worker decodes as `model.MessageEvent`**
(`bot-message-worker/handler.go:77-78`). Pre-existing, not introduced here, but it
defeats this branch: the mismatch does not error, it decodes to a zero
`MessageEvent`, so the worker writes an empty message and the new counter never
fires — `orElse` sees `""`/`""` → `unknown`.

**low — `senderFromHeader` swallows the unmarshal error silently**
(`bot-message-worker/handler.go:44-46`). A malformed BP header degrades
attribution with zero signal; the handler treats the same condition as
`BotInvalidHeader` (`handler.go:231`). Count it under an outcome, or log once.

**nitpick — `unknown` (`metrics.go:22`) can collide with a real bot account of
that name.** Prefix it (`__unattributed__`).

**nitpick — `bot-message-worker` never calls `logctx.SetupDefault`/`Configure`,**
so the X-Debug rung that `ConsumeContext` now admits can never emit.

## Cleared

- **No secret or body leakage.** `logctx.ConsumeContext` → `CapturePayload` is
  double-gated (`pkg/logctx/limiter.go:86-90`): `capturePayloads` defaults false
  and this service never calls `Configure`, so payload capture is inert. The new
  log lines carry `botID`/`botAccount`/`request_id` only — no tokens, no bodies.
- **No nil-pointer risk.** `nats.Header.Get` is nil-safe (`nats.go:4134`), and
  `jetstream.PublishMsg` initialises a nil `Header` before `Set`
  (`jetstream/publish.go:173`), so the "NewMsg returns nil Header" path at
  `bot-message-handler/handler.go:210-216` is safe on both branches.

---

# Performance

Measured on the branch with a throwaway benchmark in `package main` of
bot-message-worker (Xeon @2.8 GHz; benchmark since deleted, tree clean):

| bench | ns/op | B/op | allocs |
|---|---|---|---|
| `senderFromHeader` (167-B header) | 2801 | 592 | 13 |
| `logctx.ConsumeContext` (inbound req-id) | 268 | 64 | 2 |
| `logctx.ConsumeContext` (minting) | 438 | 128 | 4 |
| `botFailureTotal.WithLabelValues(...).Inc()` | 63 | 0 | 0 |
| body `json.Unmarshal` (218-B event) | 3566 | 760 | 13 |

**low — eager header parse on every message, consumed only on failure**
(`bot-message-worker/handler.go:75`, helper at `:38`). `senderFromHeader` costs
2.8 µs / 592 B / 13 allocs per consumed message — about 78% of the body unmarshal
— paid on 100% of traffic, while `sender` is read only in the three failure
branches (`:79`, `:91`, `:98`) and the ack-failed log. **The comment at `:73`
claims the parse must precede the unmarshal for malformed-payload attribution; it
does not.** `msg.Headers()` is still readable after the decode fails, so calling
`senderFromHeader` inside the `if err := json.Unmarshal` branch (and once after
`orElse` in the write-error branch) attributes exactly the same and costs nothing
on the success path. `orElse` at `:87` is likewise dead work on success.

Absolute impact is small — at 5k msg/s that is ~14 ms CPU/s (1.4% of one core) and
~3 MB/s of garbage, against a Cassandra `LocalQuorum` write measured in
milliseconds (~0.1% of per-message wall time) — but the fix is moving two lines,
so the cost/benefit as written is clearly negative.

**negligible — `logctx.ConsumeContext`** (`handler.go:72`). 268 ns / 64 B on the
normal path; bot-message-handler now always stamps `X-Request-ID`, so the 438 ns
UUIDv7 minting path is the exception rather than the rule. `Admit` is two header
lookups; `CapturePayload` short-circuits in `ShouldCapture` because
`logctx.capturePayloads` is false by default and this service never calls
`logctx.Configure`, so no payload copy or string conversion ever happens. Under
0.02% of per-message cost.

*Non-performance note carried from this lens:* because `Configure` is never
called, the limiter is `denyAll`, so the X-Debug rung this change now admits is
never actually honored in this service. The request-id half works. (Cross-listed
in Observability.)

**negligible — `WithLabelValues`** (`metrics.go:31`, three call sites). Confirmed
failure-only: no increment on the success path, which
`TestHandleJetStreamMsg_SuccessRecordsNoFailure` pins. 63 ns, zero allocations;
even in a full Cassandra outage NAKing every message, that is noise beside the
write attempt and the Warn log.

**negligible — `*nats.Msg` + header map per publish**
(`bot-message-handler/handler.go:210-215`). One to three allocations on a
request/reply path that already does a Mongo find, a JSON marshal and a JetStream
publish RTT under a 2 s timeout. Forwarding the identity as the raw header string
rather than re-marshalling from the message is the right call.

**Note, pre-existing, not a finding — `encoding/json` vs sonic.**
bot-message-worker was already on `encoding/json`; this diff adds a second decode,
taking JSON from ~3.6 µs to ~6.4 µs per message. Still under 1% of per-message
wall time, and fixing the finding above removes most of the increment, so adopting
sonic here is not justified — especially given the `cassandra.Message.Reactions`
struct-keyed map that already forced a workaround in message-gatekeeper.

**Note, pre-existing, untouched** — `canonicalizeMentions` does one `FindUser` per
mention (`bot-message-handler/handler.go:302`), an N+1 bounded by mention count.
Not made worse by this diff.

No goroutine leaks, no new blocking calls in the consume loop, no batching or
pagination regressions: `main.go`'s semaphore + WaitGroup pool and the shutdown
ordering are unchanged, and everything added to the per-message path is CPU-only.

---

# Observability

**high — the metric is not scraped anywhere.** `bot-message-worker/metrics.go:32`
registers on the `promauto` default registry, but `bot-message-worker/main.go:77`
calls `obs.Init` and the health server (`pkg/health/health.go:121-122`) serves only
`/healthz` and `/readyz`; the only `promhttp` handler in the repo is
`tools/loadgen/metrics.go:731`. `docs/specs/o11y/o11y-metrics-inventory.md:232-243`
states that these promauto families are **not** on the SDK `:2112` endpoint and
carry no `service_name`/`site` attributes — and lists this file's sibling
`bot_msg_worker_permanent_error_total` as one of them.
`search-service/metrics.go:22-26` is the precedent that migrated off
`client_golang` for exactly this reason. As shipped, an on-call engineer cannot
query the series at all, and with no `site` label it would be ambiguous across
sites even if it were scraped.

*Verified independently against primary sources during synthesis.*

**high — §13.4 rule 1 is not satisfied.** No dashboard or alert consumes the
metric: nothing under `tools/observability/grafana/dashboards`, nothing in §11
Required Alerts. The Read-by cell (`nats-metrics-contract.md:834`) is a prose
argument, the same shape §13.3 already flags as provisional for
`preview_warmback_*` ("if the honest answer is that nothing reads these, §13.4
step 1 applies"). The honest answer here is "nothing yet". Ship a panel or alert
with it, or hold the metric.

**medium — the exception is defensible but unenforced and split across docs.** The
argument (bounded provisioned set, failure-path-only emission, named trip-wire if
provisioning becomes self-service) is sound and correctly placed beside the row.
But the guard it claims exception from never applies:
`.semgrep/metrics.yml:110-114,198` taints only
`metric.WithAttributes`/`attribute.NewSet`, and
`pkg/obs/instrument_registry_test.go:28-34` matches only OTel constructors — a
promauto label slice trips neither. So the row is unenforced. Its sibling
`bot_msg_worker_permanent_error_total` also lives in the *other* document
(`o11y-metrics-inventory.md:243`), whose "Seven application metrics" (`:234`) is
now stale at eight.

**medium — `obs.ContextWithIdentity` is missing and should be added.** Siblings
call it at the identical position: `message-worker/handler.go:100`,
`broadcast-worker/handler.go:189`, `notification-worker/handler.go:123`,
`roomlist-worker/main.go:375`. Add
`ctx = obs.ContextWithIdentity(ctx, sender.Account, m.RoomID, evt.SiteID)` after
`handler.go:84`. This matters more here than elsewhere: the metric deliberately
omits room and message id, so the span is where they belong — and this worker
currently emits neither.

**medium — `ConsumeContext` adopted, but only a third of it works.**
`handler.go:72` stamps the request id, yet `main.go:77` uses `obs.Init` rather
than `obs.InitWithLoggerHandler(ctx, logctx.LevelTrace, logctx.NewHandler)` and
the config declares no `logctx.Config \`envPrefix:"DEBUG_LOG_"\`` (compare
`message-worker/main.go:71,75,83,109`). So `Admit`'s X-Debug rung is inert and
`CapturePayload` is permanently off (`pkg/logctx/limiter.go:78`).

**medium — remaining propagation gap: a second producer.**
`bot-room-service/sysmsg.go:65` publishes to `BotCanonicalCreated` with no headers
— no `X-Request-ID` (the worker mints a fresh one, breaking the chain) and no
`X-Bot-Identity`. Worse, it marshals a bare `model.Message`, not
`model.MessageEvent` (`pkg/model/event.go:29-31`), so the worker decodes a zero
`Message` and the payload fallback (`handler.go:85`) yields nothing — every such
failure lands in `bot_account="unknown"`. The envelope mismatch is pre-existing;
the new metric surfaces it. botplatform → handler → worker is otherwise complete
(`botplatform-service/bot_forwarder.go:124` → `natsrouter` `RequireRequestID` →
`bot-message-handler/handler.go:210`).

**low — field-name mixing is pre-existing.** `botAccount` already exists at
`room-worker/handler.go:2210,2220`; `messageID`/`roomID` match the file's prior
lines; `request_id` is the repo-wide form (165 sites). No new inconsistency is
introduced.

**low — doc drift:** `pkg/logctx/consume.go:24-27` still lists bot-message-worker
as stamping "nothing at all".

**nitpick — help text.** `metrics.go:35` explains the outcome enum well but omits
the reserved `unknown` value and the fact that successes are uncounted, so no
ratio is derivable from this family alone. Both facts currently live only in code
comments (`:22-25`, `:29-31`); move them into `Help`.

**Clean:** no tokens, passwords or message bodies are logged; `slog` JSON
discipline holds throughout.
