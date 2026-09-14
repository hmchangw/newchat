# message-gatekeeper — Production Readiness Review

**Service:** `message-gatekeeper` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.3 / 5

The first hop on every user message, and the hot path is genuinely well-engineered: precomputed metric attribute sets, L1+L2 caches with singleflight, precise Mongo projections, correct semaphore consumer pattern, sonic + `Pretouch`, clean `jsretry` discipline. Excluding `main.go` the package is ~91% covered. Three things stand out. **The consumer binds MESSAGES with no `FilterSubjects`, so every verb under `msg.>` is processed as a create** — a client publishing `msg.edit` today is validated as a send and republished to the canonical `.created` subject. **The parent-resolution path has no overall deadline** — a reply that quotes a different parent can hold a worker slot for ~6 s. And the derived client-API view **contradicts the canonical doc** on bot-DM fan-out.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 4 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 7 | 22 | 15 | 6 | **50** |

---

## 2. Go code quality — 4 / 5

Disciplined error tiering, typed `errcode` usage and zero string-matching on errors; marred by four ctx-less `slog` calls on the per-request path, two "log AND return" violations, and two unwrapped error returns.

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | **Log-and-return double-logs the large-room rejection**: `slog.Info("send blocked")` fires, then the same error reaches `errcode.Classify`, which logs `"request failed"` at Info for a `Forbidden`. **Two Info lines per blocked send** | `handler.go:425`, returned `:432`, marshalled `:211`; `pkg/errcode/classify.go:40`, `:49` |
| medium | Same double-log on the invalid-subject path | `handler.go:160`, marshal at `:164` |
| medium | **Four per-request logs use the ctx-less `slog` variants**, dropping the request/trace correlation the surrounding code just built — `ctx` is enriched with `request_id`/`account` at `:145` and `room_id` at `:173`. Two of them land in Loki with **no `request_id` at all** — precisely the two lines an operator would want to join to a failing send. Sibling calls in the same file use the `*Context` forms, so this is inconsistency, not house style | `handler.go:160`, `:298`, `:425`, `:472` |
| low | **Unwrapped error escapes `processMessage`**: `return sonic.Marshal(msg)` hands the caller a bare sonic error, which is then classified as an *infra* failure and **NAKed for redelivery — even though a marshal failure is permanent.** The immediately preceding marshal wraps correctly. This is the only such tail-return in any service handler repo-wide | `handler.go:519` vs `:507` |
| low | Bare `return nil, err` from the cache constructor — the message survives only because the caller happens to supply one | `metacache.go:21` |
| low | Audit-coverage gap (environmental): `govulncheck` + registry packs blocked | — |
| nitpick | Unchecked type assertion inside a JetStream worker goroutine — safe today, but a panic here takes down a `MaxWorkers` goroutine; `interface{}` instead of `any` on a Go 1.25 module; an empty `default:` clause; `accountFromSubject` computed twice on the rejection path | `subcache.go:111`, `:81`; `handler.go:251`, `:147`, `:164` |

**Verified clean:** no `fmt.Println`/`log.Println`; no `errors.Is`-by-string; no `WithCause` chaining an `*errcode.Error`; **no token/password/body logging** — the flow breadcrumbs carry sizes and coarse tags only, and `:180` **deliberately declines `WithCause(parseErr)` to keep the offending substring out of the log**; `WithReason` used only where the frontend must branch; infra failures returned as raw `fmt.Errorf`; `errCanonicalPublish` is a sentinel matched by `errors.Is`, not text. **The documented sonic exception at `fetcher_history.go:53` is correctly scoped** — the projection omits `Reactions` and is correspondingly excluded from `pretouch.go`.

### Recommendations
- `medium` — Delete the two `slog` calls that precede a returned error; move any field worth keeping into `errcode.WithLogValues(ctx, …)` so `Classify` emits them on its single line.
- `medium` — Convert `handler.go:298` and `:472` to the `*Context` variants so the enriched `request_id`/`room_id`/trace ride the line.
- `low` — Wrap the tail marshal as `errcode.Permanent` preserving the marshal cause, so a permanently unmarshalable message **Acks instead of burning `MaxDeliver` NAKs** — `errcode.Internal` would not, since only `IsPermanent` drives the Ack-drop; wrap `metacache.go:21`.
- `nitpick` — Make the singleflight assertion comma-ok with a defensive fallback; switch `interface{}` → `any`.

---

## 3. Architecture — 4 / 5

Boundaries, DI, bootstrap gating and the high-throughput consumer pattern are all correct and well-documented; the deductions are an unfiltered client-facing consumer, per-service re-declaration of shared cache knobs, and a constructor that has outgrown positional DI.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | **The consumer binds MESSAGES with no `FilterSubjects`**, so every verb under `chat.user.*.room.*.{siteID}.msg.>` is processed as a create. `buildConsumerConfig` sets only `Durable`; the stream captures `msg.>`, and `subject.ParseUserRoomSiteSubject` returns `ok` from the first four tokens, **never inspecting the trailing verb**. A client publishing `…msg.edit` today is validated as a send and republished to `chat.msg.canonical.{siteID}.created`. `CLAUDE.md` explicitly reserves `.edited`/`.deleted` "for future", so **the gate will silently mis-route them the day they exist.** Fix is one line | `main.go:269-275`; `pkg/stream/stream.go:18`; `pkg/subject/subject.go:113-118` |
| medium | **L1 cache knobs re-declared per service** — `ROOM_META_CACHE_TTL`, `USER_CACHE_SIZE`, `USER_CACHE_TTL` carry their own tag + `envDefault` here and again in four sibling services. **The L2 siblings already do it right** (`RoomMetaL2 roommetacache.TTLConfig`, `UserL2 userstore.TTLConfig`); the L1 tier never got it. The consequence is the documented one: two services reading the same data drift apart on a default nobody notices | `main.go:52-53`, `:58-59` |
| medium | `NewHandler` takes **10 positional parameters**, four of which are policy scalars (`largeRoomThreshold, maxAttachments, maxAttachmentBytes, chatBaseURL`) — call-site-indistinguishable ints and strings. The options pattern is **already present** in the same file | `handler.go:107`, `:82-102` |
| medium | The gate performs a **synchronous cross-service RPC plus a timed re-check inside the JetStream worker slot**: quote and thread-parent resolution issue a 2 s NATS request to history-service, and a missing thread parent adds a 150 ms retry. **Ingest availability is therefore coupled to history-service latency** — each in-flight send holds a `MaxAckPending` slot for up to ~4.2 s. Architecturally this is *enrichment* work living in the *validation* gate | `fetcher_history.go:82`; `handler.go:547-561` |
| low | MESSAGES-CANONICAL is **not exclusively produced by this service**, so the invariants enforced here (20-char message ID, 20 KB content cap, attachment caps) apply only to the client lane. Legitimate for system messages, but "gatekeeper validates → canonical" is not the whole truth | `room-worker/handler.go:533`, `:732`, `:1260`, `:1981`; `room-service/handler_teams.go:269` |
| low | Subject-shape knowledge duplicated outside `pkg/subject`: `accountFromSubject` re-implements the `chat.user.{account}.…` split with a raw `strings.Split`, existing only because `ParseUserRoomSiteSubject` is all-or-nothing | `handler.go:305-311` |
| nitpick | Dangling reference to a file that does not exist (`see doc.go`) | `handler.go:179` |

**Verified correct:** `Store` is consumer-defined with exactly the two methods used; `bootstrapStreams` sets only `Name + Subjects` from `pkg/stream`, verifies-and-fails-fast when disabled, matching the repo-wide shape; the high-throughput pattern is intact (`cons.Messages()` + `PullMaxMessages(2*MaxWorkers)` + semaphore/WaitGroup), never mixed with `Consume()`; shutdown order is correct under `shutdown.Wait`; publish/reply injected as fields; zero `os.Getenv`.

### Recommendations
- `high` — **Set `FilterSubjects` to the `msg.send` pattern on the durable** (or reject non-`send` verbs before `processMessage`), and add a `pkg/subject` parser that returns the verb.
- `medium` — Move the `USER_CACHE_*` / `ROOM_META_CACHE_*` L1 knobs into `userstore` / `roommetacache` as mounted config structs, mirroring the existing `TTLConfig` fields.
- `medium` — Collapse the four policy scalars into a `sendPolicy` struct or handler options.
- `medium` — Extract quote/thread-parent resolution behind a `parentResolver` type so the gate is validate-and-publish and the enrichment coupling is isolated and separately testable.
- `low` — Add `subject.AccountFromUserSubject` and delete the local helper; fix the `doc.go` reference.

---

## 4. Test coverage — 2 / 5

**65.5% (444 statements)**, below the §4 80% floor. The gap is **one file**: `main.go` is 126/444 statements at **2.4%**, while every other file is 87–100% (`handler.go` 92.6%, `subcache.go` 96.9%, `bootstrap.go` 100%). **Excluding `main.go` the package is ~91%.**

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | 65.5%, under the §4 80% floor | `handler.go:1` |
| high | The gap is `main.go` at 2.4%. The one piece of it with real logic and a real failure mode — **the `shutdown.Wait` worker-drain closure that converts a `wg.Wait()` overrun into `worker drain timed out`** — is untested | `main.go:247-256` |
| medium | **The NAK test double cannot distinguish a compliant backoff from a forbidden bare Nak**: `fakeJSMsg.Nak()` and `NakWithDelay(d)` both just set `naked = true` and discard `d`. The retry-outcome test asserting `msg.naked` **would stay green if `jsretry.Nak` were replaced with `msg.Nak()`** — precisely the regression `CLAUDE.md` says burns `MaxDeliver` in milliseconds | `handler_test.go:2170-2171` |
| medium | `gatekeeperReason` is 33.3% covered: only `invalid_subject` is asserted anywhere. **The `not_subscribed`, `room_restricted` and `invalid_payload` mappings — the labels operators alert on for the two validation rejections this service exists to make — are never exercised**, so an `errcode` reason rename silently collapses them to `unknown` | `handler.go:245`; `nats_metrics_test.go:77` |
| medium | `metacache.go` is 0% yet is production wiring. **Nothing tests that `cachedMetaStore` actually puts the L1 in front of `GetRoomMeta`** — the struct embeds `w.S` and overrides with `c.cache`, so an embed-only mis-wiring would bypass the cache and pass compilation. Its sibling `newCachedSubStore` has 11 tests | `metacache.go:18`, `:27`; `main.go:154` |
| medium | **The sonic wire-compat test is decode-only.** Nothing pins the two *encode* sites: the canonical `MessageEvent` — **consumed off MESSAGES-CANONICAL by `search-sync-worker`, which is not on the sonic list and decodes with `encoding/json`** — and the client reply. `broadcast-worker` has the `SemanticEquivalence` + `CrossCodecRoundTrip` pair this file is missing | `sonic_wire_test.go:18`; `handler.go:505`, `:519` |
| low | `store_mongo.go` is 66.7%: the `fmt.Errorf` wrap paths are only reachable under the `integration` tag, so the unit run **never proves `errNotSubscribed` survives the wrap for `errors.Is`** | `store_mongo.go:41`, `:61` |
| low | `debugFlowReceived` is 33.3% — only the disabled fast path is covered; the `stream_wait_ms` derivation and the `Metadata()` error fallback are untested in both suites | `handler.go:263` |
| low | All four `failed to ack message` branches are unreachable in tests because `fakeJSMsg.Ack()` always returns nil | `handler_test.go:2168` |
| nitpick | Three tests mutate the global `slog.SetDefault`; restored via cleanup and safe only because no test calls `t.Parallel()` | `debug_log_test.go:73`; `handler_test.go:2605`, `:2752` |

**Compliant:** mocks mockgen-generated; no real DB/NATS in unit tests; publish and reply funcs injected as fields; integration files carry `//go:build integration` with `TestMain → testutil.RunTests`, use `testutil.MongoDB`/`SharedValkeyCluster` with `t.Cleanup(FlushValkey)`, and no inline `GenericContainer`.

### Recommendations
- `high` — Extract the testable body of `main.go` into `run(ctx, cfg) error` (or per-concern `wireStore`/`buildIter` helpers) and unit-test the drain-timeout closure; **that alone moves the package over 80% without a single new container.**
- `medium` — Make `fakeJSMsg` record the delay (`nakDelay time.Duration`, plus a distinct `bareNak bool`), then assert `nakDelay > 0` in the two retry subtests so a regression to a bare Nak fails the build.
- `medium` — Add a table-driven `TestGatekeeperReason` over all four inputs asserting the exact label constants.
- `medium` — Add `TestNewCachedMetaStore_*`: a hit/miss pair proving the inner store is called once for two `GetRoomMeta` calls, plus the invalid size/ttl error — mirroring `subcache_test.go`.
- `medium` — Extend `sonic_wire_test.go` with a cross-codec test (sonic-marshal a `MessageEvent` with HTML metacharacters → `encoding/json.Unmarshal` → assert equality with the stdlib round trip), covering both encode sites.
- `low` — Give `fakeJSMsg` an injectable `ackErr`; add a `debugFlowReceived` test under a `logctx`-enabled context.

---

## 5. Maintainability — 3 / 5

Dependencies are cleanly injected and the WHY-comments are genuinely excellent, but the core `processMessage` has outgrown a single function, `NewHandler`'s 10-positional signature is pinned by 28 call sites, and several doc comments have drifted from the code they describe.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | **`processMessage` is 205 lines with ~30 decision points**, mixing eight input-validation rules, subscription auth, large-room policy, quote resolution, thread-parent resolution, display-name enrichment, event construction and the canonical publish in one body. **This is also why the 65.5% coverage clusters here** | `handler.go:316-520` |
| high | **`NewHandler` takes 10 positional parameters, three of them adjacent bare `int`s** (`largeRoomThreshold, maxAttachments, maxAttachmentBytes`). **28 call sites pass `500, 1, 8192` as literals** — any two of the three can be transposed with **no compile error**, and adding an 11th knob is a 28-site edit. The `gatekeeperHandlerOption` machinery already exists but carries only two fields | `handler.go:107`; `handler_test.go:1111`…`:2905` |
| medium | **`ParentMessageFetcher`'s doc comment is stale**: it states implementations' errors are all soft-failed and "the handler … ships the message without the quote". The handler has since **tiered** errors — terminal ones *reject* the send. A reader implementing a second fetcher against this comment **would get the failure semantics backwards** | `store.go:79-85` vs `handler.go:612-621`, `:563-578` |
| medium | Three validation comments and **three client-facing error strings** claim message IDs "must be a 20-char base62 string", but `idgen.IsValidMessageID` accepts **17 or 20**, as `CLAUDE.md` requires. **The error text reaches clients**, so a legacy 17-char ID that validates fine is described by a rule that would have rejected it | `handler.go:335-337`, `:341`, `:348`; `pkg/idgen/idgen.go:96` |
| medium | `HandleJetStreamMsg` repeats the same Ack-and-log block **four times** and the reject triad **three times** — every new rejection reason is four copy-pasted lines that must stay in sync | `handler.go:165-167`, `:183-185`, `:212-214`, `:240-242` |
| medium | `handler_test.go` is 2,913 lines, of which `TestHandler_ProcessMessage` alone spans ~1,050; 25 sibling tests each rebuild a controller + handler by hand, and only `threadReplyHarness` factors setup | `handler_test.go:51-1101`, `:2434` |
| low | **File organisation has drifted from its own names**: `nats_metrics.go` holds the *domain* outcome metrics rather than NATS metrics; `metrics_test.go` tests `cachedSubStore`; and `config_test.go`/`consumer_config_test.go`/`debug_log_test.go` test code that lives in `main.go` with **no matching source file**. "Where does this test go?" has no answer a newcomer can derive | `nats_metrics.go:42`; `metrics_test.go:23`; `config_test.go:28` |
| nitpick | No complexity linter is enabled, so the first finding cannot regress-fail in CI; two operational timeouts are hardcoded consts while every other knob is env-driven | `.golangci.yml:5-12`; `subcache.go:18`; `fetcher_history.go:150` |

### Recommendations
- `high` — Split `processMessage` into three: a **pure, dependency-free** `validateSendRequest(req, limits) *errcode.Error` (lines 317–393, directly table-testable), an `authorize(...)` covering subscription + large-room gate, and an `enrich`/`publish` tail. Fold the three near-identical ID checks into one loop over `{field, value}` pairs.
- `high` — Replace `NewHandler`'s positional tail with a `handlerDeps`/`handlerLimits` struct (or options). Give tests one `newTestHandler(t, opts…)` builder and **delete the 28 literal `500, 1, 8192` sites**.
- `medium` — Fix the stale `ParentMessageFetcher` contract comment to state the terminal-vs-transient tiering, and correct the "20-char" wording in the three ID messages to "17- or 20-char base62".
- `medium` — Extract `ackOrLog(ctx, msg, requestID)` and `rejectAndAck(...)`; `HandleJetStreamMsg` then reads as three one-line branches.
- `medium` — Break `TestHandler_ProcessMessage` into per-concern tables matching the `processMessage` split, routing all setup through the existing harness shape.
- `low` — Rename `nats_metrics.go` → `metrics.go`, move the cached-store tests into `subcache_test.go`, and lift `config` + `buildConsumerConfig` out of `main.go` into `config.go` so the orphan test files have real counterparts. Enable `gocyclo`/`cyclop` with a threshold near 15 and a short baseline exclusion, to lock the refactor in.


---

## 6. Integration — 3 / 5

Subject builders, stream/consumer wiring, `Timestamp` and `idgen` validation are all correct and test-asserted. The deductions are documentation drift on the one contract this service owns — the client `msg.send` RPC — plus two undocumented client-facing error surfaces and a silently-swallowed dedup suppression.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | **The derived view contradicts the canonical doc on `msg.send` fan-out.** It states "botDM rooms receive no `new_message` fan-out — `broadcast-worker` skips botDM types". The canonical doc and the code say the opposite: `broadcast-worker` routes `RoomTypeBotDM` through `publishDMEvents` and delivers to the non-bot member. **A client reading the derived view will not subscribe for bot-DM messages** | `docs/client-api/request-reply.md:2278` vs `docs/client-api.md:6469`, `broadcast-worker/handler.go:915`, `:295` |
| high | The same derived view **omits the `type` field from the `msg.send` request body table**, although it is a real client-settable field on the request struct, validated here and documented canonically. Its "Emits" line also omits `new_thread_message`, which the events view documents as the thread-reply variant | `docs/client-api/request-reply.md:2283-2291`, `:2320`; `pkg/model/message.go:79`; `handler.go:381`; `docs/client-api.md:6360`; `docs/client-api/events.md:485` |
| medium | **Three client-facing `bad_request` errors are unreachable from the docs** — `too many attachments: max %d`, `attachment[%d] must not be empty`, `attachments exceed maximum size of %d bytes`. None appears in the §4 error table; the caps live in a field note only, so a client cannot map the wire string to a cause | `handler.go:368`, `:373`, `:378` vs `docs/client-api.md:6448-6461` |
| medium | `invalid quoted parent message ID %q` is likewise absent from the §4 error table, which documents only the quoted-parent *not-found* and *thread-context* rejections | `handler.go:348` |
| medium | **The canonical publish discards `PubAck.Duplicate`.** The dedup key is the **client-supplied** `Message.ID`. Two distinct sends colliding on one `id` inside the stream's duplicate window are silently suppressed — yet the sender still gets a success reply and `resultAccepted` is recorded, **indistinguishable from a real publish in logs and metrics** | `main.go:182-187`; publish site `handler.go:512`; `pkg/natsutil/canonical_dedup.go:44` |
| low | The event-level `Timestamp` is **not** taken at the publish site: `now` is captured before the quote fetch (2 s) and the thread-parent re-check (a further 2 s + delay). CLAUDE.md requires the publish-site value; on the degraded path the event timestamp can lag the actual publish by seconds | `handler.go:440` → `:504`; `fetcher_history.go:19` |
| low | The durable consumer sets no `FilterSubject` although `pkg/stream/consumer.go:36` invites it — the same root cause as the architecture finding, seen from the contract side | `main.go:270-274`; `pkg/stream/stream.go:18`; `pkg/subject/subject.go:113` |
| nitpick | The two pre-validation error replies are built before `natsutil.WithRequestID` runs, so they carry **no `X-Request-ID` header** even when `req.RequestID` parsed fine | `handler.go:174`, `:194` vs `:329` |

**Verified clean:** no raw `fmt.Sprintf` subject construction anywhere in the service; `subject.MsgCanonicalCreated(siteID)` is the sole canonical subject and is asserted in `handler_test.go:118`; `idgen.IsValidMessageID` (17-or-20 base62) and `idgen.IsValidUUID` are used for ID and requestId validation; `msgbucket`/`MESSAGE_BUCKET_HOURS` correctly absent — the gate reaches history over `subject.MsgGet` RPC and never touches Cassandra; no OUTBOX/INBOX participation, so the partition and lane rules do not bind; `bootstrapStreams` sets only `Name + Subjects` and fail-fast-verifies in production (`bootstrap.go:44-71`).

### Recommendations
- `high` — Fix `docs/client-api/request-reply.md:2278` to match the botDM DM-path behaviour, add the `type` row, and add `new_thread_message` to the Emits line. This is the derived-view-must-not-drift rule in CLAUDE.md §5.
- `medium` — Add the three attachment errors and `invalid quoted parent message ID` to the §4 error table in `docs/client-api.md`, then re-derive the view.
- `medium` — Inspect `ack.Duplicate` in the publish closure (`main.go:184`), log it, and record a distinct metric reason so a suppressed canonical publish is visible.
- `low` — Stamp `time.Now().UTC().UnixMilli()` into `evt.Timestamp` at `handler.go:504`, keeping `now` only for `Message.CreatedAt`.
- `low` — Set `cc.FilterSubject = subject.MsgSendWildcard(cfg.SiteID)` in `buildConsumerConfig`, and have `ParseUserRoomSiteSubject` callers assert the `msg.send` verb.
- `nitpick` — Stamp the request ID onto ctx immediately after the payload parse so early error replies carry `X-Request-ID`.

---

## 7. Performance — 4 / 5

The hot path is genuinely well-engineered: precomputed metric attribute sets, L1+L2 caches with singleflight, precise Mongo projections, the correct semaphore consumer pattern, sonic with `Pretouch`, and clean `jsretry` discipline. What holds it back is the parent-resolution path — no deadline, no cache, no concurrency — on the first hop of every user message.

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | **`processMessage` has no overall deadline.** A thread reply that also quotes a *different* parent issues two serial 2 s history RPCs, and a missing thread parent adds `waitFor` plus a third — **~6.15 s worst case holding a worker slot and the client's reply.** At `MAX_WORKERS=100` that caps throughput near **16 msg/s during a history-service brownout**, on the first hop of every send | `handler.go:442`, `:455`, `:553`; `fetcher_history.go:19` |
| medium | **Parent-fetch results are never cached.** Every reply in a hot thread re-issues an identical `FetchQuotedParent` for the same parent, and `resolveThreadParent` consumes only `snap.CreatedAt` + `snap.Sender.Account` — **both immutable.** A small TTL'd LRU keyed `(roomID, messageID)` removes one Cassandra-backed RPC per thread reply; the service already has three LRU tiers to copy | `handler.go:548-570`; `fetcher_history.go:70-113` |
| medium | **Four sub-INFO log calls on the accepted-message path build their variadic `[]any` unguarded**, boxing ints and bools per message. `pkg/logctx/handler.go:40-41` documents exactly this hazard, and `handler.go:264` already applies the `logctx.Enabled` guard — so the pattern is known and inconsistently applied | `handler.go:406`, `:436`, `:448`, `:516` |
| medium | **Negative subscription results are cached at neither tier**, so every send from a non-subscriber reaches MongoDB. MESSAGES is client-facing: a client looping sends at a room it left bypasses L1+L2 entirely. `singleflight` collapses only *concurrent* misses, and the breaker fences failures, not load | `subcache.go:87-90`; `pkg/subauthcache/subauthcache.go:8-9` |
| medium | **No startup index assertion** for the `(roomId, u.account)` subscription read, though lower-traffic readers of the same shape do assert it. The service most exposed to a missing index is the only one that will not warn about it | `store_mongo.go:31-40`; cf. `inbox-worker/main.go:562`, `user-service/mongorepo/subscriptions.go:75` |
| low | The infra Nak uses `jsretry.DefaultBackoff` (first rung 1 s). `pkg/jsretry/jsretry.go:74-76` reserves `LowLatencyBackoff` for "where the first retry must be near-immediate so a sub-second hiccup isn't user-visible" — and **the gatekeeper is the most user-visible consumer in the fleet**: the client is blocked awaiting `chat.user.{account}.response.{requestId}` and gets no reply at all on this branch | `handler.go:232` |
| low | The quote fetch and the thread-parent fetch are independent but run serially; they share a snapshot only when the two IDs match, otherwise the latencies add rather than overlap | `handler.go:442` then `:455`, `:537-540` |
| low | `getMessageByIDRequest` and `quotedParentProjection` are absent from `pretouchTypes`; the comment justifies the omission by the type being narrow, but sonic's JIT cost is per-type regardless of width | `pretouch.go:11-17`; `fetcher_history.go:43-64` |
| low | `historyRequestTimeout` is a hardcoded const, not an env knob — unlike every other latency-relevant setting in `config` | `fetcher_history.go:19`; `main.go:31-70` |
| nitpick | `model.Message` is marshaled **twice per accepted send** — once embedded in `evt`, once standalone for the reply: up to ~2×20 KB of redundant encode per message at peak | `handler.go:505`, `:519` |

**Verified clean:** no bare `Nak()`/`NakWithDelay(0)`; no `time.Sleep` (the recheck uses the ctx-aware `waitFor`, `helper.go:43-49`); no goroutine leaks — the singleflight fetch is bounded by `subFetchTimeout` (`subcache.go:20`) and `natsmetrics.Consume` uses the documented semaphore + `WaitGroup` high-throughput pattern; no `$lookup`; projections explicit; `MaxAckPending=1000` ≥ `PullMaxMessages=200`.

### Recommendations
- `medium` — Wrap `processMessage`'s parent-resolution block in a single `context.WithTimeout` (~1 s) so client-visible send latency is bounded regardless of how many fetches fire.
- `medium` — Add a TTL'd LRU for `FetchQuotedParent` keyed `(roomID, messageID)`; scope it to thread-parent resolution first, where the cached fields are immutable.
- `medium` — Apply the `logctx.Enabled(ctx, …)` guard already used at `handler.go:264` to lines 406, 436, 448 and 516.
- `medium` — Add `mongoutil.WarnMissingIndexes(ctx, subCol, "roomId_1_u.account_1")` in `NewMongoStore`, matching `inbox-worker`/`user-service`.
- `medium` — Cache negative subscription results in L1 with a short TTL (5–15 s) so a non-subscriber's send loop cannot pin Mongo; keep L2 positive-only.
- `low` — Switch `handler.go:232` to `jsretry.LowLatencyBackoff`, and run the quote and thread-parent fetches concurrently when the two IDs differ.
- `low` — Add the two projection types to `pretouchTypes`, and move `historyRequestTimeout` into `config` as `HISTORY_REQUEST_TIMEOUT`.

---

## 8. Prioritized action list

Ordered by severity, then impact ÷ effort. No `critical` findings surfaced: nothing here is broken in production *today*. The top three items are all cases where the service is correct only by accident of what clients currently send.

| # | Sev | Action | Dim | Evidence | Why |
|---|-----|--------|-----|----------|-----|
| 1 | `high` | **Set `cc.FilterSubject`/`FilterSubjects` on the durable consumer to the `msg.send` verb only** | Architecture / Integration | `main.go:269-275`; `pkg/stream/stream.go:18`; `pkg/subject/subject.go:113-118` | The consumer binds all of `msg.>`, and `ParseUserRoomSiteSubject` never inspects the trailing verb. A client publishing `msg.edit` today is validated as a *send* and republished to `…canonical.created`. CLAUDE.md reserves `.edited`/`.deleted` for future — **the gate will silently mis-route them the day they exist.** One line. |
| 2 | `high` | **Reconcile `docs/client-api/request-reply.md` with the canonical doc** — botDM fan-out, the `type` request field, `new_thread_message` in Emits | Integration | `docs/client-api/request-reply.md:2278`, `:2283-2291`, `:2320` | The derived view tells clients bot-DM rooms get no `new_message`; the code delivers one. A client built from that view **silently misses every bot DM.** CLAUDE.md §5 forbids the views drifting from the canonical doc. |
| 3 | `high` | **Extract `main.go`'s body into `run(ctx, cfg) error` and unit-test the drain-timeout closure** | Test coverage | `main.go:247-256` | 65.5% is under the 80% merge gate, and the gap is one file at 2.4% — the rest of the package is ~91%. This single refactor clears the gate **with no new container** and covers the one piece of `main.go` with a real failure mode. |
| 4 | `high` | **Split `processMessage` (205 lines, ~30 decision points) into `validateSendRequest` / `authorize` / `enrich`+`publish`** | Maintainability | `handler.go:316-520` | It mixes eight validation rules, subscription auth, large-room policy, quote and thread-parent resolution, enrichment and the publish in one body — and is *why* the coverage clusters here. A pure `validateSendRequest(req, limits)` is directly table-testable. |
| 5 | `high` | **Replace `NewHandler`'s 10 positional parameters with a deps/limits struct** | Architecture / Maintainability | `handler.go:107`; 28 call sites in `handler_test.go` | Three adjacent bare `int`s (`largeRoomThreshold, maxAttachments, maxAttachmentBytes`) are passed as the literals `500, 1, 8192` at 28 sites — **any two can be transposed with no compile error.** The options machinery already exists in the same file. |
| 6 | `medium` | **Bound parent resolution with one `context.WithTimeout` (~1 s), and cache `FetchQuotedParent`** | Performance | `handler.go:442`, `:455`, `:548-570`; `fetcher_history.go:19` | Worst case is ~6.15 s holding a worker slot on the first hop of every send — ≈16 msg/s at `MAX_WORKERS=100` during a history brownout. The cached fields (`CreatedAt`, `Sender.Account`) are immutable, so the LRU is safe. |
| 7 | `medium` | **Make the NAK test double record the delay, and assert it** | Test coverage | `handler_test.go:2170-2171` | `Nak()` and `NakWithDelay(d)` both set the same flag and discard `d`, so the retry test **stays green if `jsretry.Nak` is replaced by a bare `msg.Nak()`** — the exact regression CLAUDE.md says burns `MaxDeliver` in milliseconds. |
| 8 | `medium` | **Delete the two log-and-return sites; convert the four ctx-less `slog` calls to `*Context`** | Code quality | `handler.go:160`, `:298`, `:425`, `:472` | Every blocked send logs twice, and the two lines an operator would join to a failing send **land in Loki with no `request_id`** — the ctx was enriched three lines earlier. |
| 9 | `medium` | **Cache negative subscription results in L1, and assert the subscription index at startup** | Performance | `subcache.go:87-90`; `store_mongo.go:31-40` | MESSAGES is client-facing: a client looping sends at a room it left reaches Mongo on every attempt, bypassing both cache tiers. And the service most exposed to a missing `(roomId, u.account)` index is the only reader of that shape that will not warn about it. |
| 10 | `medium` | **Inspect `ack.Duplicate` on the canonical publish; add the four missing error rows to `docs/client-api.md` §4** | Integration | `main.go:182-187`; `handler.go:348`, `:368`, `:373`, `:378` | A dedup-suppressed publish currently returns success and records `resultAccepted` — indistinguishable from a real publish in logs *and* metrics. And four client-facing `bad_request` strings have no documented cause a client can map them to. |

**Deferred, deliberately.** The synchronous history RPC inside the JetStream worker slot (`fetcher_history.go:82`) is architecturally enrichment work living in the validation gate, and moving it is a design change, not a fix — item 6 bounds its blast radius without that surgery. The `MESSAGES-CANONICAL` producer set is wider than this service (`room-worker`, `room-service` also publish), so the invariants enforced here cover the client lane only; that is legitimate for system messages but worth stating in the stream's own documentation rather than changing code.
