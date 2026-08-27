# Production readiness: `broadcast-worker`

| | |
|---|---|
| **Service** | `broadcast-worker` |
| **Date** | 2026-08-27 |
| **Branch** | `claude/pr-188-dry-refactor-7v5g7s` |
| **Overall score** | **3.7 / 5** |

## TL;DR

`broadcast-worker` is the fan-out service and the largest thing on the message path — ~3,335
production lines, 1,047 statements. It has just been split: `roomlist-worker` was extracted out
of it, taking every room-level MongoDB write derived from a canonical message. **The central
claim of that split verifies.** `Store` is read-only, the one remaining write sits behind a
separate consumer-defined interface reached only through a mutex-guarded map insert with no
error return, and both writers of the room document call the same `msgbucket.NewerRow`
comparator. The residue is the right residue.

What holds the score down is not the split. It is that the hot path has no deadline against
`AckWait`, one fan-out lane spawns unbounded goroutines while its sibling lane 800 lines away
does the same job with a semaphore, a cross-site mention badge is silently lost whenever a
best-effort user lookup fails, and the post-split cleanup missed the one artifact that teaches
newcomers what the service does — its own test harness, which still asserts fields this service
no longer writes.

## Dimension scores

| Dimension | Score | Verdict |
|---|---|---|
| Go code quality | 4 / 5 | Unusually disciplined against CLAUDE.md §3; one real Tier-3 gap and one config rule the rest of the fleet follows |
| Architecture | 4 / 5 | The split's residue is coherent and the "no awaited write" claim verifies; the coherence window is now jointly owned by three services |
| Test coverage | **3 / 5** | 67.3%, below the 80% floor — but `main()` is 203 of 342 uncovered statements; `handler.go` itself is 86.9% |
| Maintainability | **3 / 5** | Disciplined extraction, but `handler.go` has outgrown one file and the deploy test harness still describes the pre-split service |
| Integration | 4 / 5 | Contracts unusually well kept; two silent shared-field disagreements with `roomlist-worker` |
| Performance | 4 / 5 | Fan-out is O(1) in room size and nowhere near a bottleneck at target load; two unbounded-resource defects |

Overall = mean of six = **3.7 / 5**.

## Findings by severity

| Severity | Count |
|---|---|
| critical | 0 |
| high | 6 |
| medium | 20 |
| low | 15 |
| nitpick | 5 |

The six `high` findings:

1. `publishToThreadAccounts` spawns one goroutine per recipient, unbounded — *performance*
2. No handler deadline bounds a message against `AckWait` — *performance*
3. A failed user lookup silently loses every cross-site mention badge — *architecture, integration*
4. Unit coverage 67.3%, below the repo minimum of 80% — *test coverage*
5. The `deploy/test/` harness still asserts fields this service no longer writes — *maintainability*
6. `publishChannelThreadEvent` takes two adjacent `[]byte` params, one of which must be sealed — *maintainability*

## Method, and what was re-verified

Six independent expert passes, each reading `CLAUDE.md` and the whole service before judging,
then cross-checked against source by the synthesizer. Every `high` finding in this report was
re-verified by hand, and one apparent contradiction between two experts was resolved by reading
the code rather than picking a side — see the Integration chapter's tie-break entry.

**The SAST gate is partial, and the gap is environmental.** `gosec` and the repository's own
`.semgrep/` rules both ran clean over this service. `govulncheck` and the semgrep registry
rulesets could not run: this environment's egress proxy answers 403 for `vuln.go.dev` and
`semgrep.dev`. `make sast` therefore exits 1 overall. CI is authoritative for those two.

---

# 2. Code quality — 4 / 5

Unusually disciplined against CLAUDE.md §3: every error wrapped with what the function was
doing, no bare `err`, no log-and-return, no string error comparison, no `time.Sleep` for
synchronization, every goroutine with a termination path, config fully typed through
`caarlos0/env`. **No leak of message content, DEKs or room keys was found at any log site** —
`preview.go`, `preview_writer.go` and `roomactivity.go` log only ids, counts and durations,
which matters in a service that handles both bodies and encryption keys.

## Findings

| Severity | Location | Defect |
|---|---|---|
| `medium` | `store_mongo.go:88-97`, call sites `handler.go:269,312,391,538,589,714,750,775,820` | **Verified.** A missing room makes `GetRoom`/`GetRoomMeta` return a wrapped `mongo.ErrNoDocuments` — deliberately preserved by `pkg/roommetacache/valkey.go:135` "which callers branch on" — but *no handler branches on it*. The only `ErrNoDocuments` test in production code is `store_mongo.go:201`, inside `GetThreadFollowers`. So a permanently-unsatisfiable message NAKs through the full retry budget, which **this branch widened from 5 to ~17 deliveries across roughly an hour**. The precedent exists 100 lines away: `handler.go:176` already returns `errcode.Permanent` for a malformed payload with exactly this reasoning, and peers do the same (`message-worker/store_mongo.go:73`). Currently unreachable in production because nothing deletes rooms — the same latent class the PR body already records as "no room-existence gate on the send path". |
| `medium` | `main.go:46,53` | **Verified.** `NATS_URL` and `MONGO_URI` carry `envDefault` localhost connection strings, against CLAUDE.md §3's "never default secrets or connection strings — mark them `required`". An unset `MONGO_URI` starts silently against localhost and surfaces as an outage rather than a config error. This is the identical defect just fixed in `roomlist-worker`; `room-service`, `message-worker`, `message-gatekeeper` and `bot-message-worker` all mark both required. |
| `low` | `handler.go:1061-1068` | `publishRoomEvent` overwrites `pubErr` each iteration, so when both dual-publish targets fail only the last error survives. `roomActivityPublisher` (`roomactivity.go:193-199`) already does this correctly with `errors.Join`. |
| `low` | `roomactivity.go:5,186` | `encoding/json` marshals `RoomActivityEvent` on the created-message path, inside a service CLAUDE.md explicitly names as a sonic hot-path worker, with no documented exception — contrast `message-gatekeeper/fetcher_history.go`, whose exception is spelled out. `model.RoomActivityEvent` is also absent from `pretouch.go:15`. |
| `low` | `roomactivity.go:78-85` | `throttled` stamps `lastRefreshed[roomID]` *before* `publish` runs, so a failed announce suppresses retries for a whole interval. The doc comment's "the next message re-establishes it" is true only after the interval elapses. |
| `low` | `main.go:404,508` | `logctx.ConsumeContext(..., msg.Data())` feeds the canonical payload to `CapturePayload` (`pkg/logctx/limiter.go:94-97`), which logs `"payload", string(data)` — a full message body. Double-gated behind an inbound `X-Debug` rung *and* `DEBUG_LOG_PAYLOADS`, and repo-shared rather than this service's invention, but this is the stream where CLAUDE.md's "never log full message bodies" actually bites, and the exemption list covers only `.sso.*`. |
| `low` | `preview.go:120-129`, `handler.go:803/813/863` vs `:918/1074` | Log key casing mixes `room_id`/`roomID` and `messageID` within and across call sites, degrading structured-log queryability. |
| `nitpick` | `handler.go:1264` | `account := account` is a dead loop-variable copy under Go 1.22+ semantics. |
| `nitpick` | `keycache.go:110` | `metrics.Miss` fires even when the singleflight re-check served from the LRU, over-counting misses. |

## SAST gate — partial

| Tool | Result | Detail |
|---|---|---|
| `gosec` | **PASS** | Via `make sast`, `-severity medium -confidence medium -tests=true -exclude-generated`. Zero findings repo-wide, none in `broadcast-worker/`. |
| `semgrep` — repo rules | **PASS** | `.semgrep/`, 9 Go rules over 14 files. Zero findings, including `room-subject-publish-must-route` and the `errcode.WithCause` guard. |
| `semgrep` — registry | **COULD NOT RUN** | `p/golang` and `p/security-audit` need `semgrep.dev`; the proxy answers 403 on CONNECT. |
| `govulncheck` | **COULD NOT RUN** | `vuln.go.dev:443` denied by gateway policy. Dependency-CVE reachability is **unverified, not clean**. |

`make lint` reports 0 issues and `make test SERVICE=broadcast-worker` passes. **Do not read this
chapter as a clean SAST result** — `make sast` exits 1 overall because two of three scanners did
not execute.

## Recommendations

1. `medium` — Add `errors.Is(err, mongo.ErrNoDocuments)` branches at the `GetRoom`/`GetRoomMeta` call sites returning `errcode.Permanent(errcode.NotFound("room not found"))`, matching `handler.go:176`. This closes before any room-deletion feature lands, and the widened retry budget makes it more expensive than it was.
2. `medium` — `main.go:46,53`: change to `env:"NATS_URL,required"` / `env:"MONGO_URI,required"`, keeping the localhost values in `deploy/*/docker-compose.yml`. Mirrors the fix already applied to `roomlist-worker`.
3. `low` — `handler.go:1061`: accumulate with `errors.Join` in `publishRoomEvent`.
4. `low` — `roomactivity.go`: either switch `roomActivityPublisher` to sonic and add `model.RoomActivityEvent` to `pretouch.go`, or add one line stating why stdlib is retained.
5. `low` — `roomactivity.go:78`: record the throttle watermark only after a successful publish, or say plainly that a failed announce is not retried within the interval.
6. `low` — Normalize log keys to `room_id`/`message_id` in one pass.

---

# 3. Architecture — 4 / 5

The extraction left a genuinely coherent residue: fan-out plus one un-awaited, structurally
inert preview write, with unusually well-argued invariants. What keeps it off 5 is that the
`previewForMsgId == lastMsgId` invariant is now jointly owned by three services with three
different durability regimes and two independently-named flush knobs, and that the cross-site
mention badge depends on a read whose failure is swallowed.

## The split verdict — the claim verifies

`Store` (`store.go:17-29`) is read-only. The sole write lives behind a separate
consumer-defined `bulkRoomPreviewWriter` (`preview_writer.go:52`), reached only via
`h.previews.buffer` — a mutex-guarded map insert with no error return, **structurally unable to
fail a handler**. The one residual awaited MongoDB dependency is a *read* (bot app name),
breaker-fenced, LRU-cached, and double-bounded by `previewSealTimeout` + `previewSealReserve`.

Both writers call `msgbucket.NewerRow` (`preview_writer.go:114,120`; `roomlist-worker/batch.go:93`).
`previewUpdate` (`store_mongo.go:160-187`) touches only preview fields under `previewAsOf` and
never `lastMsgAt`/`lastMsgId`, so the two halves cannot overwrite each other.
`GuardedAdvanceKeyFields`' second conjunct — the key must already equal `lastMsgId` — is the
correct conservative choice.

**The leak is not in the writes but in the read gate.** Between the two flushes every active
room has `previewForMsgId != lastMsgId`, so `history-service` walks Cassandra. At 250 ms/250 ms
that is a rounding error. But when `roomlist-worker` holds batches un-acked through a MongoDB
hiccup (`MaxDeliver=-1`) while `broadcast-worker`'s writer *drops* failed batches and moves on,
the two diverge for the whole outage and every room-list load walks — **a Cassandra load spike
correlated with MongoDB degradation.** That failure mode is the one thing the otherwise
exhaustive comments never name.

## Findings

| Severity | Location | Defect |
|---|---|---|
| `high` | `handler.go:243-249` → `:296`, `:419` | A `FindUsersByAccounts` failure is logged and swallowed; `mention.ResolveFromParsed` then emits no `Participant`, so `federateMentions` sees no `SiteID` and relays **nothing** — every cross-site mention badge is permanently lost for the duration of a user-store or MongoDB outage, while `roomlist-worker`'s local badge (parse-only, no read) still lands. The written justification, "simply relays nothing rather than failing the edit", is a false dichotomy: destination sites are derivable without user enrichment. Independently found by the integration expert. |
| `medium` | `main.go:115` (`PREVIEW_FLUSH_INTERVAL`) vs `roomlist-worker/main.go:50` (`FLUSH_INTERVAL`) | Two separately-named env knobs in two services jointly define one coherence window, against CLAUDE.md §6's rule that a knob shared by more than one service is declared once in the package owning the thing it configures. Nothing validates them against each other — contrast `retiredTTLSafe` at `main.go:371`, which does exactly this for a comparable cross-service pair. |
| `medium` | `handler.go:426-500` | `federateMentions` publishes to OUTBOX best-effort: a JetStream publish failure is logged, never returned, so the badge never reaches the stream whose entire purpose is durable retry. The 5 s `mentionFanoutTimeout` bounds it correctly, but a fan-out with `dropped > 0` is unrecoverable. |
| `medium` | `main.go:228-239` | A transient Vault blip at startup **permanently disables preview persistence for that pod's whole lifetime**, with no re-attempt. In a rolling restart this yields a mixed fleet where some pods advance `previewForMsgId` and some do not, so rooms served by a degraded pod fall out of key/pointer agreement until an enabled pod handles their next message. |
| `low` | `handler.go:262` | The preview is buffered *before* `GetRoomMeta` and before the room-type switch, so a room whose fan-out is refused (unknown type) or whose meta read fails still gets a `previewForMsgId` advance. Harmless today; wrong ordering if the buffer ever gains a side effect. |
| `low` | `main.go:436` | Shutdown calls `broadcastSub.Unsubscribe()` rather than `Drain()`, discarding buffered server-broadcast (thread tcount) messages instead of processing them. |
| `low` | `roomlist-worker/store_mongo.go:42` vs `pkg/msgbucket/order.go:20` | The tie-break rule exists in two forms — Go comparator and hand-written BSON filter — in two packages, pinned together only by comments. No conformance test renders one against the other. |

## What checks out

`cons.Messages()` + `natsmetrics.Start` semaphore sized by `MAX_WORKERS` — the correct
high-throughput choice for fan-out. `WithOutageRetryBudget` with the *same* `LowLatencyBackoff`
passed to `jsretry.Settle`. No bare `Nak()`, no hardcoded `cc.BackOff`. `bootstrap.go` sets only
Name+Subjects and verifies-only in production. No raw `fmt.Sprintf` on any subject.
`InboxSubscriptionMention` in exactly one `pkg/outbox` filter set. Shutdown order
`Unsubscribe → iter.Stop → wg.Wait → flush → Drain → Mongo`.

## Recommendations

1. `high` — Route the cross-site badge off `mention.Parse` accounts plus a site lookup that fails *closed*, or return a transient error when mentions exist and the lookup failed so JetStream retries. Do not let a decorative read gate a durable federation event.
2. `medium` — Have `federateMentions` return an error when `dropped > 0` or an `outbox.Publish` fails, and settle it as transient. Duplicate broadcasts are already accepted on every other error path in this handler.
3. `medium` — Hoist the flush cadence into one shared config type mounted by both services, and add the coherence constraint to CLAUDE.md §6 beside `MESSAGE_BUCKET_HOURS`.
4. `medium` — Make the Vault wrapper lazily retryable, or expose the disabled state on `/healthz`, so a startup blip is not a silent lifetime degradation.
5. `medium` — Emit a metric for `previewForMsgId != lastMsgId` observed at read time in `history-service`. It is the only observable signal that the split's one gap is open, and the outage-correlated case above is invisible without it.
6. `low` — Move `roomLastMsgFilter` into `pkg/msgbucket` beside `NewerRow` and add a test driving both forms over the same tie cases.
7. `low` — Move `h.previews.buffer` below the room-type switch; swap `Unsubscribe()` for `Drain()` on the server-broadcast subscription.

---

# 4. Test coverage — 3 / 5

Measured unit coverage is **67.3%**, below the repo's 80% floor, so the floor rule applies. But
the shortfall is concentrated almost entirely in wiring no unit test can reach, and the tested
logic is genuinely well tested — which is why this scores 3 rather than the 2 the raw number
alone would suggest.

## Measurements

```
make test SERVICE=broadcast-worker
go test -race ./broadcast-worker/...
ok  github.com/hmchangw/chat/broadcast-worker        PASS

go tool cover -func=/tmp/covbw.out
total:  (statements)  67.3%     (705/1047)
```

`make generate SERVICE=broadcast-worker` produced **no diff** — mocks are current. Tree verified
clean at exit.

| Scope | Coverage |
|---|---|
| Whole package | **67.3%** (705/1047) |
| Excluding `main()` | **82.3%** (705/857) |
| Excluding all of `main.go` | 83.4% |
| Excluding `main.go` + integration-only stores | **88.8%** |

Per file: `main.go` 3.3% (203 uncovered) · `store_mongo.go` 26.2% (48) · `preview.go` 81.0% ·
**`handler.go` 86.9%** (69) · `roomactivity.go` 96.0% · `keycache.go` 96.3% ·
`preview_writer.go` 97.1% · `helper.go` / `bootstrap.go` 100%.

Docker was unavailable, so the integration tag could not be measured. `EnsureIndexes`,
`BulkUpdateRoomPreview`, `GetThreadFollowers`, `GetHistorySharedSince` and Mongo `GetRoomMeta`
*are* exercised by `integration_test.go` / `metacache_integration_test.go`, so their 0–13% unit
figures understate reality.

For calibration: `roomlist-worker` 63.4%, `room-worker` 63.1%, `notification-worker` 58.1%,
`message-worker` 51.3%, `inbox-worker` 44.3%, `outbox-worker` 37.3%. **The floor breach is
fleet-wide**, and CI enforces no coverage gate.

## Findings

| Severity | Location | Defect |
|---|---|---|
| `high` | package | Coverage below repo minimum 80%, currently **67.3%**. |
| `medium` | `preview_writer.go:127` | The `default:` over-cap shed branch — the one whose comment says getting it wrong reintroduces #224 — is the single uncovered statement in the file, and `maxPendingPreviews` appears in no test at all. |
| `medium` | `handler.go:683-699` | `publishThreadMetadata`'s entire DM/BotDM arm — the per-account loop, the `isBot` skip, the publish-error path — plus the unknown-room-type default are untested. Only the channel arm runs. |
| `medium` | `store_mongo.go:111` | `mongoStore.ListRoomMembers`, the query behind all DM fan-out, has zero coverage in unit *or* integration tests. |
| `medium` | `store_mongo_breaker_test.go:78` | **Verified.** `TestMongoStore_FencedReadsDoNotSpendBudgetOnAbsence` never touches `mongoStore`. It constructs a breaker directly and feeds it `mongo.ErrNoDocuments`, so it tests the `mongoBreakerFailure` predicate — real, but not what its name claims — and would pass with all three fenced reads deleted. |
| `medium` | `metacache.go:18,27` | `cachedMetaStore`, wired at `main.go:270`, is never constructed by any test. The L1 read-through has no coverage at either level. |
| `low` | `handler_test.go:3233` | `TestPublishToThreadAccounts_PartialFail_ReturnsNil` asserts only `NoError`; an implementation that publishes nothing would pass. |
| `low` | `roomactivity.go:165` | `remotePeers`' duplicate-peer `continue` is uncovered — the case named "tolerates blanks and self repeated" repeats only *self*, caught by an earlier branch. |
| `low` | `preview_test.go:148` | `fakeBulkWriter.err` is dead — never set — so `Flush`'s bulk-write-failure return is never exercised. |
| `low` | `consumeloop_test.go:42`, `parent_fetcher_test.go:24` | Untagged unit tests start real in-process `nats-server` instances. Hermetic (`Port:-1`, `t.TempDir`, `t.Cleanup`), but literally connecting to NATS in a unit test. |
| `info` | `handler_test.go:89` | Package-level `*model.Room` fixtures shared across 88 tests. No mutation found today, but a pointer fixture is one assignment from cross-test bleed. |

## A cross-cutting observation: tests written to the intent, not the call

`store_mongo_breaker_test.go:78` is the **third** instance of one shape in this repository:

| Test | Name claims | Body exercises |
|---|---|---|
| `pkg/roomsubcache/lookup_test.go:281` | `GuardLoader` serving L2 while the breaker is open | Seeds `CachedAt = now`, so the fresh branch returns and the loader is never reached |
| `roomlist-worker/main_test.go:35` | `buildConsumerConfig` bot-mode durable | Only `stream.PipelineBot.ConsumerName` |
| `broadcast-worker/store_mongo_breaker_test.go:78` | Fenced store reads not spending breaker budget | Only the `mongoBreakerFailure` predicate |

All three pass with the production code they name deleted. The common failure is writing a test
to the *intent* rather than to the *call* — precisely what a green suite is supposed to rule
out. Worth treating as a review habit rather than three unrelated fixes: **when a test names a
function, assert that the function ran.**

## What is genuinely well tested

Preview seal-failure vs ineligible (`preview_test.go:234`, table-driven, four ordering cases);
`FindUsersByAccounts` failure fallback (`handler_test.go:520`); breaker-open on all three fenced
reads (`store_mongo_breaker_test.go:40`); key-cache miss/hit/expiry/singleflight/ctx-cancel
(`keycache_test.go`); the refresher's prune bounding **and** the degenerate nothing-to-reclaim
case (`roomactivity_test.go:264,295`); the four sonic wire-compat tests CLAUDE.md mandates; and
a poison-panic-does-not-wedge-the-consumer test against real JetStream. Integration tests use
`testutil.MongoDB`/`NATS`/`SharedValkeyCluster` with `FlushValkey` cleanup, and `main_test.go`
has the required `testutil.RunTests(m)` TestMain.

## Recommendations

1. `medium` — Test `previewWriter.buffer`'s over-cap path: fill `maxPendingPreviews` rooms with bodies, buffer a new eligible room, assert the new entry lands as `pvwFailed=true` rather than an ineligible key-advance. This is the branch guarding #224.
2. `medium` — Add a `publishThreadMetadata` DM case: a `RoomTypeDM` room with `Accounts{alice, weather.bot}`, asserting one publish to alice, none to the bot, plus a publish-error case asserting the wrapped error.
3. `medium` — Give `mongoStore.ListRoomMembers` an integration test (members present, room absent → empty, projection shape), matching the existing `GetThreadFollowers` test.
4. `medium` — Rewrite `TestMongoStore_FencedReadsDoNotSpendBudgetOnAbsence` to go through `GetRoom`/`GetThreadFollowers` against a real MongoDB with a missing document, asserting the breaker stays `StateClosed`.
5. `low` — Cover `cachedMetaStore` with a fake `Store` counting `GetRoomMeta` calls: miss→inner, hit→no inner call, pass-through of embedded methods.
6. `low` — Set `fakeBulkWriter.err` in one case; add a `remotePeers` case `{"b","b"}`; strengthen `TestPublishToThreadAccounts_PartialFail_ReturnsNil` to assert two attempts and alice's payload.
7. `info` — Extracting `main.go`'s wiring into a testable `run(ctx, cfg) error` would move the headline number from 67.3% toward the 82–89% the rest of the package already achieves.

---

# 5. Maintainability — 3 / 5

The extraction itself was disciplined — no dead helpers, no orphaned mocks,
`LAST_MSG_FLUSH_INTERVAL` genuinely gone, and an integration test that actively pins the split
boundary. What costs it two points: `handler.go` has outgrown a single file, the deploy test
harness still describes the pre-split service, and two of the sharpest invariants are enforced
only by call-site discipline.

## Findings

| Severity | Location | Defect |
|---|---|---|
| `high` | `deploy/test/README.md:7-10`, `deploy/test/verify/*.sh` | **Verified.** The harness's scenario table and three of four verify scripts assert `rooms.lastMsgAt`, `subscriptions.hasMention` and `rooms.lastMentionAllAt` — all `roomlist-worker`'s now. Sharper than first reported: both *dev* compose files were correctly updated (`deploy/user/docker-compose.yml` and `deploy/bot/docker-compose.yml` each reference `roomlist-worker` twice), but `deploy/user/docker-compose.test.yml` has **zero** references. The documented demo flow fails at step 4. The one artifact that teaches a newcomer what this service does now teaches the pre-split model. |
| `high` | `handler.go:1073`, call sites `:365`, `:564`, `:612` | `publishChannelThreadEvent(…, viewPayload, followerPayload []byte, …)` — two adjacent same-typed parameters where the first must be sealed (it lands on a room-namespace subject) and the second must not. `:612` deliberately passes the same buffer twice. A newcomer copying that pattern for an event *with* a body publishes plaintext room-wide, and neither the signature nor a type resists it. |
| `medium` | `handler.go:396-399` | **Verified.** The comment states "broadcast-worker makes no MongoDB writes". `preview_writer.go:63` contradicts it in the same service, under the heading "Why this is broadcast-worker's only MongoDB write". The precise claim — *no MongoDB write that any handler awaits* — is the true one and is what the PR body says; this comment dropped the qualifier that carries all the meaning. Textbook drift of duplicated rationale. |
| `medium` | `handler.go` (1,407 lines, 30 functions) | `handleCreated:230` alone owns mention resolution, preview sealing, room-type routing, cross-site mention federation and the activity announce — five reasons to change. The file spans two transports (JetStream plus core-NATS `HandleServerBroadcast`) and seven event kinds. |
| `medium` | `main.go:350-358`, `:456` | `go previews.Run(flushCtx, …)` starts on a `*previewWriter` that is nil whenever `sealer.enabled()` is false, and a shutdown hook unconditionally waits on `flushDone`. Correct only because of nil-receiver guards in `preview_writer.go:108,150,178`; nothing at the call site says so, and the `if previews != nil` log at `:356` reads as though the guard were there. |
| `medium` | `main.go:255-266` ↔ `handler.go:138` | "The buffered writer exists only while the sealer does" is stated in `main` and enforced nowhere: `withPreviewSealer(sealer, previews)` takes them as two independent parameters. Decoupling them sends every ineligible message down `previewUpdate`'s `GuardedAdvanceKeyFields` branch with no body ever written — contained today only by that guard's `previewForMsgId == lastMsgId` conjunct, in a third package. |
| `medium` | `main.go:45-116`, `:118-482` | 30 direct env knobs plus ~15 mounted, validated in eight separate places scattered through a 365-line `main()` — some `os.Exit`, some `slog.Warn`, some reachable only behind a feature flag. There is no `cfg.Validate()`. |
| `low` | `main.go:80`, `:83` | `VALKEY_KEY_GRACE_PERIOD` and `METRICS_ADDR` are parsed and never read. `METRICS_ADDR` is dead fleet-wide (six services); `VALKEY_KEY_GRACE_PERIOD` is unique to this service and was born dead in `abedb10`. |
| `low` | `roomactivity.go:88-111`, `:119-139` | The prune-resume rationale is told twice — ~25 comment lines guarding a 13-line function. Two copies of one argument is the drift shape already realised above. |
| `low` | `preview.go:97` | Refers to `roomLastMessage.Preview/PreviewFailed`; the type is `roomPreview` (`preview_writer.go:19`). A pre-split name. |
| `low` | `handler.go:1-2` | The package comment covers fan-out and the reaction notification only — silent on the preview write, the server-broadcast subscriber and OUTBOX federation. |

## Post-split residue

**Cleaned up properly:** no unused package-level functions; `mock_store_test.go` carries exactly
the five surviving `Store` methods; `LAST_MSG_FLUSH_INTERVAL` is absent from config, both dev
compose files and all code; no `SetSubscriptionMentions`/`UpdateRoomLastMessage` test helpers
survive; `integration_test.go:652-666` actively asserts the room document's `roomlist-worker`
half is untouched.

**Left behind:** the entire `deploy/test/` harness; the "no MongoDB writes" comment; the
`roomLastMessage` type reference; and a six-site duplication of the "roomlist-worker owns the
room pointer now" rationale (`handler.go:252,296,344,396`, `main.go:112,329`,
`roomactivity.go:26`, `preview_writer.go:63`, `store_mongo.go:156`) — one copy of which has
already drifted into falsehood.

## Adding a per-message side effect — the newcomer walkthrough

1. **`handler.go:230`** `handleCreated` — insert the call. *Trap:* placement matters and nothing says so. Before `GetRoomMeta:266` it runs on every NAK'd redelivery; after the fan-out only on success.
2. **`handler.go:79,104,138`** — new `Handler` field, new `handlerOptions` field, new `withX` option, wired in `NewHandler:142`. Four edits for one dependency.
3. **`store.go:18`** + the `//go:generate` block, `store_mongo.go` implementation, then `make generate`. *Trap:* `store_mongo.go:77-83` — a new fenced read with its own not-found sentinel **must** be added to `mongoBreakerFailure` or it re-splits the single failure budget. Documented at the definition, invisible from the call site.
4. If it writes and buffers: a second `flushloop` goroutine in `main.go` plus a shutdown hook placed **after** `wg.Wait()` and **before** `mongoutil.Disconnect` — stated only in a comment at `main.go:453-455`.
5. *Trap:* `preview.go:115` — anything on the pre-fan-out path must re-implement the deadline-reserve check (`timeout + previewSealReserve`) or it spends the fan-out's budget. There is no shared helper; you must notice `previewForInserted` did it.
6. *Trap:* never return an error once the broadcast has left — a NAK redelivers to every client. Asserted in six independent comments (`preview_writer.go:86`, `roomactivity.go:66`, `handler.go:426`, `:733`, `:1106`, `:1184`), nowhere central.
7. *Trap:* call `withBroadcastMetricLabels` on any new publish path or it lands in the `unknown` room-kind series, and `natsmetrics.MarkTerminalFromContext` on any swallowed publish failure.

**4–6 files, and three invariants a newcomer must discover unaided.**

## Recommendations

1. `high` — Fix or delete `deploy/test/`: point the verify scripts at what this service now produces (the sealed `previewCiphertext`/`previewForMsgId` fields and the NATS events), or add `roomlist-worker` to `docker-compose.test.yml`. A harness that fails on the happy path teaches the wrong model.
2. `high` — Make the thread-lane payload distinction type-safe: `type sealedPayload []byte` / `type followerPayload []byte`, or collapse `publishChannelThreadEvent` to take the plaintext plus a `seal func()`. Then `:612`'s "both lanes share it" becomes an explicit constructor rather than two identical arguments.
3. `medium` — Delete the "makes no MongoDB writes" clause at `handler.go:397` and collapse the six-site rationale to one canonical statement (`preview_writer.go:63-88` is the best-written copy), leaving one-line pointers elsewhere.
4. `medium` — Split `handler.go`: `handler_thread.go` (the five `handleThread*` plus `channelThreadFanOut`/`threadFanOutAccounts`, ~450 lines) and `handler_mutation.go` (pin/unpin/react/`publishMutation`/`build*RoomEvent`, ~300). `handler_test.go` splits along the same seams.
5. `medium` — Add `func (c *config) validate() error` collecting the eight scattered checks and call it once after `env.ParseAs`. Delete `VALKEY_KEY_GRACE_PERIOD`; leave `METRICS_ADDR` for a fleet-wide sweep.
6. `medium` — Couple the sealer and writer structurally: have `withPreviewSealer` derive the writer, and move the `go …Run(…)`/`flushDone` wiring behind a `previews != nil` branch so the nil-receiver guards are a safety net rather than the mechanism.
7. `low` — Fix `preview.go:97`'s stale type name, extend the package comment, cut the duplicated prune rationale to one site.
