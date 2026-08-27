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
