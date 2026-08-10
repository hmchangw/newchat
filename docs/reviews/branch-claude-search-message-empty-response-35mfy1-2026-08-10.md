# Branch review: claude/search-message-empty-response-35mfy1

**Date:** 2026-08-10 · **Base:** origin/main (9a9f7646) · **Mode:** branch (working tree clean)
**Services touched:** room-worker, search-service, search-sync-worker (+ pkg/model, data-migration/es-index-migrator)
**Reviewers:** 3 per-service generalists + 5 global lenses (Go, test-automation, bug & security, performance, observability)

## Executive summary

| Severity | Count (deduplicated) |
|---|---|
| critical | 0 |
| high | 1 |
| medium | 7 |
| low | 9 |
| nitpick | 8 |

**Top-line risk:** one HIGH, and it is in-branch: the es-index-migrator system-message
filter added on this branch is a **silent no-op on real backfills** — the Cassandra
projection (`messagesource_cassandra.go:32`) deliberately excludes the `type` column, so
`msg.Type` is always empty and `IsSystemMessageType("")` is false. The unit test passes
only because its mock injects `Type` directly. Must be fixed before merge (verified
independently against the source: `type` absent from `messageColumns` and from `iter.Scan`).

Everything else is medium or below. The three core fixes (INBOX envelope wrap, env-var
unprefixing, system-message filtering on the live path) were each verified sound by
multiple lenses, including rolling-deploy compatibility in both directions and the
double-wrap regression guard. The mediums cluster into: log hygiene on the new
empty-result WARN (raw query + missing request_id), an unidentifiable poison-drop log in
search-sync-worker, two test-coverage gaps (searchOrgs empty path; stale fixtures now
crossing the WARN branch), a partial-move hole in the AST constants guard, and a missing
ops migration note for the breaking env rename.

**SAST status:** gosec clean. govulncheck (egress blocked) and semgrep (not installed)
could not run in this environment — both are blocking CI gates and must pass in CI.

## Service: room-worker

(a) Diff correctness vs existing conventions

- No findings of substance. The wrap at room-worker/teamsroomcreate.go:261-270 exactly mirrors the service's three sibling internal-lane publishers (handler.go:468-477 processRemoveIndividual, handler.go:1808-1817 finishCreateRoom, plus 677/1184): `InboxEvent{Type, SiteID, DestSiteID: h.siteID, Payload, Timestamp}` published to `subject.InboxInternal`. Verified against the consumer contract: search-sync-worker/inbox_stream.go:65-87 (`parseMemberEvent`) hard-rejects a bare payload ("missing payload envelope (unwrapped publish?)"), so the pre-fix bare `InboxMemberEvent` was genuinely dropped — the fix is real, not cosmetic. Dedup seed (teamsroomcreate.go:271) is unchanged and deterministic across redeliveries. `make test SERVICE=room-worker` passes with -race.
- **low** — teamsroomcreate.go:266: envelope `Timestamp: acceptedAt.UnixMilli()` derives from the inbound batch's `evt.Timestamp` (teamsroomcreate.go:31), not `time.Now()` as CLAUDE.md's Event Timestamps rule prescribes and as the sibling sites do (handler.go:473, 1813 use `now`). Defensible: it matches the pre-existing federated lane's `ts` (teamsroomcreate.go:290) and keeps the envelope deterministic for the dedup seed. But a batch with a missing/zero `Timestamp` yields `Timestamp: 0`, which `parseMemberEvent` (inbox_stream.go:70-72) rejects — the event is dropped again, just with a different error. No validation of `evt.Timestamp` exists at teamsroomcreate.go:31. Worth a guard or a comment.
- **nitpick** — teamsroomcreate.go:268-270 handles the envelope-marshal error while the sibling sites discard it (`internalData, _ :=`, handler.go:475, 1815). Inconsistent, but in the correct direction; not worth changing.

(b) Scope drift / refactor-readiness

- No drift. The production diff is one envelope wrap plus its error path; the test diff is the helper rework plus two targeted pin tests. Nothing unrelated touched. The four near-identical `InboxEvent`-wrap sites now existing across handler.go/teamsroomcreate.go invite a small `internalInboxPublish` helper someday, but declining to refactor here matches "keep changes minimal and focused" — correctly left alone.

(c) Abstraction changes

- None introduced; reuses `model.InboxEvent`, `subject.InboxInternal`, `natsutil.InboxDedupID`. The test helper change (teamsroomcreate_test.go:44-63) is a justified strengthening: decoding through the envelope on both lanes asserts the consumer's contract rather than mirroring the producer's bytes — this is what let the original bug hide.

(d) Design coherence

- Fits the service's job. The asymmetry — wrap the internal lane yourself, pass the inner event to `h.federate` because `outbox.Publish` builds the envelope (verified: pkg/outbox/outbox.go:83-89) — is the established shape (handler.go:311-322 documents it) and is explicitly pinned by `TestFederateTeamsMembership_FederatedLaneStaysSingleWrapped` (teamsroomcreate_test.go:487-518), guarding the double-wrap regression an over-eager future "consistency" edit would cause. Good defensive test.

(e) Project-pattern adherence

- Clean. `pkg/subject` builder used (teamsroomcreate.go:272), no raw sprintf subjects; cross-site lane still rides the outbox (`h.federate` → `outbox.Publish`); no new streams/consumers/idgen usage; envelope Timestamp set (see (a) for the source caveat); injected `h.publish` field keeps tests NATS-free per CLAUDE.md; error wraps are contextual (`"marshal internal inbox envelope: %w"`). Test naming (`TestFederateTeamsMembership_<Scenario>`) and independence conventions followed.

(f) Client-API doc rule

- Not applicable — no violation. The touched publish targets `chat.inbox.{siteID}.internal.{eventType}`, a server-internal search feed consumed by search-sync-worker; the handler's inbound subject is the ROOMS-TEAMS migration stream (main.go:181-186), not `chat.user.{account}.…` or the msg.send route, and no `pkg/model` client-facing request/reply or server→client event struct changed in this diff (`InboxEvent` is inter-service, not client-facing). docs/client-api.md correctly untouched.

## Service: search-service

(a) Diff correctness vs conventions

- Verified the rename motive is real: the writer side is unprefixed — `search-sync-worker/main.go:47-53` (`SPOTLIGHT_INDEX`, `USER_ROOM_INDEX`) and `data-migration/es-index-migrator/config.go:19-20` (`required,notEmpty`). Moving the three fields off `SearchConfig` (which injects `envPrefix:"SEARCH_"`) to root `Config` (`search-service/main.go:94-96`) makes reader==writer spelling, and `required,notEmpty` matches the migrator's precedent. Failure mode is good: old-name deployments fail startup loudly naming the missing var, not silently reading an empty index. Correct.
- **medium** — `search-service/handler.go:185-187`: the zero-shard WARN logs the raw user `query` (and fires on every empty request while the config is broken — unbounded repeat). CLAUDE.md forbids logging message bodies; a search query is adjacent user content. `kind`+`pattern` alone diagnose the misconfig; keep `query` on the flow line only (which is X-Debug-gated).
- **low** — `search-service/response.go:35-37`: malformed/`_shards`-absent JSON returns 0, which `logEmptyResult` classifies as "always broken". Unreachable in production (parseRooms/parseOrgs already parsed `raw`, and ES always emits `_shards`), but the comment overclaims.
- **low** — `searchMessages` (`handler.go:78-128`) is the third ES read yet gets no `logEmptyResult`, despite the branch being named "search-message-empty-response". Risk is lower (`MessageIndexPattern` is hardcoded, `query_messages.go:18`), but on a fresh site `messages-*` matches nothing and the same silent-empty applies. Note the asymmetry or extend.
- **nitpick** — `handler_test.go:340-343`: the `wantWarn:false` leg only proves no WARN; the FLOW line is never asserted anywhere.

(b) Scope drift

Clean within scope: rename + `notEmpty` + tests + compose + the `query_messages.go:140-143` comment that referenced the old var name. Comment-only commit 4b9df6d is tidy-up of this branch's own comments. No unrelated refactoring. (The diff also touches search-sync-worker/room-worker/pkg/model — outside this reviewer's scope.)

(c) Abstraction changes

Both helpers are justified: two call sites for `logEmptyResult`, and `searchShardTotal` is genuine envelope parsing so `response.go:29` is the right file; the log helper in `handler.go:183` is a handler concern. Free function over method is right — `pattern` differs per caller so nothing on `h` is needed, and it stays independently testable. **nitpick**: six positional params is at the readability edge.

(d) Design coherence

The `_shards.total==0` discriminator is the correct ES signal for wildcard + `allow_no_indices=true` (both read patterns end in `-*`, `main.go:118,125`). Two-tier WARN/FLOW severity split is sound. Double-parsing `raw` only on empty results is negligible. Test-side `slog.SetDefault` swap follows the existing `debug_log_test.go` precedent in five other services, and no test in the package uses `t.Parallel()`.

(e) Project-pattern adherence

- "Never log AND return" — not violated: `logEmptyResult` runs only on the nil-error success path (`handler.go:174-177,237-239`), so `Classify` never sees these requests.
- `slog.WarnContext(ctx, …)`/`slog.Log(ctx, logctx.LevelFlow, …)` is correct and matches `pkg/natsrouter/middleware.go:117-124` (`Logging()` uses the same FLOW-level, ctx-aware call), so request_id attaches via the logctx handler — actually stricter than the pre-existing ctx-less `slog.Warn` at `handler.go:249,279` (not this diff's problem). *(Editor's note: the observability lens verified the plumbing deeper and found request_id does NOT attach via ctx — see the Observability chapter; that finding supersedes this line.)*
- Config, fail-fast, structured KV logging: all conform.

(f) Client-API doc rule

- No `docs/client-api.md` update needed — correct call. The rule triggers on schema/error-case/event changes; this diff changes none (same wire types, same errors, log-only). Env names are ops-facing, not client-facing.
- Env-rename staleness: the live doc `docs/search_index_migration_spec.md` already uses the unprefixed names (its subject is the migrator). Old `SEARCH_SPOTLIGHT_INDEX`/`SEARCH_USER_ROOM_INDEX` spellings survive only in dated planning/spec archives (`docs/superpowers/plans/2026-05-13-*.md`, `docs/superpowers/specs/2026-04-21-search-service-design.md`) — historical records, acceptable to leave.
- **medium** — the rename is still a breaking ops change for out-of-repo prod manifests (compose is fixed in-diff, `deploy/docker-compose.yml:34-36`; IaC is not). The PR description must carry an explicit migration note: set the three unprefixed vars before rolling this image, or the service exits at startup.

## Service: search-sync-worker

(a) Diff correctness

- Guard order is correct. inbox_stream.go:70-81 checks timestamp → payload → type. Payload-before-type matters: `model.InboxMemberEvent` (pkg/model/event.go:106-115) has neither `type` nor `payload` fields, so an unwrapped publish trips the payload guard first and gets the diagnostic "unwrapped publish?" message. Test `TestParseMemberEvent` (inbox_stream_test.go:60) pins this. Wording follows the sibling guard's `parse member event:` prefix; no `%w` is correct since there's no underlying error. [ok]
- Redelivery semantics unchanged — verified. Before the diff, a bare payload produced `json.Unmarshal(nil, &payload)` → error at inbox_stream.go:83-84 → Ack at handler.go:91-95; an empty `Type` with valid payload errored later in the collection switch (spotlight.go:96, user_room.go:96) → Ack. After the diff, both fail earlier in parse → same error+Ack. No previously-tolerated message becomes rejected; no previously-rejected message becomes retried. In-flight bare publishes from old room-worker during a rolling deploy were already dropped (that loss is the bug the room-worker commit fixes); the new guards only improve the log line. Deploy order between the two services doesn't matter — wrapped envelopes were always parseable. [ok]
- `(nil, nil)` from BuildAction is the established filtered idiom: syncFrom (messages.go:185-187) and actionableEvent (messages.go:190-192) both use it, and handler.go:97-99 Acks zero-action messages. The new filter (messages.go:196-198) matches, and sits after actionableEvent / before the ES parent lookup — no wasted resolver call. [ok]
- **low** (messages.go:196): system messages indexed before this fix stay in ES; an `EventDeleted` for such a doc is now also filtered, so the stale doc can never be removed via the stream. The branch's es-index-migrator filter handles reindex-time cleanup, but live indices keep stale sys-docs until a reindex. Worth a note in the PR.

(b) Scope drift / refactor-readiness

Within `search-sync-worker/` the diff is tight: two guards + filter + tests, nothing unrelated touched. The branch bundles three distinct fixes (room-worker envelope, this worker, search-service) but in separate commits — acceptable. **nitpick** (inbox_stream_test.go:106-126): `TestParseMemberEvent_UnwrappedInnerEventClearsTimestampGuard` asserts stdlib decode behavior on model structs, not worker code; it's a documentation pin, arguably belongs next to the model types.

(c) Abstraction — errcode.Permanent

The handler does not distinguish permanent from transient at all: every BuildAction error is hard-coded Ack (handler.go:90-95); Nak is reserved for ES bulk failures (handler.go:132, 171). No `errcode` import exists in the service. Since every current BuildAction error path is a parse/validation poison (the one transient-ish dependency, `teamsUsers.ResolveIdentities`, is swallowed at messages.go:295-299, never returned), plain `fmt.Errorf` + unconditional Ack is coherent, and adopting `errcode.Permanent` here would add machinery with a single caller. The diff correctly follows the file's existing convention. **low**: if BuildAction ever gains a genuinely transient error, the Ack-all becomes silently lossy — a comment on handler.go:90 stating the "all build errors are poison" invariant would future-proof it.

(d) Design coherence

Good. The filter closes a real outlier — message-worker/handler.go:90, notification-worker/handler.go:59, and history-service already gate on `IsSystemMessageType`; the teams-batch branch (messages.go:243) already applied the equivalent rule against its own type field. One filter covers user/bot/teams collections since they share `*messageCollection.BuildAction`. The envelope guards convert an opaque decode failure into a named contract violation, with a regression test pinning why a timestamp-only guard can't catch it.

(e) Project-pattern adherence

`pkg/subject`/`pkg/stream` usage untouched and correct (inbox_stream.go:28, 36). Codec: search-sync-worker is not one of the four sonic hot-path workers; all touched code uses `encoding/json` — correct per CLAUDE.md. Tests are table-driven, `package main`, testify — compliant. **nitpick** (messages_test.go:320): `data, _ := json.Marshal(evt)` discards the error; prefer `require.NoError` as inbox_stream_test.go:42 does.

(f) Client-API doc rule

n/a — the worker is a pure JetStream consumer; no `chat.user.*` subscriptions, no natsrouter, no HTTP routes (verified by grep across non-test files). No `docs/client-api.md` update required.

## Go expert

All touched packages compile; `go test -race` green across the five packages. Findings, ordered by severity:

**[medium] pkg/model/event_test.go:91 — AST guard only covers event.go.** Parsing by relative filename is robust (`go test` runs with cwd = package dir), and the `found == 0` fatal at event_test.go:135 correctly catches a wholesale move of the constants. But the guard has a partial-move hole: a *new* `MessageType*` constant added in another file of the package (message.go already exists) escapes silently while `found > 0` keeps the test green — exactly the drift class the test exists to prevent. Parse every non-`_test.go` file in the package dir instead. Also, aliased constants (`MessageTypeX = MessageTypeY`) or non-literal values are silently skipped by the `BasicLit` filter.

**[medium] search-service/main.go:91-96 — breaking env rename needs coordinated rollout.** `SEARCH_USER_ROOM_INDEX` → `USER_ROOM_INDEX` etc. is the right fix (aligns reader with writer; `required,notEmpty` fails fast; stale `SEARCH_`-prefixed values proven inert in config_test.go:310). But existing prod deployments will crash-loop at startup until IaC is updated. Compose is updated; the PR description must flag the ops-side lockstep change.

**[low] search-service/handler.go:186-192 — WARN logs the user's query text.** Not a §3 hard violation (not a token/body), but `query` and `account` are user content shipped to ops logs; `kind`+`pattern` alone identify the misconfiguration. Consider dropping `query` from the WARN. The routine-miss line is flow-level-gated — fine.

**[low] search-service/handler.go:115 — searchMessages left uninstrumented.** It reads a wildcard `MessageIndexPattern` with the identical allow_no_indices failure class but gets no `logEmptyResult`. If intentional scope, fine; otherwise a cheap follow-up.

**[low] search-service/response.go — `searchShardTotal` swallows the unmarshal error.** Returning 0 routes a malformed body to the "matched no index" WARN — a slightly misleading message. Practically unreachable (the same `raw` already parsed upstream) and pinned by response_test.go, but the doc comment should state the swallow, not just the test.

**[nitpick] search-sync-worker/inbox_stream.go:73 — guard order.** Timestamp is checked before the envelope-only fields, so an unwrapped publish with zero timestamp reports "missing timestamp" instead of the far more diagnostic "unwrapped publish?". Check payload/type first.

**[nitpick] pkg/model/event_test.go:116 — use `strconv.Unquote`** instead of `strings.Trim(lit.Value, "\"")`; correct for escapes and backtick strings.

**[nitpick] search-service/handler_test.go:465-467 — `slog.SetDefault` is process-global state.** Restored via `t.Cleanup` and no `t.Parallel` in the package, so safe today, but fragile if parallelism is added. Note the Warn-level handler means the "no WARN" assertion also passes if code logged at Info — matches the stated intent, acceptable.

**Resolved review questions:**
- `logEmptyResult` as a package-level free function (handler.go:183) is correct: it touches no handler state (pattern passed explicitly), stays independently testable, and matches the Go idiom that state-free helpers shouldn't be methods.
- The checked marshal error in room-worker/teamsroomcreate.go:258-266 diverges from the sibling `internalData, _ := json.Marshal(...)` at room-worker/handler.go:475 — but in the stricter direction CLAUDE.md mandates ("never ignore errors silently"). Justified; the pre-existing ignored-error sites (handler.go:447,462,475, no justifying comment) are the noncompliant ones, out of this diff's scope. Naming (`internalData`) matches the siblings; both wrap texts describe the operation performed.
- Test helpers (`membershipEvents`, `envelope`, `mkEvent`) all live in `_test.go` files — compliant.

**§3 sweep:** no error-by-string comparison in production code (test `err.Error()` Contains at inbox_stream_test.go:763 is acceptable, sentinels would be sturdier); no tokens/bodies logged (query flagged above); all logging is structured slog KV; no struct-tag changes; error wraps describe the calling function's action throughout. The migrator skip (runner.go:60-64) and its "skip ≠ RecordSkipped" rationale are well-documented and test-pinned.

## Test-automation

Working tree verified clean before and after (no mock regeneration was needed — `git diff 9a9f7646...HEAD -- '*store*.go'` is empty and no interface changed in the diff).

**TDD heuristic — PASS.** Every new function/branch has a same-diff test: `searchShardTotal` → `TestSearchShardTotal` (search-service/response_test.go:14), `logEmptyResult` → `TestHandler_SearchRooms_EmptyResultLogsReadPattern` (handler_test.go:319), `parseMemberEvent` guards → `TestParseMemberEvent` + `TestParseMemberEvent_UnwrappedInnerEventClearsTimestampGuard` (search-sync-worker/inbox_stream_test.go:15,113), `BuildAction` filter → `TestMessageCollection_BuildAction_SystemMessagesNotIndexed` (messages_test.go:307), runner skip → `TestRunMessages_SkipsSystemMessages` (runner_test.go:29), room-worker envelope both lanes → teamsroomcreate_test.go:209,248, config change → config_test.go:304,322. No TDD violations.

**Findings**

- **medium** — search-service/handler.go:237-239: the `searchOrgs` empty-result branch is untested. The rooms-path test covers `logEmptyResult` internals, but nothing asserts orgs passes `SpotlightOrgReadPattern`/`kind="orgs"` — a swapped pattern argument here would go uncaught. CLAUDE.md §4 requires per-handler scenario coverage.
- **medium** — search-service/handler_test.go:874 (`TestHandler_SearchOrgs_EmptyESResultReturnsEmptySlice`) and any empty-hit fixture lacking `_shards`: `searchShardTotal` now returns 0 for these bodies, so pre-existing tests traverse the new WARN branch ("read pattern matched no index") on the real default logger — the fixtures now model the *broken-config* state and emit unasserted log noise. Add `"_shards":{"total":N}` to empty-result fixtures so they represent the state they claim to test.
- **low** — search-service/handler_test.go:465-467: the `slog.SetDefault` swap is safe today — no test in search-service (or any affected package) calls `t.Parallel()`, restore is via `t.Cleanup`, and `go test -race` passes on all five packages (verified). It becomes a data-race/cross-talk hazard the day parallelism is introduced; precedent exists (room-worker/debug_log_test.go:70), so consider a shared serialized helper if the pattern spreads.
- **low** — pkg/model/event_test.go:91: `parser.ParseFile(fset, "event.go", …)` is correct — `go test` runs with cwd = package dir. The `found==0` guard (line 135) covers wholesale rename/move of event.go, but a *partial* move (some `MessageType*` constants relocated to another pkg/model file) keeps `found>0` and silently un-guards the moved constants. Parsing the package dir (`parser.ParseDir`) would close this; all constants live in event.go today (verified — message.go only references one in a comment).
- **nitpick** — search-sync-worker/inbox_stream_test.go: the guard in inbox_stream.go:70 is `Timestamp <= 0` but the table only tests `0`; add a negative-timestamp case.
- **nitpick** — pkg/model/event_test.go:111: constants whose values aren't string `BasicLit`s (concatenation, typed consts) are silently skipped by the AST walk; harmless today, invisible tomorrow.

**Checks passed:** table-driven structure with descriptive subtest names throughout; `searchShardTotal` malformed-body case covered; parseMemberEvent error paths all covered incl. the unwrapped-inner case; Makefile `test` target uses `-race` (all affected packages pass with `-race -count=1`); no mock staleness.

**Untestable in this diff:** the producer→consumer envelope contract is pinned only by two independent unit tests in separate packages; search-sync-worker/inbox_integration_test.go:67-81 builds its *own* envelope, so it would not catch renewed drift — a true end-to-end check needs Docker (testcontainers NATS+ES) and is absent. Compose/main.go wiring is untestable by repo convention.

## Bug & security

Unit tests for all changed packages pass with `-race`. Builds clean.

**HIGH — es-index-migrator system-message filter is a silent no-op on real backfills.**
`data-migration/es-index-migrator/runner.go:62` gates on `model.IsSystemMessageType(msg.Type)`, but the Cassandra projection deliberately excludes `type`: `messagesource_cassandra.go:32` (`messageColumns` — the comment at lines 21–23 even lists `type` among excluded columns) and the `iter.Scan` at lines 79–81 never populates `row.Type`. So on a real backfill `msg.Type == ""` always, `IsSystemMessageType("")` is false, and system messages are still re-indexed — exactly what commit 74bc82b set out to prevent. `TestRunMessages_SkipsSystemMessages` passes only because the mock injects `Type` directly; it gives false confidence. Fix: add `type` to `messageColumns` + Scan into `&row.Type` (and ideally an integration-test assertion through the real source). *(Independently re-verified by the synthesizer against the source before this chapter was written: confirmed.)*

**MEDIUM — env rename is an undocumented crash-loop for ops-owned deployments.**
`SEARCH_USER_ROOM_INDEX`/`SEARCH_SPOTLIGHT_INDEX`/`SEARCH_SPOTLIGHT_ORG_INDEX` → unprefixed `required,notEmpty` (`search-service/main.go`). Any deployment still setting only the old names fails at startup. In-repo stragglers: none (only `search-service/deploy/docker-compose.yml`, updated, plus historical docs — `docs/superpowers/plans/2026-05-13-search-index-env-vars.md:599` now states the old operator contract and is stale). Fail-fast is the right behavior, but no migration/release note exists anywhere; prod manifests are ops-owned and out of repo. Add a deploy note to the PR/docs.

**LOW — WARN path logs account + raw query.** `search-service/handler.go:185-187`. A room/org typeahead query is not a token or message body, so no hard CLAUDE.md violation; the routine-miss line is safely gated at `logctx.LevelFlow` (sub-INFO, X-Debug-admitted only). But during an index misconfiguration the WARN fires on *every* empty search, bulk-persisting user-typed query strings keyed by account, unbounded/un-ratelimited. Suggest logging query length (or dropping `query`) on the WARN path — `pattern` + `hint` carry the diagnostic value.

**LOW — dedup-window hazard is real but tiny.** Seed unchanged (`teamsroomcreate.go:271`); `InboxDedupID` (pkg/natsutil/request_id.go:130) bases on X-Request-ID, which a JetStream redelivery preserves (room-worker/main.go:364-373). No stream sets `Duplicates`, so the window is the JetStream default 2 min. Loss requires: old worker publishes bare event, fails to Ack, redelivery to a new worker within 2 min → enveloped publish deduped away, source Ack'd, event never indexed. Teams redeliveries are "minutes-to-hours later" (`teams-room-creation/publisher.go:25`), and the migrator can rebuild spotlight/user-room from Mongo. Accept; note in PR.

**VERIFIED OK (no findings):**
- Rolling deploy both directions: old `parseMemberEvent` (base `9a9f7646`) already errored on empty `Payload` ("unexpected end of JSON input") → Ack; new code Acks with a clearer error — same drop, no regression. Old worker decodes new enveloped events fine.
- `pkg/model/event_test.go:21` `parser.ParseFile("event.go")`: `go test` runs the binary in the package source dir — safe under `make test`.
- Index env names now consistent with `search-sync-worker/main.go:47,53` and `es-index-migrator/config.go:19-20`.

**NITPICKS:** `searchShardTotal` returns 0 on malformed JSON → mislabeled "no index" WARN (unreachable today — `parseRooms` parsed the same bytes first). `TestSystemMessageTypesCoverEveryConstant` silently skips constants not defined by string literal (e.g. aliasing another const).

**SAST:** `make sast-gosec` — clean, zero findings. `make sast-semgrep` — fails: semgrep not installed. `make sast-vuln` — fails: govulncheck egress blocked (`vuln.go.dev … Forbidden`). Both are blocking CI gates that could not be evaluated in this environment; run them in CI before merge.
