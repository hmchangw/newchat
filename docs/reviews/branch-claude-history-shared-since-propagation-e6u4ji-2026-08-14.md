# Branch Review — claude/history-shared-since-propagation-e6u4ji

- **Date:** 2026-08-14
- **Branch:** `claude/history-shared-since-propagation-e6u4ji` @ `2bce990`
- **Base:** `main`
- **Services touched:** 2 — `room-service`, `room-worker` (plus `pkg/model` and `docs/client-api*`)
- **Reviewers:** 2 per-service generalists + 5 global lenses (Go, test-automation, bug & security incl. `make sast`, performance, observability)

## Executive summary

**Findings by severity (raw per-chapter counts):** critical **0** · high **0** · medium **6** · low **10** · nitpick **11**. Several findings are the same issue surfaced through two lenses (mode validation, ctx-less `slog.Error`, wrong type name in a comment, forged-value test hardening); the deduplicated set is in the prioritized action list. Two of the six mediums are pre-existing service-size observations not caused by this branch.

**Top-line risk: LOW.** The feature — inheriting the requester's `historySharedSince` as a server-set cap on share-all member adds — is implemented in the right place (accept-time stamp in room-service, mechanical application in room-worker), with the client-forged-value hole closed by an unconditional server-side reset, the nil-never-`&0` event invariant preserved on both sides, zero added DB round-trips, and complete propagation to subscriptions, the local `MemberAddEvent`, and the federated OUTBOX copy — each pinned by tests. `make sast` passes all three gates (gosec, govulncheck, semgrep). The client-API docs (canonical + both derived views) were updated in the same diff.

**Main asks before merge:** log the inherited cap so capped adds are diagnosable (medium), validate `history.mode` at the boundary instead of treating unknown strings as share-all (medium), collapse the duplicated share-all predicate into one model method (medium), and add one table case hardening the server-set reset on the `mode:"none"` branch (medium). All are small, local changes.

## Service: room-service

### (a) Diff correctness vs conventions

- **No findings of incorrectness.** The reset-then-conditionally-set of the server-set field (`room-service/handler.go:978-983`) exactly mirrors the existing server-stamping block for `RequesterID`/`RequesterAccount`/`Timestamp` (handler.go:964-969), and the client-forged-value test pins it. The `!IsZero()` + `ms > 0` guards honor the model invariant "publishers MUST emit nil, never `&0`" (`pkg/model/event.go:125`). `sub` is guaranteed non-nil at the stamp site (handler.go:859-865). The handler condition `Mode != HistoryModeNone` matches room-worker's `historySharedSincePtr` share-all branch, so the two sides can't disagree on which modes inherit.
- **low** — redundant guard: a zero `time.Time` has a negative `UnixMilli()`, so `!sub.HistorySharedSince.IsZero()` (handler.go:979) is subsumed by `ms > 0` (handler.go:980). Harmless defensive belt-and-suspenders.
- **nitpick** — projection update done right: `subscriptionReadProjection` gains `historySharedSince` (`store_mongo.go:322`) and the drift-guard integration test is extended in the same diff (`integration_test.go:295-296`), per the sync-comment contract at store_mongo.go:311-317. Test helpers (`ptrInt64`, handler_test.go:152-154) follow the existing `ptrBool`/`strPtr` pattern.

### (b) Scope drift / refactor-readiness

- **medium (pre-existing, not caused by this diff)** — room-service is straining: 29 registered RPCs (handler.go:102-129), a 2,583-line handler.go, and a ~50-method `RoomStore` (`store.go:61-274`) that stretches "only the methods it needs." The Teams/Graph meeting surface (handler_teams.go, own `TeamsMeetingStore` at store.go:320-331) is a natural split seam if this continues. This diff itself adds only ~14 lines inside the existing addMembers step-9 block — zero new drift.

### (c) Abstraction changes

- **No findings.** No new interfaces or helpers in room-service; one pointer field on an existing model struct, documented at the declaration (`pkg/model/member.go:29-33`). room-worker extends the existing `historySharedSincePtr` helper with an `inherited` parameter rather than minting a parallel resolver — the right altitude, no premature abstraction.

### (d) Design coherence

- **No findings.** The change fits room-service's stated job (validate, authorize, normalize, stamp, publish canonical); the cap is *applied* in room-worker, preserving the single-writer split. Cross-site requesters work because subscription replicas carry `historySharedSince` via `MemberAddEvent` (`pkg/model/event.go:132`).
- **low** — behavior/doc clarification bundled in: the docs previously claimed `history.mode` defaulted to `"none"`; commit 2bce990 corrects this to `"all"`, matching long-standing worker behavior (`historySharedSincePtr`: any mode ≠ "none" is share-all). Correct fix, but worth flagging in the PR description since it changes documented client-facing semantics.

### (e) Project-pattern adherence

- **No violations.** Publish uses `subject.RoomCanonical` builder (handler.go:988); `encoding/json` retained (room-service is not a sonic hot-path service); no new event struct — `AddMembersRequest` already carries `Timestamp` set at the publish site (handler.go:969); no direct cross-site publish added — `member_added` federation still rides room-worker's outbox lane; explicit projection maintained per "always project precisely."

### (f) Client-API doc rule

- **Satisfied.** `addMembers` is registered on `subject.MemberAddPattern` (handler.go:119, a `chat.user.{account}.…` subject), and the request struct in `pkg/model/member.go` changed — the same branch diff updates `docs/client-api.md` and both derived views `docs/client-api/events.md` and `docs/client-api/request-reply.md`, including the server-set-field note and the `member_added.historySharedSince` semantics.
- **nitpick** — branch commits `docs/superpowers/plans/` + `specs/` working documents; CLAUDE.md Section 5 only mandates deleting `docs/reviews/`, and the plans directory is an established convention (2026-03 plans exist), so acceptable.

## Service: room-worker

### (a) Diff correctness

- **low** — The new doc comment on `historySharedSincePtr` (room-worker/handler.go:139-140) cites "model.RoomMemberEvent invariant" — no such type exists. The nil-never-`&0` invariant actually lives on `model.InboxMemberEvent` (pkg/model/event.go:123-125); `subject.RoomMemberEvent` is a subject builder, so the reference misleads.
- **nitpick** — handler.go:144 returns the caller's `inherited` pointer, aliasing `req.HistorySharedSince` into every published event struct. Read-only today so safe, but the mode-none branch returns a fresh `&timestamp` (handler.go:152); copying would be symmetric and future-proof.
- Otherwise correct: the cap is resolved through the single existing helper and lands consistently at all three consumption sites — subscription build (handler.go:981-983), same-site `MemberAddEvent` (handler.go:1162, 1173), and the federated per-destination copy (handler.go:1281). The `> 0` guard honors the nil-never-`&0` invariant; mode-none correctly ignores the cap (accept timestamp ≥ any inherited cap by construction). Tests cover the truth table (`TestHistorySharedSincePtr_InheritedCap`) plus end-to-end same-site and cross-site propagation, including the OUTBOX-wrapped payload (`unwrapOutbox` assert on `chat.outbox.` publishes).

### (b) Scope drift / refactor-readiness

- **medium (pre-existing)** — handler.go is 2,546 lines carrying member add/remove, key rotation/fan-out, sys-msg composition, federation, DM create, rename, and Teams reconcile — past the point where a split (e.g. membership vs. key lifecycle) would pay off. The diff itself adds ~12 production lines and does not worsen this; noted for the backlog, not this PR.

### (c) Abstraction changes

- No new types or helpers in this service — the diff threads one parameter through the existing `historySharedSincePtr` resolver, which is exactly the right altitude: the "resolve once, use at three sites" comment (handler.go:978-980) stays true. The one new model field (`AddMembersRequest.HistorySharedSince`, pkg/model/member.go:33-41) is well-documented (server-set, nil semantics). No premature abstraction.

### (d) Design coherence

- Fits the service's job cleanly: policy (whose cap, overwrite client input) stays in room-service; room-worker mechanically materializes the canonical event onto subscriptions and events. The trust boundary is sound — only room-service publishes to ROOMS, and its handler resets the field before stamping. No findings.

### (e) Project-pattern adherence

- No violations. No new subjects/streams/IDs; cross-site propagation rides the existing durable OUTBOX path (`h.federate` → `outbox.Publish`, handler.go:1287) rather than direct INBOX publish; consumer pattern untouched (high-throughput `cons.Messages()`+semaphore, main.go:247-281, matches convention). New field carries `json`+`bson` camelCase tags with `omitempty`, and the wire-omission test (pkg/model/model_test.go:1654-1658) pins the nil-not-null contract. Tests are table-driven with named subtests. `AddMembersRequest` already carries `Timestamp`; no new event struct was introduced.
- **nitpick** — `TestProcessAddMembers_InheritedCapPropagates_CrossSite` duplicates ~35 lines of mock setup from its same-site sibling (handler_test.go:234-345); a shared arrange helper would trim it. Acceptable as-is.

### (f) Client-API doc rule

- **Compliant.** room-worker's own entry point is a JetStream consumer (not `chat.user.…`), but the branch changes the client-observable `member.add` semantics and `member_added.historySharedSince` meaning, and the full diff updates all three docs in the same PR: `docs/client-api.md` (history.mode + historySharedSince rows, server-set field list), plus both derived views `docs/client-api/events.md` and `docs/client-api/request-reply.md`. No critical finding.

## Go expert

Overall: clean, idiomatic, well-tested. The security rule (no history escalation via share-all) is implemented in the right place (accept-time in room-service, applied in room-worker), the rollout is order-safe in both deploy directions (old worker drops the unknown JSON field; new worker treats nil as old behavior), and the "never emit `&0`" invariant is preserved on both branches. No findings above medium.

### Findings

1. **medium — duplicated share-all predicate, drift risk.** `room-service/handler.go:906` (`req.History.Mode != model.HistoryModeNone && …`) and `room-worker/handler.go:141` (`history.Mode != model.HistoryModeNone`) independently encode "anything not `none` is share-all" (including empty/unknown modes). If the default or an added mode ever changes, the stamp site and the apply site can disagree — the exact class of bug the first commit's doc fix (`"none" (default)` was wrong) shows already happened once in prose. Suggest a single method on the model, e.g. `func (h HistoryConfig) SharesAll() bool { return h.Mode != HistoryModeNone }`, used by both sites.
2. **medium — no validation of `history.mode`; unknown values silently grant share-all.** Pre-existing, but this PR documents the contract (`"none"` or `"all"`, docs/client-api.md) while the code accepts any string and treats e.g. a typo'd `"nonee"` as `"all"` — the permissive direction for a history-visibility control. A `BadRequest` on unrecognized modes in `room-service/handler.go` `addMembers` would close it; at minimum worth a tracked follow-up.
3. **low — `slog.Error` without context in `historySharedSincePtr`** (`room-worker/handler.go:1148`). The function was rewritten (new signature, new doc comment) but kept plain `slog.Error`, dropping request-ID correlation that CLAUDE.md §3 requires. Since both call sites have `ctx` in scope, threading it and using `slog.ErrorContext` is a two-line change while touching this function.
4. **nitpick — comment references a non-existent type.** `room-worker/handler.go:140` says "see model.RoomMemberEvent invariant: nil, never &0" — no such type exists; the invariant is documented on `model.InboxMemberEvent` (`pkg/model/event.go:125`). Same wrong name appears in the committed plan doc.
5. **nitpick — 640 lines of plan/spec committed to the branch** (`docs/superpowers/plans/2026-08-14-addmember-hss-inheritance.md`, `docs/superpowers/specs/…-design.md`). CLAUDE.md only mandates deleting `docs/reviews/`, so this is allowed, but the plan embeds session-specific commit trailers and step-by-step agent instructions — working notes rather than durable docs. Consider whether they belong in the PR.

### Verified clean

- Struct tags: new field `json:"historySharedSince,omitempty" bson:"historySharedSince,omitempty"` — camelCase, `omitempty` correct for the nil-means-unset contract; wire-omission pinned by test (`pkg/model/model_test.go:867-871`).
- Error handling: no new error paths; existing `fmt.Errorf("…: %w", err)` wrapping untouched and compliant; no logging-and-returning. `errcode` tiering unaffected (no new client-facing errors).
- Server-set overwrite (`req.HistorySharedSince = nil` before conditional stamp, `room-service/handler.go:905`) is unconditional and covered by the "client-forged value overwritten" table case (`room-service/handler_test.go:980`).
- Negative/zero-ms guards on both sides (`ms > 0` in handler, `*inherited > 0` in worker) keep the `hss<=0` sentinel sound; zero-`time.Time` case tested.
- Test quality: table-driven with descriptive names, `t.Run` subtests, independent state, cross-site OUTBOX propagation covered (`room-worker/handler_test.go:1306`), and the projection drift-guard integration test (`room-service/integration_test.go:1079`) closes the trap where unit tests pass while production silently no-ops.
- Docs: canonical + both derived views updated with identical wording; server-set field note updated. §5 workflow rule satisfied.

## Test-automation

**Verification performed:** `make test SERVICE=room-service`, `make test SERVICE=room-worker` (both green, `-race` via Makefile), targeted `-race -count=1` runs of every new test (all pass). Working tree clean before and after. `room-service/store.go` is untouched — only the `subscriptionReadProjection` var in `room-service/store_mongo.go` changed, no interface method changed, so no `make generate` was required and no staleness check applied.

**TDD compliance — clean.** Every new behavior in the diff has a corresponding new test in the same diff:

- `AddMembersRequest.HistorySharedSince` (pkg/model/member.go) → extended `TestAddMembersRequestJSON` incl. set round-trip, wire presence, and `omitempty`-on-nil (`pkg/model/model_test.go:1640`).
- `historySharedSincePtr` new 4-arg signature (room-worker/handler.go:140) → `TestHistorySharedSincePtr_InheritedCap`, 7-case table (`room-worker/handler_test.go:2451`), plus two `processAddMembers` end-to-end tests covering subscription + local `MemberAddEvent` and the federated OUTBOX copy.
- room-service stamping logic (handler.go:964-975) → `TestHandler_AddMembers_HistorySharedSinceInheritance`, 6-case table capturing the published payload via injected `publishToStream` (`room-service/handler_test.go:965`) — correctly follows the CLAUDE.md "inject the publish function" rule; no real NATS.
- Projection change → integration drift-guard test extended (`room-service/integration_test.go:1047,1079`). Excellent mock-vs-integration judgment: the plan explicitly identified that unit tests (mocked store) would pass even if the projection silently stripped the field, and closed that blind spot with the testcontainers-backed guard.

Table-driven structure, descriptive subtest names, testify require/assert usage, and `package main` placement all conform to Section 4.

### Findings

- **medium — forged client `historySharedSince` untested on the mode-`none` branch.** `room-service/handler_test.go:980` — the "client-forged value overwritten" case uses mode `all` + uncapped requester; the mode-`none` case (line 979) uses `reqHSS: nil`. The implementation's unconditional reset (`req.HistorySharedSince = nil` at handler.go:969) is correct today, but a refactor that moves the reset inside the `!= HistoryModeNone` branch would leak a client-forged cap through on `mode: "none"` adds and no test would fail. Add one case: `{mode none, reqHSS: &clientForged, wantHSS: nil}` — one line in the existing table.
- **low — no room-worker e2e test for a cap arriving with `mode: "none"`.** `TestHistorySharedSincePtr_InheritedCap` covers "mode none ignores inherited cap" at the helper level (handler_test.go:2462), which is adequate; the two `processAddMembers` e2e tests both use mode `all`, so the none-branch's use of the new 4-arg call sites is exercised only via pre-existing tests. Acceptable.
- **nitpick — negative cap untested.** The helper table tests `&0` ("non-positive cap emits nil", handler_test.go:2467) but not a negative value; the `> 0` guard covers both, so this is cosmetic.
- **nitpick — BSON round-trip not asserted for the new field.** `TestAddMembersRequestJSON` checks JSON only, matching the file's existing pattern for this struct; consistent with precedent, no action required unless this struct is ever persisted.

**Race usage:** all runs went through Makefile targets (`-race` applied).

**Verdict:** Strong test work — genuine TDD sequencing (plan documents red-phase evidence per task), correct mock/integration split, thorough error/edge coverage including the `&0` invariant and cross-site federation. Only the medium finding warrants a change before merge.

## Bug & security

`make sast`: **PASS** (all three gates green). gosec PASS, govulncheck PASS (0 vulnerabilities in called code; 1 imported-package + 4 module-level vulns exist but are not called — pre-existing, non-blocking), semgrep PASS (0 findings, 98 rules). Note: semgrep was not installed in this review environment and `make sast` initially exited 2 on that tooling gap alone; after installing semgrep 1.163.0, `make sast-semgrep` ran clean.

### Findings

- **low — malformed-event fallback grants unrestricted history despite an available cap** — room-worker/handler.go:1146-1151. In `historySharedSincePtr`, mode `"none"` with `timestamp <= 0` logs and returns nil (unrestricted) even when `inherited` is non-nil. A malformed/legacy canonical event from a capped requester with `history.mode:"none"` would grant full history — strictly worse than falling back to the inherited cap. Only reachable via malformed events (room-service always stamps `Timestamp`), so defense-in-depth: prefer `return inherited` over nil on that branch.
- **low — rolling-deploy escalation window; deploy room-worker first** — pkg/model/member.go:75 + room-worker/handler.go:1140. An old room-worker binary unmarshals the new `historySharedSince` field into nothing (unknown JSON field ignored), so a new room-service stamping the cap has no effect until room-worker is upgraded — the escalation the branch fixes persists during the window. Degrades gracefully (no crash, no misparse), but the deploy order should be room-worker → room-service and noted in the PR.
- **low (pre-existing surface the branch now documents) — `history.mode` is unvalidated free text** — room-worker/handler.go:1141, room-service/handler.go:979. Any value except the exact string `"none"` (case-sensitive: `"None"`, `"nonee"`, garbage) is silently treated as share-all — the permissive default for a typo'd restrictive intent. The `!= HistoryModeNone` shape predates this branch, and the docs now correctly state empty→all, but consider rejecting unknown modes at the room-service boundary (`BadRequest`) rather than widening trust in unvalidated input.
- **nitpick — forged-value + capped-requester combination untested** — room-service/handler_test.go:980. The client-forged case is only paired with an uncapped requester. The unconditional `req.HistorySharedSince = nil` reset makes the combined case provably equivalent, but a table row (forged value + capped sub → requester's cap, not the forged value) would pin the ordering against future refactors.
- **nitpick — pre-existing doc/code mismatch on admin bypass** — docs/client-api.md:1239 (unchanged context) claims platform admins bypass the member check on Add Members, but room-service/handler.go:859-865 hard-fails any account without a subscription (`errNotRoomMember`). Not introduced or touched by this branch; flagging so the doc claim gets verified separately.

### Explicitly verified clean

- **Client smuggling of `historySharedSince` is blocked**: unconditional server-side reset before the conditional stamp (room-service/handler.go:978-983), field documented as server-set, covered by the forged-value test and the `omitempty` round-trip test.
- **No TOCTOU of consequence**: the cap is read from the requester's subscription in the same handler invocation that gates membership; HSS is immutable post-join, and redelivery is deterministic because the value rides the canonical event.
- **Non-positive/zero-time guards on both sides** — the "never emit `&0`" event invariant holds.
- **Propagation is complete**: cap lands on created subscriptions, the local `MemberAddEvent`, and the federated OUTBOX copy, each pinned by tests including the cross-site case.
- **Projection fix** (room-service/store_mongo.go) closes the silent-no-op path and is drift-guarded by the integration test.
- No injection, secret leakage, swallowed errors, or race-prone constructs introduced by the diff.

## Performance

**Verdict: clean from a performance standpoint.** The design explicitly avoided the main perf trap, and the diff is nearly all docs and tests.

- **No extra DB round-trip (confirmed) — informational.** The inheritance lookup adds zero Mongo queries. `room-service/handler.go:859` already fetches the requester's subscription as the membership gate at the top of `addMembers`; the new block at `room-service/handler.go:905-910` reuses that in-hand `sub` and stamps `req.HistorySharedSince` onto the canonical event before publish. room-worker then reads the cap off the event payload (`room-worker/handler.go:1167,1190`) — no fetch of the requester's subscription in the worker. Redelivery is also deterministic and read-free since the value rides the event.
- **Projection is precise — informational.** `room-service/store_mongo.go:1106` adds exactly one field (`historySharedSince`, a single BSON date) to the existing tight `subscriptionReadProjection`, guarded by the projection-drift integration test. The projection is shared by all seven `GetSubscription` call sites, so every one now decodes the extra field — cost is one date per read, negligible; not worth a per-call-site projection split.
- **low — loop-invariant call inside the per-member subscription loop.** `room-worker/handler.go:1166-1170`: inside `for _, c := range needSub`, `historySharedSincePtr(...)` is called once per new member with arguments that never change across iterations, and each iteration re-allocates a fresh `time.Time`. For a large org/channel expansion that is N redundant calls and N small heap allocs where 1 would do — hoist the call (and optionally the converted `*time.Time`) above the loop. This pattern predates the branch (the diff only added an argument), and the cost is trivial next to the `BulkCreateSubscriptions` Mongo write on the same path — cleanup, not a blocker.
- **nitpick — shared `*int64` on the wire path (no action needed).** `historySharedSincePtr` returns the `inherited` pointer itself (room-worker/handler.go:1142-1144), so call sites and the `MemberAddEvent` alias the same `*int64` from the decoded request. An allocation saving, not a hazard — nothing mutates it; flagged only so a future writer doesn't mutate through it.

**Checked and clean:** no new goroutines (no leak surface); no new blocking calls in NATS handlers (pure in-memory branch work between the existing `GetSubscription` and the existing JetStream publish); hot-path allocations negligible (room-admin path, not the message hot path — `encoding/json` per repo convention); batching/pagination unchanged (cap rides the single existing canonical event and the existing batched outbox relay); no sync primitives or caching needed — carrying the accept-time value on the event is itself the cache, strictly better than any worker-side lookup.

## Observability

- **medium (diagnosability) — the cap is applied silently at both ends; an operator cannot tell from logs that inheritance happened or what the boundary was.**
  - room-service/handler.go:978-983 stamps `req.HistorySharedSince` from the requester's subscription with no log, and the existing flow log at handler.go:992-993 (`"room-service member.add handoff"`) carries only `request_id`, `room_id`, `new_count` — no `history_mode`, no cap value.
  - room-worker/handler.go:1140-1152 (`historySharedSincePtr`) returns the inherited cap silently, and the resolution debug log at handler.go:907-910 (`"room-worker add members resolved"`) has no history fields either.
  - When room-worker caps a share-all add, nothing in the logs distinguishes "share-all, uncapped" from "share-all, inherited cap = T". The first support ticket this feature generates ("I was added with full history but can't see old messages") is undiagnosable without dumping the canonical event from JetStream. Fix is cheap and safe: the value is an epoch-ms timestamp (not a secret, not payload content) — add `history_mode` + `history_shared_since` to the room-service flow log and/or room-worker's debug log. Both sites already propagate `request_id` from ctx, so correlation comes free.
- **low (request-ID propagation) — touched function keeps a context-less `slog.Error`.** room-worker/handler.go:149 — `slog.Error("restricted history with missing timestamp, emitting nil", …)` inside `historySharedSincePtr`. Pre-existing, but this branch rewrote the function's signature and both call sites without threading `ctx`, so the log line still violates the CLAUDE.md rule that the request ID appears in all log lines (`slog.ErrorContext` + `request_id`, as `messageDedupSeed` at handler.go:127-128 does correctly). Worth fixing while the paint is wet.

**Clean areas (verified):** no secret/payload leakage (the only new datum in flight is an epoch-ms timestamp; no message bodies, tokens, or marshaled requests logged); slog JSON discipline maintained (structured key-value pairs throughout the touched code); "never log AND return" respected (typed `errcode` sentinels and wrapped raw errors without pre-logging); no missing OTel span (no new external calls, goroutines, or fan-out stages — pure branch inside handlers already instrumented via `obs.Init`); Prometheus metrics acceptable as-is (room-worker's only metric family is `pkg/roomkeymetrics`, scoped to key operations; a counter for inheritance-fired would be a product follow-up, not this PR); test request IDs use valid 36-char hyphenated UUIDs per the CLAUDE.md format rule.
