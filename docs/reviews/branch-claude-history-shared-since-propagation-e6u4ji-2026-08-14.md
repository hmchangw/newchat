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
