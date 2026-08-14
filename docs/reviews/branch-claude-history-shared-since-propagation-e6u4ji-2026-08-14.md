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
