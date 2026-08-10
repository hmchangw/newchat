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
