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
