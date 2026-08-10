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
