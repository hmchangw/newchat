# Branch review — claude/adoring-bell-egysmi

- **Date:** 2026-07-15
- **Base:** `main` (a6fa309)
- **Head:** e6fa16d — feat: room last-message preview on `message_deleted` + `rooms.lastMsg` upkeep (encryption-ready)
- **Diff size:** 36 files, +2,881 / −259
- **Services touched (2):** broadcast-worker, history-service (plus `pkg/model`, `pkg/subject`, docs)
- **Reviewers:** 2 per-service generalists + 5 global lenses (Go expert, test-automation, bug & security, performance, observability)

## Finding counts (as reported per reviewer; cross-reviewer overlaps noted below)

| Severity | Count |
|----------|-------|
| critical | 0 |
| high | 7 |
| medium | 18 |
| low | 15 |
| nitpick | 13 |

The 7 highs collapse to **6 distinct issues** after dedup: the legacy-room edit-guard defect and the delete-path availability coupling were each independently flagged by three reviewers at different severities; the lane-duplication issue was rated high by both service generalists.

## Top-line risk assessment

No criticals. Test discipline, errcode tiering, docs sync (client-api.md + both derived views), and project-pattern adherence are consistently strong — several reviewers called the test suite exemplary. Hold merge for:

1. **Coalescer flush race (high, bug & security):** the unguarded 250ms bulk flush can *resurrect a deleted message's full content* into `rooms.lastMsg` (indefinitely in a quiet room) or persist a pre-edit body after clients saw the edit. Fix is local: purge/patch the pending buffer in the guarded-store overrides.
2. **System-message previews (high, broadcast-worker):** `handleCreated` has no `msg.Type` gate, so every membership event overwrites `rooms.lastMsg` with a system preview — violating the field's own documented invariant.
3. **Convergence with main's parallel `rooms.get` lane (high, both generalists):** two walkers, two wire shapes, two skip rules, two trim rules — the same room preview flip-flops depending on which path refreshed it. Needs a team decision; recommended target: this branch's skip semantics + `LastMessagePreview` shape, reimplementing `roomLastMessage` atop `GetLastRoomMessage`.
4. **Two cheap ops bounds (high, performance):** add a total-row cap to the bucket walk (`rooms.get` precedent: 250 rows) and gate the delete-path RPC on `room.LastMsgID == msg.ID` to shrink head-of-line blocking on the shared MaxWorkers semaphore.
5. **Security-model sign-off (medium, bug & security):** plaintext preview content newly at rest in Mongo for non-encrypted rooms; survivor previews bypass per-member history windows.

SAST: gosec **pass** (no medium+); govulncheck and semgrep environment-blocked in this sandbox (proxy 403) — must run in CI.
