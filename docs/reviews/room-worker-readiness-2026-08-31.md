# room-worker — Production Readiness Review

**Service:** `room-worker` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.3 / 5

The federation plumbing is correct and unusually well-reasoned — every OUTBOX type is correctly partitioned onto the FIFO lane, subjects all come from `pkg/subject`, `bootstrapStreams` is *stricter* than the spec, and the high-throughput consumer pattern is textbook. The problems are elsewhere. **A rename can permanently diverge `rooms.name` from `subscriptions.name`**: the room-name `$set` is unguarded and commits before a NAK-able federate, while the subscription write *is* high-water-mark guarded and refuses to follow it back. **The teams-mode deploy silently serves live client DM-create RPCs** on the shared queue group. And structurally the service is the hardest in the fleet to change safely: a 476-line function inside a 2,625-line `handler.go`, a 7,920-line test file, a 31-method store interface, and five copy-pasted federation blocks.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 2 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 4 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 12 | 22 | 15 | 6 | **55** |

> **Audit-coverage caveat.** `gosec` and the repo-owned `semgrep` rules are clean repo-wide; `govulncheck` and the registry packs could not run (blocked egress), so dependency-CVE coverage is unverified.

