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
