# Production readiness: `roomlist-worker`

| | |
|---|---|
| **Service** | `roomlist-worker` |
| **Date** | 2026-08-27 |
| **Branch** | `claude/pr-188-dry-refactor-7v5g7s` |
| **Head** | `17c8c2b` |
| **Overall score** | **3.7 / 5** |

## TL;DR

`roomlist-worker` is a small, cohesive, unusually well-reasoned service whose production
logic is better than the repo average — error classification, replay-safe writes, flush-budget
validation against `EffectiveAckWait`, back-pressure that demonstrably engages, and a hot path
measured at ~3.9 µs and ~11 allocations per message with roughly 500× CPU headroom at the
target rate. Five of six dimensions score 4/5. The score is dragged down by test coverage
(63.4%, below the repo's mandatory 80% floor) and by a small number of genuine production
risks that cluster around one theme: **this service is designed to survive a MongoDB outage by
holding messages un-acked under `MaxDeliver=-1`, and it has no instrumentation to tell anyone
when it is doing so.** The single strongest finding is measured, not theoretical: the `mentions`
map amplifies ~2,600× per maximum-size message, so a slow-flush window can hold ~2.6 M entries
and build a bulk write that cannot finish inside `FLUSH_TIMEOUT` — a livelock, invisible,
with `/readyz` still green.

## Dimension scores

| Dimension | Score | One-line verdict |
|---|---|---|
| Go code quality | 4 / 5 | Strong idioms and slog discipline; an ERROR log on every clean shutdown and defaulted connection strings |
| Architecture | 4 / 5 | Conventions honoured and deviations justified; the only canonical consumer with no instrumentation |
| Test coverage | **2 / 5** | Excellent test *design*, but 63.4% measured — below the 80% floor, so the floor rule applies |
| Maintainability | 4 / 5 | Dense comments that mostly earn their length; `main.go` has outgrown its file |
| Integration | 4 / 5 | Subjects, streams and comparators correctly shared; two real cross-service field disagreements |
| Performance | 4 / 5 | Measured-fast and correctly batched; one unbounded map defeats the design's only bound |

Overall = mean of six = **3.7 / 5**.

## Findings by severity

| Severity | Count |
|---|---|
| critical | 0 |
| high | 5 |
| medium | 15 |
| low | 14 |
| nitpick | 4 |

The five `high` findings are:

1. `mentions` map amplification is unbounded and can livelock the consumer — *performance*
2. No service or consumer instrumentation, under `MaxDeliver=-1` — *architecture, performance*
3. Unit coverage 63.4%, below the repo minimum of 80% — *test coverage*
4. A cross-site sender's `lastSeenAt` advance is never federated home — *integration*
5. The Ack-vs-Nak decision rests on an invariant recorded nowhere near its use — *maintainability*

## How this report was produced

Six independent expert passes, each reading `CLAUDE.md` and the full service before judging,
then cross-checked against source by the synthesizer. Where two experts disagreed on a line
number or scope, the claim was re-verified against the file and the corrected form recorded
here — three findings were amended that way, and one was downgraded after it turned out to be
a fleet-wide pattern rather than a defect this service introduced.

**Verification status of the SAST gate** is recorded in the Code quality chapter and is
partial: `gosec` and the repo's own semgrep rules ran clean, but `govulncheck` and the semgrep
registry rulesets could not run in this environment. CI remains authoritative for those two.
