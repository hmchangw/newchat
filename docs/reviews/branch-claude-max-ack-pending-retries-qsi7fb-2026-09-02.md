# Branch review: `claude/max-ack-pending-retries-qsi7fb`

**Date:** 2026-09-02
**Base:** `origin/main`
**Head:** `02d1fd5` — feat(bot-lane): attribute bot-message-worker failures to the sending bot
**Services touched (2):** `bot-message-handler`, `bot-message-worker`
**Also touched:** `docs/specs/o11y/nats-metrics-contract.md`. No `pkg/` changes.

## Findings by severity

| Severity | Count |
|---|---|
| critical | 0 |
| high | 5 |
| medium | 10 |
| low | 9 |
| nitpick | 6 |

Counts are deduplicated across the seven lenses: four separate reviewers raised
the `bot-room-service` envelope mismatch, and it is counted once.

## Top-line risk assessment

**The code is correct; the feature does not reach an operator.** Nothing in the
diff is unsafe, the build is clean, both service suites and the full-repo
`go test -race ./...` pass, `gosec` reports zero findings and the 19 repo-owned
semgrep rules report zero findings over 1800 files. The cardinality exception
the change takes against the metrics contract was independently traced end to
end and holds: no external actor can drive distinct `bot_account` label values.

The risk is that the change's headline deliverable — an alertable per-bot
failure series — is not queryable as shipped. `bot_msg_worker_failure_total` is
registered on the `promauto` default registry, and
`docs/specs/o11y/o11y-metrics-inventory.md` §2.3 states that those families are
**not** exposed on the SDK `:2112` endpoint and carry no `service_name`/`site`
attributes. `pkg/health/health.go:121-122` serves only `/healthz` and `/readyz`;
there is no `promhttp` handler in any service path. The metric therefore has the
same reachability as its sibling `bot_msg_worker_permanent_error_total`, which
that inventory already lists as an orphan. Consistency with an existing gap is
not the same as working.

Second-order: the branch's stated fallback ("messages published before this
change still attribute via the payload") is only half true. `bot-room-service`
publishes system messages onto the same canonical subject as a bare
`model.Message` rather than the `model.MessageEvent` the worker decodes, so
those decode to a zero-valued message and land under `unknown`. That bug
predates this branch, but the new counter absorbs it rather than exposing it.

Third: test coverage of the new code is uneven. The worker's attribution paths
are well covered by behaviour-asserting tests, but the DM handler that the diff
edited has zero tests, and the one adapter that actually attaches the JetStream
dedup id is unexercised.

None of this is a reason to revert. Items 1-3 of the action list are small and
land the feature properly.

## Verification run for this review

| Check | Result |
|---|---|
| `make lint` | 0 issues |
| `make test` (full repo, `-race`) | pass |
| `make test SERVICE=bot-message-{worker,handler}` | pass, cache cleared |
| `make sast-gosec` | pass, 0 findings |
| `make sast-semgrep-test` (rule fixtures) | 2/2 pass |
| repo-owned semgrep rules over tree | 0 findings, 19 rules, 1800 files |
| `govulncheck` | could not run — `vuln.go.dev` 403 from the environment proxy |
| `p/golang` + `p/security-audit` semgrep | could not run — `semgrep.dev` 403 from the environment proxy |

The two blocked scanners are environment limits, not results. `go.mod` is
untouched by this branch, so govulncheck has no new dependency surface to see.
