# teams-hr-sync — Production Readiness Review

**Service:** `teams-hr-sync` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 2.8 / 5

The producer of the workforce feed the whole Teams pipeline depends on, and the service where documentation drift has become a correctness risk in its own right. The code itself is small and well-factored — a well-chosen `emitter` seam for its two modes, correct `pkg/subject` and `pkg/idgen` usage, a request ID minted and propagated onto outbound messages, and a hand-rolled shutdown that is *correct* (the drain deadline is deliberately detached from the cancelled context).

Three things stand out. **`README.md` — which explicitly presents itself as the contract for "an external persister" replacing this worker — is materially wrong in four places**, including a `pkg/hrstore` package that does not exist and a `source:"teams"` scoping that the query does not perform. **A partial publish loses the users half of the feed forever**: the employees upsert is published first and persisted downstream, so the next run's diff finds the rows equal and never re-emits the users — directly contradicting `main.go:38-39`'s claim that "a lost publish self-heals". And **the entire direct-write path is at 0% coverage**, including the two guards whose own comments describe data corruption.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 2 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 2 | 5 | 10 | 9 | 5 | **31** |

---

## 2. Go code quality — 4 / 5

Error wrapping, naming and secret hygiene are genuinely good; the gaps are request-ID log coverage and missing `bson` tags on the wire structs this service owns.

### Findings
- `medium` — `requestId` is attached to only two log lines; every other log call uses the package-level `slog` and carries no correlation id — `teams-hr-sync/main.go:94` vs `main.go:147`, `write_store.go:69`. `WarnContext(ctx)` at `write_store.go:69` looks ctx-aware but the handler installed at `main.go:27` is a plain `slog.NewJSONHandler` that ignores ctx, so the id is dropped. CLAUDE.md requires the id "in all log lines".
- `medium` — `model.IEmployeeWithChange.ChangeType` and `IUserWithChange.ChangeType` have a `json` tag but no `bson` tag; `IHRSyncEmployeeQuitBatch` has no `bson` tags at all — `pkg/model/teams_employee.go:47,53,60-62`. CLAUDE.md: "All model structs get both `json` and `bson` tags."
- `low` — No log-level knob: `slog.NewJSONHandler(os.Stdout, nil)` hardcodes Info — `main.go:27`. CLAUDE.md asks for an `envDefault` log level.
- `low` — `.semgrep/msgraph-secrets.yml` — the rule that guards the Graph credential path this service feeds (`main.go:81-86`) — has **no fixture**: there is no `.semgrep/msgraph-secrets.go` beside it, so `make sast-semgrep-test` never exercises it (only `metrics.go` exists). CLAUDE.md §2 warns an unverified rule "can be disabled by a pattern edit without any scan failing".
- `low` — SAST audit coverage: gosec + repo-owned semgrep clean repo-wide; `govulncheck` and semgrep registry packs could not run (blocked egress, per GLOBAL_PREP) — environmental, not a defect.
- `nitpick` — CI runs raw `go vet`/`go test` and has neither a lint nor a `sast` stage, despite CLAUDE.md §5 calling SAST a blocking gate — `teams-hr-sync/deploy/azure-pipelines.yml:36-46`. Fleet-wide (only `translation-service` has it).

Secret handling verified clean: `TEAMS_CLIENT_SECRET` is read into config (`config.go:18`), passed straight to `msgraph.Config` (`main.go:84`), and never appears in any log or error; `grep` over all non-test files shows no secret reaching a sink.

### Recommendations
- `medium` — Replace the bare handler at `main.go:27` with one that reads the request id off ctx (or pass `logger` into `runStreamMode`/`runDirectMode`/the write store) so `main.go:147` and `write_store.go:69` correlate.
- `medium` — Add `bson` tags to `ChangeType` (both structs) and to `IHRSyncEmployeeQuitBatch`.
- `low` — Add `.semgrep/msgraph-secrets.go` fixture with `// ruleid: msgraph-no-credential-body-logging` lines so the credential rule is actually tested.
- `low` — Add `LOG_LEVEL` with `envDefault:"info"`; add lint + sast stages to the pipeline, copying `translation-service`.

---
