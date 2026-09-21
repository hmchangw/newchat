# teams-user-sync — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `a857cdc` (base `main`)  
**Overall score:** 3.3 / 5 (baseline 2026-09-01: 3.2, Δ +0.1)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

`teams-user-sync` walks the Microsoft Graph directory and materialises `teams_user`, the collection every other Teams job joins against. It is the best-built of the seven Teams CronJobs on craftsmanship — 4s in code quality, architecture, maintainability and performance — with a consumer-owned three-method store interface, separated read/write Mongo lanes, explicit projections everywhere, `Pool mongoutil.PoolConfig` correctly mounted rather than re-declared, and `handler.go` at 58/58 statements with genuine table-driven tests for every error path. It scores 3.3 because of one behavioural gap and the fleet-wide Graph-config problem.

The behavioural gap is that **reconciliation is insert-only**. `syncPage` skips every id already present in `teams_user` *before* the HR join runs (`handler.go:71`), so `siteId`, `engName`, `mail` and `displayName` are written exactly once and never refreshed. Nothing else in the repo writes those fields. A user synced before their `hr_employee` row exists keeps `siteId: ""` permanently — and `teams-chat-sync` drops empty-`siteId` members from its per-chat site vote, so those chats silently fall back to `DefaultSiteID` and get materialised at the wrong site. Site transfers never propagate either, and there is no backfill path anywhere in the repo. Meanwhile `teams-hr-sync` keeps mutating `hr_employee` and `hr-sync-worker` deletes rows from it, so the two collections diverge monotonically.

Two related integration hazards sit on the same join. `splitUPN` — the function that derives the `account` key tying `teams_user`, `hr_employee` and `users` together — is duplicated byte-for-byte in this service and `teams-hr-sync/transform`, so a one-sided edit (handling `#EXT#` guest UPNs, say) breaks the join with no compile-time or test signal. And `teams_user.account` can collide where every peer collection treats `account` as globally unique: the account is the UPN local part for every tenant user across all domains, but the upsert keys only on `_id`, so two AAD objects sharing a local part across domains produce two rows with one account — and `search-sync-worker` builds an `account → teamsUserID` map that then silently drops one of them. The service also diffs its own write target through a `SecondaryPreferred` client, a read-your-own-writes against a lagging replica; `teams-chat-sync` pins the same collection to the primary for exactly this reason.

The Graph config problem is shared with four siblings and is security-relevant: `GRAPH_TLS_INSECURE_SKIP_VERIFY` defaults to `true` here (and in `teams-chat-sync` and `teams-chat-member-sync`) but `false` in `teams-hr-sync` and `user-presence-service/sync`, because `pkg/msgraph.Config` carries no env tags and six services each re-declare the same operator-facing names. CLAUDE.md §Configuration forbids exactly this. `config_test.go:30` even locks the insecure default in as intended behaviour.

On throughput one finding stands out, and it lives in `pkg/msgraph` rather than here: `fetchUsersPage` calls `httpClient.Do` raw with no 429/503 handling, while every other paged walk in the same package routes through `getThrottled` with `Retry-After` backoff and a tenant-wide gate. A full-directory walk is Graph's most throttle-prone call — 400+ pages for a large tenant — and a single 429 mid-walk aborts the entire run, which under `concurrencyPolicy: Forbid` means new users simply never land. It is close to a one-line fix.

Coverage is 53.4%, floored at 1 by the dimension rule, but the shape is favourable: the business logic is at 100% and the deficit is `main.go` wiring plus a store layer covered only by integration tests that the pipeline never runs — no service pipeline in this repo passes `-tags integration`. One consequence worth fixing regardless: both integration tests construct `newMongoStore(db, db)`, so transposing the read and write lanes would ship with a green suite.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 1 |
| Maintainability | 4 |
| Integration | 3 |
| Performance | 4 |

**Findings by severity:** 1 critical, 7 high, 12 medium, 16 low, 9 nitpick (45 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 4

### Evidence

- [high] Graph TLS verification is disabled by default, not opt-in — `teams-user-sync/config.go:20` — `GraphTLSInsecureSkipVerify bool \`env:"GRAPH_TLS_INSECURE_SKIP_VERIFY" envDefault:"true"\`` means an operator who sets nothing gets MITM-able Graph traffic carrying the app-only bearer token (`pkg/msgraph/msgraph.go:278` sets `InsecureSkipVerify: true` under a `#nosec G402` whose justification at `pkg/msgraph/msgraph.go:132` explicitly reads "Opt-in, dev/on-prem"). The directly comparable CronJob sibling `teams-hr-sync/config.go:28` defaults it to `false`; `user-presence-service/sync/main.go:44` also defaults false. `config_test.go:30` locks the insecure default in as intended behaviour, so this is a deliberate inversion of secure-by-default rather than an oversight. gosec is clean only because the unsafe call lives in `pkg/`, suppressed there.
- [low] The one error log line of the whole run drops the run's request id — `teams-user-sync/main.go:24` — `run()` builds `logger := slog.With("requestId", idgen.GenerateRequestID())` (`main.go:76`) and every progress line carries it, but the terminal failure is logged by `main` on the *default* logger, so the log that an operator actually greps for is the only one without the correlation id that ties it to the run's other lines.
- [low] Hand-written projections duplicate the decode struct's bson tags — `teams-user-sync/store_mongo.go:75` vs the `hrRow` tags at `store_mongo.go:26-31` — adding a field to `hrRow` without also editing the `bson.M{"account":1,"siteId":1,"engName":1,"mail":1}` literal decodes silently as a zero value. The sibling solves exactly this by deriving the projection from the struct (`teams-hr-sync/store_mongo.go:22-41`, `bsonProjection`). Projection discipline itself is correct — both reads project explicitly.
- [low] No `pkg/obs.Init`; logging is hand-wired — `teams-user-sync/main.go:21` — `slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))` bypasses the SDK CLAUDE.md §1 says each service wires once, so this job emits no traces/metrics and no OTLP log export, and has no level knob (`nil` HandlerOptions pins Info). Context: `teams-hr-sync/main.go:141` carries an explicit "One-shot job: no obs.Init" comment, so there is repo precedent for batch binaries opting out — flagging for the synthesizer's consistency call, not as a lone defect.
- [nitpick] Capacity arithmetic can panic on a misbehaving store — `teams-user-sync/handler.go:68` — `make([]model.TeamsUser, 0, len(users)-len(existing))` panics if `ExistingIDs` ever returns more keys than requested. Unreachable with the real Mongo store (the `$in` filter bounds it), but the slice is only a growth hint and `len(users)` would be free of the assumption.
- [nitpick] One Info line per HR-unmatched user — `teams-user-sync/handler.go:106` — unbounded by directory size on a first run against a large tenant; the aggregate at `handler.go:117` already reports the same fact as counts. Self-limiting on later runs (unmatched users get upserted and then count as existing). The choice to log the Graph GUID rather than the UPN-derived account is correct and commented.

### Recommendations

- [high] Flip `envDefault` to `"false"` on `GRAPH_TLS_INSECURE_SKIP_VERIFY` and set it to `true` explicitly in the on-prem deployment manifest — `teams-user-sync/config.go:20`, plus the assertion at `config_test.go:30` — makes the insecure path an audited opt-in, matches `teams-hr-sync`, and removes a silent token-exposure default.
- [medium] Log the run failure through the request-id logger — `teams-user-sync/main.go:23-26` — have `run()` return the logger (or call `slog.SetDefault(logger)` after `main.go:76`) so the failure line correlates with the run's other output.
- [low] Derive the `hr_employee` projection from `hrRow` instead of a literal — `teams-user-sync/store_mongo.go:73-75` — reuse the `bsonProjection`-style helper already proven in `teams-hr-sync`; kills the rename-drift class of bug.
- [low] Decide obs wiring repo-wide for one-shot jobs — `teams-user-sync/main.go:21` — either wire `pkg/obs.Init` here or add the same explicit "one-shot job: no obs.Init" comment `teams-hr-sync/main.go:141` carries, so the omission reads as a decision rather than a miss.
- [nitpick] Use `len(users)` as the `candidates` capacity — `teams-user-sync/handler.go:68`.

### Reviewer notes

Verified clean on this dimension, so not reported as findings: every error is wrapped with a what-this-function-was-doing prefix (no bare `err`, no `"error: %w"`); no string error comparisons; no `map[string]interface{}`, `os.Getenv`, `fmt.Println`, `time.Sleep` or unterminated goroutines in the service; `model.TeamsUser` carries paired camelCase `json`+`bson` tags with `bson:"_id"` (`pkg/model/teamsuser.go:11-31`); no secret is logged (`GraphProxyPassword` never reaches a log line, `mongoutil` redacts URIs); `Pool mongoutil.PoolConfig` (`config.go:47`) is mounted from the owning package rather than re-declared. `pkg/errcode` tiering is N/A — this binary registers no NATS or HTTP handler, so there is no error boundary to adapt. `go vet ./teams-user-sync/...` exits 0.
SAST per the shared summary: gosec medium+ clean and the 20 repo-owned semgrep rules clean; `govulncheck` and the semgrep registry packs were blocked by sandbox egress (403) and were not retried, so dependency-vulnerability coverage for this service is UNVERIFIED. No SAST finding under `teams-user-sync/` to fold in.
