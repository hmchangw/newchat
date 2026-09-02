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

---

# Service: bot-message-handler

## (a) Diff correctness

Correct and consistent. `publishCanonical` now builds the outbound message with
`natsutil.NewMsg` (`bot-message-handler/handler.go:210`), matching the repo-wide
idiom (`message-gatekeeper/handler.go:524`, `outbox-worker/main.go:91`,
`broadcast-worker/main.go:404`). The nil-header guard at `handler.go:211-216` is
real, not cargo-culted: `natsutil.HeaderForContext` returns `nil` when ctx
carries nothing (`pkg/natsutil/request_id.go:43-49`), and it returns a freshly
allocated map, so mutating it cannot alias the inbound request header. The
request id genuinely reaches ctx in production — `pkg/natsrouter/middleware.go:42-45`
stamps it before handlers run — so the new assertion in
`TestHandleSendRoom_StampsRequestIDOnCanonicalPublish` reflects real behaviour
rather than a test-only path. Consumer side matches: `bot-message-worker` reads
`model.HeaderBotIdentity` from `msg.Headers()` before the unmarshal, which is
exactly the failure mode the change exists for.

**medium** — The DM call site (`handler.go:123`) is the one edited line with zero
test coverage: `handleSendDM` is at 0.0% statement coverage, and
`handler_test.go` contains no reference to it, on this branch or on
`origin/main`. Both new tests exercise only `handleSendRoom`. The two call sites
are textually identical, so the risk is low, but the diff extends an untested
handler.

**low** — Package coverage is 42.4%, under the 80% floor in CLAUDE.md §4.
Pre-existing (`store_mongo.go` and `handleSendDM` are wholly untested), not
introduced here, but the diff does not move it.

## (b) Scope drift

Responsibilities are cohesive: parse BP headers → authz against local Mongo →
canonicalize mentions → publish one canonical event. No creep.

**low** — Duplication between `handleSendDM` (`handler.go:113-126`) and
`handleSendRoom` (`handler.go:166-185`) grew by this diff: the near-identical
`model.Message` literal plus the now-identical
`h.publishCanonical(c, &msg, c.Msg.Header.Get(model.HeaderBotIdentity))` line. A
shared `buildAndPublish(c, roomID, ident, req)` would collapse both and remove
the chance of the two paths drifting.

## (c) Abstraction changes

The `Publisher` reshape is justified. Headers cannot ride a `Publish(subj, data)`
signature, so the change was forced by the requirement. The interface stays
narrow (one method) and is defined in the consumer, per CLAUDE.md §3.

**nitpick** — `jetStreamAPI` + `JetStreamPublisher` (`handler.go:39-48`) is a
second interface plus adapter serving one call site, and the adapter itself is at
0.0% coverage, so a regression in the one line that attaches `WithMsgID` would
not be caught. (See also the Go-expert finding that the comment's stated
justification for keeping `msgID` a named parameter is now stale.)

## (d) Design coherence

Fits. The service's job is to turn an authenticated BP request into one canonical
event; carrying the authenticated identity forward on that event is the same
class of concern as the `MsgID` dedup this file already handled. Forwarding the
raw header verbatim rather than re-deriving from `msg.UserAccount` is the right
call and is correctly explained at `handler.go:192-194` — a re-encoded value
would be unavailable in precisely the decode-failure case the change targets.

## (e) Project patterns

All green: `pkg/subject` builders throughout (`handler.go:75,77,210`), no raw
`fmt.Sprintf` on subjects; `pkg/stream.BotMessagesCanonical` in `bootstrap.go:25`
behind the opt-in `BOOTSTRAP_STREAMS` gate; `pkg/idgen` for message-ID validation
and DM room IDs (`handler.go:95,246`). No JetStream consumer here (natsrouter
request/reply), so that pattern is N/A; no cross-site publish, so the outbox
pattern is correctly not involved. No new `pkg/model` event structs.

**low** — `MessageEvent.Timestamp` is set from `msg.CreatedAt` (`handler.go:200`),
i.e. the BP-supplied `X-Bot-Created-At` header, not `time.Now().UTC().UnixMilli()`
as CLAUDE.md's event-timestamp rule requires. Pre-existing and untouched by this
diff, but it means a caller controls the event-level timestamp.

## (f) Client-API doc rule

**Does not fire — no finding.** This service registers only
`chat.server.bot.request.room.{siteID}.{roomID}.msg.send` and
`chat.server.bot.request.dm.{siteID}.{userID}.msg.send` (`handler.go:74-78` →
`pkg/subject/bot.go:44,49`); neither begins with `chat.user.`, and the service
owns no `auth-service` HTTP route. The changed publish subject
`chat.bot.canonical.{siteID}.created` is an internal JetStream hop. No
request/response schema, error code, or client-facing `pkg/model` struct changed,
so leaving `docs/client-api.md` untouched is correct.
