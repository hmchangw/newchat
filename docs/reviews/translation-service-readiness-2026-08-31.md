# translation-service — Production Readiness Review

**Service:** `translation-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.7 / 5

**The only service in the 35-service fleet that clears the CLAUDE.md 80% coverage floor** — 82.3%, against 34 services below it and 19 below 60%. And the number is not vanity: the error taxonomy, transport drops and concurrency are genuinely exercised. Contract discipline is equally strong — subject builders throughout, `docs/client-api.md` and **both** derived views accurate and in sync, which is rarer in this repo than it should be. Seventeen small single-purpose files, WHY-shaped comments, correct `errcode` tiering, a lock-free token cache with single-flight refresh.

What holds it at 3.7 is that the outbound path has **no deadline and no connection pooling**, and the service ships **exactly half of the router's overload protection**. `pkg/natsrouter` provides `DefaultGuarded` specifically so a service cannot apply the admission cap without the companion timeout — and this service applies the cap alone. The consequence is concrete: a caller gives up in ~2s while a degraded upstream keeps all 100 admission slots occupied for ~35s doing work nobody will read, and every other caller gets "service busy".

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 3 / 5 |
| 3 | Test coverage | 4 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 3 | 5 | 13 | 3 | **24** |

---

## 2. Go code quality — 4 / 5

Idiomatic, well-commented Go with correct `errcode` tiering, clean error wrapping and zero logging violations; only small linting-grade blemishes and one sloppy SAST suppression.

### Findings
- `low` — duplicate, mutually contradictory `#nosec G304` justifications stacked on one statement — `translation-service/j1source.go:26-27`. Only the line directly above the statement is honoured by gosec, so line 26's correct justification ("operator-configured token mount") is inert; the effective one is the copy-pasted "developer-supplied path in dev tooling, not attacker-controlled", which is false for a production service (that boilerplate otherwise appears only in `tools/loadgen` and `_test.go` files).
- `low` — `readErr == io.EOF` direct comparison instead of `errors.Is` — `translation-service/translator_stream.go:154`. Works today because `bufio.ReadString` returns the sentinel unwrapped, but a wrapped EOF would fall through to the `Unavailable` branch and misclassify a clean stream end as an upstream outage.
- `low` — `pkg/model/translation.go:11,20` carries `json` tags only; CLAUDE.md §3 requires both `json` and `bson` on all model structs. The doc comment argues wire-only, but CLAUDE.md states no such carve-out — the exception should be added to CLAUDE.md rather than asserted in a comment.
- `nitpick` — `fmt.Errorf` with no format verbs where `errors.New` is correct — `translator_stream.go:171`, `token.go:124`, `main.go:54,57,71`, `j1source.go:54`.
- `nitpick` — `main.go:61` `fmt.Errorf("%w when TRANSLATION_BACKEND=stream", err)` leads with the wrapped error rather than describing what this function was doing.
- `low` — audit-coverage gap, not a service defect: gosec and the repo-owned semgrep rules are clean repo-wide, but `govulncheck` and the semgrep registry packs could not run (egress blocked, per GLOBAL_PREP).

### Recommendations
- `low` — Delete `j1source.go:27`; keep one accurate justification directly above `os.ReadFile`.
- `low` — Switch to `errors.Is(readErr, io.EOF)`.
- `low` — Either add the wire-only-struct exception to CLAUDE.md §3 or add `bson` tags to `pkg/model/translation.go`.
- `nitpick` — Replace verb-less `fmt.Errorf` calls with `errors.New`.

---

---

## 3. Architecture — 3 / 5

Textbook stateless request/reply service — consumer-defined interface, constructor DI, correct subject builders, `shutdown.Wait` — undercut by re-declaring a shared knob locally and shipping exactly half of the router's overload protection.

### Findings
- `high` — the service applies the admission cap without the companion request timeout — `translation-service/main.go:112-116`. `pkg/natsrouter/guard.go:57-66` documents `DefaultGuarded` as existing precisely "so a service can't apply only half of the overload protection — the cap … and the timeout … are otherwise two separate calls that are easy to forget one of." No `HandlerTimeout` middleware is installed anywhere in `main.go`, so handler contexts have no deadline.
- `high` — `MAX_CONCURRENCY` is re-declared in the service Config with a divergent default — `translation-service/main.go:41` (`envDefault:"100"`) vs the owning package `pkg/natsrouter/guard.go:21` (`envDefault:"256"`). CLAUDE.md §6 Configuration: a knob shared by more than one service is declared once in the owning package and mounted as a named field. Six other services (`room-service`, `media-service`, `history-service`, `search-service`, `room-worker`, …) mount `natsrouter.GuardConfig`; this one does not, and consequently has no `REQUEST_TIMEOUT` knob at all.
- `low` — the `Translator` interface is defined in `translator.go` rather than the CLAUDE.md-named `store.go`; defensible (there is no store — the service is stateless) but it is a departure from the documented per-service file layout worth one line in the service README/deploy notes.

Correctly done and worth stating: no JetStream usage at all, so the absence of `bootstrap.go`/`BOOTSTRAP_STREAMS` is right, not a gap; `subject.TranslateRequestPattern` is used, never `fmt.Sprintf` (`main.go:117`); config is fully `caarlos0/env` with `required` on `SITE_ID`/`NATS_URL`/`TRANSLATION_BACKEND` and fail-fast startup validation of the stream backend (`main.go:48-77`); shutdown order is router → drain → obs (`main.go:121-125`).

### Recommendations
- `high` — Replace the hand-rolled cap with `natsrouter.GuardConfig` mounted as a named field and `natsrouter.DefaultGuarded(nc, "translation-service", cfg.Guard, natsrouter.WithSiteID(...), natsrouter.WithMetrics(...))`; drop the local `MaxConcurrency` field.
- `high` — Set `REQUEST_TIMEOUT` below the client's NATS request timeout (2–5s) and map `context.DeadlineExceeded` to `errcode.Unavailable` in `Handler.Translate` per `pkg/natsrouter/doc.go:15-21`, then document the new `unavailable` case in `docs/client-api.md` §3.6.
- `low` — Add a one-line note in the service explaining why there is no `store.go`.

---

---

## 4. Test coverage — 4 / 5

At **82.3%** this is the **only service in the entire 35-service fleet that clears the CLAUDE.md 80% floor** (34 are under it, 19 under 60%), and the percentage is not vanity — the error taxonomy, transport drops and concurrency are genuinely exercised; the gaps left are narrow but sit on production-only paths.

### Findings
- No floor finding: 82.3% ≥ 80%. Structure is compliant throughout — `package main` tests, table-driven subtests with descriptive names (`lang_test.go:9-55`, `token_test.go:211-240`, `main_test.go:38-59`), mockgen mock in `mock_translator_test.go` (generated, and repo-wide `make generate` produced zero diff), no real DB/NATS in unit tests, and `integration_test.go:1,25` correctly uses `//go:build integration`, `testutil.RunTests(m)` and `testutil.NATS(t)` with no inline testcontainers.
- `medium` — the 5s token-exchange clamp is never exercised — `token.go:60-62` uncovered. Every test passes `5*time.Second`, so the branch that actually runs in production (`TRANSLATION_HTTP_TIMEOUT` default 30s → clamp to 5s) is untested; the whole point of that clamp is bounding how long a hung accessToken endpoint holds `p.mu` and every waiting translate.
- `medium` — the handler never asserts that a typed upstream error survives the wrap — `handler.go:39` wraps in `fmt.Errorf("translate backend: %w", err)` and `handler_test.go:72-86` only feeds it a plain `errors.New`. `docs/client-api.md:6317-6318` promises clients `unavailable`/`upstream_unavailable` and `too_many_requests`/`rate_limited`; nothing tests that contract *through the handler*, only inside `translator_stream_test.go`.
- `medium` — token-provider transport failure uncovered — `token.go:113-115`. An unreachable accessToken host (the most likely production failure) has no test; only non-200 / malformed-body cases do (`token_test.go:211`).
- `low` — the double-checked-lock re-entry uncovered — `token.go:81-83`. `TestTokenProvider_ConcurrentReadsNoExtraFetch` (`token_test.go:173`) primes the cache first, so all 50 goroutines take the lock-free fast path and the contended-refresh window never runs.
- `low` — the second-attempt error path after a forced refresh is uncovered — `translator_stream.go:74-76`.
- `low` — the integration test builds its own `natsrouter.Default` (`integration_test.go:37`) rather than production wiring, so `WithSiteID`/`WithMetrics`/`WithMaxConcurrency` and the saturation "service busy" reply are never integration-covered; the backend exercised is `mockTranslator`, so no SSE path crosses NATS.

### Recommendations
- `medium` — Add a `newTokenProvider(url, j1, 30*time.Second, skew)` test asserting the resulting client timeout is 5s.
- `medium` — Add handler cases where the mock `Translator` returns `errcode.Unavailable(..., WithReason(TranslateUpstreamUnavailable))` and `errcode.TooManyRequests(...)`, asserting `errors.As` still yields those codes after the wrap.
- `medium` — Cover `token.go:113` with a closed-listener accessToken URL, mirroring `TestStreamTranslator_TransportErrorUnavailable`.
- `low` — Cover `token.go:81` with a blocking accessToken handler and two concurrent `Token` calls asserting exactly one exchange.
- `low` — Extend `integration_test.go` to register via the same helper `main` uses, and add a saturation subtest asserting the `unavailable`/"service busy" envelope documented at `docs/client-api.md:6316`.

---

---

## 5. Maintainability — 4 / 5

Seventeen small, single-purpose files with genuinely WHY-shaped comments; only `translateOnce` has outgrown one function's worth of responsibility.

### Findings
- `medium` — `translateOnce` (88 lines, `translator_stream.go:87-174`) does four distinct jobs: request construction, HTTP status classification, SSE framing/merge, and post-loop error taxonomy — with a labeled loop, a nested `switch`, and a deferred `readErr` check whose ordering subtlety needs its own comment (`:150-152`). Adding one more upstream behaviour (e.g. a `[HEARTBEAT]` sentinel or a size cap) means editing the middle of this loop.
- `low` — decoded-but-unused response fields: `accessTokenResponse.Username` / `.JwtRequestID` (`token.go:29-30`) and `streamChunk.ReturnMessage` (`token.go`/`translator_stream.go:34`) — the latter is deliberately not propagated (`:142-143`), the former two are simply dead.
- `nitpick` — `backendRequest.ApplyWiki` is hardcoded `false` at the only call site (`translator_stream.go:95`); if it is never going to vary, it belongs in a comment, and if it will, it belongs in `Config`.

The refactor I would actually do: extract `readStream(r io.Reader) (merged string, sawData, sawDone bool, err error)` from `:120-173`, leaving `translateOnce` as request → status classification → `readStream` → taxonomy. That makes the SSE parser unit-testable against a `strings.Reader` without an `httptest` server and shrinks the largest function by half.

### Recommendations
- `medium` — Extract the SSE read loop into `readStream` and unit-test it directly.
- `low` — Delete `Username`/`JwtRequestID` from `accessTokenResponse` unless they are wanted for logging.
- `nitpick` — Promote `ApplyWiki` to config or document why it is pinned false.

---

---

## 6. Integration — 4 / 5

Contract discipline is strong — subject builders throughout, and `docs/client-api.md` plus both derived views are accurate and in sync — with only a model-tag deviation and one undocumented operational knob.

### Findings
- Verified correct: the client-facing subject is built by `subject.TranslateRequestPattern` (`main.go:117`), never `fmt.Sprintf`; `pkg/subject/subject.go:1792-1800` matches `docs/client-api.md:6264` and `docs/client-api/request-reply.md:2330` exactly; the error table (`docs/client-api.md:6313-6319`) enumerates every code the handler and translator can actually produce — `empty_text` (`handler.go:27`), `unsupported_lang` (`handler.go:34`), `upstream_unavailable` (`translator_stream.go:101,116,158`), `rate_limited` (`translator_stream.go:110`), saturation `unavailable`, and `internal`. `docs/client-api/events.md` correctly carries no translate entry — the service emits no events.
- Not applicable and correctly absent: no JetStream stream, no INBOX/OUTBOX participation, no `outbox.Publish` partition membership to check, no Cassandra/`msgbucket`, no `pkg/idgen` IDs, no `ROOM_KEY_RETIRED_TTL`. The event-`Timestamp` rule does not bind: `TranslateRequest`/`TranslateResult` are request/reply payloads, not NATS events.
- `low` — `pkg/model/translation.go:11,20` violates the CLAUDE.md §3 "both `json` and `bson` tags" rule (repeated from D1 as it is a cross-service contract type).
- `low` — `MAX_CONCURRENCY` is a client-visible behaviour (it produces the `unavailable`/"service busy" reply documented at `docs/client-api.md:6316`) but the knob and its default appear in no deploy artifact: `translation-service/deploy/docker-compose.yml` sets only `TRANSLATION_BACKEND=mock`, and the stream-backend env (`TRANSLATION_ENDPOINT`, `TRANSLATION_ACCESS_TOKEN_URL`, `TRANSLATION_J1_TOKEN_FILE`) is documented nowhere in the repo.

### Recommendations
- `low` — Add the full env surface (with defaults) to the service's `deploy/` as commented-out entries, so operators can see `MAX_CONCURRENCY`, `TRANSLATION_HTTP_TIMEOUT`, `TRANSLATION_TOKEN_SKEW`.
- `low` — Resolve the `bson`-tag deviation in CLAUDE.md or in `pkg/model/translation.go`.
- `nitpick` — When `REQUEST_TIMEOUT` is added (D2), document the resulting timeout `unavailable` case in the §3.6 error table and in `docs/client-api/request-reply.md`.

---

---

## 7. Performance — 3 / 5

The token cache is genuinely well-engineered (lock-free reads, single-flight refresh), but the outbound HTTP path has no request deadline and no connection-pool sizing, so a slow upstream converts directly into saturation.

### Findings
- `high` — no per-request deadline anywhere: `main.go:112-116` installs no `HandlerTimeout`, so a handler runs until resty's own client timeout (`TRANSLATION_HTTP_TIMEOUT`, default 30s at `main.go:36`) plus up to 5s of token exchange (`token.go:59-62`). A NATS `Request` caller gives up in ~2s (the pattern used in `integration_test.go:48`), so with a degraded upstream all 100 admission slots stay occupied for ~35s doing work no one will read, and every other caller gets "service busy". The two knobs are unrelated numbers today with nothing tying them together.
- `medium` — the idle connection pool is never sized — `translator_stream.go:51` and `token.go:64` call `restyutil.New` without `restyutil.WithMaxIdleConns`. `pkg/restyutil` documents the reason it exists: "the stdlib keeps only 2, so a third concurrent request pays a fresh handshake". Both clients are single-host with `MAX_CONCURRENCY=100`, so at load ~98 of 100 concurrent translate calls pay a fresh TCP+TLS handshake to the third-party endpoint. `media-service/main.go:112` and `upload-service/main.go:191` already use `WithMaxIdleConns(32)`.
- `low` — on the 429 and 5XX early returns (`translator_stream.go:109-118`) the raw body is closed without being drained, so those connections cannot be reused — exactly the paths where the upstream is already struggling and reconnect cost is highest.
- `low` — no input size cap: `handler.go:26` only rejects empty text, and `docs/client-api.md:6275` confirms "No length cap is enforced by the service". The merge buffers (`merged`, `nonSSE` at `translator_stream.go:121-122`) grow unbounded within the HTTP timeout, and oversized text is forwarded verbatim to a metered third-party API.

No `time.Sleep`-for-synchronization, no goroutine without a termination path (the only concurrency is `sync.Mutex` + `atomic.Pointer` in `token.go:52-53`, with the read path correctly lock-free and the double-checked lock at `token.go:74-84` collapsing a stampede to one exchange). No MongoDB, Cassandra, JetStream or `jsretry` surface in this service, so the projection, `$lookup`, `Nak()`/`BackOff` and `USING TIMESTAMP` rules do not apply.

### Recommendations
- `high` — Install `HandlerTimeout` via `natsrouter.GuardConfig` and set `TRANSLATION_HTTP_TIMEOUT` below it (e.g. `REQUEST_TIMEOUT=5s`, HTTP 4s), so a handler can never outlive the caller that is waiting on it.
- `medium` — Pass `restyutil.WithMaxIdleConns(n)` to both `restyutil.New` calls, sized from `MAX_CONCURRENCY`.
- `low` — Drain a bounded prefix of the body (`io.CopyN(io.Discard, body, 4<<10)`) before closing on the 429/5XX returns to keep connections reusable.
- `low` — Add a configurable `TRANSLATION_MAX_TEXT_BYTES` rejected as `bad_request` with a new reason, and document it in `docs/client-api.md` §3.6.
