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
