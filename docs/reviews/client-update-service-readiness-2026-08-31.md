# client-update-service — Production Readiness Review

**Service:** `client-update-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.3 / 5

A small, unusually well-crafted service. The upload path never buffers the artifact — parts stream from the multipart header straight into `Put`, and a peak-heap assertion proves it at a 48 MiB artifact under a 24 MiB ceiling. Concurrent cache misses collapse through `singleflight` with a generation counter that stops an upload-racing fill from reviving stale bytes. Credentials are compared in constant time with no early break, and every discarded error carries a justification comment.

One defect undercuts all of it. **`ReadTimeout` is hardcoded to 30 seconds**, and `ReadTimeout` bounds reading the *entire request including the body* — so with `UPLOAD_MAX_BYTES` at 2 GiB, a full-size artifact needs a sustained ~570 Mbps to finish in time, and a 200 MiB one needs ~57 Mbps. `HTTP_WRITE_TIMEOUT` (10m) does not help; it governs the response. This silently breaks `admin-service`'s documented 10-minute relay budget, and the sibling `upload-service` gets it right *and says why*. It is invisible to tests because it lives in `run()`, which is 0% covered — the same 50 statements that account for nearly the whole coverage shortfall.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 4 | 5 | 13 | 2 | **24** |

---

## 2. Go code quality — 4 / 5

Idiomatic, well-wrapped, secret-safe code that follows CLAUDE.md Section 3 closely; the only real defects are a non-`errors.Is` sentinel comparison and a missing response hardening header.

### Findings
- `low` — `err != http.ErrServerClosed` compares a sentinel by identity instead of `errors.Is` — `client-update-service/main.go:101`
  It works only because `ListenAndServe` returns the sentinel unwrapped; any future wrapping silently turns a clean shutdown into a fatal error.
- `low` — download responses echo an uploader-supplied `Content-Type` (`fh.Header.Get("Content-Type")`, `version.go:105`) back to unauthenticated GETs with no `X-Content-Type-Options: nosniff` — `client-update-service/version.go:199-203`
  `Content-Disposition: attachment` is the only mitigation; a `text/html` part header stored today is served from this origin tomorrow.
- `low` — audit-coverage gap, not a service defect: `gosec` and the 18 repo-owned semgrep rules are clean repo-wide, but `govulncheck` and the semgrep registry packs could not run (blocked egress) — `GLOBAL_PREP.md:8-9`
- `nitpick` — `errhttp.Write(ctx, c, fmt.Errorf(...))` at `version.go:152` and `:188` is the correct Tier-1 pattern for infra errors, and nothing in the service logs-and-returns; noted as verified, not as a defect.

Confirmed clean: `errcode` Tier-1 constructors only (`BadRequest`/`NotFound`/`Unauthenticated`), no `WithCause` over an `*errcode.Error`, no `WithReason` where the frontend does not branch; `log/slog` structured k/v only, no `fmt.Println`; token never reaches an error string or a log line (`config.go:113`, asserted by `config_test.go:103` and `middleware_test.go:157`); constant-time token compare with no early break (`middleware.go:36-47`); every discarded error carries a justification comment (`version.go:102-104`, `store_minio.go:45`).

### Recommendations
- `low` — replace `err != http.ErrServerClosed` with `errors.Is(err, http.ErrServerClosed)` (`main.go:101`).
- `low` — set `X-Content-Type-Options: nosniff` in `serveBytes` and `streamObject`, or allow-list the stored content type to the two the upload path already fallbacks to.
- `low` — record the `govulncheck`/registry-pack gap in the PR so the CI `sast` gate is understood to have run partially in this environment.

---
