# upload-service — Production Readiness Review

**Service:** `upload-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.0 / 5

A clean, conventional Gin service with genuinely strong streaming and projection discipline — the multipart body never lands in memory, sniff and decode read only headers, both Mongo queries are precisely projected, and the WHY-comments are unusually good. `handler.go` is at 92%, `middleware.go` 98.9%, `mediatype.go` 98.3%.

**It also carries the fleet's one `critical` security finding.** `drive_host` is taken **verbatim from the client query string** and used as the upstream base URL — with the Drive `api-token` header attached. A client picks the host that receives the service's credential. Two more findings compound the exposure: **`resolveMediaType` returns the client-declared Content-Type whenever it is anything but empty or `application/octet-stream`**, so the byte-sniff, `looksLikeSVG` check and extension logic **never run for a lying client** — contradicting the function's own security comment; and **no request-body cap exists**, so `c.MultipartForm()` spools the entire body to the OS temp dir, for up to 15 minutes, before any size limit is consulted.

Coverage is 76.5%, below the floor, with `run()` at 0/70 — and `run()` holds the DEV_MODE/OIDC required-config guard.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 2 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 7 | 21 | 17 | 7 | **53** |
