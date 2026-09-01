# media-service — Production Readiness Review

**Service:** `media-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.3 / 5

A conventional, well-built Gin service: consumer-defined store interfaces, constructor DI, `pkg/subject` builders, shared knobs mounted correctly, streaming with ETag/304, and a singleflight LRU. The handler, upload, serve, drive and middleware layers all sit at 95–100% coverage with real error-path assertions.

Two performance findings have security consequences. **`Cache-Control: public, max-age=21600` and the `ETag` are written before the blob fetch**, so a MinIO 500 or a not-found inherits them and becomes a **shared-cache-storable error response** — on both the avatar and the emoji serve paths. And **the bot-avatar upload calls `image.Decode` with no `DecodeConfig` dimension pre-check**, so a small compressed PNG declaring huge dimensions allocates the full pixel buffer before any bound applies — while the emoji path one file over *does* have that guard, having diverged from a shared shape that was copy-pasted rather than factored.

Around those: `drive.members` bypasses `pkg/errcode` entirely for a bespoke envelope and is absent from `docs/client-api.md`; `Access-Control-Allow-Origin: *` is hardcoded onto two authenticated PUT routes with `x-auth-token` in the allowed headers; and authorization is asymmetric across transports — HTTP emoji upload is admin-gated while the NATS `emoji.delete` RPC is open to any authenticated account.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 3 | 21 | 18 | 10 | **52** |
