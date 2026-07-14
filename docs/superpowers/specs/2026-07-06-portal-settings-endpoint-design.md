# Portal `/api/settings` Endpoint — Design

**Date:** 2026-07-06
**Service:** portal-service
**Status:** Approved

## Purpose

The frontend needs two pieces of deployment-level configuration before it can
talk to the backend properly:

1. **`apiVersion`** — which generation of backend endpoints to call.
2. **`otelBaseUrl`** — the base URL for frontend telemetry; the frontend
   appends `/trace` and `/log` to it.

Both are deployment configuration, so the portal (the first thing the frontend
talks to) serves them from environment variables.

## Endpoint contract

`GET /api/settings` — no input parameters, no auth (same discovery-tier trust
as `GET /api/userInfo`).

### Success response — `HTTP 200`

| Field | Type | Notes |
|---|---|---|
| `apiVersion` | string | Backend API generation the frontend should target (e.g. `v2`). Opaque string, not parsed by the portal. |
| `otelBaseUrl` | string | Base URL for frontend OTEL telemetry. No trailing slash — the frontend appends `/trace` and `/log`. |

```json
{
  "apiVersion": "v2",
  "otelBaseUrl": "https://otel.site-a.example.com/v1"
}
```

### Error response

Shared `errcode` envelope (`errhttp.Write`), consistent with the rest of
portal-service. This endpoint has **no runtime error path** — both values are
validated at startup — so only the framework-level `500 internal` row exists.

## Configuration

Two new fields on the `config` struct in `main.go`. Both are **critical**
config: no `envDefault`, fail fast at startup. `PORTAL_`-prefixed per the
CLAUDE.md service-prefix rule (revised after review: the earlier unprefixed
names collided with the OTel SDK's reserved `OTEL_*` env namespace).

```go
APIVersion  string `env:"PORTAL_API_VERSION,notEmpty"`
OTELBaseURL string `env:"PORTAL_OTEL_BASE_URL,notEmpty"`
```

`notEmpty` (user-service precedent) fails when the variable is unset **or**
set-but-empty.

### Startup validation

`parseOTELBaseURL(raw string) (string, error)` in `handler.go` (mirrors
`parseSiteURLs`):

- Must parse as an absolute `http` / `https` URL (non-empty host).
- Must not carry credentials, a query string, or a fragment — any of those
  breaks or leaks when the client appends `/trace` / `/log`. Rejection errors
  report `u.Redacted()` so a mistyped credential never reaches the startup log.
- Trailing slashes are trimmed from the path and the normalized `u.String()`
  is returned, so a configured `https://host/v1/` cannot produce
  `https://host/v1//trace` on the frontend.
- Any violation exits the service at startup instead of surfacing at the
  first frontend telemetry call.

`APIVersion` is an opaque string — `notEmpty` is its only validation.

## Wiring

The validated values are packed into a `settingsResponse` struct at startup
and passed as one extra parameter to `NewPortalHandler`. `HandleSettings`
writes the precomputed struct — a single in-memory read, no per-request work.

Route registered in `routes.go`: `r.GET("/api/settings", h.HandleSettings)`.

## Testing (TDD, red → green → refactor)

- `parseOTELBaseURL` — table-driven: valid http/https passes; trailing
  slash(es) trimmed; empty, garbage, relative URL, and non-http scheme
  rejected.
- `HandleSettings` — `HTTP 200` with exact JSON field names (`assert.JSONEq`),
  values round-tripped from handler construction.

## Documentation (same PR)

- New subsection in `docs/client-api.md` §2 (numbered without disturbing
  existing sections), following the §2.3 portal style.
- Stub entry in the derived view `docs/client-api/request-reply.md` linking
  back to the canonical section.

## Deployment

`portal-service/deploy/docker-compose.yml` gains both variables with dev
values:

```yaml
- PORTAL_API_VERSION=v2
- PORTAL_OTEL_BASE_URL=http://localhost:4318/v1
```
