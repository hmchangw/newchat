# admin-frontend

Internal admin console for the chat platform. Admin operators log in (reusing
the `/admin-login` flow served by `portal-service`/`auth-service`) and manage
users — search, create, edit roles, set passwords, activate/deactivate,
inspect/revoke sessions — and review the audit log of admin actions, all over
`admin-service`'s REST API.

This is a separate Vite/React app from `chat-frontend`; it does not embed a
chat client.

## Environment variables

**Dev (`npm run dev`)** — read via `import.meta.env`:

| Variable | Purpose | Default |
|---|---|---|
| `VITE_PORTAL_URL` | portal-service base URL (login) | `http://localhost:8081` |
| `VITE_ADMIN_SERVICE_URL` | admin-service base URL (REST API) | `http://localhost:8082` |

**Container (nginx runtime, `/config.js` rendered by `deploy/30-render-config.sh`)**:

| Variable | Purpose | Required |
|---|---|---|
| `PORTAL_URL` | portal-service base URL | yes — container fails to start if unset |
| `ADMIN_SERVICE_URL` | admin-service base URL | yes — container fails to start if unset |

`src/lib/runtimeConfig.js` reads `window.__APP_CONFIG__` first (prod), falling
back to the `VITE_*` env vars (dev), falling back to the literal defaults
above as a last resort.

## Commands

```
npm run dev        # start Vite dev server (port 3001)
npm run build      # production build to dist/
npm test           # vitest run
npm run typecheck  # tsc --noEmit
```

## Deploy

`deploy/Dockerfile` builds `dist/` with `node:22-alpine` and serves it from
`nginx:alpine`. Build context is the repo root (mirrors `chat-frontend`):

```
docker build -f admin-frontend/deploy/Dockerfile -t admin-frontend .
```

`deploy/nginx.conf` serves the SPA (`try_files` fallback to `index.html`) and
`/config.js`. `deploy/30-render-config.sh` renders `deploy/config.js.template`
via `envsubst` at container start and fails fast if `PORTAL_URL` or
`ADMIN_SERVICE_URL` is unset.

## Phase 2 (not in this app yet)

The embedded NATS chat client (rooms/messages, `NatsContext`, room
crypto, etc.) is out of scope for Phase 1 and will be added in a follow-up
plan.
