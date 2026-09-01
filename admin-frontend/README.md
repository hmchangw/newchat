# admin-frontend

Internal admin console for the chat platform. Admin operators log in via
`admin-service`'s `POST /v1/login` (see `docs/client-api.md` §9.10) and manage
users — search, create, edit roles, set passwords, activate/deactivate,
inspect/revoke sessions — and review the audit log of admin actions, all over
`admin-service`'s REST API.

This is a separate Vite/React app from `chat-frontend`; it does not embed a
chat client.

## Environment variables

**Dev (`npm run dev`)** — read via `import.meta.env`:

| Variable | Purpose | Default |
|---|---|---|
| `VITE_ADMIN_SERVICE_URL` | admin-service base URL (REST API) | `http://localhost:8082` |
| `VITE_PERMISSIONS_ENABLED` | shows the Permissions tab when the literal string `true` | unset (hidden) |
| `VITE_UPDATES_ENABLED` | shows the Updates tab when the literal string `true` | unset (hidden) |
| `VITE_ROOM_ONDUTY_MIN_MEMBERS` | member floor below which the Rooms tab hides "set onduty" — must equal room-service's `RESTRICTED_ROOM_MIN_MEMBERS` | `5` |

**Container (nginx runtime, `/config.js` rendered by `deploy/30-render-config.sh`)**:

| Variable | Purpose | Required |
|---|---|---|
| `ADMIN_SERVICE_URL` | admin-service base URL | yes — container fails to start if unset |
| `PERMISSIONS_ENABLED` | shows the Permissions tab when the literal string `true` | no — defaults to `false` (tab hidden) |
| `UPDATES_ENABLED` | shows the Updates tab when the literal string `true` | no — defaults to `false` (tab hidden) |
| `ROOM_ONDUTY_MIN_MEMBERS` | member floor below which the Rooms tab hides "set onduty". Must equal room-service's `RESTRICTED_ROOM_MIN_MEMBERS`, which is the real enforcement | no — defaults to `5` |

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
via `envsubst` at container start and fails fast if `ADMIN_SERVICE_URL` is unset.

## Phase 2 (not in this app yet)

The embedded NATS chat client (rooms/messages, `NatsContext`, room
crypto, etc.) is out of scope for Phase 1 and will be added in a follow-up
plan.
