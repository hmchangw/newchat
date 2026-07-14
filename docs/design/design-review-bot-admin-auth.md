# Design Review — Bot/Admin Auth, Admin Portal & Unified `/api/v1` Gateway

**Author:** Ashwini  ·  **Date:** 2026-07-08  ·  **For:** Manager design review

**Scope:** PR #472 (backend) + PR #473 (frontend). Traffic-sizing context from PR #461.

---

## Problem Statement

The chat platform is adding a **bot platform** as a first-class traffic tier, but there is
no way for **bot or admin accounts to authenticate** and no console to manage them:

- Bot and admin accounts never appear in the HR employee feed, so the existing HR-driven
  directory **cannot resolve them** — they literally cannot log in today.
- There is no admin console to create bots/admins, set passwords, manage sessions, or audit
  actions. Operations would be manual DB edits.
- Auth/upload/media/botplatform endpoints are **scattered across services and ports**, with
  no single, versioned client surface.

Without this, the bot platform has no trusted front door — nothing can admit, authenticate,
or govern bot traffic.

---

## Degree of Impact if not done well

**Impact factor:** **Critical**

**Impact type:**
- Release/adoption blocker
- Loss of productivity
- Potential secret leaks

### Details

- **Blocks ~half of all message traffic.** Capacity planning (PR #461) sizes steady state at
  **4M messages/day = 2.1M human + 1.9M bot** — bots are a **~50% traffic tier**. The bot
  pipeline runs on parallel `BOT_*` streams (`BOT_MESSAGES_CANONICAL`, `BOT_PUSH_NOTIFICATION`,
  `BOT_PLATFORM`) sized at **1.9M bot messages/day** and **~190M bot room deliveries/day**.
  None of that traffic can be admitted or trusted without bot auth. *(Volumes are planned/
  placeholder capacity, not yet measured in production.)*
- **Adoption blocker:** every planned bot/admin integration story depends on this login +
  session layer. It gates the whole botplatform roadmap.
- **Security risk if done poorly:** password hashing, session tokens, and NATS-JWT scoping
  must be exactly right. Inconsistent hashing across services or over-broad NATS scope would
  leak or over-privilege credentials.
- **Productivity:** with no admin console, provisioning and troubleshooting bot/admin
  accounts is manual and error-prone.

---

## Proposed Design / Architecture

Two runtime-coupled PRs — **backend (#472)** and **frontend (#473)** — split only to keep
each reviewable. Deploy backend first.

### Backend (PR #472) — 4 services, +10,705 / -283 across 77 files
- **botplatform-service** — bot/admin password login (`POST /api/v1/login`), session-token
  validation (`/api/v1/auth/validate`), password rotation (`/api/v1/password/change`),
  per-site session management with a FIFO session cap, CORS.
- **admin-service** (new) — site-scoped admin REST API: user search/create (any role),
  set password, activate/deactivate, session list/revoke, audit log. `requireAuth` /
  `requireAdmin` middleware validate the botplatform session token; password change proxies
  to botplatform. Paginated endpoints clamp page size.
- **Unified `/api/v1` gateway** — Traefik gateway (`:7777`) fronting auth/upload/media/
  botplatform under `/api/v1/*`. auth-service mints **scoped NATS JWTs** for session tokens.
- **portal directory fix** — rebuilt **users-primary** with `hr_employee` LEFT-joined, so
  bot/admin accounts (never in the HR feed) resolve and can log in; humans still get HR
  enrichment.
- **Shared packages** — `pkg/pwhash` (`bcrypt(sha256hex(pw))`, byte-identical across
  services), `pkg/sessiontoken`, `pkg/principal` (NATS scope).

### Frontend (PR #473) — 2 apps, +11,559 / -36 across 100 files
- **admin-frontend** (new) — Vite/React admin console: `/admin-login`, Users management
  (search, create, edit roles, set password, sessions), audit view. Talks only to
  admin-service + portal.
- **chat-frontend** (edits) — username/password login for bot/admin accounts, forced
  first-login password change, session-token → NATS-JWT refresh.

### Auth flow
Client discovers home site via **portal** → hits `{baseUrl}/api/v1/auth` (auth-service) →
auth-service validates the **botplatform session token** → mints a **scoped NATS user JWT**.
**Bots/admins get the same NATS scope as users — no god-mode.**

---

## Expected Benefits

- **Unblocks the bot platform** — provides the trusted front door for the ~50% bot traffic
  tier so bots/admins can authenticate and be admitted.
- **Fault isolation** — bot traffic runs on a **parallel `BOT_*` pipeline** deliberately
  split from the human flow so it can be *scaled, throttled, and observed independently*;
  a bot flood cannot drown human messages.
- **Least privilege** — bots/admins get the **same scoped NATS JWT as users**, not elevated
  access, limiting blast radius of a compromised bot credential.
- **Consistency & maintainability** — one shared `pkg/pwhash` recipe (byte-identical hashing
  everywhere) and a single versioned `/api/v1` surface reduce drift and maintenance effort.
- **Operability** — admin console + audit log replace manual DB edits with a governed,
  auditable workflow.

**Impact Reduction Type:**
- Release/adoption unblocked
- Enhanced productivity
- Secret leaks prevented

### Details
The bot platform cannot ship without an auth layer; this delivers it, so the release is
unblocked rather than stalled. Centralizing hashing and NATS-JWT scoping removes the classes
of bug (inconsistent hashing, over-broad scope) that would otherwise leak or over-privilege
credentials. The admin console removes manual, error-prone account operations.

---

## Feasibility

**Complexity:** **Medium**

**Why:** The work spans 4 backend services + a gateway + 2 frontend apps and is
runtime-coupled (backend must deploy before frontend). But it **reuses the existing
NATS-JWT scoping pattern** (no new auth primitive), extracts shared logic into small
packages, and is already implemented and verified:

- `make test` (with `-race`) green across botplatform / auth / portal / admin-service +
  `pkg/pwhash`; `make lint` 0 issues; `go build ./...` clean.
- admin-frontend: typecheck clean, **117** vitest tests green.
- chat-frontend: typecheck clean, **689** vitest tests green.

The main risk is **deploy ordering** (backend before frontend) — a coordination concern,
not a design unknown.

**Effective Date:** **in one month** — backend (#472) first, then frontend (#473).

---

## Appendix — Traffic context (PR #461, planned capacity)

Doc-only PR sizing the NATS/JetStream traffic. Bot pipeline is **planned/placeholder**
capacity — not yet reflected in `pkg/stream` / `pkg/subject`, volumes to be replaced when
the bot pipeline is specified.

| Metric | Value |
|---|---|
| Total messages/day | 4.0M (**2.1M human + 1.9M bot**) |
| Bot-originated messages/day → `BOT_MESSAGES_CANONICAL` | 1.9M |
| Bot push notifications/day → `BOT_PUSH_NOTIFICATION` | 1.9M |
| User→bot events/day → `BOT_PLATFORM` (webhook to external platforms) | 100K |
| Bot fan-out per message (F_bot) | 100 |
| Bot room deliveries/day | ~190M |

Rationale in the doc: bot traffic is split onto a parallel `BOT_*` pipeline *"so it can be
scaled, throttled, and observed independently."*
