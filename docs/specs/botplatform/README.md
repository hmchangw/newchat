# Bot Platform NextGen — Spec Set

Three Markdown files + four `.drawio` diagrams cover the **bot-platform-nextgen migration**: password auth + durable sessions for bots and admins, migrating from legacy Rocket.Chat to the nextgen NATS-native stack with zero bot code changes.

## Files in this directory

| File | Purpose | Audience | Lines |
|---|---|---|---|
| **[`auth.md`](./auth.md)** | **Main design spec.** Combined Parts I/II/III under H1 dividers — Part I = architecture & requirements, Part II = technical design, Part III = components & integration. | Everyone reviewing the auth migration | ~1160 |
| **[`migration-runbook.md`](./migration-runbook.md)** | **Operational runbook.** AS-IS vs TO-BE per Mongo collection, idempotent migration pseudocode, per-phase rollback, reconciliation queries. | SRE / ops + the engineer writing the migration job | ~445 |
| **[`traffic-isolation.md`](./traffic-isolation.md)** | **Companion spec.** Routing bot traffic to separate worker pools so bot bursts can't degrade human SLOs. Consumes the `principal.class` field defined in the auth spec. | Platform team owning the chat workers | ~745 |
| **[`diagrams/`](./diagrams/)** | Editable `.drawio` source + rendered PNGs (round-trip via embedded XML) | Visual review | 4 diagrams |

## Reading order

**If you have 15 minutes (team / lead overview):**
1. `auth.md` Part I §1 — executive summary
2. `auth.md` Part I §3 — architecture decision (Option B / DEDICATED-SERVICE)
3. `auth.md` Part I §10 — diagrams (all 4 embedded inline)
4. `auth.md` Part I §9 — phased rollout

**If you have 45 minutes (implementer):**
1. All of the above
2. `auth.md` Part II — full technical design (data model, token format, login flow, validate hot path, config, test plan)
3. `auth.md` Part III §4 — integration points (ApiGW, WebSocket, EventConsumer)

**If you have 90 minutes (full review):**
1. All of the above
2. `migration-runbook.md` — every section
3. `traffic-isolation.md` — every section

**If you're SRE / ops doing the migration:**
- `migration-runbook.md` is the primary doc — read it end-to-end before touching anything in production.
- Cross-reference `auth.md` Part II §4 (data model) for the schemas and §10 (cutover) for the Istio canary mechanics.

## Diagrams

| File | What it shows |
|---|---|
| [`diagrams/login-old-vs-new.drawio`](./diagrams/login-old-vs-new.drawio) | Side-by-side: legacy Rocket.Chat login vs nextgen botplatform-service. Bottom panel = wire-level backward compatibility. |
| [`diagrams/token-gen-validate-flow.drawio`](./diagrams/token-gen-validate-flow.drawio) | Generation pipeline (top) + validation flow with prefix-dispatch (bottom). Middle band = design rationale for `bp1_` + HMAC. |
| [`diagrams/bot-login-flow.drawio`](./diagrams/bot-login-flow.drawio) | 17-step end-to-end sequence: DNS → chat-GW → wsp-GW → botplatform-service → stores. |
| [`diagrams/cross-cluster-cutover.drawio`](./diagrams/cross-cluster-cutover.drawio) | Topology + canary control surface. Per-namespace DNS binding, weighted VirtualService, post-sunset steady state. |

Each `.drawio` has a paired `.drawio.png` (rendered preview with embedded XML — opens back in draw.io desktop as fully editable).

## Cross-spec citation convention

When this spec set says "Part II §9.8" or "Part III §4.1", it refers to a **section within the combined auth spec** (`auth.md`). The three parts are separated by H1 dividers within that single file. Use Ctrl-F / Cmd-F on the part heading to jump.

When citing across files, the convention is full filename + section:
- `auth.md` Part II §9.8 — `/v1/auth/validate` response schema
- `migration-runbook.md` §5 — per-site rollout ordering table

## Status

| Spec | Status |
|---|---|
| `auth.md` | DESIGN-COMPLETE — pending implementation. Architecture DECIDED 2026-06-15 (Option B / DEDICATED-SERVICE). |
| `migration-runbook.md` | DRAFT — see §7 for 5 items ops/external teams must confirm before moving out of DRAFT. |
| `traffic-isolation.md` | DESIGN-COMPLETE — all decisions resolved 2026-06-16. Follow-on to the auth migration. |

## Companion (out-of-scope here)

- **PR #295** — portal-service + provisioning gate for human SSO. Touches `pkg/subject` (which the bot-account namespace fix also touches). Coordinate `pkg/subject` edits during implementation.
