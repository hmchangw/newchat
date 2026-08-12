# SP4 (core) — Operator-driven failover state in portal-service

> **Revised 2026-08-12 — rewritten after a collaborative brainstorm.** The first
> draft designed auto-detection (a health probe, `suspected` state, config-gated
> auto-failover, `hold_healthy`). We chose an **operator-confirmed** posture and
> **deferred the probe**, so this slice is purely the operator-driven state
> machine + control surface + the `servingTarget` signal. Auto-detection/alerting
> is a later add-on that only ever writes an advisory status and fires an alert —
> it never flips anything. The original probe design is superseded, not retained.
>
> - **Governing design:** `docs/superpowers/specs/2026-07-28-cross-site-failover-design.md` (§3 "Failover trigger" knob, §4.2, §6.3)
> - **Roadmap:** `docs/superpowers/plans/2026-07-28-cross-site-failover-roadmap.md` (SP4 — "feeds SP3")
> - **Consumer:** SP3 (portal routing override) reads the `servingTarget` signal. **Hands off to:** SP5 (failback lag-gate).

## 1. Goal

Give `portal-service` — the one globally-scoped service, already the cross-site
routing authority and split-brain fence (§4.2) — an authoritative, per-site
answer to *"is this site served from home or from the backup right now, and who
decided that?"*, plus the operator controls to drive it. An operator who knows
(from existing infra monitoring) that site A is down can **fail it over** to the
backup and later **fail it back**, and SP3 reads the resulting signal to route.

Explicitly **not** in this slice: automatic detection (deferred), the routing
override itself (SP3), the failback data replay + lag gate (SP5), and any UI /
per-operator login (a deliberate follow-up — see §8).

## 2. Decisions (from the brainstorm)

- **Operator-confirmed, not automatic.** A human performs every flip. There is no
  auto-failover, so no `AUTO_FAILOVER`, no dwell-to-promote, no `hold_healthy`
  (those only existed to manage automation).
- **Probe deferred.** No health probe in this slice. Operators learn of outages
  from existing monitoring; the probe would only add a convenience alert and is
  additive later (it will write an advisory status + alert, never flip state).
- **Control surface on `portal-service`.** The failover decision is *global*; the
  existing `admin-service` auth is *per-site* (`requireAdmin` checks
  `sess.SiteID == siteID`), so it does not fit. Portal is the correct home.
- **Auth = dedicated ops bearer token, internal-only.** A break-glass ops secret
  gates the control routes on an internal listener, kept off portal's public
  browser-facing API. `operator` id is carried in the request for audit
  (self-asserted under this model — see §8 for the option-3 upgrade path).

## 3. The `FailoverState` signal (the SP3 contract)

One document per active site (`_id = siteID`) in portal's existing Mongo DB:

| field | type | meaning |
|------|------|---------|
| `siteId` | string (`_id`) | the active site this state governs |
| `status` | enum | `healthy` \| `failed_over` \| `failing_back` |
| `servingTarget` | enum (derived) | `home` unless `status ∈ {failed_over, failing_back}` → `backup` |
| `reason` | string | operator note (why) |
| `operator` | string | ops identity that made the last transition (audit) |
| `since` | int64 (ms) | when the current `status` began |
| `version` | int64 | optimistic-concurrency guard for the sole-writer CAS (§7) |
| `timestamp` | int64 (ms) | event-level publish time, per CLAUDE.md "Event Timestamps" |

**`servingTarget` is the entire SP3 contract.** SP3's `resolve()` reads it: if
`backup`, return the backup's coordinates instead of the home site's; else
unchanged. Nothing else about SP4 leaks into SP3 — a single-field seam.

## 4. State machine

Operator-driven; every edge is CAS-guarded on `version` (§7):

```
   healthy ──── failover ────► failed_over ──── failback ────► failing_back
      ▲                            │                                │
      │                            │ resume (false alarm)           │ complete
      └──────────────┬─────────────┘                                │
                     └──────────────── (terminal flip) ◄────────────┘
```

- `failover`: `healthy → failed_over` (operator declares site down; serve from backup).
- `resume`: `failed_over → healthy` directly (false alarm; skip the drain).
- `failback`: `failed_over → failing_back` (begin draining home).
- `complete`: `failing_back → healthy` (drain done).
- `servingTarget` stays `backup` through `failing_back` — users keep writing to a
  stable backup while home drains.

**Forward-compat hook for SP5:** `failing_back` and its terminal `complete` flip
exist so SP3's contract is stable and SP5 has its seam. **In this slice `complete`
is a manual operator action** (there is no backup serving or replay to gate on
yet). When SP5 lands, it owns gating `complete` on lag≈0; the state and the
transition are unchanged — SP5 just becomes the actor that calls it.

Illegal transitions (e.g. `healthy → failing_back`) are rejected.

## 5. Control surface

New routes on an **internal-only HTTP listener** in portal (separate from the
public discovery server, so no privileged write shares the browser-facing
surface), gated by the ops-token middleware:

- `GET  /internal/v1/failover` — list every site's `FailoverState`.
- `GET  /internal/v1/failover/:siteId` — one site's state.
- `POST /internal/v1/failover/:siteId` — body
  `{ "action": "failover"|"failback"|"complete"|"resume", "operator": "...", "reason": "..." }`.

Each write validates the transition against §4, CAS-updates on `version`, and
records `operator`/`reason`/`since`/`timestamp`. A stale-`version` write (racing
replica or stale client) returns `409 conflict` (`errcode.Conflict`) and the
caller re-reads. All actions are audit-logged via structured `slog` (from/to
status, `operator`, `reason`) — never the token, per CLAUDE.md logging rules.

## 6. Read path (the signal SP3 consumes)

A `FailoverReader` exposing `ServingTarget(ctx, siteID) → home|backup`, backed by
the store behind a **short TTL cache** (`FAILOVER_STATE_TTL`, default a few
seconds): long enough to shield Mongo from per-login reads, short enough that a
flip propagates to routing within seconds. This is **separate** from portal's 24h
directory cache and must not be merged into it.

**Fail-safe = `home`.** A missing/unreadable state derives `home` (normal
routing) — an unreachable failover store must never strand a healthy site's users
on the backup. SP4 builds this reader; SP3 wires it into `resolve()`.

## 7. Split-brain fence — sole writer, CAS, multi-replica

- **Single source of truth:** one `FailoverState` document per site; `servingTarget`
  is single-valued, so a site is `home` xor `backup`, never both.
- **Sole writer, CAS-guarded:** only portal writes it, each transition a Mongo
  conditional update on `version`. Multiple portal replicas stay correct without
  leader election — concurrent identical transitions are idempotent, a divergent
  one loses the CAS and re-reads.

## 8. Forward-compatibility to the option-3 console (later)

A per-operator login + failover console UI is a planned follow-up. This slice is
shaped so that lands as an extension, not a rewrite:

- **Auth is a Gin middleware seam** (`requireOps`), mirroring admin-service's
  `requireAdmin`. Option 3 swaps in session-based platform-admin auth by replacing
  the middleware — handlers and state machine untouched.
- **Endpoint contract stays stable**, so a future console calls the same routes.
- **`operator` stays in the model** — self-asserted now, session-derived once real
  login exists; the audit schema does not change.

## 9. Config (`caarlos0/env`)

| Env | Default | Notes |
|-----|---------|-------|
| `FAILOVER_OPS_TOKEN` | — | required to enable the control surface; the break-glass ops secret |
| `FAILOVER_INTERNAL_ADDR` | `:8090` | internal listener for the control routes |
| `FAILOVER_STATE_TTL` | `5s` | read-path cache TTL |

Portal already holds a Mongo handle (the directory store), so the collection
lives there; no new datastore.

## 10. Testing (TDD, per CLAUDE.md §4)

- **Unit — state machine:** table-driven over every legal edge; illegal edges
  rejected; `servingTarget` derivation per status; CAS version bump per write;
  stale-version → conflict.
- **Unit — control surface:** missing/blank token → 401; wrong token → 403; each
  action drives the correct transition + audit fields; unknown action → 400; CAS
  conflict → 409; token never logged.
- **Unit — reader:** TTL cache returns cached value within the window and refreshes
  after; missing document → `home` (fail-safe).
- **Integration** (`//go:build integration`, `testutil.MongoDB`): `FailoverStore`
  round-trip; two concurrent CAS writers — exactly one wins, the other re-reads;
  TTL-cached read reflects a write after the TTL.
- **Coverage** — ≥80% floor; ≥90% on the state machine + control-surface logic.

## 11. Out of scope (each its own slice)

- **Auto-detection / health probe / alerting** — deferred; a later additive slice
  that writes an advisory status + fires an alert, never flips state.
- **SP3** — the routing override (`resolve()` returning backup coordinates), the
  backup-URL registry entry, and reconnect nudging. SP4 only produces the signal.
- **SP5** — failback replay, convergence, and gating `complete` on lag≈0.
- **UI + per-operator login** — the option-3 console follow-up (§8).
- **SP6 — ops/IaC:** the internal listener's network exposure, the ops-token
  secret provisioning, and the backup deployment.

## 12. Open sub-decisions (call out in the plan)

1. **Internal listener mechanism** — a separate `http.Server` on
   `FAILOVER_INTERNAL_ADDR` (clean separation, this spec's lean) vs. a route group
   on the main server protected by network policy. Plan-level call.
2. **`complete` in this slice** — expose it as a manual operator action now (so
   failback is operable end-to-end pre-SP5), confirmed above; SP5 later becomes the
   actor that gates and calls it. No schema change when that happens.
