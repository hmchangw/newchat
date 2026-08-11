# SP4 (core) — Failover trigger: health detection & operator control surface

> **Scope discipline.** This slice resolves design open-question §11.3: **how a
> site is declared down** (auto-detection + manual override) and **the control
> surface** an operator drives it from. Its load-bearing output is the
> **failover-state signal** that SP3 (the routing brain) consumes — so this spec
> is deliberately precise about that signal's shape and authority, and defers the
> routing *use* of it to SP3. Everything downstream — the actual reroute in
> `resolve()` (SP3), the failback replay (SP5) — is out of scope.
>
> - **Governing design:** `docs/superpowers/specs/2026-07-28-cross-site-failover-design.md` (§3 "Failover trigger" knob, §4.2, §6.3, §11.3)
> - **Roadmap:** `docs/superpowers/plans/2026-07-28-cross-site-failover-roadmap.md` (SP4 — "feeds SP3")
> - **Consumer:** SP3 (portal health-aware routing override); **hands off to:** SP5 (failback).

## 1. Goal

Give `portal-service` — already the routing brain and the design's designated
**split-brain fence** (§4.2) — an authoritative, per-site answer to *"is this
site being served from home or from the backup right now, and who decided
that?"*, and the operator controls to drive it. Concretely:

1. A **persistent, portal-owned failover-state record** per site, whose derived
   `ServingTarget` (home | backup) is the single fact SP3 reads to route.
2. **Auto-detection** that watches each site's reachability and *proposes* a
   failover, conservatively (multi-sample + dwell, flap-resistant).
3. A **manual operator control surface** (portal-owned, admin-gated) that is
   **always authoritative** over the automation: force a failover, hold a site
   healthy (maintenance), or initiate failback.

This is the design's stated knob — "automatic health-detection with a manual
operator override" (§3). This slice **does not** reroute traffic (SP3), does not
replay outage writes (SP5), and does not mint JWTs (SP2). It produces and governs
the *state*; SP3 acts on it.

## 2. Decision — a portal-owned failover-state machine; auto-detection proposes, the operator disposes

Three moves, in priority of what unblocks SP3:

**(a) The signal is a Mongo-backed per-site `FailoverState` record, portal is its
sole writer.** Portal already owns a Mongo connection (the directory store's live
`GetByAccount` fallback) and is the single routing authority, so co-locating the
failover state there keeps the fence in one place. It must be **persistent**
(survive portal restart) and **shared across portal replicas** — ruling out the
in-memory directory-cache pattern. SP3 reads it on the routing path via a short
TTL cache (seconds, §6), *not* the 24h directory refresh, so a failover takes
effect promptly.

**(b) Detection is conservative and, by default, advisory.** A probe loop marks a
site `suspected` on sustained unreachability and **alerts**, but the
reroute-causing transition to `failed_over` is **operator-confirmed by default**.
Fully-automatic promotion is a **config-gated maturity step** (`AUTO_FAILOVER`,
default off) with a dwell window — because a false-positive failover is
expensive (needless failover + a full failback protocol) and, if the automation
and an operator disagree, split-brain-adjacent. Portal sees the world from **one
vantage point**, so it cannot by itself distinguish "site A is down" from "portal
can't reach site A"; operator-confirm (or an external-monitor feed, §4) is the
safe resolution of that ambiguity for v1.

**(c) Manual override always wins.** An operator action (`force_failover`,
`hold_healthy`, `failback`) sets an override that the auto-detector must respect —
it can never auto-flip a site the operator has pinned, nor auto-fail-back one the
operator forced over.

### 2.1 Approaches considered (detection mechanism)

- **A — portal active-probe + operator-confirm, external-monitor feed optional
  (CHOSEN).** Portal probes each site's user-facing reachability; sustained
  failure ⇒ `suspected` + alert; operator confirms (or `AUTO_FAILOVER` promotes
  after dwell). Reuses portal's existing loop shape; keeps the fence and the
  decision in one authority; conservative by construction. Optional: consume an
  external monitor's verdict (below) as a corroborating signal.
- **B — external monitoring drives it end-to-end** (Prometheus/Alertmanager or
  k8s health flips the state; portal only reads). Decouples detection from
  portal's single vantage point (multiple probers, battle-tested), but moves the
  *trigger* authority outside the split-brain fence and couples DR to a specific
  monitoring stack this repo doesn't standardize. Kept as the corroborating feed
  in A, not the sole authority.
- **C — sites heartbeat to portal** (absence ⇒ down). Symmetric to probing and
  same partition ambiguity, but a site that is *up but can't reach portal* looks
  down — a worse failure mode than A. Rejected.
- **D — manual-only** (no automation, operator declares every outage). Simplest
  and safest against false positives, but RTO becomes "time until a human
  notices." Rejected as the *only* mechanism; retained as the effective default
  behavior when `AUTO_FAILOVER` is off (auto-detect still alerts, human still
  acts — but faster, because the alert fires).

## 3. The `FailoverState` signal (the load-bearing SP3 contract)

One document per active site (`_id = siteID`). This is the artifact SP3 is
blocked on; it is defined here in full.

| Field | Type | Meaning |
|------|------|---------|
| `siteId` | string (`_id`) | the active site this state governs |
| `status` | enum | `healthy` \| `suspected` \| `failed_over` \| `failing_back` |
| `override` | enum | `none` \| `force_failover` \| `hold_healthy` \| `force_failback` — operator lock the detector must honor |
| `servingTarget` | enum (derived) | `home` unless `status ∈ {failed_over, failing_back}` → `backup` |
| `reason` | string | free-text (operator note or detector cause, e.g. `nats_unreachable`) |
| `actor` | string | `auto` or the admin account that made the last transition |
| `since` | int64 (ms) | when the current `status` began (for RTO/observability) |
| `version` | int64 | optimistic-concurrency guard for the sole-writer CAS (§6) |
| `timestamp` | int64 (ms) | event-level publish time, per CLAUDE.md "Event Timestamps" |

**`servingTarget` is the entire SP3 contract.** SP3's `resolve()` computes: read
the account's home `siteID`, look up its `FailoverState`; if `servingTarget ==
backup`, return the **backup** site's coordinates from the registry instead of
the home site's; else unchanged. Nothing else about SP4's internals leaks into
SP3 — a clean, single-field seam.

State machine (portal is the only writer; every edge is CAS-guarded on `version`):

```
                 detector: sustained-unreachable + dwell
   healthy ─────────────────────────────────────────────► suspected
      ▲                                                        │
      │ operator failback done / detector: recovered           │ operator confirm
      │ (servingTarget flips home)                             │ OR AUTO_FAILOVER+dwell
      │                                                        ▼
  failing_back ◄───────────── operator: begin failback ─── failed_over
   (still served from backup;                              (served from backup)
    SP5 drains; flip on lag≈0)
```

- `hold_healthy` override pins `healthy` and blocks the `healthy→suspected→
  failed_over` path (maintenance windows, known-flaky links).
- `force_failover` jumps to `failed_over` regardless of probe state (operator
  knows the site is down before the probe agrees).
- `force_failback` / "begin failback" moves `failed_over → failing_back`; the
  terminal `failing_back → healthy` flip is **gated on SP5 reporting lag≈0** and
  is the atomic split-brain-safe cutover (§7).

## 4. Detection mechanism — what portal probes, and how it avoids flapping

- **Probe target = user-serviceability, not liveness.** A site is "servable" iff
  a displaced-nothing user could actually connect and mint: its **NATS is
  reachable** (the client connect target) **and its `auth-service` is healthy**
  (JWT minting). Portal probes the site's NATS monitoring `/healthz`
  (JetStream-enabled check) and `auth-service` `/healthz` on an interval. These
  two being up is the closest cheap proxy for "users can be served from home";
  deeper datastore health is the site's own concern and, if broken, surfaces as
  serving failures the operator escalates.
- **Flap resistance:** require **M consecutive failed samples over a dwell
  window** (config: `FAILOVER_PROBE_INTERVAL`, `FAILOVER_UNHEALTHY_SAMPLES`,
  `FAILOVER_DWELL`) before `healthy → suspected`; require a symmetric healthy
  streak before the detector proposes recovery. No single blip moves state.
- **Advisory vs. auto:** on reaching the unhealthy threshold, always write
  `suspected` + emit a high-severity alert (structured `slog` + the o11y metric
  path). Promotion `suspected → failed_over` happens **only** on operator confirm,
  **unless** `AUTO_FAILOVER=true`, in which case a further auto-dwell promotes it —
  and even then `hold_healthy` blocks it.
- **Corroboration (optional):** an external monitor may `POST` its own verdict
  into the control surface (§5) as a second signal; portal records it in `reason`
  but the state machine and CAS remain portal's.

## 5. Manual-override control surface (portal-owned, admin-gated)

New portal HTTP endpoints (admin-authenticated — reuse the platform-admin
identity already in the codebase, `model.IsPlatformAdmin` / the admin console's
auth; the exact middleware is a plan detail):

- `GET /api/v1/failover` — list every site's `FailoverState` (admin console view
  + operational dashboard source). Read-only.
- `GET /api/v1/failover/{siteId}` — one site's state.
- `POST /api/v1/failover/{siteId}` — operator action, body
  `{ "action": "failover" | "failback" | "hold" | "release", "reason": "..." }`:
  - `failover` → set `override=force_failover`, `status=failed_over`,
    `actor=<admin>`.
  - `hold` → `override=hold_healthy` (block auto-failover; maintenance).
  - `release` → `override=none` (re-arm the detector).
  - `failback` → `status=failing_back`, begin the SP5 drain; the terminal flip to
    `healthy` is completed by SP5's lag≈0 report, not this call.

Every write is CAS-guarded on `version` (§6) so a stale admin UI or a racing
detector cannot clobber a newer transition; a losing writer gets a `409 conflict`
(`errcode.Conflict`) and re-reads. All actions are audit-logged (`actor`,
`reason`, from/to status) via structured `slog` — never the operator's
credentials, per CLAUDE.md logging rules.

## 6. Split-brain fence — sole writer, CAS, multi-replica

The design leans the entire split-brain guarantee on portal being the sole
routing authority (§4.2, §9). This slice makes that concrete:

- **Single source of truth:** the `FailoverState` document is the *only* place a
  site's serving target is decided. A site is `home` xor `backup`, never both,
  because `servingTarget` is derived from one `status` field on one document.
- **Sole writer, CAS-guarded:** only portal writes it, and every transition is a
  Mongo conditional update on `version` (optimistic concurrency). With multiple
  portal replicas each running the detector, concurrent identical transitions are
  idempotent and a divergent one loses the CAS and re-reads — no leader election
  required for correctness (a single-flight/leader detector is an optional
  efficiency, §9, not a correctness need).
- **Read path freshness:** SP3 reads `FailoverState` behind a **short TTL cache**
  (`FAILOVER_STATE_TTL`, default a few seconds) — long enough to shield Mongo
  from per-login reads, short enough that a failover propagates to routing within
  seconds. This is a *different* cache from the 24h directory cache and must not
  be conflated with it.
- **Fail-safe default:** if the state document is missing or unreadable, the
  derived target is **`home`** (fail toward normal routing) — an unreachable
  failover store must not itself trigger or freeze a failover.

## 7. Failback interaction (the SP5 boundary)

SP4 owns the *state transitions* around failback; SP5 owns the *data work*:

- The operator (or, later, automation) moves `failed_over → failing_back` via the
  control surface. `servingTarget` **stays `backup`** through `failing_back` —
  users keep writing to a stable backup while SP5 drains the outage log home
  (design §6.3 steps 1–4).
- SP5 replays + verifies convergence and reports **lag≈0**; on that signal the
  terminal `failing_back → healthy` flip runs (atomic, CAS-guarded), `servingTarget`
  flips to `home`, and clients reconnect home via the existing reconnect-self-heal
  path (§6.3 step 5). SP4 exposes the transition + the lag-gate hook; **SP5 owns
  the replay, the lag measurement, and the convergence diff.**
- The lag signal itself is the per-site replication-lag monitor (design §8, open
  q §11.8, SP6). SP4 *reads* it to gate the flip; it does not build it.

## 8. Testing (TDD, per CLAUDE.md §4)

- **Unit — state machine:** table-driven transition tests (every legal edge;
  illegal edges rejected; `hold_healthy` blocks auto-promotion; `force_failover`
  jumps regardless of probe; `servingTarget` derivation for each status; CAS
  version bump on each write; stale-version write → conflict).
- **Unit — detector:** with an injected clock and a mock prober, assert M-of-N +
  dwell before `suspected`; a single blip does not transition; `AUTO_FAILOVER`
  off leaves promotion to the operator; on with dwell promotes; `hold_healthy`
  still blocks. Probe function is an injected interface (no real NATS/HTTP).
- **Unit — control surface:** each action → correct transition + audit fields;
  non-admin → `403`; unknown action → `400`; CAS conflict → `409`; reason/actor
  recorded, credentials never logged.
- **Integration** (`//go:build integration`, `testutil.MongoDB`) — `FailoverStore`
  round-trips; concurrent CAS writes (two goroutines) — exactly one wins, the
  other re-reads; TTL-cached read reflects a write within the TTL; missing
  document derives `home`.
- **Coverage** — ≥80% floor; ≥90% on the state machine + detector logic.

## 9. Open sub-decisions (call out in the plan)

1. **`AUTO_FAILOVER` default & dwell.** *Leaning: default off* (advisory alert +
   operator confirm) for the first production cut; enable per-deployment once the
   probe's false-positive rate is understood. Dwell/sample tunables land in the
   plan with defaults.
2. **Detector run shape across replicas** — every-replica-with-CAS (correct,
   simplest) vs. a single leader (fewer redundant probes). *Leaning:
   every-replica-with-CAS*; revisit only if probe volume matters.
3. **Backup-site coordinates in the registry** — how `servingTarget=backup`
   resolves to URLs: a reserved backup entry in `PORTAL_SITE_URLS` vs. a dedicated
   `PORTAL_BACKUP_SITE_URLS`. Decided in SP3 (the consumer); noted here because the
   signal references it.
4. **Admin auth middleware** — exact gating for the control surface (platform-
   admin session vs. a dedicated ops credential). A plan detail; must be an
   existing pattern, not a new auth scheme.
5. **Alert transport** — `slog`+metric only vs. also a direct pager hook.
   *Leaning: structured signal only*; paging is an ops/SP6 wiring concern off the
   o11y metrics.

## 10. Out of scope (explicit — each its own slice)

- **SP3** — the routing override itself (`resolve()` returning backup coordinates
  when `servingTarget=backup`), the backup-URL registry entry, and the
  reconnect-nudge to displaced clients. SP4 only produces the signal.
- **SP5** — failback replay, convergence diff, and the lag measurement SP4's flip
  gates on.
- **SP2** — JWT minting on the backup.
- **SP6 — ops/IaC:** the per-site replication-lag monitor SP4 reads, alert paging
  wiring, and the backup's deployment.
