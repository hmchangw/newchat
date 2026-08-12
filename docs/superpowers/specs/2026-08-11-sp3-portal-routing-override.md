# SP3 (core) — Routing brain: portal-service health-aware override

> **Scope discipline.** This slice makes `portal-service` return the **backup
> site's** connection coordinates for a down site's SSO users, keyed entirely on
> SP4's `servingTarget` signal. It is **codebase-local** — a thin override at
> portal's existing resolve seam — and deliberately owns *only* the routing
> decision. Detection/state (SP4), JWT minting (SP2), and failback replay (SP5)
> are out of scope.
>
> **Revised 2026-08-12 after a collaborative brainstorm.** Decisions locked:
> backup coordinates as a reserved entry in `PORTAL_SITE_URLS` (§4); the override
> is scoped to the **SSO `/api/userInfo` path only** — the bot/admin
> `/api/v1/login` path is left unchanged (§2.2); failback reconnect is drain-only,
> **no SP3 code** (§5); the TTL-cached reader is built here (§6).
>
> - **Governing design:** `docs/superpowers/specs/2026-07-28-cross-site-failover-design.md` (§4.2, §6.3, §7)
> - **Roadmap:** `docs/superpowers/plans/2026-07-28-cross-site-failover-roadmap.md` (SP3)
> - **Consumes:** SP4's `FailoverState.ServingTarget()` (home | backup), already built in `portal-service`.

## 1. Goal

When SP4 marks a site `failed_over`/`failing_back`, `GET /api/userInfo` for that
site's accounts must return the **backup's** `baseUrl`/`natsUrl` — so displaced
SSO clients connect to the backup's NATS and mint from the backup's `auth-service`
(SP2) — while a healthy site's accounts route home, unchanged. Portal is the
design's **single routing authority and split-brain fence** (§4.2); this slice is
where that authority becomes an actual reroute.

The only observable outcome: for an account whose home site has
`servingTarget=backup`, `GET /api/userInfo` returns the backup's coordinates with
`siteId` **still the home site**; when the site is healthy, it returns the home
site's, exactly as today.

## 2. Decision — a thin override at the resolve seam, keyed on `servingTarget`

Portal already resolves coordinates in one place: `handler.go:resolve()` looks up
the account's home `siteID`, then returns `sites[siteID]`'s `BaseURL`/`NATSURL`
(`PORTAL_SITE_URLS` registry). The override is a **single interception at that
lookup**:

> resolve the account's home `siteID` → read SP4's `ServingTarget(siteID)` →
> if `backup`, substitute the **backup** registry entry's URLs; else use the home
> entry. Everything else about the response is unchanged.

- **Reject — a separate "failover router" service or a new client protocol.** The
  frontend already re-queries portal on connection failure and connects to
  whatever URL it is handed (design §4.2, §7); a new service or protocol would
  duplicate the directory and the reconnect path for no gain. The reroute *is*
  the existing path with a different answer.
- **Reject — rewriting the `siteID` to the backup's.** The backup materializes
  **per-origin-`siteID` namespaces** and the message send subject carries
  `{siteID}` (`chat.user.{account}.room.{roomID}.{siteID}.msg.send`; streams
  `MESSAGES_{siteID}`). The client must keep reporting its **home** `siteID` so
  its subjects land in the backup's copy *of that site*. Only the transport URLs
  move; `siteID` does not (§3).

### 2.1 Centralize the override in one helper

The lookup moves into a single helper so the swap logic lives in one place and is
unit-testable in isolation:

```go
// servingURLs returns the coordinates to hand the client for an account homed
// on siteID, applying SP4's failover override. siteID (the home site) is
// returned unchanged for data scoping; only the transport URLs may swap.
func (h *PortalHandler) servingURLs(ctx context.Context, siteID string) (siteURL, error) {
    if h.failover.ServingTarget(ctx, siteID) == ServingBackup {
        b, ok := h.sites[h.backupSiteID]
        if !ok {
            // A failover with no configured backup is a real misconfig — surface
            // it loudly rather than silently routing to the down home site.
            return siteURL{}, fmt.Errorf("serving target is backup but backup site %q missing from registry", h.backupSiteID)
        }
        return b, nil
    }
    home, ok := h.sites[siteID]
    if !ok {
        return siteURL{}, fmt.Errorf("no URLs configured for siteId %q", siteID)
    }
    return home, nil
}
```

`resolve()` calls `servingURLs(ctx, e.SiteID)` and keeps returning `SiteID:
e.SiteID` (home) in the body. Dev-mode fallback is preserved (the dev-fallback
site resolves before the failover check and is never failed over).

### 2.2 Scope — the SSO `/api/userInfo` path only; bots deferred

The override applies to `/api/userInfo` (the SSO discovery path — the lifeboat
population). `POST /api/v1/login` (bot/admin password login) is **left
unchanged**, because:

- Bot/admin functionality is **out of lifeboat scope** (design §2, §10).
- That path *forwards the login to botplatform* at the home site, which is **down**
  during the outage — so the login fails at the forward step regardless of which
  URLs portal would return. Overriding its response URLs is moot until a backup
  botplatform exists.

**Bots are business-critical, and this is a deliberate deferral, not a drop.**
Supporting bot failover is a follow-up whose weight lives in SP1 (materialize
bot/botplatform state) and SP2 (run a botplatform + wire the backup
`auth-service` session branch) — **not** here. When that lands, the SP3 side is a
single `servingURLs(...)` call added to `HandleLogin`. Recorded so the priority
is not lost.

## 3. What swaps, and what must not

| Response field | Healthy | Failed over | Why |
|---|---|---|---|
| `baseUrl` | home | **backup** | JWT minting (SP2) + HTTP APIs served by the backup |
| `natsUrl` | home | **backup** | client connects to the backup's NATS (intra-cluster) |
| `siteId` | home | **home (unchanged)** | data is namespaced by origin `siteID` on the backup; `msg.send` subjects carry it |
| `account`, `employeeId` | unchanged | unchanged | identity is site-independent |

Keeping `siteId` = home is the load-bearing correctness point: it makes the
displaced client publish/subscribe under the same site namespace the backup
materialized (SP1) and mints grants for (SP2), so send/receive "just works"
against the backup copy.

## 4. Backup coordinates in the registry

The backup is **one more entry** in `PORTAL_SITE_URLS`, under a reserved id from
`PORTAL_BACKUP_SITE_ID` (e.g. `_backup`). No schema change — the existing
`siteURL{baseUrl,natsUrl}` shape already carries what the client needs, and
startup validation (`parseSiteURLs`) already requires both URLs, so a
misconfigured backup fails at boot. A single backup id suffices for the design's
N→1 topology; a multi-backup future is a registry extension, not a redesign.
`PORTAL_BACKUP_SITE_ID` may be empty in single-site/dev deployments where no
failover occurs (`servingTarget` is always `home`, so the backup lookup is never
reached).

## 5. Client reconnect — reuse the existing self-heal, both directions

No new client protocol; the reroute rides the path the frontend already has
(design §7 "reconnect-self-heal", §4.2):

- **Failover (home→backup):** the home site is down, so the displaced client's
  NATS connection has **already dropped**. Its existing reconnect logic re-queries
  portal, now gets the backup's URLs, mints via the backup's `auth-service` (SP2),
  connects, and re-reads `subscription.list` (materialized by SP1). Automatic.
- **Failback (backup→home):** after SP4 flips `failing_back → healthy`, the backup
  **tears down its impersonation** and drains those connections (design §6.3 step
  6). The drop is itself the nudge: the client reconnects, re-queries portal, now
  gets home's URLs, and resumes on the home site. **SP3 has no code here** — it
  just answers home once the flip has happened; the drain is owned by SP2/SP5.

## 6. The failover reader (built here)

`failoverReader` wraps SP4's `FailoverStore` (same `package main`) behind a short
TTL cache and exposes `ServingTarget(ctx, siteID) ServingTarget`:

- On a cache hit within the TTL, return the cached target. On miss/expiry, call
  `store.Get`, derive `state.ServingTarget()`, cache it with expiry `now + TTL`.
- **Fail-safe = `home`.** A store error returns `ServingHome` and is **not**
  cached (so a transient blip doesn't pin a stale answer) — an unreachable
  failover store must never strand a healthy site's users on the backup.
- TTL is `FAILOVER_STATE_TTL` (default `5s`): long enough to shield Mongo from
  per-login reads, short enough that a flip reaches routing within seconds. It is
  **separate** from portal's 24h directory cache and must not be merged into it.
- The clock is injectable for deterministic TTL tests.

### 6.1 main.go wiring refactor

SP4 constructs the `mongoFailoverStore` **only** inside the
`FAILOVER_OPS_TOKEN != ""` block (for the control surface). The reader needs the
store **unconditionally** — routing must consult failover state even where the
control surface is disabled. So the store construction moves out of that block:
build `mongoFailoverStore` once, hand it to the always-on `failoverReader`, and
let the optional control server reuse the same instance. `PortalHandler` gains a
`failover *failoverReader` and a `backupSiteID string`.

## 7. Split-brain fence — trust SP4's one record, never second-guess

- SP3 **never** derives failover from its own probing or a login failure — it
  reads `ServingTarget` and nothing else. A site is `home` xor `backup` because
  the value is single-valued; SP3 cannot manufacture a third answer.
- **Fail-safe = home** on any read miss/error (§6).
- Reads go through the short TTL cache, never the 24h directory cache.

## 8. Testing (TDD, per CLAUDE.md §4)

- **Unit — `servingURLs`** (mock reader): healthy → home URLs; `backup` → backup
  URLs; `servingTarget=backup` but backup id missing from registry → internal
  error; home site missing from registry → internal error.
- **Unit — `/api/userInfo`** (via `HandleUserInfo` with a mock reader): healthy →
  home coords; `failed_over` → backup coords with **`siteId` still home** (the
  "must not swap `siteId`" invariant, locked by a test); reader returns home on
  error → home coords (fail-safe); dev fallback unchanged.
- **Unit — `failoverReader`** (injected clock + mock store): cache hit within TTL
  returns cached value without a second store call; refresh after expiry; store
  error → `home` and not cached.
- **Integration** (`//go:build integration`, `testutil.MongoDB`): seed a
  `FailoverState` of `failed_over` via the store, hit `/api/userInfo`, assert
  backup coords + home `siteId`; flip to `healthy`, assert home coords after the
  TTL; a healthy account and a failed-over account resolve independently.
- **Coverage** — ≥80% floor; ≥90% on `servingURLs` + the reader.

## 9. Documentation

`docs/client-api.md` §2 (site discovery): note that `baseUrl`/`natsUrl` may point
at the backup during a home-site outage while `siteId` remains the home site, and
that the client's existing reconnect-on-failure path is the failover/failback
trigger. Field tables gain no new fields; only the narrative note is added, per
the doc's current style. (`/api/userInfo` is a discovery endpoint, not a
`chat.user.*` RPC, so this is a courtesy note, not a required client-API RPC
change.)

## 10. Out of scope (each its own slice)

- **SP4** — producing/owning `FailoverState`; the detector and control surface.
  SP3 only reads `ServingTarget`.
- **SP2** — the backup's JWT minting and serving handlers the rerouted client
  then talks to.
- **SP5** — failback replay/convergence and the backup-impersonation drain that
  triggers the failback reconnect.
- **Bot failover** — the `/api/v1/login` override + botplatform-DR (§2.2). Critical,
  deferred; the SP3 hook is one line once SP1/SP2 support it.
- **SP6 — ops/IaC:** the backup deployment behind the registry URLs, and the
  backup as a supercluster gateway peer.
