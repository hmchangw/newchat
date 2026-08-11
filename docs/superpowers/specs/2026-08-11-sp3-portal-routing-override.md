# SP3 (core) — Routing brain: portal-service health-aware override

> **Scope discipline.** This slice makes `portal-service` return the **backup
> site's** connection coordinates for a down site's accounts, keyed entirely on
> SP4's `servingTarget` signal. It is **codebase-local** — a thin override at
> portal's existing resolve seam — and deliberately owns *only* the routing
> decision. Detection/state (SP4), JWT minting (SP2), and failback replay (SP5)
> are out of scope.
>
> - **Governing design:** `docs/superpowers/specs/2026-07-28-cross-site-failover-design.md` (§4.2, §6.3, §7)
> - **Roadmap:** `docs/superpowers/plans/2026-07-28-cross-site-failover-roadmap.md` (SP3)
> - **Consumes:** SP4's `FailoverState.servingTarget` (home | backup). **Blocked on** SP4's signal shape (now decided).

## 1. Goal

When SP4 marks a site `failed_over`/`failing_back`, every login/discovery answer
portal gives for that site's accounts must point at the **backup** — so displaced
clients connect to the backup's NATS and mint from the backup's `auth-service`
(SP2) — while a healthy site's accounts route home, unchanged. Portal is the
design's **single routing authority and split-brain fence** (§4.2); this slice is
where that authority becomes an actual reroute.

The only observable outcome: for an account whose home site has
`servingTarget=backup`, `GET /api/userInfo` (and `POST /api/v1/login`) return the
backup's `baseUrl`/`natsUrl`; when the site is healthy, they return the home
site's, exactly as today.

## 2. Decision — a thin override at the resolve seam, keyed on `servingTarget`

Portal already resolves coordinates in one place: `handler.go:resolve()` looks up
the account's home `siteID`, then returns `sites[siteID]`'s `BaseURL`/`NATSURL`
(`PORTAL_SITE_URLS` registry). `HandleLogin` does the same for bot/admin logins.
The override is a **single interception at that lookup**:

> resolve the account's home `siteID` → read SP4's `FailoverState[siteID]` →
> if `servingTarget == backup`, substitute the **backup** registry entry's URLs;
> else use the home entry. Everything else about the response is unchanged.

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
  move; `siteID` does not (see §3).

### 2.1 Centralize the override so both entry points honor it

`resolve()` and `HandleLogin` both hand out site URLs, so the override lives in
**one helper** both call — else a bot/admin of a down site would password-login
straight to a dead site. Sketch:

```go
// servingURLs returns the coordinates to hand the client for an account homed
// on siteID, applying SP4's failover override. siteID (the home site) is
// returned unchanged for data scoping; only the transport URLs may swap.
func (h *PortalHandler) servingURLs(ctx context.Context, siteID string) (siteURL, error) {
    target := h.failover.ServingTarget(ctx, siteID) // SP4 read, TTL-cached; home on miss/error
    if target == servingBackup {
        b, ok := h.sites[h.backupSiteID]
        if !ok { return siteURL{}, fmt.Errorf("backup site %q missing from registry", h.backupSiteID) }
        return b, nil
    }
    home, ok := h.sites[siteID]
    if !ok { return siteURL{}, fmt.Errorf("no URLs configured for siteId %q", siteID) }
    return home, nil
}
```

Both handlers call `servingURLs(ctx, e.SiteID)` and keep returning `SiteID:
e.SiteID` (home) in the body. Dev-mode fallback and the "site missing from
registry" internal-error path are preserved.

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

The backup is **one more entry** in `PORTAL_SITE_URLS`, under a reserved id
(`PORTAL_BACKUP_SITE_ID`, e.g. `_backup`). No schema change — the existing
`siteURL{baseUrl,natsUrl}` shape already carries what the client needs, and
startup validation (`parseSiteURLs`) already requires both URLs, so a
misconfigured backup fails at boot, not at a user's failover. A single backup id
suffices for the design's N→1 topology (§3); a multi-backup future is a registry
extension, not a redesign.

## 5. Client reconnect — reuse the existing self-heal, both directions

No new client protocol; the reroute rides the path the frontend already has
(design §7 "reconnect-self-heal", §4.2):

- **Failover (home→backup):** the home site is down, so the displaced client's
  NATS connection has **already dropped**. Its existing reconnect logic re-queries
  portal, now gets the backup's URLs, mints via the backup's `auth-service` (SP2),
  connects, and re-reads `subscription.list` (materialized by SP1) on the correct
  prefixes. Automatic.
- **Failback (backup→home):** after SP4 flips `failing_back → healthy` on SP5's
  lag≈0 report, the backup **tears down its impersonation** and drains those
  connections (design §6.3 step 6, §6.6). The drop is itself the nudge: the client
  reconnects, re-queries portal, now gets home's URLs, and resumes on the home
  site — the same self-heal, in reverse. SP3 does not invent a separate
  "reconnect now" push; the drain-driven reconnect is sufficient and reuses tested
  behavior.

SP3's responsibility ends at *handing out the right URL when asked*. The drain
that forces the failback reconnect is the backup serving stack's (SP2/SP5); SP3
just answers home once the flip has happened.

## 6. Split-brain fence — trust SP4's one record, never second-guess

Portal is the fence because the serving target comes from **one** authoritative
`FailoverState` document (SP4, §6 there), and SP3 is a pure reader of it:

- SP3 **never** derives failover from its own probing or from a login failure — it
  reads `servingTarget` and nothing else. A site is `home` xor `backup` because
  the field is single-valued; SP3 cannot manufacture a third answer.
- **Fail-safe = home.** If the failover read misses or errors, `ServingTarget`
  returns `home` (SP4 §6) — normal routing. An unreachable failover store must
  never strand a healthy site's users on the backup.
- **No caching of the decision past its TTL.** SP3 reads through SP4's short
  `FAILOVER_STATE_TTL` cache (seconds), so a flip in either direction reaches new
  logins within seconds without a per-login Mongo hit. This is separate from the
  24h directory cache and must not be merged into it.

## 7. Testing (TDD, per CLAUDE.md §4)

- **Unit (`handler_test.go`)** — table-driven over `servingURLs` + both handlers
  with a mock failover reader: healthy → home URLs; `failed_over` → backup URLs
  with `siteId` **still home**; `failing_back` → backup URLs; failover read
  error/miss → home (fail-safe); backup id missing from registry → internal;
  bot/admin login path honors the override identically; dev fallback unchanged.
- **Unit** — assert the response body's `siteId` is the home site in every
  failed-over case (the "must not swap `siteId`" invariant, locked by a test).
- **Integration** (`//go:build integration`, `testutil.MongoDB`) — seed a
  `FailoverState` of `failed_over` for a site, hit `/api/userInfo`, assert backup
  coordinates + home `siteId`; flip to `healthy`, assert home coordinates after
  the TTL; concurrent healthy/failed accounts resolve independently.
- **Coverage** — ≥80% floor; ≥90% on `servingURLs` + the two handlers' override
  branches.

## 8. Documentation

`docs/client-api.md` §2 (site discovery): note that `baseUrl`/`natsUrl` may point
at the backup during a home-site outage while `siteId` remains the home site, and
that the client's existing reconnect-on-failure path is the failover/failback
trigger. Field tables gain no new fields (the shapes are unchanged); only the
narrative and the reason/behavior notes are added, per the doc's current style.

## 9. Open sub-decisions (call out in the plan)

1. **Backup id convention** — reserved entry in `PORTAL_SITE_URLS`
   (`PORTAL_BACKUP_SITE_ID`) vs. a dedicated `PORTAL_BACKUP_SITE_URLS`. *Leaning:
   reserved entry* (no new config surface, reuses `parseSiteURLs` validation).
2. **Failover reader placement** — a `FailoverReader` interface defined in
   portal (the consumer, per CLAUDE.md DI) with the Mongo + TTL-cache
   implementation, shared in shape with SP4's writer but a distinct read
   interface. Confirm SP4 and SP3 agree on the `FailoverState` document schema
   (§3 in SP4) as the contract between them.
3. **Proactive failback nudge** — rely solely on the backup drain (§5) vs. also
   publishing a reconnect hint on the global `chat.user.{account}.>` tree (§7 of
   the design says the per-user tree stays global, so a nudge is possible).
   *Leaning: drain-only* for this core; add a nudge only if failback reconnect
   latency proves too slow in practice.

## 10. Out of scope (explicit — each its own slice)

- **SP4** — producing/owning `FailoverState`; the detector and control surface.
  SP3 only reads `servingTarget`.
- **SP2** — the backup's JWT minting and serving handlers the rerouted client
  then talks to.
- **SP5** — failback replay/convergence and the backup-impersonation drain that
  triggers the failback reconnect.
- **SP6 — ops/IaC:** the backup deployment behind the registry URLs, and the
  backup as a supercluster gateway peer.
