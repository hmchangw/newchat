# SP2 (core) — Backup JWT minting in the shared NATS account

> **Revised 2026-08-12 — rewritten for World 1.** An earlier draft of this spec
> designed a key-custody scheme (a remote Ed25519 signer / Vault, so the backup
> would never hold per-site signing seeds). That design assumed **per-site NATS
> accounts**. Production actually runs **one shared org-level NATS account** for
> all clients (confirmed). In that world the backup mints in the *same* account
> every site uses — it is **not** impersonating anyone and needs no per-site
> keys — so the custody problem the old draft solved **does not exist**. This
> revision replaces that draft entirely and shrinks SP2 to its real, small
> footprint.
>
> - **Governing design:** `docs/superpowers/specs/2026-07-28-cross-site-failover-design.md` (§4.3, §7)
> - **Roadmap:** `docs/superpowers/plans/2026-07-28-cross-site-failover-roadmap.md` (SP2)
> - **Depends on:** SP0 (the `chat.local.room.>` prefix) and SP1 (materialized data to *serve* — not needed to *mint*).

## 1. Goal

When `portal-service` reroutes a down site's accounts to the backup (§4.2), a
displaced user must be able to obtain a NATS credential from the backup and
connect. This slice establishes that the backup can do so — and documents that,
in World 1, it requires **essentially no new code**.

The only observable outcome: a displaced user presenting a valid OIDC token to
the backup's `auth-service` receives a NATS JWT that connects to the backup's
NATS and can subscribe both `chat.room.>` and (post-SP0) `chat.local.room.>`.

## 2. Why this is nearly a no-op — World 1

NATS here uses the decentralized-JWT model, and the identity topology is the
load-bearing fact:

- There is **one shared org-level NATS account** for all clients, trusted by
  every site's NATS (and the backup's). The chat `account` (`alice`) is **not** a
  NATS account — it is a **tag** on a scoped user JWT. Permissions come from a
  single server-side **scoped-signing-key template**; `{{tag(account)}}` resolves
  to the caller's own `chat.user.{account}.>` subtree.
- Signing is one call — `uc.Encode(signingKey)` in `auth-service/handler.go:
  signNATSJWT` — with the shared account signing key (`AUTH_SCOPED_SIGNING_KEY`).
  That same key is trusted at every site.

Because the account is shared, the backup minting `alice`'s token **is** minting
in its own account — the one `alice` already belongs to. No impersonation, no
per-site key, no signer indirection. The backup runs the **same `auth-service`,
unchanged**, and the JWT it mints is indistinguishable from one `alice`'s home
site would mint.

## 3. The mint path (reused verbatim)

`auth-service` today, on the OIDC branch (`handleSSO`): validate the SSO token
against the central Keycloak (reachable from anywhere) → derive `account` from the
token → `signNATSJWT` with the shared key → return the JWT. **There is no
provisioning/eligibility gate** (the one the portal-service design once proposed
was never shipped — `auth-service` has no `SITE_ID`, no Mongo). Minting is gated
by OIDC alone.

The backup reuses this path with **zero changes**. "Does this user have rooms to
serve?" is answered by SP1's materialized data at *serving* time, never at mint
time — so the mint path needs nothing from SP1.

## 4. The two actual deliverables

Neither is new minting logic; both are config/ops.

### 4.1 The `chat.local.room.>` grant (shared template, SP0-coupled)
Post-SP0, same-site rooms move to `chat.local.room.{id}.>`. A user's JWT must then
**subscribe `chat.local.room.>`** or they lose the majority of their rooms
(design §7, req #1). Under the shared account this is **one edit to the shared
scoped-signing template** (`docker-local/setup.sh` and its prod equivalent): add
`--allow-sub "chat.local.room.>"` next to the existing `chat.room.>`. Because the
template is shared, *every* user — home and displaced — gets it identically,
which **eliminates** the design's "backup silently missing the grant" failure mode
rather than mitigating it (there is no separate backup template to drift).

Coupled to SP0 landing (the prefix must exist first). Granting a subscribe on a
prefix nobody publishes to yet is a harmless no-op, so it *may* ship early, but
there is nothing to exercise until SP0 is live. Ship it **with or immediately
after SP0**, and in the same change update the `signNATSJWT` "effective grants"
docstring and `docs/client-api.md §2.1` (both must stay in sync with the
template).

### 4.2 Backup `auth-service` deployment (ops / SP6 handoff)
Deploy `auth-service` at the backup with the **shared** `AUTH_SCOPED_SIGNING_KEY`
+ `AUTH_ACCOUNT_PUB_KEY`, the central OIDC config, reachable at the backup
`baseUrl` that portal (SP3) hands displaced clients. No app change. This is an
ops/IaC deliverable, tracked under SP6; recorded here as SP2's operational
precondition.

## 5. What was dropped, and why

Removed from SP2 entirely (all were World-2 concerns):
- The remote-signer / Vault Transit Ed25519 signing scheme, the `Signer`
  interface, and the `nkeys.KeyPair` delegating adapter (`pkg/natssign`).
- Per-site signing keys and the "backup concentrates every site's keys"
  blast-radius argument.

In World 1 the single shared signing key **already lives on every site's
`auth-service`**; the backup holds one more copy, not a new class of risk. If the
org ever wants to reduce that exposure, the right project is fronting the **one
shared key with a remote signer fleet-wide** — a security-hygiene effort
**independent of failover**, which must not block or hide inside this program.

## 6. Eligibility gate — considered, deferred

We considered making the backup mint only for accounts whose home site is
currently failed-over (reading SP4's state), as defense-in-depth. **Deferred**,
because: no site has any mint-time eligibility gate today; the minted token is
**site-agnostic** in World 1 (identical regardless of who mints it or which site
the account is "from"), so an over-broad mint grants no extra privilege; and
portal (SP3) already routes only failed-over sites' users to the backup. Whether
`auth-service` should gain an eligibility gate at all is a **system-wide policy
question, not a failover one** — out of scope here.

## 7. Serving-path trust (trivial in World 1)

The backup's NATS `resolver` preloads the **same** shared account JWT + scoped
signing key as every site, so it validates a displaced user's JWT exactly as the
home site would — no new trust wiring. The backup is a supercluster gateway peer
(needed anyway for global-room delivery, design §7); that peering is ops/SP6.

## 8. Failover mint storm (operational note)

A whole site reconnecting at once is an OIDC-validate + sign burst on the backup's
`auth-service` — now a **local** cost (no external signer on the path). Mitigated
by the existing client reconnect backoff/jitter and proactive-refresh jitter
(`2026-06-05-seamless-nats-jwt-refresh-design.md`); size the backup's
`auth-service` (and Keycloak reachability) for one largest site's reconnect peak.

## 9. Bots/admins out of scope

Admin/bot functionality is out of lifeboat scope (design §2, §10), so
`auth-service`'s botplatform session-token branch need not work at the backup —
leave `BOTPLATFORM_URL` unset there. Only the OIDC mint path serves displaced
users.

## 10. Testing (TDD, per CLAUDE.md §4)

Honestly minimal — there is **no new Go logic in the mint path**, so most
assurance is config-level:

- **Grant regression (unit):** once the shared template carries
  `chat.local.room.>`, a test that fails if a minted JWT's effective grants
  (asserted against the account-template fixture) omit it — locking §4.1.
- **Integration** (`//go:build integration`, `testutil.NATS`): stand up NATS
  trusting the shared account signing key; run `auth-service`; mint via the OIDC
  path (fake `TokenValidator`); assert the JWT **connects** and can **subscribe
  `chat.room.>` and `chat.local.room.>`** but is denied outside its
  `chat.user.{account}.>` — proving the end-to-end grant path at the backup.
- No new `remoteSignerKP` / signer tests — that code no longer exists.

## 11. Out of scope (each its own slice)

- **The serving handlers** — send/receive + history-read against the backup's
  materialized data (the rest of SP2, needs SP1).
- **SP3 / SP4 / SP5** — routing override, health detection, failback.
- **SP6 — ops/IaC:** the backup's NATS `resolver`/gateway trust config and the
  `auth-service` deployment (§4.2).
- **SP0 template mechanics** (the prefix + leaf deny) — this slice only adds the
  one grant line and depends on that prefix existing.
- **Fleet-wide signing-key hardening** — a separate, optional security project
  (§5), not failover.

## 12. Open sub-decisions (call out in the plan)

1. **Grant timing** — ship `chat.local.room.>` on the shared template *now* (a
   harmless no-op until SP0) vs. bundled into the SP0 PR. *Leaning: with SP0*, so
   it is exercised the moment the prefix exists.
2. **Eligibility gate** — leave `auth-service` OIDC-only (status quo, this spec's
   choice) vs. introduce a system-wide mint-time gate later. Not a failover
   decision; noted only so the option is on record.
