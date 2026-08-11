# SP2 (core) — Backup identity: cross-site NATS-JWT minting & key custody

> **Scope discipline.** This is the *core* first slice of SP2, deliberately
> narrowed to the **identity question that gates everything else**: how the
> backup site mints valid NATS user JWTs for a down site's accounts — including
> the `chat.local.room.>` grant — **without becoming a key-custody
> catastrophe**. It resolves design open-question §11.4. The rest of SP2 — the
> actual send/receive and history-read *serving* handlers pointed at the
> materialized copy — is a **separate slice** (it needs SP1 live first) and is
> out of scope here, exactly as SP1a/SP1b narrowed to their first slice.
>
> - **Governing design:** `docs/superpowers/specs/2026-07-28-cross-site-failover-design.md` (§4.3, §7, §11.4)
> - **Roadmap:** `docs/superpowers/plans/2026-07-28-cross-site-failover-roadmap.md` (SP2)
> - **Depends on:** SP0 (local/global room subjects — the `chat.local.room.>` prefix) and SP1 (materialized identity slice to authorize against).

## 1. Goal

When `portal-service` reroutes a down site's accounts to the backup (§4.2), the
backup's `auth-service` must mint each displaced user a NATS user JWT that:

1. **Validates on the backup's own NATS** — same decentralized-JWT trust the
   home site relies on (the NATS server checks the signature against a trusted
   account signing key; there is **no** `auth_callout` on the connect path —
   confirmed in `2026-06-05-seamless-nats-jwt-refresh-design.md`).
2. **Carries the correct scoped grants** for lifeboat traffic, crucially
   including **subscribe on `chat.local.room.>`** so the majority (post-SP0
   local) rooms are reachable. Missing this silently breaks the lifeboat's core
   promise (design §7, integration requirement #1 — "highest-risk coupling").

…and to do this **without placing a long-lived account signing seed on the
backup host**. The backup is the single deployment that holds every site's
materialized data *and* can mint every site's users — a uniquely attractive
target. Concentrating raw signing seeds there is the design's largest security
trade-off (§4.3); this slice's whole job is to defuse it.

Nothing in this slice *serves* chat traffic. The only observable outcome: given
a valid upstream (OIDC / botplatform) credential for a rerouted account, the
backup returns a NATS JWT that connects successfully to the backup's NATS and
can subscribe both `chat.room.>` and `chat.local.room.>`.

## 2. Identity model today (what we are extending)

`auth-service` (`handler.go:signNATSJWT`) mints **scoped** user JWTs:

- The chat `account` is **not** a NATS account — it is a **tag** (`account:<name>`)
  on the user JWT. Pub/sub permissions are **not** in the code; they come from a
  **scoped signing key template** attached to the NATS account (`docker-local/setup.sh`),
  which resolves `{{tag(account)}}` to the caller's own `chat.user.{account}.>`
  subtree.
- Signing is a single call — `uc.Encode(h.signingKey)` — where `h.signingKey`
  is an **Ed25519** `nkeys.KeyPair` (the account *scoped signing key* seed, env
  `AUTH_SCOPED_SIGNING_KEY`). `IssuerAccount` is stamped so the resolver
  attributes the signing key back to its account.
- Both the architecture doc ("signed with the account NKey", singular) and local
  dev provision **one** org-level `chatapp` account with **one** scoped signing
  key. We take that as the production model (see §9 open sub-decision 1 if it is
  not).

**The load-bearing seam:** `jwt.UserClaims.Encode(pair nkeys.KeyPair)` needs
only two things from `pair` — `Sign(msg) []byte` and `PublicKey() string`.
Everything else about "who mints" is orthogonal to the claims. That is the hook
this whole slice hangs on.

## 3. Decision — one org-level signing identity, custody behind a remote signer; the backup holds no seed

Resolve §11.4 in two moves:

**(a) Consolidate on a single org-level signing identity** (already the model),
rather than N per-site account seeds. Because `account` is a tag and permissions
come from one shared scoped template, the backup mints a down site's users with
the *same* signing identity every site already uses — there is no per-site key to
copy in the first place. This turns "the backup needs N sites' NKeys" into "the
backup needs signing access to the one org key," collapsing the custody surface
from N seeds to one and making the `chat.local.room.>` grant a **single template
edit** that serves every site uniformly (§4).

**(b) Never materialize that one seed on the backup (or any auth-service host).**
Front the Ed25519 signature with a **remote signer** that holds the seed and
exposes only a narrow "sign these user-JWT bytes" operation, authenticated and
audited per call. `auth-service` becomes a signing *client*: it builds the exact
same claims and calls `Encode` with a `nkeys.KeyPair` implementation whose
`Sign()` delegates to the signer and whose `PublicKey()` returns the known
signing-key public key. Zero change to claim construction; the seed never enters
the backup's address space.

```go
// remoteSignerKP implements nkeys.KeyPair by delegating Sign to the signer.
// PublicKey returns the account signing key's public key (config, not secret).
// Seed/PrivateKey return an error — the seed is unreachable by construction.
// nkeys.KeyPair.Sign has no context param, so the adapter is constructed
// per-mint carrying the request ctx (deadline/trace) it forwards to the signer.
type remoteSignerKP struct {
    ctx    context.Context
    pub    string
    signer Signer // Sign(ctx context.Context, msg []byte) ([]byte, error)
}
func (k *remoteSignerKP) PublicKey() (string, error)      { return k.pub, nil }
func (k *remoteSignerKP) Sign(msg []byte) ([]byte, error) { return k.signer.Sign(k.ctx, msg) }
func (k *remoteSignerKP) Seed() ([]byte, error)           { return nil, errSeedUnavailable }
func (k *remoteSignerKP) PrivateKey() ([]byte, error)     { return nil, errSeedUnavailable }
// Verify/Wipe: trivial. Drops straight into jwt.UserClaims.Encode(kp).
```

### 3.1 What "remote signer" concretely means (Ed25519 is the constraint)

NKeys are **Ed25519**, and that rules out most managed KMS: AWS KMS has no
Ed25519 signing primitive (only ECC_NIST / RSA), so "just use KMS" is not a drop-
in. The two realizable forms:

- **HashiCorp Vault Transit** — natively supports an `ed25519` key type with a
  `sign`/`verify` API; the key never leaves Vault, access is policy-gated and
  audit-logged, and rotation/revocation are first-class. **Recommended concrete
  backing.**
- **A dedicated in-house mint-service** — a minimal, hardened `package main`
  service that is the *only* process holding the seed (in memory from a secret
  store, on a tightly-scoped node), exposing one RPC: sign user-JWT bytes for a
  validated request. Use this only if Vault Transit is unavailable; it
  reintroduces a seed-holding host, so it must be the smallest, most locked-down
  surface in the fleet.

Either way the signer sits **behind the same trust boundary as the account
scoped signing key it wields**, and the backup's `auth-service` authenticates to
it with revocable, rate-limitable, per-deployment credentials.

### 3.2 Approaches considered

- **A — one org signing identity + remote signer (CHOSEN).** Smallest custody
  surface (one key), the `chat.local.room.>` grant is one shared template edit,
  and a backup compromise yields only *revocable, audited signing requests*, not
  a stolen key. Cost: a signer round-trip on the JWT-mint path (mitigated §6) and
  operating the signer as an HA dependency.
- **B — per-site account signing keys, each behind the signer.** Retains NATS-
  account isolation per site (revoke one site without touching others; the
  backup's NATS must `resolver`-trust N account JWTs). Strongest blast-radius
  containment, but N keys to provision/rotate and a per-site template to keep the
  `chat.local.room.>` grant in sync across. This is the **fallback if production
  actually runs per-site NATS accounts** (§9.1) — the signer seam is identical,
  only keyed per site. Not chosen for the single-account model because it adds
  fan-out with no isolation win when there is only one account.
- **C — copy raw seed(s) onto the backup (REJECTED).** The naive baseline the
  design rejects (§4.3). Simplest to build; worst posture — a backup compromise
  exfiltrates a permanent, org-wide (or every-site) user-minting key that cannot
  be revoked without re-keying the whole trust chain. Documented and rejected.

## 4. The `chat.local.room.>` grant (SP0 coupling — the highest-risk edge)

Post-SP0, same-site rooms move to a `chat.local.room.{id}.>` prefix denied at the
leaf (never advertised cross-gateway). Displaced users connect **intra-cluster**
on the backup, so the leaf deny is irrelevant to their delivery (design §7) — but
their JWT must still carry **subscribe `chat.local.room.>`** or they cannot see
local rooms at all.

Under the §3 single-identity decision this is **one edit to the shared scoped
template** (`setup.sh` / the production equivalent): add `--allow-sub
"chat.local.room.>"` alongside the existing `chat.room.>`. Because the template
is shared, every user — on their home site *and* displaced to the backup — gets
the grant identically. This **eliminates** the design's "backup silently missing
the grant" failure mode (§7 req #1) rather than merely mitigating it: there is no
separate backup template to drift.

The grant change is **coupled to SP0 landing** (the prefix must exist and be
leaf-denied first). Until SP0 lands, local rooms are still on `chat.room.>` and
the existing grant already covers them; the template edit ships **with or
immediately after** SP0, and the `signNATSJWT` docstring's "effective grants"
comment (kept in sync with `setup.sh` and `docs/client-api.md §2.1`) is updated
in the same change.

## 5. Serving-path trust (how the backup's NATS accepts these JWTs)

For a minted JWT to connect, the backup's NATS must trust the signing key that
signed it:

- **Single-identity (§3a):** the backup's NATS `resolver` preloads the **same**
  org account JWT (with the same scoped signing key) as every site. The backup is
  a supercluster gateway peer (design §7, needed anyway for global-room delivery),
  so the account and its signing key are already trusted there. No new trust
  wiring — the backup validates a displaced user's JWT exactly as the home site
  would.
- **Per-site fallback (§3b):** the backup's NATS must additionally `resolver`-
  trust each site's account JWT. An ops/IaC concern (SP6), not app code.

This slice asserts the trust precondition and validates it in integration
(§8); it does **not** own the NATS server config (SP6).

## 6. Failover critical path — the mint storm vs. a remote signer

Failover triggers a reconnect thundering herd (design §8): a TLS + JWT-mint storm
as a whole site's users reconnect to the backup at once. Routing every mint
through a remote signer adds that signer to the critical path, so:

- **The signature is per-connect/refresh, not per-message.** JWTs are long-lived
  (`NATS_JWT_EXPIRY`, default 2h) and the frontend already refreshes proactively
  with **jitter** (`2026-06-05-seamless-nats-jwt-refresh-design.md`), so mint QPS
  is bounded by (displaced users ÷ refresh interval) plus the one-time herd — not
  message throughput.
- **Mitigations (mostly already in the design):** client reconnect backoff/jitter
  and rate-limited/pre-warmed auth (design §8); a persistent, pooled signer
  connection warmed at backup startup; the signer co-located in the backup's
  cluster (LAN round-trip, not WAN); and the signer itself **HA** — it is on the
  failover-serving critical path, so it inherits the backup's own "must be HA
  within itself" requirement (design §9). A signer outage during failover fails
  *new* mints (degraded, alerting) but does not drop already-connected sessions.
- **No pre-minting / no seed caching "for speed."** Caching the seed locally to
  dodge the round-trip would re-create exactly the custody surface §3b removes.
  The round-trip is the price of the guarantee; we pay it and size the signer for
  the herd instead.

## 7. Key rotation & revocation

- **Rotation** is a signer concern, not a redeploy: Vault Transit rotates the
  `ed25519` key in place; existing JWTs remain valid until `exp` (≤2h), new mints
  use the new version. The backup never holds the key, so no host needs
  re-provisioning.
- **Revocation on suspected backup compromise:** revoke the backup deployment's
  **signer credential** (Vault token/role) — instantly cuts the backup's ability
  to mint, with **no** re-keying of the org signing identity and no impact on
  healthy sites. This is the concrete payoff of §3b over §3c: the response to
  compromise is a credential revoke, not a fleet-wide re-key.

## 8. Testing (TDD, per CLAUDE.md §4)

- **Unit** — `remoteSignerKP` table tests: `Sign` delegates to a mocked signer
  and the resulting JWT verifies against the configured public key; `Seed`/
  `PrivateKey` return the sentinel error (seed genuinely unreachable);
  signer error → mint returns a wrapped infra error (collapses to `internal` at
  the boundary, no seed/claims leaked into the cause). Assert the built claims are
  **byte-identical** to the current in-process path (same tags, `IssuerAccount`,
  scoped flag, jittered `exp`) so only the signer backing changed.
- **Unit** — grant/template regression: a test that fails if the minted JWT's
  effective grants (asserted via the account template fixture) omit
  `chat.local.room.>`, locking the §4 coupling.
- **Integration** (`//go:build integration`, `testutil.NATS`) — stand up NATS
  trusting the test account signing key; run `auth-service` against a **local
  Vault Transit `ed25519`** (or an in-process fake signer implementing the
  `Signer` interface); mint a JWT and assert it **connects** and can **subscribe
  `chat.local.room.>`** and `chat.room.>` but is denied outside its
  `chat.user.{account}.>` — proving the end-to-end trust + grant path, not just
  the signature.
- **Coverage** — ≥80% floor; ≥90% on the signer-delegation + mint logic.

## 9. Open sub-decisions (call out in the plan)

1. **Production NATS-account topology — the pivot.** This slice assumes **one
   org-level account** (matches local dev + the architecture doc). If production
   actually runs **per-site accounts**, switch to approach §3b: the `Signer` seam
   and every test above are unchanged; only the signing identity is keyed per
   site and the backup's `resolver` trusts N account JWTs (SP6). **Confirm the
   topology with the platform/NATS team before planning.**
2. **Signer backing — Vault Transit vs. in-house mint-service.** *Leaning Vault
   Transit* (native Ed25519, audited, rotation/revocation first-class, no new
   seed-holding host). Fall back to a minimal in-house signer only if Vault is
   unavailable.
3. **Signer interface placement** — a new `pkg/natssign` (or similar; **not**
   `utils`/`common` per CLAUDE.md naming) defining the `Signer` interface + the
   `nkeys.KeyPair` adapter, consumed by `auth-service` and reusable by any future
   minting node. Keep the interface in the consumer per CLAUDE.md DI rules.
4. **Do site auth-services adopt the signer too, or only the backup?** Adopting
   it fleet-wide removes the raw seed from *every* auth-service (strictly better
   posture, one code path) at the cost of putting the signer on every site's
   login path. *Leaning: fleet-wide*, phased — backup first (where the risk is
   acute), sites second — but the phasing is a plan-level call.

## 10. Out of scope (explicit — each its own slice)

- **The serving handlers** — send/receive and history-read paths pointed at the
  backup's materialized Cassandra/Mongo. That is the rest of SP2 and needs SP1
  live; this slice is identity/minting only.
- **SP3 / SP4** — routing override and health detection that *decide* to reroute.
- **SP5** — failback replay.
- **SP6 — ops/IaC:** the backup's NATS `resolver`/gateway trust config, Vault
  Transit provisioning + policies, per-deployment signer credentials, and the
  leaf-node `chat.local.>` deny on the backup's leaf.
- **The SP0 template mechanics** themselves (prefix + leaf deny) — owned by the
  local/global room-subjects work; this slice only adds the one grant line and
  depends on that prefix existing.
