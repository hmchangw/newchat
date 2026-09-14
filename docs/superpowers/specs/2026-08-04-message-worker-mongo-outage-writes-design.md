# At-Rest DEK Mongo-Outage Survival (+ message-worker Write Path)

## Summary

Make the **at-rest encryption key (DEK) available without MongoDB**, so both
halves of the message lifecycle survive a Mongo outage for active rooms:

- **Reading encrypted history** (`history-service`) — every Cassandra row is
  decrypted through the Mongo-backed DEK store, so once a room's DEK leaves the
  in-process cache, history reads fail even though PR #188's authz check passes.
- **Writing a plain message** (`message-worker`) — `SaveMessage` must *encrypt*
  through the same DEK, and additionally has two Mongo *enrichment* reads that
  gate the persist.

The core mechanism is one shared change in `pkg/atrest`: a Valkey **L2 for
Vault-wrapped DEKs**, fronted by the shared circuit breaker. Because both
services use the same `atrest.Cipher`, that single fix serves the read path and
the write path. On top of it, `message-worker`'s two enrichment reads fail-open
to the self-describing canonical event. Cold rooms keep today's behavior
(buffer-and-retry on write; error on read).

This is a **separate feature** from PR #188, shipping as its own PR, but it
**depends on #188** — it reuses the shared `pkg/circuitbreaker` introduced there
— so it is developed *stacked on* the #188 branch and merges after it. See
**Branch strategy** below.

## Motivation

PR #188 made the **send accept** (`message-gatekeeper`) and the **history-read
authorization decision** (`history-service`) survive a Mongo outage — the latter
now indefinitely, thanks to the breaker-gated sliding L2 TTL. Verifying the
end-to-end "survive 1 hour with Mongo down" claim after that work exposed the
**next binding constraint, which #188 does not address: the at-rest DEK.**

### The DEK is the new ceiling (read path)

`history-service` decrypts every Cassandra row via `cassrepo.decrypt` →
`atrest.Cipher.Decrypt(ctx, roomID, …)` → `dekFor(roomID)`. That resolves from an
**in-process cache only — there is no L2** — and misses fall through to the
Mongo DEK store (`atrest.NewMongoDEKStore`, pinned to `readpref.Primary()`).

The cache (`pkg/atrest/cache.go`) is a 2Q LRU whose entries carry a fixed
`expiresAt` stamped at insertion; **`get` does not refresh it**, and only a
successful *Mongo* fetch re-stamps it. Since fetches stop the moment Mongo dies:

| Situation at outage start | Remaining DEK life |
|---|---|
| DEK just fetched | up to `ATREST_DEK_CACHE_TTL` (default **1h**) |
| DEK fetched mid-window | uniformly distributed 0–1h (**~30 min average**) |
| Pod restarts / scales mid-outage | **zero** (cold cache, cannot refetch) |
| Room evicted from the 2Q LRU (default 10 000 rooms) | zero |

So for a 60-minute outage, encrypted history reads are **not reliably
survivable**: the average room loses its key roughly half-way through, and any
pod churn drops it immediately. At-rest encryption is on by default
(`ATREST_ENABLED` envDefault `true`, and set `true` in both services' compose),
so this is the production path.

Raising `ATREST_DEK_CACHE_TTL` only shifts the window and still dies on a pod
restart — it is not a fix.

### The same DEK blocks the write path

`message-worker` — the sole persister of message history to Cassandra — must
*encrypt* through the same cipher, so it inherits the identical constraint.
Tracing `message-worker/handler.go:processMessage`, three Mongo touchpoints run
before `store.SaveMessage`:

| # | Touchpoint | Nature | On Mongo outage today |
|---|---|---|---|
| 1 | `userStore.FindUserByAccount(sender)` (`users`) — every message | Enrichment | Cold-cached sender → error → NAK |
| 2 | `mention.Resolve` → `userStore.FindUsersByAccounts` (`users`) — only when the message has @mentions | Enrichment | Cold mentions → error → NAK |
| 3 | `atrest.Cipher` DEK fetch (`atrest.NewMongoDEKStore`) inside `SaveMessage`'s encrypt | Cryptographic | Cold/expired room DEK → error → NAK |

`#1` and `#2` are **degradable**: the canonical event already carries the
sender's identity (`UserID`, `UserAccount`, and the gatekeeper-composed
`UserDisplayName`), and message content (including the literal `@tokens`) is
intact regardless of resolution. `#3` is **not degradable**: the body must be
encrypted at rest (persisting plaintext would break the guarantee) and a DEK
cannot be fabricated — so a cold room's DEK is a hard blocker.

The at-rest cipher (`pkg/atrest/cipher.go`) uses a **per-room** DEK, Vault-
wrapped, held in an in-process **LRU with a TTL**. So a *warm* room encrypts
from the cached (already-unwrapped) DEK with no Mongo — and no Vault — while a
*cold or TTL-expired* room misses to Mongo and fails during the outage. Because
DEKs are keyed by room and the rooms being written to during an outage are
exactly the active ones, keeping the DEK available without Mongo covers the
write traffic that matters — the same **active-rooms-is-enough** posture used by
the read/send work.

(The DEK dependency applies only when `ATREST_ENABLED=true` — production. With
at-rest encryption off, `SaveMessage` stores plaintext and `#3` does not exist.)

## Design principle

Keep the **per-room DEK reachable without Mongo** for active rooms, so both
encrypt (write) and decrypt (read) survive an outage; then remove the remaining
non-cryptographic blockers on the write path. Concretely:

1. **A Valkey L2 for Vault-wrapped DEKs** in `pkg/atrest`, fronted by the shared
   circuit breaker (`#3`). One change, two beneficiaries: `history-service`
   (decrypt → history reads keep working) and `message-worker` (encrypt → the
   persist completes).
2. **Fail-opening the enrichment reads** (`#1`, `#2`) to the self-describing
   canonical event, on the write path.

The **cold path is unchanged**: a genuinely unavailable DEK (or any still-
transient failure) NAK-buffers in `MESSAGES-CANONICAL` and persists once Mongo
returns on the write side — no dropping, no dead-letter — and surfaces as an
error on the read side, exactly as today.

Accepted posture (carried from #188): fail-open, active-rooms-is-enough. Truly
cold rooms (no cached/L2 DEK) can neither persist nor decrypt during the outage
— an unavoidable gap, since there is no safe way to write or read an encrypted
row without the key.

### Target after this change

| Journey | Today (post-#188) | After this design |
|---|---|---|
| Subscription authz (send + read) | Survives indefinitely (#188 sliding L2 TTL) | unchanged |
| Read encrypted history, active room | ~30 min average; 0 on pod restart | Survives the outage (L2-served DEK) |
| Persist a plain message to Cassandra, active room | NAK-buffers until Mongo returns | Completes during the outage |
| Cold room (either journey) | Fails | Fails (accepted) |

## Delivery (shipped in PR #188)

This feature was developed on `claude/message-worker-mongo-outage-writes`,
stacked on the #188 branch because it reuses two things introduced there:
`pkg/circuitbreaker` (absent on `main` at the time) and the Valkey client
`history-service` already connects for the subauth L2 (`cfg.ValkeyAddrs`).

It has since been **folded into PR #188 itself** — the stacked branch was a
strict superset of #188's tip, so it fast-forwarded in cleanly and the combined
branch was rebased onto `main`. There is no longer a separate PR, no stacking
dependency, and no merge-order constraint: #188 now carries both the
subscription-authz outage work and this DEK work as one linear history.

**Naming note:** the branch and this file are named `…-writes` from when the
scope was write-path-only. The scope later widened to the shared DEK constraint
(read + write); the names are kept as historical artifacts and the title above
reflects the real scope.

## Mechanisms

### Mechanism 1 — At-rest DEK Valkey L2 (`pkg/atrest`) — the core

Insert a shared L2 (Valkey) tier into the cipher's DEK resolution, mirroring the
shape of `pkg/roommetacache` / `pkg/subauthcache`.

Today `cipherImpl.dekFor(ctx, roomID)`:
1. L1 LRU (`c.cache.get(roomID)`) — unwrapped AEAD → hit returns immediately.
2. Miss → `c.store.Get(ctx, roomID)` (Mongo `DEKStore`) → `*RoomDataKey`
   (`WrappedDEK`) → Vault unwrap → `c.cache.set(roomID, aead)`.

New behavior on L1 miss:
1. **L2 read**: GET the wrapped-DEK record from Valkey (key `atrest.DEKKey(roomID)`,
   i.e. `dek:{roomID}` — `{roomID}` hash-tag per house convention; always build it
   through the helper rather than hard-coding the string) → on hit, Vault-unwrap →
   populate L1 → return.
2. **L2 miss** → the Mongo `DEKStore` fetch **wrapped by the circuit breaker**,
   then **write-through** the wrapped record to Valkey with `ATREST_DEK_L2_TTL`
   → Vault-unwrap → populate L1.

Properties:
- **Valkey stores only the Vault-wrapped DEK** (`RoomDataKey.WrappedDEK` +
  the key-version / metadata needed to unwrap) — never a plaintext key.
  Confidentiality is unchanged from Mongo-at-rest storage: an attacker with
  Valkey access still needs the Vault KEK.
- **Fail-open**: a nil client or any Valkey error degrades to the Mongo fetch;
  only the Mongo/Vault result governs the returned error. Never block on L2.
  `ATREST_DEK_L2_TTL <= 0` skips L2 population (mirrors the subauthcache
  `ttl<=0` fix: never cache a wrapped DEK without an expiry).
- **Circuit breaker** (`pkg/circuitbreaker`, shared with #188) wraps the Mongo
  `DEKStore.Get` so a cold-DEK miss during the outage fast-fails instead of
  stalling the worker goroutine on the Mongo fetch (bounded today by the
  cipher's `dekFetchTimeout` + singleflight coalescing; the breaker turns the
  repeated stalls into immediate fast-fails once open).
- **Vault** is still required to *unwrap* an L2 (or Mongo) hit; a warm **L1**
  (already-unwrapped AEAD) survives both a Mongo and a Vault outage. Vault
  availability is out of scope — the target failure is a *Mongo* outage, with
  Vault assumed up.

The L2 is additive and lives behind the existing `Cipher` interface, so both
`dekFor` callers benefit with no call-site changes:

- **`Cipher.Decrypt`** — `history-service`'s read path (`cassrepo.decrypt`, every
  history row) and its edit path. **This is what restores the 1-hour read
  guarantee.**
- **`Cipher.Encrypt`** — `message-worker`'s `SaveMessage`.

Every service that constructs a cipher must opt in by passing a Valkey client;
services that don't are unaffected (nil client → today's behavior). Both
`history-service` and `message-worker` opt in under this design.

### Mechanism 2 — Sender fail-open (`message-worker/handler.go`)

`FindUserByAccount` remains the preferred, full-fidelity source. On error
(Mongo unavailable), instead of returning (→ NAK), build the Cassandra sender
participant from the canonical event:
- `ID = evt.Message.UserID`, `Account = evt.Message.UserAccount`,
  name from `evt.Message.UserDisplayName`.

Because the gatekeeper already composed `UserDisplayName` from EngName +
ChineseName at send time, the rendered sender name is **not visibly degraded** —
only the internal EngName/ChineseName split is lost (cosmetic, and baked into
the Cassandra row at write time). Emit a `slog.Warn` (IDs only). The
system-message nil-sender path is unchanged.

### Mechanism 3 — Mention fail-open (`message-worker/handler.go`)

On `mention.Resolve` error, log a warn and proceed with unresolved mentions
(empty `Mentions`) rather than NAK. The message content — including the literal
`@tokens` — persists intact. Consequence: mentioned users may miss a
notification during the outage (best-effort, accepted).

### Cold path (unchanged)

When the DEK is genuinely unavailable (L1 miss + L2 miss + Mongo down →
breaker-open fast-fail), or any other transient failure occurs, `processMessage`
returns a transient error and the existing `jsretry.Settle(..., DefaultBackoff,
...)` NAK-backoff keeps the message durable in `MESSAGES-CANONICAL`. Malformed
events remain `errcode.Permanent` (Ack-drop) as today.

**This buffer is short, and it is not a drop-free guarantee.** With the current
defaults the message gets 5 delivery attempts (`CONSUMER_MAX_DELIVER`, default
`5` in `pkg/stream/consumer.go`) spaced by `jsretry.DefaultBackoff`
(1s → 5s → 30s → 2m, last entry reused), so the NAK delays total **2m36s** —
call it 2.5–3 minutes end to end with processing time. If Mongo has not returned
by the 5th attempt, JetStream stops redelivering and the message is dropped.

So the cold path covers a brief blip, not the 1-hour outage this design targets.
The L1+L2 DEK tiers are what carry warm rooms through a long outage; a room whose
DEK is cold in both tiers, and a thread reply whose Mongo-side work cannot be
faked open, fall back to this ~3-minute window. Extending them to the 1-hour
target is an **ops change, not a code change**: raise `CONSUMER_MAX_DELIVER` for
`message-worker` (with `MESSAGES-CANONICAL` retention sized to match). We do not
raise it by default here, because a larger `MaxDeliver` also lengthens how long a
genuinely poisonous message is retried.

## Configuration

New knobs (`caarlos0/env`, envDefault, SCREAMING_SNAKE_CASE):

| Env var | Default | Purpose |
|---|---|---|
| `VALKEY_ADDRS` (+ `VALKEY_PASSWORD`) | empty | Valkey cluster fronting the DEK L2. Empty → L2 disabled (fail-open to today's Mongo-only behavior) |
| `ATREST_DEK_L2_TTL` | `90m` | Wrapped-DEK L2 retention (the outage buffer) |
| `ATREST_DEK_BREAKER_FAILS` | `5` | Consecutive Mongo-DEK-fetch failures to open the breaker; `0` disables it (calls always pass through) |
| `ATREST_DEK_BREAKER_COOLDOWN` | `10s` | Open→half-open cooldown |

L2 wiring is guarded on `len(VALKEY_ADDRS) > 0`.

Both cipher-constructing services wire it identically — connect Valkey (guarded),
construct the breaker from the knobs, then wrap `atrest.NewMongoDEKStore(...)` in
`atrest.NewL2DEKStore(...)` and hand the resulting `DEKStore` to the **unchanged**
`atrest.NewCipher`. The L2 is a decorator, so `NewCipher` needs no new option:

- **`message-worker/main.go`** — adds a Valkey client (it has none today).
- **`history-service/cmd/main.go`** — **reuses the Valkey client #188 already
  connects** for the subauth L2 (`cfg.ValkeyAddrs`); no second connection. Its
  DEK breaker is a *separate* instance from #188's subscription breaker, so
  DEK-fetch health and subscription-fetch health can't reset each other (the same
  independent-breaker rule #188 established for gatekeeper's room-meta reads).

## Observability

- Reuse `pkg/cachemetrics` — the DEK L2 emits `cache="atrestdek",tier="l2"`
  hit/miss/error series (the existing `dekCacheHits`/`dekCacheMisses` L1 metrics
  stay).
- Reuse the shared breaker's transition hook (from #188): a breaker-state gauge
  + `slog.Warn` on transition (metadata only), consistent with the read/send
  work's observability.
- A counter for "message persisted with degraded (event-fallback) sender"
  during a Mongo outage, so the fail-open path is measurable.
- A counter for **decrypt failures attributable to an unresolvable DEK**, split
  from other decrypt errors — this is the metric that tells operators an outage
  has started costing them history reads (the gap this design closes).

## Testing (TDD)

Red-Green-Refactor; ≥80% coverage, ≥90% on new/changed `pkg/atrest` L2 code.

**Unit**
- `pkg/atrest`: L1 hit (no L2/Mongo); L1-miss→L2-hit→unwrap→L1 populate;
  L1+L2-miss→Mongo→L2 write-through + L1 populate; fail-open on Valkey
  error/nil client (→ Mongo); `ttl<=0` skips L2 population; breaker-open
  fast-fails the Mongo fetch (loader not invoked); Valkey holds only wrapped
  ciphertext (assert the stored blob is the wrapped form, not the plaintext key).
- `message-worker`: sender fail-open — a Mongo `FindUserByAccount` error still
  persists (sender built from the event, `SaveMessage` called); mention
  fail-open — a resolver error still persists with empty mentions; warm-DEK
  persist under simulated Mongo-down (cipher served from L1/L2); cold-DEK →
  transient error → NAK path (not Ack/drop).

- `history-service`: a `LoadHistory` over encrypted rows succeeds while the Mongo
  DEK fetch errors but the DEK is L1/L2-resolvable; and the **expiry regression
  guard** — with the DEK L1 entry expired and Mongo failing, the read still
  succeeds via L2 (this is the case that fails today).

**Integration (`//go:build integration`, testcontainers via `pkg/testutil`; execution CI-deferred where Docker is unavailable)**
- `pkg/atrest`: Valkey up + Mongo DEK store unavailable → a warmed room's
  Encrypt/Decrypt succeed via the L2; a cold room errors.
- `message-worker`: end-to-end persist of a plain message with the Mongo
  user/DEK reads made to fail while Valkey (DEK L2) is warm → row lands in
  Cassandra with correct identity.
- `history-service`: write an encrypted row, warm the DEK L2, drop the DEK L1 and
  make Mongo fail → `LoadHistory` still returns the decrypted body.

## Non-goals

- **Thread replies** — they need Mongo thread-room/subscription state (and a
  `threadRoomID` partition coordinate) not present on the event; they continue
  to NAK-buffer and persist once Mongo returns. Deferred.
- **Truly cold rooms** (no cached/L2 DEK) — can neither persist nor decrypt
  during the outage (no key, no safe write or read); accepted gap.
- **Vault outages** — a different failure surface; only a warm L1 survives one,
  and hardening Vault availability is out of scope.
- **The subscription-authz decision** (send accept + history access check) —
  owned by PR #188; this design does not change it.
- **Raising `ATREST_DEK_CACHE_TTL` as the fix** — it only shifts the expiry
  window and still dies on a pod restart; the L2 is the actual fix.

## Files touched (anticipated)

- **`pkg/atrest`**: add the L2 (Valkey) read-through + write-through as a
  `DEKStore` decorator (`dek_store_l2.go`) with breaker injection and
  `cachemetrics` wiring; unit + integration tests. `cipher.go` is untouched —
  it already takes a `DEKStore`.
- **`pkg/circuitbreaker`**: reused **as-is** from the #188 base (this branch is
  stacked on #188). No changes.
- **`message-worker`**: `handler.go` sender + mention fail-opens; `main.go`
  Valkey + breaker + cipher-L2 wiring and config knobs; unit + integration
  tests.
- **`history-service/cmd/main.go`**: pass the **existing** (#188-connected)
  Valkey client + a *separate* DEK breaker into `atrest.NewL2DEKStore`, whose
  result is handed to `atrest.NewCipher`; config knobs.
  No change to its read handlers — the fix is entirely behind the cipher.
- **`message-worker/deploy/docker-compose.yml`**: add `VALKEY_ADDRS=valkey:6379`
  so the outage-survival write path is exercisable in local dev.
  (`history-service`'s compose already sets it, from #188.)
- **Docs**: no `docs/client-api.md` change (no client-facing wire schema/event
  change).
