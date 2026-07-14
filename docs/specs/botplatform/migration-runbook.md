# Bot Platform NextGen — Schema & Migration Runbook

> **Operational companion** to the design spec:
> - [Bot Platform NextGen Auth Migration](./auth.md) — combined Parts I (requirements) + II (technical design) + III (components/integration)
> - [Bot Traffic Isolation](./traffic-isolation.md) — combined Parts I (requirements) + II (technical design)
>
> **Audience:** SRE/ops running the migration; engineers writing the migration jobs. Designed in AS-IS vs TO-BE format so the delta and the mechanic are explicit at every step.
>
> **Status:** DRAFT — pending verification against the legacy v2 Mongo dump and the current nextgen `chat` DB. **Revised 2026-06-24** for the mixed-storage design (auth.md Part II §4): passwords stay on `users` in-place; sessions live in a new dedicated `sessions` collection seeded once at cutover from legacy `users.services.resume.loginTokens[]`.

---

## 1. Scope

Two Mongo collections are touched by this migration:

| Collection | AS-IS location | TO-BE location | Status |
|---|---|---|---|
| **`users`** | Legacy RC DB (Meteor) + nextgen `chat` DB | Nextgen `chat` DB (siteA, siteB, …) — same shape, password material in-place at `services.password.bcrypt` | **Modified** — provisioning index added if PR #295 didn't already; auth read-paths reused **in place** |
| **`sessions`** | (none — embedded as `loginTokens[]` array inside legacy `users`) | Nextgen `chat` DB, owned by `botplatform-service` | **NEW** — bulk-imported once at cutover from `users.services.resume.loginTokens[]` |

No other collections are added or changed by this design. There is **no** separate `credentials` collection (the 2026-06-24 pivot dropped it — passwords live on `users` in-place; see auth.md Part II §4.1).

---

## 2. Collection: `users` (modified, shared)

### AS-IS — legacy RC Mongo (source of truth for legacy traffic)

Meteor/RC schema, single-site, monolithic. Auth and identity live in one doc.

```js
// Database: meteor (legacy RC)
// Collection: users
{
  _id:        "<17-char Meteor base62>",          // preserved into nextgen verbatim
  username:   "alice"  | "xxx.bot",
  name:       "Alice Smith",
  emails:     [{ address: "...", verified: true }],
  active:     true,
  roles:      ["bot"] | ["admin"] | ["user"],
  createdAt:  ISODate(...),
  services: {
    password: {
      bcrypt: "$2b$10$..."                         // read directly by nextgen (SAME path; no extraction)
    },
    resume: {
      loginTokens: [
        { when: ISODate(...),
          hashedToken: "b64sha256...",             // → bulk-imported to sessions._id at cutover (§3.4)
          type: undefined                          // regular login token → import
        },
        { ..., type: "personalAccessToken"         // PAT → SKIP (humans only)
        },
        ...                                        // capped at 50 in RC
      ]
    }
  },
  requirePasswordChange: true | false              // read directly by nextgen (SAME path)
}
```

**Indexes (legacy):** `_id` (primary), `username` (unique), `emails.address` (sparse unique).

**Properties:** single-site, monolithic, no `siteId` field anywhere.

### AS-IS — nextgen `chat` DB (current state pre-bot-platform)

After PR #295 (portal-service + provisioning gate), nextgen `users` has the provisioning-related fields. Per-site DB (one `chat` DB per site). The identity-sync that PR #295 wires up already preserves the legacy `_id` verbatim AND carries the `services.password.*` + `requirePasswordChange` paths end-to-end (open verification item §6).

```js
// Database: chat (nextgen, per site)
// Collection: users
{
  _id:       "<17-char Meteor base62>",            // imported verbatim from legacy
  account:   "alice"  | "xxx.bot",                 // = legacy username
  siteId:    "siteA",                              // (PR #295) home site of this account
  roles:     ["bot"] | ["admin"] | ["user"],
  name:      "Alice Smith",
  active:    true,
  createdAt: 1718500000000,                        // ms since epoch

  // Auth paths (legacy schema, in-place — auth.md Part II §4.1):
  services: {
    password: { bcrypt: "$2b$10$..." }             // synced from legacy
  },
  requirePasswordChange: true | false              // synced from legacy
  // NB: services.resume.loginTokens[] is NOT read at runtime by nextgen — only
  // by the one-shot session-import job at cutover (§3.4). After that, sessions
  // live in the dedicated `sessions` collection.
}
```

**Indexes (existing):** `_id` (primary), `account` (unique).

**Open against PR #295 (to verify):** does it add a compound index `{account: 1, siteId: 1}`? The provisioning gate needs it to hit an index, not a collscan.

### TO-BE — nextgen `chat` DB (post-rollout)

Same shape as AS-IS. Auth read-paths unchanged: nextgen reads `services.password.bcrypt` and `requirePasswordChange` from the same paths the legacy app already populates.

**Indexes (TO-BE):**
- `_id` (primary, unchanged)
- `account` (unique, unchanged)
- **`{ account: 1, siteId: 1 }`** (NEW unique compound, gate the provisioning lookup) — confirm vs PR #295

**Owner:** shared. Read by botplatform-service (login verify + provisioning gate), auth-service (provisioning gate), portal-service (lookup). Written by:
- botplatform-service on `/changepwd` (password rotation by the user — see auth.md §6.3 write-authority guard during the canary window)
- admin-service on `POST /v1/admin/bots/{id}/password` (admin rotation) and `POST /v1/admin/bots/{id}/suspend` (`active: false`)
- the legacy → nextgen users-sync (identity + auth fields, until 100% cutover)

### Delta vs AS-IS (post-cutover)

| Change | Direction | Notes |
|---|---|---|
| `services.password.*` | **Unchanged shape** | Same path the legacy stack uses; nextgen reads/writes in place |
| `services.resume.loginTokens[]` | **Read once at cutover, then ignored by nextgen runtime** | One-shot bulk import into the new `sessions` collection (§3.4). After cutover, legacy continues to write here; nextgen never re-reads it (the canary monotonically shifts traffic to nextgen-issued tokens). |
| `requirePasswordChange` | **Unchanged** | Same path |
| `{account, siteId}` index | **Added** (if not already by PR #295) | Provisioning-gate performance |

### Migration mechanic

`users` itself is **not** migrated as a one-shot job — PR #295 + the live users-sync own that, and they preserve the auth read-paths verbatim. The only ops actions this design requires on `users` are:

1. Create the provisioning-gate compound index (idempotent — safe to run on every deploy):
   ```js
   db.users.createIndex(
     { account: 1, siteId: 1 },
     { unique: true, name: "account_site_unique" }
   )
   ```
2. Confirm identity-sync carries `services.password.bcrypt` + `requirePasswordChange` (verification item §6).

### Verification

```js
// 1. Provisioning-gate lookup uses the new index
db.users.find({ account: "xxx.bot", siteId: "siteA" }).explain("queryPlanner")
// Expect: IXSCAN on account_site_unique, not COLLSCAN

// 2. Auth read-paths are populated for migrated bots (sample)
u = db.users.findOne({ account: "xxx.bot" })
assert(u.services.password.bcrypt.startsWith("$2b$10$"), "bcrypt present")
```

---

## 3. Collection: `sessions` (NEW)

### AS-IS

Does not exist as a collection. Equivalent data lives as `legacy_users.services.resume.loginTokens[]` — a capped array (50 entries) embedded on each user doc. **Nextgen replaces this with one-doc-per-session under a configurable per-account cap (`SESSIONS_MAX_PER_ACCOUNT`, default 100), FIFO eviction by `issuedAt`** — see auth.md Part II §5.6. The legacy 50-token max fits comfortably under the default nextgen cap of 100, so the import is lossless; if `SESSIONS_MAX_PER_ACCOUNT` is ever set below 50, the import would truncate.

### TO-BE — nextgen `chat` DB

```js
// Database: chat (per site)
// Collection: sessions
// Owner: botplatform-service (writes on login + cap-eviction);
//        admin-service (writes on admin revoke / suspend / rotate-password)
{
  _id:        "b64hash...",         // token hash, function depends on scheme (auth.md Part II §4.6)
                                     //   "legacy":  base64(sha256(rawToken))
                                     //   "v1":      base64(HMAC-SHA-256(server_secret, rawToken))
  userId:     "<17-char Meteor ID>",
  account:    "xxx.bot",            // denormalized — validate returns it directly
  siteId:     "siteA",              // home site — set at issue, never changes
  scheme:     "legacy" | "v1",
  issuedAt:   1718500000000          // ms since epoch; FIFO ordering key for cap eviction (§5.6)
  // NB: NO lastUsedAt — validate is pure-read (auth.md §5.3, REVISED 2026-06-24)
  // NB: NO expiresAt — sessions are permanent until cap-evicted or admin-revoked (auth.md §5.5)
  // NB: NO schemaVersion — design decision: skip until we actually need it
}
```

**Indexes:**
- `_id` (primary, auto) — token hash lookup, the hot path
- `{ userId: 1, issuedAt: 1 }` compound (name: `userId_issuedAt`) — IXSCAN serves BOTH the cap-eviction victim lookup at login (`find({userId}).sort({issuedAt:1}).limit(over)`) AND admin's list-sessions-by-user query

No TTL index — sessions don't time-expire (auth.md Part II §5.5).

### Delta vs AS-IS

Everything is new. Per-source mapping:

| TO-BE field | AS-IS source | Transformation |
|---|---|---|
| `_id` | `users.services.resume.loginTokens[].hashedToken` | **Verbatim copy** (already `base64(sha256(...))`) — preserves zero-bot-change compatibility |
| `userId` | `users._id` (the containing user) | Copy |
| `account` | `users.username` | Copy (denormalized for fast validate response) |
| `siteId` | n/a in legacy; from migration job config | Constant = the nextgen site this import lands at |
| `scheme` | n/a | Constant `"legacy"` for imported rows |
| `issuedAt` | `users.services.resume.loginTokens[].when` → ms epoch | Type conversion |

**Skipped entries (allow-list):** any `loginTokens[]` entry where `type` is set to a non-empty value — regular login tokens have `type` absent/empty. PATs (`type:"personalAccessToken"`) are a human-user feature; any future non-empty `type` (e.g. impersonation) is also skipped until explicitly added to the allow-list. See auth.md Part II §6.2 import filter.

### Migration mechanic

```python
# Pseudocode — idempotent, resumable
import_time = now_ms()

for user in legacy_users.find(
    { "services.resume.loginTokens.0": { "$exists": True } },
    sort=[("_id", 1)],
    cursor_batch_size=500
):
    for token in user["services"]["resume"]["loginTokens"]:
        # Allow-list: import ONLY tokens whose type is absent/empty
        # (regular login tokens). Any non-empty type (PAT or future
        # variants) is skipped — auth.md Part II §6.2 import filter.
        if token.get("type"):              # any truthy type → SKIP
            continue
        if not token.get("hashedToken"):
            continue                       # malformed entry — skip safely

        nextgen_chat.sessions.update_one(
            { "_id": token["hashedToken"] },
            { "$setOnInsert": {
                "_id":      token["hashedToken"],
                "userId":   user["_id"],
                "account":  user["username"],
                "siteId":   CONFIG.siteId,           # this site's ID
                "scheme":   "legacy",
                "issuedAt": to_epoch_ms(token["when"]),
              }
            },
            upsert=True
        )
    checkpoint(user["_id"])
```

**Properties:**
- **Idempotent:** keyed on `_id` (token hash) — re-running the job won't dupe.
- **Resumable:** outer cursor on `users._id`; intra-user loop is bounded by the legacy 50-cap.
- **PAT skip:** explicit and tested; golden fixture validates the skip predicate.

### Verification

```js
// 1. Count import vs source (PATs subtracted)
nextgenCount = db.sessions.countDocuments({ scheme: "legacy" })
legacyCount  = legacy.users.aggregate([
  { $unwind: "$services.resume.loginTokens" },
  { $match:  { "services.resume.loginTokens.type": { $in: [null, ""] } } },
  { $count:  "total" }
]).next().total
assert(nextgenCount === legacyCount, "session count mismatch")

// 2. All imported rows have valid siteId
assert(db.sessions.countDocuments({ scheme: "legacy", siteId: { $ne: CONFIG.siteId }}) === 0)

// 3. Validate-path performance — random spot check
db.sessions.find({ _id: "<sample hash>" }).explain("executionStats")
// Expect: nReturned: 1, totalDocsExamined: 1, IXSCAN on _id, executionTimeMillis: <5

// 4. Cap-eviction victim lookup uses the compound index
db.sessions.find({ userId: "<sample uid>" }).sort({ issuedAt: 1 }).limit(1).explain("queryPlanner")
// Expect: IXSCAN on userId_issuedAt, not COLLSCAN+SORT
```

### Rollback

```js
db.sessions.drop()  // safe — token hashes are reproducible from legacy loginTokens[]
```

---

## 4. Migration job — end-to-end ordering

Per site, in this order. Each step is reversible.

| Step | Action | Pre-check | Post-check | Rollback |
|---|---|---|---|---|
| **0** | Deploy `botplatform-service` (dark, no traffic) | nextgen `chat` DB reachable | `/healthz` returns 200 | Scale to 0 |
| **1** | Create `users` compound index `{account, siteId}` (if PR #295 didn't) + create `sessions` collection + its indexes | shell access to nextgen `chat` DB | `db.users.getIndexes()` shows `account_site_unique`; `db.sessions.getIndexes()` shows `userId_issuedAt` | drop indexes / collection |
| **2** | Run **sessions bulk import** (§3.4) — copies non-PAT entries from legacy `users.services.resume.loginTokens[]` into `sessions` | step 1 complete; identity-sync confirmed live for `services.password.*` | `db.sessions.countDocuments({scheme:"legacy"})` matches reconciliation query (§5) | `db.sessions.drop()`; rerun |
| **3** | Smoke-test login + validate against a known bot on nextgen (out-of-band, no traffic shift) | step 2 complete | `POST /api/v1/login` returns success; nextgen-issued `bp1_…` token validates; the bot's prior legacy token also validates against the imported session row | n/a (no destructive change) |
| **4** | Deploy `admin-service` + `admin-portal` (dark, ops-only access) | steps 0–3 complete | admin can log in via admin-portal and list/suspend a test bot end-to-end | scale to 0 |
| **5** | Start **chat-GW canary** (1% → 10% → 50% → 100%) | step 3 green; identity-sync confirmed live | per-canary-step SLOs hold (auth.md Part II §10.2) | re-weight VirtualService back to legacy |
| **6** | **Sunset legacy** — disable legacy-fallback path in `/v1/auth/validate`; remove fz2/chat backend | 100% traffic on nextgen for ≥ 1 week with zero legacy-fallback validations observed in metrics | no 401s spike post-flip | re-enable legacy fallback; restore fz2 backend |

**Critical: steps are PER SITE.** A multi-site rollout repeats steps 0–6 per site, with the chat-GW canary (step 5) coordinated across sites via separate VirtualServices.

### Source-of-truth during the canary

- **Password material** lives on the same `users` doc both stacks read; identity-sync keeps it current. **One write-authority guard:** during the canary window, keep nextgen-side password changes (`/changepwd`, admin rotate-password on `admin-service`) **disabled** until 100% cutover. Otherwise simultaneous legacy + nextgen writes to the same doc could lose updates. After 100% cutover, nextgen owns all writes to the password paths.
- **Sessions** live in the new `sessions` collection nextgen owns end-to-end after the bulk import (step 2). Legacy continues to write `users.services.resume.loginTokens[]` during the canary; we **do not** sync those new legacy writes back into nextgen's `sessions` collection mid-canary. A bot that re-logs in via legacy gets a legacy-shaped token; if traffic then shifts to nextgen, that bot misses-fast on the unknown token and re-logs via nextgen — acceptable because the canary monotonically shifts traffic forward and re-login is cheap (one round-trip).
- **Tokens (downstream re-validation).** Nextgen-issued `v1` tokens don't exist on the legacy side, so any downstream that re-validates a bearer token must use our dual-token validator — see **auth.md Q14 / §9.8**.

---

## 5. Reconciliation queries (cheat sheet)

Bookmark these for the canary-phase health-checks.

```js
// 1. All non-PAT legacy tokens landed in sessions
nextgenChat.sessions.countDocuments({ scheme: "legacy" }) ===
  legacy.users.aggregate([
    { $unwind: "$services.resume.loginTokens" },
    { $match:  { "services.resume.loginTokens.type": { $in: [null, ""] } } },
    { $count:  "total" }
  ]).next().total

// 2. Identity-sync has carried the password path into nextgen for every bot
nextgenChat.users.countDocuments({ "services.password.bcrypt": { $exists: true } })
// Compare against legacy.users.countDocuments({ "services.password.bcrypt": { $exists: true } })
// Expect parity (modulo accounts intentionally not synced — humans without SSO-only login, etc.)

// 3. All sessions in this site's DB are pinned to the expected siteId
assert(nextgenChat.sessions.countDocuments({ siteId: { $ne: CONFIG.siteId } }) === 0,
       "session site mismatch — some rows landed in the wrong site's DB")

// 4. Validate-path performance — random spot check (one sessions doc, by _id)
db.sessions.find({ _id: "<sample hash>" }).explain("executionStats")
// Expect: nReturned: 1, totalDocsExamined: 1, IXSCAN on _id, executionTimeMillis: <5

// 5. Cap enforcement — no account exceeds SESSIONS_MAX_PER_ACCOUNT
db.sessions.aggregate([
  { $group: { _id: "$userId", n: { $sum: 1 } } },
  { $match: { n: { $gt: 100 } } }
])
// Expect: empty result. Any returned row indicates either (a) the cap config
// is < 100 in your site, (b) the import bulk-loaded a bot with > cap legacy
// tokens (legacy cap was 50, so this shouldn't happen for cap=100), or (c) a bug.
```

---

## 6. Open questions / pending verification

- [ ] **`users.{account, siteId}` compound index** — confirm PR #295 added it, or add in step 1 of this runbook.
- [ ] **JSON Schema validator** on `sessions` — decision: include or defer? (Mechanical to add; would catch malformed rows from the import job before they land.)
- [ ] **Per-site DB topology** — confirm whether each nextgen site has its own `chat` DB or a shared cluster with site-prefixed collections. Affects how the migration job is parameterized.
- [ ] **Live users-sync mechanic** — owned by external infra team; confirm it carries `services.password.bcrypt` + `requirePasswordChange` (not just identity fields). The session bulk import (step 2) reads legacy directly, so it doesn't depend on the sync.
- [ ] **Legacy DB read credentials** — ops provisions; this runbook assumes a read-only Mongo user with access to `legacy.users`.

---

## 7. References

- Auth spec **Part II** §4 (data model: `users` for password, `sessions` collection for tokens), §5.6 (cap + FIFO eviction), §6 (mixed migration: passwords no-extract, sessions bulk-import), §10 (cutover)
- Auth spec **Part I** §5 (critical constraints), §6 (migration overview), §8 (admin-portal + admin-service split, 2026-06-24)
- Auth spec **Part III** §3 (data-flow), §4.3 (token compatibility)
- PR #295 — portal-service + provisioning gate (the `users.siteId` field origin + identity-sync that preserves the auth read-paths)
