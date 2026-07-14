# Spec: media-service — User / Bot / Room avatar resolver + image server

> **Status:** DESIGN — not yet implemented. This document is the agreed design
> record from the brainstorming session on branch `claude/media-service`.
> Forward-looking language ("the service will …", "the handler …") describes
> planned work, not shipped behaviour.

*A new Gin HTTP service that, given an account or a room id, either **307-redirects**
to where the avatar actually lives (external employee-photo service for users, or
the owning cluster) or **proxy-streams the image bytes** from MinIO — falling
back to a generated/default image so a request never dead-ends.*

---

## 1. Goal

Replace the soon-to-be-retired legacy (rocketchat) avatar endpoint with a
first-party service that serves avatars for **users**, **bots**, and **rooms**
across a multi-cluster (multi-domain) deployment, where:

- **User** data is synced to every cluster → resolvable locally.
- **Bot** and **room** data are cluster-bound (not synced) → may require a
  single cross-cluster hop to the owning cluster.

Public read endpoints + authenticated write endpoints:

| Endpoint | Purpose | Auth |
|----------|---------|------|
| `GET /api/v1/avatar/:accountName` | User **and** bot avatar (frontend routes dm/botDM room avatars here too) | public |
| `GET /api/v1/avatar/room/:roomID` | Room avatar — **channel / discussion only** | public |
| `PUT /api/v1/avatar/bot/:botName` | Upload a custom bot avatar | **🔴 none (v1)** |

**v1 write scope = bot uploads only.** Room and user avatars are never uploaded
through this service: users resolve to the external employee-photo service, and
room custom avatars are **read-only** — they arrive via a legacy-data migration
that writes directly into the `avatars` collection + MinIO (§4.4), not through
any endpoint. Room `PUT` and all `DELETE`/reset are out of scope for v1.

For any kind, a custom image (when present) takes priority; the dynamic SVG (§8)
is the universal fallback.

Non-goals: per-size rendering (`_120` is fixed for the employee-photo redirect);
room/user uploads; deleting/resetting a custom avatar. **Read** endpoints are
public; **🔴 the bot-upload endpoint is UNAUTHENTICATED in v1** — auth is
deferred until the model is decided (§7a.4). This is a known risk (anyone can
overwrite any bot's avatar / fill storage) and **MUST be gated before
production**.

> **Bot detection** uses the codebase's canonical `botPattern` (`` `\.bot$|^p_` ``):
> an account is a bot if it ends in `.bot` **or** begins with `p_`. (Earlier
> drafts said `.bot`-only.)

## 2. Service shape

A new flat service `media-service/` at repo root, following the per-service
layout. **It does not use NATS, and v1 has no auth** (§7a.4). Mongo + MinIO backed.

| File | Responsibility |
|------|----------------|
| `main.go` | Config (`caarlos0/env`), wire Mongo + MinIO, Gin server + timeouts, graceful shutdown (`pkg/shutdown.Wait`) |
| `routes.go` | Register GET ×2 (public), `PUT /bot/:botName` (open in v1), `GET /healthz` |
| `handler.go` | Read path: resolve owning site → cross-cluster redirect → avatars-doc lookup → stream/default |
| `upload.go` | Bot-upload write path: validate (botPattern/type/size/decode), locality+existence, store to MinIO, upsert `avatars` doc |
| `avatar.go` | `renderDefaultSVG(seed, initial)` pure deterministic generator + object-key helpers |
| `store.go` | `avatarStore` interface — `EmployeeID` (user), `BotSite` (bot owning siteID + existence via user record), `RoomSite` (siteID+type+name via subscriptions), `Avatar` (avatars-doc lookup), `SetBotAvatar` (upsert) + `//go:generate mockgen` |
| `store_mongo.go` | Mongo implementation (`users`, `subscriptions`, `avatars`) |
| `handler_test.go` | Unit tests with mocked store + fake MinIO/stream seam |
| `integration_test.go` | testcontainers (Mongo + MinIO via `pkg/testutil`), `//go:build integration` |
| `mock_store_test.go` | Generated mock (never hand-edited) |
| `deploy/` | `Dockerfile`, `docker-compose.yml`, `azure-pipelines.yml` |

Mandatory cross-cutting: `GET /healthz` (liveness 200; dependency-readiness
probing is out of scope for v1); the auth-service middleware trio —
`requestIDMiddleware` (via `idgen.ResolveRequestID` + `natsutil.WithRequestID`),
`accessLogMiddleware` (`slog` JSON: method/path/status/latency/request_id), CORS;
`errcode`/`errhttp` for client-facing errors; server timeouts; ≥80% coverage via
TDD.

**Observability scope = auth-service parity** (slog + request-id + access-log).
OTel tracing + Prometheus `/metrics` are **deferred to post-v1** (§9) — but v1
preserves the seams so adding them later is additive: `context.Context` is
threaded through every store/MinIO call, and the read handler returns a **typed
outcome** (`kind` ∈ user/bot/room × `outcome` ∈ `redirect`/`stream`/`default`/
`304`) that `accessLogMiddleware` records. That outcome gives v1 traffic-split
visibility in logs and becomes the future `resolution_total{kind,outcome}` metric
label — instrumented at one seam, not scattered across the decision tree.

## 3. Configuration (env, `caarlos0/env`)

| Var | Meaning | Default / required |
|-----|---------|--------------------|
| `PORT` | HTTP port | `8080` |
| `LOG_LEVEL` | slog level | `info` |
| `SITE_ID` | this cluster's site id | required |
| `CLUSTER_DOMAINS` | JSON array of `{siteID, domain}` for cross-cluster redirects | required |
| `EMPLOYEE_PHOTO_BASE_URL` | external employee-photo base (host + path); the service appends `/{eid}_120.JPG` | required |
| `MONGO_URI` / `MONGO_DB` | operational DB | required / `chat` |
| `MINIO_ENDPOINT` / `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | object storage (custom uploads) | required |
| `AVATAR_BUCKET` | MinIO bucket for avatars | `avatars` |
| `MAX_UPLOAD_BYTES` | reject uploads larger than this | `1048576` (1 MiB) |
| `CACHE_MAX_AGE_SECONDS` | `Cache-Control: public, max-age=` value | `21600` (6h) |
| `EID_CACHE_TTL` | account→employeeId cache TTL (near-immutable → long) | `24h` |
| `EID_CACHE_CAPACITY` | account→employeeId cache max entries (≈ employee population) | `120000` |

`CLUSTER_DOMAINS` is a **JSON array** of `{"siteID","domain"}` objects mapping
each `siteID` to the **full base URL (including scheme)** of *that cluster's*
media-service, e.g.
`[{"siteID":"site2","domain":"https://media-service-site2"}]`. Parsed via a
`TextUnmarshaler` on the config type (not env's key/val splitting). Redirect
targets use the `domain` value **verbatim** — `clusterBaseURL(siteID)` returns it
and the handler never prepends a scheme. Cross-cluster redirects and
`EMPLOYEE_PHOTO_BASE_URL` are **config**, never hardcoded.

MinIO is **required** in v1 — it holds custom bot uploads and migrated room
images (§4.4).

## 4. Common mechanisms

### 4.1 Serving a stored image (the MinIO path)

The read path (§6/§7) reaches here **with the `avatars` doc `av` already in hand**
(the `_id` lookup that proved the image exists). Serving is driven by the doc, so
warm-cache revalidation never touches MinIO:

1. Set `Cache-Control: public, max-age=<cfg>` and `ETag: av.ETag`.
2. **Conditional revalidation — no MinIO call.** If `If-None-Match == av.ETag` →
   `304 Not Modified`, empty body, return. This is the dominant warm-cache path;
   the denormalized `av.ETag` (§4.4) lets it skip MinIO entirely.
3. Otherwise fetch the bytes: `obj := mc.GetObject(ctx, bucket, av.MinioKey)`,
   then `st, err := obj.Stat()` (`defer obj.Close()`):
   - **NotFound → dynamic default (§8).** A doc without its object is an
     inconsistency (e.g. a migrated `path` that no longer resolves); fall back
     rather than error, so the request never dead-ends.
   - other error → `fmt.Errorf("stat avatar object: %w", err)` → collapses to `internal`.
4. `c.DataFromReader(http.StatusOK, st.Size, st.ContentType, obj, nil)` — streams.

Rationale: the cacheable URL stays stable (media-service's own URL), so
`Cache-Control`/`ETag` work and the 304 is answered from Mongo alone; redirecting
to a MinIO presigned URL would defeat caching (expiring `Location`) and add a hop.
The 200 (cold-fetch) path uses the authoritative `Stat` values so it stays correct
even if doc and object disagree — `av.Size`/`av.ContentType` are kept for audit
and a future no-Stat fast path, **not** the live 200 response (only `av.ETag` is
on the hot path).

A doc-miss in §6/§7 (no `av`) never reaches here — the default is generated on the
fly (§8) and never written back.

### 4.2 Cross-cluster loop breaker (`?fwd=1`)

A request resolves to at most **one** cross-cluster hop. When forwarding,
append `?fwd=1`. A handler that sees `fwd=1` MUST resolve locally or fall back
to the dynamic default image — it MUST NOT redirect cross-cluster again. The
dynamic default (§8) is the universal backstop that guarantees termination.

If the resolved owning site has **no `CLUSTER_DOMAINS` entry** (misconfig /
unknown site), the handler cannot build a redirect target → it serves the dynamic
default rather than redirecting to nowhere, preserving the never-dead-end
guarantee.

### 4.3 Caching

- Baseline: `ETag` (from the MinIO object, or the deterministic hash for a
  generated default — §8) + `Cache-Control: public, max-age`.
- Cache-busting via `?v=`: **deferred, and not stored.** There is no `version`
  field on the `avatars` doc (§4.4), and the frontend's room/bot metadata
  (sourced from room-service / apps) carries no version it could append anyway.
  v1 relies on `ETag` revalidation; `?v`-based busting is revisited once version
  propagation is designed (§9). A request that does carry a `?v` MAY still be
  served with a long `max-age` + `immutable`.

### 4.4 The `avatars` collection (custom-image existence source)

A dedicated Mongo collection **owned by media-service**. **Presence of a
document = "this subject has a custom image in MinIO";** absence = serve the
dynamic default (§8). It is the authoritative existence check for both kinds, so
the common "no custom image" case is a cheap `_id` point-lookup that never
touches MinIO.

- **Writers:** media-service writes a doc on a bot upload (§7a); the legacy-data
  **migration** writes docs for pre-existing room (and bot) images. media-service
  never writes into room-service's `rooms` or the upstream `apps` collection — it
  owns only `avatars`, respecting service data boundaries.
- **Readers:** the GET path looks up the doc by `_id` to decide
  stream-from-MinIO vs dynamic default.
- **Cluster-local invariant:** every document belongs to **this** site — a
  subject owned by another cluster has its avatar data only in that cluster's
  `avatars` + MinIO. Cross-cluster routing is decided upstream (§6/§7) before the
  lookup, so the doc needs no `siteId`.

**Schema** (`pkg/model/avatar.go`; added to the `model_test` round-trip):

```go
type AvatarSubjectType string

const (
    AvatarSubjectRoom AvatarSubjectType = "room"
    AvatarSubjectBot  AvatarSubjectType = "bot"
)

// Avatar is a custom (uploaded or migrated) avatar for a room or bot. Presence
// of a document means the subject has a custom image in MinIO; absence means the
// service serves a generated default (§8). The collection is cluster-local, so
// no siteId is stored.
type Avatar struct {
    ID          string            `json:"id"          bson:"_id"`         // "<subjectType>:<subjectId>"
    SubjectType AvatarSubjectType `json:"subjectType" bson:"subjectType"` // "room" | "bot"
    // SubjectID is the id this service looks the subject up by:
    //   room → roomID;  bot → bot account (".bot").
    SubjectID   string    `json:"subjectId"   bson:"subjectId"`
    MinioKey    string    `json:"minioKey"    bson:"minioKey"`    // MinIO object key, used verbatim
    ContentType string    `json:"contentType" bson:"contentType"` // detected type (image/png|image/jpeg)
    Size        int64     `json:"size"        bson:"size"`        // object size, bytes (Content-Length)
    ETag        string    `json:"etag"        bson:"etag"`        // MinIO ETag — 304 without a MinIO hit
    CreatedAt   time.Time `json:"createdAt"   bson:"createdAt"`
    UpdatedAt   time.Time `json:"updatedAt"   bson:"updatedAt"`
}
```

`_id` is the deterministic composite `"<subjectType>:<subjectId>"` (e.g.
`room:r123`, `bot:helper.bot`) — the natural key, so "one custom image per
subject" is structural (no surrogate id, no extra unique index) and an upload
**upserts by `_id`**. No `version` field (§4.3); if `?v` is ever wired, a missing
int reads as 0.

**Migration from the legacy `avatars` collection** (one-time; writes this
cluster's docs — MinIO objects are **not** moved):

| legacy field | → `Avatar` |
|--------------|------------|
| `rid` | `subjectType=room`, `subjectId=rid` |
| `userId` (a bot) | `subjectType=bot`, `subjectId=`account (resolve `userId`→account) |
| `path` | `minioKey` (used verbatim) |
| `type` | `contentType` |
| `size` | `size` |
| `etag` | `etag` |
| `uploadedAt` | `createdAt` |
| `updatedAt` | `updatedAt` |

Migration rules:
1. **Only migrate `complete == true` records** — skip in-flight/abandoned legacy
   uploads (`uploading` / incomplete), whose MinIO object may be partial or absent.
2. **Bot records:** resolve legacy `userId` → bot `account` (join to `users`) to
   build `subjectId` / `_id`; the read path keys bots by account, not user id.
3. **Human-user avatars are not migrated** — real users resolve to the external
   employee-photo service and have no `avatars` doc.
4. Legacy `progress` / `complete` / `uploading` are **not** carried over — the
   single-PUT upload model (§7a.2) tracks no upload state.

The migration is a **separate one-off job, run outside media-service**, and is
**idempotent** (upsert by `_id`), so it can be re-run safely. It needs no
coordination with the service: a doc becomes live the moment it is written.

## 5. Account format, type, and owning-site resolution (Endpoint 1)

Accounts are **bare** — no `@domain` segment (any stray `@…` is stripped and
ignored). **Type is decided by `isBot(account)`** (`botPattern` = `` `\.bot$|^p_` ``):
bots end in `.bot` or begin with `p_`; everything else is a user.

| Kind | Routing |
|------|---------|
| user | synced to every cluster → **always local** (no cross-cluster hop, no owning-site lookup) |
| bot | cluster-bound avatar data → owning site resolved from the bot's **user record** (`User.SiteID`), then a cross-cluster redirect if remote |

**Owning-site resolution (bots and rooms):**
1. **`?siteid=` query hint (fast path).** If the request carries `?siteid=<id>`,
   use it directly — **no DB lookup** for the site. The frontend already knows the
   owning site, so this skips the resolution query entirely.
2. **Otherwise look it up** — bot: `store.BotSite(account)` → `User.SiteID`
   (`found=false` → bot has no record → default); room: `store.RoomSite(roomID)`
   via subscriptions (§7).

`CLUSTER_DOMAINS` (siteID→base URL) maps the resolved `siteID` to a redirect
target. Users never need this (always local).

## 6. Endpoint 1 — `GET /api/v1/avatar/:accountName`

```text
account := stripDomain(accountName)          # bare account; tolerate stray @…
if isBot(account):                           # ── bot (avatar data is cluster-bound)
    owning := c.Query("siteid")              # fast path: trust the hint, no DB
    if owning == "":
        owning, found := store.BotSite(ctx, account)   # User.SiteID
        if !found: serveDefault(account, account); return
    if owning != cfg.SiteID && !fwd:
        if base := clusterBaseURL(owning); base != "":
            307 → {base}/api/v1/avatar/{account}?fwd=1   # value incl. scheme
        # else unknown site → fall through to default
    if av, found := store.Avatar(ctx, "bot", account); found:
        serveStored(av)                                # 304 / stream (§4.1)
    else:
        serveDefault(account, account)                 # dynamic SVG (§8)
else:                                        # ── user (synced everywhere → always local)
    eid, found := cache[account]
    if !found: eid, found = store.EmployeeID(ctx, account)   # MISS MUST hit DB
    if found:
        cache.put(account, eid)
        307 → {EMPLOYEE_PHOTO_BASE_URL}/{eid}_120.JPG
    else:
        serveDefault(account, account)
```

- **Bot read path:** resolve owning site (`?siteid=` hint, else `User.SiteID` via
  `store.BotSite`) → cross-cluster redirect if remote → `avatars` doc present ?
  stream the MinIO object : dynamic default. There is **no** redirect to an
  app-provided URL — every avatar is served through this GET endpoint, so
  `App.AvatarURL` is not used.
- **Users are always local** (synced everywhere) — no owning-site lookup, no
  `?siteid=` needed; the hint applies to bots and rooms only.
- `serveDefault()` **dynamically generates** a deterministic SVG from the
  account (§8) and returns it directly — it does not read from MinIO and does
  not store anything.
- `Cache-Control: public, max-age=<cfg>` on every response (incl. redirects).
- **Correctness rule:** a mapping-cache *miss* falls back to the DB; it must
  **not** skip to the default branch (that would give a real user the wrong
  avatar). The cache is an accelerator only — bounded LRU + TTL, and
  **thread-safe** (Gin serves requests concurrently).
- `accountName`, `eid` are validated/escaped (`url.PathEscape`, allowlist
  regex) before being placed in a redirect `Location`.
- **Frontend-default contract:** once we 307 to the employee-photo host we no
  longer control the outcome — a user with an `employeeID` but no actual photo
  gets a `404` there. The client MUST render its own fallback on image-load
  failure (`<img onerror>`); our server-side default only covers bots, rooms, and
  users with no `employeeID` (§9, accepted).

## 7. Endpoint 2 — `GET /api/v1/avatar/room/:roomID`

```text
if hint := c.Query("siteid"); hint != "":   # fast path: trust hint, skip the subscription query
    if hint != cfg.SiteID && !fwd:
        if base := clusterBaseURL(hint); base != "":
            307 → {base}/api/v1/avatar/room/{roomID}?fwd=1; return
        # else unknown site → fall through to default
    # local (or unknown site): no RoomType/Name available → seed+initial = roomID
    if av, found := store.Avatar(ctx, "room", roomID); found: serveStored(av); return
    serveDefault(roomID, roomID); return

# no hint → resolve via subscriptions (yields SiteID + RoomType + Name)
room, found := store.RoomSite(ctx, roomID)
if !found:                          serveDefault(roomID, roomID)     # unknown here → can't forward
if room.RoomType in {dm, botDM}:    serveDefault(roomID, room.Name)  # frontend should use Endpoint 1
if room.SiteID != cfg.SiteID && !fwd:
    307 → {clusterBaseURL(room.SiteID)}/api/v1/avatar/room/{roomID}?fwd=1   # value incl. scheme
if av, found := store.Avatar(ctx, "room", roomID); found:
    serveStored(av)                                      # 304 / stream (§4.1)
else:
    serveDefault(roomID, room.Name)                      # dynamic SVG (§8)
```

- **`?siteid=` fast path.** When the frontend supplies the owning site, the
  subscription query is skipped: a remote hint → immediate redirect; a local hint
  → straight to the `avatars` lookup. Trade-off (accepted): without the
  subscription we have no `RoomType` or `Name`, so the default's initial uses
  `roomID`, and the dm/botDM guard is not applied (the frontend is trusted not to
  route dm/botDM rooms to this endpoint). Without the hint, the full path below
  runs.
- Owning site, room type, and room name come from the `subscriptions` collection
  (`Subscription.SiteID` / `.RoomType` / `.Name`), so media-service does not read
  room-service's `rooms` collection, and the default's initial is the room's name
  (§8.1). **`Subscription.SiteID` is the room's *owning* site** (verified:
  `inbox-worker` mirrors a remote room's membership onto each member's home site
  with `SiteID = event.SiteID`, the room's site) — so it correctly drives the
  cross-cluster redirect, and a member's home cluster does hold a local
  subscription for a remote room.
- **Resolution requires ≥1 local subscription for the room.** On the room's
  owning cluster this always holds (a channel/discussion has an owner). Elsewhere
  with no local member it returns not-found → default — the correct defensive
  outcome, since the frontend normally resolves the owning domain first (via the
  room-location service) and hits the right cluster directly.
- Room custom images are **read-only and migrated** — there is no room upload
  (§1). The `avatars` doc (written by the migration, §4.4) is the existence
  check; present → stream from MinIO, absent → dynamic default with no MinIO hit.
- The generated default is **never written back** to MinIO.
- dm/botDM are **user-type** avatars; the frontend fetches them via Endpoint 1
  using the counterpart user / bot account. If such a roomID nonetheless lands
  here, return the dynamic default (safe, not a 4xx).

## 7a. Upload API (custom bot/room avatars)

`PUT /api/v1/avatar/bot/:botName` accepts a custom bot image (request body = raw
image bytes; `Content-Type` declares the format). **Bots are the only uploadable
kind in v1** — users and rooms never upload (§1). On success the custom image
takes priority over the dynamic default on the bot's GET path.

`:botName` is a **bare bot account** (any stray `@…` stripped, §5). The avatars
doc keys on it (`_id = bot:{account}`), **identical to the GET read key** (§6), so
an upload and its later read always address the same doc. The bot's owning site
(for the locality check, §7a.3) comes from its user record, not the path.

**Success → `200 OK`** with a small JSON body `{etag, contentType, size, updatedAt}`,
so the uploader gets the new `ETag` for immediate cache-busting without a
follow-up `GET`. (`200`+body rather than `204`, since the result is useful.)

### 7a.1 Validation & security (mandatory)

- **Well-formed bot account.** The `:botName` (stray `@…` stripped) MUST satisfy
  `isBot` (`botPattern` = `` `\.bot$|^p_` ``); otherwise `400`
  (`errcode.BadRequest`). This prevents `_id` pollution from `PUT /bot/<anything>`.
- **Raster only — reject `image/svg+xml` uploads.** A user-supplied SVG served
  from our origin is **stored XSS** (SVG can carry `<script>`/`foreignObject`).
  v1 allowlist: **`image/png`, `image/jpeg`** (WebP deferred — §9). The *default*
  avatar is SVG because **we** generate it (trusted); uploads never are.
- **Size cap**: enforce `MAX_UPLOAD_BYTES` via `http.MaxBytesReader` before
  reading the body.
- **Verify the bytes are really an image**: decode with the stdlib `image`
  package (`image/png`, `image/jpeg`) and reject on decode failure. **v1 stores
  the original bytes** (no re-encode) — uploads are admin-only with low EXIF risk,
  and polyglots are neutralized on serving by the correct `Content-Type` +
  `nosniff` + CSP (below), not by re-encoding. (Re-encode-to-normalize is a future
  option, §9.)
- GET image responses (streamed custom image and generated default SVG) set `X-Content-Type-Options: nosniff` and `Content-Security-Policy: default-src 'none'`; redirects do not, and the upload sets `nosniff` only. This prevents MIME-sniffing and blocks script execution if a generated SVG is opened as a top-level document (§8.1).

### 7a.2 Storage

- **Write order: MinIO object first, then the `avatars` doc.** The doc is upserted
  only after the object is durably stored, so "doc exists ⟺ a complete image
  exists" — no upload-state flags are needed (contrast the legacy
  `progress`/`complete`/`uploading` fields, dropped in §4.4).
- The object's key is chosen by media-service and stored **verbatim** in
  `minioKey`, used as-is on reads — never re-derived from a convention (migrated
  room objects keep their legacy paths, §4.4). The **detected** content-type
  (from decode, not the client header) is set as object metadata and stored.
- The upserted doc (`_id = bot:{localPart}`, §4.4) records `minioKey`,
  `contentType`, `size`, `etag` and bumps `updatedAt`; its presence is the GET
  existence check (§6). A re-upload **overwrites** in place.
- No `DELETE`/reset in v1 (§1): a custom bot avatar can be overwritten by a new
  upload but not cleared back to the default.

### 7a.3 Cluster locality & existence

A bot's `avatars`/MinIO data lives only on its **owning cluster**, so an upload
must land there. Both checks come from one `store.BotSite(account)` lookup (the
bot's user record is synced to every cluster, so its `SiteID` is resolvable
locally even for a remote bot):

1. **Existence.** `found == false` → `404` (`errcode.NotFound`): no such bot.
2. **Wrong cluster.** `siteID != SITE_ID` → reject with an errcode carrying the
   correct target (`clusterBaseURL(siteID)`, a `wrong-cluster` reason). We do
   **not** proxy or `307` the body (server-side proxying would re-send up to 1 MiB
   and add an outbound-HTTP dependency); the client re-issues the PUT to the
   correct domain itself.
3. Otherwise (`siteID == SITE_ID`, exists) → proceed.

### 7a.4 Authorization — 🔴 NONE in v1

**The bot-upload endpoint is unauthenticated in v1** — no OIDC, no role check;
**anyone who can reach it can upload/overwrite any existing bot's avatar.** This
is a deliberate interim decision: the auth model is deferred until it is decided
(candidates: OIDC + platform-admin role, an internal/service token, or a per-bot
owner source). **It is a known risk and MUST be gated before any production
exposure** (network-restrict the endpoint in the meantime). media-service
therefore has **no `pkg/oidc` dependency and no auth config** in v1.

Read endpoints (GET) are public by design.

## 8. Default image — dynamic, deterministic, not persisted

The universal fallback for **every** kind (user, bot, room) — whenever the
external employee photo / MinIO custom image is absent — is an
**SVG "initials" avatar generated on the fly and returned directly to the
client**. It is **never written back to MinIO or Mongo**.

### 8.1 The generator is a pure, deterministic function

```go
// renderDefaultSVG returns the same bytes for the same (seed, initial) every
// time, on every replica. No time, no randomness, no map-iteration order.
// Callers pass renderDefaultSVG(seed, nameForInitial); the first sanitized rune
// of nameForInitial is the glyph (else placeholder), seed picks the colour.
func renderDefaultSVG(seed, initial string) []byte
```

- **Background colour** = `palette[ stableHash(seed) % len(palette) ]` using a
  fixed hash (e.g. FNV-1a). Same `seed` → same colour, forever, everywhere.
- **Initial** = the first display glyph; CJK names render via the client's
  system fonts (SVG `<text>`), so **no embedded font and zero new dependencies**.
- **Injection-safe (mandatory).** Only `initial` reaches the output; `seed` is
  hash-only and never rendered. `initial` is reduced to the **first rune**, then
  allowlisted to a letter/digit (Unicode `L*`/`Nd`, incl. CJK; uppercased when
  cased); anything else (punctuation, symbol, control, combining/bidi/zero-width,
  or empty) falls back to a single placeholder **`?`**. The chosen glyph is then
  run through `html.EscapeString` before embedding (defense-in-depth — the
  allowlist already excludes `<>&"'`). The output is served as `image/svg+xml`,
  so an unescaped `<`/`>` would be the **same stored-XSS** we reject uploads for
  (§7a.1); responses also carry `nosniff` + CSP `default-src 'none'` (§7a.1).
- `Content-Type: image/svg+xml`.

**Seed / initial sources:**

| Kind | `seed` (colour) | `initial` source |
|------|-----------------|------------------|
| room | `roomID` | `room.Name` (subscription, §7); if unknown-here → `roomID` |
| user | `localPart` | `localPart` (read path fetches no display name) |
| bot | `localPart` (`.bot` account) | `localPart` |

### 8.2 Caching a generated default

Because the output is deterministic, the default is still cacheable:

- `ETag` = `"<templateVersion>-<hex(stableHash(seed + sanitizedInitial))>"` —
  over the **sanitized** glyph (§8.1), so names that render to the same
  colour+glyph share an ETag; identical across replicas/requests, so
  `If-None-Match` → `304` works.
- `templateVersion` is a build-time constant; bump it when the SVG template
  changes so existing caches re-fetch.
- `Cache-Control: public, max-age=<cfg>` as usual.

### 8.3 What this removes vs. a stored default

No write-back path, no `singleflight`, no embedded static asset, no
generation/storage consistency concerns. The fallback is stateless and
self-healing.

### 8.4 MinIO's role (custom uploads only)

MinIO holds **only** custom/uploaded bot/room images (§7a). Generated defaults
are never stored, so there is no lazy "generate-and-store" and no pre-warm hook
— a deterministic default needs no warming. A shared `renderDefaultSVG` keeps
the rendering in one place.

## 9. Resolved decisions & open items

**Resolved:**
- **Write scope** → **bot uploads only** in v1; no room/user upload, no
  `DELETE`/reset (§1).
- **`avatars` collection** → media-service-owned; **doc presence = has custom
  image**; authoritative existence source for room + bot; migration writes room
  docs, media-service writes bot docs (§4.4).
- **Unified read model** → resolve owning `siteID` → cross-cluster redirect →
  local `avatars`-doc lookup → MinIO stream or dynamic default (§6, §7).
- **Bot owning site** → from `User.SiteID` via `store.BotSite` (bots are users,
  synced everywhere, so a remote bot's site is resolvable locally) (§5).
- **Room owning site + type** → from the `subscriptions` collection, not `rooms`
  (§7).
- **`?siteid=` fast path** → a frontend-supplied owning site skips the
  site-resolution query for room/bot reads; trade-off documented (§5, §7).
- **`App.AvatarURL` removed** → every avatar is served through this GET endpoint;
  no redirect to an app-provided URL (§6).
- **Upload formats** → PNG/JPEG only; SVG uploads rejected (§7a.1).
- **`avatars` field schema** → finalized: `_id = subjectType:subjectId`,
  `subjectType`/`subjectId`/`minioKey`/`contentType`/`size`/`etag`/`createdAt`/
  `updatedAt`; no `siteId`/`version`; migration maps the legacy collection (§4.4).

- **Default-SVG injection (S1)** → first rune + letter/digit allowlist (else
  placeholder `?`) + `html.EscapeString`, plus `nosniff` + CSP `default-src
  'none'` on responses (§8.1, §7a.1).

**Deferred (post-v1, decided):**
- **🔴 Bot-upload authentication/authorization:** removed in v1 — the endpoint is
  **OPEN** (§7a.4). Deferred until the model is decided (OIDC + platform-admin /
  internal service token / per-bot owner). **Must be gated before production**;
  network-restrict the endpoint until then.
- **OTel tracing + Prometheus `/metrics`:** deferred to post-v1. v1 ships
  auth-service-parity logging (slog + request-id + access-log, §2). The infra is
  ready to copy (`pkg/otelutil`; search-service's promauto + separate `/metrics`
  listener) and purely additive; v1 preserves the seams (ctx everywhere + a typed
  read-outcome in the access log → future `resolution_total{kind,outcome}`).
- **WebP support:** deferred — needs `golang.org/x/image` (new dep, ask first).
- **Re-encode-to-normalize uploads** (strip EXIF / extra polyglot defense):
  deferred — v1 stores original bytes (§7a.1).
- **`?v` cache-busting:** not in v1 — no `version` is stored (§4.4) and the
  frontend's room/bot metadata carries none to append; v1 relies on `ETag`
  revalidation (§4.3). Revisit if/when version propagation to the frontend is
  designed.

**Not yet considered (tracked):**
- **Public-GET abuse** (rate limiting / negative caching / account enumeration) —
  explicitly out of scope for now; revisit before production.

**Accepted residual risks (by design):**
- **Read-path privacy:** the employee-photo redirect `Location` exposes the
  `employeeID` (org-info leakage / account enumeration). **Accepted** — GET is
  public and must be `<img>`-loadable; gating would defeat the redirect design.
- **Employee-photo 404 → frontend default (contract):** for a user who *has* an
  `employeeID` but no actual photo, the external host 404s and we cannot serve our
  own default (we already 307'd; we never fetch the photo). **Accepted**, relying
  on a **frontend contract**: the client renders its own fallback on image-load
  failure (`<img onerror>`, §6). Consequence: that one case shows the *frontend's*
  default rather than our initials SVG — a minor, accepted visual inconsistency;
  our server-side default still covers bots, rooms, and users with no `employeeID`.

## 10. Testing plan (TDD)

- **Handler unit tests** (`handler_test.go`, mocked `avatarStore` + a stream
  seam): table-driven over Endpoint 1 (user local hit/miss, cache hit/miss→DB,
  bot site via `BotSite` local vs cross-cluster, `?siteid=` hint skips `BotSite`,
  `fwd=1` no-re-redirect, bot avatars-doc hit→stream, miss→dynamic default) and
  Endpoint 2 (room resolved via subscription: channel avatars-doc hit→stream,
  miss→dynamic default, dm/botDM→default, remote→307, not-found→default;
  `?siteid=` remote hint→redirect without subscription query, local hint→avatars
  lookup with roomID default; `If-None-Match`→304). Assert status code,
  `Location`, `Cache-Control`, `ETag`, and body bytes/Content-Type.
- **Bot-upload unit tests** (`upload.go`): malformed botName (not `botPattern`)
  → 400; unknown bot (`BotSite` found=false) → 404; bot owned by another site
  (`BotSite` siteID ≠ local) → wrong-cluster error carrying the correct domain;
  accept PNG/JPEG within size; reject oversize (`MAX_UPLOAD_BYTES`), reject
  `image/svg+xml` and non-image bytes, reject decode failures; on success store to
  MinIO + upsert the `avatars` doc; assert `nosniff`. (No auth tests — v1 endpoint
  is open, §7a.4.)
- **Generation unit tests:** `renderDefaultSVG` is **deterministic** — same
  `(seed, initial)` yields byte-identical SVG *and* the same `ETag` across
  repeated calls; stable colour per seed, correct initial (incl. CJK), valid +
  injection-safe XML (escapes hostile names).
- **Integration** (`integration_test.go`, `//go:build integration`): Mongo +
  MinIO from `pkg/testutil`; real GetObject/Stat/stream round-trip for a stored
  custom image, 304 path, and avatars-doc-miss → dynamic default (nothing written
  back).
- Coverage ≥80% (target 90% on handler + generation).

## 11. Docs to update on implementation

- `docs/client-api.md` is NATS/auth-HTTP-scoped; media-service is a new public
  HTTP surface — add a section there (or link this spec) describing the two
  endpoints, redirect semantics, cache headers, and the default-image behaviour.
- Delete any `docs/reviews/*` working notes before opening a PR.
