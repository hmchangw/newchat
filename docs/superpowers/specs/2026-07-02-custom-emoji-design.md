# Custom Emoji Management — Design

**Date:** 2026-07-02
**Status:** Approved
**Service:** media-service (+ `pkg/model`, `pkg/emoji`, `pkg/subject`, `pkg/errcode`)

## 1. Overview

Add the full custom-emoji lifecycle (upload, list, serve image, delete) to media-service. The reaction *validation* side already shipped (issue #382/#383): `pkg/emoji.Validator`, the `custom_emojis` Mongo collection with a unique `(siteId, shortcode)` index, and history-service wiring. Its repo comment reads "Admin CRUD lives elsewhere" — this feature is that CRUD, plus the image-serving path.

**Core semantics (inherited from the shipped validation side):** emoji are site-scoped; the emoji usable in a room are the *room's origin site's* emoji set (react requests route to the room's site and validate against that site's collection). In v1 the FE fetches only the local site's list and never renders non-local shortcodes, so the image URL `GET /api/v1/emoji/{shortcode}` defaults to the serving cluster's site; the optional `?siteid=` query param carries cross-site identity when needed (see Amendment).

Decisions made during brainstorming:

| Topic | Decision |
|---|---|
| Scope | Per-site shared set (Slack workspace model) |
| Upload semantics | `PUT` upsert — same-name upload replaces the image (mirrors avatar upload) |
| Delete permission | Anyone may delete (v1); uploader recorded for audit |
| Formats | PNG / JPEG / GIF (animated GIF supported — stored & streamed verbatim) |
| Limits | Env-configurable: max bytes (default 256 KB), max dimension (default 512) |
| List | Per requested `{siteID}` (clients query the room's origin site) |
| Realtime notify on add/delete | Not in v1 (clients fetch on demand; validator cache TTL bounds staleness) |
| Image URL shape | ~~`siteID` in the path~~ — superseded by the 2026-07-08 Amendment: shortcode-only path + optional lowercase `?siteid=` (defaults to the local site) |

## 2. Data model

Extend the existing `pkg/model.CustomEmoji` (additive only; the validator's existence-check reader is unaffected):

```go
type CustomEmoji struct {
    ID        string `json:"id"        bson:"_id"`       // existing; set to "{siteID}:{shortcode}" (deterministic, upsert-friendly)
    SiteID    string `json:"siteId"    bson:"siteId"`     // existing
    Shortcode string `json:"shortcode" bson:"shortcode"`  // existing; bare, no colons
    ImageURL  string `json:"imageUrl"  bson:"imageUrl"`   // existing; set to "/api/v1/emoji/{shortcode}?siteid={siteID}" (relative, self-describing)
    CreatedBy string `json:"createdBy" bson:"createdBy"`  // existing; first uploader, preserved on overwrite
    CreatedAt int64  `json:"createdAt" bson:"createdAt"`  // existing; epoch ms, first upload
    // new:
    UpdatedBy   string `json:"updatedBy"   bson:"updatedBy"`   // last uploader (audit)
    UpdatedAt   int64  `json:"updatedAt"   bson:"updatedAt"`   // epoch ms
    MinioKey    string `json:"minioKey"    bson:"minioKey"`    // "emoji/{siteID}/{shortcode}" (site-scoped: shortcodes are only unique per site)
    ContentType string `json:"contentType" bson:"contentType"` // image/png | image/jpeg | image/gif
    Size        int64  `json:"size"        bson:"size"`
    ETag        string `json:"etag"        bson:"etag"`        // MinIO ETag; HTTP cache validator + FE cache-buster
}
```

Blobs share the avatar bucket (`MINIO_BUCKET`) under the `emoji/` key prefix. Invariant, mirrored from avatar upload: **doc exists ⟺ object exists** — write MinIO first, then upsert the doc. Delete is the reverse: delete the doc first, then the object (object-delete failure is logged only; a doc-less object is invisible and harmless).

## 3. API surface

### 3a. Upload — `PUT {mediaBaseURL}/api/v1/emoji/{shortcode}` (REST)

- Upsert semantics; raw image bytes as the request body (same as avatar upload).
- Always writes to this cluster's site (`SITE_ID` env); there is no siteID in the path (see Amendment).
- Validation chain, in order:
  1. Shortcode matches `^[a-z0-9_+-]{1,32}$` after NFC normalisation (identical to `pkg/emoji.Validator`).
  2. Shortcode is not a built-in standard emoji — new exported `pkg/emoji.IsStandard(shortcode) bool`; a colliding custom emoji would be permanently shadowed by the validator, i.e. dead data. Reject with reason `EmojiShortcodeReserved`.
  3. `http.MaxBytesReader` capped at `EMOJI_MAX_UPLOAD_BYTES`.
  4. Decode-validate as PNG/JPEG/GIF (blank import `image/gif` in addition to avatar's png/jpeg).
  5. Width and height ≤ `EMOJI_MAX_DIMENSION`.
- Uploader identity: media-service REST is unauthenticated in v1 (same as avatar's `v1: no auth`). The uploader account arrives via `?uploader={account}` — documented as unauthenticated, audit-only.
- `200` response: `{shortcode, etag, contentType, size, updatedAt}`.

### 3b. Serve image — `GET {mediaBaseURL}/api/v1/emoji/{shortcode}?siteid=…` (REST)

- `?siteid=` absent or equal to the local site → doc lookup → miss = `404` (no generated default, unlike avatar) → hit = `ETag`/`If-None-Match` 304 short-circuit, else stream from MinIO with the avatar cache headers (`Cache-Control: public, max-age=…`, `X-Content-Type-Options: nosniff`, CSP `default-src 'none'`).
- `?siteid=` names a known remote site → `307` to `{CLUSTER_DOMAINS[siteid]}/api/v1/emoji/{shortcode}` (redirect target omits the param, so the owning cluster resolves locally — no redirect loop is possible). Unknown siteid → `404`.

### 3c. List — NATS request-reply `chat.user.{account}.request.emoji.{siteID}.list`

- Subject follows the existing non-room convention (cf. `…request.search.{siteID}.messages`); each site's media-service subscribes with its own siteID, so the supercluster routes the request to the target site. Builder + pattern go in `pkg/subject`.
- Response: `{"emojis": [{shortcode, imageUrl, contentType, etag, createdBy, updatedAt}]}`, sorted by shortcode; empty set → `[]`.
- Mongo find uses an explicit projection of exactly those fields.

### 3d. Delete — NATS request-reply `chat.user.{account}.request.emoji.{siteID}.delete`

- Request: `{"shortcode": "..."}`. Anyone may delete (v1). Missing doc → `not_found`.
- Caller identity comes from the JWT-enforced `{account}` subject token (authenticated, unlike REST).
- Response: `{"shortcode": "...", "deleted": true}`.
- Gated by a kill-switch (see Amendment): `EMOJI_DELETE_ENABLED=false` (default) → `forbidden`/`emoji_delete_disabled`, mirroring history-service's `PIN_ENABLED` precedent.

## 4. media-service architecture changes

Today media-service is a pure Gin HTTP service. It becomes **HTTP + NATS**:

- `main.go`: connect NATS (log-and-exit on failure, no startup retry); register list/delete on a `natsrouter.Router` (queue group `media-service`; request-id middleware comes free).
- Graceful shutdown (HTTP-service ordering per CLAUDE.md): `nc.Drain()` → `srv.Shutdown()` → disconnect Mongo/MinIO.
- Errors: Tier 1 named constructors in handlers; natsrouter marshals the envelope automatically; HTTP side uses `errhttp.Write`.
- `store.go` gains: `CustomEmoji(ctx, siteID, shortcode)`, `ListCustomEmojis(ctx, siteID)`, `UpsertCustomEmoji(ctx, *model.CustomEmoji)`, `DeleteCustomEmoji(ctx, siteID, shortcode)`; regenerate mocks via `make generate`.
- New config (env, `caarlos0/env`): `NATS_URL` (required), `EMOJI_MAX_UPLOAD_BYTES` (default `262144`), `EMOJI_MAX_DIMENSION` (default `512`).
- `deploy/docker-compose.yml` gains the NATS dependency.

## 5. Consistency & caching

history-service validates through `pkg/emoji.CachedLookup` (LRU+TTL, default 60 s, negative results cached, no active invalidation — its documented contract is "admin add/delete becomes visible at most TTL after the change"). Consequences, accepted for v1: a new emoji is reactable at most 60 s after upload; a deleted emoji stays reactable for at most 60 s. Image responses use `CACHE_MAX_AGE_SECONDS`; the FE cache-busts with `?v={etag}`.

## 6. Error reasons

New `pkg/errcode/codes_emoji.go`:

- `EmojiShortcodeReserved` — shortcode collides with a built-in standard emoji.
- `EmojiDeleteDisabled` — `emoji.delete` kill-switch is off (`EMOJI_DELETE_ENABLED=false`, the default; see Amendment).
- (`EmojiWrongCluster` existed pre-Amendment and was retired with the wrong-cluster check.)

Everything else uses plain named constructors (`BadRequest`, `NotFound`) without a reason.

## 7. Testing (TDD, Red-Green-Refactor throughout)

- **Unit (mocked store/blobs, table-driven):** upload validation matrix (bad regex / reserved name / oversize bytes / oversize dimensions / non-image / animated GIF passes / store & blob errors); GET (local hit incl. explicit `?siteid=local`, miss 404, 304, cross-site 307, unknown site 404); list (populated, empty, store error); delete (existing, missing, store error).
- **Integration (testcontainers: `testutil.MongoDB` + `testutil.MinIO` + `testutil.NATS`):** upload→list→get→delete end-to-end incl. NATS request-reply; doc⟺object invariant; unique-index compatibility with the pre-existing collection; `TestMain` via `testutil.RunTests`.
- **pkg:** `pkg/emoji.IsStandard` unit tests; `CustomEmoji` round-trip in `pkg/model/model_test.go`.
- Coverage: ≥ 80 % floor, 90 %+ target on handlers.

## 8. Documentation

`docs/client-api.md` updated in the same PR (mandatory — list/delete are `chat.user.…` client-facing handlers): new emoji section covering all four endpoints with field tables, JSON success examples, error tables, and the cross-site model (room's-site semantics, optional `?siteid=` hint, 307 behaviour).

## Out of scope (v1)

- Auth on the REST surface (tracked with avatar's existing `v1: no auth` stance).
- Realtime emoji-set change events; active validator-cache invalidation.
- Reacting with the *reactor's* site's emoji in remote rooms (Slack-style) — requires per-reaction site tagging in the Cassandra model; separate project.
- WebP / APNG; server-side resizing.

## Amendment (2026-07-08): REST path shape

Product decision for v1: the FE only ever fetches the *local* site's emoji list and never renders non-local shortcodes, so the REST surface no longer needs `siteID` as a required path segment:

- `siteID` is removed from both REST paths — `PUT /api/v1/emoji/{shortcode}` always writes to this cluster's site (the wrong-cluster declared-intent check is retired, along with `EmojiWrongCluster`); `GET /api/v1/emoji/{shortcode}` takes an optional lowercase `?siteid=` query param (matching avatar's existing hint param) that defaults to local when absent and 307-redirects to a known remote cluster when present (redirect target omits the param, so it resolves locally there — still no loop possible).
- `imageUrl` now carries `?siteid={siteID}` so list entries stay self-describing regardless of which cluster serves them.
- NATS `emoji.list` / `emoji.delete` RPCs, the Mongo doc `_id` (`{siteID}:{shortcode}`), and the MinIO key (`emoji/{siteID}/{shortcode}`) are all unchanged.

## Amendment (2026-07-08): delete kill-switch

`emoji.delete` is now gated behind a new `EMOJI_DELETE_ENABLED` config (bool, default `false` — delete disabled by default), mirroring history-service's `PIN_ENABLED` kill-switch precedent. When disabled, the RPC returns `forbidden` with reason `emoji_delete_disabled` (new `pkg/errcode/codes_emoji.go` entry) before any store/blob access. Local `docker-compose.yml` sets `EMOJI_DELETE_ENABLED=true` for dev convenience. Upload/serve/list are unaffected.

## Amendment (2026-07-09): post-merge review adjustments (PR #457 follow-up)

Three adjustments from post-merge review, applied on `ds-feat/fix_customized_emoji`:

- **Stored `imageUrl` is now the bare path** `/api/v1/emoji/{shortcode}` — no `?siteid=` (supersedes the §2 comment and the 2026-07-08 path-shape amendment's `imageUrl` bullet). The doc's `siteId` field and the site the list was fetched from already identify the owner; the `?siteid=` query param remains available on the GET endpoint for callers that need cross-site resolution. FE cache-busts with `?v={etag}`.
- **`createdBy` dropped from the `emoji.list` wire response** (`EmojiEntry`). The field stays on the Mongo doc for audit; it is simply no longer serialized — no current consumer.
- **Reaction gate simplified to format-only** (supersedes §5 "Consistency & caching"): `pkg/emoji.Validator`, `CachedLookup`, `CustomEmojiLookup`, and history-service's `custom_emojis` existence check are deleted. `msg.react` now validates via `emoji.Canonicalize` alone (256-byte cap → NFC → `^[a-z0-9_+-]{1,32}$`); the `"unknown reaction shortcode"` error is retired. Rationale: the FE picker only offers shortcodes from the standard set plus the local site's `emoji.list`, so registration enforcement added a Mongo dependency + cache to the hot path without changing reachable behaviour. Consequence: upload/delete propagation delays no longer exist (no validator cache); a deleted shortcode remains technically reactable via direct API use — accepted, auditable. `emoji.IsStandard` is retained for the upload reserved-name check.
