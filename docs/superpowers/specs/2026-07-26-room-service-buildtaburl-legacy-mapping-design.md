# room-service: buildTabURL legacy `${roomType}` / `${roomOrigin}` substitution

**Date:** 2026-07-26
**Service:** `room-service`
**Function:** `buildTabURL` (`room-service/handler.go`), called by `getRoomAppTabs`

## Problem

Channel-tab URL templates now carry a full URL (e.g.
`https://template-a.com?roomType=${roomType}&roomOrigin=${roomOrigin}`), but
`buildTabURL` still force-rewrites the template's scheme/host/path-prefix with
`SITE_URL` and only knows the `${roomId}` / `${siteId}` variables. Legacy tab
apps additionally need the room's type expressed in the *old* (pre-redesign)
room-type vocabulary and the legacy origin URL of the room's home site.

## Behavior

`buildTabURL` performs **substitution only** — no base-URL rewrite. The
template is returned with these variables replaced:

| Variable | Replaced with |
|---|---|
| `${roomId}` | request roomID (unchanged behavior) |
| `${siteId}` | local `h.siteID` (unchanged behavior) |
| `${roomType}` | legacy room type mapped from `room.Type` (see table) |
| `${roomOrigin}` | `LegacyRoomOrigins[room.SiteID]`, or `""` on miss |

### Legacy room-type map (hard-coded)

| new `model.RoomType` | legacy value |
|---|---|
| `channel` | `p` |
| `dm` | `d` |
| `botDM` | `d` |
| `discussion` | `p` |
| any other / unknown | `p` (fallback) |

Package-level `var legacyRoomTypes = map[model.RoomType]string{...}` in
`room-service/handler.go`; a lookup miss falls back to `"p"`, so
`${roomType}` always resolves. (Derived from the reverse of
`data-migration/oplog-collections-transformer/classify.go`, with
channel→`p` chosen per product decision.)

### Legacy room-origin map (env-configured)

New config field in `room-service/main.go`:

```go
LegacyRoomOrigins map[string]string `env:"LEGACY_ROOM_ORIGINS" envDefault:""`
```

- Wire format (native `caarlos0/env` v11 map parsing, `SplitN(pair, ":", 2)`
  so URL values keep `://`):
  `LEGACY_ROOM_ORIGINS=site-a:https://legacy.site-a.com,site-b:https://legacy.site-b.com`
- Startup normalization: `strings.TrimSpace` each key and value (tolerates
  `site-a: https://legacy.site-a.com`). Empty var ⇒ empty map (valid: every
  `${roomOrigin}` substitutes to `""`).
- Lookup key is **`room.SiteID`** — the room's origin site, consistent with
  `pkg/drive.GetBaseURLFromRoomOrigin`.
- Miss policy: substitute the **empty string**; the tab is still returned
  (e.g. `...&roomOrigin=`). No skip, no literal placeholder.

### What buildTabURL keeps and drops

Keeps:
- empty template ⇒ `("", false)` (tab skipped, Warn-logged by caller)
- URL-safety check on `roomID` and `h.siteID` (`isURLSafeIDToken`) ⇒ `false`
- substitute **before** `url.Parse` so values aren't percent-encoded
- final check requires an absolute http(s) URL with a host ⇒ malformed,
  relative, or non-http(s) result ⇒ `("", false)`

Drops:
- the `siteURL.JoinPath` scheme/host/path-prefix rewrite — the substituted
  template string is returned as-is (not re-serialized via `url.URL.String()`)
- `Handler.siteURL` field, the `NewHandler` `siteURL` param, and the
  `SITE_URL` env var (removed from config, `main.go` validation, and
  `deploy/docker-compose.yml`). `buildTabURL` was its only consumer;
  deployments still setting the env var are unaffected (ignored).

## Data flow

Signature becomes `buildTabURL(tmpl string, room *model.Room) (string, bool)`.
The handler needs a `roomID` no longer as a param — the room doc carries
`room.ID`.

`authorizeRoomAppRead` authorizes AND returns the room, narrowly projected
via a dedicated `store.GetRoomAppRead` (`_id`, `type`, `siteId`). The room
fetch runs concurrently with the auth checks (separate collections; a single
joined query would need a forbidden `$lookup`):

- `mongo.ErrNoDocuments` ⇒ `errAppAccessDenied` on every path — room
  existence now gates members too, not just the admin bypass
- other error ⇒ `fmt.Errorf("get room for app read: %w", err)` (⇒ `internal`)

Exactly one room read per request on all paths. `getRoomAppCommandMenu`
discards the returned room (`_, err :=`).

`Handler` gains a `legacyRoomOrigins map[string]string` field, injected via
`NewHandler` (replacing the removed `siteURL` param).

### Projection

A new `roomAppReadProjection` (`room-service/store_mongo.go`) backs
`GetRoomAppRead` with only `_id`/`type`/`siteId`; `roomReadProjection`
stays untouched. A drift-guard integration test pins the narrow set.

## Error handling

No new client-facing error cases. Existing `errAppAccessDenied` covers the
room-missing race; infra failures stay raw-wrapped per `pkg/errcode` Tier 1.
No secrets/URLs logged beyond existing Warn lines (appId/roomId/request_id).

## Testing (TDD — red first)

- `TestHandler_buildTabURL` table rewritten:
  - each room type maps correctly (`channel`→`p`, `dm`→`d`, `botDM`→`d`,
    `discussion`→`p`) and unknown type falls back to `p`
  - origin hit substitutes the mapped URL; origin miss substitutes `""`
  - full-URL template's scheme/host are **preserved** (no SITE_URL rewrite)
  - `${roomId}` / `${siteId}` still substituted
  - empty template, malformed template, non-URL-safe roomID/siteID ⇒ `false`
- `getRoomAppTabs` handler tests: add `GetRoom` mock expectations; new cases
  for room-not-found ⇒ access denied and `GetRoom` infra error.
- `main.go` origin-map normalization: unit test for the trim helper.
- Projection integration test: `siteId` added to the guarded field set.

## Docs (same PR)

- `docs/client-api.md` §Get Room App Tabs: `tabUrl` row rewritten — computed
  from the template URL itself (no `SITE_URL` rewrite), documents all four
  variables, legacy type map, and empty-string origin fallback; JSON example
  updated. §App examples showing `channelTab.url.default` updated to the
  full-URL template shape.
- Response *schema* is unchanged, so
  `docs/client-api/request-reply.md` / `events.md` only change if they
  duplicate the `tabUrl` computation prose (verify during implementation).

## Out of scope

- No new placeholder syntax beyond `${...}`.
- No percent-encoding of substituted values (template authors control
  placement; existing behavior).
- `getRoomAppCommandMenu` and all other `authorizeRoomAppRead` consumers
  unchanged.
