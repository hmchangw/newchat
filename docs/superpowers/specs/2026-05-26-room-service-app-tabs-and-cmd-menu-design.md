# Room App Tabs & Command Menu RPCs (room-service)

## Summary

Add two read-only, client-facing NATS request/reply RPCs to `room-service`
that expose per-room app metadata:

- `GetRoomAppTabs` — returns the apps with default channel tabs, each with
  a per-room tab URL produced by substituting `${roomId}` and `${siteId}`
  into the app's URL template and rewriting scheme+host+path-prefix to
  the local `SITE_URL`.
- `GetRoomAppCommandMenu` — for every bot subscribed to the room, returns
  the bot's active command-menu blocks joined with the owning app's name.

Both handlers are gated by the same rule: caller must either be a
member of the room (a `subscriptions` row exists for `(account, roomID)`)
or a platform admin (caller's `users` doc carries `"admin"` in
`Roles`). Admin check is **per-site** — room-service consults only
its local `users` collection (the room's site). An admin whose
`users` doc lives on another site is denied. All data comes from
local Mongo (`subscriptions`, `users`, `apps`, `bot_cmd_menu`);
no external lookups.

## Subjects

| Method | Concrete | Wildcard |
|---|---|---|
| `GetRoomAppTabs` | `chat.user.{account}.request.room.{roomID}.{siteID}.app.tabs` | `chat.user.*.request.room.*.{siteID}.app.tabs` |
| `GetRoomAppCommandMenu` | `chat.user.{account}.request.room.{roomID}.{siteID}.app.cmd-menu` | `chat.user.*.request.room.*.{siteID}.app.cmd-menu` |

Queue group: `room-service` (mirrors every other `room-service` handler).
Subject parsing reuses `subject.ParseUserRoomSubject`, which walks for
the `room` token and works for any room-scoped form regardless of
trailing-token count (the new subjects are 9-token; existing
`message.read`-style subjects are 7-token).

## Wire Format

Both handlers accept an empty body (`{}` is tolerated). Every input is
encoded in the subject; there are no request bodies to validate.

`pkg/model/app.go` (additions):

```go
type RoomApp struct {
    ID        string        `json:"id"`
    Name      string        `json:"name"`        // = apps.channelTab.name
    TabURL    string        `json:"tabUrl"`      // computed (see "URL Rewrite")
    Assistant *AppAssistant `json:"assistant,omitempty"`
    AvatarURL string        `json:"avatarUrl,omitempty"`
}

type GetRoomAppTabsResponse struct {
    Apps []RoomApp `json:"apps"`
}

type RoomAppAssistant struct {
    AppName   string     `json:"appName"`   // = apps.name
    Name      string     `json:"name"`      // = apps.assistant.name (bot account)
    CmdBlocks []CmdBlock `json:"cmdBlocks,omitempty"`
}

type GetRoomAppCommandMenuResponse struct {
    AppAssistants []RoomAppAssistant `json:"appAssistants"`
}
```

Empty result: `{"apps":[]}` / `{"appAssistants":[]}` — always 200 OK, never
an error.

## Subject Builders

`pkg/subject/subject.go` (additions):

```go
func RoomAppTabs(account, roomID, siteID string) string {
    return fmt.Sprintf("chat.user.%s.request.room.%s.%s.app.tabs", account, roomID, siteID)
}

func RoomAppTabsWildcard(siteID string) string {
    return fmt.Sprintf("chat.user.*.request.room.*.%s.app.tabs", siteID)
}

func RoomAppCmdMenu(account, roomID, siteID string) string {
    return fmt.Sprintf("chat.user.%s.request.room.%s.%s.app.cmd-menu", account, roomID, siteID)
}

func RoomAppCmdMenuWildcard(siteID string) string {
    return fmt.Sprintf("chat.user.*.request.room.*.%s.app.cmd-menu", siteID)
}
```

## Model additions

`pkg/model/user.go`:

```go
// Roles carries platform-level role assignments (e.g. "user", "admin").
// Absent on legacy user documents — treat nil as "no platform roles".
Roles []string `json:"roles,omitempty" bson:"roles,omitempty"`

// PlatformRoleAdmin is the platform-admin role string carried in User.Roles.
const PlatformRoleAdmin = "admin"

// IsPlatformAdmin reports whether u holds the platform admin role.
// Returns false for nil receivers and for users without the role.
func IsPlatformAdmin(u *User) bool {
    if u == nil {
        return false
    }
    for _, r := range u.Roles {
        if r == PlatformRoleAdmin {
            return true
        }
    }
    return false
}
```

`pkg/model/subscription.go`:

```go
// IsRoomMember reports whether sub represents an active membership.
// Returns false for nil so callers can pass the result of a store lookup
// that returned (nil, ErrSubscriptionNotFound) — the caller is expected
// to have already classified the error and set sub to nil on not-found.
func IsRoomMember(sub *Subscription) bool {
    return sub != nil
}
```

`pkg/model/app.go`:

```go
type App struct {
    ID          string         `json:"id"                    bson:"_id"`
    Name        string         `json:"name"                  bson:"name"`
    Description string         `json:"description,omitempty" bson:"description,omitempty"`
    AvatarURL   string         `json:"avatarUrl,omitempty"   bson:"avatarUrl,omitempty"`  // NEW
    Assistant   *AppAssistant  `json:"assistant,omitempty"   bson:"assistant,omitempty"`
    ChannelTab  *AppChannelTab `json:"channelTab,omitempty"  bson:"channelTab,omitempty"`  // NEW
    Sponsors    []AppSponsor   `json:"sponsors,omitempty"    bson:"sponsors,omitempty"`
}

type AppChannelTab struct {
    Enabled bool             `json:"enabled" bson:"enabled"`
    Default bool             `json:"default" bson:"default"`
    Name    string           `json:"name"    bson:"name"`
    URL     AppChannelTabURL `json:"url"     bson:"url"`
}

type AppChannelTabURL struct {
    Default string `json:"default" bson:"default"`
}
```

`pkg/model/botcmdmenu.go` (new file):

```go
package model

// BotCmdMenu is a row in the bot_cmd_menu collection. Name matches an
// AppAssistant.Name (the bot account) and joins back to the owning App via
// that field. ActiveStatus gates whether the menu is currently exposed.
type BotCmdMenu struct {
    ID           string     `json:"id"           bson:"_id"`
    Name         string     `json:"name"         bson:"name"`
    ActiveStatus bool       `json:"activeStatus" bson:"activeStatus"`
    CmdBlocks    []CmdBlock `json:"cmdBlocks,omitempty" bson:"cmdBlocks,omitempty"`
}

// CmdBlock is the recursive building block of a bot command menu.
type CmdBlock struct {
    Text        string     `json:"text,omitempty"        bson:"text,omitempty"`
    ActionType  string     `json:"actionType,omitempty"  bson:"actionType,omitempty"`
    Description string     `json:"description,omitempty" bson:"description,omitempty"`
    Payload     string     `json:"payload,omitempty"     bson:"payload,omitempty"`
    Modal       *CmdModal  `json:"modal,omitempty"       bson:"modal,omitempty"`
    Blocks      []CmdBlock `json:"blocks,omitempty"      bson:"blocks,omitempty"`
}

// CmdModal carries the slash-style command + param a modal-triggering
// block invokes. CmdModal does NOT nest its own blocks — recursive
// rendering happens via CmdBlock.Blocks on the enclosing CmdBlock.
type CmdModal struct {
    Command string `json:"command,omitempty" bson:"command,omitempty"`
    Param   string `json:"param,omitempty"   bson:"param,omitempty"`
}
```

## Authorization helper

`room-service/handler.go`:

```go
func (h *Handler) authorizeRoomAppRead(ctx context.Context, account, roomID string) error {
    sub, err := h.store.GetSubscription(ctx, account, roomID)
    if err != nil && !errors.Is(err, model.ErrSubscriptionNotFound) {
        return fmt.Errorf("check room membership: %w", err)
    }
    if model.IsRoomMember(sub) {
        return nil
    }
    user, err := h.store.GetUser(ctx, account)
    if err != nil && !errors.Is(err, ErrUserNotFound) {
        return fmt.Errorf("check platform admin: %w", err)
    }
    if !model.IsPlatformAdmin(user) {
        return errAppAccessDenied
    }
    // Admin bypass: verify the room exists, else fabricated room IDs
    // would receive plausible-looking responses.
    if _, err := h.store.GetRoom(ctx, roomID); err != nil {
        if errors.Is(err, mongo.ErrNoDocuments) {
            return errAppAccessDenied
        }
        return fmt.Errorf("check room existence: %w", err)
    }
    return nil
}
```

Used unchanged by both handlers. Subscription check first (common
case); admin fallback second AND requires the room to exist. Local-site
Mongo only — see Summary on cross-site admin scope.

## Handler flow

Both handlers start with `ctx, cancel := context.WithTimeout(ctx, 5*time.Second); defer cancel()`
(mirrors `handleRoomsInfoBatch`) and set OTel attributes
`room.id`, `site.id`, `account` on the active span (mirrors
`handleCreateRoom`). `handleGetRoomAppCommandMenu` also records
`bot.count` after step 3. `siteID` for URL substitution and tracing
comes from `h.siteID` (the wildcard subscription binds the subject's
`{siteID}` to the service's own site).

`handleGetRoomAppTabs`:

1. `subject.ParseUserRoomSubject(subj)` → `(account, roomID)`.
2. `authorizeRoomAppRead(ctx, account, roomID)`.
3. `apps, err := h.store.ListDefaultChannelTabApps(ctx)`.
4. Initialize `out := make([]model.RoomApp, 0, len(apps))` so an empty
   result marshals to `[]` not `null`.
5. For each `app`, build `tabUrl` via the URL Rewrite rules below; if the
   rewrite fails (empty/unparseable URL), `slog.Warn` and skip the entry.
6. Reply `GetRoomAppTabsResponse{Apps: out}`.

DM/botDM rooms return empty by construction (no `GetRoom` pre-filter).

`handleGetRoomAppCommandMenu`:

1. `subject.ParseUserRoomSubject(subj)` → `(account, roomID)`.
2. `authorizeRoomAppRead(ctx, account, roomID)`.
3. `bots, err := h.store.ListRoomBotApps(ctx, roomID)`. If empty, reply
   `{"appAssistants":[]}` (with the slice initialized as
   `make([]model.RoomAppAssistant, 0)`) without touching `bot_cmd_menu`.
4. Collect the distinct `assistantName` values; call
   `h.store.ListActiveCmdMenus(ctx, assistantNames)`.
5. Build a `name → CmdBlocks` map from the returned menus
   (`bot_cmd_menu` invariant: at most one active row per `name`; see
   Stores). Initialize `out := make([]model.RoomAppAssistant, 0, len(bots))`;
   for each `(appName, assistantName)` from step 3, emit a
   `RoomAppAssistant` with `CmdBlocks` from the map (nil if absent).
6. Reply `GetRoomAppCommandMenuResponse{AppAssistants: out}`.

`natsGetRoomAppTabs` / `natsGetRoomAppCommandMenu` follow the standard
`room-service` wrapper, with one difference: success replies go through
the `replyBoundedJSON` helper (see "Response Payload Cap") so an
oversized payload surfaces as a clean error envelope rather than a
NATS-level `ErrMaxPayload`:

```go
func (h *Handler) natsGetRoomAppTabs(m otelnats.Msg) {
    ctx := wrappedCtx(m)
    resp, err := h.handleGetRoomAppTabs(ctx, m.Msg.Subject, m.Msg.Data)
    if err != nil {
        slog.Error("get room app tabs failed", "error", err, "subject", m.Msg.Subject)
        natsutil.ReplyError(m.Msg, sanitizeError(err))
        return
    }
    h.replyBoundedJSON(m.Msg, resp)
}
```

Both register via `RegisterCRUD` with the standard queue group:

```go
if _, err := nc.QueueSubscribe(subject.RoomAppTabsWildcard(h.siteID), queue, h.natsGetRoomAppTabs); err != nil {
    return fmt.Errorf("subscribe app tabs: %w", err)
}
if _, err := nc.QueueSubscribe(subject.RoomAppCmdMenuWildcard(h.siteID), queue, h.natsGetRoomAppCommandMenu); err != nil {
    return fmt.Errorf("subscribe app cmd-menu: %w", err)
}
```

## Errors

> **Implementation note — rebased onto `main` after #250 "centralized error
> codes".** `room-service` no longer uses `sanitizeError` + `natsutil.ReplyError`
> (both removed by #250). The sentinels below are now typed `*errcode.Error` and
> replies go through `errnats.Reply`, which classifies + logs once:
> `errAppAccessDenied = errcode.Forbidden("not authorized to access this room's apps", errcode.WithReason(errcode.RoomNotMember))`
> and `errResponseTooLarge = errcode.Internal("response payload exceeds maximum size")`.
> Invalid-subject parse failures return `errcode.BadRequest("invalid request")`.
> The original `errors.New` + allow-list design below is kept for historical context.

Add to `room-service/helper.go` (sentinels):

```go
errAppAccessDenied   = errors.New("not authorized to access this room's apps")
errResponseTooLarge  = errors.New("response payload exceeds maximum size")
```

Add both to the `sanitizeError` switch's allow-list so the literal
sentinel message is surfaced verbatim to clients (no leakage of
internal context). No other new sentinels — the URL-rewrite skip is
silent (warn-only), and empty results return 200 OK with an empty
array.

## Response Payload Cap

> **Implementation note (post-#250).** `marshalBounded` now returns
> `([]byte, error)` — `errResponseTooLarge` (an `*errcode.Error`) on overflow, a
> wrapped marshal error otherwise — and `replyBoundedJSON(ctx, msg, v)` sends the
> error via `errnats.Reply`. The `([]byte, string)` + `natsutil.ReplyError` design
> below is historical.

Replies larger than the negotiated `max_payload` (ops runs at 64KB)
would otherwise be dropped silently by NATS. The handler short-circuits
with `errResponseTooLarge` instead. Pure marshaling + bound check lives
in a testable helper:

```go
// room-service/helper.go

// marshalBounded marshals v and enforces h.maxResponseBytes (<= 0
// disables the bound). Returns (body, "") or (nil, errMsg).
func (h *Handler) marshalBounded(v any) ([]byte, string) {
    body, err := json.Marshal(v)
    if err != nil {
        slog.Error("marshal response failed", "error", err)
        return nil, "internal error"
    }
    if h.maxResponseBytes > 0 && int64(len(body)) > h.maxResponseBytes {
        slog.Error("response exceeds max payload",
            "size", len(body), "limit", h.maxResponseBytes)
        return nil, errResponseTooLarge.Error()
    }
    return body, ""
}

func (h *Handler) replyBoundedJSON(msg *nats.Msg, v any) {
    body, errMsg := h.marshalBounded(v)
    if errMsg != "" {
        natsutil.ReplyError(msg, errMsg)
        return
    }
    if err := msg.Respond(body); err != nil {
        slog.Error("reply failed", "error", err)
    }
}
```

`maxResponseBytes` is sourced from `nc.MaxPayload()` after connect (in
`main.go`) and passed into `NewHandler`. Applied to both handlers so
`app.tabs` shares the same guard.

## Stores

`room-service/store.go`:

```go
// RoomBotAppEntry pairs an assistant's bot account with its owning app
// name — the joined output of ListRoomBotApps.
type RoomBotAppEntry struct {
    AssistantName string `bson:"assistantName"`
    AppName       string `bson:"appName"`
}

// ListDefaultChannelTabApps returns apps whose channelTab.enabled AND
// channelTab.default are both true, sorted by channelTab.name asc.
// Projection: _id, avatarUrl, assistant, channelTab (app.name is
// excluded; the response uses channelTab.name). Empty result is
// ([], nil).
ListDefaultChannelTabApps(ctx context.Context) ([]model.App, error)

// ListRoomBotApps runs a single aggregation against `subscriptions` that
// $matches { roomId, "u.isBot": true }, $lookups `apps` where
// assistant.enabled AND assistant.name == "$u.account", $unwinds the
// joined app, and projects { assistantName: "$app.assistant.name",
// appName: "$app.name" }. Empty result is ([], nil).
ListRoomBotApps(ctx context.Context, roomID string) ([]RoomBotAppEntry, error)

// ListActiveCmdMenus returns bot_cmd_menu documents where activeStatus
// is true AND name IN assistantNames, sorted by name asc. Returns
// ([], nil) when assistantNames is empty (skips the query entirely).
ListActiveCmdMenus(ctx context.Context, assistantNames []string) ([]model.BotCmdMenu, error)
```

`room-service/store_mongo.go`:

- `MongoStore` gains one new collection handle: `botCmdMenus = db.Collection("bot_cmd_menu")`.
- `ListDefaultChannelTabApps` — `Find` with filter
  `{"channelTab.enabled": true, "channelTab.default": true}`, sort
  `{"channelTab.name": 1}`, projection
  `{_id:1, avatarUrl:1, assistant:1, channelTab:1}` (`name` excluded —
  the response uses `channelTab.name`, not `app.name`).
- `ListRoomBotApps` — `Aggregate` on `subscriptions` with the pipeline.
  The trailing `$sort` on `assistantName` makes the response order
  deterministic for tests and clients:
  ```json
  [ {"$match": {"roomId": roomID, "u.isBot": true}},
    {"$lookup": {
        "from": "apps",
        "let":  {"acct": "$u.account"},
        "pipeline": [
          {"$match": {"$expr": {"$and": [
            {"$eq": ["$assistant.enabled", true]},
            {"$eq": ["$assistant.name", "$$acct"]} ]}}},
          {"$project": {"_id": 0,
                        "assistantName": "$assistant.name",
                        "appName":       "$name"}} ],
        "as": "app"}},
    {"$unwind": "$app"},
    {"$replaceRoot": {"newRoot": "$app"}},
    {"$sort": {"assistantName": 1}} ]
  ```
- `ListActiveCmdMenus` — `Find` with filter
  `{"activeStatus": true, "name": {"$in": assistantNames}}`, sort
  `{"name": 1}`, projection `{_id:0, name:1, cmdBlocks:1}`. Upstream
  invariant: at most one active row per `name` (recommend a partial
  unique index `{name:1} partialFilterExpression:{activeStatus:true}`
  on the writer side — out of scope here). On violation, the sort
  makes last-write-wins deterministic.

`EnsureIndexes` additions (best-effort, idempotent):

- `apps`: `{channelTab.default: 1, channelTab.enabled: 1, channelTab.name: 1}`
  (compound; serves `ListDefaultChannelTabApps` filter **and** sort
  from a single index scan).
- `subscriptions`: `{roomId: 1, "u.isBot": 1}` (compound; serves the
  `$match` stage of `ListRoomBotApps` directly, instead of riding the
  existing `(roomId, u.account)` index on the `roomId` prefix and
  filtering `u.isBot` in memory).
- `bot_cmd_menu`: `{activeStatus: 1, name: 1}` (compound, ESR order —
  activeStatus is the equality match, name is the `$in` selector;
  serves `ListActiveCmdMenus`).

`assistant.name` already has an index in `EnsureIndexes` (added for
botDM creation) and is reused by the `$lookup` step in `ListRoomBotApps`.

`mock_store_test.go` regenerated via `make generate SERVICE=room-service`.

## URL Rewrite

For each app returned by `ListDefaultChannelTabApps`:

1. If `app.ChannelTab == nil || app.ChannelTab.URL.Default == ""`: skip + warn.
2. Substitute placeholders **before** parsing (otherwise `url.URL.String()`
   percent-encodes the substituted values):
   ```go
   tmpl = strings.ReplaceAll(app.ChannelTab.URL.Default, "${roomId}", roomID)
   tmpl = strings.ReplaceAll(tmpl, "${siteId}", h.siteID)
   ```
   `roomID` / `siteID` are URL-safe by construction (subject parsing
   rejects NATS wildcards; ID generators produce base62/UUIDv7-hex).
3. `u, err := url.Parse(tmpl)`. On error: skip + warn.
4. Merge with the configured `SITE_URL`, preserving its path prefix:
   ```go
   joined := h.siteURL.JoinPath(u.Path)
   joined.User     = nil           // strip any userinfo
   joined.RawQuery = u.RawQuery
   joined.Fragment = u.Fragment
   ```
   `JoinPath` carries `Scheme`/`Host` from `h.siteURL` and collapses
   duplicate slashes. Template's `Scheme`/`Host` are discarded.
5. `tabUrl := joined.String()`.

`h.siteURL` is parsed once at construction from `SITE_URL`; `main.go`
exits non-zero if `Scheme` or `Host` is empty. `siteURL.Path` (e.g.
`/chat`) is preserved as a prefix on every generated `tabUrl`.

## Wiring

`room-service/main.go`:

```go
type config struct {
    // ...existing fields...
    SiteURL string `env:"SITE_URL,required"`
}
```

Startup validation (before constructing the handler):

```go
siteURL, err := url.Parse(cfg.SiteURL)
if err != nil || siteURL.Scheme == "" || siteURL.Host == "" {
    slog.Error("invalid SITE_URL: must be an absolute URL with scheme and host",
        "value", cfg.SiteURL, "error", err)
    os.Exit(1)
}
```

`siteURL.Path` is allowed and preserved (see URL Rewrite). Trailing
slashes are normalized by `JoinPath`.

`Handler` gains two fields, threaded through `NewHandler`:

```go
siteURL          *url.URL  // pre-parsed SITE_URL
maxResponseBytes int64     // = nc.MaxPayload(), set in main.go after natsutil.Connect
```

Tests exercising the new handlers MUST set `siteURL` (otherwise
`JoinPath` panics on nil); `maxResponseBytes` is optional and defaults
to 0 (cap disabled).

## Local dev

`room-service/deploy/docker-compose.yml`: add `SITE_URL=http://localhost:3000`
(or whatever absolute URL — including optional path prefix — the local
frontend runs behind) to the `room-service` service environment.

No Dockerfile change. No CI change (env-only).

## Client API Doc

Per CLAUDE.md §5, update `docs/client-api.md` in the same PR. Add two
entries under §3.1 (room-service) following the existing `member.list`
format: subject, request body shape, success response shape, error
envelope, and `Triggered events` section ("None — reply only" for both).

## Testing (TDD)

### Subject builders (`pkg/subject/subject_test.go`)

- `TestRoomAppTabs` / `TestRoomAppTabsWildcard` — exact-string asserts.
- `TestRoomAppCmdMenu` / `TestRoomAppCmdMenuWildcard` — exact-string asserts.
- `TestRoomAppTabs_ParseUserRoomSubject` — round-trip parse via the
  shared parser.
- `TestRoomAppCmdMenu_ParseUserRoomSubject` — round-trip parse.

### Model round-trips (`pkg/model/model_test.go`)

- `TestUserJSON_WithRoles` + omitempty check (nil/empty Roles must NOT
  appear in JSON output).
- `TestAppRoundtrip_WithChannelTabAndAvatar` — App with ChannelTab and
  AvatarURL populated.
- `TestAppChannelTabRoundtrip` — standalone round-trip.
- `TestBotCmdMenuRoundtrip` — with active and inactive menus.
- `TestCmdBlockRoundtrip_Recursive` — block with `Modal` set, plus nested
  `Blocks` on the enclosing block (recursion lives on `CmdBlock`, not
  `CmdModal`).
- `TestGetRoomAppTabsResponseRoundtrip` and
  `TestGetRoomAppCommandMenuResponseRoundtrip` — full wrapper structs.
- `TestIsPlatformAdmin` — nil receiver false; empty roles false;
  "admin" present true; "admin" absent false.
- `TestIsRoomMember` — nil false; non-nil true.

### Handler unit tests (`room-service/handler_test.go`)

`TestHandler_handleGetRoomAppTabs` table-driven:

| Case | Setup | Expected outcome |
|------|-------|------------------|
| member allowed | `GetSubscription` returns sub | 200, apps from store, URL rewrite applied |
| admin allowed | `GetSubscription` → `ErrSubscriptionNotFound`; `GetUser` returns user with `Roles=["admin"]` | 200, apps returned |
| denied | `GetSubscription` → not-found; `GetUser` returns user without admin role | `errAppAccessDenied` |
| denied (no user) | `GetSubscription` → not-found; `GetUser` → `ErrUserNotFound` | `errAppAccessDenied` (covers the cross-site-admin path: their `users` doc is absent on this site) |
| empty result | `ListDefaultChannelTabApps` returns `nil` | `{"apps":[]}` (verified by JSON-encoding the response and asserting on the literal `[]`, not `null`) |
| URL rewrite happy path | app with `https://upstream.example.com/app/${roomId}/${siteId}/index`, `SITE_URL=https://chat.example.com` | `https://chat.example.com/app/<roomID>/<siteID>/index` |
| URL rewrite preserves SITE_URL path prefix | `SITE_URL=https://chat.example.com/chat`, template path `/tab/${roomId}` | `https://chat.example.com/chat/tab/<roomID>` |
| URL rewrite strips userinfo | template `https://user:pass@upstream/path/${roomId}` | `<SITE_URL>/path/<roomID>` — no userinfo in result |
| URL rewrite preserves query and fragment | template `https://upstream/path?room=${roomId}#tab=${siteId}` | `<SITE_URL>/path?room=<roomID>#tab=<siteID>` |
| URL rewrite multiple apps sorted | store returns 3 apps in `channelTab.name asc` | order preserved, all URLs rewritten |
| URL rewrite empty url | mixed input including `ChannelTab.URL.Default == ""` | bad entry skipped, others emitted |
| URL rewrite malformed url | mixed input including `"://bad"` | bad entry skipped, others emitted |
| invalid subject | subj missing `room` token | parse error |
| sub-check transient error | `GetSubscription` returns a non-sentinel error | wrapped error |
| user-lookup transient error | `GetUser` returns a non-sentinel error | wrapped error |
| store list error | `ListDefaultChannelTabApps` errors | wrapped error |
| context timeout propagation | mock reads `ctx.Done()` and returns `ctx.Err()`; test passes a pre-cancelled parent | wrapped context error (`Canceled` or `DeadlineExceeded`) |

`TestHandler_handleGetRoomAppCommandMenu` table-driven:

| Case | Setup | Expected outcome |
|------|-------|------------------|
| member allowed, no bots | `ListRoomBotApps` returns `[]` | `{"appAssistants":[]}` (literal `[]`, not `null`), `ListActiveCmdMenus` NOT called |
| member allowed, bots with active menus | bots `[{a, A}, {b, B}]`, menus `[{name:a,blocks:[...]},{name:b,blocks:[...]}]` | both entries with their blocks; order matches the store's `assistantName asc` |
| member allowed, bot with no active menu | bots `[{a, A}]`, menus `[]` | one entry with nil cmdBlocks |
| admin allowed | sub not-found + admin user | same as member path |
| denied | sub not-found + non-admin user | `errAppAccessDenied` |
| invalid subject | malformed `subj` | parse error |
| store list error (bots) | `ListRoomBotApps` errors | wrapped error |
| store list error (menus) | `ListActiveCmdMenus` errors | wrapped error |
| context timeout propagation | mock reads `ctx.Done()`; pre-cancelled parent | wrapped context error |

### Response cap (`room-service/handler_test.go`)

`TestHandler_marshalBounded` table-driven, no NATS connection
required — exercises the pure helper:

| Case | Setup | Expected outcome |
|------|-------|------------------|
| under cap | `Handler.maxResponseBytes = 1024`, value marshals to 512 bytes | `(body, "")` returned; `body` is the marshaled JSON |
| over cap | `Handler.maxResponseBytes = 64`, value marshals to ~200 bytes | `(nil, errResponseTooLarge.Error())` returned |
| disabled (zero) | `Handler.maxResponseBytes = 0` | `(body, "")` returned regardless of body size |
| marshal failure | value is a `func() {}` (json.Marshal errors on funcs) | `(nil, "internal error")` returned |

Mocks: `make generate SERVICE=room-service` regenerates
`mock_store_test.go` with the three new methods.

### Integration tests (`room-service/integration_test.go`, build tag `integration`)

- `TestMongoStore_ListDefaultChannelTabApps` — seed 4 apps: two
  `channelTab.default=true,enabled=true` (one of each name order), one
  `enabled=false`, one with no `channelTab` field; assert exactly the
  two default+enabled apps come back in `channelTab.name asc` order.
- `TestMongoStore_ListRoomBotApps` — seed users, apps (one with
  `assistant.enabled=true` + matching `name`, one with
  `assistant.enabled=false`, one with no assistant), subscriptions in
  two rooms (one bot in roomA, one bot in roomB, one human in roomA);
  assert `ListRoomBotApps("roomA")` returns only the bot whose app has
  `assistant.enabled=true`. A second assertion attempts to insert a
  second subscription for the same `(roomA, botAccount)` pair and
  expects a `mongo.IsDuplicateKeyError` — exercises the
  `(roomId, u.account)` unique-index invariant that protects
  `ListRoomBotApps` from returning duplicate rows.
- `TestMongoStore_ListActiveCmdMenus` — seed three menu docs (one
  `activeStatus=true` with matching name, one `activeStatus=false`,
  one with non-matching name); assert only the first comes back.
- `TestMongoStore_ListActiveCmdMenus_Empty` — empty `assistantNames`
  returns `([], nil)` without hitting Mongo.
- `TestMongoStore_EnsureIndexes_NewCompoundIndexes` — after
  `EnsureIndexes`, assert the new compound indexes on `apps`,
  `subscriptions`, and `bot_cmd_menu` exist with the expected key
  ordering (cheap regression guard against accidental removal).

### Coverage

≥80% package-wide per CLAUDE.md mandate. Aim ≥90% on the new handler
methods and the three new store methods.

## Out of scope

- Cross-site federation (no outbox events, no inbox handler changes,
  no cross-site admin authority — see Summary).
- Write paths (no new MongoDB writes; this PR is read-only).
- JetStream streams, canonical events, room-worker involvement.
- Notifications, push, or pub/sub event emission.
- Frontend changes — owned by `chat-frontend` in a separate PR.
- Backfilling `User.Roles` or migrating existing user documents — ops
  concern; this PR adds the field and the reader, no migration.

## Future optimizations

- **Parallel auth + data fetch** via `errgroup.WithContext` — saves one
  round-trip on the happy path; deferred for simplicity.
- **Response caching** (short TTL, per-site for tabs, per-room for
  cmd-menus) — deferred until measured contention.

## Risks

- **`User.Roles` absence on legacy docs.** Decodes to nil →
  `IsPlatformAdmin` returns false. Real admins lose access until ops
  backfills via `updateMany`.
- **`Subscription.User.IsBot` absence on pre-#219 subs.** Bot subs
  written before the field existed have `isBot=false` and miss the
  `ListRoomBotApps` join. Same backfill class as `mute.toggle`.
- **`SITE_URL` misconfigured.** Caught at startup; service exits.
- **Response exceeds NATS `max_payload`.** Fails with
  `errResponseTooLarge` (clean client error, not a silent drop).
  Mitigation: tighten the offending menu, or follow up with a
  per-assistant endpoint.
- **`bot_cmd_menu` schema drift.** No existing reader; the types here
  are the canonical schema.
