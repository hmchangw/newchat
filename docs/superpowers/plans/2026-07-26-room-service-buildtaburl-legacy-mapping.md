# room-service buildTabURL Legacy Mapping Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `buildTabURL` stops rewriting tab-URL templates onto `SITE_URL` and instead substitutes four `${...}` variables into the full URL the template already carries — including new `${roomType}` (legacy room-type vocabulary) and `${roomOrigin}` (env-configured legacy origin URL per site).

**Architecture:** All logic lives in `room-service` (flat `package main`). A hard-coded `legacyRoomTypes` map converts `model.RoomType` to the legacy vocabulary (fallback `"p"`). A `map[string]string` parsed from the `LEGACY_ROOM_ORIGINS` env var (native `caarlos0/env` v11 map support, `key:value,key:value`) maps a room's origin `SiteID` to its legacy origin URL (miss ⇒ empty string). `getRoomAppTabs` fetches the room doc once after authorization and passes it to `buildTabURL`. `SITE_URL` is removed entirely (its only consumer was the dropped rewrite).

**Tech Stack:** Go 1.25, `caarlos0/env/v11`, `go.uber.org/mock` (existing `MockRoomStore`), testify, testcontainers (projection integration test).

**Spec:** `docs/superpowers/specs/2026-07-26-room-service-buildtaburl-legacy-mapping-design.md`

## Global Constraints

- Branch: `claude/room-service-buildtaburl-mapping-8adzbk` — never push elsewhere.
- Always use `make` targets, never raw `go` commands: `make test SERVICE=room-service`, `make test-integration SERVICE=room-service`, `make lint`, `make fmt`.
- TDD red-green: run the test and see it FAIL (a compile error on an undefined symbol counts as red) before implementing.
- Error wrapping: `fmt.Errorf("<what this function was doing>: %w", err)`; client-facing errors via `pkg/errcode` sentinels only (this change reuses existing `errAppAccessDenied`).
- Every commit message ends with (verbatim, after a blank line):
  ```
  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01LoAt5zogndVghAPSyzmTJU
  ```
- The store interface (`store.go`) is NOT touched — `GetRoom` already exists — so `make generate` is not needed.
- If Docker is unavailable for `make test-integration`, say so explicitly in the task result instead of claiming the integration test ran.

---

### Task 1: `legacyRoomType` helper

**Files:**
- Modify: `room-service/handler.go` (near `buildTabURL`, ~line 2239)
- Test: `room-service/handler_test.go` (near `TestHandler_buildTabURL`, ~line 6080)

**Interfaces:**
- Consumes: `model.RoomType` constants from `pkg/model/room.go` (`RoomTypeChannel`, `RoomTypeDM`, `RoomTypeBotDM`, `RoomTypeDiscussion`).
- Produces: `func legacyRoomType(t model.RoomType) string` — used by Task 2's `buildTabURL`.

- [ ] **Step 1: Write the failing test**

Add to `room-service/handler_test.go` (above `TestHandler_buildTabURL`):

```go
func TestLegacyRoomType(t *testing.T) {
	tests := []struct {
		name string
		in   model.RoomType
		want string
	}{
		{name: "channel maps to p", in: model.RoomTypeChannel, want: "p"},
		{name: "dm maps to d", in: model.RoomTypeDM, want: "d"},
		{name: "botDM maps to d", in: model.RoomTypeBotDM, want: "d"},
		{name: "discussion maps to p", in: model.RoomTypeDiscussion, want: "p"},
		{name: "unknown type falls back to p", in: model.RoomType("livechat"), want: "p"},
		{name: "empty type falls back to p", in: model.RoomType(""), want: "p"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, legacyRoomType(tt.in))
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=room-service`
Expected: FAIL — compile error `undefined: legacyRoomType`.

- [ ] **Step 3: Write minimal implementation**

Add to `room-service/handler.go`, directly above the `buildTabURL` doc comment:

```go
// legacyRoomTypes maps the redesigned RoomType vocabulary to the legacy
// values pre-redesign channel-tab apps expect in their ${roomType} URL
// template variable.
var legacyRoomTypes = map[model.RoomType]string{
	model.RoomTypeChannel:    "p",
	model.RoomTypeDM:         "d",
	model.RoomTypeBotDM:      "d",
	model.RoomTypeDiscussion: "p",
}

// legacyRoomType returns the legacy vocabulary value for t, falling back
// to "p" for unknown types so ${roomType} always resolves.
func legacyRoomType(t model.RoomType) string {
	if v, ok := legacyRoomTypes[t]; ok {
		return v
	}
	return "p"
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=room-service`
Expected: PASS (all room-service tests green).

- [ ] **Step 5: Commit**

```bash
git add room-service/handler.go room-service/handler_test.go
git commit -m "feat(room-service): legacyRoomType map for tab-URL templates

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01LoAt5zogndVghAPSyzmTJU"
```

---

### Task 2: Substitution-only `buildTabURL` + room fetch in `getRoomAppTabs` + `SITE_URL` removal

**Files:**
- Modify: `room-service/handler.go` — `Handler` struct (~line 55), `NewHandler` (~line 71), `buildTabURL` (~line 2244), `getRoomAppTabs` (~line 2269)
- Modify: `room-service/main.go` — config struct (~line 32), `SITE_URL` validation block (~lines 98–103), `NewHandler` call (~line 218)
- Modify: `room-service/deploy/docker-compose.yml` — remove `- SITE_URL=http://localhost:3000` (line 18)
- Test: `room-service/handler_test.go` — `newTabsTestHandler` (~line 6060), `TestHandler_buildTabURL` (~line 6080), all `TestHandler_handleGetRoomAppTabs_*`

**Interfaces:**
- Consumes: `legacyRoomType(model.RoomType) string` from Task 1; existing `store.GetRoom(ctx, id) (*model.Room, error)` (wraps `mongo.ErrNoDocuments` on miss); existing `isURLSafeIDToken`, `errAppAccessDenied`.
- Produces: `func (h *Handler) buildTabURL(tmpl string, room *model.Room) (string, bool)`; `Handler.legacyRoomOrigins map[string]string`; `NewHandler(..., legacyRoomOrigins map[string]string, maxResponseBytes int64)` — the 12th parameter changes from `siteURL *url.URL` to `legacyRoomOrigins map[string]string`. Every existing caller except `main.go` passes `nil` there and needs NO change (nil is valid for both types). Task 4 replaces `main.go`'s temporary `nil` with the parsed config map.

- [ ] **Step 1: Rewrite `TestHandler_buildTabURL` as the failing test**

Replace the entire existing `TestHandler_buildTabURL` function in `room-service/handler_test.go` with:

```go
func TestHandler_buildTabURL(t *testing.T) {
	origins := map[string]string{
		"site-a": "https://legacy.site-a.com",
		"site-b": "https://legacy.site-b.com",
	}
	channelRoom := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}

	tests := []struct {
		name    string
		handler *Handler
		tmpl    string
		room    *model.Room
		wantURL string
		wantOK  bool
	}{
		{
			name:    "full template preserved with all four variables substituted",
			handler: &Handler{siteID: "site-a", legacyRoomOrigins: origins},
			tmpl:    "https://template-a.com/tab?room=${roomId}&site=${siteId}&roomType=${roomType}&roomOrigin=${roomOrigin}",
			room:    channelRoom,
			wantURL: "https://template-a.com/tab?room=r1&site=site-a&roomType=p&roomOrigin=https://legacy.site-a.com",
			wantOK:  true,
		},
		{
			name:    "no SITE_URL rewrite: template scheme, host and path kept verbatim",
			handler: &Handler{siteID: "site-a", legacyRoomOrigins: origins},
			tmpl:    "https://upstream.example.com/deep/path/${roomId}",
			room:    channelRoom,
			wantURL: "https://upstream.example.com/deep/path/r1",
			wantOK:  true,
		},
		{
			name:    "dm maps to d",
			handler: &Handler{siteID: "site-a", legacyRoomOrigins: origins},
			tmpl:    "https://t.example.com?rt=${roomType}",
			room:    &model.Room{ID: "r1", Type: model.RoomTypeDM, SiteID: "site-a"},
			wantURL: "https://t.example.com?rt=d",
			wantOK:  true,
		},
		{
			name:    "botDM maps to d",
			handler: &Handler{siteID: "site-a", legacyRoomOrigins: origins},
			tmpl:    "https://t.example.com?rt=${roomType}",
			room:    &model.Room{ID: "r1", Type: model.RoomTypeBotDM, SiteID: "site-a"},
			wantURL: "https://t.example.com?rt=d",
			wantOK:  true,
		},
		{
			name:    "discussion maps to p",
			handler: &Handler{siteID: "site-a", legacyRoomOrigins: origins},
			tmpl:    "https://t.example.com?rt=${roomType}",
			room:    &model.Room{ID: "r1", Type: model.RoomTypeDiscussion, SiteID: "site-a"},
			wantURL: "https://t.example.com?rt=p",
			wantOK:  true,
		},
		{
			name:    "unknown room type falls back to p",
			handler: &Handler{siteID: "site-a", legacyRoomOrigins: origins},
			tmpl:    "https://t.example.com?rt=${roomType}",
			room:    &model.Room{ID: "r1", Type: model.RoomType("livechat"), SiteID: "site-a"},
			wantURL: "https://t.example.com?rt=p",
			wantOK:  true,
		},
		{
			name:    "roomOrigin keyed by the room's own SiteID, not the local site",
			handler: &Handler{siteID: "site-a", legacyRoomOrigins: origins},
			tmpl:    "https://t.example.com?o=${roomOrigin}",
			room:    &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-b"},
			wantURL: "https://t.example.com?o=https://legacy.site-b.com",
			wantOK:  true,
		},
		{
			name:    "unmapped origin site substitutes empty string",
			handler: &Handler{siteID: "site-a", legacyRoomOrigins: origins},
			tmpl:    "https://t.example.com?o=${roomOrigin}&x=1",
			room:    &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-x"},
			wantURL: "https://t.example.com?o=&x=1",
			wantOK:  true,
		},
		{
			name:    "nil origins map substitutes empty string",
			handler: &Handler{siteID: "site-a"},
			tmpl:    "https://t.example.com?o=${roomOrigin}",
			room:    channelRoom,
			wantURL: "https://t.example.com?o=",
			wantOK:  true,
		},
		{
			name:    "empty template",
			handler: &Handler{siteID: "site-a", legacyRoomOrigins: origins},
			tmpl:    "",
			room:    channelRoom,
			wantOK:  false,
		},
		{
			name:    "malformed template",
			handler: &Handler{siteID: "site-a", legacyRoomOrigins: origins},
			tmpl:    "://malformed",
			room:    channelRoom,
			wantOK:  false,
		},
		{
			name:    "non-URL-safe roomID",
			handler: &Handler{siteID: "site-a", legacyRoomOrigins: origins},
			tmpl:    "https://t.example.com/${roomId}",
			room:    &model.Room{ID: "r1/../../etc", Type: model.RoomTypeChannel, SiteID: "site-a"},
			wantOK:  false,
		},
		{
			name:    "non-URL-safe siteID",
			handler: &Handler{siteID: "site/../a", legacyRoomOrigins: origins},
			tmpl:    "https://t.example.com/${roomId}",
			room:    channelRoom,
			wantOK:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := tt.handler.buildTabURL(tt.tmpl, tt.room)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantURL, got)
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=room-service`
Expected: FAIL — compile errors (`unknown field legacyRoomOrigins`, wrong argument count/type for `buildTabURL`).

- [ ] **Step 3: Implement `buildTabURL`, `Handler` field, `NewHandler`, and the `getRoomAppTabs` room fetch**

In `room-service/handler.go`:

3a. In the `Handler` struct, replace the field `siteURL *url.URL` (line 55) with:

```go
	legacyRoomOrigins map[string]string
```

3b. In `NewHandler`, replace the parameter `siteURL *url.URL` with `legacyRoomOrigins map[string]string` and the struct literal line `siteURL: siteURL,` with `legacyRoomOrigins: legacyRoomOrigins,`:

```go
func NewHandler(store RoomStore, keyStore RoomKeyStore, memberListClient MemberListClient, msgReader MessageReader, siteID string, maxRoomSize, maxBatchSize int, memberListTimeout time.Duration, restrictedRoomMinMembers int, publishToStream func(context.Context, string, []byte, string) error, publishCore func(context.Context, string, []byte) error, legacyRoomOrigins map[string]string, maxResponseBytes int64) *Handler {
```

3c. Replace `buildTabURL` (function AND its doc comment) with:

```go
// buildTabURL substitutes the ${roomId}, ${siteId}, ${roomType} and
// ${roomOrigin} template variables into a channelTab URL template. The
// template carries the full URL — no base-URL rewrite is applied.
// ${roomType} is the legacy room-type vocabulary (legacyRoomType);
// ${roomOrigin} is the legacy origin URL of the room's home site, or ""
// when unconfigured. Returns (url, true) on success; (_, false) when the
// template is empty or unparseable, or the IDs fail the URL-safety check.
func (h *Handler) buildTabURL(tmpl string, room *model.Room) (string, bool) {
	if tmpl == "" {
		return "", false
	}
	if !isURLSafeIDToken(room.ID) || !isURLSafeIDToken(h.siteID) {
		return "", false
	}
	// Substitute BEFORE parsing so url.Parse only validates the final
	// string and never percent-encodes the substituted values.
	tmpl = strings.ReplaceAll(tmpl, "${roomId}", room.ID)
	tmpl = strings.ReplaceAll(tmpl, "${siteId}", h.siteID)
	tmpl = strings.ReplaceAll(tmpl, "${roomType}", legacyRoomType(room.Type))
	tmpl = strings.ReplaceAll(tmpl, "${roomOrigin}", h.legacyRoomOrigins[room.SiteID])
	if _, err := url.Parse(tmpl); err != nil {
		return "", false
	}
	return tmpl, true
}
```

3d. In `getRoomAppTabs`, after the `authorizeRoomAppRead` block, fetch the room and pass it through (replacing the old `h.buildTabURL(app.ChannelTab.URL.Default, roomID)` call):

```go
	if err := h.authorizeRoomAppRead(ctx, account, roomID); err != nil {
		return nil, err
	}

	room, err := h.store.GetRoom(ctx, roomID)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, errAppAccessDenied
		}
		return nil, fmt.Errorf("get room for app tabs: %w", err)
	}
```

and inside the loop:

```go
		tabURL, ok := h.buildTabURL(app.ChannelTab.URL.Default, room)
```

3e. `handler.go` still needs the `net/url` import (`url.Parse` remains); do NOT remove it.

In `room-service/main.go`:

3f. Delete the config field `SiteURL string \`env:"SITE_URL,required"\`` (line 32).

3g. Delete the `SITE_URL` validation block (lines 98–103):

```go
	siteURL, err := url.Parse(cfg.SiteURL)
	if err != nil || siteURL.Scheme == "" || siteURL.Host == "" {
		slog.Error("invalid SITE_URL: must be an absolute URL with scheme and host",
			"value", cfg.SiteURL, "error", err)
		os.Exit(1)
	}
```

3h. In the `NewHandler` call (~line 218), replace the argument `siteURL,` with `nil,` (Task 4 replaces this with the parsed origins map).

3i. Remove the now-unused `"net/url"` import from `main.go`.

In `room-service/deploy/docker-compose.yml`:

3j. Delete the line `- SITE_URL=http://localhost:3000`.

- [ ] **Step 4: Update the tab-handler tests**

In `room-service/handler_test.go`:

4a. Replace `newTabsTestHandler` with (note: no URL parameter):

```go
func newTabsTestHandler(t *testing.T) (*Handler, *MockRoomStore, *gomock.Controller) {
	t.Helper()
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	return &Handler{
		store:             store,
		siteID:            "site-a",
		legacyRoomOrigins: map[string]string{"site-a": "https://legacy.site-a.com"},
	}, store, ctrl
}
```

4b. Update EVERY call site `newTabsTestHandler(t, "...")` → `newTabsTestHandler(t)`. Find them all with: `grep -n 'newTabsTestHandler(t,' room-service/handler_test.go` (they include the `TestHandler_handleGetRoomAppTabs_*` and `TestHandler_handleGetRoomAppCommandMenu_*` tests). Remove the `net/url` import from `handler_test.go` only if nothing else in the file uses it (check with `grep -n 'url\.' room-service/handler_test.go`).

4c. Every `getRoomAppTabs` test whose flow reaches the room fetch needs a `GetRoom` expectation. Member-path tests (`GetSubscription` returns a member) add ONE `GetRoom`; the admin-path test's existing `GetRoom` expectation becomes `.Times(2)` (authorize existence gate + tab fetch). Denied tests (`_Denied`, `_DeniedNoUser`) are unchanged — they never reach the fetch. Concretely:

`TestHandler_handleGetRoomAppTabs_MemberAllowed` becomes:

```go
func TestHandler_handleGetRoomAppTabs_MemberAllowed(t *testing.T) {
	h, store, _ := newTabsTestHandler(t)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1"}, nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").
		Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}, nil)
	store.EXPECT().ListDefaultChannelTabApps(gomock.Any()).Return([]model.App{
		mockTabApp("app1", "Calendar", "https://upstream.example.com/cal/${roomId}/${siteId}?type=${roomType}&origin=${roomOrigin}"),
	}, nil)

	resp, err := h.getRoomAppTabs(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)
	require.Len(t, resp.Apps, 1)
	assert.Equal(t, "app1", resp.Apps[0].ID)
	assert.Equal(t, "Calendar", resp.Apps[0].Name)
	assert.Equal(t, "https://upstream.example.com/cal/r1/site-a?type=p&origin=https://legacy.site-a.com", resp.Apps[0].TabURL)
	require.NotNil(t, resp.Apps[0].Assistant)
	assert.Equal(t, "app1.bot", resp.Apps[0].Assistant.Name)
}
```

`TestHandler_handleGetRoomAppTabs_AdminAllowed`: change its `GetRoom` expectation to:

```go
	store.EXPECT().GetRoom(gomock.Any(), "r1").
		Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}, nil).Times(2)
```

`TestHandler_handleGetRoomAppTabs_EmptyResultIsEmptyArray` and `TestHandler_handleGetRoomAppTabs_SkipsAppWithNilChannelTab`: add the same single-call `GetRoom` expectation as MemberAllowed (channel room, site-a).

4d. DELETE these three rewrite-era tests (the rewrite no longer exists): `TestHandler_handleGetRoomAppTabs_URLRewritePathPrefix`, `TestHandler_handleGetRoomAppTabs_URLRewriteStripsUserinfo`, `TestHandler_handleGetRoomAppTabs_URLRewritePreservesQueryAndFragment`. Replace with one preservation test:

```go
func TestHandler_handleGetRoomAppTabs_TemplateURLPreserved(t *testing.T) {
	h, store, _ := newTabsTestHandler(t)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"}, nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").
		Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}, nil)
	store.EXPECT().ListDefaultChannelTabApps(gomock.Any()).Return([]model.App{
		mockTabApp("app1", "X", "https://template-a.com/path?room=${roomId}#tab=${siteId}"),
	}, nil)

	resp, err := h.getRoomAppTabs(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)
	assert.Equal(t, "https://template-a.com/path?room=r1#tab=site-a", resp.Apps[0].TabURL,
		"template scheme/host must be preserved — no SITE_URL rewrite")
}
```

4e. Rename `TestHandler_handleGetRoomAppTabs_URLRewriteSkipsEmptyAndMalformed` to `TestHandler_handleGetRoomAppTabs_SkipsEmptyAndMalformed` and add the single-call `GetRoom` expectation (channel room, site-a); the rest of its body is unchanged.

4f. Add the two new room-fetch failure tests:

```go
func TestHandler_handleGetRoomAppTabs_RoomNotFound(t *testing.T) {
	h, store, _ := newTabsTestHandler(t)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"}, nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").
		Return(nil, fmt.Errorf("room %q not found: %w", "r1", mongo.ErrNoDocuments))

	_, err := h.getRoomAppTabs(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	assert.ErrorIs(t, err, errAppAccessDenied)
}

func TestHandler_handleGetRoomAppTabs_RoomFetchError(t *testing.T) {
	h, store, _ := newTabsTestHandler(t)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"}, nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").
		Return(nil, errors.New("mongo down"))

	_, err := h.getRoomAppTabs(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.Error(t, err)
	assert.NotErrorIs(t, err, errAppAccessDenied, "infra failure must collapse to internal, not access-denied")
}
```

(`fmt`, `errors`, and `mongo` are already imported in `handler_test.go`; verify with `grep -n '"fmt"\|"errors"\|mongo-driver' room-service/handler_test.go` and add any that are missing.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `make test SERVICE=room-service`
Expected: PASS.

- [ ] **Step 6: Verify the integration tests still compile**

Run: `make lint`
Expected: PASS (this also catches the `nil`-arg call sites in `integration_test.go`, which need no edits since `nil` is a valid `map[string]string`).

- [ ] **Step 7: Commit**

```bash
git add room-service/handler.go room-service/handler_test.go room-service/main.go room-service/deploy/docker-compose.yml
git commit -m "feat(room-service): substitution-only buildTabURL with roomType/roomOrigin variables

Templates now carry the full URL; the SITE_URL scheme/host rewrite and the
SITE_URL config are removed. getRoomAppTabs fetches the room doc to supply
the room's type and origin site.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01LoAt5zogndVghAPSyzmTJU"
```

---

### Task 3: `siteId` in the GetRoom projection

**Files:**
- Modify: `room-service/store_mongo.go` — `roomReadProjection` (~line 244)
- Test: `room-service/integration_test.go` — `TestMongoStore_GetRoom_ProjectionFields_Integration` (~line 110)

**Interfaces:**
- Consumes: existing `MongoStore.GetRoom` and the `mustInsertRoom` test helper (the fixture at ~line 118 already sets `SiteID: "site-a"`).
- Produces: `GetRoom` results with `Room.SiteID` populated — required for Task 2's `${roomOrigin}` lookup to work against real Mongo.

- [ ] **Step 1: Write the failing assertion**

In `TestMongoStore_GetRoom_ProjectionFields_Integration`, after the `got.Type` assertion (line 129), add:

```go
	assert.Equal(t, "site-a", got.SiteID, "siteId must be in the projection (buildTabURL reads room.SiteID for ${roomOrigin})")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test-integration SERVICE=room-service`
Expected: FAIL — `got.SiteID` is `""` because the projection omits `siteId`. (Requires Docker; if unavailable, run `make lint` to confirm compilation and state explicitly that the red run is deferred to CI.)

- [ ] **Step 3: Add the field to the projection**

In `room-service/store_mongo.go`, `roomReadProjection` becomes:

```go
var roomReadProjection = bson.D{
	{Key: "_id", Value: 1}, {Key: "type", Value: 1}, {Key: "name", Value: 1},
	{Key: "siteId", Value: 1},
	{Key: "userCount", Value: 1}, {Key: "appCount", Value: 1},
	{Key: "restricted", Value: 1}, {Key: "externalAccess", Value: 1},
	{Key: "lastMsgAt", Value: 1}, {Key: "minUserLastSeenAt", Value: 1},
	{Key: "lastMentionAllAt", Value: 1},
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test-integration SERVICE=room-service`
Expected: PASS (same Docker caveat as Step 2).

- [ ] **Step 5: Commit**

```bash
git add room-service/store_mongo.go room-service/integration_test.go
git commit -m "feat(room-service): project siteId in GetRoom for tab-URL origin lookup

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01LoAt5zogndVghAPSyzmTJU"
```

---

### Task 4: `LEGACY_ROOM_ORIGINS` config

**Files:**
- Modify: `room-service/main.go` — config struct + `NewHandler` call (`nil` → parsed map) + normalization helper
- Modify: `room-service/deploy/docker-compose.yml` — add a dev value
- Create: `room-service/config_test.go` (untagged unit test file, `package main`)

**Interfaces:**
- Consumes: `caarlos0/env` v11 native `map[string]string` parsing — items split on `,`, each pair split on the FIRST `:` (`strings.SplitN(pair, ":", 2)`), so URL values keep `://`. An unset or empty env var skips parsing and leaves the field `nil`.
- Produces: `config.LegacyRoomOrigins map[string]string`; `func normalizeLegacyRoomOrigins(in map[string]string) map[string]string` (never returns nil); the handler receives the normalized map.

- [ ] **Step 1: Write the failing tests**

Create `room-service/config_test.go`:

```go
package main

import (
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeLegacyRoomOrigins(t *testing.T) {
	tests := []struct {
		name string
		in   map[string]string
		want map[string]string
	}{
		{name: "nil map yields empty map", in: nil, want: map[string]string{}},
		{
			name: "trims whitespace around keys and values",
			in:   map[string]string{" site-a": " https://legacy.site-a.com "},
			want: map[string]string{"site-a": "https://legacy.site-a.com"},
		},
		{
			name: "drops entries empty after trimming",
			in:   map[string]string{"site-a": "  ", "": "https://x.example.com"},
			want: map[string]string{},
		},
		{
			name: "clean entries pass through",
			in: map[string]string{
				"site-a": "https://legacy.site-a.com",
				"site-b": "https://legacy.site-b.com",
			},
			want: map[string]string{
				"site-a": "https://legacy.site-a.com",
				"site-b": "https://legacy.site-b.com",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, normalizeLegacyRoomOrigins(tt.in))
		})
	}
}

// TestLegacyRoomOrigins_EnvWireFormat pins the LEGACY_ROOM_ORIGINS wire
// format: comma-separated entries, first-colon key:value split (URL values
// keep "://"), spaces after the colon tolerated via normalization.
func TestLegacyRoomOrigins_EnvWireFormat(t *testing.T) {
	type originsConfig struct {
		LegacyRoomOrigins map[string]string `env:"LEGACY_ROOM_ORIGINS"`
	}

	t.Setenv("LEGACY_ROOM_ORIGINS", "site-a:https://legacy.site-a.com,site-b: https://legacy.site-b.com")
	cfg, err := env.ParseAs[originsConfig]()
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"site-a": "https://legacy.site-a.com",
		"site-b": "https://legacy.site-b.com",
	}, normalizeLegacyRoomOrigins(cfg.LegacyRoomOrigins))

	t.Setenv("LEGACY_ROOM_ORIGINS", "")
	cfg, err = env.ParseAs[originsConfig]()
	require.NoError(t, err, "empty value must not be a parse error")
	assert.Empty(t, normalizeLegacyRoomOrigins(cfg.LegacyRoomOrigins))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=room-service`
Expected: FAIL — compile error `undefined: normalizeLegacyRoomOrigins`.

- [ ] **Step 3: Implement config field, helper, and wiring**

In `room-service/main.go`:

3a. Add to the `config` struct (after the `SiteID` field):

```go
	// LegacyRoomOrigins maps a room's origin siteID to the legacy origin URL
	// substituted into channel-tab ${roomOrigin} template variables. Wire
	// format: "site-a:https://legacy.site-a.com,site-b:https://legacy.site-b.com"
	// (comma-separated, first-colon split — URL values keep "://"). Unset ⇒
	// every ${roomOrigin} substitutes to "".
	LegacyRoomOrigins map[string]string `env:"LEGACY_ROOM_ORIGINS"`
```

3b. Add the helper (package-level, near the bottom of `main.go`):

```go
// normalizeLegacyRoomOrigins trims whitespace around the keys and values of
// the LEGACY_ROOM_ORIGINS map (tolerating "site-a: https://…" with a space
// after the colon) and drops entries left empty after trimming.
func normalizeLegacyRoomOrigins(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if k == "" || v == "" {
			continue
		}
		out[k] = v
	}
	return out
}
```

Add `"strings"` to `main.go` imports if not already present.

3c. In the `NewHandler` call, replace the Task 2 placeholder `nil,` with:

```go
		normalizeLegacyRoomOrigins(cfg.LegacyRoomOrigins),
```

In `room-service/deploy/docker-compose.yml`:

3d. Where `SITE_URL` used to be (environment list), add:

```yaml
      - LEGACY_ROOM_ORIGINS=site-local:https://legacy.localhost
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=room-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add room-service/main.go room-service/config_test.go room-service/deploy/docker-compose.yml
git commit -m "feat(room-service): LEGACY_ROOM_ORIGINS config for tab-URL origin mapping

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01LoAt5zogndVghAPSyzmTJU"
```

---

### Task 5: Client API docs

**Files:**
- Modify: `docs/client-api.md` — Get Room App Tabs `tabUrl` row (~line 2242) + success-response JSON example (~line 2245)

**Interfaces:**
- Consumes: final behavior from Tasks 1–4.
- Produces: docs matching the shipped behavior (CLAUDE.md requires this in the same PR as any client-facing handler change).

- [ ] **Step 1: Rewrite the `tabUrl` row**

Replace line 2242 of `docs/client-api.md` with:

```markdown
| `tabUrl` | string | Computed from `apps.channelTab.url.default`, which carries the full URL (no server-side base-URL rewrite). Substituted template variables: `${roomId}`, `${siteId}`, `${roomType}` (legacy room-type vocabulary: `channel`/`discussion` → `p`, `dm`/`botDM` → `d`, unknown → `p`), `${roomOrigin}` (legacy origin URL of the room's home site from `LEGACY_ROOM_ORIGINS`; empty string when the site is unconfigured). Apps whose template URL is empty or unparseable are silently skipped. |
```

- [ ] **Step 2: Update the success-response example**

Replace the JSON example (lines 2245–2256) with:

```json
{
  "apps": [
    {
      "id": "app-weather",
      "name": "Weather",
      "tabUrl": "https://template-a.com/apps/weather?room=01970a4f8c2d7c9aQ&roomType=p&roomOrigin=https://legacy.site-a.com",
      "assistant": { "enabled": true, "name": "weather.bot" }
    }
  ]
}
```

- [ ] **Step 3: Verify the derived views need no change**

Run: `grep -rn "tabUrl\|SITE_URL" docs/client-api/`
Expected: no output — `docs/client-api/request-reply.md` links to the canonical schema without duplicating the `tabUrl` prose, and `events.md` is unaffected (reply-only RPC). If the grep DOES match, update the matching row(s) to the Step 1 wording.

- [ ] **Step 4: Commit**

```bash
git add docs/client-api.md
git commit -m "docs(client-api): tabUrl template variables roomType/roomOrigin, no base-URL rewrite

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01LoAt5zogndVghAPSyzmTJU"
```

---

### Task 6: Full verification and push

**Files:** none created/modified unless a check fails.

- [ ] **Step 1: Format and lint**

Run: `make fmt && make lint`
Expected: no diffs, lint PASS. If `make fmt` changed files, re-run `make test SERVICE=room-service`, then amend them into the most recent relevant commit or add a `style:` commit.

- [ ] **Step 2: Unit tests (race detector via Makefile)**

Run: `make test SERVICE=room-service`
Expected: PASS.

- [ ] **Step 3: Integration tests**

Run: `make test-integration SERVICE=room-service`
Expected: PASS. (Requires Docker; if unavailable, state that explicitly — do not claim it ran.)

- [ ] **Step 4: SAST**

Run: `make sast`
Expected: PASS (no medium+ findings). The change removes an URL-rewrite and adds string substitution — no new gosec surface expected.

- [ ] **Step 5: Push with retry**

```bash
git push -u origin claude/room-service-buildtaburl-mapping-8adzbk
```

On network failure retry after 2s, 4s, 8s, 16s. Do NOT create a pull request (not requested).

---

## Self-Review Notes

- **Spec coverage:** type map + fallback (Task 1), substitution-only rewrite / room fetch / `errAppAccessDenied` on missing room / `SITE_URL` removal (Task 2), projection (Task 3), env config + trim normalization + nil-safe miss-to-empty (Tasks 2 & 4), docs (Task 5). Out-of-scope items in the spec (no percent-encoding, cmd-menu untouched) require no task.
- **Type consistency:** `buildTabURL(tmpl string, room *model.Room) (string, bool)`, `legacyRoomType(model.RoomType) string`, `Handler.legacyRoomOrigins map[string]string`, `normalizeLegacyRoomOrigins(map[string]string) map[string]string` — names used identically across Tasks 1–4.
- **Known behavior change accepted by design:** templates with userinfo (`user:pass@host`) are no longer stripped (the strip was part of the deleted rewrite; templates are admin-configured data).
