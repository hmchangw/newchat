# Search Sender/Room AppInfo Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reshape `search.messages` hit enrichment — `sender` carries `hr` (humans) / `appInfo` (bots) instead of `displayName`; `room` regains a compact `appInfo` for botDM (with `isSubscribed`) and rekeys dm `hrInfo` to `chineseName`.

**Architecture:** New compact wire types `MessageHRInfo` / `MessageAppInfo` in `pkg/model`; `search-service/enrich.go` maps its three existing batched Mongo lookups onto them. Zero new queries — only two projection tweaks (`isSubscribed` on subscriptions, explicit `_id` on apps).

**Tech Stack:** Go 1.25, mongo-driver v2, testify, per CLAUDE.md all commands via `make`.

**Spec:** `docs/superpowers/specs/2026-07-31-search-sender-room-appinfo-design.md`

## Global Constraints

- TDD Red-Green-Refactor for every task; run tests via `make test SERVICE=<name>` (never raw `go test`).
- Docker is unavailable in this environment: integration-tagged tests are compile-checked with `go vet -tags integration ./search-service/` and executed by CI.
- Client-facing response structs changed → `docs/client-api.md` **and** `docs/client-api/request-reply.md` must be updated in the same PR.
- No store interface signature changes → no `make generate` needed.
- Branch: `claude/search-message-sender-room-appinfo`; commits per task.

---

### Task 1: Compact model types + reshaped sender/room structs

**Files:**
- Modify: `pkg/model/search.go` (MessageSender/MessageRoom + new types)
- Test: `pkg/model/model_test.go` (`TestSearchMessageEnrichmentJSON`, new `TestMessageAppInfoJSON`)

**Interfaces:**
- Produces: `model.MessageHRInfo{Account, ChineseName, EngName string}`, `model.MessageAppInfo{ID, Name, AssistantName string; IsSubscribed *bool}`, `model.MessageSender{Account string; HR *MessageHRInfo; AppInfo *MessageAppInfo}`, `model.MessageRoom{ID, Name string; Type RoomType; HRInfo *MessageHRInfo; AppInfo *MessageAppInfo}` — Task 3 populates these.

- [ ] **Step 1: Rewrite the enrichment JSON tests (failing)**

Replace `TestSearchMessageEnrichmentJSON` in `pkg/model/model_test.go` and add an appInfo test:

```go
func TestSearchMessageEnrichmentJSON(t *testing.T) {
	subscribed := true
	m := model.SearchMessage{
		MessageID:   "m1",
		RoomID:      "r1",
		SiteID:      "site-a",
		UserAccount: "alice",
		Content:     "hi",
		CreatedAt:   time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC),
		TShow:       true,
		Sender: &model.MessageSender{
			Account: "alice",
			HR:      &model.MessageHRInfo{Account: "alice", ChineseName: "愛麗絲", EngName: "Alice Wang"},
		},
		Room: &model.MessageRoom{
			ID:      "r1",
			Name:    "Weather App",
			Type:    model.RoomTypeBotDM,
			AppInfo: &model.MessageAppInfo{ID: "app-1", Name: "Weather App", AssistantName: "weather.bot", IsSubscribed: &subscribed},
		},
	}
	roundTrip(t, &m, &model.SearchMessage{})

	// dm room: hrInfo serializes chineseName (not the legacy "name" key).
	b, err := json.Marshal(model.MessageRoom{
		ID: "r2", Type: model.RoomTypeDM,
		HRInfo: &model.MessageHRInfo{Account: "bob", ChineseName: "陳", EngName: "Bob Chan"},
	})
	require.NoError(t, err)
	assert.Contains(t, string(b), `"chineseName":"陳"`)
	assert.NotContains(t, string(b), `"appInfo"`)

	// omitempty: a zero-value SearchMessage must not emit room/sender/tshow keys.
	b, err = json.Marshal(model.SearchMessage{MessageID: "x"})
	require.NoError(t, err)
	assert.NotContains(t, string(b), "\"room\"")
	assert.NotContains(t, string(b), "\"sender\"")
	assert.NotContains(t, string(b), "\"tshow\"")
}

func TestMessageAppInfoJSON(t *testing.T) {
	// Sender variant: IsSubscribed nil → key absent.
	b, err := json.Marshal(model.MessageSender{
		Account: "weather.bot",
		AppInfo: &model.MessageAppInfo{ID: "app-1", Name: "Weather App", AssistantName: "weather.bot"},
	})
	require.NoError(t, err)
	assert.NotContains(t, string(b), "isSubscribed")
	assert.NotContains(t, string(b), "displayName")
	assert.NotContains(t, string(b), `"hr"`)

	// Room variant: explicit false must serialize (pointer, not omitted).
	unsubscribed := false
	b, err = json.Marshal(model.MessageAppInfo{ID: "app-1", Name: "W", AssistantName: "w.bot", IsSubscribed: &unsubscribed})
	require.NoError(t, err)
	assert.Contains(t, string(b), `"isSubscribed":false`)
}
```

- [ ] **Step 2: Run to verify failure**

Run: `make test SERVICE=pkg/model`
Expected: compile FAIL (`unknown field HR`, `MessageHRInfo` undefined).

- [ ] **Step 3: Implement the model changes**

In `pkg/model/search.go`, replace the `MessageRoom` and `MessageSender` types:

```go
// MessageRoom is the enriched room object attached to a SearchMessage.
// Type is the room type from the caller's subscription. HRInfo is set only
// for dm rooms; AppInfo only for botDM rooms. Name is the app name (botDM),
// the counterpart's display name (dm), or the canonical room name (channel/
// discussion, from the RoomsInfoBatch RPC).
type MessageRoom struct {
	ID      string          `json:"id"`
	Name    string          `json:"name,omitempty"`
	Type    RoomType        `json:"type,omitempty"`
	HRInfo  *MessageHRInfo  `json:"hrInfo,omitempty"`
	AppInfo *MessageAppInfo `json:"appInfo,omitempty"`
}

// MessageSender is the enriched author object attached to a SearchMessage.
// HR is set for human senders, AppInfo for bot senders; both are omitted
// when the lookup missed — the client renders the display name.
type MessageSender struct {
	Account string          `json:"account"`
	HR      *MessageHRInfo  `json:"hr,omitempty"`
	AppInfo *MessageAppInfo `json:"appInfo,omitempty"`
}

// MessageHRInfo is the compact HR record on search sender/room objects.
type MessageHRInfo struct {
	Account     string `json:"account"`
	ChineseName string `json:"chineseName,omitempty"`
	EngName     string `json:"engName,omitempty"`
}

// MessageAppInfo is the compact app record on search sender/room objects.
// IsSubscribed is set only on room.appInfo (botDM) — explicit true/false from
// the caller's subscription row — and stays nil (absent) on sender.appInfo.
type MessageAppInfo struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	AssistantName string `json:"assistantName"`
	IsSubscribed  *bool  `json:"isSubscribed,omitempty"`
}
```

NOTE: `search-service/enrich.go` still references `DisplayName` and builds `SubscriptionHRInfo` — it breaks compilation here. Patch it minimally in the same step so the repo compiles (full rewrite is Task 3): in the hit loop replace the `out[i].Sender = ...` assignment and DM branch with

```go
		out[i].Sender = &model.MessageSender{Account: hits[i].UserAccount}
		room := &model.MessageRoom{ID: rid}
		if meta, ok := subs[rid]; ok {
			room.Type = meta.RoomType
			switch meta.RoomType {
			case model.RoomTypeDM:
				if hr, ok := users[meta.Name]; ok {
					room.HRInfo = &model.MessageHRInfo{Account: hr.Account, ChineseName: hr.ChineseName, EngName: hr.EngName}
					room.Name = displayfmt.CombineWithFallback(hr.EngName, hr.ChineseName, meta.Name)
				} else {
					room.Name = meta.Name
				}
```

and delete the now-unused `resolveSenderName` (and its `displayfmt` usage stays for the DM branch). Delete the sender-related assertions in `search-service/enrich_test.go` that reference `DisplayName` (they are rewritten in Task 3).

- [ ] **Step 4: Run to verify pass**

Run: `make test SERVICE=pkg/model && make test SERVICE=search-service`
Expected: PASS (search-service tests trimmed of DisplayName assertions).

- [ ] **Step 5: Commit**

```bash
git add pkg/model/search.go pkg/model/model_test.go search-service/enrich.go search-service/enrich_test.go
git commit -m "Add compact MessageHRInfo/MessageAppInfo search wire types"
```

---

### Task 2: Store projections — isSubscribed + explicit app _id

**Files:**
- Modify: `search-service/store.go` (`SubscriptionMeta`), `search-service/store_mongo.go`
- Test: `search-service/integration_enrich_test.go`

**Interfaces:**
- Produces: `SubscriptionMeta{RoomType model.RoomType; Name string; IsSubscribed bool}`; `AppsByAssistantNames` map values carry `ID`, `Name`, `Assistant.Name` (all other App fields zero). Task 3 consumes both.

- [ ] **Step 1: Extend the integration test (failing on CI, compile-checked here)**

In `integration_enrich_test.go`, extend the subscriptions fixture (`s2` gains `"isSubscribed": true`) and the assertions:

```go
		bson.M{"_id": "s2", "u": bson.M{"account": "alice"}, "roomId": "rBot", "roomType": "botDM", "name": "helper.bot", "isSubscribed": true},
```

```go
	assert.True(t, subs["rBot"].IsSubscribed)
	assert.False(t, subs["rDM"].IsSubscribed) // absent in fixture → zero value
```

and for apps:

```go
	assert.Equal(t, "app-1", apps["helper.bot"].ID)
	assert.Equal(t, "helper.bot", apps["helper.bot"].Assistant.Name)
```

- [ ] **Step 2: Implement**

`store.go`:

```go
// SubscriptionMeta is the caller's subscription projection used for enrichment:
// the room type, the join-key Name (DM counterpart account / botDM bot
// account), and the botDM IsSubscribed flag.
type SubscriptionMeta struct {
	RoomType     model.RoomType
	Name         string
	IsSubscribed bool
}
```

`store_mongo.go` — `SubscriptionsByRoomIDs`: projection `bson.M{"roomId": 1, "roomType": 1, "name": 1, "isSubscribed": 1}`, row struct gains `IsSubscribed bool \`bson:"isSubscribed"\``, map fill passes it through. `AppsByAssistantNames`: projection becomes `bson.M{"_id": 1, "name": 1, "assistant.name": 1}` (makes the default `_id` inclusion explicit) and the doc comment says the map values carry only ID/Name/Assistant.Name.

- [ ] **Step 3: Verify**

Run: `make test SERVICE=search-service && go vet -tags integration ./search-service/`
Expected: PASS / clean.

- [ ] **Step 4: Commit**

```bash
git add search-service/store.go search-service/store_mongo.go search-service/integration_enrich_test.go
git commit -m "Project isSubscribed and app id for search enrichment"
```

---

### Task 3: Enrichment rewrite

**Files:**
- Modify: `search-service/enrich.go`
- Test: `search-service/enrich_test.go`

**Interfaces:**
- Consumes: Task 1 model types, Task 2 `SubscriptionMeta.IsSubscribed` + app `ID`.
- Produces: `buildSender(account string, users map[string]HRUser, apps map[string]model.App) *model.MessageSender` (unexported helper).

- [ ] **Step 1: Rewrite the enrichment tests (failing)**

Replace the sender/botDM-related tests in `enrich_test.go`:

```go
func TestEnrichMessages_DM(t *testing.T) {
	m := &fakeMongo{
		subs:  map[string]SubscriptionMeta{"rDM": {RoomType: model.RoomTypeDM, Name: "bob"}},
		users: map[string]HRUser{"bob": {Account: "bob", EngName: "Bob Chan", ChineseName: "陳"}, "alice": {Account: "alice", EngName: "Alice", ChineseName: "愛麗絲"}},
	}
	h := enrichHandler(m, &fakeRoom{})
	out := h.enrichMessages(context.Background(), "alice", []messageSearchHit{hit("m1", "rDM", "site-a", "alice")})
	require.Len(t, out, 1)
	require.NotNil(t, out[0].Room)
	assert.Equal(t, model.RoomTypeDM, out[0].Room.Type)
	assert.Equal(t, "Bob Chan 陳", out[0].Room.Name)
	require.NotNil(t, out[0].Room.HRInfo)
	assert.Equal(t, &model.MessageHRInfo{Account: "bob", ChineseName: "陳", EngName: "Bob Chan"}, out[0].Room.HRInfo)
	assert.Nil(t, out[0].Room.AppInfo)
	// human sender: account + hr, no appInfo
	require.NotNil(t, out[0].Sender)
	assert.Equal(t, "alice", out[0].Sender.Account)
	assert.Equal(t, &model.MessageHRInfo{Account: "alice", ChineseName: "愛麗絲", EngName: "Alice"}, out[0].Sender.HR)
	assert.Nil(t, out[0].Sender.AppInfo)
}

func TestEnrichMessages_BotDM(t *testing.T) {
	m := &fakeMongo{
		subs: map[string]SubscriptionMeta{"rBot": {RoomType: model.RoomTypeBotDM, Name: "helper.bot", IsSubscribed: true}},
		apps: map[string]model.App{"helper.bot": {ID: "app1", Name: "Helper", Assistant: &model.AppAssistant{Name: "helper.bot"}}},
	}
	h := enrichHandler(m, &fakeRoom{})
	// sender is the bot itself
	out := h.enrichMessages(context.Background(), "alice", []messageSearchHit{hit("m1", "rBot", "site-a", "helper.bot")})
	require.NotNil(t, out[0].Room)
	assert.Equal(t, model.RoomTypeBotDM, out[0].Room.Type)
	assert.Equal(t, "Helper", out[0].Room.Name)
	assert.Nil(t, out[0].Room.HRInfo)
	require.NotNil(t, out[0].Room.AppInfo)
	assert.Equal(t, "app1", out[0].Room.AppInfo.ID)
	assert.Equal(t, "Helper", out[0].Room.AppInfo.Name)
	assert.Equal(t, "helper.bot", out[0].Room.AppInfo.AssistantName)
	require.NotNil(t, out[0].Room.AppInfo.IsSubscribed)
	assert.True(t, *out[0].Room.AppInfo.IsSubscribed)
	// bot sender: account + appInfo (no isSubscribed), no hr
	require.NotNil(t, out[0].Sender.AppInfo)
	assert.Equal(t, &model.MessageAppInfo{ID: "app1", Name: "Helper", AssistantName: "helper.bot"}, out[0].Sender.AppInfo)
	assert.Nil(t, out[0].Sender.HR)
}

func TestEnrichMessages_BotDM_Unsubscribed(t *testing.T) {
	m := &fakeMongo{
		subs: map[string]SubscriptionMeta{"rBot": {RoomType: model.RoomTypeBotDM, Name: "helper.bot", IsSubscribed: false}},
		apps: map[string]model.App{"helper.bot": {ID: "app1", Name: "Helper"}},
	}
	h := enrichHandler(m, &fakeRoom{})
	out := h.enrichMessages(context.Background(), "alice", []messageSearchHit{hit("m1", "rBot", "site-a", "alice")})
	require.NotNil(t, out[0].Room.AppInfo)
	require.NotNil(t, out[0].Room.AppInfo.IsSubscribed)
	assert.False(t, *out[0].Room.AppInfo.IsSubscribed)
	// explicit false serializes on the room object
	b, err := json.Marshal(out[0].Room)
	require.NoError(t, err)
	assert.Contains(t, string(b), `"isSubscribed":false`)
}

func TestEnrichMessages_BotDM_AppLookupMiss(t *testing.T) {
	m := &fakeMongo{
		subs: map[string]SubscriptionMeta{"rBot": {RoomType: model.RoomTypeBotDM, Name: "helper.bot", IsSubscribed: true}},
	}
	h := enrichHandler(m, &fakeRoom{})
	out := h.enrichMessages(context.Background(), "alice", []messageSearchHit{hit("m1", "rBot", "site-a", "helper.bot")})
	// no app doc → name falls back to the subscription name, no half-empty appInfo
	assert.Equal(t, "helper.bot", out[0].Room.Name)
	assert.Nil(t, out[0].Room.AppInfo)
	assert.Nil(t, out[0].Sender.AppInfo)
	assert.Equal(t, "helper.bot", out[0].Sender.Account)
}
```

Update the surviving tests to the new shape: in `TestEnrichMessages_DegradesOnAllErrors`, `TestEnrichMessages_NilMongoDegrades`, `TestEnrichMessages_NilMongoAndRoomDegrades` replace `assert.Equal(t, "alice", out[0].Sender.DisplayName)` with

```go
	assert.Equal(t, "alice", out[0].Sender.Account)
	assert.Nil(t, out[0].Sender.HR)
```

and in `TestEnrichMessages_ChannelUsesRoomBatch` keep the room assertions, adding `assert.Nil(t, out[0].Room.AppInfo)`.

- [ ] **Step 2: Run to verify failure**

Run: `make test SERVICE=search-service`
Expected: FAIL — sender `hr`/`appInfo` never populated, `room.AppInfo` nil.

- [ ] **Step 3: Implement**

In `enrich.go`, replace the sender assignment in the hit loop with `out[i].Sender = buildSender(hits[i].UserAccount, users, apps)`, fill the botDM branch:

```go
			case model.RoomTypeBotDM:
				if app, ok := apps[meta.Name]; ok {
					isSubscribed := meta.IsSubscribed
					room.AppInfo = &model.MessageAppInfo{ID: app.ID, Name: app.Name, AssistantName: meta.Name, IsSubscribed: &isSubscribed}
					room.Name = app.Name
				} else {
					room.Name = meta.Name
				}
```

and replace `resolveSenderName` with:

```go
// buildSender assembles the sender object: hr for human senders, appInfo for
// bot senders; either is omitted when its lookup missed — the client renders
// the display name.
func buildSender(account string, users map[string]HRUser, apps map[string]model.App) *model.MessageSender {
	s := &model.MessageSender{Account: account}
	if model.IsBot(account) {
		if app, ok := apps[account]; ok {
			s.AppInfo = &model.MessageAppInfo{ID: app.ID, Name: app.Name, AssistantName: account}
		}
	} else if hr, ok := users[account]; ok {
		s.HR = &model.MessageHRInfo{Account: hr.Account, ChineseName: hr.ChineseName, EngName: hr.EngName}
	}
	return s
}
```

(`model.IsBot("")` is false and the users map has no `""` key, so the empty-account hit degrades to `{account: ""}` with no lookup branch.)

- [ ] **Step 4: Run to verify pass**

Run: `make test SERVICE=search-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add search-service/enrich.go search-service/enrich_test.go
git commit -m "Enrich search sender/room with compact hr and appInfo objects"
```

---

### Task 4: Client API docs

**Files:**
- Modify: `docs/client-api.md` (Search Messages §: `SearchMessage` field table rows for `sender`/`room`, `MessageSender`/`MessageRoom` tables, new `MessageHRInfo`/`MessageAppInfo` tables, JSON example if present)
- Modify: `docs/client-api/request-reply.md` (Search Messages schema lines)

- [ ] **Step 1: Rewrite the canonical tables**

`MessageSender`:

```markdown
| Field | Type | Notes |
|---|---|---|
| `account` | string | sender's account |
| `hr` | [MessageHRInfo](#messagehrinfo) | human senders only; omitted when the users lookup missed |
| `appInfo` | [MessageAppInfo](#messageappinfo) | bot senders only (`isSubscribed` never set here); omitted when the apps lookup missed |
```

`MessageRoom`:

```markdown
| Field | Type | Notes |
|---|---|---|
| `id` | string | roomId |
| `name` | string | app name (`botDM`) / counterpart display name (`dm`) / canonical room name (`channel`, `discussion`). Omitted when unresolved. |
| `type` | string | `channel` \| `dm` \| `botDM` \| `discussion`. Omitted when the caller has no subscription for the room. |
| `hrInfo` | [MessageHRInfo](#messagehrinfo) | present **only for `dm` rooms** |
| `appInfo` | [MessageAppInfo](#messageappinfo) | present **only for `botDM` rooms**; `isSubscribed` always set here |
```

New tables:

```markdown
##### MessageHRInfo

| Field | Type | Notes |
|---|---|---|
| `account` | string | HR-directory account |
| `chineseName` | string | omitted when empty |
| `engName` | string | omitted when empty |

##### MessageAppInfo

| Field | Type | Notes |
|---|---|---|
| `id` | string | app document id |
| `name` | string | app display name |
| `assistantName` | string | bot account (`assistant.name`) |
| `isSubscribed` | boolean | `room.appInfo` only — the caller's subscription state for the bot; absent on `sender.appInfo` |
```

Also update the `SearchMessage` table rows for `sender` (`account` always set, `hr`/`appInfo` best-effort) and `room` (`name`/`type`/`hrInfo`/`appInfo` best-effort), and the JSON success example if the section carries one.

- [ ] **Step 2: Mirror in the derived view**

In `request-reply.md`, the `SearchMessage` schema line becomes:

```markdown
`tshow` (boolean, omitted when false),
`sender` (`{account, hr?, appInfo?}` — `hr` `{account, chineseName, engName}` for
human senders, `appInfo` `{id, name, assistantName}` for bot senders),
`room` (`{id, name, type, hrInfo?, appInfo?}` — `hrInfo` `{account, chineseName,
engName}` only for `dm`, `appInfo` `{id, name, assistantName, isSubscribed}` only
for `botDM`).
```

- [ ] **Step 3: Commit**

```bash
git add docs/client-api.md docs/client-api/request-reply.md
git commit -m "Document reshaped search sender/room enrichment objects"
```

---

### Task 5: Verification, simplify pass, PR

- [ ] Run `make test` (full, `-race`) — all packages pass.
- [ ] Run `make lint` — 0 issues.
- [ ] Run `go vet -tags integration ./search-service/` — clean.
- [ ] Run `make sast` — gosec/govulncheck/semgrep pass.
- [ ] Run the `simplify` skill over the branch diff; apply fixes; re-run `make test SERVICE=search-service && make test SERVICE=pkg/model`.
- [ ] Final self-review of `git diff main...HEAD` against the spec.
- [ ] Ensure `docs/reviews/` is empty on the branch.
- [ ] Push `claude/search-message-sender-room-appinfo`, open the PR (base `main`).
