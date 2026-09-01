# Per-Subscriber Room Type Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Each subscription row stores the room type as its own subscriber sees it — a bot's row in a bot↔human DM is `dm`, the human's is `botDM` — so `subscription.update` matches the database and every read gate returns to its original form.

**Architecture:** Two pure functions in `pkg/model` decide the room document's type and each row's type at creation. Six write sites adopt them. All read-side remapping added by the previous design is reverted; only the `subscription.update` stamp survives, as defence against corrupt rows.

**Tech Stack:** Go 1.25, MongoDB (`go.mongodb.org/mongo-driver/v2`), `go.uber.org/mock`, `stretchr/testify`, `testcontainers-go`.

**Spec:** `docs/superpowers/specs/2026-08-29-per-subscriber-room-type-design.md`

## Global Constraints

- Run `make` targets only. `make test SERVICE=<name>` (use `SERVICE=pkg/model` for `pkg/` dirs), `make lint`, `make fmt`, `make generate SERVICE=<name>`.
- All tests run with `-race` (the Makefile handles it).
- Test files live in the same package as the code under test. Integration tests carry `//go:build integration` and use `pkg/testutil` containers.
- Minimum 80% coverage per package; 90%+ for `pkg/` and handlers.
- Error wrapping: `fmt.Errorf("short description: %w", err)`. Never a bare `err`.
- `log/slog` only, key-value pairs, never interpolated.
- No new third-party dependencies.
- **Code comments: short and neat.** One line where one line does; two at most. No comment that restates the code.
- A row's counterpart account is its `name` field: the bot account on the human's row, the human account on the bot's row.
- `p_admin` stays bot-like everywhere except DM classification — `filterBots`, `errBotInChannel`, the role rules and `newSub`'s `u.isBot` flag are NOT touched.
- Commit after each task, once its tests pass.

---

### Task 1: The two write-time functions

**Files:**
- Modify: `pkg/model/subscription.go`
- Test: `pkg/model/model_test.go`

**Interfaces:**
- Consumes: `model.IsBot(account string) bool` (`pkg/model/user.go:175`), `RoomTypeBotDM` / `RoomTypeDM`
- Produces: `model.DMRoomType(a, b string) RoomType`, `model.SubscriptionRoomType(counterpart string) RoomType`

- [ ] **Step 1: Write the failing test**

Append to `pkg/model/model_test.go`:

```go
func TestDMRoomType(t *testing.T) {
	tests := []struct {
		name string
		a, b string
		want model.RoomType
	}{
		{"human pair", "alice", "bob", model.RoomTypeDM},
		{"bot counterpart", "alice", "weather.bot", model.RoomTypeBotDM},
		{"bot requester", "weather.bot", "alice", model.RoomTypeBotDM},
		{"two bots", "weather.bot", "sales.bot", model.RoomTypeBotDM},
		{"platform admin is an ordinary partner", "alice", "p_adminsiteA", model.RoomTypeDM},
		{"QA p_ account is an ordinary partner", "alice", "p_qa1", model.RoomTypeDM},
		{"self DM", "alice", "alice", model.RoomTypeDM},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, model.DMRoomType(tt.a, tt.b))
		})
	}
}

func TestSubscriptionRoomType(t *testing.T) {
	tests := []struct {
		counterpart string
		want        model.RoomType
	}{
		{"weather.bot", model.RoomTypeBotDM},
		{"weather.site-a.bot", model.RoomTypeBotDM},
		{"alice", model.RoomTypeDM},
		{"p_adminsiteA", model.RoomTypeDM},
		{"p_qa1", model.RoomTypeDM},
		{"", model.RoomTypeDM},
	}
	for _, tt := range tests {
		t.Run(tt.counterpart, func(t *testing.T) {
			assert.Equal(t, tt.want, model.SubscriptionRoomType(tt.counterpart))
		})
	}
}

// The two sides of a bot<->human DM classify differently; that asymmetry is the
// point. The room doc keeps one type.
func TestRoomAndSubscriptionTypesDisagreeOnABotDM(t *testing.T) {
	assert.Equal(t, model.RoomTypeBotDM, model.DMRoomType("alice", "weather.bot"))
	assert.Equal(t, model.RoomTypeBotDM, model.SubscriptionRoomType("weather.bot")) // alice's row
	assert.Equal(t, model.RoomTypeDM, model.SubscriptionRoomType("alice"))          // the bot's row
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/model`
Expected: FAIL — `undefined: model.DMRoomType`, `undefined: model.SubscriptionRoomType`

- [ ] **Step 3: Write minimal implementation**

Append to `pkg/model/subscription.go`:

```go
// DMRoomType is the room document's type for a two-party DM: botDM when either
// participant is a ".bot" app. p_admin owns no app, so its DMs are ordinary.
func DMRoomType(a, b string) RoomType {
	if IsBot(a) || IsBot(b) {
		return RoomTypeBotDM
	}
	return RoomTypeDM
}

// SubscriptionRoomType is one row's type — the room as its own subscriber sees
// it. The two sides of a bot<->human DM therefore differ.
func SubscriptionRoomType(counterpart string) RoomType {
	if IsBot(counterpart) {
		return RoomTypeBotDM
	}
	return RoomTypeDM
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=pkg/model`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/model/subscription.go pkg/model/model_test.go
git commit -m "feat(model): add DMRoomType and SubscriptionRoomType"
```

---

### Task 2: room-worker writes the per-subscriber type

**Files:**
- Modify: `room-worker/handler.go` — `newSub` (~:1492), `buildDMSubs` (~:1383), `buildBotDMSubs` (~:1391), `buildSelfDMSub` (~:1400), `determineRoomTypeFromPayload` (~:1698), and the two builder call sites (~:1645, ~:2101)
- Test: `room-worker/handler_test.go`

**Interfaces:**
- Consumes: `model.DMRoomType`, `model.SubscriptionRoomType` from Task 1
- Produces: `buildDMPairSubs(requester, other *model.User, room *model.Room, acceptedAt time.Time) []*model.Subscription` replacing `buildDMSubs` and `buildBotDMSubs`; `newSub` gains a `roomType model.RoomType` parameter after `roles`

- [ ] **Step 1: Write the failing test**

Append to `room-worker/handler_test.go`:

```go
// Each row stores the room as ITS OWN subscriber sees it, so the two sides of a
// bot<->human DM differ. The room doc keeps a single type.
func TestBuildDMPairSubs_PerSubscriberRoomType(t *testing.T) {
	tests := []struct {
		name             string
		requester, other string
		roomType         model.RoomType
		wantRequester    model.RoomType
		wantOther        model.RoomType
	}{
		{"user creates a DM with a bot", "alice", "weather.bot", model.RoomTypeBotDM, model.RoomTypeBotDM, model.RoomTypeDM},
		{"bot creates a DM with a user", "weather.bot", "alice", model.RoomTypeBotDM, model.RoomTypeDM, model.RoomTypeBotDM},
		{"two humans", "alice", "bob", model.RoomTypeDM, model.RoomTypeDM, model.RoomTypeDM},
		{"two bots", "weather.bot", "sales.bot", model.RoomTypeBotDM, model.RoomTypeBotDM, model.RoomTypeBotDM},
		{"platform admin", "alice", "p_adminsiteA", model.RoomTypeDM, model.RoomTypeDM, model.RoomTypeDM},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requester := &model.User{ID: "u_" + tt.requester, Account: tt.requester}
			other := &model.User{ID: "u_" + tt.other, Account: tt.other}
			room := &model.Room{ID: "r1", SiteID: "site-A", Type: tt.roomType}

			subs := buildDMPairSubs(requester, other, room, time.Now().UTC())

			require.Len(t, subs, 2)
			assert.Equal(t, tt.other, subs[0].Name, "requester's row names the counterpart")
			assert.Equal(t, tt.wantRequester, subs[0].RoomType)
			assert.Equal(t, tt.requester, subs[1].Name, "counterpart's row names the requester")
			assert.Equal(t, tt.wantOther, subs[1].RoomType)
		})
	}
}

// Only a row facing a real app is soft-unsubscribable; the bot's own row is not.
func TestBuildDMPairSubs_IsSubscribed(t *testing.T) {
	room := &model.Room{ID: "r1", SiteID: "site-A", Type: model.RoomTypeBotDM}
	subs := buildDMPairSubs(
		&model.User{ID: "u_a", Account: "alice"},
		&model.User{ID: "u_w", Account: "weather.bot"}, room, time.Now().UTC())

	assert.True(t, subs[0].IsSubscribed, "alice faces an app")
	assert.False(t, subs[1].IsSubscribed, "the bot faces a person")
}

func TestDetermineRoomTypeFromPayload_BotRequesterYieldsBotDM(t *testing.T) {
	req := model.CreateRoomRequest{RequesterAccount: "weather.bot", Users: []string{"alice"}}
	assert.Equal(t, model.RoomTypeBotDM, determineRoomTypeFromPayload(&req),
		"either participant being a bot makes the ROOM a botDM")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=room-worker`
Expected: FAIL — `undefined: buildDMPairSubs`, and `TestDetermineRoomTypeFromPayload_BotRequesterYieldsBotDM` reports `dm`

- [ ] **Step 3: Write minimal implementation**

In `room-worker/handler.go`, give `newSub` a `roomType` parameter:

```go
// newSub constructs a Subscription. roomType is the room as THIS subscriber
// sees it, which on a bot<->human DM differs from the room document's type.
func newSub(id string, user *model.User, room *model.Room, roles []model.Role,
	roomType model.RoomType, name string, isSubscribed bool, joinedAt time.Time) *model.Subscription {
```

and inside it replace `RoomType: room.Type,` with `RoomType: roomType,`.

Replace `buildDMSubs` and `buildBotDMSubs` with one builder:

```go
// buildDMPairSubs returns the two rows of a DM pair. Each names its counterpart
// and stores the type that counterpart implies; only a row facing a real app is
// soft-unsubscribable.
func buildDMPairSubs(requester, other *model.User, room *model.Room, acceptedAt time.Time) []*model.Subscription {
	requesterType := model.SubscriptionRoomType(other.Account)
	otherType := model.SubscriptionRoomType(requester.Account)
	return []*model.Subscription{
		preRead(newSub(idgen.GenerateUUIDv7(), requester, room, nil, requesterType,
			other.Account, requesterType == model.RoomTypeBotDM, acceptedAt), acceptedAt),
		newSub(idgen.GenerateUUIDv7(), other, room, nil, otherType,
			requester.Account, otherType == model.RoomTypeBotDM, acceptedAt),
	}
}
```

At both call sites, replace the `if roomType == model.RoomTypeBotDM { … } else { … }` selection with the single call. At `~:1645`:

```go
		subs := buildDMPairSubs(requester, counterpart, room, acceptedAt)
```

At `~:2101`:

```go
	subs := buildDMPairSubs(requester, other, room, joinedAt)
```

Update `buildSelfDMSub` and the channel-path `newSub` calls (`~:983`, `~:1373`, `~:1376`) to pass `room.Type` as the new argument — a channel or self-DM has no counterpart asymmetry.

In `determineRoomTypeFromPayload`, classify from both participants:

```go
// determineRoomTypeFromPayload mirrors room-service's determineRoomType: a DM is
// a botDM when either participant is a ".bot" app.
func determineRoomTypeFromPayload(req *model.CreateRoomRequest) model.RoomType {
	if req.Name == "" && len(req.Orgs) == 0 && len(req.Channels) == 0 && len(req.Users) == 1 {
		return model.DMRoomType(req.RequesterAccount, req.Users[0])
	}
	return model.RoomTypeChannel
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=room-worker`
Expected: PASS. `TestProcessCreateRoom_DM_BuildsTwoSubs` must still pass unchanged — a human pair is unaffected.

- [ ] **Step 5: Commit**

```bash
git add room-worker/handler.go room-worker/handler_test.go
git commit -m "feat(room-worker): store the room type each subscriber sees"
```

---

### Task 3: room-service classifies from both participants

**Files:**
- Modify: `room-service/helper.go` — `determineRoomType` (~:178); `room-service/handler.go` — the app gate (~:294)
- Test: `room-service/helper_test.go`, `room-service/handler_test.go`

**Interfaces:**
- Consumes: `model.DMRoomType` from Task 1
- Produces: no new symbols

- [ ] **Step 1: Write the failing test**

In `room-service/helper_test.go`, extend the `TestDetermineRoomType` table with:

```go
		{
			name: "bot requester with a human counterpart → botDM",
			req:  model.CreateRoomRequest{RequesterAccount: "weather.bot", Users: []string{"alice"}},
			want: model.RoomTypeBotDM,
		},
		{
			name: "two bots → botDM",
			req:  model.CreateRoomRequest{RequesterAccount: "weather.bot", Users: []string{"sales.bot"}},
			want: model.RoomTypeBotDM,
		},
```

Append to `room-service/handler_test.go`:

```go
// The gate stops a user opening a DM with a missing or disabled app. A bot
// initiating is already authenticated and has no app to validate on the far side.
func TestBotRequesterSkipsAppAvailabilityGate(t *testing.T) {
	assert.True(t, skipAppGate("weather.bot"), "bot requester skips the gate")
	assert.False(t, skipAppGate("alice"), "a user requester is still gated")
	assert.False(t, skipAppGate("p_adminsiteA"), "p_admin is not a .bot app")
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=room-service`
Expected: FAIL — the bot-requester row reports `dm`, and `undefined: skipAppGate`

- [ ] **Step 3: Write minimal implementation**

In `room-service/helper.go`:

```go
// determineRoomType classifies a post-strip request; caller must guarantee
// non-empty input. A DM is a botDM when either participant is a ".bot" app.
func determineRoomType(req *model.CreateRoomRequest) model.RoomType {
	if req.Name == "" && len(req.Orgs) == 0 && len(req.Channels) == 0 && len(req.Users) == 1 {
		return model.DMRoomType(req.RequesterAccount, req.Users[0])
	}
	return model.RoomTypeChannel
}

// skipAppGate reports whether the bot-availability check should be bypassed: a
// bot initiating a DM has no app to validate on the counterpart side.
func skipAppGate(requesterAccount string) bool { return model.IsBot(requesterAccount) }
```

In `room-service/handler.go`, guard the gate:

```go
	if roomType == model.RoomTypeBotDM && !skipAppGate(requester.Account) {
		app, err := h.store.GetApp(ctx, other.Account)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=room-service`
Expected: PASS, including the existing `errBotNotAvailable` cases for a user requester.

- [ ] **Step 5: Commit**

```bash
git add room-service/helper.go room-service/handler.go room-service/helper_test.go room-service/handler_test.go
git commit -m "feat(room-service): classify DMs from both participants"
```

---

### Task 4: inbox-worker derives the cross-site row's type

**Files:**
- Modify: `inbox-worker/handler.go` — the subscription build (~:296-310)
- Test: `inbox-worker/handler_test.go`

**Interfaces:**
- Consumes: `model.SubscriptionRoomType` from Task 1
- Produces: no new symbols

- [ ] **Step 1: Write the failing test**

Append to `inbox-worker/handler_test.go`:

```go
// A federated DM row stores the room as ITS OWN subscriber sees it, derived from
// the counterpart the row is named after. Channels are unaffected.
func TestSubscriptionRowType(t *testing.T) {
	tests := []struct {
		name      string
		roomType  model.RoomType
		roomName  string
		requester string
		want      model.RoomType
	}{
		{"human targeted by a bot", model.RoomTypeBotDM, "", "weather.bot", model.RoomTypeBotDM},
		{"bot targeted by a human", model.RoomTypeBotDM, "", "alice", model.RoomTypeDM},
		{"human targeted by a human", model.RoomTypeDM, "", "alice", model.RoomTypeDM},
		{"platform admin counterpart", model.RoomTypeBotDM, "", "p_adminsiteA", model.RoomTypeDM},
		{"channel keeps the room type", model.RoomTypeChannel, "eng", "alice", model.RoomTypeChannel},
		{"discussion keeps the room type", model.RoomTypeDiscussion, "eng", "alice", model.RoomTypeDiscussion},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name := subscriptionName(tt.roomType, tt.roomName, tt.requester)
			assert.Equal(t, tt.want, subscriptionRowType(tt.roomType, name))
		})
	}
}

// The flag is written from the row's own type, so a human facing an app is
// subscribed and a bot facing a person is not — unchanged from before.
func TestSubscriptionIsSubscribed_UsesRowType(t *testing.T) {
	human := model.User{Account: "alice"}
	bot := model.User{Account: "weather.bot"}
	assert.True(t, subscriptionIsSubscribed(model.RoomTypeBotDM, &human))
	assert.False(t, subscriptionIsSubscribed(model.RoomTypeDM, &bot))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=inbox-worker`
Expected: FAIL — `undefined: subscriptionRowType`

- [ ] **Step 3: Write minimal implementation**

In `inbox-worker/handler.go`, add beside `subscriptionName`:

```go
// subscriptionRowType is the room as this row's own subscriber sees it. Only DMs
// are asymmetric; a channel row keeps the room's type.
func subscriptionRowType(roomType model.RoomType, name string) model.RoomType {
	if roomType == model.RoomTypeDM || roomType == model.RoomTypeBotDM {
		return model.SubscriptionRoomType(name)
	}
	return roomType
}
```

Then in the subscription build (~:296), hoist the name and derive both fields from it:

```go
		name := subscriptionName(roomType, event.RoomName, event.RequesterAccount)
		subType := subscriptionRowType(roomType, name)
		sub := &model.Subscription{
			ID:                 idgen.GenerateUUIDv7(),
			User:               model.SubscriptionUser{ID: user.ID, Account: user.Account},
			RoomID:             event.RoomID,
			RoomType:           subType,
			SiteID:             event.SiteID,
			Roles:              rolesForType(roomType),
			Name:               name,
			IsSubscribed:       subscriptionIsSubscribed(subType, &user),
```

Leave the remaining fields of the literal unchanged.

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=inbox-worker`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add inbox-worker/handler.go inbox-worker/handler_test.go
git commit -m "feat(inbox-worker): derive the federated row's type from its counterpart"
```

---

### Task 5: bot-room-service writes roomType and name

**Files:**
- Modify: `bot-room-service/store.go` — the `Subscription` struct (~:83); `bot-room-service/store_mongo.go` — `UpsertSubscription` (~:100); `bot-room-service/handler.go` — the two upserts in `handleEnsureDM` (~:143-158)
- Test: `bot-room-service/handler_test.go`

**Interfaces:**
- Consumes: `model.SubscriptionRoomType` from Task 1
- Produces: `Subscription` gains `RoomType model.RoomType` and `Name string`

- [ ] **Step 1: Write the failing test**

Append to `bot-room-service/handler_test.go`. It already provides `fakeStore` (with `UpsertSubscriptionFn`), `ident()` (account `myapp.bot`), `withIdentity`, `newHandler`, `testKeyStore`, `testKeySender` and `captureOutboxPayload` — reuse them:

```go
// dm.ensure wrote neither roomType nor name, so the rows matched no list bucket
// and the DM was invisible to both parties.
func TestHandleDMEnsure_WritesRoomTypeAndName(t *testing.T) {
	var upserted []*Subscription
	store := &fakeStore{
		InsertRoomFn: func(_ context.Context, _ *Room) error { return nil },
		UpsertSubscriptionFn: func(_ context.Context, s *Subscription) (bool, error) {
			upserted = append(upserted, s)
			return true, nil
		},
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "alice", SiteID: "site-a"}, nil
		},
	}
	h := newHandler(store, "site-a", nil, (&captureOutboxPayload{}).publish, testKeyStore, testKeySender)

	_, err := h.handleDMEnsure(withIdentity(t, "", ident()), BotDMEnsureRequest{TargetUserID: "alice-id"})
	require.NoError(t, err)

	require.Len(t, upserted, 2)

	bot := upserted[0]
	assert.Equal(t, "myapp.bot", bot.Account)
	assert.Equal(t, "alice", bot.Name, "the bot's row names the person")
	assert.Equal(t, model.RoomTypeDM, bot.RoomType, "the bot faces a person")

	target := upserted[1]
	assert.Equal(t, "alice", target.Account)
	assert.Equal(t, "myapp.bot", target.Name, "the person's row names the bot")
	assert.Equal(t, model.RoomTypeBotDM, target.RoomType, "the person faces an app")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=bot-room-service`
Expected: FAIL — `bot.Name` and `bot.RoomType` are empty

- [ ] **Step 3: Write minimal implementation**

In `bot-room-service/store.go`, extend the struct:

```go
type Subscription struct {
	ID        string
	RoomID    string
	UserID    string
	Account   string
	SiteID    string
	Name      string
	RoomType  model.RoomType
	CreatedAt time.Time
	IsBot     bool
}
```

In `bot-room-service/store_mongo.go`, persist them:

```go
		"$setOnInsert": bson.M{
			"_id":       sub.ID,
			"roomId":    sub.RoomID,
			"u":         bson.M{"_id": sub.UserID, "account": sub.Account, "isBot": sub.IsBot},
			"siteId":    sub.SiteID,
			"name":      sub.Name,
			"roomType":  string(sub.RoomType),
			"createdAt": sub.CreatedAt,
		},
```

In `bot-room-service/handler.go`, name each row after its counterpart:

```go
	if _, err := h.store.UpsertSubscription(c, &Subscription{
		ID: h.newUUIDv7(), RoomID: roomID, UserID: ident.ID, Account: ident.Account,
		SiteID: h.siteID, CreatedAt: createdAt, IsBot: true,
		Name: target.Account, RoomType: model.SubscriptionRoomType(target.Account),
	}); err != nil {
```

and for the same-site target:

```go
		if _, err := h.store.UpsertSubscription(c, &Subscription{
			ID: h.newUUIDv7(), RoomID: roomID, UserID: target.ID, Account: target.Account,
			SiteID: h.siteID, CreatedAt: createdAt,
			Name: ident.Account, RoomType: model.SubscriptionRoomType(ident.Account),
		}); err != nil {
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=bot-room-service`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add bot-room-service/
git commit -m "fix(bot-room-service): write roomType and name on dm.ensure rows"
```

---

### Task 6: Revert the read-side remap

**Files:**
- Modify: `user-service/mongorepo/subscriptions.go`, `user-service/mongorepo/threadsubscriptions.go`, `user-service/service/subscriptions.go`, `user-service/service/threads.go`, `user-service/service/threadunread.go`, `history-service/internal/mongorepo/pipelines.go`, `search-service/enrich.go`, `pkg/pipelines/subscription.go`, `pkg/pipelines/member.go`, `pkg/model/threadsubscription.go`
- Delete: `pkg/pipelines/subscription_approom_test.go`, `user-service/mongorepo/subscriptions_approom_test.go`
- Test: the affected `_test.go` files revert with their subjects

**Interfaces:**
- Consumes: nothing new
- Produces: `pipelines.AppRoomFilter`, `pipelines.UnsubscribedAppFilter`, `pipelines.botSuffixRegex`, `mongorepo.applyListType`, `mongorepo.dmMatch`, `model.ThreadUnreadRow.Name` all cease to exist

- [ ] **Step 1: Revert each file to its pre-PR form**

The previous design's read-side commits are `78b11f7`, `81e7374`, `2d4e1f5`, `14182f3`, `5455524` plus the read-side half of `e246138`. Rather than reverting commits (which would also undo the write-side work being kept), restore each hunk by hand against `origin/main`:

```bash
git diff origin/main -- user-service/mongorepo/subscriptions.go
```

Restore: the `switch listType` block inside `AggregateSubscriptions`, `activeSubscriptionFilter`'s inline `$or`, and `GetDMSubscription`'s `roomType:"dm"` match. Delete `applyListType`, `dmMatch`, and the now-unused `maps` and `pipelines` imports.

Do the same for: `threadsubscriptions.go` (restore the `$or` gate, drop the `name` projection and its `$arrayElemAt` lift), `service/subscriptions.go` (drop the normalize loop; switches read `subs[i].RoomType`), `service/threads.go` (drop the normalize loop), `service/threadunread.go` (`roomType: r.RoomType`), `history-service/.../pipelines.go` (restore the `$or` gate, drop the `pipelines` import), `search-service/enrich.go` (drop the normalize loop; both switches read `meta.RoomType`), `pkg/model/threadsubscription.go` (drop `ThreadUnreadRow.Name`), `pkg/pipelines/subscription.go` (drop `botSuffixRegex`, `AppRoomFilter`, `UnsubscribedAppFilter` and the `model` import), `pkg/pipelines/member.go` (restore the inline `\.bot$` literal in `botOrPseudoAccountRegex`).

Revert the tests that pinned the removed shapes: `user-service/mongorepo/subscriptions_count_test.go` (`TestActiveFilter`'s `base()` back to a literal `$or`), and delete `pkg/pipelines/subscription_approom_test.go` and `user-service/mongorepo/subscriptions_approom_test.go`.

**Keep** `model.IsAppRoom` and `model.EffectiveRoomType`, both `subscription.update` stamps, `resolveSubUpdateCounterpart`'s `IsAppRoom` call, and `tools/loadgen`'s `isSoakRoomMember`.

- [ ] **Step 2: Verify the reverted filters match main exactly**

Run:
```bash
git diff origin/main -- user-service/mongorepo/subscriptions.go user-service/mongorepo/threadsubscriptions.go history-service/internal/mongorepo/pipelines.go search-service/enrich.go pkg/pipelines/
```
Expected: empty. Any remaining hunk is an incomplete revert.

- [ ] **Step 3: Restore the test fixtures the gate change had forced**

The fixture corrections stay — they describe production shapes and are correct either way. But the assertions that depended on the widened gate revert:

- `user-service/mongorepo/threadsubscriptions_test.go` — `TestThreadSubscriptionRepo_ListByAccount_MembershipAndAppGate` drops the `trAdm` row and returns to `require.Len(t, rows, 2)`.
- `history-service/internal/mongorepo/threadsubscription_test.go` — delete `TestThreadSubscriptionRepo_ListUserThreadSubscriptions_NonAppBotDMSurvivesGate` and `…_PlatformAdminDMSurvivesGate`; both asserted the widened gate.
- `user-service/service/subscriptions_test.go` and `threads_test.go` — the `p_admin` and bot-viewer render cases now describe rows written as `dm`, so set their `RoomType` to `model.RoomTypeDM` and keep the `hrInfo` assertions.
- `user-service/service/threadunread_test.go` — same: the non-app rows carry `RoomType: model.RoomTypeDM`.

- [ ] **Step 4: Run the full unit suite**

Run: `make test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: read the stored room type directly"
```

---

### Task 7: End-to-end verification of every scenario

**Files:**
- Create: `room-worker/roomtype_e2e_test.go`
- Test: same file

**Interfaces:**
- Consumes: everything from Tasks 1-6; `newCreateRoomTestHandler`, `makeCreateRoomBody`, `subscriptionUpdates`, `publishedMsg` from `room-worker/handler_test.go`; `subject.SubscriptionUpdate`
- Produces: no new symbols

- [ ] **Step 1: Write the test**

Create `room-worker/roomtype_e2e_test.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

type dmOutcome struct {
	storedType model.RoomType
	evtType    model.RoomType
	hasHR      bool
	hasApp     bool
}

// createDMAndCapture drives processCreateRoom and returns each account's stored
// row and the subscription.update it received.
func createDMAndCapture(t *testing.T, requesterAcct, counterpartAcct, roomID string, hasApp bool) map[string]dmOutcome {
	t.Helper()
	h, mockStore, getPublished := newCreateRoomTestHandler(t)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	requester := &model.User{ID: "u_" + requesterAcct, Account: requesterAcct, EngName: "R Eng", ChineseName: "請求", SiteID: "site-A"}
	other := &model.User{ID: "u_" + counterpartAcct, Account: counterpartAcct, EngName: "O Eng", ChineseName: "對方", SiteID: "site-A"}

	mockStore.EXPECT().GetUser(gomock.Any(), requesterAcct).Return(requester, nil)
	mockStore.EXPECT().GetUser(gomock.Any(), counterpartAcct).Return(other, nil)
	mockStore.EXPECT().CreateRoom(gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil)
	if hasApp {
		mockStore.EXPECT().GetApp(gomock.Any(), gomock.Any()).
			Return(&model.App{ID: "app1", Name: "Weather", Assistant: &model.AppAssistant{Name: counterpartAcct}}, nil).AnyTimes()
	}

	var captured []*model.Subscription
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, subs []*model.Subscription) error { captured = subs; return nil })
	mockStore.EXPECT().FindDMSubscriptionPair(gomock.Any(), roomID, requesterAcct).
		DoAndReturn(func(_ context.Context, _, _ string) (*model.Subscription, *model.Subscription, error) {
			return captured[0], captured[1], nil
		})
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)

	body := makeCreateRoomBody(t, &model.CreateRoomRequest{
		RoomID: roomID, RequesterAccount: requesterAcct,
		Users: []string{counterpartAcct}, Timestamp: time.Now().UnixMilli(),
	})
	require.NoError(t, h.processCreateRoom(ctx, body))

	out := map[string]dmOutcome{}
	for _, sub := range captured {
		out[sub.User.Account] = dmOutcome{storedType: sub.RoomType}
	}
	for _, p := range subscriptionUpdates(getPublished()) {
		var evt model.SubscriptionUpdateEvent
		require.NoError(t, json.Unmarshal(p.data, &evt))
		acct := evt.Subscription.User.Account
		o := out[acct]
		o.evtType, o.hasHR, o.hasApp = evt.Subscription.RoomType, evt.HRInfo != nil, evt.AppInfo != nil
		out[acct] = o
	}
	return out
}

func TestE2E_DMScenarios(t *testing.T) {
	tests := []struct {
		name             string
		requester, other string
		roomID           string
		hasApp           bool
		want             map[string]dmOutcome
	}{
		{
			name: "user creates a DM with a bot", requester: "alice", other: "weather.bot",
			roomID: "r1", hasApp: true,
			want: map[string]dmOutcome{
				"alice":       {model.RoomTypeBotDM, model.RoomTypeBotDM, false, true},
				"weather.bot": {model.RoomTypeDM, model.RoomTypeDM, true, false},
			},
		},
		{
			name: "bot creates a DM with a user", requester: "weather.bot", other: "alice",
			roomID: "r2", hasApp: true,
			want: map[string]dmOutcome{
				"weather.bot": {model.RoomTypeDM, model.RoomTypeDM, true, false},
				"alice":       {model.RoomTypeBotDM, model.RoomTypeBotDM, false, true},
			},
		},
		{
			name: "two humans", requester: "alice", other: "bob", roomID: "r3",
			want: map[string]dmOutcome{
				"alice": {model.RoomTypeDM, model.RoomTypeDM, true, false},
				"bob":   {model.RoomTypeDM, model.RoomTypeDM, true, false},
			},
		},
		{
			name: "user and platform admin", requester: "alice", other: "p_adminsiteA", roomID: "r4",
			want: map[string]dmOutcome{
				"alice":        {model.RoomTypeDM, model.RoomTypeDM, true, false},
				"p_adminsiteA": {model.RoomTypeDM, model.RoomTypeDM, true, false},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := createDMAndCapture(t, tt.requester, tt.other, tt.roomID, tt.hasApp)
			for acct, want := range tt.want {
				assert.Equal(t, want.storedType, got[acct].storedType, "%s: stored roomType", acct)
				assert.Equal(t, want.evtType, got[acct].evtType, "%s: event roomType", acct)
				assert.Equal(t, want.storedType, got[acct].evtType, "%s: event must equal stored", acct)
				assert.Equal(t, want.hasHR, got[acct].hasHR, "%s: hrInfo", acct)
				assert.Equal(t, want.hasApp, got[acct].hasApp, "%s: appInfo", acct)
			}
		})
	}
}
```

- [ ] **Step 2: Run it**

Run: `make test SERVICE=room-worker`
Expected: PASS, all four scenarios.

- [ ] **Step 3: Commit**

```bash
git add room-worker/roomtype_e2e_test.go
git commit -m "test(room-worker): end-to-end room type per DM scenario"
```

---

### Task 8: Integration proof that the original filters serve the bot

**Files:**
- Modify: `user-service/mongorepo/subscriptions_test.go` (`//go:build integration`)

**Interfaces:**
- Consumes: `newTestSubscriptionRepo`, `seed` from `user-service/mongorepo/setup_test.go`

- [ ] **Step 1: Replace the previous partition test**

Delete `TestAggregateSubscriptions_AppRoomPartition` (it asserted the widened buckets) and add:

```go
// Rows written with the per-subscriber type are served by the ORIGINAL filters:
// the bot's row is dm, so the current branch — which carries no isSubscribed
// condition — admits it.
func TestAggregateSubscriptions_PerSubscriberRoomType(t *testing.T) {
	r, db := newTestSubscriptionRepo(t)
	ctx := context.Background()
	now := time.Now().UTC()

	seed(t, db, "rooms",
		bson.M{"_id": "r-bot-alice", "siteId": "site-a", "userCount": 2, "lastMsgAt": now},
		bson.M{"_id": "r-sales", "siteId": "site-a", "userCount": 2, "lastMsgAt": now},
	)
	seed(t, db, "subscriptions",
		// the bot's own row: dm, isSubscribed false — admitted anyway
		bson.M{"_id": "s1", "u": bson.M{"_id": "u-w", "account": "weather.bot"}, "roomId": "r-bot-alice",
			"name": "alice", "roomType": "dm", "siteId": "site-a", "isSubscribed": false, "createdAt": now},
		// alice's row: botDM, subscribed
		bson.M{"_id": "s2", "u": bson.M{"_id": "u-a", "account": "alice"}, "roomId": "r-bot-alice",
			"name": "weather.bot", "roomType": "botDM", "siteId": "site-a", "isSubscribed": true, "createdAt": now},
		// alice unsubscribed from another app — stays hidden
		bson.M{"_id": "s3", "u": bson.M{"_id": "u-a", "account": "alice"}, "roomId": "r-sales",
			"name": "sales.bot", "roomType": "botDM", "siteId": "site-a", "isSubscribed": false, "createdAt": now},
	)

	roomIDs := func(account, listType string) []string {
		page, err := r.AggregateSubscriptions(ctx, account, listType, false, nil,
			mongoutil.OffsetPageRequest{Offset: 0, Limit: 100})
		require.NoError(t, err)
		out := make([]string, 0, len(page.Data))
		for i := range page.Data {
			out = append(out, page.Data[i].RoomID)
		}
		return out
	}

	assert.ElementsMatch(t, []string{"r-bot-alice"}, roomIDs("weather.bot", "rooms"),
		"the bot's DM is an ordinary chat")
	assert.ElementsMatch(t, []string{"r-bot-alice"}, roomIDs("weather.bot", "current"))
	assert.Empty(t, roomIDs("weather.bot", "apps"), "never in the App section")
	assert.ElementsMatch(t, []string{"r-bot-alice"}, roomIDs("alice", "apps"),
		"alice's subscribed app is in the App section")
	assert.Empty(t, roomIDs("alice", "rooms"), "alice has no plain chats here")
	assert.ElementsMatch(t, []string{"r-bot-alice"}, roomIDs("alice", "current"),
		"the unsubscribed sales.bot app stays hidden")
}
```

- [ ] **Step 2: Run it**

Run: `make test-integration SERVICE=user-service`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add user-service/mongorepo/subscriptions_test.go
git commit -m "test(user-service): original filters serve per-subscriber rows"
```

---

### Task 9: Documentation

**Files:**
- Modify: `docs/client-api.md`, `docs/client-api/request-reply.md`, `docs/client-api/events.md`

- [ ] **Step 1: Rewrite the effective-room-type section**

Replace the `#### Effective room type` section in `docs/client-api.md` with a per-subscriber statement, keeping the heading and anchor so the existing links resolve:

> `roomType` is the room's type **as seen by the requesting subscriber**, and it is stored that way. A DM between a person and an app is `botDM` on the person's subscription and `dm` on the app's own, because each side records the counterpart it faces. The room document keeps a single type (`botDM` when either participant is a `.bot` app). A bot signed into the client therefore sees its DMs with people as ordinary chats, and `subscription.update` always reports the same `roomType` the subscription stores.

Remove the paragraph describing the `isSubscribed` gate exemption — the gate is unchanged now — and remove the `search.rooms` exclusion note, which only existed because the type was derived at read time.

- [ ] **Step 2: Update the room-creation classification line**

In `docs/client-api.md`, the `name` empty + one `users` entry bullet becomes:

> - `name` empty + exactly one entry in `users` → `dm`, or `botDM` when **either** participant is a `.bot` bot. `p_admin` and QA `p_` accounts are ordinary users, so both yield a regular `dm`

Mirror the same sentence in `docs/client-api/request-reply.md`.

- [ ] **Step 3: Check the derived views**

Run: `grep -n "effective room type\|Effective room type" docs/client-api.md docs/client-api/request-reply.md docs/client-api/events.md`
Expected: every hit reads as a per-subscriber statement; no reference claims the type is derived at read time.

- [ ] **Step 4: Commit**

```bash
git add docs/client-api.md docs/client-api/request-reply.md docs/client-api/events.md
git commit -m "docs: room type is stored per subscriber"
```

---

### Task 10: Full verification

- [ ] **Step 1: Format and lint**

Run: `make fmt` then `make lint`
Expected: 0 issues.

- [ ] **Step 2: Regenerate mocks**

Run: `make generate`
Expected: a diff only if a store interface changed — Task 5 changes `bot-room-service`'s `Subscription` struct but not its interface, so expect none. Commit any diff.

- [ ] **Step 3: Full unit suite**

Run: `make test`
Expected: PASS

- [ ] **Step 4: Integration suites**

Run each: `make test-integration SERVICE=user-service`, `SERVICE=history-service`, `SERVICE=room-service`, `SERVICE=room-worker`, `SERVICE=search-service`, `SERVICE=bot-room-service`, `SERVICE=inbox-worker`
Expected: PASS. `pkg/roomcrypto` needs npm inside a container and fails in a network-restricted environment — that is environmental, not a regression.

- [ ] **Step 5: SAST**

Run: `make sast`
Expected: gosec, govulncheck and semgrep all pass.

- [ ] **Step 6: Commit any fixes**

```bash
git add -A
git commit -m "chore: lint and format fixes"
```
