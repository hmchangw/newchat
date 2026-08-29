# botDM Effective Room Type Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `botDM` is an app room only when its counterpart account carries a `.bot` suffix; every other `botDM` renders as `dm` with `hrInfo` and escapes the `isSubscribed` gate, so a bot sees its DMs with humans as ordinary chats and a user's `p_admin` DM leaves the App section.

**Architecture:** One predicate, expressed twice — as Go helpers in `pkg/model` for render paths, and as reusable `bson.M` fragments in `pkg/pipelines` for Mongo filters. Every read gate and every render switch in user-service, history-service and search-service is rewritten in terms of those two. Separately, `room-service` and `room-worker` stop classifying `p_admin` DMs as `botDM` at creation time.

**Tech Stack:** Go 1.25, MongoDB (`go.mongodb.org/mongo-driver/v2`), `go.uber.org/mock`, `stretchr/testify`, `testcontainers-go`.

**Spec:** `docs/superpowers/specs/2026-08-29-botdm-effective-room-type-design.md`

## Global Constraints

- Run `make` targets only — never raw `go` commands. `make test SERVICE=<name>`, `make lint`, `make fmt`, `make generate SERVICE=<name>`.
- All tests run with `-race` (the Makefile handles it).
- Test files live in the same package as the code under test. Integration tests carry `//go:build integration` and use `pkg/testutil` containers.
- Minimum 80% coverage per package; target 90%+ for `pkg/` and handlers.
- Error wrapping: `fmt.Errorf("short description: %w", err)`. Never a bare `err`.
- Structured logging via `log/slog` only, key-value pairs, never interpolated.
- No new third-party dependencies.
- The counterpart account is the subscription's `name` field (bson `name`), which holds the bot account on the human's row and the human account on the bot's row.
- `p_admin` stays bot-like everywhere outside DM classification: `filterBots`, `errBotInChannel`, the role rules and `newSub`'s `u.isBot` flag are NOT touched.
- Commit after each task, once its tests pass.

---

### Task 1: The predicate in `pkg/model`

**Files:**
- Modify: `pkg/model/subscription.go` (append after `IsRoomMember`, around line 227)
- Test: `pkg/model/model_test.go`

**Interfaces:**
- Consumes: `model.IsBot(account string) bool` (`pkg/model/user.go:175`), `model.RoomTypeBotDM` / `RoomTypeDM` (`pkg/model/room.go:8-12`)
- Produces: `model.IsAppRoom(t RoomType, name string) bool`, `model.EffectiveRoomType(t RoomType, name string) RoomType`

- [ ] **Step 1: Write the failing test**

Append to `pkg/model/model_test.go`:

```go
func TestEffectiveRoomType(t *testing.T) {
	tests := []struct {
		name      string
		roomType  model.RoomType
		counter   string
		wantType  model.RoomType
		wantIsApp bool
	}{
		{"botDM with bot counterpart is an app room", model.RoomTypeBotDM, "weather.bot", model.RoomTypeBotDM, true},
		{"botDM with human counterpart renders as dm", model.RoomTypeBotDM, "alice", model.RoomTypeDM, false},
		{"botDM with p_admin counterpart renders as dm", model.RoomTypeBotDM, "p_admin_ops", model.RoomTypeDM, false},
		{"botDM with QA p_ counterpart renders as dm", model.RoomTypeBotDM, "p_qa_bob", model.RoomTypeDM, false},
		{"botDM with empty counterpart renders as dm", model.RoomTypeBotDM, "", model.RoomTypeDM, false},
		{"dm is unchanged", model.RoomTypeDM, "alice", model.RoomTypeDM, false},
		{"channel is unchanged", model.RoomTypeChannel, "", model.RoomTypeChannel, false},
		{"discussion is unchanged", model.RoomTypeDiscussion, "", model.RoomTypeDiscussion, false},
		{"a bot-named channel is still a channel", model.RoomTypeChannel, "weather.bot", model.RoomTypeChannel, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantType, model.EffectiveRoomType(tt.roomType, tt.counter))
			assert.Equal(t, tt.wantIsApp, model.IsAppRoom(tt.roomType, tt.counter))
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=model` (or `go test ./pkg/model/ -run TestEffectiveRoomType` if the Makefile has no `model` target — check `make test` first)
Expected: FAIL — `undefined: model.EffectiveRoomType`, `undefined: model.IsAppRoom`

- [ ] **Step 3: Write minimal implementation**

Append to `pkg/model/subscription.go`:

```go
// IsAppRoom reports whether a room of type t whose counterpart account is name
// is an app room — a botDM facing a real ".bot" app. A botDM facing anything
// else (a human, or the p_admin platform-admin pseudo-account) is an ordinary
// DM from the subscriber's point of view: the bot side of a bot↔human DM, and
// every side of a p_admin DM.
func IsAppRoom(t RoomType, name string) bool {
	return t == RoomTypeBotDM && IsBot(name)
}

// EffectiveRoomType is the room type a subscription is presented to its own
// subscriber as. A botDM that is not an app room renders as dm; every other
// type is returned unchanged. name is the subscription's per-subscriber display
// name, which holds the counterpart account for dm and botDM rows.
func EffectiveRoomType(t RoomType, name string) RoomType {
	if t == RoomTypeBotDM && !IsBot(name) {
		return RoomTypeDM
	}
	return t
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=model`
Expected: PASS, all 9 subtests

- [ ] **Step 5: Commit**

```bash
git add pkg/model/subscription.go pkg/model/model_test.go
git commit -m "feat(model): add IsAppRoom and EffectiveRoomType predicates"
```

---

### Task 2: The Mongo filter fragments in `pkg/pipelines`

**Files:**
- Modify: `pkg/pipelines/subscription.go` (append; the file already imports `bson`)
- Test: `pkg/pipelines/subscription_approom_test.go` (create)

**Interfaces:**
- Consumes: `model.PlatformAdminAccountPrefix()` is NOT used here — the fragments key on the `.bot` suffix only, matching `model.IsBot`.
- Produces: `pipelines.AppRoomFilter() bson.M`, `pipelines.NonAppRoomFilter() bson.M`

- [ ] **Step 1: Write the failing test**

Create `pkg/pipelines/subscription_approom_test.go`:

```go
package pipelines

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// The two fragments must partition botDM rows exactly as model.IsBot does, so a
// query and the Go render path can never disagree about which rows are apps.
func TestAppRoomFilterShape(t *testing.T) {
	app := AppRoomFilter()
	assert.Equal(t, "botDM", app["roomType"], "app rooms are botDM rows")

	nonApp := NonAppRoomFilter()
	assert.Equal(t, "botDM", nonApp["roomType"], "non-app rooms are botDM rows too")

	name, ok := nonApp["name"].(bson.M)
	require.True(t, ok, "the name clause must be a bson.M")
	_, negated := name["$not"]
	assert.True(t, negated, "NonAppRoomFilter must negate the bot-suffix match")
}

// The regex the fragments embed must agree with model.IsBot on every account
// shape the system produces.
func TestAppRoomFilterRegexMatchesIsBot(t *testing.T) {
	re := regexp.MustCompile(botSuffixRegex())
	for _, tc := range []struct {
		account string
		isBot   bool
	}{
		{"weather.bot", true},
		{"weather.site-a.bot", true},
		{"alice", false},
		{"p_admin_ops", false},
		{"p_qa_bob", false},
		{"bot", false},
		{"robot", false},
		{"a.bot.b", false},
		{"", false},
	} {
		t.Run(tc.account, func(t *testing.T) {
			assert.Equal(t, tc.isBot, re.MatchString(tc.account))
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pipelines`
Expected: FAIL — `undefined: AppRoomFilter`, `undefined: NonAppRoomFilter`, `undefined: botSuffixRegex`

- [ ] **Step 3: Write minimal implementation**

Append to `pkg/pipelines/subscription.go`:

```go
// botSuffixRegex is the wire-side equivalent of model.IsBot: it matches the
// ".bot" account suffix and nothing else. Unlike botOrPseudoAccountRegex it
// deliberately excludes the platform-admin prefix — a p_admin DM is an ordinary
// DM, not an app room.
func botSuffixRegex() string { return `\.bot$` }

// AppRoomFilter matches subscription rows that are app rooms: a botDM facing a
// real ".bot" app. It is the wire-side twin of model.IsAppRoom, and the only
// shape the isSubscribed soft-unsubscribe gate still applies to.
//
// The regex is never the index-driving term — every call site leads with a
// selective u.account match or an (u.account, roomId) point read — so it is
// evaluated only over candidate documents.
func AppRoomFilter() bson.M {
	return bson.M{"roomType": string(model.RoomTypeBotDM),
		"name": bson.M{"$regex": botSuffixRegex()}}
}

// NonAppRoomFilter matches botDM rows that are NOT app rooms — the bot's own
// side of a bot↔human DM, and either side of a p_admin DM. These rows render as
// dm and are exempt from the isSubscribed gate.
func NonAppRoomFilter() bson.M {
	return bson.M{"roomType": string(model.RoomTypeBotDM),
		"name": bson.M{"$not": bson.M{"$regex": botSuffixRegex()}}}
}
```

Add `"github.com/hmchangw/chat/pkg/model"` to the file's import block.

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=pipelines`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/pipelines/subscription.go pkg/pipelines/subscription_approom_test.go
git commit -m "feat(pipelines): add app-room / non-app-room subscription filters"
```

---

### Task 3: user-service list buckets and badge filter

**Files:**
- Modify: `user-service/mongorepo/subscriptions.go:274-285` (`AggregateSubscriptions` match), `:769-781` (`activeSubscriptionFilter`)
- Test: `user-service/mongorepo/subscriptions_approom_test.go` (create)

**Interfaces:**
- Consumes: `pipelines.AppRoomFilter()`, `pipelines.NonAppRoomFilter()` from Task 2
- Produces: no new exported symbols; `listTypeMatch(listType string) bson.M` unexported helper used by both call sites

- [ ] **Step 1: Write the failing test**

Create `user-service/mongorepo/subscriptions_approom_test.go`:

```go
package mongorepo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// hasNonAppBranch reports whether an $or branch list admits non-app botDM rows
// (the bot's own side of a human DM, and p_admin DMs) with no isSubscribed gate.
func hasNonAppBranch(t *testing.T, branches bson.A) bool {
	t.Helper()
	for _, b := range branches {
		m, ok := b.(bson.M)
		if !ok || m["roomType"] != "botDM" {
			continue
		}
		name, ok := m["name"].(bson.M)
		if !ok {
			continue
		}
		if _, negated := name["$not"]; negated {
			_, gated := m["isSubscribed"]
			assert.False(t, gated, "non-app botDM rows must not be gated on isSubscribed")
			return true
		}
	}
	return false
}

func TestListTypeMatch_AdmitsNonAppBotDMs(t *testing.T) {
	for _, listType := range []string{"current", "rooms"} {
		t.Run(listType, func(t *testing.T) {
			m := listTypeMatch(listType)
			branches, ok := m["$or"].(bson.A)
			require.True(t, ok, "%s must use an $or over room-type branches", listType)
			assert.True(t, hasNonAppBranch(t, branches),
				"%s must admit the bot's own botDM rows, which carry isSubscribed=false", listType)
		})
	}
}

// The App section must keep holding only real apps: an unsubscribed .bot app
// stays hidden, and a bot's human DM never appears there.
func TestListTypeMatch_AppsKeepsSubscribedGate(t *testing.T) {
	m := listTypeMatch("apps")
	assert.Equal(t, true, m["isSubscribed"], "apps must still require isSubscribed")
	assert.Equal(t, "botDM", m["roomType"])
	name, ok := m["name"].(bson.M)
	require.True(t, ok, "apps must constrain the counterpart name")
	assert.Equal(t, `\.bot$`, name["$regex"], "apps admit only .bot counterparts")
}

// The badge count and the list must select identical rows, or a client folding
// its badge from the list can never reconcile with the server's count.
func TestActiveSubscriptionFilter_MatchesCurrentBucket(t *testing.T) {
	branches, ok := activeSubscriptionFilter("alice")["$or"].(bson.A)
	require.True(t, ok)
	assert.True(t, hasNonAppBranch(t, branches),
		"the badge filter must admit the same non-app botDM rows the list does")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=user-service`
Expected: FAIL — `undefined: listTypeMatch`, and `TestActiveSubscriptionFilter_MatchesCurrentBucket` fails because the current `$or` has no negated-name branch

- [ ] **Step 3: Write minimal implementation**

In `user-service/mongorepo/subscriptions.go`, add the shared helper (place it directly above `AggregateSubscriptions`):

```go
// listTypeMatch is the room-type half of the subscription-list filter, shared
// with activeSubscriptionFilter so the badge count and the list can never
// select different rows.
//
// A botDM is an app room only when its counterpart carries a ".bot" suffix;
// only those rows keep the isSubscribed gate that hides an app the user
// unsubscribed from. Every other botDM — the bot's own side of a bot↔human DM,
// and either side of a p_admin DM — is an ordinary DM and rides the dm/channel
// lane, which is what puts it in the sidebar's chat section.
func listTypeMatch(listType string) bson.M {
	plain := bson.M{"roomType": bson.M{"$in": bson.A{"dm", "channel"}}}
	subscribedApp := pipelines.AppRoomFilter()
	subscribedApp["isSubscribed"] = true
	switch listType {
	case "current":
		return bson.M{"$or": bson.A{plain, subscribedApp, pipelines.NonAppRoomFilter()}}
	case "rooms":
		return bson.M{"$or": bson.A{plain, pipelines.NonAppRoomFilter()}}
	case "apps":
		return subscribedApp
	}
	return bson.M{}
}
```

Replace the `switch listType` block at `:275-285` with:

```go
	maps.Copy(match, listTypeMatch(listType))
```

(`maps` is already imported in the service layer; in `mongorepo` add `"maps"` to the import block if absent.)

In `activeSubscriptionFilter` (`:769`), replace the inline `"$or": bson.A{...}` with the shared branches:

```go
func activeSubscriptionFilter(account string) bson.M {
	filter := bson.M{"u.account": account, "muted": bson.M{"$ne": true},
		// Rooms the user closed are hidden from subscription.list, so counting
		// them here would put the two endpoints permanently out of step — and
		// a client folding its badge from the list could never reconcile.
		// Missing field (legacy docs) and open:true both pass, as in the list.
		"open": bson.M{"$ne": false}}
	maps.Copy(filter, listTypeMatch("current"))
	return filter
}
```

Add `"github.com/hmchangw/chat/pkg/pipelines"` to the import block.

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=user-service`
Expected: PASS — including the pre-existing `TestActiveSubscriptionFilter_ExcludesClosedRooms`, which must still see `open: {$ne: false}`

- [ ] **Step 5: Commit**

```bash
git add user-service/mongorepo/subscriptions.go user-service/mongorepo/subscriptions_approom_test.go
git commit -m "feat(user-service): admit non-app botDM rows into list and badge filters"
```

---

### Task 4: user-service `getDM` and thread-unread gate

**Files:**
- Modify: `user-service/mongorepo/subscriptions.go:716-719` (`GetDMSubscription` `$match`), `user-service/mongorepo/threadsubscriptions.go:60-68` (join gate)
- Test: `user-service/mongorepo/subscriptions_approom_test.go` (extend), `user-service/mongorepo/threadsubscriptions_approom_test.go` (create)

**Interfaces:**
- Consumes: `pipelines.AppRoomFilter()`, `pipelines.NonAppRoomFilter()` from Task 2
- Produces: no new exported symbols; `dmMatch(account, target string) bson.M` and `threadRoomGate() bson.M` unexported helpers

- [ ] **Step 1: Write the failing tests**

Append to `user-service/mongorepo/subscriptions_approom_test.go`:

```go
// A bot calling subscription.getDM on a human target must resolve the room. Its
// own row is stored roomType=botDM, so a hard roomType:"dm" match 404s it.
func TestDMMatch_AcceptsNonAppBotDM(t *testing.T) {
	m := dmMatch("weather.bot", "alice")
	assert.Equal(t, "weather.bot", m["u.account"])
	assert.Equal(t, "alice", m["name"])

	branches, ok := m["$or"].(bson.A)
	require.True(t, ok, "the match must accept dm OR a non-app botDM")
	var sawDM, sawNonApp bool
	for _, b := range branches {
		bm, ok := b.(bson.M)
		require.True(t, ok)
		if bm["roomType"] == "dm" {
			sawDM = true
		}
		if bm["roomType"] == "botDM" {
			name, ok := bm["name"].(bson.M)
			require.True(t, ok)
			_, negated := name["$not"]
			assert.True(t, negated, "only non-app botDMs resolve as DMs")
			sawNonApp = true
		}
	}
	assert.True(t, sawDM, "a plain dm must still resolve")
	assert.True(t, sawNonApp, "a non-app botDM must also resolve")
}
```

Create `user-service/mongorepo/threadsubscriptions_approom_test.go`:

```go
package mongorepo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// The gate drops threads in apps the user unsubscribed from, but must not drop
// a bot's threads in its own DMs with humans (those rows carry isSubscribed=false).
func TestThreadRoomGate_KeepsNonAppBotDMs(t *testing.T) {
	branches, ok := threadRoomGate()["$or"].(bson.A)
	require.True(t, ok, "the gate must be an $or")
	require.Len(t, branches, 2)

	notApp, ok := branches[0].(bson.M)
	require.True(t, ok)
	_, negated := notApp["$nor"]
	assert.True(t, negated, "the first branch must admit everything that is not an app room")

	subscribed, ok := branches[1].(bson.M)
	require.True(t, ok)
	assert.Equal(t, true, subscribed["isSubscribed"],
		"an app room still has to be subscribed to contribute threads")
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=user-service`
Expected: FAIL — `undefined: dmMatch`, `undefined: threadRoomGate`

- [ ] **Step 3: Write minimal implementation**

In `user-service/mongorepo/subscriptions.go`, add above `GetDMSubscription`:

```go
// dmMatch selects account's DM subscription with target. A bot's own side of a
// bot↔human DM is stored roomType=botDM, and a p_admin DM likewise, so a hard
// roomType:"dm" match would 404 exactly the rooms that render as DMs.
func dmMatch(account, target string) bson.M {
	return bson.M{"u.account": account, "name": target,
		"$or": bson.A{bson.M{"roomType": "dm"}, pipelines.NonAppRoomFilter()}}
}
```

Replace the `$match` stage at `:718` with `bson.M{"$match": dmMatch(account, target)}`. The `$limit: 1` short-circuit stays: `(account, name)` still identifies at most one room, since a pair has one DM room whatever its stored type. Update the adjacent comment to say so.

In `user-service/mongorepo/threadsubscriptions.go`, add above the aggregation:

```go
// threadRoomGate keeps a thread only when its room subscription still grants
// access. Unsubscribing from an app is a soft toggle (isSubscribed=false, row
// retained), unlike a room leave that purges the row — so app rooms are gated on
// isSubscribed. Non-app botDM rows (a bot's own side of a human DM, a p_admin
// DM) carry isSubscribed=false by construction and must NOT be gated.
func threadRoomGate() bson.M {
	return bson.M{"$or": bson.A{
		bson.M{"$nor": bson.A{pipelines.AppRoomFilter()}},
		bson.M{"isSubscribed": true},
	}}
}
```

Replace the inline `"$or": bson.A{{"roomType": {"$ne": "botDM"}}, {"isSubscribed": true}}` in the `$lookup` sub-pipeline `$match` with `"$or": threadRoomGate()["$or"]`. Add `"github.com/hmchangw/chat/pkg/pipelines"` to the imports.

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=user-service`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add user-service/mongorepo/subscriptions.go user-service/mongorepo/threadsubscriptions.go \
        user-service/mongorepo/subscriptions_approom_test.go user-service/mongorepo/threadsubscriptions_approom_test.go
git commit -m "feat(user-service): resolve non-app botDMs in getDM and thread gate"
```

---

### Task 5: user-service subscription-list rendering

**Files:**
- Modify: `user-service/service/subscriptions.go:126-211` (`buildListItems`, `distinctListNames`), `:684-708` (`GetDM`)
- Test: `user-service/service/subscriptions_test.go` (extend)

**Interfaces:**
- Consumes: `model.EffectiveRoomType` from Task 1
- Produces: rows whose `Base().RoomType` is the effective type; `distinctListNames` returns the same two slices as before

- [ ] **Step 1: Write the failing test**

Append to `user-service/service/subscriptions_test.go`. `newThreadSvc` (`user-service/service/threads_test.go:23`) returns `(*UserService, *mocks.MockHistoryClient, *mocks.MockUserRepository, *mocks.MockAppRepository)` and is the constructor that wires the user and app repos this seam needs:

```go
// A bot logging into the frontend must see its DM with a human as an ordinary
// chat: roomType dm, the counterpart's hrInfo, and no app object.
func TestBuildListItems_BotViewerHumanCounterpartRendersAsDM(t *testing.T) {
	svc, _, users, _ := newThreadSvc(t)
	users.EXPECT().
		GetHRInfoByAccounts(gomock.Any(), []string{"alice"}).
		Return(map[string]*model.SubscriptionHRInfo{
			"alice": {Account: "alice", Name: "愛麗絲", EngName: "Alice"},
		}, nil)

	subs := []model.EnrichedSubscription{{Subscription: model.Subscription{
		ID: "s1", RoomType: model.RoomTypeBotDM, Name: "alice",
	}}}

	items := svc.buildListItems(context.Background(), "weather.bot", subs)

	require.Len(t, items, 1)
	dm, ok := items[0].(*model.DMSubscription)
	require.True(t, ok, "a botDM facing a human must render as a DMSubscription")
	assert.Equal(t, model.RoomTypeDM, dm.Base().RoomType, "the wire type must be dm")
	assert.Equal(t, "alice", dm.Base().Name, "the counterpart account is the display name")
	require.NotNil(t, dm.HRInfo)
	assert.Equal(t, "Alice", dm.HRInfo.EngName)
}

// A user's DM with p_admin is stored botDM but must render as an ordinary DM.
func TestBuildListItems_PlatformAdminCounterpartRendersAsDM(t *testing.T) {
	svc, _, users, _ := newThreadSvc(t)
	users.EXPECT().
		GetHRInfoByAccounts(gomock.Any(), []string{"p_admin_ops"}).
		Return(map[string]*model.SubscriptionHRInfo{}, nil)

	subs := []model.EnrichedSubscription{{Subscription: model.Subscription{
		ID: "s1", RoomType: model.RoomTypeBotDM, Name: "p_admin_ops",
	}}}

	items := svc.buildListItems(context.Background(), "alice", subs)

	require.Len(t, items, 1)
	dm, ok := items[0].(*model.DMSubscription)
	require.True(t, ok)
	assert.Equal(t, model.RoomTypeDM, dm.Base().RoomType)
	assert.Nil(t, dm.HRInfo, "a missing HR record degrades to no hrInfo, never an error")
}

// A real app keeps its app object, its name swap and its botDM type.
func TestBuildListItems_BotCounterpartStaysAppRoom(t *testing.T) {
	svc, _, _, apps := newThreadSvc(t)
	apps.EXPECT().
		GetAppsByAssistants(gomock.Any(), []string{"weather.bot"}).
		Return(map[string]*model.App{"weather.bot": {ID: "a1", Name: "Weather"}}, nil)

	subs := []model.EnrichedSubscription{{Subscription: model.Subscription{
		ID: "s1", RoomType: model.RoomTypeBotDM, Name: "weather.bot",
	}}}

	items := svc.buildListItems(context.Background(), "alice", subs)

	require.Len(t, items, 1)
	bot, ok := items[0].(*model.BotDMSubscription)
	require.True(t, ok, "a botDM facing a .bot app must stay a BotDMSubscription")
	assert.Equal(t, model.RoomTypeBotDM, bot.Base().RoomType)
	assert.Equal(t, "Weather", bot.Base().Name, "the app display name still replaces the bot account")
	require.NotNil(t, bot.App)
}

func TestDistinctListNames_SplitsByEffectiveType(t *testing.T) {
	subs := []model.EnrichedSubscription{
		{Subscription: model.Subscription{RoomType: model.RoomTypeBotDM, Name: "weather.bot"}},
		{Subscription: model.Subscription{RoomType: model.RoomTypeBotDM, Name: "alice"}},
		{Subscription: model.Subscription{RoomType: model.RoomTypeBotDM, Name: "p_admin_ops"}},
		{Subscription: model.Subscription{RoomType: model.RoomTypeDM, Name: "bob"}},
		{Subscription: model.Subscription{RoomType: model.RoomTypeChannel, Name: "general"}},
	}

	bots, dms := distinctListNames(subs)

	assert.Equal(t, []string{"weather.bot"}, bots, "only real apps drive the app lookup")
	assert.Equal(t, []string{"alice", "p_admin_ops", "bob"}, dms, "every effective DM drives the HR lookup")
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=user-service`
Expected: FAIL — the botDM rows render as `*model.BotDMSubscription` with `RoomType` still `botDM`, and `distinctListNames` puts `alice` and `p_admin_ops` in `bots`

- [ ] **Step 3: Write minimal implementation**

In `buildListItems`, switch on the effective type and stamp it onto the base row:

```go
	for i := range subs {
		base := &subs[i].Subscription
		switch model.EffectiveRoomType(subs[i].RoomType, subs[i].Name) {
		case model.RoomTypeBotDM:
			botDM := &model.BotDMSubscription{Subscription: base}
			if app, ok := apps[subs[i].Name]; ok && app != nil {
				if app.Name != "" {
					base.Name = app.Name
				}
				botDM.App = model.AppSubscriptionFromApp(app)
			}
			items[i] = botDM
		case model.RoomTypeDM:
			// A botDM facing a human or p_admin reaches here; stamp the effective
			// type so the client files it under the chat section, not the App list.
			base.RoomType = model.RoomTypeDM
			dm := &model.DMSubscription{Subscription: base}
			if hr, ok := hrInfo[subs[i].Name]; ok {
				dm.HRInfo = hr
			}
			items[i] = dm
		default:
			items[i] = &model.ChannelSubscription{Subscription: base}
		}
	}
```

Update the doc comment above `buildListItems` to describe the effective-type rule.

In `distinctListNames`, replace `switch subs[i].RoomType` with `switch model.EffectiveRoomType(subs[i].RoomType, subs[i].Name)` — the two case bodies are unchanged.

In `GetDM` (`:705`), stamp the type before building the reply:

```go
	one[0].Subscription.RoomType = model.EffectiveRoomType(one[0].RoomType, one[0].Name)
	return &models.DMResponse{Subscription: model.DMSubscription{
		Subscription: &one[0].Subscription,
		HRInfo:       dm.HRInfo,
	}}, nil
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=user-service`
Expected: PASS — including the pre-existing `buildListItems` tests for channel and app rows

- [ ] **Step 5: Commit**

```bash
git add user-service/service/subscriptions.go user-service/service/subscriptions_test.go
git commit -m "feat(user-service): render non-app botDMs as dm with hrInfo"
```

---

### Task 6: user-service thread-list rendering

**Files:**
- Modify: `user-service/service/threads.go:256-274` (`enrichThreadPage`), `:325-350` (`distinctDMAndBotNames`)
- Test: `user-service/service/threads_test.go` (extend)

**Interfaces:**
- Consumes: `model.EffectiveRoomType` from Task 1
- Produces: `model.ThreadListItem` rows whose `RoomType` is the effective type

- [ ] **Step 1: Write the failing test**

Append to `user-service/service/threads_test.go`. `ctx(account, siteID)` (`user-service/service/service_test.go:45`) builds the `*natsrouter.Context` these take:

```go
// A bot's thread in its DM with a human is an ordinary DM thread: dm type,
// counterpart hrInfo, and the room name left as the human account.
func TestEnrichThreadPage_BotViewerHumanCounterpartRendersAsDM(t *testing.T) {
	svc, _, users, _ := newThreadSvc(t)
	users.EXPECT().
		GetHRInfoByAccounts(gomock.Any(), []string{"alice"}).
		Return(map[string]*model.SubscriptionHRInfo{
			"alice": {Account: "alice", Name: "愛麗絲", EngName: "Alice"},
		}, nil)

	items := []model.ThreadListItem{{RoomType: model.RoomTypeBotDM, RoomName: "alice"}}

	svc.enrichThreadPage(ctx("weather.bot", "site-a"), items)

	assert.Equal(t, model.RoomTypeDM, items[0].RoomType)
	assert.Equal(t, "alice", items[0].RoomName)
	require.NotNil(t, items[0].HRInfo)
	assert.Equal(t, "Alice", items[0].HRInfo.EngName)
}

// A real app's thread keeps the app display-name swap and the botDM type.
func TestEnrichThreadPage_BotCounterpartStaysAppRoom(t *testing.T) {
	svc, _, _, apps := newThreadSvc(t)
	apps.EXPECT().
		GetAppsByAssistants(gomock.Any(), []string{"weather.bot"}).
		Return(map[string]*model.App{"weather.bot": {ID: "a1", Name: "Weather"}}, nil)

	items := []model.ThreadListItem{{RoomType: model.RoomTypeBotDM, RoomName: "weather.bot"}}

	svc.enrichThreadPage(ctx("alice", "site-a"), items)

	assert.Equal(t, model.RoomTypeBotDM, items[0].RoomType)
	assert.Equal(t, "Weather", items[0].RoomName)
	assert.Nil(t, items[0].HRInfo)
}

func TestDistinctDMAndBotNames_SplitsByEffectiveType(t *testing.T) {
	items := []model.ThreadListItem{
		{RoomType: model.RoomTypeBotDM, RoomName: "weather.bot"},
		{RoomType: model.RoomTypeBotDM, RoomName: "alice"},
		{RoomType: model.RoomTypeBotDM, RoomName: "p_admin_ops"},
		{RoomType: model.RoomTypeDM, RoomName: "bob"},
		{RoomType: model.RoomTypeChannel, RoomName: "general"},
		{RoomType: model.RoomTypeBotDM, RoomName: ""},
	}

	dms, bots := distinctDMAndBotNames(items)

	assert.Equal(t, []string{"alice", "p_admin_ops", "bob"}, dms)
	assert.Equal(t, []string{"weather.bot"}, bots)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=user-service`
Expected: FAIL — `RoomType` stays `botDM` and the human counterpart lands in the app lookup set

- [ ] **Step 3: Write minimal implementation**

In `enrichThreadPage`, switch on the effective type and stamp it:

```go
	for i := range items {
		switch model.EffectiveRoomType(items[i].RoomType, items[i].RoomName) {
		case model.RoomTypeDM:
			// A botDM facing a human or p_admin reaches here; stamp the effective
			// type so the row files under the chat section like any other DM.
			items[i].RoomType = model.RoomTypeDM
			if info, ok := hr[items[i].RoomName]; ok {
				items[i].HRInfo = info
			}
		case model.RoomTypeBotDM:
			if app, ok := apps[items[i].RoomName]; ok && app != nil && app.Name != "" {
				items[i].RoomName = app.Name
			}
		case model.RoomTypeChannel, model.RoomTypeDiscussion:
			// No per-row enrichment: roomName/roomType already carry the final values.
		}
	}
```

In `distinctDMAndBotNames`, replace `switch items[i].RoomType` with `switch model.EffectiveRoomType(items[i].RoomType, name)` — the blank-name `continue` above it stays, and the case bodies are unchanged.

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=user-service`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add user-service/service/threads.go user-service/service/threads_test.go
git commit -m "feat(user-service): render non-app botDM threads as dm with hrInfo"
```

---

### Task 7: history-service thread-list gate

**Files:**
- Modify: `history-service/internal/mongorepo/pipelines.go:62-79` (membership `$lookup` sub-pipeline)
- Test: `history-service/internal/mongorepo/threadsubscription_unit_test.go` (extend)

**Interfaces:**
- Consumes: `pipelines.AppRoomFilter()` from Task 2
- Produces: no new exported symbols; `threadMembershipGate() bson.M` unexported helper

- [ ] **Step 1: Write the failing test**

Append to `history-service/internal/mongorepo/threadsubscription_unit_test.go`:

```go
// A bot's own subscription row carries isSubscribed=false, so gating every
// botDM on it hides the bot's threads in its DMs with humans. Only real apps —
// botDM rows facing a ".bot" counterpart — keep the soft-unsubscribe gate.
func TestThreadMembershipGate_KeepsNonAppBotDMs(t *testing.T) {
	branches, ok := threadMembershipGate()["$or"].(bson.A)
	require.True(t, ok, "the gate must be an $or")
	require.Len(t, branches, 2)

	notApp, ok := branches[0].(bson.M)
	require.True(t, ok)
	nor, ok := notApp["$nor"].(bson.A)
	require.True(t, ok, "the first branch must exclude app rooms via $nor")
	require.Len(t, nor, 1)

	app, ok := nor[0].(bson.M)
	require.True(t, ok)
	assert.Equal(t, "botDM", app["roomType"])
	name, ok := app["name"].(bson.M)
	require.True(t, ok)
	assert.Equal(t, `\.bot$`, name["$regex"])

	subscribed, ok := branches[1].(bson.M)
	require.True(t, ok)
	assert.Equal(t, true, subscribed["isSubscribed"])
}
```

Add `"go.mongodb.org/mongo-driver/v2/bson"` and the testify imports to the file if absent.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=history-service`
Expected: FAIL — `undefined: threadMembershipGate`

- [ ] **Step 3: Write minimal implementation**

In `history-service/internal/mongorepo/pipelines.go`, add above `userThreadSubscriptionsPipeline`:

```go
// threadMembershipGate keeps a thread only when its room subscription still
// grants access. Unsubscribing from an app is a soft toggle (isSubscribed=false,
// row retained), unlike a room leave that purges the row — so app rooms are
// gated on isSubscribed. A botDM facing a human or p_admin is not an app room:
// its bot-side row carries isSubscribed=false by construction, so gating it
// would hide every thread a bot has in its own DMs.
func threadMembershipGate() bson.M {
	return bson.M{"$or": bson.A{
		bson.M{"$nor": bson.A{pipelines.AppRoomFilter()}},
		bson.M{"isSubscribed": true},
	}}
}
```

In the `$lookup` sub-pipeline `$match` at `:66-76`, replace the inline `"$or": bson.A{{"roomType": {"$ne": "botDM"}}, {"isSubscribed": true}}` with `"$or": threadMembershipGate()["$or"]`, and update the comment block at `:42-46` to describe the app-room rule. Add `"github.com/hmchangw/chat/pkg/pipelines"` to the imports.

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=history-service`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add history-service/internal/mongorepo/pipelines.go history-service/internal/mongorepo/threadsubscription_unit_test.go
git commit -m "feat(history-service): exempt non-app botDMs from the thread membership gate"
```

---

### Task 8: search-service enrichment

**Files:**
- Modify: `search-service/enrich.go:60-72` (partition switch), `:112-131` (room-object switch)
- Test: `search-service/enrich_test.go` (extend)

**Interfaces:**
- Consumes: `model.EffectiveRoomType` from Task 1
- Produces: `model.MessageRoom` values whose `Type` is the effective type

- [ ] **Step 1: Write the failing test**

Append to `search-service/enrich_test.go`, using the file's existing `enrichHandler(m MongoStore, r RoomInfoClient) *handler` (`:31`), `hit(id, room, site, sender)` (`:37`), `fakeRoom` (`:23`) and the `fakeMongo` fake declared in `search-service/handler_test.go:274-296`:

```go
// A bot's search hit in its DM with a human must resolve the counterpart through
// the HR directory, not through a doomed app lookup.
func TestEnrichMessages_BotViewerHumanCounterpartUsesHRInfo(t *testing.T) {
	m := &fakeMongo{
		subs:  map[string]SubscriptionMeta{"rBot": {RoomType: model.RoomTypeBotDM, Name: "alice"}},
		users: map[string]HRUser{"alice": {Account: "alice", EngName: "Alice", ChineseName: "愛麗絲"}},
	}
	h := enrichHandler(m, &fakeRoom{})

	out := h.enrichMessages(context.Background(), "weather.bot",
		[]messageSearchHit{hit("m1", "rBot", "site-a", "alice")})

	require.NotNil(t, out[0].Room)
	assert.Equal(t, model.RoomTypeDM, out[0].Room.Type)
	require.NotNil(t, out[0].Room.HRInfo)
	assert.Equal(t, "alice", out[0].Room.HRInfo.Account)
	assert.Nil(t, out[0].Room.AppInfo, "a non-app room never carries appInfo")
	assert.Equal(t, []string{"alice"}, m.appBots, "the human counterpart never reaches the app lookup")
}

// A user's DM with p_admin is stored botDM but enriches like any other DM.
func TestEnrichMessages_PlatformAdminCounterpartUsesHRInfo(t *testing.T) {
	m := &fakeMongo{
		subs:  map[string]SubscriptionMeta{"rAdm": {RoomType: model.RoomTypeBotDM, Name: "p_admin_ops"}},
		users: map[string]HRUser{"p_admin_ops": {Account: "p_admin_ops", EngName: "Ops"}},
	}
	h := enrichHandler(m, &fakeRoom{})

	out := h.enrichMessages(context.Background(), "alice",
		[]messageSearchHit{hit("m1", "rAdm", "site-a", "p_admin_ops")})

	require.NotNil(t, out[0].Room)
	assert.Equal(t, model.RoomTypeDM, out[0].Room.Type)
	require.NotNil(t, out[0].Room.HRInfo)
	assert.Nil(t, out[0].Room.AppInfo)
}
```

`TestEnrichMessages_BotDM` and `TestEnrichMessages_BotDM_Unsubscribed` (`:72`, `:95`) already pin the app-room branch — a `.bot` counterpart keeping `botDM`, its `appInfo`, its name swap and its `isSubscribed` flag. They must still pass untouched; if either fails, the app-room branch was broken, not the DM branch.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=search-service`
Expected: FAIL — the first test reports `Room.Type == botDM` and a nil `HRInfo`, because the human account went to the app lookup

- [ ] **Step 3: Write minimal implementation**

In the partition loop (`:60`), switch on the effective type:

```go
		switch model.EffectiveRoomType(meta.RoomType, meta.Name) {
		case model.RoomTypeDM:
			if meta.Name != "" {
				dmCounterparts[meta.Name] = struct{}{}
			}
		case model.RoomTypeBotDM:
			if meta.Name != "" {
				botAccounts[meta.Name] = struct{}{}
			}
		default:
			channelRoomsBySite[roomSite[rid]] = append(channelRoomsBySite[roomSite[rid]], rid)
		}
```

In the room-object loop (`:112`), do the same and report the effective type:

```go
		if meta, ok := subs[rid]; ok {
			effective := model.EffectiveRoomType(meta.RoomType, meta.Name)
			room.Type = effective
			switch effective {
			case model.RoomTypeDM:
				if hr, ok := users[meta.Name]; ok {
					room.HRInfo = hrInfoOf(hr)
					room.Name = displayfmt.CombineWithFallback(hr.EngName, hr.ChineseName, meta.Name)
				} else {
					room.Name = meta.Name
				}
			case model.RoomTypeBotDM:
				if app, ok := apps[meta.Name]; ok {
					room.AppInfo = appInfoOf(app)
					room.AppInfo.IsSubscribed = &meta.IsSubscribed
					room.Name = app.Name
				} else {
					room.Name = meta.Name
				}
			default:
				room.Name = roomNames[rid]
			}
		}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=search-service`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add search-service/enrich.go search-service/enrich_test.go
git commit -m "feat(search-service): enrich non-app botDM hits as dm with hrInfo"
```

---

### Task 9: `p_admin` DMs are created as `dm`

**Files:**
- Modify: `room-service/helper.go:178-186` (`determineRoomType`), `room-worker/handler.go:1693-1705` (`determineRoomTypeFromPayload`)
- Test: `room-service/helper_test.go` (extend), `room-worker/handler_test.go` (extend)

**Interfaces:**
- Consumes: `model.IsBot` (`pkg/model/user.go:175`)
- Produces: `determineRoomType` / `determineRoomTypeFromPayload` return `RoomTypeDM` for a `p_admin` counterpart

- [ ] **Step 1: Write the failing tests**

Append to `room-service/helper_test.go`:

```go
// p_admin is a human-operated pseudo-account: its DM is an ordinary DM, not an
// app room. Classifying it as botDM also made room.create fail, because the
// botDM branch demands an app document and p_admin has none.
func TestDetermineRoomType_PlatformAdminIsDM(t *testing.T) {
	tests := []struct {
		name string
		req  model.CreateRoomRequest
		want model.RoomType
	}{
		{"bot counterpart is a botDM", model.CreateRoomRequest{Users: []string{"weather.bot"}}, model.RoomTypeBotDM},
		{"platform admin counterpart is a dm", model.CreateRoomRequest{Users: []string{"p_admin_ops"}}, model.RoomTypeDM},
		{"human counterpart is a dm", model.CreateRoomRequest{Users: []string{"alice"}}, model.RoomTypeDM},
		{"QA p_ counterpart is a dm", model.CreateRoomRequest{Users: []string{"p_qa_bob"}}, model.RoomTypeDM},
		{"two users is a channel", model.CreateRoomRequest{Users: []string{"alice", "bob"}}, model.RoomTypeChannel},
		{"named single-user request is a channel", model.CreateRoomRequest{Name: "team", Users: []string{"alice"}}, model.RoomTypeChannel},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, determineRoomType(&tt.req))
		})
	}
}
```

Append the identical table to `room-worker/handler_test.go`, calling `determineRoomTypeFromPayload` and naming the test `TestDetermineRoomTypeFromPayload_PlatformAdminIsDM`. The two functions are deliberate mirrors, so both need the case.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=room-service` then `make test SERVICE=room-worker`
Expected: FAIL on the `p_admin_ops` row in each — got `botDM`, want `dm`

- [ ] **Step 3: Write minimal implementation**

In `room-service/helper.go`, replace the body's bot test and rewrite the doc comment:

```go
// determineRoomType classifies a post-strip request; caller must guarantee non-empty input.
// A single-user DM whose counterpart is a bot (".bot") is a botDM. The "p_admin"
// platform-admin pseudo-account is human-operated and has no app document, so its
// DM is an ordinary DM — it stays bot-like for channel membership and ownership
// (filterBots, errBotInChannel), just not for DM classification. A QA "p_"
// counterpart is an ordinary user too.
func determineRoomType(req *model.CreateRoomRequest) model.RoomType {
	if req.Name == "" && len(req.Orgs) == 0 && len(req.Channels) == 0 && len(req.Users) == 1 {
		if model.IsBot(req.Users[0]) {
			return model.RoomTypeBotDM
		}
		return model.RoomTypeDM
	}
	return model.RoomTypeChannel
}
```

Apply the same change to `determineRoomTypeFromPayload` in `room-worker/handler.go`, keeping its "mirrors room-service's determineRoomType" opening line.

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=room-service` then `make test SERVICE=room-worker`
Expected: PASS. Existing tests asserting the bot-in-channel guard and `filterBots` must still pass unchanged — if one fails, it was asserting DM classification indirectly and should be read before being edited.

- [ ] **Step 5: Commit**

```bash
git add room-service/helper.go room-service/helper_test.go room-worker/handler.go room-worker/handler_test.go
git commit -m "fix(room): classify p_admin DMs as dm, not botDM"
```

---

### Task 10: Integration coverage for the read gates

**Files:**
- Modify: `user-service/mongorepo/subscriptions_test.go` (extend, `//go:build integration`), `history-service/internal/mongorepo/threadsubscription_test.go` (extend, `//go:build integration`)

**Interfaces:**
- Consumes: everything from Tasks 1-4 and 7; `testutil.MongoDB(t, prefix)` per CLAUDE.md
- Produces: no new symbols

- [ ] **Step 1: Write the failing tests**

Append to `user-service/mongorepo/subscriptions_test.go`, using the file's `newTestSubscriptionRepo(t) (*SubscriptionRepo, *mongo.Database)` (`setup_test.go:28`) and `seed(t, db, coll, docs ...any)` (`setup_test.go:62`). Production always pairs a botDM with a room document, so seed both collections:

```go
// The five predicate cases, end to end against Mongo. The regression that
// matters most is the last one: a human's unsubscribed app must stay hidden.
func TestAggregateSubscriptions_AppRoomPartition(t *testing.T) {
	repo, db := newTestSubscriptionRepo(t)
	ctx := context.Background()
	now := time.Now().UTC()

	seed(t, db, "rooms",
		bson.M{"_id": "r-bot-alice", "name": "", "siteId": "site-a", "userCount": 2, "lastMsgAt": now},
		bson.M{"_id": "r-sales", "name": "sales.bot", "siteId": "site-a", "userCount": 1, "lastMsgAt": now},
		bson.M{"_id": "r-admin", "name": "", "siteId": "site-a", "userCount": 2, "lastMsgAt": now},
		bson.M{"_id": "r-bot-bot", "name": "", "siteId": "site-a", "userCount": 2, "lastMsgAt": now},
	)

	seed(t, db, "subscriptions",
		// the bot's own side of its DM with alice — isSubscribed=false by construction
		bson.M{"_id": "s1", "u": bson.M{"_id": "u-w", "account": "weather.bot"}, "roomId": "r-bot-alice",
			"name": "alice", "roomType": "botDM", "siteId": "site-a", "isSubscribed": false, "createdAt": now},
		// alice's side of the same room — a subscribed app
		bson.M{"_id": "s2", "u": bson.M{"_id": "u-a", "account": "alice"}, "roomId": "r-bot-alice",
			"name": "weather.bot", "roomType": "botDM", "siteId": "site-a", "isSubscribed": true, "createdAt": now},
		// alice unsubscribed from the sales app — must stay hidden everywhere
		bson.M{"_id": "s3", "u": bson.M{"_id": "u-a", "account": "alice"}, "roomId": "r-sales",
			"name": "sales.bot", "roomType": "botDM", "siteId": "site-a", "isSubscribed": false, "createdAt": now},
		// alice's DM with the platform admin — stored botDM, renders as a chat
		bson.M{"_id": "s4", "u": bson.M{"_id": "u-a", "account": "alice"}, "roomId": "r-admin",
			"name": "p_admin_ops", "roomType": "botDM", "siteId": "site-a", "isSubscribed": true, "createdAt": now},
		// bot↔bot: still an app room for the viewing bot, still gated
		bson.M{"_id": "s5", "u": bson.M{"_id": "u-w", "account": "weather.bot"}, "roomId": "r-bot-bot",
			"name": "sales.bot", "roomType": "botDM", "siteId": "site-a", "isSubscribed": false, "createdAt": now},
	)

	roomIDs := func(account, listType string) []string {
		res, err := repo.AggregateSubscriptions(ctx, account, listType, false, nil,
			mongoutil.OffsetPageRequest{Offset: 0, Limit: 50})
		require.NoError(t, err)
		out := make([]string, 0, len(res.Data))
		for i := range res.Data {
			out = append(out, res.Data[i].RoomID)
		}
		return out
	}

	assert.ElementsMatch(t, []string{"r-bot-alice"}, roomIDs("weather.bot", "rooms"),
		"a bot sees its human DM in the chat bucket, despite isSubscribed=false")
	assert.ElementsMatch(t, []string{"r-bot-alice"}, roomIDs("weather.bot", "current"),
		"the bot's DM with another bot stays gated and hidden")
	assert.Empty(t, roomIDs("weather.bot", "apps"),
		"a bot's human DM never appears in the App section")
	assert.ElementsMatch(t, []string{"r-admin"}, roomIDs("alice", "rooms"),
		"a user's p_admin DM is an ordinary chat; the weather app is not")
	assert.ElementsMatch(t, []string{"r-bot-alice"}, roomIDs("alice", "apps"),
		"only the subscribed .bot app is in the App section")
	assert.ElementsMatch(t, []string{"r-bot-alice", "r-admin"}, roomIDs("alice", "current"),
		"the unsubscribed sales.bot app stays hidden everywhere")
}
```

Then append to `history-service/internal/mongorepo/threadsubscription_test.go`, following that file's existing seeding style for `subscriptions`, `thread_subscriptions` and `thread_rooms`:

```go
// A bot's thread in its own DM with a human must survive the membership gate,
// even though its subscription row carries isSubscribed=false. A human's thread
// in an app she unsubscribed from must not.
func TestUserThreadSubscriptions_AppRoomGate(t *testing.T) {
	repo, db := newTestRepo(t)
	ctx := context.Background()
	now := time.Now().UTC()

	seed(t, db, "subscriptions",
		bson.M{"_id": "s1", "u": bson.M{"_id": "u-w", "account": "weather.bot"}, "roomId": "r1",
			"name": "alice", "roomType": "botDM", "siteId": "site-a", "isSubscribed": false},
		bson.M{"_id": "s2", "u": bson.M{"_id": "u-a", "account": "alice"}, "roomId": "r2",
			"name": "sales.bot", "roomType": "botDM", "siteId": "site-a", "isSubscribed": false},
	)
	seed(t, db, "thread_rooms",
		bson.M{"_id": "t1", "roomId": "r1", "siteId": "site-a", "lastMsgAt": now},
		bson.M{"_id": "t2", "roomId": "r2", "siteId": "site-a", "lastMsgAt": now},
	)
	seed(t, db, "thread_subscriptions",
		bson.M{"_id": "ts1", "userAccount": "weather.bot", "roomId": "r1", "threadRoomId": "t1", "siteId": "site-a"},
		bson.M{"_id": "ts2", "userAccount": "alice", "roomId": "r2", "threadRoomId": "t2", "siteId": "site-a"},
	)

	botRows, err := repo.UserThreadSubscriptions(ctx, "weather.bot", nil, "", 50)
	require.NoError(t, err)
	require.Len(t, botRows, 1, "the bot's thread in its human DM must survive the gate")
	assert.Equal(t, "t1", botRows[0].ThreadRoomID)

	userRows, err := repo.UserThreadSubscriptions(ctx, "alice", nil, "", 50)
	require.NoError(t, err)
	assert.Empty(t, userRows, "a thread in an unsubscribed app stays hidden")
}
```

Match `newTestRepo` and `UserThreadSubscriptions` to the constructor and method names as they actually appear in `history-service/internal/mongorepo/`; the seed documents and assertions stay as written.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test-integration SERVICE=user-service`
Expected: FAIL if Tasks 3-4 were not applied. If those tasks are already committed, these tests should pass on first run — that is the intended confirmation, not a TDD violation, because the unit tests in Tasks 3-4 already drove the implementation.

- [ ] **Step 3: Run the history-service integration suite**

Run: `make test-integration SERVICE=history-service`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add user-service/mongorepo/subscriptions_test.go history-service/internal/mongorepo/threadsubscription_test.go
git commit -m "test: integration coverage for the app-room partition"
```

---

### Task 11: Client API documentation

**Files:**
- Modify: `docs/client-api.md`, `docs/client-api/request-reply.md`, `docs/client-api/events.md`

**Interfaces:**
- Consumes: the behavior shipped in Tasks 1-9
- Produces: documentation only

- [ ] **Step 1: Locate every affected section**

Run: `grep -n "botDM" docs/client-api.md docs/client-api/request-reply.md docs/client-api/events.md`

Expect hits under `subscription.list`, `subscription.getDM`, `subscription.getByRoomID`, `subscription.getChannels`, `thread.list`, the search response, and the shared `Subscription` / `roomType` schema.

- [ ] **Step 2: Add the rule to the shared schema section**

In `docs/client-api.md` §3.0 Shared schemas, on the `roomType` field row, state the rule in one sentence:

> `roomType` is the room's type **as seen by the requesting subscriber**. A `botDM` is reported as `botDM` only when the counterpart account ends in `.bot` (a real app); a `botDM` facing an ordinary user or the platform-admin account is reported as `dm` and carries `hrInfo` instead of `app`. A bot logging in therefore sees its DMs with users as `dm`.

- [ ] **Step 3: Update each affected response table**

For every response that documents `app` or `hrInfo`, note that the two are mutually exclusive and selected by the effective `roomType`. Keep the existing field-table style — explicit types, no `object`, linked names for compound types — and change no other prose.

- [ ] **Step 4: Mirror into the derived views**

Apply the same edits to `docs/client-api/request-reply.md` (subscription and thread RPCs) and `docs/client-api/events.md` (any event carrying `roomType` or a subscription row). The derived views must never drift from the canonical file.

- [ ] **Step 5: Verify and commit**

Run: `grep -n "botDM" docs/client-api.md docs/client-api/request-reply.md docs/client-api/events.md`
Expected: every hit sits near a statement of the effective-type rule.

```bash
git add docs/client-api.md docs/client-api/request-reply.md docs/client-api/events.md
git commit -m "docs: document the botDM effective room type rule"
```

---

### Task 12: Full verification

**Files:** none modified

- [ ] **Step 1: Format and lint**

Run: `make fmt` then `make lint`
Expected: clean. Fix anything reported before continuing.

- [ ] **Step 2: Regenerate mocks if any store interface moved**

Run: `make generate`
Expected: no diff. If a mock changed, commit it — no store interface was intended to change in this plan, so a diff means something was edited that should not have been.

- [ ] **Step 3: Full unit suite**

Run: `make test`
Expected: PASS

- [ ] **Step 4: Integration suites for the touched services**

Run: `make test-integration SERVICE=user-service`, `make test-integration SERVICE=history-service`, `make test-integration SERVICE=room-service`, `make test-integration SERVICE=room-worker`, `make test-integration SERVICE=search-service`
Expected: PASS

- [ ] **Step 5: SAST**

Run: `make sast`
Expected: no medium-or-higher findings. The added regexes are constants, not user input, so no injection surface is introduced.

- [ ] **Step 6: Coverage check on the changed packages**

Run: `go test -coverprofile=coverage.out ./pkg/model/ ./pkg/pipelines/ ./user-service/... ./search-service/` then `go tool cover -func=coverage.out | tail -1`
Expected: ≥80% per package; `pkg/model` and `pkg/pipelines` ≥90%.

- [ ] **Step 7: Commit any fixes**

```bash
git add -A
git commit -m "chore: lint and format fixes for botDM effective room type"
```
