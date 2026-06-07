# Room Member Statuses & Mentionable Subscriptions — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Drift from shipped implementation.** This plan was written before the final
> design decisions landed. The following items diverge from what shipped — treat
> the code, tests, and `docs/client-api.md` as the source of truth and use the
> snippets below only as starting points:
>
> - **Bot/admin classification was split.** The plan reuses a single
>   `botPattern` regex `(\.bot$|^p_)` Go-side and Mongo-side. The shipped code
>   has two narrower constants — `botAccountRegex = `\.bot$`` for assistant
>   bots and `platformAdminRegex = `^p_`` for platform-admin / webhook
>   accounts — and exposes the predicates as `(*model.User).IsBot()` and
>   `(*model.User).IsPlatformAdmin()`. The mentionable pipeline classifies
>   only `.bot` accounts as `app` and hides `p_` accounts entirely.
> - **`subscription.mentionable` default limit is 3, not 1.** When `Limit`
>   is nil the server returns `min(3, room.UserCount + room.AppCount)` rows;
>   empty rooms return an empty list.
> - **Shared membership-error text is `"only room members can perform this action"`.**
>   The single sentinel (`errNotRoomMember`) is reused across the four
>   membership-gated RPCs (the wording is intentionally RPC-agnostic) and
>   was inherited from existing room-service code rather than introduced
>   by this PR.
> - **The membership probe runs in parallel with `GetRoom`** via a
>   `requireMembershipAndGetRoom` helper (sync.WaitGroup, not
>   errgroup.WithContext) that applies membership-error precedence
>   post-Wait — a fast GetRoom failure cannot cancel the membership probe
>   and mask the not-member sentinel as `context.Canceled`.

**Goal:** Add two new NATS request/reply RPCs to `room-service` — `member.statuses` (list members + display names + presence status) and `subscription.mentionable` (mention autocomplete with user/app discriminated union).

**Architecture:** Both handlers sit alongside the existing `handleListMembers` in `room-service`, share the existing membership-check pattern, classify accounts via `(*model.User).IsBot` Go-side + `botAccountRegex` Mongo-side (see drift note above), and route errors through `sanitizeError`. Each RPC has its own store method backed by a single Mongo aggregation pipeline against the `subscriptions` collection with `$lookup` joins to `users` and (for mentionable) `apps`.

**Tech Stack:** Go 1.25, NATS request/reply, MongoDB (`go.mongodb.org/mongo-driver/v2`), `go.uber.org/mock`, `stretchr/testify`, `testcontainers-go` (via `pkg/testutil`).

**Source spec:** `docs/superpowers/specs/2026-05-29-room-member-statuses-and-mentionable-subscriptions-design.md`

---

## Task 1: Add `StatusIsShow` / `StatusText` fields to `User` model

**Files:**
- Modify: `pkg/model/user.go`
- Modify: `pkg/model/model_test.go` (test `TestUserJSON_WithSectAndDept` — extend coverage to status fields via a new round-trip test case)

- [ ] **Step 1.1: Write the failing round-trip test**

Append to `pkg/model/model_test.go` immediately after `TestUserJSON_WithSectAndDept` (around line 41):

```go
func TestUserJSON_WithStatus(t *testing.T) {
	u := model.User{
		ID: "u1", Account: "alice", SiteID: "site-a",
		EngName: "Alice Wang", ChineseName: "愛麗絲",
		StatusIsShow: true,
		StatusText:   "available",
	}
	roundTrip(t, &u, &model.User{})
}
```

- [ ] **Step 1.2: Run test, verify it fails**

```
make test SERVICE=pkg/model 2>&1 | tail -30
```

Expected: compile failure — `unknown field StatusIsShow in struct literal of type model.User`.

- [ ] **Step 1.3: Add fields to `User`**

In `pkg/model/user.go`, replace the entire struct (currently lines 3–16) with:

```go
type User struct {
	ID           string `json:"id"           bson:"_id"`
	Account      string `json:"account"      bson:"account"`
	SiteID       string `json:"siteId"       bson:"siteId"`
	SectID       string `json:"sectId"       bson:"sectId"`
	SectName     string `json:"sectName"     bson:"sectName"`
	SectTCName   string `json:"sectTCName"   bson:"sectTCName"`
	DeptID       string `json:"deptId"       bson:"deptId"`
	DeptName     string `json:"deptName"     bson:"deptName"`
	DeptTCName   string `json:"deptTCName"   bson:"deptTCName"`
	EngName      string `json:"engName"      bson:"engName"`
	ChineseName  string `json:"chineseName"  bson:"chineseName"`
	EmployeeID   string `json:"employeeId"   bson:"employeeId"`
	StatusIsShow bool   `json:"statusIsShow" bson:"statusIsShow"`
	StatusText   string `json:"statusText"   bson:"statusText"`
}
```

- [ ] **Step 1.4: Run test, verify it passes**

```
make test SERVICE=pkg/model 2>&1 | tail -20
```

Expected: PASS (all model tests, including the new `TestUserJSON_WithStatus`).

- [ ] **Step 1.5: Commit**

```bash
git add pkg/model/user.go pkg/model/model_test.go
git commit -m "feat(model): add StatusIsShow and StatusText to User"
```

---

## Task 2: Add Feature 1 request/response types

**Files:**
- Modify: `pkg/model/member.go` (append types after `ListOrgMembersResponse`, around line 132)
- Modify: `pkg/model/model_test.go` (append round-trip tests)

- [ ] **Step 2.1: Write the failing round-trip test**

Append to `pkg/model/model_test.go` near the end of the file, just before `func roundTrip`:

```go
func TestListMemberStatusesRequestJSON(t *testing.T) {
	limit := 5
	r := model.ListMemberStatusesRequest{Limit: &limit}
	roundTrip(t, &r, &model.ListMemberStatusesRequest{})
}

func TestMemberStatusJSON(t *testing.T) {
	m := model.MemberStatus{
		Account: "alice", EngName: "Alice Wang", ChineseName: "愛麗絲",
		StatusIsShow: true, StatusText: "in a meeting",
	}
	roundTrip(t, &m, &model.MemberStatus{})
}

func TestListMemberStatusesResponseJSON(t *testing.T) {
	r := model.ListMemberStatusesResponse{Members: []model.MemberStatus{
		{Account: "alice", EngName: "Alice", ChineseName: "愛麗絲", StatusIsShow: true, StatusText: "busy"},
		{Account: "bob", EngName: "Bob", ChineseName: "陳博"},
	}}
	roundTrip(t, &r, &model.ListMemberStatusesResponse{})
}
```

- [ ] **Step 2.2: Run test, verify it fails**

```
make test SERVICE=pkg/model 2>&1 | tail -10
```

Expected: compile failure — `undefined: model.ListMemberStatusesRequest`.

- [ ] **Step 2.3: Add types**

Append to `pkg/model/member.go` (after the existing `ListOrgMembersResponse` struct, around line 132):

```go
// ListMemberStatusesRequest is the body for the member.statuses RPC.
// Limit defaults to 3 when nil; must be > 0 and <= room.UserCount.
type ListMemberStatusesRequest struct {
	Limit *int `json:"limit,omitempty"`
}

// MemberStatus is the projection returned by the member.statuses RPC.
// All five fields are sourced from the users collection via $lookup.
type MemberStatus struct {
	Account      string `json:"account"      bson:"account"`
	EngName      string `json:"engName"      bson:"engName"`
	ChineseName  string `json:"chineseName"  bson:"chineseName"`
	StatusIsShow bool   `json:"statusIsShow" bson:"statusIsShow"`
	StatusText   string `json:"statusText"   bson:"statusText"`
}

type ListMemberStatusesResponse struct {
	Members []MemberStatus `json:"members"`
}
```

- [ ] **Step 2.4: Run test, verify it passes**

```
make test SERVICE=pkg/model 2>&1 | tail -10
```

Expected: PASS.

- [ ] **Step 2.5: Commit**

```bash
git add pkg/model/member.go pkg/model/model_test.go
git commit -m "feat(model): add ListMemberStatuses request/response types"
```

---

## Task 3: Add Feature 2 request/response types

**Files:**
- Modify: `pkg/model/member.go` (append types after the Feature 1 types from Task 2)
- Modify: `pkg/model/model_test.go` (append round-trip tests)

- [ ] **Step 3.1: Write the failing round-trip tests**

Append to `pkg/model/model_test.go` just before `func roundTrip`:

```go
func TestMentionableSubscriptionsRequestJSON(t *testing.T) {
	limit := 10
	r := model.MentionableSubscriptionsRequest{Limit: &limit, Filter: "ali"}
	roundTrip(t, &r, &model.MentionableSubscriptionsRequest{})
}

func TestMentionableSubscription_UserShape_JSON(t *testing.T) {
	s := model.MentionableSubscription{
		OptionType: "user",
		UserID:     "u-alice",
		Account:    "alice",
		SiteID:     "site-a",
		HRInfo:     &model.MentionableHRInfo{EngName: "Alice Wang", ChineseName: "愛麗絲"},
	}
	roundTrip(t, &s, &model.MentionableSubscription{})

	data, err := json.Marshal(s)
	require.NoError(t, err)
	assert.NotContains(t, string(data), `"app"`, "user-shape must omit app")
}

func TestMentionableSubscription_AppShape_JSON(t *testing.T) {
	s := model.MentionableSubscription{
		OptionType: "app",
		UserID:     "u-bot",
		Account:    "helper.bot",
		App: &model.MentionableApp{
			Name:      "Helper",
			Assistant: model.MentionableAppAssistant{Name: "helper.bot"},
		},
	}
	roundTrip(t, &s, &model.MentionableSubscription{})

	data, err := json.Marshal(s)
	require.NoError(t, err)
	assert.NotContains(t, string(data), `"hrInfo"`, "app-shape must omit hrInfo")
}

func TestMentionableSubscriptionsResponseJSON(t *testing.T) {
	r := model.MentionableSubscriptionsResponse{Subscriptions: []model.MentionableSubscription{
		{OptionType: "user", UserID: "u-a", Account: "a", SiteID: "site-a",
			HRInfo: &model.MentionableHRInfo{EngName: "A", ChineseName: "A"}},
		{OptionType: "app", UserID: "u-b", Account: "b.bot",
			App: &model.MentionableApp{Name: "B", Assistant: model.MentionableAppAssistant{Name: "b.bot"}}},
	}}
	roundTrip(t, &r, &model.MentionableSubscriptionsResponse{})
}
```

- [ ] **Step 3.2: Run test, verify it fails**

```
make test SERVICE=pkg/model 2>&1 | tail -10
```

Expected: compile failure — `undefined: model.MentionableSubscriptionsRequest`.

- [ ] **Step 3.3: Add types**

Append to `pkg/model/member.go` (after the Feature 1 block from Task 2):

```go
// MentionableSubscriptionsRequest is the body for the subscription.mentionable RPC.
// When Limit is nil the server returns min(3, room.UserCount + room.AppCount)
// rows; explicit values must be > 0 and <= room.UserCount + room.AppCount.
// Filter is treated as a literal substring (regex metacharacters are escaped by the handler).
type MentionableSubscriptionsRequest struct {
	Limit  *int   `json:"limit,omitempty"`
	Filter string `json:"filter,omitempty"`
}

type MentionableHRInfo struct {
	EngName     string `json:"engName"     bson:"engName"`
	ChineseName string `json:"chineseName" bson:"chineseName"`
}

type MentionableAppAssistant struct {
	Name string `json:"name" bson:"name"`
}

type MentionableApp struct {
	Name      string                  `json:"name"      bson:"name"`
	Assistant MentionableAppAssistant `json:"assistant" bson:"assistant"`
}

// MentionableSubscription is the projection returned by the
// subscription.mentionable RPC. HRInfo is populated only for user rows
// (OptionType == "user") and App only for app rows (OptionType == "app").
// SiteID is empty for app rows because apps have no per-site identity.
type MentionableSubscription struct {
	OptionType string             `json:"optionType" bson:"optionType"`
	UserID     string             `json:"userId"     bson:"userId"`
	Account    string             `json:"account"    bson:"account"`
	SiteID     string             `json:"siteId"     bson:"siteId"`
	HRInfo     *MentionableHRInfo `json:"hrInfo,omitempty" bson:"hrInfo,omitempty"`
	App        *MentionableApp    `json:"app,omitempty"    bson:"app,omitempty"`
}

type MentionableSubscriptionsResponse struct {
	Subscriptions []MentionableSubscription `json:"subscriptions"`
}
```

- [ ] **Step 3.4: Run test, verify it passes**

```
make test SERVICE=pkg/model 2>&1 | tail -10
```

Expected: PASS.

- [ ] **Step 3.5: Commit**

```bash
git add pkg/model/member.go pkg/model/model_test.go
git commit -m "feat(model): add MentionableSubscriptions request/response types"
```

---

## Task 4: Subject builders + wildcards

**Files:**
- Modify: `pkg/subject/subject.go` (insert next to `MemberList` and `MemberListWildcard`)
- Modify: `pkg/subject/subject_test.go` (append cases to `TestSubjectBuilders` and `TestWildcardPatterns`)

- [ ] **Step 4.1: Write the failing tests**

In `pkg/subject/subject_test.go`, append four cases to the `TestSubjectBuilders` table (insert before the closing `}` of the table, around line 63 where existing cases like `MemberRoleUpdate` live — match the existing pattern):

```go
		{"MemberStatuses", subject.MemberStatuses("alice", "r1", "site-a"),
			"chat.user.alice.request.room.r1.site-a.member.statuses"},
		{"MentionableSubscriptions", subject.MentionableSubscriptions("alice", "r1", "site-a"),
			"chat.user.alice.request.room.r1.site-a.subscription.mentionable"},
```

Append two cases to `TestWildcardPatterns` (insert before the closing `}` of the table, around line 231):

```go
		{"MemberStatusesWild", subject.MemberStatusesWildcard("site-a"),
			"chat.user.*.request.room.*.site-a.member.statuses"},
		{"MentionableSubscriptionsWild", subject.MentionableSubscriptionsWildcard("site-a"),
			"chat.user.*.request.room.*.site-a.subscription.mentionable"},
```

Also append a parse-check test (after `TestMessageRead_ParseUserRoomSubject`):

```go
func TestMemberStatuses_ParseUserRoomSubject(t *testing.T) {
	subj := subject.MemberStatuses("alice", "r1", "site-a")
	account, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok || account != "alice" || roomID != "r1" {
		t.Errorf("parse: got (%q,%q,%v), want (alice,r1,true)", account, roomID, ok)
	}
}

func TestMentionableSubscriptions_ParseUserRoomSubject(t *testing.T) {
	subj := subject.MentionableSubscriptions("alice", "r1", "site-a")
	account, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok || account != "alice" || roomID != "r1" {
		t.Errorf("parse: got (%q,%q,%v), want (alice,r1,true)", account, roomID, ok)
	}
}
```

- [ ] **Step 4.2: Run test, verify it fails**

```
make test SERVICE=pkg/subject 2>&1 | tail -10
```

Expected: compile failure — `undefined: subject.MemberStatuses`.

- [ ] **Step 4.3: Add subject builders**

In `pkg/subject/subject.go`, insert the following block immediately after `MemberListWildcard` (around line 211):

```go
// MemberStatuses is the concrete subject for the per-room member.statuses RPC.
// Pair with MemberStatusesWildcard for room-service's QueueSubscribe.
func MemberStatuses(account, roomID, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.member.statuses", account, roomID, siteID)
}

// MemberStatusesWildcard is the per-site subscription pattern for member.statuses.
func MemberStatusesWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.*.%s.member.statuses", siteID)
}

// MentionableSubscriptions is the concrete subject for the per-room
// subscription.mentionable RPC. Pair with MentionableSubscriptionsWildcard
// for room-service's QueueSubscribe.
func MentionableSubscriptions(account, roomID, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.subscription.mentionable", account, roomID, siteID)
}

// MentionableSubscriptionsWildcard is the per-site subscription pattern for subscription.mentionable.
func MentionableSubscriptionsWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.*.%s.subscription.mentionable", siteID)
}
```

- [ ] **Step 4.4: Run test, verify it passes**

```
make test SERVICE=pkg/subject 2>&1 | tail -10
```

Expected: PASS (all subject tests).

- [ ] **Step 4.5: Commit**

```bash
git add pkg/subject/subject.go pkg/subject/subject_test.go
git commit -m "feat(subject): add member.statuses + subscription.mentionable subjects"
```

---

## Task 5: Promote bot regex to constant + add sentinels + wire into sanitizeError

**Files:**
- Modify: `room-service/helper.go` (lines 14–64)
- Modify: `room-service/helper_test.go` (append cases to `TestSanitizeError`)

- [ ] **Step 5.1: Write failing sanitizeError test cases**

In `room-service/helper_test.go`, inside `TestSanitizeError` (around line 61, after the `errListOffsetInvalid` case), insert:

```go
		{"sentinel: member statuses limit invalid", errMemberStatusesLimitInvalid, "limit must be > 0 and <= room user count"},
		{"sentinel: mentionable limit invalid", errMentionableLimitInvalid, "limit must be > 0 and <= room user count + app count"},
```

- [ ] **Step 5.2: Run test, verify it fails**

```
make test SERVICE=room-service 2>&1 | tail -10
```

Expected: compile failure — `undefined: errMemberStatusesLimitInvalid`.

- [ ] **Step 5.3: Add two regex constants and two new limit-validation sentinels**

> **Shipped:** the original plan combined bot and platform-admin into a single
> regex. The final code splits them into two narrower constants
> (`botAccountRegex` = `.bot` suffix; `platformAdminRegex` = `p_` prefix) and
> exposes the Go-side predicates as `(*model.User).IsBot()` and
> `(*model.User).IsPlatformAdmin()` (defined in `pkg/model/user.go`). The
> standalone `botPattern` variable is no longer needed — all classification
> happens through the regex constants Mongo-side or the User methods Go-side.

In `room-service/helper.go`, replace lines 14–64 (the `var (...)` block) with:

```go
// botAccountRegex matches assistant-bot accounts by their `.bot` suffix.
// Used Mongo-side in pipelines such as ListMentionableSubscriptions to
// classify a subscription row as an `app` in the discriminated union.
// Go-side the equivalent is (*model.User).IsBot.
const botAccountRegex = `\.bot$`

// platformAdminRegex matches platform-admin / webhook accounts by their
// `p_` prefix. Mentionable-autocomplete hides these accounts entirely so
// they do not appear as `@`-mention targets. Go-side the equivalent is
// (*model.User).IsPlatformAdmin.
const platformAdminRegex = `^p_`

// Sentinel errors for user-facing validation failures.
var (
	errInvalidRole      = errors.New("invalid role: must be owner or member")
	errOnlyOwners       = errors.New("only owners can update roles")
	errAlreadyOwner     = errors.New("user is already an owner")
	errNotOwner         = errors.New("user is not an owner")
	errCannotDemoteLast = errors.New("cannot demote the last owner")
	errRoomTypeGuard    = errors.New("role update is only allowed in channel rooms")
	errTargetNotMember  = errors.New("target user is not a member of this room")
	// Used by both list-members (requester subscription check) and add-member
	// channel-source expansion. Both contexts mean "the requester is not a
	// member of the room they are asking about".
	errNotRoomMember     = errors.New("not a room member")
	errInvalidOrg        = errors.New("invalid org")
	errInvalidThreadID   = errors.New("threadId is required")
	errThreadSubNotFound = errors.New("thread subscription not found")
	// Only subscribers with an individual membership source can hold the owner
	// role. Remove-member's dual-membership path relies on this invariant:
	// stripping the owner role during an individual-leave is only sound when
	// the role can only be held alongside an individual entry.
	errPromoteRequiresIndividual = errors.New("only individual members can be promoted to owner")

	// Sentinels for create-room validation.
	errEmptyCreateRequest  = errors.New("request must include at least one of users, orgs, channels, or name")
	errSelfDM              = errors.New("cannot create a DM with yourself")
	errBotInChannel        = errors.New("bots cannot be added to a channel")
	errBotNotAvailable     = errors.New("bot not available")
	errInvalidUserData     = errors.New("user is missing required name fields")
	errMissingRequestID    = errors.New("missing X-Request-ID header")
	errInvalidRequestID    = errors.New("invalid X-Request-ID format")
	errChannelNameRequired = errors.New("channel name is required")
	errChannelNameTooLong  = errors.New("channel name must be at most 100 characters")
	errUserNotFound        = errors.New("user not found")

	errMessageNotFound     = errors.New("message not found")
	errMessageRoomMismatch = errors.New("message does not belong to this room")
	errNotMessageSender    = errors.New("only the message sender can view read receipts")

	// Sentinels for remove-member validation (surfaced to the client verbatim).
	errRemoveTargetAmbiguous    = errors.New("exactly one of account or orgId must be set")
	errCannotRemoveLastMember   = errors.New("cannot remove the last member of the room")
	errLastOwnerCannotLeave     = errors.New("last owner cannot leave the room")
	errOrgMemberCannotLeaveSolo = errors.New("org members cannot leave individually")
	errRoomIDMismatch           = errors.New("room ID mismatch")
	errRemoveChannelOnly        = errors.New("remove-member only supported on channel rooms")

	// Sentinels for list-members pagination validation.
	errListLimitInvalid  = errors.New("limit must be > 0")
	errListOffsetInvalid = errors.New("offset must be >= 0")

	// Sentinels for member.statuses + subscription.mentionable limit validation.
	errMemberStatusesLimitInvalid = errors.New("limit must be > 0 and <= room user count")
	errMentionableLimitInvalid    = errors.New("limit must be > 0 and <= room user count + app count")
)

```

Then, in `sanitizeError` (around line 184–224), add the two new sentinels to the pass-through `switch`. Find this block:

```go
			errors.Is(err, errListLimitInvalid),
			errors.Is(err, errListOffsetInvalid),
			errors.Is(err, &dmExistsError{}),
```

Replace with:

```go
			errors.Is(err, errListLimitInvalid),
			errors.Is(err, errListOffsetInvalid),
			errors.Is(err, errMemberStatusesLimitInvalid),
			errors.Is(err, errMentionableLimitInvalid),
			errors.Is(err, &dmExistsError{}),
```

- [ ] **Step 5.4: Run test, verify it passes**

```
make test SERVICE=room-service 2>&1 | tail -10
```

Expected: PASS (including the two new `TestSanitizeError` cases).

- [ ] **Step 5.5: Commit**

```bash
git add room-service/helper.go room-service/helper_test.go
git commit -m "refactor(room-service): expose bot regex constant + add limit sentinels"
```

---

## Task 6: Add `ListMemberStatuses` (interface + Mongo impl + integration test)

Combines the interface declaration, mock regeneration, and `MongoStore` implementation into one commit so the working tree never lands in a "interface declared but unimplemented" state (which would fail the repo's pre-commit hook).

**Files:**
- Modify: `room-service/store.go` (interface)
- Regenerate: `room-service/mock_store_test.go`
- Modify: `room-service/store_mongo.go`
- Modify: `room-service/integration_test.go`

- [ ] **Step 6.1: Write the failing integration test**

Append to `room-service/integration_test.go`:

```go
func TestMongoStore_ListMemberStatuses_Integration(t *testing.T) {
	ctx := context.Background()

	t.Run("projects five fields and respects limit", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)

		mustInsertUser(t, db, &model.User{
			ID: "u-alice", Account: "alice", EngName: "Alice Wang", ChineseName: "愛麗絲",
			StatusIsShow: true, StatusText: "available",
		})
		mustInsertUser(t, db, &model.User{
			ID: "u-bob", Account: "bob", EngName: "Bob Chen", ChineseName: "陳博",
			StatusIsShow: false, StatusText: "in a meeting",
		})
		mustInsertSub(t, db, &model.Subscription{
			ID: "sub-a", User: model.SubscriptionUser{ID: "u-alice", Account: "alice"},
			RoomID: "r1", SiteID: "site-a",
		})
		mustInsertSub(t, db, &model.Subscription{
			ID: "sub-b", User: model.SubscriptionUser{ID: "u-bob", Account: "bob"},
			RoomID: "r1", SiteID: "site-a",
		})

		got, err := store.ListMemberStatuses(ctx, "r1", 5)
		require.NoError(t, err)
		require.Len(t, got, 2)
		byAcct := map[string]model.MemberStatus{}
		for _, m := range got {
			byAcct[m.Account] = m
		}
		assert.Equal(t, model.MemberStatus{
			Account: "alice", EngName: "Alice Wang", ChineseName: "愛麗絲",
			StatusIsShow: true, StatusText: "available",
		}, byAcct["alice"])
		assert.Equal(t, model.MemberStatus{
			Account: "bob", EngName: "Bob Chen", ChineseName: "陳博",
			StatusIsShow: false, StatusText: "in a meeting",
		}, byAcct["bob"])
	})

	t.Run("limit caps the result count", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		for i := 0; i < 5; i++ {
			acct := fmt.Sprintf("user%d", i)
			mustInsertUser(t, db, &model.User{ID: "u-" + acct, Account: acct, EngName: acct, ChineseName: acct})
			mustInsertSub(t, db, &model.Subscription{
				ID: "sub-" + acct, User: model.SubscriptionUser{ID: "u-" + acct, Account: acct},
				RoomID: "r1", SiteID: "site-a",
			})
		}
		got, err := store.ListMemberStatuses(ctx, "r1", 2)
		require.NoError(t, err)
		require.Len(t, got, 2)
	})

	t.Run("subscription with missing user doc is dropped", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		mustInsertUser(t, db, &model.User{
			ID: "u-alice", Account: "alice", EngName: "Alice", ChineseName: "愛", StatusText: "x",
		})
		mustInsertSub(t, db, &model.Subscription{
			ID: "sub-a", User: model.SubscriptionUser{ID: "u-alice", Account: "alice"},
			RoomID: "r1", SiteID: "site-a",
		})
		mustInsertSub(t, db, &model.Subscription{
			ID: "sub-ghost", User: model.SubscriptionUser{ID: "u-ghost", Account: "ghost"},
			RoomID: "r1", SiteID: "site-a",
		})
		got, err := store.ListMemberStatuses(ctx, "r1", 10)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "alice", got[0].Account)
	})

	t.Run("empty room returns empty slice", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		got, err := store.ListMemberStatuses(ctx, "r-empty", 5)
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}
```

- [ ] **Step 6.2: Run integration test, verify it fails**

```
make test-integration SERVICE=room-service 2>&1 | tail -20
```

Expected: compile failure — `s.ListMemberStatuses undefined`.

- [ ] **Step 6.3: Add the method signature to `RoomStore`**

In `room-service/store.go`, in the `RoomStore` interface (around line 43), insert this method after `GetSubscription` (around line 46):

```go
	// ListMemberStatuses returns up to `limit` members of roomID, each
	// projected from the corresponding users document as {account, engName,
	// chineseName, statusIsShow, statusText}. Subscriptions whose user
	// document is missing are dropped. Caller is responsible for the limit
	// cap (handler enforces > 0 and <= room.UserCount).
	ListMemberStatuses(ctx context.Context, roomID string, limit int) ([]model.MemberStatus, error)
```

- [ ] **Step 6.4: Regenerate mocks**

```
make generate SERVICE=room-service
```

Expected: `mock_store_test.go` rewritten with a `ListMemberStatuses` mock method.

- [ ] **Step 6.5: Implement `ListMemberStatuses` on `MongoStore`**

Append to `room-service/store_mongo.go`:

```go
// ListMemberStatuses returns up to `limit` members of roomID, each projected
// from the joined users document as MemberStatus. Subscriptions whose user
// document has been deleted are dropped by the $unwind with
// preserveNullAndEmptyArrays:false rather than returned half-populated.
func (s *MongoStore) ListMemberStatuses(ctx context.Context, roomID string, limit int) ([]model.MemberStatus, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"roomId": roomID}}},
		{{Key: "$lookup", Value: bson.M{
			"from": "users",
			"let":  bson.M{"acct": "$u.account"},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$eq": bson.A{"$account", "$$acct"}}}},
				bson.M{"$limit": 1},
				bson.M{"$project": bson.M{
					"_id":          0,
					"account":      1,
					"engName":      1,
					"chineseName":  1,
					"statusIsShow": 1,
					"statusText":   1,
				}},
			},
			"as": "user",
		}}},
		{{Key: "$unwind", Value: bson.M{"path": "$user", "preserveNullAndEmptyArrays": false}}},
		{{Key: "$replaceWith", Value: "$user"}},
		{{Key: "$limit", Value: int64(limit)}},
	}
	cursor, err := s.subscriptions.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate member statuses for %q: %w", roomID, err)
	}
	defer cursor.Close(ctx)
	members := []model.MemberStatus{}
	if err := cursor.All(ctx, &members); err != nil {
		return nil, fmt.Errorf("decode member statuses for %q: %w", roomID, err)
	}
	return members, nil
}
```

- [ ] **Step 6.6: Run unit + integration tests, verify they pass**

```
make test SERVICE=room-service && make test-integration SERVICE=room-service 2>&1 | tail -10
```

Expected: all `TestMongoStore_ListMemberStatuses_Integration` subtests PASS; mock-driven unit tests still green.

- [ ] **Step 6.7: Commit**

```bash
git add room-service/store.go room-service/mock_store_test.go room-service/store_mongo.go room-service/integration_test.go
git commit -m "feat(room-service): add ListMemberStatuses store method + integration tests"
```

---

## Task 7: Implement `handleListMemberStatuses` + register subscription (unit TDD)

**Files:**
- Modify: `room-service/handler.go` (add nats wrapper, handler func, registration)
- Modify: `room-service/handler_test.go` (table-driven test)

- [ ] **Step 7.1: Write the failing handler test**

Append to `room-service/handler_test.go`:

```go
func TestHandler_ListMemberStatuses(t *testing.T) {
	const siteID = "site-a"
	const roomID = "r1"
	const requester = "alice"
	subj := subject.MemberStatuses(requester, roomID, siteID)

	stub := []model.MemberStatus{
		{Account: "alice", EngName: "Alice", ChineseName: "愛", StatusIsShow: true, StatusText: "available"},
		{Account: "bob", EngName: "Bob", ChineseName: "博"},
	}

	type want struct {
		errContains string
		errIs       error
		members     []model.MemberStatus
	}
	tests := []struct {
		name      string
		subject   string
		body      []byte
		setupMock func(*MockRoomStore)
		want      want
	}{
		{
			name:    "default limit 3, happy path",
			subject: subj,
			body:    nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 10}, nil)
				s.EXPECT().ListMemberStatuses(gomock.Any(), roomID, 3).Return(stub, nil)
			},
			want: want{members: stub},
		},
		{
			name:    "explicit limit passes through",
			subject: subj,
			body:    []byte(`{"limit":7}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 10}, nil)
				s.EXPECT().ListMemberStatuses(gomock.Any(), roomID, 7).Return(stub, nil)
			},
			want: want{members: stub},
		},
		{
			name:    "requester not a member",
			subject: subj,
			body:    nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(nil, fmt.Errorf("missing: %w", model.ErrSubscriptionNotFound))
			},
			want: want{errIs: errNotRoomMember},
		},
		{
			name:    "limit zero",
			subject: subj,
			body:    []byte(`{"limit":0}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).Return(&model.Room{ID: roomID, UserCount: 10}, nil)
			},
			want: want{errIs: errMemberStatusesLimitInvalid},
		},
		{
			name:    "limit negative",
			subject: subj,
			body:    []byte(`{"limit":-1}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).Return(&model.Room{ID: roomID, UserCount: 10}, nil)
			},
			want: want{errIs: errMemberStatusesLimitInvalid},
		},
		{
			name:    "limit exceeds room.UserCount",
			subject: subj,
			body:    []byte(`{"limit":11}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).Return(&model.Room{ID: roomID, UserCount: 10}, nil)
			},
			want: want{errIs: errMemberStatusesLimitInvalid},
		},
		{
			name:    "GetRoom errors",
			subject: subj,
			body:    nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).Return(nil, fmt.Errorf("mongo exploded"))
			},
			want: want{errContains: "get room"},
		},
		{
			name:    "store errors",
			subject: subj,
			body:    nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).Return(&model.Room{ID: roomID, UserCount: 10}, nil)
				s.EXPECT().ListMemberStatuses(gomock.Any(), roomID, 3).
					Return(nil, fmt.Errorf("mongo exploded"))
			},
			want: want{errContains: "list member statuses"},
		},
		{
			name:      "invalid subject",
			subject:   "chat.garbage",
			body:      nil,
			setupMock: func(s *MockRoomStore) {},
			want:      want{errContains: "invalid member-statuses subject"},
		},
		{
			name:    "malformed JSON body",
			subject: subj,
			body:    []byte("{not json"),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
			},
			want: want{errContains: "invalid request"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockRoomStore(ctrl)
			tc.setupMock(store)

			h := &Handler{store: store, siteID: siteID}
			resp, err := h.handleListMemberStatuses(context.Background(), tc.subject, tc.body)

			if tc.want.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.want.errContains)
				return
			}
			if tc.want.errIs != nil {
				require.Error(t, err)
				assert.True(t, errors.Is(err, tc.want.errIs), "error chain should contain %v, got %v", tc.want.errIs, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.want.members, resp.Members)
		})
	}
}
```

- [ ] **Step 7.2: Run test, verify it fails**

```
make test SERVICE=room-service 2>&1 | tail -10
```

Expected: compile failure — `h.handleListMemberStatuses undefined`.

- [ ] **Step 7.3: Implement handler + register**

In `room-service/handler.go`, insert after the existing `natsListMembers` / `handleListMembers` pair (around line 452):

```go
func (h *Handler) natsListMemberStatuses(m otelnats.Msg) {
	ctx := wrappedCtx(m)
	resp, err := h.handleListMemberStatuses(ctx, m.Msg.Subject, m.Msg.Data)
	if err != nil {
		slog.Error("list member statuses failed", "error", err)
		natsutil.ReplyError(m.Msg, sanitizeError(err))
		return
	}
	natsutil.ReplyJSON(m.Msg, resp)
}

func (h *Handler) handleListMemberStatuses(ctx context.Context, subj string, data []byte) (model.ListMemberStatusesResponse, error) {
	requesterAccount, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok {
		return model.ListMemberStatusesResponse{}, errcode.BadRequest("invalid member-statuses subject")
	}

	var req model.ListMemberStatusesRequest
	if len(data) > 0 {
		if err := json.Unmarshal(data, &req); err != nil {
			return model.ListMemberStatusesResponse{}, errcode.BadRequest("invalid request")
		}
	}

	// Parallel membership probe + room load with membership-error precedence.
	// See requireMembershipAndGetRoom; sync.WaitGroup (not errgroup.WithContext)
	// so a fast GetRoom failure cannot cancel GetSubscription and mask the
	// not-member sentinel as context.Canceled.
	room, err := h.requireMembershipAndGetRoom(ctx, requesterAccount, roomID)
	if err != nil {
		return model.ListMemberStatusesResponse{}, err
	}

	// Default = min(3, room.UserCount); empty room short-circuits with []
	// (not zero-value, so the wire shape stays an empty array).
	var limit int
	if req.Limit == nil {
		if room.UserCount == 0 {
			return model.ListMemberStatusesResponse{Members: []model.MemberStatus{}}, nil
		}
		limit = min(defaultMemberStatusesLimit, room.UserCount)
	} else {
		limit = *req.Limit
		if limit <= 0 || limit > room.UserCount {
			return model.ListMemberStatusesResponse{}, errMemberStatusesLimitInvalid
		}
	}

	members, err := h.store.ListMemberStatuses(ctx, roomID, limit)
	if err != nil {
		return model.ListMemberStatusesResponse{}, fmt.Errorf("list member statuses: %w", err)
	}
	return model.ListMemberStatusesResponse{Members: members}, nil
}
```

Register in `RegisterCRUD` (around line 104, just before the closing `return nil`):

```go
	if _, err := nc.QueueSubscribe(subject.MemberStatusesWildcard(h.siteID), queue, h.natsListMemberStatuses); err != nil {
		return fmt.Errorf("subscribe member statuses: %w", err)
	}
```

- [ ] **Step 7.4: Run test, verify it passes**

```
make test SERVICE=room-service 2>&1 | tail -10
```

Expected: PASS — all `TestHandler_ListMemberStatuses` subtests green, no regressions.

- [ ] **Step 7.5: Commit**

```bash
git add room-service/handler.go room-service/handler_test.go
git commit -m "feat(room-service): add member.statuses RPC handler"
```

---

## Task 8: Add `ListMentionableSubscriptions` (interface + Mongo impl + integration test)

Same merged shape as Task 6 — keeps every commit in a passing state.

**Files:**
- Modify: `room-service/store.go` (interface)
- Regenerate: `room-service/mock_store_test.go`
- Modify: `room-service/store_mongo.go`
- Modify: `room-service/integration_test.go`

- [ ] **Step 8.1: Write the failing integration test**

Append to `room-service/integration_test.go`:

```go
func TestMongoStore_ListMentionableSubscriptions_Integration(t *testing.T) {
	ctx := context.Background()

	seedThree := func(t *testing.T, db *mongo.Database) {
		t.Helper()
		mustInsertUser(t, db, &model.User{ID: "u-alice", Account: "alice", SiteID: "site-a",
			EngName: "Alice Wang", ChineseName: "愛麗絲"})
		mustInsertUser(t, db, &model.User{ID: "u-bob", Account: "bob", SiteID: "site-b",
			EngName: "Bob Chen", ChineseName: "陳博"})
		// Bot user document — apps still join through this row when present.
		mustInsertUser(t, db, &model.User{ID: "u-bot", Account: "helper.bot"})
		_, err := db.Collection("apps").InsertOne(ctx, model.App{
			ID:        "app-1",
			Name:      "Helper",
			Assistant: &model.AppAssistant{Enabled: true, Name: "helper.bot"},
		})
		require.NoError(t, err)
		mustInsertSub(t, db, &model.Subscription{ID: "sub-a",
			User: model.SubscriptionUser{ID: "u-alice", Account: "alice"}, RoomID: "r1", SiteID: "site-a"})
		mustInsertSub(t, db, &model.Subscription{ID: "sub-b",
			User: model.SubscriptionUser{ID: "u-bob", Account: "bob"}, RoomID: "r1", SiteID: "site-a"})
		mustInsertSub(t, db, &model.Subscription{ID: "sub-bot",
			User: model.SubscriptionUser{ID: "u-bot", Account: "helper.bot", IsBot: true},
			RoomID: "r1", SiteID: "site-a"})
	}

	t.Run("classifies user vs app and shapes response", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		seedThree(t, db)

		got, err := store.ListMentionableSubscriptions(ctx, "r1", "", "", 10)
		require.NoError(t, err)
		require.Len(t, got, 3)

		byAcct := map[string]model.MentionableSubscription{}
		for _, s := range got {
			byAcct[s.Account] = s
		}

		require.Contains(t, byAcct, "alice")
		assert.Equal(t, "user", byAcct["alice"].OptionType)
		assert.Equal(t, "u-alice", byAcct["alice"].UserID)
		assert.Equal(t, "site-a", byAcct["alice"].SiteID)
		require.NotNil(t, byAcct["alice"].HRInfo)
		assert.Equal(t, "Alice Wang", byAcct["alice"].HRInfo.EngName)
		assert.Equal(t, "愛麗絲", byAcct["alice"].HRInfo.ChineseName)
		assert.Nil(t, byAcct["alice"].App)

		require.Contains(t, byAcct, "helper.bot")
		assert.Equal(t, "app", byAcct["helper.bot"].OptionType)
		assert.Equal(t, "u-bot", byAcct["helper.bot"].UserID)
		assert.Equal(t, "", byAcct["helper.bot"].SiteID, "app rows must have empty siteId")
		assert.Nil(t, byAcct["helper.bot"].HRInfo)
		require.NotNil(t, byAcct["helper.bot"].App)
		assert.Equal(t, "Helper", byAcct["helper.bot"].App.Name)
		assert.Equal(t, "helper.bot", byAcct["helper.bot"].App.Assistant.Name)
	})

	t.Run("excludeAccount filters caller", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		seedThree(t, db)

		got, err := store.ListMentionableSubscriptions(ctx, "r1", "alice", "", 10)
		require.NoError(t, err)
		require.Len(t, got, 2)
		for _, s := range got {
			assert.NotEqual(t, "alice", s.Account)
		}
	})

	t.Run("filter is case-insensitive substring on keyword", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		seedThree(t, db)

		got, err := store.ListMentionableSubscriptions(ctx, "r1", "", "BOB", 10)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "bob", got[0].Account)

		got, err = store.ListMentionableSubscriptions(ctx, "r1", "", "陳", 10)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "bob", got[0].Account)

		got, err = store.ListMentionableSubscriptions(ctx, "r1", "", "Helper", 10)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "helper.bot", got[0].Account)
	})

	t.Run("p_ prefix is classified as app", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		mustInsertUser(t, db, &model.User{ID: "u-pbot", Account: "p_assistant"})
		_, err := db.Collection("apps").InsertOne(ctx, model.App{
			ID: "app-p", Name: "PApp",
			Assistant: &model.AppAssistant{Enabled: true, Name: "p_assistant"},
		})
		require.NoError(t, err)
		mustInsertSub(t, db, &model.Subscription{ID: "sub-p",
			User: model.SubscriptionUser{ID: "u-pbot", Account: "p_assistant", IsBot: true},
			RoomID: "r1", SiteID: "site-a"})

		got, err := store.ListMentionableSubscriptions(ctx, "r1", "", "", 10)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "app", got[0].OptionType)
	})

	t.Run("limit caps the result count", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		seedThree(t, db)

		got, err := store.ListMentionableSubscriptions(ctx, "r1", "", "", 2)
		require.NoError(t, err)
		require.Len(t, got, 2)
	})

	t.Run("empty room returns empty slice", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		got, err := store.ListMentionableSubscriptions(ctx, "r-empty", "", "", 5)
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("orphan bot subscription returns empty app strings, not null", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		mustInsertUser(t, db, &model.User{ID: "u-ghost", Account: "ghost.bot"})
		mustInsertSub(t, db, &model.Subscription{ID: "sub-ghost",
			User: model.SubscriptionUser{ID: "u-ghost", Account: "ghost.bot", IsBot: true},
			RoomID: "r1", SiteID: "site-a"})

		got, err := store.ListMentionableSubscriptions(ctx, "r1", "", "", 5)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "app", got[0].OptionType)
		require.NotNil(t, got[0].App)
		assert.Equal(t, "", got[0].App.Name)
		assert.Equal(t, "", got[0].App.Assistant.Name)
	})
}
```

- [ ] **Step 8.2: Run integration test, verify it fails**

```
make test-integration SERVICE=room-service 2>&1 | tail -20
```

Expected: compile failure — `s.ListMentionableSubscriptions undefined`.

- [ ] **Step 8.3: Add the method signature to `RoomStore`**

In `room-service/store.go`, in the `RoomStore` interface, insert immediately after `ListMemberStatuses` (added in Task 6):

```go
	// ListMentionableSubscriptions returns up to `limit` mentionable members
	// of roomID (users + apps), excluding excludeAccount, whose searchable
	// keyword matches escapedFilter (case-insensitive substring). escapedFilter
	// must already be regex-escaped (the handler runs regexp.QuoteMeta).
	// Empty escapedFilter matches everything.
	ListMentionableSubscriptions(ctx context.Context, roomID, excludeAccount, escapedFilter string, limit int) ([]model.MentionableSubscription, error)
```

- [ ] **Step 8.4: Regenerate mocks**

```
make generate SERVICE=room-service
```

Expected: `mock_store_test.go` rewritten with a `ListMentionableSubscriptions` mock method.

- [ ] **Step 8.5: Implement `ListMentionableSubscriptions` on `MongoStore`**

Append to `room-service/store_mongo.go`:

```go
// ListMentionableSubscriptions returns up to `limit` mentionable members of
// roomID (users + apps) whose dash-joined keyword (account, engName,
// chineseName, app.name, app.assistant.name) matches escapedFilter under
// case-insensitive regex. excludeAccount is dropped at the $match stage so
// the caller never sees themselves. App rows carry empty SiteID and a non-nil
// App; user rows carry a non-nil HRInfo. Orphan rows (bot sub with no apps
// doc, or human sub with no users doc) return empty strings rather than null
// leaves so the wire shape is well-typed.
func (s *MongoStore) ListMentionableSubscriptions(
	ctx context.Context, roomID, excludeAccount, escapedFilter string, limit int,
) ([]model.MentionableSubscription, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{
			"roomId": roomID,
			"u.account": bson.M{
				"$ne":  excludeAccount,
				"$not": bson.M{"$regex": platformAdminRegex},
			},
		}}},
		{{Key: "$lookup", Value: bson.M{
			"from": "users",
			"let":  bson.M{"acct": "$u.account"},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$eq": bson.A{"$account", "$$acct"}}}},
				bson.M{"$limit": 1},
				bson.M{"$project": bson.M{
					"_id": 0, "account": 1, "engName": 1, "chineseName": 1, "siteId": 1,
				}},
			},
			"as": "_users",
		}}},
		{{Key: "$lookup", Value: bson.M{
			"from": "apps",
			"let":  bson.M{"acct": "$u.account"},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$eq": bson.A{"$assistant.name", "$$acct"}}}},
				bson.M{"$limit": 1},
				bson.M{"$project": bson.M{
					"_id": 0, "name": 1, "assistant.name": 1,
				}},
			},
			"as": "_apps",
		}}},
		{{Key: "$addFields", Value: bson.M{
			"isApp":   bson.M{"$regexMatch": bson.M{"input": "$u.account", "regex": botAccountRegex}},
			"userDoc": bson.M{"$arrayElemAt": bson.A{"$_users", 0}},
			"appDoc":  bson.M{"$arrayElemAt": bson.A{"$_apps", 0}},
		}}},
		{{Key: "$addFields", Value: bson.M{
			"keyword": bson.M{"$concat": bson.A{
				bson.M{"$ifNull": bson.A{"$u.account", ""}}, "-",
				bson.M{"$ifNull": bson.A{"$userDoc.engName", ""}}, "-",
				bson.M{"$ifNull": bson.A{"$userDoc.chineseName", ""}}, "-",
				bson.M{"$ifNull": bson.A{"$appDoc.name", ""}}, "-",
				bson.M{"$ifNull": bson.A{"$appDoc.assistant.name", ""}},
			}},
		}}},
		{{Key: "$match", Value: bson.M{
			"keyword": bson.M{"$regex": escapedFilter, "$options": "i"},
		}}},
		{{Key: "$limit", Value: int64(limit)}},
		{{Key: "$project", Value: bson.M{
			"_id":        0,
			"optionType": bson.M{"$cond": bson.A{"$isApp", "app", "user"}},
			"userId":     "$u._id",
			"account":    "$u.account",
			"siteId": bson.M{"$cond": bson.A{
				"$isApp",
				"",
				bson.M{"$ifNull": bson.A{"$userDoc.siteId", ""}},
			}},
			"hrInfo": bson.M{"$cond": bson.A{
				"$isApp",
				"$$REMOVE",
				bson.M{
					"engName":     bson.M{"$ifNull": bson.A{"$userDoc.engName", ""}},
					"chineseName": bson.M{"$ifNull": bson.A{"$userDoc.chineseName", ""}},
				},
			}},
			"app": bson.M{"$cond": bson.A{
				"$isApp",
				bson.M{
					"name": bson.M{"$ifNull": bson.A{"$appDoc.name", ""}},
					"assistant": bson.M{
						"name": bson.M{"$ifNull": bson.A{"$appDoc.assistant.name", ""}},
					},
				},
				"$$REMOVE",
			}},
		}}},
	}

	cursor, err := s.subscriptions.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate mentionable subscriptions for %q: %w", roomID, err)
	}
	defer cursor.Close(ctx)
	subs := []model.MentionableSubscription{}
	if err := cursor.All(ctx, &subs); err != nil {
		return nil, fmt.Errorf("decode mentionable subscriptions for %q: %w", roomID, err)
	}
	return subs, nil
}
```

- [ ] **Step 8.6: Run unit + integration tests, verify they pass**

```
make test SERVICE=room-service && make test-integration SERVICE=room-service 2>&1 | tail -10
```

Expected: all `TestMongoStore_ListMentionableSubscriptions_Integration` subtests PASS; mock-driven unit tests still green.

- [ ] **Step 8.7: Commit**

```bash
git add room-service/store.go room-service/mock_store_test.go room-service/store_mongo.go room-service/integration_test.go
git commit -m "feat(room-service): add ListMentionableSubscriptions store method + integration tests"
```

---

## Task 9: Implement `handleListMentionableSubscriptions` + register (unit TDD)

**Files:**
- Modify: `room-service/handler.go` (add nats wrapper, handler func, registration; add `regexp` import)
- Modify: `room-service/handler_test.go` (table-driven test)

- [ ] **Step 9.1: Write the failing handler test**

Append to `room-service/handler_test.go`:

```go
func TestHandler_ListMentionableSubscriptions(t *testing.T) {
	const siteID = "site-a"
	const roomID = "r1"
	const requester = "alice"
	subj := subject.MentionableSubscriptions(requester, roomID, siteID)

	stub := []model.MentionableSubscription{
		{OptionType: "user", UserID: "u-bob", Account: "bob", SiteID: "site-a",
			HRInfo: &model.MentionableHRInfo{EngName: "Bob", ChineseName: "博"}},
	}

	type want struct {
		errContains string
		errIs       error
		subs        []model.MentionableSubscription
	}
	tests := []struct {
		name      string
		subject   string
		body      []byte
		setupMock func(*MockRoomStore)
		want      want
	}{
		{
			name:    "default limit 3, empty filter, happy path",
			subject: subj,
			body:    nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 5, AppCount: 2}, nil)
				s.EXPECT().
					ListMentionableSubscriptions(gomock.Any(), roomID, requester, "", 3).
					Return(stub, nil)
			},
			want: want{subs: stub},
		},
		{
			name:    "explicit limit and filter passed through",
			subject: subj,
			body:    []byte(`{"limit":3,"filter":"bo"}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 5, AppCount: 2}, nil)
				s.EXPECT().
					ListMentionableSubscriptions(gomock.Any(), roomID, requester, "bo", 3).
					Return(stub, nil)
			},
			want: want{subs: stub},
		},
		{
			name:    "regex metacharacters in filter are escaped",
			subject: subj,
			body:    []byte(`{"limit":3,"filter":"a.b(c"}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 5, AppCount: 2}, nil)
				s.EXPECT().
					ListMentionableSubscriptions(gomock.Any(), roomID, requester, `a\.b\(c`, 3).
					Return([]model.MentionableSubscription{}, nil)
			},
			want: want{subs: []model.MentionableSubscription{}},
		},
		{
			name:    "requester not a member",
			subject: subj,
			body:    nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(nil, fmt.Errorf("missing: %w", model.ErrSubscriptionNotFound))
			},
			want: want{errIs: errNotRoomMember},
		},
		{
			name:    "limit zero",
			subject: subj,
			body:    []byte(`{"limit":0}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 5, AppCount: 2}, nil)
			},
			want: want{errIs: errMentionableLimitInvalid},
		},
		{
			name:    "limit negative",
			subject: subj,
			body:    []byte(`{"limit":-1}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 5, AppCount: 2}, nil)
			},
			want: want{errIs: errMentionableLimitInvalid},
		},
		{
			name:    "limit exceeds UserCount + AppCount",
			subject: subj,
			body:    []byte(`{"limit":8}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 5, AppCount: 2}, nil)
			},
			want: want{errIs: errMentionableLimitInvalid},
		},
		{
			name:    "limit at cap is accepted",
			subject: subj,
			body:    []byte(`{"limit":7}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 5, AppCount: 2}, nil)
				s.EXPECT().
					ListMentionableSubscriptions(gomock.Any(), roomID, requester, "", 7).
					Return(stub, nil)
			},
			want: want{subs: stub},
		},
		{
			name:    "GetRoom errors",
			subject: subj,
			body:    nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).Return(nil, fmt.Errorf("mongo exploded"))
			},
			want: want{errContains: "get room"},
		},
		{
			name:    "store errors",
			subject: subj,
			body:    nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 5, AppCount: 2}, nil)
				s.EXPECT().
					ListMentionableSubscriptions(gomock.Any(), roomID, requester, "", 3).
					Return(nil, fmt.Errorf("mongo exploded"))
			},
			want: want{errContains: "list mentionable subscriptions"},
		},
		{
			name:      "invalid subject",
			subject:   "chat.garbage",
			body:      nil,
			setupMock: func(s *MockRoomStore) {},
			want:      want{errContains: "invalid mentionable-subscriptions subject"},
		},
		{
			name:    "malformed JSON body",
			subject: subj,
			body:    []byte("{not json"),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
			},
			want: want{errContains: "invalid request"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockRoomStore(ctrl)
			tc.setupMock(store)

			h := &Handler{store: store, siteID: siteID}
			resp, err := h.handleListMentionableSubscriptions(context.Background(), tc.subject, tc.body)

			if tc.want.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.want.errContains)
				return
			}
			if tc.want.errIs != nil {
				require.Error(t, err)
				assert.True(t, errors.Is(err, tc.want.errIs), "error chain should contain %v, got %v", tc.want.errIs, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.want.subs, resp.Subscriptions)
		})
	}
}
```

- [ ] **Step 9.2: Run test, verify it fails**

```
make test SERVICE=room-service 2>&1 | tail -10
```

Expected: compile failure — `h.handleListMentionableSubscriptions undefined`.

- [ ] **Step 9.3: Implement handler + register + add `regexp` import**

In `room-service/handler.go`, add `"regexp"` to the import block (after `"log/slog"` near line 9):

```go
	"log/slog"
	"regexp"
	"slices"
```

Insert after the new `handleListMemberStatuses` from Task 7:

```go
func (h *Handler) natsListMentionableSubscriptions(m otelnats.Msg) {
	ctx := wrappedCtx(m)
	resp, err := h.handleListMentionableSubscriptions(ctx, m.Msg.Subject, m.Msg.Data)
	if err != nil {
		slog.Error("list mentionable subscriptions failed", "error", err)
		natsutil.ReplyError(m.Msg, sanitizeError(err))
		return
	}
	natsutil.ReplyJSON(m.Msg, resp)
}

func (h *Handler) handleListMentionableSubscriptions(ctx context.Context, subj string, data []byte) (model.MentionableSubscriptionsResponse, error) {
	requesterAccount, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok {
		return model.MentionableSubscriptionsResponse{}, errcode.BadRequest("invalid mentionable-subscriptions subject")
	}

	var req model.MentionableSubscriptionsRequest
	if len(data) > 0 {
		if err := json.Unmarshal(data, &req); err != nil {
			return model.MentionableSubscriptionsResponse{}, errcode.BadRequest("invalid request")
		}
	}

	// Parallel membership probe + room load with membership-error precedence —
	// see requireMembershipAndGetRoom (sync.WaitGroup; membership wins over
	// a racing GetRoom error).
	room, err := h.requireMembershipAndGetRoom(ctx, requesterAccount, roomID)
	if err != nil {
		return model.MentionableSubscriptionsResponse{}, err
	}

	// Default = min(3, UserCount+AppCount); empty room short-circuits with [].
	mentionableCap := room.UserCount + room.AppCount
	var limit int
	if req.Limit == nil {
		if mentionableCap == 0 {
			return model.MentionableSubscriptionsResponse{Subscriptions: []model.MentionableSubscription{}}, nil
		}
		limit = min(defaultMentionableLimit, mentionableCap)
	} else {
		limit = *req.Limit
		if limit <= 0 || limit > mentionableCap {
			return model.MentionableSubscriptionsResponse{}, errMentionableLimitInvalid
		}
	}

	// Filter is a literal substring. QuoteMeta escapes regex metacharacters
	// so a user typing "a.b" doesn't match every "a<any>b" account.
	escapedFilter := regexp.QuoteMeta(req.Filter)

	subs, err := h.store.ListMentionableSubscriptions(ctx, roomID, requesterAccount, escapedFilter, limit)
	if err != nil {
		return model.MentionableSubscriptionsResponse{}, fmt.Errorf("list mentionable subscriptions: %w", err)
	}
	return model.MentionableSubscriptionsResponse{Subscriptions: subs}, nil
}
```

Register in `RegisterCRUD` just after the Task 7 registration:

```go
	if _, err := nc.QueueSubscribe(subject.MentionableSubscriptionsWildcard(h.siteID), queue, h.natsListMentionableSubscriptions); err != nil {
		return fmt.Errorf("subscribe mentionable subscriptions: %w", err)
	}
```

- [ ] **Step 9.4: Run test, verify it passes**

```
make test SERVICE=room-service 2>&1 | tail -10
```

Expected: PASS — all `TestHandler_ListMentionableSubscriptions` subtests green; no regressions.

- [ ] **Step 9.5: Commit**

```bash
git add room-service/handler.go room-service/handler_test.go
git commit -m "feat(room-service): add subscription.mentionable RPC handler"
```

---

## Task 10: Cross-check Go vs Mongo bot classification

Asserts the Go-side `isBot()` predicate and the Mongo `$regexMatch` against `botAccountRegex` classify the same set of accounts — protects against drift if either source mutates.

**Files:**
- Modify: `room-service/integration_test.go`

- [ ] **Step 10.1: Write the failing test**

Append to `room-service/integration_test.go`:

```go
func TestBotPattern_GoAndMongoAgree_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)

	// Probe set covers .bot suffix, p_ prefix, non-bots, and tricky lookalikes.
	probes := []string{
		"alice",
		"bob.bot",
		"p_assistant",
		"botanist",           // contains "bot" but not at end
		"p",                  // single char, no underscore
		"weird.botanist",     // ends in 'ist', not '.bot'
		"helper.bot.archive", // ".bot" not anchored at end
		"p_",                 // edge: p_ with nothing after
		"P_admin",            // case-sensitive — uppercase P should NOT match
	}

	// Seed users + room + subscriptions so the existing ListMentionableSubscriptions
	// pipeline can classify each account via $regexMatch and tag optionType.
	for _, acct := range probes {
		mustInsertUser(t, db, &model.User{ID: "u-" + acct, Account: acct, EngName: acct})
		mustInsertSub(t, db, &model.Subscription{
			ID:     "sub-" + acct,
			User:   model.SubscriptionUser{ID: "u-" + acct, Account: acct, IsBot: strings.HasSuffix(acct, ".bot")},
			RoomID: "r1", SiteID: "site-a",
		})
	}

	got, err := store.ListMentionableSubscriptions(ctx, "r1", "", "", len(probes)+5)
	require.NoError(t, err)

	// Build the observed Mongo classification: presence in results plus optionType.
	type seen struct {
		present bool
		isApp   bool
	}
	mongo := map[string]seen{}
	for _, s := range got {
		mongo[s.Account] = seen{present: true, isApp: s.OptionType == "app"}
	}

	// Locks Go and Mongo in agreement on bot vs platform-admin vs human:
	//   `.bot` suffix => present + optionType "app"   (Mongo: botAccountRegex)
	//   `p_` prefix   => absent                       (Mongo: $not platformAdminRegex)
	//   otherwise     => present + optionType "user"
	for _, acct := range probes {
		switch {
		case strings.HasSuffix(acct, ".bot"):
			assert.True(t, mongo[acct].present, "%q: bot should appear", acct)
			assert.True(t, mongo[acct].isApp, "%q: bot should be optionType=app", acct)
		case strings.HasPrefix(acct, "p_"):
			assert.False(t, mongo[acct].present, "%q: platform admin must be hidden", acct)
		default:
			assert.True(t, mongo[acct].present, "%q: human should appear", acct)
			assert.False(t, mongo[acct].isApp, "%q: human should be optionType=user", acct)
		}
	}
}
```

- [ ] **Step 10.2: Run test, verify it passes**

```
make test-integration SERVICE=room-service 2>&1 | tail -10
```

Expected: PASS. (No new implementation — this only locks in the Go/Mongo agreement.)

- [ ] **Step 10.3: Commit**

```bash
git add room-service/integration_test.go
git commit -m "test(room-service): assert Go isBot and Mongo regex agree"
```

---

## Task 11: Update `docs/client-api.md`

Per CLAUDE.md's client-facing-handler rule, the docs must ship in the same PR as the handler code.

**Files:**
- Modify: `docs/client-api.md` (insert two new sections after the existing "List Members" section, around line 624)

- [ ] **Step 11.1: Insert "Get Member Statuses" section**

In `docs/client-api.md`, immediately after the "List Members" section's trailing `---` separator (around line 624, just before `#### Mark Messages Read`), insert:

```markdown
#### Get Member Statuses

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.member.statuses`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

##### Request body

| Field   | Type   | Required | Notes |
|---------|--------|----------|-------|
| `limit` | number | no       | Defaults to `3`. Must be `> 0` and `<= room.userCount`. |

```json
{ "limit": 5 }
```

##### Success response

| Field     | Type                | Notes |
|-----------|---------------------|-------|
| `members` | array<MemberStatus> | One entry per room subscription, projected from the joined `users` document. |

`MemberStatus`:

| Field          | Type    | Notes |
|----------------|---------|-------|
| `account`      | string  | The user's account. |
| `engName`      | string  | English display name. |
| `chineseName`  | string  | Chinese display name. |
| `statusIsShow` | boolean | Whether the user has chosen to surface their status text. |
| `statusText`   | string  | Free-form presence text (e.g. `"available"`, `"in a meeting"`). Empty for users who have never set a status. |

```json
{
  "members": [
    {
      "account": "alice",
      "engName": "Alice Wang",
      "chineseName": "愛麗絲",
      "statusIsShow": true,
      "statusText": "available"
    }
  ]
}
```

##### Error response

See [Error envelope](#6-error-envelope-reference). Common errors:

- `"not a room member"` — caller has no subscription in the room (sentinel reused across membership-gated RPCs).
- `"limit must be > 0 and <= room user count"` — limit was `0`, negative, or larger than the room's current `userCount`.

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Get Mentionable Subscriptions

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.subscription.mentionable`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`**.

Used by the message composer's `@…` mention autocomplete. Returns subscriptions discriminated as `user` or `app`. The caller is always excluded from the result set.

##### Request body

| Field    | Type   | Required | Notes |
|----------|--------|----------|-------|
| `limit`  | number | no       | Defaults to `3` (effectively `min(3, room.userCount + room.appCount)`). Must be `> 0` and `<= room.userCount + room.appCount`. |
| `filter` | string | no       | Defaults to `""` (matches everything). Treated as a literal substring; regex metacharacters are escaped server-side. Matched case-insensitively against a dash-joined keyword built from `account`, `engName`, `chineseName`, `app.name`, and `app.assistant.name`. |

```json
{ "limit": 10, "filter": "ali" }
```

##### Success response

| Field           | Type                            | Notes |
|-----------------|---------------------------------|-------|
| `subscriptions` | array<MentionableSubscription>  | At most `limit` rows in arbitrary order. |

`MentionableSubscription` (discriminated by `optionType`):

| Field        | Type    | Notes |
|--------------|---------|-------|
| `optionType` | string  | `"user"` or `"app"`. |
| `userId`     | string  | The subscription's `u._id` (the user's `_id`). |
| `account`    | string  | The user/bot account. |
| `siteId`     | string  | User's home site for `"user"` rows; **empty string** for `"app"` rows. |
| `hrInfo`     | object  | Present **only for `"user"` rows**. `{ engName, chineseName }`. |
| `app`        | object  | Present **only for `"app"` rows**. `{ name, assistant: { name } }`. |

```json
{
  "subscriptions": [
    {
      "optionType": "user",
      "userId": "u-alice",
      "account": "alice",
      "siteId": "site-a",
      "hrInfo": { "engName": "Alice Wang", "chineseName": "愛麗絲" }
    },
    {
      "optionType": "app",
      "userId": "u-helper",
      "account": "helper.bot",
      "siteId": "",
      "app": { "name": "Helper", "assistant": { "name": "helper.bot" } }
    }
  ]
}
```

##### Error response

See [Error envelope](#6-error-envelope-reference). Common errors:

- `"not a room member"` — caller has no subscription in the room.
- `"limit must be > 0 and <= room user count + app count"` — limit was `0`, negative, or larger than the room's combined user + app population.

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---
```

- [ ] **Step 11.2: Verify the docs render**

```
grep -n "Get Member Statuses\|Get Mentionable Subscriptions" docs/client-api.md
```

Expected: two matches showing the new section headers were inserted correctly.

- [ ] **Step 11.3: Sanity-check the full test suite still passes**

```
make lint && make test SERVICE=room-service && make test SERVICE=pkg/model && make test SERVICE=pkg/subject
```

Expected: all green. No regressions.

- [ ] **Step 11.4: Commit**

```bash
git add docs/client-api.md
git commit -m "docs(client-api): document member.statuses + subscription.mentionable RPCs"
```

---

## Self-review checklist (reviewer's eyes only — already done by author)

**Spec coverage:**
- §2 (User fields) → Task 1
- §4.2 Feature 1 types → Task 2; Feature 2 types → Task 3
- §5 subject builders → Task 4
- §3.3 bot regex constant → Task 5
- §8 sentinels + sanitizeError → Task 5
- §6.2 ListMemberStatuses store + pipeline → Task 6
- §6.1 handler + registration → Task 7
- §7.2 ListMentionableSubscriptions store + pipeline → Task 8
- §7.1 handler + registration → Task 9
- §9.3 Go/Mongo regex agreement test → Task 10
- §10 docs → Task 11

**Type / signature consistency:**
- `ListMemberStatuses(ctx, roomID, limit int)` — same signature in store.go, store_mongo.go, and handler (all within Tasks 6–7).
- `ListMentionableSubscriptions(ctx, roomID, excludeAccount, escapedFilter string, limit int)` — same signature in store.go, store_mongo.go, and handler (all within Tasks 8–9).
- `MemberStatus` / `MentionableSubscription` struct field names match the Mongo projection in store_mongo.go and the docs-table column names.
- `errMemberStatusesLimitInvalid` / `errMentionableLimitInvalid` strings match the docs error-envelope examples in Task 11.
- Default-limit numbers (3 / 3) match between handler code, tests, docs, and the spec.

**Placeholder scan:** No "TBD", "TODO", or "similar to" placeholders found.

**Commit-state invariant:** Tasks 6 and 8 bundle the interface change, mock regen, Mongo impl, and integration test into one commit so the working tree never lands in an "interface declared but unimplemented" state that would trip the repo's pre-commit hook.
