# Unread Count Projection Narrowing — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop shipping a 41-field `model.EnrichedSubscription` per row from `GetActiveSubscriptions` when the unread badge reads five fields.

**Architecture:** Add a terminal `$project` to the badge path's existing aggregation and decode into a new narrow row type. The pipeline's stages, filters, cap-before-join ordering and soft-delete drop are all unchanged — this removes fields from the *result*, not work from the *query*. A reflection-based unit test pins the projection to the row type's bson tags so the two cannot drift apart silently.

**Tech Stack:** Go 1.25, MongoDB (`go.mongodb.org/mongo-driver/v2`), `go.uber.org/mock` (mockgen), `stretchr/testify`, `testcontainers-go` via `pkg/testutil`.

**Spec:** `docs/superpowers/specs/2026-08-23-unread-count-projection-narrowing-design.md`

## Global Constraints

- Go 1.25. Single `go.mod` at repo root.
- **Always use `make` targets — never run raw `go` commands.** `make lint`, `make test SERVICE=user-service`, `make test-integration SERVICE=user-service`, `make generate SERVICE=user-service`, `make sast`.
- **TDD is mandatory (Red-Green-Refactor), no exceptions.** Never write implementation before its test exists. Never skip the Red phase — if a test passes before implementation, the test is wrong.
- All tests run with `-race` (the Makefile handles this).
- Minimum 80% coverage per package; 90%+ for handlers, stores and `pkg/`.
- Generated mocks (`user-service/service/mocks/mock_repository.go`) are **never** edited by hand — regenerate with `make generate SERVICE=user-service`.
- Model structs carry both `json` and `bson` tags, `camelCase`, except `bson:"_id"`.
- Errors always wrapped with context: `fmt.Errorf("short description: %w", err)`. Never a bare `err`.
- MongoDB reads must project precisely — never fetch whole documents when a subset suffices.
- Integration tests: `//go:build integration`, same package, containers from `pkg/testutil`. `user-service/mongorepo/main_test.go` already provides `TestMain`; do not add another.
- A pre-commit hook runs lint and tests. Fix failures before retrying — never bypass it.
- Branch: `claude/unread-path-optimization-sggzrk`, based on `main`. Never commit to `main`.
- **No `docs/client-api.md` change:** this touches no client-facing handler, request/response struct or server→client event struct.

---

## File Structure

| File | Responsibility | Action |
|---|---|---|
| `user-service/models/subscription.go` | Service-owned types; gains the badge row type | Modify |
| `user-service/mongorepo/subscriptions.go` | The aggregation: new collection handle, projection helper, changed return type | Modify |
| `user-service/mongorepo/subscriptions_activeproj_test.go` | Unit: projection/row-type drift guard, row-size and no-key-material guards | Create |
| `user-service/service/service.go` | Consumer-defined `SubscriptionRepository` interface | Modify |
| `user-service/service/mocks/mock_repository.go` | Generated mocks | Regenerate |
| `user-service/service/subscriptions.go` | `unreadRooms` — the only consumer of the result | Modify |
| `user-service/service/subscriptions_test.go` | 22 mocked `GetActiveSubscriptions` returns | Modify |
| `user-service/service/badge_test.go` | 7 mocked `GetActiveSubscriptions` returns | Modify |
| `user-service/service/service_test.go` | 1 mocked `GetActiveSubscriptions` return | Modify |
| `user-service/mongorepo/subscriptions_test.go` | Integration: 9 `GetActiveSubscriptions` call sites, 3 of which key on `sub.ID` | Modify |

**Why `user-service/models/` and not `pkg/model/`:** `mongorepo` already imports `user-service/models` (`apps.go:13`, `users.go:15`), so the direction is established. The interface lives in `service/`, which cannot reference a `mongorepo` type without inverting the dependency, so the type must live in a package both can import.

---

### Task 1: The row type, the projection, and the drift guard

Adds the type and the projection helper with their unit tests. Changes no production behavior — the helper is not wired in until Task 2, so this task compiles, tests green, and is independently reviewable.

**Files:**
- Modify: `user-service/models/subscription.go` (append at end of file)
- Modify: `user-service/mongorepo/subscriptions.go` (append helper near `activeSubscriptionFilter`, around `:428`)
- Create: `user-service/mongorepo/subscriptions_activeproj_test.go`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `models.ActiveSubscription` — struct with exported fields `RoomID string`, `SiteID string`, `LastSeenAt *time.Time`, `ThreadUnread []string`, `LastMsgAt *time.Time`.
  - `mongorepo.activeSubscriptionProjection() bson.M` — unexported, returns the terminal `$project` document.

- [ ] **Step 1: Write the failing unit test**

Create `user-service/mongorepo/subscriptions_activeproj_test.go`:

```go
package mongorepo

import (
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/user-service/models"
)

// bsonTagKeys returns the stored bson field names of a flat struct, sorted.
// Tag options like ",omitempty" are stripped so a differently written tag still
// matches, and bson:"-" fields are skipped as never stored.
func bsonTagKeys(t *testing.T, typ reflect.Type) []string {
	t.Helper()
	keys := []string{}
	for i := 0; i < typ.NumField(); i++ {
		tag := typ.Field(i).Tag.Get("bson")
		if tag == "" || tag == "-" {
			continue
		}
		keys = append(keys, strings.Split(tag, ",")[0])
	}
	sort.Strings(keys)
	return keys
}

// The badge path decodes into models.ActiveSubscription, so the terminal
// $project must name exactly that struct's fields. Adding a field to the struct
// without projecting it would decode as a zero value — a wrong badge with no
// error anywhere — so the two are pinned to each other here.
func TestActiveSubscriptionProjection_MatchesRowType(t *testing.T) {
	proj := activeSubscriptionProjection()

	idVal, hasID := proj["_id"]
	require.True(t, hasID, "_id must be named explicitly (Mongo returns it by default)")
	assert.Equal(t, 0, idVal, "_id must be excluded, not included")

	got := []string{}
	for k, v := range proj {
		if k == "_id" {
			continue
		}
		assert.Equal(t, 1, v, "projection %q must be an inclusion", k)
		got = append(got, k)
	}
	sort.Strings(got)

	assert.Equal(t, bsonTagKeys(t, reflect.TypeOf(models.ActiveSubscription{})), got,
		"activeSubscriptionProjection and models.ActiveSubscription must name the same fields")
}

// The point of the narrow row is what it does NOT carry: the room's E2E key
// slot, its counts, and the rest of the subscription document.
func TestActiveSubscriptionRow_IsLeanAndCarriesNoKeyMaterial(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)

	lean, err := bson.Marshal(models.ActiveSubscription{
		RoomID:       "01970a4f8c2d7c9aabcde01234567890",
		SiteID:       "site-a",
		LastSeenAt:   &now,
		ThreadUnread: []string{"01970a4f8c2d7c9aabcde01234567891"},
		LastMsgAt:    &now,
	})
	require.NoError(t, err)

	fat, err := bson.Marshal(model.EnrichedSubscription{
		Subscription: model.Subscription{
			ID:       "01970a4f8c2d7c9aabcde01234567892",
			User:     model.SubscriptionUser{ID: "01970a4f8c2d7c9aabcde01234567893", Account: "alice"},
			RoomID:   "01970a4f8c2d7c9aabcde01234567890",
			SiteID:   "site-a",
			Roles:    []model.Role{model.RoleMember},
			Name:     "engineering-general",
			RoomType: model.RoomTypeChannel,
			JoinedAt: now, LastSeenAt: &now,
			ThreadUnread:      []string{"01970a4f8c2d7c9aabcde01234567891"},
			Alert:             true,
			Open:              true,
			FavoriteUpdatedAt: &now, MuteUpdatedAt: &now, RolesUpdatedAt: &now,
			NameUpdatedAt: &now, RestrictUpdatedAt: &now, SectionUpdatedAt: &now,
		},
		UserCount: 42, LastMsgAt: &now, LastMsgID: "01970a4f8c2d7c9aabcde0123456789a",
		LastMentionAllAt: &now, MinUserLastSeenAt: &now, AppCount: 2,
		RoomName:    "engineering-general",
		RoomKeyPriv: make([]byte, 120),
		RoomKeyVer:  3,
	})
	require.NoError(t, err)

	// The number goes in the PR description; the assertion just guards the direction.
	t.Logf("ActiveSubscription=%dB EnrichedSubscription=%dB ratio=%.1fx",
		len(lean), len(fat), float64(len(fat))/float64(len(lean)))
	assert.Less(t, len(lean), len(fat), "the badge row must be smaller than the enriched row")
	assert.NotContains(t, string(lean), "encKey", "no room key material may reach the badge path")
}

// The row type's bson tags must match what the subscriptions collection actually
// stores. The projection guard above only proves the projection and the struct
// agree with each other — decoding a marshalled Subscription proves both name
// real stored fields.
func TestActiveSubscription_DecodesFromFullSubscriptionDocument(t *testing.T) {
	seen := time.Date(2026, 8, 23, 9, 0, 0, 0, time.UTC)
	sub := model.Subscription{
		ID:           "01970a4f8c2d7c9aabcde01234567892",
		User:         model.SubscriptionUser{Account: "alice"},
		RoomID:       "01970a4f8c2d7c9aabcde01234567890",
		SiteID:       "site-a",
		RoomType:     model.RoomTypeChannel,
		LastSeenAt:   &seen,
		ThreadUnread: []string{"pm-1"},
	}
	b, err := bson.Marshal(&sub)
	require.NoError(t, err)

	var row models.ActiveSubscription
	require.NoError(t, bson.Unmarshal(b, &row))
	assert.Equal(t, sub.RoomID, row.RoomID)
	assert.Equal(t, sub.SiteID, row.SiteID)
	require.NotNil(t, row.LastSeenAt)
	assert.Equal(t, seen.UTC(), row.LastSeenAt.UTC())
	assert.Equal(t, sub.ThreadUnread, row.ThreadUnread)
	assert.Nil(t, row.LastMsgAt, "lastMsgAt is added by the rooms join, never stored on the subscription")
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=user-service`

Expected: FAIL to compile — `undefined: activeSubscriptionProjection` and `undefined: models.ActiveSubscription` (all three tests in the file). A compile failure is a valid Red phase in Go; do not proceed until you have seen it.

- [ ] **Step 3: Add the row type**

Append to `user-service/models/subscription.go` (the file already has `import "github.com/hmchangw/chat/pkg/model"`; add `"time"` to the import block, making it a parenthesized group):

```go
// ActiveSubscription is the badge path's row: the five fields unreadRooms reads
// off each active subscription. GetActiveSubscriptions projects to exactly these,
// so a field absent from this struct is a field the query does not fetch — the
// narrow type is the contract, not a convenience. json tags are present per the
// repo's struct-tag rule; nothing serializes this type to a client.
type ActiveSubscription struct {
	RoomID       string     `json:"roomId"                 bson:"roomId"`
	SiteID       string     `json:"siteId"                 bson:"siteId"`
	LastSeenAt   *time.Time `json:"lastSeenAt,omitempty"   bson:"lastSeenAt,omitempty"`
	ThreadUnread []string   `json:"threadUnread,omitempty" bson:"threadUnread,omitempty"`
	// LastMsgAt is the joined room's activity timestamp, added by roomsEnrichStages.
	// Nil for a cross-site sub (no local room document) and for a room with no
	// messages — unread() treats both as "not unread".
	LastMsgAt *time.Time `json:"lastMsgAt,omitempty" bson:"lastMsgAt,omitempty"`
}
```

- [ ] **Step 4: Add the projection helper**

In `user-service/mongorepo/subscriptions.go`, directly after `activeSubscriptionFilter` (ends around `:443`):

```go
// activeSubscriptionProjection is the terminal $project for the badge path: the
// exact fields models.ActiveSubscription decodes, and nothing else. Everything
// roomsEnrichStages adds beyond lastMsgAt — the room's counts, its E2E key slot,
// the sort key — is dropped here instead of being shipped and discarded.
// TestActiveSubscriptionProjection_MatchesRowType pins it to the struct.
func activeSubscriptionProjection() bson.M {
	return bson.M{
		"_id":          0,
		"roomId":       1,
		"siteId":       1,
		"lastSeenAt":   1,
		"threadUnread": 1,
		"lastMsgAt":    1,
	}
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `make test SERVICE=user-service`
Expected: PASS. Note the byte numbers printed by `TestActiveSubscriptionRow_IsLeanAndCarriesNoKeyMaterial` — they are needed in Task 3.

- [ ] **Step 6: Lint and commit**

```bash
make lint
git add user-service/models/subscription.go user-service/mongorepo/subscriptions.go user-service/mongorepo/subscriptions_activeproj_test.go
git commit -m "feat(user-service): narrow row type and projection for the badge path

models.ActiveSubscription names the five fields unreadRooms reads;
activeSubscriptionProjection is pinned to it by a reflection test so a
field added to one without the other fails the build rather than
decoding as a silent zero value."
```

---

### Task 2: Wire the projection into `GetActiveSubscriptions`

The type change crosses the repo, the interface, the mocks and the service at once — `setup_test.go` asserts `_ service.SubscriptionRepository = (*SubscriptionRepo)(nil)` at compile time, so a partial switch does not build. This task is therefore atomic by necessity, and is the one a reviewer accepts or rejects as a whole.

**Files:**
- Modify: `user-service/mongorepo/subscriptions.go:22-56` (struct + constructor), `:467-479` (`GetActiveSubscriptions`)
- Modify: `user-service/service/service.go:26-30` (interface entry)
- Regenerate: `user-service/service/mocks/mock_repository.go`
- Modify: `user-service/service/subscriptions.go:586-673` (`unreadRooms`)
- Modify: `user-service/mongorepo/subscriptions_test.go` (integration, 9 call sites)
- Modify: `user-service/service/subscriptions_test.go`, `user-service/service/badge_test.go`, `user-service/service/service_test.go`

**Interfaces:**
- Consumes: `models.ActiveSubscription`, `activeSubscriptionProjection()` from Task 1.
- Produces: `SubscriptionRepo.GetActiveSubscriptions(ctx context.Context, account string, limit int) ([]models.ActiveSubscription, error)` — same name and parameters, new element type.

- [ ] **Step 1: Write the failing integration test**

Add to `user-service/mongorepo/subscriptions_test.go` (already `//go:build integration`, package `mongorepo`, and already importing `time`, `bson`, `assert` and `require`). Add one import to that file:

```go
	"github.com/hmchangw/chat/user-service/models"
```


```go
// The badge path must come back with exactly the five fields it reads, correctly
// populated for a local room, a room with no messages, and a cross-site sub with
// no local room document at all.
func TestGetActiveSubscriptions_ProjectsBadgeFields_Integration(t *testing.T) {
	r, db := newTestSubscriptionRepo(t)
	ctx := context.Background()
	lastMsg := time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC)
	seen := time.Date(2026, 8, 23, 9, 0, 0, 0, time.UTC)

	seed(t, db, "rooms",
		bson.M{"_id": "p-room", "name": "Eng", "siteId": "site-a", "lastMsgAt": lastMsg,
			"userCount": 9, "appCount": 1, "encKey": bson.M{"priv": make([]byte, 120), "ver": 3}},
		bson.M{"_id": "p-quiet", "name": "Quiet", "siteId": "site-a"},
	)
	seed(t, db, "subscriptions",
		bson.M{"_id": "p-sub", "u": bson.M{"_id": "u-alice", "account": "alice"}, "name": "Eng",
			"roomId": "p-room", "roomType": "channel", "siteId": "site-a",
			"lastSeenAt": seen, "threadUnread": []string{"pm-1", "pm-2"}},
		bson.M{"_id": "p-sub-quiet", "u": bson.M{"_id": "u-alice", "account": "alice"}, "name": "Quiet",
			"roomId": "p-quiet", "roomType": "channel", "siteId": "site-a"},
		bson.M{"_id": "p-sub-remote", "u": bson.M{"_id": "u-alice", "account": "alice"}, "name": "Remote",
			"roomId": "p-remote", "roomType": "channel", "siteId": "site-b", "lastSeenAt": seen},
	)

	subs, err := r.GetActiveSubscriptions(ctx, "alice", 100)
	require.NoError(t, err)

	byRoom := map[string]models.ActiveSubscription{}
	for _, s := range subs {
		byRoom[s.RoomID] = s
	}
	require.Len(t, byRoom, 3)

	local := byRoom["p-room"]
	assert.Equal(t, "site-a", local.SiteID)
	require.NotNil(t, local.LastSeenAt)
	assert.Equal(t, seen.UTC(), local.LastSeenAt.UTC())
	require.NotNil(t, local.LastMsgAt, "the joined room's lastMsgAt must survive the projection")
	assert.Equal(t, lastMsg.UTC(), local.LastMsgAt.UTC())
	assert.Equal(t, []string{"pm-1", "pm-2"}, local.ThreadUnread)

	quiet := byRoom["p-quiet"]
	assert.Nil(t, quiet.LastMsgAt, "a room with no messages has no lastMsgAt")
	assert.Nil(t, quiet.LastSeenAt, "an unread-from-birth sub has no lastSeenAt")

	remote := byRoom["p-remote"]
	assert.Equal(t, "site-b", remote.SiteID)
	assert.Nil(t, remote.LastMsgAt, "a cross-site sub has no local room document")
	require.NotNil(t, remote.LastSeenAt)
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `make test-integration SERVICE=user-service`

Expected: FAIL to compile — `GetActiveSubscriptions` still returns `[]model.EnrichedSubscription`, which has no such literal shape, and `models` is not imported in the test file. Confirm the failure before implementing.

- [ ] **Step 3: Add the typed collection handle**

In `user-service/mongorepo/subscriptions.go`, add the field to `SubscriptionRepo` (after `enrichedSecondary`, `:33`):

```go
	// activeSecondary decodes the badge path's narrow projection over the same
	// subscriptions collection; routed to a secondary like the other read handles.
	activeSecondary *mongoutil.Collection[models.ActiveSubscription]
```

In `NewSubscriptionRepo`, after `enriched := mongoutil.NewCollection[model.EnrichedSubscription](col)`:

```go
	active := mongoutil.NewCollection[models.ActiveSubscription](col)
```

and in the returned struct literal, after `enrichedSecondary: enriched.WithReadPreference(s.readPref),`:

```go
		activeSecondary:        active.WithReadPreference(s.readPref),
```

Add `"github.com/hmchangw/chat/user-service/models"` to the import block.

- [ ] **Step 4: Switch the query**

Replace `GetActiveSubscriptions` (`:467-479`) entirely with:

```go
// GetActiveSubscriptions returns the active set used by the unread count,
// capped by limit. The cap runs before the rooms join so $lookup touches
// ≤limit rows; the deleted-room filter runs after it, so a capped page can
// come back slightly short — tolerable for the unread count, its only consumer.
// The terminal projection returns only the fields unreadRooms reads: the room
// baseline beyond lastMsgAt (counts, key slot, sort key) is dropped here rather
// than decoded and discarded.
func (r *SubscriptionRepo) GetActiveSubscriptions(ctx context.Context, account string, limit int) ([]models.ActiveSubscription, error) {
	pipeline := bson.A{bson.M{"$match": activeSubscriptionFilter(account)}}
	pipeline = append(pipeline, r.originFilterStage(account)...)
	// MongoDB rejects $limit:0 — treat it as "no cap".
	if limit > 0 {
		pipeline = append(pipeline, bson.M{"$limit": int64(limit)})
	}
	pipeline = append(pipeline, roomsEnrichStages(true)...)
	pipeline = append(pipeline, bson.M{"$project": activeSubscriptionProjection()})
	return r.activeSecondary.Aggregate(ctx, pipeline)
}
```

The `$project` goes **after** `roomsEnrichStages`: the `^Del-` `$match` inside those stages consumes `room.name`, and `originFilterStage` has already matched on `origin`, so neither filter can be affected by dropping those fields from the output.

- [ ] **Step 5: Update the interface**

In `user-service/service/service.go`, replace the `GetActiveSubscriptions` entry and its comment (`:26-30`) with:

```go
	// GetActiveSubscriptions returns up to limit active subscriptions, projected
	// to the five fields the unread count reads. The cap is applied before the
	// deleted-room filter, so a page whose capped slice contains soft-deleted
	// rooms comes back slightly short of limit — fine for the unread count (its
	// only consumer), not a general pagination surface.
	GetActiveSubscriptions(ctx context.Context, account string, limit int) ([]models.ActiveSubscription, error)
```

`models` is already imported in this file.

- [ ] **Step 6: Regenerate the mocks**

Run: `make generate SERVICE=user-service`

Never hand-edit `user-service/service/mocks/mock_repository.go`. Confirm the diff shows only the `GetActiveSubscriptions` return type changing.

- [ ] **Step 7: Migrate `unreadRooms`**

In `user-service/service/subscriptions.go`, inside `unreadRooms` (`:586`), change the one declaration that names the old type:

```go
	crossBySite := map[string][]model.EnrichedSubscription{}
```

to:

```go
	crossBySite := map[string][]models.ActiveSubscription{}
```

Every field access in the function (`subs[i].SiteID`, `.LastSeenAt`, `.LastMsgAt`, `.ThreadUnread`, `.RoomID`, and the same five on `siteSubs[j]`) is unchanged — the new type carries all of them with identical names and types. Verify `models` is imported in this file; add it if not.

- [ ] **Step 8: Migrate the service unit tests**

In `user-service/service/subscriptions_test.go` (22 sites), `user-service/service/badge_test.go` (7 sites) and `user-service/service/service_test.go` (1 site), rewrite each mocked return. The embedded-struct literal flattens:

```go
// before
Return([]model.EnrichedSubscription{
    {Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, LastMsgAt: &newer},
}, nil)

// after
Return([]models.ActiveSubscription{
    {RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen, LastMsgAt: &newer},
}, nil)
```

Empty and nil returns change type only: `[]model.EnrichedSubscription{}` → `[]models.ActiveSubscription{}`; `Return(nil, errors.New("db down"))` is untouched. Do not change any assertion or expected count — this migration must not alter a single test's meaning. If a test stops passing, the production change is wrong, not the test.

- [ ] **Step 9: Migrate the integration tests that key on `sub.ID`**

The projection drops `_id`, so three sites in `user-service/mongorepo/subscriptions_test.go` must key on `RoomID` instead. Use this fixture mapping:

| subscription `_id` | `roomId` |
|---|---|
| `a-dm` | `r-dm` |
| `a-ch` | `r-ch` |
| `a-bot` | `r-bot` |
| `x-ch` | `rx` |
| `gone-ch` | `r-missing` |
| `m-ch` | `r-noisy` |
| `u-bot` | `r-offbot` |
| `mu-bot` | `r-mutedbot` |
| `del-ch` | `r-del` |
| `closed-ch` | `r-closed` |
| `open-ch` | `r-open` |
| `sub-xsite-teams` | `r-xsite-teams` |
| `sub-xsite-native` | `r-xsite-native` |

**Site A** — `~:379`, the Teams-origin test: `unreadIDs[s.ID] = true` becomes `unreadIDs[s.RoomID] = true`, and the assertion becomes:

```go
	assert.False(t, unreadIDs["r-xsite-teams"], "unread set must exclude the cross-site Teams room")
```

**Site B** — `~:716`, the "closed rooms" subtest: `got[sub.ID] = true` becomes `got[sub.RoomID] = true`, then:

```go
		assert.False(t, got["r-closed"], "open:false must not count")
		assert.True(t, got["r-open"], "open:true must count")
		assert.True(t, got["r-ch"], "a sub with no open field must count")
```

**Site C** — `~:731`, the "get active returns the same set" subtest: `got[sub.ID] = true` becomes `got[sub.RoomID] = true`, then:

```go
		assert.True(t, got["r-dm"])
		assert.True(t, got["r-ch"])
		assert.True(t, got["r-bot"])
		assert.True(t, got["rx"], "cross-site sub kept despite no local room")
		assert.True(t, got["r-missing"], "missing local room now kept (empty enrichment) — siteID filter removed, deleted-filter is room.name-based")
		assert.False(t, got["r-noisy"], "muted channel excluded from the active/count set")
		assert.False(t, got["r-offbot"])
		assert.False(t, got["r-mutedbot"], "muted botDM excluded by activeSubscriptionFilter before room lookup")
		assert.False(t, got["r-del"], "local sub to a ^Del- room must be filtered out")
```

The remaining call sites (`~:748` limit cap, `~:756` zero limit, `~:771` empty set) assert only on `len(subs)` / emptiness and need no change.

- [ ] **Step 10: Run the unit tests**

Run: `make test SERVICE=user-service`
Expected: PASS, with no assertion or expected-count changes from Step 8.

- [ ] **Step 11: Run the integration tests**

Run: `make test-integration SERVICE=user-service`
Expected: PASS, including the new `TestGetActiveSubscriptions_ProjectsBadgeFields_Integration` from Step 1.

Requires a running Docker daemon. If none is available in your environment, say so explicitly in the handoff rather than reporting the suite as passing — CI's `test-integration (user-service)` job is then the gate.

- [ ] **Step 12: Lint and commit**

```bash
make lint
git add user-service/mongorepo/subscriptions.go user-service/service/service.go user-service/service/mocks/mock_repository.go user-service/service/subscriptions.go user-service/service/subscriptions_test.go user-service/service/badge_test.go user-service/service/service_test.go user-service/mongorepo/subscriptions_test.go
git commit -m "perf(user-service): project the badge path to the fields it reads

GetActiveSubscriptions returned a full EnrichedSubscription per row — the
whole subscription document plus eleven joined room fields including the
room's E2E key slot — for the five fields unreadRooms reads. A terminal
\$project and models.ActiveSubscription cut that to the read set.

The pipeline is otherwise unchanged: same filters, same cap-before-join
ordering, same soft-delete drop, so the short-page contract holds. The
projection sits after the enrich stages, whose \$match consumes room.name
before it is dropped."
```

---

### Task 3: Verification sweep and measurement

Produces the numbers the PR description needs and confirms the repo-wide gates.

**Files:**
- Modify: `docs/superpowers/specs/2026-08-23-unread-count-projection-narrowing-design.md` (§8, record the measured sizes)

**Interfaces:**
- Consumes: the byte figures logged by `TestActiveSubscriptionRow_IsLeanAndCarriesNoKeyMaterial` (Task 1, Step 5).
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Capture the measurement**

Run: `make test SERVICE=user-service` and read the `t.Logf` line from `TestActiveSubscriptionRow_IsLeanAndCarriesNoKeyMaterial`.

Record the two byte counts and the ratio. The `test` target (`Makefile:106-111`) is a
bare `go test -race ./$(SERVICE)/...` with no verbose flag and no way to select a
single test — there is no "Makefile's verbose path" to fall back to, and this plan's
"never run raw `go` commands" rule rules out routing around `make test` with a
hand-invoked `go test -v`. If `make test`'s output does not surface the `t.Logf`
line for a passing test, take the figures from the implementer's own report of
having seen them during Task 1 instead of trying to obtain them a second way here.

- [ ] **Step 2: Record it in the spec**

In `docs/superpowers/specs/2026-08-23-unread-count-projection-narrowing-design.md`, §8 currently says the row's exact size is a deliverable. Replace that sentence with the measured figures, in the form:

```markdown
Measured: `models.ActiveSubscription` <N> B against `model.EnrichedSubscription`
<M> B on representative rows, <R>×. PR #197's 759 B figure for the enriched row
is the same measurement on its own fixture.
```

- [ ] **Step 3: Run the full repo test suite**

Run: `make test`
Expected: PASS. `unreadRooms`'s type change is contained to user-service, but the whole suite is the gate for a store-interface change.

- [ ] **Step 4: Run SAST**

Run: `make sast`
Expected: no medium-or-above findings. `gosec`, `govulncheck` and `semgrep` are a blocking CI gate. If `govulncheck` or the semgrep registry cannot reach their hosts from your environment, say so explicitly and rely on CI's `sast` job — do not report a scan you did not run.

- [ ] **Step 5: Commit and push**

```bash
git add docs/superpowers/specs/2026-08-23-unread-count-projection-narrowing-design.md
git commit -m "docs(user-service): record the measured badge-row sizes"
git push -u origin claude/unread-path-optimization-sggzrk
```

Do **not** open a pull request unless asked.

---

## Notes for the reviewer of this plan

- **No behavior change is intended anywhere.** Every filter, the cap-before-join order, the short-page contract, the per-site degradation semantics in `unreadRooms` and the `degraded` caching rule are untouched. If a pre-existing test needs its expectations changed, that is a signal the implementation is wrong.
- **What this does not do:** the `$lookup` still fetches eleven room fields per row inside the pipeline, `encKey.priv` among them. The projection stops them crossing the wire and being decoded; it does not stop the read, and it saves no index seeks. Removing the read needs a caller-specific `$lookup` — deliberately out of scope (spec §11).
- **Task 2 is atomic on purpose.** `setup_test.go`'s compile-time interface assertion means the repo, interface, mocks and service cannot change in separate commits.
