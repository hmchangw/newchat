# Notification Settings Enforcement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `notification-worker` honour three stored user settings — `muteAllNotifications`, `alwaysAllowPriorityNotifications`, `showNotificationsInCall` — when deciding whether to emit a push.

**Architecture:** A new `UserSettingsSnapshotter` batches indexed Mongo `$in` reads against the `users` collection — one `Find` per chunk of `USER_SETTINGS_BATCH_SIZE` (512) accounts, so `ceil(candidates/512)` queries per message: one for almost every room, more only where a large room's candidate set survives the `EligibleForPush` throttle (an `@all` in a several-thousand-member room). It is placed in `handler.go` after the candidate loop (so it reads only push-eligible accounts) and before the survivor loop, where `shouldPush` combines it with the presence snapshot (so `showNotificationsInCall` can modify the presence decision) — its order relative to `Presence.Snapshot` itself is unconstrained, since the two calls share no state and aren't combined until the survivor loop reads both maps. The lookup resolves the stored `*bool` fields into a pointer-free `notifSettings` whose zero value is exactly today's behaviour, which is what makes the fail-open contract free of special cases. `shouldPush` grows two parameters and consumes it.

**Tech Stack:** Go 1.25, MongoDB (`go.mongodb.org/mongo-driver/v2`), testify, testcontainers via `pkg/testutil`, `caarlos0/env`.

**Spec:** `docs/superpowers/specs/2026-08-10-notification-settings-enforcement-design.md`

## Global Constraints

- **TDD is mandatory** (CLAUDE.md §4): write the test, run it, confirm it fails, then implement. Never write implementation before its test.
- **Never run raw `go` commands** — always `make` targets (`make test SERVICE=notification-worker`, `make lint`, `make test-integration SERVICE=notification-worker`). The single exception is the coverage check in Task 4 Step 5, which CLAUDE.md §4 specifies as `go test -coverprofile` + `go tool cover -func` because no `make` target emits a profile.
- **Fail-open everywhere.** Every error path in the snapshotter returns `(partialOrEmptyMap, nil)`. No error ever propagates to the handler, and the handler discards the error with `_` exactly as it already does for presence.
- **The zero `notifSettings` must reproduce today's truth table exactly**: not muted, no pierce, in-call suppressed.
- **No new wire surface.** No new RPC, no `pkg/model` field, no event struct change. Therefore `docs/client-api/request-reply.md` and `docs/client-api/events.md` are NOT touched — the CLAUDE.md same-PR derived-views rule is not triggered.
- **Projection is narrow** (CLAUDE.md §6 "always project precisely"): the four gated fields plus `account`, never `{"settings": 1}`.
- **Active-user filter is `{"active": {"$ne": false}}`** — matches `activeUserFilter` in `user-service/mongorepo/users.go:55`. Missing `active` counts as active.
- **The index this read depends on is owned by another service.** `users.account` is a unique index created by `UserRepo.EnsureIndexes` (`user-service/mongorepo/users.go:33-36`), which fails startup with an operator-facing message if it cannot be created. `notification-worker` must NOT create it — but whoever ships this should confirm it exists in each target environment (`db.users.getIndexes()`) before leaving `USER_SETTINGS_ENABLED=true`, because an unindexed `$in` here would collection-scan `users` once per message.
- **Test files are `package main`** in the service directory; integration tests carry `//go:build integration`.
- Coverage floor 80%; the gate and snapshotter are core logic — target 90%+.
- Error wrapping: `fmt.Errorf("short description: %w", err)` describing what the current function was doing.
- Logging: `log/slog` with key-value fields, never interpolated strings. Never log a full message body.

---

### Task 1: `notifSettings` value type, interface, and noop

**Files:**
- Create: `notification-worker/usersettings.go`
- Create: `notification-worker/usersettings_test.go`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `type UserSettingsSnapshotter interface { Snapshot(ctx context.Context, accounts []string) (map[string]notifSettings, error) }`
  - `type notifSettings struct { muteAll, allowPriority, showInCall bool; priorityContacts map[string]struct{} }`
  - `func (n notifSettings) isPriority(account string) bool`
  - `type noopUserSettings struct{}` implementing `UserSettingsSnapshotter`

- [ ] **Step 1: Write the failing tests**

Create `notification-worker/usersettings_test.go`:

```go
package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNotifSettings_ZeroValueIsPreEnforcementBehaviour(t *testing.T) {
	var ns notifSettings
	assert.False(t, ns.muteAll, "zero value must not mute")
	assert.False(t, ns.allowPriority, "zero value must not pierce")
	assert.False(t, ns.showInCall, "zero value must keep in-call suppressed")
	assert.False(t, ns.isPriority("alice"), "zero value has no priority contacts")
}

func TestNotifSettings_IsPriority(t *testing.T) {
	tests := []struct {
		name     string
		contacts map[string]struct{}
		account  string
		want     bool
	}{
		{"listed user", map[string]struct{}{"alice": {}}, "alice", true},
		{"listed bot", map[string]struct{}{"helper.bot": {}}, "helper.bot", true},
		{"not listed", map[string]struct{}{"alice": {}}, "bob", false},
		{"nil map", nil, "alice", false},
		{"empty map", map[string]struct{}{}, "alice", false},
		{"empty account never matches", map[string]struct{}{"": {}}, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns := notifSettings{priorityContacts: tt.contacts}
			assert.Equal(t, tt.want, ns.isPriority(tt.account))
		})
	}
}

func TestNoopUserSettings_EmptySnapshot(t *testing.T) {
	var s UserSettingsSnapshotter = noopUserSettings{}
	got, err := s.Snapshot(context.Background(), []string{"alice", "bob"})
	require.NoError(t, err)
	assert.Empty(t, got, "kill switch yields the zero notifSettings for every account")
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=notification-worker`
Expected: FAIL — compile error, `undefined: notifSettings`, `undefined: UserSettingsSnapshotter`, `undefined: noopUserSettings`.

- [ ] **Step 3: Write the implementation**

Create `notification-worker/usersettings.go`:

```go
package main

import "context"

// UserSettingsSnapshotter batches notification-settings lookups for push-eligible
// accounts. Errors are swallowed; an absent account defaults to current behaviour.
type UserSettingsSnapshotter interface {
	Snapshot(ctx context.Context, accounts []string) (map[string]notifSettings, error)
}

// notifSettings is the resolved, pointer-free view of the three settings this
// worker gates on. The stored type is all *bool; resolving once at this boundary
// keeps the gate total instead of making every read site re-decide what nil means.
//
// The zero value is exactly pre-enforcement behaviour — not muted, no pierce,
// in-call suppressed. That is what makes fail-open free: a missing user, an unset
// settings sub-document, a Mongo error and the kill switch all converge here.
type notifSettings struct {
	muteAll          bool
	allowPriority    bool
	showInCall       bool
	priorityContacts map[string]struct{}
}

// isPriority reports whether account is one of this recipient's priority contacts.
// Decoded to a set at the snapshot boundary so the gate is O(1) per candidate.
func (n notifSettings) isPriority(account string) bool {
	if account == "" {
		return false
	}
	_, ok := n.priorityContacts[account]
	return ok
}

// noopUserSettings returns an empty map so every recipient takes the zero
// notifSettings. Backs USER_SETTINGS_ENABLED=false, mirroring noopPresenceSnapshotter.
type noopUserSettings struct{}

func (noopUserSettings) Snapshot(context.Context, []string) (map[string]notifSettings, error) {
	return map[string]notifSettings{}, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=notification-worker`
Expected: PASS — all three new tests green, no existing test broken.

- [ ] **Step 5: Lint**

Run: `make lint`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add notification-worker/usersettings.go notification-worker/usersettings_test.go
git commit -m "feat(notification-worker): add notifSettings value type and snapshotter interface"
```

---

### Task 2: `isInCall` predicate and the two-parameter `shouldPush`

**Files:**
- Modify: `notification-worker/presence.go:119-127` (replace `shouldPush`)
- Modify: `notification-worker/handler.go:202` (update the call site to compile)
- Modify: `notification-worker/presence_test.go:27-45` (replace `TestShouldPush`)

**Interfaces:**
- Consumes: `notifSettings` from Task 1.
- Produces:
  - `func isInCall(p model.Presence) bool`
  - `func shouldPush(p model.Presence, ns notifSettings, isPrioritySender bool) bool`

**Note:** this task deliberately keeps behaviour identical. The handler passes a zero `notifSettings` and `false` — Task 4 wires the real values. That keeps this task independently reviewable: the gate learns the new shape, nothing changes for users yet.

- [ ] **Step 1: Write the failing test**

In `notification-worker/presence_test.go`, replace the whole `TestShouldPush` function (currently lines 27-45) with:

```go
func TestShouldPush(t *testing.T) {
	tests := []struct {
		name             string
		status           string
		ns               notifSettings
		isPrioritySender bool
		want             bool
	}{
		// Zero notifSettings must reproduce the pre-enforcement truth table exactly.
		{"zero settings online", "online", notifSettings{}, false, true},
		{"zero settings offline", "offline", notifSettings{}, false, true},
		{"zero settings away", "away", notifSettings{}, false, true},
		{"zero settings busy", "busy", notifSettings{}, false, false},
		{"zero settings in-call", "in-call", notifSettings{}, false, false},
		{"zero settings missing status", "", notifSettings{}, false, true},
		{"zero settings unknown status", "unknown", notifSettings{}, false, true},

		// muteAll suppresses unless a priority sender pierces it.
		{"muted, no pierce", "online", notifSettings{muteAll: true}, false, false},
		{"muted, priority sender but pierce disabled", "online", notifSettings{muteAll: true}, true, false},
		{"muted, pierce enabled but sender not priority", "online", notifSettings{muteAll: true, allowPriority: true}, false, false},
		{"muted, pierce enabled and sender is priority", "online", notifSettings{muteAll: true, allowPriority: true}, true, true},
		{"unmuted, pierce enabled, non-priority sender", "online", notifSettings{allowPriority: true}, false, true},

		// showNotificationsInCall governs both suppressed statuses.
		{"in-call, opted in", "in-call", notifSettings{showInCall: true}, false, true},
		{"busy, opted in", "busy", notifSettings{showInCall: true}, false, true},
		{"in-call, not opted in", "in-call", notifSettings{}, false, false},

		// The pierce does not cross the in-call gate.
		{"muted+pierced but in-call without opt-in", "in-call", notifSettings{muteAll: true, allowPriority: true}, true, false},
		{"muted+pierced and in-call with opt-in", "in-call", notifSettings{muteAll: true, allowPriority: true, showInCall: true}, true, true},

		// Both suppressors clear.
		{"muted+pierced, online", "online", notifSettings{muteAll: true, allowPriority: true, showInCall: true}, true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldPush(model.Presence{AggregatedStatus: tt.status}, tt.ns, tt.isPrioritySender)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsInCall(t *testing.T) {
	tests := []struct {
		status string
		want   bool
	}{
		{"busy", true},
		{"in-call", true},
		{"online", false},
		{"offline", false},
		{"away", false},
		{"", false},
		{"unknown", false},
	}
	for _, tt := range tests {
		t.Run(tt.status, func(t *testing.T) {
			assert.Equal(t, tt.want, isInCall(model.Presence{AggregatedStatus: tt.status}))
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=notification-worker`
Expected: FAIL — compile error, `too many arguments in call to shouldPush` and `undefined: isInCall`.

- [ ] **Step 3: Write the implementation**

In `notification-worker/presence.go`, replace lines 119-127 (the current `shouldPush`) with:

```go
// isInCall reports whether presence alone suppresses push. "busy" and "in-call"
// are deliberately one bucket: showNotificationsInCall is the only user-facing
// control over either, so splitting them would leave "busy" ungovernable.
func isInCall(p model.Presence) bool {
	switch p.AggregatedStatus {
	case "busy", "in-call":
		return true
	default:
		return false
	}
}

// shouldPush applies the two independent suppressors. Mute is pierced by a
// priority sender only when the recipient enabled alwaysAllowPriorityNotifications;
// the pierce deliberately does not cross the in-call gate, which
// showNotificationsInCall governs on its own. Unknown presence fails open.
func shouldPush(p model.Presence, ns notifSettings, isPrioritySender bool) bool {
	if ns.muteAll && !(ns.allowPriority && isPrioritySender) {
		return false
	}
	if isInCall(p) && !ns.showInCall {
		return false
	}
	return true
}
```

In `notification-worker/handler.go`, change line 202 from:

```go
		if !shouldPush(snapshot[c.Account]) {
```

to:

```go
		// Task 4 replaces the zero value with the recipient's real settings.
		if !shouldPush(snapshot[c.Account], notifSettings{}, false) {
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=notification-worker`
Expected: PASS — the new table green and every pre-existing handler test still green (behaviour is unchanged by construction).

- [ ] **Step 5: Lint**

Run: `make lint`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add notification-worker/presence.go notification-worker/presence_test.go notification-worker/handler.go
git commit -m "feat(notification-worker): extract isInCall and widen shouldPush for settings"
```

---

### Task 3: Mongo-backed snapshotter

**Files:**
- Modify: `notification-worker/usersettings.go` (append the Mongo implementation)
- Modify: `notification-worker/usersettings_test.go` (append `resolveNotifSettings` unit tests)
- Modify: `notification-worker/integration_test.go` (append Mongo-backed tests)

**Interfaces:**
- Consumes: `notifSettings`, `UserSettingsSnapshotter` from Task 1; `chunkStrings(in []string, size int) [][]string` from `presence.go:104`.
- Produces:
  - `func newMongoUserSettings(col *mongo.Collection, batchSize int, timeout time.Duration) *mongoUserSettings`
  - `func resolveNotifSettings(s *model.UserSettings) notifSettings`

- [ ] **Step 1: Write the failing unit tests**

Both the unit tests here and the integration tests in Step 2 must exist and be
confirmed red before any implementation is written. Append to
`notification-worker/usersettings_test.go`:

```go
func TestResolveNotifSettings(t *testing.T) {
	boolPtr := func(b bool) *bool { return &b }

	t.Run("nil settings yields zero value", func(t *testing.T) {
		assert.Equal(t, notifSettings{}, resolveNotifSettings(nil))
	})

	t.Run("empty settings yields zero value", func(t *testing.T) {
		assert.Equal(t, notifSettings{}, resolveNotifSettings(&model.UserSettings{}))
	})

	t.Run("all three set", func(t *testing.T) {
		got := resolveNotifSettings(&model.UserSettings{
			MuteAllNotifications:             boolPtr(true),
			AlwaysAllowPriorityNotifications: boolPtr(true),
			ShowNotificationsInCall:          boolPtr(true),
		})
		assert.True(t, got.muteAll)
		assert.True(t, got.allowPriority)
		assert.True(t, got.showInCall)
	})

	t.Run("explicit false is false, not unset", func(t *testing.T) {
		got := resolveNotifSettings(&model.UserSettings{
			MuteAllNotifications: boolPtr(false),
		})
		assert.False(t, got.muteAll)
	})

	t.Run("priority contacts become a set, empties dropped", func(t *testing.T) {
		got := resolveNotifSettings(&model.UserSettings{
			PriorityContacts: []string{"alice", "helper.bot", ""},
		})
		assert.True(t, got.isPriority("alice"))
		assert.True(t, got.isPriority("helper.bot"))
		assert.Len(t, got.priorityContacts, 2, "empty account must not enter the set")
	})

	t.Run("no priority contacts leaves a nil set", func(t *testing.T) {
		got := resolveNotifSettings(&model.UserSettings{})
		assert.Nil(t, got.priorityContacts)
		assert.False(t, got.isPriority("alice"))
	})
}

func TestMongoUserSettings_EmptyAccountsSkipsQuery(t *testing.T) {
	// A nil collection proves no query is attempted on the empty-accounts path.
	s := newMongoUserSettings(nil, 512, time.Second)
	got, err := s.Snapshot(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestNewMongoUserSettings_Defaults(t *testing.T) {
	s := newMongoUserSettings(nil, 0, 0)
	assert.Equal(t, 512, s.batchSize)
	assert.Equal(t, 2*time.Second, s.timeout)
}
```

Add `"time"` and `"github.com/hmchangw/chat/pkg/model"` to that file's imports.

- [ ] **Step 2: Write the failing integration tests**

Append to `notification-worker/integration_test.go` (it already carries
`//go:build integration` and `package main`; `TestMain` lives in `main_test.go`).
Add `"fmt"` and `"go.mongodb.org/mongo-driver/v2/bson"` to that file's imports —
`context`, `testing`, `time`, `mongo`, `testutil`, `require` and `assert` are
already there.

```go
func seedUserSettings(t *testing.T, ctx context.Context, col *mongo.Collection) {
	t.Helper()
	docs := []any{
		bson.M{
			"_id": "u-muted", "account": "muted-user", "active": true,
			"settings": bson.M{
				"muteAllNotifications":             true,
				"alwaysAllowPriorityNotifications": true,
				"priorityContacts":                 []string{"alice", "helper.bot"},
			},
		},
		bson.M{
			"_id": "u-partial", "account": "partial-user", "active": true,
			"settings": bson.M{"showNotificationsInCall": true},
		},
		bson.M{"_id": "u-nosettings", "account": "nosettings-user", "active": true},
		bson.M{"_id": "u-nofield", "account": "noactive-user"}, // missing active → active
		bson.M{
			"_id": "u-inactive", "account": "inactive-user", "active": false,
			"settings": bson.M{"muteAllNotifications": true},
		},
	}
	_, err := col.InsertMany(ctx, docs)
	require.NoError(t, err)
}

func TestMongoUserSettings_Snapshot_Integration(t *testing.T) {
	db := testutil.MongoDB(t, "notification_worker_settings")
	ctx := context.Background()
	usersCol := db.Collection("users")
	seedUserSettings(t, ctx, usersCol)

	s := newMongoUserSettings(usersCol, 512, 5*time.Second)
	got, err := s.Snapshot(ctx, []string{
		"muted-user", "partial-user", "nosettings-user", "noactive-user",
		"inactive-user", "absent-user",
	})
	require.NoError(t, err)

	muted := got["muted-user"]
	assert.True(t, muted.muteAll)
	assert.True(t, muted.allowPriority)
	assert.False(t, muted.showInCall, "unset field resolves to false")
	assert.True(t, muted.isPriority("alice"))
	assert.True(t, muted.isPriority("helper.bot"), "bot accounts are valid priority contacts")
	assert.False(t, muted.isPriority("bob"))

	partial := got["partial-user"]
	assert.False(t, partial.muteAll)
	assert.True(t, partial.showInCall)

	assert.Equal(t, notifSettings{}, got["nosettings-user"], "no settings sub-document → zero value")
	assert.Contains(t, got, "noactive-user", "missing active field counts as active")

	assert.NotContains(t, got, "inactive-user", "active:false is treated as absent, not zero-filled")
	assert.NotContains(t, got, "absent-user", "unknown account is absent, not zero-filled")
}

func TestMongoUserSettings_ChunkingBoundary_Integration(t *testing.T) {
	db := testutil.MongoDB(t, "notification_worker_settings_chunk")
	ctx := context.Background()
	usersCol := db.Collection("users")

	accounts := make([]string, 0, 7)
	docs := make([]any, 0, 7)
	for i := 0; i < 7; i++ {
		account := fmt.Sprintf("chunk-user-%d", i)
		accounts = append(accounts, account)
		docs = append(docs, bson.M{
			"_id": account, "account": account, "active": true,
			"settings": bson.M{"muteAllNotifications": true},
		})
	}
	_, err := usersCol.InsertMany(ctx, docs)
	require.NoError(t, err)

	// Batch size below the seeded count forces ceil(7/3) = 3 chunked $in queries.
	s := newMongoUserSettings(usersCol, 3, 5*time.Second)
	got, err := s.Snapshot(ctx, accounts)
	require.NoError(t, err)

	assert.Len(t, got, 7, "every chunk's results must be merged into one map")
	for _, a := range accounts {
		assert.True(t, got[a].muteAll, "account %s", a)
	}
}
```

- [ ] **Step 3: Run both suites to verify they fail**

Run: `make test SERVICE=notification-worker`
Expected: FAIL — `undefined: resolveNotifSettings`, `undefined: newMongoUserSettings`.

Run: `make test-integration SERVICE=notification-worker`
Expected: FAIL — same undefined symbols, now from the integration build.

- [ ] **Step 4: Write the implementation**

Append to `notification-worker/usersettings.go` (and extend its import block to `context`, `fmt`, `log/slog`, `time`, `go.mongodb.org/mongo-driver/v2/bson`, `go.mongodb.org/mongo-driver/v2/mongo`, `go.mongodb.org/mongo-driver/v2/mongo/options`, `github.com/hmchangw/chat/pkg/model`):

```go
// userSettingsProjection takes only the four gated fields. Deliberately NOT the
// whole-sub-document {"settings":1} projection user-service's fanouts need —
// nothing here re-publishes the settings object.
var userSettingsProjection = bson.M{
	"_id":     0,
	"account": 1,
	"settings.muteAllNotifications":             1,
	"settings.alwaysAllowPriorityNotifications": 1,
	"settings.showNotificationsInCall":          1,
	"settings.priorityContacts":                 1,
}

// mongoUserSettings reads notification settings straight from the users
// collection — one indexed $in per chunk, no cache. Per-user keys make a Valkey
// tier strictly worse than this (one round trip per candidate, and per-account
// keys cross cluster slots), so there is no L2 here by design.
type mongoUserSettings struct {
	col       *mongo.Collection
	batchSize int
	timeout   time.Duration
}

func newMongoUserSettings(col *mongo.Collection, batchSize int, timeout time.Duration) *mongoUserSettings {
	if batchSize <= 0 {
		batchSize = 512
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &mongoUserSettings{col: col, batchSize: batchSize, timeout: timeout}
}

// Snapshot returns settings keyed by account. It never returns an error: a failed
// or timed-out read yields whatever was collected so far, and absent accounts take
// the zero notifSettings, i.e. today's behaviour. See the spec on why this gate
// fails open like its two neighbours rather than silencing the site.
func (m *mongoUserSettings) Snapshot(ctx context.Context, accounts []string) (map[string]notifSettings, error) {
	out := make(map[string]notifSettings, len(accounts))
	if len(accounts) == 0 {
		return out, nil
	}
	// Bound the new dependency rather than inheriting the consumer's deadline.
	qctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()

	for _, chunk := range chunkStrings(accounts, m.batchSize) {
		if err := m.appendChunk(qctx, chunk, out); err != nil {
			slog.Warn("user settings lookup failed, defaulting to push",
				"error", err, "chunk", len(chunk))
			return out, nil
		}
	}
	return out, nil
}

func (m *mongoUserSettings) appendChunk(ctx context.Context, chunk []string, out map[string]notifSettings) error {
	// active:{$ne:false} matches activeUserFilter in user-service so this read
	// agrees with user-service about what an active user is.
	filter := bson.M{"account": bson.M{"$in": chunk}, "active": bson.M{"$ne": false}}
	cur, err := m.col.Find(ctx, filter, options.Find().SetProjection(userSettingsProjection))
	if err != nil {
		return fmt.Errorf("find user settings: %w", err)
	}
	defer cur.Close(ctx)

	for cur.Next(ctx) {
		var doc struct {
			Account  string              `bson:"account"`
			Settings *model.UserSettings `bson:"settings"`
		}
		if err := cur.Decode(&doc); err != nil {
			return fmt.Errorf("decode user settings: %w", err)
		}
		if doc.Account == "" {
			continue
		}
		out[doc.Account] = resolveNotifSettings(doc.Settings)
	}
	if err := cur.Err(); err != nil {
		return fmt.Errorf("iterate user settings: %w", err)
	}
	return nil
}

// resolveNotifSettings collapses the stored pointer fields into a total value and
// decodes priorityContacts into a set, so the gate never dereferences a nil.
func resolveNotifSettings(s *model.UserSettings) notifSettings {
	if s == nil {
		return notifSettings{}
	}
	ns := notifSettings{
		muteAll:       boolValue(s.MuteAllNotifications),
		allowPriority: boolValue(s.AlwaysAllowPriorityNotifications),
		showInCall:    boolValue(s.ShowNotificationsInCall),
	}
	if len(s.PriorityContacts) > 0 {
		ns.priorityContacts = make(map[string]struct{}, len(s.PriorityContacts))
		for _, a := range s.PriorityContacts {
			if a != "" {
				ns.priorityContacts[a] = struct{}{}
			}
		}
	}
	return ns
}

// boolValue treats an unset pointer as false — an absent setting means the user
// never enabled it.
func boolValue(p *bool) bool { return p != nil && *p }
```

- [ ] **Step 5: Run unit tests to verify they pass**

Run: `make test SERVICE=notification-worker`
Expected: PASS.

- [ ] **Step 6: Run integration tests to verify they pass**

Run: `make test-integration SERVICE=notification-worker`
Expected: PASS — both new tests green, plus the pre-existing `TestNotificationWorker_CacheBackedFanOut`.

- [ ] **Step 7: Lint**

Run: `make lint`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add notification-worker/usersettings.go notification-worker/usersettings_test.go notification-worker/integration_test.go
git commit -m "feat(notification-worker): add Mongo-backed user settings snapshotter"
```

---

### Task 4: Handler wiring — fetch placement and the survivor loop

**Files:**
- Modify: `notification-worker/handler.go:36-46` (`HandlerDeps`), `:62-70` (`NewHandler`), `:197-206` (fetch + survivor loop)
- Modify: `notification-worker/handler_test.go` (add a stub and four tests)

**Interfaces:**
- Consumes: `UserSettingsSnapshotter`, `notifSettings`, `noopUserSettings` from Task 1; `shouldPush(p, ns, isPrioritySender)` from Task 2.
- Produces: `HandlerDeps.Settings UserSettingsSnapshotter` — Task 5 populates it from `main.go`.

**Why `NewHandler` must nil-default `Settings`:** every existing construction site (`newTestHandler` in `handler_test.go:119`, and `integration_test.go`'s `HandlerDeps` literal) omits the field. Defaulting to `noopUserSettings{}` mirrors the existing `LargeRoomThreshold`/`RecipientBatchSize` defaulting and keeps those call sites compiling and passing unchanged.

- [ ] **Step 1: Write the failing tests**

Add this stub near `stubPresence` in `notification-worker/handler_test.go` (after line 89):

```go
// stubSettings records the accounts slice it was called with so tests can pin
// where in the pipeline the fetch runs.
type stubSettings struct {
	mu       sync.Mutex
	out      map[string]notifSettings
	err      error
	gotCalls [][]string
}

func (s *stubSettings) Snapshot(_ context.Context, accounts []string) (map[string]notifSettings, error) {
	s.mu.Lock()
	got := make([]string, len(accounts))
	copy(got, accounts)
	s.gotCalls = append(s.gotCalls, got)
	s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	return s.out, nil
}

func (s *stubSettings) lastAccounts() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.gotCalls) == 0 {
		return nil
	}
	return s.gotCalls[len(s.gotCalls)-1]
}

// accountVetoer rejects exactly the named accounts, so a test can exercise the
// hook-veto exclusion without rejectHook's all-or-nothing behaviour.
type accountVetoer struct {
	deny map[string]struct{}
}

func (a accountVetoer) Allow(_ context.Context, _ *model.Message, m roomsubcache.Member) (bool, error) {
	_, denied := a.deny[m.Account]
	return !denied, nil
}

// newTestHandlerWithSettings mirrors newTestHandler but injects a settings
// snapshotter and a caller-supplied Vetoer.
func newTestHandlerWithSettings(members MemberCache, presence PresenceSnapshotter, settings UserSettingsSnapshotter, hook Vetoer, emit Emitter) *Handler {
	return NewHandler(HandlerDeps{
		Members:            members,
		Followers:          &stubFollowers{},
		Parent:             stubParent{},
		Presence:           presence,
		Settings:           settings,
		Hook:               hook,
		Emitter:            emit,
		LargeRoomThreshold: 500,
	})
}
```

Then append these four tests:

```go
// TestHandle_SettingsFetchedOnlyForSurvivingCandidates pins the design: the fetch
// sits after the candidate loop, so it must never see accounts that an upstream
// filter already excluded. Hoisting it above the loop fails this loudly.
func TestHandle_SettingsFetchedOnlyForSurvivingCandidates(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},              // sender — excluded
			{ID: "bob", Account: "bob", Muted: true},     // muted — excluded
			{ID: "carol", Account: "carol", IsBot: true}, // bot — excluded by EligibleForPush
			{ID: "dave", Account: "dave", HistorySharedSince: int64Ptr(time.Now().Add(time.Hour).UnixMilli())}, // restricted — excluded
			{ID: "frank", Account: "frank"}, // hook veto — excluded
			{ID: "erin", Account: "erin"},   // survivor
		},
	}}
	settings := &stubSettings{out: map[string]notifSettings{}}
	emit := &recordingEmitter{}
	hook := accountVetoer{deny: map[string]struct{}{"frank": {}}}
	h := newTestHandlerWithSettings(members, noopPresenceSnapshotter{}, settings, hook, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		CreatedAt: time.Now(),
	})))

	assert.Equal(t, []string{"erin"}, settings.lastAccounts(),
		"settings must be fetched only for accounts that survived the candidate loop")
	assert.Equal(t, []string{"erin"}, emit.accounts())
}

func TestHandle_SettingsErrorFailsOpen(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {{ID: "alice", Account: "alice"}, {ID: "bob", Account: "bob"}, {ID: "carol", Account: "carol"}},
	}}
	settings := &stubSettings{err: errors.New("mongo: connection refused")}
	emit := &recordingEmitter{}
	h := newTestHandlerWithSettings(members, noopPresenceSnapshotter{}, settings, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		CreatedAt: time.Now(),
	})))
	assert.Equal(t, []string{"bob", "carol"}, emit.accounts(),
		"a settings read failure must not silence pushes")
}

func TestHandle_SettingsPartialMapFailsOpenForAbsentAccounts(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {{ID: "alice", Account: "alice"}, {ID: "bob", Account: "bob"}, {ID: "carol", Account: "carol"}},
	}}
	settings := &stubSettings{out: map[string]notifSettings{
		"bob": {muteAll: true}, // carol is absent → zero value → pushes
	}}
	emit := &recordingEmitter{}
	h := newTestHandlerWithSettings(members, noopPresenceSnapshotter{}, settings, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		CreatedAt: time.Now(),
	})))
	assert.Equal(t, []string{"carol"}, emit.accounts(),
		"bob is muted; carol is absent from the map and takes the zero value")
}

// TestHandle_BotAuthoredMessagePiercesMute is the Spec 1 affordance that only pays
// off because the gate runs in bot mode too: a .bot account listed as a priority
// contact pierces muteAllNotifications.
func TestHandle_BotAuthoredMessagePiercesMute(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "helper", Account: "helper.bot", IsBot: true},
			{ID: "bob", Account: "bob"},
			{ID: "carol", Account: "carol"},
		},
	}}
	settings := &stubSettings{out: map[string]notifSettings{
		"bob": {
			muteAll:          true,
			allowPriority:    true,
			priorityContacts: map[string]struct{}{"helper.bot": {}},
		},
		"carol": {muteAll: true, allowPriority: true}, // no priority contacts → stays muted
	}}
	emit := &recordingEmitter{}
	h := newTestHandlerWithSettings(members, noopPresenceSnapshotter{}, settings, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "helper", UserAccount: "helper.bot",
		CreatedAt: time.Now(),
	})))
	assert.Equal(t, []string{"bob"}, emit.accounts(),
		"bob listed helper.bot as priority; carol did not")
}

func int64Ptr(v int64) *int64 { return &v }
```

All imports these tests need (`context`, `errors`, `sync`, `testing`, `time`, `assert`, `require`, `model`, `roomsubcache`) are already in that file's import block. No `int64Ptr` or `boolPtr` helper exists anywhere in `notification-worker`'s test files, so the declaration above is the only one.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=notification-worker`
Expected: FAIL — `unknown field Settings in struct literal of type HandlerDeps`.

- [ ] **Step 3: Write the implementation**

In `notification-worker/handler.go`, add the field to `HandlerDeps` (after `Presence` on line 40):

```go
	Presence           PresenceSnapshotter
	Settings           UserSettingsSnapshotter // nil → noopUserSettings (pre-enforcement behaviour)
```

In `NewHandler`, add the nil-default alongside the existing ones:

```go
func NewHandler(deps HandlerDeps) *Handler { //nolint:gocritic // hugeParam: one-time constructor arg
	if deps.LargeRoomThreshold <= 0 {
		deps.LargeRoomThreshold = 500
	}
	if deps.RecipientBatchSize <= 0 {
		deps.RecipientBatchSize = defaultRecipientBatchSize
	}
	if deps.Settings == nil {
		deps.Settings = noopUserSettings{}
	}
	return &Handler{deps: deps}
}
```

Replace lines 197-206 (the presence fetch and survivor loop) with:

```go
	// Both lookups run over the narrowed candidate set, and settings must be in
	// hand before presence is evaluated because showNotificationsInCall modifies
	// the presence decision. TestHandle_SettingsFetchedOnlyForSurvivingCandidates
	// pins this placement.
	settings, _ := h.deps.Settings.Snapshot(ctx, accounts) // fail-open: error → empty map
	snapshot, _ := h.deps.Presence.Snapshot(ctx, accounts) // fail-open: error → empty map

	// Sort survivors so batch N has a deterministic account set across redeliveries — required for the {messageID}-b{N} Nats-Msg-Id to dedup correctly.
	survivors := make([]string, 0, len(candidates))
	for _, c := range candidates {
		ns := settings[c.Account]
		if !shouldPush(snapshot[c.Account], ns, ns.isPriority(msg.UserAccount)) {
			continue
		}
		survivors = append(survivors, c.Account)
	}
```

Also update the `Handler` doc comment on line 48-49 so it names the new gate:

```go
// Handler runs the per-message fan-out pipeline: exclusion filters, hook veto, EligibleForPush
// routing, then the settings- and presence-gated shouldPush — one Emitter.Emit per surviving recipient.
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=notification-worker`
Expected: PASS — the four new tests green and all pre-existing handler tests still green (they get `noopUserSettings` via the nil default).

- [ ] **Step 5: Verify coverage clears the floor**

The Makefile's `test` target does not emit a profile, and CLAUDE.md §4 names these
exact commands for checking coverage — this is the one sanctioned raw-`go` use:

```bash
go test -race -coverprofile=coverage.out ./notification-worker/...
go tool cover -func=coverage.out | grep -E "shouldPush|isInCall|isPriority|resolveNotifSettings|Snapshot|HandleMessage"
```

Expected: package total ≥ 80%; `shouldPush`, `isInCall`, `isPriority` and
`resolveNotifSettings` at 100%; `mongoUserSettings.Snapshot` ≥ 90% (its Mongo error
branch is covered by the integration suite, so read that number from a run that
includes `-tags integration` before concluding it is short). Delete `coverage.out`
when done — it must not be committed.

- [ ] **Step 6: Lint**

Run: `make lint`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add notification-worker/handler.go notification-worker/handler_test.go
git commit -m "feat(notification-worker): gate push on user notification settings"
```

---

### Task 5: Configuration, wiring, and both deployments

**Files:**
- Modify: `notification-worker/main.go:35-61` (config), `:143-146` (collections), `:200-220` (wiring), `:325-331` (startup log)
- Modify: `notification-worker/config_test.go` (add a defaults test)
- Modify: `notification-worker/deploy/user/docker-compose.yml`
- Modify: `notification-worker/deploy/bot/docker-compose.yml`

**Interfaces:**
- Consumes: `newMongoUserSettings` (Task 3), `noopUserSettings` (Task 1), `HandlerDeps.Settings` (Task 4).
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Write the failing test**

Append to `notification-worker/config_test.go`:

```go
func TestConfig_UserSettingsDefaults(t *testing.T) {
	t.Setenv("VALKEY_ADDRS", "valkey:6379")
	t.Setenv("MODE", "user")
	// env.ParseAs reads os.Environ(), so an inherited USER_SETTINGS_ENABLED or
	// PRESENCE_RPC_ENABLED on the host would shadow the envDefault this test
	// exists to pin. t.Setenv first so the original value is restored on cleanup,
	// then unset — caarlos0/env treats a defined-but-empty var as set.
	t.Setenv("USER_SETTINGS_ENABLED", "")
	require.NoError(t, os.Unsetenv("USER_SETTINGS_ENABLED"))
	t.Setenv("PRESENCE_RPC_ENABLED", "")
	require.NoError(t, os.Unsetenv("PRESENCE_RPC_ENABLED"))

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)

	// Enforcement is on by default: Mongo is already a hard dependency of this
	// service, and a gate that ships defaulted off is a gate nobody turns on.
	// PRESENCE_RPC_ENABLED defaults the other way because presence-service may not exist yet.
	require.True(t, cfg.UserSettingsEnabled, "USER_SETTINGS_ENABLED must default to true")
	require.False(t, cfg.PresenceEnabled, "PRESENCE_RPC_ENABLED must stay defaulted to false")
	require.Equal(t, 512, cfg.UserSettingsBatchSize)
	require.Equal(t, 2*time.Second, cfg.UserSettingsTimeout)
}

func TestConfig_UserSettingsKillSwitch(t *testing.T) {
	t.Setenv("VALKEY_ADDRS", "valkey:6379")
	t.Setenv("MODE", "user")
	t.Setenv("USER_SETTINGS_ENABLED", "false")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.False(t, cfg.UserSettingsEnabled)
}
```

`os` and `env` are already imported in that file; add `"time"`.

This unset-then-restore dance is the same shape `TestConfig_Mode` already uses for
`MODE` at `notification-worker/config_test.go:31-34`, and for the same reason.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=notification-worker`
Expected: FAIL — `cfg.UserSettingsEnabled undefined`.

- [ ] **Step 3: Write the implementation**

In `notification-worker/main.go`, add three fields to `config` immediately after `PresenceEnabled` (line 54):

```go
	PresenceEnabled        bool                    `env:"PRESENCE_RPC_ENABLED"      envDefault:"false"`  // false → noopPresenceSnapshotter; set true once presence service is available
	UserSettingsEnabled    bool                    `env:"USER_SETTINGS_ENABLED"     envDefault:"true"`   // false → noopUserSettings, i.e. pre-enforcement behaviour; kill switch, not a rollout gate
	UserSettingsBatchSize  int                     `env:"USER_SETTINGS_BATCH_SIZE"  envDefault:"512"`
	UserSettingsTimeout    time.Duration           `env:"USER_SETTINGS_TIMEOUT"     envDefault:"2s"`
```

Add the collection next to the others (after line 146):

```go
	roomsCol := db.Collection("rooms")
	usersCol := db.Collection("users")
```

Add the wiring immediately after the presence block (after line 208):

```go
	var settings UserSettingsSnapshotter = noopUserSettings{}
	if cfg.UserSettingsEnabled {
		settings = newMongoUserSettings(usersCol, cfg.UserSettingsBatchSize, cfg.UserSettingsTimeout)
	}
```

Add the dep to the `HandlerDeps` literal, after `Presence:` (line 214):

```go
		Presence:           presence,
		Settings:           settings,
```

Add to the startup log (after line 330):

```go
		"presence_enabled", cfg.PresenceEnabled,
		"user_settings_enabled", cfg.UserSettingsEnabled,
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=notification-worker`
Expected: PASS.

- [ ] **Step 5: Update the user deployment compose**

In `notification-worker/deploy/user/docker-compose.yml`, replace the stale invariant comment — the block currently reading:

```yaml
      # Title is resolved here from the rooms collection; sender display name is
      # pre-composed by message-gatekeeper and propagated on the canonical message,
      # so no users-collection lookup runs in this service.
```

with:

```yaml
      # Title is resolved here from the rooms collection; sender display name is
      # pre-composed by message-gatekeeper and propagated on the canonical message.
      # The users collection IS read, but only for notification settings: an
      # indexed $in per 512-account chunk over the already-narrowed candidate
      # set — one query for almost every room — never a per-recipient lookup and
      # never on the sender-name path.
```

Then add the three vars after `PRESENCE_RPC_ENABLED=false`:

```yaml
      - PRESENCE_RPC_ENABLED=false
      # Enforces muteAllNotifications / alwaysAllowPriorityNotifications /
      # showNotificationsInCall. Kill switch: false reverts to pre-enforcement
      # delivery without rolling back the binary.
      - USER_SETTINGS_ENABLED=true
      - USER_SETTINGS_BATCH_SIZE=512
      - USER_SETTINGS_TIMEOUT=2s
```

- [ ] **Step 6: Update the bot deployment compose**

In `notification-worker/deploy/bot/docker-compose.yml`, add the same three vars after its `PRESENCE_RPC_ENABLED=false` line (line 28):

```yaml
      - PRESENCE_RPC_ENABLED=false
      # Same gate as user mode — bot-authored messages fan out to human members,
      # so a .bot priority contact pierces mute only if this runs here too.
      - USER_SETTINGS_ENABLED=true
      - USER_SETTINGS_BATCH_SIZE=512
      - USER_SETTINGS_TIMEOUT=2s
```

- [ ] **Step 7: Verify the binary builds and lint is clean**

Run: `make build SERVICE=notification-worker`
Expected: builds.

Run: `make lint`
Expected: clean.

- [ ] **Step 8: Run the SAST gate**

Run: `make sast`
Expected: no medium+ findings.

- [ ] **Step 9: Commit**

```bash
git add notification-worker/main.go notification-worker/config_test.go \
  notification-worker/deploy/user/docker-compose.yml notification-worker/deploy/bot/docker-compose.yml
git commit -m "feat(notification-worker): wire settings snapshotter and kill switch"
```

---

### Task 6: Client API documentation

**Files:**
- Modify: `docs/client-api.md:4701-4704`

**Interfaces:**
- Consumes: nothing.
- Produces: nothing.

**Scope note:** the three field descriptions gain an enforcement note and the priority-contact interaction is stated once. No schema, struct, or event changed, so `docs/client-api/request-reply.md` and `docs/client-api/events.md` stay untouched — do not edit them.

- [ ] **Step 1: Edit the settings field table**

In `docs/client-api.md`, in the settings field table, replace these three rows:

```markdown
| `muteAllNotifications` | boolean | Mute all notifications. |
| `alwaysAllowPriorityNotifications` | boolean | Always allow priority-contact notifications, even when muted. |
```

and

```markdown
| `showNotificationsInCall` | boolean | Show notifications in call. |
```

with:

```markdown
| `muteAllNotifications` | boolean | Mute all notifications. Enforced server-side by `notification-worker`: push delivery is suppressed for this user unless pierced — see `alwaysAllowPriorityNotifications`. |
| `alwaysAllowPriorityNotifications` | boolean | Always allow priority-contact notifications, even when muted. Enforced server-side: a message whose sender is in [`priorityContacts`](#settingsprioritycontactsadd) pierces `muteAllNotifications` in **any** room type — DM and channel alike — and for `.bot` senders as well as users. The pierce does not override `showNotificationsInCall`. |
```

and

```markdown
| `showNotificationsInCall` | boolean | Show notifications in call. Enforced server-side: when unset or `false`, push is suppressed while the user's presence is `"busy"` or `"in-call"`. A priority-contact pierce of `muteAllNotifications` does not bypass this — set both to receive priority pushes while in a call. |
```

- [ ] **Step 2: Verify no derived view drifted**

Run: `grep -n "muteAllNotifications\|showNotificationsInCall" docs/client-api/request-reply.md docs/client-api/events.md`
Expected: whatever those files said before is unchanged — this task must not have touched them.

- [ ] **Step 3: Commit**

```bash
git add docs/client-api.md
git commit -m "docs(client-api): note server-side enforcement of the three notification settings"
```

---

## Notes for whoever ships this

**Release note is required.** This deploy changes delivery for settings users already stored via Spec 1 — accounts carrying `muteAllNotifications: true` stop receiving pushes the moment it lands. The release note should say: notification settings stored via the settings API are now enforced for push delivery; users who previously enabled "mute all notifications" will stop receiving pushes; `USER_SETTINGS_ENABLED=false` reverts to prior behaviour without a rollback.

**Size the blast radius before deploying.** Run against production `users`:

```javascript
db.users.countDocuments({"settings.muteAllNotifications": true, "active": {"$ne": false}})
```

so the change lands as a known number rather than a surprise.

**Deliberately out of scope, do not fix here:** candidates come from `subscriptions`, not `users`, so a deactivated account with a live subscription stays in the slice, misses the settings map, takes the zero value, and pushes. That is today's behaviour — this service consults `users` nowhere at present. Whether deactivated users should receive pushes is a real pre-existing gap that belongs in its own change against the member-loading path; fixing it here would silently change delivery for a population this work never set out to touch.
