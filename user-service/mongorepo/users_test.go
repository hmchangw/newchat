//go:build integration

package mongorepo

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
)

func TestGetUserStatus_Integration(t *testing.T) {
	r, db := newTestUserRepo(t)
	ctx := context.Background()
	seed(t, db, "users",
		bson.M{"_id": "u-bob", "account": "bob", "active": true, "statusText": "hi", "engName": "Bob", "chineseName": "鮑勃"},
		bson.M{"_id": "u-ghost", "account": "ghost", "active": false, "statusText": "gone"},
		// `active` field absent — scheduled to land later; must be treated as active.
		bson.M{"_id": "u-noactive", "account": "noactive", "statusText": "present", "engName": "NoActive"},
	)

	t.Run("active user found", func(t *testing.T) {
		u, err := r.GetUserStatus(ctx, "bob")
		require.NoError(t, err)
		require.NotNil(t, u)
		assert.Equal(t, "bob", u.Account)
		assert.Equal(t, "hi", u.StatusText)
		assert.Equal(t, "Bob", u.EngName)
	})

	t.Run("explicitly inactive user (active:false) dropped", func(t *testing.T) {
		u, err := r.GetUserStatus(ctx, "ghost")
		require.NoError(t, err)
		assert.Nil(t, u)
	})

	t.Run("missing active field is treated as active", func(t *testing.T) {
		u, err := r.GetUserStatus(ctx, "noactive")
		require.NoError(t, err)
		require.NotNil(t, u, "absent `active` must count as active ({$ne:false})")
		assert.Equal(t, "present", u.StatusText)
	})

	t.Run("missing user not found", func(t *testing.T) {
		u, err := r.GetUserStatus(ctx, "nobody")
		require.NoError(t, err)
		assert.Nil(t, u)
	})
}

func TestGetUserStatus_ProjectsOnlyStatusFields_Integration(t *testing.T) {
	r, db := newTestUserRepo(t)
	ctx := context.Background()
	seed(t, db, "users", bson.M{
		"_id": "u-carol", "account": "carol", "active": true,
		"statusText": "deep work", "statusIsShow": true,
		"chineseName": "卡蘿", "engName": "Carol",
		// Outside the projection — must come back zero-valued.
		"deptId": "dept-42", "roles": bson.A{"admin"},
	})

	u, err := r.GetUserStatus(ctx, "carol")
	require.NoError(t, err)
	require.NotNil(t, u)
	assert.Equal(t, "carol", u.Account)
	assert.Equal(t, "deep work", u.StatusText)
	assert.True(t, u.StatusIsShow)
	assert.Equal(t, "卡蘿", u.ChineseName)
	assert.Equal(t, "Carol", u.EngName)
	assert.Empty(t, u.DeptID, "deptId is outside the status projection")
	assert.Empty(t, u.Roles, "roles are outside the status projection")
}

func TestGetHRInfoByAccounts_Integration(t *testing.T) {
	r, db := newTestUserRepo(t)
	ctx := context.Background()
	seed(t, db, "users",
		bson.M{"_id": "u-bob", "account": "bob", "active": true, "chineseName": "鮑勃", "engName": "Bob Chen",
			// Outside the projection — must not leak into the result.
			"statusText": "deep work", "deptId": "dept-1"},
		bson.M{"_id": "u-carol", "account": "carol", "active": true, "chineseName": "卡蘿", "engName": "Carol"},
	)

	t.Run("maps accounts to HR records (name = chineseName)", func(t *testing.T) {
		hr, err := r.GetHRInfoByAccounts(ctx, []string{"bob", "carol"})
		require.NoError(t, err)
		require.Len(t, hr, 2)
		require.NotNil(t, hr["bob"])
		assert.Equal(t, "bob", hr["bob"].Account)
		assert.Equal(t, "鮑勃", hr["bob"].Name, "hrInfo.name must mirror chineseName")
		assert.Equal(t, "Bob Chen", hr["bob"].EngName)
		require.NotNil(t, hr["carol"])
		assert.Equal(t, "卡蘿", hr["carol"].Name)
	})

	t.Run("unknown account omitted", func(t *testing.T) {
		hr, err := r.GetHRInfoByAccounts(ctx, []string{"bob", "ghost"})
		require.NoError(t, err)
		require.Len(t, hr, 1)
		require.NotNil(t, hr["bob"])
	})

	t.Run("empty input yields empty map", func(t *testing.T) {
		hr, err := r.GetHRInfoByAccounts(ctx, []string{})
		require.NoError(t, err)
		assert.Empty(t, hr)
	})
}

func TestSetUserStatus_Integration(t *testing.T) {
	ctx := context.Background()

	t.Run("updates text and isShow, returns the updated doc", func(t *testing.T) {
		r, db := newTestUserRepo(t)
		seed(t, db, "users",
			bson.M{"_id": "u-bob", "account": "bob", "active": true, "statusText": "hi",
				"chineseName": "鮑勃", "engName": "Bob"},
		)

		show := true
		u, err := r.SetUserStatus(ctx, "bob", "busy", &show)
		require.NoError(t, err)
		require.NotNil(t, u, "existing active user must be returned")
		// ReturnDocument:After ⇒ the returned doc is the persisted post-update state.
		assert.Equal(t, "bob", u.Account)
		assert.Equal(t, "busy", u.StatusText)
		assert.True(t, u.StatusIsShow)
		assert.Equal(t, "鮑勃", u.ChineseName)
		assert.Equal(t, "Bob", u.EngName)
	})

	t.Run("nil isShow leaves flag untouched", func(t *testing.T) {
		r, db := newTestUserRepo(t)
		seed(t, db, "users",
			bson.M{"_id": "u-bob", "account": "bob", "active": true, "statusText": "hi", "statusIsShow": true},
		)

		u, err := r.SetUserStatus(ctx, "bob", "away", nil)
		require.NoError(t, err)
		require.NotNil(t, u)
		assert.Equal(t, "away", u.StatusText)
		assert.True(t, u.StatusIsShow, "previously-set flag must survive a text-only update")
	})

	t.Run("unknown account returns nil", func(t *testing.T) {
		r, _ := newTestUserRepo(t)
		u, err := r.SetUserStatus(ctx, "nobody", "busy", nil)
		require.NoError(t, err)
		assert.Nil(t, u, "no active user doc ⇒ nil")
	})

	t.Run("explicitly inactive account returns nil", func(t *testing.T) {
		r, db := newTestUserRepo(t)
		seed(t, db, "users",
			bson.M{"_id": "u-ghost", "account": "ghost", "active": false, "statusText": "gone"},
		)
		u, err := r.SetUserStatus(ctx, "ghost", "busy", nil)
		require.NoError(t, err)
		assert.Nil(t, u, "active:false user is excluded by the filter ⇒ nil")
	})
}

func TestGetUserPriorityContacts_Integration(t *testing.T) {
	r, db := newTestUserRepo(t)
	ctx := context.Background()
	seed(t, db, "users",
		bson.M{"_id": "u1", "account": "alice", "settings": bson.M{"priorityContacts": []string{"bob", "helper.bot"}}},
		bson.M{"_id": "u2", "account": "carol"},
		bson.M{"_id": "u3", "account": "dave", "active": false},
	)

	t.Run("returns the stored list in order", func(t *testing.T) {
		u, err := r.GetUserPriorityContacts(ctx, "alice")
		require.NoError(t, err)
		require.NotNil(t, u)
		require.NotNil(t, u.Settings)
		assert.Equal(t, []string{"bob", "helper.bot"}, u.Settings.PriorityContacts)
	})

	t.Run("user with no settings returns a user with nil settings", func(t *testing.T) {
		u, err := r.GetUserPriorityContacts(ctx, "carol")
		require.NoError(t, err)
		require.NotNil(t, u)
		assert.Nil(t, u.Settings)
	})

	t.Run("inactive user is not found", func(t *testing.T) {
		u, err := r.GetUserPriorityContacts(ctx, "dave")
		require.NoError(t, err)
		assert.Nil(t, u)
	})

	t.Run("unknown account is not found", func(t *testing.T) {
		u, err := r.GetUserPriorityContacts(ctx, "ghost")
		require.NoError(t, err)
		assert.Nil(t, u)
	})
}

func TestUserExists_Integration(t *testing.T) {
	r, db := newTestUserRepo(t)
	ctx := context.Background()
	seed(t, db, "users",
		bson.M{"_id": "u1", "account": "alice"},
		bson.M{"_id": "u2", "account": "dave", "active": false},
	)

	t.Run("active user exists", func(t *testing.T) {
		ok, err := r.UserExists(ctx, "alice")
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("deactivated user does not", func(t *testing.T) {
		ok, err := r.UserExists(ctx, "dave")
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("unknown account does not", func(t *testing.T) {
		ok, err := r.UserExists(ctx, "ghost")
		require.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestGetPriorityContactUsers_Integration(t *testing.T) {
	r, db := newTestUserRepo(t)
	ctx := context.Background()
	seed(t, db, "users",
		bson.M{"_id": "u1", "account": "bob", "engName": "Bob", "chineseName": "鮑伯", "employeeId": "E9", "sectName": "Ops"},
		bson.M{"_id": "u2", "account": "erin", "engName": "Erin", "chineseName": "艾琳", "employeeId": "E8", "sectName": "QA", "active": false},
	)

	t.Run("projects all four display fields", func(t *testing.T) {
		got, err := r.GetPriorityContactUsers(ctx, []string{"bob"})
		require.NoError(t, err)
		require.NotNil(t, got["bob"])
		assert.Equal(t, "Bob", got["bob"].EngName)
		assert.Equal(t, "鮑伯", got["bob"].ChineseName)
		assert.Equal(t, "E9", got["bob"].EmployeeID)
		assert.Equal(t, "Ops", got["bob"].SectName)
	})

	// Deliberately NOT active-filtered: a contact deactivated after being added
	// still renders their name instead of collapsing to a bare account row.
	t.Run("includes deactivated users", func(t *testing.T) {
		got, err := r.GetPriorityContactUsers(ctx, []string{"erin"})
		require.NoError(t, err)
		require.NotNil(t, got["erin"])
		assert.Equal(t, "Erin", got["erin"].EngName)
	})

	t.Run("unknown accounts are omitted", func(t *testing.T) {
		got, err := r.GetPriorityContactUsers(ctx, []string{"bob", "ghost"})
		require.NoError(t, err)
		assert.Len(t, got, 1)
	})

	t.Run("empty input returns an empty map", func(t *testing.T) {
		got, err := r.GetPriorityContactUsers(ctx, []string{})
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}

func TestAddPriorityContact_Integration(t *testing.T) {
	r, db := newTestUserRepo(t)
	ctx := context.Background()
	at := time.UnixMilli(1_700_000_000_000).UTC()
	seed(t, db, "users",
		bson.M{"_id": "u1", "account": "alice", "settings": bson.M{"themePreference": "dark"}},
		bson.M{"_id": "u2", "account": "dave", "active": false},
	)

	t.Run("adds and returns the whole settings sub-document", func(t *testing.T) {
		u, err := r.AddPriorityContact(ctx, "alice", "bob", 30, at)
		require.NoError(t, err)
		require.NotNil(t, u)
		require.NotNil(t, u.Settings)
		assert.Equal(t, []string{"bob"}, u.Settings.PriorityContacts)
		// The projection MUST stay {"_id":0,"settings":1}: this object is the
		// cross-site fanout payload and inbox-worker $sets it whole.
		require.NotNil(t, u.Settings.ThemePreference)
		assert.Equal(t, "dark", *u.Settings.ThemePreference)
	})

	t.Run("stamps settingsUpdatedAt", func(t *testing.T) {
		var doc struct {
			SettingsUpdatedAt time.Time `bson:"settingsUpdatedAt"`
		}
		require.NoError(t, db.Collection("users").FindOne(ctx, bson.M{"account": "alice"}).Decode(&doc))
		assert.Equal(t, at, doc.SettingsUpdatedAt.UTC())
	})

	t.Run("re-adding is a no-op", func(t *testing.T) {
		u, err := r.AddPriorityContact(ctx, "alice", "bob", 30, at)
		require.NoError(t, err)
		require.NotNil(t, u)
		assert.Equal(t, []string{"bob"}, u.Settings.PriorityContacts)
	})

	t.Run("at cap returns no match", func(t *testing.T) {
		u, err := r.AddPriorityContact(ctx, "alice", "carol", 1, at)
		require.NoError(t, err)
		assert.Nil(t, u)
	})

	t.Run("inactive user returns no match", func(t *testing.T) {
		u, err := r.AddPriorityContact(ctx, "dave", "bob", 30, at)
		require.NoError(t, err)
		assert.Nil(t, u)
	})
}

// The whole reason the cap rides in the filter: a read-then-write guard lets two
// concurrent adds both pass at limit-1 and overshoot.
func TestAddPriorityContact_ConcurrentAddsRespectCap_Integration(t *testing.T) {
	r, db := newTestUserRepo(t)
	ctx := context.Background()
	at := time.UnixMilli(1_700_000_000_000).UTC()

	// Named `existing`, not `seed` — `seed` is the package-level helper.
	existing := make([]string, 0, 28)
	for i := 0; i < 28; i++ {
		existing = append(existing, fmt.Sprintf("existing%02d", i))
	}
	seed(t, db, "users",
		bson.M{"_id": "u1", "account": "alice", "settings": bson.M{"priorityContacts": existing}},
	)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_, _ = r.AddPriorityContact(ctx, "alice", fmt.Sprintf("race%02d", n), 30, at)
		}(i)
	}
	wg.Wait()

	u, err := r.GetUserPriorityContacts(ctx, "alice")
	require.NoError(t, err)
	require.NotNil(t, u.Settings)
	assert.Len(t, u.Settings.PriorityContacts, 30, "cap must hold under concurrent adds")
}

func TestRemovePriorityContact_Integration(t *testing.T) {
	r, db := newTestUserRepo(t)
	ctx := context.Background()
	at := time.UnixMilli(1_700_000_000_000).UTC()
	seed(t, db, "users",
		bson.M{"_id": "u1", "account": "alice", "settings": bson.M{
			"themePreference": "dark", "priorityContacts": []string{"bob", "helper.bot"},
		}},
		bson.M{"_id": "u2", "account": "dave", "active": false},
	)

	t.Run("removes and returns the whole settings sub-document", func(t *testing.T) {
		u, err := r.RemovePriorityContact(ctx, "alice", "bob", at)
		require.NoError(t, err)
		require.NotNil(t, u)
		assert.Equal(t, []string{"helper.bot"}, u.Settings.PriorityContacts)
		require.NotNil(t, u.Settings.ThemePreference)
		assert.Equal(t, "dark", *u.Settings.ThemePreference)
	})

	t.Run("removing an absent entry is a no-op", func(t *testing.T) {
		u, err := r.RemovePriorityContact(ctx, "alice", "ghost", at)
		require.NoError(t, err)
		require.NotNil(t, u)
		assert.Equal(t, []string{"helper.bot"}, u.Settings.PriorityContacts)
	})

	t.Run("inactive user returns no match", func(t *testing.T) {
		u, err := r.RemovePriorityContact(ctx, "dave", "bob", at)
		require.NoError(t, err)
		assert.Nil(t, u)
	})
}

func TestUpdateUserSettings_StampsSettingsUpdatedAt_Integration(t *testing.T) {
	r, db := newTestUserRepo(t)
	ctx := context.Background()
	at := time.UnixMilli(1_700_000_000_000).UTC()
	seed(t, db, "users", bson.M{"_id": "u1", "account": "alice"})

	dark := "dark"
	u, err := r.UpdateUserSettings(ctx, "alice", &model.UserSettings{ThemePreference: &dark}, at)
	require.NoError(t, err)
	require.NotNil(t, u)

	// Without this stamp the origin doc has no settingsUpdatedAt, so inbox-worker's
	// $exists:false branch always matches and a stale remote event overwrites a
	// newer local edit.
	var doc struct {
		SettingsUpdatedAt time.Time `bson:"settingsUpdatedAt"`
	}
	require.NoError(t, db.Collection("users").FindOne(ctx, bson.M{"account": "alice"}).Decode(&doc))
	assert.Equal(t, at, doc.SettingsUpdatedAt.UTC())
}
