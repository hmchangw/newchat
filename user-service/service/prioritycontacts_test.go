package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/user-service/models"
)

// requireReason asserts the error carries a specific domain reason, which requireCode
// (code only) cannot express. Consumed by the add/remove handler tests (Task 8/9).
func requireReason(t *testing.T, err error, want errcode.Reason) {
	t.Helper()
	require.Error(t, err)
	var ee *errcode.Error
	require.True(t, errors.As(err, &ee), "want *errcode.Error, got %T", err)
	assert.Equal(t, want, ee.Reason)
}

func TestGetPriorityContacts_MixedListPreservesOrder(t *testing.T) {
	svc, _, users, apps, _, _, _ := newSvc(t)

	users.EXPECT().GetUserPriorityContacts(gomock.Any(), "alice").
		Return(&model.User{Settings: &model.UserSettings{
			PriorityContacts: []string{"bob", "helper.bot", "carol"},
		}}, nil)
	users.EXPECT().GetPriorityContactUsers(gomock.Any(), []string{"bob", "carol"}).
		Return(map[string]*models.PriorityContactUser{
			"bob":   {EngName: "Bob", ChineseName: "鮑伯", EmployeeID: "E9", SectName: "Ops"},
			"carol": {EngName: "Carol", ChineseName: "卡蘿", EmployeeID: "E7", SectName: "QA"},
		}, nil)
	apps.EXPECT().GetAppsByAssistants(gomock.Any(), []string{"helper.bot"}).
		Return(map[string]*model.App{"helper.bot": {Name: "Helper"}}, nil)

	resp, err := svc.GetPriorityContacts(ctx("alice", "site-a"))
	require.NoError(t, err)
	require.Len(t, resp.Contacts, 3)

	assert.Equal(t, "bob", resp.Contacts[0].Account)
	assert.Equal(t, models.PriorityContactTypeUser, resp.Contacts[0].Type)
	require.NotNil(t, resp.Contacts[0].User)
	assert.Equal(t, "E9", resp.Contacts[0].User.EmployeeID)
	assert.Nil(t, resp.Contacts[0].App)

	assert.Equal(t, "helper.bot", resp.Contacts[1].Account)
	assert.Equal(t, models.PriorityContactTypeBot, resp.Contacts[1].Type)
	require.NotNil(t, resp.Contacts[1].App)
	assert.Equal(t, "Helper", resp.Contacts[1].App.Name)
	assert.Nil(t, resp.Contacts[1].User)

	assert.Equal(t, "carol", resp.Contacts[2].Account)
}

func TestGetPriorityContacts_EmptyList(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)

	users.EXPECT().GetUserPriorityContacts(gomock.Any(), "alice").
		Return(&model.User{Settings: nil}, nil)

	resp, err := svc.GetPriorityContacts(ctx("alice", "site-a"))
	require.NoError(t, err)
	assert.Empty(t, resp.Contacts)
}

func TestGetPriorityContacts_UserNotFound(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)

	users.EXPECT().GetUserPriorityContacts(gomock.Any(), "ghost").Return(nil, nil)

	_, err := svc.GetPriorityContacts(ctx("ghost", "site-a"))
	requireCode(t, err, errcode.CodeNotFound)
}

// An account that no longer resolves keeps account+type so the client renders a
// placeholder instead of the row vanishing.
func TestGetPriorityContacts_UnresolvedAccountsDegrade(t *testing.T) {
	svc, _, users, apps, _, _, _ := newSvc(t)

	users.EXPECT().GetUserPriorityContacts(gomock.Any(), "alice").
		Return(&model.User{Settings: &model.UserSettings{
			PriorityContacts: []string{"ghost", "gone.bot"},
		}}, nil)
	users.EXPECT().GetPriorityContactUsers(gomock.Any(), []string{"ghost"}).
		Return(map[string]*models.PriorityContactUser{}, nil)
	apps.EXPECT().GetAppsByAssistants(gomock.Any(), []string{"gone.bot"}).
		Return(map[string]*model.App{}, nil)

	resp, err := svc.GetPriorityContacts(ctx("alice", "site-a"))
	require.NoError(t, err)
	require.Len(t, resp.Contacts, 2)
	assert.Equal(t, models.PriorityContactTypeUser, resp.Contacts[0].Type)
	assert.Nil(t, resp.Contacts[0].User)
	assert.Equal(t, models.PriorityContactTypeBot, resp.Contacts[1].Type)
	assert.Nil(t, resp.Contacts[1].App)
}

// A lookup failure degrades the rows rather than failing the call — same posture as
// the thread-list enrichment.
func TestGetPriorityContacts_LookupFailureDegrades(t *testing.T) {
	svc, _, users, apps, _, _, _ := newSvc(t)

	users.EXPECT().GetUserPriorityContacts(gomock.Any(), "alice").
		Return(&model.User{Settings: &model.UserSettings{
			PriorityContacts: []string{"bob", "helper.bot"},
		}}, nil)
	users.EXPECT().GetPriorityContactUsers(gomock.Any(), []string{"bob"}).
		Return(nil, errors.New("db down"))
	apps.EXPECT().GetAppsByAssistants(gomock.Any(), []string{"helper.bot"}).
		Return(nil, errors.New("db down"))

	resp, err := svc.GetPriorityContacts(ctx("alice", "site-a"))
	require.NoError(t, err)
	require.Len(t, resp.Contacts, 2)
	assert.Nil(t, resp.Contacts[0].User)
	assert.Nil(t, resp.Contacts[1].App)
}

func TestGetPriorityContacts_RepoError(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)

	users.EXPECT().GetUserPriorityContacts(gomock.Any(), "alice").Return(nil, errors.New("db down"))

	_, err := svc.GetPriorityContacts(ctx("alice", "site-a"))
	// Raw wrapped error — the router classifies it, the handler must not pre-classify.
	require.Error(t, err)
	var ee *errcode.Error
	assert.False(t, errors.As(err, &ee))
}

func TestAddPriorityContact_Validation(t *testing.T) {
	cases := []struct {
		name    string
		contact string
	}{
		{"empty contact", ""},
		{"self add", "alice"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, _, _, _, _, _, _ := newSvc(t)
			_, err := svc.AddPriorityContact(ctx("alice", "site-a"),
				models.PriorityContactMutateRequest{ContactAccount: tc.contact})
			requireCode(t, err, errcode.CodeBadRequest)
		})
	}
}

func TestAddPriorityContact_UnknownUserIs404(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)

	users.EXPECT().UserExists(gomock.Any(), "ghost").Return(false, nil)

	_, err := svc.AddPriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "ghost"})
	requireCode(t, err, errcode.CodeNotFound)
	requireReason(t, err, errcode.UserPriorityContactNotFound)
}

func TestAddPriorityContact_UnknownBotIs404(t *testing.T) {
	svc, _, _, apps, _, _, _ := newSvc(t)

	apps.EXPECT().GetAppsByAssistants(gomock.Any(), []string{"gone.bot"}).
		Return(map[string]*model.App{}, nil)

	_, err := svc.AddPriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "gone.bot"})
	requireReason(t, err, errcode.UserPriorityContactNotFound)
}

// A client-only fanout would leave every remote site with a stale list, so both
// fanouts must fire off one timestamp. One mock backs both publishers — they are
// told apart by subject.
func TestAddPriorityContact_PublishesBothFanoutsWithOneTimestamp(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)

	users.EXPECT().UserExists(gomock.Any(), "bob").Return(true, nil)
	users.EXPECT().AddPriorityContact(gomock.Any(), "alice", "bob", 30, gomock.Any()).
		Return(&model.User{Settings: &model.UserSettings{PriorityContacts: []string{"bob"}}}, nil)
	users.EXPECT().GetPriorityContactUsers(gomock.Any(), []string{"bob"}).
		Return(map[string]*models.PriorityContactUser{"bob": {EngName: "Bob"}}, nil)

	var clientTS, inboxTS int64
	var clientContacts []string
	pub.EXPECT().Publish(gomock.Any(), subject.SettingsUpdate("alice"), gomock.Any()).
		DoAndReturn(func(_ any, _ string, data []byte) error {
			var evt model.SettingsUpdateEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			clientTS = evt.Timestamp
			clientContacts = evt.Settings.PriorityContacts
			return nil
		})
	pub.EXPECT().Publish(gomock.Any(), subject.InboxExternal("site-b", model.InboxUserSettingsUpdated), gomock.Any()).
		DoAndReturn(func(_ any, _ string, data []byte) error {
			var evt model.InboxEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			var payload model.UserSettingsUpdated
			require.NoError(t, json.Unmarshal(evt.Payload, &payload))
			inboxTS = payload.Timestamp
			return nil
		})

	resp, err := svc.AddPriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "bob"})
	require.NoError(t, err)
	require.Len(t, resp.Contacts, 1)
	assert.Equal(t, "bob", resp.Contacts[0].Account)

	assert.NotZero(t, clientTS)
	assert.Equal(t, clientTS, inboxTS)
	// The event carries raw accounts; devices refetch to render names.
	assert.Equal(t, []string{"bob"}, clientContacts)
}

func TestAddPriorityContact_AtCapIsForbidden(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)

	full := make([]string, 30)
	for i := range full {
		full[i] = fmt.Sprintf("seed%02d", i)
	}
	users.EXPECT().UserExists(gomock.Any(), "bob").Return(true, nil)
	users.EXPECT().AddPriorityContact(gomock.Any(), "alice", "bob", 30, gomock.Any()).Return(nil, nil)
	users.EXPECT().GetUserPriorityContacts(gomock.Any(), "alice").
		Return(&model.User{Settings: &model.UserSettings{PriorityContacts: full}}, nil)

	_, err := svc.AddPriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "bob"})
	requireCode(t, err, errcode.CodeForbidden)
	requireReason(t, err, errcode.UserPriorityContactLimit)
}

// The write misses because the list was at cap, then a concurrent RemovePriorityContact
// for the same account drops some other contact before the re-read. The re-read then
// sees: caller's doc exists, the added contact is absent, and the list is under cap —
// none of the three disambiguation branches fit, so the handler must report a conflict
// (retry-able) rather than a false 404 for a user that plainly exists.
func TestAddPriorityContact_ConcurrentRemoveDuringMissReturnsConflict(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)

	users.EXPECT().UserExists(gomock.Any(), "bob").Return(true, nil)
	users.EXPECT().AddPriorityContact(gomock.Any(), "alice", "bob", 30, gomock.Any()).Return(nil, nil)
	users.EXPECT().GetUserPriorityContacts(gomock.Any(), "alice").
		Return(&model.User{Settings: &model.UserSettings{
			PriorityContacts: []string{"carol"},
		}}, nil)

	_, err := svc.AddPriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "bob"})
	requireCode(t, err, errcode.CodeConflict)
}

// A duplicate add at exactly the cap is a no-op, not a violation: the cap filter
// rejects the write, but the contact is already present, so it must succeed.
func TestAddPriorityContact_DuplicateAtCapSucceeds(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)

	full := []string{"bob"}
	for i := 1; i < 30; i++ {
		full = append(full, fmt.Sprintf("seed%02d", i))
	}
	users.EXPECT().UserExists(gomock.Any(), "bob").Return(true, nil)
	users.EXPECT().AddPriorityContact(gomock.Any(), "alice", "bob", 30, gomock.Any()).Return(nil, nil)
	users.EXPECT().GetUserPriorityContacts(gomock.Any(), "alice").
		Return(&model.User{Settings: &model.UserSettings{PriorityContacts: full}}, nil)
	users.EXPECT().GetPriorityContactUsers(gomock.Any(), gomock.Any()).
		Return(map[string]*models.PriorityContactUser{}, nil)

	resp, err := svc.AddPriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "bob"})
	require.NoError(t, err)
	assert.Len(t, resp.Contacts, 30)
}

func TestAddPriorityContact_CallerNotFound(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)

	users.EXPECT().UserExists(gomock.Any(), "bob").Return(true, nil)
	users.EXPECT().AddPriorityContact(gomock.Any(), "alice", "bob", 30, gomock.Any()).Return(nil, nil)
	users.EXPECT().GetUserPriorityContacts(gomock.Any(), "alice").Return(nil, nil)

	_, err := svc.AddPriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "bob"})
	requireCode(t, err, errcode.CodeNotFound)
}

func TestAddPriorityContact_RepoError(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)

	users.EXPECT().UserExists(gomock.Any(), "bob").Return(true, nil)
	users.EXPECT().AddPriorityContact(gomock.Any(), "alice", "bob", 30, gomock.Any()).
		Return(nil, errors.New("db down"))

	_, err := svc.AddPriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "bob"})
	require.Error(t, err)
}

func TestRemovePriorityContact_EmptyContactIsBadRequest(t *testing.T) {
	svc, _, _, _, _, _, _ := newSvc(t)

	_, err := svc.RemovePriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: ""})
	requireCode(t, err, errcode.CodeBadRequest)
}

func TestRemovePriorityContact_PublishesBothFanoutsWithOneTimestamp(t *testing.T) {
	svc, _, users, apps, _, _, pub := newSvc(t)

	users.EXPECT().RemovePriorityContact(gomock.Any(), "alice", "bob", gomock.Any()).
		Return(&model.User{Settings: &model.UserSettings{PriorityContacts: []string{"helper.bot"}}}, nil)
	// Enrichment runs on the post-removal list — only the bot remains, so
	// GetPriorityContactUsers is never called (no user accounts left to look up).
	apps.EXPECT().GetAppsByAssistants(gomock.Any(), []string{"helper.bot"}).
		Return(map[string]*model.App{"helper.bot": {Name: "Helper"}}, nil)

	var clientTS, inboxTS int64
	pub.EXPECT().Publish(gomock.Any(), subject.SettingsUpdate("alice"), gomock.Any()).
		DoAndReturn(func(_ any, _ string, data []byte) error {
			var evt model.SettingsUpdateEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			clientTS = evt.Timestamp
			return nil
		})
	pub.EXPECT().Publish(gomock.Any(), subject.InboxExternal("site-b", model.InboxUserSettingsUpdated), gomock.Any()).
		DoAndReturn(func(_ any, _ string, data []byte) error {
			var evt model.InboxEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			var payload model.UserSettingsUpdated
			require.NoError(t, json.Unmarshal(evt.Payload, &payload))
			inboxTS = payload.Timestamp
			return nil
		})

	resp, err := svc.RemovePriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "bob"})
	require.NoError(t, err)
	require.Len(t, resp.Contacts, 1)
	assert.Equal(t, "helper.bot", resp.Contacts[0].Account)

	assert.NotZero(t, clientTS)
	assert.Equal(t, clientTS, inboxTS)
}

// Removing an entry that isn't in the list is a no-op that still succeeds.
func TestRemovePriorityContact_AbsentContactSucceeds(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)

	users.EXPECT().RemovePriorityContact(gomock.Any(), "alice", "ghost", gomock.Any()).
		Return(&model.User{Settings: &model.UserSettings{PriorityContacts: []string{}}}, nil)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	resp, err := svc.RemovePriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "ghost"})
	require.NoError(t, err)
	assert.Empty(t, resp.Contacts)
}

func TestRemovePriorityContact_CallerNotFound(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)

	users.EXPECT().RemovePriorityContact(gomock.Any(), "alice", "bob", gomock.Any()).Return(nil, nil)

	_, err := svc.RemovePriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "bob"})
	requireCode(t, err, errcode.CodeNotFound)
}

func TestRemovePriorityContact_RepoError(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)

	users.EXPECT().RemovePriorityContact(gomock.Any(), "alice", "bob", gomock.Any()).
		Return(nil, errors.New("db down"))

	_, err := svc.RemovePriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "bob"})
	require.Error(t, err)
}

// respondPriorityContacts' nil-settings branch is defensive (a matched write should
// always come back with a settings sub-document), but is reachable given a store that
// returns one anyway, and must not panic.
func TestRemovePriorityContact_NilSettingsRespondsEmpty(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)

	users.EXPECT().RemovePriorityContact(gomock.Any(), "alice", "bob", gomock.Any()).
		Return(&model.User{Settings: nil}, nil)
	// No pub.EXPECT(): gomock's strict mode fails the test if respondPriorityContacts
	// fans out for a nil-settings match — there is nothing to replicate.

	resp, err := svc.RemovePriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "bob"})
	require.NoError(t, err)
	assert.Empty(t, resp.Contacts)
}

// priorityContactExists' bot-lookup error propagates out of AddPriorityContact raw
// (infra failure, not an errcode).
func TestAddPriorityContact_ExistsCheckBotLookupError(t *testing.T) {
	svc, _, _, apps, _, _, _ := newSvc(t)

	apps.EXPECT().GetAppsByAssistants(gomock.Any(), []string{"ghost.bot"}).
		Return(nil, errors.New("db down"))

	_, err := svc.AddPriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "ghost.bot"})
	require.Error(t, err)
	var ee *errcode.Error
	assert.False(t, errors.As(err, &ee))
}

// priorityContactExists' user-lookup error propagates out of AddPriorityContact raw.
func TestAddPriorityContact_ExistsCheckUserLookupError(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)

	users.EXPECT().UserExists(gomock.Any(), "bob").Return(false, errors.New("db down"))

	_, err := svc.AddPriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "bob"})
	require.Error(t, err)
	var ee *errcode.Error
	assert.False(t, errors.As(err, &ee))
}

// The re-read inside resolveAddPriorityContactMiss can itself fail against the store.
func TestAddPriorityContact_ResolveMissRepoError(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)

	users.EXPECT().UserExists(gomock.Any(), "bob").Return(true, nil)
	users.EXPECT().AddPriorityContact(gomock.Any(), "alice", "bob", 30, gomock.Any()).Return(nil, nil)
	users.EXPECT().GetUserPriorityContacts(gomock.Any(), "alice").Return(nil, errors.New("db down"))

	_, err := svc.AddPriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "bob"})
	require.Error(t, err)
	var ee *errcode.Error
	assert.False(t, errors.As(err, &ee))
}
