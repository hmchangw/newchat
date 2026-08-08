package service

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/user-service/models"
)

// requireReason asserts the error carries a specific domain reason, which requireCode
// (code only) cannot express. Consumed by the add/remove handler tests (Task 8/9).
//
//nolint:unused // consumed by Task 8/9
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
