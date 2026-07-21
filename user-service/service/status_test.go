package service

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/user-service/config"
	"github.com/hmchangw/chat/user-service/models"
	"github.com/hmchangw/chat/user-service/service/mocks"
)

func TestGetStatusByName(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	users.EXPECT().GetUserStatus(gomock.Any(), "bob").
		Return(&model.User{Account: "bob", StatusText: "hi", StatusIsShow: true, EngName: "Bob", ChineseName: "鮑勃"}, nil)
	resp, err := svc.GetStatusByName(ctx("alice", "site-a"), models.StatusGetByNameRequest{Name: "bob"})
	require.NoError(t, err)
	assert.Equal(t, "bob", resp.Account)
	assert.Equal(t, "鮑勃", resp.ChineseName)
	assert.True(t, resp.StatusIsShow)
}

func TestGetStatusByName_NotFound(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	users.EXPECT().GetUserStatus(gomock.Any(), "ghost").Return(nil, nil)
	_, err := svc.GetStatusByName(ctx("alice", "site-a"), models.StatusGetByNameRequest{Name: "ghost"})
	requireCode(t, err, errcode.CodeNotFound)
}

// profile.getByName is an intentional twin of status.getByName (same request,
// same response fields, same users-collection query) — divergence is a bug.
func TestGetProfileByName_IdenticalToStatus(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	users.EXPECT().GetUserStatus(gomock.Any(), "bob").
		Return(&model.User{Account: "bob", StatusText: "hi", StatusIsShow: true, EngName: "Bob", ChineseName: "鮑勃"}, nil).
		Times(2)
	status, err := svc.GetStatusByName(ctx("alice", "site-a"), models.StatusGetByNameRequest{Name: "bob"})
	require.NoError(t, err)
	profile, err := svc.GetProfileByName(ctx("alice", "site-a"), models.StatusGetByNameRequest{Name: "bob"})
	require.NoError(t, err)
	assert.Equal(t, status, profile, "profile.getByName must return exactly what status.getByName returns")
}

func TestGetProfileByName_NotFound(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	users.EXPECT().GetUserStatus(gomock.Any(), "ghost").Return(nil, nil)
	_, err := svc.GetProfileByName(ctx("alice", "site-a"), models.StatusGetByNameRequest{Name: "ghost"})
	requireCode(t, err, errcode.CodeNotFound)
}

func TestGetProfileByName_StoreError(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	users.EXPECT().GetUserStatus(gomock.Any(), "bob").Return(nil, errors.New("db unavailable"))
	_, err := svc.GetProfileByName(ctx("alice", "site-a"), models.StatusGetByNameRequest{Name: "bob"})
	requireCode(t, err, errcode.CodeInternal)
}

func TestSetStatus_TooLong(t *testing.T) {
	svc, _, _, _, _, _, _ := newSvc(t)
	_, err := svc.SetStatus(ctx("alice", "site-a"), models.StatusSetRequest{Text: string(make([]byte, 513))})
	requireCode(t, err, errcode.CodeBadRequest)
}

func TestSetStatus_InboxExcludesSelf(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)
	users.EXPECT().SetUserStatus(gomock.Any(), "alice", "busy", gomock.Any()).Return(&model.User{Account: "alice", StatusText: "busy"}, nil)
	pub.EXPECT().Publish(gomock.Any(), subject.InboxExternal("site-b", model.InboxUserStatusUpdated), gomock.Any()).Return(nil)
	_, err := svc.SetStatus(ctx("alice", "site-a"), models.StatusSetRequest{Text: "busy"})
	require.NoError(t, err)
}

func TestGetStatusByName_StoreError(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	users.EXPECT().GetUserStatus(gomock.Any(), "bob").Return(nil, errors.New("db unavailable"))
	_, err := svc.GetStatusByName(ctx("alice", "site-a"), models.StatusGetByNameRequest{Name: "bob"})
	requireCode(t, err, errcode.CodeInternal)
}

func TestSetStatus_StoreError(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	users.EXPECT().SetUserStatus(gomock.Any(), "alice", "away", gomock.Any()).Return(nil, errors.New("write failed"))
	_, err := svc.SetStatus(ctx("alice", "site-a"), models.StatusSetRequest{Text: "away"})
	requireCode(t, err, errcode.CodeInternal)
}

func TestSetStatus_PublishError_StillSucceeds(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)
	users.EXPECT().SetUserStatus(gomock.Any(), "alice", "here", gomock.Any()).Return(&model.User{Account: "alice", StatusText: "here"}, nil)
	pub.EXPECT().Publish(gomock.Any(), subject.InboxExternal("site-b", model.InboxUserStatusUpdated), gomock.Any()).
		Return(errors.New("no responders"))
	_, err := svc.SetStatus(ctx("alice", "site-a"), models.StatusSetRequest{Text: "here"})
	require.NoError(t, err)
}

func TestSetStatus_UnknownAccount_NotFound_NoPublish(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	// nil user ⇒ NotFound; pub has no Publish expectation so gomock fails if Publish is called.
	users.EXPECT().SetUserStatus(gomock.Any(), "ghost", "busy", gomock.Any()).Return(nil, nil)
	_, err := svc.SetStatus(ctx("ghost", "site-a"), models.StatusSetRequest{Text: "busy"})
	requireCode(t, err, errcode.CodeNotFound)
}

func TestGetStatusByName_EmptyName_BadRequest_NoLookup(t *testing.T) {
	// An empty name is rejected by a handler-level guard BEFORE any store lookup:
	// users has no GetUserStatus expectation, so gomock fails if it is called.
	svc, _, _, _, _, _, _ := newSvc(t)
	_, err := svc.GetStatusByName(ctx("alice", "site-a"), models.StatusGetByNameRequest{Name: ""})
	requireCode(t, err, errcode.CodeBadRequest)
}

func TestGetProfileByName_EmptyName_BadRequest_NoLookup(t *testing.T) {
	// profile.getByName shares the guard: empty name ⇒ bad_request, no store lookup.
	svc, _, _, _, _, _, _ := newSvc(t)
	_, err := svc.GetProfileByName(ctx("alice", "site-a"), models.StatusGetByNameRequest{Name: ""})
	requireCode(t, err, errcode.CodeBadRequest)
}

func TestSetStatus_NilIsShow_TextOnly(t *testing.T) {
	// Text-only update: IsShow is nil, so the store receives a nil *bool
	// (leave-flag-untouched semantics) and the write still publishes cross-site.
	svc, _, users, _, _, _, pub := newSvc(t)
	users.EXPECT().SetUserStatus(gomock.Any(), "alice", "brb", gomock.Nil()).Return(&model.User{Account: "alice", StatusText: "brb"}, nil)
	pub.EXPECT().Publish(gomock.Any(), subject.InboxExternal("site-b", model.InboxUserStatusUpdated), gomock.Any()).Return(nil)
	resp, err := svc.SetStatus(ctx("alice", "site-a"), models.StatusSetRequest{Text: "brb"})
	require.NoError(t, err)
	assert.Equal(t, "brb", resp.StatusText)
}

func TestSetStatus_AtLimit(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)
	text512 := string(make([]byte, 512))
	users.EXPECT().SetUserStatus(gomock.Any(), "alice", text512, gomock.Any()).Return(&model.User{Account: "alice", StatusText: text512}, nil)
	pub.EXPECT().Publish(gomock.Any(), subject.InboxExternal("site-b", model.InboxUserStatusUpdated), gomock.Any()).Return(nil)
	_, err := svc.SetStatus(ctx("alice", "site-a"), models.StatusSetRequest{Text: text512})
	require.NoError(t, err)
}

func TestPublishStatus_SkipsEmptyDest(t *testing.T) {
	ctrl := gomock.NewController(t)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	users := mocks.NewMockUserRepository(ctrl)
	apps := mocks.NewMockAppRepository(ctrl)
	rooms := mocks.NewMockRoomClient(ctrl)
	history := mocks.NewMockHistoryClient(ctrl)
	presence := mocks.NewMockPresenceClient(ctrl)
	pub := mocks.NewMockEventPublisher(ctrl)
	cfg := &config.Config{SiteID: "site-a", AllSiteIDs: []string{"site-a", "", "site-b"}, MaxSubscriptionLimit: 1000}
	threadSubs := mocks.NewMockThreadSubscriptionRepository(ctrl)
	svc := New(subs, users, apps, threadSubs, rooms, history, presence, pub, pub, nil, nil, nil, cfg)
	// Only "site-b" must receive a publish; self "site-a" and the blank "" are skipped.
	pub.EXPECT().Publish(gomock.Any(), subject.InboxExternal("site-b", model.InboxUserStatusUpdated), gomock.Any()).Return(nil)
	svc.publishStatus(ctx("alice", "site-a"), "alice", "busy", nil)
}
