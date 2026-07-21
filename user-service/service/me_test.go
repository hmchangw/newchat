package service

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/user-service/config"
	"github.com/hmchangw/chat/user-service/service/mocks"
)

// newMeSvc builds a UserService exposing the user + presence mocks /me drives.
func newMeSvc(t *testing.T) (*UserService, *mocks.MockUserRepository, *mocks.MockPresenceClient) {
	t.Helper()
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserRepository(ctrl)
	presence := mocks.NewMockPresenceClient(ctrl)
	cfg := &config.Config{SiteID: "site-a", AllSiteIDs: []string{"site-a", "site-b"}, MaxAccountNames: 100}
	svc := New(
		mocks.NewMockSubscriptionRepository(ctrl),
		users,
		mocks.NewMockAppRepository(ctrl),
		mocks.NewMockThreadSubscriptionRepository(ctrl),
		mocks.NewMockRoomClient(ctrl),
		mocks.NewMockHistoryClient(ctrl),
		presence,
		mocks.NewMockEventPublisher(ctrl),
		mocks.NewMockEventPublisher(ctrl),
		nil, nil, nil,
		cfg,
	)
	return svc, users, presence
}

func TestUserService_Me(t *testing.T) {
	t.Run("returns status view plus presence", func(t *testing.T) {
		svc, users, presence := newMeSvc(t)
		users.EXPECT().GetUserStatus(gomock.Any(), "alice").Return(&model.User{
			Account: "alice", StatusText: "busy", StatusIsShow: true, ChineseName: "愛麗絲", EngName: "Alice",
		}, nil)
		presence.EXPECT().QueryPresence(gomock.Any(), "site-a", []string{"alice"}).Return(
			[]model.PresenceState{{Account: "alice", SiteID: "site-a", Status: model.StatusOnline}}, nil)

		resp, err := svc.Me(ctx("alice", "site-a"))
		require.NoError(t, err)
		require.Equal(t, "alice", resp.Account)
		require.Equal(t, "busy", resp.StatusText)
		require.True(t, resp.StatusIsShow)
		require.Equal(t, "愛麗絲", resp.ChineseName)
		require.Equal(t, "Alice", resp.EngName)
		require.Equal(t, model.StatusOnline, resp.Presence)
	})

	t.Run("user not found does not query presence", func(t *testing.T) {
		svc, users, _ := newMeSvc(t)
		users.EXPECT().GetUserStatus(gomock.Any(), "ghost").Return(nil, nil)
		// No presence EXPECT — a call would fail the gomock controller.
		_, err := svc.Me(ctx("ghost", "site-a"))
		requireCode(t, err, errcode.CodeNotFound)
	})

	t.Run("store error surfaces as internal", func(t *testing.T) {
		svc, users, _ := newMeSvc(t)
		users.EXPECT().GetUserStatus(gomock.Any(), "alice").Return(nil, errors.New("db down"))
		_, err := svc.Me(ctx("alice", "site-a"))
		requireCode(t, err, errcode.CodeInternal)
	})

	t.Run("presence rpc failure degrades to offline", func(t *testing.T) {
		svc, users, presence := newMeSvc(t)
		users.EXPECT().GetUserStatus(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
		presence.EXPECT().QueryPresence(gomock.Any(), "site-a", []string{"alice"}).Return(nil, errors.New("presence down"))
		resp, err := svc.Me(ctx("alice", "site-a"))
		require.NoError(t, err)
		require.Equal(t, model.StatusOffline, resp.Presence)
	})

	t.Run("presence none defaults to offline", func(t *testing.T) {
		svc, users, presence := newMeSvc(t)
		users.EXPECT().GetUserStatus(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
		presence.EXPECT().QueryPresence(gomock.Any(), "site-a", []string{"alice"}).Return(
			[]model.PresenceState{{Account: "alice", Status: model.StatusNone}}, nil)
		resp, err := svc.Me(ctx("alice", "site-a"))
		require.NoError(t, err)
		require.Equal(t, model.StatusOffline, resp.Presence)
	})

	t.Run("presence missing account defaults to offline", func(t *testing.T) {
		svc, users, presence := newMeSvc(t)
		users.EXPECT().GetUserStatus(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
		presence.EXPECT().QueryPresence(gomock.Any(), "site-a", []string{"alice"}).Return(
			[]model.PresenceState{}, nil)
		resp, err := svc.Me(ctx("alice", "site-a"))
		require.NoError(t, err)
		require.Equal(t, model.StatusOffline, resp.Presence)
	})
}
