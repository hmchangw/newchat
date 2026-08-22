package service

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/user-service/config"
	"github.com/hmchangw/chat/user-service/service/mocks"
)

func newSvc(t *testing.T) (*UserService, *mocks.MockSubscriptionRepository, *mocks.MockUserRepository, *mocks.MockAppRepository, *mocks.MockRoomClient, *mocks.MockHistoryClient, *mocks.MockEventPublisher) {
	t.Helper()
	ctrl := gomock.NewController(t)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	users := mocks.NewMockUserRepository(ctrl)
	apps := mocks.NewMockAppRepository(ctrl)
	rooms := mocks.NewMockRoomClient(ctrl)
	history := mocks.NewMockHistoryClient(ctrl)
	presence := mocks.NewMockPresenceClient(ctrl)
	pub := mocks.NewMockEventPublisher(ctrl)
	cfg := &config.Config{SiteID: "site-a", AllSiteIDs: []string{"site-a", "site-b"}, MaxSubscriptionLimit: 1000, DefaultSubscriptionLimit: 40, MaxAppsLimit: 100, DefaultAppsLimit: 20, MaxAccountNames: 100, SSORefreshWindow: time.Hour, BadgeCountCap: 10, RoomBatchChunk: 100}
	threadSubs := mocks.NewMockThreadSubscriptionRepository(ctrl)
	ssoTokens := mocks.NewMockSSOTokenRepository(ctrl)
	validator := mocks.NewMockTokenValidator(ctrl)
	refresher := mocks.NewMockTokenRefresher(ctrl)
	// ListSubscriptions now enriches last-message via history.RoomsGet; default it to a
	// no-op so list tests that don't exercise last-message need no per-test stub.
	history.EXPECT().RoomsGet(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	// The same mock backs both publishers (federation + client fanout) —
	// expectations are subject-scoped, so tests stay unambiguous.
	return New(subs, users, apps, threadSubs, rooms, history, presence, pub, pub, &fakeBadgeCache{}, ssoTokens, validator, refresher, cfg), subs, users, apps, rooms, history, pub
}

// ctx builds a handler context. siteID is retained for readability but unused
// by handlers — site isolation is structural at the subject level.
func ctx(account, siteID string) *natsrouter.Context {
	return natsrouter.NewContext(map[string]string{"account": account, "siteID": siteID})
}

// localUnreadSub returns an EnrichedSubscription for a room on siteID with an
// unread baseline: LastMsgAt is newer than LastSeenAt and already populated by
// the $lookup, so the caller's own-site rows count with no cross-site RPC.
// account is accepted for call-site readability (it mirrors the account the
// surrounding GetActiveSubscriptions mock is scoped to) though the fixture
// itself carries no per-account field.
func localUnreadSub(account, roomID, siteID string) model.EnrichedSubscription {
	_ = account
	seen := time.UnixMilli(100).UTC()
	newer := time.UnixMilli(200).UTC()
	return model.EnrichedSubscription{
		Subscription: model.Subscription{RoomID: roomID, SiteID: siteID, LastSeenAt: &seen},
		LastMsgAt:    &newer,
	}
}

// crossSiteSub returns an EnrichedSubscription for a room on a remote siteID
// with no LastMsgAt baseline — unlike localUnreadSub, read state is unknown
// until unreadRooms RPCs that site's RoomClient.GetRoomsMeta.
func crossSiteSub(roomID, siteID string) model.EnrichedSubscription {
	seen := time.UnixMilli(100).UTC()
	return model.EnrichedSubscription{
		Subscription: model.Subscription{RoomID: roomID, SiteID: siteID, LastSeenAt: &seen},
	}
}

// failingRoomClient stubs rooms.GetRoomsMeta for siteID to return an error,
// as if that remote site were unreachable — the cross-site fan-out must skip
// it rather than fail the whole count.
func failingRoomClient(rooms *mocks.MockRoomClient, siteID string) {
	rooms.EXPECT().GetRoomsMeta(gomock.Any(), siteID, gomock.Any()).Return(nil, errors.New("down"))
}

func requireCode(t *testing.T, err error, code errcode.Code) {
	t.Helper()
	require.Error(t, err)
	var ee *errcode.Error
	if errors.As(err, &ee) {
		assert.Equal(t, code, ee.Code)
		return
	}
	// Raw wrapped errors (no *errcode.Error in chain) classify to CodeInternal.
	assert.Equal(t, errcode.CodeInternal, code, "raw error %T classifies to CodeInternal, not %q", err, code)
}
