package service

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
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
	cfg := &config.Config{SiteID: "site-a", AllSiteIDs: []string{"site-a", "site-b"}, MaxSubscriptionLimit: 1000, DefaultSubscriptionLimit: 40, MaxAppsLimit: 100, DefaultAppsLimit: 20, MaxAccountNames: 100}
	threadSubs := mocks.NewMockThreadSubscriptionRepository(ctrl)
	// ListSubscriptions now enriches last-message via history.RoomsGet; default it to a
	// no-op so list tests that don't exercise last-message need no per-test stub.
	history.EXPECT().RoomsGet(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	// countUnread's thread phase reads thread-subs; default to none so room-count
	// tests that don't exercise threads need no per-test stub.
	threadSubs.EXPECT().ListByAccount(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	return New(subs, users, apps, threadSubs, rooms, history, presence, pub, cfg), subs, users, apps, rooms, history, pub
}

// ctx builds a handler context. siteID is retained for readability but unused
// by handlers — site isolation is structural at the subject level.
func ctx(account, siteID string) *natsrouter.Context {
	return natsrouter.NewContext(map[string]string{"account": account, "siteID": siteID})
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
