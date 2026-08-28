package service

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/user-service/models"
	"github.com/hmchangw/chat/user-service/service/mocks"
)

// The client fan-out of a botDM subscription.update is best-effort: a publish
// failure is swallowed after the write has already landed, so the RPC still
// reports success on both the removed and the added path.
func TestSetAppSubscription_PublishFailureIsBestEffort(t *testing.T) {
	tests := []struct {
		name       string
		subscribed bool
		setup      func(subs *mocks.MockSubscriptionRepository, rooms *mocks.MockRoomClient)
	}{
		{
			name:       "unsubscribe removed event",
			subscribed: false,
			setup: func(subs *mocks.MockSubscriptionRepository, _ *mocks.MockRoomClient) {
				subs.EXPECT().GetAppSubscription(gomock.Any(), "alice", "helper.bot").Return(appSub(false), nil)
				subs.EXPECT().SetAppSubscribed(gomock.Any(), "alice", "helper.bot", false, true).Return(nil)
			},
		},
		{
			name:       "reactivate added event",
			subscribed: true,
			setup: func(subs *mocks.MockSubscriptionRepository, rooms *mocks.MockRoomClient) {
				subs.EXPECT().GetAppSubscription(gomock.Any(), "alice", "helper.bot").Return(appSub(true), nil)
				subs.EXPECT().SetAppSubscribed(gomock.Any(), "alice", "helper.bot", true, false).Return(nil)
				rooms.EXPECT().GetRoomsMeta(gomock.Any(), gomock.Any(), []string{"room1"}).
					Return([]model.RoomInfo{{RoomID: "room1", Found: true, SiteID: "site-a", Name: "Test Bot"}}, nil)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, subs, _, apps, rooms, _, pub := newSvc(t)
			apps.EXPECT().GetApp(gomock.Any(), "app1").Return(appWith(true), nil)
			tt.setup(subs, rooms)
			pub.EXPECT().Publish(gomock.Any(), subject.SubscriptionUpdate("alice"), gomock.Any()).
				Return(errors.New("nats down"))

			resp, err := svc.SetAppSubscription(ctx("alice", "site-a"), models.SetAppSubscriptionRequest{AppID: "app1", Subscribed: tt.subscribed})
			require.NoError(t, err, "a failed client fan-out must not fail the RPC — the write already landed")
			require.NotNil(t, resp)
			assert.True(t, resp.Success)
		})
	}
}
