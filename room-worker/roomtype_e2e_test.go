package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
)

type dmOutcome struct {
	storedType model.RoomType
	evtType    model.RoomType
	hasHR      bool
	hasApp     bool
}

// createDMAndCapture drives processCreateRoom and returns, per account, the row
// that was stored and the subscription.update that account received.
func createDMAndCapture(t *testing.T, requesterAcct, counterpartAcct, roomID string, hasApp bool) (map[string]dmOutcome, model.RoomType) {
	t.Helper()
	h, mockStore, getPublished := newCreateRoomTestHandler(t)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	requester := &model.User{ID: "u_" + requesterAcct, Account: requesterAcct, EngName: "R Eng", ChineseName: "請求", SiteID: "site-A"}
	other := &model.User{ID: "u_" + counterpartAcct, Account: counterpartAcct, EngName: "O Eng", ChineseName: "對方", SiteID: "site-A"}

	mockStore.EXPECT().GetUser(gomock.Any(), requesterAcct).Return(requester, nil)
	mockStore.EXPECT().GetUser(gomock.Any(), counterpartAcct).Return(other, nil)

	var roomType model.RoomType
	mockStore.EXPECT().CreateRoom(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, room *model.Room, _ any) (bool, error) {
			roomType = room.Type
			return true, nil
		})
	if hasApp {
		mockStore.EXPECT().GetApp(gomock.Any(), gomock.Any()).
			Return(&model.App{ID: "app1", Name: "Weather", Assistant: &model.AppAssistant{Name: counterpartAcct}}, nil).AnyTimes()
	}

	var captured []*model.Subscription
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, subs []*model.Subscription) error { captured = subs; return nil })
	mockStore.EXPECT().FindDMSubscriptionPair(gomock.Any(), roomID, requesterAcct).
		DoAndReturn(func(_ context.Context, _, _ string) (*model.Subscription, *model.Subscription, error) {
			return captured[0], captured[1], nil
		})
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)

	body := makeCreateRoomBody(t, &model.CreateRoomRequest{
		RoomID: roomID, RequesterAccount: requesterAcct,
		Users: []string{counterpartAcct}, Timestamp: time.Now().UnixMilli(),
	})
	require.NoError(t, h.processCreateRoom(ctx, body))

	out := map[string]dmOutcome{}
	for _, sub := range captured {
		out[sub.User.Account] = dmOutcome{storedType: sub.RoomType}
	}
	for _, p := range subscriptionUpdates(getPublished()) {
		var evt model.SubscriptionUpdateEvent
		require.NoError(t, json.Unmarshal(p.data, &evt))
		acct := evt.Subscription.User.Account
		o := out[acct]
		o.evtType, o.hasHR, o.hasApp = evt.Subscription.RoomType, evt.HRInfo != nil, evt.AppInfo != nil
		out[acct] = o
	}
	return out, roomType
}

func TestE2E_DMScenarios(t *testing.T) {
	tests := []struct {
		name             string
		requester, other string
		roomID           string
		hasApp           bool
		wantRoomType     model.RoomType
		want             map[string]dmOutcome
	}{
		{
			name: "user creates a DM with a bot", requester: "alice", other: "weather.bot",
			roomID: "r1", hasApp: true, wantRoomType: model.RoomTypeBotDM,
			want: map[string]dmOutcome{
				"alice":       {model.RoomTypeBotDM, model.RoomTypeBotDM, false, true},
				"weather.bot": {model.RoomTypeDM, model.RoomTypeDM, true, false},
			},
		},
		{
			name: "bot creates a DM with a user", requester: "weather.bot", other: "alice",
			roomID: "r2", hasApp: true, wantRoomType: model.RoomTypeBotDM,
			want: map[string]dmOutcome{
				"weather.bot": {model.RoomTypeDM, model.RoomTypeDM, true, false},
				"alice":       {model.RoomTypeBotDM, model.RoomTypeBotDM, false, true},
			},
		},
		{
			name: "two humans", requester: "alice", other: "bob",
			roomID: "r3", wantRoomType: model.RoomTypeDM,
			want: map[string]dmOutcome{
				"alice": {model.RoomTypeDM, model.RoomTypeDM, true, false},
				"bob":   {model.RoomTypeDM, model.RoomTypeDM, true, false},
			},
		},
		{
			name: "user and platform admin", requester: "alice", other: "p_adminsiteA",
			roomID: "r4", wantRoomType: model.RoomTypeDM,
			want: map[string]dmOutcome{
				"alice":        {model.RoomTypeDM, model.RoomTypeDM, true, false},
				"p_adminsiteA": {model.RoomTypeDM, model.RoomTypeDM, true, false},
			},
		},
		{
			name: "two bots", requester: "weather.bot", other: "sales.bot",
			roomID: "r5", hasApp: true, wantRoomType: model.RoomTypeBotDM,
			want: map[string]dmOutcome{
				"weather.bot": {model.RoomTypeBotDM, model.RoomTypeBotDM, false, true},
				"sales.bot":   {model.RoomTypeBotDM, model.RoomTypeBotDM, false, true},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, roomType := createDMAndCapture(t, tt.requester, tt.other, tt.roomID, tt.hasApp)
			assert.Equal(t, tt.wantRoomType, roomType, "room document type")
			for acct, want := range tt.want {
				assert.Equal(t, want.storedType, got[acct].storedType, "%s: stored roomType", acct)
				assert.Equal(t, want.evtType, got[acct].evtType, "%s: event roomType", acct)
				assert.Equal(t, want.storedType, got[acct].evtType, "%s: event must equal stored", acct)
				assert.Equal(t, want.hasHR, got[acct].hasHR, "%s: hrInfo", acct)
				assert.Equal(t, want.hasApp, got[acct].hasApp, "%s: appInfo", acct)
			}
		})
	}
}
