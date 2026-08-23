package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// A member who has just joined a channel has not had time to subscribe to
// chat.room.{id}.event, and NATS offers no way to know when that subscription
// is live. Within the grace window the event is therefore also published to
// each fresh member's own user subject, which they have held since login.
func TestPublishChannelEvent_JoinGraceWindow(t *testing.T) {
	msgTime := time.Date(2026, 3, 26, 9, 0, 0, 0, time.UTC)

	tests := []struct {
		name         string
		grace        time.Duration
		joinedAt     map[string]time.Time
		wantSubjects []string
	}{
		{
			name:         "grace disabled publishes only to the room subject",
			grace:        0,
			wantSubjects: []string{subject.RoomEvent("room-1", true)},
		},
		{
			name:  "fresh member also gets a copy on their user subject",
			grace: 30 * time.Second,
			joinedAt: map[string]time.Time{
				"alice": msgTime.Add(-2 * time.Second),
				"bob":   msgTime.Add(-time.Hour),
			},
			wantSubjects: []string{
				subject.RoomEvent("room-1", true),
				subject.UserRoomEvent("alice"),
			},
		},
		{
			name:  "every member fresh, every member gets a copy",
			grace: 30 * time.Second,
			joinedAt: map[string]time.Time{
				"alice": msgTime.Add(-time.Second),
				"bob":   msgTime.Add(-3 * time.Second),
			},
			wantSubjects: []string{
				subject.RoomEvent("room-1", true),
				subject.UserRoomEvent("alice"),
				subject.UserRoomEvent("bob"),
			},
		},
		{
			name:  "no member fresh, no extra publish",
			grace: 30 * time.Second,
			joinedAt: map[string]time.Time{
				"alice": msgTime.Add(-time.Hour),
				"bob":   msgTime.Add(-time.Hour),
			},
			wantSubjects: []string{subject.RoomEvent("room-1", true)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockStore(ctrl)
			us := NewMockUserStore(ctrl)
			keyStore := NewMockRoomKeyProvider(ctrl)
			pub := &mockPublisher{}

			store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", msgTime, false).Return(nil)
			store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "room-1", "sender", msgTime).Return(nil)
			store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
			us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return(nil, nil)
			if tc.grace > 0 {
				store.EXPECT().ListSubscriptions(gomock.Any(), "room-1").Return(subsJoinedAt(tc.joinedAt), nil)
			}

			h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal,
				withJoinGrace(tc.grace), withClock(func() time.Time { return msgTime }))
			require.NoError(t, h.HandleMessage(context.Background(), makeMessageEvent("room-1", "hello", msgTime)))

			var got []string
			for _, r := range pub.records {
				got = append(got, r.subject)
			}
			assert.ElementsMatch(t, tc.wantSubjects, got)

			for _, r := range pub.records {
				var evt model.RoomEvent
				require.NoError(t, json.Unmarshal(r.data, &evt))
				assert.Equal(t, model.RoomEventNewMessage, evt.Type)
				assert.Equal(t, "msg-1", evt.LastMsgID)
			}
		})
	}
}

// A failed roster lookup must not fail the handler: the room-subject publish
// already happened, and a JetStream redelivery would duplicate it.
func TestPublishChannelEvent_JoinGrace_ListSubscriptionsError(t *testing.T) {
	msgTime := time.Date(2026, 3, 26, 9, 0, 0, 0, time.UTC)
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	keyStore := NewMockRoomKeyProvider(ctrl)
	pub := &mockPublisher{}

	store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", msgTime, false).Return(nil)
	store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "room-1", "sender", msgTime).Return(nil)
	store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return(nil, nil)
	store.EXPECT().ListSubscriptions(gomock.Any(), "room-1").Return(nil, errors.New("db down"))

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal,
		withJoinGrace(30*time.Second), withClock(func() time.Time { return msgTime }))
	require.NoError(t, h.HandleMessage(context.Background(), makeMessageEvent("room-1", "hello", msgTime)))

	require.Len(t, pub.records, 1)
	assert.Equal(t, subject.RoomEvent("room-1", true), pub.records[0].subject)
}

// Bots receive messages through their own integration, never the websocket
// event channel — the same rule publishDMEvents follows.
func TestPublishChannelEvent_JoinGrace_SkipsBots(t *testing.T) {
	msgTime := time.Date(2026, 3, 26, 9, 0, 0, 0, time.UTC)
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	keyStore := NewMockRoomKeyProvider(ctrl)
	pub := &mockPublisher{}

	store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", msgTime, false).Return(nil)
	store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "room-1", "sender", msgTime).Return(nil)
	store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return(nil, nil)
	store.EXPECT().ListSubscriptions(gomock.Any(), "room-1").Return([]model.Subscription{
		{User: model.SubscriptionUser{Account: "alice"}, JoinedAt: msgTime.Add(-time.Second)},
		{User: model.SubscriptionUser{Account: "weather.bot", IsBot: true}, JoinedAt: msgTime.Add(-time.Second)},
	}, nil)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal,
		withJoinGrace(30*time.Second), withClock(func() time.Time { return msgTime }))
	require.NoError(t, h.HandleMessage(context.Background(), makeMessageEvent("room-1", "hello", msgTime)))

	var got []string
	for _, r := range pub.records {
		got = append(got, r.subject)
	}
	assert.ElementsMatch(t, []string{subject.RoomEvent("room-1", true), subject.UserRoomEvent("alice")}, got)
}

func subsJoinedAt(joined map[string]time.Time) []model.Subscription {
	out := make([]model.Subscription, 0, len(joined))
	for account, at := range joined {
		out = append(out, model.Subscription{
			User:     model.SubscriptionUser{Account: account},
			RoomID:   "room-1",
			JoinedAt: at,
		})
	}
	return out
}
