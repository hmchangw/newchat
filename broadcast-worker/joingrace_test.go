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
	"github.com/hmchangw/chat/pkg/subject"
)

// A member who has just joined a channel cannot yet be relied on to hold
// chat.room.{id}.event: their subscribe causally follows the subscription.update
// that told them the room exists, and NATS exposes no signal for when a
// subscription is live cluster-wide. Inside the grace window the event is
// therefore also published to each fresh member's own user subject, which they
// have held since connect. The roster comes from room-worker's join notices, so
// a channel message stays one publish plus one map lookup.
func TestPublishChannelEvent_JoinGraceWindow(t *testing.T) {
	msgTime := time.Date(2026, 3, 26, 9, 0, 0, 0, time.UTC)

	tests := []struct {
		name         string
		grace        time.Duration
		notices      []model.JoinGraceEvent
		wantSubjects []string
	}{
		{
			name:         "grace disabled publishes only to the room subject",
			grace:        0,
			notices:      []model.JoinGraceEvent{{RoomID: "room-1", Accounts: []string{"alice"}, JoinedAt: msgTime.UnixMilli()}},
			wantSubjects: []string{subject.RoomEvent("room-1", true)},
		},
		{
			name:         "no notice for this room, no extra publish",
			grace:        30 * time.Second,
			notices:      []model.JoinGraceEvent{{RoomID: "other-room", Accounts: []string{"alice"}, JoinedAt: msgTime.UnixMilli()}},
			wantSubjects: []string{subject.RoomEvent("room-1", true)},
		},
		{
			name:  "fresh member also gets a copy on their user subject",
			grace: 30 * time.Second,
			notices: []model.JoinGraceEvent{
				{RoomID: "room-1", Accounts: []string{"alice"}, JoinedAt: msgTime.Add(-2 * time.Second).UnixMilli()},
			},
			wantSubjects: []string{
				subject.RoomEvent("room-1", true),
				subject.UserRoomEvent("alice"),
			},
		},
		{
			name:  "every fresh member gets a copy",
			grace: 30 * time.Second,
			notices: []model.JoinGraceEvent{
				{RoomID: "room-1", Accounts: []string{"alice", "bob"}, JoinedAt: msgTime.Add(-time.Second).UnixMilli()},
			},
			wantSubjects: []string{
				subject.RoomEvent("room-1", true),
				subject.UserRoomEvent("alice"),
				subject.UserRoomEvent("bob"),
			},
		},
		{
			name:  "a join older than the window is gone",
			grace: 30 * time.Second,
			notices: []model.JoinGraceEvent{
				{RoomID: "room-1", Accounts: []string{"alice"}, JoinedAt: msgTime.Add(-time.Hour).UnixMilli()},
			},
			wantSubjects: []string{subject.RoomEvent("room-1", true)},
		},
		{
			name:  "bots are skipped - they consume messages server-side",
			grace: 30 * time.Second,
			notices: []model.JoinGraceEvent{
				{RoomID: "room-1", Accounts: []string{"alice", "weather.site-a.bot"}, JoinedAt: msgTime.UnixMilli()},
			},
			wantSubjects: []string{
				subject.RoomEvent("room-1", true),
				subject.UserRoomEvent("alice"),
			},
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

			h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal,
				withJoinGrace(tc.grace), withClock(func() time.Time { return msgTime }))
			for _, n := range tc.notices {
				data, err := json.Marshal(n)
				require.NoError(t, err)
				h.HandleJoinGraceNotice(context.Background(), data)
			}

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

// A malformed notice must not take the worker down or poison the registry.
func TestHandleJoinGraceNotice_Malformed(t *testing.T) {
	msgTime := time.Date(2026, 3, 26, 9, 0, 0, 0, time.UTC)
	ctrl := gomock.NewController(t)
	h := NewHandler(NewMockStore(ctrl), NewMockUserStore(ctrl), &mockPublisher{}, NewMockRoomKeyProvider(ctrl),
		defaultParentFetcher, false, subject.RouteGlobal,
		withJoinGrace(30*time.Second), withClock(func() time.Time { return msgTime }))

	h.HandleJoinGraceNotice(context.Background(), []byte("{not json"))
	assert.Zero(t, h.joins.Len())
}
