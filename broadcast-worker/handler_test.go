package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/roomcrypto"
	"github.com/hmchangw/chat/pkg/roommetacache"
	"github.com/hmchangw/chat/pkg/subject"
)

// ptrBool returns a pointer to b, for constructing tri-state *bool fields
// (e.g. model.Room.CrossSite) in test literals.
func ptrBool(b bool) *bool { return &b }

type publishRecord struct {
	subject string
	data    []byte
}

type mockPublisher struct {
	mu      sync.Mutex
	records []publishRecord
	// failOn maps a subject to the error Publish should return for it; the
	// attempt is still recorded so tests can assert every target was tried.
	failOn map[string]error
}

func (m *mockPublisher) Publish(_ context.Context, subj string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, publishRecord{subject: subj, data: data})
	return m.failOn[subj]
}

// stubParentFetcher is a ParentFetcher test double: FetchParent returns a fixed
// parent (or error) regardless of arguments. The zero value returns an empty
// ParentMessageInfo — fine for tests that exercise only the follower/sender
// fan-out and never the history-visibility gate or parent-sender inclusion.
type stubParentFetcher struct {
	info *ParentMessageInfo
	err  error
}

func (s stubParentFetcher) FetchParent(context.Context, string, string, string, string) (*ParentMessageInfo, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.info != nil {
		return s.info, nil
	}
	return &ParentMessageInfo{}, nil
}

// defaultParentFetcher is the no-op fetcher passed to handlers whose test does
// not traverse the channel thread fan-out path (or does but ignores the parent's
// author/createdAt).
var defaultParentFetcher = stubParentFetcher{}

func decodeRoomEvent(t *testing.T, data []byte) model.RoomEvent {
	t.Helper()
	var e model.RoomEvent
	require.NoError(t, json.Unmarshal(data, &e))
	return e
}

var (
	testChannelRoom = &model.Room{
		ID: "room-1", Name: "general", Type: model.RoomTypeChannel,
		SiteID: "site-a", UserCount: 5,
	}
	testDMRoom = &model.Room{
		ID: "dm-1", Name: "", Type: model.RoomTypeDM,
		SiteID: "site-a", UserCount: 2,
	}
	testDMSubs = []model.Subscription{
		{User: model.SubscriptionUser{ID: "alice-id", Account: "alice"}, RoomID: "dm-1"},
		{User: model.SubscriptionUser{ID: "bob-id", Account: "bob"}, RoomID: "dm-1"},
	}
	testUsers = []model.User{
		{ID: "u-alice", Account: "alice", EngName: "Alice Wang", ChineseName: "愛麗絲", EmployeeID: "E001", SiteID: "site-a"},
		{ID: "u-bob", Account: "bob", EngName: "Bob Chen", ChineseName: "鮑勃", EmployeeID: "E002", SiteID: "site-a"},
	}
)

func metaOf(r *model.Room) roommetacache.Meta {
	return roommetacache.Meta{
		ID:        r.ID,
		Type:      r.Type,
		Name:      r.Name,
		SiteID:    r.SiteID,
		UserCount: r.UserCount,
	}
}

func makeMessageEvent(roomID, content string, msgTime time.Time) []byte {
	evt := model.MessageEvent{
		Event:     model.EventCreated,
		SiteID:    "site-a",
		Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "msg-1", RoomID: roomID, UserID: "user-1", UserAccount: "sender",
			Content: content, CreatedAt: msgTime,
		},
	}
	data, _ := json.Marshal(evt)
	return data
}

func TestHandleMessage_DispatchesByEvent(t *testing.T) {
	msgTime := time.Date(2026, 3, 26, 9, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		event       model.EventType
		wantErr     bool
		wantErrText string
	}{
		{
			name:    "created event",
			event:   model.EventCreated,
			wantErr: false,
		},
		{
			name:        "updated event without timestamps fails missing-timestamp guard",
			event:       model.EventUpdated,
			wantErr:     true,
			wantErrText: "missing EditedAt",
		},
		{
			name:        "deleted event without timestamp fails missing-timestamp guard",
			event:       model.EventDeleted,
			wantErr:     true,
			wantErrText: "missing UpdatedAt",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockStore(ctrl)
			us := NewMockUserStore(ctrl)
			pub := &mockPublisher{}
			keyStore := NewMockRoomKeyProvider(ctrl)

			evt := model.MessageEvent{
				Event:  tc.event,
				SiteID: "site-a",
				Message: model.Message{
					ID: "msg-1", RoomID: "room-1", UserID: "user-1", UserAccount: "sender",
					Content: "hello", CreatedAt: msgTime,
				},
			}
			data, err := json.Marshal(evt)
			require.NoError(t, err)

			if !tc.wantErr {
				// Created path: expect the full created-flow mock calls.
				key := testRoomKey(t)
				keyStore.EXPECT().Get(gomock.Any(), "room-1").Return(key, nil)
				store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", msgTime, false).Return(nil)
				store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "room-1", "sender", msgTime).Return(nil)
				store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
				us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return(nil, nil)
			}

			h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
			err = h.HandleMessage(context.Background(), data)

			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrText)
				assert.Empty(t, pub.records, "stub handlers must not publish")
				return
			}

			require.NoError(t, err)
			require.Len(t, pub.records, 1)
			gotEvt := model.RoomEvent{}
			require.NoError(t, json.Unmarshal(pub.records[0].data, &gotEvt))
			assert.Equal(t, model.RoomEventNewMessage, gotEvt.Type)
		})
	}
}

func TestHandler_HandleMessage_ChannelRoom(t *testing.T) {
	msgTime := time.Date(2026, 3, 26, 10, 0, 0, 0, time.UTC)
	senderUser := model.User{ID: "u-sender", Account: "sender", EngName: "Sender Lin", ChineseName: "寄件者", SiteID: "site-a"}

	tests := []struct {
		name            string
		content         string
		wantMentionAll  bool
		wantMentions    []string // expected accounts in evt.Mentions (includes "all" if present)
		wantSetMentions []string // accounts for SetSubscriptionMentions (nil = not called)
	}{
		{
			name:           "no mentions",
			content:        "hello group",
			wantMentionAll: false,
		},
		{
			name:            "individual mentions",
			content:         "hey @alice and @bob",
			wantMentions:    []string{"alice", "bob"},
			wantSetMentions: []string{"alice", "bob"},
		},
		{
			name:           "mention all case insensitive",
			content:        "attention @all",
			wantMentionAll: true,
			wantMentions:   []string{"all"},
		},
		{
			name:            "mention all and individual",
			content:         "@All and @alice",
			wantMentionAll:  true,
			wantMentions:    []string{"alice", "all"},
			wantSetMentions: []string{"alice"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockStore(ctrl)
			us := NewMockUserStore(ctrl)
			pub := &mockPublisher{}

			key := testRoomKey(t)
			keyStore := NewMockRoomKeyProvider(ctrl)
			keyStore.EXPECT().Get(gomock.Any(), "room-1").Return(key, nil)

			store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", msgTime, tc.wantMentionAll).Return(nil)
			store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "room-1", "sender", msgTime).Return(nil)
			store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)

			if tc.wantSetMentions != nil {
				store.EXPECT().SetSubscriptionMentions(gomock.Any(), "room-1", gomock.InAnyOrder(tc.wantSetMentions), msgTime).Return(nil)
			}

			// Single user lookup: sender + mentions, deduped, sender first.
			switch tc.name {
			case "individual mentions":
				us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender", "alice", "bob"}).
					Return([]model.User{senderUser, testUsers[0], testUsers[1]}, nil)
			case "mention all and individual":
				us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender", "alice"}).
					Return([]model.User{senderUser, testUsers[0]}, nil)
			default:
				us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).
					Return([]model.User{senderUser}, nil)
			}

			h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
			err := h.HandleMessage(context.Background(), makeMessageEvent("room-1", tc.content, msgTime))
			require.NoError(t, err)

			require.Len(t, pub.records, 1)
			assert.Equal(t, subject.RoomEvent("room-1", true), pub.records[0].subject)

			evt, msg := decryptClientMessage(t, pub.records[0].data, key)
			assert.Equal(t, model.RoomEventNewMessage, evt.Type)
			assert.Equal(t, "room-1", evt.RoomID)
			assert.Equal(t, "general", evt.RoomName)
			assert.Equal(t, "site-a", evt.SiteID)
			assert.Equal(t, 5, evt.UserCount)
			assert.Equal(t, "msg-1", evt.LastMsgID)
			assert.Positive(t, evt.Timestamp, "Timestamp must be the broadcast-worker publish time")
			assert.Equal(t, msgTime.UnixMilli(), evt.EventTimestamp)
			assert.Equal(t, tc.wantMentionAll, evt.MentionAll)

			assert.Equal(t, "msg-1", msg.ID)
			require.NotNil(t, msg.Sender)
			assert.Equal(t, "user-1", msg.Sender.UserID)
			assert.Equal(t, "sender", msg.Sender.Account)
			assert.Equal(t, "寄件者", msg.Sender.ChineseName)
			assert.Equal(t, "Sender Lin", msg.Sender.EngName)

			if tc.wantMentions != nil {
				require.Len(t, evt.Mentions, len(tc.wantMentions))
				mentionAccounts := make([]string, len(evt.Mentions))
				for i, m := range evt.Mentions {
					mentionAccounts[i] = m.Account
				}
				assert.ElementsMatch(t, tc.wantMentions, mentionAccounts)
			} else {
				assert.Empty(t, evt.Mentions)
			}
		})
	}
}

func TestHandler_HandleMessage_DMRoom(t *testing.T) {
	msgTime := time.Date(2026, 3, 26, 11, 0, 0, 0, time.UTC)

	tests := []struct {
		name            string
		content         string
		wantSetMentions bool
		mentionedUsers  []string
		aliceHasMention bool
		bobHasMention   bool
	}{
		{
			name:            "no mentions",
			content:         "hey bob",
			wantSetMentions: false,
			aliceHasMention: false,
			bobHasMention:   false,
		},
		{
			name:            "with mention",
			content:         "hey @bob",
			wantSetMentions: true,
			mentionedUsers:  []string{"bob"},
			aliceHasMention: false,
			bobHasMention:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockStore(ctrl)
			us := NewMockUserStore(ctrl)
			pub := &mockPublisher{}

			evt := model.MessageEvent{
				Event:     model.EventCreated,
				SiteID:    "site-a",
				Timestamp: msgTime.UnixMilli(),
				Message: model.Message{
					ID: "msg-1", RoomID: "dm-1", UserID: "alice-id", UserAccount: "alice",
					Content: tc.content, CreatedAt: msgTime,
				},
			}
			data, _ := json.Marshal(evt)

			store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "dm-1", "msg-1", msgTime, false).Return(nil)
			store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "dm-1", "alice", msgTime).Return(nil)
			store.EXPECT().GetRoomMeta(gomock.Any(), "dm-1").Return(metaOf(testDMRoom), nil)
			store.EXPECT().ListSubscriptions(gomock.Any(), "dm-1").Return(testDMSubs, nil)

			if tc.wantSetMentions {
				store.EXPECT().SetSubscriptionMentions(gomock.Any(), "dm-1", gomock.InAnyOrder(tc.mentionedUsers), msgTime).Return(nil)
			}

			// Single user lookup: sender first, then mentioned accounts.
			if tc.wantSetMentions {
				wantAccounts := append([]string{"alice"}, tc.mentionedUsers...)
				us.EXPECT().FindUsersByAccounts(gomock.Any(), wantAccounts).
					Return([]model.User{testUsers[0], testUsers[1]}, nil)
			} else {
				us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice"}).
					Return([]model.User{testUsers[0]}, nil)
			}

			keyStore := NewMockRoomKeyProvider(ctrl)
			h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
			err := h.HandleMessage(context.Background(), data)
			require.NoError(t, err)

			require.Len(t, pub.records, 2)

			evtBySubject := map[string]model.RoomEvent{}
			for _, rec := range pub.records {
				evtBySubject[rec.subject] = decodeRoomEvent(t, rec.data)
			}

			aliceEvt := evtBySubject[subject.UserRoomEvent("alice")]
			assert.Equal(t, model.RoomEventNewMessage, aliceEvt.Type)
			assert.Positive(t, aliceEvt.Timestamp, "Timestamp must be the broadcast-worker publish time")
			assert.Equal(t, msgTime.UnixMilli(), aliceEvt.EventTimestamp)
			require.NotNil(t, aliceEvt.Message, "DM events must carry Message payload")
			assert.Equal(t, "msg-1", aliceEvt.Message.ID)
			require.NotNil(t, aliceEvt.Message.Sender)
			assert.Equal(t, "alice-id", aliceEvt.Message.Sender.UserID)
			assert.Equal(t, "alice", aliceEvt.Message.Sender.Account)
			assert.Equal(t, tc.aliceHasMention, aliceEvt.HasMention)

			bobEvt := evtBySubject[subject.UserRoomEvent("bob")]
			require.NotNil(t, bobEvt.Message)
			assert.Positive(t, bobEvt.Timestamp, "Timestamp must be the broadcast-worker publish time")
			assert.Equal(t, msgTime.UnixMilli(), bobEvt.EventTimestamp)
			assert.Equal(t, "msg-1", bobEvt.Message.ID)
			require.NotNil(t, bobEvt.Message.Sender)
			assert.Equal(t, tc.bobHasMention, bobEvt.HasMention)
		})
	}
}

func TestHandler_HandleMessage_Errors(t *testing.T) {
	msgTime := time.Date(2026, 3, 26, 12, 0, 0, 0, time.UTC)

	t.Run("invalid json", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		us := NewMockUserStore(ctrl)
		pub := &mockPublisher{}
		keyStore := NewMockRoomKeyProvider(ctrl)
		h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)

		err := h.HandleMessage(context.Background(), []byte("not json"))
		require.Error(t, err)
		_, perm := errcode.IsPermanent(err)
		assert.True(t, perm, "a malformed payload can never parse on redelivery — must be a permanent (drop) error")
		assert.Empty(t, pub.records)
	})

	t.Run("room not found", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		us := NewMockUserStore(ctrl)
		pub := &mockPublisher{}

		us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return(nil, nil) // combined lookup runs before UpdateRoomLastMessage
		store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", msgTime, false).Return(errors.New("not found"))

		keyStore := NewMockRoomKeyProvider(ctrl)
		h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
		err := h.HandleMessage(context.Background(), makeMessageEvent("room-1", "hello", msgTime))
		require.Error(t, err)
		assert.Empty(t, pub.records)
	})

	t.Run("update room fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		us := NewMockUserStore(ctrl)
		pub := &mockPublisher{}

		us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return(nil, nil) // combined lookup runs before UpdateRoomLastMessage
		store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", msgTime, false).Return(errors.New("db error"))

		keyStore := NewMockRoomKeyProvider(ctrl)
		h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
		err := h.HandleMessage(context.Background(), makeMessageEvent("room-1", "hello", msgTime))
		require.Error(t, err)
		assert.Empty(t, pub.records)
	})

	t.Run("set subscription mentions fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		us := NewMockUserStore(ctrl)
		pub := &mockPublisher{}

		us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender", "alice"}).Return(testUsers[:1], nil) // single combined lookup
		store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", msgTime, false).Return(nil)
		store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "room-1", "sender", msgTime).Return(nil)
		store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
		store.EXPECT().SetSubscriptionMentions(gomock.Any(), "room-1", gomock.Any(), gomock.Any()).Return(errors.New("db error"))

		keyStore := NewMockRoomKeyProvider(ctrl)
		h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
		err := h.HandleMessage(context.Background(), makeMessageEvent("room-1", "hey @alice", msgTime))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "set subscription mentions")
		assert.Empty(t, pub.records)
	})

	t.Run("unknown room type", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		us := NewMockUserStore(ctrl)
		pub := &mockPublisher{}

		unknownRoom := &model.Room{
			ID: "room-1", Name: "general", Type: "unknown",
			SiteID: "site-a", UserCount: 5,
		}
		store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", msgTime, false).Return(nil)
		store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "room-1", "sender", msgTime).Return(nil)
		store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(unknownRoom), nil)
		us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return(nil, nil) // sender lookup

		keyStore := NewMockRoomKeyProvider(ctrl)
		h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
		err := h.HandleMessage(context.Background(), makeMessageEvent("room-1", "hello", msgTime))
		require.NoError(t, err)
		assert.Empty(t, pub.records)
	})

	t.Run("list subscriptions fails for DM", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		us := NewMockUserStore(ctrl)
		pub := &mockPublisher{}

		store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "dm-1", "msg-1", msgTime, false).Return(nil)
		store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "dm-1", "sender", msgTime).Return(nil)
		store.EXPECT().GetRoomMeta(gomock.Any(), "dm-1").Return(metaOf(testDMRoom), nil)
		us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return(nil, nil) // sender lookup
		store.EXPECT().ListSubscriptions(gomock.Any(), "dm-1").Return(nil, errors.New("db error"))

		keyStore := NewMockRoomKeyProvider(ctrl)
		h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
		evt := model.MessageEvent{
			Event:  model.EventCreated,
			SiteID: "site-a",
			Message: model.Message{
				ID: "msg-1", RoomID: "dm-1", UserID: "user-1", UserAccount: "sender",
				Content: "hello", CreatedAt: msgTime,
			},
		}
		data, _ := json.Marshal(evt)
		err := h.HandleMessage(context.Background(), data)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "list subscriptions")
		assert.Empty(t, pub.records)
	})

	t.Run("sender mentioned resolves with user data", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		us := NewMockUserStore(ctrl)
		pub := &mockPublisher{}

		senderUser := model.User{ID: "u-sender", Account: "sender", EngName: "Sender Lin", ChineseName: "寄件者", SiteID: "site-a"}
		key := testRoomKey(t)
		keyStore := NewMockRoomKeyProvider(ctrl)
		keyStore.EXPECT().Get(gomock.Any(), "room-1").Return(key, nil)

		store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", msgTime, false).Return(nil)
		store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "room-1", "sender", msgTime).Return(nil)
		store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
		store.EXPECT().SetSubscriptionMentions(gomock.Any(), "room-1", []string{"sender"}, msgTime).Return(nil)
		// Single lookup: sender is both the message author and the mentioned account, so the deduped list is just ["sender"].
		us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return([]model.User{senderUser}, nil)

		h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
		err := h.HandleMessage(context.Background(), makeMessageEvent("room-1", "hey @sender", msgTime))
		require.NoError(t, err)

		require.Len(t, pub.records, 1)
		evt, _ := decryptClientMessage(t, pub.records[0].data, key)
		require.Len(t, evt.Mentions, 1)
		assert.Equal(t, "sender", evt.Mentions[0].Account)
		assert.Equal(t, "寄件者", evt.Mentions[0].ChineseName)
		assert.Equal(t, "u-sender", evt.Mentions[0].UserID)
	})

	t.Run("sender lookup fails fallback to account", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		us := NewMockUserStore(ctrl)
		pub := &mockPublisher{}

		key := testRoomKey(t)
		keyStore := NewMockRoomKeyProvider(ctrl)
		keyStore.EXPECT().Get(gomock.Any(), "room-1").Return(key, nil)

		store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", msgTime, false).Return(nil)
		store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "room-1", "sender", msgTime).Return(nil)
		store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
		us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return(nil, errors.New("db error")) // sender lookup

		h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
		err := h.HandleMessage(context.Background(), makeMessageEvent("room-1", "hello", msgTime))
		require.NoError(t, err)

		require.Len(t, pub.records, 1)
		_, msg := decryptClientMessage(t, pub.records[0].data, key)
		require.NotNil(t, msg.Sender)
		assert.Equal(t, "sender", msg.Sender.Account)
		assert.Equal(t, "sender", msg.Sender.ChineseName)
		assert.Equal(t, "sender", msg.Sender.EngName)
	})
}

type failingPublisher struct {
	mu        sync.Mutex
	callCount int
	failAfter int
	records   []publishRecord
}

func (p *failingPublisher) Publish(_ context.Context, subj string, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.callCount++
	if p.callCount > p.failAfter {
		return errors.New("publish failed")
	}
	p.records = append(p.records, publishRecord{subject: subj, data: data})
	return nil
}

func TestHandler_HandleMessage_DMRoom_PublishError(t *testing.T) {
	msgTime := time.Date(2026, 3, 26, 11, 0, 0, 0, time.UTC)

	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	pub := &failingPublisher{failAfter: 0}

	us := NewMockUserStore(ctrl)
	store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "dm-1", "msg-1", msgTime, false).Return(nil)
	store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "dm-1", "alice", msgTime).Return(nil)
	store.EXPECT().GetRoomMeta(gomock.Any(), "dm-1").Return(metaOf(testDMRoom), nil)
	store.EXPECT().ListSubscriptions(gomock.Any(), "dm-1").Return(testDMSubs, nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice"}).Return([]model.User{testUsers[0]}, nil) // sender lookup

	keyStore := NewMockRoomKeyProvider(ctrl)
	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
	evt := model.MessageEvent{
		Event:  model.EventCreated,
		SiteID: "site-a",
		Message: model.Message{
			ID: "msg-1", RoomID: "dm-1", UserID: "alice-id", UserAccount: "alice",
			Content: "hello", CreatedAt: msgTime,
		},
	}
	data, _ := json.Marshal(evt)

	err := h.HandleMessage(context.Background(), data)
	require.NoError(t, err)
	assert.Equal(t, 2, pub.callCount)
}

func TestHandler_HandleMessage_ChannelRoom_Encryption(t *testing.T) {
	msgTime := time.Date(2026, 3, 26, 10, 0, 0, 0, time.UTC)

	t.Run("keystore returns nil key", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		us := NewMockUserStore(ctrl)
		pub := &mockPublisher{}

		keyStore := NewMockRoomKeyProvider(ctrl)
		keyStore.EXPECT().Get(gomock.Any(), "room-1").Return(nil, nil)

		store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", msgTime, false).Return(nil)
		store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "room-1", "sender", msgTime).Return(nil)
		store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
		us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return(nil, nil)

		h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
		err := h.HandleMessage(context.Background(), makeMessageEvent("room-1", "hello", msgTime))
		require.Error(t, err)
		assert.ErrorIs(t, err, errNoCurrentKey)
		assert.Empty(t, pub.records)
	})

	t.Run("keystore returns error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		us := NewMockUserStore(ctrl)
		pub := &mockPublisher{}

		keyStore := NewMockRoomKeyProvider(ctrl)
		keyStore.EXPECT().Get(gomock.Any(), "room-1").Return(nil, errors.New("valkey down"))

		store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", msgTime, false).Return(nil)
		store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "room-1", "sender", msgTime).Return(nil)
		store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
		us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return(nil, nil)

		h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
		err := h.HandleMessage(context.Background(), makeMessageEvent("room-1", "hello", msgTime))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "get room key")
		assert.Contains(t, err.Error(), "valkey down")
		assert.Empty(t, pub.records)
	})

	t.Run("published event has encrypted message and plaintext metadata", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		us := NewMockUserStore(ctrl)
		pub := &mockPublisher{}

		key := testRoomKey(t)
		keyStore := NewMockRoomKeyProvider(ctrl)
		keyStore.EXPECT().Get(gomock.Any(), "room-1").Return(key, nil)

		store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", msgTime, false).Return(nil)
		store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "room-1", "sender", msgTime).Return(nil)
		store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
		us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return([]model.User{{ID: "u-sender", Account: "sender", EngName: "Sender Lin", ChineseName: "寄件者", SiteID: "site-a"}}, nil)

		h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
		err := h.HandleMessage(context.Background(), makeMessageEvent("room-1", "hello", msgTime))
		require.NoError(t, err)

		require.Len(t, pub.records, 1)
		assert.Equal(t, subject.RoomEvent("room-1", true), pub.records[0].subject)

		// Verify plaintext metadata on the RoomEvent
		var rawEvt map[string]any
		require.NoError(t, json.Unmarshal(pub.records[0].data, &rawEvt))
		assert.Equal(t, "room-1", rawEvt["roomId"])
		assert.Equal(t, "general", rawEvt["roomName"])
		assert.Equal(t, "site-a", rawEvt["siteId"])
		assert.Nil(t, rawEvt["message"], "message must be nil in published JSON")
		assert.NotNil(t, rawEvt["encryptedMessage"], "encryptedMessage must be present")

		// Verify encrypted message structure
		var evt model.RoomEvent
		require.NoError(t, json.Unmarshal(pub.records[0].data, &evt))
		require.Nil(t, evt.Message)
		require.NotEmpty(t, evt.EncryptedMessage)

		var env roomcrypto.EncryptedMessage
		require.NoError(t, json.Unmarshal(evt.EncryptedMessage, &env))
		assert.Equal(t, key.Version, env.Version)
		assert.NotEmpty(t, env.Nonce)
		assert.NotEmpty(t, env.Ciphertext)

		// Decrypt and verify the ClientMessage
		_, msg := decryptClientMessage(t, pub.records[0].data, key)
		assert.Equal(t, "msg-1", msg.ID)
		assert.Equal(t, "room-1", msg.RoomID)
		assert.Equal(t, "hello", msg.Content)
		require.NotNil(t, msg.Sender)
		assert.Equal(t, "user-1", msg.Sender.UserID)
		assert.Equal(t, "sender", msg.Sender.Account)
		assert.Equal(t, "寄件者", msg.Sender.ChineseName)
		assert.Equal(t, "Sender Lin", msg.Sender.EngName)
	})
}

func TestBuildClientMessage(t *testing.T) {
	msg := &model.Message{
		ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
		Content: "hello", CreatedAt: time.Now(),
	}

	t.Run("user found", func(t *testing.T) {
		users := map[string]model.User{
			"alice": {ID: "u-alice", Account: "alice", EngName: "Alice Wang", ChineseName: "愛麗絲"},
		}
		cm := buildClientMessage(msg, users)
		assert.Equal(t, "m1", cm.ID)
		require.NotNil(t, cm.Sender)
		assert.Equal(t, "u1", cm.Sender.UserID)
		assert.Equal(t, "alice", cm.Sender.Account)
		assert.Equal(t, "愛麗絲", cm.Sender.ChineseName)
		assert.Equal(t, "Alice Wang", cm.Sender.EngName)
	})

	t.Run("user not found", func(t *testing.T) {
		cm := buildClientMessage(msg, map[string]model.User{})
		require.NotNil(t, cm.Sender)
		assert.Equal(t, "alice", cm.Sender.ChineseName)
		assert.Equal(t, "alice", cm.Sender.EngName)
	})
}

func TestHandler_FetchAndUpdateRoom_Missing(t *testing.T) {
	msgTime := time.Date(2026, 3, 26, 12, 0, 0, 0, time.UTC)

	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}

	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return(nil, nil) // single combined lookup
	store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "ghost-room", "msg-1", msgTime, false).
		Return(fmt.Errorf("update room last message ghost-room: %w", mongo.ErrNoDocuments))

	keyStore := NewMockRoomKeyProvider(ctrl)
	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)

	err := h.HandleMessage(context.Background(), makeMessageEvent("ghost-room", "hello", msgTime))
	require.Error(t, err)
	require.ErrorIs(t, err, mongo.ErrNoDocuments)
	assert.Empty(t, pub.records)
}

func TestHandleUpdated_ChannelRoomScopedPublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	roomID := "r1"
	room := &model.Room{ID: roomID, Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)

	edited := time.Date(2026, 5, 14, 12, 5, 0, 0, time.UTC)
	evt := model.MessageEvent{
		Event:     model.EventUpdated,
		SiteID:    "site-a",
		Timestamp: edited.UnixMilli(),
		Message: model.Message{
			ID:          "msg-1",
			RoomID:      roomID,
			UserID:      "u-alice",
			UserAccount: "alice",
			Content:     "updated content",
			CreatedAt:   time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
			EditedAt:    &edited,
			UpdatedAt:   &edited,
		},
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	require.Len(t, pub.records, 1, "channel: single room-scoped publish")
	c := pub.records[0]
	assert.Equal(t, subject.RoomEvent(roomID, true), c.subject)
	var roomEvt model.EditRoomEvent
	require.NoError(t, json.Unmarshal(c.data, &roomEvt))
	assert.Equal(t, model.RoomEventMessageEdited, roomEvt.Type)
	assert.Equal(t, roomID, roomEvt.RoomID)
	assert.Equal(t, "site-a", roomEvt.SiteID)
	assert.Equal(t, "msg-1", roomEvt.MessageID)
	assert.Equal(t, "updated content", roomEvt.NewContent)
	assert.Empty(t, roomEvt.EncryptedNewContent)
	assert.Equal(t, "alice", roomEvt.EditedBy)
	assert.True(t, roomEvt.EditedAt.Equal(edited))
	assert.True(t, roomEvt.UpdatedAt.Equal(edited))
}

func TestHandleUpdated_RelaysPreviewObject(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	roomID := "r1"
	room := &model.Room{ID: roomID, Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)

	edited := time.Date(2026, 5, 14, 12, 5, 0, 0, time.UTC)
	evt := model.MessageEvent{
		Event:     model.EventUpdated,
		SiteID:    "site-a",
		Timestamp: edited.UnixMilli(),
		Message: model.Message{
			ID: "msg-1", RoomID: roomID, UserID: "u-alice", UserAccount: "alice",
			Content: "updated", CreatedAt: edited.Add(-time.Hour), EditedAt: &edited, UpdatedAt: &edited,
		},
		PreviewMessage: &model.PreviewMessage{MessageID: "msg-1", Content: "updated"},
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	require.Len(t, pub.records, 1)
	var roomEvt model.EditRoomEvent
	require.NoError(t, json.Unmarshal(pub.records[0].data, &roomEvt))
	require.NotNil(t, roomEvt.PreviewMessage, "edit fan-out must carry the refreshed preview")
	assert.Equal(t, "msg-1", roomEvt.PreviewMessage.MessageID)
	assert.Equal(t, "updated", roomEvt.PreviewMessage.Content)
	assert.Empty(t, roomEvt.ThreadParentMessageID, "top-level edit carries no thread parent")
}

func TestHandleDeleted_OmitsPreviewWhenNil(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	roomID := "r1"
	room := &model.Room{ID: roomID, Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)

	deleted := time.Date(2026, 5, 14, 12, 5, 0, 0, time.UTC)
	evt := model.MessageEvent{
		Event:     model.EventDeleted,
		SiteID:    "site-a",
		Timestamp: deleted.UnixMilli(),
		Message: model.Message{
			ID: "msg-1", RoomID: roomID, UserID: "u-alice", UserAccount: "alice",
			CreatedAt: deleted.Add(-time.Hour), UpdatedAt: &deleted,
		},
		// PreviewMessage nil -> no eligible message / read error
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	require.Len(t, pub.records, 1)
	assert.NotContains(t, string(pub.records[0].data), "previewMessage",
		"a nil preview must be omitted from the fan-out, not sent as null")
}

func TestHandleUpdated_EncryptedChannel_EncryptsContent(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	roomID := "r1"
	room := &model.Room{ID: roomID, Type: model.RoomTypeChannel, SiteID: "site-a"}
	key := testRoomKey(t)
	store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)
	keyStore.EXPECT().Get(gomock.Any(), roomID).Return(key, nil)

	edited := time.Date(2026, 5, 14, 12, 5, 0, 0, time.UTC)
	evt := model.MessageEvent{
		Event:     model.EventUpdated,
		SiteID:    "site-a",
		Timestamp: edited.UnixMilli(),
		Message: model.Message{
			ID: "msg-1", RoomID: roomID, UserID: "u-alice", UserAccount: "alice",
			Content:   "secret edit",
			CreatedAt: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
			EditedAt:  &edited, UpdatedAt: &edited,
		},
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	require.Len(t, pub.records, 1, "channel: single room-scoped publish")
	c := pub.records[0]
	assert.Equal(t, subject.RoomEvent(roomID, true), c.subject)
	var roomEvt model.EditRoomEvent
	require.NoError(t, json.Unmarshal(c.data, &roomEvt))
	assert.Empty(t, roomEvt.NewContent, "plaintext must be cleared when encrypted")
	require.NotEmpty(t, roomEvt.EncryptedNewContent)
	var env roomcrypto.EncryptedMessage
	require.NoError(t, json.Unmarshal(roomEvt.EncryptedNewContent, &env))
	assert.Equal(t, key.Version, env.Version)
	plaintext, err := decryptForTest(&env, key.KeyPair.PrivateKey)
	require.NoError(t, err)
	assert.Equal(t, "secret edit", plaintext)
}

func TestHandleUpdated_BadgesNewlyAddedMentions(t *testing.T) {
	edited := time.Date(2026, 5, 14, 12, 5, 0, 0, time.UTC)

	// The worker forwards every parsed mention to SetSubscriptionMentions; the
	// additive / no-re-badge / skip-non-subscriber properties are enforced by the
	// store filter (read-guard) and covered by TestSetSubscriptionMentions_ReadGuard_Integration.
	tests := []struct {
		name            string
		content         string
		wantSetMentions []string // nil = SetSubscriptionMentions must not be called
	}{
		{
			name:            "edit adds a mention",
			content:         "hey @bob check this",
			wantSetMentions: []string{"bob"},
		},
		{
			name:    "no mentions badges nobody",
			content: "no mentions here anymore",
		},
		{
			name:            "every parsed mention is forwarded to the store",
			content:         "hey @alice and @bob",
			wantSetMentions: []string{"alice", "bob"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockStore(ctrl)
			us := NewMockUserStore(ctrl)
			pub := &mockPublisher{}
			keyStore := NewMockRoomKeyProvider(ctrl)

			store.EXPECT().GetRoom(gomock.Any(), "room-1").Return(testChannelRoom, nil)
			if tc.wantSetMentions != nil {
				// The lookup only resolves mentionees to home sites for the cross-site relay.
				us.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.InAnyOrder(tc.wantSetMentions)).Return(nil, nil)
				store.EXPECT().SetSubscriptionMentions(gomock.Any(), "room-1", gomock.InAnyOrder(tc.wantSetMentions), edited).Return(nil)
			}

			evt := model.MessageEvent{
				Event:     model.EventUpdated,
				SiteID:    "site-a",
				Timestamp: edited.UnixMilli(),
				Message: model.Message{
					ID: "msg-1", RoomID: "room-1", UserID: "u-alice", UserAccount: "alice",
					Content:   tc.content,
					CreatedAt: edited.Add(-time.Hour),
					EditedAt:  &edited, UpdatedAt: &edited,
				},
			}
			data, err := json.Marshal(&evt)
			require.NoError(t, err)

			h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
			require.NoError(t, h.HandleMessage(context.Background(), data))
			require.Len(t, pub.records, 1, "edit must still fan out regardless of mention badging")
		})
	}
}

func TestHandleUpdated_MissingEditedAt_ReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	evt := model.MessageEvent{
		Event: model.EventUpdated,
		Message: model.Message{
			ID: "msg-1", RoomID: "r1", UserAccount: "alice", Content: "x",
		},
		// EditedAt / UpdatedAt deliberately nil
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
	err = h.HandleMessage(context.Background(), data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing EditedAt")
	assert.Empty(t, pub.records)
}

func TestHandleDeleted_ChannelRoomScopedPublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	roomID := "r1"
	room := &model.Room{ID: roomID, Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)

	deletedAt := time.Date(2026, 5, 14, 12, 10, 0, 0, time.UTC)
	evt := model.MessageEvent{
		Event:     model.EventDeleted,
		SiteID:    "site-a",
		Timestamp: deletedAt.UnixMilli(),
		Message: model.Message{
			ID:          "msg-1",
			RoomID:      roomID,
			UserID:      "u-alice",
			UserAccount: "alice",
			CreatedAt:   time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
			UpdatedAt:   &deletedAt,
		},
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	require.Len(t, pub.records, 1, "channel: single room-scoped publish")
	c := pub.records[0]
	assert.Equal(t, subject.RoomEvent(roomID, true), c.subject)
	var roomEvt model.DeleteRoomEvent
	require.NoError(t, json.Unmarshal(c.data, &roomEvt))
	assert.Equal(t, model.RoomEventMessageDeleted, roomEvt.Type)
	assert.Equal(t, roomID, roomEvt.RoomID)
	assert.Equal(t, "site-a", roomEvt.SiteID)
	assert.Equal(t, "msg-1", roomEvt.MessageID)
	assert.Equal(t, "alice", roomEvt.DeletedBy)
	assert.True(t, roomEvt.DeletedAt.Equal(deletedAt))
	assert.True(t, roomEvt.UpdatedAt.Equal(deletedAt))
}

// buildDeletedEventData is a small helper shared by the ROOM_SUBJECT_MODE
// routing tests below: it builds a HandleMessage-ready deleted-message
// payload for room r1, so each test only needs to vary the handler's
// routeMode and the room's CrossSite flag.
func buildDeletedEventData(t *testing.T) []byte {
	t.Helper()
	deletedAt := time.Date(2026, 5, 14, 12, 10, 0, 0, time.UTC)
	evt := model.MessageEvent{
		Event:     model.EventDeleted,
		SiteID:    "site-a",
		Timestamp: deletedAt.UnixMilli(),
		Message: model.Message{
			ID:          "msg-1",
			RoomID:      "r1",
			UserID:      "u-alice",
			UserAccount: "alice",
			CreatedAt:   time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
			UpdatedAt:   &deletedAt,
		},
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)
	return data
}

// subjectsOf extracts the recorded publish subjects, in order.
func subjectsOf(pub *mockPublisher) []string {
	subs := make([]string, len(pub.records))
	for i, r := range pub.records {
		subs[i] = r.subject
	}
	return subs
}

func TestBroadcast_GlobalMode_AlwaysGlobal(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a", CrossSite: ptrBool(false)}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), buildDeletedEventData(t)))

	capturedSubjects := subjectsOf(pub)
	assert.Contains(t, capturedSubjects, "chat.room.r1.event")
	assert.NotContains(t, capturedSubjects, "chat.local.room.r1.event")
}

func TestBroadcast_LocalMode_SameSiteUsesLocal(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a", CrossSite: ptrBool(false)}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteLocal)
	require.NoError(t, h.HandleMessage(context.Background(), buildDeletedEventData(t)))

	capturedSubjects := subjectsOf(pub)
	assert.Contains(t, capturedSubjects, "chat.local.room.r1.event")
	assert.NotContains(t, capturedSubjects, "chat.room.r1.event")
}

func TestBroadcast_DualMode_SameSitePublishesBoth(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a", CrossSite: ptrBool(false)}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteDual)
	require.NoError(t, h.HandleMessage(context.Background(), buildDeletedEventData(t)))

	capturedSubjects := subjectsOf(pub)
	assert.Contains(t, capturedSubjects, "chat.local.room.r1.event")
	assert.Contains(t, capturedSubjects, "chat.room.r1.event")
}

// TestHandler_publishRoomEvent covers the routing fan-out: which target
// subject(s) the helper publishes to for each (crossSite, routeMode) pair.
func TestHandler_publishRoomEvent(t *testing.T) {
	cases := []struct {
		name      string
		mode      subject.RoomRouteMode
		crossSite *bool
		want      []string
	}{
		{"global mode routes global", subject.RouteGlobal, ptrBool(false), []string{"chat.room.r1.event"}},
		{"local mode same-site routes local", subject.RouteLocal, ptrBool(false), []string{"chat.local.room.r1.event"}},
		{"local mode cross-site stays global", subject.RouteLocal, ptrBool(true), []string{"chat.room.r1.event"}},
		{"local mode unknown stays global (fail-safe)", subject.RouteLocal, nil, []string{"chat.room.r1.event"}},
		{"dual mode same-site publishes both", subject.RouteDual, ptrBool(false), []string{"chat.local.room.r1.event", "chat.room.r1.event"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pub := &mockPublisher{}
			h := &Handler{pub: pub, routeMode: tc.mode}
			require.NoError(t, h.publishRoomEvent(context.Background(), "r1", tc.crossSite, nil, []byte("{}"), "test"))
			assert.Equal(t, tc.want, subjectsOf(pub))
		})
	}
}

// TestHandler_publishRoomEvent_TransitionGrace verifies that a room which just
// flipped same-site→cross-site keeps a local copy during the grace window, so a
// same-site member still on the local subject keeps receiving until it
// re-subscribes.
func TestHandler_publishRoomEvent_TransitionGrace(t *testing.T) {
	pub := &mockPublisher{}
	h := &Handler{pub: pub, routeMode: subject.RouteLocal}
	flip := time.Now().UTC() // just flipped → within grace
	require.NoError(t, h.publishRoomEvent(context.Background(), "r1", ptrBool(true), &flip, []byte("{}"), "test"))
	assert.Equal(t, []string{"chat.local.room.r1.event", "chat.room.r1.event"}, subjectsOf(pub),
		"during the grace window a flipped room publishes to both lanes")
}

// TestHandler_publishRoomEvent_DualPartialFailure verifies that a failure on an
// earlier target (the local subject) does NOT abort the fan-out: the global
// safety-net publish is still attempted, and the last error is returned so
// JetStream redelivers.
func TestHandler_publishRoomEvent_DualPartialFailure(t *testing.T) {
	pub := &mockPublisher{failOn: map[string]error{"chat.local.room.r1.event": errors.New("local lane down")}}
	h := &Handler{pub: pub, routeMode: subject.RouteDual}

	err := h.publishRoomEvent(context.Background(), "r1", ptrBool(false), nil, []byte("{}"), "test")

	require.Error(t, err)
	assert.Equal(t, []string{"chat.local.room.r1.event", "chat.room.r1.event"}, subjectsOf(pub),
		"global target must still be attempted after the local target fails")
}

func TestBroadcast_CrossSiteRoomAlwaysGlobal(t *testing.T) {
	for _, mode := range []subject.RoomRouteMode{subject.RouteGlobal, subject.RouteDual, subject.RouteLocal} {
		t.Run(fmt.Sprintf("mode=%d", mode), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockStore(ctrl)
			us := NewMockUserStore(ctrl)
			pub := &mockPublisher{}
			keyStore := NewMockRoomKeyProvider(ctrl)

			room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a", CrossSite: ptrBool(true)}
			store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)

			h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, mode)
			require.NoError(t, h.HandleMessage(context.Background(), buildDeletedEventData(t)))

			capturedSubjects := subjectsOf(pub)
			assert.Contains(t, capturedSubjects, "chat.room.r1.event")
			assert.NotContains(t, capturedSubjects, "chat.local.room.r1.event")
		})
	}
}

// TestBroadcast_UnclassifiedRoomAlwaysGlobal is the fail-safe regression: a
// room whose CrossSite is nil (never classified / not yet backfilled) must
// route global in every mode, exactly like a confirmed cross-site room — an
// unclassified room must never be silently treated as confirmed same-site.
func TestBroadcast_UnclassifiedRoomAlwaysGlobal(t *testing.T) {
	for _, mode := range []subject.RoomRouteMode{subject.RouteGlobal, subject.RouteDual, subject.RouteLocal} {
		t.Run(fmt.Sprintf("mode=%d", mode), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockStore(ctrl)
			us := NewMockUserStore(ctrl)
			pub := &mockPublisher{}
			keyStore := NewMockRoomKeyProvider(ctrl)

			room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"} // CrossSite unset (nil)
			store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)

			h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, mode)
			require.NoError(t, h.HandleMessage(context.Background(), buildDeletedEventData(t)))

			capturedSubjects := subjectsOf(pub)
			assert.Contains(t, capturedSubjects, "chat.room.r1.event")
			assert.NotContains(t, capturedSubjects, "chat.local.room.r1.event")
		})
	}
}

func TestHandleDeleted_MissingUpdatedAt_ReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	evt := model.MessageEvent{
		Event: model.EventDeleted,
		Message: model.Message{
			ID: "msg-1", RoomID: "r1", UserAccount: "alice",
		},
		// UpdatedAt deliberately nil
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
	err = h.HandleMessage(context.Background(), data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing UpdatedAt")
	assert.Empty(t, pub.records)
}

func TestHandleUpdated_DMRoom_FansOutToBothMembers(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	roomID := "dm-alice-bob"
	room := &model.Room{
		ID:       roomID,
		Type:     model.RoomTypeDM,
		SiteID:   "site-a",
		Accounts: []string{"alice", "bob"},
	}
	store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)

	edited := time.Date(2026, 5, 14, 12, 5, 0, 0, time.UTC)
	evt := model.MessageEvent{
		Event:     model.EventUpdated,
		SiteID:    "site-a",
		Timestamp: edited.UnixMilli(),
		Message: model.Message{
			ID:          "msg-1",
			RoomID:      roomID,
			UserID:      "u-alice",
			UserAccount: "alice",
			Content:     "updated content",
			CreatedAt:   time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
			EditedAt:    &edited,
			UpdatedAt:   &edited,
		},
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	require.Len(t, pub.records, 2, "per-user fan-out: one publish per DM member")
	subjects := map[string]bool{}
	for _, c := range pub.records {
		subjects[c.subject] = true
		var roomEvt model.EditRoomEvent
		require.NoError(t, json.Unmarshal(c.data, &roomEvt))
		assert.Equal(t, model.RoomEventMessageEdited, roomEvt.Type)
		assert.Equal(t, roomID, roomEvt.RoomID)
		assert.Equal(t, "site-a", roomEvt.SiteID)
		assert.Equal(t, "msg-1", roomEvt.MessageID)
		assert.Equal(t, "updated content", roomEvt.NewContent)
		assert.Equal(t, "alice", roomEvt.EditedBy)
		assert.True(t, roomEvt.EditedAt.Equal(edited))
		assert.True(t, roomEvt.UpdatedAt.Equal(edited))
	}
	assert.True(t, subjects[subject.UserRoomEvent("alice")])
	assert.True(t, subjects[subject.UserRoomEvent("bob")])
}

func TestHandleDeleted_DMRoom_FansOutToBothMembers(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	roomID := "dm-alice-bob"
	room := &model.Room{
		ID:       roomID,
		Type:     model.RoomTypeDM,
		SiteID:   "site-a",
		Accounts: []string{"alice", "bob"},
	}
	store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)

	deletedAt := time.Date(2026, 5, 14, 12, 10, 0, 0, time.UTC)
	evt := model.MessageEvent{
		Event:     model.EventDeleted,
		SiteID:    "site-a",
		Timestamp: deletedAt.UnixMilli(),
		Message: model.Message{
			ID:          "msg-1",
			RoomID:      roomID,
			UserID:      "u-alice",
			UserAccount: "alice",
			CreatedAt:   time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
			UpdatedAt:   &deletedAt,
		},
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	require.Len(t, pub.records, 2, "per-user fan-out: one publish per DM member")
	subjects := map[string]bool{}
	for _, c := range pub.records {
		subjects[c.subject] = true
		var roomEvt model.DeleteRoomEvent
		require.NoError(t, json.Unmarshal(c.data, &roomEvt))
		assert.Equal(t, model.RoomEventMessageDeleted, roomEvt.Type)
		assert.Equal(t, roomID, roomEvt.RoomID)
		assert.Equal(t, "site-a", roomEvt.SiteID)
		assert.Equal(t, "msg-1", roomEvt.MessageID)
		assert.Equal(t, "alice", roomEvt.DeletedBy)
		assert.True(t, roomEvt.DeletedAt.Equal(deletedAt))
		assert.True(t, roomEvt.UpdatedAt.Equal(deletedAt))
	}
	assert.True(t, subjects[subject.UserRoomEvent("alice")])
	assert.True(t, subjects[subject.UserRoomEvent("bob")])
}

func TestHandleUpdated_BotDMRoom_SkipsBotAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	roomID := "botdm-alice-helper.bot"
	room := &model.Room{
		ID:       roomID,
		Type:     model.RoomTypeBotDM,
		SiteID:   "site-a",
		Accounts: []string{"alice", "helper.bot"},
	}
	store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)

	edited := time.Date(2026, 5, 14, 12, 5, 0, 0, time.UTC)
	evt := model.MessageEvent{
		Event:     model.EventUpdated,
		SiteID:    "site-a",
		Timestamp: edited.UnixMilli(),
		Message: model.Message{
			ID:          "msg-1",
			RoomID:      roomID,
			UserID:      "u-alice",
			UserAccount: "alice",
			Content:     "updated content",
			CreatedAt:   time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
			EditedAt:    &edited,
			UpdatedAt:   &edited,
		},
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	require.Len(t, pub.records, 1, "botDM: only the human recipient gets the live event")
	assert.Equal(t, subject.UserRoomEvent("alice"), pub.records[0].subject)
}

func TestHandler_HandleMessage_ChannelEncryptionDisabled(t *testing.T) {
	msgTime := time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	senderUser := model.User{ID: "u-sender", Account: "sender", EngName: "Sender Lin", ChineseName: "寄件者", SiteID: "site-a"}

	tests := []struct {
		name            string
		content         string
		wantMentionAll  bool
		wantMentions    []string
		wantSetMentions []string
	}{
		{
			name:           "plaintext no mentions",
			content:        "hello group",
			wantMentionAll: false,
		},
		{
			name:            "plaintext individual mention",
			content:         "hey @alice",
			wantMentions:    []string{"alice"},
			wantSetMentions: []string{"alice"},
		},
		{
			name:           "plaintext mention all",
			content:        "attention @all",
			wantMentionAll: true,
			wantMentions:   []string{"all"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockStore(ctrl)
			us := NewMockUserStore(ctrl)
			pub := &mockPublisher{}

			store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", msgTime, tc.wantMentionAll).Return(nil)
			store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "room-1", "sender", msgTime).Return(nil)
			store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
			if tc.wantSetMentions != nil {
				store.EXPECT().SetSubscriptionMentions(gomock.Any(), "room-1", gomock.InAnyOrder(tc.wantSetMentions), msgTime).Return(nil)
			}

			if tc.name == "plaintext individual mention" {
				us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender", "alice"}).
					Return([]model.User{senderUser, testUsers[0]}, nil)
			} else {
				us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).
					Return([]model.User{senderUser}, nil)
			}

			// nil keyStore — handler must NOT dereference it when encrypt=false
			h := NewHandler(store, us, pub, nil, defaultParentFetcher, false, subject.RouteGlobal)
			err := h.HandleMessage(context.Background(), makeMessageEvent("room-1", tc.content, msgTime))
			require.NoError(t, err)

			require.Len(t, pub.records, 1)
			assert.Equal(t, subject.RoomEvent("room-1", true), pub.records[0].subject)

			var evt model.RoomEvent
			require.NoError(t, json.Unmarshal(pub.records[0].data, &evt))
			require.NotNil(t, evt.Message, "plaintext channel events must carry Message")
			assert.Empty(t, evt.EncryptedMessage, "plaintext channel events must NOT carry EncryptedMessage")
			assert.Equal(t, "msg-1", evt.Message.ID)
			require.NotNil(t, evt.Message.Sender)
			assert.Equal(t, "sender", evt.Message.Sender.Account)
			assert.Equal(t, tc.wantMentionAll, evt.MentionAll)

			if tc.wantMentions != nil {
				accounts := make([]string, len(evt.Mentions))
				for i, m := range evt.Mentions {
					accounts[i] = m.Account
				}
				assert.ElementsMatch(t, tc.wantMentions, accounts)
			} else {
				assert.Empty(t, evt.Mentions)
			}
		})
	}
}

func TestHandleReacted_ChannelRoomScopedPublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	roomID := "r1"
	room := &model.Room{ID: roomID, Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)

	reactedAt := time.Date(2026, 5, 14, 12, 15, 0, 0, time.UTC)
	evt := model.MessageEvent{
		Event:     model.EventReacted,
		SiteID:    "site-a",
		Timestamp: reactedAt.UnixMilli(),
		Message: model.Message{
			ID:          "msg-1",
			RoomID:      roomID,
			UserID:      "u-bob",
			UserAccount: "bob",
			CreatedAt:   time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
			UpdatedAt:   &reactedAt,
		},
		ReactionDelta: &model.ReactionDelta{
			Shortcode: "thumbsup",
			Action:    "added",
			Actor:     model.Participant{UserID: "u-alice", Account: "alice", EngName: "Alice"},
		},
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	require.Len(t, pub.records, 2, "channel: room-scoped publish + author notification")
	roomRec := findPublishRecord(pub.records, subject.RoomEvent(roomID, true))
	require.NotNil(t, roomRec, "room-scoped publish expected")
	var roomEvt model.ReactRoomEvent
	require.NoError(t, json.Unmarshal(roomRec.data, &roomEvt))
	assert.Equal(t, model.RoomEventMessageReacted, roomEvt.Type)
	assert.Equal(t, roomID, roomEvt.RoomID)
	assert.Equal(t, "msg-1", roomEvt.MessageID)
	assert.Equal(t, "thumbsup", roomEvt.Shortcode)
	assert.Equal(t, model.ReactionActionAdded, roomEvt.Action)
	assert.Equal(t, "alice", roomEvt.Actor.Account)
	assert.True(t, roomEvt.ReactedAt.Equal(reactedAt))
	assert.True(t, roomEvt.UpdatedAt.Equal(reactedAt))
}

func TestHandleReacted_DMFanOut(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	roomID := "dm-r1"
	room := &model.Room{
		ID: roomID, Type: model.RoomTypeDM, SiteID: "site-a",
		Accounts: []string{"alice", "bob"},
	}
	store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)

	reactedAt := time.Date(2026, 5, 14, 12, 20, 0, 0, time.UTC)
	evt := model.MessageEvent{
		Event:     model.EventReacted,
		SiteID:    "site-a",
		Timestamp: reactedAt.UnixMilli(),
		Message: model.Message{
			ID: "msg-1", RoomID: roomID, UserID: "u-bob", UserAccount: "bob",
			CreatedAt: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
			UpdatedAt: &reactedAt,
		},
		ReactionDelta: &model.ReactionDelta{
			Shortcode: "tada", Action: "added",
			Actor: model.Participant{UserID: "u-alice", Account: "alice"},
		},
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	require.Len(t, pub.records, 3, "DM: one event per non-bot account + author notification")
	subjects := make([]string, len(pub.records))
	for i, r := range pub.records {
		subjects[i] = r.subject
	}
	assert.ElementsMatch(t,
		[]string{subject.UserRoomEvent("alice"), subject.UserRoomEvent("bob"), subject.Notification("bob")},
		subjects,
	)
}

// TestHandleReacted_MissingDelta_LogsAndDrops covers the poison-pill
// guard for malformed reaction events with a nil ReactionDelta. The
// publisher-side dedup_id deliberately collides such events with the
// EventCreated key (see pkg/natsutil/canonical_dedup.go), so the
// publisher bug is "loud" — but the consumer must not respond by
// NAK-ing forever. The handler logs the malformed event and Acks
// (returns nil) so JetStream drains it instead of looping.
func TestHandleReacted_MissingDelta_LogsAndDrops(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	evt := model.MessageEvent{
		Event:   model.EventReacted,
		Message: model.Message{ID: "msg-1", RoomID: "r1"},
	}
	data, _ := json.Marshal(&evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
	var err error
	terminal := terminalCountFor(t, "invalid_payload", func(ctx context.Context) {
		err = h.HandleMessage(ctx, data)
	})
	require.NoError(t, err, "malformed event must be acked, not NAK-ed")
	assert.Empty(t, pub.records)
	assert.Equal(t, int64(1), terminal, "a permanently dropped malformed event must leave terminal evidence")
}

// TestHandleReacted_MissingUpdatedAt_LogsAndDrops mirrors the missing-
// Delta guard for the missing-UpdatedAt branch. Same poison-pill
// rationale: a malformed event from a future publisher path (federation
// replay, legacy producer) must drop cleanly rather than block the
// consumer with infinite redelivery.
func TestHandleReacted_MissingUpdatedAt_LogsAndDrops(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	evt := model.MessageEvent{
		Event:   model.EventReacted,
		Message: model.Message{ID: "msg-1", RoomID: "r1"},
		ReactionDelta: &model.ReactionDelta{
			Shortcode: "thumbsup", Action: "added",
			Actor: model.Participant{Account: "alice"},
		},
		// UpdatedAt deliberately nil.
	}
	data, _ := json.Marshal(&evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
	var err error
	terminal := terminalCountFor(t, "invalid_payload", func(ctx context.Context) {
		err = h.HandleMessage(ctx, data)
	})
	require.NoError(t, err, "malformed event must be acked, not NAK-ed")
	assert.Empty(t, pub.records)
	assert.Equal(t, int64(1), terminal, "a permanently dropped malformed event must leave terminal evidence")
}

// findPublishRecord returns the first record whose subject matches, or nil.
func findPublishRecord(records []publishRecord, subj string) *publishRecord {
	for i := range records {
		if records[i].subject == subj {
			return &records[i]
		}
	}
	return nil
}

func TestHandleReacted_AuthorNotificationPolicy(t *testing.T) {
	cases := []struct {
		name          string
		action        model.ReactionAction
		authorAccount string
		actorAccount  string
		wantNotify    bool
	}{
		{name: "added notifies author when actor differs", action: model.ReactionActionAdded, authorAccount: "bob", actorAccount: "alice", wantNotify: true},
		{name: "removed never notifies", action: model.ReactionActionRemoved, authorAccount: "bob", actorAccount: "alice", wantNotify: false},
		{name: "self-react does not notify actor", action: model.ReactionActionAdded, authorAccount: "alice", actorAccount: "alice", wantNotify: false},
		{name: "empty author (system message) does not notify", action: model.ReactionActionAdded, authorAccount: "", actorAccount: "alice", wantNotify: false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockStore(ctrl)
			us := NewMockUserStore(ctrl)
			pub := &mockPublisher{}
			keyStore := NewMockRoomKeyProvider(ctrl)

			roomID := "r1"
			room := &model.Room{ID: roomID, Type: model.RoomTypeChannel, SiteID: "site-a"}
			store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)

			reactedAt := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
			evt := model.MessageEvent{
				Event:     model.EventReacted,
				SiteID:    "site-a",
				Timestamp: reactedAt.UnixMilli(),
				Message: model.Message{
					ID: "m1", RoomID: roomID, UserAccount: tc.authorAccount,
					CreatedAt: reactedAt.Add(-time.Hour), UpdatedAt: &reactedAt,
				},
				ReactionDelta: &model.ReactionDelta{
					Shortcode: "thumbsup",
					Action:    tc.action,
					Actor:     model.Participant{UserID: "u-alice", Account: tc.actorAccount, EngName: "Alice"},
				},
			}
			data, err := json.Marshal(&evt)
			require.NoError(t, err)

			h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
			require.NoError(t, h.HandleMessage(context.Background(), data))

			notif := findPublishRecord(pub.records, subject.Notification(tc.authorAccount))
			if !tc.wantNotify {
				assert.Nil(t, notif, "policy gate must suppress the author notification")
				assert.Len(t, pub.records, 1, "only the room publish should have happened")
				return
			}
			require.NotNil(t, notif, "author notification must be published on chat.user.%s.notification", tc.authorAccount)
			require.Len(t, pub.records, 2, "room publish + author notification")
			var got model.NotificationEvent
			require.NoError(t, json.Unmarshal(notif.data, &got))
			assert.Equal(t, "reaction", got.Type)
			assert.Equal(t, model.RoomTypeChannel, got.RoomType)
			require.NotNil(t, got.ReactionDelta)
			assert.Equal(t, "thumbsup", got.ReactionDelta.Shortcode)
			assert.Equal(t, tc.actorAccount, got.ReactionDelta.Actor.Account)
		})
	}
}

func TestHandleReacted_AuthorNotification_RoomType(t *testing.T) {
	cases := []struct {
		name     string
		roomType model.RoomType
	}{
		{name: "channel room", roomType: model.RoomTypeChannel},
		{name: "dm room", roomType: model.RoomTypeDM},
		{name: "botDM room", roomType: model.RoomTypeBotDM},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockStore(ctrl)
			us := NewMockUserStore(ctrl)
			pub := &mockPublisher{}
			keyStore := NewMockRoomKeyProvider(ctrl)

			roomID := "r1"
			room := &model.Room{ID: roomID, Type: tc.roomType, SiteID: "site-a"}
			store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)

			reactedAt := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
			evt := model.MessageEvent{
				Event:     model.EventReacted,
				SiteID:    "site-a",
				Timestamp: reactedAt.UnixMilli(),
				Message: model.Message{
					ID: "m1", RoomID: roomID, UserAccount: "bob",
					CreatedAt: reactedAt.Add(-time.Hour), UpdatedAt: &reactedAt,
				},
				ReactionDelta: &model.ReactionDelta{
					Shortcode: "thumbsup",
					Action:    model.ReactionActionAdded,
					Actor:     model.Participant{UserID: "u-alice", Account: "alice", EngName: "Alice"},
				},
			}
			data, err := json.Marshal(&evt)
			require.NoError(t, err)

			h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
			require.NoError(t, h.HandleMessage(context.Background(), data))

			notif := findPublishRecord(pub.records, subject.Notification("bob"))
			require.NotNil(t, notif, "author notification must be published")
			var got model.NotificationEvent
			require.NoError(t, json.Unmarshal(notif.data, &got))
			assert.Equal(t, tc.roomType, got.RoomType)
		})
	}
}

// partialFailPublisher errors on one subject and records the rest.
type partialFailPublisher struct {
	mu       sync.Mutex
	records  []publishRecord
	failSubj string
	failErr  error
}

func (p *partialFailPublisher) Publish(_ context.Context, subj string, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if subj == p.failSubj {
		return p.failErr
	}
	p.records = append(p.records, publishRecord{subject: subj, data: data})
	return nil
}

func TestHandleReacted_AuthorPublishFailure_RoomFanOutStillSucceeds(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &partialFailPublisher{
		failSubj: subject.Notification("bob"),
		failErr:  errors.New("nats down"),
	}
	keyStore := NewMockRoomKeyProvider(ctrl)

	roomID := "r1"
	room := &model.Room{ID: roomID, Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)

	reactedAt := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	evt := model.MessageEvent{
		Event: model.EventReacted, SiteID: "site-a", Timestamp: reactedAt.UnixMilli(),
		Message: model.Message{
			ID: "m1", RoomID: roomID, UserAccount: "bob",
			CreatedAt: reactedAt.Add(-time.Hour), UpdatedAt: &reactedAt,
		},
		ReactionDelta: &model.ReactionDelta{
			Shortcode: "thumbsup",
			Action:    model.ReactionActionAdded,
			Actor:     model.Participant{Account: "alice"},
		},
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data),
		"author-notify failure must not NAK the canonical event")
	require.NotNil(t, findPublishRecord(pub.records, subject.RoomEvent(roomID, true)),
		"room fan-out must still have published")
}

func TestHandlePinned_ChannelRoomScopedPublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	roomID := "r1"
	room := &model.Room{ID: roomID, Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)

	pinnedAt := time.Date(2026, 5, 14, 12, 5, 0, 0, time.UTC)
	evt := model.MessageEvent{
		Event:     model.EventPinned,
		SiteID:    "site-a",
		Timestamp: pinnedAt.UnixMilli(),
		Message: model.Message{
			ID:          "msg-1",
			RoomID:      roomID,
			UserID:      "u-author",
			UserAccount: "author",
			CreatedAt:   time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
			PinnedAt:    &pinnedAt,
			PinnedBy:    &model.Participant{UserID: "u-alice", Account: "alice"},
		},
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	require.Len(t, pub.records, 1)
	c := pub.records[0]
	assert.Equal(t, subject.RoomEvent(roomID, true), c.subject)
	var roomEvt model.PinStateRoomEvent
	require.NoError(t, json.Unmarshal(c.data, &roomEvt))
	assert.Equal(t, model.RoomEventMessagePinned, roomEvt.Type)
	assert.Equal(t, roomID, roomEvt.RoomID)
	assert.Equal(t, "site-a", roomEvt.SiteID)
	assert.Equal(t, "msg-1", roomEvt.MessageID)
	assert.True(t, roomEvt.Pinned)
	require.NotNil(t, roomEvt.By)
	assert.Equal(t, "alice", roomEvt.By.Account)
	assert.True(t, roomEvt.At.Equal(pinnedAt))
}

func TestHandlePinned_DMRoom_FansOutToBothMembers(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	roomID := "dm-alice-bob"
	room := &model.Room{
		ID:       roomID,
		Type:     model.RoomTypeDM,
		SiteID:   "site-a",
		Accounts: []string{"alice", "bob"},
	}
	store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)

	pinnedAt := time.Date(2026, 5, 14, 12, 5, 0, 0, time.UTC)
	evt := model.MessageEvent{
		Event:     model.EventPinned,
		SiteID:    "site-a",
		Timestamp: pinnedAt.UnixMilli(),
		Message: model.Message{
			ID: "msg-1", RoomID: roomID, UserAccount: "alice",
			CreatedAt: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
			PinnedAt:  &pinnedAt,
			PinnedBy:  &model.Participant{UserID: "u-alice", Account: "alice"},
		},
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	require.Len(t, pub.records, 2)
	subjects := map[string]bool{}
	for _, c := range pub.records {
		subjects[c.subject] = true
		var roomEvt model.PinStateRoomEvent
		require.NoError(t, json.Unmarshal(c.data, &roomEvt))
		assert.Equal(t, model.RoomEventMessagePinned, roomEvt.Type)
		assert.Equal(t, "msg-1", roomEvt.MessageID)
		assert.True(t, roomEvt.Pinned)
	}
	assert.True(t, subjects[subject.UserRoomEvent("alice")])
	assert.True(t, subjects[subject.UserRoomEvent("bob")])
}

func TestHandlePinned_MissingPinnedAt_ReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	evt := model.MessageEvent{
		Event: model.EventPinned,
		Message: model.Message{
			ID: "msg-1", RoomID: "r1", UserAccount: "alice",
		},
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
	err = h.HandleMessage(context.Background(), data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing PinnedAt")
	assert.Empty(t, pub.records)
}

func TestHandleUnpinned_ChannelRoomScopedPublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	roomID := "r1"
	room := &model.Room{ID: roomID, Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)

	unpinnedAtMs := time.Date(2026, 5, 14, 12, 10, 0, 0, time.UTC).UnixMilli()
	evt := model.MessageEvent{
		Event:     model.EventUnpinned,
		SiteID:    "site-a",
		Timestamp: unpinnedAtMs,
		Message: model.Message{
			ID:        "msg-1",
			RoomID:    roomID,
			CreatedAt: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
			PinnedBy:  &model.Participant{UserID: "u-alice", Account: "alice"},
		},
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	require.Len(t, pub.records, 1)
	c := pub.records[0]
	assert.Equal(t, subject.RoomEvent(roomID, true), c.subject)
	var roomEvt model.PinStateRoomEvent
	require.NoError(t, json.Unmarshal(c.data, &roomEvt))
	assert.Equal(t, model.RoomEventMessageUnpinned, roomEvt.Type)
	assert.Equal(t, "msg-1", roomEvt.MessageID)
	assert.False(t, roomEvt.Pinned)
	require.NotNil(t, roomEvt.By)
	assert.Equal(t, "alice", roomEvt.By.Account)
	assert.Equal(t, unpinnedAtMs, roomEvt.At.UnixMilli())
}

func TestHandleUnpinned_DMRoom_FansOutToBothMembers(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	roomID := "dm-alice-bob"
	room := &model.Room{
		ID:       roomID,
		Type:     model.RoomTypeDM,
		SiteID:   "site-a",
		Accounts: []string{"alice", "bob"},
	}
	store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)

	unpinnedAtMs := time.Date(2026, 5, 14, 12, 10, 0, 0, time.UTC).UnixMilli()
	evt := model.MessageEvent{
		Event:     model.EventUnpinned,
		SiteID:    "site-a",
		Timestamp: unpinnedAtMs,
		Message: model.Message{
			ID: "msg-1", RoomID: roomID, UserAccount: "alice",
			CreatedAt: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
			PinnedBy:  &model.Participant{UserID: "u-alice", Account: "alice"},
		},
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	require.Len(t, pub.records, 2)
	subjects := map[string]bool{}
	for _, c := range pub.records {
		subjects[c.subject] = true
		var roomEvt model.PinStateRoomEvent
		require.NoError(t, json.Unmarshal(c.data, &roomEvt))
		assert.Equal(t, model.RoomEventMessageUnpinned, roomEvt.Type)
		assert.False(t, roomEvt.Pinned)
	}
	assert.True(t, subjects[subject.UserRoomEvent("alice")])
	assert.True(t, subjects[subject.UserRoomEvent("bob")])
}

// ---------------------------------------------------------------------------
// Thread handler tests
// ---------------------------------------------------------------------------

func TestThreadFanOutAccounts(t *testing.T) {
	tests := []struct {
		name          string
		sender        string
		parentSender  string
		followers     map[string]struct{}
		extraAccounts []string
		want          []string
	}{
		{
			name:          "sender alone still notified (own devices, no other followers)",
			sender:        "alice",
			followers:     map[string]struct{}{},
			extraAccounts: nil,
			want:          []string{"alice"},
		},
		{
			name:          "sender included even when not yet in replyAccounts (race-free)",
			sender:        "alice",
			followers:     map[string]struct{}{"bob": {}},
			extraAccounts: nil,
			want:          []string{"alice", "bob"},
		},
		{
			name:      "sender included when also a follower (multi-device support)",
			sender:    "alice",
			followers: map[string]struct{}{"alice": {}, "bob": {}},
			want:      []string{"alice", "bob"},
		},
		{
			name:          "sender included when only in extra accounts",
			sender:        "alice",
			followers:     map[string]struct{}{"bob": {}},
			extraAccounts: []string{"alice"},
			want:          []string{"bob", "alice"},
		},
		{
			name:          "extra accounts merged deduped",
			sender:        "alice",
			followers:     map[string]struct{}{"bob": {}},
			extraAccounts: []string{"bob", "carol"},
			want:          []string{"alice", "bob", "carol"},
		},
		{
			name:          "bot accounts skipped even if sender is bot",
			sender:        "helper.bot",
			followers:     map[string]struct{}{"helper.bot": {}, "bob": {}},
			extraAccounts: []string{"other.bot"},
			want:          []string{"bob"},
		},
		{
			name:          "sender not duplicated when in both followers and extras",
			sender:        "alice",
			followers:     map[string]struct{}{"alice": {}, "bob": {}},
			extraAccounts: []string{"alice", "carol"},
			want:          []string{"alice", "bob", "carol"},
		},
		{
			name:         "parent sender always included, even without a thread_rooms row yet",
			sender:       "alice",
			parentSender: "carol",
			followers:    map[string]struct{}{},
			want:         []string{"alice", "carol"},
		},
		{
			name:         "parent sender deduped against sender and followers",
			sender:       "alice",
			parentSender: "alice",
			followers:    map[string]struct{}{"bob": {}},
			want:         []string{"alice", "bob"},
		},
		{
			name:         "bot parent sender skipped",
			sender:       "alice",
			parentSender: "system.bot",
			followers:    map[string]struct{}{"bob": {}},
			want:         []string{"alice", "bob"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := threadFanOutAccounts(tc.sender, tc.parentSender, tc.followers, tc.extraAccounts)
			assert.ElementsMatch(t, tc.want, got)
		})
	}
}

func TestHandleServerBroadcast_ThreadReplyAdded_FansOutBadge(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	tcount := 3
	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)

	evt := model.MessageEvent{
		Event:     model.EventThreadReplyAdded,
		SiteID:    "site-a",
		Timestamp: msgTime.UnixMilli(),
		NewTCount: &tcount,
		Message: model.Message{
			ID:                    "reply-1",
			RoomID:                "r1",
			UserAccount:           "alice",
			ThreadParentMessageID: "parent-1",
			CreatedAt:             msgTime,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	h.HandleServerBroadcast(context.Background(), data)

	require.Len(t, pub.records, 1)
	var tmEvt model.ThreadMetadataUpdatedEvent
	require.NoError(t, json.Unmarshal(pub.records[0].data, &tmEvt))
	assert.Equal(t, model.RoomEventThreadMetadataUpdated, tmEvt.Type)
	assert.Equal(t, "r1", tmEvt.RoomID)
	assert.Equal(t, "site-a", tmEvt.SiteID)
	assert.Equal(t, "parent-1", tmEvt.ParentMessageID)
	assert.Equal(t, "reply-1", tmEvt.ReplyMessageID)
	assert.Equal(t, 3, tmEvt.NewTCount)
	assert.Equal(t, model.ThreadActionReplyAdded, tmEvt.Action)
	assert.Positive(t, tmEvt.Timestamp, "Timestamp must be the broadcast-worker publish time")
	assert.Equal(t, msgTime.UnixMilli(), tmEvt.EventTimestamp)
}

func TestHandleServerBroadcast_ThreadReplyAdded_PropagatesNewTlm(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 6, 18, 10, 0, 0, 0, time.UTC)
	tcount := 1
	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)

	evt := model.MessageEvent{
		Event:              model.EventThreadReplyAdded,
		SiteID:             "site-a",
		Timestamp:          msgTime.UnixMilli(),
		NewTCount:          &tcount,
		NewThreadLastMsgAt: &msgTime,
		Message: model.Message{
			ID:                    "reply-1",
			RoomID:                "r1",
			UserAccount:           "alice",
			ThreadParentMessageID: "parent-1",
			CreatedAt:             msgTime,
		},
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	h.HandleServerBroadcast(context.Background(), data)

	require.Len(t, pub.records, 1)
	var tmEvt model.ThreadMetadataUpdatedEvent
	require.NoError(t, json.Unmarshal(pub.records[0].data, &tmEvt))
	require.NotNil(t, tmEvt.NewThreadLastMsgAt, "NewThreadLastMsgAt must be forwarded to ThreadMetadataUpdatedEvent")
	assert.True(t, tmEvt.NewThreadLastMsgAt.Equal(msgTime))
}

func TestHandleServerBroadcast_ThreadReplyAdded_NilNewTlm_NoFieldInOutput(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 6, 18, 10, 0, 0, 0, time.UTC)
	tcount := 1
	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)

	evt := model.MessageEvent{
		Event:     model.EventThreadReplyAdded,
		SiteID:    "site-a",
		Timestamp: msgTime.UnixMilli(),
		NewTCount: &tcount,
		// NewThreadLastMsgAt intentionally nil
		Message: model.Message{
			ID:                    "reply-1",
			RoomID:                "r1",
			UserAccount:           "alice",
			ThreadParentMessageID: "parent-1",
			CreatedAt:             msgTime,
		},
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	h.HandleServerBroadcast(context.Background(), data)

	require.Len(t, pub.records, 1)
	var tmEvt model.ThreadMetadataUpdatedEvent
	require.NoError(t, json.Unmarshal(pub.records[0].data, &tmEvt))
	assert.Nil(t, tmEvt.NewThreadLastMsgAt, "nil NewThreadLastMsgAt must not appear in downstream event")
	// Also verify via raw JSON — no newThreadLastMsgAt key.
	var raw map[string]any
	require.NoError(t, json.Unmarshal(pub.records[0].data, &raw))
	_, present := raw["newThreadLastMsgAt"]
	assert.False(t, present, "newThreadLastMsgAt must be absent from JSON when nil")
}

func TestHandleServerBroadcast_ThreadReplyAdded_MissingNewTCount_Skips(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)
	// No store calls expected — event is silently dropped.

	msgTime := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	evt := model.MessageEvent{
		Event:     model.EventThreadReplyAdded,
		SiteID:    "site-a",
		Timestamp: msgTime.UnixMilli(),
		// NewTCount intentionally nil
		Message: model.Message{
			ID:                    "reply-1",
			RoomID:                "r1",
			UserAccount:           "alice",
			ThreadParentMessageID: "parent-1",
			CreatedAt:             msgTime,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	h.HandleServerBroadcast(context.Background(), data)
	assert.Empty(t, pub.records)
}

func TestHandleServerBroadcast_ThreadReplyAdded_MissingParentMessageID_Skips(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	tcount := 2
	evt := model.MessageEvent{
		Event:     model.EventThreadReplyAdded,
		SiteID:    "site-a",
		Timestamp: msgTime.UnixMilli(),
		NewTCount: &tcount,
		Message: model.Message{
			ID:          "reply-1",
			RoomID:      "r1",
			UserAccount: "alice",
			// ThreadParentMessageID intentionally empty
			CreatedAt: msgTime,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	h.HandleServerBroadcast(context.Background(), data)
	assert.Empty(t, pub.records)
}

func TestHandleServerBroadcast_ThreadReplyAdded_GetRoomError_LogsAndContinues(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	tcount := 2
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(nil, errors.New("db error"))

	evt := model.MessageEvent{
		Event:     model.EventThreadReplyAdded,
		SiteID:    "site-a",
		Timestamp: msgTime.UnixMilli(),
		NewTCount: &tcount,
		Message: model.Message{
			ID:                    "reply-1",
			RoomID:                "r1",
			UserAccount:           "alice",
			ThreadParentMessageID: "parent-1",
			CreatedAt:             msgTime,
		},
	}
	data, _ := json.Marshal(evt)

	// HandleServerBroadcast is fire-and-forget: errors are logged, not returned.
	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	h.HandleServerBroadcast(context.Background(), data)
	assert.Empty(t, pub.records)
}

func TestHandleThreadCreated_ChannelRoom_FansOutToFollowers(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	parentMsgID := "parent-1"
	siteID := "site-a"
	roomID := "r1"

	followers := map[string]struct{}{"bob": {}, "carol": {}}
	store.EXPECT().GetRoomMeta(gomock.Any(), roomID).Return(metaOf(testChannelRoom), nil)
	store.EXPECT().GetThreadFollowers(gomock.Any(), parentMsgID).Return(followers, nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice"}).Return([]model.User{testUsers[0]}, nil)

	evt := model.MessageEvent{
		Event:     model.EventCreated,
		SiteID:    siteID,
		Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID:                    "reply-1",
			RoomID:                roomID,
			UserID:                "u-alice",
			UserAccount:           "alice",
			Content:               "a thread reply",
			CreatedAt:             msgTime,
			ThreadParentMessageID: parentMsgID,
			TShow:                 false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	// bob and carol (followers) + alice (sender) included for multi-device parity
	require.Len(t, pub.records, 3)
	subjects := map[string]bool{}
	for _, r := range pub.records {
		subjects[r.subject] = true
		var roomEvt model.RoomEvent
		require.NoError(t, json.Unmarshal(r.data, &roomEvt))
		assert.Equal(t, model.RoomEventNewThreadMessage, roomEvt.Type, "thread reply must publish new_thread_message, not new_message")
		assert.Positive(t, roomEvt.Timestamp, "Timestamp must be the broadcast-worker publish time")
		assert.Equal(t, msgTime.UnixMilli(), roomEvt.EventTimestamp)
	}
	assert.True(t, subjects[subject.UserRoomEvent("alice")])
	assert.True(t, subjects[subject.UserRoomEvent("bob")])
	assert.True(t, subjects[subject.UserRoomEvent("carol")])
}

func TestHandleThreadCreated_ChannelRoom_NoFollowers_SendsToSenderOnly(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)

	store.EXPECT().GetRoomMeta(gomock.Any(), "r1").Return(metaOf(testChannelRoom), nil)
	store.EXPECT().GetThreadFollowers(gomock.Any(), "parent-1").Return(map[string]struct{}{}, nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice"}).Return([]model.User{testUsers[0]}, nil)

	evt := model.MessageEvent{
		Event:     model.EventCreated,
		SiteID:    "site-a",
		Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID:                    "reply-1",
			RoomID:                "r1",
			UserID:                "u-alice",
			UserAccount:           "alice",
			Content:               "hello",
			CreatedAt:             msgTime,
			ThreadParentMessageID: "parent-1",
			TShow:                 false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))
	// No other followers → sender still receives their own echo (multi-device parity).
	require.Len(t, pub.records, 1)
	assert.Equal(t, subject.UserRoomEvent("alice"), pub.records[0].subject)
}

// Race regression guard: on the first reply thread_rooms may not exist yet, so
// GetThreadFollowers returns empty and replyAccounts is unavailable. The parent
// author (fetched from history-service) must still receive the reply — they are
// never in replyAccounts on the first reply, so an empty follower set must not drop
// them.
func TestHandleThreadCreated_ChannelRoom_ParentAuthorFannedOutBeforeThreadRoomExists(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	parentAt := msgTime.Add(-time.Hour)

	// thread_rooms not created yet → no followers; parent authored by carol.
	store.EXPECT().GetRoomMeta(gomock.Any(), "r1").Return(metaOf(testChannelRoom), nil)
	store.EXPECT().GetThreadFollowers(gomock.Any(), "parent-1").Return(map[string]struct{}{}, nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice"}).Return([]model.User{testUsers[0]}, nil)
	parentFetcher := stubParentFetcher{info: &ParentMessageInfo{SenderAccount: "carol", CreatedAt: parentAt}}

	evt := model.MessageEvent{
		Event:     model.EventCreated,
		SiteID:    "site-a",
		Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID:                    "reply-1",
			RoomID:                "r1",
			UserID:                "u-alice",
			UserAccount:           "alice",
			Content:               "a plain thread reply with no mentions",
			CreatedAt:             msgTime,
			ThreadParentMessageID: "parent-1",
			TShow:                 false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, parentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	got := map[string]bool{}
	for _, r := range pub.records {
		got[r.subject] = true
	}
	require.Len(t, pub.records, 2)
	assert.True(t, got[subject.UserRoomEvent("alice")], "reply sender receives their own echo")
	assert.True(t, got[subject.UserRoomEvent("carol")], "parent author receives the reply despite empty replyAccounts")
}

func TestHandleThreadCreated_DMRoom_FansOutToAllMembers(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 11, 0, 0, 0, time.UTC)

	store.EXPECT().GetRoomMeta(gomock.Any(), "dm-1").Return(metaOf(testDMRoom), nil)
	store.EXPECT().ListSubscriptions(gomock.Any(), "dm-1").Return(testDMSubs, nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice"}).Return([]model.User{testUsers[0]}, nil)

	evt := model.MessageEvent{
		Event:     model.EventCreated,
		SiteID:    "site-a",
		Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID:                    "reply-1",
			RoomID:                "dm-1",
			UserID:                "u-alice",
			UserAccount:           "alice",
			Content:               "thread reply in DM",
			CreatedAt:             msgTime,
			ThreadParentMessageID: "parent-dm",
			TShow:                 false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	require.Len(t, pub.records, 2, "DM thread reply fans out to all members")
	subjects := map[string]bool{}
	for _, r := range pub.records {
		subjects[r.subject] = true
		roomEvt := decodeRoomEvent(t, r.data)
		assert.Equal(t, model.RoomEventNewThreadMessage, roomEvt.Type, "DM thread reply must publish new_thread_message, not new_message")
	}
	assert.True(t, subjects[subject.UserRoomEvent("alice")])
	assert.True(t, subjects[subject.UserRoomEvent("bob")])
}

func TestHandleThreadCreated_BotDMRoom_EmitsNewThreadMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 11, 0, 0, 0, time.UTC)

	// botDM thread replies fan out per member — the human member gets the event,
	// the bot account is skipped (same as an ordinary botDM new_message). The
	// production branch handles RoomTypeBotDM too.
	botDMRoom := &model.Room{ID: "dm-1", Type: model.RoomTypeBotDM, SiteID: "site-a", UserCount: 2}
	store.EXPECT().GetRoomMeta(gomock.Any(), "dm-1").Return(metaOf(botDMRoom), nil)
	store.EXPECT().ListSubscriptions(gomock.Any(), "dm-1").Return(testDMSubs, nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice"}).Return([]model.User{testUsers[0]}, nil)

	evt := model.MessageEvent{
		Event:     model.EventCreated,
		SiteID:    "site-a",
		Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "reply-1", RoomID: "dm-1", UserID: "u-alice", UserAccount: "alice",
			Content: "thread reply in botDM", CreatedAt: msgTime,
			ThreadParentMessageID: "parent-dm", TShow: false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	require.NotEmpty(t, pub.records, "botDM thread reply fans out per member")
	for _, r := range pub.records {
		roomEvt := decodeRoomEvent(t, r.data)
		assert.Equal(t, model.RoomEventNewThreadMessage, roomEvt.Type, "botDM thread reply must publish new_thread_message")
	}
}

func TestHandleThreadCreated_DMRoom_WithMention_NoSubscriptionWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 11, 0, 0, 0, time.UTC)

	// DM thread-reply mention badges are owned entirely by message-worker
	// (markThreadMentions on the thread_subscriptions row); broadcast-worker touches
	// no subscription here. SetSubscriptionMentions (room sub) must NOT be called —
	// no EXPECT registered.
	store.EXPECT().GetRoomMeta(gomock.Any(), "dm-1").Return(metaOf(testDMRoom), nil)
	store.EXPECT().ListSubscriptions(gomock.Any(), "dm-1").Return(testDMSubs, nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice", "bob"}).Return(testUsers, nil)

	evt := model.MessageEvent{
		Event:     model.EventCreated,
		SiteID:    "site-a",
		Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID:                    "reply-1",
			RoomID:                "dm-1",
			UserID:                "u-alice",
			UserAccount:           "alice",
			Content:               "hey @bob",
			CreatedAt:             msgTime,
			ThreadParentMessageID: "parent-dm",
			TShow:                 false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))
	require.Len(t, pub.records, 2)
}

func TestHandleThreadCreated_ChannelExcludesRestrictedAndNonMemberMentions(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	parentAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	joinedAfter := parentAt.Add(time.Hour)
	msgTime := parentAt.Add(2 * time.Hour)

	// bob: member, full access → included. carol: member, joined after parent → excluded.
	// dave: absent from the map → non-member → excluded.
	store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
	store.EXPECT().GetHistorySharedSince(gomock.Any(), "room-1", gomock.Any()).
		Return(map[string]*time.Time{"bob": nil, "carol": &joinedAfter}, nil)
	store.EXPECT().GetThreadFollowers(gomock.Any(), "parent-1").Return(map[string]struct{}{}, nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return(testUsers, nil)

	evt := model.MessageEvent{
		Event:     model.EventCreated,
		SiteID:    "site-a",
		Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID:                    "reply-1",
			RoomID:                "room-1",
			UserID:                "u-alice",
			UserAccount:           "alice",
			Content:               "@bob @carol @dave hi",
			CreatedAt:             msgTime,
			ThreadParentMessageID: "parent-1",
			TShow:                 false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, stubParentFetcher{info: &ParentMessageInfo{CreatedAt: parentAt}}, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	got := map[string]bool{}
	for _, r := range pub.records {
		got[r.subject] = true
	}
	assert.True(t, got[subject.UserRoomEvent("alice")], "sender receives their own echo")
	assert.True(t, got[subject.UserRoomEvent("bob")], "unrestricted member mentionee receives the reply")
	assert.False(t, got[subject.UserRoomEvent("carol")], "member who joined after the parent is excluded")
	assert.False(t, got[subject.UserRoomEvent("dave")], "non-member mentionee is excluded")
}

// When the gatekeeper already resolved the parent, the event carries both the
// parent createdAt and the parent sender account. broadcast-worker must use them
// directly and skip the history-service FetchParent round-trip — while still
// delivering to the parent author (race-free) and gating mentions by the event's
// createdAt. The MockParentFetcher registers no EXPECT, so any FetchParent call
// fails the test.
func TestHandleThreadCreated_ChannelRoom_UsesEventParent_SkipsFetch(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)
	parentFetcher := NewMockParentFetcher(ctrl) // no EXPECT → FetchParent must NOT be called

	parentAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	joinedAfter := parentAt.Add(time.Hour)
	msgTime := parentAt.Add(2 * time.Hour)

	// bob: unrestricted member → included. carol: joined after the parent → excluded
	// (proves the gate ran against the event-carried createdAt, not a fetched value).
	store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
	store.EXPECT().GetHistorySharedSince(gomock.Any(), "room-1", gomock.Any()).
		Return(map[string]*time.Time{"bob": nil, "carol": &joinedAfter}, nil)
	store.EXPECT().GetThreadFollowers(gomock.Any(), "parent-1").Return(map[string]struct{}{}, nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return(testUsers, nil)

	evt := model.MessageEvent{
		Event:                     model.EventCreated,
		SiteID:                    "site-a",
		Timestamp:                 msgTime.UnixMilli(),
		ThreadParentSenderAccount: "zoe", // gatekeeper-resolved parent author
		Message: model.Message{
			ID:                           "reply-1",
			RoomID:                       "room-1",
			UserID:                       "u-alice",
			UserAccount:                  "alice",
			Content:                      "@bob @carol hi",
			CreatedAt:                    msgTime,
			ThreadParentMessageID:        "parent-1",
			ThreadParentMessageCreatedAt: &parentAt, // gatekeeper-resolved createdAt
			TShow:                        false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, parentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	got := map[string]bool{}
	for _, r := range pub.records {
		got[r.subject] = true
	}
	assert.True(t, got[subject.UserRoomEvent("alice")], "sender receives their own echo")
	assert.True(t, got[subject.UserRoomEvent("zoe")], "parent author (from event) receives the reply race-free")
	assert.True(t, got[subject.UserRoomEvent("bob")], "unrestricted member mentionee receives the reply")
	assert.False(t, got[subject.UserRoomEvent("carol")], "member who joined after the event-carried parent createdAt is excluded")
}

// When the event lacks the parent sender account (e.g. gatekeeper soft-fail),
// broadcast-worker must fall back to FetchParent even if createdAt is present —
// both values come from the same fetch.
func TestHandleThreadCreated_ChannelRoom_MissingSenderAccount_FallsBackToFetch(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)
	parentFetcher := NewMockParentFetcher(ctrl)

	msgTime := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	parentAt := msgTime.Add(-time.Hour)

	store.EXPECT().GetRoomMeta(gomock.Any(), "r1").Return(metaOf(testChannelRoom), nil)
	store.EXPECT().GetThreadFollowers(gomock.Any(), "parent-1").Return(map[string]struct{}{}, nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice"}).Return([]model.User{testUsers[0]}, nil)
	// createdAt present but no sender account → must fetch (returns the parent author).
	parentFetcher.EXPECT().
		FetchParent(gomock.Any(), "alice", "r1", "site-a", "parent-1").
		Return(&ParentMessageInfo{SenderAccount: "carol", CreatedAt: parentAt}, nil)

	evt := model.MessageEvent{
		Event:     model.EventCreated,
		SiteID:    "site-a",
		Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID:                           "reply-1",
			RoomID:                       "r1",
			UserID:                       "u-alice",
			UserAccount:                  "alice",
			Content:                      "a plain thread reply",
			CreatedAt:                    msgTime,
			ThreadParentMessageID:        "parent-1",
			ThreadParentMessageCreatedAt: &parentAt,
			TShow:                        false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, parentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	got := map[string]bool{}
	for _, r := range pub.records {
		got[r.subject] = true
	}
	assert.True(t, got[subject.UserRoomEvent("alice")], "reply sender receives their own echo")
	assert.True(t, got[subject.UserRoomEvent("carol")], "parent author (from fetch fallback) receives the reply")
}

func TestHandleThreadUpdated_ChannelRoom_FansOutToFollowers(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	editedAt := msgTime.Add(time.Minute)
	parentMsgID := "parent-1"
	siteID := "site-a"
	roomID := "r1"

	room := &model.Room{ID: roomID, Type: model.RoomTypeChannel, SiteID: siteID}
	followers := map[string]struct{}{"bob": {}, "carol": {}}
	store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)
	store.EXPECT().GetThreadFollowers(gomock.Any(), parentMsgID).Return(followers, nil)

	evt := model.MessageEvent{
		Event:     model.EventUpdated,
		SiteID:    siteID,
		Timestamp: editedAt.UnixMilli(),
		Message: model.Message{
			ID:                    "reply-1",
			RoomID:                roomID,
			UserAccount:           "alice",
			Content:               "updated thread reply",
			CreatedAt:             msgTime,
			EditedAt:              &editedAt,
			UpdatedAt:             &editedAt,
			ThreadParentMessageID: parentMsgID,
			TShow:                 false,
		},
		// Thread replies carry no room preview (history-service skips the walk); clients
		// tell it apart via threadParentMessageId instead.
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	// bob and carol (followers) + alice (sender) included for multi-device parity
	require.Len(t, pub.records, 3)
	subjects := map[string]bool{}
	for _, r := range pub.records {
		subjects[r.subject] = true
		var roomEvt model.EditRoomEvent
		require.NoError(t, json.Unmarshal(r.data, &roomEvt))
		assert.Equal(t, model.RoomEventMessageEdited, roomEvt.Type)
		assert.Equal(t, "reply-1", roomEvt.MessageID)
		assert.Equal(t, "updated thread reply", roomEvt.NewContent)
		assert.Positive(t, roomEvt.Timestamp, "Timestamp must be the broadcast-worker publish time")
		assert.Equal(t, editedAt.UnixMilli(), roomEvt.EventTimestamp)
		assert.Equal(t, parentMsgID, roomEvt.ThreadParentMessageID, "thread edit must carry threadParentMessageId so clients can tell it's a thread reply")
		assert.Empty(t, roomEvt.PreviewMessage, "thread edit carries no room preview")
	}
	assert.True(t, subjects[subject.UserRoomEvent("alice")])
	assert.True(t, subjects[subject.UserRoomEvent("bob")])
	assert.True(t, subjects[subject.UserRoomEvent("carol")])
}

func TestHandleThreadUpdated_ChannelExcludesRestrictedAndNonMemberMentions(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	parentAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	joinedAfter := parentAt.Add(time.Hour)
	msgTime := parentAt.Add(2 * time.Hour)
	editedAt := msgTime.Add(time.Minute)

	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)
	// bob: member full access → included. carol: joined after parent → excluded.
	// dave: absent → non-member → excluded.
	store.EXPECT().GetHistorySharedSince(gomock.Any(), "r1", gomock.Any()).
		Return(map[string]*time.Time{"bob": nil, "carol": &joinedAfter}, nil)
	store.EXPECT().GetThreadFollowers(gomock.Any(), "parent-1").Return(map[string]struct{}{}, nil)

	evt := model.MessageEvent{
		Event:     model.EventUpdated,
		SiteID:    "site-a",
		Timestamp: editedAt.UnixMilli(),
		Message: model.Message{
			ID:                    "reply-1",
			RoomID:                "r1",
			UserAccount:           "alice",
			Content:               "@bob @carol @dave hi",
			CreatedAt:             msgTime,
			EditedAt:              &editedAt,
			UpdatedAt:             &editedAt,
			ThreadParentMessageID: "parent-1",
			TShow:                 false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, stubParentFetcher{info: &ParentMessageInfo{CreatedAt: parentAt}}, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	got := map[string]bool{}
	for _, r := range pub.records {
		got[r.subject] = true
	}
	assert.True(t, got[subject.UserRoomEvent("alice")], "sender receives their own echo")
	assert.True(t, got[subject.UserRoomEvent("bob")], "unrestricted member mentionee receives the edit")
	assert.False(t, got[subject.UserRoomEvent("carol")], "member who joined after the parent is excluded")
	assert.False(t, got[subject.UserRoomEvent("dave")], "non-member mentionee is excluded")
}

func TestHandleThreadDeleted_ChannelExcludesRestrictedAndNonMemberMentions(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	parentAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	joinedAfter := parentAt.Add(time.Hour)
	msgTime := parentAt.Add(2 * time.Hour)
	deletedAt := msgTime.Add(time.Minute)

	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)
	store.EXPECT().GetHistorySharedSince(gomock.Any(), "r1", gomock.Any()).
		Return(map[string]*time.Time{"bob": nil, "carol": &joinedAfter}, nil)
	store.EXPECT().GetThreadFollowers(gomock.Any(), "parent-1").Return(map[string]struct{}{}, nil)

	evt := model.MessageEvent{
		Event:     model.EventDeleted,
		SiteID:    "site-a",
		Timestamp: deletedAt.UnixMilli(),
		Message: model.Message{
			ID:                    "reply-1",
			RoomID:                "r1",
			UserAccount:           "alice",
			Content:               "@bob @carol @dave hi",
			CreatedAt:             msgTime,
			UpdatedAt:             &deletedAt,
			ThreadParentMessageID: "parent-1",
			TShow:                 false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, stubParentFetcher{info: &ParentMessageInfo{CreatedAt: parentAt}}, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	got := map[string]bool{}
	for _, r := range pub.records {
		got[r.subject] = true
	}
	assert.True(t, got[subject.UserRoomEvent("alice")], "sender receives their own echo")
	assert.True(t, got[subject.UserRoomEvent("bob")], "unrestricted member mentionee receives the delete")
	assert.False(t, got[subject.UserRoomEvent("carol")], "member who joined after the parent is excluded")
	assert.False(t, got[subject.UserRoomEvent("dave")], "non-member mentionee is excluded")
}

func TestHandleThreadUpdated_ChannelRoom_GetThreadFollowersError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	editedAt := msgTime.Add(time.Minute)

	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)
	store.EXPECT().GetThreadFollowers(gomock.Any(), "parent-1").Return(nil, errors.New("db error"))

	evt := model.MessageEvent{
		Event:     model.EventUpdated,
		SiteID:    "site-a",
		Timestamp: editedAt.UnixMilli(),
		Message: model.Message{
			ID:                    "reply-1",
			RoomID:                "r1",
			UserAccount:           "alice",
			Content:               "edit",
			CreatedAt:             msgTime,
			EditedAt:              &editedAt,
			UpdatedAt:             &editedAt,
			ThreadParentMessageID: "parent-1",
			TShow:                 false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	err := h.HandleMessage(context.Background(), data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "thread fan-out")
	assert.Empty(t, pub.records)
}

func TestHandleThreadUpdated_DMRoom_FansOutToAllMembers(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 11, 0, 0, 0, time.UTC)
	editedAt := msgTime.Add(time.Minute)

	room := &model.Room{
		ID:       "dm-alice-bob",
		Type:     model.RoomTypeDM,
		SiteID:   "site-a",
		Accounts: []string{"alice", "bob"},
	}
	store.EXPECT().GetRoom(gomock.Any(), "dm-alice-bob").Return(room, nil)

	evt := model.MessageEvent{
		Event:     model.EventUpdated,
		SiteID:    "site-a",
		Timestamp: editedAt.UnixMilli(),
		Message: model.Message{
			ID:                    "reply-1",
			RoomID:                "dm-alice-bob",
			UserAccount:           "alice",
			Content:               "dm thread edit",
			CreatedAt:             msgTime,
			EditedAt:              &editedAt,
			UpdatedAt:             &editedAt,
			ThreadParentMessageID: "parent-dm",
			TShow:                 false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	require.Len(t, pub.records, 2)
	subjects := map[string]bool{}
	for _, r := range pub.records {
		subjects[r.subject] = true
		var roomEvt model.EditRoomEvent
		require.NoError(t, json.Unmarshal(r.data, &roomEvt))
		assert.Equal(t, model.RoomEventMessageEdited, roomEvt.Type)
		assert.Equal(t, "dm thread edit", roomEvt.NewContent)
	}
	assert.True(t, subjects[subject.UserRoomEvent("alice")])
	assert.True(t, subjects[subject.UserRoomEvent("bob")])
}

func TestHandleThreadDeleted_ChannelRoom_FansOutToFollowers(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	deletedAt := msgTime.Add(time.Minute)
	parentMsgID := "parent-1"
	siteID := "site-a"
	roomID := "r1"

	room := &model.Room{ID: roomID, Type: model.RoomTypeChannel, SiteID: siteID}
	followers := map[string]struct{}{"bob": {}, "carol": {}}
	store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)
	store.EXPECT().GetThreadFollowers(gomock.Any(), parentMsgID).Return(followers, nil)
	// No NewTCount → no badge update.

	evt := model.MessageEvent{
		Event:     model.EventDeleted,
		SiteID:    siteID,
		Timestamp: deletedAt.UnixMilli(),
		Message: model.Message{
			ID:                    "reply-1",
			RoomID:                roomID,
			UserAccount:           "alice",
			Content:               "deleted thread reply",
			CreatedAt:             msgTime,
			UpdatedAt:             &deletedAt,
			ThreadParentMessageID: parentMsgID,
			TShow:                 false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	// bob and carol (followers) + alice (sender) included for multi-device parity
	require.Len(t, pub.records, 3)
	subjects := map[string]bool{}
	for _, r := range pub.records {
		subjects[r.subject] = true
		var roomEvt model.DeleteRoomEvent
		require.NoError(t, json.Unmarshal(r.data, &roomEvt))
		assert.Equal(t, model.RoomEventMessageDeleted, roomEvt.Type)
		assert.Equal(t, "reply-1", roomEvt.MessageID)
		assert.Positive(t, roomEvt.Timestamp, "Timestamp must be the broadcast-worker publish time")
		assert.Equal(t, deletedAt.UnixMilli(), roomEvt.EventTimestamp)
	}
	assert.True(t, subjects[subject.UserRoomEvent("alice")])
	assert.True(t, subjects[subject.UserRoomEvent("bob")])
	assert.True(t, subjects[subject.UserRoomEvent("carol")])
}

func TestHandleThreadDeleted_ChannelRoom_WithBadgeUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	deletedAt := msgTime.Add(time.Minute)
	tcount := 4
	// tlm = the newest surviving reply's createdAt, carried on the canonical delete event.
	survivingTlm := msgTime.Add(-time.Hour)

	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)
	store.EXPECT().GetThreadFollowers(gomock.Any(), "parent-1").Return(map[string]struct{}{"bob": {}}, nil)

	evt := model.MessageEvent{
		Event:              model.EventDeleted,
		SiteID:             "site-a",
		Timestamp:          deletedAt.UnixMilli(),
		NewTCount:          &tcount,
		NewThreadLastMsgAt: &survivingTlm,
		Message: model.Message{
			ID:                    "reply-1",
			RoomID:                "r1",
			UserAccount:           "alice",
			Content:               "",
			CreatedAt:             msgTime,
			UpdatedAt:             &deletedAt,
			ThreadParentMessageID: "parent-1",
			TShow:                 false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	// delete events to bob (follower) + alice (sender, multi-device parity) + 1 badge update (to room channel)
	require.Len(t, pub.records, 3)
	var sawBadge bool
	deleteSubjects := map[string]bool{}
	for _, r := range pub.records {
		if r.subject == subject.RoomEvent("r1", true) {
			var tmEvt model.ThreadMetadataUpdatedEvent
			require.NoError(t, json.Unmarshal(r.data, &tmEvt))
			assert.Equal(t, model.ThreadActionReplyDeleted, tmEvt.Action)
			assert.Equal(t, 4, tmEvt.NewTCount)
			require.NotNil(t, tmEvt.NewThreadLastMsgAt, "badge must forward the surviving thread last-message time on delete")
			assert.True(t, tmEvt.NewThreadLastMsgAt.Equal(survivingTlm), "NewThreadLastMsgAt must equal the newest surviving reply's createdAt")
			sawBadge = true
		} else {
			var roomEvt model.DeleteRoomEvent
			require.NoError(t, json.Unmarshal(r.data, &roomEvt))
			assert.Equal(t, model.RoomEventMessageDeleted, roomEvt.Type)
			deleteSubjects[r.subject] = true
		}
	}
	assert.True(t, deleteSubjects[subject.UserRoomEvent("bob")], "delete event must be published to follower")
	assert.True(t, deleteSubjects[subject.UserRoomEvent("alice")], "delete event must be published to sender for multi-device parity")
	assert.True(t, sawBadge, "badge update must be published to room channel")
}

func TestHandleThreadDeleted_DMRoom_FansOutToAllMembers(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 11, 0, 0, 0, time.UTC)
	deletedAt := msgTime.Add(time.Minute)

	room := &model.Room{
		ID:       "dm-alice-bob",
		Type:     model.RoomTypeDM,
		SiteID:   "site-a",
		Accounts: []string{"alice", "bob"},
	}
	store.EXPECT().GetRoom(gomock.Any(), "dm-alice-bob").Return(room, nil)

	evt := model.MessageEvent{
		Event:     model.EventDeleted,
		SiteID:    "site-a",
		Timestamp: deletedAt.UnixMilli(),
		Message: model.Message{
			ID:                    "reply-1",
			RoomID:                "dm-alice-bob",
			UserAccount:           "alice",
			CreatedAt:             msgTime,
			UpdatedAt:             &deletedAt,
			ThreadParentMessageID: "parent-dm",
			TShow:                 false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	require.Len(t, pub.records, 2)
	subjects := map[string]bool{}
	for _, r := range pub.records {
		subjects[r.subject] = true
		var roomEvt model.DeleteRoomEvent
		require.NoError(t, json.Unmarshal(r.data, &roomEvt))
		assert.Equal(t, model.RoomEventMessageDeleted, roomEvt.Type)
	}
	assert.True(t, subjects[subject.UserRoomEvent("alice")])
	assert.True(t, subjects[subject.UserRoomEvent("bob")])
}

func TestPublishToThreadAccounts_AllFail_ReturnsError(t *testing.T) {
	failPub := &failingPublisher{failAfter: 0}

	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	keyStore := NewMockRoomKeyProvider(ctrl)

	h := NewHandler(store, us, failPub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	err := h.publishToThreadAccounts(context.Background(), []string{"alice", "bob"}, []byte(`{}`), "parent-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "all 2 thread account publishes failed")
}

func TestPublishToThreadAccounts_PartialFail_ReturnsNil(t *testing.T) {
	// failAfter=1: first publish succeeds, subsequent ones fail.
	failPub := &failingPublisher{failAfter: 1}

	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	keyStore := NewMockRoomKeyProvider(ctrl)

	h := NewHandler(store, us, failPub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	// alice succeeds, bob fails — partial failure must not trigger redelivery.
	err := h.publishToThreadAccounts(context.Background(), []string{"alice", "bob"}, []byte(`{}`), "parent-1")
	require.NoError(t, err)
}

func TestPublishToThreadAccounts_Empty_NoOp(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.publishToThreadAccounts(context.Background(), nil, []byte(`{}`), "parent-1"))
	assert.Empty(t, pub.records)
}

func TestBuildClientMessage_DecodesAttachments(t *testing.T) {
	blob, err := json.Marshal(cassandra.Attachment{ID: "f1", Title: "a.png", Type: "file"})
	require.NoError(t, err)
	msg := model.Message{
		ID: "m1", UserAccount: "alice", Attachments: [][]byte{blob},
		QuotedParentMessage: &cassandra.QuotedParentMessage{
			DecodedAttachments: []cassandra.Attachment{{ID: "f1", Title: "a.png", Type: "file"}},
		},
	}

	cm := buildClientMessage(&msg, map[string]model.User{})

	// Main message: raw blobs decoded onto the wrapper; embedded raw nilled (Task F).
	require.Len(t, cm.Attachments, 1)
	assert.Equal(t, "f1", cm.Attachments[0].ID)
	assert.Nil(t, cm.Message.Attachments)

	// Quoted parent: passed through already-decoded (no re-decode).
	require.Len(t, cm.QuotedParentMessage.DecodedAttachments, 1)
	assert.Equal(t, "f1", cm.QuotedParentMessage.DecodedAttachments[0].ID)

	// The delivered JSON carries attachments as objects, not base64 strings.
	out, err := json.Marshal(cm)
	require.NoError(t, err)
	assert.Contains(t, string(out), `"id":"f1"`)
	assert.NotContains(t, string(out), base64.StdEncoding.EncodeToString(blob))
}

func TestHandleCreated_NonRename_NoRoomRenamedEvent(t *testing.T) {
	// Regression: a normal created message must NOT publish a RoomRenamedRoomEvent.
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC)
	store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", msgTime, false).Return(nil)
	store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "room-1", "sender", msgTime).Return(nil)
	store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return(nil, nil)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), makeMessageEvent("room-1", "hello", msgTime)))

	require.Len(t, pub.records, 1, "normal message: exactly one publish")
	var evt model.RoomEvent
	require.NoError(t, json.Unmarshal(pub.records[0].data, &evt))
	assert.Equal(t, model.RoomEventNewMessage, evt.Type, "normal message must publish new_message, not room_renamed")
}

func TestHandleCreated_AdvancesSenderLastSeen(t *testing.T) {
	msgTime := time.Date(2026, 6, 17, 11, 0, 0, 0, time.UTC)
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", msgTime, false).Return(nil)
	store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "room-1", "sender", msgTime).Return(nil)
	store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return(nil, nil)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), makeMessageEvent("room-1", "hello", msgTime)))
}

// Advance failure is best-effort: logged + swallowed, the broadcast still proceeds.
func TestHandleCreated_AdvanceSenderLastSeen_FailureSwallowed(t *testing.T) {
	msgTime := time.Date(2026, 6, 17, 11, 0, 0, 0, time.UTC)
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", msgTime, false).Return(nil)
	store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "room-1", "sender", msgTime).Return(errors.New("mongo down"))
	// Fan-out still runs after the swallowed advance error.
	store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return(nil, nil)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), makeMessageEvent("room-1", "hello", msgTime)))
}

// stubJSMsg is the minimum jetstream.Msg surface Consumer.Track needs to build
// a tracked delivery in a unit test. The drop paths under test Ack via the
// tracked wrapper, so the disposition methods only need to succeed.
type stubJSMsg struct {
	subject string
	data    []byte
}

func (s stubJSMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: 1}, nil
}
func (s stubJSMsg) Data() []byte                     { return s.data }
func (s stubJSMsg) Headers() nats.Header             { return nats.Header{} }
func (s stubJSMsg) Subject() string                  { return s.subject }
func (s stubJSMsg) Reply() string                    { return "" }
func (s stubJSMsg) Ack() error                       { return nil }
func (s stubJSMsg) DoubleAck(context.Context) error  { return nil }
func (s stubJSMsg) Nak() error                       { return nil }
func (s stubJSMsg) NakWithDelay(time.Duration) error { return nil }
func (s stubJSMsg) InProgress() error                { return nil }
func (s stubJSMsg) Term() error                      { return nil }
func (s stubJSMsg) TermWithReason(string) error      { return nil }

// terminalCountFor runs fn under a tracked delivery and returns how many
// terminal failures it recorded for reason. A log-and-drop path returns nil, so
// the counter is the only evidence the message was permanently discarded.
func terminalCountFor(t *testing.T, reason string, fn func(context.Context)) int64 {
	t.Helper()
	m, reader := newTestBroadcastMetrics(t)
	consumer := m.Consumer(natsmetrics.ConsumerConfig{
		Site:   "s1",
		Stream: "MESSAGES-CANONICAL-s1", Consumer: "broadcast-worker",
	})
	tracked := consumer.Track(context.Background(), stubJSMsg{subject: "chat.msg.canonical.s1.created"}, natsmetrics.EventCreated, 5)
	fn(tracked.Context(context.Background()))
	tracked.Finish(context.Background())

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	return sumOf(t, rm, "chat.nats.terminal.failures", map[string]string{"reason": reason})
}

// mentionOutboxRecorder captures OUTBOX publishes so tests can assert the
// per-destination fan-out without a real JetStream connection.
type mentionOutboxRecorder struct {
	mu      sync.Mutex
	records []outboxRecord
	err     error
}

type outboxRecord struct {
	subject string
	msgID   string
	event   model.SubscriptionMentionEvent
}

func (r *mentionOutboxRecorder) publish(_ context.Context, subj string, data []byte, msgID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	var outboxEvt model.OutboxEvent
	if err := json.Unmarshal(data, &outboxEvt); err != nil {
		return err
	}
	var envelope model.InboxEvent
	if err := json.Unmarshal(outboxEvt.Envelope, &envelope); err != nil {
		return err
	}
	var evt model.SubscriptionMentionEvent
	if err := json.Unmarshal(envelope.Payload, &evt); err != nil {
		return err
	}
	r.records = append(r.records, outboxRecord{subject: subj, msgID: msgID, event: evt})
	return nil
}

func (r *mentionOutboxRecorder) sorted() []outboxRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := append([]outboxRecord(nil), r.records...)
	sort.Slice(out, func(i, j int) bool { return out[i].subject < out[j].subject })
	return out
}

func TestHandler_HandleCreated_FederatesMentions(t *testing.T) {
	msgTime := time.Now().UTC().Truncate(time.Millisecond)

	tests := []struct {
		name        string
		content     string
		users       []model.User
		publishErr  error
		noFederate  bool
		wantRecords []outboxRecord
	}{
		{
			name:    "all mentionees are local",
			content: "hi @alice",
			users:   []model.User{{ID: "u1", Account: "alice", SiteID: "site-a"}},
		},
		{
			name:    "one event per remote site carrying only that site's accounts",
			content: "hi @alice @bob @carol",
			users: []model.User{
				{ID: "u1", Account: "alice", SiteID: "site-a"},
				{ID: "u2", Account: "bob", SiteID: "site-b"},
				{ID: "u3", Account: "carol", SiteID: "site-c"},
			},
			wantRecords: []outboxRecord{
				{
					subject: "chat.outbox.site-a.site-b.subscription_mention",
					msgID:   "mention:room-1:msg-1:site-b",
					event: model.SubscriptionMentionEvent{
						RoomID: "room-1", Accounts: []string{"bob"}, MessageCreatedAt: msgTime.UnixMilli(),
					},
				},
				{
					subject: "chat.outbox.site-a.site-c.subscription_mention",
					msgID:   "mention:room-1:msg-1:site-c",
					event: model.SubscriptionMentionEvent{
						RoomID: "room-1", Accounts: []string{"carol"}, MessageCreatedAt: msgTime.UnixMilli(),
					},
				},
			},
		},
		{
			name:    "unresolved mentionee has no home site to route to",
			content: "hi @ghost",
			users:   nil,
		},
		{
			name:    "mention-all alone federates nothing",
			content: "hi @all",
			users:   nil,
		},
		{
			name:       "publish error is swallowed",
			content:    "hi @bob",
			users:      []model.User{{ID: "u2", Account: "bob", SiteID: "site-b"}},
			publishErr: errors.New("jetstream down"),
		},
		{
			name:       "federation disabled",
			content:    "hi @bob",
			users:      []model.User{{ID: "u2", Account: "bob", SiteID: "site-b"}},
			noFederate: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockStore(ctrl)
			us := NewMockUserStore(ctrl)
			pub := &mockPublisher{}
			keyStore := NewMockRoomKeyProvider(ctrl)
			rec := &mentionOutboxRecorder{err: tc.publishErr}

			mentionAll := strings.Contains(tc.content, "@all")
			store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", msgTime, mentionAll).Return(nil)
			store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "room-1", "sender", msgTime).Return(nil)
			store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
			us.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return(tc.users, nil)
			store.EXPECT().SetSubscriptionMentions(gomock.Any(), "room-1", gomock.Any(), msgTime).Return(nil).AnyTimes()
			keyStore.EXPECT().Get(gomock.Any(), "room-1").Return(testRoomKey(t), nil)

			var opts []handlerOption
			if !tc.noFederate {
				opts = append(opts, withOutboxFederation("site-a", rec.publish))
			}
			h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal, opts...)

			data, err := json.Marshal(model.MessageEvent{
				Event:  model.EventCreated,
				SiteID: "site-a",
				Message: model.Message{
					ID: "msg-1", RoomID: "room-1", UserID: "user-1", UserAccount: "sender",
					Content: tc.content, CreatedAt: msgTime,
				},
			})
			require.NoError(t, err)

			require.NoError(t, h.HandleMessage(context.Background(), data))
			assert.Len(t, pub.records, 1, "the client fan-out must still happen")

			got := rec.sorted()
			require.Len(t, got, len(tc.wantRecords))
			for i, want := range tc.wantRecords {
				assert.Equal(t, want.subject, got[i].subject)
				assert.Equal(t, want.msgID, got[i].msgID)
				assert.Equal(t, want.event.RoomID, got[i].event.RoomID)
				assert.Equal(t, want.event.Accounts, got[i].event.Accounts)
				assert.Equal(t, want.event.MessageCreatedAt, got[i].event.MessageCreatedAt)
				assert.NotZero(t, got[i].event.Timestamp)
			}
		})
	}
}

func TestHandler_HandleUpdated_FederatesMentions(t *testing.T) {
	createdAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	editedAt := time.Now().UTC().Truncate(time.Millisecond)

	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)
	rec := &mentionOutboxRecorder{}

	store.EXPECT().GetRoom(gomock.Any(), "room-1").Return(testChannelRoom, nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).
		Return([]model.User{{ID: "u2", Account: "bob", SiteID: "site-b"}}, nil)
	store.EXPECT().SetSubscriptionMentions(gomock.Any(), "room-1", []string{"bob"}, editedAt).Return(nil)
	keyStore.EXPECT().Get(gomock.Any(), "room-1").Return(testRoomKey(t), nil)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal,
		withOutboxFederation("site-a", rec.publish))

	data, err := json.Marshal(model.MessageEvent{
		Event:  model.EventUpdated,
		SiteID: "site-a",
		Message: model.Message{
			ID: "msg-1", RoomID: "room-1", UserID: "user-1", UserAccount: "sender",
			Content: "hi @bob", CreatedAt: createdAt, EditedAt: &editedAt, UpdatedAt: &editedAt,
		},
	})
	require.NoError(t, err)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	got := rec.sorted()
	require.Len(t, got, 1)
	assert.Equal(t, "chat.outbox.site-a.site-b.subscription_mention", got[0].subject)
	assert.Equal(t, fmt.Sprintf("mention-edit:room-1:msg-1:%d:site-b", editedAt.UnixMilli()), got[0].msgID)
	assert.Equal(t, []string{"bob"}, got[0].event.Accounts)
	assert.Equal(t, editedAt.UnixMilli(), got[0].event.MessageCreatedAt)
}

func TestHandler_HandleUpdated_NoMentionsSkipsLookup(t *testing.T) {
	createdAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	editedAt := time.Now().UTC().Truncate(time.Millisecond)

	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)
	rec := &mentionOutboxRecorder{}

	// No FindUsersByAccounts and no SetSubscriptionMentions expectations: gomock
	// fails the test if the mention-free edit calls either.
	store.EXPECT().GetRoom(gomock.Any(), "room-1").Return(testChannelRoom, nil)
	keyStore.EXPECT().Get(gomock.Any(), "room-1").Return(testRoomKey(t), nil)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal,
		withOutboxFederation("site-a", rec.publish))

	data, err := json.Marshal(model.MessageEvent{
		Event:  model.EventUpdated,
		SiteID: "site-a",
		Message: model.Message{
			ID: "msg-1", RoomID: "room-1", UserID: "user-1", UserAccount: "sender",
			Content: "no mentions here", CreatedAt: createdAt, EditedAt: &editedAt, UpdatedAt: &editedAt,
		},
	})
	require.NoError(t, err)
	require.NoError(t, h.HandleMessage(context.Background(), data))
	assert.Empty(t, rec.sorted())
}
