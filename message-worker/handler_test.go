package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/userstore"
)

func ptrTime(t time.Time) *time.Time { return &t }

func TestHandler_ProcessMessage(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	parentCreatedAt := now.Add(-time.Hour)
	user := &model.User{
		ID:          "u-1",
		Account:     "alice",
		SiteID:      "site-a",
		EngName:     "Alice Wang",
		ChineseName: "愛麗絲",
	}
	msg := model.Message{
		ID:          "msg-1",
		RoomID:      "r1",
		UserID:      "u-1",
		UserAccount: "alice",
		Content:     "hello",
		CreatedAt:   now,
	}
	evt := model.MessageEvent{Message: msg, SiteID: "site-a", Timestamp: now.UnixMilli()}
	validData, _ := json.Marshal(evt)

	threadMsg := model.Message{
		ID:                           "msg-2",
		RoomID:                       "r1",
		UserID:                       "u-1",
		UserAccount:                  "alice",
		Content:                      "thread reply",
		CreatedAt:                    now,
		ThreadParentMessageID:        "msg-1",
		ThreadParentMessageCreatedAt: &parentCreatedAt,
	}
	threadEvt := model.MessageEvent{Message: threadMsg, SiteID: "site-a", Timestamp: now.UnixMilli()}
	threadData, _ := json.Marshal(threadEvt)

	bobUser := &model.User{
		ID:          "u-bob",
		Account:     "bob",
		SiteID:      "site-a",
		EngName:     "Bob Chen",
		ChineseName: "鮑勃",
	}

	// Thread reply that mentions @bob (non-participant).
	threadMentionMsg := model.Message{
		ID:                    "msg-thread-mention",
		RoomID:                "r1",
		UserID:                "u-1",
		UserAccount:           "alice",
		Content:               "thread reply @bob",
		CreatedAt:             now,
		ThreadParentMessageID: "msg-1",
	}
	threadMentionEvt := model.MessageEvent{Message: threadMentionMsg, SiteID: "site-a", Timestamp: now.UnixMilli()}
	threadMentionData, _ := json.Marshal(threadMentionEvt)

	// Thread reply where sender self-mentions — must be excluded.
	threadSelfMsg := model.Message{
		ID:                    "msg-thread-self",
		RoomID:                "r1",
		UserID:                "u-1",
		UserAccount:           "alice",
		Content:               "thread reply @alice",
		CreatedAt:             now,
		ThreadParentMessageID: "msg-1",
	}
	threadSelfEvt := model.MessageEvent{Message: threadSelfMsg, SiteID: "site-a", Timestamp: now.UnixMilli()}
	threadSelfData, _ := json.Marshal(threadSelfEvt)

	// Thread reply with @all only — must be ignored at thread level.
	threadAllMsg := model.Message{
		ID:                    "msg-thread-all",
		RoomID:                "r1",
		UserID:                "u-1",
		UserAccount:           "alice",
		Content:               "thread reply @all",
		CreatedAt:             now,
		ThreadParentMessageID: "msg-1",
	}
	threadAllEvt := model.MessageEvent{Message: threadAllMsg, SiteID: "site-a", Timestamp: now.UnixMilli()}
	threadAllData, _ := json.Marshal(threadAllEvt)

	// Thread reply mixing @all + @bob — only bob gets marked.
	threadMixMsg := model.Message{
		ID:                    "msg-thread-mix",
		RoomID:                "r1",
		UserID:                "u-1",
		UserAccount:           "alice",
		Content:               "thread reply @all and @bob",
		CreatedAt:             now,
		ThreadParentMessageID: "msg-1",
	}
	threadMixEvt := model.MessageEvent{Message: threadMixMsg, SiteID: "site-a", Timestamp: now.UnixMilli()}
	threadMixData, _ := json.Marshal(threadMixEvt)

	// Event with a real user mention — Mentions field is absent in the inbound event
	// and will be populated by resolveMentions.
	evtWithMention := model.MessageEvent{
		Message: model.Message{
			ID: "msg-3", RoomID: "r1", UserID: "u-1", UserAccount: "alice",
			Content:   "hey @bob can you check this?",
			CreatedAt: now,
		},
		SiteID: "site-a", Timestamp: now.UnixMilli(),
	}
	dataWithMention, _ := json.Marshal(evtWithMention)

	// Expected stored message: Mentions resolved to full Participant.
	msgWithMention := model.Message{
		ID: "msg-3", RoomID: "r1", UserID: "u-1", UserAccount: "alice",
		Content:   "hey @bob can you check this?",
		CreatedAt: now,
		Mentions: []model.Participant{{
			UserID: "u-bob", Account: "bob", SiteID: "site-a", ChineseName: "鮑勃", EngName: "Bob Chen",
		}},
	}

	// Event with @all — no user lookup should occur.
	evtWithAll := model.MessageEvent{
		Message: model.Message{
			ID: "msg-4", RoomID: "r1", UserID: "u-1", UserAccount: "alice",
			Content:   "hello @all please read",
			CreatedAt: now,
		},
		SiteID: "site-a", Timestamp: now.UnixMilli(),
	}
	dataWithAll, _ := json.Marshal(evtWithAll)

	msgWithAll := model.Message{
		ID: "msg-4", RoomID: "r1", UserID: "u-1", UserAccount: "alice",
		Content:   "hello @all please read",
		CreatedAt: now,
		Mentions:  []model.Participant{{Account: "all", EngName: "all"}},
	}

	expectedSender := cassParticipant{
		ID:          user.ID,
		EngName:     user.EngName,
		CompanyName: user.ChineseName,
		Account:     msg.UserAccount,
	}

	tests := []struct {
		name       string
		data       []byte
		migration  bool
		setupMocks func(store *MockStore, userStore *MockUserStore, threadStore *MockThreadStore)
		wantErr    bool
	}{
		{
			name: "happy path — user found and message saved",
			data: validData,
			setupMocks: func(store *MockStore, us *MockUserStore, ts *MockThreadStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)
				store.EXPECT().SaveMessage(gomock.Any(), &msg, &expectedSender, "site-a").Return(nil)
			},
		},
		{
			name: "user not found — NAK without saving",
			data: validData,
			setupMocks: func(store *MockStore, us *MockUserStore, ts *MockThreadStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-1").
					Return(nil, errors.New("user not found"))
			},
			wantErr: true,
		},
		{
			name: "user store DB error — NAK without saving",
			data: validData,
			setupMocks: func(store *MockStore, us *MockUserStore, ts *MockThreadStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-1").
					Return(nil, errors.New("mongo: connection refused"))
			},
			wantErr: true,
		},
		{
			name: "save error — NAK after user lookup",
			data: validData,
			setupMocks: func(store *MockStore, us *MockUserStore, ts *MockThreadStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)
				store.EXPECT().SaveMessage(gomock.Any(), &msg, &expectedSender, "site-a").
					Return(errors.New("cassandra: write timeout"))
			},
			wantErr: true,
		},
		{
			name:       "malformed JSON — NAK immediately",
			data:       []byte("{invalid"),
			setupMocks: func(store *MockStore, us *MockUserStore, ts *MockThreadStore) {},
			wantErr:    true,
		},
		{
			name: "thread message — calls SaveThreadMessage not SaveMessage",
			data: threadData,
			setupMocks: func(store *MockStore, us *MockUserStore, ts *MockThreadStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)
				// handleThreadRoomAndSubscriptions runs first to resolve the threadRoomID.
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(errThreadRoomExists)
				ts.EXPECT().GetThreadRoomByParentMessageID(gomock.Any(), "msg-1").
					Return(&model.ThreadRoom{ID: "tr-1"}, nil)
				// Subsequent-reply path: upsert parent and replier subscriptions.
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-1").
					Return(&cassParticipant{ID: "u-parent", Account: "parent-user"}, nil)
				us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
					Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil)
				ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
				ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
				ts.EXPECT().UpdateThreadRoomLastMessage(gomock.Any(), "tr-1", "msg-2", gomock.Any(), now).Return(nil)
				ts.EXPECT().AdvanceThreadSubscriptionLastSeen(gomock.Any(), "tr-1", "alice", now).Return(nil)
				// SaveThreadMessage receives the resolved threadRoomID.
				store.EXPECT().SaveThreadMessage(gomock.Any(), &threadMsg, &expectedSender, "site-a", "tr-1").Return((*int)(nil), nil)
			},
		},
		{
			name: "thread message save error — NAK after user lookup",
			data: threadData,
			setupMocks: func(store *MockStore, us *MockUserStore, ts *MockThreadStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)
				// handleThreadRoomAndSubscriptions runs before SaveThreadMessage.
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(errThreadRoomExists)
				ts.EXPECT().GetThreadRoomByParentMessageID(gomock.Any(), "msg-1").
					Return(&model.ThreadRoom{ID: "tr-1"}, nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-1").
					Return(&cassParticipant{ID: "u-parent", Account: "parent-user"}, nil)
				us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
					Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil)
				ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
				ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
				ts.EXPECT().UpdateThreadRoomLastMessage(gomock.Any(), "tr-1", "msg-2", gomock.Any(), now).Return(nil)
				ts.EXPECT().AdvanceThreadSubscriptionLastSeen(gomock.Any(), "tr-1", "alice", now).Return(nil)
				store.EXPECT().SaveThreadMessage(gomock.Any(), &threadMsg, &expectedSender, "site-a", "tr-1").
					Return((*int)(nil), errors.New("cassandra: write timeout"))
			},
			wantErr: true,
		},
		{
			name: "mention resolved to Participant and stored",
			data: dataWithMention,
			setupMocks: func(store *MockStore, us *MockUserStore, ts *MockThreadStore) {
				us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).
					Return([]model.User{*bobUser}, nil)
				us.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)
				store.EXPECT().SaveMessage(gomock.Any(), &msgWithMention, &expectedSender, "site-a").Return(nil)
			},
		},
		{
			name: "@all stored as special Participant without DB lookup",
			data: dataWithAll,
			setupMocks: func(store *MockStore, us *MockUserStore, ts *MockThreadStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)
				store.EXPECT().SaveMessage(gomock.Any(), &msgWithAll, &expectedSender, "site-a").Return(nil)
			},
		},
		{
			name: "mention user lookup error — NAK before sender lookup",
			data: dataWithMention,
			setupMocks: func(store *MockStore, us *MockUserStore, ts *MockThreadStore) {
				us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).
					Return(nil, errors.New("mongo: connection refused"))
				// FindUserByID and SaveMessage must NOT be called
			},
			wantErr: true,
		},
		{
			name: "system message with unknown user — saved with nil sender",
			data: func() []byte {
				sysMsg := model.Message{
					ID: "msg-sys-1", RoomID: "r1", Content: "added members",
					CreatedAt: now, Type: "members_added",
					SysMsgData: []byte(`{"individuals":["bob"]}`),
				}
				e := model.MessageEvent{Message: sysMsg, SiteID: "site-a", Timestamp: now.UnixMilli()}
				d, _ := json.Marshal(e)
				return d
			}(),
			setupMocks: func(store *MockStore, us *MockUserStore, _ *MockThreadStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "").
					Return(nil, errors.New("user not found"))
				expectedMsg := model.Message{
					ID: "msg-sys-1", RoomID: "r1", Content: "added members",
					CreatedAt: now, Type: "members_added",
					SysMsgData: []byte(`{"individuals":["bob"]}`),
				}
				store.EXPECT().SaveMessage(gomock.Any(), &expectedMsg, (*cassParticipant)(nil), "site-a").Return(nil)
			},
		},
		{
			name: "regular message with user lookup error — still returns error",
			data: validData,
			setupMocks: func(_ *MockStore, us *MockUserStore, _ *MockThreadStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-1").
					Return(nil, errors.New("user not found"))
			},
			wantErr: true,
		},
		{
			name: "thread reply mentioning non-participant — marks that user's subscription",
			data: threadMentionData,
			setupMocks: func(store *MockStore, us *MockUserStore, ts *MockThreadStore) {
				us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).
					Return([]model.User{*bobUser}, nil)
				us.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)
				// First-reply path: create the thread room.
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-1").
					Return(&cassParticipant{ID: "u-parent", Account: "parent-user"}, nil)
				us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
					Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil)
				// Parent + replier subscriptions inserted.
				ts.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
				ts.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
				ts.EXPECT().AdvanceThreadSubscriptionLastSeen(gomock.Any(), gomock.Any(), "alice", now).Return(nil)
				// Mentionee @bob gets MarkThreadSubscriptionMention — assert sub fields.
				ts.EXPECT().MarkThreadSubscriptionMention(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, sub *model.ThreadSubscription) error {
						assert.Equal(t, "u-bob", sub.UserID)
						assert.Equal(t, "bob", sub.UserAccount)
						assert.Equal(t, "msg-1", sub.ParentMessageID)
						assert.Equal(t, "r1", sub.RoomID)
						assert.Equal(t, "site-a", sub.SiteID)
						assert.True(t, sub.HasMention)
						assert.Nil(t, sub.LastSeenAt)
						return nil
					})
				store.EXPECT().SaveThreadMessage(gomock.Any(), gomock.Any(), gomock.Any(), "site-a", gomock.Any()).Return((*int)(nil), nil)
			},
		},
		{
			name: "thread reply where sender self-mentions — no MarkThreadSubscriptionMention call",
			data: threadSelfData,
			setupMocks: func(store *MockStore, us *MockUserStore, ts *MockThreadStore) {
				// Sender's own account looked up; returns the sender user.
				us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice"}).
					Return([]model.User{*user}, nil)
				us.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-1").
					Return(&cassParticipant{ID: "u-parent", Account: "parent-user"}, nil)
				us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
					Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil)
				ts.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
				ts.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
				ts.EXPECT().AdvanceThreadSubscriptionLastSeen(gomock.Any(), gomock.Any(), "alice", now).Return(nil)
				// MarkThreadSubscriptionMention must NOT be called — sender excluded.
				store.EXPECT().SaveThreadMessage(gomock.Any(), gomock.Any(), gomock.Any(), "site-a", gomock.Any()).Return((*int)(nil), nil)
			},
		},
		{
			name: "thread reply with @all only — no MarkThreadSubscriptionMention call",
			data: threadAllData,
			setupMocks: func(store *MockStore, us *MockUserStore, ts *MockThreadStore) {
				// No account lookup — @all bypasses the user-by-accounts query.
				us.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-1").
					Return(&cassParticipant{ID: "u-parent", Account: "parent-user"}, nil)
				us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
					Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil)
				ts.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
				ts.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
				ts.EXPECT().AdvanceThreadSubscriptionLastSeen(gomock.Any(), gomock.Any(), "alice", now).Return(nil)
				// MarkThreadSubscriptionMention must NOT be called — @all is thread-ignored.
				store.EXPECT().SaveThreadMessage(gomock.Any(), gomock.Any(), gomock.Any(), "site-a", gomock.Any()).Return((*int)(nil), nil)
			},
		},
		{
			name: "thread reply with @all + @bob — only bob marked",
			data: threadMixData,
			setupMocks: func(store *MockStore, us *MockUserStore, ts *MockThreadStore) {
				us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).
					Return([]model.User{*bobUser}, nil)
				us.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-1").
					Return(&cassParticipant{ID: "u-parent", Account: "parent-user"}, nil)
				us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
					Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil)
				ts.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
				ts.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
				ts.EXPECT().AdvanceThreadSubscriptionLastSeen(gomock.Any(), gomock.Any(), "alice", now).Return(nil)
				ts.EXPECT().MarkThreadSubscriptionMention(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, sub *model.ThreadSubscription) error {
						assert.Equal(t, "u-bob", sub.UserID)
						assert.True(t, sub.HasMention)
						return nil
					})
				store.EXPECT().SaveThreadMessage(gomock.Any(), gomock.Any(), gomock.Any(), "site-a", gomock.Any()).Return((*int)(nil), nil)
			},
		},
		{
			name: "thread reply mentioning non-participant — MarkThreadSubscriptionMention error is propagated",
			data: threadMentionData,
			setupMocks: func(store *MockStore, us *MockUserStore, ts *MockThreadStore) {
				us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).
					Return([]model.User{*bobUser}, nil)
				us.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-1").
					Return(&cassParticipant{ID: "u-parent", Account: "parent-user"}, nil)
				us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
					Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil)
				ts.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
				ts.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
				ts.EXPECT().AdvanceThreadSubscriptionLastSeen(gomock.Any(), gomock.Any(), "alice", now).Return(nil)
				ts.EXPECT().MarkThreadSubscriptionMention(gomock.Any(), gomock.Any()).
					Return(errors.New("mongo: write error"))
				// SaveThreadMessage must NOT be called — mention-mark error aborts before save.
			},
			wantErr: true,
		},
		{
			// Migrated first reply: persists + creates thread_room, but writes no thread_subscriptions
			// (collections owns them; a raw insert would dup-key the (threadRoomId,userAccount) index).
			name:      "migrated first thread reply — persists + creates room, skips subscription writes",
			data:      threadData,
			migration: true,
			setupMocks: func(store *MockStore, us *MockUserStore, ts *MockThreadStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-1").
					Return(&cassParticipant{ID: "u-parent", Account: "parent-user"}, nil)
				// No InsertThreadSubscription / owner-site lookup / MarkThreadSubscriptionMention — all suppressed for migrated events.
				store.EXPECT().SaveThreadMessage(gomock.Any(), &threadMsg, &expectedSender, "site-a", gomock.Any()).
					Return((*int)(nil), nil)
				ts.EXPECT().AdvanceThreadSubscriptionLastSeen(gomock.Any(), gomock.Any(), "alice", now).Return(nil)
			},
		},
		{
			name:      "migrated subsequent thread reply — persists + bumps room, skips subscription writes",
			data:      threadData,
			migration: true,
			setupMocks: func(store *MockStore, us *MockUserStore, ts *MockThreadStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(errThreadRoomExists)
				ts.EXPECT().GetThreadRoomByParentMessageID(gomock.Any(), "msg-1").
					Return(&model.ThreadRoom{ID: "tr-1"}, nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-1").
					Return(&cassParticipant{ID: "u-parent", Account: "parent-user"}, nil)
				// No UpsertThreadSubscription/owner-site lookup; replyAccounts + lastMsg pointer still written (thread_rooms, not subs).
				ts.EXPECT().UpdateThreadRoomLastMessage(gomock.Any(), "tr-1", "msg-2", []string{"alice", "parent-user"}, now).Return(nil)
				store.EXPECT().SaveThreadMessage(gomock.Any(), &threadMsg, &expectedSender, "site-a", "tr-1").
					Return((*int)(nil), nil)
				ts.EXPECT().AdvanceThreadSubscriptionLastSeen(gomock.Any(), "tr-1", "alice", now).Return(nil)
			},
		},
		{
			name:      "migrated thread reply mentioning @bob — skips mention-mark, keeps reply-accounts",
			data:      threadMentionData,
			migration: true,
			setupMocks: func(store *MockStore, us *MockUserStore, ts *MockThreadStore) {
				us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).
					Return([]model.User{*bobUser}, nil)
				us.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-1").
					Return(&cassParticipant{ID: "u-parent", Account: "parent-user"}, nil)
				// MarkThreadSubscriptionMention + InsertThreadSubscription must NOT be called.
				store.EXPECT().SaveThreadMessage(gomock.Any(), gomock.Any(), gomock.Any(), "site-a", gomock.Any()).
					Return((*int)(nil), nil)
				ts.EXPECT().AdvanceThreadSubscriptionLastSeen(gomock.Any(), gomock.Any(), "alice", now).Return(nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockStore := NewMockStore(ctrl)
			mockUserStore := NewMockUserStore(ctrl)
			mockThreadStore := NewMockThreadStore(ctrl)
			mockThreadStore.EXPECT().AddReplyAccounts(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			stubHistoryAllMembers(mockThreadStore)
			// Thread replies resolve the parent createdAt from messages_by_id; default to
			// found so cases proceed (the parent-row stamp is allowed but not asserted here).
			mockStore.EXPECT().GetMessageCreatedAt(gomock.Any(), gomock.Any()).Return(parentCreatedAt, true, nil).AnyTimes()
			mockStore.EXPECT().UpdateParentMessageThreadRoomID(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			tt.setupMocks(mockStore, mockUserStore, mockThreadStore)

			h := NewHandler(mockStore, mockUserStore, mockThreadStore, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error {
				return nil
			})
			err := h.processMessage(context.Background(), tt.data, tt.migration)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestHandler_ProcessMessage_ThreadReply_PublishesBadgeEvent verifies that when
// SaveThreadMessage returns a non-nil tcount (first write or redelivery recovery),
// the handler calls publishThreadReplyEvent on the MESSAGES_CANONICAL stream.
func TestHandler_ProcessMessage_ThreadReply_PublishesBadgeEvent(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	parentCreatedAt := now.Add(-time.Hour)
	user := &model.User{ID: "u-1", Account: "alice", SiteID: "site-a", EngName: "Alice Wang", ChineseName: "愛麗絲"}
	threadMsg := model.Message{
		ID:                           "msg-reply",
		RoomID:                       "r1",
		UserID:                       "u-1",
		UserAccount:                  "alice",
		Content:                      "reply",
		CreatedAt:                    now,
		ThreadParentMessageID:        "msg-parent",
		ThreadParentMessageCreatedAt: &parentCreatedAt,
	}
	threadEvt := model.MessageEvent{Message: threadMsg, SiteID: "site-a", Timestamp: now.UnixMilli()}
	data, _ := json.Marshal(threadEvt)
	expectedSender := cassParticipant{ID: "u-1", EngName: "Alice Wang", CompanyName: "愛麗絲", Account: "alice"}
	expectedTcount := 3

	ctrl := gomock.NewController(t)
	mockStore := NewMockStore(ctrl)
	mockUserStore := NewMockUserStore(ctrl)
	mockThreadStore := NewMockThreadStore(ctrl)

	mockStore.EXPECT().GetMessageCreatedAt(gomock.Any(), gomock.Any()).Return(parentCreatedAt, true, nil).AnyTimes()
	mockUserStore.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)
	mockThreadStore.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(errThreadRoomExists)
	mockThreadStore.EXPECT().GetThreadRoomByParentMessageID(gomock.Any(), "msg-parent").
		Return(&model.ThreadRoom{ID: "tr-99"}, nil)
	mockStore.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
		Return(&cassParticipant{ID: "u-parent", Account: "parent-user"}, nil)
	mockStore.EXPECT().UpdateParentMessageThreadRoomID(gomock.Any(), "msg-parent", "r1", parentCreatedAt, "tr-99").Return(nil)
	mockUserStore.EXPECT().FindUserByID(gomock.Any(), "u-parent").
		Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil)
	mockThreadStore.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
	mockThreadStore.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
	mockThreadStore.EXPECT().UpdateThreadRoomLastMessage(gomock.Any(), "tr-99", "msg-reply", gomock.Any(), now).Return(nil)
	mockThreadStore.EXPECT().AdvanceThreadSubscriptionLastSeen(gomock.Any(), "tr-99", "alice", now).Return(nil)
	// SaveThreadMessage returns a non-nil tcount (simulates first write or redelivery recovery).
	mockStore.EXPECT().SaveThreadMessage(gomock.Any(), &threadMsg, &expectedSender, "site-a", "tr-99").
		Return(&expectedTcount, nil)

	var capturedSubj string
	var capturedData []byte
	h := NewHandler(mockStore, mockUserStore, mockThreadStore, "site-a",
		func(_ context.Context, subj string, data []byte, _ string) error {
			capturedSubj = subj
			capturedData = data
			return nil
		},
	)
	require.NoError(t, h.processMessage(context.Background(), data, false))

	assert.Equal(t, subject.ServerBroadcastThreadTCount("site-a"), capturedSubj,
		"badge event must be published via core NATS server-broadcast, not MESSAGES_CANONICAL")
	var badgeEvt model.MessageEvent
	require.NoError(t, json.Unmarshal(capturedData, &badgeEvt))
	assert.Equal(t, model.EventThreadReplyAdded, badgeEvt.Event)
	assert.Equal(t, "msg-reply", badgeEvt.Message.ID)
	require.NotNil(t, badgeEvt.NewTCount)
	assert.Equal(t, expectedTcount, *badgeEvt.NewTCount)
	// NewThreadLastMsgAt must be set to the reply's CreatedAt.
	require.NotNil(t, badgeEvt.NewThreadLastMsgAt, "NewThreadLastMsgAt must be set on the badge event")
	assert.True(t, badgeEvt.NewThreadLastMsgAt.Equal(now), "NewThreadLastMsgAt must equal reply CreatedAt")
}

// TestHandler_ProcessMessage_MigratedThreadReply_SuppressesBadgeAndOutbox verifies a migrated thread
// reply persists + bumps the room but publishes nothing — no tcount badge, no cross-site sub inbox.
func TestHandler_ProcessMessage_MigratedThreadReply_SuppressesBadgeAndOutbox(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	parentCreatedAt := now.Add(-time.Hour)
	user := &model.User{ID: "u-1", Account: "alice", SiteID: "site-a", EngName: "Alice Wang", ChineseName: "愛麗絲"}
	threadMsg := model.Message{
		ID:                           "msg-reply",
		RoomID:                       "r1",
		UserID:                       "u-1",
		UserAccount:                  "alice",
		Content:                      "reply",
		CreatedAt:                    now,
		ThreadParentMessageID:        "msg-parent",
		ThreadParentMessageCreatedAt: &parentCreatedAt,
	}
	threadEvt := model.MessageEvent{Message: threadMsg, SiteID: "site-a", Timestamp: now.UnixMilli()}
	data, _ := json.Marshal(threadEvt)
	expectedSender := cassParticipant{ID: "u-1", EngName: "Alice Wang", CompanyName: "愛麗絲", Account: "alice"}
	expectedTcount := 3

	ctrl := gomock.NewController(t)
	mockStore := NewMockStore(ctrl)
	mockUserStore := NewMockUserStore(ctrl)
	mockThreadStore := NewMockThreadStore(ctrl)
	mockThreadStore.EXPECT().AddReplyAccounts(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	mockUserStore.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)
	// No GetMessageCreatedAt: the migrated event carries the parent createdAt.
	mockThreadStore.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(errThreadRoomExists)
	mockThreadStore.EXPECT().GetThreadRoomByParentMessageID(gomock.Any(), "msg-parent").
		Return(&model.ThreadRoom{ID: "tr-99"}, nil)
	mockStore.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
		Return(&cassParticipant{ID: "u-parent", Account: "parent-user"}, nil)
	// No UpsertThreadSubscription, no owner-site lookup. lastMsg pointer kept.
	mockStore.EXPECT().UpdateParentMessageThreadRoomID(gomock.Any(), "msg-parent", "r1", parentCreatedAt, "tr-99").Return(nil)
	mockThreadStore.EXPECT().UpdateThreadRoomLastMessage(gomock.Any(), "tr-99", "msg-reply", gomock.Any(), now).Return(nil)
	// SaveThreadMessage returns a non-nil tcount — in the live path this would trigger the badge.
	mockStore.EXPECT().SaveThreadMessage(gomock.Any(), &threadMsg, &expectedSender, "site-a", "tr-99").
		Return(&expectedTcount, nil)
	mockThreadStore.EXPECT().AdvanceThreadSubscriptionLastSeen(gomock.Any(), "tr-99", "alice", now).Return(nil)

	var publishCalled bool
	h := NewHandler(mockStore, mockUserStore, mockThreadStore, "site-a",
		func(_ context.Context, _ string, _ []byte, _ string) error {
			publishCalled = true
			return nil
		},
	)
	require.NoError(t, h.processMessage(context.Background(), data, true))

	assert.False(t, publishCalled,
		"migrated thread reply must not publish the tcount badge or any cross-site subscription outbox")
}

// TestHandler_ProcessMessage_ThreadReply_AdvancesReplierLastSeen verifies the replier's
// own thread lastSeenAt is advanced (threadRoomID, replierAccount, msg.CreatedAt) on
// both a normal reply and a migration replay (#396; migration per mliu33 review on #398).
func TestHandler_ProcessMessage_ThreadReply_AdvancesReplierLastSeen(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	parentCreatedAt := now.Add(-time.Hour)
	user := &model.User{ID: "u-1", Account: "alice", SiteID: "site-a", EngName: "Alice Wang", ChineseName: "愛麗絲"}
	threadMsg := model.Message{
		ID: "msg-reply", RoomID: "r1", UserID: "u-1", UserAccount: "alice",
		Content: "reply", CreatedAt: now, ThreadParentMessageID: "msg-parent",
	}
	data, _ := json.Marshal(model.MessageEvent{Message: threadMsg, SiteID: "site-a", Timestamp: now.UnixMilli()})

	// subsequent-reply scaffolding shared by both subtests (known threadRoomID for exact-arg assert).
	setupSubsequentReply := func(store *MockStore, us *MockUserStore, ts *MockThreadStore, migration bool) {
		us.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)
		store.EXPECT().GetMessageCreatedAt(gomock.Any(), "msg-parent").Return(parentCreatedAt, true, nil)
		ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(errThreadRoomExists)
		ts.EXPECT().GetThreadRoomByParentMessageID(gomock.Any(), "msg-parent").Return(&model.ThreadRoom{ID: "tr-77"}, nil)
		store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").Return(&cassParticipant{ID: "u-parent", Account: "parent-user"}, nil)
		store.EXPECT().UpdateParentMessageThreadRoomID(gomock.Any(), "msg-parent", "r1", parentCreatedAt, "tr-77").Return(nil)
		if !migration {
			us.EXPECT().FindUserByID(gomock.Any(), "u-parent").Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil)
			ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil).Times(2)
		}
		ts.EXPECT().UpdateThreadRoomLastMessage(gomock.Any(), "tr-77", "msg-reply", gomock.Any(), now).Return(nil)
		ts.EXPECT().AddReplyAccounts(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		store.EXPECT().SaveThreadMessage(gomock.Any(), gomock.Any(), gomock.Any(), "site-a", "tr-77").Return((*int)(nil), nil)
	}

	t.Run("normal reply advances replier", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store, us, ts := NewMockStore(ctrl), NewMockUserStore(ctrl), NewMockThreadStore(ctrl)
		setupSubsequentReply(store, us, ts, false)
		ts.EXPECT().AdvanceThreadSubscriptionLastSeen(gomock.Any(), "tr-77", "alice", now).Return(nil)

		h := NewHandler(store, us, ts, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error { return nil })
		require.NoError(t, h.processMessage(context.Background(), data, false))
	})

	t.Run("migration reply also advances", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store, us, ts := NewMockStore(ctrl), NewMockUserStore(ctrl), NewMockThreadStore(ctrl)
		setupSubsequentReply(store, us, ts, true)
		// Advance runs on migration too ($max only moves forward) — mliu33 review on #398.
		ts.EXPECT().AdvanceThreadSubscriptionLastSeen(gomock.Any(), "tr-77", "alice", now).Return(nil)

		h := NewHandler(store, us, ts, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error { return nil })
		require.NoError(t, h.processMessage(context.Background(), data, true))
	})

	t.Run("advance failure is swallowed — reply still persists (best-effort)", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store, us, ts := NewMockStore(ctrl), NewMockUserStore(ctrl), NewMockThreadStore(ctrl)
		setupSubsequentReply(store, us, ts, false)
		// AdvanceThreadSubscriptionLastSeen errors → logged + swallowed; SaveThreadMessage
		// in setupSubsequentReply still runs and processMessage returns nil (#398 CodeRabbit).
		ts.EXPECT().AdvanceThreadSubscriptionLastSeen(gomock.Any(), "tr-77", "alice", now).Return(errors.New("mongo down"))

		h := NewHandler(store, us, ts, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error { return nil })
		require.NoError(t, h.processMessage(context.Background(), data, false))
	})
}

func TestHandler_HandleThreadRoomAndSubscriptions(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	parentSender := &cassParticipant{
		ID:      "u-parent",
		Account: "parent-user",
	}

	msg := &model.Message{
		ID:                    "msg-reply",
		RoomID:                "r1",
		UserID:                "u-replier",
		UserAccount:           "replier",
		Content:               "thread reply",
		CreatedAt:             now,
		ThreadParentMessageID: "msg-parent",
	}

	tests := []struct {
		name                string
		msg                 *model.Message
		siteID              string
		setupMocks          func(store *MockStore, ts *MockThreadStore)
		extraUserStoreSetup func(us *MockUserStore)
		expectReplierInsert bool
		wantErr             bool
	}{
		{
			name:   "first reply — different users — creates room and two subscriptions",
			msg:    msg,
			siteID: "site-a",
			setupMocks: func(store *MockStore, ts *MockThreadStore) {
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, room *model.ThreadRoom) error {
						assert.Equal(t, "msg-parent", room.ParentMessageID)
						assert.Equal(t, "r1", room.RoomID)
						assert.Equal(t, "site-a", room.SiteID)
						assert.Equal(t, "msg-reply", room.LastMsgID)
						assert.Equal(t, []string{"replier"}, room.ReplyAccounts)
						return nil
					})
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
					Return(parentSender, nil)
				ts.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, sub *model.ThreadSubscription) error {
						assert.Equal(t, "u-parent", sub.UserID)
						assert.Equal(t, "parent-user", sub.UserAccount)
						assert.Nil(t, sub.LastSeenAt, "parent's LastSeenAt should be nil on init")
						return nil
					})
				ts.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, sub *model.ThreadSubscription) error {
						assert.Equal(t, "u-replier", sub.UserID)
						assert.Equal(t, "replier", sub.UserAccount)
						assert.Nil(t, sub.LastSeenAt, "replier's LastSeenAt should be nil on init")
						return nil
					})
			},
			extraUserStoreSetup: func(us *MockUserStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
					Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil)
			},
		},
		{
			name:   "first reply — parent message not found — ack-and-skip",
			msg:    msg,
			siteID: "site-a",
			setupMocks: func(store *MockStore, ts *MockThreadStore) {
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
					Return(nil, fmt.Errorf("wrap: %w", errMessageNotFound))
			},
			wantErr: false,
		},
		{
			name: "first reply — same user — creates room and one subscription",
			msg: &model.Message{
				ID:                    "msg-reply",
				RoomID:                "r1",
				UserID:                "u-parent",
				UserAccount:           "parent-user",
				Content:               "self reply",
				CreatedAt:             now,
				ThreadParentMessageID: "msg-parent",
			},
			siteID: "site-a",
			setupMocks: func(store *MockStore, ts *MockThreadStore) {
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
					Return(parentSender, nil)
				ts.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, sub *model.ThreadSubscription) error {
						assert.Equal(t, "u-parent", sub.UserID)
						return nil
					})
			},
			extraUserStoreSetup: func(us *MockUserStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
					Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil)
			},
		},
		{
			name:   "first reply — GetMessageSender fails — returns error",
			msg:    msg,
			siteID: "site-a",
			setupMocks: func(store *MockStore, ts *MockThreadStore) {
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
					Return(nil, errors.New("cassandra: read timeout"))
			},
			wantErr: true,
		},
		{
			name:   "first reply — parent InsertThreadSubscription fails — returns error",
			msg:    msg,
			siteID: "site-a",
			setupMocks: func(store *MockStore, ts *MockThreadStore) {
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
					Return(parentSender, nil)
				ts.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).
					Return(errors.New("mongo: write error"))
			},
			extraUserStoreSetup: func(us *MockUserStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
					Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil)
			},
			wantErr: true,
		},
		{
			name:   "first reply — replier InsertThreadSubscription fails — returns error",
			msg:    msg,
			siteID: "site-a",
			setupMocks: func(store *MockStore, ts *MockThreadStore) {
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
					Return(parentSender, nil)
				// Parent insert succeeds
				ts.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
				// Replier insert fails
				ts.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).
					Return(errors.New("mongo: write error"))
			},
			extraUserStoreSetup: func(us *MockUserStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
					Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil)
			},
			wantErr: true,
		},
		{
			name:   "subsequent reply — upserts parent and replier subscriptions and updates last message",
			msg:    msg,
			siteID: "site-a",
			setupMocks: func(store *MockStore, ts *MockThreadStore) {
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).
					Return(errThreadRoomExists)
				ts.EXPECT().GetThreadRoomByParentMessageID(gomock.Any(), "msg-parent").
					Return(&model.ThreadRoom{ID: "tr-existing"}, nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
					Return(parentSender, nil)
				ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, sub *model.ThreadSubscription) error {
						assert.Equal(t, "tr-existing", sub.ThreadRoomID)
						assert.Equal(t, "u-parent", sub.UserID)
						assert.Nil(t, sub.LastSeenAt, "parent's LastSeenAt should be nil on init")
						return nil
					})
				ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, sub *model.ThreadSubscription) error {
						assert.Equal(t, "tr-existing", sub.ThreadRoomID)
						assert.Equal(t, "u-replier", sub.UserID)
						assert.Nil(t, sub.LastSeenAt, "replier's LastSeenAt should be nil on init")
						return nil
					})
				ts.EXPECT().UpdateThreadRoomLastMessage(gomock.Any(), "tr-existing", "msg-reply", gomock.Any(), now).
					Return(nil)
			},
			extraUserStoreSetup: func(us *MockUserStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
					Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil)
			},
		},
		{
			name: "subsequent reply — same user as parent — upserts one subscription and updates last message",
			msg: &model.Message{
				ID:                    "msg-reply",
				RoomID:                "r1",
				UserID:                "u-parent",
				UserAccount:           "parent-user",
				Content:               "self reply",
				CreatedAt:             now,
				ThreadParentMessageID: "msg-parent",
			},
			siteID: "site-a",
			setupMocks: func(store *MockStore, ts *MockThreadStore) {
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).
					Return(errThreadRoomExists)
				ts.EXPECT().GetThreadRoomByParentMessageID(gomock.Any(), "msg-parent").
					Return(&model.ThreadRoom{ID: "tr-existing"}, nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
					Return(parentSender, nil)
				ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, sub *model.ThreadSubscription) error {
						assert.Equal(t, "u-parent", sub.UserID)
						return nil
					})
				ts.EXPECT().UpdateThreadRoomLastMessage(gomock.Any(), "tr-existing", "msg-reply", gomock.Any(), now).
					Return(nil)
			},
			extraUserStoreSetup: func(us *MockUserStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
					Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil)
			},
		},
		{
			name:   "subsequent reply — parent message not found — skips parent upsert and upserts replier",
			msg:    msg,
			siteID: "site-a",
			setupMocks: func(store *MockStore, ts *MockThreadStore) {
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).
					Return(errThreadRoomExists)
				ts.EXPECT().GetThreadRoomByParentMessageID(gomock.Any(), "msg-parent").
					Return(&model.ThreadRoom{ID: "tr-existing"}, nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
					Return(nil, fmt.Errorf("wrap: %w", errMessageNotFound))
				ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, sub *model.ThreadSubscription) error {
						assert.Equal(t, "u-replier", sub.UserID)
						return nil
					})
				ts.EXPECT().UpdateThreadRoomLastMessage(gomock.Any(), "tr-existing", "msg-reply", gomock.Any(), now).
					Return(nil)
			},
		},
		{
			name:   "subsequent reply — GetThreadRoomByParentMessageID fails — returns error",
			msg:    msg,
			siteID: "site-a",
			setupMocks: func(store *MockStore, ts *MockThreadStore) {
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).
					Return(errThreadRoomExists)
				ts.EXPECT().GetThreadRoomByParentMessageID(gomock.Any(), "msg-parent").
					Return(nil, errors.New("mongo: connection refused"))
			},
			wantErr: true,
		},
		{
			name:   "subsequent reply — GetMessageSender fails — returns error",
			msg:    msg,
			siteID: "site-a",
			setupMocks: func(store *MockStore, ts *MockThreadStore) {
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).
					Return(errThreadRoomExists)
				ts.EXPECT().GetThreadRoomByParentMessageID(gomock.Any(), "msg-parent").
					Return(&model.ThreadRoom{ID: "tr-existing"}, nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
					Return(nil, errors.New("cassandra: read timeout"))
			},
			wantErr: true,
		},
		{
			name:   "subsequent reply — UpsertThreadSubscription for parent fails — returns error",
			msg:    msg,
			siteID: "site-a",
			setupMocks: func(store *MockStore, ts *MockThreadStore) {
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).
					Return(errThreadRoomExists)
				ts.EXPECT().GetThreadRoomByParentMessageID(gomock.Any(), "msg-parent").
					Return(&model.ThreadRoom{ID: "tr-existing"}, nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
					Return(parentSender, nil)
				ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).
					Return(errors.New("mongo: write error"))
			},
			extraUserStoreSetup: func(us *MockUserStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
					Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil)
			},
			wantErr: true,
		},
		{
			name:   "subsequent reply — UpsertThreadSubscription for replier fails — returns error",
			msg:    msg,
			siteID: "site-a",
			setupMocks: func(store *MockStore, ts *MockThreadStore) {
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).
					Return(errThreadRoomExists)
				ts.EXPECT().GetThreadRoomByParentMessageID(gomock.Any(), "msg-parent").
					Return(&model.ThreadRoom{ID: "tr-existing"}, nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
					Return(parentSender, nil)
				ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
				ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).
					Return(errors.New("mongo: write error"))
			},
			extraUserStoreSetup: func(us *MockUserStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
					Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil)
			},
			wantErr: true,
		},
		{
			name:   "subsequent reply — UpdateThreadRoomLastMessage fails — returns error",
			msg:    msg,
			siteID: "site-a",
			setupMocks: func(store *MockStore, ts *MockThreadStore) {
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).
					Return(errThreadRoomExists)
				ts.EXPECT().GetThreadRoomByParentMessageID(gomock.Any(), "msg-parent").
					Return(&model.ThreadRoom{ID: "tr-existing"}, nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
					Return(parentSender, nil)
				ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
				ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
				ts.EXPECT().UpdateThreadRoomLastMessage(gomock.Any(), "tr-existing", "msg-reply", gomock.Any(), now).
					Return(errors.New("mongo: write error"))
			},
			extraUserStoreSetup: func(us *MockUserStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
					Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil)
			},
			wantErr: true,
		},
		{
			name:   "CreateThreadRoom unexpected error — returns error",
			msg:    msg,
			siteID: "site-a",
			setupMocks: func(store *MockStore, ts *MockThreadStore) {
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).
					Return(errors.New("mongo: connection refused"))
			},
			wantErr: true,
		},
		{
			name: "first reply — stamps thread_room_id on parent when parentCreatedAt known",
			msg: &model.Message{
				ID:                           "msg-reply",
				RoomID:                       "r1",
				UserID:                       "u-replier",
				UserAccount:                  "replier",
				CreatedAt:                    now,
				ThreadParentMessageID:        "msg-parent",
				ThreadParentMessageCreatedAt: ptrTime(now.Add(-5 * time.Minute)),
			},
			siteID: "site-a",
			setupMocks: func(store *MockStore, ts *MockThreadStore) {
				var capturedRoomID string
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, room *model.ThreadRoom) error {
						capturedRoomID = room.ID
						return nil
					})
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
					Return(parentSender, nil)
				ts.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
				ts.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
				store.EXPECT().UpdateParentMessageThreadRoomID(
					gomock.Any(), "msg-parent", "r1",
					now.Add(-5*time.Minute),
					gomock.Cond(func(x any) bool { s, ok := x.(string); return ok && s == capturedRoomID }),
				).Return(nil)
			},
			extraUserStoreSetup: func(us *MockUserStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
					Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil)
			},
		},
		{
			name: "first reply — UpdateParentMessageThreadRoomID fails — returns error",
			msg: &model.Message{
				ID:                           "msg-reply",
				RoomID:                       "r1",
				UserID:                       "u-replier",
				UserAccount:                  "replier",
				CreatedAt:                    now,
				ThreadParentMessageID:        "msg-parent",
				ThreadParentMessageCreatedAt: ptrTime(now.Add(-5 * time.Minute)),
			},
			siteID: "site-a",
			setupMocks: func(store *MockStore, ts *MockThreadStore) {
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
					Return(parentSender, nil)
				ts.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
				ts.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
				store.EXPECT().UpdateParentMessageThreadRoomID(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(errors.New("cassandra: write timeout"))
			},
			extraUserStoreSetup: func(us *MockUserStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
					Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil)
			},
			wantErr: true,
		},
		{
			name: "subsequent reply — stamps thread_room_id on parent when parentCreatedAt known",
			msg: &model.Message{
				ID:                           "msg-reply",
				RoomID:                       "r1",
				UserID:                       "u-replier",
				UserAccount:                  "replier",
				CreatedAt:                    now,
				ThreadParentMessageID:        "msg-parent",
				ThreadParentMessageCreatedAt: ptrTime(now.Add(-5 * time.Minute)),
			},
			siteID: "site-a",
			setupMocks: func(store *MockStore, ts *MockThreadStore) {
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(errThreadRoomExists)
				ts.EXPECT().GetThreadRoomByParentMessageID(gomock.Any(), "msg-parent").
					Return(&model.ThreadRoom{ID: "tr-existing"}, nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").Return(parentSender, nil)
				ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
				ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
				ts.EXPECT().UpdateThreadRoomLastMessage(gomock.Any(), "tr-existing", "msg-reply", gomock.Any(), now).Return(nil)
				store.EXPECT().UpdateParentMessageThreadRoomID(
					gomock.Any(), "msg-parent", "r1",
					now.Add(-5*time.Minute),
					"tr-existing",
				).Return(nil)
			},
			extraUserStoreSetup: func(us *MockUserStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
					Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil)
			},
		},
		{
			name: "subsequent reply — UpdateParentMessageThreadRoomID fails — returns error",
			msg: &model.Message{
				ID:                           "msg-reply",
				RoomID:                       "r1",
				UserID:                       "u-replier",
				UserAccount:                  "replier",
				CreatedAt:                    now,
				ThreadParentMessageID:        "msg-parent",
				ThreadParentMessageCreatedAt: ptrTime(now.Add(-5 * time.Minute)),
			},
			siteID: "site-a",
			setupMocks: func(store *MockStore, ts *MockThreadStore) {
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(errThreadRoomExists)
				ts.EXPECT().GetThreadRoomByParentMessageID(gomock.Any(), "msg-parent").
					Return(&model.ThreadRoom{ID: "tr-existing"}, nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").Return(parentSender, nil)
				ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
				ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
				ts.EXPECT().UpdateThreadRoomLastMessage(gomock.Any(), "tr-existing", "msg-reply", gomock.Any(), now).Return(nil)
				store.EXPECT().UpdateParentMessageThreadRoomID(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(errors.New("cassandra: write timeout"))
			},
			extraUserStoreSetup: func(us *MockUserStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
					Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil)
			},
			wantErr: true,
		},
		{
			name: "subsequent reply — parent not found but parentCreatedAt known — skips UpdateParentMessageThreadRoomID",
			msg: &model.Message{
				ID:                           "msg-reply",
				RoomID:                       "r1",
				UserID:                       "u-replier",
				UserAccount:                  "replier",
				CreatedAt:                    now,
				ThreadParentMessageID:        "msg-parent",
				ThreadParentMessageCreatedAt: ptrTime(now.Add(-5 * time.Minute)),
			},
			siteID: "site-a",
			setupMocks: func(store *MockStore, ts *MockThreadStore) {
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(errThreadRoomExists)
				ts.EXPECT().GetThreadRoomByParentMessageID(gomock.Any(), "msg-parent").
					Return(&model.ThreadRoom{ID: "tr-existing"}, nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
					Return(nil, fmt.Errorf("wrap: %w", errMessageNotFound))
				ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, sub *model.ThreadSubscription) error {
						assert.Equal(t, "u-replier", sub.UserID)
						return nil
					})
				ts.EXPECT().UpdateThreadRoomLastMessage(gomock.Any(), "tr-existing", "msg-reply", gomock.Any(), now).Return(nil)
				// UpdateParentMessageThreadRoomID must NOT be called — parent doesn't exist
				// FindUserByID also not called — short-circuited by errMessageNotFound branch
			},
		},
		{
			name:   "first reply — parent user not found in userStore — still inserts parent + replier locally, skips inbox",
			msg:    msg,
			siteID: "site-a",
			setupMocks: func(store *MockStore, ts *MockThreadStore) {
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").Return(parentSender, nil)
				// Parent insert still runs (independent of owner-site lookup) —
				// only the cross-site inbox publish is gated on the lookup.
				ts.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, sub *model.ThreadSubscription) error {
						assert.Equal(t, "u-parent", sub.UserID, "parent insert must still happen on missing owner-site")
						return nil
					})
			},
			extraUserStoreSetup: func(us *MockUserStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
					Return(nil, fmt.Errorf("wrap: %w", userstore.ErrUserNotFound))
			},
			expectReplierInsert: true,
		},
		{
			name:   "subsequent reply — parent user not found in userStore — still upserts parent + replier locally, skips parent inbox",
			msg:    msg,
			siteID: "site-a",
			setupMocks: func(store *MockStore, ts *MockThreadStore) {
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).
					Return(errThreadRoomExists)
				ts.EXPECT().GetThreadRoomByParentMessageID(gomock.Any(), "msg-parent").
					Return(&model.ThreadRoom{ID: "tr-existing"}, nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
					Return(parentSender, nil)
				// Parent upsert still runs (independent of owner-site lookup);
				// only the cross-site inbox publish is gated.
				ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, sub *model.ThreadSubscription) error {
						assert.Equal(t, "u-parent", sub.UserID)
						return nil
					})
				ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, sub *model.ThreadSubscription) error {
						assert.Equal(t, "u-replier", sub.UserID)
						return nil
					})
				ts.EXPECT().UpdateThreadRoomLastMessage(gomock.Any(), "tr-existing", "msg-reply", gomock.Any(), now).
					Return(nil)
			},
			extraUserStoreSetup: func(us *MockUserStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
					Return(nil, fmt.Errorf("wrap: %w", userstore.ErrUserNotFound))
			},
		},
		{
			name:   "subsequent reply — parent user lookup DB error — returns error",
			msg:    msg,
			siteID: "site-a",
			setupMocks: func(store *MockStore, ts *MockThreadStore) {
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).
					Return(errThreadRoomExists)
				ts.EXPECT().GetThreadRoomByParentMessageID(gomock.Any(), "msg-parent").
					Return(&model.ThreadRoom{ID: "tr-existing"}, nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
					Return(parentSender, nil)
				// Lookup error short-circuits — no upserts, no UpdateThreadRoomLastMessage.
			},
			extraUserStoreSetup: func(us *MockUserStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
					Return(nil, errors.New("mongo: connection refused"))
			},
			wantErr: true,
		},
		{
			name:   "first reply — parent user lookup DB error — returns error",
			msg:    msg,
			siteID: "site-a",
			setupMocks: func(store *MockStore, ts *MockThreadStore) {
				ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(nil)
				store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").Return(parentSender, nil)
			},
			extraUserStoreSetup: func(us *MockUserStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
					Return(nil, errors.New("mongo: connection refused"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockStore := NewMockStore(ctrl)
			mockThreadStore := NewMockThreadStore(ctrl)
			mockThreadStore.EXPECT().AddReplyAccounts(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			mockUserStore := NewMockUserStore(ctrl)
			tt.setupMocks(mockStore, mockThreadStore)
			if tt.extraUserStoreSetup != nil {
				tt.extraUserStoreSetup(mockUserStore)
			}
			if tt.expectReplierInsert {
				mockThreadStore.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
			}

			h := NewHandler(mockStore, mockUserStore, mockThreadStore, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error {
				return nil
			})
			replier := &model.User{ID: tt.msg.UserID, Account: tt.msg.UserAccount, SiteID: "site-a"}
			_, err := h.handleThreadRoomAndSubscriptions(context.Background(), tt.msg, tt.siteID, replier, false)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestHandler_PublishThreadSubInboxIfRemote(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	// Subscription's SiteID is the room's site (here, site-a — the local handler).
	baseSub := &model.ThreadSubscription{
		ID:              "sub-1",
		ParentMessageID: "pm-1",
		RoomID:          "r1",
		ThreadRoomID:    "tr-1",
		UserID:          "u-bob",
		UserAccount:     "bob",
		SiteID:          "site-a",
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	t.Run("same site — no publish", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		var called bool
		h := NewHandler(NewMockStore(ctrl), NewMockUserStore(ctrl), NewMockThreadStore(ctrl), "site-a",
			func(_ context.Context, _ string, _ []byte, _ string) error {
				called = true
				return nil
			})

		err := h.publishThreadSubInboxIfRemote(context.Background(), baseSub, "site-a", "msg-1")
		require.NoError(t, err)
		assert.False(t, called, "publish must not be called when ownerSiteID == h.siteID")
	})

	t.Run("empty ownerSiteID — skip with warn, no publish", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		var called bool
		h := NewHandler(NewMockStore(ctrl), NewMockUserStore(ctrl), NewMockThreadStore(ctrl), "site-a",
			func(_ context.Context, _ string, _ []byte, _ string) error {
				called = true
				return nil
			})

		err := h.publishThreadSubInboxIfRemote(context.Background(), baseSub, "", "msg-1")
		require.NoError(t, err)
		assert.False(t, called, "publish must not be called when ownerSiteID is empty")
	})

	t.Run("remote owner — publishes with expected subject and dedup ID", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		var captured struct {
			subj    string
			data    []byte
			msgID   string
			callCnt int
		}
		h := NewHandler(NewMockStore(ctrl), NewMockUserStore(ctrl), NewMockThreadStore(ctrl), "site-a",
			func(_ context.Context, subj string, data []byte, msgID string) error {
				captured.subj = subj
				captured.data = data
				captured.msgID = msgID
				captured.callCnt++
				return nil
			})

		err := h.publishThreadSubInboxIfRemote(context.Background(), baseSub, "site-b", "msg-1")
		require.NoError(t, err)
		require.Equal(t, 1, captured.callCnt)
		assert.Equal(t, "chat.inbox.site-b.external.thread_subscription_upserted", captured.subj)
		assert.NotEmpty(t, captured.msgID, "dedup ID must be set")

		// Same inputs → same dedup ID (stable across redeliveries).
		var second string
		h2 := NewHandler(NewMockStore(ctrl), NewMockUserStore(ctrl), NewMockThreadStore(ctrl), "site-a",
			func(_ context.Context, _ string, _ []byte, msgID string) error {
				second = msgID
				return nil
			})
		require.NoError(t, h2.publishThreadSubInboxIfRemote(context.Background(), baseSub, "site-b", "msg-1"))
		assert.Equal(t, captured.msgID, second, "dedup ID must be deterministic for the same (threadRoomID, userID, msgID) seed")

		// Different msgID → different dedup ID.
		var third string
		h3 := NewHandler(NewMockStore(ctrl), NewMockUserStore(ctrl), NewMockThreadStore(ctrl), "site-a",
			func(_ context.Context, _ string, _ []byte, msgID string) error {
				third = msgID
				return nil
			})
		require.NoError(t, h3.publishThreadSubInboxIfRemote(context.Background(), baseSub, "site-b", "msg-2"))
		assert.NotEqual(t, captured.msgID, third)

		// Payload is an InboxEvent whose inner Payload decodes back to the ThreadSubscription
		// — and the inner SiteID is unchanged (still the room's site, "site-a").
		var outer model.InboxEvent
		require.NoError(t, json.Unmarshal(captured.data, &outer))
		assert.Equal(t, model.InboxThreadSubscriptionUpserted, outer.Type)
		assert.Equal(t, "site-a", outer.SiteID)
		assert.Equal(t, "site-b", outer.DestSiteID)
		assert.Greater(t, outer.Timestamp, int64(0))

		var inner model.ThreadSubscription
		require.NoError(t, json.Unmarshal(outer.Payload, &inner))
		assert.Equal(t, *baseSub, inner)
		assert.Equal(t, "site-a", inner.SiteID, "inner SiteID stays as the room's site")
	})

	t.Run("publish error returned", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		boom := errors.New("publish boom")
		h := NewHandler(NewMockStore(ctrl), NewMockUserStore(ctrl), NewMockThreadStore(ctrl), "site-a",
			func(_ context.Context, _ string, _ []byte, _ string) error {
				return boom
			})

		err := h.publishThreadSubInboxIfRemote(context.Background(), baseSub, "site-b", "msg-1")
		require.Error(t, err)
		assert.ErrorIs(t, err, boom)
	})
}

func TestHandler_FirstReply_InboxPublishes(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	parentSender := &cassParticipant{ID: "u-parent", Account: "parent-user"}
	parentUserAtA := &model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}
	parentUserAtC := &model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-c"}

	type publishCall struct {
		subj  string
		data  []byte
		msgID string
	}

	tests := []struct {
		name              string
		replierSite       string
		parentUser        *model.User
		wantPublishToSite map[string]int // destSite → expected count
	}{
		{
			name:              "both local — no publish",
			replierSite:       "site-a",
			parentUser:        parentUserAtA,
			wantPublishToSite: map[string]int{},
		},
		{
			name:              "replier remote — one publish to replier site",
			replierSite:       "site-b",
			parentUser:        parentUserAtA,
			wantPublishToSite: map[string]int{"site-b": 1},
		},
		{
			name:              "parent remote — one publish to parent site",
			replierSite:       "site-a",
			parentUser:        parentUserAtC,
			wantPublishToSite: map[string]int{"site-c": 1},
		},
		{
			name:              "both remote, different sites — two publishes",
			replierSite:       "site-b",
			parentUser:        parentUserAtC,
			wantPublishToSite: map[string]int{"site-b": 1, "site-c": 1},
		},
		{
			name:              "both remote, same site — two publishes to that site",
			replierSite:       "site-b",
			parentUser:        &model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-b"},
			wantPublishToSite: map[string]int{"site-b": 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockStore(ctrl)
			us := NewMockUserStore(ctrl)
			ts := NewMockThreadStore(ctrl)
			ts.EXPECT().AddReplyAccounts(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

			store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").Return(parentSender, nil)
			us.EXPECT().FindUserByID(gomock.Any(), "u-parent").Return(tt.parentUser, nil)
			ts.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
			ts.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)

			var calls []publishCall
			h := NewHandler(store, us, ts, "site-a", func(_ context.Context, subj string, data []byte, msgID string) error {
				calls = append(calls, publishCall{subj: subj, data: data, msgID: msgID})
				return nil
			})

			replier := &model.User{ID: "u-replier", Account: "replier", SiteID: tt.replierSite}
			msg := &model.Message{
				ID:                    "msg-reply",
				RoomID:                "r1",
				UserID:                "u-replier",
				UserAccount:           "replier",
				CreatedAt:             now,
				ThreadParentMessageID: "msg-parent",
			}

			err := h.handleFirstThreadReply(context.Background(), msg, "site-a", "tr-1", replier, now, false)
			require.NoError(t, err)

			gotByDest := map[string]int{}
			for _, c := range calls {
				var outer model.InboxEvent
				require.NoError(t, json.Unmarshal(c.data, &outer))
				assert.Equal(t, model.InboxThreadSubscriptionUpserted, outer.Type)
				gotByDest[outer.DestSiteID]++
			}
			assert.Equal(t, tt.wantPublishToSite, gotByDest)
		})
	}
}

func TestHandler_FirstReply_InboxPublishError_NAKs(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	ts := NewMockThreadStore(ctrl)
	ts.EXPECT().AddReplyAccounts(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
		Return(&cassParticipant{ID: "u-parent", Account: "parent-user"}, nil)
	us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
		Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-c"}, nil)
	ts.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
	// Replier insert never reached because parent-publish fails first.

	boom := errors.New("publish boom")
	h := NewHandler(store, us, ts, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error {
		return boom
	})

	msg := &model.Message{
		ID: "msg-reply", RoomID: "r1", UserID: "u-replier", UserAccount: "replier",
		CreatedAt: now, ThreadParentMessageID: "msg-parent",
	}
	err := h.handleFirstThreadReply(context.Background(), msg, "site-a",
		"tr-1", &model.User{ID: "u-replier", SiteID: "site-b"}, now, false)
	require.Error(t, err)
	assert.ErrorIs(t, err, boom)
}

func TestHandler_FirstReply_ReplierInboxPublishError_NAKs(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	ts := NewMockThreadStore(ctrl)
	ts.EXPECT().AddReplyAccounts(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	// Parent at the local site → no parent publish.
	store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
		Return(&cassParticipant{ID: "u-parent", Account: "parent-user"}, nil)
	us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
		Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil)
	// Both inserts run; replier publish fails.
	ts.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
	ts.EXPECT().InsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)

	boom := errors.New("publish boom")
	h := NewHandler(store, us, ts, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error {
		return boom
	})

	msg := &model.Message{
		ID: "msg-reply", RoomID: "r1", UserID: "u-replier", UserAccount: "replier",
		CreatedAt: now, ThreadParentMessageID: "msg-parent",
	}
	err := h.handleFirstThreadReply(context.Background(), msg, "site-a",
		"tr-1", &model.User{ID: "u-replier", Account: "replier", SiteID: "site-b"}, now, false)
	require.Error(t, err)
	assert.ErrorIs(t, err, boom)
}

func TestHandler_SubsequentReply_InboxPublishes(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	parentSender := &cassParticipant{ID: "u-parent", Account: "parent-user"}
	parentUserAtA := &model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}
	parentUserAtC := &model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-c"}

	tests := []struct {
		name              string
		replierSite       string
		parentUser        *model.User
		wantPublishToSite map[string]int
	}{
		{
			name:              "both local — no publish",
			replierSite:       "site-a",
			parentUser:        parentUserAtA,
			wantPublishToSite: map[string]int{},
		},
		{
			name:              "replier remote — one publish",
			replierSite:       "site-b",
			parentUser:        parentUserAtA,
			wantPublishToSite: map[string]int{"site-b": 1},
		},
		{
			name:              "parent remote — one publish",
			replierSite:       "site-a",
			parentUser:        parentUserAtC,
			wantPublishToSite: map[string]int{"site-c": 1},
		},
		{
			name:              "both remote, different sites — two publishes",
			replierSite:       "site-b",
			parentUser:        parentUserAtC,
			wantPublishToSite: map[string]int{"site-b": 1, "site-c": 1},
		},
		{
			name:              "both remote, same site — two publishes to that site",
			replierSite:       "site-b",
			parentUser:        &model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-b"},
			wantPublishToSite: map[string]int{"site-b": 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockStore(ctrl)
			us := NewMockUserStore(ctrl)
			ts := NewMockThreadStore(ctrl)
			ts.EXPECT().AddReplyAccounts(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

			ts.EXPECT().GetThreadRoomByParentMessageID(gomock.Any(), "msg-parent").
				Return(&model.ThreadRoom{ID: "tr-existing"}, nil)
			store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").Return(parentSender, nil)
			us.EXPECT().FindUserByID(gomock.Any(), "u-parent").Return(tt.parentUser, nil)
			ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
			ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
			ts.EXPECT().UpdateThreadRoomLastMessage(gomock.Any(), "tr-existing", "msg-reply", gomock.Any(), now).Return(nil)

			var publishedDests []string
			h := NewHandler(store, us, ts, "site-a", func(_ context.Context, _ string, data []byte, _ string) error {
				var outer model.InboxEvent
				if err := json.Unmarshal(data, &outer); err != nil {
					return err
				}
				publishedDests = append(publishedDests, outer.DestSiteID)
				return nil
			})

			replier := &model.User{ID: "u-replier", Account: "replier", SiteID: tt.replierSite}
			msg := &model.Message{
				ID:                    "msg-reply",
				RoomID:                "r1",
				UserID:                "u-replier",
				UserAccount:           "replier",
				CreatedAt:             now,
				ThreadParentMessageID: "msg-parent",
			}

			roomID, err := h.handleSubsequentThreadReply(context.Background(), msg, "site-a", replier, now, false)
			require.NoError(t, err)
			assert.Equal(t, "tr-existing", roomID)

			gotByDest := map[string]int{}
			for _, d := range publishedDests {
				gotByDest[d]++
			}
			assert.Equal(t, tt.wantPublishToSite, gotByDest)
		})
	}
}

func TestHandler_SubsequentReply_InboxPublishError_NAKs(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	ts := NewMockThreadStore(ctrl)
	ts.EXPECT().AddReplyAccounts(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	ts.EXPECT().GetThreadRoomByParentMessageID(gomock.Any(), "msg-parent").
		Return(&model.ThreadRoom{ID: "tr-1"}, nil)
	store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
		Return(&cassParticipant{ID: "u-parent", Account: "parent-user"}, nil)
	us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
		Return(&model.User{ID: "u-parent", SiteID: "site-c"}, nil)
	ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)

	boom := errors.New("publish boom")
	h := NewHandler(store, us, ts, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error {
		return boom
	})

	msg := &model.Message{
		ID: "msg-reply", RoomID: "r1", UserID: "u-replier", UserAccount: "replier",
		CreatedAt: now, ThreadParentMessageID: "msg-parent",
	}
	_, err := h.handleSubsequentThreadReply(context.Background(), msg, "site-a",
		&model.User{ID: "u-replier", SiteID: "site-b"}, now, false)
	require.Error(t, err)
	assert.ErrorIs(t, err, boom)
}

func TestHandler_MarkThreadMentions_InboxPublishes(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name              string
		mentionees        []model.Participant
		wantPublishToSite map[string]int
	}{
		{
			name:              "no mentions — no publish",
			mentionees:        nil,
			wantPublishToSite: map[string]int{},
		},
		{
			name: "local mentionee — mark only, no publish",
			mentionees: []model.Participant{
				{UserID: "u-bob", Account: "bob", SiteID: "site-a"},
			},
			wantPublishToSite: map[string]int{},
		},
		{
			name: "remote mentionee — mark and publish",
			mentionees: []model.Participant{
				{UserID: "u-bob", Account: "bob", SiteID: "site-b"},
			},
			wantPublishToSite: map[string]int{"site-b": 1},
		},
		{
			name: "two remote mentionees in different sites — two publishes",
			mentionees: []model.Participant{
				{UserID: "u-bob", Account: "bob", SiteID: "site-b"},
				{UserID: "u-carol", Account: "carol", SiteID: "site-c"},
			},
			wantPublishToSite: map[string]int{"site-b": 1, "site-c": 1},
		},
		{
			name: "@all is skipped — no mark, no publish",
			mentionees: []model.Participant{
				{Account: "all", EngName: "all"},
			},
			wantPublishToSite: map[string]int{},
		},
		{
			name: "sender self-mention is skipped",
			mentionees: []model.Participant{
				{UserID: "u-sender", Account: "sender", SiteID: "site-b"},
			},
			wantPublishToSite: map[string]int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ts := NewMockThreadStore(ctrl)
			ts.EXPECT().AddReplyAccounts(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			stubHistoryAllMembers(ts)

			expectedMarks := 0
			for _, p := range tt.mentionees {
				if p.Account == "all" {
					continue
				}
				if p.UserID == "u-sender" {
					continue
				}
				expectedMarks++
			}
			ts.EXPECT().MarkThreadSubscriptionMention(gomock.Any(), gomock.Any()).
				Times(expectedMarks).Return(nil)

			var publishedDests []string
			h := NewHandler(NewMockStore(ctrl), NewMockUserStore(ctrl), ts, "site-a",
				func(_ context.Context, _ string, data []byte, _ string) error {
					var outer model.InboxEvent
					if err := json.Unmarshal(data, &outer); err != nil {
						return err
					}
					publishedDests = append(publishedDests, outer.DestSiteID)
					return nil
				})

			msg := &model.Message{
				ID:                    "msg-reply",
				RoomID:                "r1",
				UserID:                "u-sender",
				UserAccount:           "sender",
				CreatedAt:             now,
				ThreadParentMessageID: "msg-parent",
				Mentions:              tt.mentionees,
			}
			err := h.markThreadMentions(context.Background(), msg, "tr-1", "site-a", false)
			require.NoError(t, err)

			gotByDest := map[string]int{}
			for _, d := range publishedDests {
				gotByDest[d]++
			}
			assert.Equal(t, tt.wantPublishToSite, gotByDest)
		})
	}
}

func TestHandler_MarkThreadMentions_InboxPublishError_NAKs(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	ctrl := gomock.NewController(t)
	ts := NewMockThreadStore(ctrl)
	ts.EXPECT().AddReplyAccounts(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	ts.EXPECT().MarkThreadSubscriptionMention(gomock.Any(), gomock.Any()).Return(nil)
	stubHistoryAllMembers(ts)

	boom := errors.New("publish boom")
	h := NewHandler(NewMockStore(ctrl), NewMockUserStore(ctrl), ts, "site-a",
		func(_ context.Context, _ string, _ []byte, _ string) error { return boom })

	msg := &model.Message{
		ID: "msg-reply", RoomID: "r1", UserID: "u-sender", UserAccount: "sender",
		CreatedAt: now, ThreadParentMessageID: "msg-parent",
		Mentions: []model.Participant{{UserID: "u-bob", Account: "bob", SiteID: "site-b"}},
	}
	err := h.markThreadMentions(context.Background(), msg, "tr-1", "site-a", false)
	require.Error(t, err)
	assert.ErrorIs(t, err, boom)
}

func TestHandler_MarkThreadMentions_HasMentionInPayload(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	ctrl := gomock.NewController(t)
	ts := NewMockThreadStore(ctrl)
	ts.EXPECT().AddReplyAccounts(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	ts.EXPECT().MarkThreadSubscriptionMention(gomock.Any(), gomock.Any()).Return(nil)
	stubHistoryAllMembers(ts)

	var captured []byte
	h := NewHandler(NewMockStore(ctrl), NewMockUserStore(ctrl), ts, "site-a",
		func(_ context.Context, _ string, data []byte, _ string) error {
			captured = data
			return nil
		})

	msg := &model.Message{
		ID: "msg-reply", RoomID: "r1", UserID: "u-sender", UserAccount: "sender",
		CreatedAt: now, ThreadParentMessageID: "msg-parent",
		Mentions: []model.Participant{{UserID: "u-bob", Account: "bob", SiteID: "site-b"}},
	}
	require.NoError(t, h.markThreadMentions(context.Background(), msg, "tr-1", "site-a", false))

	var outer model.InboxEvent
	require.NoError(t, json.Unmarshal(captured, &outer))
	var sub model.ThreadSubscription
	require.NoError(t, json.Unmarshal(outer.Payload, &sub))
	assert.True(t, sub.HasMention, "inbox-emitted ThreadSubscription must carry HasMention=true")
	assert.Equal(t, "u-bob", sub.UserID)
	assert.Equal(t, "site-a", sub.SiteID, "Subscription.SiteID is the room's site, not the mentionee's owner site")
}

// stubHistoryAllMembers makes GetHistorySharedSince treat every queried account as a
// full-access member (nil window), so the history gate in markThreadMentions includes
// them all. Used by thread-mention tests that predate the visibility gate.
func stubHistoryAllMembers(ts *MockThreadStore) {
	ts.EXPECT().GetHistorySharedSince(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, accounts []string) (map[string]*time.Time, error) {
			m := make(map[string]*time.Time, len(accounts))
			for _, a := range accounts {
				m[a] = nil
			}
			return m, nil
		}).AnyTimes()
}

func TestHandler_MarkThreadMentions_SkipsRestrictedAndNonMembers(t *testing.T) {
	parentAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	joinedAfter := parentAt.Add(time.Hour)
	replyAt := parentAt.Add(2 * time.Hour)

	ctrl := gomock.NewController(t)
	ts := NewMockThreadStore(ctrl)

	// alice: member, full access → kept. bob: member, joined after parent → skipped.
	// carol: absent from the map → non-member → skipped.
	ts.EXPECT().GetHistorySharedSince(gomock.Any(), "r1", gomock.Any()).
		Return(map[string]*time.Time{"alice": nil, "bob": &joinedAfter}, nil)
	// Only alice is subscribed and added to replyAccounts.
	ts.EXPECT().MarkThreadSubscriptionMention(gomock.Any(), gomock.Cond(func(x any) bool {
		return x.(*model.ThreadSubscription).UserAccount == "alice"
	})).Return(nil)
	ts.EXPECT().AddReplyAccounts(gomock.Any(), "tr-1", []string{"alice"}).Return(nil)

	h := NewHandler(NewMockStore(ctrl), NewMockUserStore(ctrl), ts, "site-a",
		func(_ context.Context, _ string, _ []byte, _ string) error { return nil })

	msg := &model.Message{
		ID: "msg-reply", RoomID: "r1", UserID: "u-sender", UserAccount: "sender",
		CreatedAt: replyAt, ThreadParentMessageID: "msg-parent", ThreadParentMessageCreatedAt: &parentAt,
		Mentions: []model.Participant{
			{UserID: "u-al", Account: "alice", SiteID: "site-a"},
			{UserID: "u-bo", Account: "bob", SiteID: "site-a"},
			{UserID: "u-ca", Account: "carol", SiteID: "site-a"},
		},
	}
	require.NoError(t, h.markThreadMentions(context.Background(), msg, "tr-1", "site-a", false))
}

// fakeJSMsg is a minimal jetstream.Msg test double that records whether Ack or
// Nak was called so tests can assert on ack/nak behaviour. numDelivered seeds
// the metadata so backoff selection can be exercised; nakDelay captures the
// delay passed to NakWithDelay.
type fakeJSMsg struct {
	data         []byte
	numDelivered uint64
	acked        bool
	naked        bool
	nakDelay     time.Duration
}

func (m *fakeJSMsg) Data() []byte { return m.data }
func (m *fakeJSMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: m.numDelivered}, nil
}
func (m *fakeJSMsg) Headers() nats.Header            { return nil }
func (m *fakeJSMsg) Subject() string                 { return "test.subject" }
func (m *fakeJSMsg) Reply() string                   { return "" }
func (m *fakeJSMsg) Ack() error                      { m.acked = true; return nil }
func (m *fakeJSMsg) DoubleAck(context.Context) error { m.acked = true; return nil }
func (m *fakeJSMsg) Nak() error                      { m.naked = true; return nil }
func (m *fakeJSMsg) NakWithDelay(d time.Duration) error {
	m.naked = true
	m.nakDelay = d
	return nil
}
func (m *fakeJSMsg) InProgress() error           { return nil }
func (m *fakeJSMsg) Term() error                 { return nil }
func (m *fakeJSMsg) TermWithReason(string) error { return nil }

func TestHandler_HandleJetStreamMsg(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	user := &model.User{
		ID: "u-1", Account: "alice", SiteID: "site-a",
		EngName: "Alice Wang", ChineseName: "愛麗絲",
	}
	msg := model.Message{
		ID: "msg-1", RoomID: "r1", UserID: "u-1", UserAccount: "alice",
		Content: "hello", CreatedAt: now,
	}
	evt := model.MessageEvent{Message: msg, SiteID: "site-a", Timestamp: now.UnixMilli()}
	validData, _ := json.Marshal(evt)
	invalidData := []byte("{invalid")

	expectedSender := cassParticipant{
		ID: user.ID, EngName: user.EngName, CompanyName: user.ChineseName, Account: msg.UserAccount,
	}

	tests := []struct {
		name         string
		msgData      []byte
		setupMocks   func(store *MockStore, us *MockUserStore, ts *MockThreadStore)
		wantAck      bool
		wantNak      bool
		wantNakDelay bool
	}{
		{
			name:    "success — Ack called",
			msgData: validData,
			setupMocks: func(store *MockStore, us *MockUserStore, ts *MockThreadStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)
				store.EXPECT().SaveMessage(gomock.Any(), &msg, &expectedSender, "site-a").Return(nil)
			},
			wantAck: true,
		},
		{
			// A malformed event can never parse on redelivery, so it is a poison
			// message: Ack to drop it rather than retry forever / waste the budget.
			name:       "malformed event — Ack to drop poison",
			msgData:    invalidData,
			setupMocks: func(store *MockStore, us *MockUserStore, ts *MockThreadStore) {},
			wantAck:    true,
		},
		{
			// A transient Cassandra failure must NOT be dropped — Nak with a
			// positive backoff delay so a brief outage doesn't burn through
			// MaxDeliver and silently lose a persisted message.
			name:    "transient store error — Nak with backoff delay",
			msgData: validData,
			setupMocks: func(store *MockStore, us *MockUserStore, ts *MockThreadStore) {
				us.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)
				store.EXPECT().SaveMessage(gomock.Any(), &msg, &expectedSender, "site-a").Return(errors.New("cassandra unavailable"))
			},
			wantNak:      true,
			wantNakDelay: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockStore := NewMockStore(ctrl)
			mockUserStore := NewMockUserStore(ctrl)
			mockThreadStore := NewMockThreadStore(ctrl)
			mockThreadStore.EXPECT().AddReplyAccounts(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			stubHistoryAllMembers(mockThreadStore)
			tt.setupMocks(mockStore, mockUserStore, mockThreadStore)

			h := NewHandler(mockStore, mockUserStore, mockThreadStore, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error {
				return nil
			})

			fakeMsg := &fakeJSMsg{data: tt.msgData}
			h.HandleJetStreamMsg(context.Background(), fakeMsg)

			assert.Equal(t, tt.wantAck, fakeMsg.acked, "acked")
			assert.Equal(t, tt.wantNak, fakeMsg.naked, "naked")
			if tt.wantNakDelay {
				assert.Positive(t, fakeMsg.nakDelay, "nak delay should be a positive backoff")
			}
		})
	}
}

func TestHandler_ProcessMessage_Quote(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	user := &model.User{
		ID:          "u-1",
		Account:     "alice",
		SiteID:      "site-a",
		EngName:     "Alice Wang",
		ChineseName: "愛麗絲",
	}
	expectedSender := cassParticipant{
		ID:          user.ID,
		EngName:     user.EngName,
		CompanyName: user.ChineseName,
		Account:     "alice",
	}

	snapshot := &cassandra.QuotedParentMessage{
		MessageID:   "parent-msg-uuid",
		RoomID:      "r1",
		Sender:      cassandra.Participant{ID: "u-bob", Account: "bob", EngName: "Bob Chen"},
		CreatedAt:   time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC),
		Msg:         "the original message",
		MessageLink: "http://localhost:3000/r1/parent-msg-uuid",
	}

	quotedMsg := model.Message{
		ID:                  "msg-quote-1",
		RoomID:              "r1",
		UserID:              "u-1",
		UserAccount:         "alice",
		Content:             "great point!",
		CreatedAt:           now,
		QuotedParentMessage: snapshot,
	}
	quotedEvt := model.MessageEvent{Message: quotedMsg, SiteID: "site-a", Timestamp: now.UnixMilli()}
	quotedData, err := json.Marshal(quotedEvt)
	require.NoError(t, err)

	t.Run("quote snapshot reaches SaveMessage unchanged", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		userStore := NewMockUserStore(ctrl)
		threadStore := NewMockThreadStore(ctrl)
		threadStore.EXPECT().AddReplyAccounts(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		userStore.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)
		store.EXPECT().
			SaveMessage(gomock.Any(), &quotedMsg, &expectedSender, "site-a").
			DoAndReturn(func(_ context.Context, m *model.Message, _ *cassParticipant, _ string) error {
				require.NotNil(t, m.QuotedParentMessage, "QuotedParentMessage must be forwarded")
				assert.Equal(t, "parent-msg-uuid", m.QuotedParentMessage.MessageID)
				assert.Equal(t, "the original message", m.QuotedParentMessage.Msg)
				return nil
			})

		h := NewHandler(store, userStore, threadStore, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error {
			return nil
		})
		err := h.processMessage(context.Background(), quotedData, false)
		require.NoError(t, err)
	})
}

func TestHandler_ProcessMessage_Quote_Unverified(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	user := &model.User{ID: "u-1", Account: "alice", SiteID: "site-a", EngName: "Alice Wang", ChineseName: "愛麗絲"}

	// clientFallback is the UNTRUSTED snapshot the gatekeeper fell back to: the
	// sender and text could be fabricated. The link/id are already sanitized by
	// the gatekeeper.
	clientFallback := func() *cassandra.QuotedParentMessage {
		return &cassandra.QuotedParentMessage{
			MessageID:   "parent-msg-uuid",
			RoomID:      "r1",
			Sender:      cassandra.Participant{ID: "u-evil", Account: "evil", EngName: "Not Bob"},
			CreatedAt:   time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
			Msg:         "fabricated words in bob's mouth",
			MessageLink: "http://localhost:3000/r1/parent-msg-uuid",
		}
	}

	buildEvt := func(fb *cassandra.QuotedParentMessage) []byte {
		evt := model.MessageEvent{
			Message: model.Message{
				ID: "msg-quote-1", RoomID: "r1", UserID: "u-1", UserAccount: "alice",
				Content: "great point!", CreatedAt: now, QuotedParentMessage: fb,
			},
			SiteID:                 "site-a",
			Timestamp:              now.UnixMilli(),
			QuotedParentUnverified: true,
		}
		data, err := json.Marshal(evt)
		require.NoError(t, err)
		return data
	}

	t.Run("re-projects authoritative snapshot, preserving the trusted link", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		userStore := NewMockUserStore(ctrl)
		threadStore := NewMockThreadStore(ctrl)
		threadStore.EXPECT().AddReplyAccounts(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		userStore.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)

		authoritative := &cassandra.QuotedParentMessage{
			MessageID: "parent-msg-uuid",
			RoomID:    "r1",
			Sender:    cassandra.Participant{ID: "u-bob", Account: "bob", EngName: "Bob Chen"},
			CreatedAt: time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC),
			Msg:       "the real original message",
			// no MessageLink — the store leaves it to the caller
		}
		store.EXPECT().GetQuotedParentSnapshot(gomock.Any(), "parent-msg-uuid").Return(authoritative, true, nil)
		store.EXPECT().
			SaveMessage(gomock.Any(), gomock.Any(), gomock.Any(), "site-a").
			DoAndReturn(func(_ context.Context, m *model.Message, _ *cassParticipant, _ string) error {
				require.NotNil(t, m.QuotedParentMessage)
				assert.Equal(t, "bob", m.QuotedParentMessage.Sender.Account, "fabricated sender must be overwritten")
				assert.Equal(t, "the real original message", m.QuotedParentMessage.Msg, "fabricated text must be overwritten")
				assert.Equal(t, "http://localhost:3000/r1/parent-msg-uuid", m.QuotedParentMessage.MessageLink, "trusted link preserved")
				return nil
			})

		h := NewHandler(store, userStore, threadStore, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error { return nil })
		require.NoError(t, h.processMessage(context.Background(), buildEvt(clientFallback()), false))
	})

	t.Run("drops the quote when the parent cannot be confirmed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		userStore := NewMockUserStore(ctrl)
		threadStore := NewMockThreadStore(ctrl)
		threadStore.EXPECT().AddReplyAccounts(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		userStore.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)

		store.EXPECT().GetQuotedParentSnapshot(gomock.Any(), "parent-msg-uuid").Return(nil, false, nil)
		store.EXPECT().
			SaveMessage(gomock.Any(), gomock.Any(), gomock.Any(), "site-a").
			DoAndReturn(func(_ context.Context, m *model.Message, _ *cassParticipant, _ string) error {
				assert.Nil(t, m.QuotedParentMessage, "unverifiable fabricated quote must be dropped, not persisted")
				return nil
			})

		h := NewHandler(store, userStore, threadStore, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error { return nil })
		require.NoError(t, h.processMessage(context.Background(), buildEvt(clientFallback()), false))
	})

	t.Run("drops the quote when the authoritative parent is in a different room (security)", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		userStore := NewMockUserStore(ctrl)
		threadStore := NewMockThreadStore(ctrl)
		threadStore.EXPECT().AddReplyAccounts(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		userStore.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)

		// The ID resolves to a real message — but in room r2, which the sender
		// referenced by raw ID during the outage. Its body must not leak into r1.
		foreignRoom := &cassandra.QuotedParentMessage{
			MessageID: "parent-msg-uuid",
			RoomID:    "r2",
			Sender:    cassandra.Participant{ID: "u-bob", Account: "bob", EngName: "Bob Chen"},
			CreatedAt: time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC),
			Msg:       "secret from another room",
		}
		store.EXPECT().GetQuotedParentSnapshot(gomock.Any(), "parent-msg-uuid").Return(foreignRoom, true, nil)
		store.EXPECT().
			SaveMessage(gomock.Any(), gomock.Any(), gomock.Any(), "site-a").
			DoAndReturn(func(_ context.Context, m *model.Message, _ *cassParticipant, _ string) error {
				assert.Nil(t, m.QuotedParentMessage, "cross-room authoritative quote must be dropped, not persisted")
				return nil
			})

		h := NewHandler(store, userStore, threadStore, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error { return nil })
		require.NoError(t, h.processMessage(context.Background(), buildEvt(clientFallback()), false))
	})

	t.Run("re-projection store error NAKs (no persist)", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		userStore := NewMockUserStore(ctrl)
		threadStore := NewMockThreadStore(ctrl)
		userStore.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)

		store.EXPECT().GetQuotedParentSnapshot(gomock.Any(), "parent-msg-uuid").Return(nil, false, fmt.Errorf("cassandra down"))
		// SaveMessage must NOT be called — the error short-circuits before persist.

		h := NewHandler(store, userStore, threadStore, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error { return nil })
		err := h.processMessage(context.Background(), buildEvt(clientFallback()), false)
		require.Error(t, err)
	})
}

func TestAuthoritativeQuoteMatchesConversation(t *testing.T) {
	tests := []struct {
		name      string
		msgRoom   string
		msgThread string // Message.ThreadParentMessageID
		snapRoom  string
		snapID    string // QuotedParentMessage.MessageID
		snapPID   string // QuotedParentMessage.ThreadParentID
		want      bool
	}{
		{name: "main-room quotes main-room parent, same room", msgRoom: "r1", snapRoom: "r1", snapID: "p1", want: true},
		{name: "different room rejected", msgRoom: "r1", snapRoom: "r2", snapID: "p1", want: false},
		{name: "main-room quoting a thread reply rejected", msgRoom: "r1", snapRoom: "r1", snapID: "p1", snapPID: "t1", want: false},
		{name: "thread reply quotes same-thread message", msgRoom: "r1", msgThread: "t1", snapRoom: "r1", snapID: "p1", snapPID: "t1", want: true},
		{name: "thread reply quotes unrelated main-room message rejected", msgRoom: "r1", msgThread: "t1", snapRoom: "r1", snapID: "p2", snapPID: "", want: false},
		{name: "thread reply quotes different thread rejected", msgRoom: "r1", msgThread: "t1", snapRoom: "r1", snapID: "p1", snapPID: "t2", want: false},
		{name: "thread reply quotes its own thread root", msgRoom: "r1", msgThread: "t1", snapRoom: "r1", snapID: "t1", snapPID: "", want: true},
		{name: "thread reply quotes a foreign-room same-thread-id rejected", msgRoom: "r1", msgThread: "t1", snapRoom: "r2", snapID: "p1", snapPID: "t1", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := &model.Message{RoomID: tc.msgRoom, ThreadParentMessageID: tc.msgThread}
			snap := &cassandra.QuotedParentMessage{RoomID: tc.snapRoom, MessageID: tc.snapID, ThreadParentID: tc.snapPID}
			assert.Equal(t, tc.want, authoritativeQuoteMatchesConversation(msg, snap))
		})
	}
}

func TestHandler_ProcessMessage_ThreadReplyPublish(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	parentCreatedAt := now.Add(-10 * time.Minute)

	user := &model.User{
		ID:          "u-1",
		Account:     "alice",
		SiteID:      "site-a",
		EngName:     "Alice Wang",
		ChineseName: "愛麗絲",
	}
	expectedSender := cassParticipant{
		ID:          user.ID,
		EngName:     user.EngName,
		CompanyName: user.ChineseName,
		Account:     "alice",
	}
	threadMsg := model.Message{
		ID:                           "msg-reply",
		RoomID:                       "r1",
		UserID:                       "u-1",
		UserAccount:                  "alice",
		Content:                      "thread reply",
		CreatedAt:                    now,
		ThreadParentMessageID:        "msg-parent",
		ThreadParentMessageCreatedAt: &parentCreatedAt,
	}
	// The wire event omits the parent createdAt (gatekeeper soft-failed), so this
	// suite exercises the messages_by_id fallback; threadMsg above (with the
	// resolved value) is what the fallback must reconstruct for the writes.
	evtMsg := threadMsg
	evtMsg.ThreadParentMessageCreatedAt = nil
	threadEvt := model.MessageEvent{Message: evtMsg, SiteID: "site-a", Timestamp: now.UnixMilli()}
	threadData, _ := json.Marshal(threadEvt)

	setupCommonMocks := func(store *MockStore, us *MockUserStore, ts *MockThreadStore) {
		// Parent resolved from messages_by_id; the stamp below uses this value.
		store.EXPECT().GetMessageCreatedAt(gomock.Any(), "msg-parent").Return(parentCreatedAt, true, nil)
		us.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)
		ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(errThreadRoomExists)
		ts.EXPECT().GetThreadRoomByParentMessageID(gomock.Any(), "msg-parent").
			Return(&model.ThreadRoom{ID: "tr-1"}, nil)
		store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
			Return(&cassParticipant{ID: "u-parent", Account: "parent-user"}, nil)
		us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
			Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil)
		ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
		ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
		ts.EXPECT().UpdateThreadRoomLastMessage(gomock.Any(), "tr-1", "msg-reply", gomock.Any(), now).Return(nil)
		ts.EXPECT().AdvanceThreadSubscriptionLastSeen(gomock.Any(), "tr-1", "alice", now).Return(nil)
		// parentFound && ThreadParentMessageCreatedAt != nil → stamps thread_room_id on parent.
		store.EXPECT().UpdateParentMessageThreadRoomID(
			gomock.Any(), "msg-parent", "r1", parentCreatedAt, "tr-1",
		).Return(nil)
	}

	t.Run("publishes MessageEvent to canonical thread reply subject", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		us := NewMockUserStore(ctrl)
		ts := NewMockThreadStore(ctrl)
		setupCommonMocks(store, us, ts)

		newTcount := 3
		store.EXPECT().SaveThreadMessage(gomock.Any(), &threadMsg, &expectedSender, "site-a", "tr-1").
			Return(&newTcount, nil)

		var capturedSubj string
		var capturedData []byte
		var capturedMsgID string
		publishCount := 0
		h := NewHandler(store, us, ts, "site-a", func(_ context.Context, subj string, data []byte, msgID string) error {
			publishCount++
			capturedSubj = subj
			capturedData = data
			capturedMsgID = msgID
			return nil
		})

		require.NoError(t, h.processMessage(context.Background(), threadData, false))

		require.Equal(t, 1, publishCount, "exactly one publish call for thread reply event")
		assert.Equal(t, subject.ServerBroadcastThreadTCount("site-a"), capturedSubj)
		assert.Equal(t, "", capturedMsgID, "core NATS publish must have empty msgID")

		var evt model.MessageEvent
		require.NoError(t, json.Unmarshal(capturedData, &evt))
		assert.Equal(t, model.EventThreadReplyAdded, evt.Event)
		assert.Equal(t, "msg-reply", evt.Message.ID)
		assert.Equal(t, "r1", evt.Message.RoomID)
		assert.Equal(t, "msg-parent", evt.Message.ThreadParentMessageID)
		assert.Equal(t, "site-a", evt.SiteID)
		require.NotNil(t, evt.NewTCount)
		assert.Equal(t, 3, *evt.NewTCount)
		assert.Greater(t, evt.Timestamp, int64(0))
	})

	t.Run("event-carried (gatekeeper-resolved) parent createdAt is trusted — no Cassandra lookup", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		us := NewMockUserStore(ctrl)
		ts := NewMockThreadStore(ctrl)

		// The event carries a server-resolved value distinct from what Cassandra
		// would return; all writes must use the event value, and GetMessageCreatedAt
		// must not be called (no expectation registered — any call fails).
		eventValue := now.Add(-30 * time.Minute)
		carriedMsg := threadMsg
		carriedMsg.ThreadParentMessageCreatedAt = &eventValue
		carriedEvt := model.MessageEvent{Message: carriedMsg, SiteID: "site-a", Timestamp: now.UnixMilli()}
		carriedData, _ := json.Marshal(carriedEvt)

		us.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)
		ts.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(errThreadRoomExists)
		ts.EXPECT().GetThreadRoomByParentMessageID(gomock.Any(), "msg-parent").
			Return(&model.ThreadRoom{ID: "tr-1"}, nil)
		store.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
			Return(&cassParticipant{ID: "u-parent", Account: "parent-user"}, nil)
		us.EXPECT().FindUserByID(gomock.Any(), "u-parent").
			Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil)
		ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
		ts.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
		ts.EXPECT().UpdateThreadRoomLastMessage(gomock.Any(), "tr-1", "msg-reply", gomock.Any(), now).Return(nil)
		ts.EXPECT().AdvanceThreadSubscriptionLastSeen(gomock.Any(), "tr-1", "alice", now).Return(nil)
		// The stamp must use the EVENT value.
		store.EXPECT().UpdateParentMessageThreadRoomID(
			gomock.Any(), "msg-parent", "r1", eventValue, "tr-1",
		).Return(nil)
		// The persisted reply must carry the event value.
		newTcount := 1
		store.EXPECT().SaveThreadMessage(gomock.Any(), &carriedMsg, &expectedSender, "site-a", "tr-1").
			Return(&newTcount, nil)

		h := NewHandler(store, us, ts, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error {
			return nil
		})

		require.NoError(t, h.processMessage(context.Background(), carriedData, false))
	})

	t.Run("NAKs (returns error) when parent createdAt resolution fails — Cassandra down", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		us := NewMockUserStore(ctrl)
		ts := NewMockThreadStore(ctrl)

		us.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)
		// A genuine Cassandra failure (not a clean miss) must surface so the worker NAKs
		// and replays once Cassandra recovers — nothing is dropped or written downstream.
		store.EXPECT().GetMessageCreatedAt(gomock.Any(), "msg-parent").
			Return(time.Time{}, false, errors.New("gocql: no hosts available"))

		h := NewHandler(store, us, ts, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error {
			return nil
		})

		err := h.processMessage(context.Background(), threadData, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "resolve thread parent createdAt")
	})

	t.Run("NAKs (returns error) when parent not yet in messages_by_id — no partial writes", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		us := NewMockUserStore(ctrl)
		ts := NewMockThreadStore(ctrl)

		us.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)
		// Clean miss (found=false): the parent's own canonical write hasn't landed yet.
		// NAK for redelivery rather than persist the reply with a null parent createdAt.
		// No thread-room or Cassandra write must happen — gomock fails on any unexpected call.
		store.EXPECT().GetMessageCreatedAt(gomock.Any(), "msg-parent").Return(time.Time{}, false, nil)

		h := NewHandler(store, us, ts, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error {
			return nil
		})

		err := h.processMessage(context.Background(), threadData, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not yet persisted")
	})

	t.Run("publish error propagates for JetStream retry", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		us := NewMockUserStore(ctrl)
		ts := NewMockThreadStore(ctrl)
		setupCommonMocks(store, us, ts)

		newTcount := 3
		store.EXPECT().SaveThreadMessage(gomock.Any(), &threadMsg, &expectedSender, "site-a", "tr-1").
			Return(&newTcount, nil)

		h := NewHandler(store, us, ts, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error {
			return errors.New("nats: publish failed")
		})

		// Publish failure propagates so the caller can log it; badge events are
		// best-effort via core NATS so a transient error is expected to be swallowed
		// by the caller rather than retried via JetStream.
		require.Error(t, h.processMessage(context.Background(), threadData, false))
	})

	t.Run("no publish when SaveThreadMessage returns nil tcount (CAS skipped)", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		us := NewMockUserStore(ctrl)
		ts := NewMockThreadStore(ctrl)
		setupCommonMocks(store, us, ts)

		store.EXPECT().SaveThreadMessage(gomock.Any(), &threadMsg, &expectedSender, "site-a", "tr-1").
			Return((*int)(nil), nil)

		publishCount := 0
		h := NewHandler(store, us, ts, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error {
			publishCount++
			return nil
		})

		require.NoError(t, h.processMessage(context.Background(), threadData, false))
		assert.Equal(t, 0, publishCount, "must not publish when CAS was skipped")
	})
}

func TestHandler_PublishThreadReplyEvent(t *testing.T) {
	type publishCall struct {
		subj  string
		data  []byte
		msgID string
	}

	msg := &model.Message{
		ID:                    "msg-2",
		RoomID:                "r1",
		ThreadParentMessageID: "msg-1",
	}

	var captured publishCall
	h := NewHandler(nil, nil, nil, "site-a", func(_ context.Context, subj string, data []byte, msgID string) error {
		captured = publishCall{subj: subj, data: data, msgID: msgID}
		return nil
	})

	err := h.publishThreadReplyEvent(context.Background(), msg, 5)
	require.NoError(t, err)

	assert.Equal(t, subject.ServerBroadcastThreadTCount("site-a"), captured.subj)
	assert.Equal(t, "", captured.msgID, "core NATS publish must have empty msgID")

	var evt model.MessageEvent
	require.NoError(t, json.Unmarshal(captured.data, &evt))
	assert.Equal(t, model.EventThreadReplyAdded, evt.Event)
	require.NotNil(t, evt.NewTCount)
	assert.Equal(t, 5, *evt.NewTCount)
	assert.Equal(t, "msg-2", evt.Message.ID)
	assert.Equal(t, "r1", evt.Message.RoomID)
	assert.Equal(t, "msg-1", evt.Message.ThreadParentMessageID)
	assert.Equal(t, "site-a", evt.SiteID)
	assert.Greater(t, evt.Timestamp, int64(0))
}

func TestHandler_PublishThreadReplyEvent_PublishError(t *testing.T) {
	msg := &model.Message{ID: "msg-2", RoomID: "r1", ThreadParentMessageID: "msg-1"}
	h := NewHandler(nil, nil, nil, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error {
		return errors.New("nats: connection closed")
	})
	err := h.publishThreadReplyEvent(context.Background(), msg, 3)
	require.Error(t, err)
}

// A thread reply whose event already carries the gatekeeper-resolved parent
// createdAt must NOT hit Cassandra to re-resolve it: no GetMessageCreatedAt
// expectation is registered, so any call to it fails the test.
func TestHandler_ProcessMessage_ThreadReply_EventCarriedParentCreatedAt_SkipsLookup(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	parentCreatedAt := now.Add(-time.Hour)
	user := &model.User{ID: "u-1", Account: "alice", SiteID: "site-a", EngName: "Alice Wang", ChineseName: "愛麗絲"}
	threadMsg := model.Message{
		ID:                           "msg-reply",
		RoomID:                       "r1",
		UserID:                       "u-1",
		UserAccount:                  "alice",
		Content:                      "reply",
		CreatedAt:                    now,
		ThreadParentMessageID:        "msg-parent",
		ThreadParentMessageCreatedAt: &parentCreatedAt,
	}
	threadEvt := model.MessageEvent{Message: threadMsg, SiteID: "site-a", Timestamp: now.UnixMilli()}
	data, _ := json.Marshal(threadEvt)
	expectedSender := cassParticipant{ID: "u-1", EngName: "Alice Wang", CompanyName: "愛麗絲", Account: "alice"}
	expectedTcount := 3

	ctrl := gomock.NewController(t)
	mockStore := NewMockStore(ctrl)
	mockUserStore := NewMockUserStore(ctrl)
	mockThreadStore := NewMockThreadStore(ctrl)

	mockUserStore.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)
	mockThreadStore.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(errThreadRoomExists)
	mockThreadStore.EXPECT().GetThreadRoomByParentMessageID(gomock.Any(), "msg-parent").
		Return(&model.ThreadRoom{ID: "tr-99"}, nil)
	mockStore.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
		Return(&cassParticipant{ID: "u-parent", Account: "parent-user"}, nil)
	mockStore.EXPECT().UpdateParentMessageThreadRoomID(gomock.Any(), "msg-parent", "r1", parentCreatedAt, "tr-99").Return(nil)
	mockUserStore.EXPECT().FindUserByID(gomock.Any(), "u-parent").
		Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil)
	mockThreadStore.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
	mockThreadStore.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil)
	mockThreadStore.EXPECT().UpdateThreadRoomLastMessage(gomock.Any(), "tr-99", "msg-reply", gomock.Any(), now).Return(nil)
	mockThreadStore.EXPECT().AdvanceThreadSubscriptionLastSeen(gomock.Any(), "tr-99", "alice", now).Return(nil)
	mockStore.EXPECT().SaveThreadMessage(gomock.Any(), &threadMsg, &expectedSender, "site-a", "tr-99").
		Return(&expectedTcount, nil)

	h := NewHandler(mockStore, mockUserStore, mockThreadStore, "site-a",
		func(_ context.Context, _ string, _ []byte, _ string) error { return nil })
	require.NoError(t, h.processMessage(context.Background(), data, false))
}
