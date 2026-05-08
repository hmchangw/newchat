package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

func makePublishFunc(published *[]publishedMsg, returnErr error) publishFunc {
	return func(_ context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
		if published != nil {
			*published = append(*published, publishedMsg{subject: msg.Subject, data: msg.Data})
		}
		if returnErr != nil {
			return nil, returnErr
		}
		return &jetstream.PubAck{}, nil
	}
}

type publishedMsg struct {
	subject string
	data    []byte
}

func TestHandler_ProcessMessage(t *testing.T) {
	validID := idgen.GenerateMessageID()
	validContent := "hello world"
	validSiteID := "site-a"
	validRoomID := "room-1"
	validAccount := "alice"

	sub := &model.Subscription{
		User:   model.SubscriptionUser{ID: "u1", Account: validAccount},
		RoomID: validRoomID,
		Roles:  []model.Role{model.RoleMember},
	}

	tests := []struct {
		name          string
		account       string
		roomID        string
		siteID        string
		buildData     func() []byte
		setupStore    func(s *MockStore)
		setupPub      func() (publishFunc, *[]publishedMsg)
		wantErr       bool
		wantInfra     bool
		threshold     int                           // 0 → use 500
		checkErr      func(t *testing.T, err error) // optional; called on wantErr cases
		wantNoPublish bool                          // assert published slice is empty on wantErr
		checkResult   func(t *testing.T, data []byte, published []publishedMsg)
	}{
		{
			name:    "happy path",
			account: validAccount,
			roomID:  validRoomID,
			siteID:  validSiteID,
			buildData: func() []byte {
				req := model.SendMessageRequest{ID: validID, Content: validContent}
				data, _ := json.Marshal(req)
				return data
			},
			setupStore: func(s *MockStore) {
				s.EXPECT().
					GetSubscription(gomock.Any(), validAccount, validRoomID).
					Return(sub, nil)
				s.EXPECT().
					GetRoomUserCount(gomock.Any(), validRoomID).
					Return(1, nil)
			},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				var published []publishedMsg
				return makePublishFunc(&published, nil), &published
			},
			wantErr: false,
			checkResult: func(t *testing.T, data []byte, published []publishedMsg) {
				require.NotNil(t, data)
				var msg model.Message
				err := json.Unmarshal(data, &msg)
				require.NoError(t, err)
				assert.Equal(t, validContent, msg.Content)
				assert.Equal(t, "u1", msg.UserID)
				assert.Equal(t, validRoomID, msg.RoomID)
				assert.Equal(t, validAccount, msg.UserAccount)
				assert.NotEmpty(t, msg.ID)
				assert.Len(t, published, 1)
				assert.Equal(t, subject.MsgCanonicalCreated(validSiteID), published[0].subject)
				// Verify MessageEvent has Timestamp set
				var evt model.MessageEvent
				err = json.Unmarshal(published[0].data, &evt)
				require.NoError(t, err)
				assert.Greater(t, evt.Timestamp, int64(0))
			},
		},
		{
			name:    "happy path with thread parent",
			account: validAccount,
			roomID:  validRoomID,
			siteID:  validSiteID,
			buildData: func() []byte {
				parentID := idgen.GenerateMessageID()
				parentMillis := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC).UnixMilli()
				return []byte(fmt.Sprintf(
					`{"id":%q,"content":%q,"requestId":"req-1","threadParentMessageId":%q,"threadParentMessageCreatedAt":%d}`,
					validID, validContent, parentID, parentMillis,
				))
			},
			setupStore: func(s *MockStore) {
				s.EXPECT().
					GetSubscription(gomock.Any(), validAccount, validRoomID).
					Return(sub, nil)
			},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				var published []publishedMsg
				return makePublishFunc(&published, nil), &published
			},
			wantErr: false,
			checkResult: func(t *testing.T, data []byte, published []publishedMsg) {
				parentTS := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
				require.NotNil(t, data)
				var msg model.Message
				require.NoError(t, json.Unmarshal(data, &msg))
				require.NotEmpty(t, msg.ThreadParentMessageID)
				assert.Len(t, msg.ThreadParentMessageID, 20)
				require.NotNil(t, msg.ThreadParentMessageCreatedAt)
				assert.Equal(t, parentTS, msg.ThreadParentMessageCreatedAt.UTC())

				require.Len(t, published, 1)
				var evt model.MessageEvent
				require.NoError(t, json.Unmarshal(published[0].data, &evt))
				assert.NotEmpty(t, evt.Message.ThreadParentMessageID)
				assert.Len(t, evt.Message.ThreadParentMessageID, 20)
				require.NotNil(t, evt.Message.ThreadParentMessageCreatedAt)
				assert.Equal(t, parentTS, evt.Message.ThreadParentMessageCreatedAt.UTC())
				assert.Greater(t, evt.Timestamp, int64(0))
			},
		},
		{
			name:    "thread parent ID without timestamp",
			account: validAccount,
			roomID:  validRoomID,
			siteID:  validSiteID,
			buildData: func() []byte {
				req := model.SendMessageRequest{
					ID:                    validID,
					Content:               validContent,
					ThreadParentMessageID: "parent-msg-uuid",
				}
				data, _ := json.Marshal(req)
				return data
			},
			setupStore: func(s *MockStore) {},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				return makePublishFunc(nil, nil), nil
			},
			wantErr:   true,
			wantInfra: false,
		},
		{
			name:    "invalid UUID",
			account: validAccount,
			roomID:  validRoomID,
			siteID:  validSiteID,
			buildData: func() []byte {
				req := model.SendMessageRequest{ID: "not-a-uuid", Content: validContent}
				data, _ := json.Marshal(req)
				return data
			},
			setupStore: func(s *MockStore) {},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				return makePublishFunc(nil, nil), nil
			},
			wantErr:   true,
			wantInfra: false,
		},
		{
			name:    "empty content",
			account: validAccount,
			roomID:  validRoomID,
			siteID:  validSiteID,
			buildData: func() []byte {
				req := model.SendMessageRequest{ID: validID, Content: ""}
				data, _ := json.Marshal(req)
				return data
			},
			setupStore: func(s *MockStore) {},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				return makePublishFunc(nil, nil), nil
			},
			wantErr:   true,
			wantInfra: false,
		},
		{
			name:    "content exceeds 20KB",
			account: validAccount,
			roomID:  validRoomID,
			siteID:  validSiteID,
			buildData: func() []byte {
				req := model.SendMessageRequest{ID: validID, Content: strings.Repeat("x", 20*1024+1)}
				data, _ := json.Marshal(req)
				return data
			},
			setupStore: func(s *MockStore) {},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				return makePublishFunc(nil, nil), nil
			},
			wantErr:   true,
			wantInfra: false,
		},
		{
			name:    "user not in room",
			account: validAccount,
			roomID:  validRoomID,
			siteID:  validSiteID,
			buildData: func() []byte {
				req := model.SendMessageRequest{ID: validID, Content: validContent}
				data, _ := json.Marshal(req)
				return data
			},
			setupStore: func(s *MockStore) {
				s.EXPECT().
					GetSubscription(gomock.Any(), validAccount, validRoomID).
					Return(nil, fmt.Errorf("user alice not subscribed to room room-1: %w", errNotSubscribed))
			},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				return makePublishFunc(nil, nil), nil
			},
			wantErr:   true,
			wantInfra: false,
		},
		{
			name:    "store infra error",
			account: validAccount,
			roomID:  validRoomID,
			siteID:  validSiteID,
			buildData: func() []byte {
				req := model.SendMessageRequest{ID: validID, Content: validContent}
				data, _ := json.Marshal(req)
				return data
			},
			setupStore: func(s *MockStore) {
				s.EXPECT().
					GetSubscription(gomock.Any(), validAccount, validRoomID).
					Return(nil, fmt.Errorf("connection refused"))
			},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				return makePublishFunc(nil, nil), nil
			},
			wantErr:   true,
			wantInfra: true,
		},
		{
			name:    "publish fails",
			account: validAccount,
			roomID:  validRoomID,
			siteID:  validSiteID,
			buildData: func() []byte {
				req := model.SendMessageRequest{ID: validID, Content: validContent}
				data, _ := json.Marshal(req)
				return data
			},
			setupStore: func(s *MockStore) {
				s.EXPECT().
					GetSubscription(gomock.Any(), validAccount, validRoomID).
					Return(sub, nil)
				s.EXPECT().
					GetRoomUserCount(gomock.Any(), validRoomID).
					Return(1, nil)
			},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				return makePublishFunc(nil, fmt.Errorf("nats publish error")), nil
			},
			wantErr:   true,
			wantInfra: true,
		},
		{
			name:    "malformed JSON",
			account: validAccount,
			roomID:  validRoomID,
			siteID:  validSiteID,
			buildData: func() []byte {
				return []byte("{not valid json}")
			},
			setupStore: func(s *MockStore) {},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				return makePublishFunc(nil, nil), nil
			},
			wantErr:   true,
			wantInfra: false,
		},
		{
			name:    "siteID mismatch",
			account: validAccount,
			roomID:  validRoomID,
			siteID:  "site-b",
			buildData: func() []byte {
				req := model.SendMessageRequest{ID: validID, Content: validContent}
				data, _ := json.Marshal(req)
				return data
			},
			setupStore: func(s *MockStore) {},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				return makePublishFunc(nil, nil), nil
			},
			wantErr:   true,
			wantInfra: false,
		},
		{
			name:    "owner sends in big room — fast-path skips GetRoom",
			account: validAccount,
			roomID:  validRoomID,
			siteID:  validSiteID,
			buildData: func() []byte {
				req := model.SendMessageRequest{ID: idgen.GenerateMessageID(), Content: validContent}
				data, _ := json.Marshal(req)
				return data
			},
			setupStore: func(s *MockStore) {
				s.EXPECT().
					GetSubscription(gomock.Any(), validAccount, validRoomID).
					Return(&model.Subscription{
						User:  model.SubscriptionUser{ID: "u1", Account: validAccount},
						Roles: []model.Role{model.RoleOwner},
					}, nil)
				// No GetRoom expectation: owners must skip the fetch entirely.
			},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				var published []publishedMsg
				return makePublishFunc(&published, nil), &published
			},
			wantErr: false,
			checkResult: func(t *testing.T, _ []byte, published []publishedMsg) {
				assert.Len(t, published, 1, "bypass path must still publish to MESSAGES_CANONICAL")
			},
		},
		{
			name:    "admin sends in big room — fast-path skips GetRoom",
			account: validAccount,
			roomID:  validRoomID,
			siteID:  validSiteID,
			buildData: func() []byte {
				req := model.SendMessageRequest{ID: idgen.GenerateMessageID(), Content: validContent}
				data, _ := json.Marshal(req)
				return data
			},
			setupStore: func(s *MockStore) {
				s.EXPECT().
					GetSubscription(gomock.Any(), validAccount, validRoomID).
					Return(&model.Subscription{
						User:  model.SubscriptionUser{ID: "u1", Account: validAccount},
						Roles: []model.Role{model.RoleAdmin},
					}, nil)
				// No GetRoom expectation: admins must skip the fetch entirely.
			},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				var published []publishedMsg
				return makePublishFunc(&published, nil), &published
			},
			wantErr: false,
			checkResult: func(t *testing.T, _ []byte, published []publishedMsg) {
				assert.Len(t, published, 1, "bypass path must still publish to MESSAGES_CANONICAL")
			},
		},
		{
			name:    "bot account in big room with member role — fast-path skips GetRoom",
			account: "helper.bot",
			roomID:  validRoomID,
			siteID:  validSiteID,
			buildData: func() []byte {
				req := model.SendMessageRequest{ID: idgen.GenerateMessageID(), Content: validContent}
				data, _ := json.Marshal(req)
				return data
			},
			setupStore: func(s *MockStore) {
				s.EXPECT().
					GetSubscription(gomock.Any(), "helper.bot", validRoomID).
					Return(&model.Subscription{
						User:  model.SubscriptionUser{ID: "u-bot", Account: "helper.bot"},
						Roles: []model.Role{model.RoleMember},
					}, nil)
				// No GetRoom expectation: bot accounts must skip the fetch entirely.
			},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				var published []publishedMsg
				return makePublishFunc(&published, nil), &published
			},
			wantErr: false,
			checkResult: func(t *testing.T, _ []byte, published []publishedMsg) {
				assert.Len(t, published, 1, "bypass path must still publish to MESSAGES_CANONICAL")
			},
		},
		{
			name:    "member sends in big room — rejected with codedError",
			account: validAccount,
			roomID:  validRoomID,
			siteID:  validSiteID,
			buildData: func() []byte {
				req := model.SendMessageRequest{ID: idgen.GenerateMessageID(), Content: validContent}
				data, _ := json.Marshal(req)
				return data
			},
			setupStore: func(s *MockStore) {
				s.EXPECT().
					GetSubscription(gomock.Any(), validAccount, validRoomID).
					Return(&model.Subscription{
						User:  model.SubscriptionUser{ID: "u1", Account: validAccount},
						Roles: []model.Role{model.RoleMember},
					}, nil)
				s.EXPECT().
					GetRoomUserCount(gomock.Any(), validRoomID).
					Return(600, nil)
			},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				var published []publishedMsg
				return makePublishFunc(&published, nil), &published
			},
			wantErr:   true,
			wantInfra: false,
			checkErr: func(t *testing.T, err error) {
				assert.ErrorIs(t, err, errLargeRoomPostRestricted)
			},
			wantNoPublish: true,
		},
		{
			name:    "member sends in small room — allowed",
			account: validAccount,
			roomID:  validRoomID,
			siteID:  validSiteID,
			buildData: func() []byte {
				req := model.SendMessageRequest{ID: idgen.GenerateMessageID(), Content: validContent}
				data, _ := json.Marshal(req)
				return data
			},
			setupStore: func(s *MockStore) {
				s.EXPECT().
					GetSubscription(gomock.Any(), validAccount, validRoomID).
					Return(&model.Subscription{
						User:  model.SubscriptionUser{ID: "u1", Account: validAccount},
						Roles: []model.Role{model.RoleMember},
					}, nil)
				s.EXPECT().
					GetRoomUserCount(gomock.Any(), validRoomID).
					Return(50, nil)
			},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				var published []publishedMsg
				return makePublishFunc(&published, nil), &published
			},
			wantErr: false,
		},
		{
			name:    "boundary: count == threshold — allowed (strict greater-than)",
			account: validAccount,
			roomID:  validRoomID,
			siteID:  validSiteID,
			buildData: func() []byte {
				req := model.SendMessageRequest{ID: idgen.GenerateMessageID(), Content: validContent}
				data, _ := json.Marshal(req)
				return data
			},
			setupStore: func(s *MockStore) {
				s.EXPECT().
					GetSubscription(gomock.Any(), validAccount, validRoomID).
					Return(&model.Subscription{
						User:  model.SubscriptionUser{ID: "u1", Account: validAccount},
						Roles: []model.Role{model.RoleMember},
					}, nil)
				s.EXPECT().
					GetRoomUserCount(gomock.Any(), validRoomID).
					Return(500, nil)
			},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				var published []publishedMsg
				return makePublishFunc(&published, nil), &published
			},
			wantErr: false,
		},
		{
			name:    "boundary: count == threshold + 1 — rejected",
			account: validAccount,
			roomID:  validRoomID,
			siteID:  validSiteID,
			buildData: func() []byte {
				req := model.SendMessageRequest{ID: idgen.GenerateMessageID(), Content: validContent}
				data, _ := json.Marshal(req)
				return data
			},
			setupStore: func(s *MockStore) {
				s.EXPECT().
					GetSubscription(gomock.Any(), validAccount, validRoomID).
					Return(&model.Subscription{
						User:  model.SubscriptionUser{ID: "u1", Account: validAccount},
						Roles: []model.Role{model.RoleMember},
					}, nil)
				s.EXPECT().
					GetRoomUserCount(gomock.Any(), validRoomID).
					Return(501, nil)
			},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				var published []publishedMsg
				return makePublishFunc(&published, nil), &published
			},
			wantErr:   true,
			wantInfra: false,
			checkErr: func(t *testing.T, err error) {
				assert.ErrorIs(t, err, errLargeRoomPostRestricted)
			},
			wantNoPublish: true,
		},
		{
			name:    "member thread reply in big room — fast-path skips GetRoom",
			account: validAccount,
			roomID:  validRoomID,
			siteID:  validSiteID,
			buildData: func() []byte {
				parentID := idgen.GenerateMessageID()
				parentMillis := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC).UnixMilli()
				return []byte(fmt.Sprintf(
					`{"id":%q,"content":%q,"requestId":"req-1","threadParentMessageId":%q,"threadParentMessageCreatedAt":%d}`,
					idgen.GenerateMessageID(), validContent, parentID, parentMillis,
				))
			},
			setupStore: func(s *MockStore) {
				s.EXPECT().
					GetSubscription(gomock.Any(), validAccount, validRoomID).
					Return(&model.Subscription{
						User:  model.SubscriptionUser{ID: "u1", Account: validAccount},
						Roles: []model.Role{model.RoleMember},
					}, nil)
				// No GetRoom expectation: thread replies must skip the fetch entirely.
			},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				var published []publishedMsg
				return makePublishFunc(&published, nil), &published
			},
			wantErr: false,
			checkResult: func(t *testing.T, _ []byte, published []publishedMsg) {
				assert.Len(t, published, 1, "bypass path must still publish to MESSAGES_CANONICAL")
			},
		},
		{
			name:    "GetRoom infra failure — wrapped as infraError",
			account: validAccount,
			roomID:  validRoomID,
			siteID:  validSiteID,
			buildData: func() []byte {
				req := model.SendMessageRequest{ID: idgen.GenerateMessageID(), Content: validContent}
				data, _ := json.Marshal(req)
				return data
			},
			setupStore: func(s *MockStore) {
				s.EXPECT().
					GetSubscription(gomock.Any(), validAccount, validRoomID).
					Return(&model.Subscription{
						User:  model.SubscriptionUser{ID: "u1", Account: validAccount},
						Roles: []model.Role{model.RoleMember},
					}, nil)
				s.EXPECT().
					GetRoomUserCount(gomock.Any(), validRoomID).
					Return(0, errors.New("mongo unreachable"))
			},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				return makePublishFunc(nil, nil), nil
			},
			wantErr:   true,
			wantInfra: true,
		},
		{
			// Distinct from the generic-error case above: GetRoom returns
			// ErrNoDocuments (wrapped, mirroring MongoStore.GetRoom). The
			// handler must still classify this as infraError — unlike
			// GetSubscription, GetRoom does not convert ErrNoDocuments to a
			// user-facing error, since reaching this call already implies a
			// subscription for the room exists.
			name:    "GetRoom returns ErrNoDocuments — wrapped as infraError",
			account: validAccount,
			roomID:  validRoomID,
			siteID:  validSiteID,
			buildData: func() []byte {
				req := model.SendMessageRequest{ID: idgen.GenerateMessageID(), Content: validContent}
				data, _ := json.Marshal(req)
				return data
			},
			setupStore: func(s *MockStore) {
				s.EXPECT().
					GetSubscription(gomock.Any(), validAccount, validRoomID).
					Return(&model.Subscription{
						User:  model.SubscriptionUser{ID: "u1", Account: validAccount},
						Roles: []model.Role{model.RoleMember},
					}, nil)
				s.EXPECT().
					GetRoomUserCount(gomock.Any(), validRoomID).
					Return(0, fmt.Errorf("find user count for room %q: %w", validRoomID, mongo.ErrNoDocuments))
			},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				return makePublishFunc(nil, nil), nil
			},
			wantErr:   true,
			wantInfra: true,
		},
		{
			name:    "custom threshold (env=2), 3-person room — rejected",
			account: validAccount,
			roomID:  validRoomID,
			siteID:  validSiteID,
			buildData: func() []byte {
				req := model.SendMessageRequest{ID: idgen.GenerateMessageID(), Content: validContent}
				data, _ := json.Marshal(req)
				return data
			},
			setupStore: func(s *MockStore) {
				s.EXPECT().
					GetSubscription(gomock.Any(), validAccount, validRoomID).
					Return(&model.Subscription{
						User:  model.SubscriptionUser{ID: "u1", Account: validAccount},
						Roles: []model.Role{model.RoleMember},
					}, nil)
				s.EXPECT().
					GetRoomUserCount(gomock.Any(), validRoomID).
					Return(3, nil)
			},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				var published []publishedMsg
				return makePublishFunc(&published, nil), &published
			},
			threshold: 2,
			wantErr:   true,
			wantInfra: false,
			checkErr: func(t *testing.T, err error) {
				assert.ErrorIs(t, err, errLargeRoomPostRestricted)
			},
			wantNoPublish: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockStore(ctrl)
			tc.setupStore(store)

			pub, publishedPtr := tc.setupPub()

			threshold := tc.threshold
			if threshold == 0 {
				threshold = 500
			}
			h := &Handler{
				store:              store,
				publish:            pub,
				siteID:             validSiteID,
				largeRoomThreshold: threshold,
			}

			data, err := h.processMessage(context.Background(), tc.account, tc.roomID, tc.siteID, tc.buildData())

			if tc.wantErr {
				require.Error(t, err)
				if tc.wantInfra {
					var ie *infraError
					assert.True(t, errors.As(err, &ie), "expected infraError, got %T: %v", err, err)
				} else {
					var ie *infraError
					assert.False(t, errors.As(err, &ie), "expected non-infra error, got infraError: %v", err)
				}
				if tc.checkErr != nil {
					tc.checkErr(t, err)
				}
				if tc.wantNoPublish && publishedPtr != nil {
					assert.Empty(t, *publishedPtr, "no canonical publish should occur on rejection")
				}
			} else {
				require.NoError(t, err)
				if tc.checkResult != nil {
					var published []publishedMsg
					if publishedPtr != nil {
						published = *publishedPtr
					}
					tc.checkResult(t, data, published)
				}
			}
		})
	}
}

func TestHandler_processMessage_RejectsInvalidThreadParentMessageID(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	// No store expectations: validation must fail before any store call.

	pub := func(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
		return &jetstream.PubAck{}, nil
	}
	reply := func(ctx context.Context, msg *nats.Msg) error { return nil }
	h := NewHandler(store, pub, reply, "site1", nil, 500)

	parentTs := int64(1000)
	req := model.SendMessageRequest{
		ID:                           idgen.GenerateMessageID(),
		Content:                      "reply",
		ThreadParentMessageID:        "not-a-valid-msg-id",
		ThreadParentMessageCreatedAt: &parentTs,
	}
	data, _ := json.Marshal(req)
	_, err := h.processMessage(context.Background(), "alice", "room-1", "site1", data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid thread parent message ID")
}

func TestHandler_processMessage_PropagatesRequestIDOnCanonicalPublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "room-1").
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u-alice", Account: "alice"}}, nil)
	store.EXPECT().GetRoomUserCount(gomock.Any(), "room-1").
		Return(1, nil)

	var capturedHeader nats.Header
	pub := func(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
		capturedHeader = msg.Header
		return &jetstream.PubAck{}, nil
	}
	reply := func(ctx context.Context, msg *nats.Msg) error { return nil }

	h := NewHandler(store, pub, reply, "site1", nil, 500)

	ctx := natsutil.WithRequestID(context.Background(), "req-mg-test-id")
	req := model.SendMessageRequest{ID: idgen.GenerateMessageID(), Content: "hello"}
	data, _ := json.Marshal(req)

	_, err := h.processMessage(ctx, "alice", "room-1", "site1", data)
	require.NoError(t, err)
	require.NotNil(t, capturedHeader, "publish must propagate header from ctx")
	assert.Equal(t, "req-mg-test-id", capturedHeader.Get(natsutil.RequestIDHeader))
}

func TestHandler_ProcessMessage_WithQuote(t *testing.T) {
	validID := idgen.GenerateMessageID()
	validContent := "great point!"
	validSiteID := "site-a"
	validRoomID := "room-1"
	validAccount := "alice"
	parentMessageID := idgen.GenerateMessageID()

	sub := &model.Subscription{
		User:   model.SubscriptionUser{ID: "u1", Account: validAccount},
		RoomID: validRoomID,
		Roles:  []model.Role{model.RoleMember},
	}

	threadID := idgen.GenerateMessageID()
	threadParentTS := time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC)

	// mainRoomSnapshot represents a parent that lives in the main room
	// (not inside any thread): ThreadParentID == "".
	mainRoomSnapshot := &cassandra.QuotedParentMessage{
		MessageID:   parentMessageID,
		RoomID:      validRoomID,
		Sender:      cassandra.Participant{ID: "u-bob", Account: "bob", EngName: "Bob Chen"},
		CreatedAt:   time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
		Msg:         "the original message",
		MessageLink: "http://localhost:3000/" + validRoomID + "/" + parentMessageID,
	}

	// inThreadSnapshot represents a parent that is itself a reply inside thread T.
	inThreadSnapshot := &cassandra.QuotedParentMessage{
		MessageID:             parentMessageID,
		RoomID:                validRoomID,
		Sender:                cassandra.Participant{ID: "u-bob", Account: "bob", EngName: "Bob Chen"},
		CreatedAt:             time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
		Msg:                   "a reply inside thread T",
		MessageLink:           "http://localhost:3000/" + validRoomID + "/" + parentMessageID,
		ThreadParentID:        threadID,
		ThreadParentCreatedAt: &threadParentTS,
	}

	// inDifferentThreadSnapshot is in thread T'.
	inDifferentThreadSnapshot := &cassandra.QuotedParentMessage{
		MessageID:             parentMessageID,
		RoomID:                validRoomID,
		Sender:                cassandra.Participant{ID: "u-bob", Account: "bob", EngName: "Bob Chen"},
		CreatedAt:             time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
		Msg:                   "a reply inside thread T'",
		MessageLink:           "http://localhost:3000/" + validRoomID + "/" + parentMessageID,
		ThreadParentID:        "different-thread-uuid",
		ThreadParentCreatedAt: &threadParentTS,
	}

	tests := []struct {
		name          string
		buildData     func() []byte
		setupStore    func(s *MockStore)
		setupFetcher  func(f *MockParentMessageFetcher)
		setupPub      func() (publishFunc, *[]publishedMsg)
		wantErr       bool
		assertMessage func(t *testing.T, msg model.Message)
	}{
		{
			name: "main-room msg quoting main-room parent — snapshot embedded",
			buildData: func() []byte {
				req := model.SendMessageRequest{
					ID:                    validID,
					Content:               validContent,
					QuotedParentMessageID: parentMessageID,
				}
				data, _ := json.Marshal(req)
				return data
			},
			setupStore: func(s *MockStore) {
				s.EXPECT().GetSubscription(gomock.Any(), validAccount, validRoomID).Return(sub, nil)
				s.EXPECT().GetRoomUserCount(gomock.Any(), validRoomID).Return(1, nil)
			},
			setupFetcher: func(f *MockParentMessageFetcher) {
				f.EXPECT().
					FetchQuotedParent(gomock.Any(), validAccount, validRoomID, validSiteID, parentMessageID).
					Return(mainRoomSnapshot, nil)
			},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				var published []publishedMsg
				return makePublishFunc(&published, nil), &published
			},
			assertMessage: func(t *testing.T, msg model.Message) {
				require.NotNil(t, msg.QuotedParentMessage)
				assert.Equal(t, parentMessageID, msg.QuotedParentMessage.MessageID)
				assert.Equal(t, "the original message", msg.QuotedParentMessage.Msg)
				assert.Equal(t, "bob", msg.QuotedParentMessage.Sender.Account)
				assert.Equal(t, mainRoomSnapshot.MessageLink, msg.QuotedParentMessage.MessageLink)
				assert.Empty(t, msg.QuotedParentMessage.ThreadParentID)
			},
		},
		{
			name: "fetcher error — request fails",
			buildData: func() []byte {
				req := model.SendMessageRequest{
					ID:                    validID,
					Content:               validContent,
					QuotedParentMessageID: parentMessageID,
				}
				data, _ := json.Marshal(req)
				return data
			},
			setupStore: func(s *MockStore) {
				s.EXPECT().GetSubscription(gomock.Any(), validAccount, validRoomID).Return(sub, nil)
				s.EXPECT().GetRoomUserCount(gomock.Any(), validRoomID).Return(1, nil)
			},
			setupFetcher: func(f *MockParentMessageFetcher) {
				f.EXPECT().
					FetchQuotedParent(gomock.Any(), validAccount, validRoomID, validSiteID, parentMessageID).
					Return(nil, fmt.Errorf("history response error: not found"))
			},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				return makePublishFunc(nil, nil), nil
			},
			wantErr: true,
		},
		{
			name: "thread T msg quoting another reply in thread T — snapshot embedded",
			buildData: func() []byte {
				return []byte(fmt.Sprintf(
					`{"id":%q,"content":%q,"requestId":"req-1","threadParentMessageId":%q,"threadParentMessageCreatedAt":%d,"quotedParentMessageId":%q}`,
					validID, validContent, threadID, threadParentTS.UnixMilli(), parentMessageID,
				))
			},
			setupStore: func(s *MockStore) {
				s.EXPECT().GetSubscription(gomock.Any(), validAccount, validRoomID).Return(sub, nil)
			},
			setupFetcher: func(f *MockParentMessageFetcher) {
				f.EXPECT().
					FetchQuotedParent(gomock.Any(), validAccount, validRoomID, validSiteID, parentMessageID).
					Return(inThreadSnapshot, nil)
			},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				var published []publishedMsg
				return makePublishFunc(&published, nil), &published
			},
			assertMessage: func(t *testing.T, msg model.Message) {
				assert.Equal(t, threadID, msg.ThreadParentMessageID)
				require.NotNil(t, msg.QuotedParentMessage)
				assert.Equal(t, parentMessageID, msg.QuotedParentMessage.MessageID)
				assert.Equal(t, threadID, msg.QuotedParentMessage.ThreadParentID)
			},
		},
		{
			name: "main-room msg quoting a thread reply — request fails (cross-thread)",
			buildData: func() []byte {
				req := model.SendMessageRequest{
					ID:                    validID,
					Content:               validContent,
					QuotedParentMessageID: parentMessageID,
				}
				data, _ := json.Marshal(req)
				return data
			},
			setupStore: func(s *MockStore) {
				s.EXPECT().GetSubscription(gomock.Any(), validAccount, validRoomID).Return(sub, nil)
				s.EXPECT().GetRoomUserCount(gomock.Any(), validRoomID).Return(1, nil)
			},
			setupFetcher: func(f *MockParentMessageFetcher) {
				f.EXPECT().
					FetchQuotedParent(gomock.Any(), validAccount, validRoomID, validSiteID, parentMessageID).
					Return(inThreadSnapshot, nil)
			},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				return makePublishFunc(nil, nil), nil
			},
			wantErr: true,
		},
		{
			name: "thread T msg quoting main-room parent — request fails (strict)",
			buildData: func() []byte {
				return []byte(fmt.Sprintf(
					`{"id":%q,"content":%q,"requestId":"req-1","threadParentMessageId":%q,"threadParentMessageCreatedAt":%d,"quotedParentMessageId":%q}`,
					validID, validContent, threadID, threadParentTS.UnixMilli(), parentMessageID,
				))
			},
			setupStore: func(s *MockStore) {
				s.EXPECT().GetSubscription(gomock.Any(), validAccount, validRoomID).Return(sub, nil)
			},
			setupFetcher: func(f *MockParentMessageFetcher) {
				f.EXPECT().
					FetchQuotedParent(gomock.Any(), validAccount, validRoomID, validSiteID, parentMessageID).
					Return(mainRoomSnapshot, nil)
			},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				return makePublishFunc(nil, nil), nil
			},
			wantErr: true,
		},
		{
			name: "thread T msg quoting reply in different thread T' — request fails",
			buildData: func() []byte {
				return []byte(fmt.Sprintf(
					`{"id":%q,"content":%q,"requestId":"req-1","threadParentMessageId":%q,"threadParentMessageCreatedAt":%d,"quotedParentMessageId":%q}`,
					validID, validContent, threadID, threadParentTS.UnixMilli(), parentMessageID,
				))
			},
			setupStore: func(s *MockStore) {
				s.EXPECT().GetSubscription(gomock.Any(), validAccount, validRoomID).Return(sub, nil)
			},
			setupFetcher: func(f *MockParentMessageFetcher) {
				f.EXPECT().
					FetchQuotedParent(gomock.Any(), validAccount, validRoomID, validSiteID, parentMessageID).
					Return(inDifferentThreadSnapshot, nil)
			},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				return makePublishFunc(nil, nil), nil
			},
			wantErr: true,
		},
		{
			name: "no quote field — fetcher not called",
			buildData: func() []byte {
				req := model.SendMessageRequest{ID: validID, Content: validContent}
				data, _ := json.Marshal(req)
				return data
			},
			setupStore: func(s *MockStore) {
				s.EXPECT().GetSubscription(gomock.Any(), validAccount, validRoomID).Return(sub, nil)
				s.EXPECT().GetRoomUserCount(gomock.Any(), validRoomID).Return(1, nil)
			},
			setupFetcher: func(f *MockParentMessageFetcher) {
				// no EXPECT — gomock will fail the test if FetchQuotedParent is called
			},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				var published []publishedMsg
				return makePublishFunc(&published, nil), &published
			},
			assertMessage: func(t *testing.T, msg model.Message) {
				assert.Nil(t, msg.QuotedParentMessage)
			},
		},
		{
			name: "fetcher returns (nil, nil) — request fails (defensive guard)",
			buildData: func() []byte {
				req := model.SendMessageRequest{
					ID:                    validID,
					Content:               validContent,
					QuotedParentMessageID: parentMessageID,
				}
				data, _ := json.Marshal(req)
				return data
			},
			setupStore: func(s *MockStore) {
				s.EXPECT().GetSubscription(gomock.Any(), validAccount, validRoomID).Return(sub, nil)
				s.EXPECT().GetRoomUserCount(gomock.Any(), validRoomID).Return(1, nil)
			},
			setupFetcher: func(f *MockParentMessageFetcher) {
				f.EXPECT().
					FetchQuotedParent(gomock.Any(), validAccount, validRoomID, validSiteID, parentMessageID).
					Return(nil, nil)
			},
			setupPub: func() (publishFunc, *[]publishedMsg) {
				return makePublishFunc(nil, nil), nil
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockStore(ctrl)
			fetcher := NewMockParentMessageFetcher(ctrl)
			tc.setupStore(store)
			tc.setupFetcher(fetcher)

			pub, publishedPtr := tc.setupPub()

			h := &Handler{
				store:              store,
				publish:            pub,
				siteID:             validSiteID,
				parentFetcher:      fetcher,
				largeRoomThreshold: 500,
			}

			data, err := h.processMessage(context.Background(), validAccount, validRoomID, validSiteID, tc.buildData())

			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, data)

			var msg model.Message
			require.NoError(t, json.Unmarshal(data, &msg))
			tc.assertMessage(t, msg)

			// Also verify the snapshot reaches the canonical event.
			require.NotNil(t, publishedPtr)
			require.Len(t, *publishedPtr, 1)
			var evt model.MessageEvent
			require.NoError(t, json.Unmarshal((*publishedPtr)[0].data, &evt))
			if msg.QuotedParentMessage != nil {
				require.NotNil(t, evt.Message.QuotedParentMessage)
				assert.Equal(t, msg.QuotedParentMessage.MessageID, evt.Message.QuotedParentMessage.MessageID)
			} else {
				assert.Nil(t, evt.Message.QuotedParentMessage)
			}
		})
	}
}

func TestIsBot(t *testing.T) {
	cases := []struct {
		name    string
		account string
		want    bool
	}{
		{name: ".bot suffix", account: "helper.bot", want: true},
		{name: "p_ prefix", account: "p_scheduler", want: true},
		{name: "another bot suffix", account: "scheduler.bot", want: true},
		{name: "another p_ prefix", account: "p_webhook", want: true},
		{name: "plain account", account: "alice", want: false},
		{name: "contains bot but not suffix", account: "botmaster", want: false},
		{name: "empty string", account: "", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isBot(tc.account))
		})
	}
}

func TestCanBypassLargeRoomCap(t *testing.T) {
	cases := []struct {
		name    string
		roles   []model.Role
		account string
		want    bool
	}{
		{name: "owner role bypasses", roles: []model.Role{model.RoleOwner}, account: "alice", want: true},
		{name: "admin role bypasses", roles: []model.Role{model.RoleAdmin}, account: "alice", want: true},
		{name: "member role does not bypass", roles: []model.Role{model.RoleMember}, account: "alice", want: false},
		{name: "owner + member bypasses", roles: []model.Role{model.RoleMember, model.RoleOwner}, account: "alice", want: true},
		{name: "admin + member bypasses", roles: []model.Role{model.RoleMember, model.RoleAdmin}, account: "alice", want: true},
		{name: "empty roles, plain account", roles: nil, account: "alice", want: false},
		{name: "bot account .bot suffix bypasses regardless of roles", roles: []model.Role{model.RoleMember}, account: "helper.bot", want: true},
		{name: "bot account p_ prefix bypasses regardless of roles", roles: []model.Role{model.RoleMember}, account: "p_scheduler", want: true},
		{name: "bot account with empty roles bypasses", roles: nil, account: "p_webhook", want: true},
		{name: "unknown role string with plain account", roles: []model.Role{"superuser"}, account: "alice", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sub := &model.Subscription{
				User:  model.SubscriptionUser{Account: tc.account},
				Roles: tc.roles,
			}
			got := canBypassLargeRoomCap(sub)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestHandler_marshalErrorReply(t *testing.T) {
	h := &Handler{}

	t.Run("plain error produces uncoded reply", func(t *testing.T) {
		data := h.marshalErrorReply(errors.New("user alice is not subscribed to room R"))
		var got model.ErrorResponse
		require.NoError(t, json.Unmarshal(data, &got))
		assert.Equal(t, "user alice is not subscribed to room R", got.Error)
		assert.Empty(t, got.Code)
		// omitempty: the wire bytes must not contain a "code" key.
		assert.NotContains(t, string(data), `"code"`)
	})

	t.Run("codedError produces coded reply", func(t *testing.T) {
		data := h.marshalErrorReply(errLargeRoomPostRestricted)
		var got model.ErrorResponse
		require.NoError(t, json.Unmarshal(data, &got))
		assert.Equal(t, "posting is restricted to owners and admins in this room", got.Error)
		assert.Equal(t, "large_room_post_restricted", got.Code)
	})

	t.Run("wrapped codedError still dispatches", func(t *testing.T) {
		wrapped := fmt.Errorf("context: %w", errLargeRoomPostRestricted)
		data := h.marshalErrorReply(wrapped)
		var got model.ErrorResponse
		require.NoError(t, json.Unmarshal(data, &got))
		assert.Equal(t, "posting is restricted to owners and admins in this room", got.Error)
		assert.Equal(t, "large_room_post_restricted", got.Code)
	})
}
