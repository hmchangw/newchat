package model_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
)

func TestUserJSON(t *testing.T) {
	u := model.User{
		ID:          "u1",
		Account:     "alice",
		SiteID:      "site-a",
		SectID:      "sect-1",
		SectName:    "Engineering",
		EngName:     "Alice Wang",
		ChineseName: "愛麗絲",
		EmployeeID:  "EMP001",
	}
	roundTrip(t, &u, &model.User{})
}

func TestRoomJSON(t *testing.T) {
	lastMsg := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	lastMention := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	r := model.Room{
		ID: "r1", Name: "general", Type: model.RoomTypeChannel,
		CreatedBy: "u1", SiteID: "site-a", UserCount: 5,
		LastMsgAt:        &lastMsg,
		LastMsgID:        "m1",
		LastMentionAllAt: &lastMention,
		CreatedAt:        time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt:        time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	roundTrip(t, &r, &model.Room{})
}

func TestRoomJSON_NilTimestampsOmitted(t *testing.T) {
	r := model.Room{
		ID: "r1", Name: "general", Type: model.RoomTypeChannel,
		CreatedBy: "u1", SiteID: "site-a", UserCount: 1,
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	data, err := json.Marshal(&r)
	require.NoError(t, err)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))

	_, hasMsg := raw["lastMsgAt"]
	assert.False(t, hasMsg, "nil LastMsgAt must be omitted from JSON")

	_, hasMention := raw["lastMentionAllAt"]
	assert.False(t, hasMention, "nil LastMentionAllAt must be omitted from JSON")

	var dst model.Room
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Nil(t, dst.LastMsgAt, "absent JSON field must unmarshal to nil pointer")
	assert.Nil(t, dst.LastMentionAllAt, "absent JSON field must unmarshal to nil pointer")
}

func TestThreadRoomJSON(t *testing.T) {
	tr := model.ThreadRoom{
		ID:              "tr-1",
		ParentMessageID: "msg-parent",
		RoomID:          "r1",
		SiteID:          "site-a",
		LastMsgAt:       time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		LastMsgID:       "msg-2",
		CreatedAt:       time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt:       time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
	}
	roundTrip(t, &tr, &model.ThreadRoom{})
}

func TestThreadSubscriptionJSON(t *testing.T) {
	ts := model.ThreadSubscription{
		ID:              "ts-1",
		ParentMessageID: "msg-parent",
		RoomID:          "r1",
		ThreadRoomID:    "tr-1",
		UserID:          "u-1",
		UserAccount:     "alice",
		SiteID:          "site-a",
		LastSeenAt:      nil,
		HasMention:      true,
		CreatedAt:       time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt:       time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
	}
	roundTrip(t, &ts, &model.ThreadSubscription{})
}

func TestMessageJSON(t *testing.T) {
	t.Run("with threadParentMessageId", func(t *testing.T) {
		m := model.Message{
			ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			Content:               "hello",
			CreatedAt:             time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
			ThreadParentMessageID: "parent-msg-uuid",
		}
		data, err := json.Marshal(&m)
		require.NoError(t, err)
		var dst model.Message
		require.NoError(t, json.Unmarshal(data, &dst))
		assert.Equal(t, m, dst)
	})

	t.Run("threadParentMessageId omitted when empty", func(t *testing.T) {
		m := model.Message{
			ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			Content:   "hello",
			CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		}
		data, err := json.Marshal(&m)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["threadParentMessageId"]
		assert.False(t, present, "threadParentMessageId should be omitted when empty")
	})

	t.Run("threadParentMessageCreatedAt omitted when nil", func(t *testing.T) {
		m := model.Message{
			ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			Content:   "hello",
			CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		}
		data, err := json.Marshal(&m)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["threadParentMessageCreatedAt"]
		assert.False(t, present, "threadParentMessageCreatedAt should be omitted when nil")
	})

	t.Run("with threadParentMessageCreatedAt", func(t *testing.T) {
		raw := `{"id":"m1","roomId":"r1","userId":"u1","userAccount":"alice","content":"reply","createdAt":"2026-01-01T12:00:00Z","threadParentMessageId":"parent-msg-uuid","threadParentMessageCreatedAt":"2026-01-01T11:00:00Z"}`
		var m model.Message
		require.NoError(t, json.Unmarshal([]byte(raw), &m))
		assert.Equal(t, "parent-msg-uuid", m.ThreadParentMessageID)
		require.NotNil(t, m.ThreadParentMessageCreatedAt)
		assert.Equal(t, time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC), m.ThreadParentMessageCreatedAt.UTC())
	})

	t.Run("mentions round-trip", func(t *testing.T) {
		m := model.Message{
			ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			Content:   "hello @bob",
			CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
			Mentions: []model.Participant{{
				UserID:      "u-bob",
				Account:     "bob",
				ChineseName: "鮑勃",
				EngName:     "Bob Chen",
			}},
		}
		data, err := json.Marshal(&m)
		require.NoError(t, err)
		var dst model.Message
		require.NoError(t, json.Unmarshal(data, &dst))
		assert.Equal(t, m, dst)
	})

	t.Run("mentions omitted when nil", func(t *testing.T) {
		m := model.Message{
			ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			Content:   "hello",
			CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		}
		data, err := json.Marshal(&m)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["mentions"]
		assert.False(t, present, "mentions should be omitted when nil")
	})

	t.Run("type and sysMsgData round-trip", func(t *testing.T) {
		m := model.Message{
			ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			Content:    "system",
			CreatedAt:  time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
			Type:       "system",
			SysMsgData: []byte(`{"key":"value"}`),
		}
		data, err := json.Marshal(&m)
		require.NoError(t, err)
		var dst model.Message
		require.NoError(t, json.Unmarshal(data, &dst))
		assert.Equal(t, m, dst)
	})

	t.Run("type and sysMsgData omitted when empty", func(t *testing.T) {
		m := model.Message{
			ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			Content:   "hello",
			CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		}
		data, err := json.Marshal(&m)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, hasType := raw["type"]
		assert.False(t, hasType, "type should be omitted when empty")
		_, hasSysMsgData := raw["sysMsgData"]
		assert.False(t, hasSysMsgData, "sysMsgData should be omitted when nil")
	})

	t.Run("tshow round-trips when true", func(t *testing.T) {
		m := model.Message{
			ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			Content:   "thread reply shown in main feed",
			CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
			TShow:     true,
		}
		data, err := json.Marshal(&m)
		require.NoError(t, err)

		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		assert.Equal(t, true, raw["tshow"])

		var dst model.Message
		require.NoError(t, json.Unmarshal(data, &dst))
		assert.True(t, dst.TShow)
	})

	t.Run("tshow omitted on the wire when false", func(t *testing.T) {
		m := model.Message{
			ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			Content:   "plain message",
			CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		}
		data, err := json.Marshal(&m)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["tshow"]
		assert.False(t, present, "tshow should be omitted when false")
	})
}

func TestSendMessageRequestJSON(t *testing.T) {
	t.Run("with threadParentMessageId", func(t *testing.T) {
		r := model.SendMessageRequest{
			ID:                    "msg-uuid-1",
			Content:               "hello world",
			RequestID:             "req-1",
			ThreadParentMessageID: "parent-msg-uuid",
		}
		roundTrip(t, &r, &model.SendMessageRequest{})
	})

	t.Run("threadParentMessageId omitted when empty", func(t *testing.T) {
		r := model.SendMessageRequest{
			ID:        "msg-uuid-1",
			Content:   "hello world",
			RequestID: "req-1",
		}
		data, err := json.Marshal(&r)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["threadParentMessageId"]
		assert.False(t, present, "threadParentMessageId should be omitted when empty")
	})

	t.Run("with threadParentMessageCreatedAt", func(t *testing.T) {
		parentMillis := time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC).UnixMilli()
		raw := fmt.Sprintf(`{"id":"msg-uuid-1","content":"reply","requestId":"req-1","threadParentMessageId":"parent-msg-uuid","threadParentMessageCreatedAt":%d}`, parentMillis)
		var r model.SendMessageRequest
		require.NoError(t, json.Unmarshal([]byte(raw), &r))
		assert.Equal(t, "parent-msg-uuid", r.ThreadParentMessageID)
		require.NotNil(t, r.ThreadParentMessageCreatedAt)
		assert.Equal(t, parentMillis, *r.ThreadParentMessageCreatedAt)
	})

	t.Run("threadParentMessageCreatedAt omitted when nil", func(t *testing.T) {
		r := model.SendMessageRequest{
			ID:        "msg-uuid-1",
			Content:   "hello world",
			RequestID: "req-1",
		}
		data, err := json.Marshal(&r)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["threadParentMessageCreatedAt"]
		assert.False(t, present, "threadParentMessageCreatedAt should be omitted when nil")
	})
}

func TestMessageEventJSON(t *testing.T) {
	e := model.MessageEvent{
		Message: model.Message{
			ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			Content:   "hello",
			CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		},
		SiteID:    "site-a",
		Timestamp: 1735689600000,
	}
	data, err := json.Marshal(&e)
	require.NoError(t, err)
	var dst model.MessageEvent
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Equal(t, e, dst)
}

func TestEventTypeValues(t *testing.T) {
	assert.Equal(t, model.EventType("created"), model.EventCreated)
	assert.Equal(t, model.EventType("updated"), model.EventUpdated)
	assert.Equal(t, model.EventType("deleted"), model.EventDeleted)
}

func TestMessageEventJSON_WithEvent(t *testing.T) {
	t.Run("created event round-trip", func(t *testing.T) {
		src := model.MessageEvent{
			Event: model.EventCreated,
			Message: model.Message{
				ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
				Content:   "hello",
				CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
			},
			SiteID:    "site-a",
			Timestamp: 1735689600000,
		}
		data, err := json.Marshal(src)
		require.NoError(t, err)
		var dst model.MessageEvent
		require.NoError(t, json.Unmarshal(data, &dst))
		assert.Equal(t, src, dst)
	})

	t.Run("event field omitted when empty (backward compat)", func(t *testing.T) {
		src := model.MessageEvent{
			Message: model.Message{
				ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
				Content:   "hello",
				CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
			},
			SiteID:    "site-a",
			Timestamp: 1735689600000,
		}
		data, err := json.Marshal(src)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["event"]
		assert.False(t, present, "event should be omitted when empty")
	})

	t.Run("deleted event round-trip", func(t *testing.T) {
		src := model.MessageEvent{
			Event: model.EventDeleted,
			Message: model.Message{
				ID:        "m1",
				RoomID:    "r1",
				CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
			},
			SiteID:    "site-a",
			Timestamp: 1735689600000,
		}
		data, err := json.Marshal(src)
		require.NoError(t, err)
		var dst model.MessageEvent
		require.NoError(t, json.Unmarshal(data, &dst))
		assert.Equal(t, src, dst)
	})
}

func TestSubscriptionJSON(t *testing.T) {
	hss := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := model.Subscription{
		ID:                 "s1",
		User:               model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID:             "r1",
		SiteID:             "site-a",
		Roles:              []model.Role{model.RoleOwner},
		HistorySharedSince: &hss,
		JoinedAt:           time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		LastSeenAt:         time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		HasMention:         true,
	}

	data, err := json.Marshal(&s)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var dst model.Subscription
	if err := json.Unmarshal(data, &dst); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(s, dst) {
		t.Errorf("round-trip mismatch:\n  got  %+v\n  want %+v", dst, s)
	}
}

func TestRoomTypeValues(t *testing.T) {
	if model.RoomTypeChannel != "channel" {
		t.Errorf("RoomTypeChannel = %q", model.RoomTypeChannel)
	}
	if model.RoomTypeDM != "dm" {
		t.Errorf("RoomTypeDM = %q", model.RoomTypeDM)
	}
}

func TestRoleValues(t *testing.T) {
	if model.RoleOwner != "owner" {
		t.Errorf("RoleOwner = %q", model.RoleOwner)
	}
	if model.RoleMember != "member" {
		t.Errorf("RoleMember = %q", model.RoleMember)
	}
}

func TestRoomEventJSON(t *testing.T) {
	now := time.Date(2026, 3, 26, 12, 0, 0, 0, time.UTC)
	msg := model.Message{
		ID: "msg-1", RoomID: "room-1", UserID: "user-1",
		UserAccount: "alice", Content: "hello", CreatedAt: now,
	}

	t.Run("all fields populated", func(t *testing.T) {
		src := model.RoomEvent{
			Type:       model.RoomEventNewMessage,
			RoomID:     "room-1",
			Timestamp:  now.UnixMilli(),
			RoomName:   "General",
			RoomType:   model.RoomTypeChannel,
			SiteID:     "site-a",
			UserCount:  5,
			LastMsgAt:  now,
			LastMsgID:  "msg-1",
			Mentions:   []model.Participant{{Account: "user-2", ChineseName: "user-2", EngName: "user-2"}, {Account: "user-3", ChineseName: "user-3", EngName: "user-3"}},
			MentionAll: true,
			HasMention: true,
			Message:    &model.ClientMessage{Message: msg, Sender: &model.Participant{UserID: "user-1", Account: "alice", ChineseName: "愛麗絲", EngName: "Alice Wang"}},
		}

		data, err := json.Marshal(src)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var dst model.RoomEvent
		if err := json.Unmarshal(data, &dst); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if !reflect.DeepEqual(src, dst) {
			t.Errorf("round-trip mismatch:\n  got  %+v\n  want %+v", dst, src)
		}
	})

	t.Run("nil message and empty mentions omitted", func(t *testing.T) {
		src := model.RoomEvent{
			Type:      model.RoomEventNewMessage,
			RoomID:    "room-2",
			Timestamp: now.UnixMilli(),
			RoomName:  "Lobby",
			RoomType:  model.RoomTypeChannel,
			SiteID:    "site-b",
			UserCount: 3,
			LastMsgAt: now,
			LastMsgID: "msg-2",
		}

		data, err := json.Marshal(src)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}

		var raw map[string]any
		if err := json.Unmarshal(data, &raw); err != nil {
			t.Fatalf("unmarshal raw: %v", err)
		}
		for _, key := range []string{"mentions", "mentionAll", "hasMention", "message", "encryptedMessage"} {
			if _, ok := raw[key]; ok {
				t.Errorf("expected %q to be omitted from JSON", key)
			}
		}

		var dst model.RoomEvent
		if err := json.Unmarshal(data, &dst); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if !reflect.DeepEqual(src, dst) {
			t.Errorf("round-trip mismatch:\n  got  %+v\n  want %+v", dst, src)
		}
	})

	t.Run("encrypted message populated with nil plaintext message", func(t *testing.T) {
		src := model.RoomEvent{
			Type:             model.RoomEventNewMessage,
			RoomID:           "room-3",
			Timestamp:        now.UnixMilli(),
			RoomName:         "Encrypted",
			RoomType:         model.RoomTypeChannel,
			SiteID:           "site-c",
			UserCount:        4,
			LastMsgAt:        now,
			LastMsgID:        "msg-3",
			EncryptedMessage: json.RawMessage(`{"version":3,"ephemeralPublicKey":"AQID","nonce":"BAUG","ciphertext":"BwgJ"}`),
		}
		data, err := json.Marshal(src)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var raw map[string]any
		if err := json.Unmarshal(data, &raw); err != nil {
			t.Fatalf("unmarshal raw: %v", err)
		}
		if _, ok := raw["encryptedMessage"]; !ok {
			t.Error("expected encryptedMessage to be present in JSON")
		}
		if _, ok := raw["message"]; ok {
			t.Error("expected message to be omitted when nil")
		}
		var dst model.RoomEvent
		if err := json.Unmarshal(data, &dst); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if !reflect.DeepEqual(src, dst) {
			t.Errorf("round-trip mismatch:\n  got  %+v\n  want %+v", dst, src)
		}
	})
}

func TestRoomEventTypeValues(t *testing.T) {
	if model.RoomEventNewMessage != "new_message" {
		t.Errorf("RoomEventNewMessage = %q", model.RoomEventNewMessage)
	}
}

func TestParticipantJSON(t *testing.T) {
	t.Run("with userID", func(t *testing.T) {
		p := model.Participant{
			UserID:      "u1",
			Account:     "alice",
			ChineseName: "愛麗絲",
			EngName:     "Alice Wang",
		}
		roundTrip(t, &p, &model.Participant{})
	})

	t.Run("without userID omitted", func(t *testing.T) {
		p := model.Participant{
			Account:     "bob",
			ChineseName: "鮑勃",
			EngName:     "Bob Chen",
		}
		data, err := json.Marshal(p)
		require.NoError(t, err)

		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, hasUserID := raw["userId"]
		assert.False(t, hasUserID, "userId should be omitted when empty")

		var dst model.Participant
		require.NoError(t, json.Unmarshal(data, &dst))
		assert.Equal(t, p, dst)
	})
}

func TestClientMessageJSON(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	cm := model.ClientMessage{
		Message: model.Message{
			ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			Content: "hello", CreatedAt: now,
		},
		Sender: &model.Participant{
			UserID:      "u1",
			Account:     "alice",
			ChineseName: "愛麗絲",
			EngName:     "Alice Wang",
		},
	}
	data, err := json.Marshal(cm)
	require.NoError(t, err)

	var dst model.ClientMessage
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Equal(t, cm, dst)

	// Verify inline embedding — message fields should be at top level
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	assert.Contains(t, raw, "id")
	assert.Contains(t, raw, "roomId")
	assert.Contains(t, raw, "sender")
}

func TestRoomKeyEventJSON(t *testing.T) {
	src := model.RoomKeyEvent{
		RoomID:     "room-1",
		Version:    42,
		PublicKey:  []byte{0x04, 0x01, 0x02, 0x03},
		PrivateKey: []byte{0x0a, 0x0b, 0x0c},
		Timestamp:  1735689600000,
	}

	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var dst model.RoomKeyEvent
	if err := json.Unmarshal(data, &dst); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(src, dst) {
		t.Errorf("round-trip mismatch:\n  got  %+v\n  want %+v", dst, src)
	}
}

func TestNotificationEventJSON(t *testing.T) {
	src := model.NotificationEvent{
		Type:   "new_message",
		RoomID: "room-1",
		Message: model.Message{
			ID: "m1", RoomID: "room-1", UserID: "u1", UserAccount: "alice",
			Content: "hello", CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		},
		Timestamp: 1735689600000,
	}
	data, err := json.Marshal(&src)
	require.NoError(t, err)
	var dst model.NotificationEvent
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Equal(t, src, dst)
}

func TestUpdateRoleRequestJSON(t *testing.T) {
	src := model.UpdateRoleRequest{RoomID: "r1", Account: "bob", NewRole: model.RoleOwner, Timestamp: 1735689600000}
	roundTrip(t, &src, &model.UpdateRoleRequest{})
}

func TestSubscriptionUpdateEventJSON(t *testing.T) {
	src := model.SubscriptionUpdateEvent{
		UserID: "u1",
		Subscription: model.Subscription{
			ID:       "s1",
			User:     model.SubscriptionUser{ID: "u1", Account: "alice"},
			RoomID:   "r1",
			SiteID:   "site-a",
			Roles:    []model.Role{model.RoleMember},
			JoinedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		Action:    "added",
		Timestamp: 1735689600000,
	}
	data, err := json.Marshal(&src)
	require.NoError(t, err)
	var dst model.SubscriptionUpdateEvent
	require.NoError(t, json.Unmarshal(data, &dst))
	if !reflect.DeepEqual(src, dst) {
		t.Errorf("round-trip mismatch:\n  got  %+v\n  want %+v", dst, src)
	}
}

func TestOutboxEventJSON(t *testing.T) {
	src := model.OutboxEvent{
		Type:       "member_added",
		SiteID:     "site-a",
		DestSiteID: "site-b",
		Payload:    []byte(`{"inviterId":"u1"}`),
		Timestamp:  1735689600000,
	}
	data, err := json.Marshal(&src)
	require.NoError(t, err)
	var dst model.OutboxEvent
	require.NoError(t, json.Unmarshal(data, &dst))
	if !reflect.DeepEqual(src, dst) {
		t.Errorf("round-trip mismatch:\n  got  %+v\n  want %+v", dst, src)
	}
}

func TestInboxMemberEventJSON(t *testing.T) {
	t.Run("add event, unrestricted room", func(t *testing.T) {
		src := model.InboxMemberEvent{
			RoomID:    "r1",
			RoomName:  "engineering",
			RoomType:  model.RoomTypeChannel,
			SiteID:    "site-a",
			Accounts:  []string{"alice", "bob"},
			JoinedAt:  1735689600000,
			Timestamp: 1735689600000,
		}
		data, err := json.Marshal(&src)
		require.NoError(t, err)
		var dst model.InboxMemberEvent
		require.NoError(t, json.Unmarshal(data, &dst))
		assert.Equal(t, src, dst)
	})

	t.Run("add event, restricted room carries HistorySharedSince", func(t *testing.T) {
		hss := int64(1735689500000)
		src := model.InboxMemberEvent{
			RoomID:             "r1",
			RoomName:           "engineering",
			RoomType:           model.RoomTypeChannel,
			SiteID:             "site-a",
			Accounts:           []string{"alice"},
			HistorySharedSince: &hss,
			JoinedAt:           1735689600000,
			Timestamp:          1735689600000,
		}
		data, err := json.Marshal(&src)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		assert.EqualValues(t, hss, raw["historySharedSince"])

		var dst model.InboxMemberEvent
		require.NoError(t, json.Unmarshal(data, &dst))
		require.NotNil(t, dst.HistorySharedSince)
		assert.Equal(t, hss, *dst.HistorySharedSince)
	})

	t.Run("remove event omits HistorySharedSince and JoinedAt when nil/zero", func(t *testing.T) {
		src := model.InboxMemberEvent{
			RoomID:    "r1",
			RoomName:  "engineering",
			RoomType:  model.RoomTypeChannel,
			SiteID:    "site-a",
			Accounts:  []string{"alice", "bob"},
			Timestamp: 1735689600000,
		}
		data, err := json.Marshal(&src)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, hasHSS := raw["historySharedSince"]
		assert.False(t, hasHSS, "historySharedSince should be omitted when nil")
		_, hasJoinedAt := raw["joinedAt"]
		assert.False(t, hasJoinedAt, "joinedAt should be omitted when zero")

		var dst model.InboxMemberEvent
		require.NoError(t, json.Unmarshal(data, &dst))
		assert.Nil(t, dst.HistorySharedSince, "unrestricted event must decode HistorySharedSince as nil")
	})
}

func TestRoomMetadataUpdateEventJSON(t *testing.T) {
	src := model.RoomMetadataUpdateEvent{
		RoomID:        "r1",
		Name:          "general",
		UserCount:     5,
		LastMessageAt: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		UpdatedAt:     time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		Timestamp:     1735689600000,
	}
	roundTrip(t, &src, &model.RoomMetadataUpdateEvent{})
}

func TestRemoveMemberRequestJSON(t *testing.T) {
	t.Run("with account", func(t *testing.T) {
		r := model.RemoveMemberRequest{
			RoomID:    "r1",
			Account:   "alice",
			Timestamp: 1735689600000,
		}
		roundTrip(t, &r, &model.RemoveMemberRequest{})
	})

	t.Run("with orgId", func(t *testing.T) {
		r := model.RemoveMemberRequest{
			RoomID:    "r1",
			OrgID:     "org-1",
			Timestamp: 1735689600000,
		}
		roundTrip(t, &r, &model.RemoveMemberRequest{})
	})

	t.Run("account and orgId omitted when empty", func(t *testing.T) {
		r := model.RemoveMemberRequest{RoomID: "r1"}
		data, err := json.Marshal(&r)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, hasAccount := raw["account"]
		assert.False(t, hasAccount, "account should be omitted when empty")
		_, hasOrgID := raw["orgId"]
		assert.False(t, hasOrgID, "orgId should be omitted when empty")
	})
}

func TestMemberRemoveEventJSON(t *testing.T) {
	src := model.MemberRemoveEvent{
		Type:     "member_removed",
		RoomID:   "r1",
		Accounts: []string{"alice", "bob"},
		SiteID:   "site-a",
	}
	data, err := json.Marshal(&src)
	require.NoError(t, err)
	var dst model.MemberRemoveEvent
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Equal(t, src, dst)
}

func TestRoomTypeChannel(t *testing.T) {
	assert.Equal(t, model.RoomType("channel"), model.RoomTypeChannel)
}

func TestRoom_RestrictedJSON(t *testing.T) {
	room := model.Room{ID: "r1", Name: "general", Type: model.RoomTypeChannel, Restricted: true, SiteID: "site-a"}
	data, err := json.Marshal(room)
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(data, &m))
	assert.Equal(t, "channel", m["type"])
	assert.Equal(t, true, m["restricted"])

	room.Restricted = false
	data2, _ := json.Marshal(room)
	var m2 map[string]any
	require.NoError(t, json.Unmarshal(data2, &m2))
	_, exists := m2["restricted"]
	assert.False(t, exists, "restricted=false should be omitted")
}

func TestUser_SectIDJSON(t *testing.T) {
	user := model.User{ID: "u1", Account: "alice", SiteID: "site-a", SectID: "engineering"}
	var dst model.User
	data, err := json.Marshal(user)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Equal(t, "engineering", dst.SectID)
}

func TestMessage_TypeAndSysMsgDataJSON(t *testing.T) {
	sysData := []byte(`{"individuals":["alice"]}`)
	msg := model.Message{ID: "m1", RoomID: "r1", Content: "added members", Type: "members_added", SysMsgData: sysData}
	data, err := json.Marshal(msg)
	require.NoError(t, err)
	var dst model.Message
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Equal(t, "members_added", dst.Type)
	assert.Equal(t, sysData, dst.SysMsgData)

	regular := model.Message{ID: "m2", RoomID: "r1", Content: "hello"}
	data2, _ := json.Marshal(regular)
	var m map[string]any
	require.NoError(t, json.Unmarshal(data2, &m))
	_, hasType := m["type"]
	assert.False(t, hasType, "type should be omitted for regular messages")
}

func TestAddMembersRequestJSON(t *testing.T) {
	req := model.AddMembersRequest{
		RoomID:   "r1",
		Users:    []string{"alice", "bob"},
		Orgs:     []string{"engineering"},
		Channels: []model.ChannelRef{{RoomID: "general", SiteID: "site-a"}},
		History:  model.HistoryConfig{Mode: model.HistoryModeAll},
	}
	data, err := json.Marshal(req)
	require.NoError(t, err)
	var dst model.AddMembersRequest
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Equal(t, req, dst)
}

func TestHistoryModeConstants(t *testing.T) {
	assert.Equal(t, model.HistoryMode("none"), model.HistoryModeNone)
	assert.Equal(t, model.HistoryMode("all"), model.HistoryModeAll)
}

func TestRoomMemberJSON(t *testing.T) {
	rm := model.RoomMember{
		ID:     "rm1",
		RoomID: "r1",
		Ts:     time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		Member: model.RoomMemberEntry{
			ID:      "u1",
			Type:    model.RoomMemberIndividual,
			Account: "alice",
		},
	}
	data, err := json.Marshal(&rm)
	require.NoError(t, err)
	var dst model.RoomMember
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Equal(t, rm, dst)
}

func TestSysMsgUserJSON(t *testing.T) {
	u := model.SysMsgUser{
		Account:     "alice",
		EngName:     "Alice Wang",
		ChineseName: "愛麗絲",
	}
	roundTrip(t, &u, &model.SysMsgUser{})
}

func TestMemberLeftJSON(t *testing.T) {
	ml := model.MemberLeft{
		User: model.SysMsgUser{
			Account:     "alice",
			EngName:     "Alice Wang",
			ChineseName: "愛麗絲",
		},
	}
	roundTrip(t, &ml, &model.MemberLeft{})
}

func TestMemberRemovedJSON(t *testing.T) {
	t.Run("with user", func(t *testing.T) {
		mr := model.MemberRemoved{
			User:              &model.SysMsgUser{Account: "alice", EngName: "Alice Wang", ChineseName: "愛麗絲"},
			RemovedUsersCount: 1,
		}
		data, err := json.Marshal(&mr)
		require.NoError(t, err)
		var dst model.MemberRemoved
		require.NoError(t, json.Unmarshal(data, &dst))
		assert.Equal(t, mr, dst)
	})

	t.Run("with org", func(t *testing.T) {
		mr := model.MemberRemoved{
			OrgID:             "org-1",
			SectName:          "Engineering",
			RemovedUsersCount: 5,
		}
		data, err := json.Marshal(&mr)
		require.NoError(t, err)
		var dst model.MemberRemoved
		require.NoError(t, json.Unmarshal(data, &dst))
		assert.Equal(t, mr, dst)
	})

	t.Run("user, orgId, sectName omitted when empty", func(t *testing.T) {
		mr := model.MemberRemoved{RemovedUsersCount: 3}
		data, err := json.Marshal(&mr)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, hasUser := raw["user"]
		assert.False(t, hasUser, "user should be omitted when nil")
		_, hasOrgID := raw["orgId"]
		assert.False(t, hasOrgID, "orgId should be omitted when empty")
		_, hasSectName := raw["sectName"]
		assert.False(t, hasSectName, "sectName should be omitted when empty")
	})
}

func TestRoomMemberTypeValues(t *testing.T) {
	assert.Equal(t, model.RoomMemberType("individual"), model.RoomMemberIndividual)
	assert.Equal(t, model.RoomMemberType("org"), model.RoomMemberOrg)
}

func TestMembersAddedJSON(t *testing.T) {
	ma := model.MembersAdded{
		Individuals:     []string{"alice", "bob"},
		Orgs:            []string{"engineering"},
		Channels:        []model.ChannelRef{{RoomID: "general", SiteID: "site-a"}},
		AddedUsersCount: 5,
	}
	data, err := json.Marshal(ma)
	require.NoError(t, err)
	var dst model.MembersAdded
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Equal(t, ma, dst)
}

func TestMemberAddEventJSON(t *testing.T) {
	t.Run("restricted room round-trips HistorySharedSince pointer", func(t *testing.T) {
		hss := int64(1735689600000)
		src := model.MemberAddEvent{
			Type:               "member_added",
			RoomID:             "r1",
			Accounts:           []string{"alice", "bob"},
			SiteID:             "site-a",
			JoinedAt:           1735689600000,
			HistorySharedSince: &hss,
			Timestamp:          1735689600000,
		}
		data, err := json.Marshal(src)
		require.NoError(t, err)

		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		assert.EqualValues(t, hss, raw["historySharedSince"])

		var dst model.MemberAddEvent
		require.NoError(t, json.Unmarshal(data, &dst))
		require.NotNil(t, dst.HistorySharedSince)
		assert.Equal(t, hss, *dst.HistorySharedSince)
	})

	t.Run("unrestricted room omits historySharedSince on the wire", func(t *testing.T) {
		src := model.MemberAddEvent{
			Type:      "member_added",
			RoomID:    "r1",
			Accounts:  []string{"alice"},
			SiteID:    "site-a",
			JoinedAt:  1735689600000,
			Timestamp: 1735689600000,
		}
		data, err := json.Marshal(src)
		require.NoError(t, err)

		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, hasHSS := raw["historySharedSince"]
		assert.False(t, hasHSS, "historySharedSince must be omitted when nil")

		var dst model.MemberAddEvent
		require.NoError(t, json.Unmarshal(data, &dst))
		assert.Nil(t, dst.HistorySharedSince)
	})
}

func TestListRoomMembersRequestJSON(t *testing.T) {
	t.Run("with limit and offset", func(t *testing.T) {
		limit, offset := 10, 5
		r := model.ListRoomMembersRequest{Limit: &limit, Offset: &offset}
		data, err := json.Marshal(&r)
		require.NoError(t, err)
		var dst model.ListRoomMembersRequest
		require.NoError(t, json.Unmarshal(data, &dst))
		require.NotNil(t, dst.Limit)
		require.NotNil(t, dst.Offset)
		assert.Equal(t, 10, *dst.Limit)
		assert.Equal(t, 5, *dst.Offset)
	})

	t.Run("omitempty when nil", func(t *testing.T) {
		r := model.ListRoomMembersRequest{}
		data, err := json.Marshal(&r)
		require.NoError(t, err)
		assert.Equal(t, "{}", string(data))
	})

	t.Run("with enrich true", func(t *testing.T) {
		r := model.ListRoomMembersRequest{Enrich: true}
		data, err := json.Marshal(&r)
		require.NoError(t, err)
		assert.Equal(t, `{"enrich":true}`, string(data))

		var dst model.ListRoomMembersRequest
		require.NoError(t, json.Unmarshal(data, &dst))
		assert.True(t, dst.Enrich)
	})
}

func TestListRoomMembersResponseJSON(t *testing.T) {
	resp := model.ListRoomMembersResponse{
		Members: []model.RoomMember{
			{
				ID:     "rm1",
				RoomID: "r1",
				Ts:     time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
				Member: model.RoomMemberEntry{ID: "alice", Type: model.RoomMemberIndividual, Account: "alice"},
			},
		},
	}
	data, err := json.Marshal(&resp)
	require.NoError(t, err)
	var dst model.ListRoomMembersResponse
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Equal(t, resp, dst)
}

func TestRoomMemberEntry_DisplayFields_JSON(t *testing.T) {
	entry := model.RoomMemberEntry{
		ID: "u1", Type: model.RoomMemberIndividual, Account: "alice",
		EngName: "Alice Wang", ChineseName: "愛麗絲", IsOwner: true,
	}
	data, err := json.Marshal(&entry)
	require.NoError(t, err)

	// JSON carries all fields, including the display ones.
	var got map[string]any
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, "u1", got["id"])
	assert.Equal(t, "individual", got["type"])
	assert.Equal(t, "alice", got["account"])
	assert.Equal(t, "Alice Wang", got["engName"])
	assert.Equal(t, "愛麗絲", got["chineseName"])
	assert.Equal(t, true, got["isOwner"])
}

func TestRoomMemberEntry_DisplayFields_OmittedWhenZero(t *testing.T) {
	entry := model.RoomMemberEntry{
		ID: "u1", Type: model.RoomMemberIndividual, Account: "alice",
	}
	data, err := json.Marshal(&entry)
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, json.Unmarshal(data, &got))
	for _, k := range []string{"engName", "chineseName", "isOwner", "sectName", "memberCount"} {
		_, present := got[k]
		assert.False(t, present, "display field %q should be omitted when zero", k)
	}
}

func TestRoomMemberEntry_DisplayFields_NotPersistedToBSON(t *testing.T) {
	entry := model.RoomMemberEntry{
		ID: "org-1", Type: model.RoomMemberOrg,
		SectName: "Engineering", MemberCount: 42,
	}
	data, err := bson.Marshal(&entry)
	require.NoError(t, err)

	var got bson.M
	require.NoError(t, bson.Unmarshal(data, &got))
	assert.Equal(t, "org-1", got["id"])
	assert.Equal(t, "org", got["type"])
	for _, k := range []string{"engName", "chineseName", "isOwner", "sectName", "memberCount"} {
		_, present := got[k]
		assert.False(t, present, "display field %q must not be persisted to BSON", k)
	}
}

func TestOrgMemberJSON(t *testing.T) {
	m := model.OrgMember{
		ID:          "u-alice",
		Account:     "alice",
		EngName:     "Alice Wang",
		ChineseName: "愛麗絲",
		SiteID:      "site-a",
	}
	data, err := json.Marshal(&m)
	require.NoError(t, err)
	var dst model.OrgMember
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Equal(t, m, dst)
}

func TestListOrgMembersResponseJSON(t *testing.T) {
	resp := model.ListOrgMembersResponse{
		Members: []model.OrgMember{
			{ID: "u-alice", Account: "alice", EngName: "Alice Wang", ChineseName: "愛麗絲", SiteID: "site-a"},
		},
	}
	data, err := json.Marshal(&resp)
	require.NoError(t, err)
	var dst model.ListOrgMembersResponse
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Equal(t, resp, dst)
}

func TestRoomsInfoBatchRequestJSON(t *testing.T) {
	src := model.RoomsInfoBatchRequest{
		RoomIDs: []string{"r1", "r2", "r3"},
	}
	data, err := json.Marshal(&src)
	require.NoError(t, err)
	var dst model.RoomsInfoBatchRequest
	require.NoError(t, json.Unmarshal(data, &dst))
	if !reflect.DeepEqual(src, dst) {
		t.Errorf("round-trip mismatch:\n  got  %+v\n  want %+v", dst, src)
	}
}

func TestRoomInfoJSON(t *testing.T) {
	t.Run("happy path with all fields", func(t *testing.T) {
		pk := "dGVzdC1wcml2YXRlLWtleS1iYXNlNjQ="
		kv := 7
		lastMsg := int64(1735689600000)
		lastMention := int64(1735693200000)
		src := model.RoomInfo{
			RoomID:           "r1",
			Found:            true,
			SiteID:           "site-a",
			Name:             "general",
			LastMsgAt:        &lastMsg,
			LastMentionAllAt: &lastMention,
			PrivateKey:       &pk,
			KeyVersion:       &kv,
		}
		data, err := json.Marshal(&src)
		require.NoError(t, err)
		var dst model.RoomInfo
		require.NoError(t, json.Unmarshal(data, &dst))
		if !reflect.DeepEqual(src, dst) {
			t.Errorf("round-trip mismatch:\n  got  %+v\n  want %+v", dst, src)
		}
	})

	t.Run("found=false omits all optional fields", func(t *testing.T) {
		src := model.RoomInfo{
			RoomID: "r1",
			Found:  false,
		}
		data, err := json.Marshal(&src)
		require.NoError(t, err)

		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))

		assert.Contains(t, raw, "roomId")
		assert.Equal(t, "r1", raw["roomId"])

		foundVal, foundPresent := raw["found"]
		assert.True(t, foundPresent, "found must be present")
		assert.Equal(t, false, foundVal)

		for _, key := range []string{"siteId", "name", "lastMsgAt", "lastMentionAllAt", "privateKey", "keyVersion", "error"} {
			_, present := raw[key]
			assert.False(t, present, "%q should be omitted", key)
		}
	})

	t.Run("found=true with nil PrivateKey omits zero-valued optional fields", func(t *testing.T) {
		src := model.RoomInfo{
			RoomID: "r1",
			Found:  true,
			SiteID: "site-a",
			Name:   "general",
		}
		data, err := json.Marshal(&src)
		require.NoError(t, err)

		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))

		for _, key := range []string{"lastMsgAt", "lastMentionAllAt", "privateKey", "keyVersion"} {
			_, present := raw[key]
			assert.False(t, present, "%q should be omitted when zero/nil", key)
		}
	})

	t.Run("nil LastMsgAt omitted; pointer to zero LastMentionAllAt emitted as 0", func(t *testing.T) {
		zero := int64(0)
		src := model.RoomInfo{
			RoomID:           "r1",
			Found:            true,
			LastMsgAt:        nil,
			LastMentionAllAt: &zero,
		}
		data, err := json.Marshal(&src)
		require.NoError(t, err)

		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))

		_, hasLastMsg := raw["lastMsgAt"]
		assert.False(t, hasLastMsg, "nil LastMsgAt must be omitted from JSON")

		lastMention, hasMention := raw["lastMentionAllAt"]
		require.True(t, hasMention, "non-nil LastMentionAllAt must be present even when value is 0")
		assert.Equal(t, float64(0), lastMention, "zero value must round-trip as JSON number 0")

		var dst model.RoomInfo
		require.NoError(t, json.Unmarshal(data, &dst))
		assert.Nil(t, dst.LastMsgAt, "absent JSON field must unmarshal to nil pointer")
		require.NotNil(t, dst.LastMentionAllAt)
		assert.Equal(t, int64(0), *dst.LastMentionAllAt)
	})
}

func TestRoomsInfoBatchResponseJSON(t *testing.T) {
	pk := "dGVzdC1rZXk="
	kv := 3
	lastMsg := int64(1735689600000)
	lastMention := int64(1735693200000)
	src := model.RoomsInfoBatchResponse{
		Rooms: []model.RoomInfo{
			{
				RoomID:           "r1",
				Found:            true,
				SiteID:           "site-a",
				Name:             "general",
				LastMsgAt:        &lastMsg,
				LastMentionAllAt: &lastMention,
				PrivateKey:       &pk,
				KeyVersion:       &kv,
			},
			{
				RoomID: "r2",
				Found:  false,
			},
		},
	}
	data, err := json.Marshal(&src)
	require.NoError(t, err)
	var dst model.RoomsInfoBatchResponse
	require.NoError(t, json.Unmarshal(data, &dst))
	if !reflect.DeepEqual(src, dst) {
		t.Errorf("round-trip mismatch:\n  got  %+v\n  want %+v", dst, src)
	}
}

func TestSearchMessagesRequestJSON(t *testing.T) {
	t.Run("full", func(t *testing.T) {
		req := model.SearchMessagesRequest{
			SearchText: "hello",
			RoomIDs:    []string{"r1", "r2"},
			Size:       50,
			Offset:     25,
		}
		data, err := json.Marshal(&req)
		require.NoError(t, err)
		var dst model.SearchMessagesRequest
		require.NoError(t, json.Unmarshal(data, &dst))
		assert.True(t, reflect.DeepEqual(req, dst))
	})

	t.Run("global (roomIds omitted when nil)", func(t *testing.T) {
		req := model.SearchMessagesRequest{SearchText: "hello"}
		data, err := json.Marshal(&req)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["roomIds"]
		assert.False(t, present, "roomIds must be omitted when nil")
	})
}

func TestSearchMessagesResponseJSON(t *testing.T) {
	created := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	parent := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
	resp := model.SearchMessagesResponse{
		Total: 3,
		Results: []model.MessageSearchHit{
			{
				MessageID:             "m1",
				RoomID:                "r1",
				SiteID:                "site-a",
				UserID:                "u1",
				UserAccount:           "alice",
				Content:               "hello",
				CreatedAt:             created,
				ThreadParentMessageID: "p1",
				ThreadParentCreatedAt: &parent,
			},
			{
				MessageID:   "m2",
				RoomID:      "r2",
				SiteID:      "site-b",
				UserID:      "u2",
				UserAccount: "bob",
				Content:     "world",
				CreatedAt:   created,
			},
		},
	}
	data, err := json.Marshal(&resp)
	require.NoError(t, err)
	var dst model.SearchMessagesResponse
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.True(t, reflect.DeepEqual(resp, dst))
}

func TestMessageSearchHitThreadFieldsOmitted(t *testing.T) {
	hit := model.MessageSearchHit{
		MessageID: "m1", RoomID: "r1", SiteID: "site-a",
		UserID: "u1", UserAccount: "alice", Content: "hi",
		CreatedAt: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
	}
	data, err := json.Marshal(&hit)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	_, hasPid := raw["threadParentMessageId"]
	_, hasPts := raw["threadParentMessageCreatedAt"]
	assert.False(t, hasPid, "threadParentMessageId must be omitted when empty")
	assert.False(t, hasPts, "threadParentMessageCreatedAt must be omitted when nil")
}

func TestSearchRoomsRequestJSON(t *testing.T) {
	t.Run("full", func(t *testing.T) {
		req := model.SearchRoomsRequest{
			SearchText: "general",
			Scope:      "channel",
			Size:       25,
			Offset:     0,
		}
		roundTrip(t, &req, &model.SearchRoomsRequest{})
	})

	t.Run("scope omitted when empty", func(t *testing.T) {
		req := model.SearchRoomsRequest{SearchText: "x"}
		data, err := json.Marshal(&req)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["scope"]
		assert.False(t, present, "scope must be omitted when empty")
	})
}

func TestSearchRoomsResponseJSON(t *testing.T) {
	joined := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	resp := model.SearchRoomsResponse{
		Total: 2,
		Results: []model.RoomSearchHit{
			{RoomID: "r1", RoomName: "general", RoomType: "p", UserAccount: "alice", SiteID: "site-a", JoinedAt: joined},
			{RoomID: "r2", RoomName: "alice-bob", RoomType: "d", UserAccount: "alice", SiteID: "site-a", JoinedAt: joined},
		},
	}
	data, err := json.Marshal(&resp)
	require.NoError(t, err)
	var dst model.SearchRoomsResponse
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.True(t, reflect.DeepEqual(resp, dst))
}

func TestChannelRefJSONBSON(t *testing.T) {
	src := model.ChannelRef{RoomID: "room-eng", SiteID: "site-us"}

	t.Run("json", func(t *testing.T) {
		data, err := json.Marshal(&src)
		require.NoError(t, err)
		// Tag spelling matters — the wire contract with frontends uses camelCase.
		assert.Equal(t, `{"roomId":"room-eng","siteId":"site-us"}`, string(data))
		var dst model.ChannelRef
		require.NoError(t, json.Unmarshal(data, &dst))
		assert.Equal(t, src, dst)
	})

	t.Run("bson", func(t *testing.T) {
		data, err := bson.Marshal(&src)
		require.NoError(t, err)
		var dst model.ChannelRef
		require.NoError(t, bson.Unmarshal(data, &dst))
		assert.Equal(t, src, dst)
	})
}

// roundTrip marshals src to JSON, unmarshals into dst, and compares.
func roundTrip[T any](t *testing.T, src *T, dst *T) {
	t.Helper()
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := json.Unmarshal(data, dst); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(*src, *dst) {
		t.Errorf("round-trip mismatch:\n  got  %+v\n  want %+v", *dst, *src)
	}
}
