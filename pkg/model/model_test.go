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
	"github.com/hmchangw/chat/pkg/model/cassandra"
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

func TestUserJSON_WithSectAndDept(t *testing.T) {
	u := model.User{
		ID: "u1", Account: "alice", SiteID: "site-a",
		SectID: "S", SectName: "Sect", SectTCName: "部",
		DeptID: "D", DeptName: "Dept", DeptTCName: "處",
		EngName: "Alice", ChineseName: "爱丽丝",
	}
	roundTrip(t, &u, &model.User{})
}

func TestUserJSON_RolesOmittedWhenEmpty(t *testing.T) {
	u := model.User{ID: "u1", Account: "alice", SiteID: "site-a"}
	data, err := json.Marshal(&u)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	_, has := raw["roles"]
	assert.False(t, has, "nil Roles must be omitted from JSON")
}

func TestIsPlatformAdmin(t *testing.T) {
	tests := []struct {
		name string
		u    *model.User
		want bool
	}{
		{"nil receiver", nil, false},
		{"nil roles", &model.User{Account: "alice"}, false},
		{"empty roles", &model.User{Account: "alice", Roles: []model.UserRole{}}, false},
		{"user role only", &model.User{Account: "alice", Roles: []model.UserRole{model.UserRoleUser}}, false},
		{"admin role present", &model.User{Account: "alice", Roles: []model.UserRole{model.UserRoleAdmin}}, true},
		{"admin among many", &model.User{Account: "alice", Roles: []model.UserRole{model.UserRoleUser, model.UserRoleAdmin, "auditor"}}, true},
		{"case-sensitive (Admin not admin)", &model.User{Account: "alice", Roles: []model.UserRole{"Admin"}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, model.IsPlatformAdmin(tt.u))
		})
	}
}

func TestRoomJSON(t *testing.T) {
	lastMsg := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	lastMention := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	minSeen := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	r := model.Room{
		ID: "r1", Name: "general", Type: model.RoomTypeChannel,
		SiteID: "site-a", UserCount: 5,
		LastMsgAt:         &lastMsg,
		LastMsgID:         "m1",
		LastMentionAllAt:  &lastMention,
		MinUserLastSeenAt: &minSeen,
		CreatedAt:         time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt:         time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	roundTrip(t, &r, &model.Room{})
}

func TestRoomJSON_NilTimestampsOmitted(t *testing.T) {
	r := model.Room{
		ID: "r1", Name: "general", Type: model.RoomTypeChannel,
		SiteID: "site-a", UserCount: 1,
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

	_, hasMinSeen := raw["minUserLastSeenAt"]
	assert.False(t, hasMinSeen, "nil MinUserLastSeenAt must be omitted from JSON")

	var dst model.Room
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Nil(t, dst.LastMsgAt, "absent JSON field must unmarshal to nil pointer")
	assert.Nil(t, dst.LastMentionAllAt, "absent JSON field must unmarshal to nil pointer")
	assert.Nil(t, dst.MinUserLastSeenAt, "absent JSON field must unmarshal to nil pointer")
}

func TestRoomJSON_WithDMParticipants(t *testing.T) {
	r := model.Room{
		ID: "r1", Name: "dm", Type: model.RoomTypeDM,
		SiteID: "site-a", UserCount: 2,
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		UIDs:      []string{"u1", "u2"},
		Accounts:  []string{"alice", "bob"},
	}
	roundTrip(t, &r, &model.Room{})
}

func TestRoomJSON_NilDMParticipantsOmitted(t *testing.T) {
	r := model.Room{
		ID: "r1", Name: "general", Type: model.RoomTypeChannel,
		SiteID: "site-a", UserCount: 1,
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	data, err := json.Marshal(&r)
	require.NoError(t, err)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))

	_, hasUIDs := raw["uids"]
	assert.False(t, hasUIDs, "nil UIDs must be omitted from JSON")

	_, hasAccounts := raw["accounts"]
	assert.False(t, hasAccounts, "nil Accounts must be omitted from JSON")

	var dst model.Room
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Nil(t, dst.UIDs, "absent JSON field must unmarshal to nil slice")
	assert.Nil(t, dst.Accounts, "absent JSON field must unmarshal to nil slice")
}

func TestThreadRoomJSON(t *testing.T) {
	tr := model.ThreadRoom{
		ID:                    "tr-1",
		ParentMessageID:       "msg-parent",
		RoomID:                "r1",
		SiteID:                "site-a",
		ThreadParentCreatedAt: time.Date(2025, 12, 31, 23, 0, 0, 0, time.UTC),
		LastMsgAt:             time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		LastMsgID:             "msg-2",
		ReplyAccounts:         []string{"alice", "bob"},
		CreatedAt:             time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt:             time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
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

	t.Run("editedAt + updatedAt round-trip", func(t *testing.T) {
		edited := time.Date(2026, 1, 1, 12, 5, 0, 0, time.UTC)
		updated := time.Date(2026, 1, 1, 12, 6, 0, 0, time.UTC)
		m := model.Message{
			ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			Content:   "hello (edited)",
			CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
			EditedAt:  &edited,
			UpdatedAt: &updated,
		}
		roundTrip(t, &m, &model.Message{})
	})

	t.Run("editedAt + updatedAt omitted when nil", func(t *testing.T) {
		m := model.Message{
			ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			Content:   "hello",
			CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		}
		data, err := json.Marshal(&m)
		require.NoError(t, err)
		got := string(data)
		assert.NotContains(t, got, `"editedAt"`, "nil EditedAt must be omitted")
		assert.NotContains(t, got, `"updatedAt"`, "nil UpdatedAt must be omitted")
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
	t.Run("with optional fields set", func(t *testing.T) {
		hss := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		lsa := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
		s := model.Subscription{
			ID:                 "s1",
			User:               model.SubscriptionUser{ID: "u1", Account: "alice"},
			RoomID:             "r1",
			RoomType:           model.RoomTypeChannel,
			SiteID:             "site-a",
			Roles:              []model.Role{model.RoleOwner},
			HistorySharedSince: &hss,
			JoinedAt:           time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			LastSeenAt:         &lsa,
			HasMention:         true,
			ThreadUnread:       []string{"parent-1", "parent-2"},
			Alert:              true,
			Muted:              true,
			Favorite:           true,
		}
		roundTrip(t, &s, &model.Subscription{})
	})

	t.Run("lastSeenAt omitted when nil", func(t *testing.T) {
		s := model.Subscription{
			ID:       "s1",
			User:     model.SubscriptionUser{ID: "u1", Account: "alice"},
			RoomID:   "r1",
			RoomType: model.RoomTypeChannel,
			SiteID:   "site-a",
			Roles:    []model.Role{model.RoleMember},
			JoinedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		}

		data, err := json.Marshal(&s)
		require.NoError(t, err)

		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["lastSeenAt"]
		assert.False(t, present, "lastSeenAt should be omitted when nil")

		roundTrip(t, &s, &model.Subscription{})
	})
}

func TestIsRoomMember(t *testing.T) {
	assert.False(t, model.IsRoomMember(nil), "nil sub is not a member")
	assert.True(t, model.IsRoomMember(&model.Subscription{}), "zero-value sub is a member (no role gating)")
	assert.True(t, model.IsRoomMember(&model.Subscription{RoomID: "r1"}), "populated sub is a member")
}

func TestSubscriptionJSON_ThreadUnreadOmittedAlertAlwaysPresent(t *testing.T) {
	s := model.Subscription{
		ID:       "s1",
		User:     model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID:   "r1",
		RoomType: model.RoomTypeChannel,
		SiteID:   "site-a",
		Roles:    []model.Role{model.RoleMember},
		JoinedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	data, err := json.Marshal(&s)
	require.NoError(t, err)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))

	_, hasThreadUnread := raw["threadUnread"]
	assert.False(t, hasThreadUnread, "nil/empty ThreadUnread must be omitted from JSON")

	alertVal, hasAlert := raw["alert"]
	assert.True(t, hasAlert, "alert must be present in JSON even when false")
	assert.Equal(t, false, alertVal)

	mutedVal, hasMuted := raw["muted"]
	assert.True(t, hasMuted, "muted must be present in JSON even when false")
	assert.Equal(t, false, mutedVal)

	favoriteVal, hasFavorite := raw["favorite"]
	assert.True(t, hasFavorite, "favorite must be present in JSON even when false")
	assert.Equal(t, false, favoriteVal)

	var dst model.Subscription
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Nil(t, dst.ThreadUnread, "absent threadUnread must unmarshal to nil")
	assert.False(t, dst.Alert)
	assert.False(t, dst.Favorite)
}

func TestDMSubscriptionJSON_EmbeddedFlattensWithHRInfo(t *testing.T) {
	// Verify Go's embedded *Subscription serialisation flattens onto the
	// top-level object — the frontend depends on this in api/types.ts
	// (DMSubscription extends Subscription).
	d := model.DMSubscription{
		Subscription: &model.Subscription{
			ID:       "s-dm-1",
			User:     model.SubscriptionUser{ID: "u-alice", Account: "alice"},
			RoomID:   "r-dm-1",
			SiteID:   "site-A",
			Roles:    []model.Role{model.RoleMember},
			Name:     "bob-dm",
			RoomType: model.RoomTypeDM,
			JoinedAt: time.Date(2026, 5, 14, 0, 0, 0, 0, time.UTC),
		},
		HRInfo: &model.SubscriptionHRInfo{
			Account: "bob",
			Name:    "鮑勃",
			EngName: "Bob Chen",
		},
	}

	data, err := json.Marshal(&d)
	require.NoError(t, err)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))

	// Subscription fields must appear top-level (flattening).
	assert.Equal(t, "s-dm-1", raw["id"])
	assert.Equal(t, "r-dm-1", raw["roomId"])
	assert.Equal(t, "dm", raw["roomType"])

	// hrInfo is nested.
	hr, ok := raw["hrInfo"].(map[string]any)
	require.True(t, ok, "hrInfo must be a nested object on the wire")
	assert.Equal(t, "bob", hr["account"])
	assert.Equal(t, "鮑勃", hr["name"])
	assert.Equal(t, "Bob Chen", hr["engName"])
}

func TestDMSubscriptionJSON_HRInfoOmittedWhenNil(t *testing.T) {
	// `*SubscriptionHRInfo` with `omitempty` should disappear from the
	// JSON when nil — channels/botDMs that share this wrapper shouldn't
	// have a phantom hrInfo: null on the wire.
	d := model.DMSubscription{
		Subscription: &model.Subscription{
			ID:       "s-c-1",
			User:     model.SubscriptionUser{ID: "u-alice", Account: "alice"},
			RoomID:   "r-c-1",
			SiteID:   "site-A",
			Roles:    []model.Role{model.RoleMember},
			RoomType: model.RoomTypeChannel,
			JoinedAt: time.Date(2026, 5, 14, 0, 0, 0, 0, time.UTC),
		},
	}

	data, err := json.Marshal(&d)
	require.NoError(t, err)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	_, hasHRInfo := raw["hrInfo"]
	assert.False(t, hasHRInfo, "nil HRInfo must be omitted from JSON")
}

func TestSubscriptionHRInfoJSON(t *testing.T) {
	// All three fields are required strings (no omitempty) — when the
	// HRInfo pointer is non-nil, every field is on the wire.
	hr := model.SubscriptionHRInfo{
		Account: "bob",
		Name:    "鮑勃",
		EngName: "Bob Chen",
	}
	data, err := json.Marshal(&hr)
	require.NoError(t, err)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	assert.Equal(t, "bob", raw["account"])
	assert.Equal(t, "鮑勃", raw["name"])
	assert.Equal(t, "Bob Chen", raw["engName"])

	var dst model.SubscriptionHRInfo
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Equal(t, hr, dst)
}

func TestRoomBotDMRoundtrip(t *testing.T) {
	r := model.Room{
		ID:        "r1",
		Name:      "weather chat",
		Type:      model.RoomTypeBotDM,
		SiteID:    "site-A",
		UserCount: 1,
		AppCount:  1,
		CreatedAt: time.Date(2026, 4, 28, 0, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 4, 28, 0, 0, 0, 0, time.UTC),
	}

	var dst model.Room
	roundTrip(t, &r, &dst)
	assert.Equal(t, model.RoomTypeBotDM, dst.Type)
	assert.Equal(t, "botDM", string(dst.Type))
	assert.Equal(t, 1, dst.AppCount)
}

func TestRoomTypeValues(t *testing.T) {
	if model.RoomTypeChannel != "channel" {
		t.Errorf("RoomTypeChannel = %q", model.RoomTypeChannel)
	}
	if model.RoomTypeDM != "dm" {
		t.Errorf("RoomTypeDM = %q", model.RoomTypeDM)
	}
	if model.RoomTypeBotDM != "botDM" {
		t.Errorf("RoomTypeBotDM = %q", model.RoomTypeBotDM)
	}
	if model.RoomTypeDiscussion != "discussion" {
		t.Errorf("RoomTypeDiscussion = %q", model.RoomTypeDiscussion)
	}
}

func TestRoleValues(t *testing.T) {
	if model.RoleOwner != "owner" {
		t.Errorf("RoleOwner = %q", model.RoleOwner)
	}
	if model.RoleAdmin != "admin" {
		t.Errorf("RoleAdmin = %q", model.RoleAdmin)
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

	t.Run("with siteID round-trips", func(t *testing.T) {
		p := model.Participant{
			UserID:      "u1",
			Account:     "alice",
			SiteID:      "site-a",
			ChineseName: "愛麗絲",
			EngName:     "Alice Wang",
		}
		roundTrip(t, &p, &model.Participant{})
	})

	t.Run("siteID omitted when empty", func(t *testing.T) {
		p := model.Participant{
			UserID:  "u1",
			Account: "alice",
			EngName: "Alice Wang",
		}
		data, err := json.Marshal(p)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, hasSiteID := raw["siteId"]
		assert.False(t, hasSiteID, "siteId should be omitted when empty")
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

func TestRoomKeyEnsureRequestJSON(t *testing.T) {
	src := model.RoomKeyEnsureRequest{RoomID: "room-abc"}
	roundTrip(t, &src, &model.RoomKeyEnsureRequest{})
}

func TestRoomKeyEnsureResponseJSON(t *testing.T) {
	src := model.RoomKeyEnsureResponse{RoomID: "room-abc", Version: 3}
	roundTrip(t, &src, &model.RoomKeyEnsureResponse{})
}

func TestRoomKeyGetRequestJSON(t *testing.T) {
	t.Run("RoomKeyGetRequest_explicitVersion", func(t *testing.T) {
		v := 3
		src := model.RoomKeyGetRequest{Version: &v}
		dst := model.RoomKeyGetRequest{}
		roundTrip(t, &src, &dst)
		require.NotNil(t, dst.Version)
		require.Equal(t, 3, *dst.Version)
	})

	t.Run("RoomKeyGetRequest_nilVersion", func(t *testing.T) {
		src := model.RoomKeyGetRequest{Version: nil}
		dst := model.RoomKeyGetRequest{}
		roundTrip(t, &src, &dst)
		require.Nil(t, dst.Version)
	})
}

func TestRoomKeyGetResponseJSON(t *testing.T) {
	src := model.RoomKeyGetResponse{
		RoomID:     "r1",
		Version:    2,
		PrivateKey: []byte{0x01, 0x02, 0x03},
	}
	dst := model.RoomKeyGetResponse{}
	roundTrip(t, &src, &dst)
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

func TestOutboxEventJSON_ThreadSubscriptionUpserted(t *testing.T) {
	src := model.OutboxEvent{
		Type:       model.OutboxThreadSubscriptionUpserted,
		SiteID:     "site-a",
		DestSiteID: "site-b",
		Payload:    []byte(`{"id":"sub-1","threadRoomId":"tr-1"}`),
		Timestamp:  1735689600000,
	}
	data, err := json.Marshal(&src)
	require.NoError(t, err)

	var dst model.OutboxEvent
	require.NoError(t, json.Unmarshal(data, &dst))
	if !reflect.DeepEqual(src, dst) {
		t.Errorf("round-trip mismatch:\n  got  %+v\n  want %+v", dst, src)
	}
	if dst.Type != "thread_subscription_upserted" {
		t.Errorf("Type = %q, want thread_subscription_upserted", dst.Type)
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
	t.Run("basic", func(t *testing.T) {
		e := model.MemberRemoveEvent{
			Type:     "member_removed",
			RoomID:   "r1",
			Accounts: []string{"alice", "bob"},
			SiteID:   "site-a",
		}
		roundTrip(t, &e, &model.MemberRemoveEvent{})
	})
}

func TestRoomTypeChannel(t *testing.T) {
	assert.Equal(t, model.RoomType("channel"), model.RoomTypeChannel)
}

func TestEventPinnedConstants(t *testing.T) {
	assert.Equal(t, model.EventType("pinned"), model.EventPinned)
	assert.Equal(t, model.EventType("unpinned"), model.EventUnpinned)
}

func TestMessagePinnedFieldsRoundTrip(t *testing.T) {
	at := time.Date(2026, 5, 19, 12, 0, 0, 0, time.UTC)
	src := model.Message{
		ID:       "m1",
		RoomID:   "r1",
		PinnedAt: &at,
		PinnedBy: &model.Participant{UserID: "u-123", Account: "alice"},
	}
	dst := model.Message{}
	roundTrip(t, &src, &dst)
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

func TestMemberAddEventCarriesRoomName(t *testing.T) {
	evt := model.MemberAddEvent{
		Type:      "member_added",
		RoomID:    "r1",
		RoomName:  "deal team",
		Accounts:  []string{"bob"},
		SiteID:    "site-A",
		JoinedAt:  1,
		Timestamp: 1,
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	assert.Equal(t, "deal team", raw["roomName"])

	var dst model.MemberAddEvent
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Equal(t, "deal team", dst.RoomName)
}

func TestMemberAddEventRoomNameOmitemptyOnZero(t *testing.T) {
	evt := model.MemberAddEvent{
		Type:      "member_added",
		RoomID:    "r1",
		Accounts:  []string{},
		SiteID:    "site-a",
		Timestamp: 1,
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	// Note: RoomName is NOT omitempty, so empty string still appears
	assert.Equal(t, "", raw["roomName"])

	var dst model.MemberAddEvent
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Empty(t, dst.RoomName)
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
	for _, k := range []string{"engName", "chineseName", "name", "isOwner", "orgName", "memberCount"} {
		_, present := got[k]
		assert.False(t, present, "display field %q should be omitted when zero", k)
	}
}

func TestRoomMemberEntry_DisplayFields_NotPersistedToBSON(t *testing.T) {
	entry := model.RoomMemberEntry{
		ID: "org-1", Type: model.RoomMemberOrg,
		OrgName: "Engineering", MemberCount: 42,
	}
	data, err := bson.Marshal(&entry)
	require.NoError(t, err)

	var got bson.M
	require.NoError(t, bson.Unmarshal(data, &got))
	assert.Equal(t, "org-1", got["id"])
	assert.Equal(t, "org", got["type"])
	for _, k := range []string{"engName", "chineseName", "name", "isOwner", "orgName", "memberCount"} {
		_, present := got[k]
		assert.False(t, present, "display field %q must not be persisted to BSON", k)
	}
}

func TestRoomMemberEntry_BotName_RoundTrip(t *testing.T) {
	t.Run("bot member name round-trips via JSON", func(t *testing.T) {
		entry := model.RoomMemberEntry{
			ID:      "u-bot",
			Type:    model.RoomMemberIndividual,
			Account: "weather.bot",
			Name:    "Weather App",
		}
		data, err := json.Marshal(&entry)
		require.NoError(t, err)

		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		assert.Equal(t, "Weather App", raw["name"])
		// Human display fields must be absent for a bot entry.
		_, hasEngName := raw["engName"]
		assert.False(t, hasEngName, "engName must be absent for bot entry")
		_, hasChineseName := raw["chineseName"]
		assert.False(t, hasChineseName, "chineseName must be absent for bot entry")

		var dst model.RoomMemberEntry
		require.NoError(t, json.Unmarshal(data, &dst))
		assert.Equal(t, entry, dst)
	})

	t.Run("name not persisted to BSON", func(t *testing.T) {
		entry := model.RoomMemberEntry{
			ID:      "u-bot",
			Type:    model.RoomMemberIndividual,
			Account: "weather.bot",
			Name:    "Weather App",
		}
		data, err := bson.Marshal(&entry)
		require.NoError(t, err)
		var got bson.M
		require.NoError(t, bson.Unmarshal(data, &got))
		_, hasName := got["name"]
		assert.False(t, hasName, "name must not be persisted to BSON")
	})

	t.Run("orgName round-trips via JSON", func(t *testing.T) {
		entry := model.RoomMemberEntry{
			ID:      "sect-eng",
			Type:    model.RoomMemberOrg,
			OrgName: "Engineering",
		}
		data, err := json.Marshal(&entry)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		assert.Equal(t, "Engineering", raw["orgName"])
		_, hasSectName := raw["sectName"]
		assert.False(t, hasSectName, "sectName key must not appear (renamed to orgName)")

		var dst model.RoomMemberEntry
		require.NoError(t, json.Unmarshal(data, &dst))
		assert.Equal(t, entry, dst)
	})
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
			Query:   "hello",
			RoomIDs: []string{"r1", "r2"},
			Size:    50,
			Offset:  25,
		}
		data, err := json.Marshal(&req)
		require.NoError(t, err)
		var dst model.SearchMessagesRequest
		require.NoError(t, json.Unmarshal(data, &dst))
		assert.True(t, reflect.DeepEqual(req, dst))
	})

	t.Run("global (roomIds omitted when nil)", func(t *testing.T) {
		req := model.SearchMessagesRequest{Query: "hello"}
		data, err := json.Marshal(&req)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["roomIds"]
		assert.False(t, present, "roomIds must be omitted when nil")
	})
}

func TestSearchMessagesResponseJSON(t *testing.T) {
	t.Run("non-empty", func(t *testing.T) {
		resp := model.SearchMessagesResponse{
			Messages: []model.SearchMessage{
				{
					MessageID:   "m1",
					RoomID:      "r1",
					Content:     "hello",
					UserAccount: "alice",
					CreatedAt:   time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC),
				},
			},
			Total: 1,
		}
		roundTrip(t, &resp, &model.SearchMessagesResponse{})
	})

	t.Run("empty messages marshals as []", func(t *testing.T) {
		resp := model.SearchMessagesResponse{Messages: []model.SearchMessage{}, Total: 0}
		data, err := json.Marshal(&resp)
		require.NoError(t, err)
		assert.Contains(t, string(data), `"messages":[]`, "empty slice must marshal as [] not null")
	})

	t.Run("json keys", func(t *testing.T) {
		resp := model.SearchMessagesResponse{
			Messages: []model.SearchMessage{{MessageID: "m1", RoomID: "r1"}},
			Total:    5,
		}
		data, err := json.Marshal(&resp)
		require.NoError(t, err)
		assert.Contains(t, string(data), `"messages"`, "field must be 'messages' not 'results'")
		assert.Contains(t, string(data), `"total":5`)
		assert.NotContains(t, string(data), `"results"`, "'results' must not appear in new response shape")
	})
}

func TestSearchMessageJSON(t *testing.T) {
	t.Run("full", func(t *testing.T) {
		edited := time.Date(2026, 4, 1, 12, 5, 0, 0, time.UTC)
		updated := time.Date(2026, 4, 1, 12, 6, 0, 0, time.UTC)
		msg := model.SearchMessage{
			MessageID:             "m1",
			RoomID:                "r1",
			SiteID:                "site-a",
			Content:               "hello world (edited)",
			UserAccount:           "alice",
			CreatedAt:             time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC),
			EditedAt:              &edited,
			UpdatedAt:             &updated,
			ThreadParentMessageID: "p1",
		}
		roundTrip(t, &msg, &model.SearchMessage{})
	})

	t.Run("optional fields omitted when zero", func(t *testing.T) {
		msg := model.SearchMessage{MessageID: "m1", RoomID: "r1", Content: "hi"}
		data, err := json.Marshal(&msg)
		require.NoError(t, err)
		got := string(data)
		assert.NotContains(t, got, `"threadParentMessageId":""`,
			"empty thread parent must be omitted")
		assert.NotContains(t, got, `"editedAt"`, "nil EditedAt must be omitted")
		assert.NotContains(t, got, `"updatedAt"`, "nil UpdatedAt must be omitted")
	})
}

func TestSearchMessagesRequestJSON_WithRoomIDs(t *testing.T) {
	req := model.SearchMessagesRequest{
		Query:   "hello",
		RoomIDs: []string{"r1", "r2"},
		Size:    10,
		Offset:  0,
	}
	roundTrip(t, &req, &model.SearchMessagesRequest{})

	// Verify omitempty for nil RoomIDs
	reqNoRooms := model.SearchMessagesRequest{Query: "hello"}
	data, err := json.Marshal(&reqNoRooms)
	require.NoError(t, err)
	assert.NotContains(t, string(data), `"roomIds"`,
		"nil RoomIDs must be omitted via omitempty")
}

func TestSearchRoomsRequestJSON(t *testing.T) {
	t.Run("full", func(t *testing.T) {
		req := model.SearchRoomsRequest{
			Query:    "engineering",
			RoomType: "channel",
			Size:     25,
			Offset:   5,
		}
		roundTrip(t, &req, &model.SearchRoomsRequest{})
	})

	t.Run("roomType omitted when empty", func(t *testing.T) {
		req := model.SearchRoomsRequest{Query: "x"}
		data, err := json.Marshal(&req)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["roomType"]
		assert.False(t, present, "roomType must be omitted when empty")
	})

	t.Run("size and offset omitted when zero", func(t *testing.T) {
		req := model.SearchRoomsRequest{Query: "x"}
		data, err := json.Marshal(&req)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, hasSize := raw["size"]
		_, hasOffset := raw["offset"]
		assert.False(t, hasSize, "size must be omitted when zero")
		assert.False(t, hasOffset, "offset must be omitted when zero")
	})
}

func TestSearchRoomsResponseJSON(t *testing.T) {
	resp := model.SearchRoomsResponse{
		Rooms: []model.SearchRoom{
			{RoomID: "r1", Name: "engineering-announcements", RoomType: "channel", SiteID: "site-a"},
			{RoomID: "r2", Name: "alice-bob", RoomType: "dm", SiteID: "site-b"},
		},
	}
	roundTrip(t, &resp, &model.SearchRoomsResponse{})
}

func TestSearchRoomsResponseJSON_EmptyRooms(t *testing.T) {
	resp := model.SearchRoomsResponse{Rooms: []model.SearchRoom{}}
	data, err := json.Marshal(&resp)
	require.NoError(t, err)
	assert.Equal(t, `{"rooms":[]}`, string(data), "empty slice must marshal as [] not null")
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

func TestSendMessageRequest_QuotedParentMessageID_JSON(t *testing.T) {
	t.Run("with quotedParentMessageId", func(t *testing.T) {
		r := model.SendMessageRequest{
			ID:                    "msg-uuid-1",
			Content:               "great point!",
			RequestID:             "req-1",
			QuotedParentMessageID: "parent-msg-uuid",
		}
		data, err := json.Marshal(&r)
		require.NoError(t, err)

		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		assert.Equal(t, "parent-msg-uuid", raw["quotedParentMessageId"])

		var dst model.SendMessageRequest
		require.NoError(t, json.Unmarshal(data, &dst))
		assert.Equal(t, "parent-msg-uuid", dst.QuotedParentMessageID)
	})

	t.Run("quotedParentMessageId omitted when empty", func(t *testing.T) {
		r := model.SendMessageRequest{
			ID:        "msg-uuid-1",
			Content:   "hello",
			RequestID: "req-1",
		}
		data, err := json.Marshal(&r)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["quotedParentMessageId"]
		assert.False(t, present, "quotedParentMessageId should be omitted when empty")
	})
}

func TestMessage_QuotedParentMessage_JSON(t *testing.T) {
	parentTS := time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)
	threadParentTS := time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC)
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	t.Run("populated snapshot round-trips", func(t *testing.T) {
		m := model.Message{
			ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			Content:   "great point!",
			CreatedAt: now,
			QuotedParentMessage: &cassandra.QuotedParentMessage{
				MessageID: "parent-msg-uuid",
				RoomID:    "r1",
				Sender: cassandra.Participant{
					ID:      "u-bob",
					EngName: "Bob Chen",
					Account: "bob",
				},
				CreatedAt:             parentTS,
				Msg:                   "the original message",
				Mentions:              []cassandra.Participant{{ID: "u-carol", Account: "carol", EngName: "Carol Lee"}},
				MessageLink:           "http://localhost:3000/r1/parent-msg-uuid",
				ThreadParentID:        "thread-parent-uuid",
				ThreadParentCreatedAt: &threadParentTS,
			},
		}
		data, err := json.Marshal(&m)
		require.NoError(t, err)

		var dst model.Message
		require.NoError(t, json.Unmarshal(data, &dst))
		require.NotNil(t, dst.QuotedParentMessage)
		assert.Equal(t, "parent-msg-uuid", dst.QuotedParentMessage.MessageID)
		assert.Equal(t, "r1", dst.QuotedParentMessage.RoomID)
		assert.Equal(t, "the original message", dst.QuotedParentMessage.Msg)
		assert.Equal(t, "bob", dst.QuotedParentMessage.Sender.Account)
		assert.Equal(t, "Bob Chen", dst.QuotedParentMessage.Sender.EngName)
		assert.Equal(t, parentTS, dst.QuotedParentMessage.CreatedAt.UTC())
		assert.Equal(t, "http://localhost:3000/r1/parent-msg-uuid", dst.QuotedParentMessage.MessageLink)
		require.Len(t, dst.QuotedParentMessage.Mentions, 1)
		assert.Equal(t, "carol", dst.QuotedParentMessage.Mentions[0].Account)
		assert.Equal(t, "thread-parent-uuid", dst.QuotedParentMessage.ThreadParentID)
		require.NotNil(t, dst.QuotedParentMessage.ThreadParentCreatedAt)
		assert.Equal(t, threadParentTS, dst.QuotedParentMessage.ThreadParentCreatedAt.UTC())
	})

	t.Run("quotedParentMessage omitted when nil", func(t *testing.T) {
		m := model.Message{
			ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			Content:   "hello",
			CreatedAt: now,
		}
		data, err := json.Marshal(&m)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["quotedParentMessage"]
		assert.False(t, present, "quotedParentMessage should be omitted when nil")
	})
}

func TestSubscriptionNewFields(t *testing.T) {
	t.Run("channel sub round-trips with empty IsSubscribed", func(t *testing.T) {
		sub := model.Subscription{
			ID:       "s1",
			User:     model.SubscriptionUser{ID: "u1", Account: "alice"},
			RoomID:   "r1",
			SiteID:   "site-A",
			Roles:    []model.Role{model.RoleOwner},
			Name:     "deal team",
			RoomType: model.RoomTypeChannel,
			JoinedAt: time.Date(2026, 4, 28, 0, 0, 0, 0, time.UTC),
		}
		raw, err := json.Marshal(sub)
		require.NoError(t, err)
		var dst model.Subscription
		require.NoError(t, json.Unmarshal(raw, &dst))
		assert.Equal(t, "deal team", dst.Name)
		assert.Equal(t, model.RoomTypeChannel, dst.RoomType)
		assert.False(t, dst.IsSubscribed)
		assert.NotContains(t, string(raw), "isSubscribed")
	})
	t.Run("botDM human sub round-trips", func(t *testing.T) {
		sub := model.Subscription{
			ID:           "s2",
			User:         model.SubscriptionUser{ID: "u1", Account: "alice"},
			RoomID:       "r2",
			SiteID:       "site-A",
			Name:         "weather.bot",
			RoomType:     model.RoomTypeBotDM,
			IsSubscribed: true,
			JoinedAt:     time.Date(2026, 4, 28, 0, 0, 0, 0, time.UTC),
		}
		raw, err := json.Marshal(sub)
		require.NoError(t, err)
		var dst model.Subscription
		require.NoError(t, json.Unmarshal(raw, &dst))
		assert.Equal(t, "weather.bot", dst.Name)
		assert.Equal(t, model.RoomTypeBotDM, dst.RoomType)
		assert.True(t, dst.IsSubscribed)
		assert.Contains(t, string(raw), `"isSubscribed":true`)
	})
}

func TestSubscriptionJSON_NoSidebarName(t *testing.T) {
	s := model.Subscription{
		ID:       "s1",
		User:     model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID:   "r1",
		RoomType: model.RoomTypeChannel,
		Name:     "deal team",
	}
	data, err := json.Marshal(&s)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	_, hasSidebar := raw["sidebarName"]
	assert.False(t, hasSidebar, "sidebarName must not appear in Subscription JSON")
}

func TestCreateRoomRequestRoundtrip(t *testing.T) {
	req := model.CreateRoomRequest{
		Name:             "team",
		Users:            []string{"bob", "carol"},
		Orgs:             []string{"org-fx"},
		Channels:         []model.ChannelRef{{RoomID: "r0", SiteID: "site-A"}},
		RoomID:           "r_xyz",
		RequesterID:      "u_alice",
		RequesterAccount: "alice",
		Timestamp:        1740000000000,
	}
	var dst model.CreateRoomRequest
	roundTrip(t, &req, &dst)
	assert.Equal(t, "team", dst.Name)
	assert.Equal(t, []string{"bob", "carol"}, dst.Users)
	assert.Equal(t, "r_xyz", dst.RoomID)
	assert.Equal(t, "u_alice", dst.RequesterID)
	assert.Equal(t, int64(1740000000000), dst.Timestamp)
}

// TestErrorResponseRoomIDOmitempty was removed: model.ErrorResponse was deleted
// alongside the rest of the legacy error machinery (see pkg/errcode for the
// canonical client-facing error type). The DM-exists path now returns a success
// reply (model.CreateRoomReply{Status: CreateRoomStatusExists, RoomID}).

func TestAsyncJobResultShape(t *testing.T) {
	r := model.AsyncJobResult{
		RequestID: "req-1",
		Operation: model.AsyncJobOpRoomCreate,
		Status:    "ok",
		RoomID:    "r1",
		Timestamp: 1,
	}
	data, err := json.Marshal(&r)
	require.NoError(t, err)
	var dst model.AsyncJobResult
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Equal(t, "ok", dst.Status)
	assert.Equal(t, model.AsyncJobOpRoomCreate, dst.Operation)
	assert.Equal(t, "r1", dst.RoomID)
	assert.NotContains(t, string(data), `"job"`)
	assert.NotContains(t, string(data), `"success"`)

	// Success case must omit the error-only fields.
	assert.NotContains(t, string(data), `"code"`)
	assert.NotContains(t, string(data), `"reason"`)

	r2 := model.AsyncJobResult{
		Operation: model.AsyncJobOpRoomMemberAdd,
		Status:    "error",
		Error:     "not subscribed",
		Code:      "forbidden",
		Reason:    "not_subscribed",
	}
	raw2, err := json.Marshal(r2)
	require.NoError(t, err)
	assert.NotContains(t, string(raw2), `"roomId"`)
	var dst2 model.AsyncJobResult
	require.NoError(t, json.Unmarshal(raw2, &dst2))
	assert.Equal(t, "forbidden", dst2.Code)
	assert.Equal(t, "not_subscribed", dst2.Reason)
}

func TestAsyncJobResultOpConstants(t *testing.T) {
	assert.Equal(t, "room.create", model.AsyncJobOpRoomCreate)
	assert.Equal(t, "room.member.add", model.AsyncJobOpRoomMemberAdd)
	assert.Equal(t, "room.member.remove", model.AsyncJobOpRoomMemberRemove)
	assert.Equal(t, "room.member.remove_org", model.AsyncJobOpRoomMemberRemoveOrg)
	assert.Equal(t, "room.member.role_update", model.AsyncJobOpRoomMemberRoleUpdate)
}

func TestAddMembersRequestNoRequestIDField(t *testing.T) {
	body, err := json.Marshal(model.AddMembersRequest{
		RoomID:           "r1",
		Users:            []string{"bob"},
		RequesterID:      "u_alice",
		RequesterAccount: "alice",
		Timestamp:        1,
	})
	require.NoError(t, err)
	assert.NotContains(t, string(body), "requestId")
}

// TestErrorResponseJSON was removed alongside model.ErrorResponse. The wire
// envelope is now owned by pkg/errcode (see pkg/errcode/error_test.go).

func TestReadReceiptRequestJSON(t *testing.T) {
	r := model.ReadReceiptRequest{MessageID: "m1"}
	roundTrip(t, &r, &model.ReadReceiptRequest{})
}

func TestReadReceiptEntryJSON(t *testing.T) {
	e := model.ReadReceiptEntry{
		UserID:      "01970a4f8c2d7c9a01970a4f8c2d7c9a",
		Account:     "alice",
		ChineseName: "愛麗絲",
		EngName:     "Alice",
	}
	roundTrip(t, &e, &model.ReadReceiptEntry{})
}

func TestReadReceiptResponseJSON(t *testing.T) {
	r := model.ReadReceiptResponse{
		Readers: []model.ReadReceiptEntry{
			{UserID: "u1", Account: "alice", ChineseName: "愛麗絲", EngName: "Alice"},
		},
	}
	roundTrip(t, &r, &model.ReadReceiptResponse{})
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

func TestSubscriptionReadEventJSON(t *testing.T) {
	src := model.SubscriptionReadEvent{
		Account:    "alice",
		RoomID:     "r1",
		LastSeenAt: 1735689600000,
		Alert:      true,
		Timestamp:  1735689600001,
	}
	data, err := json.Marshal(&src)
	require.NoError(t, err)
	var dst model.SubscriptionReadEvent
	require.NoError(t, json.Unmarshal(data, &dst))
	if !reflect.DeepEqual(src, dst) {
		t.Errorf("round-trip mismatch:\n  got  %+v\n  want %+v", dst, src)
	}
}

func TestOutboxSubscriptionReadConstant(t *testing.T) {
	if model.OutboxSubscriptionRead != "subscription_read" {
		t.Errorf("got %q, want %q", model.OutboxSubscriptionRead, "subscription_read")
	}
}

func TestAppRoundtrip(t *testing.T) {
	a := model.App{
		ID:          "app1",
		Name:        "Weather Bot",
		Description: "Forecasts and alerts",
		Assistant: &model.AppAssistant{
			Enabled:     true,
			Name:        "weather.bot",
			SettingsURL: "https://example.com/weather/settings",
		},
		Sponsors: []model.AppSponsor{
			{Name: "Alice", Phone: "555-0100"},
		},
	}
	var dst model.App
	roundTrip(t, &a, &dst)
	require.NotNil(t, dst.Assistant)
	assert.True(t, dst.Assistant.Enabled)
	assert.Equal(t, "weather.bot", dst.Assistant.Name)
	assert.Equal(t, "Weather Bot", dst.Name)
	assert.Len(t, dst.Sponsors, 1)
	assert.Equal(t, "Alice", dst.Sponsors[0].Name)
}

func TestAppAssistantDisabledRoundtrip(t *testing.T) {
	a := model.App{
		ID:        "app2",
		Name:      "Disabled Bot",
		Assistant: &model.AppAssistant{Enabled: false, Name: "disabled.bot"},
	}
	var dst model.App
	roundTrip(t, &a, &dst)
	require.NotNil(t, dst.Assistant)
	assert.False(t, dst.Assistant.Enabled)
}

func TestAppRoundtrip_WithChannelTabAndAvatar(t *testing.T) {
	a := model.App{
		ID:        "app1",
		Name:      "Calendar",
		AvatarURL: "https://cdn.example.com/avatars/calendar.png",
		Assistant: &model.AppAssistant{Enabled: true, Name: "calendar.bot"},
		ChannelTab: &model.AppChannelTab{
			Enabled: true,
			Default: true,
			Name:    "Calendar",
			URL: model.AppChannelTabURL{
				Default: "https://upstream.example.com/calendar/${roomId}/${siteId}/index",
			},
		},
	}
	var dst model.App
	roundTrip(t, &a, &dst)
	require.NotNil(t, dst.ChannelTab)
	assert.True(t, dst.ChannelTab.Enabled)
	assert.True(t, dst.ChannelTab.Default)
	assert.Equal(t, "Calendar", dst.ChannelTab.Name)
	assert.Equal(t, "https://upstream.example.com/calendar/${roomId}/${siteId}/index",
		dst.ChannelTab.URL.Default)
	assert.Equal(t, "https://cdn.example.com/avatars/calendar.png", dst.AvatarURL)
}

func TestAppChannelTabRoundtrip(t *testing.T) {
	tab := model.AppChannelTab{
		Enabled: true,
		Default: false,
		Name:    "Notes",
		URL:     model.AppChannelTabURL{Default: "https://upstream/notes"},
	}
	var dst model.AppChannelTab
	roundTrip(t, &tab, &dst)
}

func TestBotCmdMenuRoundtrip(t *testing.T) {
	m := model.BotCmdMenu{
		ID:           "bcm1",
		Name:         "weather.bot",
		ActiveStatus: true,
		CmdBlocks: []model.CmdBlock{
			{Text: "/weather", ActionType: "command", Payload: "weather"},
		},
	}
	var dst model.BotCmdMenu
	roundTrip(t, &m, &dst)
	require.Len(t, dst.CmdBlocks, 1)
	assert.Equal(t, "/weather", dst.CmdBlocks[0].Text)
}

func TestBotCmdMenuRoundtrip_Inactive(t *testing.T) {
	m := model.BotCmdMenu{
		ID:           "bcm2",
		Name:         "weather.bot",
		ActiveStatus: false,
	}
	var dst model.BotCmdMenu
	roundTrip(t, &m, &dst)
	assert.False(t, dst.ActiveStatus)
	assert.Nil(t, dst.CmdBlocks)
}

func TestCmdBlockRoundtrip_Recursive(t *testing.T) {
	block := model.CmdBlock{
		Text:        "menu",
		ActionType:  "open",
		Description: "open the menu",
		Modal: &model.CmdModal{
			Command: "menu.open",
			Param:   "weather",
		},
		Blocks: []model.CmdBlock{
			{Text: "today", Payload: "today"},
			{Text: "tomorrow", Payload: "tomorrow", Blocks: []model.CmdBlock{
				{Text: "morning", Payload: "tomorrow.am"},
			}},
		},
	}
	var dst model.CmdBlock
	roundTrip(t, &block, &dst)
	require.NotNil(t, dst.Modal)
	assert.Equal(t, "menu.open", dst.Modal.Command)
	assert.Equal(t, "weather", dst.Modal.Param)
	require.Len(t, dst.Blocks, 2)
	require.Len(t, dst.Blocks[1].Blocks, 1)
	assert.Equal(t, "morning", dst.Blocks[1].Blocks[0].Text)
}

func TestGetRoomAppTabsResponseRoundtrip(t *testing.T) {
	src := model.GetRoomAppTabsResponse{
		Apps: []model.RoomApp{
			{
				ID:        "app1",
				Name:      "Calendar",
				TabURL:    "https://chat.example.com/calendar/r1/site-a/index",
				Assistant: &model.AppAssistant{Enabled: true, Name: "cal.bot"},
				AvatarURL: "https://cdn/cal.png",
			},
		},
	}
	var dst model.GetRoomAppTabsResponse
	roundTrip(t, &src, &dst)
	require.Len(t, dst.Apps, 1)
	assert.Equal(t, "Calendar", dst.Apps[0].Name)
}

func TestGetRoomAppTabsResponse_EmptyIsArrayNotNull(t *testing.T) {
	src := model.GetRoomAppTabsResponse{Apps: []model.RoomApp{}}
	data, err := json.Marshal(&src)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"apps":[]`)
	assert.NotContains(t, string(data), `"apps":null`)
}

func TestGetRoomAppCommandMenuResponseRoundtrip(t *testing.T) {
	src := model.GetRoomAppCommandMenuResponse{
		AppAssistants: []model.RoomAppAssistant{
			{
				AppName: "Weather Bot",
				Name:    "weather.bot",
				CmdBlocks: []model.CmdBlock{
					{Text: "/forecast", Payload: "forecast"},
				},
			},
		},
	}
	var dst model.GetRoomAppCommandMenuResponse
	roundTrip(t, &src, &dst)
	require.Len(t, dst.AppAssistants, 1)
	assert.Equal(t, "weather.bot", dst.AppAssistants[0].Name)
}

func TestGetRoomAppCommandMenuResponse_EmptyIsArrayNotNull(t *testing.T) {
	src := model.GetRoomAppCommandMenuResponse{
		AppAssistants: []model.RoomAppAssistant{},
	}
	data, err := json.Marshal(&src)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"appAssistants":[]`)
	assert.NotContains(t, string(data), `"appAssistants":null`)
}

func TestCreateRoomReplyRoundtrip(t *testing.T) {
	r := model.CreateRoomReply{
		Status:   model.CreateRoomReplyAccepted,
		RoomID:   "r-abc123",
		RoomType: string(model.RoomTypeChannel),
	}
	var dst model.CreateRoomReply
	roundTrip(t, &r, &dst)
	assert.Equal(t, model.CreateRoomReplyAccepted, dst.Status)
	assert.Equal(t, "r-abc123", dst.RoomID)
	assert.Equal(t, string(model.RoomTypeChannel), dst.RoomType)
}

func TestCreateRoomReplyAcceptedConstant(t *testing.T) {
	assert.Equal(t, "accepted", model.CreateRoomReplyAccepted)
}

func TestCreateRoomReplyBotDMRoundtrip(t *testing.T) {
	r := model.CreateRoomReply{
		Status:   model.CreateRoomReplyAccepted,
		RoomID:   "dm-r1",
		RoomType: string(model.RoomTypeBotDM),
	}
	data, err := json.Marshal(&r)
	require.NoError(t, err)
	var dst model.CreateRoomReply
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Equal(t, string(model.RoomTypeBotDM), dst.RoomType)
	assert.Contains(t, string(data), `"roomType":"botDM"`)
}

func TestRoomCreatedRoundtrip(t *testing.T) {
	rc := model.RoomCreated{
		Name:  "deal team",
		Users: []string{"alice", "bob"},
		Orgs:  []string{"org-fx"},
		Channels: []model.ChannelRef{
			{RoomID: "r-src", SiteID: "site-b"},
		},
		AddedUsersCount: 3,
	}
	var dst model.RoomCreated
	roundTrip(t, &rc, &dst)
	assert.Equal(t, "deal team", dst.Name)
	assert.Equal(t, []string{"alice", "bob"}, dst.Users)
	assert.Equal(t, []string{"org-fx"}, dst.Orgs)
	assert.Equal(t, []model.ChannelRef{{RoomID: "r-src", SiteID: "site-b"}}, dst.Channels)
	assert.Equal(t, 3, dst.AddedUsersCount)
}

func TestMessageTypeAndAsyncJobStatusConstants(t *testing.T) {
	assert.Equal(t, "room_created", model.MessageTypeRoomCreated)
	assert.Equal(t, "members_added", model.MessageTypeMembersAdded)
	assert.Equal(t, "ok", model.AsyncJobStatusOK)
	assert.Equal(t, "error", model.AsyncJobStatusError)
}

func TestMuteToggleResponseJSON(t *testing.T) {
	src := model.MuteToggleResponse{
		Status: "ok",
		Muted:  true,
	}
	data, err := json.Marshal(src)
	require.NoError(t, err)

	var dst model.MuteToggleResponse
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Equal(t, src, dst)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	assert.Equal(t, "ok", raw["status"])
	assert.Equal(t, true, raw["muted"])
}

func TestSubscriptionMuteToggledEventJSON(t *testing.T) {
	src := model.SubscriptionMuteToggledEvent{
		Account:   "alice",
		RoomID:    "r1",
		Muted:     true,
		Timestamp: 1234567890,
	}
	data, err := json.Marshal(src)
	require.NoError(t, err)

	var dst model.SubscriptionMuteToggledEvent
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Equal(t, src, dst)
}

func TestOutboxSubscriptionMuteToggledConst(t *testing.T) {
	assert.Equal(t, model.OutboxEventType("subscription_mute_toggled"), model.OutboxSubscriptionMuteToggled)
}

func TestFavoriteToggleResponseJSON(t *testing.T) {
	src := model.FavoriteToggleResponse{
		Status:   "ok",
		Favorite: true,
	}
	data, err := json.Marshal(src)
	require.NoError(t, err)

	var dst model.FavoriteToggleResponse
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Equal(t, src, dst)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	assert.Equal(t, "ok", raw["status"])
	assert.Equal(t, true, raw["favorite"])
}

func TestSubscriptionFavoriteToggledEventJSON(t *testing.T) {
	src := model.SubscriptionFavoriteToggledEvent{
		Account:   "alice",
		RoomID:    "r1",
		Favorite:  true,
		Timestamp: 1234567890,
	}
	data, err := json.Marshal(src)
	require.NoError(t, err)

	var dst model.SubscriptionFavoriteToggledEvent
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Equal(t, src, dst)
}

func TestOutboxSubscriptionFavoriteToggledConst(t *testing.T) {
	assert.Equal(t, model.OutboxEventType("subscription_favorite_toggled"), model.OutboxSubscriptionFavoriteToggled)
}

func TestSyncCreateDMRequestJSON(t *testing.T) {
	src := model.SyncCreateDMRequest{
		RoomType:         model.RoomTypeDM,
		RequesterAccount: "alice",
		OtherAccount:     "bob",
	}
	b, err := json.Marshal(src)
	require.NoError(t, err)

	assert.JSONEq(t, `{"roomType":"dm","requesterAccount":"alice","otherAccount":"bob"}`, string(b))

	var dst model.SyncCreateDMRequest
	require.NoError(t, json.Unmarshal(b, &dst))
	assert.Equal(t, src, dst)
}

func TestSyncCreateDMReplyJSON(t *testing.T) {
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	src := model.SyncCreateDMReply{
		Success: true,
		Subscription: model.Subscription{
			ID:           "sub1",
			User:         model.SubscriptionUser{ID: "u1", Account: "alice"},
			RoomID:       "room1",
			SiteID:       "site-a",
			Name:         "bob",
			RoomType:     model.RoomTypeDM,
			IsSubscribed: true,
			JoinedAt:     now,
		},
	}
	b, err := json.Marshal(src)
	require.NoError(t, err)

	var dst model.SyncCreateDMReply
	require.NoError(t, json.Unmarshal(b, &dst))
	assert.True(t, dst.Success)
	assert.Equal(t, src.Subscription.ID, dst.Subscription.ID)
	assert.Equal(t, src.Subscription.User, dst.Subscription.User)
	assert.Equal(t, src.Subscription.RoomID, dst.Subscription.RoomID)
}

func TestSearchAppsRequestJSON(t *testing.T) {
	enabled := true
	r := model.SearchAppsRequest{
		Query:            "weather",
		AssistantEnabled: &enabled,
		Size:             50,
		Offset:           0,
	}
	roundTrip(t, &r, &model.SearchAppsRequest{})
}

func TestSearchAppsRequestJSON_OmitOptional(t *testing.T) {
	r := model.SearchAppsRequest{Query: "weather"}
	roundTrip(t, &r, &model.SearchAppsRequest{})

	data, err := json.Marshal(&r)
	require.NoError(t, err)
	got := string(data)
	assert.NotContains(t, got, "assistantEnabled", "nil AssistantEnabled must be omitted")
	assert.NotContains(t, got, `"size":0`, "zero Size must be omitted")
	assert.NotContains(t, got, `"offset":0`, "zero Offset must be omitted")
}

func TestSearchAppsResponseJSON(t *testing.T) {
	r := model.SearchAppsResponse{
		Apps: []model.App{
			{ID: "a1", Name: "Weather"},
			{ID: "a2", Name: "Calendar"},
		},
	}
	roundTrip(t, &r, &model.SearchAppsResponse{})
}

func TestSearchAppsResponseJSON_EmptyApps(t *testing.T) {
	r := model.SearchAppsResponse{Apps: []model.App{}}
	data, err := json.Marshal(&r)
	require.NoError(t, err)
	assert.Equal(t, `{"apps":[]}`, string(data), "empty slice must marshal as [] not null")
}

func TestSearchUsersRequestJSON(t *testing.T) {
	r := model.SearchUsersRequest{Query: "alice"}
	roundTrip(t, &r, &model.SearchUsersRequest{})
}

func TestSearchUsersRequestJSON_OmitEmpty(t *testing.T) {
	r := model.SearchUsersRequest{Query: ""}
	data, err := json.Marshal(&r)
	require.NoError(t, err)
	// query="" is still marshalled (not omitempty) — the handler validates
	// after trim, not the model layer.
	assert.Contains(t, string(data), `"query"`)
}

func TestSearchUserJSON(t *testing.T) {
	u := model.SearchUser{
		Account:     "alice",
		EngName:     "Alice Wang",
		ChineseName: "愛麗絲王",
	}
	roundTrip(t, &u, &model.SearchUser{})
}

func TestSearchUserJSON_OmitEmpty(t *testing.T) {
	u := model.SearchUser{Account: "alice"}
	data, err := json.Marshal(&u)
	require.NoError(t, err)
	got := string(data)
	assert.NotContains(t, got, "engName", "empty EngName must be omitted")
	assert.NotContains(t, got, "chineseName", "empty ChineseName must be omitted")
}

func TestEditRoomEventJSON(t *testing.T) {
	editedAt := time.Date(2026, 5, 14, 12, 5, 0, 0, time.UTC)
	updatedAt := time.Date(2026, 5, 14, 12, 5, 0, 0, time.UTC)
	evt := model.EditRoomEvent{
		Type:       model.RoomEventMessageEdited,
		RoomID:     "r1",
		SiteID:     "site-a",
		Timestamp:  1746518700123,
		MessageID:  "msg-uuid",
		NewContent: "hello (edited)",
		EditedBy:   "alice",
		EditedAt:   editedAt,
		UpdatedAt:  updatedAt,
	}
	roundTrip(t, &evt, &model.EditRoomEvent{})
}

func TestDeleteRoomEventJSON(t *testing.T) {
	deletedAt := time.Date(2026, 5, 14, 12, 10, 0, 0, time.UTC)
	evt := model.DeleteRoomEvent{
		Type:      model.RoomEventMessageDeleted,
		RoomID:    "r1",
		SiteID:    "site-a",
		Timestamp: 1746518800123,
		MessageID: "msg-uuid",
		DeletedBy: "alice",
		DeletedAt: deletedAt,
		UpdatedAt: deletedAt,
	}
	roundTrip(t, &evt, &model.DeleteRoomEvent{})
}

func TestMessageThreadReadRequestJSON(t *testing.T) {
	src := model.MessageThreadReadRequest{ThreadID: "01970a4f8c2d7c9aQRST"}
	roundTrip(t, &src, &model.MessageThreadReadRequest{})
}

func TestThreadReadEventJSON(t *testing.T) {
	src := model.ThreadReadEvent{
		Account:         "alice",
		RoomID:          "r1",
		ThreadRoomID:    "tr1",
		ParentMessageID: "01970a4f8c2d7c9aQRST",
		NewThreadUnread: []string{"t2", "t3"},
		Alert:           true,
		LastSeenAt:      1735689600000,
		Timestamp:       1735689600001,
	}
	roundTrip(t, &src, &model.ThreadReadEvent{})
}

func TestOutboxEventJSON_ThreadRead(t *testing.T) {
	payload := model.ThreadReadEvent{
		Account: "alice", RoomID: "r1", ThreadRoomID: "tr1",
		ParentMessageID: "p1", NewThreadUnread: []string{"t2"},
		Alert: false, LastSeenAt: 1735689600000, Timestamp: 1735689600001,
	}
	data, err := json.Marshal(&payload)
	require.NoError(t, err)
	src := model.OutboxEvent{
		Type:       model.OutboxThreadRead,
		SiteID:     "site-a",
		DestSiteID: "site-b",
		Payload:    data,
		Timestamp:  1735689600002,
	}
	out, err := json.Marshal(&src)
	require.NoError(t, err)
	var dst model.OutboxEvent
	require.NoError(t, json.Unmarshal(out, &dst))
	if !reflect.DeepEqual(src, dst) {
		t.Errorf("round-trip mismatch:\n  got  %+v\n  want %+v", dst, src)
	}
	if dst.Type != "thread_read" {
		t.Errorf("Type = %q, want thread_read", dst.Type)
	}
	var gotPayload model.ThreadReadEvent
	require.NoError(t, json.Unmarshal(dst.Payload, &gotPayload))
	assert.Equal(t, payload, gotPayload)
}

func TestSubscriptionRemovedEventJSON(t *testing.T) {
	evt := model.SubscriptionRemovedEvent{
		UserID: "01970a4f8c2d7c9a01970a4f8c2d7c9a",
		Subscription: model.RemovedSubscriptionRef{
			RoomID:   "r1",
			RoomType: model.RoomTypeChannel,
			U:        model.SubscriptionUser{ID: "01970a4f8c2d7c9a01970a4f8c2d7c9a", Account: "bob"},
		},
		Action:    "removed",
		Timestamp: 1746518483000,
	}
	roundTrip(t, &evt, &model.SubscriptionRemovedEvent{})
}

func TestSubscriptionRemovedEventOmitsZeroValueFields(t *testing.T) {
	evt := model.SubscriptionRemovedEvent{
		Subscription: model.RemovedSubscriptionRef{
			RoomID:   "r1",
			RoomType: model.RoomTypeChannel,
			U:        model.SubscriptionUser{Account: "bob"},
		},
		Action:    "removed",
		Timestamp: 1746518483000,
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &raw))
	sub := raw["subscription"]
	require.NotNil(t, sub)
	var subRaw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(sub, &subRaw))
	// The lean ref must NOT carry the full Subscription's fields.
	for _, leaked := range []string{"roles", "name", "joinedAt", "alert", "muted", "siteId", "id"} {
		_, present := subRaw[leaked]
		assert.False(t, present, "removed event subscription must not carry %q", leaked)
	}
}

func TestPinRoomEventJSON(t *testing.T) {
	pinnedAt := time.Date(2026, 5, 14, 12, 15, 0, 0, time.UTC)
	evt := model.PinRoomEvent{
		Type:      model.RoomEventMessagePinned,
		RoomID:    "r1",
		SiteID:    "site-a",
		Timestamp: pinnedAt.UnixMilli(),
		MessageID: "msg-uuid",
		PinnedBy:  &model.Participant{UserID: "u1", Account: "alice"},
		PinnedAt:  pinnedAt,
	}
	roundTrip(t, &evt, &model.PinRoomEvent{})
}

func TestUnpinRoomEventJSON(t *testing.T) {
	unpinnedAt := time.Date(2026, 5, 14, 12, 20, 0, 0, time.UTC)
	evt := model.UnpinRoomEvent{
		Type:       model.RoomEventMessageUnpinned,
		RoomID:     "r1",
		SiteID:     "site-a",
		Timestamp:  unpinnedAt.UnixMilli(),
		MessageID:  "msg-uuid",
		UnpinnedBy: &model.Participant{UserID: "u1", Account: "alice"},
		UnpinnedAt: unpinnedAt,
	}
	roundTrip(t, &evt, &model.UnpinRoomEvent{})
}

// --- User.Roles ---

func TestUserJSON_WithRoles(t *testing.T) {
	u := model.User{ID: "u1", Account: "admin1", SiteID: "site-a",
		Roles: []model.UserRole{model.UserRoleAdmin}}
	roundTrip(t, &u, &model.User{})
}

func TestUserRoleConstants(t *testing.T) {
	if model.UserRoleAdmin != "admin" || model.UserRoleUser != "user" {
		t.Fatalf("UserRole consts: admin=%q user=%q", model.UserRoleAdmin, model.UserRoleUser)
	}
}

// --- Room.ExternalAccess ---

func TestRoomJSON_RestrictedAndExternalAccess(t *testing.T) {
	r := model.Room{
		ID: "r1", Name: "x", Type: model.RoomTypeChannel, SiteID: "site-a", UserCount: 5,
		CreatedAt:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Restricted: true, ExternalAccess: true,
	}
	roundTrip(t, &r, &model.Room{})
}

func TestRoomJSON_ExternalAccessOmittedWhenFalse(t *testing.T) {
	r := model.Room{ID: "r1", Name: "x", Type: model.RoomTypeChannel, SiteID: "site-a",
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	data, err := json.Marshal(&r)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	_, has := raw["externalAccess"]
	assert.False(t, has, "false ExternalAccess must omit")
}

// --- Subscription.Restricted / ExternalAccess ---

func TestSubscriptionJSON_RestrictedAndExternalAccess(t *testing.T) {
	s := model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a",
		Roles: []model.Role{model.RoleMember}, Name: "x", RoomType: model.RoomTypeChannel,
		JoinedAt:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Restricted: true, ExternalAccess: true,
	}
	roundTrip(t, &s, &model.Subscription{})
}

// --- Request types ---

func TestRenameRoomRequestJSON(t *testing.T) {
	r := model.RenameRoomRequest{RoomID: "r1", NewName: "new", Account: "alice", Timestamp: 1700000000000}
	roundTrip(t, &r, &model.RenameRoomRequest{})
}

func TestRoomRestrictedRequestJSON(t *testing.T) {
	r := model.RoomRestrictedRequest{
		RoomID: "r1", Restricted: true, ExternalAccess: false,
		OwnerAccount: "alice", Account: "admin1", Timestamp: 1700000000000,
	}
	roundTrip(t, &r, &model.RoomRestrictedRequest{})
}

func TestRoomRestrictedRequest_OwnerOmittedWhenEmpty(t *testing.T) {
	r := model.RoomRestrictedRequest{RoomID: "r1", Account: "admin1", Timestamp: 1700000000000}
	data, err := json.Marshal(&r)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	_, has := raw["ownerAccount"]
	assert.False(t, has)
}

// --- Sys message + outbox payloads ---

func TestRoomRenamedSysDataJSON(t *testing.T) {
	d := model.RoomRenamedSysData{NewName: "renamed", ByAccount: "alice"}
	roundTrip(t, &d, &model.RoomRenamedSysData{})
}

func TestRoomRenamedRoomEventJSON(t *testing.T) {
	e := model.RoomRenamedRoomEvent{
		Type:      model.RoomEventRoomRenamed,
		RoomID:    "r1",
		SiteID:    "site-a",
		Timestamp: 1700000000000,
		NewName:   "engineering",
		ByAccount: "alice",
		RenamedAt: time.UnixMilli(1700000000000).UTC(),
	}
	roundTrip(t, &e, &model.RoomRenamedRoomEvent{})
}

func TestRoomRestrictedRoomEventJSON(t *testing.T) {
	e := model.RoomRestrictedRoomEvent{
		Type:           model.RoomEventRoomRestricted,
		RoomID:         "r1",
		SiteID:         "site-a",
		Timestamp:      1700000000000,
		Restricted:     true,
		ExternalAccess: false,
		OwnerAccount:   "alice",
		ByAccount:      "admin1",
		ChangedAt:      time.UnixMilli(1700000000000).UTC(),
	}
	roundTrip(t, &e, &model.RoomRestrictedRoomEvent{})
}

func TestRoomRestrictedSysDataJSON(t *testing.T) {
	d := model.RoomRestrictedSysData{
		Restricted: true, ExternalAccess: false, ByAccount: "admin1", OwnerAccount: "alice",
	}
	roundTrip(t, &d, &model.RoomRestrictedSysData{})
}

func TestRoomRenamedOutboxPayloadJSON(t *testing.T) {
	p := model.RoomRenamedOutboxPayload{RoomID: "r1", NewName: "x", Timestamp: 1700000000000}
	roundTrip(t, &p, &model.RoomRenamedOutboxPayload{})
}

func TestRoomRestrictedOutboxPayloadJSON(t *testing.T) {
	p := model.RoomRestrictedOutboxPayload{
		RoomID: "r1", Restricted: true, ExternalAccess: false,
		OwnerAccount: "alice", Timestamp: 1700000000000,
	}
	roundTrip(t, &p, &model.RoomRestrictedOutboxPayload{})
}

// --- Constants ---

func TestMessageAndOutboxAndAsyncOpConstants(t *testing.T) {
	assert.Equal(t, "room_renamed", model.MessageTypeRoomRenamed)
	assert.Equal(t, "room_restricted", model.MessageTypeRoomRestricted)
	assert.Equal(t, "room_renamed", model.OutboxRoomRenamed)
	assert.Equal(t, "room_restricted", model.OutboxRoomRestricted)
	assert.Equal(t, "room.rename", model.AsyncJobOpRoomRename)
	assert.Equal(t, model.RoomEventType("room_renamed"), model.RoomEventRoomRenamed)
	assert.Equal(t, model.RoomEventType("room_restricted"), model.RoomEventRoomRestricted)
}

func TestPushNotificationEvent_RoundTrip(t *testing.T) {
	in := model.PushNotificationEvent{
		ID:       "m1-b0",
		Accounts: []string{"alice", "bob"},
		Title:    "general",
		Body:     "hello",
		RoomID:   "r1",
		Data: model.PushNotificationData{
			RoomID:    "r1",
			MessageID: "m1",
			Type:      "c",
			Sender:    &model.Participant{Account: "bob", ChineseName: "張三", EngName: "Bob"},
			PushTime:  "2026-05-27T00:00:00Z",
		},
		Timestamp: 1700000000000,
	}
	data, err := json.Marshal(in)
	require.NoError(t, err)
	var out model.PushNotificationEvent
	require.NoError(t, json.Unmarshal(data, &out))
	assert.Equal(t, in, out)
}

func TestMessage_SenderDisplayName(t *testing.T) {
	tests := []struct {
		name string
		msg  model.Message
		want string
	}{
		{
			name: "uses UserDisplayName when populated",
			msg:  model.Message{UserAccount: "alice", UserDisplayName: "Alice Wang 愛麗絲"},
			want: "Alice Wang 愛麗絲",
		},
		{
			name: "falls back to UserAccount on legacy in-flight message",
			msg:  model.Message{UserAccount: "alice", UserDisplayName: ""},
			want: "alice",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.msg.SenderDisplayName())
		})
	}
}

func TestPresenceSnapshotReply_RoundTrip(t *testing.T) {
	in := model.PresenceSnapshotReply{
		Presences: map[string]model.Presence{
			"alice": {AggregatedStatus: "online"},
			"bob":   {AggregatedStatus: "busy"},
		},
	}
	data, err := json.Marshal(in)
	require.NoError(t, err)
	var out model.PresenceSnapshotReply
	require.NoError(t, json.Unmarshal(data, &out))
	assert.Equal(t, in, out)
}
