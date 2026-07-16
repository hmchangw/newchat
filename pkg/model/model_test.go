package model_test

import (
	"encoding/json"
	"reflect"
	"strings"
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
		SectID: "S", SectName: "Sect", SectTCName: "部", SectDescription: "Sect desc",
		DeptID: "D", DeptName: "Dept", DeptTCName: "處", DeptDescription: "Dept desc",
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
		{"p_ account without admin role is not a role admin", &model.User{Account: "p_webhook"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, model.IsPlatformAdmin(tt.u))
		})
	}
}

func TestIsPlatformAdminAccount(t *testing.T) {
	tests := []struct {
		name    string
		account string
		want    bool
	}{
		{"p_ prefix webhook", "p_webhook", true},
		{"bare p_ prefix", "p_", true},
		{"plain human account", "alice", false},
		{"bot account", "weather.bot", false},
		{"case-sensitive prefix (P_)", "P_upper", false},
		{"p underscore not at start", "alice_p_x", false},
		{"empty", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, model.IsPlatformAdminAccount(tt.account))
		})
	}
}

func TestIsBot(t *testing.T) {
	tests := []struct {
		name    string
		account string
		want    bool
	}{
		{"dot-bot suffix", "weather.bot", true},
		{"bare dot-bot", ".bot", true},
		{"p_ prefix is not a bot", "p_webhook", false},
		{"plain human account", "alice", false},
		{"ends with bot but no dot", "robot", false},
		{"dot-bot not at end", "alice.bot.com", false},
		{"case-sensitive suffix", "weather.BOT", false},
		{"empty", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, model.IsBot(tt.account))
		})
	}
}

func TestUserJSON_WithStatus(t *testing.T) {
	u := model.User{
		ID: "u1", Account: "alice", SiteID: "site-a",
		EngName: "Alice Wang", ChineseName: "愛麗絲",
		StatusIsShow: true,
		StatusText:   "available",
	}
	roundTrip(t, &u, &model.User{})
}

func TestUser_DeactivatedRoundTrip(t *testing.T) {
	u := model.User{ID: "u1", Account: "alice", SiteID: "site-local", Deactivated: true}
	got := &model.User{}
	roundTrip(t, &u, got)
	assert.True(t, got.Deactivated)
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
	// Embedded *Subscription must flatten onto the top-level JSON object — frontend api/types.ts depends on this.
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
	// nil *SubscriptionHRInfo with omitempty must not produce a phantom hrInfo:null on the wire.
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

func TestSubscriptionItem_Base(t *testing.T) {
	base := &model.Subscription{ID: "s1", Name: "row"}
	items := []model.SubscriptionItem{
		&model.ChannelSubscription{Subscription: base},
		&model.DMSubscription{Subscription: base},
		&model.BotDMSubscription{Subscription: base},
	}
	for _, it := range items {
		assert.Same(t, base, it.Base(), "Base() must return the embedded base subscription")
	}
}

func TestSubscriptionHRInfoJSON(t *testing.T) {
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

	// #nosec G117 -- test roundtrip on a model whose PrivateKey field is part of the wire schema
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

func TestNotificationEventJSON_Reaction(t *testing.T) {
	src := model.NotificationEvent{
		Type:     "reaction",
		RoomID:   "room-1",
		RoomType: model.RoomTypeChannel,
		Message: model.Message{
			ID: "m1", RoomID: "room-1", UserID: "u1", UserAccount: "bob",
			CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		},
		ReactionDelta: &model.ReactionDelta{
			Shortcode: "thumbsup",
			Action:    "added",
			Actor: model.Participant{
				UserID: "u-alice", Account: "alice", SiteID: "site-a", EngName: "Alice",
			},
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

func TestInboxEventJSON(t *testing.T) {
	src := model.InboxEvent{
		Type:       "member_added",
		SiteID:     "site-a",
		DestSiteID: "site-b",
		Payload:    []byte(`{"inviterId":"u1"}`),
		Timestamp:  1735689600000,
	}
	data, err := json.Marshal(&src)
	require.NoError(t, err)
	var dst model.InboxEvent
	require.NoError(t, json.Unmarshal(data, &dst))
	if !reflect.DeepEqual(src, dst) {
		t.Errorf("round-trip mismatch:\n  got  %+v\n  want %+v", dst, src)
	}
}

func TestInboxEventJSON_ThreadSubscriptionUpserted(t *testing.T) {
	src := model.InboxEvent{
		Type:       model.InboxThreadSubscriptionUpserted,
		SiteID:     "site-a",
		DestSiteID: "site-b",
		Payload:    []byte(`{"id":"sub-1","threadRoomId":"tr-1"}`),
		Timestamp:  1735689600000,
	}
	data, err := json.Marshal(&src)
	require.NoError(t, err)

	var dst model.InboxEvent
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

	t.Run("members round-trips enriched member.list-shaped entries", func(t *testing.T) {
		src := model.MemberAddEvent{
			Type:     "member_added",
			RoomID:   "r1",
			Accounts: []string{"bob", "carol"},
			SiteID:   "site-a",
			JoinedAt: 1735689600000,
			Members: []model.RoomMemberEntry{
				{
					ID:             "DEPT-100",
					Type:           model.RoomMemberOrg,
					OrgName:        "Cardiology Department",
					OrgDescription: "Inpatient cardiac care",
					MemberCount:    42,
				},
				{
					ID:          "u-bob",
					Type:        model.RoomMemberIndividual,
					Account:     "bob",
					EngName:     "Bob",
					ChineseName: "鮑",
					SectName:    "Cardiology",
					EmployeeID:  "E10293",
				},
			},
			Timestamp: 1735689600000,
		}
		data, err := json.Marshal(src)
		require.NoError(t, err)

		// Wire key "members" carries the RoomMemberEntry display fields directly (no id/rid/ts envelope).
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		entries, ok := raw["members"].([]any)
		require.True(t, ok, "members must marshal as an array")
		require.Len(t, entries, 2)
		orgEntry, ok := entries[0].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "org", orgEntry["type"])
		assert.Equal(t, "Cardiology Department", orgEntry["orgName"])
		assert.Equal(t, "Inpatient cardiac care", orgEntry["orgDescription"])
		assert.EqualValues(t, 42, orgEntry["memberCount"])
		_, hasEnvelope := orgEntry["rid"]
		assert.False(t, hasEnvelope, "entries carry no membership envelope (rid/ts)")

		var dst model.MemberAddEvent
		require.NoError(t, json.Unmarshal(data, &dst))
		assert.Equal(t, src.Members, dst.Members)
	})

	t.Run("nil members is omitted on the wire", func(t *testing.T) {
		src := model.MemberAddEvent{
			Type:      "member_added",
			RoomID:    "r1",
			Accounts:  []string{"alice"},
			SiteID:    "site-a",
			Timestamp: 1735689600000,
		}
		data, err := json.Marshal(src)
		require.NoError(t, err)

		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, hasMembers := raw["members"]
		assert.False(t, hasMembers, "members must be omitted when nil so INBOX copies stay lean")
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
		SectName: "Cardiology", EmployeeID: "E10293",
	}
	data, err := json.Marshal(&entry)
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, "u1", got["id"])
	assert.Equal(t, "individual", got["type"])
	assert.Equal(t, "alice", got["account"])
	assert.Equal(t, "Alice Wang", got["engName"])
	assert.Equal(t, "愛麗絲", got["chineseName"])
	assert.Equal(t, true, got["isOwner"])
	assert.Equal(t, "Cardiology", got["sectName"])
	assert.Equal(t, "E10293", got["employeeId"])
}

func TestRoomMemberEntry_DisplayFields_OmittedWhenZero(t *testing.T) {
	entry := model.RoomMemberEntry{
		ID: "u1", Type: model.RoomMemberIndividual, Account: "alice",
	}
	data, err := json.Marshal(&entry)
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, json.Unmarshal(data, &got))
	for _, k := range []string{"engName", "chineseName", "name", "isOwner", "orgName", "orgCode", "memberCount", "sectName", "employeeId", "orgDescription"} {
		_, present := got[k]
		assert.False(t, present, "display field %q should be omitted when zero", k)
	}
}

func TestRoomMemberEntry_DisplayFields_NotPersistedToBSON(t *testing.T) {
	entry := model.RoomMemberEntry{
		ID: "org-1", Type: model.RoomMemberOrg,
		OrgName: "Engineering", OrgCode: "Engineering", MemberCount: 42, OrgDescription: "Eng dept",
		SectName: "Cardiology", EmployeeID: "E10293",
	}
	data, err := bson.Marshal(&entry)
	require.NoError(t, err)

	var got bson.M
	require.NoError(t, bson.Unmarshal(data, &got))
	assert.Equal(t, "org-1", got["id"])
	assert.Equal(t, "org", got["type"])
	for _, k := range []string{"engName", "chineseName", "name", "isOwner", "orgName", "orgCode", "memberCount", "sectName", "employeeId", "orgDescription"} {
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

	t.Run("orgName and orgDescription round-trip via JSON", func(t *testing.T) {
		entry := model.RoomMemberEntry{
			ID:             "sect-eng",
			Type:           model.RoomMemberOrg,
			OrgName:        "Engineering",
			OrgCode:        "Engineering",
			OrgDescription: "Eng dept",
		}
		data, err := json.Marshal(&entry)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		assert.Equal(t, "Engineering", raw["orgName"])
		assert.Equal(t, "Engineering", raw["orgCode"])
		assert.Equal(t, "Eng dept", raw["orgDescription"])
		_, hasSectName := raw["sectName"]
		assert.False(t, hasSectName, "sectName absent on an org entry")

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

func TestSubscriptionRoomJSON(t *testing.T) {
	t.Run("round trip with all fields", func(t *testing.T) {
		pk := "dGVzdC1wcml2YXRlLWtleS1iYXNlNjQ="
		kv := 7
		lastMsg := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
		lastMention := time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC)
		minSeen := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
		r := model.SubscriptionRoom{
			SiteID:            "site-a",
			Name:              "general",
			UserCount:         42,
			AppCount:          3,
			LastMsgAt:         &lastMsg,
			LastMsgID:         "m-100",
			LastMentionAllAt:  &lastMention,
			MinUserLastSeenAt: &minSeen,
			PrivateKey:        &pk,
			KeyVersion:        &kv,
		}
		roundTrip(t, &r, &model.SubscriptionRoom{})
	})

	t.Run("timestamps serialize as RFC3339 strings", func(t *testing.T) {
		lastMsg := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
		lastMention := time.Date(2025, 1, 3, 6, 7, 8, 0, time.UTC)
		minSeen := time.Date(2025, 1, 4, 9, 10, 11, 0, time.UTC)
		r := model.SubscriptionRoom{LastMsgAt: &lastMsg, LastMentionAllAt: &lastMention, MinUserLastSeenAt: &minSeen}
		// #nosec G117 -- test roundtrip on a model whose PrivateKey field is part of the wire schema
		data, err := json.Marshal(&r)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		assert.Equal(t, "2025-01-02T03:04:05Z", raw["lastMsgAt"], "lastMsgAt must be RFC3339, not epoch millis")
		assert.Equal(t, "2025-01-03T06:07:08Z", raw["lastMentionAllAt"], "lastMentionAllAt must be RFC3339, not epoch millis")
		assert.Equal(t, "2025-01-04T09:10:11Z", raw["minUserLastSeenAt"], "minUserLastSeenAt must be RFC3339, not epoch millis")
	})

	t.Run("zero value omits all fields", func(t *testing.T) {
		// #nosec G117 -- test roundtrip on a model whose PrivateKey field is part of the wire schema
		data, err := json.Marshal(&model.SubscriptionRoom{})
		require.NoError(t, err)
		assert.JSONEq(t, `{}`, string(data))
	})
}

func TestRoomInfo_MinUserLastSeenAt(t *testing.T) {
	t.Run("set serializes as epoch millis and round-trips", func(t *testing.T) {
		floor := int64(1735693200000)
		src := model.RoomInfo{RoomID: "r1", Found: true, MinUserLastSeenAt: &floor}
		// #nosec G117 -- test roundtrip on a model whose PrivateKey field is part of the wire schema
		data, err := json.Marshal(&src)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		got, present := raw["minUserLastSeenAt"]
		require.True(t, present, "non-nil MinUserLastSeenAt must be present")
		assert.Equal(t, float64(1735693200000), got, "MinUserLastSeenAt must serialize as epoch millis, not RFC3339")

		var dst model.RoomInfo
		require.NoError(t, json.Unmarshal(data, &dst))
		require.NotNil(t, dst.MinUserLastSeenAt)
		assert.Equal(t, floor, *dst.MinUserLastSeenAt)
	})

	t.Run("nil is omitted", func(t *testing.T) {
		// #nosec G117 -- test roundtrip on a model whose PrivateKey field is part of the wire schema
		data, err := json.Marshal(&model.RoomInfo{RoomID: "r1", Found: true})
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["minUserLastSeenAt"]
		assert.False(t, present, "nil MinUserLastSeenAt must be omitted from JSON")
	})
}

func TestSubscriptionJSON_NestedRoom(t *testing.T) {
	lastMsg := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	// The read-time room baseline lives on EnrichedSubscription (all json:"-"); this
	// pins that those fields never flatten onto the wire while Room nests under "room".
	s := model.EnrichedSubscription{
		Subscription: model.Subscription{
			ID:       "s1",
			User:     model.SubscriptionUser{ID: "u1", Account: "alice"},
			RoomID:   "r1",
			SiteID:   "site-a",
			RoomType: model.RoomTypeChannel,
			Name:     "general",
			JoinedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			Room: &model.SubscriptionRoom{
				SiteID:    "site-a",
				Name:      "general-canonical",
				UserCount: 42,
				LastMsgID: "m-100",
			},
		},
		UserCount: 42,
		LastMsgAt: &lastMsg,
		LastMsgID: "m-100",
	}
	data, err := json.Marshal(&s)
	require.NoError(t, err)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))

	// Room-derived fields are nested under "room" — never flattened on the wire.
	for _, key := range []string{"userCount", "lastMsgAt", "lastMsgId"} {
		_, present := raw[key]
		assert.False(t, present, "flattened %q must not serialize at the top level", key)
	}
	room, ok := raw["room"].(map[string]any)
	require.True(t, ok, "room object must be present")
	assert.Equal(t, "general-canonical", room["name"])
	assert.Equal(t, float64(42), room["userCount"])
	assert.Equal(t, "m-100", room["lastMsgId"])
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
			UserCount:        42,
			AppCount:         3,
			LastMsgAt:        &lastMsg,
			LastMsgID:        "m-100",
			LastMentionAllAt: &lastMention,
			PrivateKey:       &pk,
			KeyVersion:       &kv,
		}
		// #nosec G117 -- test roundtrip on a model whose PrivateKey field is part of the wire schema
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
		// #nosec G117 -- test roundtrip on a model whose PrivateKey field is part of the wire schema
		data, err := json.Marshal(&src)
		require.NoError(t, err)

		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))

		assert.Contains(t, raw, "roomId")
		assert.Equal(t, "r1", raw["roomId"])

		foundVal, foundPresent := raw["found"]
		assert.True(t, foundPresent, "found must be present")
		assert.Equal(t, false, foundVal)

		for _, key := range []string{"siteId", "name", "userCount", "appCount", "lastMsgAt", "lastMsgId", "lastMentionAllAt", "privateKey", "keyVersion", "error"} {
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
		// #nosec G117 -- test roundtrip on a model whose PrivateKey field is part of the wire schema
		data, err := json.Marshal(&src)
		require.NoError(t, err)

		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))

		for _, key := range []string{"userCount", "lastMsgAt", "lastMsgId", "lastMentionAllAt", "privateKey", "keyVersion"} {
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
		// #nosec G117 -- test roundtrip on a model whose PrivateKey field is part of the wire schema
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
			Attachments: []model.Attachment{{
				ID: "f1", Title: "q3.pdf", Description: "numbers",
				Type: "file", FileType: "application/pdf",
				TitleLink: "api/v1/file/rooms/r1/file/f1", TitleLinkDownload: true,
			}},
			Card: &model.Card{Template: "expense-v1", Data: []byte(`{"amount":42}`)},
		}
		roundTrip(t, &msg, &model.SearchMessage{})
	})

	t.Run("attachment and card fields omitted when empty", func(t *testing.T) {
		msg := model.SearchMessage{MessageID: "m1", RoomID: "r1", Content: "hi"}
		data, err := json.Marshal(&msg)
		require.NoError(t, err)
		for _, key := range []string{"attachments", "card"} {
			assert.NotContains(t, string(data), `"`+key+`"`)
		}
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

func TestMessageEvent_QuotedParentUnverified_JSON(t *testing.T) {
	t.Run("flag round-trips when set", func(t *testing.T) {
		evt := model.MessageEvent{
			Event:                  model.EventCreated,
			Message:                model.Message{ID: "m1", RoomID: "r1"},
			SiteID:                 "site-a",
			Timestamp:              123,
			QuotedParentUnverified: true,
		}
		data, err := json.Marshal(&evt)
		require.NoError(t, err)

		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		assert.Equal(t, true, raw["quotedParentUnverified"])

		var dst model.MessageEvent
		require.NoError(t, json.Unmarshal(data, &dst))
		assert.True(t, dst.QuotedParentUnverified)
	})

	t.Run("omitted when false", func(t *testing.T) {
		evt := model.MessageEvent{
			Event:     model.EventCreated,
			Message:   model.Message{ID: "m1", RoomID: "r1"},
			SiteID:    "site-a",
			Timestamp: 123,
		}
		data, err := json.Marshal(&evt)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["quotedParentUnverified"]
		assert.False(t, present, "quotedParentUnverified should be omitted when false")
	})

	t.Run("never persisted to BSON (envelope-only)", func(t *testing.T) {
		evt := model.MessageEvent{
			Event:                  model.EventCreated,
			Message:                model.Message{ID: "m1", RoomID: "r1"},
			SiteID:                 "site-a",
			Timestamp:              123,
			QuotedParentUnverified: true,
		}
		data, err := bson.Marshal(&evt)
		require.NoError(t, err)
		var raw bson.M
		require.NoError(t, bson.Unmarshal(data, &raw))
		_, present := raw["quotedParentUnverified"]
		assert.False(t, present, "quotedParentUnverified must be excluded from BSON (bson:\"-\")")
		_, present = raw["quotedparentunverified"]
		assert.False(t, present, "QuotedParentUnverified must not be BSON-encoded under the default (lowercase) field name")
	})
}

func TestMessageEvent_ThreadParentSenderAccount_JSON(t *testing.T) {
	t.Run("account round-trips when set", func(t *testing.T) {
		evt := model.MessageEvent{
			Event:                     model.EventCreated,
			Message:                   model.Message{ID: "m1", RoomID: "r1"},
			SiteID:                    "site-a",
			Timestamp:                 123,
			ThreadParentSenderAccount: "alice",
		}
		data, err := json.Marshal(&evt)
		require.NoError(t, err)

		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		assert.Equal(t, "alice", raw["threadParentSenderAccount"])

		var dst model.MessageEvent
		require.NoError(t, json.Unmarshal(data, &dst))
		assert.Equal(t, "alice", dst.ThreadParentSenderAccount)
	})

	t.Run("omitted when empty", func(t *testing.T) {
		evt := model.MessageEvent{
			Event:     model.EventCreated,
			Message:   model.Message{ID: "m1", RoomID: "r1"},
			SiteID:    "site-a",
			Timestamp: 123,
		}
		data, err := json.Marshal(&evt)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["threadParentSenderAccount"]
		assert.False(t, present, "threadParentSenderAccount should be omitted when empty")
	})

	t.Run("never persisted to BSON (envelope-only)", func(t *testing.T) {
		evt := model.MessageEvent{
			Event:                     model.EventCreated,
			Message:                   model.Message{ID: "m1", RoomID: "r1"},
			SiteID:                    "site-a",
			Timestamp:                 123,
			ThreadParentSenderAccount: "alice",
		}
		data, err := bson.Marshal(&evt)
		require.NoError(t, err)
		var raw bson.M
		require.NoError(t, bson.Unmarshal(data, &raw))
		_, present := raw["threadParentSenderAccount"]
		assert.False(t, present, "threadParentSenderAccount must be excluded from BSON (bson:\"-\")")
		_, present = raw["threadparentsenderaccount"]
		assert.False(t, present, "ThreadParentSenderAccount must not be BSON-encoded under the default (lowercase) field name")
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

func TestListMemberStatusesRequestJSON(t *testing.T) {
	limit := 5
	r := model.ListMemberStatusesRequest{Limit: &limit}
	roundTrip(t, &r, &model.ListMemberStatusesRequest{})
}

// Nil Limit must omit the wire key (omitempty contract) so the server can
// distinguish "client supplied an explicit value" from "use the server default".
func TestListMemberStatusesRequestJSON_NilLimitOmitted(t *testing.T) {
	data, err := json.Marshal(model.ListMemberStatusesRequest{})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if got := string(data); got != "{}" {
		t.Errorf("nil Limit must marshal to {}, got %s", got)
	}
}

func TestMemberStatusJSON(t *testing.T) {
	m := model.MemberStatus{
		Account: "alice", EngName: "Alice Wang", ChineseName: "愛麗絲",
		StatusIsShow: true, StatusText: "in a meeting",
	}
	roundTrip(t, &m, &model.MemberStatus{})
}

func TestListMemberStatusesResponseJSON(t *testing.T) {
	r := model.ListMemberStatusesResponse{Members: []model.MemberStatus{
		{Account: "alice", EngName: "Alice", ChineseName: "愛麗絲", StatusIsShow: true, StatusText: "busy"},
		{Account: "bob", EngName: "Bob", ChineseName: "陳博"},
	}}
	roundTrip(t, &r, &model.ListMemberStatusesResponse{})
}

func TestMentionableSubscriptionsRequestJSON(t *testing.T) {
	limit := 10
	r := model.MentionableSubscriptionsRequest{Limit: &limit, Filter: "ali"}
	roundTrip(t, &r, &model.MentionableSubscriptionsRequest{})
}

// Nil Limit + empty Filter must marshal to {} so the server can apply its
// default limit and treat the filter as "match everything".
func TestMentionableSubscriptionsRequestJSON_NilLimitOmitted(t *testing.T) {
	data, err := json.Marshal(model.MentionableSubscriptionsRequest{})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if got := string(data); got != "{}" {
		t.Errorf("nil Limit + empty Filter must marshal to {}, got %s", got)
	}
}

func TestMentionableSubscription_UserShape_JSON(t *testing.T) {
	s := model.MentionableSubscription{
		OptionType: "user",
		UserID:     "u-alice",
		Account:    "alice",
		SiteID:     "site-a",
		HRInfo:     &model.MentionableHRInfo{EngName: "Alice Wang", ChineseName: "愛麗絲"},
	}
	roundTrip(t, &s, &model.MentionableSubscription{})

	data, err := json.Marshal(s)
	require.NoError(t, err)
	assert.NotContains(t, string(data), `"app"`, "user-shape must omit app")
}

func TestMentionableSubscription_AppShape_JSON(t *testing.T) {
	s := model.MentionableSubscription{
		OptionType: "app",
		UserID:     "u-bot",
		Account:    "helper.bot",
		App: &model.MentionableApp{
			Name:      "Helper",
			Assistant: model.MentionableAppAssistant{Name: "helper.bot"},
		},
	}
	roundTrip(t, &s, &model.MentionableSubscription{})

	data, err := json.Marshal(s)
	require.NoError(t, err)
	assert.NotContains(t, string(data), `"hrInfo"`, "app-shape must omit hrInfo")
}

func TestMentionableSubscriptionsResponseJSON(t *testing.T) {
	r := model.MentionableSubscriptionsResponse{Subscriptions: []model.MentionableSubscription{
		{OptionType: "user", UserID: "u-a", Account: "a", SiteID: "site-a",
			HRInfo: &model.MentionableHRInfo{EngName: "A", ChineseName: "A"}},
		{OptionType: "app", UserID: "u-b", Account: "b.bot",
			App: &model.MentionableApp{Name: "B", Assistant: model.MentionableAppAssistant{Name: "b.bot"}}},
	}}
	roundTrip(t, &r, &model.MentionableSubscriptionsResponse{})
}

func TestSearchOrgsJSON(t *testing.T) {
	t.Run("request round-trips", func(t *testing.T) {
		req := model.SearchOrgsRequest{Query: "engineering", Size: 20, Offset: 5}
		roundTrip(t, &req, &model.SearchOrgsRequest{})
	})

	t.Run("response round-trips", func(t *testing.T) {
		resp := model.SearchOrgsResponse{Orgs: []model.SearchOrg{
			{
				SectID: "S1", SectName: "Engineering", SectTCName: "工程", SectDescription: "Eng sect",
				DeptID: "D1", DeptName: "Technology", DeptTCName: "科技", DeptDescription: "Tech dept",
				DivisionID: "DIV1",
			},
		}}
		roundTrip(t, &resp, &model.SearchOrgsResponse{})
	})

	t.Run("optional org fields omitted when empty", func(t *testing.T) {
		data, err := json.Marshal(model.SearchOrg{SectID: "S1", SectName: "Engineering"})
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		assert.Equal(t, "S1", raw["sectId"])
		assert.Equal(t, "Engineering", raw["sectName"])
		_, hasDesc := raw["sectDescription"]
		assert.False(t, hasDesc, "empty sectDescription must be omitted")
		_, hasDeptID := raw["deptId"]
		assert.False(t, hasDeptID, "empty deptId must be omitted")
	})

	// Round-trip alone can't catch a self-consistent wrong json tag, so pin the
	// exact wire keys the client contract depends on.
	t.Run("request wire keys", func(t *testing.T) {
		data, err := json.Marshal(model.SearchOrgsRequest{Query: "engineering", Size: 20, Offset: 5})
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		assert.Equal(t, "engineering", raw["query"])
		assert.EqualValues(t, 20, raw["size"])
		assert.EqualValues(t, 5, raw["offset"])
	})

	t.Run("response and SearchOrg wire keys", func(t *testing.T) {
		data, err := json.Marshal(model.SearchOrgsResponse{Orgs: []model.SearchOrg{{
			SectID: "S1", SectName: "Engineering", SectTCName: "工程", SectDescription: "Eng sect",
			DeptID: "D1", DeptName: "Technology", DeptTCName: "科技", DeptDescription: "Tech dept",
			DivisionID: "DIV1",
		}}})
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))

		orgs, ok := raw["orgs"].([]any)
		require.True(t, ok, "response must carry the `orgs` key as an array")
		require.Len(t, orgs, 1)

		org := orgs[0].(map[string]any)
		assert.Equal(t, "S1", org["sectId"])
		assert.Equal(t, "Engineering", org["sectName"])
		assert.Equal(t, "工程", org["sectTCName"])
		assert.Equal(t, "Eng sect", org["sectDescription"])
		assert.Equal(t, "D1", org["deptId"])
		assert.Equal(t, "Technology", org["deptName"])
		assert.Equal(t, "科技", org["deptTCName"])
		assert.Equal(t, "Tech dept", org["deptDescription"])
		assert.Equal(t, "DIV1", org["divisionId"])
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

func TestThreadReadAllRoundTrip(t *testing.T) {
	respFull := model.ThreadReadAllResponse{UnavailableSites: []string{"site-b"}}
	roundTrip(t, &respFull, &model.ThreadReadAllResponse{})
	respEmpty := model.ThreadReadAllResponse{}
	roundTrip(t, &respEmpty, &model.ThreadReadAllResponse{})
	roomReq := model.RoomThreadReadAllRequest{Account: "alice"}
	roundTrip(t, &roomReq, &model.RoomThreadReadAllRequest{})
	roomResp := model.RoomThreadReadAllResponse{}
	roundTrip(t, &roomResp, &model.RoomThreadReadAllResponse{})
	ev := model.ThreadReadAllEvent{Account: "alice", LastSeenAt: 1717000000000, Timestamp: 1717000000001}
	roundTrip(t, &ev, &model.ThreadReadAllEvent{})
}

func TestPresenceStatusValues(t *testing.T) {
	assert.Equal(t, model.PresenceStatus("online"), model.StatusOnline)
	assert.Equal(t, model.PresenceStatus("away"), model.StatusAway)
	assert.Equal(t, model.PresenceStatus("busy"), model.StatusBusy)
	assert.Equal(t, model.PresenceStatus("offline"), model.StatusOffline)
	assert.Equal(t, model.PresenceStatus("appear_offline"), model.StatusAppearOffline)
	assert.Equal(t, model.PresenceStatus(""), model.StatusNone)
}

func TestPresenceTypesJSON(t *testing.T) {
	roundTrip(t, &model.Hello{ConnID: "c1", Timestamp: 1735689600000}, &model.Hello{})
	roundTrip(t, &model.Ping{ConnID: "c1", Timestamp: 1735689600000}, &model.Ping{})
	roundTrip(t, &model.Activity{ConnID: "c1", Away: true, Timestamp: 1735689600000}, &model.Activity{})
	roundTrip(t, &model.ByeRequest{ConnID: "c1", Timestamp: 1735689600000}, &model.ByeRequest{})
	roundTrip(t, &model.ManualStatusRequest{Status: model.StatusBusy, Timestamp: 1735689600000}, &model.ManualStatusRequest{})
	roundTrip(t, &model.ManualStatusResponse{Account: "alice", Status: model.StatusBusy, SetAt: 1735689600000, Effective: model.StatusBusy}, &model.ManualStatusResponse{})
	roundTrip(t, &model.PresenceQuery{Accounts: []string{"alice", "bob"}}, &model.PresenceQuery{})
	roundTrip(t, &model.PresenceState{Account: "alice", SiteID: "site-a", Status: model.StatusOnline, Timestamp: 1735689600000}, &model.PresenceState{})
	roundTrip(t, &model.PresenceQueryResponse{
		States:    []model.PresenceState{{Account: "alice", SiteID: "site-a", Status: model.StatusOnline, Timestamp: 1735689600000}},
		Timestamp: 1735689600000,
	}, &model.PresenceQueryResponse{})
}

func TestInboxSubscriptionReadConstant(t *testing.T) {
	if model.InboxSubscriptionRead != "subscription_read" {
		t.Errorf("got %q, want %q", model.InboxSubscriptionRead, "subscription_read")
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

func TestAppRoundtrip_WithChannelTab(t *testing.T) {
	a := model.App{
		ID:        "app1",
		Name:      "Calendar",
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

func TestAppRoundtrip_NewMetaFields(t *testing.T) {
	a := model.App{
		ID:   "app-meta",
		Name: "Meta Bot",
		Assistant: &model.AppAssistant{
			Enabled:  true,
			Name:     "meta.bot",
			Username: "Meta Assistant",
		},
		AppViewURL:    map[string]string{"default": "https://upstream/meta/view"},
		ReportURL:     "https://upstream/meta/report",
		ForumURL:      "https://upstream/meta/forum",
		UserManualURL: "https://upstream/meta/manual",
		Version:       "1.2.3",
		Sponsors:      []model.AppSponsor{{Name: "Acme", Phone: "555-0199"}},
	}
	var dst model.App
	roundTrip(t, &a, &dst)
	require.NotNil(t, dst.Assistant)
	assert.Equal(t, "Meta Assistant", dst.Assistant.Username)
	assert.Equal(t, map[string]string{"default": "https://upstream/meta/view"}, dst.AppViewURL)
	assert.Equal(t, "https://upstream/meta/report", dst.ReportURL)
	assert.Equal(t, "https://upstream/meta/forum", dst.ForumURL)
	assert.Equal(t, "https://upstream/meta/manual", dst.UserManualURL)
	assert.Equal(t, "1.2.3", dst.Version)
}

func TestAppRoundtrip_NewMetaFields_OmitEmpty(t *testing.T) {
	a := model.App{ID: "app-bare", Name: "Bare"}
	b, err := json.Marshal(&a)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(b, &raw))
	for _, k := range []string{"appViewUrl", "reportUrl", "forumUrl", "userManualUrl", "version"} {
		_, present := raw[k]
		assert.False(t, present, "empty %s must be omitted", k)
	}
}

func TestAppSubscriptionRoundtrip(t *testing.T) {
	m := model.AppSubscription{
		AppID:       "app-meta",
		Name:        "Meta App",
		Description: "A metadata overlay",
		Assistant: &model.AppAssistant{
			Enabled:  true,
			Name:     "meta.bot",
			Username: "Meta Assistant",
		},
		AppViewURL:    map[string]string{"default": "https://upstream/meta/view"},
		ReportURL:     "https://upstream/meta/report",
		ForumURL:      "https://upstream/meta/forum",
		UserManualURL: "https://upstream/meta/manual",
		Version:       "1.2.3",
		Sponsors:      []model.AppSponsor{{Name: "Acme", Phone: "555-0199"}},
	}
	var dst model.AppSubscription
	roundTrip(t, &m, &dst)
	require.NotNil(t, dst.Assistant)
	assert.Equal(t, "meta.bot", dst.Assistant.Name)
	assert.Equal(t, "app-meta", dst.AppID)
	assert.Equal(t, "Meta App", dst.Name)
	assert.Len(t, dst.Sponsors, 1)
}

func TestAppSubscriptionRoundtrip_OmitEmpty(t *testing.T) {
	m := model.AppSubscription{}
	b, err := json.Marshal(&m)
	require.NoError(t, err)
	assert.Equal(t, "{}", string(b), "a zero AppSubscription must marshal to an empty object")
}

func TestAppSubscriptionFromApp(t *testing.T) {
	a := &model.App{
		ID:          "app-x",
		Name:        "Display Name",
		Description: "desc",
		Assistant:   &model.AppAssistant{Enabled: true, Name: "x.bot", Username: "X"},
		// ChannelTab is intentionally NOT carried onto the app object.
		ChannelTab:    &model.AppChannelTab{Enabled: true, Name: "tab"},
		AppViewURL:    map[string]string{"default": "https://upstream/x/view"},
		ReportURL:     "https://upstream/x/report",
		ForumURL:      "https://upstream/x/forum",
		UserManualURL: "https://upstream/x/manual",
		Version:       "9.9",
		Sponsors:      []model.AppSponsor{{Name: "Sponsor", Phone: "555-0000"}},
	}
	meta := model.AppSubscriptionFromApp(a)
	require.NotNil(t, meta)
	assert.Equal(t, "app-x", meta.AppID, "AppID must come from App.ID")
	assert.Equal(t, "Display Name", meta.Name, "Name must come from App.Name")
	assert.Equal(t, "desc", meta.Description)
	require.NotNil(t, meta.Assistant)
	assert.Equal(t, "x.bot", meta.Assistant.Name)
	assert.Equal(t, "X", meta.Assistant.Username)
	assert.Equal(t, map[string]string{"default": "https://upstream/x/view"}, meta.AppViewURL)
	assert.Equal(t, "https://upstream/x/report", meta.ReportURL)
	assert.Equal(t, "https://upstream/x/forum", meta.ForumURL)
	assert.Equal(t, "https://upstream/x/manual", meta.UserManualURL)
	assert.Equal(t, "9.9", meta.Version)
	assert.Len(t, meta.Sponsors, 1)
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

func TestInboxSubscriptionMuteToggledConst(t *testing.T) {
	assert.Equal(t, model.InboxEventType("subscription_mute_toggled"), model.InboxSubscriptionMuteToggled)
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

func TestInboxSubscriptionFavoriteToggledConst(t *testing.T) {
	assert.Equal(t, model.InboxEventType("subscription_favorite_toggled"), model.InboxSubscriptionFavoriteToggled)
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

func TestEventReactedConstant(t *testing.T) {
	assert.Equal(t, model.EventType("reacted"), model.EventReacted)
}

func TestRoomEventMessageReactedConstant(t *testing.T) {
	assert.Equal(t, model.RoomEventType("message_reacted"), model.RoomEventMessageReacted)
}

func TestReactionDeltaRoundtrip(t *testing.T) {
	d := model.ReactionDelta{
		Shortcode: "thumbsup",
		Action:    "added",
		Actor: model.Participant{
			UserID:  "u-1",
			Account: "alice",
			SiteID:  "site-a",
			EngName: "Alice",
		},
	}
	var dst model.ReactionDelta
	roundTrip(t, &d, &dst)
}

func TestMessageEventWithReactionDeltaJSON(t *testing.T) {
	evt := model.MessageEvent{
		Event: model.EventReacted,
		Message: model.Message{
			ID:          "msg-uuid",
			RoomID:      "r1",
			UserID:      "u-author",
			UserAccount: "bob",
			CreatedAt:   time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
		},
		SiteID:    "site-a",
		Timestamp: 1747800000000,
		ReactionDelta: &model.ReactionDelta{
			Shortcode: "thumbsup",
			Action:    "added",
			Actor: model.Participant{
				UserID:  "u-1",
				Account: "alice",
				SiteID:  "site-a",
				EngName: "Alice",
			},
		},
	}
	roundTrip(t, &evt, &model.MessageEvent{})
}

func TestMessageEventOmitsReactionDeltaWhenAbsent(t *testing.T) {
	evt := model.MessageEvent{
		Event:     model.EventCreated,
		Message:   model.Message{ID: "msg-uuid", RoomID: "r1"},
		SiteID:    "site-a",
		Timestamp: 1747800000000,
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)
	assert.NotContains(t, string(data), "reactionDelta")
}

func TestReactRoomEventJSON(t *testing.T) {
	reactedAt := time.Date(2026, 5, 14, 12, 15, 0, 0, time.UTC)
	evt := model.ReactRoomEvent{
		Type:      model.RoomEventMessageReacted,
		RoomID:    "r1",
		SiteID:    "site-a",
		Timestamp: 1746518900123,
		MessageID: "msg-uuid",
		Shortcode: "thumbsup",
		Action:    "added",
		Actor: model.Participant{
			UserID:  "u-1",
			Account: "alice",
			SiteID:  "site-a",
			EngName: "Alice",
		},
		ReactedAt: reactedAt,
		UpdatedAt: reactedAt,
	}
	roundTrip(t, &evt, &model.ReactRoomEvent{})
}

func TestCustomEmojiRoundtrip(t *testing.T) {
	e := model.CustomEmoji{
		ID:          "site-a:acme_party",
		SiteID:      "site-a",
		Shortcode:   "acme_party",
		ImageURL:    "/api/v1/emoji/acme_party",
		CreatedBy:   "alice",
		CreatedAt:   1747800000000,
		UpdatedBy:   "bob",
		UpdatedAt:   1747900000000,
		MinioKey:    "emoji/site-a/acme_party",
		ContentType: "image/gif",
		Size:        20480,
		ETag:        "abc123",
	}
	var dst model.CustomEmoji
	roundTrip(t, &e, &dst)
	assert.Equal(t, "acme_party", dst.Shortcode)
	assert.Equal(t, "emoji/site-a/acme_party", dst.MinioKey)
}

func TestCustomEmojiBSON(t *testing.T) {
	e := model.CustomEmoji{
		ID:          "site-a:acme_party",
		SiteID:      "site-a",
		Shortcode:   "acme_party",
		ImageURL:    "/api/v1/emoji/acme_party",
		CreatedBy:   "alice",
		CreatedAt:   1747800000000,
		UpdatedBy:   "bob",
		UpdatedAt:   1747900000000,
		MinioKey:    "emoji/site-a/acme_party",
		ContentType: "image/gif",
		Size:        20480,
		ETag:        "abc123",
	}
	data, err := bson.Marshal(&e)
	require.NoError(t, err)
	var dst model.CustomEmoji
	require.NoError(t, bson.Unmarshal(data, &dst))
	assert.Equal(t, e, dst)
}

func TestEmojiListResponseRoundtrip(t *testing.T) {
	src := model.EmojiListResponse{Emojis: []model.EmojiEntry{{
		Shortcode:   "acme_party",
		ImageURL:    "/api/v1/emoji/acme_party",
		ContentType: "image/png",
		ETag:        "abc123",
		UpdatedAt:   time.Date(2026, 5, 22, 8, 26, 40, 0, time.UTC),
	}}}
	roundTrip(t, &src, &model.EmojiListResponse{})

	t.Run("updatedAt serializes as RFC3339 string", func(t *testing.T) {
		data, err := json.Marshal(&src)
		require.NoError(t, err)
		var raw struct {
			Emojis []map[string]any `json:"emojis"`
		}
		require.NoError(t, json.Unmarshal(data, &raw))
		require.Len(t, raw.Emojis, 1)
		assert.Equal(t, "2026-05-22T08:26:40Z", raw.Emojis[0]["updatedAt"],
			"updatedAt must be RFC3339, not epoch millis")
	})
}

func TestEmojiDeleteRequestRoundtrip(t *testing.T) {
	src := model.EmojiDeleteRequest{Shortcode: "acme_party"}
	roundTrip(t, &src, &model.EmojiDeleteRequest{})
}

func TestEmojiDeleteResponseRoundtrip(t *testing.T) {
	src := model.EmojiDeleteResponse{Shortcode: "acme_party", Deleted: true}
	roundTrip(t, &src, &model.EmojiDeleteResponse{})
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

func TestInboxEventJSON_ThreadRead(t *testing.T) {
	payload := model.ThreadReadEvent{
		Account: "alice", RoomID: "r1", ThreadRoomID: "tr1",
		ParentMessageID: "p1", NewThreadUnread: []string{"t2"},
		Alert: false, LastSeenAt: 1735689600000, Timestamp: 1735689600001,
	}
	data, err := json.Marshal(&payload)
	require.NoError(t, err)
	src := model.InboxEvent{
		Type:       model.InboxThreadRead,
		SiteID:     "site-a",
		DestSiteID: "site-b",
		Payload:    data,
		Timestamp:  1735689600002,
	}
	out, err := json.Marshal(&src)
	require.NoError(t, err)
	var dst model.InboxEvent
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

func TestPinStateRoomEventJSON_Pinned(t *testing.T) {
	pinnedAt := time.Date(2026, 5, 14, 12, 15, 0, 0, time.UTC)
	evt := model.PinStateRoomEvent{
		Type:      model.RoomEventMessagePinned,
		RoomID:    "r1",
		SiteID:    "site-a",
		Timestamp: pinnedAt.UnixMilli(),
		MessageID: "msg-uuid",
		Pinned:    true,
		By:        &model.Participant{UserID: "u1", Account: "alice"},
		At:        pinnedAt,
	}
	roundTrip(t, &evt, &model.PinStateRoomEvent{})
}

func TestPinStateRoomEventJSON_Unpinned(t *testing.T) {
	unpinnedAt := time.Date(2026, 5, 14, 12, 20, 0, 0, time.UTC)
	evt := model.PinStateRoomEvent{
		Type:      model.RoomEventMessageUnpinned,
		RoomID:    "r1",
		SiteID:    "site-a",
		Timestamp: unpinnedAt.UnixMilli(),
		MessageID: "msg-uuid",
		Pinned:    false,
		By:        &model.Participant{UserID: "u1", Account: "alice"},
		At:        unpinnedAt,
	}
	roundTrip(t, &evt, &model.PinStateRoomEvent{})
}

func TestPinStateRoomEventJSON_PinnedFieldAlwaysPresent(t *testing.T) {
	// pinned has no omitempty: the false (unpinned) state must serialize
	// explicitly so clients never infer state from field absence.
	evt := model.PinStateRoomEvent{
		Type:      model.RoomEventMessageUnpinned,
		RoomID:    "r1",
		SiteID:    "site-a",
		Timestamp: 1747224000000,
		MessageID: "msg-uuid",
		Pinned:    false,
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &raw))
	pinned, present := raw["pinned"]
	require.True(t, present, "pinned must serialize even when false")
	assert.Equal(t, "false", string(pinned))
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

// --- Sys message + inbox payloads ---

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

func TestRoomRenamedInboxPayloadJSON(t *testing.T) {
	p := model.RoomRenamedInboxPayload{RoomID: "r1", NewName: "x", Timestamp: 1700000000000}
	roundTrip(t, &p, &model.RoomRenamedInboxPayload{})
}

func TestRoomRestrictedInboxPayloadJSON(t *testing.T) {
	p := model.RoomRestrictedInboxPayload{
		RoomID: "r1", Restricted: true, ExternalAccess: false,
		OwnerAccount: "alice", Timestamp: 1700000000000,
	}
	roundTrip(t, &p, &model.RoomRestrictedInboxPayload{})
}

// --- Constants ---

func TestMessageAndInboxAndAsyncOpConstants(t *testing.T) {
	assert.Equal(t, "room_renamed", model.MessageTypeRoomRenamed)
	assert.Equal(t, "room_restricted", model.MessageTypeRoomRestricted)
	assert.Equal(t, "room_renamed", model.InboxRoomRenamed)
	assert.Equal(t, "room_restricted", model.InboxRoomRestricted)
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

func TestStatusReply_RoundTrip(t *testing.T) {
	roundTrip(t, &model.StatusReply{Status: "ok"}, &model.StatusReply{})
	roundTrip(t, &model.StatusReply{Status: "accepted"}, &model.StatusReply{})
}

func TestStatusWithRequestReply_RoundTrip(t *testing.T) {
	roundTrip(t, &model.StatusWithRequestReply{Status: "accepted", RequestID: "01970a4f-8c2d-7c9a-abcd-e0123456789f"}, &model.StatusWithRequestReply{})
}

func TestRoomRenameRequest_RoundTrip(t *testing.T) {
	roundTrip(t, &model.RoomRenameRequest{NewName: "New Name"}, &model.RoomRenameRequest{})
}

func TestStatusReply_JSONShape(t *testing.T) {
	b, err := json.Marshal(model.StatusReply{Status: "accepted"})
	require.NoError(t, err)
	assert.JSONEq(t, `{"status":"accepted"}`, string(b))
}

func TestStatusWithRequestReply_JSONShape(t *testing.T) {
	b, err := json.Marshal(model.StatusWithRequestReply{Status: "ok", RequestID: "rid"})
	require.NoError(t, err)
	assert.JSONEq(t, `{"status":"ok","requestId":"rid"}`, string(b))
}

func TestAvatarJSON(t *testing.T) {
	src := &model.Avatar{
		ID:          "bot:helper.bot",
		SubjectType: model.AvatarSubjectBot,
		SubjectID:   "helper.bot",
		MinioKey:    "bot/helper.bot",
		ContentType: "image/png",
		Size:        2048,
		ETag:        "abc123",
		CreatedAt:   time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC),
		UpdatedAt:   time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC),
	}
	roundTrip(t, src, &model.Avatar{})
}

func TestMessageEvent_NewTCount(t *testing.T) {
	t.Run("NewTCount nil is omitted from JSON", func(t *testing.T) {
		e := model.MessageEvent{
			Message:   model.Message{ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice", CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)},
			SiteID:    "site-a",
			Timestamp: 1735689600000,
		}
		data, err := json.Marshal(e)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["newTcount"]
		assert.False(t, present, "nil NewTCount must be omitted from JSON")
	})

	t.Run("NewTCount zero is included in JSON", func(t *testing.T) {
		zero := 0
		e := model.MessageEvent{
			Message:   model.Message{ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice", CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)},
			SiteID:    "site-a",
			Timestamp: 1735689600000,
			NewTCount: &zero,
		}
		data, err := json.Marshal(e)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		val, present := raw["newTcount"]
		assert.True(t, present, "non-nil zero NewTCount must be present in JSON")
		assert.Equal(t, float64(0), val, "zero NewTCount must marshal as 0")

		var dst model.MessageEvent
		require.NoError(t, json.Unmarshal(data, &dst))
		require.NotNil(t, dst.NewTCount)
		assert.Equal(t, 0, *dst.NewTCount)
	})

	t.Run("NewTCount positive round-trips", func(t *testing.T) {
		count := 3
		e := model.MessageEvent{
			Message:   model.Message{ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice", CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)},
			SiteID:    "site-a",
			Timestamp: 1735689600000,
			NewTCount: &count,
		}
		data, err := json.Marshal(e)
		require.NoError(t, err)
		var dst model.MessageEvent
		require.NoError(t, json.Unmarshal(data, &dst))
		require.NotNil(t, dst.NewTCount)
		assert.Equal(t, 3, *dst.NewTCount)
	})

	t.Run("NewTCount zero in BSON round-trips — omitempty must not drop zero", func(t *testing.T) {
		zero := 0
		e := model.MessageEvent{
			Message:   model.Message{ID: "m1", RoomID: "r1"},
			SiteID:    "site-a",
			Timestamp: 1735689600000,
			NewTCount: &zero,
		}
		data, err := bson.Marshal(e)
		require.NoError(t, err)
		var raw bson.M
		require.NoError(t, bson.Unmarshal(data, &raw))
		val, present := raw["newTcount"]
		assert.True(t, present, "zero NewTCount must be present in BSON — bson omitempty must not be used")
		assert.EqualValues(t, 0, val, "zero BSON value must be 0, not missing")
	})
}
func TestMessageEvent_NewThreadLastMsgAt(t *testing.T) {
	ts := time.Date(2026, 6, 18, 10, 0, 0, 0, time.UTC)

	t.Run("nil NewThreadLastMsgAt omitted from JSON", func(t *testing.T) {
		e := model.MessageEvent{
			Message:   model.Message{ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice", CreatedAt: ts},
			SiteID:    "site-a",
			Timestamp: ts.UnixMilli(),
		}
		data, err := json.Marshal(e)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["newThreadLastMsgAt"]
		assert.False(t, present, "nil NewThreadLastMsgAt must be omitted from JSON")
	})

	t.Run("non-nil NewThreadLastMsgAt round-trips JSON", func(t *testing.T) {
		e := model.MessageEvent{
			Message:            model.Message{ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice", CreatedAt: ts},
			SiteID:             "site-a",
			Timestamp:          ts.UnixMilli(),
			NewThreadLastMsgAt: &ts,
		}
		data, err := json.Marshal(e)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["newThreadLastMsgAt"]
		assert.True(t, present, "non-nil NewThreadLastMsgAt must appear in JSON as newThreadLastMsgAt")
		var dst model.MessageEvent
		require.NoError(t, json.Unmarshal(data, &dst))
		require.NotNil(t, dst.NewThreadLastMsgAt)
		assert.True(t, dst.NewThreadLastMsgAt.Equal(ts))
	})

	t.Run("non-nil NewThreadLastMsgAt round-trips BSON", func(t *testing.T) {
		e := model.MessageEvent{
			Message:            model.Message{ID: "m1", RoomID: "r1"},
			SiteID:             "site-a",
			Timestamp:          ts.UnixMilli(),
			NewThreadLastMsgAt: &ts,
		}
		data, err := bson.Marshal(e)
		require.NoError(t, err)
		var raw bson.M
		require.NoError(t, bson.Unmarshal(data, &raw))
		_, present := raw["newThreadLastMsgAt"]
		assert.True(t, present, "non-nil NewThreadLastMsgAt must be present in BSON as newThreadLastMsgAt")
	})

	t.Run("nil NewThreadLastMsgAt omitted from BSON", func(t *testing.T) {
		e := model.MessageEvent{
			Message:   model.Message{ID: "m1", RoomID: "r1"},
			SiteID:    "site-a",
			Timestamp: ts.UnixMilli(),
		}
		data, err := bson.Marshal(e)
		require.NoError(t, err)
		var raw bson.M
		require.NoError(t, bson.Unmarshal(data, &raw))
		_, present := raw["newThreadLastMsgAt"]
		assert.False(t, present, "nil NewThreadLastMsgAt must be omitted from BSON")
	})
}

func TestThreadMetadataUpdatedEvent_NewThreadLastMsgAt(t *testing.T) {
	ts := time.Date(2026, 6, 18, 10, 0, 0, 0, time.UTC)

	t.Run("nil NewThreadLastMsgAt omitted from JSON", func(t *testing.T) {
		e := model.ThreadMetadataUpdatedEvent{
			Type:            model.RoomEventThreadMetadataUpdated,
			RoomID:          "r1",
			SiteID:          "site-a",
			ParentMessageID: "p1",
			ReplyMessageID:  "r1",
			NewTCount:       1,
			Action:          model.ThreadActionReplyAdded,
			Timestamp:       ts.UnixMilli(),
		}
		data, err := json.Marshal(e)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["newThreadLastMsgAt"]
		assert.False(t, present, "nil NewThreadLastMsgAt must be omitted from JSON")
	})

	t.Run("non-nil NewThreadLastMsgAt round-trips JSON", func(t *testing.T) {
		e := model.ThreadMetadataUpdatedEvent{
			Type:               model.RoomEventThreadMetadataUpdated,
			RoomID:             "r1",
			SiteID:             "site-a",
			ParentMessageID:    "p1",
			ReplyMessageID:     "r1",
			NewTCount:          2,
			NewThreadLastMsgAt: &ts,
			Action:             model.ThreadActionReplyAdded,
			Timestamp:          ts.UnixMilli(),
		}
		data, err := json.Marshal(e)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["newThreadLastMsgAt"]
		assert.True(t, present, "non-nil NewThreadLastMsgAt must appear as newThreadLastMsgAt in JSON")
		var dst model.ThreadMetadataUpdatedEvent
		require.NoError(t, json.Unmarshal(data, &dst))
		require.NotNil(t, dst.NewThreadLastMsgAt)
		assert.True(t, dst.NewThreadLastMsgAt.Equal(ts))
	})
}

func TestMessageReadEventJSON(t *testing.T) {
	floor := time.Date(2026, 6, 9, 10, 30, 0, 0, time.UTC)

	t.Run("floor present round-trips", func(t *testing.T) {
		src := model.MessageReadEvent{
			Type:              model.RoomEventMessageRead,
			RoomID:            "room-1",
			MinUserLastSeenAt: &floor,
			Timestamp:         floor.UnixMilli(),
		}
		data, err := json.Marshal(src)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var dst model.MessageReadEvent
		if err := json.Unmarshal(data, &dst); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if !reflect.DeepEqual(src, dst) {
			t.Errorf("round-trip mismatch:\n  got  %+v\n  want %+v", dst, src)
		}
	})

	t.Run("nil floor omitted from wire", func(t *testing.T) {
		src := model.MessageReadEvent{
			Type:      model.RoomEventMessageRead,
			RoomID:    "room-2",
			Timestamp: floor.UnixMilli(),
		}
		data, err := json.Marshal(src)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if strings.Contains(string(data), "minUserLastSeenAt") {
			t.Errorf("nil floor must be omitted, got %s", data)
		}
		var dst model.MessageReadEvent
		if err := json.Unmarshal(data, &dst); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if dst.MinUserLastSeenAt != nil {
			t.Errorf("expected nil floor, got %v", dst.MinUserLastSeenAt)
		}
	})
}

func TestRoomEventMessageReadValue(t *testing.T) {
	if model.RoomEventMessageRead != "message_read" {
		t.Errorf("RoomEventMessageRead = %q, want %q", model.RoomEventMessageRead, "message_read")
	}
}

func TestThreadMessageReadEventJSON(t *testing.T) {
	floor := time.Date(2026, 6, 9, 10, 30, 0, 0, time.UTC)

	t.Run("floor present round-trips", func(t *testing.T) {
		src := model.ThreadMessageReadEvent{
			Type:              model.RoomEventThreadMessageRead,
			RoomID:            "room-1",
			ThreadRoomID:      "tr-1",
			MinUserLastSeenAt: &floor,
			Timestamp:         floor.UnixMilli(),
		}
		data, err := json.Marshal(src)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var dst model.ThreadMessageReadEvent
		if err := json.Unmarshal(data, &dst); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if !reflect.DeepEqual(src, dst) {
			t.Errorf("round-trip mismatch:\n  got  %+v\n  want %+v", dst, src)
		}
	})

	t.Run("nil floor omitted from wire", func(t *testing.T) {
		src := model.ThreadMessageReadEvent{
			Type:         model.RoomEventThreadMessageRead,
			RoomID:       "room-2",
			ThreadRoomID: "tr-2",
			Timestamp:    floor.UnixMilli(),
		}
		data, err := json.Marshal(src)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if strings.Contains(string(data), "minUserLastSeenAt") {
			t.Errorf("nil floor must be omitted, got %s", data)
		}
		var dst model.ThreadMessageReadEvent
		if err := json.Unmarshal(data, &dst); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if dst.MinUserLastSeenAt != nil {
			t.Errorf("expected nil floor, got %v", dst.MinUserLastSeenAt)
		}
	})
}

func TestRoomEventThreadMessageReadValue(t *testing.T) {
	if model.RoomEventThreadMessageRead != "thread_message_read" {
		t.Errorf("RoomEventThreadMessageRead = %q, want %q", model.RoomEventThreadMessageRead, "thread_message_read")
	}
}

func TestOplogEventJSON_Insert(t *testing.T) {
	src := model.OplogEvent{
		EventID:      "8265A1B2",
		Op:           "insert",
		DB:           "rocketchat",
		Collection:   "rocketchat_message",
		DocumentKey:  json.RawMessage(`{"_id":"abc"}`),
		ClusterTime:  1718100000000,
		FullDocument: json.RawMessage(`{"_id":"abc","msg":"hi"}`),
		SiteID:       "site1",
		Timestamp:    1718100000123,
	}
	roundTrip(t, &src, &model.OplogEvent{})
}

func TestOplogEventJSON_UpdateDelta(t *testing.T) {
	src := model.OplogEvent{
		EventID:           "DEAD01",
		Op:                "update",
		DB:                "rocketchat",
		Collection:        "rocketchat_message",
		DocumentKey:       json.RawMessage(`{"_id":"abc"}`),
		ClusterTime:       1718100000000,
		UpdateDescription: json.RawMessage(`{"updatedFields":{"msg":"edited"},"removedFields":[]}`),
		SiteID:            "site1",
		Timestamp:         1718100000123,
	}
	roundTrip(t, &src, &model.OplogEvent{})
}

func TestOplogEventJSON_Degraded(t *testing.T) {
	src := model.OplogEvent{
		EventID:        "F00D01",
		Op:             "insert",
		DB:             "rocketchat",
		Collection:     "rocketchat_message",
		DocumentKey:    json.RawMessage(`{"_id":"abc"}`),
		ClusterTime:    1718100000000,
		Degraded:       true,
		DegradedReason: "x",
		SiteID:         "site1",
		Timestamp:      1718100000123,
	}
	roundTrip(t, &src, &model.OplogEvent{})
}

func TestUserStatusFields_RoundTrip(t *testing.T) {
	src := model.User{
		ID:           "u1",
		Account:      "alice",
		SiteID:       "site-a",
		StatusText:   "busy",
		StatusIsShow: true,
	}
	dst := model.User{}
	roundTrip(t, &src, &dst)
}

func TestUserStatusUpdated_RoundTrip(t *testing.T) {
	show := true
	src := model.UserStatusUpdated{
		Account:      "alice",
		StatusText:   "in a meeting",
		StatusIsShow: &show,
		Timestamp:    1735689600000,
	}
	dst := model.UserStatusUpdated{}
	roundTrip(t, &src, &dst)
}

func TestUserSettingsUpdated_RoundTrip(t *testing.T) {
	mute, width := true, false
	lang := "ja"
	src := model.UserSettingsUpdated{
		Account: "alice",
		Settings: model.UserSettings{
			MuteAllNotifications: &mute,
			FullWidth:            &width,
			TranslateMessageInto: &lang,
		},
		Timestamp: 1735689600000,
	}
	dst := model.UserSettingsUpdated{}
	roundTrip(t, &src, &dst)
}

func TestUserSettingsUpdated_UnsetSettingsOmittedFromJSON(t *testing.T) {
	mute := true
	src := model.UserSettingsUpdated{
		Account:   "alice",
		Settings:  model.UserSettings{MuteAllNotifications: &mute},
		Timestamp: 1735689600000,
	}
	data, err := json.Marshal(&src)
	require.NoError(t, err)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	settings, ok := raw["settings"].(map[string]any)
	require.True(t, ok)
	_, present := settings["fullWidth"]
	assert.False(t, present, "an unset setting must be omitted, so absent keeps meaning client-default")
}

func TestUserStatusUpdated_StatusIsShowOmittedWhenNil(t *testing.T) {
	src := model.UserStatusUpdated{
		Account:    "alice",
		StatusText: "in a meeting",
		Timestamp:  1735689600000,
	}
	data, err := json.Marshal(&src)
	require.NoError(t, err)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	_, present := raw["statusIsShow"]
	assert.False(t, present, "nil StatusIsShow must be omitted from JSON")
}

func TestSubscriptionEnrichmentFields_RoundTrip(t *testing.T) {
	// The flattened $lookup baseline fields are internal (json:"-"); the wire
	// carries room-derived data only via the nested Room object.
	lastMsg := time.Date(2025, 1, 2, 12, 0, 0, 0, time.UTC)
	src := model.Subscription{
		ID:       "s1",
		User:     model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID:   "r1",
		SiteID:   "site-a",
		Roles:    []model.Role{model.RoleMember},
		JoinedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Room: &model.SubscriptionRoom{
			SiteID:    "site-a",
			UserCount: 42,
			LastMsgAt: &lastMsg,
			LastMsgID: "msg-abc",
		},
	}
	dst := model.Subscription{}
	roundTrip(t, &src, &dst)
}

func TestSubscriptionBaseMetadata_RoundTrip(t *testing.T) {
	favoriteUpdatedAt := time.Date(2026, 2, 1, 9, 0, 0, 0, time.UTC)
	muteUpdatedAt := time.Date(2026, 2, 2, 9, 0, 0, 0, time.UTC)
	rolesUpdatedAt := time.Date(2026, 2, 3, 9, 0, 0, 0, time.UTC)
	nameUpdatedAt := time.Date(2026, 2, 4, 9, 0, 0, 0, time.UTC)
	restrictUpdatedAt := time.Date(2026, 2, 5, 9, 0, 0, 0, time.UTC)
	src := model.Subscription{
		ID:                "s1",
		User:              model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID:            "r1",
		SiteID:            "site-a",
		Roles:             []model.Role{model.RoleMember},
		RoomType:          model.RoomTypeChannel,
		JoinedAt:          time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		HasUnread:         true,
		FavoriteUpdatedAt: &favoriteUpdatedAt,
		MuteUpdatedAt:     &muteUpdatedAt,
		RolesUpdatedAt:    &rolesUpdatedAt,
		NameUpdatedAt:     &nameUpdatedAt,
		RestrictUpdatedAt: &restrictUpdatedAt,
	}
	dst := model.Subscription{}
	roundTrip(t, &src, &dst)

	// hasUnread is always emitted; the nullable metadata is omitted when unset.
	raw := map[string]any{}
	b, err := json.Marshal(&src)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(b, &raw))
	assert.Equal(t, true, raw["hasUnread"])

	zero := map[string]any{}
	zb, err := json.Marshal(&model.Subscription{ID: "z", JoinedAt: time.Now().UTC()})
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(zb, &zero))
	for _, k := range []string{"favoriteUpdatedAt", "muteUpdatedAt", "rolesUpdatedAt", "nameUpdatedAt", "restrictUpdatedAt", "updatedAt"} {
		_, present := zero[k]
		assert.False(t, present, "%q must be omitted when unset", k)
	}
}

func TestSubscription_HasUnreadNotPersisted(t *testing.T) {
	// hasUnread is computed at read time (room lastMsgAt vs lastSeenAt), never
	// stored, so it must not be written to Mongo.
	src := model.Subscription{ID: "s1", RoomID: "r1", SiteID: "site-a", HasUnread: true}
	data, err := bson.Marshal(&src)
	require.NoError(t, err)
	var raw bson.M
	require.NoError(t, bson.Unmarshal(data, &raw))
	_, hasUnread := raw["hasUnread"]
	assert.False(t, hasUnread, `hasUnread must not be persisted (bson:"-")`)
}

func TestSubscription_HasGroupMentionNotPersisted(t *testing.T) {
	// hasGroupMention is computed at read time (room lastMentionAllAt vs lastSeenAt),
	// so it rides the client wire but must never be persisted to Mongo.
	src := model.Subscription{ID: "s1", RoomID: "r1", SiteID: "site-a", HasGroupMention: true}
	bdata, err := bson.Marshal(&src)
	require.NoError(t, err)
	var braw bson.M
	require.NoError(t, bson.Unmarshal(bdata, &braw))
	_, persisted := braw["hasGroupMention"]
	assert.False(t, persisted, `hasGroupMention must not be persisted (bson:"-")`)

	jdata, err := json.Marshal(&model.Subscription{ID: "s1", JoinedAt: time.Now().UTC()})
	require.NoError(t, err)
	var jraw map[string]any
	require.NoError(t, json.Unmarshal(jdata, &jraw))
	_, onWire := jraw["hasGroupMention"]
	assert.True(t, onWire, "hasGroupMention is part of the client wire schema")
}
func TestSubscriptionUpdateEvent_RoomNameRoundTrips(t *testing.T) {
	evt := model.SubscriptionUpdateEvent{
		UserID:       "u1",
		Subscription: model.Subscription{ID: "s1", RoomID: "r1", RoomType: model.RoomTypeDM},
		Action:       "added",
		RoomName:     "Alice Wang",
		Timestamp:    1735689600000,
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"roomName":"Alice Wang"`)

	var dst model.SubscriptionUpdateEvent
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Equal(t, "Alice Wang", dst.RoomName)
}

func TestSubscriptionUpdateEvent_RoomNameOmittedWhenEmpty(t *testing.T) {
	data, err := json.Marshal(&model.SubscriptionUpdateEvent{Action: "mute_toggled"})
	require.NoError(t, err)
	assert.NotContains(t, string(data), "roomName")
}

func TestMigrationRequests_RoundTrip(t *testing.T) {
	ts := time.Date(2023, 1, 2, 3, 4, 5, 0, time.UTC)
	roundTrip(t, &model.MigrationEditRequest{MessageID: "m1", RoomID: "r1", CreatedAt: ts, Content: "edited", EditedAt: ts}, &model.MigrationEditRequest{})
	roundTrip(t, &model.MigrationDeleteRequest{MessageID: "m1", DeletedAt: ts}, &model.MigrationDeleteRequest{})
	roundTrip(t, &model.MigrationAck{OK: true}, &model.MigrationAck{})
}

func TestThreadUnreadSummaryResponseJSON(t *testing.T) {
	ts := int64(1717000000000)
	r := model.ThreadUnreadSummaryResponse{
		Unread: true, UnreadDirectMessage: false, UnreadMention: true,
		LastMessageAt: &ts, UnavailableSites: []string{"site-b"},
	}
	roundTrip(t, &r, &model.ThreadUnreadSummaryResponse{})
}

func TestThreadUnreadSummaryRequestJSON(t *testing.T) {
	roundTrip(t, &model.ThreadUnreadSummaryRequest{}, &model.ThreadUnreadSummaryRequest{})
}

func TestThreadRoomInfoBatchRequestJSON(t *testing.T) {
	r := model.ThreadRoomInfoBatchRequest{ThreadRoomIDs: []string{"tr1", "tr2"}}
	roundTrip(t, &r, &model.ThreadRoomInfoBatchRequest{})
}

func TestThreadRoomInfoBatchResponseJSON(t *testing.T) {
	r := model.ThreadRoomInfoBatchResponse{Threads: []model.ThreadRoomInfo{
		{ThreadRoomID: "tr1", Found: true, LastMsgAt: 1717000000000},
		{ThreadRoomID: "tr2", Found: false},
	}}
	roundTrip(t, &r, &model.ThreadRoomInfoBatchResponse{})
}

func TestThreadUnreadRowJSON(t *testing.T) {
	seen := time.UnixMilli(1717000000000).UTC()
	r := model.ThreadUnreadRow{
		ThreadRoomID: "tr1", RoomID: "r1", SiteID: "site-a", RoomType: model.RoomTypeDM,
		LastSeenAt: &seen, HasMention: true,
	}
	roundTrip(t, &r, &model.ThreadUnreadRow{})
}

func TestOutboxEvent_RoundTrip(t *testing.T) {
	inner := model.InboxEvent{
		Type:       model.InboxSubscriptionMuteToggled,
		SiteID:     "site-a",
		DestSiteID: "site-b",
		Payload:    []byte(`{"account":"alice","roomId":"r1","muted":true,"timestamp":1}`),
		Timestamp:  1,
	}
	innerData, err := json.Marshal(inner)
	require.NoError(t, err)

	evt := model.OutboxEvent{
		RoomID:    "r1",
		Envelope:  innerData,
		DedupID:   "req-1:site-b",
		Timestamp: 2,
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)

	var got model.OutboxEvent
	require.NoError(t, json.Unmarshal(data, &got))
	require.Equal(t, evt, got)
}

func TestLastMessagePreviewJSON(t *testing.T) {
	ts := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	edited := ts.Add(time.Minute)
	p := model.LastMessagePreview{
		MessageID:       "m1",
		Type:            "text",
		SenderAccount:   "alice",
		SenderName:      "Alice Wang",
		Msg:             "hello",
		CreatedAt:       ts,
		EditedAt:        &edited,
		AttachmentCount: 2,
	}
	roundTrip(t, &p, &model.LastMessagePreview{})
}

func TestLastMessagePreview_OmitemptyFields(t *testing.T) {
	ts := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	p := model.LastMessagePreview{MessageID: "m1", SenderAccount: "alice", CreatedAt: ts}
	data, err := json.Marshal(p)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	for _, key := range []string{"type", "senderName", "msg", "encMsg", "editedAt", "attachmentCount"} {
		_, present := raw[key]
		assert.False(t, present, "zero %s must be omitted from JSON", key)
	}
	for _, key := range []string{"messageId", "senderAccount", "createdAt"} {
		_, present := raw[key]
		assert.True(t, present, "%s must always be present in JSON", key)
	}
}

func TestLastMessagePreview_EncMsg(t *testing.T) {
	ts := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)

	t.Run("EncMsg round-trips JSON", func(t *testing.T) {
		p := model.LastMessagePreview{
			MessageID:     "m1",
			SenderAccount: "alice",
			CreatedAt:     ts,
			EncMsg:        json.RawMessage(`{"ciphertext":"abc"}`),
		}
		data, err := json.Marshal(p)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["encMsg"]
		assert.True(t, present, "non-empty EncMsg must appear in JSON as encMsg")
		var dst model.LastMessagePreview
		require.NoError(t, json.Unmarshal(data, &dst))
		assert.JSONEq(t, `{"ciphertext":"abc"}`, string(dst.EncMsg))
	})

	t.Run("EncMsg round-trips BSON", func(t *testing.T) {
		p := model.LastMessagePreview{
			MessageID:     "m1",
			SenderAccount: "alice",
			CreatedAt:     ts,
			EncMsg:        json.RawMessage(`{"ciphertext":"abc"}`),
		}
		data, err := bson.Marshal(p)
		require.NoError(t, err)
		var raw bson.M
		require.NoError(t, bson.Unmarshal(data, &raw))
		_, present := raw["encMsg"]
		assert.True(t, present, "non-empty EncMsg must be present in BSON as encMsg")
		var dst model.LastMessagePreview
		require.NoError(t, bson.Unmarshal(data, &dst))
		assert.JSONEq(t, `{"ciphertext":"abc"}`, string(dst.EncMsg))
	})

	t.Run("nil EncMsg omitted from JSON and BSON", func(t *testing.T) {
		p := model.LastMessagePreview{MessageID: "m1", SenderAccount: "alice", CreatedAt: ts}
		data, err := json.Marshal(p)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["encMsg"]
		assert.False(t, present, "nil EncMsg must be omitted from JSON")

		bdata, err := bson.Marshal(p)
		require.NoError(t, err)
		var braw bson.M
		require.NoError(t, bson.Unmarshal(bdata, &braw))
		_, present = braw["encMsg"]
		assert.False(t, present, "nil EncMsg must be omitted from BSON")
	})
}

func TestLastRoomMessageRequestJSON(t *testing.T) {
	r := model.LastRoomMessageRequest{RoomID: "r1"}
	roundTrip(t, &r, &model.LastRoomMessageRequest{})
}

func TestLastRoomMessageResponseJSON(t *testing.T) {
	ts := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)

	t.Run("with last message round-trips", func(t *testing.T) {
		r := model.LastRoomMessageResponse{LastMessage: &model.LastMessagePreview{
			MessageID: "m1", SenderAccount: "alice", Msg: "hello", CreatedAt: ts,
		}}
		roundTrip(t, &r, &model.LastRoomMessageResponse{})
	})

	t.Run("nil last message omitted from JSON", func(t *testing.T) {
		data, err := json.Marshal(model.LastRoomMessageResponse{})
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["lastMessage"]
		assert.False(t, present, "nil LastMessage must be omitted from JSON")
	})
}

func TestDeleteRoomEvent_LastMessage(t *testing.T) {
	ts := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)

	t.Run("nil LastMessage omitted from JSON", func(t *testing.T) {
		e := model.DeleteRoomEvent{
			Type:      model.RoomEventMessageDeleted,
			RoomID:    "r1",
			SiteID:    "site-a",
			MessageID: "m1",
			DeletedBy: "alice",
			DeletedAt: ts,
			Timestamp: ts.UnixMilli(),
		}
		data, err := json.Marshal(e)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["lastMessage"]
		assert.False(t, present, "nil LastMessage must be omitted from JSON")
	})

	t.Run("non-nil LastMessage round-trips JSON", func(t *testing.T) {
		e := model.DeleteRoomEvent{
			Type:      model.RoomEventMessageDeleted,
			RoomID:    "r1",
			SiteID:    "site-a",
			MessageID: "m1",
			DeletedBy: "alice",
			DeletedAt: ts,
			Timestamp: ts.UnixMilli(),
			LastMessage: &model.LastMessagePreview{
				MessageID: "m0", SenderAccount: "bob", Msg: "previous", CreatedAt: ts,
			},
		}
		data, err := json.Marshal(e)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["lastMessage"]
		assert.True(t, present, "non-nil LastMessage must appear in JSON as lastMessage")
		var dst model.DeleteRoomEvent
		require.NoError(t, json.Unmarshal(data, &dst))
		require.NotNil(t, dst.LastMessage)
		assert.Equal(t, "m0", dst.LastMessage.MessageID)
		assert.Equal(t, "bob", dst.LastMessage.SenderAccount)
	})

	t.Run("encrypted LastMessage carries EncMsg through JSON", func(t *testing.T) {
		e := model.DeleteRoomEvent{
			Type:      model.RoomEventMessageDeleted,
			RoomID:    "r1",
			SiteID:    "site-a",
			MessageID: "m1",
			DeletedBy: "alice",
			DeletedAt: ts,
			Timestamp: ts.UnixMilli(),
			LastMessage: &model.LastMessagePreview{
				MessageID:     "m0",
				SenderAccount: "bob",
				CreatedAt:     ts,
				EncMsg:        json.RawMessage(`{"ciphertext":"abc"}`),
			},
		}
		data, err := json.Marshal(e)
		require.NoError(t, err)
		var dst model.DeleteRoomEvent
		require.NoError(t, json.Unmarshal(data, &dst))
		require.NotNil(t, dst.LastMessage)
		assert.Empty(t, dst.LastMessage.Msg, "encrypted preview must not carry plaintext Msg")
		assert.JSONEq(t, `{"ciphertext":"abc"}`, string(dst.LastMessage.EncMsg))
	})

	t.Run("non-nil LastMessage present in BSON", func(t *testing.T) {
		e := model.DeleteRoomEvent{
			Type:      model.RoomEventMessageDeleted,
			RoomID:    "r1",
			MessageID: "m1",
			Timestamp: ts.UnixMilli(),
			LastMessage: &model.LastMessagePreview{
				MessageID: "m0", SenderAccount: "bob", CreatedAt: ts,
			},
		}
		data, err := bson.Marshal(e)
		require.NoError(t, err)
		var raw bson.M
		require.NoError(t, bson.Unmarshal(data, &raw))
		_, present := raw["lastMessage"]
		assert.True(t, present, "non-nil LastMessage must be present in BSON as lastMessage")
	})

	t.Run("nil LastMessage omitted from BSON", func(t *testing.T) {
		e := model.DeleteRoomEvent{
			Type:      model.RoomEventMessageDeleted,
			RoomID:    "r1",
			MessageID: "m1",
			Timestamp: ts.UnixMilli(),
		}
		data, err := bson.Marshal(e)
		require.NoError(t, err)
		var raw bson.M
		require.NoError(t, bson.Unmarshal(data, &raw))
		_, present := raw["lastMessage"]
		assert.False(t, present, "nil LastMessage must be omitted from BSON")
	})
}

func TestRoom_LastMsg(t *testing.T) {
	ts := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)

	t.Run("nil LastMsg omitted from JSON and BSON", func(t *testing.T) {
		r := model.Room{ID: "r1", Name: "general", Type: model.RoomTypeChannel, SiteID: "site-a"}
		data, err := json.Marshal(r)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["lastMsg"]
		assert.False(t, present, "nil LastMsg must be omitted from JSON")

		bdata, err := bson.Marshal(r)
		require.NoError(t, err)
		var braw bson.M
		require.NoError(t, bson.Unmarshal(bdata, &braw))
		_, present = braw["lastMsg"]
		assert.False(t, present, "nil LastMsg must be omitted from BSON")
	})

	t.Run("non-nil LastMsg round-trips JSON", func(t *testing.T) {
		r := model.Room{
			ID: "r1", Name: "general", Type: model.RoomTypeChannel, SiteID: "site-a",
			LastMsgID: "m1",
			LastMsg: &model.LastMessagePreview{
				MessageID: "m1", SenderAccount: "alice", Msg: "hello", CreatedAt: ts,
			},
		}
		data, err := json.Marshal(r)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))
		_, present := raw["lastMsg"]
		assert.True(t, present, "non-nil LastMsg must appear in JSON as lastMsg")
		var dst model.Room
		require.NoError(t, json.Unmarshal(data, &dst))
		require.NotNil(t, dst.LastMsg)
		assert.Equal(t, "m1", dst.LastMsg.MessageID)
		assert.True(t, dst.LastMsg.CreatedAt.Equal(ts))
	})

	t.Run("non-nil LastMsg present in BSON", func(t *testing.T) {
		r := model.Room{
			ID: "r1", Type: model.RoomTypeChannel,
			LastMsg: &model.LastMessagePreview{MessageID: "m1", SenderAccount: "alice", CreatedAt: ts},
		}
		data, err := bson.Marshal(r)
		require.NoError(t, err)
		var raw bson.M
		require.NoError(t, bson.Unmarshal(data, &raw))
		_, present := raw["lastMsg"]
		assert.True(t, present, "non-nil LastMsg must be present in BSON as lastMsg")
	})
}

func TestTeamsUserJSON(t *testing.T) {
	from := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	src := model.TeamsUser{
		ID:          "8f4c9e2a-0b1d-4e5f-9a6b-7c8d9e0f1a2b",
		UPN:         "Alice@corp.example",
		Account:     "alice",
		DisplayName: "Alice Smith",
		SiteID:      "site-a",
		EngName:     "Alice Smith",
		Mail:        "alice@corp.example",
		From:        &from,
	}
	var dst model.TeamsUser
	roundTrip(t, &src, &dst)
}

func TestTeamsUserJSON_NoFrom(t *testing.T) {
	u := model.TeamsUser{ID: "aad-user-2", UPN: "bob@corp.example", SiteID: "site-b", Account: "bob"}
	data, err := json.Marshal(&u)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	_, has := raw["from"]
	assert.False(t, has, "nil From must be omitted from JSON")
	assert.Equal(t, "", raw["displayName"], "empty DisplayName must serialize as empty string, not be omitted")
	assert.Equal(t, "", raw["engName"], "empty EngName must serialize as empty string, not be omitted")
	assert.Equal(t, "", raw["mail"], "empty Mail must serialize as empty string, not be omitted")
}

func TestTeamsChatJSON(t *testing.T) {
	c := model.TeamsChat{
		ID:                  "19:meeting_abc@thread.v2",
		Name:                "Project X",
		ChatType:            "group",
		CreatedDateTime:     time.Date(2026, 4, 1, 8, 0, 0, 0, time.UTC),
		LastUpdatedDateTime: time.Date(2026, 7, 1, 12, 30, 0, 0, time.UTC),
		Members: []model.TeamsChatMember{
			{ID: "aad-user-1", Account: "alice", VisibleHistoryStartDateTime: time.Date(2026, 4, 1, 8, 0, 0, 0, time.UTC)},
			{ID: "aad-guest-9", Account: "", VisibleHistoryStartDateTime: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)},
		},
		SiteID:         "site-a",
		UpdatedAt:      time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC),
		NeedMemberSync: true,
	}
	roundTrip(t, &c, &model.TeamsChat{})
}

func TestTeamsUserBSON(t *testing.T) {
	from := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	u := model.TeamsUser{ID: "aad-user-1", UPN: "alice@corp.example", SiteID: "site-a", Account: "alice", DisplayName: "Alice Smith", EngName: "Alice Smith", Mail: "alice@corp.example", From: &from}
	data, err := bson.Marshal(&u)
	require.NoError(t, err)

	// Check raw BSON keys — these are the on-disk contract with teams-user-sync.
	var rawDoc bson.M
	require.NoError(t, bson.Unmarshal(data, &rawDoc))
	assert.Contains(t, rawDoc, "_id", "BSON doc must have _id key")
	assert.Contains(t, rawDoc, "siteId", "BSON doc must have siteId key")
	assert.Contains(t, rawDoc, "upn", "BSON doc must have upn key")
	assert.Contains(t, rawDoc, "account", "BSON doc must have account key")
	assert.Contains(t, rawDoc, "from", "BSON doc must have from key when From is non-nil")
	assert.Equal(t, "aad-user-1", rawDoc["_id"])
	assert.Equal(t, "site-a", rawDoc["siteId"])
	assert.Equal(t, "alice", rawDoc["account"])
	assert.Equal(t, "Alice Smith", rawDoc["displayName"])
	assert.Equal(t, "Alice Smith", rawDoc["engName"])
	assert.Equal(t, "alice@corp.example", rawDoc["mail"])

	// Round-trip to struct and verify equality
	var dst model.TeamsUser
	require.NoError(t, bson.Unmarshal(data, &dst))
	assert.Equal(t, u.ID, dst.ID)
	assert.Equal(t, u.SiteID, dst.SiteID)
	assert.Equal(t, u.Account, dst.Account)
	assert.Equal(t, u.DisplayName, dst.DisplayName)
	assert.Equal(t, u.EngName, dst.EngName)
	assert.Equal(t, u.Mail, dst.Mail)
	require.NotNil(t, dst.From)
	assert.True(t, dst.From.UTC().Equal(from.UTC()), "From time must match")
}

func TestTeamsUserBSON_NoFrom(t *testing.T) {
	u := model.TeamsUser{ID: "aad-user-2", SiteID: "site-b", Account: "bob"}
	data, err := bson.Marshal(&u)
	require.NoError(t, err)

	// Check that 'from' key is absent when From is nil
	var rawDoc bson.M
	require.NoError(t, bson.Unmarshal(data, &rawDoc))
	_, has := rawDoc["from"]
	assert.False(t, has, "BSON doc must not have from key when From is nil (omitempty)")
	assert.Equal(t, "aad-user-2", rawDoc["_id"])
	assert.Equal(t, "site-b", rawDoc["siteId"])
	assert.Equal(t, "bob", rawDoc["account"])
	assert.Equal(t, "", rawDoc["displayName"], "empty DisplayName must be present as empty string (no omitempty)")
	assert.Equal(t, "", rawDoc["engName"], "empty EngName must be present as empty string (no omitempty)")
	assert.Equal(t, "", rawDoc["mail"], "empty Mail must be present as empty string (no omitempty)")
}

func TestTeamsChatBSON(t *testing.T) {
	c := model.TeamsChat{
		ID:                  "19:meeting_abc@thread.v2",
		Name:                "Project X",
		ChatType:            "group",
		CreatedDateTime:     time.Date(2026, 4, 1, 8, 0, 0, 0, time.UTC),
		LastUpdatedDateTime: time.Date(2026, 7, 1, 12, 30, 0, 0, time.UTC),
		Members: []model.TeamsChatMember{
			{ID: "aad-user-1", Account: "alice", VisibleHistoryStartDateTime: time.Date(2026, 4, 1, 8, 0, 0, 0, time.UTC)},
			{ID: "aad-guest-9", Account: "", VisibleHistoryStartDateTime: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)},
		},
		SiteID:         "site-a",
		UpdatedAt:      time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC),
		NeedMemberSync: true,
	}
	data, err := bson.Marshal(&c)
	require.NoError(t, err)

	// Check raw BSON keys
	var rawDoc bson.M
	require.NoError(t, bson.Unmarshal(data, &rawDoc))
	assert.Contains(t, rawDoc, "_id", "BSON doc must have _id key")
	assert.Contains(t, rawDoc, "siteId", "BSON doc must have siteID key")
	assert.Contains(t, rawDoc, "needMemberSync", "BSON doc must have needMemberSync key")
	assert.Contains(t, rawDoc, "members", "BSON doc must have members key")
	assert.Equal(t, "19:meeting_abc@thread.v2", rawDoc["_id"])
	assert.Equal(t, "site-a", rawDoc["siteId"])
	assert.Equal(t, true, rawDoc["needMemberSync"])

	// Round-trip to struct and verify equality
	var dst model.TeamsChat
	require.NoError(t, bson.Unmarshal(data, &dst))
	assert.Equal(t, c.ID, dst.ID)
	assert.Equal(t, c.Name, dst.Name)
	assert.Equal(t, c.ChatType, dst.ChatType)
	assert.True(t, c.CreatedDateTime.UTC().Equal(dst.CreatedDateTime.UTC()), "CreatedDateTime must match")
	assert.True(t, c.LastUpdatedDateTime.UTC().Equal(dst.LastUpdatedDateTime.UTC()), "LastUpdatedDateTime must match")
	assert.Equal(t, c.SiteID, dst.SiteID)
	assert.True(t, c.UpdatedAt.UTC().Equal(dst.UpdatedAt.UTC()), "UpdatedAt must match")
	assert.Equal(t, c.NeedMemberSync, dst.NeedMemberSync)
	require.Equal(t, len(c.Members), len(dst.Members), "Members count must match")
	for i, member := range c.Members {
		assert.Equal(t, member.ID, dst.Members[i].ID)
		assert.Equal(t, member.Account, dst.Members[i].Account)
		assert.True(t, member.VisibleHistoryStartDateTime.UTC().Equal(dst.Members[i].VisibleHistoryStartDateTime.UTC()), "VisibleHistoryStartDateTime must match")
	}
}

func TestTeamsChatJSON_NeedCreateRoom(t *testing.T) {
	c := model.TeamsChat{
		ID:                  "19:g1@thread.v2",
		ChatType:            "group",
		CreatedDateTime:     time.Date(2026, 4, 1, 8, 0, 0, 0, time.UTC),
		LastUpdatedDateTime: time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC),
		Members:             []model.TeamsChatMember{{ID: "u1", Account: "alice"}},
		SiteID:              "site-a",
		UpdatedAt:           time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC),
		NeedMemberSync:      false,
		NeedCreateRoom:      true,
	}
	roundTrip(t, &c, &model.TeamsChat{})

	data, err := bson.Marshal(&c)
	require.NoError(t, err)
	var raw bson.M
	require.NoError(t, bson.Unmarshal(data, &raw))
	assert.Contains(t, raw, "needCreateRoom", "BSON doc must have needCreateRoom key")
	assert.Equal(t, true, raw["needCreateRoom"])
}

func TestTeamsRoomCreateEventJSON(t *testing.T) {
	e := model.TeamsRoomCreateEvent{
		Chats: []model.TeamsRoomCreateChat{{
			ID:              "chat-1",
			Name:            "Project X",
			CreatedDateTime: time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC),
			Members: []model.TeamsRoomCreateMember{{
				ID:                          "aad-user-1",
				Account:                     "alice",
				VisibleHistoryStartDateTime: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
			}},
		}},
		Timestamp: 1_700_000_000_000,
	}
	roundTrip(t, &e, &model.TeamsRoomCreateEvent{})
}

func TestSSOTokenJSON(t *testing.T) {
	// Secrets carry json:"-" so a src with them unset round-trips cleanly.
	src := model.SSOToken{
		ID:         "abc123",
		Username:   "alice",
		IDTokenExp: 1735689600000,
		UpdatedAt:  time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC),
	}
	var dst model.SSOToken
	roundTrip(t, &src, &dst)
}

func TestSSOToken_SecretsNeverSerialize(t *testing.T) {
	tok := model.SSOToken{
		ID: "abc123", Username: "alice",
		IDToken: "SECRET-ACCESS-TOKEN", RefreshToken: "SECRET-REFRESH-TOKEN",
	}
	data, err := json.Marshal(tok)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(data), "SECRET") {
		t.Errorf("token material leaked into JSON: %s", data)
	}
	if s := tok.String(); strings.Contains(s, "SECRET") {
		t.Errorf("token material leaked into String(): %s", s)
	}
}
