package models

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestSubscriptionListRequest_RoundTrip(t *testing.T) {
	fav := true
	days := 7
	in := SubscriptionListRequest{Type: "rooms", Favorite: &fav, UpdatedWithinDays: &days, Offset: 80, Limit: 20}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out SubscriptionListRequest
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, in, out)
}

func TestSubscriptionListResponse_Marshal(t *testing.T) {
	// Subscriptions is a []model.SubscriptionItem (interface) — a server-only
	// response type, so this verifies the marshaled wire shape (it is never
	// unmarshaled back into the interface slice).
	joined := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	in := SubscriptionListResponse{
		Subscriptions: []model.SubscriptionItem{
			&model.ChannelSubscription{Subscription: &model.Subscription{
				ID: "s1", RoomID: "r1", SiteID: "site-a", Name: "General", JoinedAt: joined, RoomType: model.RoomTypeChannel,
			}},
		},
		Total: 1,
	}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out struct {
		Subscriptions []map[string]any `json:"subscriptions"`
		Total         int              `json:"total"`
	}
	require.NoError(t, json.Unmarshal(b, &out))
	require.Len(t, out.Subscriptions, 1)
	require.Equal(t, "s1", out.Subscriptions[0]["id"])
	require.Equal(t, "General", out.Subscriptions[0]["name"])
	_, hasApp := out.Subscriptions[0]["app"]
	require.False(t, hasApp, "channel row carries no app object")
	_, hasHR := out.Subscriptions[0]["hrInfo"]
	require.False(t, hasHR, "channel row carries no hrInfo")
	require.Equal(t, 1, out.Total)
}

func TestPagedSubscriptionListResponse_Marshal(t *testing.T) {
	joined := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	in := PagedSubscriptionListResponse{
		Subscriptions: []model.SubscriptionItem{
			&model.ChannelSubscription{Subscription: &model.Subscription{
				ID: "s1", RoomID: "r1", SiteID: "site-a", Name: "General", JoinedAt: joined, RoomType: model.RoomTypeChannel,
			}},
		},
		HasMore: true,
	}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out struct {
		Subscriptions []map[string]any `json:"subscriptions"`
		HasMore       bool             `json:"hasMore"`
	}
	require.NoError(t, json.Unmarshal(b, &out))
	require.Len(t, out.Subscriptions, 1)
	require.Equal(t, "s1", out.Subscriptions[0]["id"])
	require.True(t, out.HasMore, "hasMore signals another page follows")
}

// TestSubscriptionItem_HeterogeneousRows pins the wire shape per room type:
// channel = base only; dm adds top-level hrInfo; botDM adds a nested app object
// (appId/name/description/assistant/…) and carries NO hrInfo.
func TestSubscriptionItem_HeterogeneousRows(t *testing.T) {
	t.Run("channel row is base only", func(t *testing.T) {
		item := &model.ChannelSubscription{
			Subscription: &model.Subscription{ID: "c1", RoomID: "rc1", SiteID: "site-a", Name: "general", RoomType: model.RoomTypeChannel},
		}
		raw := marshalToMap(t, item)
		require.Equal(t, "general", raw["name"])
		_, hasHR := raw["hrInfo"]
		require.False(t, hasHR, "channel row must not carry hrInfo")
		_, hasApp := raw["app"]
		require.False(t, hasApp, "channel row must not carry an app object")
	})

	t.Run("dm row adds top-level hrInfo", func(t *testing.T) {
		item := &model.DMSubscription{
			Subscription: &model.Subscription{ID: "d1", RoomID: "rd1", SiteID: "site-a", Name: "bob", RoomType: model.RoomTypeDM},
			HRInfo:       &model.SubscriptionHRInfo{Account: "bob", Name: "鮑勃", EngName: "Bob Chen"},
		}
		raw := marshalToMap(t, item)
		require.Equal(t, "bob", raw["name"])
		hr, ok := raw["hrInfo"].(map[string]any)
		require.True(t, ok, "dm row must carry a top-level hrInfo object")
		require.Equal(t, "鮑勃", hr["name"])
		require.Equal(t, "Bob Chen", hr["engName"])
		_, hasApp := raw["app"]
		require.False(t, hasApp, "dm row must not carry an app object")
	})

	t.Run("botDM row nests app metadata under app and carries no hrInfo", func(t *testing.T) {
		item := &model.BotDMSubscription{
			Subscription: &model.Subscription{ID: "b1", RoomID: "rb1", SiteID: "site-a", Name: "Helper App", RoomType: model.RoomTypeBotDM},
			App: model.AppSubscriptionFromApp(&model.App{
				ID:          "app-helper",
				Name:        "Helper App",
				Description: "does helpful things",
				Assistant:   &model.AppAssistant{Enabled: true, Name: "helper.bot", Username: "Helper"},
				Version:     "1.0.0",
			}),
		}
		raw := marshalToMap(t, item)
		require.Equal(t, "Helper App", raw["name"], "base subscription name carries the app display name")
		app, ok := raw["app"].(map[string]any)
		require.True(t, ok, "botDM row must carry a nested app object")
		require.Equal(t, "app-helper", app["appId"])
		require.Equal(t, "Helper App", app["name"], "app object carries its own display name")
		require.Equal(t, "does helpful things", app["description"])
		require.Equal(t, "1.0.0", app["version"])
		assistant, ok := app["assistant"].(map[string]any)
		require.True(t, ok, "assistant nests inside the app object")
		require.Equal(t, "helper.bot", assistant["name"])
		_, hasHR := raw["hrInfo"]
		require.False(t, hasHR, "botDM row must not carry hrInfo")
	})
}

func marshalToMap(t *testing.T, v any) map[string]any {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(b, &raw))
	return raw
}

func TestGetChannelsRequest_RoundTrip(t *testing.T) {
	in := GetChannelsRequest{AccountNames: []string{"alice", "bob"}}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out GetChannelsRequest
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, in, out)
}

func TestDMResponse_RoundTrip(t *testing.T) {
	joined := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	in := DMResponse{
		Subscription: model.DMSubscription{
			Subscription: &model.Subscription{ID: "d1", RoomID: "r1", SiteID: "site-a", JoinedAt: joined},
			HRInfo:       &model.SubscriptionHRInfo{Account: "bob", Name: "Bob Chen", EngName: "Bob"},
		},
	}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out DMResponse
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, in, out)
}

func TestCountResponse_RoundTrip(t *testing.T) {
	in := CountResponse{Count: 42}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out CountResponse
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, in, out)
}

func TestGetDMRequest_RoundTrip(t *testing.T) {
	in := GetDMRequest{AccountName: "bob"}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out GetDMRequest
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, in, out)
}

func TestGetByRoomIDRequest_RoundTrip(t *testing.T) {
	in := GetByRoomIDRequest{RoomID: "r1"}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out GetByRoomIDRequest
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, in, out)
}

func TestCountRequest_RoundTrip(t *testing.T) {
	t.Run("UnreadNil omits key", func(t *testing.T) {
		in := CountRequest{Unread: nil}
		b, err := json.Marshal(in)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(b, &raw))
		_, present := raw["unread"]
		require.False(t, present, "unread key must be absent when Unread is nil")
		var out CountRequest
		require.NoError(t, json.Unmarshal(b, &out))
		require.Equal(t, in, out)
	})

	t.Run("UnreadTrue round-trips to non-nil true", func(t *testing.T) {
		unread := true
		in := CountRequest{Unread: &unread}
		b, err := json.Marshal(in)
		require.NoError(t, err)
		var out CountRequest
		require.NoError(t, json.Unmarshal(b, &out))
		require.Equal(t, in, out)
		require.NotNil(t, out.Unread, "Unread must not be nil after round-trip")
		require.True(t, *out.Unread, "Unread must be true after round-trip")
	})

	t.Run("UnreadFalse preserves explicit false on the wire", func(t *testing.T) {
		in := CountRequest{Unread: ptrBool(false)}
		b, err := json.Marshal(in)
		require.NoError(t, err)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(b, &raw))
		val, present := raw["unread"]
		require.True(t, present, "non-nil Unread must be present even when false")
		require.Equal(t, false, val, "explicit false must be preserved on the wire")
		var out CountRequest
		require.NoError(t, json.Unmarshal(b, &out))
		require.Equal(t, in, out)
		require.NotNil(t, out.Unread, "Unread must not be nil after round-trip")
		require.False(t, *out.Unread, "Unread must be false after round-trip")
	})
}
