package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/timeutil"
	"github.com/hmchangw/chat/user-service/config"
	"github.com/hmchangw/chat/user-service/models"
	"github.com/hmchangw/chat/user-service/service/mocks"
)

// newSvcRawHistory builds a service exposing the history mock WITHOUT newSvc's
// permissive RoomsGet default, so last-message enrichment tests can set an exact
// RoomsGet expectation (result or error).
func newSvcRawHistory(t *testing.T) (*UserService, *mocks.MockSubscriptionRepository, *mocks.MockRoomClient, *mocks.MockHistoryClient) {
	t.Helper()
	ctrl := gomock.NewController(t)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	users := mocks.NewMockUserRepository(ctrl)
	apps := mocks.NewMockAppRepository(ctrl)
	rooms := mocks.NewMockRoomClient(ctrl)
	history := mocks.NewMockHistoryClient(ctrl)
	presence := mocks.NewMockPresenceClient(ctrl)
	pub := mocks.NewMockEventPublisher(ctrl)
	threadSubs := mocks.NewMockThreadSubscriptionRepository(ctrl)
	cfg := &config.Config{SiteID: "site-a", AllSiteIDs: []string{"site-a", "site-b"}, MaxSubscriptionLimit: 1000, DefaultSubscriptionLimit: 40, MaxAppsLimit: 100, DefaultAppsLimit: 20, MaxAccountNames: 100, BadgeCountCap: 10, RoomBatchChunk: 100, MaxSiteFanout: 8}
	return New(subs, users, apps, threadSubs, rooms, history, presence, pub, pub, &fakeBadgeCache{}, nil, nil, nil, cfg), subs, rooms, history
}

func TestListSubscriptions_Types(t *testing.T) {
	for _, typ := range []string{"current", "rooms", "apps"} {
		t.Run(typ, func(t *testing.T) {
			svc, subs, _, _, rooms, _, _ := newSvc(t)
			subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", typ, false, gomock.Any(), gomock.Any()).
				Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{Data: []model.EnrichedSubscription{{Subscription: model.Subscription{ID: "s1"}}}}, nil)
			rooms.EXPECT().GetRoomsInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
			resp, err := svc.ListSubscriptions(ctx("alice", "site-a"), models.SubscriptionListRequest{Type: typ})
			require.NoError(t, err)
			assert.Len(t, resp.Subscriptions, 1)
		})
	}
}

func TestListSubscriptions_BadType(t *testing.T) {
	for _, typ := range []string{"", "bogus"} {
		t.Run(typ, func(t *testing.T) {
			svc, _, _, _, _, _, _ := newSvc(t)
			_, err := svc.ListSubscriptions(ctx("alice", "site-a"), models.SubscriptionListRequest{Type: typ})
			requireCode(t, err, errcode.CodeBadRequest)
		})
	}
}

func TestListSubscriptions_NegativeWithinDays(t *testing.T) {
	svc, _, _, _, _, _, _ := newSvc(t)
	neg := -1
	_, err := svc.ListSubscriptions(ctx("alice", "site-a"),
		models.SubscriptionListRequest{Type: "rooms", UpdatedWithinDays: &neg})
	requireCode(t, err, errcode.CodeBadRequest)
}

func TestApplyRoomInfo_NestedRoom(t *testing.T) {
	seen := time.UnixMilli(100).UTC()
	lastMsg := int64(200)
	lastMention := int64(50)
	minSeen := int64(150)
	pk := "a2V5LWJhc2U2NA=="
	kv := 3
	// Stored alert/hasMention are the opposite of what a room-timestamp compare
	// would yield — they must survive applyRoomInfo untouched.
	sub := model.Subscription{Name: "helper.bot", SiteID: "site-a", RoomID: "r1", LastSeenAt: &seen, Alert: false, HasMention: true}
	info := model.RoomInfo{
		RoomID: "r1", Found: true, SiteID: "site-a", Name: "Canonical",
		UserCount: 7, AppCount: 2, LastMsgAt: &lastMsg, LastMsgID: "m9",
		LastMentionAllAt: &lastMention, MinUserLastSeenAt: &minSeen, PrivateKey: &pk, KeyVersion: &kv,
	}
	applyRoomInfo(&sub, &info)
	assert.Equal(t, "helper.bot", sub.Name, "room canonical name must not overwrite the subscription name")
	require.NotNil(t, sub.Room)
	assert.Equal(t, "site-a", sub.Room.SiteID)
	assert.Equal(t, "Canonical", sub.Room.Name)
	assert.Equal(t, 7, sub.Room.UserCount)
	assert.Equal(t, 2, sub.Room.AppCount)
	assert.Equal(t, "m9", sub.Room.LastMsgID)
	require.NotNil(t, sub.Room.LastMsgAt)
	assert.Equal(t, time.UnixMilli(lastMsg).UTC(), *sub.Room.LastMsgAt)
	require.NotNil(t, sub.Room.LastMentionAllAt)
	assert.Equal(t, time.UnixMilli(lastMention).UTC(), *sub.Room.LastMentionAllAt)
	require.NotNil(t, sub.Room.MinUserLastSeenAt)
	assert.Equal(t, time.UnixMilli(minSeen).UTC(), *sub.Room.MinUserLastSeenAt, "cross-site min-seen converts epoch millis → RFC3339 time")
	require.NotNil(t, sub.Room.PrivateKey, "private key must be forwarded, not dropped")
	assert.Equal(t, pk, *sub.Room.PrivateKey)
	require.NotNil(t, sub.Room.KeyVersion)
	assert.Equal(t, 3, *sub.Room.KeyVersion)
	assert.False(t, sub.Alert, "stored alert must not be recomputed from room data")
	assert.True(t, sub.HasMention, "stored hasMention must not be recomputed from room data")
}

func TestApplyRoomInfo_NotFound_NoRoom(t *testing.T) {
	sub := model.Subscription{Name: "general", SiteID: "site-a", RoomID: "r1"}
	applyRoomInfo(&sub, &model.RoomInfo{RoomID: "r1", Found: false})
	assert.Nil(t, sub.Room)
	assert.Equal(t, "general", sub.Name)
}

// A LOCAL sub's minUserLastSeenAt comes from the flat $lookup baseline (already
// *time.Time), so buildLocalRoom passes it through unconverted onto sub.Room.
func TestBuildLocalRoom_MinUserLastSeenAt(t *testing.T) {
	floor := time.UnixMilli(300).UTC()
	sub := model.EnrichedSubscription{
		Subscription:      model.Subscription{SiteID: "site-a"},
		RoomName:          "Eng",
		MinUserLastSeenAt: &floor,
	}
	room := buildLocalRoom(&sub)
	require.NotNil(t, room)
	require.NotNil(t, room.MinUserLastSeenAt, "local baseline minUserLastSeenAt must reach the room object")
	assert.Equal(t, floor, *room.MinUserLastSeenAt)
}

// A LOCAL sub is enriched entirely from the single $lookup baseline (room
// metadata + key) — no room-service RPC and no separate key read. A sub whose
// baseline carries no key still yields the baseline room object, just keyless.
func TestListSubscriptions_LocalBaselineRoom_NoKey(t *testing.T) {
	svc, subs, _, _, _, _, _ := newSvc(t)
	lastMsg := time.UnixMilli(400).UTC()
	storeSubs := []model.EnrichedSubscription{{
		Subscription: model.Subscription{ID: "s1", RoomID: "r1", SiteID: "site-a", Name: "general", RoomType: model.RoomTypeChannel},
		RoomName:     "General", UserCount: 9, LastMsgAt: &lastMsg, LastMsgID: "m1",
	}}
	subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "current", false, gomock.Any(), gomock.Any()).Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{Data: storeSubs}, nil)
	resp, err := svc.ListSubscriptions(ctx("alice", "site-a"), models.SubscriptionListRequest{Type: "current"})
	require.NoError(t, err)
	require.Len(t, resp.Subscriptions, 1)
	room := resp.Subscriptions[0].Base().Room
	require.NotNil(t, room, "local sub yields a baseline room object from the $lookup values")
	assert.Equal(t, "site-a", room.SiteID)
	assert.Equal(t, "General", room.Name)
	assert.Equal(t, 9, room.UserCount)
	assert.Equal(t, "m1", room.LastMsgID)
	require.NotNil(t, room.LastMsgAt)
	assert.Equal(t, lastMsg, *room.LastMsgAt)
	assert.Nil(t, room.PrivateKey, "no baseline key ⇒ no key material")
}

func appHelper() *model.App {
	return &model.App{
		ID:          "app-helper",
		Name:        "Helper App",
		Description: "does helpful things",
		Assistant:   &model.AppAssistant{Enabled: true, Name: "helper.bot", Username: "Helper"},
		Version:     "1.0.0",
	}
}

func TestListSubscriptions_BotDM_AppDisplayNameAndMeta(t *testing.T) {
	svc, subs, _, apps, _, _, _ := newSvc(t)
	storeSubs := []model.EnrichedSubscription{
		{Subscription: model.Subscription{ID: "a1", RoomID: "rb1", SiteID: "site-a", RoomType: model.RoomTypeBotDM, Name: "helper.bot"}, RoomName: "bot-room-canonical"},
		{Subscription: model.Subscription{ID: "c1", RoomID: "rc1", SiteID: "site-a", RoomType: model.RoomTypeChannel, Name: "general"}, RoomName: "general"},
	}
	subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "current", false, gomock.Any(), gomock.Any()).Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{Data: storeSubs}, nil)
	apps.EXPECT().GetAppsByAssistants(gomock.Any(), []string{"helper.bot"}).
		Return(map[string]*model.App{"helper.bot": appHelper()}, nil)
	resp, err := svc.ListSubscriptions(ctx("alice", "site-a"), models.SubscriptionListRequest{Type: "current"})
	require.NoError(t, err)
	require.Len(t, resp.Subscriptions, 2)
	// botDM row: app display name + nested app object (type guarantees no hrInfo).
	bot, ok := resp.Subscriptions[0].(*model.BotDMSubscription)
	require.True(t, ok, "row 0 must be a botDM subscription")
	assert.Equal(t, "Helper App", bot.Name, "botDM name must be replaced by the app display name")
	require.NotNil(t, bot.Room)
	assert.Equal(t, "bot-room-canonical", bot.Room.Name)
	require.NotNil(t, bot.App, "botDM row must carry the nested app object")
	assert.Equal(t, "app-helper", bot.App.AppID, "AppID must come from App.ID")
	assert.Equal(t, "Helper App", bot.App.Name, "app object carries the app display name")
	assert.Equal(t, "does helpful things", bot.App.Description)
	assert.Equal(t, "1.0.0", bot.App.Version)
	require.NotNil(t, bot.App.Assistant)
	assert.Equal(t, "helper.bot", bot.App.Assistant.Name)
	// channel row: base only (type guarantees no app/hrInfo).
	ch, ok := resp.Subscriptions[1].(*model.ChannelSubscription)
	require.True(t, ok, "row 1 must be a channel subscription")
	assert.Equal(t, "general", ch.Name, "channel name must stay the subscription name")
}

func TestListSubscriptions_DM_CarriesHRInfo(t *testing.T) {
	svc, subs, users, _, _, _, _ := newSvc(t)
	storeSubs := []model.EnrichedSubscription{
		{Subscription: model.Subscription{ID: "d1", RoomID: "rd1", SiteID: "site-a", RoomType: model.RoomTypeDM, Name: "bob"}},
		{Subscription: model.Subscription{ID: "c1", RoomID: "rc1", SiteID: "site-a", RoomType: model.RoomTypeChannel, Name: "general"}},
	}
	subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "current", false, gomock.Any(), gomock.Any()).Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{Data: storeSubs}, nil)
	users.EXPECT().GetHRInfoByAccounts(gomock.Any(), []string{"bob"}).
		Return(map[string]*model.SubscriptionHRInfo{"bob": {Account: "bob", Name: "鮑勃", EngName: "Bob Chen"}}, nil)
	resp, err := svc.ListSubscriptions(ctx("alice", "site-a"), models.SubscriptionListRequest{Type: "current"})
	require.NoError(t, err)
	require.Len(t, resp.Subscriptions, 2)
	dm, ok := resp.Subscriptions[0].(*model.DMSubscription)
	require.True(t, ok, "row 0 must be a dm subscription")
	require.NotNil(t, dm.HRInfo, "dm row must carry hrInfo")
	assert.Equal(t, "鮑勃", dm.HRInfo.Name)
	assert.Equal(t, "Bob Chen", dm.HRInfo.EngName)
	_, isChannel := resp.Subscriptions[1].(*model.ChannelSubscription)
	assert.True(t, isChannel, "row 1 must be a channel subscription (no hrInfo)")
}

func TestListSubscriptions_DM_HRLookupDegrades(t *testing.T) {
	svc, subs, users, _, _, _, _ := newSvc(t)
	storeSubs := []model.EnrichedSubscription{
		{Subscription: model.Subscription{ID: "d1", RoomID: "rd1", SiteID: "site-a", RoomType: model.RoomTypeDM, Name: "bob"}},
	}
	subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "current", false, gomock.Any(), gomock.Any()).Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{Data: storeSubs}, nil)
	users.EXPECT().GetHRInfoByAccounts(gomock.Any(), []string{"bob"}).Return(nil, errors.New("db down"))
	resp, err := svc.ListSubscriptions(ctx("alice", "site-a"), models.SubscriptionListRequest{Type: "current"})
	require.NoError(t, err, "hr lookup failure must degrade, not fail the request")
	require.Len(t, resp.Subscriptions, 1)
	dm, ok := resp.Subscriptions[0].(*model.DMSubscription)
	require.True(t, ok, "row 0 must be a dm subscription")
	assert.Equal(t, "bob", dm.Name, "degraded lookup keeps the counterpart account name")
	assert.Nil(t, dm.HRInfo, "degraded hr lookup omits hrInfo")
}

// Two botDM subs sharing a bot account must dedup to a single GetAppsByAssistants
// argument, and both rows get the resolved display name and overlay.
func TestListSubscriptions_BotDM_DedupsBotAccount(t *testing.T) {
	svc, subs, _, apps, _, _, _ := newSvc(t)
	storeSubs := []model.EnrichedSubscription{
		{Subscription: model.Subscription{ID: "a1", RoomID: "rb1", SiteID: "site-a", RoomType: model.RoomTypeBotDM, Name: "helper.bot"}},
		{Subscription: model.Subscription{ID: "a2", RoomID: "rb2", SiteID: "site-a", RoomType: model.RoomTypeBotDM, Name: "helper.bot"}},
	}
	subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "apps", false, gomock.Any(), gomock.Any()).Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{Data: storeSubs}, nil)
	// Exactly ["helper.bot"], not duplicated — gomock fails the call on arg mismatch.
	apps.EXPECT().GetAppsByAssistants(gomock.Any(), []string{"helper.bot"}).
		Return(map[string]*model.App{"helper.bot": appHelper()}, nil)
	resp, err := svc.ListSubscriptions(ctx("alice", "site-a"), models.SubscriptionListRequest{Type: "apps"})
	require.NoError(t, err)
	require.Len(t, resp.Subscriptions, 2)
	b0, ok := resp.Subscriptions[0].(*model.BotDMSubscription)
	require.True(t, ok)
	b1, ok := resp.Subscriptions[1].(*model.BotDMSubscription)
	require.True(t, ok)
	assert.Equal(t, "Helper App", b0.Name)
	assert.Equal(t, "Helper App", b1.Name)
	require.NotNil(t, b0.App)
	require.NotNil(t, b1.App)
}

func TestListSubscriptions_BotDM_AppLookupDegrades(t *testing.T) {
	svc, subs, _, apps, _, _, _ := newSvc(t)
	storeSubs := []model.EnrichedSubscription{
		{Subscription: model.Subscription{ID: "a1", RoomID: "rb1", SiteID: "site-a", RoomType: model.RoomTypeBotDM, Name: "helper.bot"}},
	}
	subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "apps", false, gomock.Any(), gomock.Any()).Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{Data: storeSubs}, nil)
	apps.EXPECT().GetAppsByAssistants(gomock.Any(), []string{"helper.bot"}).
		Return(nil, errors.New("db down"))
	resp, err := svc.ListSubscriptions(ctx("alice", "site-a"), models.SubscriptionListRequest{Type: "apps"})
	require.NoError(t, err, "app lookup failure must degrade, not fail the request")
	require.Len(t, resp.Subscriptions, 1)
	bot, ok := resp.Subscriptions[0].(*model.BotDMSubscription)
	require.True(t, ok, "row 0 must be a botDM subscription")
	assert.Equal(t, "helper.bot", bot.Name, "degraded lookup keeps the bot account name")
	assert.Nil(t, bot.App, "degraded app lookup omits the app object")
}

func TestListSubscriptions_BotDM_NoAppMatch(t *testing.T) {
	svc, subs, _, apps, _, _, _ := newSvc(t)
	storeSubs := []model.EnrichedSubscription{
		{Subscription: model.Subscription{ID: "a1", RoomID: "rb1", SiteID: "site-a", RoomType: model.RoomTypeBotDM, Name: "orphan.bot"}},
	}
	subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "apps", false, gomock.Any(), gomock.Any()).Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{Data: storeSubs}, nil)
	apps.EXPECT().GetAppsByAssistants(gomock.Any(), []string{"orphan.bot"}).
		Return(map[string]*model.App{}, nil)
	resp, err := svc.ListSubscriptions(ctx("alice", "site-a"), models.SubscriptionListRequest{Type: "apps"})
	require.NoError(t, err)
	bot, ok := resp.Subscriptions[0].(*model.BotDMSubscription)
	require.True(t, ok, "row 0 must be a botDM subscription")
	assert.Equal(t, "orphan.bot", bot.Name, "unmatched bot keeps the account name")
	assert.Nil(t, bot.App, "unmatched bot omits the app object")
}

func TestListSubscriptions_StoreError(t *testing.T) {
	svc, subs, _, _, _, _, _ := newSvc(t)
	subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "current", false, gomock.Any(), gomock.Any()).
		Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{}, errors.New("db down"))
	_, err := svc.ListSubscriptions(ctx("alice", "site-a"), models.SubscriptionListRequest{Type: "current"})
	requireCode(t, err, errcode.CodeInternal)
}

func TestListSubscriptions_Favorite(t *testing.T) {
	svc, subs, users, _, rooms, _, _ := newSvc(t)
	// Favorite filtering + self-DM ordering now happen in the query, so the repo
	// returns the already-filtered, self-first set; the service passes it through.
	// The handler must forward favorite=true to the store.
	storeSubs := []model.EnrichedSubscription{
		{Subscription: model.Subscription{ID: "self", RoomType: model.RoomTypeDM, Name: "alice", Favorite: true}},
		{Subscription: model.Subscription{ID: "ch2", RoomType: model.RoomTypeChannel, Name: "random", Favorite: true}},
	}
	subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "current", true, gomock.Any(), gomock.Any()).
		Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{Data: storeSubs}, nil)
	users.EXPECT().GetHRInfoByAccounts(gomock.Any(), []string{"alice"}).
		Return(map[string]*model.SubscriptionHRInfo{"alice": {Account: "alice", Name: "Alice"}}, nil)
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	resp, err := svc.ListSubscriptions(ctx("alice", "site-a"), models.SubscriptionListRequest{
		Type:     "current",
		Favorite: ptrBool(true),
	})
	require.NoError(t, err)
	require.Len(t, resp.Subscriptions, 2)
	assert.Equal(t, "self", resp.Subscriptions[0].Base().ID, "favorite query returns the self-DM first")
	assert.Equal(t, "ch2", resp.Subscriptions[1].Base().ID)
}

func TestListSubscriptions_Pagination(t *testing.T) {
	// capturePage records the OffsetPageRequest the handler forwards and returns a
	// page carrying the given hasMore flag.
	capturePage := func(into *mongoutil.OffsetPageRequest, hasMore bool) func(context.Context, string, string, bool, *int, mongoutil.OffsetPageRequest) (mongoutil.OffsetPageHasMore[model.EnrichedSubscription], error) {
		return func(_ context.Context, _, _ string, _ bool, _ *int, page mongoutil.OffsetPageRequest) (mongoutil.OffsetPageHasMore[model.EnrichedSubscription], error) {
			*into = page
			return mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{Data: []model.EnrichedSubscription{}, HasMore: hasMore}, nil
		}
	}

	t.Run("omitted params default to offset 0 / configured page size", func(t *testing.T) {
		svc, subs, _, _, rooms, _, _ := newSvc(t)
		var got mongoutil.OffsetPageRequest
		subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "current", false, gomock.Any(), gomock.Any()).
			DoAndReturn(capturePage(&got, true))
		rooms.EXPECT().GetRoomsInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
		resp, err := svc.ListSubscriptions(ctx("alice", "site-a"), models.SubscriptionListRequest{Type: "current"})
		require.NoError(t, err)
		assert.Equal(t, int64(0), got.Offset)
		assert.Equal(t, int64(40), got.Limit, "omitted limit ⇒ default page size 40")
		assert.True(t, resp.HasMore, "hasMore is forwarded from the repo page")
	})

	t.Run("negative offset clamps to 0 and limit caps at MaxSubscriptionLimit", func(t *testing.T) {
		svc, subs, _, _, rooms, _, _ := newSvc(t)
		var got mongoutil.OffsetPageRequest
		subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "rooms", false, gomock.Any(), gomock.Any()).
			DoAndReturn(capturePage(&got, false))
		rooms.EXPECT().GetRoomsInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
		_, err := svc.ListSubscriptions(ctx("alice", "site-a"), models.SubscriptionListRequest{Type: "rooms", Offset: -5, Limit: 9999})
		require.NoError(t, err)
		assert.Equal(t, int64(0), got.Offset)
		assert.Equal(t, int64(1000), got.Limit)
	})

	t.Run("explicit in-range offset and limit are forwarded to the repo", func(t *testing.T) {
		svc, subs, _, _, rooms, _, _ := newSvc(t)
		var got mongoutil.OffsetPageRequest
		subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "apps", false, gomock.Any(), gomock.Any()).
			DoAndReturn(capturePage(&got, false))
		rooms.EXPECT().GetRoomsInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
		resp, err := svc.ListSubscriptions(ctx("alice", "site-a"), models.SubscriptionListRequest{Type: "apps", Offset: 80, Limit: 20})
		require.NoError(t, err)
		assert.Equal(t, int64(80), got.Offset)
		assert.Equal(t, int64(20), got.Limit)
		assert.False(t, resp.HasMore)
	})
}

func TestNormalizePage(t *testing.T) {
	cases := []struct {
		name                  string
		defaultLimit, maxSubs int
		offset, limit         int
		wantOffset            int64
		wantLimit             int64
	}{
		{"omitted limit uses the default limit", 40, 1000, 0, 0, 0, 40},
		{"omitted limit is capped when the default exceeds the max", 2000, 1000, 0, 0, 0, 1000},
		{"limit at the exact cap is kept", 40, 1000, 0, 1000, 0, 1000},
		{"limit over the cap is clamped", 40, 1000, 0, 9999, 0, 1000},
		{"negative offset clamps to 0", 40, 1000, -5, 20, 0, 20},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizePage(tc.offset, tc.limit, tc.defaultLimit, tc.maxSubs)
			assert.Equal(t, tc.wantOffset, got.Offset)
			assert.Equal(t, tc.wantLimit, got.Limit)
		})
	}
}

func ptrBool(b bool) *bool { return &b }

func TestGetChannels_ExactlyOne(t *testing.T) {
	t.Run("both_empty", func(t *testing.T) {
		svc, _, _, _, _, _, _ := newSvc(t)
		_, err := svc.GetChannels(ctx("alice", "site-a"), models.GetChannelsRequest{})
		requireCode(t, err, errcode.CodeBadRequest)
	})
	t.Run("both_set", func(t *testing.T) {
		svc, _, _, _, _, _, _ := newSvc(t)
		_, err := svc.GetChannels(ctx("alice", "site-a"), models.GetChannelsRequest{MembersContain: "x", AccountNames: []string{"y"}})
		requireCode(t, err, errcode.CodeBadRequest)
	})
}

func TestGetChannels_TooManyAccountNames(t *testing.T) {
	svc, _, _, _, _, _, _ := newSvc(t)
	names := make([]string, 101) // over the configured cap (newSvc sets MaxAccountNames=100)
	for i := range names {
		names[i] = "u"
	}
	// No store expectation — the cap must reject before FindChannelsByMembers.
	_, err := svc.GetChannels(ctx("alice", "site-a"), models.GetChannelsRequest{AccountNames: names})
	requireCode(t, err, errcode.CodeBadRequest)
}

func TestGetChannels_AccountNamesAtCap(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	names := make([]string, 100) // exactly the configured cap (newSvc sets MaxAccountNames=100)
	for i := range names {
		names[i] = "u"
	}
	subs.EXPECT().FindChannelsByMembers(gomock.Any(), "alice", names, gomock.Any()).Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{Data: []model.EnrichedSubscription{{Subscription: model.Subscription{ID: "c1"}}}}, nil)
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	resp, err := svc.GetChannels(ctx("alice", "site-a"), models.GetChannelsRequest{AccountNames: names})
	require.NoError(t, err)
	assert.Len(t, resp.Subscriptions, 1)
}

func TestGetChannels_ByMembersContain(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	subs.EXPECT().FindChannelsByMembers(gomock.Any(), "alice", []string{"carol"}, gomock.Any()).Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{Data: []model.EnrichedSubscription{{Subscription: model.Subscription{ID: "c1"}}}}, nil)
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	resp, err := svc.GetChannels(ctx("alice", "site-a"), models.GetChannelsRequest{MembersContain: "carol"})
	require.NoError(t, err)
	assert.Len(t, resp.Subscriptions, 1)
}

func TestGetChannels_ByAccountNames(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	subs.EXPECT().FindChannelsByMembers(gomock.Any(), "alice", []string{"carol", "dave"}, gomock.Any()).Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{Data: []model.EnrichedSubscription{{Subscription: model.Subscription{ID: "c1"}}}}, nil)
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	resp, err := svc.GetChannels(ctx("alice", "site-a"), models.GetChannelsRequest{AccountNames: []string{"carol", "dave"}})
	require.NoError(t, err)
	assert.Len(t, resp.Subscriptions, 1)
}

func TestGetChannels_StoreError(t *testing.T) {
	svc, subs, _, _, _, _, _ := newSvc(t)
	subs.EXPECT().FindChannelsByMembers(gomock.Any(), "alice", []string{"carol"}, gomock.Any()).Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{}, errors.New("db down"))
	_, err := svc.GetChannels(ctx("alice", "site-a"), models.GetChannelsRequest{MembersContain: "carol"})
	requireCode(t, err, errcode.CodeInternal)
}

func TestGetChannels_Pagination(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	var got mongoutil.OffsetPageRequest
	subs.EXPECT().FindChannelsByMembers(gomock.Any(), "alice", []string{"carol"}, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ []string, page mongoutil.OffsetPageRequest) (mongoutil.OffsetPageHasMore[model.EnrichedSubscription], error) {
			got = page
			return mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{Data: []model.EnrichedSubscription{}, HasMore: true}, nil
		})
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	resp, err := svc.GetChannels(ctx("alice", "site-a"), models.GetChannelsRequest{MembersContain: "carol", Offset: 10, Limit: 5})
	require.NoError(t, err)
	assert.Equal(t, int64(10), got.Offset)
	assert.Equal(t, int64(5), got.Limit)
	assert.True(t, resp.HasMore)
}

func TestGetDM_Empty(t *testing.T) {
	svc, _, _, _, _, _, _ := newSvc(t)
	_, err := svc.GetDM(ctx("alice", "site-a"), models.GetDMRequest{AccountName: ""})
	requireCode(t, err, errcode.CodeBadRequest)
}

func TestGetDM_AnyTargetIsValid(t *testing.T) {
	// A DM counterpart may be any account: an ordinary user, a QA p_ account, a
	// bot (".bot"), or the platform-admin pseudo-account. All of them can log
	// into the chat frontend and hold a DM subscription, so the lookup always
	// falls through to GetDMSubscription rather than being rejected up front.
	for _, target := range []string{"bob", "p_system", "p_", "p_qa1", "helper.bot", "p_adminsiteA"} {
		t.Run(target, func(t *testing.T) {
			svc, subs, _, _, _, _, _ := newSvc(t)
			subs.EXPECT().GetDMSubscription(gomock.Any(), "alice", target).Return(nil, nil)
			_, err := svc.GetDM(ctx("alice", "site-a"), models.GetDMRequest{AccountName: target})
			requireCode(t, err, errcode.CodeNotFound)
			assert.True(t, errcode.HasReason(err, errcode.UserSubscriptionNotFound))
		})
	}
}

func TestGetDM_NotFound(t *testing.T) {
	svc, subs, _, _, _, _, _ := newSvc(t)
	subs.EXPECT().GetDMSubscription(gomock.Any(), "alice", "bob").Return(nil, nil)
	_, err := svc.GetDM(ctx("alice", "site-a"), models.GetDMRequest{AccountName: "bob"})
	requireCode(t, err, errcode.CodeNotFound)
	assert.True(t, errcode.HasReason(err, errcode.UserSubscriptionNotFound))
}

func TestGetDM_OK(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	subs.EXPECT().GetDMSubscription(gomock.Any(), "alice", "bob").
		Return(&model.EnrichedDMSubscription{
			EnrichedSubscription: model.EnrichedSubscription{Subscription: model.Subscription{ID: "d1"}},
			HRInfo:               &model.SubscriptionHRInfo{Account: "bob", Name: "bob", EngName: "Bob"},
		}, nil)
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	resp, err := svc.GetDM(ctx("alice", "site-a"), models.GetDMRequest{AccountName: "bob"})
	require.NoError(t, err)
	assert.Equal(t, "d1", resp.Subscription.ID)
	assert.Equal(t, "Bob", resp.Subscription.HRInfo.EngName)
}

func TestGetDM_StoreError(t *testing.T) {
	svc, subs, _, _, _, _, _ := newSvc(t)
	subs.EXPECT().GetDMSubscription(gomock.Any(), "alice", "bob").Return(nil, errors.New("db down"))
	_, err := svc.GetDM(ctx("alice", "site-a"), models.GetDMRequest{AccountName: "bob"})
	requireCode(t, err, errcode.CodeInternal)
}

func TestGetDM_Enriched(t *testing.T) {
	svc, subs, _, _, _, _, _ := newSvc(t)
	subs.EXPECT().GetDMSubscription(gomock.Any(), "alice", "bob").
		Return(&model.EnrichedDMSubscription{
			// LOCAL sub: room view comes from the baseline (RoomName), not the RPC.
			EnrichedSubscription: model.EnrichedSubscription{
				Subscription: model.Subscription{ID: "d1", SiteID: "site-a", RoomID: "r1", Name: "bob"},
				RoomName:     "Renamed",
			},
			HRInfo: &model.SubscriptionHRInfo{Account: "bob", Name: "bob", EngName: "Bob"},
		}, nil)
	resp, err := svc.GetDM(ctx("alice", "site-a"), models.GetDMRequest{AccountName: "bob"})
	require.NoError(t, err)
	assert.Equal(t, "bob", resp.Subscription.Name, "subscription name must survive enrichment")
	require.NotNil(t, resp.Subscription.Room, "enriched room must propagate through GetDM write-back")
	assert.Equal(t, "Renamed", resp.Subscription.Room.Name)
	require.NotNil(t, resp.Subscription.HRInfo, "HRInfo must survive the enrichment write-back")
	assert.Equal(t, "Bob", resp.Subscription.HRInfo.EngName)
}

func TestGetByRoomID_Empty(t *testing.T) {
	svc, _, _, _, _, _, _ := newSvc(t)
	_, err := svc.GetByRoomID(ctx("alice", "site-a"), models.GetByRoomIDRequest{RoomID: ""})
	requireCode(t, err, errcode.CodeBadRequest)
}

func TestGetByRoomID_NotFound(t *testing.T) {
	svc, subs, _, _, _, _, _ := newSvc(t)
	subs.EXPECT().GetSubscriptionByRoomID(gomock.Any(), "alice", "r1").Return(nil, nil)
	resp, err := svc.GetByRoomID(ctx("alice", "site-a"), models.GetByRoomIDRequest{RoomID: "r1"})
	require.NoError(t, err)
	assert.Equal(t, 0, resp.Total)
	assert.Empty(t, resp.Subscriptions)
	assert.NotNil(t, resp.Subscriptions, "empty result must be a non-nil slice")
}

func TestGetByRoomID_StoreError(t *testing.T) {
	svc, subs, _, _, _, _, _ := newSvc(t)
	subs.EXPECT().GetSubscriptionByRoomID(gomock.Any(), "alice", "r1").Return(nil, errors.New("db down"))
	_, err := svc.GetByRoomID(ctx("alice", "site-a"), models.GetByRoomIDRequest{RoomID: "r1"})
	requireCode(t, err, errcode.CodeInternal)
}

func TestGetByRoomID_OK_Enriched(t *testing.T) {
	svc, subs, _, _, _, _, _ := newSvc(t)
	subs.EXPECT().GetSubscriptionByRoomID(gomock.Any(), "alice", "r1").
		Return(&model.EnrichedSubscription{
			Subscription: model.Subscription{ID: "s1", SiteID: "site-a", RoomID: "r1", Name: "Stale"},
			RoomName:     "Renamed",
		}, nil)
	resp, err := svc.GetByRoomID(ctx("alice", "site-a"), models.GetByRoomIDRequest{RoomID: "r1"})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Total)
	require.Len(t, resp.Subscriptions, 1)
	base := resp.Subscriptions[0].Base()
	assert.Equal(t, "s1", base.ID)
	assert.Equal(t, "Stale", base.Name, "subscription name must survive enrichment")
	require.NotNil(t, base.Room, "enriched room must propagate through the 1-elem slice")
	assert.Equal(t, "Renamed", base.Room.Name)
}

func TestGetChannels_Empty(t *testing.T) {
	for _, name := range []string{"nil_slice", "empty_slice"} {
		t.Run(name, func(t *testing.T) {
			svc, subs, _, _, _, _, _ := newSvc(t)
			var returned []model.EnrichedSubscription
			if name == "empty_slice" {
				returned = []model.EnrichedSubscription{}
			}
			subs.EXPECT().FindChannelsByMembers(gomock.Any(), "alice", []string{"carol"}, gomock.Any()).Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{Data: returned}, nil)
			resp, err := svc.GetChannels(ctx("alice", "site-a"), models.GetChannelsRequest{MembersContain: "carol"})
			require.NoError(t, err)
			assert.Empty(t, resp.Subscriptions)
		})
	}
}

func TestGetByRoomID_BotDM_AppDisplayName(t *testing.T) {
	svc, subs, _, apps, _, _, _ := newSvc(t)
	subs.EXPECT().GetSubscriptionByRoomID(gomock.Any(), "alice", "rb1").
		Return(&model.EnrichedSubscription{Subscription: model.Subscription{ID: "a1", RoomID: "rb1", SiteID: "site-a", RoomType: model.RoomTypeBotDM, Name: "helper.bot"}}, nil)
	apps.EXPECT().GetAppsByAssistants(gomock.Any(), []string{"helper.bot"}).
		Return(map[string]*model.App{"helper.bot": appHelper()}, nil)
	resp, err := svc.GetByRoomID(ctx("alice", "site-a"), models.GetByRoomIDRequest{RoomID: "rb1"})
	require.NoError(t, err)
	require.Len(t, resp.Subscriptions, 1)
	bot, ok := resp.Subscriptions[0].(*model.BotDMSubscription)
	require.True(t, ok, "row 0 must be a botDM subscription")
	assert.Equal(t, "Helper App", bot.Name, "botDM via getByRoomID must also carry the app display name")
	require.NotNil(t, bot.App, "botDM via getByRoomID must carry the nested app object")
	assert.Equal(t, "app-helper", bot.App.AppID)
}

func TestCount_Total(t *testing.T) {
	svc, subs, _, _, _, _, _ := newSvc(t)
	subs.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(7, nil)
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{})
	require.NoError(t, err)
	assert.Equal(t, 7, resp.Count)
}

func TestCount_StoreError(t *testing.T) {
	svc, subs, _, _, _, _, _ := newSvc(t)
	subs.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(0, errors.New("db down"))
	_, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{})
	requireCode(t, err, errcode.CodeInternal)
}

func TestCountUnread_Happy(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	seen := time.UnixMilli(100).UTC()
	newer := time.UnixMilli(200).UTC()
	// No CountActiveSubscriptions expectation — the unread path must not fetch the total.
	// LOCAL sub: lastMsgAt is on the $lookup baseline — counted with NO RPC.
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).
		Return([]models.ActiveSubscription{{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen, LastMsgAt: &newer}}, nil)
	rooms.EXPECT().GetRoomsMeta(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Count)
	badge := svc.badge.(*fakeBadgeCache)
	require.Equal(t, []string{"alice"}, badge.reseedCalls, "count must best-effort reconcile the badge cache exactly once")
	require.Len(t, badge.reseedRoomIDs, 1)
	assert.Equal(t, []string{"r1"}, badge.reseedRoomIDs[0], "reseed must carry the exact unread room-ID set the count returned")
}

func TestCountUnread_FailedSiteSkipped(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	seen := time.UnixMilli(100).UTC()
	newer := time.UnixMilli(200).UTC()
	// One LOCAL unread (counted from the baseline) + one CROSS-SITE sub whose site's RPC fails.
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).
		Return([]models.ActiveSubscription{
			{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen, LastMsgAt: &newer}, // local unread
			{RoomID: "r2", SiteID: "site-b", LastSeenAt: &seen},                    // cross-site, site fails
		}, nil)
	failingRoomClient(rooms, "site-b")
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	// The unreachable site is SKIPPED; the local unread still counts.
	assert.Equal(t, 1, resp.Count)
}

func TestCountUnread_PartialFailureCountsHealthySites(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	seen := time.UnixMilli(100).UTC()
	newer := int64(200)
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).Return([]models.ActiveSubscription{
		{RoomID: "rb1", SiteID: "site-b", LastSeenAt: &seen}, // healthy site, unread
		{RoomID: "rc1", SiteID: "site-c", LastSeenAt: &seen}, // failing site, skipped
	}, nil)
	rooms.EXPECT().GetRoomsMeta(gomock.Any(), "site-b", gomock.Any()).
		Return([]model.RoomInfo{{RoomID: "rb1", Found: true, LastMsgAt: &newer}}, nil)
	rooms.EXPECT().GetRoomsMeta(gomock.Any(), "site-c", gomock.Any()).Return(nil, errors.New("down"))
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	// site-b's unread counts; the unreachable site-c is skipped.
	assert.Equal(t, 1, resp.Count)
}

func TestCountUnread_GetActiveStoreError(t *testing.T) {
	svc, subs, _, _, _, _, _ := newSvc(t)
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).Return(nil, errors.New("db down"))
	yes := true
	_, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	requireCode(t, err, errcode.CodeInternal)
}

func TestCountUnread_MultiSite(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	seen := time.UnixMilli(100).UTC()
	newerT := time.UnixMilli(200).UTC()
	newer := int64(200)
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).Return([]models.ActiveSubscription{
		{RoomID: "ra1", SiteID: "site-a", LastSeenAt: &seen, LastMsgAt: &newerT}, // local unread (baseline)
		{RoomID: "ra2", SiteID: "site-a", LastSeenAt: &seen},                     // local read (no lastMsgAt)
		{RoomID: "rb1", SiteID: "site-b", LastSeenAt: &seen},
		{RoomID: "rb2", SiteID: "site-b", LastSeenAt: &seen},
	}, nil)
	// Only the CROSS-SITE site is RPC'd; local rows are counted from the baseline.
	rooms.EXPECT().GetRoomsMeta(gomock.Any(), "site-b", gomock.InAnyOrder([]string{"rb1", "rb2"})).
		Return([]model.RoomInfo{
			{RoomID: "rb1", Found: true, LastMsgAt: &newer}, // unread
			{RoomID: "rb2", Found: true, LastMsgAt: nil},    // read
		}, nil)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 2, resp.Count, "one unread local (baseline) + one unread cross-site = 2")
}

func TestCountUnread_AllRead(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	seen := time.UnixMilli(300).UTC()
	older := time.UnixMilli(100).UTC() // older than seen → not unread
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).Return([]models.ActiveSubscription{
		{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen, LastMsgAt: &older},
		{RoomID: "r2", SiteID: "site-a", LastSeenAt: &seen}, // no lastMsgAt → read
	}, nil)
	rooms.EXPECT().GetRoomsMeta(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 0, resp.Count)
	// All-read still reconciles the cache — Reseed with an empty slice clears
	// any stale entries left over from a prior unread state (cache repair).
	badge := svc.badge.(*fakeBadgeCache)
	require.Equal(t, []string{"alice"}, badge.reseedCalls)
	require.Len(t, badge.reseedRoomIDs, 1)
	assert.Empty(t, badge.reseedRoomIDs[0], "an all-read account must reseed with an empty room-ID set")
}

// TestCountUnread_GateOff_NoCacheRead: with BADGE_COUNT_CACHE_FIRST off
// (default — newSvc's cfg leaves it false), Count is never consulted and the
// Mongo path runs.
func TestCountUnread_GateOff_NoCacheRead(t *testing.T) {
	svc, subs, _, _, _, _, _ := newSvc(t)
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).
		Return([]models.ActiveSubscription{}, nil)
	yes := true
	_, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Empty(t, svc.badge.(*fakeBadgeCache).countCalls, "gate off must not touch the cache read path")
}

// TestCountUnread_CacheFirst_Fresh: gate on + fresh marker → served from the
// cache with no repo call and no reseed. A fresh marker means the set was
// verified against Mongo within BADGE_MARKER_TTL, so no re-verification is due.
func TestCountUnread_CacheFirst_Fresh(t *testing.T) {
	svc, _, _, _, _, _, _ := newSvc(t)
	svc.badgeCacheFirst = true
	badge := svc.badge.(*fakeBadgeCache)
	badge.count = func(string) (int, bool) { return 7, true }
	// No GetActiveSubscriptions expectation — a repo call fails the strict mock.
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 7, resp.Count)
	assert.Equal(t, []string{"alice"}, badge.countCalls)
	assert.Empty(t, badge.reseedCalls, "cache hit must not reseed")
}

// TestCountUnread_CacheFirst_FreshZero: fresh zero is a legitimate served
// answer — the all-read state, not a miss.
func TestCountUnread_CacheFirst_FreshZero(t *testing.T) {
	svc, _, _, _, _, _, _ := newSvc(t)
	svc.badgeCacheFirst = true
	badge := svc.badge.(*fakeBadgeCache)
	badge.count = func(string) (int, bool) { return 0, true }
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 0, resp.Count)
	assert.Empty(t, badge.reseedCalls)
}

// TestCountUnread_CacheFirst_Stale_Computes: gate on + stale → today's
// compute-from-Mongo path, which reseeds (writing the marker). A stale marker
// here stands in for BADGE_MARKER_TTL expiry: the marker is stamped only by
// Seed/Reseed and never refreshed by bumps, so its TTL is what bounds how long
// this recompute-and-reseed self-heal can be deferred.
func TestCountUnread_CacheFirst_Stale_Computes(t *testing.T) {
	svc, subs, _, _, _, _, _ := newSvc(t)
	svc.badgeCacheFirst = true
	badge := svc.badge.(*fakeBadgeCache) // count defaults to (0, false) — stale
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).
		Return([]models.ActiveSubscription{localUnreadSub("alice", "r1", "site-a")}, nil)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Count)
	assert.Equal(t, []string{"alice"}, badge.countCalls, "stale path consults the cache exactly once")
	assert.Equal(t, []string{"alice"}, badge.reseedCalls, "stale path must reseed from the Mongo truth")
}

func TestCountUnread_EmptyActive(t *testing.T) {
	svc, subs, _, _, _, _, _ := newSvc(t)
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).
		Return([]models.ActiveSubscription{}, nil)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 0, resp.Count)
}

func TestUnreadRooms_ContextCancelled_SkipsRPC(t *testing.T) {
	// A cancelled client context must short-circuit the cross-site fan-out before
	// firing any ~5s GetRoomsInfo RPC; local subs still count from the baseline.
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	seen := time.UnixMilli(100).UTC()
	newer := time.UnixMilli(200).UTC()
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).
		Return([]models.ActiveSubscription{
			{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen, LastMsgAt: &newer}, // local unread
			{RoomID: "r2", SiteID: "site-b", LastSeenAt: &seen},                    // cross-site, must be skipped
		}, nil)
	rooms.EXPECT().GetRoomsMeta(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	c := ctx("alice", "site-a")
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	c.SetContext(cancelled)
	ids, degraded, err := svc.unreadRooms(c, "alice")
	require.NoError(t, err)
	assert.Equal(t, []string{"r1"}, ids, "cross-site site skipped on cancel; local unread still counts")
	assert.True(t, degraded, "a cancelled fan-out must never be treated as complete")
}

// TestUnreadRooms_ContextCancelledMultiSite_AllSitesDegraded extends the single-site
// cancellation case to 2+ cross-sites, so the break path's degradation-marking runs
// over a slice with more than one element (len(sites) > 1) instead of the degenerate
// single-element case where "mark just the current index" and "mark the current index
// through the end of the slice" are indistinguishable by construction (index 0 is both
// the first and the last element). Deliberately uses an already-cancelled context
// (rather than cancelling mid-flight from inside a GetRoomsMeta stub): sites is derived
// from map iteration order (crossBySite is a map) and the launch loop's semaphore does
// not block for a handful of sites, so an in-flight cancel races the loop's own
// per-iteration c.Err() checks against goroutine scheduling with no way to pin which
// site's stub runs first — not deterministic, and not fixable without either sleeping
// (forbidden) or adding synchronization the production code doesn't have. Cancelling up
// front instead makes the very first loop iteration observe c.Err() != nil before any
// goroutine is launched, deterministically exercising the multi-element branch of the
// break-path marking with zero reliance on scheduling.
func TestUnreadRooms_ContextCancelledMultiSite_AllSitesDegraded(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).Return(
		[]models.ActiveSubscription{
			crossSiteSub("r1", "site-b"),
			crossSiteSub("r2", "site-c"),
			crossSiteSub("r3", "site-d"),
		}, nil)
	rooms.EXPECT().GetRoomsMeta(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	c := ctx("alice", "site-a")
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	c.SetContext(cancelled)

	ids, degraded, err := svc.unreadRooms(c, "alice")
	require.NoError(t, err)
	assert.Empty(t, ids, "no cross-site can be counted once the context is already cancelled")
	assert.True(t, degraded, "a cancelled fan-out across multiple sites must never be cached as complete")
}

// Absence, not the name, is what excludes a cross-site room from the unread count:
// a room the remote site reports Found=false contributes nothing.
func TestUnreadRooms_CrossSiteNotFoundRoomNotCounted(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	newer := int64(200) // newer than seen → WOULD count if the room resolved
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).
		Return([]models.ActiveSubscription{crossSiteSub("rd", "site-b")}, nil)
	rooms.EXPECT().GetRoomsMeta(gomock.Any(), "site-b", gomock.Any()).
		Return([]model.RoomInfo{{RoomID: "rd", Found: false, LastMsgAt: &newer}}, nil)

	ids, degraded, err := svc.unreadRooms(ctx("alice", "site-a"), "alice")
	require.NoError(t, err)
	assert.Empty(t, ids, "an unresolvable cross-site room must not be counted as unread")
	assert.False(t, degraded, "a successful RPC that simply excludes the room is not degraded")
}

// A not-found cross-site room must not become a thread candidate either — its stale
// ThreadUnread must not resurrect it via the thread phase.
func TestUnreadRooms_CrossSiteNotFoundRoomThreadNotCounted(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	older := int64(50) // older than seen → read at the message level
	sub := crossSiteSub("rd", "site-b")
	sub.ThreadUnread = []string{"p1"}
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).
		Return([]models.ActiveSubscription{sub}, nil)
	rooms.EXPECT().GetRoomsMeta(gomock.Any(), "site-b", gomock.Any()).
		Return([]model.RoomInfo{{RoomID: "rd", Found: false, LastMsgAt: &older}}, nil)

	ids, degraded, err := svc.unreadRooms(ctx("alice", "site-a"), "alice")
	require.NoError(t, err)
	assert.Empty(t, ids, "an unresolvable cross-site room's ThreadUnread must not count")
	assert.False(t, degraded, "a successful RPC that simply excludes the room is not degraded")
}

// The room name carries no meaning in the unread count: a resolvable room counts on
// its timestamps alone, whatever it is called.
func TestUnreadRooms_CrossSiteRoomNameIsNotAFilter(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	newer := int64(200) // newer than seen → counts
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).
		Return([]models.ActiveSubscription{crossSiteSub("rd", "site-b")}, nil)
	rooms.EXPECT().GetRoomsMeta(gomock.Any(), "site-b", gomock.Any()).
		Return([]model.RoomInfo{{RoomID: "rd", Found: true, Name: "Del-secret", LastMsgAt: &newer}}, nil)

	ids, degraded, err := svc.unreadRooms(ctx("alice", "site-a"), "alice")
	require.NoError(t, err)
	assert.Equal(t, []string{"rd"}, ids, "a resolvable room counts regardless of its name")
	assert.False(t, degraded)
}

// A failing cross-site RPC still yields a best-effort count, but nothing may be
// written to the cache — a partial set must never be stamped as verified.
func TestCountSubscriptions_Degraded_NoReseed(t *testing.T) {
	ctrl := gomock.NewController(t)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	rooms := mocks.NewMockRoomClient(ctrl)
	badge := &fakeBadgeCache{}
	svc := newBadgeService(t, subs, badge)
	svc.rooms = rooms
	failingRoomClient(rooms, "site-b")

	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 1000).Return(
		[]models.ActiveSubscription{crossSiteSub("r-remote", "site-b")}, nil)

	unread := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &unread})
	require.NoError(t, err)
	assert.Equal(t, 0, resp.Count, "the unreachable site's rooms drop out")
	assert.Empty(t, badge.reseedCalls, "a degraded compute must not stamp the marker")
}

// The non-degraded path is unchanged: it still writes through to the cache.
func TestCountSubscriptions_NotDegraded_StillReseeds(t *testing.T) {
	ctrl := gomock.NewController(t)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	badge := &fakeBadgeCache{}
	svc := newBadgeService(t, subs, badge)

	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 1000).Return(
		[]models.ActiveSubscription{localUnreadSub("alice", "r1", "site-a")}, nil)

	unread := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &unread})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Count)
	assert.Equal(t, []string{"alice"}, badge.reseedCalls)
}

// TestCountUnread_ReadRoomBumpedByUnreadThread: a message-read room whose subscription
// carries a single unread followed thread (Subscription.ThreadUnread, already on the
// fetched page — no RPC) bumps the count by exactly 1.
func TestCountUnread_ReadRoomBumpedByUnreadThread(t *testing.T) {
	svc, subs, _, _, _, _, _ := newSvc(t)
	seen := time.UnixMilli(100).UTC()
	older := time.UnixMilli(50).UTC()
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).Return([]models.ActiveSubscription{
		{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen, ThreadUnread: []string{"p1"}, LastMsgAt: &older},
	}, nil)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Count)
}

// TestCountUnread_AlreadyUnreadRoomNotDoubleCounted: a room that is already unread at
// the message level AND carries an unread thread still contributes exactly 1, not 2.
func TestCountUnread_AlreadyUnreadRoomNotDoubleCounted(t *testing.T) {
	svc, subs, _, _, _, _, _ := newSvc(t)
	seen := time.UnixMilli(100).UTC()
	newer := time.UnixMilli(300).UTC()
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).Return([]models.ActiveSubscription{
		{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen, ThreadUnread: []string{"p1"}, LastMsgAt: &newer},
	}, nil)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Count)
}

// TestCountUnread_MultipleUnreadThreadsCountOnce: three unread thread parent IDs in a
// single room still contribute exactly 1, not 3.
func TestCountUnread_MultipleUnreadThreadsCountOnce(t *testing.T) {
	svc, subs, _, _, _, _, _ := newSvc(t)
	seen := time.UnixMilli(100).UTC()
	older := time.UnixMilli(50).UTC()
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).Return([]models.ActiveSubscription{
		{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen, ThreadUnread: []string{"p1", "p2", "p3"}, LastMsgAt: &older},
	}, nil)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Count)
}

// TestCountUnread_CrossSiteReadRoomBumpedByThread: the thread phase applies to
// cross-site rooms too, reading ThreadUnread straight off the fetched sub — no
// separate thread RPC.
func TestCountUnread_CrossSiteReadRoomBumpedByThread(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	seen := time.UnixMilli(100).UTC()
	older := int64(50)
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).Return([]models.ActiveSubscription{
		{RoomID: "r1", SiteID: "site-b", LastSeenAt: &seen, ThreadUnread: []string{"p1"}},
	}, nil)
	rooms.EXPECT().GetRoomsMeta(gomock.Any(), "site-b", []string{"r1"}).
		Return([]model.RoomInfo{{RoomID: "r1", Found: true, LastMsgAt: &older}}, nil)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Count)
}

// TestCountUnread_MutedRoomThreadExcluded: activeSubscriptionFilter already excludes
// muted rooms at the Mongo layer (existing filter, unchanged here), so a muted room's
// ThreadUnread never reaches the fetched page and can never bump the count.
func TestCountUnread_MutedRoomThreadExcluded(t *testing.T) {
	svc, subs, _, _, _, _, _ := newSvc(t)
	seen := time.UnixMilli(100).UTC()
	older := time.UnixMilli(50).UTC()
	// Only the unmuted, read r1 (no threads) is returned.
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).Return([]models.ActiveSubscription{
		{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen, LastMsgAt: &older},
	}, nil)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 0, resp.Count)
}

func TestDistinctListNames(t *testing.T) {
	subs := []model.EnrichedSubscription{
		{Subscription: model.Subscription{Name: "helper.bot", RoomType: model.RoomTypeBotDM}},
		{Subscription: model.Subscription{Name: "bob", RoomType: model.RoomTypeDM}},
		{Subscription: model.Subscription{Name: "Eng", RoomType: model.RoomTypeChannel}},      // channels feed neither set
		{Subscription: model.Subscription{Name: "helper.bot", RoomType: model.RoomTypeBotDM}}, // duplicate bot
		{Subscription: model.Subscription{Name: "carol", RoomType: model.RoomTypeDM}},
		{Subscription: model.Subscription{Name: "bob", RoomType: model.RoomTypeDM}}, // duplicate dm counterpart
	}
	bots, dmCounterparts := distinctListNames(subs)
	assert.Equal(t, []string{"helper.bot"}, bots, "bot accounts deduped in first-seen order")
	assert.Equal(t, []string{"bob", "carol"}, dmCounterparts, "dm counterparts deduped in first-seen order")
}

func TestDistinctListNames_Empty(t *testing.T) {
	bots, dmCounterparts := distinctListNames(nil)
	assert.Empty(t, bots)
	assert.Empty(t, dmCounterparts)
}

func TestCount_UnreadFalse(t *testing.T) {
	for _, name := range []string{"nil", "false"} {
		t.Run(name, func(t *testing.T) {
			svc, subs, _, _, _, _, _ := newSvc(t)
			subs.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(9, nil)
			// No GetActiveSubscriptions expectation — short-circuit must fire before calling it.
			var unreadPtr *bool
			if name == "false" {
				f := false
				unreadPtr = &f
			}
			resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: unreadPtr})
			require.NoError(t, err)
			assert.Equal(t, 9, resp.Count)
		})
	}
}

func TestListSubscriptions_LastMessage_Populated(t *testing.T) {
	svc, subs, _, history := newSvcRawHistory(t)
	storeSubs := []model.EnrichedSubscription{{
		Subscription: model.Subscription{ID: "s1", RoomID: "r1", SiteID: "site-a", Name: "general", RoomType: model.RoomTypeChannel},
		RoomName:     "General", UserCount: 3,
	}}
	subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "current", false, gomock.Any(), gomock.Any()).
		Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{Data: storeSubs}, nil)
	history.EXPECT().RoomsGet(gomock.Any(), "site-a", []string{"r1"}, map[string]model.RoomTimeHint{}).
		Return(map[string]model.PreviewMessage{"r1": {MessageID: "m9", Content: "hi", CreatedAt: time.Unix(123, 0).UTC()}}, nil)
	resp, err := svc.ListSubscriptions(ctx("alice", "site-a"), models.SubscriptionListRequest{Type: "current"})
	require.NoError(t, err)
	require.Len(t, resp.Subscriptions, 1)
	room := resp.Subscriptions[0].Base().Room
	require.NotNil(t, room)
	require.NotNil(t, room.PreviewMessage, "last message attached from rooms.get")
	assert.Equal(t, "m9", room.PreviewMessage.MessageID)
}

// enrichLastMessage must build a hint from each room's already-resolved
// LastMsgAt (set by enrichLocal before this runs) so history-service can skip
// its own room-times read; a room with no resolved LastMsgAt contributes no
// hint entry.
func TestListSubscriptions_LastMessage_HintsFromResolvedRoom(t *testing.T) {
	svc, subs, _, history := newSvcRawHistory(t)
	lastMsgAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	storeSubs := []model.EnrichedSubscription{
		{
			Subscription: model.Subscription{ID: "s1", RoomID: "r1", SiteID: "site-a", Name: "general", RoomType: model.RoomTypeChannel},
			RoomName:     "General", UserCount: 3, LastMsgAt: &lastMsgAt,
		},
		{
			// No messages yet: nil LastMsgAt ⇒ no hint for r2.
			Subscription: model.Subscription{ID: "s2", RoomID: "r2", SiteID: "site-a", Name: "quiet-room", RoomType: model.RoomTypeChannel},
			RoomName:     "Quiet",
		},
	}
	subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "current", false, gomock.Any(), gomock.Any()).
		Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{Data: storeSubs}, nil)
	wantHints := map[string]model.RoomTimeHint{"r1": {LastMsgAt: timeutil.TimeToMillis(&lastMsgAt)}}
	history.EXPECT().RoomsGet(gomock.Any(), "site-a", []string{"r1", "r2"}, wantHints).
		Return(map[string]model.PreviewMessage{}, nil)

	resp, err := svc.ListSubscriptions(ctx("alice", "site-a"), models.SubscriptionListRequest{Type: "current"})
	require.NoError(t, err)
	require.Len(t, resp.Subscriptions, 2)
	room1 := resp.Subscriptions[0].Base().Room
	require.NotNil(t, room1)
	room2 := resp.Subscriptions[1].Base().Room
	require.NotNil(t, room2, "a room with no messages still gets a room object")
	assert.Nil(t, room2.LastMsgAt)
}

// includeLastMessage:false skips the rooms.get RPC entirely.
func TestListSubscriptions_LastMessage_SkippedWhenExcluded(t *testing.T) {
	svc, subs, _, _ := newSvcRawHistory(t)
	storeSubs := []model.EnrichedSubscription{{
		Subscription: model.Subscription{ID: "s1", RoomID: "r1", SiteID: "site-a", Name: "general", RoomType: model.RoomTypeChannel},
		RoomName:     "General", UserCount: 3,
	}}
	subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "current", false, gomock.Any(), gomock.Any()).
		Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{Data: storeSubs}, nil)
	// No history.RoomsGet EXPECT — the mock ctrl fails if it's called.
	no := false
	resp, err := svc.ListSubscriptions(ctx("alice", "site-a"), models.SubscriptionListRequest{Type: "current", IncludeLastMessage: &no})
	require.NoError(t, err)
	require.Len(t, resp.Subscriptions, 1)
	room := resp.Subscriptions[0].Base().Room
	require.NotNil(t, room)
	assert.Nil(t, room.PreviewMessage, "excluded last message stays nil")
}

func TestListSubscriptions_LastMessage_SiteDegrades(t *testing.T) {
	svc, subs, _, history := newSvcRawHistory(t)
	storeSubs := []model.EnrichedSubscription{{
		Subscription: model.Subscription{ID: "s1", RoomID: "r1", SiteID: "site-a", Name: "general", RoomType: model.RoomTypeChannel},
		RoomName:     "General", UserCount: 3,
	}}
	subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "current", false, gomock.Any(), gomock.Any()).
		Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{Data: storeSubs}, nil)
	history.EXPECT().RoomsGet(gomock.Any(), "site-a", []string{"r1"}, map[string]model.RoomTimeHint{}).
		Return(nil, errors.New("history down"))
	resp, err := svc.ListSubscriptions(ctx("alice", "site-a"), models.SubscriptionListRequest{Type: "current"})
	require.NoError(t, err, "a degraded rooms.get must not fail the list")
	require.Len(t, resp.Subscriptions, 1)
	room := resp.Subscriptions[0].Base().Room
	require.NotNil(t, room)
	assert.Nil(t, room.PreviewMessage, "degraded site leaves LastMessage nil")
}

func TestBuildLocalRoom_CrossSite(t *testing.T) {
	sub := &model.EnrichedSubscription{}
	sub.CrossSite = ptrBool(true)
	sub.RoomName = "chan"
	got := buildLocalRoom(sub)
	require.NotNil(t, got)
	require.NotNil(t, got.CrossSite)
	assert.True(t, *got.CrossSite)
}

func TestApplyRoomInfo_CrossSite(t *testing.T) {
	sub := &model.Subscription{}
	applyRoomInfo(sub, &model.RoomInfo{Found: true, Name: "chan", CrossSite: ptrBool(true)})
	require.NotNil(t, sub.Room)
	require.NotNil(t, sub.Room.CrossSite)
	assert.True(t, *sub.Room.CrossSite)
}

// TestApplyRoomInfo_CrossSite_Nil pins that an unclassified cross-site room
// (RoomInfo.CrossSite nil) passes through as nil on the SubscriptionRoom —
// never coerced to false — so the wire response omits the field and the
// frontend's `?? true` default resolves it to global (fail-safe).
func TestApplyRoomInfo_CrossSite_Nil(t *testing.T) {
	sub := &model.Subscription{}
	applyRoomInfo(sub, &model.RoomInfo{Found: true, Name: "chan"})
	require.NotNil(t, sub.Room)
	assert.Nil(t, sub.Room.CrossSite)
}

// Page bounds are parameters because HTTP and NATS have different ceilings.
func TestListSubscriptionsFor_AppliesSuppliedPageBounds(t *testing.T) {
	tests := []struct {
		name                   string
		reqLimit, reqOffset    int
		defaultLimit, maxLimit int
		wantLimit, wantOffset  int64
	}{
		{"omitted limit takes the caller's default", 0, 0, 40, 400, 40, 0},
		{"explicit limit passes through", 200, 0, 40, 400, 200, 0},
		{"limit clamps to the caller's max", 5000, 0, 40, 400, 400, 0},
		{"negative offset floors at zero", 200, -5, 40, 400, 200, 0},
		{"offset passes through", 200, 400, 40, 400, 200, 400},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc, subs, _, _, rooms, _, _ := newSvc(t)
			var got mongoutil.OffsetPageRequest
			subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "current", false, gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, _, _ string, _ bool, _ *int, page mongoutil.OffsetPageRequest) (mongoutil.OffsetPageHasMore[model.EnrichedSubscription], error) {
					got = page
					return mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{}, nil
				})
			rooms.EXPECT().GetRoomsInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

			_, err := svc.ListSubscriptionsFor(context.Background(), "alice",
				models.SubscriptionListRequest{Type: "current", Limit: tc.reqLimit, Offset: tc.reqOffset},
				tc.defaultLimit, tc.maxLimit)

			require.NoError(t, err)
			assert.Equal(t, tc.wantLimit, got.Limit)
			assert.Equal(t, tc.wantOffset, got.Offset)
		})
	}
}

// Validation lives in the shared core, so both transports inherit it.
func TestListSubscriptionsFor_ValidatesRequest(t *testing.T) {
	negative := -1
	tests := []struct {
		name string
		req  models.SubscriptionListRequest
	}{
		{"unknown type", models.SubscriptionListRequest{Type: "bogus"}},
		{"empty type", models.SubscriptionListRequest{Type: ""}},
		{"negative updatedWithinDays", models.SubscriptionListRequest{Type: "rooms", UpdatedWithinDays: &negative}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc, _, _, _, _, _, _ := newSvc(t)
			_, err := svc.ListSubscriptionsFor(context.Background(), "alice", tc.req, 40, 400)
			requireCode(t, err, errcode.CodeBadRequest)
		})
	}
}

// The NATS handler must keep its own configured bounds, not the HTTP ones.
func TestListSubscriptions_NATSHandlerUsesServiceBounds(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	var got mongoutil.OffsetPageRequest
	subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "current", false, gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, _ bool, _ *int, page mongoutil.OffsetPageRequest) (mongoutil.OffsetPageHasMore[model.EnrichedSubscription], error) {
			got = page
			return mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{}, nil
		})
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	_, err := svc.ListSubscriptions(ctx("alice", "site-a"), models.SubscriptionListRequest{Type: "current"})

	require.NoError(t, err)
	assert.Equal(t, int64(40), got.Limit, "SUBSCRIPTION_DEFAULT_LIMIT")
}

// A deadline that fires during enrichment must fail the request rather than
// return a page whose rooms are indistinguishable from deleted ones.
func TestListSubscriptionsFor_DeadlineDuringEnrichmentFails(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "current", false, gomock.Any(), gomock.Any()).
		Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{
			Data: []model.EnrichedSubscription{
				{Subscription: model.Subscription{ID: "s1", RoomID: "r1", SiteID: "site-b"}},
			},
		}, nil)
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	_, err := svc.ListSubscriptionsFor(ctx, "alice", models.SubscriptionListRequest{Type: "current"}, 40, 400)

	requireCode(t, err, errcode.CodeUnavailable)
}

// The shutdown drain cancels handlers whose clients are still connected, so that
// caller would otherwise receive a partially enriched page as 200.
func TestListSubscriptionsFor_ShutdownCancellationFails(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "current", false, gomock.Any(), gomock.Any()).
		Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{
			Data: []model.EnrichedSubscription{
				{Subscription: model.Subscription{ID: "s1", RoomID: "r1", SiteID: "site-b"}},
			},
		}, nil).AnyTimes()
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(ErrShuttingDown)

	_, err := svc.ListSubscriptionsFor(ctx, "alice", models.SubscriptionListRequest{Type: "current"}, 40, 400)

	requireCode(t, err, errcode.CodeUnavailable)
}

// A client that hung up is gone; turning that into a 503 would log an ERROR per
// abandoned request during exactly the reconnect burst this endpoint serves.
func TestListSubscriptionsFor_ClientCancellationIsNotAServerError(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "current", false, gomock.Any(), gomock.Any()).
		Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{
			Data: []model.EnrichedSubscription{
				{Subscription: model.Subscription{ID: "s1", RoomID: "r1", SiteID: "site-b"}},
			},
		}, nil)
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := svc.ListSubscriptionsFor(ctx, "alice", models.SubscriptionListRequest{Type: "current"}, 40, 400)

	require.NoError(t, err)
}

// The happy path must not be tripped by the deadline guard.
func TestListSubscriptionsFor_LiveContextSucceeds(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "current", false, gomock.Any(), gomock.Any()).
		Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{
			Data: []model.EnrichedSubscription{
				{Subscription: model.Subscription{ID: "s1", RoomID: "r1", SiteID: "site-a"}},
			},
		}, nil)
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	resp, err := svc.ListSubscriptionsFor(context.Background(), "alice",
		models.SubscriptionListRequest{Type: "current"}, 40, 400)

	require.NoError(t, err)
	assert.Len(t, resp.Subscriptions, 1)
}

// The last-message fan-out must not spend batch budget on subs with no Room: the
// fan-in discards any preview the reply carries for them, so their ids only crowd
// the 100-id cap. A site left with none emits no chunk, which is what skips its RPC.
func TestRequestableBySite_SkipsRoomlessSubs(t *testing.T) {
	withRoom := func(id, site string) model.EnrichedSubscription {
		return model.EnrichedSubscription{Subscription: model.Subscription{RoomID: id, SiteID: site, Room: &model.SubscriptionRoom{}}}
	}
	roomless := func(id, site string) model.EnrichedSubscription {
		return model.EnrichedSubscription{Subscription: model.Subscription{RoomID: id, SiteID: site}}
	}

	tests := []struct {
		name string
		subs []model.EnrichedSubscription
		size int
		want [][]string // roomIDs per emitted chunk
	}{
		{
			name: "roomless subs are dropped from the batch",
			subs: []model.EnrichedSubscription{withRoom("r1", "site-a"), roomless("r2", "site-a"), withRoom("r3", "site-a")},
			size: 100,
			want: [][]string{{"r1", "r3"}},
		},
		{
			name: "a site with only roomless subs emits no chunk at all",
			subs: []model.EnrichedSubscription{roomless("r1", "site-a"), roomless("r2", "site-a")},
			size: 100,
			want: nil,
		},
		// Chunk boundaries must fall on requestable rooms, not raw rows: counting the
		// dropped id would split a batch that fits into two RPCs.
		{
			name: "chunking counts only requestable rooms",
			subs: []model.EnrichedSubscription{withRoom("r1", "site-a"), roomless("r2", "site-a"), withRoom("r3", "site-a")},
			size: 2,
			want: [][]string{{"r1", "r3"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idxBySite := map[string][]int{}
			for i := range tc.subs {
				idxBySite[tc.subs[i].SiteID] = append(idxBySite[tc.subs[i].SiteID], i)
			}
			jobs := planChunks(tc.subs, []string{"site-a"}, requestableBySite(tc.subs, idxBySite), tc.size)
			got := make([][]string, 0, len(jobs))
			for _, j := range jobs {
				got = append(got, j.roomIDs)
			}
			if tc.want == nil {
				assert.Empty(t, got, "no requestable room must mean no chunk, hence no RPC")
				return
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

// End-to-end counterpart: a site whose subs all lack a Room issues no rooms.get at
// all. buildLocalRoom always returns a room for a LOCAL sub, so an unresolved
// CROSS-SITE room is how a sub ends up room-less.
func TestListSubscriptions_LastMessage_AllRoomless_SkipsRPC(t *testing.T) {
	svc, subs, rooms, _ := newSvcRawHistory(t)
	storeSubs := []model.EnrichedSubscription{{
		Subscription: model.Subscription{ID: "s2", RoomID: "r2", SiteID: "site-b", Name: "gone-room", RoomType: model.RoomTypeChannel},
	}}
	subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "current", false, gomock.Any(), gomock.Any()).
		Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{Data: storeSubs}, nil)
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), "site-b", []string{"r2"}).
		Return([]model.RoomInfo{{RoomID: "r2", Found: false}}, nil)
	// No history.RoomsGet EXPECT — the mock ctrl fails if it is called.

	resp, err := svc.ListSubscriptions(ctx("alice", "site-a"), models.SubscriptionListRequest{Type: "current"})
	require.NoError(t, err)
	require.Len(t, resp.Subscriptions, 1)
	assert.Nil(t, resp.Subscriptions[0].Base().Room)
}

// A system event bumps lastMsgAt but not lastUserMsgAt; a member who has read
// the room must not be counted, while a newly added member (no lastSeenAt) is.
func TestCountUnread_SystemBumpDoesNotCount(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	seen := time.UnixMilli(200).UTC()
	userAt := time.UnixMilli(100).UTC() // read
	sysAt := time.UnixMilli(300).UTC()  // newer system bump
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).
		Return([]models.ActiveSubscription{
			{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen, LastUserMsgAt: &userAt, LastMsgAt: &sysAt},
		}, nil)
	rooms.EXPECT().GetRoomsMeta(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 0, resp.Count, "system bump past the read position must not count")
}

func TestCountUnread_NewlyAddedMemberCounts(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	frozen := time.UnixMilli(100).UTC() // freeze pinned the pre-system position
	sysAt := time.UnixMilli(300).UTC()
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).
		Return([]models.ActiveSubscription{
			{RoomID: "r1", SiteID: "site-a", LastUserMsgAt: &frozen, LastMsgAt: &sysAt}, // no LastSeenAt
		}, nil)
	rooms.EXPECT().GetRoomsMeta(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Count, "a never-read subscription counts whenever the room has any user-activity reference")
}

// Cross-site rooms follow the same rule via RoomInfo.lastUserMsgAt, falling
// back to lastMsgAt for peers that predate the field.
func TestCountUnread_CrossSiteSystemBumpDoesNotCount(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	seen := time.UnixMilli(200).UTC()
	userMs, sysMs := int64(100), int64(300)
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).Return([]models.ActiveSubscription{
		{RoomID: "rb1", SiteID: "site-b", LastSeenAt: &seen},
		{RoomID: "rb2", SiteID: "site-b", LastSeenAt: &seen},
	}, nil)
	rooms.EXPECT().GetRoomsMeta(gomock.Any(), "site-b", gomock.InAnyOrder([]string{"rb1", "rb2"})).
		Return([]model.RoomInfo{
			{RoomID: "rb1", Found: true, LastUserMsgAt: &userMs, LastMsgAt: &sysMs}, // read; system bump ignored
			{RoomID: "rb2", Found: true, LastMsgAt: &sysMs},                         // legacy peer: lastMsgAt rules
		}, nil)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Count, "rb1 read (user activity older than seen); rb2 unread via legacy fallback")
}

// List path counterpart: a LOCAL sub's hasUnread must compare lastSeenAt against
// lastUserMsgAt (the system bump on lastMsgAt is ignored), and the wire room
// object must carry lastUserMsgAt through to the client.
func TestListSubscriptions_LocalUnread_PrefersLastUserMsgAt(t *testing.T) {
	svc, subs, _, _, _, _, _ := newSvc(t)
	seen := time.UnixMilli(200).UTC()
	userAt := time.UnixMilli(100).UTC()
	sysAt := time.UnixMilli(300).UTC()
	storeSubs := []model.EnrichedSubscription{{
		Subscription: model.Subscription{ID: "s1", RoomID: "r1", SiteID: "site-a", Name: "general", RoomType: model.RoomTypeChannel, LastSeenAt: &seen},
		RoomName:     "General", LastUserMsgAt: &userAt, LastMsgAt: &sysAt,
	}}
	subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "current", false, gomock.Any(), gomock.Any()).
		Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{Data: storeSubs}, nil)
	resp, err := svc.ListSubscriptionsFor(context.Background(), "alice", models.SubscriptionListRequest{Type: "current"}, 40, 400)
	require.NoError(t, err)
	require.Len(t, resp.Subscriptions, 1)
	base := resp.Subscriptions[0].Base()
	assert.False(t, base.HasUnread, "system bump past the read position must not count as unread")
	require.NotNil(t, base.Room)
	require.NotNil(t, base.Room.LastMsgAt)
	assert.Equal(t, userAt, *base.Room.LastMsgAt,
		"the wire carries ONE activity timestamp: lastMsgAt is the coalesced user-activity value, not the raw ceiling")
}

// Rows carry the type their own subscriber sees, so the split reads RoomType
// directly.
func TestDistinctListNames_SplitsAppRoomsFromDMs(t *testing.T) {
	subs := []model.EnrichedSubscription{
		{Subscription: model.Subscription{RoomType: model.RoomTypeBotDM, Name: "weather.bot"}},
		{Subscription: model.Subscription{RoomType: model.RoomTypeDM, Name: "alice"}},       // bot's own row, normalized
		{Subscription: model.Subscription{RoomType: model.RoomTypeDM, Name: "p_admin_ops"}}, // p_admin DM, normalized
		{Subscription: model.Subscription{RoomType: model.RoomTypeDM, Name: "bob"}},
		{Subscription: model.Subscription{RoomType: model.RoomTypeChannel, Name: "general"}},
		{Subscription: model.Subscription{RoomType: model.RoomTypeBotDM, Name: "weather.bot"}}, // dup
	}

	bots, dms := distinctListNames(subs)

	assert.Equal(t, []string{"weather.bot"}, bots, "only real apps drive the app lookup")
	assert.Equal(t, []string{"alice", "p_admin_ops", "bob"}, dms, "every DM drives the HR lookup")
}

// Subscriptions written before the role cutover still store the legacy "member"
// spelling; every user-service read must hand the client "user" instead.
func TestListSubscriptions_LegacyMemberRoleSerializesAsUser(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "current", false, gomock.Any(), gomock.Any()).
		Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{Data: []model.EnrichedSubscription{
			{Subscription: model.Subscription{ID: "s1", RoomID: "r1", RoomType: model.RoomTypeChannel, Roles: []model.Role{model.RoleMember}}},
		}}, nil)
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	resp, err := svc.ListSubscriptions(ctx("alice", "site-a"), models.SubscriptionListRequest{Type: "current"})
	require.NoError(t, err)
	body, err := json.Marshal(resp)
	require.NoError(t, err)
	assert.Contains(t, string(body), `"roles":["user"]`)
	assert.NotContains(t, string(body), `"member"`)
}

func TestGetByRoomID_LegacyMemberRoleSerializesAsUser(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	subs.EXPECT().GetSubscriptionByRoomID(gomock.Any(), "alice", "r1").
		Return(&model.EnrichedSubscription{Subscription: model.Subscription{
			ID: "s1", RoomID: "r1", RoomType: model.RoomTypeChannel, Roles: []model.Role{model.RoleMember, model.RoleOwner},
		}}, nil)
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	resp, err := svc.GetByRoomID(ctx("alice", "site-a"), models.GetByRoomIDRequest{RoomID: "r1"})
	require.NoError(t, err)
	body, err := json.Marshal(resp)
	require.NoError(t, err)
	assert.Contains(t, string(body), `"roles":["user","owner"]`)
}
