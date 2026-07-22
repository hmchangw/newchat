package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/user-service/config"
	"github.com/hmchangw/chat/user-service/models"
	"github.com/hmchangw/chat/user-service/service/mocks"
)

// newSvcRawHistory builds a service exposing the history mock WITHOUT newSvc's
// permissive RoomsGet default, so last-message enrichment tests can set an exact
// RoomsGet expectation (result or error).
func newSvcRawHistory(t *testing.T) (*UserService, *mocks.MockSubscriptionRepository, *mocks.MockHistoryClient) {
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
	cfg := &config.Config{SiteID: "site-a", AllSiteIDs: []string{"site-a", "site-b"}, MaxSubscriptionLimit: 1000, DefaultSubscriptionLimit: 40, MaxAppsLimit: 100, DefaultAppsLimit: 20, MaxAccountNames: 100}
	return New(subs, users, apps, threadSubs, rooms, history, presence, pub, pub, cfg), subs, history
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

func TestGetDM_InvalidTarget(t *testing.T) {
	for _, target := range []string{"p_system", "helper.bot", "p_", ".bot", "p_.bot"} {
		t.Run(target, func(t *testing.T) {
			svc, _, _, _, _, _, _ := newSvc(t)
			_, err := svc.GetDM(ctx("alice", "site-a"), models.GetDMRequest{AccountName: target})
			requireCode(t, err, errcode.CodeBadRequest)
			assert.True(t, errcode.HasReason(err, errcode.UserInvalidDMTarget))
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

// newCountSvc builds a service exposing the subscription, room, and thread-sub
// mocks the thread-aware unread tests drive. maxSubs is large; per-test GetActiveSubscriptions
// stubs control the fetched page directly.
func newCountSvc(t *testing.T) (*UserService, *mocks.MockSubscriptionRepository, *mocks.MockRoomClient, *mocks.MockThreadSubscriptionRepository) {
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
	cfg := &config.Config{SiteID: "site-a", AllSiteIDs: []string{"site-a", "site-b"}, MaxSubscriptionLimit: 1000, DefaultSubscriptionLimit: 40, MaxAppsLimit: 100, DefaultAppsLimit: 20, MaxAccountNames: 100}
	return New(subs, users, apps, threadSubs, rooms, history, presence, pub, pub, cfg), subs, rooms, threadSubs
}

func TestCountUnread_Happy(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	seen := time.UnixMilli(100).UTC()
	newer := time.UnixMilli(200).UTC()
	subs.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(2, nil)
	// LOCAL sub: lastMsgAt is on the $lookup baseline — counted with NO RPC.
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 2).
		Return([]model.EnrichedSubscription{{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, LastMsgAt: &newer}}, nil)
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Count)
}

func TestCountUnread_FailedSiteSkipped(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	seen := time.UnixMilli(100).UTC()
	newer := time.UnixMilli(200).UTC()
	subs.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(5, nil)
	// One LOCAL unread (counted from the baseline) + one CROSS-SITE sub whose site's RPC fails.
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 5).
		Return([]model.EnrichedSubscription{
			{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, LastMsgAt: &newer}, // local unread
			{Subscription: model.Subscription{RoomID: "r2", SiteID: "site-b", LastSeenAt: &seen}},                    // cross-site, site fails
		}, nil)
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), "site-b", gomock.Any()).Return(nil, errors.New("down"))
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	// The unreachable site is SKIPPED; the local unread still counts — NOT a fallback to total(5).
	assert.Equal(t, 1, resp.Count)
}

func TestCountUnread_PartialFailureCountsHealthySites(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	seen := time.UnixMilli(100).UTC()
	newer := int64(200)
	subs.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(9, nil)
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 9).Return([]model.EnrichedSubscription{
		{Subscription: model.Subscription{RoomID: "rb1", SiteID: "site-b", LastSeenAt: &seen}}, // healthy site, unread
		{Subscription: model.Subscription{RoomID: "rc1", SiteID: "site-c", LastSeenAt: &seen}}, // failing site, skipped
	}, nil)
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), "site-b", gomock.Any()).
		Return([]model.RoomInfo{{RoomID: "rb1", Found: true, LastMsgAt: &newer}}, nil)
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), "site-c", gomock.Any()).Return(nil, errors.New("down"))
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	// site-b's unread counts; the unreachable site-c is skipped — NOT a fallback to total(9).
	assert.Equal(t, 1, resp.Count)
}

func TestCountUnread_GetActiveStoreError(t *testing.T) {
	svc, subs, _, _, _, _, _ := newSvc(t)
	subs.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(3, nil)
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 3).Return(nil, errors.New("db down"))
	yes := true
	_, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	requireCode(t, err, errcode.CodeInternal)
}

func TestCountUnread_MultiSite(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	seen := time.UnixMilli(100).UTC()
	newerT := time.UnixMilli(200).UTC()
	newer := int64(200)
	subs.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(4, nil)
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 4).Return([]model.EnrichedSubscription{
		{Subscription: model.Subscription{RoomID: "ra1", SiteID: "site-a", LastSeenAt: &seen}, LastMsgAt: &newerT}, // local unread (baseline)
		{Subscription: model.Subscription{RoomID: "ra2", SiteID: "site-a", LastSeenAt: &seen}},                     // local read (no lastMsgAt)
		{Subscription: model.Subscription{RoomID: "rb1", SiteID: "site-b", LastSeenAt: &seen}},
		{Subscription: model.Subscription{RoomID: "rb2", SiteID: "site-b", LastSeenAt: &seen}},
	}, nil)
	// Only the CROSS-SITE site is RPC'd; local rows are counted from the baseline.
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), "site-b", gomock.InAnyOrder([]string{"rb1", "rb2"})).
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
	subs.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(2, nil)
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 2).Return([]model.EnrichedSubscription{
		{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, LastMsgAt: &older},
		{Subscription: model.Subscription{RoomID: "r2", SiteID: "site-a", LastSeenAt: &seen}}, // no lastMsgAt → read
	}, nil)
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 0, resp.Count)
}

func TestCountUnread_EmptyActive(t *testing.T) {
	svc, subs, _, _, _, _, _ := newSvc(t)
	subs.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(0, nil)
	// Zero active subs must short-circuit before GetActiveSubscriptions (min(0,maxSubs)=0 → rejected $limit:0).
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 0, resp.Count)
}

func TestCountUnread_ContextCancelled_SkipsRPC(t *testing.T) {
	// A cancelled client context must short-circuit the cross-site fan-out before
	// firing any ~5s GetRoomsInfo RPC; local subs still count from the baseline.
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	seen := time.UnixMilli(100).UTC()
	newer := time.UnixMilli(200).UTC()
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 2).
		Return([]model.EnrichedSubscription{
			{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, LastMsgAt: &newer}, // local unread
			{Subscription: model.Subscription{RoomID: "r2", SiteID: "site-b", LastSeenAt: &seen}},                    // cross-site, must be skipped
		}, nil)
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	resp, err := svc.countUnread(cancelled, "alice", 2)
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Count, "cross-site site skipped on cancel; local unread still counts")
}

func TestCountUnread_CrossSiteDeletedRoomNotCounted(t *testing.T) {
	// A cross-site room soft-deleted at its origin still comes back Found=true with a
	// stale lastMsgAt over the RPC; it must NOT inflate the unread count — the list
	// path surfaces it room-less, and the badge must agree.
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	seen := time.UnixMilli(100).UTC()
	stale := int64(200) // newer than seen → WOULD count if the Del- room weren't skipped
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).
		Return([]model.EnrichedSubscription{
			{Subscription: model.Subscription{RoomID: "rd", SiteID: "site-b", LastSeenAt: &seen}},
		}, nil)
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), "site-b", gomock.Any()).
		Return([]model.RoomInfo{{RoomID: "rd", Found: true, Name: "Del-secret", LastMsgAt: &stale}}, nil)

	resp, err := svc.countUnread(context.Background(), "alice", 1)
	require.NoError(t, err)
	assert.Equal(t, 0, resp.Count, "a soft-deleted cross-site room must not be counted as unread")
}

func TestCountUnread_ReadRoomBumpedByUnreadThread(t *testing.T) {
	svc, subs, rooms, threadSubs := newCountSvc(t)
	seen := time.UnixMilli(100).UTC()
	older := time.UnixMilli(50).UTC()
	subs.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(1, nil)
	// One LOCAL room, read at the message level (lastMsgAt older than lastSeen).
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 1).Return([]model.EnrichedSubscription{
		{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, LastMsgAt: &older},
	}, nil)
	// Only the pending (read) room r1 is queried; it has one unread followed thread.
	threadSubs.EXPECT().ListByAccountInRooms(gomock.Any(), "alice", []string{"r1"}).Return([]model.ThreadUnreadRow{
		{ThreadRoomID: "tr1", RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen},
	}, nil)
	rooms.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), "site-a", []string{"tr1"}).
		Return([]model.ThreadRoomInfo{{ThreadRoomID: "tr1", Found: true, LastMsgAt: 200}}, nil)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Count)
}

func TestCountUnread_AlreadyUnreadRoomNotDoubleCounted(t *testing.T) {
	svc, subs, rooms, threadSubs := newCountSvc(t)
	seen := time.UnixMilli(100).UTC()
	newer := time.UnixMilli(300).UTC()
	subs.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(1, nil)
	// r1 is already room-level unread → must not be thread-checked, contributes exactly 1.
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 1).Return([]model.EnrichedSubscription{
		{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, LastMsgAt: &newer},
	}, nil)
	// Every room already unread ⇒ pendingRooms empty ⇒ no thread-sub read.
	threadSubs.EXPECT().ListByAccountInRooms(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	rooms.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Count)
}

func TestCountUnread_MultipleUnreadThreadsCountOnce(t *testing.T) {
	svc, subs, rooms, threadSubs := newCountSvc(t)
	seen := time.UnixMilli(100).UTC()
	older := time.UnixMilli(50).UTC()
	subs.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(1, nil)
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 1).Return([]model.EnrichedSubscription{
		{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, LastMsgAt: &older},
	}, nil)
	// Three unread threads, all in r1 → +1 total.
	threadSubs.EXPECT().ListByAccountInRooms(gomock.Any(), "alice", []string{"r1"}).Return([]model.ThreadUnreadRow{
		{ThreadRoomID: "tr1", RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen},
		{ThreadRoomID: "tr2", RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen},
		{ThreadRoomID: "tr3", RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen},
	}, nil)
	rooms.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), "site-a", gomock.InAnyOrder([]string{"tr1", "tr2", "tr3"})).
		Return([]model.ThreadRoomInfo{
			{ThreadRoomID: "tr1", Found: true, LastMsgAt: 200},
			{ThreadRoomID: "tr2", Found: true, LastMsgAt: 200},
			{ThreadRoomID: "tr3", Found: true, LastMsgAt: 200},
		}, nil)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Count)
}

func TestCountUnread_ThreadInRoomOutsidePageIgnored(t *testing.T) {
	svc, subs, rooms, threadSubs := newCountSvc(t)
	seen := time.UnixMilli(100).UTC()
	older := time.UnixMilli(50).UTC()
	subs.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(1, nil)
	// Fetched page contains only r1 (read).
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 1).Return([]model.EnrichedSubscription{
		{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, LastMsgAt: &older},
	}, nil)
	// The thread read is scoped to the pending room list — only r1 is queried, so a
	// thread in some other room (rX) is never fetched. Repo returns no threads for r1.
	threadSubs.EXPECT().ListByAccountInRooms(gomock.Any(), "alice", []string{"r1"}).Return(nil, nil)
	rooms.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 0, resp.Count)
}

func TestCountUnread_CrossSiteThreadResolution(t *testing.T) {
	svc, subs, rooms, threadSubs := newCountSvc(t)
	seen := time.UnixMilli(100).UTC()
	older := int64(50)
	subs.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(1, nil)
	// r1 lives on site-b, read at the message level.
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 1).Return([]model.EnrichedSubscription{
		{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-b", LastSeenAt: &seen}},
	}, nil)
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), "site-b", []string{"r1"}).
		Return([]model.RoomInfo{{RoomID: "r1", Found: true, LastMsgAt: &older}}, nil)
	// A followed thread in r1 on site-b, unread → resolved via the site-b batch.
	threadSubs.EXPECT().ListByAccountInRooms(gomock.Any(), "alice", []string{"r1"}).Return([]model.ThreadUnreadRow{
		{ThreadRoomID: "tr1", RoomID: "r1", SiteID: "site-b", LastSeenAt: &seen},
	}, nil)
	rooms.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), "site-b", []string{"tr1"}).
		Return([]model.ThreadRoomInfo{{ThreadRoomID: "tr1", Found: true, LastMsgAt: 200}}, nil)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Count)
}

func TestCountUnread_ThreadBatchFailureDegrades(t *testing.T) {
	svc, subs, rooms, threadSubs := newCountSvc(t)
	seen := time.UnixMilli(100).UTC()
	older := time.UnixMilli(50).UTC()
	subs.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(1, nil)
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 1).Return([]model.EnrichedSubscription{
		{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, LastMsgAt: &older},
	}, nil)
	threadSubs.EXPECT().ListByAccountInRooms(gomock.Any(), "alice", []string{"r1"}).Return([]model.ThreadUnreadRow{
		{ThreadRoomID: "tr1", RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen},
	}, nil)
	// Thread resolution fails → room un-bumped, count degrades to 0, no error.
	rooms.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), "site-a", []string{"tr1"}).
		Return(nil, errors.New("down"))
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 0, resp.Count)
}

func TestCountUnread_ThreadListErrorDegrades(t *testing.T) {
	svc, subs, rooms, threadSubs := newCountSvc(t)
	seen := time.UnixMilli(100).UTC()
	older := time.UnixMilli(50).UTC()
	subs.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(1, nil)
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 1).Return([]model.EnrichedSubscription{
		{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, LastMsgAt: &older},
	}, nil)
	// Local thread-sub read fails → degrade to the room-level count (0), never error.
	threadSubs.EXPECT().ListByAccountInRooms(gomock.Any(), "alice", []string{"r1"}).Return(nil, errors.New("db down"))
	rooms.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 0, resp.Count)
}

func TestCountUnread_MutedRoomThreadExcluded(t *testing.T) {
	svc, subs, rooms, threadSubs := newCountSvc(t)
	seen := time.UnixMilli(100).UTC()
	older := time.UnixMilli(50).UTC()
	subs.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(1, nil)
	// GetActiveSubscriptions already excludes muted rooms, so a muted room's parent
	// is never in the fetched page. Only r1 (unmuted, read) is returned.
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 1).Return([]model.EnrichedSubscription{
		{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, LastMsgAt: &older},
	}, nil)
	// The muted room is not in the fetched page, so it's not in the pending-room list;
	// only r1 is queried and it has no threads. The muted room can never bump.
	threadSubs.EXPECT().ListByAccountInRooms(gomock.Any(), "alice", []string{"r1"}).Return(nil, nil)
	rooms.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
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
	svc, subs, history := newSvcRawHistory(t)
	storeSubs := []model.EnrichedSubscription{{
		Subscription: model.Subscription{ID: "s1", RoomID: "r1", SiteID: "site-a", Name: "general", RoomType: model.RoomTypeChannel},
		RoomName:     "General", UserCount: 3,
	}}
	subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "current", false, gomock.Any(), gomock.Any()).
		Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{Data: storeSubs}, nil)
	history.EXPECT().RoomsGet(gomock.Any(), "site-a", []string{"r1"}).
		Return(map[string]model.PreviewMessage{"r1": {MessageID: "m9", Content: "hi", CreatedAt: 123}}, nil)
	resp, err := svc.ListSubscriptions(ctx("alice", "site-a"), models.SubscriptionListRequest{Type: "current"})
	require.NoError(t, err)
	require.Len(t, resp.Subscriptions, 1)
	room := resp.Subscriptions[0].Base().Room
	require.NotNil(t, room)
	require.NotNil(t, room.LastMessage, "last message attached from rooms.get")
	assert.Equal(t, "m9", room.LastMessage.MessageID)
}

// includeLastMessage:false skips the rooms.get RPC entirely.
func TestListSubscriptions_LastMessage_SkippedWhenExcluded(t *testing.T) {
	svc, subs, _ := newSvcRawHistory(t)
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
	assert.Nil(t, room.LastMessage, "excluded last message stays nil")
}

func TestListSubscriptions_LastMessage_SiteDegrades(t *testing.T) {
	svc, subs, history := newSvcRawHistory(t)
	storeSubs := []model.EnrichedSubscription{{
		Subscription: model.Subscription{ID: "s1", RoomID: "r1", SiteID: "site-a", Name: "general", RoomType: model.RoomTypeChannel},
		RoomName:     "General", UserCount: 3,
	}}
	subs.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", "current", false, gomock.Any(), gomock.Any()).
		Return(mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{Data: storeSubs}, nil)
	history.EXPECT().RoomsGet(gomock.Any(), "site-a", []string{"r1"}).
		Return(nil, errors.New("history down"))
	resp, err := svc.ListSubscriptions(ctx("alice", "site-a"), models.SubscriptionListRequest{Type: "current"})
	require.NoError(t, err, "a degraded rooms.get must not fail the list")
	require.Len(t, resp.Subscriptions, 1)
	room := resp.Subscriptions[0].Base().Room
	require.NotNil(t, room)
	assert.Nil(t, room.LastMessage, "degraded site leaves LastMessage nil")
}
