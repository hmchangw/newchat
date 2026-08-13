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
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/user-service/models"
)

func appWith(enabled bool) *model.App {
	return &model.App{ID: "app1", Name: "Helper", Assistant: &model.AppAssistant{Enabled: enabled, Name: "helper.bot"}}
}

func TestSetAppSubscription_EmptyAppID(t *testing.T) {
	svc, _, _, _, _, _, _ := newSvc(t)
	_, err := svc.SetAppSubscription(ctx("alice", "site-a"), models.SetAppSubscriptionRequest{AppID: "", Subscribed: true})
	requireCode(t, err, errcode.CodeBadRequest)
}

func TestSetAppSubscription_NotFound(t *testing.T) {
	svc, _, _, apps, _, _, _ := newSvc(t)
	apps.EXPECT().GetApp(gomock.Any(), "nope").Return(nil, nil)
	_, err := svc.SetAppSubscription(ctx("alice", "site-a"), models.SetAppSubscriptionRequest{AppID: "nope", Subscribed: true})
	requireCode(t, err, errcode.CodeNotFound)
	assert.True(t, errcode.HasReason(err, errcode.UserAppNotFound))
}

func TestSetAppSubscription_NilAssistant(t *testing.T) {
	svc, _, _, apps, _, _, _ := newSvc(t)
	apps.EXPECT().GetApp(gomock.Any(), "app1").Return(&model.App{ID: "app1", Name: "NoAssistant"}, nil)
	_, err := svc.SetAppSubscription(ctx("alice", "site-a"), models.SetAppSubscriptionRequest{AppID: "app1", Subscribed: true})
	requireCode(t, err, errcode.CodeBadRequest)
	assert.True(t, errcode.HasReason(err, errcode.UserAppDisabled))
}

// A disabled assistant (Enabled=false) is now subscribable — the enabled check was removed.
func TestSetAppSubscription_DisabledAssistantAllowed(t *testing.T) {
	svc, subs, _, apps, rooms, _, _ := newSvc(t)
	apps.EXPECT().GetApp(gomock.Any(), "app1").Return(appWith(false), nil)
	subs.EXPECT().GetAppSubscription(gomock.Any(), "alice", "helper.bot").Return(nil, nil)
	rooms.EXPECT().CreateDMRoom(gomock.Any(), "alice", "helper.bot", model.RoomTypeBotDM).Return(model.Subscription{ID: "new"}, nil)
	resp, err := svc.SetAppSubscription(ctx("alice", "site-a"), models.SetAppSubscriptionRequest{AppID: "app1", Subscribed: true})
	require.NoError(t, err)
	assert.True(t, resp.Success)
}

func TestSetAppSubscription_GetAppStoreError(t *testing.T) {
	svc, _, _, apps, _, _, _ := newSvc(t)
	apps.EXPECT().GetApp(gomock.Any(), "app1").Return(nil, errors.New("db down"))
	_, err := svc.SetAppSubscription(ctx("alice", "site-a"), models.SetAppSubscriptionRequest{AppID: "app1", Subscribed: true})
	requireCode(t, err, errcode.CodeInternal)
}

func TestSetAppSubscription_SubscribeNew(t *testing.T) {
	svc, subs, _, apps, rooms, _, _ := newSvc(t)
	apps.EXPECT().GetApp(gomock.Any(), "app1").Return(appWith(true), nil)
	subs.EXPECT().GetAppSubscription(gomock.Any(), "alice", "helper.bot").Return(nil, nil)
	rooms.EXPECT().CreateDMRoom(gomock.Any(), "alice", "helper.bot", model.RoomTypeBotDM).Return(model.Subscription{ID: "new"}, nil)
	resp, err := svc.SetAppSubscription(ctx("alice", "site-a"), models.SetAppSubscriptionRequest{AppID: "app1", Subscribed: true})
	require.NoError(t, err)
	assert.True(t, resp.Success)
}

func TestSetAppSubscription_GetAppSubscriptionStoreError(t *testing.T) {
	svc, subs, _, apps, _, _, _ := newSvc(t)
	apps.EXPECT().GetApp(gomock.Any(), "app1").Return(appWith(true), nil)
	subs.EXPECT().GetAppSubscription(gomock.Any(), "alice", "helper.bot").Return(nil, errors.New("db down"))
	_, err := svc.SetAppSubscription(ctx("alice", "site-a"), models.SetAppSubscriptionRequest{AppID: "app1", Subscribed: true})
	requireCode(t, err, errcode.CodeInternal)
}

func TestSetAppSubscription_CreateDMRoomError(t *testing.T) {
	svc, subs, _, apps, rooms, _, _ := newSvc(t)
	apps.EXPECT().GetApp(gomock.Any(), "app1").Return(appWith(true), nil)
	subs.EXPECT().GetAppSubscription(gomock.Any(), "alice", "helper.bot").Return(nil, nil)
	rooms.EXPECT().CreateDMRoom(gomock.Any(), "alice", "helper.bot", model.RoomTypeBotDM).Return(model.Subscription{}, errors.New("room svc down"))
	_, err := svc.SetAppSubscription(ctx("alice", "site-a"), models.SetAppSubscriptionRequest{AppID: "app1", Subscribed: true})
	requireCode(t, err, errcode.CodeInternal)
}

// resubExisting is the pre-write row GetAppSubscription returns on the re-subscribe path.
func resubExisting() *model.Subscription {
	return &model.Subscription{
		ID:       "ex",
		User:     model.SubscriptionUser{ID: "u1", Account: "alice", IsBot: false},
		RoomID:   "r1",
		SiteID:   "site-a",
		Name:     "helper.bot",
		RoomType: model.RoomTypeBotDM,
		Muted:    true,
	}
}

// resubEnriched is the room-enriched row GetSubscriptionByRoomID returns for the
// resub fan-out; isSubscribed/muted still carry the pre-write state on purpose —
// the publish path must patch them.
func resubEnriched() *model.EnrichedSubscription {
	lastMsg := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	local := false
	return &model.EnrichedSubscription{
		Subscription: *resubExisting(),
		RoomName:     "Helper",
		UserCount:    1,
		AppCount:     1,
		LastMsgAt:    &lastMsg,
		LastMsgID:    "m1",
		CrossSite:    &local,
	}
}

func TestSetAppSubscription_Reactivate_ClearsMuted(t *testing.T) {
	svc, subs, _, apps, _, _, pub := newSvc(t)
	apps.EXPECT().GetApp(gomock.Any(), "app1").Return(appWith(true), nil)
	subs.EXPECT().GetAppSubscription(gomock.Any(), "alice", "helper.bot").Return(resubExisting(), nil)
	subs.EXPECT().SetAppSubscribed(gomock.Any(), "alice", "helper.bot", true, false).Return(nil)
	subs.EXPECT().GetSubscriptionByRoomID(gomock.Any(), "alice", "r1").Return(resubEnriched(), nil)
	pub.EXPECT().Publish(gomock.Any(), subject.SubscriptionUpdate("alice"), gomock.Any()).Return(nil)
	resp, err := svc.SetAppSubscription(ctx("alice", "site-a"), models.SetAppSubscriptionRequest{AppID: "app1", Subscribed: true})
	require.NoError(t, err)
	assert.True(t, resp.Success)
}

func TestSetAppSubscription_Resub_PublishesAddedEvent(t *testing.T) {
	svc, subs, _, apps, _, _, pub := newSvc(t)
	apps.EXPECT().GetApp(gomock.Any(), "app1").Return(appWith(true), nil)
	subs.EXPECT().GetAppSubscription(gomock.Any(), "alice", "helper.bot").Return(resubExisting(), nil)
	subs.EXPECT().SetAppSubscribed(gomock.Any(), "alice", "helper.bot", true, false).Return(nil)
	subs.EXPECT().GetSubscriptionByRoomID(gomock.Any(), "alice", "r1").Return(resubEnriched(), nil)

	var published []byte
	pub.EXPECT().Publish(gomock.Any(), subject.SubscriptionUpdate("alice"), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data []byte) error {
			published = data
			return nil
		})

	resp, err := svc.SetAppSubscription(ctx("alice", "site-a"), models.SetAppSubscriptionRequest{AppID: "app1", Subscribed: true})
	require.NoError(t, err)
	assert.True(t, resp.Success)

	require.NotNil(t, published, "resub must fan out a subscription.update event")
	var evt model.SubscriptionUpdateEvent
	require.NoError(t, json.Unmarshal(published, &evt))
	assert.Equal(t, "added", evt.Action)
	assert.Equal(t, "u1", evt.UserID)
	assert.Equal(t, "Helper", evt.RoomName, "roomName is the app display name")
	require.NotNil(t, evt.AppInfo)
	assert.Equal(t, model.CounterpartAppInfo{ID: "app1", Name: "Helper", AssistantName: "helper.bot"}, *evt.AppInfo)
	assert.Nil(t, evt.HRInfo, "botDM counterpart is an app, never HR info")
	assert.Positive(t, evt.Timestamp)

	sub := evt.Subscription
	assert.Equal(t, "ex", sub.ID)
	assert.True(t, sub.IsSubscribed, "event must carry the post-write state, not the pre-write read")
	assert.False(t, sub.Muted, "event must carry the post-write state, not the pre-write read")
	require.NotNil(t, sub.Room, "added events embed the room baseline")
	assert.Equal(t, "Helper", sub.Room.Name)
	assert.Equal(t, "site-a", sub.Room.SiteID)
	assert.Equal(t, 1, sub.Room.UserCount)
	assert.Equal(t, 1, sub.Room.AppCount)
	assert.Equal(t, "m1", sub.Room.LastMsgID)
	require.NotNil(t, sub.Room.LastMsgAt)
	assert.True(t, sub.Room.LastMsgAt.Equal(time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)))
	require.NotNil(t, sub.Room.CrossSite)
	assert.False(t, *sub.Room.CrossSite)
	assert.Nil(t, sub.Room.PreviewMessage, "added events never leak a preview message")
}

func TestSetAppSubscription_Resub_RoomNameFallsBackToBotAccount(t *testing.T) {
	svc, subs, _, apps, _, _, pub := newSvc(t)
	nameless := &model.App{ID: "app1", Assistant: &model.AppAssistant{Enabled: true, Name: "helper.bot"}}
	apps.EXPECT().GetApp(gomock.Any(), "app1").Return(nameless, nil)
	subs.EXPECT().GetAppSubscription(gomock.Any(), "alice", "helper.bot").Return(resubExisting(), nil)
	subs.EXPECT().SetAppSubscribed(gomock.Any(), "alice", "helper.bot", true, false).Return(nil)
	subs.EXPECT().GetSubscriptionByRoomID(gomock.Any(), "alice", "r1").Return(resubEnriched(), nil)

	var published []byte
	pub.EXPECT().Publish(gomock.Any(), subject.SubscriptionUpdate("alice"), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data []byte) error {
			published = data
			return nil
		})

	_, err := svc.SetAppSubscription(ctx("alice", "site-a"), models.SetAppSubscriptionRequest{AppID: "app1", Subscribed: true})
	require.NoError(t, err)
	require.NotNil(t, published)
	var evt model.SubscriptionUpdateEvent
	require.NoError(t, json.Unmarshal(published, &evt))
	assert.Equal(t, "helper.bot", evt.RoomName, "empty app name falls back to the bot account")
	require.NotNil(t, evt.AppInfo)
	assert.Empty(t, evt.AppInfo.Name)
}

func TestSetAppSubscription_Resub_EnrichErrorStillSucceeds(t *testing.T) {
	svc, subs, _, apps, _, _, _ := newSvc(t)
	apps.EXPECT().GetApp(gomock.Any(), "app1").Return(appWith(true), nil)
	subs.EXPECT().GetAppSubscription(gomock.Any(), "alice", "helper.bot").Return(resubExisting(), nil)
	subs.EXPECT().SetAppSubscribed(gomock.Any(), "alice", "helper.bot", true, false).Return(nil)
	subs.EXPECT().GetSubscriptionByRoomID(gomock.Any(), "alice", "r1").Return(nil, errors.New("db down"))
	// No Publish expectation: the fan-out is best-effort and skipped on enrich failure.
	resp, err := svc.SetAppSubscription(ctx("alice", "site-a"), models.SetAppSubscriptionRequest{AppID: "app1", Subscribed: true})
	require.NoError(t, err)
	assert.True(t, resp.Success)
}

func TestSetAppSubscription_Resub_EnrichMissStillSucceeds(t *testing.T) {
	svc, subs, _, apps, _, _, _ := newSvc(t)
	apps.EXPECT().GetApp(gomock.Any(), "app1").Return(appWith(true), nil)
	subs.EXPECT().GetAppSubscription(gomock.Any(), "alice", "helper.bot").Return(resubExisting(), nil)
	subs.EXPECT().SetAppSubscribed(gomock.Any(), "alice", "helper.bot", true, false).Return(nil)
	subs.EXPECT().GetSubscriptionByRoomID(gomock.Any(), "alice", "r1").Return(nil, nil)
	resp, err := svc.SetAppSubscription(ctx("alice", "site-a"), models.SetAppSubscriptionRequest{AppID: "app1", Subscribed: true})
	require.NoError(t, err)
	assert.True(t, resp.Success)
}

func TestSetAppSubscription_Resub_DeletedRoomSkipsPublish(t *testing.T) {
	svc, subs, _, apps, _, _, _ := newSvc(t)
	apps.EXPECT().GetApp(gomock.Any(), "app1").Return(appWith(true), nil)
	subs.EXPECT().GetAppSubscription(gomock.Any(), "alice", "helper.bot").Return(resubExisting(), nil)
	subs.EXPECT().SetAppSubscribed(gomock.Any(), "alice", "helper.bot", true, false).Return(nil)
	deleted := resubEnriched()
	deleted.RoomName = "Del-Helper"
	subs.EXPECT().GetSubscriptionByRoomID(gomock.Any(), "alice", "r1").Return(deleted, nil)
	// No Publish expectation: an "added" for a soft-deleted room would resurrect a row subscription.list drops.
	resp, err := svc.SetAppSubscription(ctx("alice", "site-a"), models.SetAppSubscriptionRequest{AppID: "app1", Subscribed: true})
	require.NoError(t, err)
	assert.True(t, resp.Success)
}

func TestSetAppSubscription_Resub_PublishErrorStillSucceeds(t *testing.T) {
	svc, subs, _, apps, _, _, pub := newSvc(t)
	apps.EXPECT().GetApp(gomock.Any(), "app1").Return(appWith(true), nil)
	subs.EXPECT().GetAppSubscription(gomock.Any(), "alice", "helper.bot").Return(resubExisting(), nil)
	subs.EXPECT().SetAppSubscribed(gomock.Any(), "alice", "helper.bot", true, false).Return(nil)
	subs.EXPECT().GetSubscriptionByRoomID(gomock.Any(), "alice", "r1").Return(resubEnriched(), nil)
	pub.EXPECT().Publish(gomock.Any(), subject.SubscriptionUpdate("alice"), gomock.Any()).Return(errors.New("nats down"))
	resp, err := svc.SetAppSubscription(ctx("alice", "site-a"), models.SetAppSubscriptionRequest{AppID: "app1", Subscribed: true})
	require.NoError(t, err)
	assert.True(t, resp.Success)
}

func TestSetAppSubscription_ReactivateSetAppSubscribedError(t *testing.T) {
	svc, subs, _, apps, _, _, _ := newSvc(t)
	apps.EXPECT().GetApp(gomock.Any(), "app1").Return(appWith(true), nil)
	subs.EXPECT().GetAppSubscription(gomock.Any(), "alice", "helper.bot").Return(&model.Subscription{ID: "ex"}, nil)
	subs.EXPECT().SetAppSubscribed(gomock.Any(), "alice", "helper.bot", true, false).Return(errors.New("db down"))
	_, err := svc.SetAppSubscription(ctx("alice", "site-a"), models.SetAppSubscriptionRequest{AppID: "app1", Subscribed: true})
	requireCode(t, err, errcode.CodeInternal)
}

func TestSetAppSubscription_Unsubscribe(t *testing.T) {
	svc, subs, _, apps, _, _, _ := newSvc(t)
	apps.EXPECT().GetApp(gomock.Any(), "app1").Return(appWith(true), nil)
	subs.EXPECT().SetAppSubscribed(gomock.Any(), "alice", "helper.bot", false, true).Return(nil)
	resp, err := svc.SetAppSubscription(ctx("alice", "site-a"), models.SetAppSubscriptionRequest{AppID: "app1", Subscribed: false})
	require.NoError(t, err)
	assert.True(t, resp.Success)
}

func TestSetAppSubscription_UnsubscribeSetAppSubscribedError(t *testing.T) {
	svc, subs, _, apps, _, _, _ := newSvc(t)
	apps.EXPECT().GetApp(gomock.Any(), "app1").Return(appWith(true), nil)
	subs.EXPECT().SetAppSubscribed(gomock.Any(), "alice", "helper.bot", false, true).Return(errors.New("db down"))
	_, err := svc.SetAppSubscription(ctx("alice", "site-a"), models.SetAppSubscriptionRequest{AppID: "app1", Subscribed: false})
	requireCode(t, err, errcode.CodeInternal)
}

func TestListApps(t *testing.T) {
	svc, _, _, apps, _, _, _ := newSvc(t)
	// Empty request → defaults: offset 0, limit 20.
	apps.EXPECT().ListApps(gomock.Any(), "alice", mongoutil.OffsetPageRequest{Offset: 0, Limit: 20}).
		Return(mongoutil.OffsetPageHasMore[models.AppListItem]{Data: []models.AppListItem{
			{App: model.App{ID: "a1"}, IsSubscribed: true},
			{App: model.App{ID: "a2"}},
		}}, nil)
	resp, err := svc.ListApps(ctx("alice", "site-a"), models.AppsListRequest{})
	require.NoError(t, err)
	assert.Len(t, resp.Apps, 2)
	assert.False(t, resp.HasMore)
	assert.True(t, resp.Apps[0].IsSubscribed)
}

func TestListApps_PageRequestForwarding(t *testing.T) {
	tests := []struct {
		name string
		req  models.AppsListRequest
		want mongoutil.OffsetPageRequest
	}{
		{"explicit values forwarded", models.AppsListRequest{Limit: 5, Offset: 40}, mongoutil.OffsetPageRequest{Offset: 40, Limit: 5}},
		{"limit capped at 100", models.AppsListRequest{Limit: 500, Offset: 1}, mongoutil.OffsetPageRequest{Offset: 1, Limit: 100}},
		{"negatives clamped", models.AppsListRequest{Limit: -1, Offset: -7}, mongoutil.OffsetPageRequest{Offset: 0, Limit: 20}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, _, _, apps, _, _, _ := newSvc(t)
			apps.EXPECT().ListApps(gomock.Any(), "alice", tt.want).
				Return(mongoutil.OffsetPageHasMore[models.AppListItem]{Data: []models.AppListItem{}}, nil)
			_, err := svc.ListApps(ctx("alice", "site-a"), tt.req)
			require.NoError(t, err)
		})
	}
}

func TestListApps_HasMoreForwarded(t *testing.T) {
	svc, _, _, apps, _, _, _ := newSvc(t)
	apps.EXPECT().ListApps(gomock.Any(), "alice", mongoutil.OffsetPageRequest{Offset: 0, Limit: 2}).
		Return(mongoutil.OffsetPageHasMore[models.AppListItem]{Data: []models.AppListItem{
			{App: model.App{ID: "a1"}},
			{App: model.App{ID: "a2"}},
		}, HasMore: true}, nil)
	resp, err := svc.ListApps(ctx("alice", "site-a"), models.AppsListRequest{Limit: 2})
	require.NoError(t, err)
	assert.Len(t, resp.Apps, 2)
	assert.True(t, resp.HasMore, "hasMore is forwarded from the repo page")
}

func TestListApps_StoreError(t *testing.T) {
	svc, _, _, apps, _, _, _ := newSvc(t)
	apps.EXPECT().ListApps(gomock.Any(), "alice", gomock.Any()).
		Return(mongoutil.OffsetPageHasMore[models.AppListItem]{}, errors.New("db down"))
	_, err := svc.ListApps(ctx("alice", "site-a"), models.AppsListRequest{})
	requireCode(t, err, errcode.CodeInternal)
}

func TestListApps_Empty(t *testing.T) {
	svc, _, _, apps, _, _, _ := newSvc(t)
	apps.EXPECT().ListApps(gomock.Any(), "alice", gomock.Any()).
		Return(mongoutil.OffsetPageHasMore[models.AppListItem]{Data: []models.AppListItem{}}, nil)
	resp, err := svc.ListApps(ctx("alice", "site-a"), models.AppsListRequest{})
	require.NoError(t, err)
	assert.False(t, resp.HasMore)
	assert.Empty(t, resp.Apps)
}

func TestListAppCategories_Success(t *testing.T) {
	svc, _, _, apps, _, _, _ := newSvc(t)
	apps.EXPECT().ListAppCategories(gomock.Any()).Return([]models.AppCategory{
		{ID: "m1", Name: "F22", SiteID: "00600000"},
		{ID: "m2", Name: "F14", SiteID: "00700000"},
	}, nil)

	resp, err := svc.ListAppCategories(ctx("alice", "site-a"))
	require.NoError(t, err)
	assert.Equal(t, []models.AppCategory{
		{ID: "m1", Name: "F22", SiteID: "00600000"},
		{ID: "m2", Name: "F14", SiteID: "00700000"},
	}, resp.Categories)
}

func TestListAppCategories_EmptyIsArrayNotNull(t *testing.T) {
	svc, _, _, apps, _, _, _ := newSvc(t)
	apps.EXPECT().ListAppCategories(gomock.Any()).Return(nil, nil)

	resp, err := svc.ListAppCategories(ctx("alice", "site-a"))
	require.NoError(t, err)
	require.NotNil(t, resp.Categories, "a nil slice marshals to JSON null; the contract is []")
	assert.Empty(t, resp.Categories)
}

func TestListAppCategories_RepoError(t *testing.T) {
	svc, _, _, apps, _, _, _ := newSvc(t)
	apps.EXPECT().ListAppCategories(gomock.Any()).Return(nil, errors.New("mongo down"))

	_, err := svc.ListAppCategories(ctx("alice", "site-a"))
	requireCode(t, err, errcode.CodeInternal)
}
