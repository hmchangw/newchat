package service

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/user-service/config"
	"github.com/hmchangw/chat/user-service/service/mocks"
)

// newThreadSvc builds a UserService whose fan-out set is site-a (local) + site-b
// (cross), from ALL_SITE_IDS. The thread inbox only depends on the history
// client, so the other deps are fresh no-expectation mocks.
func newThreadSvc(t *testing.T) (*UserService, *mocks.MockHistoryClient, *mocks.MockUserRepository, *mocks.MockAppRepository) {
	t.Helper()
	ctrl := gomock.NewController(t)
	history := mocks.NewMockHistoryClient(ctrl)
	users := mocks.NewMockUserRepository(ctrl)
	apps := mocks.NewMockAppRepository(ctrl)
	cfg := &config.Config{SiteID: "site-a", AllSiteIDs: []string{"site-a", "site-b"}, MaxSubscriptionLimit: 1000, MaxAccountNames: 100}
	svc := New(
		mocks.NewMockSubscriptionRepository(ctrl),
		users,
		apps,
		mocks.NewMockThreadSubscriptionRepository(ctrl),
		mocks.NewMockRoomClient(ctrl),
		history,
		mocks.NewMockPresenceClient(ctrl),
		mocks.NewMockEventPublisher(ctrl),
		mocks.NewMockEventPublisher(ctrl),
		nil, nil, nil,
		cfg,
	)
	return svc, history, users, apps
}

func item(site, threadRoomID string, lastMsgAt int64) model.ThreadListItem {
	return model.ThreadListItem{SiteID: site, ThreadRoomID: threadRoomID, LastMsgAt: lastMsgAt}
}

// expectThreadList stubs one site's GetThreadList with the given items / hasMore.
func expectThreadList(history *mocks.MockHistoryClient, site string, items []model.ThreadListItem, hasMore bool) {
	history.EXPECT().GetThreadList(gomock.Any(), site, gomock.Any()).
		Return(model.ThreadSubscriptionListResponse{Items: items, HasMore: hasMore}, nil)
}

func TestUserService_ListUserThreads_MergeAcrossSites(t *testing.T) {
	svc, history, _, _ := newThreadSvc(t)
	expectThreadList(history, "site-a", []model.ThreadListItem{item("site-a", "ta1", 50), item("site-a", "ta2", 20)}, false)
	expectThreadList(history, "site-b", []model.ThreadListItem{item("site-b", "tb1", 40), item("site-b", "tb2", 30)}, false)

	resp, err := svc.ListUserThreads(ctx("alice", "site-a"), model.ThreadListRequest{Limit: 10})
	require.NoError(t, err)
	require.Len(t, resp.Items, 4)
	// Global DESC by lastMsgAt: 50, 40, 30, 20.
	assert.Equal(t, []string{"ta1", "tb1", "tb2", "ta2"}, ids(resp.Items))
	assert.False(t, resp.HasNext)
	assert.Empty(t, resp.NextCursor)
	assert.Empty(t, resp.UnavailableSites)
}

func TestUserService_ListUserThreads_PaginatesAndSetsCursor(t *testing.T) {
	svc, history, _, _ := newThreadSvc(t)
	expectThreadList(history, "site-a", []model.ThreadListItem{item("site-a", "t1", 50), item("site-a", "t2", 40), item("site-a", "t3", 30)}, true)
	expectThreadList(history, "site-b", nil, false)

	resp, err := svc.ListUserThreads(ctx("alice", "site-a"), model.ThreadListRequest{Limit: 2})
	require.NoError(t, err)
	require.Len(t, resp.Items, 2)
	assert.Equal(t, []string{"t1", "t2"}, ids(resp.Items))
	assert.True(t, resp.HasNext)
	require.NotEmpty(t, resp.NextCursor)
	// Cursor encodes the last emitted item's position.
	cur, err := decodeThreadCursor(resp.NextCursor)
	require.NoError(t, err)
	require.NotNil(t, cur)
	assert.Equal(t, int64(40), cur.LastMsgAt)
	assert.Equal(t, "t2", cur.ThreadRoomID)
}

func TestUserService_ListUserThreads_AllSitesEmpty(t *testing.T) {
	svc, history, _, _ := newThreadSvc(t)
	expectThreadList(history, "site-a", nil, false)
	expectThreadList(history, "site-b", nil, false)

	resp, err := svc.ListUserThreads(ctx("alice", "site-a"), model.ThreadListRequest{})
	require.NoError(t, err)
	assert.Empty(t, resp.Items)
	assert.NotNil(t, resp.Items) // never nil — JSON [] not null
	assert.False(t, resp.HasNext)
}

func TestUserService_ListUserThreads_BadCursor(t *testing.T) {
	svc, _, _, _ := newThreadSvc(t)
	// Decode happens before any fan-out, so no GetThreadList calls are made.
	_, err := svc.ListUserThreads(ctx("alice", "site-a"), model.ThreadListRequest{Cursor: "!!!not-base64"})
	requireCode(t, err, errcode.CodeBadRequest)
}

func TestUserService_ListUserThreads_PartialFailureDegrades(t *testing.T) {
	svc, history, _, _ := newThreadSvc(t)
	expectThreadList(history, "site-a", []model.ThreadListItem{item("site-a", "ta1", 50)}, false)
	history.EXPECT().GetThreadList(gomock.Any(), "site-b", gomock.Any()).
		Return(model.ThreadSubscriptionListResponse{}, errors.New("site-b down"))

	resp, err := svc.ListUserThreads(ctx("alice", "site-a"), model.ThreadListRequest{Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, []string{"ta1"}, ids(resp.Items))
	assert.Equal(t, []string{"site-b"}, resp.UnavailableSites)
}

func TestUserService_ListUserThreads_Tiebreak(t *testing.T) {
	svc, history, _, _ := newThreadSvc(t)
	expectThreadList(history, "site-a", []model.ThreadListItem{
		item("site-a", "ta", 50), item("site-a", "tc", 50), item("site-a", "tb", 50),
	}, false)
	expectThreadList(history, "site-b", nil, false)

	resp, err := svc.ListUserThreads(ctx("alice", "site-a"), model.ThreadListRequest{Limit: 10})
	require.NoError(t, err)
	// Equal lastMsgAt ⇒ threadRoomId DESC: tc, tb, ta.
	assert.Equal(t, []string{"tc", "tb", "ta"}, ids(resp.Items))
}

func TestUserService_ListUserThreads_PassesCursorToLeaf(t *testing.T) {
	svc, history, _, _ := newThreadSvc(t)
	var got model.ThreadSubscriptionListRequest
	history.EXPECT().GetThreadList(gomock.Any(), "site-a", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, req model.ThreadSubscriptionListRequest) (model.ThreadSubscriptionListResponse, error) {
			got = req
			return model.ThreadSubscriptionListResponse{}, nil
		})
	expectThreadList(history, "site-b", nil, false)

	cursor := encodeThreadCursor(threadCursor{LastMsgAt: 1234, ThreadRoomID: "t9"})
	_, err := svc.ListUserThreads(ctx("alice", "site-a"), model.ThreadListRequest{Cursor: cursor, Limit: 7})
	require.NoError(t, err)
	assert.Equal(t, "alice", got.Account)
	assert.Equal(t, 7, got.Limit)
	require.NotNil(t, got.CursorLastMsgAt)
	assert.Equal(t, int64(1234), *got.CursorLastMsgAt)
	assert.Equal(t, "t9", got.CursorThreadRoomID)
}

// dmItem is a DM thread row whose RoomName carries the counterpart account.
func dmItem(site, threadRoomID string, lastMsgAt int64, counterpart string) model.ThreadListItem {
	return model.ThreadListItem{SiteID: site, ThreadRoomID: threadRoomID, LastMsgAt: lastMsgAt, RoomType: model.RoomTypeDM, RoomName: counterpart}
}

// On the final page, DM rows are enriched with the counterpart's HR record
// (resolved from RoomName); non-DM rows are left untouched.
func TestUserService_ListUserThreads_DM_CarriesHRInfo(t *testing.T) {
	svc, history, users, _ := newThreadSvc(t)
	expectThreadList(history, "site-a", []model.ThreadListItem{dmItem("site-a", "td", 50, "bob"), item("site-a", "tc", 40)}, false)
	expectThreadList(history, "site-b", nil, false)
	users.EXPECT().GetHRInfoByAccounts(gomock.Any(), []string{"bob"}).
		Return(map[string]*model.SubscriptionHRInfo{"bob": {Account: "bob", Name: "鮑勃", EngName: "Bob Chen"}}, nil)

	resp, err := svc.ListUserThreads(ctx("alice", "site-a"), model.ThreadListRequest{Limit: 10})
	require.NoError(t, err)
	require.Len(t, resp.Items, 2)

	dm := resp.Items[0]
	require.Equal(t, "td", dm.ThreadRoomID)
	require.NotNil(t, dm.HRInfo, "DM row must carry hrInfo")
	assert.Equal(t, "鮑勃", dm.HRInfo.Name)
	assert.Equal(t, "Bob Chen", dm.HRInfo.EngName)
	assert.Nil(t, resp.Items[1].HRInfo, "channel row must not carry hrInfo")
}

// A failed HR lookup degrades to no hrInfo — it never fails the request.
func TestUserService_ListUserThreads_HRInfoDegrades(t *testing.T) {
	svc, history, users, _ := newThreadSvc(t)
	expectThreadList(history, "site-a", []model.ThreadListItem{dmItem("site-a", "td", 50, "bob")}, false)
	expectThreadList(history, "site-b", nil, false)
	users.EXPECT().GetHRInfoByAccounts(gomock.Any(), []string{"bob"}).Return(nil, errors.New("hr down"))

	resp, err := svc.ListUserThreads(ctx("alice", "site-a"), model.ThreadListRequest{Limit: 10})
	require.NoError(t, err, "hr lookup failure must degrade, not fail the request")
	require.Len(t, resp.Items, 1)
	assert.Nil(t, resp.Items[0].HRInfo)
}

// botDMItem is a botDM thread row whose RoomName carries the bot account.
func botDMItem(site, threadRoomID string, lastMsgAt int64, botAccount string) model.ThreadListItem {
	return model.ThreadListItem{SiteID: site, ThreadRoomID: threadRoomID, LastMsgAt: lastMsgAt, RoomType: model.RoomTypeBotDM, RoomName: botAccount}
}

// On the final page, a botDM row's RoomName (the bot account) is replaced with
// the app's display name; non-botDM rows are left untouched.
func TestUserService_ListUserThreads_BotDM_ReplacesRoomNameWithAppName(t *testing.T) {
	svc, history, _, apps := newThreadSvc(t)
	channel := model.ThreadListItem{SiteID: "site-a", ThreadRoomID: "tc", LastMsgAt: 40, RoomType: model.RoomTypeChannel, RoomName: "general"}
	expectThreadList(history, "site-a", []model.ThreadListItem{botDMItem("site-a", "tb", 50, "helper.bot"), channel}, false)
	expectThreadList(history, "site-b", nil, false)
	apps.EXPECT().GetAppsByAssistants(gomock.Any(), []string{"helper.bot"}).
		Return(map[string]*model.App{"helper.bot": {ID: "app1", Name: "Helper App", Assistant: &model.AppAssistant{Name: "helper.bot"}}}, nil)

	resp, err := svc.ListUserThreads(ctx("alice", "site-a"), model.ThreadListRequest{Limit: 10})
	require.NoError(t, err)
	require.Len(t, resp.Items, 2)

	bot := resp.Items[0]
	require.Equal(t, "tb", bot.ThreadRoomID)
	assert.Equal(t, "Helper App", bot.RoomName, "botDM roomName must be replaced with the app name")
	assert.Equal(t, "general", resp.Items[1].RoomName, "channel row roomName must be left untouched")
}

// A failed/missing app lookup keeps the bot account as roomName — never fails the request.
func TestUserService_ListUserThreads_BotDM_AppLookupDegrades(t *testing.T) {
	svc, history, _, apps := newThreadSvc(t)
	expectThreadList(history, "site-a", []model.ThreadListItem{botDMItem("site-a", "tb", 50, "helper.bot")}, false)
	expectThreadList(history, "site-b", nil, false)
	apps.EXPECT().GetAppsByAssistants(gomock.Any(), []string{"helper.bot"}).Return(nil, errors.New("apps down"))

	resp, err := svc.ListUserThreads(ctx("alice", "site-a"), model.ThreadListRequest{Limit: 10})
	require.NoError(t, err, "app lookup failure must degrade, not fail the request")
	require.Len(t, resp.Items, 1)
	assert.Equal(t, "helper.bot", resp.Items[0].RoomName, "degraded app lookup keeps the bot account")
}

func ids(items []model.ThreadListItem) []string {
	out := make([]string, len(items))
	for i := range items {
		out[i] = items[i].ThreadRoomID
	}
	return out
}
