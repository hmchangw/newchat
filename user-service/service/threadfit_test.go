package service

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/pagefit"
	"github.com/hmchangw/chat/user-service/config"
	"github.com/hmchangw/chat/user-service/service/mocks"
)

// newThreadSvcWithBudget mirrors newThreadSvc but caps the reply at b.
func newThreadSvcWithBudget(t *testing.T, b pagefit.Budget) (*UserService, *mocks.MockHistoryClient, *mocks.MockUserRepository, *mocks.MockAppRepository) {
	t.Helper()
	ctrl := gomock.NewController(t)
	history := mocks.NewMockHistoryClient(ctrl)
	users := mocks.NewMockUserRepository(ctrl)
	apps := mocks.NewMockAppRepository(ctrl)
	cfg := &config.Config{SiteID: "site-a", AllSiteIDs: []string{"site-a", "site-b"}, MaxSubscriptionLimit: 1000, MaxAccountNames: 100}
	svc := New(
		mocks.NewMockSubscriptionRepository(ctrl), users, apps,
		mocks.NewMockThreadSubscriptionRepository(ctrl), mocks.NewMockRoomClient(ctrl), history,
		mocks.NewMockPresenceClient(ctrl), mocks.NewMockEventPublisher(ctrl), mocks.NewMockEventPublisher(ctrl),
		&fakeBadgeCache{}, nil, nil, nil, cfg, WithPageBudget(b),
	)
	return svc, history, users, apps
}

// fatItems builds channel rows (no enrichment) whose size is dominated by the
// forwarded parent message body, DESC by lastMsgAt.
func fatItems(site string, n, width int) []model.ThreadListItem {
	out := make([]model.ThreadListItem, n)
	for i := range out {
		out[i] = model.ThreadListItem{
			SiteID:        site,
			ThreadRoomID:  fmt.Sprintf("%s-t%02d", site, i),
			RoomType:      model.RoomTypeChannel,
			LastMsgAt:     int64(1000 - i),
			ParentMessage: json.RawMessage(fmt.Sprintf(`{"msg":%q}`, strings.Repeat("x", width))),
		}
	}
	return out
}

func TestListUserThreads_TrimsOversizePageAndSetsHasNext(t *testing.T) {
	items := fatItems("site-a", 20, 512)
	encoded, err := json.Marshal(items[:5])
	require.NoError(t, err)

	svc, history, _, _ := newThreadSvcWithBudget(t, pagefit.NewBudget(int64(len(encoded)), 0))
	expectThreadList(history, "site-a", items, false)
	expectThreadList(history, "site-b", nil, false)

	resp, err := svc.ListUserThreads(ctx("alice", "site-a"), model.ThreadListRequest{Limit: 100})
	require.NoError(t, err)

	assert.Less(t, len(resp.Items), len(items), "an oversize page must be trimmed")
	assert.NotEmpty(t, resp.Items)
	assert.True(t, resp.HasNext, "dropped rows must be advertised as more")
}

// The cursor is the position the client resumes from. Deriving it from the
// pre-trim last item would skip every row the trim dropped.
func TestListUserThreads_CursorIsDerivedFromTheLastKeptItem(t *testing.T) {
	items := fatItems("site-a", 20, 512)
	encoded, err := json.Marshal(items[:5])
	require.NoError(t, err)

	svc, history, _, _ := newThreadSvcWithBudget(t, pagefit.NewBudget(int64(len(encoded)), 0))
	expectThreadList(history, "site-a", items, false)
	expectThreadList(history, "site-b", nil, false)

	resp, err := svc.ListUserThreads(ctx("alice", "site-a"), model.ThreadListRequest{Limit: 100})
	require.NoError(t, err)
	require.NotEmpty(t, resp.NextCursor)

	cur, err := decodeThreadCursor(resp.NextCursor)
	require.NoError(t, err)
	last := resp.Items[len(resp.Items)-1]
	assert.Equal(t, last.LastMsgAt, cur.LastMsgAt)
	assert.Equal(t, last.ThreadRoomID, cur.ThreadRoomID)
}

func TestListUserThreads_PageThatFitsIsUntouched(t *testing.T) {
	items := fatItems("site-a", 3, 16)
	svc, history, _, _ := newThreadSvcWithBudget(t, pagefit.NewBudget(1<<20, 0))
	expectThreadList(history, "site-a", items, false)
	expectThreadList(history, "site-b", nil, false)

	resp, err := svc.ListUserThreads(ctx("alice", "site-a"), model.ThreadListRequest{Limit: 100})
	require.NoError(t, err)
	assert.Len(t, resp.Items, 3)
	assert.False(t, resp.HasNext)
}

// enrichThreadPage adds bytes after the count-trim, so a page measured before
// enrichment can still overflow. The byte-trim must therefore run after it.
func TestListUserThreads_TrimRunsAfterEnrichment(t *testing.T) {
	const n = 12
	items := make([]model.ThreadListItem, n)
	for i := range items {
		items[i] = model.ThreadListItem{
			SiteID:       "site-a",
			ThreadRoomID: fmt.Sprintf("t%02d", i),
			RoomType:     model.RoomTypeDM,
			RoomName:     "bob",
			LastMsgAt:    int64(1000 - i),
		}
	}
	bare, err := json.Marshal(items)
	require.NoError(t, err)

	// A budget the bare rows clear comfortably; enrichment is what tips it over.
	budget := pagefit.NewBudget(int64(len(bare)), 0)
	svc, history, users, _ := newThreadSvcWithBudget(t, budget)
	expectThreadList(history, "site-a", items, false)
	expectThreadList(history, "site-b", nil, false)
	users.EXPECT().GetHRInfoByAccounts(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ any, accounts []string) (map[string]*model.SubscriptionHRInfo, error) {
			out := make(map[string]*model.SubscriptionHRInfo, len(accounts))
			for _, a := range accounts {
				out[a] = &model.SubscriptionHRInfo{Account: a, Name: strings.Repeat("N", 256), EngName: strings.Repeat("E", 256)}
			}
			return out, nil
		}).AnyTimes()

	resp, err := svc.ListUserThreads(ctx("alice", "site-a"), model.ThreadListRequest{Limit: 100})
	require.NoError(t, err)

	encoded, err := json.Marshal(resp.Items)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(encoded), budget.Bytes()+2,
		"the enriched page must fit — trimming before enrichment would overflow again")
}
