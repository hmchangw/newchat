package service

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/user-service/config"
	"github.com/hmchangw/chat/user-service/service/mocks"
)

// newTopologyThreadSvc builds a UserService whose thread fan-out set is exactly
// allSiteIDs, so a test can pick the topology (local-only, or several remotes)
// instead of the fixed site-a/site-b pair newThreadSvc hardcodes.
func newTopologyThreadSvc(t *testing.T, siteID string, allSiteIDs []string) (*UserService, *mocks.MockHistoryClient) {
	t.Helper()
	ctrl := gomock.NewController(t)
	history := mocks.NewMockHistoryClient(ctrl)
	cfg := &config.Config{SiteID: siteID, AllSiteIDs: allSiteIDs, MaxSubscriptionLimit: 1000, MaxAccountNames: 100, MaxSiteFanout: 8}
	svc := New(
		mocks.NewMockSubscriptionRepository(ctrl),
		mocks.NewMockUserRepository(ctrl),
		mocks.NewMockAppRepository(ctrl),
		mocks.NewMockThreadSubscriptionRepository(ctrl),
		mocks.NewMockRoomClient(ctrl),
		history,
		mocks.NewMockPresenceClient(ctrl),
		mocks.NewMockEventPublisher(ctrl),
		mocks.NewMockEventPublisher(ctrl),
		&fakeBadgeCache{},
		nil, nil, nil,
		cfg,
	)
	return svc, history
}

// The caller's own site is served directly, so its failure degrades the same
// way a remote one does: the site is reported unavailable and the surviving
// sites still contribute their rows.
func TestUserService_ListUserThreads_LocalSiteFailureDegrades(t *testing.T) {
	svc, history, _, _ := newThreadSvc(t)
	history.EXPECT().GetThreadList(gomock.Any(), "site-a", gomock.Any()).
		Return(model.ThreadSubscriptionListResponse{}, errors.New("local history down"))
	expectThreadList(history, "site-b", []model.ThreadListItem{item("site-b", "tb1", 40), item("site-b", "tb2", 30)}, true)

	resp, err := svc.ListUserThreads(ctx("alice", "site-a"), model.ThreadListRequest{Limit: 10})
	require.NoError(t, err, "a failed local site must degrade, not fail the request")
	assert.Equal(t, []string{"tb1", "tb2"}, ids(resp.Items), "only the healthy site contributes rows")
	assert.Equal(t, []string{"site-a"}, resp.UnavailableSites)
	assert.True(t, resp.HasNext, "the healthy site's hasMore still drives pagination")
}

// Both sites down: every row is lost, both are reported, and the empty page is
// still a well-formed success.
func TestUserService_ListUserThreads_AllSitesFailDegrades(t *testing.T) {
	svc, history, _, _ := newThreadSvc(t)
	history.EXPECT().GetThreadList(gomock.Any(), "site-a", gomock.Any()).
		Return(model.ThreadSubscriptionListResponse{}, errors.New("local history down"))
	history.EXPECT().GetThreadList(gomock.Any(), "site-b", gomock.Any()).
		Return(model.ThreadSubscriptionListResponse{}, errors.New("site-b down"))

	resp, err := svc.ListUserThreads(ctx("alice", "site-a"), model.ThreadListRequest{Limit: 10})
	require.NoError(t, err)
	assert.Empty(t, resp.Items)
	assert.NotNil(t, resp.Items, "never nil — JSON [] not null")
	assert.Equal(t, []string{"site-a", "site-b"}, resp.UnavailableSites)
	assert.False(t, resp.HasNext)
	assert.Empty(t, resp.NextCursor)
}

// A single-site deployment has no remote peers: the fan-out machinery is
// skipped entirely and the local rows are served on their own.
func TestUserService_ListUserThreads_LocalOnlyTopology(t *testing.T) {
	tests := []struct {
		name       string
		allSiteIDs []string
	}{
		{"local site is the only configured site", []string{"site-a"}},
		{"no federation sites configured", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, history := newTopologyThreadSvc(t, "site-a", tt.allSiteIDs)
			// The strict mock has one expectation: any cross-site RPC fails the test.
			expectThreadList(history, "site-a", []model.ThreadListItem{item("site-a", "ta1", 50)}, false)

			resp, err := svc.ListUserThreads(ctx("alice", "site-a"), model.ThreadListRequest{Limit: 10})
			require.NoError(t, err)
			assert.Equal(t, []string{"ta1"}, ids(resp.Items))
			assert.Empty(t, resp.UnavailableSites)
			assert.False(t, resp.HasNext)
		})
	}
}

// A context already cancelled when the fan-out starts admits no remote site:
// each is marked unavailable without an RPC, while the direct local read still
// returns its rows.
func TestUserService_ListUserThreads_CancelledContextSkipsRemoteSites(t *testing.T) {
	svc, history := newTopologyThreadSvc(t, "site-a", []string{"site-a", "site-b", "site-c"})
	// Only the local read is expected — a remote RPC on a cancelled context fails the test.
	expectThreadList(history, "site-a", []model.ThreadListItem{item("site-a", "ta1", 50)}, false)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	c := ctx("alice", "site-a")
	c.SetContext(cancelled)

	resp, err := svc.ListUserThreads(c, model.ThreadListRequest{Limit: 10})
	require.NoError(t, err, "cancellation degrades the remote sites, it does not fail the request")
	assert.Equal(t, []string{"ta1"}, ids(resp.Items))
	assert.Equal(t, []string{"site-b", "site-c"}, resp.UnavailableSites)
}

// cancelAfterCtx stays live for the first `remaining` Err() observations and is
// cancelled from then on. It lets a test land a cancellation strictly between
// the fan-out's admission check and the worker goroutine's re-check, which real
// wall-clock cancellation could only hit by luck.
type cancelAfterCtx struct {
	context.Context
	mu        sync.Mutex
	remaining int
}

func (c *cancelAfterCtx) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.remaining > 0 {
		c.remaining--
		return nil
	}
	return context.Canceled
}

// A site admitted into the fan-out but cancelled before its goroutine runs is
// abandoned at the re-check: no RPC is issued and the site is marked failed, so
// the caller reports it unavailable rather than silently returning zero rows
// for it.
func TestUserService_EnrichCrossSiteThreads_CancelledAfterAdmission(t *testing.T) {
	// No GetThreadList expectation at all: the strict mock fails if the
	// abandoned site is still dialled.
	svc, _ := newTopologyThreadSvc(t, "site-a", []string{"site-a", "site-b"})

	c := ctx("alice", "site-a")
	// One live observation covers the admission check; the goroutine's re-check
	// then sees a cancelled context.
	c.SetContext(&cancelAfterCtx{Context: context.Background(), remaining: 1})

	sites := []string{"site-a", "site-b"}
	results := make([]threadSiteResult, len(sites))
	svc.enrichCrossSiteThreads(c, sites, results, []int{1}, model.ThreadSubscriptionListRequest{Account: "alice", Limit: 10})

	assert.True(t, results[1].failed, "the abandoned site must be marked failed")
	assert.Empty(t, results[1].items)
	assert.False(t, results[1].hasMore)
	assert.Equal(t, threadSiteResult{}, results[0], "the local slot is untouched by the cross-site pass")
}

// With no remote sites in the index set the fan-out is a no-op: it leaves every
// result slot at its zero value (available, empty) rather than marking sites
// failed.
func TestUserService_EnrichCrossSiteThreads_NoRemoteSites(t *testing.T) {
	svc, _ := newTopologyThreadSvc(t, "site-a", []string{"site-a"})

	sites := []string{"site-a"}
	results := []threadSiteResult{{items: []model.ThreadListItem{item("site-a", "ta1", 50)}, hasMore: true}}
	svc.enrichCrossSiteThreads(ctx("alice", "site-a"), sites, results, nil, model.ThreadSubscriptionListRequest{Account: "alice"})

	require.Len(t, results, 1)
	assert.False(t, results[0].failed)
	assert.Equal(t, []string{"ta1"}, ids(results[0].items), "the local result survives the no-op pass")
	assert.True(t, results[0].hasMore)
}

// enrichLocalThreads with nothing to serve leaves the results untouched — the
// aggregator relies on that when the caller's own site is absent from the set.
func TestUserService_EnrichLocalThreads_NoLocalSite(t *testing.T) {
	svc, _ := newTopologyThreadSvc(t, "site-a", []string{"site-a", "site-b"})

	results := make([]threadSiteResult, 2)
	svc.enrichLocalThreads(ctx("alice", "site-a"), []string{"site-a", "site-b"}, results, nil, model.ThreadSubscriptionListRequest{Account: "alice"})

	assert.Equal(t, make([]threadSiteResult, 2), results)
}
