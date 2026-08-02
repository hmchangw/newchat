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
	"github.com/hmchangw/chat/user-service/service/mocks"
)

// fakeBadgeCache is a handwritten test double for the badgeCache interface —
// small enough that mockgen would add noise without value. bump/seed/reseed
// are optional overrides (nil ⇒ default miss/no-op); the call slices record
// which accounts each method was invoked for, in order.
type fakeBadgeCache struct {
	bump   func(account, roomID string) (int, bool)
	seed   func(account string, roomIDs []string, trigger string) (int, bool)
	reseed func(account string, roomIDs []string)

	bumpCalls   []string
	seedCalls   []string
	reseedCalls []string
}

func (f *fakeBadgeCache) Bump(_ context.Context, account, roomID string) (int, bool) {
	f.bumpCalls = append(f.bumpCalls, account)
	if f.bump != nil {
		return f.bump(account, roomID)
	}
	return 0, false
}

func (f *fakeBadgeCache) Seed(_ context.Context, account string, roomIDs []string, trigger string) (int, bool) {
	f.seedCalls = append(f.seedCalls, account)
	if f.seed != nil {
		return f.seed(account, roomIDs, trigger)
	}
	return 0, false
}

func (f *fakeBadgeCache) Reseed(_ context.Context, account string, roomIDs []string) {
	f.reseedCalls = append(f.reseedCalls, account)
	if f.reseed != nil {
		f.reseed(account, roomIDs)
	}
}

// newBadgeService builds a UserService exposing just the deps BadgeCountBatch
// (and the unreadRooms it calls on a miss) needs.
func newBadgeService(t *testing.T, subs *mocks.MockSubscriptionRepository, badge *fakeBadgeCache) *UserService {
	t.Helper()
	return &UserService{subs: subs, badge: badge, siteID: "site-a", maxSubs: 1000}
}

func TestBadgeCountBatch_EmptyAccounts_BadRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	badge := &fakeBadgeCache{}
	svc := newBadgeService(t, subs, badge)
	_, err := svc.BadgeCountBatch(ctx("alice", "site-a"), model.BadgeCountBatchRequest{RoomID: "r1", Accounts: nil})
	requireCode(t, err, errcode.CodeBadRequest)
	assert.Empty(t, badge.bumpCalls, "must reject before touching the cache")
}

func TestBadgeCountBatch_EmptyRoomID_BadRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	badge := &fakeBadgeCache{}
	svc := newBadgeService(t, subs, badge)
	_, err := svc.BadgeCountBatch(ctx("alice", "site-a"), model.BadgeCountBatchRequest{RoomID: "", Accounts: []string{"alice"}})
	requireCode(t, err, errcode.CodeBadRequest)
	assert.Empty(t, badge.bumpCalls)
}

// A cache hit returns Bump's count directly, with no unreadRooms/repo call at all.
func TestBadgeCountBatch_Hit_NoRepoCall(t *testing.T) {
	ctrl := gomock.NewController(t)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	// No GetActiveSubscriptions expectation — a call would fail the mock.
	badge := &fakeBadgeCache{
		bump: func(account, roomID string) (int, bool) {
			assert.Equal(t, "alice", account)
			assert.Equal(t, "r1", roomID)
			return 3, true
		},
	}
	svc := newBadgeService(t, subs, badge)
	resp, err := svc.BadgeCountBatch(ctx("alice", "site-a"), model.BadgeCountBatchRequest{RoomID: "r1", Accounts: []string{"alice"}})
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"alice": 3}, resp.Counts)
	assert.Equal(t, []string{"alice"}, badge.bumpCalls)
	assert.Empty(t, badge.seedCalls)
}

// A cache miss falls through to unreadRooms (called exactly once) and Seeds
// the cache with the trigger room, returning Seed's count.
func TestBadgeCountBatch_Miss_SeedsWithTrigger(t *testing.T) {
	ctrl := gomock.NewController(t)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).
		Return([]model.EnrichedSubscription{
			{Subscription: model.Subscription{RoomID: "r2", SiteID: "site-a"}, LastMsgAt: ptrTime(100)},
		}, nil).Times(1)
	var gotIDs []string
	var gotTrigger string
	badge := &fakeBadgeCache{
		seed: func(account string, roomIDs []string, trigger string) (int, bool) {
			gotIDs = roomIDs
			gotTrigger = trigger
			return 2, true
		},
	}
	svc := newBadgeService(t, subs, badge)
	resp, err := svc.BadgeCountBatch(ctx("alice", "site-a"), model.BadgeCountBatchRequest{RoomID: "r1", Accounts: []string{"alice"}})
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"alice": 2}, resp.Counts)
	assert.Equal(t, []string{"r2"}, gotIDs, "unreadRooms result must be forwarded to Seed")
	assert.Equal(t, "r1", gotTrigger, "the triggering room must be passed to Seed")
	assert.Equal(t, []string{"alice"}, badge.seedCalls)
}

// A per-account unreadRooms failure leaves that account absent from the
// response while other accounts still succeed.
func TestBadgeCountBatch_UnreadRoomsError_AccountAbsent(t *testing.T) {
	ctrl := gomock.NewController(t)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).
		Return(nil, errors.New("db down"))
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "bob", gomock.Any()).
		Return([]model.EnrichedSubscription{}, nil)
	badge := &fakeBadgeCache{
		seed: func(account string, roomIDs []string, trigger string) (int, bool) {
			return 1, true
		},
	}
	svc := newBadgeService(t, subs, badge)
	resp, err := svc.BadgeCountBatch(ctx("alice", "site-a"), model.BadgeCountBatchRequest{RoomID: "r1", Accounts: []string{"alice", "bob"}})
	require.NoError(t, err, "a per-account degradation must not fail the whole batch")
	_, hasAlice := resp.Counts["alice"]
	assert.False(t, hasAlice, "the account whose unreadRooms failed must be absent")
	assert.Equal(t, 1, resp.Counts["bob"])
}

// When the cache is fully down (both Bump miss and Seed fail), the handler
// still returns a count computed locally from unreadRooms ∪ trigger, capped.
func TestBadgeCountBatch_CacheFullyDown_CappedUnionFallback(t *testing.T) {
	ctrl := gomock.NewController(t)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).
		Return([]model.EnrichedSubscription{
			{Subscription: model.Subscription{RoomID: "r2", SiteID: "site-a"}, LastMsgAt: ptrTime(100)},
			{Subscription: model.Subscription{RoomID: "r3", SiteID: "site-a"}, LastMsgAt: ptrTime(100)},
		}, nil)
	badge := &fakeBadgeCache{} // Bump and Seed both default to (0, false)
	svc := newBadgeService(t, subs, badge)
	resp, err := svc.BadgeCountBatch(ctx("alice", "site-a"), model.BadgeCountBatchRequest{RoomID: "r1", Accounts: []string{"alice"}})
	require.NoError(t, err)
	assert.Equal(t, 3, resp.Counts["alice"], "r1 (trigger) + r2 + r3, cache down entirely")
}

func TestCappedUnion(t *testing.T) {
	cases := []struct {
		name    string
		ids     []string
		trigger string
		want    int
	}{
		{"empty", nil, "", 0},
		{"trigger only", nil, "r1", 1},
		{"ids plus new trigger", []string{"r1", "r2"}, "r3", 3},
		{"trigger already a member", []string{"r1", "r2"}, "r1", 2},
		{"capped at 10", []string{"r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8", "r9", "r10"}, "r11", 10},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, cappedUnion(tc.ids, tc.trigger))
		})
	}
}
