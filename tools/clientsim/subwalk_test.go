package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeLister struct {
	pages []subListPage
	reqs  []subListRequest
	err   error
}

func (f *fakeLister) List(_ context.Context, req subListRequest) (*subListPage, error) {
	f.reqs = append(f.reqs, req)
	if f.err != nil {
		return nil, f.err
	}
	i := len(f.reqs) - 1
	if i >= len(f.pages) {
		return &subListPage{}, nil
	}
	return &f.pages[i], nil
}

func bptr(b bool) *bool { return &b }

func TestFetchSubscriptionPlan_PaginatesAndFilters(t *testing.T) {
	l := &fakeLister{pages: []subListPage{
		{Subscriptions: []subRow{
			{RoomID: "r1", RoomType: "channel", Room: &subRoom{CrossSite: bptr(true)}},
			{RoomID: "d1", RoomType: "dm"},    // DM: user lane, never a room sub
			{RoomID: "b1", RoomType: "botDM"}, // botDM: filtered too
		}, HasMore: true},
		{Subscriptions: []subRow{
			{RoomID: "r2", RoomType: "channel", Room: &subRoom{CrossSite: bptr(false)}}, // explicit false -> local
			{RoomID: "r1", RoomType: "channel", Room: &subRoom{CrossSite: bptr(true)}},  // cross-page duplicate
			{RoomID: "r3", RoomType: "channel"},                                         // no room object -> global fail-safe
			{RoomID: "r4", RoomType: "channel", Room: &subRoom{}},                       // nil crossSite -> global fail-safe
		}, HasMore: false},
	}}

	plan, err := fetchSubscriptionPlan(context.Background(), l)
	require.NoError(t, err)
	assert.Equal(t, map[string]bool{"r1": true, "r2": false, "r3": true, "r4": true}, plan)

	// Request contract: type is always "rooms", offset advances by the sent limit.
	require.Len(t, l.reqs, 2)
	assert.Equal(t, subListRequest{Type: "rooms", Offset: 0, Limit: subListPageLimit}, l.reqs[0])
	assert.Equal(t, subListRequest{Type: "rooms", Offset: subListPageLimit, Limit: subListPageLimit}, l.reqs[1])
}

func TestFetchSubscriptionPlan_EmptySidebar(t *testing.T) {
	l := &fakeLister{pages: []subListPage{{HasMore: false}}}
	plan, err := fetchSubscriptionPlan(context.Background(), l)
	require.NoError(t, err)
	assert.Empty(t, plan)
}

func TestFetchSubscriptionPlan_ListerError(t *testing.T) {
	l := &fakeLister{err: assert.AnError}
	_, err := fetchSubscriptionPlan(context.Background(), l)
	assert.Error(t, err)
}

func TestRoomGlobal_TriState(t *testing.T) {
	assert.True(t, roomGlobal(nil), "missing room object -> global")
	assert.True(t, roomGlobal(&subRoom{}), "nil crossSite -> global")
	assert.True(t, roomGlobal(&subRoom{CrossSite: bptr(true)}))
	assert.False(t, roomGlobal(&subRoom{CrossSite: bptr(false)}), "only explicit false is local")
}

func TestFetchSubscriptionPlan_TruncatedWalkIsAnError(t *testing.T) {
	// A page that claims hasMore but carries no rows leaves the plan
	// truncated. Returning it as a success is the worst outcome available:
	// the client subscribes to a subset, marks the plan verified, and reports
	// ready — a soak that measures a fraction of the fleet's rooms while
	// every dashboard stays green. Failing sends it back through the resync.
	rows := make([]subRow, subListPageLimit)
	for i := range rows {
		rows[i] = subRow{RoomID: string(rune('a' + i%26)), RoomType: "channel"}
	}
	l := &fakeLister{pages: []subListPage{
		{Subscriptions: rows, HasMore: true},
		{Subscriptions: nil, HasMore: true}, // server says more, sends none
	}}
	_, err := fetchSubscriptionPlan(context.Background(), l)
	require.Error(t, err)
	assert.ErrorContains(t, err, "no rows")
}

func TestNatsLister_RejectsRepliesWithoutASubscriptionsField(t *testing.T) {
	// `null`, `{}` and a reply missing the field all decode into a zero
	// subListPage, which is indistinguishable from "this user has no
	// channels" — so a broken responder would put the whole fleet in the
	// ready state with nothing subscribed. Only an explicit array counts.
	tests := []struct {
		name    string
		reply   string
		wantErr string
	}{
		{"null body", `null`, "no subscriptions"},
		{"empty object", `{}`, "no subscriptions"},
		{"hasMore but no field", `{"hasMore":true}`, "no subscriptions"},
		{"explicit empty array is legitimate", `{"subscriptions":[],"hasMore":false}`, ""},
		{"populated array", `{"subscriptions":[{"roomId":"r1","roomType":"channel"}],"hasMore":false}`, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &natsLister{conn: &requestConn{reply: []byte(tt.reply)}, subject: "s", timeout: time.Second}
			page, err := l.List(context.Background(), subListRequest{Type: "rooms"})
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.NotNil(t, page)
		})
	}
}
