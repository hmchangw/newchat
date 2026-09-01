package main

import (
	"context"
	"testing"

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
