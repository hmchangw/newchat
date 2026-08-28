package main

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
	"github.com/hmchangw/chat/pkg/roomsubcache"
)

// These tests drive the generated mocks in mock_store_test.go, pinning the
// store contract declared in store.go at the boundaries where a store failure
// changes the worker's ack decision.

// TestCachedMemberLookup_StoreErrorPropagates: a cold-miss store failure must
// surface, not be swallowed into an empty member list — an empty list silently
// means "nobody to notify".
func TestCachedMemberLookup_StoreErrorPropagates(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockMemberStore(ctrl)
	store.EXPECT().
		ListMembers(gomock.Any(), "r1").
		Return(nil, errors.New("mongo: connection reset"))

	lookup := newCachedMemberLookup(newFakeCache(), store.ListMembers, time.Minute)

	got, err := lookup.GetMembers(context.Background(), "r1")

	require.Error(t, err)
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "r1", "error must name the room it failed for")
}

// TestCachedMemberLookup_StoreResultIsCached: one store round-trip serves
// subsequent reads for the same room.
func TestCachedMemberLookup_StoreResultIsCached(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockMemberStore(ctrl)
	members := []roomsubcache.Member{{ID: "u1", Account: "alice"}}
	// Times(1) is the assertion: a second GetMembers must not reach the store.
	store.EXPECT().ListMembers(gomock.Any(), "r1").Return(members, nil).Times(1)

	lookup := newCachedMemberLookup(newFakeCache(), store.ListMembers, time.Minute)

	first, err := lookup.GetMembers(context.Background(), "r1")
	require.NoError(t, err)
	second, err := lookup.GetMembers(context.Background(), "r1")
	require.NoError(t, err)

	assert.Equal(t, members, first)
	assert.Equal(t, members, second)
}

// TestHandle_MemberFetchError_NAKs closes the gap the audit flagged: the
// member-fetch failure branch had no test at all, because the hand-written
// stub could not be made to fail. A member-store outage must NAK for
// redelivery — never Ack-drop, which would silently lose the notification.
func TestHandle_MemberFetchError_NAKs(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockMemberStore(ctrl)
	store.EXPECT().
		ListMembers(gomock.Any(), "r1").
		Return(nil, errors.New("mongo: no reachable servers"))

	emit := &recordingEmitter{}
	h := newTestHandler(
		newCachedMemberLookup(newFakeCache(), store.ListMembers, time.Minute),
		&stubFollowers{}, noopPresenceSnapshotter{}, noopVetoer{}, emit,
	)

	err := h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	}))

	require.Error(t, err)
	_, permanent := errcode.IsPermanent(err)
	assert.False(t, permanent,
		"a member-store outage is transient — it must redeliver, not Ack-drop as poison")
	assert.Empty(t, emit.accounts(), "nothing may be published when the audience is unknown")
}

// TestHandle_ThreadFollowerStoreError_NAKs: a thread_rooms read failure must
// redeliver rather than drop follower-only recipients. A clean miss is a
// different case (empty followers, nil error) and must still deliver.
func TestHandle_ThreadFollowerStoreError_NAKs(t *testing.T) {
	ctrl := gomock.NewController(t)
	followers := NewMockThreadFollowerLister(ctrl)
	followers.EXPECT().
		Lookup(gomock.Any(), "parent-1").
		Return(ThreadRoomInfo{}, errors.New("mongo: timeout"))

	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {{ID: "alice", Account: "alice"}, {ID: "bob", Account: "bob"}},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, followers, noopPresenceSnapshotter{}, noopVetoer{}, emit)

	err := h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		ThreadParentMessageID: "parent-1", TShow: false, CreatedAt: time.Now(),
	}))

	require.Error(t, err)
	_, permanent := errcode.IsPermanent(err)
	assert.False(t, permanent, "a thread_rooms outage is transient — it must redeliver")
	assert.Empty(t, emit.accounts())
}

// TestHandle_SettingsStoreError_FailsOpen: settings gate delivery, so a store
// failure must fail OPEN — degrade to pre-enforcement behaviour and still
// notify, rather than silencing the site.
func TestHandle_SettingsStoreError_FailsOpen(t *testing.T) {
	ctrl := gomock.NewController(t)
	settings := NewMockUserSettingsSnapshotter(ctrl)
	settings.EXPECT().
		Snapshot(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("mongo: connection reset"))

	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {{ID: "alice", Account: "alice"}, {ID: "bob", Account: "bob"}},
	}}
	emit := &recordingEmitter{}
	h := newTestHandlerWithSettings(members, noopPresenceSnapshotter{}, settings, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	})), "a settings outage must not fail the message")

	assert.Equal(t, []string{"bob"}, emit.accounts(),
		"recipients still receive the push when settings cannot be read")
}
