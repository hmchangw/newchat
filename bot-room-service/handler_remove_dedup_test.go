package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomkeysender"
	"github.com/hmchangw/chat/pkg/roomkeystore"
)

// dedupStore builds a store whose delete reports one specific subscription
// incarnation, so a test can drive two removals of the same (room, user) pair
// across a re-add and compare the ids they federate under.
func dedupStore(subID string) *fakeStore {
	return &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		DeleteSubscriptionFn: func(_ context.Context, _, _ string) (string, string, bool, error) {
			return subID, "bob", true, nil
		},
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "bob", SiteID: "site-b"}, nil
		},
		ListRoomMemberAccountsFn: func(_ context.Context, _ string) ([]string, error) {
			return []string{"carol"}, nil
		},
	}
}

func dedupKeyStore() *fakeKeyStore {
	return &fakeKeyStore{
		GetFn: func(_ context.Context, _ string) (*roomkeystore.VersionedKeyPair, error) {
			return &roomkeystore.VersionedKeyPair{
				Version: 3,
				KeyPair: roomkeystore.RoomKeyPair{PrivateKey: []byte("old-key-bytes-0123456789012345")},
			}, nil
		},
		RotateFn: func(_ context.Context, _ string, _ roomkeystore.RoomKeyPair) (int, error) { return 4, nil },
	}
}

// removeOnce drives one handleRemove and returns the Nats-Msg-Id the removal
// federated under.
func removeOnce(t *testing.T, subID string) string {
	t.Helper()
	out := &captureOutbox{}
	pub := &orderedPublisher{log: &[]string{}}
	h := newHandler(dedupStore(subID), "site-a", []string{"site-b"}, out.publish,
		dedupKeyStore(), roomkeysender.NewSender(pub))

	_, err := h.handleRemove(withIdentity(t, "r1", ident()), BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.NoError(t, err)
	require.Len(t, out.calls, 1, "one cross-site removal must be published")
	return out.calls[0].MsgID
}

// A removal event's dedup id has to name the membership it ends, not just the
// (room, user, site) triple. JetStream drops a publish whose Nats-Msg-Id it has
// seen inside the stream's duplicate window (2m by default, unset here). With a
// triple-only id, remove -> re-add -> remove inside that window sends the first
// removal and silently swallows the second: the destination site keeps showing
// a member the origin site has already removed, and nothing ever repairs it,
// because the id that would carry the repair is the one being suppressed.
//
// Each add writes a new subscription document with a fresh id, so the deleted
// document's id is exactly the incarnation marker that distinguishes the two
// removals while staying stable across retries of either one.
func TestFederateMemberRemoved_DistinctIncarnationsGetDistinctDedupIDs(t *testing.T) {
	first := removeOnce(t, "sub-incarnation-1")
	second := removeOnce(t, "sub-incarnation-2")

	assert.NotEqual(t, first, second,
		"a removal after a re-add must not reuse the first removal's dedup id, or JetStream drops it as a duplicate")
	assert.Contains(t, first, "sub-incarnation-1")
	assert.Contains(t, second, "sub-incarnation-2")
}

// The flip side of the property above: the id must NOT vary for one removal, or
// a publish retry of that same removal stops deduplicating and the destination
// applies it twice. Deleting a subscription is idempotent, so a duplicate is
// survivable where a dropped removal is not — but there is no reason to spend
// the redundant delivery.
func TestFederateMemberRemoved_SameIncarnationIsStableAcrossCalls(t *testing.T) {
	assert.Equal(t, removeOnce(t, "sub-incarnation-1"), removeOnce(t, "sub-incarnation-1"),
		"the same membership incarnation must federate under one stable id")
}
