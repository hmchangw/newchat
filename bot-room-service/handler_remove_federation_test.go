package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/valkeyfake"
)

// removeFedStore builds a store whose delete reports the row already gone — the
// duplicate-remove case every test here drives — with the member on destSiteID.
func removeFedStore(destSiteID string) *fakeStore {
	return &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		DeleteSubscriptionFn: func(_ context.Context, _, _ string) (string, string, bool, error) {
			return "", "", false, nil
		},
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "bob", SiteID: destSiteID}, nil
		},
	}
}

// TestHandleRemove_DuplicateRemoveStillFederates covers the retry that repairs a
// lost federation. The first call committed the delete and then failed before or
// during its outbox publish, so the caller retries — and that retry finds the row
// already gone (wasThere=false). Skipping the federation there strands the member
// as still-subscribed on their home site with nothing that ever repairs it, since
// every later retry sees the same wasThere=false.
//
// Republishing is safe and cheap: the dedup id is derived only from
// (roomID, userID, destSiteID), so JetStream drops it where the first publish
// landed and accepts it where it did not.
func TestHandleRemove_DuplicateRemoveStillFederates(t *testing.T) {
	out := &captureOutbox{}
	h := newHandler(removeFedStore("site-b"), "site-a", nil, out.publish, testKeyStore, testKeySender)
	h.valkey = valkeyfake.New()

	resp, err := h.handleRemove(withIdentity(t, "r1", ident()),
		BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.NoError(t, err)

	require.Len(t, out.calls, 1, "a duplicate remove must still re-federate: only this repairs a destination that missed the first publish")
	// User-keyed, not subscription-keyed: this call deleted nothing, so the row
	// whose id the original publish used is already gone. The "u:" prefix keeps
	// that id in a space a real subscription id can never reach.
	//
	// It is deterministic, so repeated repairs of one removal still collapse to a
	// single delivery. What it can no longer do is match the original publish and
	// be suppressed where that one landed — the original is keyed on the deleted
	// subscription, which a repair cannot reconstruct. So a repair republishes
	// even when the first attempt succeeded, and correctness rests on the
	// destination treating a removal as idempotent, which inbox-worker does:
	// deleting an already-deleted subscription is a no-op. That is the cost of
	// keying live removals on the membership incarnation, and it is the cheap
	// direction — a duplicate removal is harmless, a dropped one is a permanent
	// split-brain between the two sites.
	assert.Equal(t, "bot-remove:r1:u:bob-id:site-b", out.calls[0].MsgID,
		"a repair must use the deterministic user-keyed id so repeated repairs collapse to one delivery")
	assert.Empty(t, resp.Removed.UserIDs, "this call deleted nothing, so it reports nothing removed")
}

// TestHandleRemove_DuplicateRemoveSkipsLocalSideEffects is the other half of the
// same gate: federation is re-published, but everything that would be visible
// twice is not. A duplicate remove must not rotate the room key (a fresh version
// per retry) and must not post a second "members removed" system message.
func TestHandleRemove_DuplicateRemoveSkipsLocalSideEffects(t *testing.T) {
	store := removeFedStore("site-b")
	rotations := 0
	store.ListRoomMemberAccountsFn = func(_ context.Context, _ string) ([]string, error) {
		rotations++
		return []string{"alice"}, nil
	}
	sys := &fakeSysmsgPub{}
	out := &captureOutbox{}
	h := newHandler(store, "site-a", nil, out.publish, testKeyStore, testKeySender)
	h.sysmsgPub = sys
	h.valkey = valkeyfake.New()

	_, err := h.handleRemove(withIdentity(t, "r1", ident()),
		BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.NoError(t, err)

	assert.Zero(t, rotations, "a duplicate remove must not rotate the room key")
	assert.Zero(t, sys.calls, "a duplicate remove must not post a second members-removed system message")
	assert.Len(t, out.calls, 1, "the federation republish is the one thing a duplicate remove still does")
}

// TestHandleRemove_SameSiteRemoveDoesNotFederate guards the other direction: a
// member whose home site is this site has no remote to notify, duplicate or not.
func TestHandleRemove_SameSiteRemoveDoesNotFederate(t *testing.T) {
	out := &captureOutbox{}
	h := newHandler(removeFedStore("site-a"), "site-a", nil, out.publish, testKeyStore, testKeySender)
	h.valkey = valkeyfake.New()

	_, err := h.handleRemove(withIdentity(t, "r1", ident()),
		BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.NoError(t, err)

	assert.Empty(t, out.calls, "a same-site member has no remote site to notify")
}
