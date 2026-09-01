package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subauthcache"
	"github.com/hmchangw/chat/pkg/valkeyfake"
)

// TestHandleRemove_BustsSubL2 covers the security-critical gap: bot-room-service
// had no Valkey wiring at all, so a bot-driven member removal left the removed
// account's subauthcache L2 entry cached for up to the 90m TTL, letting them
// keep sending/reading. The bust must use the account (not the userID the
// handler is given) — SubKey/FetchFromMongo are keyed on subscriptions.u.account,
// and the account comes back from DeleteSubscription itself, read off the row
// UpsertSubscription wrote at add-time, so it matches by construction.
func TestHandleRemove_BustsSubL2(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		DeleteSubscriptionFn: func(_ context.Context, _, _ string) (string, bool, error) { return "bob", true, nil },
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "bob", SiteID: "site-a"}, nil
		},
	}
	fake := valkeyfake.New()
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, testKeyStore, testKeySender)
	h.valkey = fake
	c := withIdentity(t, "r1", ident())

	_, err := h.handleRemove(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.NoError(t, err)

	assert.Contains(t, fake.DeletedKeys(), subauthcache.SubKey("r1", "bob"),
		"the removed member's subauthcache L2 entry must be busted")
}

// TestHandleRemove_NoBustOnDuplicateRemove: a no-op remove (already absent)
// must not bust anything — DeleteSubscription reported wasThere=false.
func TestHandleRemove_NoBustOnDuplicateRemove(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		DeleteSubscriptionFn: func(_ context.Context, _, _ string) (string, bool, error) { return "", false, nil },
		// Reached before the delete now: the removal destination is resolved
		// while the row that identifies it still exists.
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "bob", SiteID: "site-a"}, nil
		},
	}
	fake := valkeyfake.New()
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, testKeyStore, testKeySender)
	h.valkey = fake
	c := withIdentity(t, "r1", ident())

	_, err := h.handleRemove(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.NoError(t, err)

	assert.Empty(t, fake.DeletedKeys(), "a no-op remove must not bust the cache")
}

// A vanished user doc is not an outage: there is no remote site to notify, so
// the removal commits and the cached decision must die with it. Any path that
// skips the bust leaves a member who has lost access still passing
// authorization from L2 for the rest of the TTL.
func TestHandleRemove_BustsWhenTheUserDocIsGone(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		DeleteSubscriptionFn: func(_ context.Context, _, _ string) (string, bool, error) {
			return "bob", true, nil
		},
		FindUserFn: func(_ context.Context, _ string) (*model.User, error) { return nil, ErrNotFound },
	}
	fake := valkeyfake.New()
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, testKeyStore, testKeySender)
	h.valkey = fake
	c := withIdentity(t, "r1", ident())

	_, err := h.handleRemove(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.NoError(t, err, "a user doc that is genuinely gone must not fail the removal")

	assert.Contains(t, fake.DeletedKeys(), subauthcache.SubKey("r1", "bob"),
		"the subscription is deleted; the cached decision must die with it")
}

// A transient lookup failure fires during exactly the Mongo trouble this branch
// exists to survive, and it now happens BEFORE the delete. Nothing was
// de-authorized, so there is nothing to bust — and nothing to federate later.
func TestHandleRemove_TransientLookupFailureDeletesAndBustsNothing(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		DeleteSubscriptionFn: func(_ context.Context, _, _ string) (string, bool, error) {
			t.Fatal("the delete must not run once the destination lookup has failed")
			return "", false, nil
		},
		FindUserFn: func(_ context.Context, _ string) (*model.User, error) {
			return nil, errors.New("mongo down")
		},
	}
	fake := valkeyfake.New()
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, testKeyStore, testKeySender)
	h.valkey = fake
	c := withIdentity(t, "r1", ident())

	_, err := h.handleRemove(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.Error(t, err, "the caller must learn the removal did not complete, so it can retry")
	assert.Empty(t, fake.DeletedKeys(), "nothing was de-authorized, so nothing needs busting")
}

// A federation publish failure is reported to the caller, but it must not
// cancel the bust: the local subscriptions are already deleted, so returning
// early leaves every account collected in this batch authorized from L2.
func TestHandleRemove_BustsWhenFederationFails(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		DeleteSubscriptionFn: func(_ context.Context, _, userID string) (string, bool, error) {
			return strings.TrimSuffix(userID, "-id"), true, nil
		},
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			// A remote home site, so the removal federates and hits the failure.
			return &model.User{ID: id, Account: strings.TrimSuffix(id, "-id"), SiteID: "site-b"}, nil
		},
	}
	fake := valkeyfake.New()
	failing := func(context.Context, string, []byte, string) error { return errors.New("outbox down") }
	h := newHandler(store, "site-a", []string{"site-b"}, failing, testKeyStore, testKeySender)
	h.valkey = fake
	c := withIdentity(t, "r1", ident())

	_, err := h.handleRemove(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.Error(t, err, "the caller must still learn federation failed")

	assert.Contains(t, fake.DeletedKeys(), subauthcache.SubKey("r1", "bob"),
		"a federation failure must not leave the removed member authorized from L2")
}
