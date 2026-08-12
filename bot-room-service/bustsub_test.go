package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subauthcache"
)

// fakeBustClient is a minimal valkeyutil.Client that records Del calls, for
// asserting subauthcache L2 invalidation fires. SetNX/IncrEx panic on use —
// bust helpers never call them. Mirrors room-worker/room-service/inbox-worker's
// test double of the same name.
type fakeBustClient struct {
	dels     []string
	delCalls int // count of Del *calls*, not keys — asserts round-trip batching
	delErr   error
}

func (f *fakeBustClient) Get(context.Context, string) (string, error) { return "", nil }
func (f *fakeBustClient) Set(context.Context, string, string, time.Duration) error {
	return nil
}
func (f *fakeBustClient) Del(_ context.Context, keys ...string) error {
	f.delCalls++
	f.dels = append(f.dels, keys...)
	return f.delErr
}
func (f *fakeBustClient) Expire(context.Context, string, time.Duration) (bool, error) {
	return true, nil
}

func (f *fakeBustClient) Close() error { return nil }
func (f *fakeBustClient) SetNX(context.Context, string, string, time.Duration) (bool, error) {
	panic("fakeBustClient.SetNX not implemented")
}
func (f *fakeBustClient) IncrEx(context.Context, string, time.Duration) (int64, error) {
	panic("fakeBustClient.IncrEx not implemented")
}

// TestHandleRemove_BustsSubL2 covers the security-critical gap: bot-room-service
// had no Valkey wiring at all, so a bot-driven member removal left the removed
// account's subauthcache L2 entry cached for up to the 90m TTL, letting them
// keep sending/reading. The bust must use the account (not the userID the
// handler is given) — SubKey/FetchFromMongo are keyed on subscriptions.u.account,
// and u.Account here comes from the same FindUser(userID) lookup that
// UpsertSubscription used to populate that field at add-time, so it matches.
func TestHandleRemove_BustsSubL2(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		DeleteSubscriptionFn: func(_ context.Context, _, _ string) (bool, error) { return true, nil },
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "bob", SiteID: "site-a"}, nil
		},
	}
	fake := &fakeBustClient{}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, testKeyStore, testKeySender)
	h.valkey = fake
	c := withIdentity(t, "r1", ident())

	_, err := h.handleRemove(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.NoError(t, err)

	assert.Contains(t, fake.dels, subauthcache.SubKey("r1", "bob"),
		"the removed member's subauthcache L2 entry must be busted")
}

// TestHandleRemove_NoBustOnDuplicateRemove: a no-op remove (already absent)
// must not bust anything — DeleteSubscription reported wasThere=false.
func TestHandleRemove_NoBustOnDuplicateRemove(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		DeleteSubscriptionFn: func(_ context.Context, _, _ string) (bool, error) { return false, nil },
	}
	fake := &fakeBustClient{}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, testKeyStore, testKeySender)
	h.valkey = fake
	c := withIdentity(t, "r1", ident())

	_, err := h.handleRemove(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.NoError(t, err)

	assert.Empty(t, fake.dels, "a no-op remove must not bust the cache")
}
