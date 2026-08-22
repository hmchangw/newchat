package main

import (
	"context"
	"errors"
	"strings"
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
		DeleteSubscriptionFn: func(_ context.Context, _, _ string) (string, bool, error) { return "", false, nil },
	}
	fake := &fakeBustClient{}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, testKeyStore, testKeySender)
	h.valkey = fake
	c := withIdentity(t, "r1", ident())

	_, err := h.handleRemove(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.NoError(t, err)

	assert.Empty(t, fake.dels, "a no-op remove must not bust the cache")
}

// The bust must not depend on a second lookup succeeding. The subscription row
// is already deleted by the time FindUser runs, so any path that skips the bust
// leaves a member who has lost access still passing authorization from L2 for
// the rest of the TTL — and the transient-error case fires during exactly the
// Mongo trouble this whole branch exists to survive.
func TestHandleRemove_BustsEvenWhenTheUserLookupFails(t *testing.T) {
	tests := []struct {
		name    string
		findErr error
	}{
		{"user doc already gone", ErrNotFound},
		{"transient store error", errors.New("mongo down")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeStore{
				FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
					return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
				},
				DeleteSubscriptionFn: func(_ context.Context, _, _ string) (string, bool, error) {
					return "bob", true, nil
				},
				FindUserFn: func(_ context.Context, _ string) (*model.User, error) { return nil, tt.findErr },
			}
			fake := &fakeBustClient{}
			h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, testKeyStore, testKeySender)
			h.valkey = fake
			c := withIdentity(t, "r1", ident())

			_, err := h.handleRemove(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
			require.NoError(t, err, "a failed enrichment lookup must not fail the removal")

			assert.Contains(t, fake.dels, subauthcache.SubKey("r1", "bob"),
				"the subscription is already deleted; the cached decision must die with it")
		})
	}
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
	fake := &fakeBustClient{}
	failing := func(context.Context, string, []byte, string) error { return errors.New("outbox down") }
	h := newHandler(store, "site-a", []string{"site-b"}, failing, testKeyStore, testKeySender)
	h.valkey = fake
	c := withIdentity(t, "r1", ident())

	_, err := h.handleRemove(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.Error(t, err, "the caller must still learn federation failed")

	assert.Contains(t, fake.dels, subauthcache.SubKey("r1", "bob"),
		"a federation failure must not leave the removed member authorized from L2")
}

// MGet loops the fake's own Get so it cannot drift from single-key behaviour.
func (f *fakeBustClient) MGet(ctx context.Context, keys []string) (map[string]string, error) {
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		v, err := f.Get(ctx, k)
		if err != nil {
			continue
		}
		out[k] = v
	}
	return out, nil
}
