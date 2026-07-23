package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomkeysender"
	"github.com/hmchangw/chat/pkg/roomkeystore"
)

// orderedPublisher records the subject of each publish into a shared, ordered
// call log so tests can assert fan-out happened before rotate.
type orderedPublisher struct {
	log      *[]string
	subjects []string
	payloads [][]byte
}

func (p *orderedPublisher) Publish(subj string, data []byte) error {
	*p.log = append(*p.log, "send:"+subj)
	p.subjects = append(p.subjects, subj)
	p.payloads = append(p.payloads, append([]byte(nil), data...))
	return nil
}

// TestHandleRemove_DiffNonEmpty_RotatesAndFansOutToSurvivorsInOrder: when at
// least one account is actually removed, survivors get the new key BEFORE
// Rotate commits it, matching room-worker.rotateAndFanOut's ordering
// guarantee (survivors hold v+1 before broadcast-worker switches).
func TestHandleRemove_DiffNonEmpty_RotatesAndFansOutToSurvivorsInOrder(t *testing.T) {
	var order []string
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		DeleteSubscriptionFn: func(_ context.Context, _, _ string) (bool, error) { return true, nil },
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "bob", SiteID: "site-a"}, nil
		},
		ListRoomMemberAccountsFn: func(_ context.Context, _ string) ([]string, error) {
			return []string{"carol", "dave"}, nil
		},
	}
	keyStore := &fakeKeyStore{
		GetFn: func(_ context.Context, _ string) (*roomkeystore.VersionedKeyPair, error) {
			return &roomkeystore.VersionedKeyPair{
				Version: 3,
				KeyPair: roomkeystore.RoomKeyPair{PrivateKey: []byte("old-key-bytes-0123456789012345")},
			}, nil
		},
		RotateFn: func(_ context.Context, roomID string, newPair roomkeystore.RoomKeyPair) (int, error) {
			order = append(order, "rotate")
			assert.Equal(t, "r1", roomID)
			assert.NotEmpty(t, newPair.PrivateKey)
			return 4, nil
		},
	}
	pub := &orderedPublisher{log: &order}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, keyStore, roomkeysender.NewSender(pub))
	c := withIdentity(t, "r1", ident())

	resp, err := h.handleRemove(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"bob-id"}, resp.Removed.UserIDs)

	require.Len(t, pub.subjects, 2, "one key event per survivor")
	for _, payload := range pub.payloads {
		var evt model.RoomKeyEvent
		require.NoError(t, json.Unmarshal(payload, &evt))
		assert.Equal(t, "r1", evt.RoomID)
		assert.Equal(t, 4, evt.Version, "survivors get v+1 = predicted rotate version")
	}

	require.Len(t, order, 3, "2 fan-out sends + 1 rotate")
	assert.Equal(t, "rotate", order[2], "rotate must be the LAST call, after both fan-out sends")
	assert.NotEqual(t, "rotate", order[0], "fan-out must happen before rotate")
	assert.NotEqual(t, "rotate", order[1], "fan-out must happen before rotate")
}

// TestHandleRemove_DiffEmpty_NoRotation: removing zero accounts (all
// duplicate/no-op removes) must not touch the key store or fan out at all.
func TestHandleRemove_DiffEmpty_NoRotation(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		DeleteSubscriptionFn: func(_ context.Context, _, _ string) (bool, error) { return false, nil },
	}
	getCalled := false
	keyStore := &fakeKeyStore{
		GetFn: func(_ context.Context, _ string) (*roomkeystore.VersionedKeyPair, error) {
			getCalled = true
			return &roomkeystore.VersionedKeyPair{}, nil
		},
		RotateFn: func(_ context.Context, _ string, _ roomkeystore.RoomKeyPair) (int, error) {
			t.Fatal("Rotate must not be called when nothing was removed")
			return 0, nil
		},
	}
	pub := &fakePublisher{}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, keyStore, roomkeysender.NewSender(pub))
	c := withIdentity(t, "r1", ident())

	resp, err := h.handleRemove(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.NoError(t, err)
	assert.Empty(t, resp.Removed.UserIDs)
	assert.False(t, getCalled, "keyStore.Get is skipped when nothing was removed")
	assert.Empty(t, pub.subjects, "no fan-out when nothing was removed")
}

// TestHandleRemove_NoCurrentKey_SkipsFanOutSetsNewKey: a legacy/broken bot
// channel with no stored key skips the survivor fan-out entirely but still
// stores a fresh key via Set so the room lands with a valid v1 key.
func TestHandleRemove_NoCurrentKey_SkipsFanOutSetsNewKey(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		DeleteSubscriptionFn: func(_ context.Context, _, _ string) (bool, error) { return true, nil },
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "bob", SiteID: "site-a"}, nil
		},
		ListRoomMemberAccountsFn: func(_ context.Context, _ string) ([]string, error) {
			return []string{"carol"}, nil
		},
	}
	var setRoomID string
	var setPair roomkeystore.RoomKeyPair
	keyStore := &fakeKeyStore{
		GetFn: func(_ context.Context, _ string) (*roomkeystore.VersionedKeyPair, error) {
			return nil, roomkeystore.ErrNoCurrentKey
		},
		SetFn: func(_ context.Context, roomID string, pair roomkeystore.RoomKeyPair) (int, error) {
			setRoomID = roomID
			setPair = pair
			return 1, nil
		},
		RotateFn: func(_ context.Context, _ string, _ roomkeystore.RoomKeyPair) (int, error) {
			t.Fatal("Rotate must not be called on the no-current-key legacy path")
			return 0, nil
		},
	}
	pub := &fakePublisher{}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, keyStore, roomkeysender.NewSender(pub))
	c := withIdentity(t, "r1", ident())

	resp, err := h.handleRemove(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"bob-id"}, resp.Removed.UserIDs)
	assert.Equal(t, "r1", setRoomID, "new key stored under the room's ID")
	assert.NotEmpty(t, setPair.PrivateKey)
	assert.Empty(t, pub.subjects, "no fan-out on the no-current-key legacy path")
}

// TestHandleRemove_RotateNoCurrentKey_FallsBackToSetWithVersion: if Rotate
// reports the key was concurrently deleted mid-rotation, fall back to
// SetWithVersion at the version already fanned out to survivors.
func TestHandleRemove_RotateNoCurrentKey_FallsBackToSetWithVersion(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		DeleteSubscriptionFn: func(_ context.Context, _, _ string) (bool, error) { return true, nil },
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "bob", SiteID: "site-a"}, nil
		},
		ListRoomMemberAccountsFn: func(_ context.Context, _ string) ([]string, error) {
			return []string{"carol"}, nil
		},
	}
	var setVersion int
	var setRoomID string
	keyStore := &fakeKeyStore{
		GetFn: func(_ context.Context, _ string) (*roomkeystore.VersionedKeyPair, error) {
			return &roomkeystore.VersionedKeyPair{
				Version: 5,
				KeyPair: roomkeystore.RoomKeyPair{PrivateKey: []byte("old-key-bytes-0123456789012345")},
			}, nil
		},
		RotateFn: func(_ context.Context, _ string, _ roomkeystore.RoomKeyPair) (int, error) {
			return 0, roomkeystore.ErrNoCurrentKey
		},
		SetWithVersionFn: func(_ context.Context, roomID string, _ roomkeystore.RoomKeyPair, version int) error {
			setRoomID = roomID
			setVersion = version
			return nil
		},
	}
	pub := &fakePublisher{}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, keyStore, roomkeysender.NewSender(pub))
	c := withIdentity(t, "r1", ident())

	resp, err := h.handleRemove(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"bob-id"}, resp.Removed.UserIDs)
	assert.Equal(t, "r1", setRoomID)
	assert.Equal(t, 6, setVersion, "fallback version matches predicted version (5+1) already fanned out")
}

// TestHandleRemove_RotateOtherError_FailsHandler: any Rotate error other than
// ErrNoCurrentKey is an infra failure and must fail the whole op.
func TestHandleRemove_RotateOtherError_FailsHandler(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		DeleteSubscriptionFn: func(_ context.Context, _, _ string) (bool, error) { return true, nil },
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "bob", SiteID: "site-a"}, nil
		},
		ListRoomMemberAccountsFn: func(_ context.Context, _ string) ([]string, error) {
			return []string{"carol"}, nil
		},
	}
	keyStore := &fakeKeyStore{
		GetFn: func(_ context.Context, _ string) (*roomkeystore.VersionedKeyPair, error) {
			return &roomkeystore.VersionedKeyPair{
				Version: 1,
				KeyPair: roomkeystore.RoomKeyPair{PrivateKey: []byte("old-key-bytes-0123456789012345")},
			}, nil
		},
		RotateFn: func(_ context.Context, _ string, _ roomkeystore.RoomKeyPair) (int, error) {
			return 0, errors.New("mongo down")
		},
	}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, keyStore, testKeySender)
	c := withIdentity(t, "r1", ident())

	_, err := h.handleRemove(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rotate key")
}

// TestHandleRemove_KeySendFailureDoesNotFailOp: a per-survivor Send failure
// is best-effort — logged, not surfaced. The handler must still succeed and
// still commit the rotation.
func TestHandleRemove_KeySendFailureDoesNotFailOp(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		DeleteSubscriptionFn: func(_ context.Context, _, _ string) (bool, error) { return true, nil },
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "bob", SiteID: "site-a"}, nil
		},
		ListRoomMemberAccountsFn: func(_ context.Context, _ string) ([]string, error) {
			return []string{"carol"}, nil
		},
	}
	rotateCalled := false
	keyStore := &fakeKeyStore{
		RotateFn: func(_ context.Context, _ string, _ roomkeystore.RoomKeyPair) (int, error) {
			rotateCalled = true
			return 2, nil
		},
	}
	failPub := &failingPublisher{}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, keyStore, roomkeysender.NewSender(failPub))
	c := withIdentity(t, "r1", ident())

	resp, err := h.handleRemove(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.NoError(t, err, "fan-out send failure must not fail remove-member")
	assert.Equal(t, []string{"bob-id"}, resp.Removed.UserIDs)
	assert.Equal(t, 1, failPub.calls, "fan-out was attempted")
	assert.True(t, rotateCalled, "rotation still commits despite fan-out failure")
}
