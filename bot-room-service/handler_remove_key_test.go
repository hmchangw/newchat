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

// Rotate must commit BEFORE fan-out: fanning first mislabels two keys with one version.
func TestHandleRemove_DiffNonEmpty_RotatesThenFansOutToSurvivors(t *testing.T) {
	var order []string
	var committed []byte
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
			committed = newPair.PrivateKey
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
	require.NotEmpty(t, committed)
	for _, payload := range pub.payloads {
		var evt model.RoomKeyEvent
		require.NoError(t, json.Unmarshal(payload, &evt))
		assert.Equal(t, "r1", evt.RoomID)
		assert.Equal(t, 4, evt.Version, "survivors get the version Rotate returned")
		assert.Equal(t, committed, evt.PrivateKey,
			"survivors must receive exactly the bytes Rotate committed")
	}

	require.Len(t, order, 3, "1 rotate + 2 fan-out sends")
	assert.Equal(t, "rotate", order[0], "rotate must be the FIRST call, before any fan-out send")
	assert.NotEqual(t, "rotate", order[1], "fan-out follows rotate")
	assert.NotEqual(t, "rotate", order[2], "fan-out follows rotate")
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

// removeKeyStore: the room exists, one account is removed, one survivor remains.
func removeKeyStore() *fakeStore {
	return &fakeStore{
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
}

// A keyless channel adopts a fresh key via SetIfAbsent and fans out its post-image,
// not the local pair: a racing fallback may have installed v0 first.
func TestHandleRemove_NoCurrentKey_SetsNewKeyAndFansOut(t *testing.T) {
	store := removeKeyStore()
	winner := []byte("winning-key-bytes-01234567890123")
	var setRoomID string
	var offeredPair roomkeystore.RoomKeyPair
	keyStore := &fakeKeyStore{
		GetFn: func(_ context.Context, _ string) (*roomkeystore.VersionedKeyPair, error) {
			return nil, roomkeystore.ErrNoCurrentKey
		},
		SetIfAbsentFn: func(_ context.Context, roomID string, pair roomkeystore.RoomKeyPair) (*roomkeystore.VersionedKeyPair, error) {
			setRoomID = roomID
			offeredPair = pair
			return &roomkeystore.VersionedKeyPair{
				Version: 0,
				KeyPair: roomkeystore.RoomKeyPair{PrivateKey: winner},
			}, nil
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
	assert.NotEmpty(t, offeredPair.PrivateKey)
	require.Len(t, pub.payloads, 1, "survivors get the fallback's committed version")
	var evt model.RoomKeyEvent
	require.NoError(t, json.Unmarshal(pub.payloads[0], &evt))
	assert.Equal(t, 0, evt.Version, "fan-out uses the post-image version")
	assert.Equal(t, winner, evt.PrivateKey,
		"the fallback must fan out the store's post-image bytes, not the locally generated pair")
	assert.NotEqual(t, offeredPair.PrivateKey, evt.PrivateKey,
		"the locally generated pair lost the race and must never reach survivors")
}

// Without an authoritative post-image the committed bytes are unknown: fan out nothing.
func TestHandleRemove_SetIfAbsentFails_FansOutNothing(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{name: "store errors", err: errors.New("mongo down")},
		{name: "room gone", err: roomkeystore.ErrRoomNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			keyStore := &fakeKeyStore{
				GetFn: func(_ context.Context, _ string) (*roomkeystore.VersionedKeyPair, error) {
					return nil, roomkeystore.ErrNoCurrentKey
				},
				SetIfAbsentFn: func(_ context.Context, _ string, _ roomkeystore.RoomKeyPair) (*roomkeystore.VersionedKeyPair, error) {
					return nil, tc.err
				},
			}
			pub := &fakePublisher{}
			h := newHandler(removeKeyStore(), "site-a", nil, (&captureOutbox{}).publish, keyStore,
				roomkeysender.NewSender(pub))
			c := withIdentity(t, "r1", ident())

			_, err := h.handleRemove(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "store room key")
			assert.Empty(t, pub.payloads, "an unconfirmed key must never reach survivors")
		})
	}
}

// Rotate reporting ErrNoCurrentKey falls back to the atomic v0 install.
func TestHandleRemove_RotateNoCurrentKey_FallsBackToSetIfAbsent(t *testing.T) {
	var setCalled bool
	var setPriv []byte
	store := removeKeyStore()
	keyStore := &fakeKeyStore{
		GetFn: func(_ context.Context, _ string) (*roomkeystore.VersionedKeyPair, error) {
			return &roomkeystore.VersionedKeyPair{
				Version: 4,
				KeyPair: roomkeystore.RoomKeyPair{PrivateKey: []byte("old-key-bytes-0123456789012345")},
			}, nil
		},
		RotateFn: func(_ context.Context, _ string, _ roomkeystore.RoomKeyPair) (int, error) {
			return 0, roomkeystore.ErrNoCurrentKey
		},
		// This caller wins the race, so the post-image carries its own bytes.
		SetIfAbsentFn: func(_ context.Context, _ string, pair roomkeystore.RoomKeyPair) (*roomkeystore.VersionedKeyPair, error) {
			setCalled = true
			setPriv = pair.PrivateKey
			return &roomkeystore.VersionedKeyPair{Version: 0, KeyPair: pair}, nil
		},
	}
	var order []string
	pub := &orderedPublisher{log: &order}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, keyStore, roomkeysender.NewSender(pub))
	c := withIdentity(t, "r1", ident())

	_, err := h.handleRemove(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.NoError(t, err)

	assert.True(t, setCalled, "the ErrNoCurrentKey fallback must adopt a fresh key via SetIfAbsent")
	require.Len(t, pub.payloads, 1)
	var evt model.RoomKeyEvent
	require.NoError(t, json.Unmarshal(pub.payloads[0], &evt))
	assert.Equal(t, 0, evt.Version, "the fallback adopts version 0")
	assert.Equal(t, setPriv, evt.PrivateKey,
		"survivors receive exactly the bytes the post-image confirmed")
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
	assert.Contains(t, err.Error(), "rotate room key")
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

// A rotation that never commits must not hand survivors a key the store lacks.
func TestHandleRemove_RotateFails_FansOutNothing(t *testing.T) {
	store := removeKeyStore()
	keyStore := &fakeKeyStore{
		GetFn: func(_ context.Context, _ string) (*roomkeystore.VersionedKeyPair, error) {
			return &roomkeystore.VersionedKeyPair{
				Version: 5,
				KeyPair: roomkeystore.RoomKeyPair{PrivateKey: []byte("old-key-bytes-0123456789012345")},
			}, nil
		},
		RotateFn: func(_ context.Context, _ string, _ roomkeystore.RoomKeyPair) (int, error) {
			return 0, errors.New("mongo down")
		},
	}
	var order []string
	pub := &orderedPublisher{log: &order}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, keyStore, roomkeysender.NewSender(pub))
	c := withIdentity(t, "r1", ident())

	_, err := h.handleRemove(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.Error(t, err)
	assert.Empty(t, pub.payloads, "a failed rotation must not hand survivors a phantom key")
}
