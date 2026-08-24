package roomkeystore

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeCommitter records call counts so tests can assert which legs of the protocol ran.
type fakeCommitter struct {
	rotateFn      func(ctx context.Context, roomID string, newPair RoomKeyPair) (int, error)
	setIfAbsentFn func(ctx context.Context, roomID string, pair RoomKeyPair) (*VersionedKeyPair, error)

	rotateCalls      int
	setIfAbsentCalls int
}

func (f *fakeCommitter) Rotate(ctx context.Context, roomID string, newPair RoomKeyPair) (int, error) {
	f.rotateCalls++
	if f.rotateFn == nil {
		return 0, errors.New("unexpected Rotate")
	}
	return f.rotateFn(ctx, roomID, newPair)
}

func (f *fakeCommitter) SetIfAbsent(ctx context.Context, roomID string, pair RoomKeyPair) (*VersionedKeyPair, error) {
	f.setIfAbsentCalls++
	if f.setIfAbsentFn == nil {
		return nil, errors.New("unexpected SetIfAbsent")
	}
	return f.setIfAbsentFn(ctx, roomID, pair)
}

func newKey(b byte) RoomKeyPair { return RoomKeyPair{PrivateKey: bytes.Repeat([]byte{b}, 32)} }

// A successful Rotate is authoritative: its post-image bound the version to these bytes.
func TestCommitRotation_RotateSuccess_NoFallback(t *testing.T) {
	newPair := newKey(0xAA)
	store := &fakeCommitter{
		rotateFn: func(_ context.Context, _ string, got RoomKeyPair) (int, error) {
			assert.Equal(t, newPair.PrivateKey, got.PrivateKey)
			return 6, nil
		},
	}
	current := &VersionedKeyPair{Version: 5, KeyPair: newKey(0x11)}

	committed, err := CommitRotation(context.Background(), store, "r1", current, &newPair)
	require.NoError(t, err)
	require.NotNil(t, committed)
	assert.Equal(t, 6, committed.Version)
	assert.Equal(t, newPair.PrivateKey, committed.KeyPair.PrivateKey)
	assert.Equal(t, 1, store.rotateCalls)
	assert.Zero(t, store.setIfAbsentCalls, "a successful Rotate must not fall back to SetIfAbsent")
}

// Key vanished between Get and Rotate: fall back to the atomic set-if-absent.
func TestCommitRotation_RotateNoCurrentKey_FallsBackToSetIfAbsent(t *testing.T) {
	newPair := newKey(0xCC)
	settled := &VersionedKeyPair{Version: 0, KeyPair: newPair}
	store := &fakeCommitter{
		rotateFn: func(_ context.Context, _ string, _ RoomKeyPair) (int, error) {
			return 0, ErrNoCurrentKey
		},
		setIfAbsentFn: func(_ context.Context, _ string, got RoomKeyPair) (*VersionedKeyPair, error) {
			assert.Equal(t, newPair.PrivateKey, got.PrivateKey)
			return settled, nil
		},
	}

	committed, err := CommitRotation(context.Background(), store, "r1",
		&VersionedKeyPair{Version: 3, KeyPair: newKey(0x22)}, &newPair)
	require.NoError(t, err)
	assert.Same(t, settled, committed, "the post-image pair is what callers must fan out")
	assert.Equal(t, 1, store.rotateCalls)
	assert.Equal(t, 1, store.setIfAbsentCalls)
}

// A room with no current key at all skips Rotate entirely and adopts v0.
func TestCommitRotation_NilCurrentPair_SetsIfAbsent(t *testing.T) {
	newPair := newKey(0xEE)
	settled := &VersionedKeyPair{Version: 0, KeyPair: newPair}
	store := &fakeCommitter{
		setIfAbsentFn: func(_ context.Context, _ string, _ RoomKeyPair) (*VersionedKeyPair, error) {
			return settled, nil
		},
	}

	committed, err := CommitRotation(context.Background(), store, "r1", nil, &newPair)
	require.NoError(t, err)
	assert.Same(t, settled, committed)
	assert.Zero(t, store.rotateCalls, "no current key means there is nothing to rotate")
	assert.Equal(t, 1, store.setIfAbsentCalls)
}

// The regression guard: a loser of the set-if-absent race must fan out the
// WINNER's bytes, never its own, or two clients hold different keys under v0.
func TestCommitRotation_LostSetIfAbsentRace_ReturnsWinnersPair(t *testing.T) {
	localPair := newKey(0x77)
	winner := &VersionedKeyPair{Version: 0, KeyPair: newKey(0x88)}
	store := &fakeCommitter{
		setIfAbsentFn: func(_ context.Context, _ string, _ RoomKeyPair) (*VersionedKeyPair, error) {
			return winner, nil
		},
	}

	committed, err := CommitRotation(context.Background(), store, "r1", nil, &localPair)
	require.NoError(t, err)
	require.NotNil(t, committed)
	assert.Equal(t, winner.KeyPair.PrivateKey, committed.KeyPair.PrivateKey,
		"the loser must converge on the winner's key, not fan out its own under the same version")
	assert.NotEqual(t, localPair.PrivateKey, committed.KeyPair.PrivateKey)
	assert.Equal(t, 0, committed.Version)
}

func TestCommitRotation_RotateError_ReturnsNoPair(t *testing.T) {
	store := &fakeCommitter{
		rotateFn: func(_ context.Context, _ string, _ RoomKeyPair) (int, error) {
			return 0, errors.New("mongo down")
		},
	}
	newPair := newKey(0x01)

	committed, err := CommitRotation(context.Background(), store, "r1",
		&VersionedKeyPair{Version: 1, KeyPair: newKey(0x02)}, &newPair)
	require.Error(t, err)
	assert.Nil(t, committed, "a failed commit must never hand back a pair")
	assert.Contains(t, err.Error(), "rotate room key")
	assert.Zero(t, store.setIfAbsentCalls, "a non-ErrNoCurrentKey Rotate failure must not fall through")
}

func TestCommitRotation_SetIfAbsentError_ReturnsNoPair(t *testing.T) {
	store := &fakeCommitter{
		setIfAbsentFn: func(_ context.Context, _ string, _ RoomKeyPair) (*VersionedKeyPair, error) {
			return nil, errors.New("mongo down")
		},
	}
	newPair := newKey(0x03)

	committed, err := CommitRotation(context.Background(), store, "r1", nil, &newPair)
	require.Error(t, err)
	assert.Nil(t, committed)
	assert.Contains(t, err.Error(), "store room key")
	assert.Equal(t, 1, store.setIfAbsentCalls)
}

// A room deleted underneath the fallback surfaces as a failed commit, not a nil pair.
func TestCommitRotation_SetIfAbsentRoomGone_ReturnsNoPair(t *testing.T) {
	store := &fakeCommitter{
		rotateFn: func(_ context.Context, _ string, _ RoomKeyPair) (int, error) {
			return 0, ErrNoCurrentKey
		},
		setIfAbsentFn: func(_ context.Context, _ string, _ RoomKeyPair) (*VersionedKeyPair, error) {
			return nil, ErrRoomNotFound
		},
	}
	newPair := newKey(0x05)

	committed, err := CommitRotation(context.Background(), store, "r1",
		&VersionedKeyPair{Version: 2, KeyPair: newKey(0x06)}, &newPair)
	require.Error(t, err)
	assert.Nil(t, committed)
	assert.ErrorIs(t, err, ErrRoomNotFound)
	assert.Contains(t, err.Error(), "store room key")
}
