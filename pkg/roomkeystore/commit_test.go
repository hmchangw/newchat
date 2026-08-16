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
	rotateFn func(ctx context.Context, roomID string, newPair RoomKeyPair) (int, error)
	setFn    func(ctx context.Context, roomID string, pair RoomKeyPair) (int, error)
	getFn    func(ctx context.Context, roomID string) (*VersionedKeyPair, error)

	rotateCalls int
	setCalls    int
	getCalls    int
}

func (f *fakeCommitter) Rotate(ctx context.Context, roomID string, newPair RoomKeyPair) (int, error) {
	f.rotateCalls++
	if f.rotateFn == nil {
		return 0, errors.New("unexpected Rotate")
	}
	return f.rotateFn(ctx, roomID, newPair)
}

func (f *fakeCommitter) Set(ctx context.Context, roomID string, pair RoomKeyPair) (int, error) {
	f.setCalls++
	if f.setFn == nil {
		return 0, errors.New("unexpected Set")
	}
	return f.setFn(ctx, roomID, pair)
}

func (f *fakeCommitter) Get(ctx context.Context, roomID string) (*VersionedKeyPair, error) {
	f.getCalls++
	if f.getFn == nil {
		return nil, errors.New("unexpected Get")
	}
	return f.getFn(ctx, roomID)
}

func newKey(b byte) RoomKeyPair { return RoomKeyPair{PrivateKey: bytes.Repeat([]byte{b}, 32)} }

// A successful Rotate is authoritative: its post-image bound the version to these bytes.
func TestCommitRotation_RotateSuccess_NoReadBack(t *testing.T) {
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
	assert.Zero(t, store.setCalls, "a successful Rotate must not fall back to Set")
	assert.Zero(t, store.getCalls, "Rotate's post-image is authoritative without a re-read")
}

// Key vanished between Get and Rotate: fall back to Set, then re-read (last-write-wins).
func TestCommitRotation_RotateNoCurrentKey_FallsBackToSetThenGet(t *testing.T) {
	settled := &VersionedKeyPair{Version: 0, KeyPair: newKey(0xBB)}
	store := &fakeCommitter{
		rotateFn: func(_ context.Context, _ string, _ RoomKeyPair) (int, error) {
			return 0, ErrNoCurrentKey
		},
		setFn: func(_ context.Context, _ string, _ RoomKeyPair) (int, error) { return 0, nil },
		getFn: func(_ context.Context, _ string) (*VersionedKeyPair, error) { return settled, nil },
	}
	newPair := newKey(0xCC)

	committed, err := CommitRotation(context.Background(), store, "r1",
		&VersionedKeyPair{Version: 3, KeyPair: newKey(0x22)}, &newPair)
	require.NoError(t, err)
	assert.Same(t, settled, committed, "the read-back pair is what callers must fan out")
	assert.Equal(t, 1, store.rotateCalls)
	assert.Equal(t, 1, store.setCalls)
	assert.Equal(t, 1, store.getCalls)
}

// A room with no current key at all skips Rotate entirely and adopts v0 via Set.
func TestCommitRotation_NilCurrentPair_SetsThenReadsBack(t *testing.T) {
	settled := &VersionedKeyPair{Version: 0, KeyPair: newKey(0xDD)}
	store := &fakeCommitter{
		setFn: func(_ context.Context, _ string, _ RoomKeyPair) (int, error) { return 0, nil },
		getFn: func(_ context.Context, _ string) (*VersionedKeyPair, error) { return settled, nil },
	}
	newPair := newKey(0xEE)

	committed, err := CommitRotation(context.Background(), store, "r1", nil, &newPair)
	require.NoError(t, err)
	assert.Same(t, settled, committed)
	assert.Zero(t, store.rotateCalls, "no current key means there is nothing to rotate")
	assert.Equal(t, 1, store.setCalls)
	assert.Equal(t, 1, store.getCalls)
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
	assert.Zero(t, store.setCalls, "a non-ErrNoCurrentKey Rotate failure must not fall through to Set")
}

func TestCommitRotation_SetError_ReturnsNoPair(t *testing.T) {
	store := &fakeCommitter{
		setFn: func(_ context.Context, _ string, _ RoomKeyPair) (int, error) {
			return 0, errors.New("mongo down")
		},
	}
	newPair := newKey(0x03)

	committed, err := CommitRotation(context.Background(), store, "r1", nil, &newPair)
	require.Error(t, err)
	assert.Nil(t, committed)
	assert.Contains(t, err.Error(), "store room key")
	assert.Zero(t, store.getCalls, "a failed Set must not be read back")
}

// The read-back is what makes the Set leg trustworthy, so its failure fails the commit.
func TestCommitRotation_GetAfterSetError_ReturnsNoPair(t *testing.T) {
	store := &fakeCommitter{
		setFn: func(_ context.Context, _ string, _ RoomKeyPair) (int, error) { return 0, nil },
		getFn: func(_ context.Context, _ string) (*VersionedKeyPair, error) {
			return nil, errors.New("mongo down")
		},
	}
	newPair := newKey(0x04)

	committed, err := CommitRotation(context.Background(), store, "r1", nil, &newPair)
	require.Error(t, err)
	assert.Nil(t, committed)
	assert.Contains(t, err.Error(), "read back stored room key")
}

// (nil, nil) means the key was deleted underneath us — a failed commit, as ErrNoCurrentKey.
func TestCommitRotation_GetAfterSetReturnsNilPair_ReturnsNoPair(t *testing.T) {
	store := &fakeCommitter{
		setFn: func(_ context.Context, _ string, _ RoomKeyPair) (int, error) { return 0, nil },
		getFn: func(_ context.Context, _ string) (*VersionedKeyPair, error) { return nil, nil },
	}
	newPair := newKey(0x05)

	committed, err := CommitRotation(context.Background(), store, "r1", nil, &newPair)
	require.Error(t, err)
	assert.Nil(t, committed)
	assert.ErrorIs(t, err, ErrNoCurrentKey)
	assert.Contains(t, err.Error(), "read back stored room key")
}
