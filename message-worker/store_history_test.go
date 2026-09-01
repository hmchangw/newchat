package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// TestHistoryStore_TagsEveryCassandraError is the guard that keeps the loss window
// shut. Before the decorator, historyWriteError was applied at three of the eight
// Store call sites in handler.go; the other five returned untagged errors, which
// settle routes down the "not a history failure" path — NAK forever with no degraded
// marker, no CQL classification and no class-labelled metric. Every method is
// asserted here so adding one to Store without a wrap fails a test rather than
// silently reopening the hole.
func TestHistoryStore_TagsEveryCassandraError(t *testing.T) {
	down := errors.New("gocql: no hosts available")

	for _, tt := range []struct {
		name string
		call func(context.Context, Store) error
		set  func(*MockStore)
	}{
		{
			name: "SaveMessage",
			set: func(m *MockStore) {
				m.EXPECT().SaveMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(down)
			},
			call: func(ctx context.Context, s Store) error { return s.SaveMessage(ctx, nil, nil, "site-a") },
		},
		{
			name: "SaveThreadMessage",
			set: func(m *MockStore) {
				m.EXPECT().SaveThreadMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, down)
			},
			call: func(ctx context.Context, s Store) error {
				_, err := s.SaveThreadMessage(ctx, nil, nil, "site-a", "tr-1")
				return err
			},
		},
		{
			name: "GetMessageSender",
			set:  func(m *MockStore) { m.EXPECT().GetMessageSender(gomock.Any(), gomock.Any()).Return(nil, down) },
			call: func(ctx context.Context, s Store) error {
				_, err := s.GetMessageSender(ctx, "m-1")
				return err
			},
		},
		{
			name: "GetQuotedParentSnapshot",
			set: func(m *MockStore) {
				m.EXPECT().GetQuotedParentSnapshot(gomock.Any(), gomock.Any()).Return(nil, false, down)
			},
			call: func(ctx context.Context, s Store) error {
				_, _, err := s.GetQuotedParentSnapshot(ctx, "m-1")
				return err
			},
		},
		{
			name: "GetMessageCreatedAt",
			set: func(m *MockStore) {
				m.EXPECT().GetMessageCreatedAt(gomock.Any(), gomock.Any()).Return(time.Time{}, false, down)
			},
			call: func(ctx context.Context, s Store) error {
				_, _, err := s.GetMessageCreatedAt(ctx, "m-1")
				return err
			},
		},
		{
			name: "UpdateParentMessageThreadRoomID",
			set: func(m *MockStore) {
				m.EXPECT().UpdateParentMessageThreadRoomID(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(down)
			},
			call: func(ctx context.Context, s Store) error {
				return s.UpdateParentMessageThreadRoomID(ctx, "p-1", "r-1", time.Time{}, "tr-1")
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			inner := NewMockStore(gomock.NewController(t))
			tt.set(inner)
			err := tt.call(context.Background(), historyStore{inner})
			require.Error(t, err)
			assert.True(t, isHistoryWriteError(err),
				"%s must tag its error, or an outage starting here leaves the site unflagged", tt.name)
			assert.ErrorIs(t, err, down, "the underlying driver error must stay in the chain for ClassifyCQL")
		})
	}
}

// TestHistoryStore_CleanMissIsNotTagged pins the one deliberate exception: a missing
// row is an ordering race between concurrent workers, not evidence about Cassandra.
func TestHistoryStore_CleanMissIsNotTagged(t *testing.T) {
	inner := NewMockStore(gomock.NewController(t))
	inner.EXPECT().GetMessageCreatedAt(gomock.Any(), "p-1").Return(time.Time{}, false, nil)

	_, found, err := historyStore{inner}.GetMessageCreatedAt(context.Background(), "p-1")
	require.NoError(t, err)
	assert.False(t, found, "a clean miss stays a clean miss: no error, nothing to tag")
}
