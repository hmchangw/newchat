package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgbucket"
)

func TestCassandraStore_SaveMessage_InstrumentedBatchOutcome(t *testing.T) {
	wantErr := errors.New("batch unavailable")
	tests := []struct {
		name     string
		batchErr error
		wantErr  string
	}{
		{name: "success"},
		{name: "failure", batchErr: wantErr, wantErr: "save message message-1: batch unavailable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewCassandraStore(nil, msgbucket.New(time.Hour), nil)
			store.newBatch = newOfflineBatch
			calls := 0
			store.executeBatch = func(_ context.Context, batch *gocql.Batch) error {
				calls++
				assert.Len(t, batch.Entries, 2)
				return tt.batchErr
			}
			msg := &model.Message{ID: "message-1", RoomID: "room-1", CreatedAt: time.UnixMilli(1_000)}

			err := store.SaveMessage(context.Background(), msg, nil, "site-a")

			assert.Equal(t, 1, calls)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tt.wantErr)
				assert.ErrorIs(t, err, wantErr)
			}
		})
	}
}
