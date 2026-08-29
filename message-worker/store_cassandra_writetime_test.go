package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgbucket"
)

// stubCipher stands in for a real Cipher: these tests assert on the write
// timestamp an INSERT carries, not on the ciphertext it carries.
type stubCipher struct{}

func (stubCipher) Encrypt(context.Context, string, atrest.EncryptedFields) ([]byte, atrest.EncMeta, error) { //nolint:gocritic // hugeParam: matches the Cipher interface.
	return []byte("payload"), atrest.EncMeta{Nonce: []byte("nonce")}, nil
}

func (stubCipher) Decrypt(context.Context, string, []byte, atrest.EncMeta) (atrest.EncryptedFields, error) { //nolint:gocritic // hugeParam: matches the Cipher interface.
	return atrest.EncryptedFields{}, nil
}

func (stubCipher) EnsureDEK(context.Context, string) error { return nil }

// TestCassandraStore_CreatesPinWriteTimestampToCreatedAt guards the edit-revert
// race. Cassandra resolves conflicts per cell by write timestamp, so a create
// stamped at execution time outranks an edit that landed earlier in wall-clock
// terms but after the create's first attempt — the redelivery silently restores
// the original body. Pinning every create to the message's own CreatedAt makes a
// redelivery re-execute the identical write, which can never outrank a later edit.
func TestCassandraStore_CreatesPinWriteTimestampToCreatedAt(t *testing.T) {
	createdAt := time.Date(2026, 3, 4, 5, 6, 7, 891_000_000, time.UTC)

	// ThreadParentMessageCreatedAt stays nil so countAndSetParentTcount
	// short-circuits before it reaches the (absent) session.
	newMsg := func(tshow bool) *model.Message {
		return &model.Message{
			ID:                    "message-1",
			RoomID:                "room-1",
			Content:               "hello",
			CreatedAt:             createdAt,
			TShow:                 tshow,
			ThreadParentMessageID: "parent-1",
		}
	}

	tests := []struct {
		name        string
		cipher      atrest.Cipher
		wantEntries int
		save        func(context.Context, *CassandraStore) error
	}{
		{
			name:        "channel message",
			wantEntries: 2,
			save: func(ctx context.Context, s *CassandraStore) error {
				return s.SaveMessage(ctx, newMsg(false), nil, "site-a")
			},
		},
		{
			name:        "channel message encrypted",
			cipher:      stubCipher{},
			wantEntries: 2,
			save: func(ctx context.Context, s *CassandraStore) error {
				return s.SaveMessage(ctx, newMsg(false), nil, "site-a")
			},
		},
		{
			name:        "thread reply with tshow mirror",
			wantEntries: 3,
			save: func(ctx context.Context, s *CassandraStore) error {
				_, err := s.SaveThreadMessage(ctx, newMsg(true), nil, "site-a", "thread-1")
				return err
			},
		},
		{
			name:        "thread reply with tshow mirror encrypted",
			cipher:      stubCipher{},
			wantEntries: 3,
			save: func(ctx context.Context, s *CassandraStore) error {
				_, err := s.SaveThreadMessage(ctx, newMsg(true), nil, "site-a", "thread-1")
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewCassandraStore(nil, msgbucket.New(time.Hour), tt.cipher)
			store.newBatch = func(ctx context.Context) *gocql.Batch {
				// No live session is allowed in a unit test; this constructor creates an offline batch.
				//nolint:staticcheck // SA1019: session.NewBatch requires the prohibited live Cassandra dependency.
				return gocql.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
			}
			var captured *gocql.Batch
			store.executeBatch = func(_ context.Context, batch *gocql.Batch) error {
				captured = batch
				return nil
			}

			require.NoError(t, tt.save(context.Background(), store))

			require.NotNil(t, captured)
			require.Len(t, captured.Entries, tt.wantEntries)
			for i, entry := range captured.Entries {
				assert.Contains(t, entry.Stmt, "USING TIMESTAMP ?", "entry %d must pin its write timestamp", i)
				require.NotEmpty(t, entry.Args, "entry %d has no bound arguments", i)
				assert.Equal(t, createdAt.UnixMicro(), entry.Args[len(entry.Args)-1],
					"entry %d must bind CreatedAt as its write timestamp", i)
				// Cassandra rejects a bind-marker/argument mismatch only at execution,
				// which no unit test reaches; count them here instead.
				assert.Equal(t, strings.Count(entry.Stmt, "?"), len(entry.Args),
					"entry %d binds a different number of arguments than it has markers", i)
			}
		})
	}
}
