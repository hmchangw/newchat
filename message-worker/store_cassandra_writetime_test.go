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
// timestamp a statement carries, not on the ciphertext it carries.
type stubCipher struct{}

func (stubCipher) Encrypt(context.Context, string, atrest.EncryptedFields) ([]byte, atrest.EncMeta, error) { //nolint:gocritic // hugeParam: matches the Cipher interface.
	return []byte("payload"), atrest.EncMeta{Nonce: []byte("nonce")}, nil
}

func (stubCipher) Decrypt(context.Context, string, []byte, atrest.EncMeta) (atrest.EncryptedFields, error) { //nolint:gocritic // hugeParam: matches the Cipher interface.
	return atrest.EncryptedFields{}, nil
}

func (stubCipher) EnsureDEK(context.Context, string) error { return nil }

const createdAtFixture = "2026-03-04T05:06:07.891Z"

// newOfflineBatch builds a batch with no live session, which a unit test may not have.
func newOfflineBatch(ctx context.Context) *gocql.Batch {
	//nolint:staticcheck // SA1019: session.NewBatch requires the prohibited live Cassandra dependency.
	return gocql.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
}

// batchCapture holds the last batch a store submitted instead of executing it.
type batchCapture struct{ batch *gocql.Batch }

func newWriteTimeStore(t *testing.T, cipher atrest.Cipher) (*CassandraStore, *batchCapture) {
	t.Helper()
	store := NewCassandraStore(nil, msgbucket.New(time.Hour), cipher)
	store.newBatch = newOfflineBatch
	captured := &batchCapture{}
	store.executeBatch = func(_ context.Context, batch *gocql.Batch) error {
		captured.batch = batch
		return nil
	}
	return store, captured
}

// writeTimeMessage builds a message fixture. ThreadParentMessageCreatedAt stays
// nil so countAndSetParentTcount short-circuits before reaching the (absent) session.
func writeTimeMessage(t *testing.T, tshow bool) *model.Message {
	t.Helper()
	createdAt, err := time.Parse(time.RFC3339, createdAtFixture)
	require.NoError(t, err)
	return &model.Message{
		ID:                    "message-1",
		RoomID:                "room-1",
		Content:               "hello",
		CreatedAt:             createdAt,
		TShow:                 tshow,
		ThreadParentMessageID: "parent-1",
	}
}

// saveCase drives one create entry point, with or without a cipher. wantWrites
// is the number of tables that create writes.
type saveCase struct {
	name       string
	cipher     atrest.Cipher
	wantWrites int
	save       func(context.Context, *CassandraStore, *model.Message) error
}

// saveCases drives both create entry points, with and without a cipher.
func saveCases() []saveCase {
	saveChannel := func(ctx context.Context, s *CassandraStore, m *model.Message) error {
		return s.SaveMessage(ctx, m, nil, "site-a")
	}
	saveThread := func(ctx context.Context, s *CassandraStore, m *model.Message) error {
		_, err := s.SaveThreadMessage(ctx, m, nil, "site-a", "thread-1")
		return err
	}
	return []saveCase{
		{name: "channel message", wantWrites: 2, save: saveChannel},
		{name: "channel message encrypted", cipher: stubCipher{}, wantWrites: 2, save: saveChannel},
		{name: "thread reply with tshow mirror", wantWrites: 3, save: saveThread},
		{name: "thread reply with tshow mirror encrypted", cipher: stubCipher{}, wantWrites: 3, save: saveThread},
	}
}

// TestCassandraStore_CreatesPinWriteTimestampToCreatedAt asserts every create
// INSERT binds CreatedAt as its write timestamp. See writeTS for why.
// A create INSERT pins its write timestamp only when the bytes it writes are the
// same on every attempt. Plaintext creates are: a redelivery re-binds identical
// values, so the pinned replay is a no-op and cannot outrank a later edit.
//
// Encrypted creates are NOT, and must not pin. cipher.Encrypt draws a fresh
// random nonce per call (pkg/atrest/cipher.go), so a redelivery produces a
// different ciphertext AND a different nonce. enc_payload and enc_meta are
// separate cells, and Cassandra breaks a same-timestamp conflict per cell by
// comparing values — independently. Two attempts pinned to one timestamp can
// therefore leave the payload from one paired with the nonce from the other,
// which AES-GCM cannot open: the message is undecryptable for good. Letting
// those ride the client clock keeps each redelivery strictly newer, so one
// attempt wins both cells and the pair stays coherent.
func TestCassandraStore_PlaintextCreatesPinWriteTimestampToCreatedAt(t *testing.T) {
	for _, tt := range saveCases() {
		if tt.cipher != nil {
			continue // encrypted creates deliberately do not pin; see the test below.
		}
		t.Run(tt.name, func(t *testing.T) {
			store, captured := newWriteTimeStore(t, tt.cipher)
			msg := writeTimeMessage(t, true)

			require.NoError(t, tt.save(context.Background(), store, msg))

			require.NotNil(t, captured.batch)
			inserts := 0
			for i, entry := range captured.batch.Entries {
				if !strings.Contains(entry.Stmt, "INSERT INTO") {
					continue
				}
				inserts++
				assert.Contains(t, entry.Stmt, "USING TIMESTAMP ?", "entry %d must pin its write timestamp", i)
				require.NotEmpty(t, entry.Args, "entry %d has no bound arguments", i)
				assert.Equal(t, msg.CreatedAt.UnixMicro(), entry.Args[len(entry.Args)-1],
					"entry %d must bind CreatedAt as its write timestamp", i)
			}
			assert.Equal(t, tt.wantWrites, inserts)

			// Cassandra rejects a bind-marker/argument mismatch only at execution,
			// which no unit test reaches; count them here instead.
			for i, entry := range captured.batch.Entries {
				assert.Equal(t, strings.Count(entry.Stmt, "?"), len(entry.Args),
					"entry %d binds a different number of arguments than it has markers", i)
			}
		})
	}
}

// TestCassandraStore_EncryptedCreatesStripLegacyColumnsOnTheClientClock asserts
// the counterpart: each encrypted create clears the plaintext body columns in a
// separate, UNPINNED statement. See stripLegacyPlaintextByRoom for why pinning
// those clears would make them land before the row they must clear.
func TestCassandraStore_EncryptedCreatesStripLegacyColumnsOnTheClientClock(t *testing.T) {
	stripped := []string{"msg = null", "attachments = null", "card = null", "card_action = null"}

	for _, tt := range saveCases() {
		if tt.cipher == nil {
			continue // plaintext creates write real values; there is nothing to strip.
		}
		t.Run(tt.name, func(t *testing.T) {
			store, captured := newWriteTimeStore(t, tt.cipher)

			require.NoError(t, tt.save(context.Background(), store, writeTimeMessage(t, true)))

			require.NotNil(t, captured.batch)
			strips := 0
			for i, entry := range captured.batch.Entries {
				if !strings.HasPrefix(strings.TrimSpace(entry.Stmt), "UPDATE") {
					continue
				}
				strips++
				for _, col := range stripped {
					assert.Contains(t, entry.Stmt, col, "strip %d must clear %s", i, col)
				}
				assert.NotContains(t, entry.Stmt, "USING TIMESTAMP",
					"strip %d must ride the client clock so it outranks the legacy row it clears", i)
			}
			assert.Equal(t, tt.wantWrites, strips, "one strip per table written")
		})
	}

	t.Run("pinned inserts do not carry the legacy strip", func(t *testing.T) {
		store, captured := newWriteTimeStore(t, stubCipher{})

		require.NoError(t, store.SaveMessage(context.Background(), writeTimeMessage(t, false), nil, "site-a"))

		require.NotNil(t, captured.batch)
		for i, entry := range captured.batch.Entries {
			if !strings.Contains(entry.Stmt, "INSERT INTO") {
				continue
			}
			// Every clear now lives in the strip. A null bound here would be
			// pinned to CreatedAt and so land before the legacy row it must clear.
			assert.NotContains(t, entry.Stmt, "null",
				"insert %d must leave the plaintext clears to the unpinned strip", i)
		}
	})
}

// The inverse invariant, and the load-bearing one: an encrypted create must
// NEVER pin. Pinning it pairs a ciphertext with a nonce drawn on a different
// attempt once a redelivery writes the same key at the same timestamp, and the
// row stops decrypting permanently. See the comment on the plaintext test for
// why the two cells can diverge.
func TestCassandraStore_EncryptedCreatesDoNotPinWriteTimestamp(t *testing.T) {
	for _, tt := range saveCases() {
		if tt.cipher == nil {
			continue // plaintext creates pin; that is the test above.
		}
		t.Run(tt.name, func(t *testing.T) {
			store, captured := newWriteTimeStore(t, tt.cipher)

			require.NoError(t, tt.save(context.Background(), store, writeTimeMessage(t, true)))

			require.NotNil(t, captured.batch)
			inserts := 0
			for i, entry := range captured.batch.Entries {
				if !strings.Contains(entry.Stmt, "INSERT INTO") {
					continue
				}
				inserts++
				assert.NotContains(t, entry.Stmt, "USING TIMESTAMP",
					"entry %d pins an encrypted create; a redelivery would write a different nonce at the same timestamp", i)
			}
			assert.Equal(t, tt.wantWrites, inserts)

			for i, entry := range captured.batch.Entries {
				assert.Equal(t, strings.Count(entry.Stmt, "?"), len(entry.Args),
					"entry %d binds a different number of arguments than it has markers", i)
			}
		})
	}
}
