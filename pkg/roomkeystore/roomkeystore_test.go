package roomkeystore

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInitialKeyDoc(t *testing.T) {
	priv := bytes.Repeat([]byte{0x07}, 32)
	doc := InitialKeyDoc(RoomKeyPair{PrivateKey: priv})

	assert.Equal(t, priv, doc["priv"], "priv must carry the raw secret bytes")
	assert.Equal(t, 0, doc["ver"], "a freshly provisioned key is version 0")
	_, hasPrev := doc["prevPriv"]
	assert.False(t, hasPrev, "previous-key slot must be unset until the first Rotate")
}

func TestKeyDoc_versioned(t *testing.T) {
	priv := bytes.Repeat([]byte{0xAB}, 32)

	t.Run("valid current key", func(t *testing.T) {
		d := &keyDoc{Priv: priv, Ver: 3}
		got, err := d.versioned()
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, 3, got.Version)
		assert.Equal(t, priv, got.KeyPair.PrivateKey)
	})

	t.Run("invalid length errors", func(t *testing.T) {
		d := &keyDoc{Priv: []byte{0x01, 0x02}, Ver: 0}
		_, err := d.versioned()
		require.Error(t, err)
	})
}

func TestKeyDoc_pairForVersion(t *testing.T) {
	cur := bytes.Repeat([]byte{0x11}, 32)
	prev := bytes.Repeat([]byte{0x22}, 32)
	now := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)
	future := now.Add(time.Hour)
	past := now.Add(-time.Hour)

	tests := []struct {
		name    string
		doc     *keyDoc
		version int
		want    []byte // nil means expect (nil, nil)
		wantErr bool   // expect a non-nil error (corrupt slot)
	}{
		{
			name:    "matches current",
			doc:     &keyDoc{Priv: cur, Ver: 5},
			version: 5,
			want:    cur,
		},
		{
			name:    "matches previous within grace",
			doc:     &keyDoc{Priv: cur, Ver: 5, PrevPriv: prev, PrevVer: 4, PrevExpiresAt: &future},
			version: 4,
			want:    prev,
		},
		{
			name:    "previous expired returns nil",
			doc:     &keyDoc{Priv: cur, Ver: 5, PrevPriv: prev, PrevVer: 4, PrevExpiresAt: &past},
			version: 4,
			want:    nil,
		},
		{
			name:    "unknown version returns nil",
			doc:     &keyDoc{Priv: cur, Ver: 5, PrevPriv: prev, PrevVer: 4, PrevExpiresAt: &future},
			version: 99,
			want:    nil,
		},
		{
			name:    "no previous slot, version not current",
			doc:     &keyDoc{Priv: cur, Ver: 5},
			version: 4,
			want:    nil,
		},
		{
			name:    "previous with nil expiry treated as absent",
			doc:     &keyDoc{Priv: cur, Ver: 5, PrevPriv: prev, PrevVer: 4},
			version: 4,
			want:    nil,
		},
		{
			// Matches the Valkey store: a current slot whose version matches but
			// whose secret is corrupt is an error, not a silent miss.
			name:    "current version matches but secret corrupt errors",
			doc:     &keyDoc{Priv: []byte{0x01, 0x02}, Ver: 5},
			version: 5,
			wantErr: true,
		},
		{
			name:    "previous within grace matches but secret corrupt errors",
			doc:     &keyDoc{Priv: cur, Ver: 5, PrevPriv: []byte{0x01}, PrevVer: 4, PrevExpiresAt: &future},
			version: 4,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.doc.pairForVersion(tc.version, now)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tc.want == nil {
				assert.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			assert.Equal(t, tc.want, got.PrivateKey)
		})
	}
}

func TestRetiredKeyID(t *testing.T) {
	assert.Equal(t, "room123:7", retiredKeyID("room123", 7))
	assert.Equal(t, "room123:0", retiredKeyID("room123", 0))
}

func TestNewMongoStore_RetiredKeysOptional(t *testing.T) {
	t.Run("omitted leaves the archive disabled", func(t *testing.T) {
		s := newMongoStore(nil, time.Hour)
		assert.Nil(t, s.retiredCol)
		assert.Zero(t, s.retiredTTL)
	})

	t.Run("option records the retention", func(t *testing.T) {
		s := newMongoStore(nil, time.Hour, WithRetiredKeys(nil, 20*time.Minute))
		assert.Equal(t, 20*time.Minute, s.retiredTTL)
	})
}

func TestMongoStore_EnsureIndexes_NoArchiveConfigured(t *testing.T) {
	s := newMongoStore(nil, time.Hour)
	require.NoError(t, s.EnsureIndexes(context.Background()),
		"EnsureIndexes must no-op when the archive is not configured")
}

func TestMongoStore_retiredDoc(t *testing.T) {
	priv := bytes.Repeat([]byte{0x33}, 32)
	now := time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC)

	s := newMongoStore(nil, time.Hour, WithRetiredKeys(nil, 20*time.Minute))
	s.now = func() time.Time { return now }

	doc := s.retiredDoc(priv)
	assert.Equal(t, priv, doc["priv"])
	assert.Equal(t, now.Add(20*time.Minute).UTC(), doc["expiresAt"],
		"expiresAt is stamped from the injected clock plus the retention")
}

func TestMongoStore_archiveRetired_NoArchiveConfigured(t *testing.T) {
	s := newMongoStore(nil, time.Hour)
	// retiredCol is nil — must return without touching Mongo rather than panic.
	s.archiveRetired(context.Background(), "room1", 4, bytes.Repeat([]byte{0x01}, 32))
}

// The len(priv) == 0 guard needs a configured collection; covered in integration_test.go.

func TestMongoStore_retiredByVersion_NoArchiveConfigured(t *testing.T) {
	s := newMongoStore(nil, time.Hour)
	pair, err := s.retiredByVersion(context.Background(), "room1", 3)
	require.NoError(t, err, "an unconfigured archive is a clean miss, not an error")
	assert.Nil(t, pair)
}
