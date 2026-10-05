package main

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/searchengine"
)

// staticWrapper wraps with a fixed key so tests can round-trip without Vault.
type staticWrapper struct{ aead cipher.AEAD }

func newStaticWrapper(t *testing.T) *staticWrapper {
	t.Helper()
	block, err := aes.NewCipher(bytes.Repeat([]byte{'k'}, 32))
	require.NoError(t, err)
	a, err := cipher.NewGCM(block)
	require.NoError(t, err)
	return &staticWrapper{aead: a}
}
func (w *staticWrapper) GenerateDataKey(context.Context) ([]byte, []byte, error) {
	dek := bytes.Repeat([]byte{'d'}, 32)
	wrapped, err := w.Wrap(context.Background(), dek)
	return dek, wrapped, err
}
func (w *staticWrapper) Wrap(_ context.Context, dek []byte) ([]byte, error) {
	nonce := bytes.Repeat([]byte{'n'}, w.aead.NonceSize())
	return append(append([]byte{}, nonce...), w.aead.Seal(nil, nonce, dek, nil)...), nil
}
func (w *staticWrapper) Unwrap(_ context.Context, ct []byte) ([]byte, error) {
	ns := w.aead.NonceSize()
	return w.aead.Open(nil, ct[:ns], ct[ns:], nil)
}

// keyDocBody decodes a KeyDoc written to the bucket.
func keyDocBody(t *testing.T, body io.Reader) auditarchive.KeyDoc {
	t.Helper()
	var d auditarchive.KeyDoc
	require.NoError(t, json.NewDecoder(body).Decode(&d))
	return d
}

// putKeyDoc expects one escrow Put at key and captures the KeyDoc written.
func putKeyDoc(t *testing.T, obj *MockobjectStore, key string, got *auditarchive.KeyDoc) *gomock.Call {
	t.Helper()
	return obj.EXPECT().Put(gomock.Any(), key, gomock.Any(), gomock.Any(), "application/json").
		DoAndReturn(func(_ context.Context, _ string, body io.Reader, size int64, _ string) error {
			b, err := io.ReadAll(body)
			require.NoError(t, err)
			assert.Equal(t, int64(len(b)), size)
			*got = keyDocBody(t, bytes.NewReader(b))
			return nil
		})
}

func TestLoadOrCreateDEK(t *testing.T) {
	ctx := context.Background()
	now := func() time.Time { return time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC) }

	t.Run("existing key with its bucket copy is unwrapped, nothing written", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		idx, obj := NewMockindexStore(ctrl), NewMockobjectStore(ctrl)
		w := newStaticWrapper(t)
		wrapped, _ := w.Wrap(ctx, bytes.Repeat([]byte{'x'}, 32))
		doc := hitJSON(t, auditarchive.KeyDoc{SiteID: "site-a", WrappedDek: wrapped, CreatedAt: now()})
		idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(doc, true, nil)
		obj.EXPECT().Stat(ctx, "site-a/keys/current.json").Return(true, nil)

		dek, keyID, err := loadOrCreateDEK(ctx, idx, obj, w, "site-a", now)
		require.NoError(t, err)
		assert.Equal(t, bytes.Repeat([]byte{'x'}, 32), dek)
		assert.Equal(t, auditarchive.KeyID(wrapped), keyID)
	})

	t.Run("existing key without a bucket copy is escrowed before returning", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		idx, obj := NewMockindexStore(ctrl), NewMockobjectStore(ctrl)
		w := newStaticWrapper(t)
		wrapped, _ := w.Wrap(ctx, bytes.Repeat([]byte{'x'}, 32))
		stored := auditarchive.KeyDoc{SiteID: "site-a", WrappedDek: wrapped, CreatedAt: now().Add(-time.Hour)}
		idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(hitJSON(t, stored), true, nil)
		var byID, current auditarchive.KeyDoc
		gomock.InOrder(
			obj.EXPECT().Stat(ctx, "site-a/keys/current.json").Return(false, nil),
			putKeyDoc(t, obj, "site-a/keys/"+auditarchive.KeyID(wrapped)+".json", &byID),
			putKeyDoc(t, obj, "site-a/keys/current.json", &current),
		)

		dek, keyID, err := loadOrCreateDEK(ctx, idx, obj, w, "site-a", now)
		require.NoError(t, err)
		assert.Equal(t, bytes.Repeat([]byte{'x'}, 32), dek)
		assert.Equal(t, auditarchive.KeyID(wrapped), keyID)
		assert.Equal(t, stored, byID, "the bucket holds the same KeyDoc as the index")
		assert.Equal(t, stored, current)
	})

	t.Run("index doc missing but bucket copy present refuses to mint", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		idx, obj := NewMockindexStore(ctrl), NewMockobjectStore(ctrl)
		idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(nil, false, nil)
		obj.EXPECT().Stat(ctx, "site-a/keys/current.json").Return(true, nil)

		dek, keyID, err := loadOrCreateDEK(ctx, idx, obj, failingWrapper{}, "site-a", now)
		require.Error(t, err)
		assert.Nil(t, dek)
		assert.Empty(t, keyID)
		assert.Contains(t, err.Error(), "audit-keys doc missing but bucket holds site-a/keys/current.json")
		assert.Contains(t, err.Error(), "restore the index document from it before starting")
	})

	t.Run("missing key is minted, escrowed in the bucket, then created in the index", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		idx, obj := NewMockindexStore(ctrl), NewMockobjectStore(ctrl)
		w := newStaticWrapper(t)
		var byID, current, indexed auditarchive.KeyDoc
		_, mintedWrapped, err := w.GenerateDataKey(ctx)
		require.NoError(t, err)
		gomock.InOrder(
			idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(nil, false, nil),
			obj.EXPECT().Stat(ctx, "site-a/keys/current.json").Return(false, nil),
			putKeyDoc(t, obj, "site-a/keys/"+auditarchive.KeyID(mintedWrapped)+".json", &byID),
			putKeyDoc(t, obj, "site-a/keys/current.json", &current),
			idx.EXPECT().Bulk(ctx, gomock.Any()).DoAndReturn(func(_ context.Context, a []searchengine.BulkAction) ([]searchengine.BulkResult, error) {
				require.Len(t, a, 1)
				assert.Equal(t, searchengine.ActionCreate, a[0].Action)
				assert.Equal(t, "audit-keys-site-a", a[0].Index)
				assert.Equal(t, "current", a[0].DocID)
				require.NoError(t, json.Unmarshal(a[0].Doc, &indexed))
				return []searchengine.BulkResult{{Status: 201}}, nil
			}),
		)
		dek, keyID, err := loadOrCreateDEK(ctx, idx, obj, w, "site-a", now)
		require.NoError(t, err)
		assert.Len(t, dek, auditarchive.DEKSize)
		assert.Equal(t, "site-a", indexed.SiteID)
		assert.Equal(t, now(), indexed.CreatedAt)
		assert.Equal(t, indexed, current, "bucket and index hold the same KeyDoc")
		assert.Equal(t, indexed, byID)
		assert.Equal(t, auditarchive.KeyID(indexed.WrappedDek), keyID)
		assert.NotEqual(t, dek, indexed.WrappedDek, "the plaintext DEK must never be stored")
		unwrapped, err := w.Unwrap(ctx, indexed.WrappedDek)
		require.NoError(t, err)
		assert.Equal(t, dek, unwrapped, "the stored document must wrap the returned DEK")
	})

	t.Run("create conflict re-reads the winner and escrows it over the loser's copy", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		idx, obj := NewMockindexStore(ctrl), NewMockobjectStore(ctrl)
		w := newStaticWrapper(t)
		_, loser, err := w.GenerateDataKey(ctx)
		require.NoError(t, err)
		winner, _ := w.Wrap(ctx, bytes.Repeat([]byte{'w'}, 32))
		winnerDoc := auditarchive.KeyDoc{SiteID: "site-a", WrappedDek: winner, CreatedAt: now().Add(-time.Second)}
		var loserByID, loserCurrent, byID, current auditarchive.KeyDoc
		gomock.InOrder(
			idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(nil, false, nil),
			obj.EXPECT().Stat(ctx, "site-a/keys/current.json").Return(false, nil),
			putKeyDoc(t, obj, "site-a/keys/"+auditarchive.KeyID(loser)+".json", &loserByID),
			putKeyDoc(t, obj, "site-a/keys/current.json", &loserCurrent),
			idx.EXPECT().Bulk(ctx, gomock.Any()).Return([]searchengine.BulkResult{{Status: 409, ErrorType: "version_conflict_engine_exception"}}, nil),
			idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(hitJSON(t, winnerDoc), true, nil),
			putKeyDoc(t, obj, "site-a/keys/"+auditarchive.KeyID(winner)+".json", &byID),
			putKeyDoc(t, obj, "site-a/keys/current.json", &current),
		)
		dek, keyID, err := loadOrCreateDEK(ctx, idx, obj, w, "site-a", now)
		require.NoError(t, err)
		assert.Equal(t, bytes.Repeat([]byte{'w'}, 32), dek)
		assert.Equal(t, auditarchive.KeyID(winner), keyID)
		assert.Equal(t, winnerDoc, current, "the newest current.json names the key the index holds")
		assert.Equal(t, winnerDoc, byID)
		assert.Equal(t, loser, loserByID.WrappedDek, "the loser's own escrow copy stays under its key id")
	})

	t.Run("index error fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		idx := NewMockindexStore(ctrl)
		idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(nil, false, errors.New("boom"))
		_, _, err := loadOrCreateDEK(ctx, idx, NewMockobjectStore(ctrl), newStaticWrapper(t), "site-a", now)
		assert.Error(t, err)
	})

	t.Run("wrapper failure fails before anything is written", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		idx, obj := NewMockindexStore(ctrl), NewMockobjectStore(ctrl)
		idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(nil, false, nil)
		obj.EXPECT().Stat(ctx, "site-a/keys/current.json").Return(false, nil)
		_, _, err := loadOrCreateDEK(ctx, idx, obj, failingWrapper{}, "site-a", now)
		assert.Error(t, err)
	})
}

// hitJSON wraps a document the way GET /{index}/_doc/{id} returns it.
func hitJSON(t *testing.T, doc any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(map[string]any{"_index": "x", "_id": "current", "found": true, "_source": doc})
	require.NoError(t, err)
	return b
}

type failingWrapper struct{}

func (failingWrapper) GenerateDataKey(context.Context) ([]byte, []byte, error) {
	return nil, nil, errors.New("vault down")
}
func (failingWrapper) Wrap(context.Context, []byte) ([]byte, error) {
	return nil, errors.New("vault down")
}
func (failingWrapper) Unwrap(context.Context, []byte) ([]byte, error) {
	return nil, errors.New("vault down")
}

func TestLoadOrCreateDEK_ErrorPaths(t *testing.T) {
	ctx := context.Background()
	now := func() time.Time { return time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC) }
	w := newStaticWrapper(t)
	shortWrapped, err := w.Wrap(ctx, []byte("too short"))
	require.NoError(t, err)
	good, err := w.Wrap(ctx, bytes.Repeat([]byte{'x'}, 32))
	require.NoError(t, err)
	goodDoc := hitJSON(t, auditarchive.KeyDoc{SiteID: "site-a", WrappedDek: good})
	missing := func(idx *MockindexStore, obj *MockobjectStore) {
		idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(nil, false, nil)
		obj.EXPECT().Stat(ctx, "site-a/keys/current.json").Return(false, nil)
	}
	escrowed := func(obj *MockobjectStore) {
		obj.EXPECT().Put(ctx, gomock.Any(), gomock.Any(), gomock.Any(), "application/json").Return(nil).Times(2)
	}

	cases := []struct {
		name  string
		setup func(idx *MockindexStore, obj *MockobjectStore)
	}{
		{"bulk transport error", func(idx *MockindexStore, obj *MockobjectStore) {
			missing(idx, obj)
			escrowed(obj)
			idx.EXPECT().Bulk(ctx, gomock.Any()).Return(nil, errors.New("es down"))
		}},
		{"bulk returns no result", func(idx *MockindexStore, obj *MockobjectStore) {
			missing(idx, obj)
			escrowed(obj)
			idx.EXPECT().Bulk(ctx, gomock.Any()).Return(nil, nil)
		}},
		{"bulk rejects with a non-conflict status", func(idx *MockindexStore, obj *MockobjectStore) {
			missing(idx, obj)
			escrowed(obj)
			idx.EXPECT().Bulk(ctx, gomock.Any()).Return([]searchengine.BulkResult{{Status: 500, ErrorType: "boom"}}, nil)
		}},
		{"conflict but the winner is not readable", func(idx *MockindexStore, obj *MockobjectStore) {
			missing(idx, obj)
			escrowed(obj)
			idx.EXPECT().Bulk(ctx, gomock.Any()).Return([]searchengine.BulkResult{{Status: 409}}, nil)
			idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(nil, false, nil)
		}},
		{"conflict and the re-read fails", func(idx *MockindexStore, obj *MockobjectStore) {
			missing(idx, obj)
			escrowed(obj)
			idx.EXPECT().Bulk(ctx, gomock.Any()).Return([]searchengine.BulkResult{{Status: 409}}, nil)
			idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(nil, false, errors.New("es down"))
		}},
		{"escrow of a new key fails before the index is touched", func(idx *MockindexStore, obj *MockobjectStore) {
			missing(idx, obj)
			obj.EXPECT().Put(ctx, gomock.Any(), gomock.Any(), gomock.Any(), "application/json").Return(errors.New("bucket down"))
		}},
		{"bucket stat fails when the index has no key", func(idx *MockindexStore, obj *MockobjectStore) {
			idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(nil, false, nil)
			obj.EXPECT().Stat(ctx, "site-a/keys/current.json").Return(false, errors.New("bucket down"))
		}},
		{"bucket stat fails when the index has the key", func(idx *MockindexStore, obj *MockobjectStore) {
			idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(goodDoc, true, nil)
			obj.EXPECT().Stat(ctx, "site-a/keys/current.json").Return(false, errors.New("bucket down"))
		}},
		{"escrow of an existing key fails", func(idx *MockindexStore, obj *MockobjectStore) {
			idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(goodDoc, true, nil)
			obj.EXPECT().Stat(ctx, "site-a/keys/current.json").Return(false, nil)
			obj.EXPECT().Put(ctx, gomock.Any(), gomock.Any(), gomock.Any(), "application/json").Return(errors.New("bucket down"))
		}},
		{"stored hit is not JSON", func(idx *MockindexStore, _ *MockobjectStore) {
			idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(json.RawMessage("{"), true, nil)
		}},
		{"stored DEK has the wrong length", func(idx *MockindexStore, _ *MockobjectStore) {
			doc := hitJSON(t, auditarchive.KeyDoc{SiteID: "site-a", WrappedDek: shortWrapped})
			idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(doc, true, nil)
		}},
		{"stored DEK cannot be unwrapped", func(idx *MockindexStore, _ *MockobjectStore) {
			doc := hitJSON(t, auditarchive.KeyDoc{SiteID: "site-a", WrappedDek: bytes.Repeat([]byte{1}, 64)})
			idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(doc, true, nil)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			idx, obj := NewMockindexStore(ctrl), NewMockobjectStore(ctrl)
			tc.setup(idx, obj)
			dek, keyID, err := loadOrCreateDEK(ctx, idx, obj, w, "site-a", now)
			require.Error(t, err)
			assert.Nil(t, dek)
			assert.Empty(t, keyID)
		})
	}
}
