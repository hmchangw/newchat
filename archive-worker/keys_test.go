package main

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/json"
	"errors"
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

func TestLoadOrCreateDEK(t *testing.T) {
	ctx := context.Background()
	now := func() time.Time { return time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC) }

	t.Run("existing key is unwrapped, nothing written", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		idx := NewMockindexStore(ctrl)
		w := newStaticWrapper(t)
		wrapped, _ := w.Wrap(ctx, bytes.Repeat([]byte{'x'}, 32))
		doc := hitJSON(t, auditarchive.KeyDoc{SiteID: "site-a", WrappedDek: wrapped, CreatedAt: now()})
		idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(doc, true, nil)

		dek, err := loadOrCreateDEK(ctx, idx, w, "site-a", now)
		require.NoError(t, err)
		assert.Equal(t, bytes.Repeat([]byte{'x'}, 32), dek)
	})

	t.Run("missing key is generated and created", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		idx := NewMockindexStore(ctrl)
		w := newStaticWrapper(t)
		var gotWrapped []byte
		idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(nil, false, nil)
		idx.EXPECT().Bulk(ctx, gomock.Any()).DoAndReturn(func(_ context.Context, a []searchengine.BulkAction) ([]searchengine.BulkResult, error) {
			require.Len(t, a, 1)
			assert.Equal(t, searchengine.ActionCreate, a[0].Action)
			assert.Equal(t, "audit-keys-site-a", a[0].Index)
			assert.Equal(t, "current", a[0].DocID)
			var d auditarchive.KeyDoc
			require.NoError(t, json.Unmarshal(a[0].Doc, &d))
			assert.Equal(t, "site-a", d.SiteID)
			assert.NotEmpty(t, d.WrappedDek)
			assert.Equal(t, now(), d.CreatedAt)
			gotWrapped = d.WrappedDek
			return []searchengine.BulkResult{{Status: 201}}, nil
		})
		dek, err := loadOrCreateDEK(ctx, idx, w, "site-a", now)
		require.NoError(t, err)
		assert.Len(t, dek, auditarchive.DEKSize)
		assert.NotEqual(t, dek, gotWrapped, "the plaintext DEK must never be stored")
		unwrapped, err := w.Unwrap(ctx, gotWrapped)
		require.NoError(t, err)
		assert.Equal(t, dek, unwrapped, "the stored document must wrap the returned DEK")
	})

	t.Run("create conflict re-reads the winner", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		idx := NewMockindexStore(ctrl)
		w := newStaticWrapper(t)
		winner, _ := w.Wrap(ctx, bytes.Repeat([]byte{'w'}, 32))
		doc := hitJSON(t, auditarchive.KeyDoc{SiteID: "site-a", WrappedDek: winner})
		gomock.InOrder(
			idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(nil, false, nil),
			idx.EXPECT().Bulk(ctx, gomock.Any()).Return([]searchengine.BulkResult{{Status: 409, ErrorType: "version_conflict_engine_exception"}}, nil),
			idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(doc, true, nil),
		)
		dek, err := loadOrCreateDEK(ctx, idx, w, "site-a", now)
		require.NoError(t, err)
		assert.Equal(t, bytes.Repeat([]byte{'w'}, 32), dek)
	})

	t.Run("index error fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		idx := NewMockindexStore(ctrl)
		idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(nil, false, errors.New("boom"))
		_, err := loadOrCreateDEK(ctx, idx, newStaticWrapper(t), "site-a", now)
		assert.Error(t, err)
	})

	t.Run("wrapper failure fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		idx := NewMockindexStore(ctrl)
		idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(nil, false, nil)
		_, err := loadOrCreateDEK(ctx, idx, failingWrapper{}, "site-a", now)
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

	cases := []struct {
		name  string
		setup func(idx *MockindexStore)
	}{
		{"bulk transport error", func(idx *MockindexStore) {
			idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(nil, false, nil)
			idx.EXPECT().Bulk(ctx, gomock.Any()).Return(nil, errors.New("es down"))
		}},
		{"bulk returns no result", func(idx *MockindexStore) {
			idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(nil, false, nil)
			idx.EXPECT().Bulk(ctx, gomock.Any()).Return(nil, nil)
		}},
		{"bulk rejects with a non-conflict status", func(idx *MockindexStore) {
			idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(nil, false, nil)
			idx.EXPECT().Bulk(ctx, gomock.Any()).Return([]searchengine.BulkResult{{Status: 500, ErrorType: "boom"}}, nil)
		}},
		{"conflict but the winner is not readable", func(idx *MockindexStore) {
			idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(nil, false, nil).Times(2)
			idx.EXPECT().Bulk(ctx, gomock.Any()).Return([]searchengine.BulkResult{{Status: 409}}, nil)
		}},
		{"conflict and the re-read fails", func(idx *MockindexStore) {
			gomock.InOrder(
				idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(nil, false, nil),
				idx.EXPECT().Bulk(ctx, gomock.Any()).Return([]searchengine.BulkResult{{Status: 409}}, nil),
				idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(nil, false, errors.New("es down")),
			)
		}},
		{"stored hit is not JSON", func(idx *MockindexStore) {
			idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(json.RawMessage("{"), true, nil)
		}},
		{"stored DEK has the wrong length", func(idx *MockindexStore) {
			doc := hitJSON(t, auditarchive.KeyDoc{SiteID: "site-a", WrappedDek: shortWrapped})
			idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(doc, true, nil)
		}},
		{"stored DEK cannot be unwrapped", func(idx *MockindexStore) {
			doc := hitJSON(t, auditarchive.KeyDoc{SiteID: "site-a", WrappedDek: bytes.Repeat([]byte{1}, 64)})
			idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(doc, true, nil)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			idx := NewMockindexStore(gomock.NewController(t))
			tc.setup(idx)
			dek, err := loadOrCreateDEK(ctx, idx, w, "site-a", now)
			require.Error(t, err)
			assert.Nil(t, dek)
		})
	}
}
