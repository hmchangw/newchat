package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/searchengine"
)

func TestFlusher_AddAutoFlushesAtBatchSize(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockESStore(ctrl)
	store.EXPECT().Bulk(gomock.Any(), gomock.Len(2)).Return([]searchengine.BulkResult{{Status: 200}, {Status: 200}}, nil)

	f := newFlusher(store, 2)
	require.NoError(t, f.Add(context.Background(), searchengine.BulkAction{Action: searchengine.ActionIndex, DocID: "a"}))
	require.NoError(t, f.Add(context.Background(), searchengine.BulkAction{Action: searchengine.ActionIndex, DocID: "b"}))

	assert.Equal(t, 0, f.FailedCount())
}

func TestFlusher_FlushSendsRemainingBufferedActions(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockESStore(ctrl)
	store.EXPECT().Bulk(gomock.Any(), gomock.Len(1)).Return([]searchengine.BulkResult{{Status: 201}}, nil)

	f := newFlusher(store, 10)
	require.NoError(t, f.Add(context.Background(), searchengine.BulkAction{Action: searchengine.ActionIndex, DocID: "a"}))
	require.NoError(t, f.Flush(context.Background()))
}

func TestFlusher_BulkRequestErrorIsPropagatedAndCountsAsFailures(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockESStore(ctrl)
	store.EXPECT().Bulk(gomock.Any(), gomock.Any()).Return(nil, errors.New("es unreachable"))

	f := newFlusher(store, 10)
	require.NoError(t, f.Add(context.Background(), searchengine.BulkAction{Action: searchengine.ActionIndex, DocID: "a"}))
	err := f.Flush(context.Background())

	require.Error(t, err)
	assert.Equal(t, 1, f.FailedCount())
}

func TestFlusher_Benign409IsNotCountedAsFailed(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockESStore(ctrl)
	store.EXPECT().Bulk(gomock.Any(), gomock.Any()).Return([]searchengine.BulkResult{{Status: 409}}, nil)

	f := newFlusher(store, 10)
	require.NoError(t, f.Add(context.Background(), searchengine.BulkAction{Action: searchengine.ActionIndex, DocID: "a"}))
	require.NoError(t, f.Flush(context.Background()))

	assert.Equal(t, 0, f.FailedCount())
}

func TestFlusher_HardFailureIsCounted(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockESStore(ctrl)
	store.EXPECT().Bulk(gomock.Any(), gomock.Any()).Return([]searchengine.BulkResult{{Status: 500, Error: "internal_error"}}, nil)

	f := newFlusher(store, 10)
	require.NoError(t, f.Add(context.Background(), searchengine.BulkAction{Action: searchengine.ActionIndex, DocID: "a"}))
	err := f.Flush(context.Background())

	require.NoError(t, err, "a per-item bulk failure logs and continues; it must not itself return an error from Flush")
	assert.Equal(t, 1, f.FailedCount())
}

func TestFlusher_FlushOnEmptyBufferIsANoOp(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockESStore(ctrl)
	// no EXPECT().Bulk(...) — must not be called on an empty buffer

	f := newFlusher(store, 10)
	require.NoError(t, f.Flush(context.Background()))
}
