package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestESParentResolver_ResolveParentCreatedAt(t *testing.T) {
	val := time.Date(2024, 6, 1, 9, 30, 0, 0, time.UTC)

	t.Run("returns the ES value when found", func(t *testing.T) {
		var gotID string
		r := &esParentResolver{
			timeout: time.Second,
			esRead: func(_ context.Context, id string) (time.Time, bool) {
				gotID = id
				return val, true
			},
		}
		got, ok := r.ResolveParentCreatedAt(context.Background(), "parent-1")
		assert.True(t, ok)
		assert.True(t, got.Equal(val))
		assert.Equal(t, "parent-1", gotID)
	})

	t.Run("ok=false when the parent isn't found", func(t *testing.T) {
		r := &esParentResolver{
			timeout: time.Second,
			esRead:  func(context.Context, string) (time.Time, bool) { return time.Time{}, false },
		}
		_, ok := r.ResolveParentCreatedAt(context.Background(), "missing")
		assert.False(t, ok)
	})
}
