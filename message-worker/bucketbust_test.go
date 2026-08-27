package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/bucketcache"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// spyValkey records the keys DEL'd so a test can assert which bucket was busted
// without a Valkey instance.
type spyValkey struct {
	deleted []string
	delErr  error
}

func (s *spyValkey) Get(context.Context, string) (string, error) {
	return "", valkeyutil.ErrCacheMiss
}
func (s *spyValkey) Set(context.Context, string, string, time.Duration) error { return nil }
func (s *spyValkey) SetNX(context.Context, string, string, time.Duration) (bool, error) {
	return true, nil
}
func (s *spyValkey) IncrEx(context.Context, string, time.Duration) (int64, error) { return 0, nil }
func (s *spyValkey) Del(_ context.Context, keys ...string) error {
	s.deleted = append(s.deleted, keys...)
	return s.delErr
}
func (s *spyValkey) Close() error { return nil }

const bustWindow = 360 * time.Hour

// TestCassandraStore_BustParentBucket pins which bucket a thread-parent rewrite
// invalidates: the PARENT's bucket, derived from the parent's created_at, not
// the reply's. A thread reply is almost always far newer than the message it
// answers, so busting the reply's bucket would clear a bucket that did not
// change and leave the one that did.
func TestCassandraStore_BustParentBucket(t *testing.T) {
	sizer := msgbucket.New(bustWindow)
	parentAt := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	t.Run("busts the parent's bucket", func(t *testing.T) {
		spy := &spyValkey{}
		s := NewCassandraStore(nil, sizer, nil, WithBucketCacheInvalidator(bucketcache.NewInvalidator(spy)))

		s.bustParentBucket(context.Background(), "r1", parentAt)

		require.Len(t, spy.deleted, 1)
		assert.Equal(t, bucketcache.Key("r1", sizer.Of(parentAt)), spy.deleted[0])
	})

	t.Run("no invalidator configured is a no-op", func(t *testing.T) {
		s := NewCassandraStore(nil, sizer, nil)
		assert.NotPanics(t, func() {
			s.bustParentBucket(context.Background(), "r1", parentAt)
		})
	})

	t.Run("a nil client is a no-op", func(t *testing.T) {
		s := NewCassandraStore(nil, sizer, nil, WithBucketCacheInvalidator(bucketcache.NewInvalidator(nil)))
		assert.NotPanics(t, func() {
			s.bustParentBucket(context.Background(), "r1", parentAt)
		})
	})

	// The Cassandra write has already committed by the time this runs, so a
	// Valkey failure must not surface: returning an error here would fail the
	// handler and trigger a redelivery that rewrites an already-correct row.
	t.Run("a Valkey error is swallowed", func(t *testing.T) {
		spy := &spyValkey{delErr: assert.AnError}
		s := NewCassandraStore(nil, sizer, nil, WithBucketCacheInvalidator(bucketcache.NewInvalidator(spy)))
		assert.NotPanics(t, func() {
			s.bustParentBucket(context.Background(), "r1", parentAt)
		})
	})
}
