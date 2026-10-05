//go:build integration

package main

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

// lockedBucket makes a per-test bucket with Object Lock and a 1-day
// compliance default. testutil.MinIO creates unlocked buckets, so this test
// owns its own; the bucket cannot be deleted while objects are retained,
// so cleanup is intentionally skipped (the container is discarded).
func lockedBucket(t *testing.T) (*minio.Client, string) {
	t.Helper()
	endpoint, ak, sk := testutil.MinIOEndpoint(t)
	c, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(ak, sk, ""), Secure: false})
	require.NoError(t, err)
	h := fnv.New64a()
	_, _ = h.Write([]byte(t.Name()))
	bucket := fmt.Sprintf("archive-%x", h.Sum64())
	ctx := context.Background()
	require.NoError(t, c.MakeBucket(ctx, bucket, minio.MakeBucketOptions{ObjectLocking: true}))
	mode, validity, unit := minio.Compliance, uint(1), minio.Days
	require.NoError(t, c.SetObjectLockConfig(ctx, bucket, &mode, &validity, &unit))
	return c, bucket
}

func TestBucketSink_LockedBucket(t *testing.T) {
	ctx := context.Background()
	c, bucket := lockedBucket(t)
	require.NoError(t, checkObjectLock(ctx, c, bucket, true))

	sink := newBucketSink(c, bucket)
	body := []byte("sealed segment bytes")
	require.NoError(t, sink.Put(ctx, "site-a/2026/10/05/14/events-1-3.seg", bytes.NewReader(body), int64(len(body)), "application/octet-stream"))

	obj, err := c.GetObject(ctx, bucket, "site-a/2026/10/05/14/events-1-3.seg", minio.GetObjectOptions{})
	require.NoError(t, err)
	got, err := io.ReadAll(obj)
	require.NoError(t, err)
	assert.Equal(t, body, got)

	t.Run("retained object cannot be deleted", func(t *testing.T) {
		err := c.RemoveObject(ctx, bucket, "site-a/2026/10/05/14/events-1-3.seg", minio.RemoveObjectOptions{})
		assert.Error(t, err, "compliance retention must refuse the delete")
	})
	t.Run("unlocked bucket is refused", func(t *testing.T) {
		plain, unlocked := testutil.MinIO(t, "archive-unlocked")
		err := checkObjectLock(ctx, plain, unlocked, true)
		assert.ErrorIs(t, err, errObjectLockRequired)
	})
}
