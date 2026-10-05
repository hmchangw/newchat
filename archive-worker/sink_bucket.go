package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/minio/minio-go/v7"

	"github.com/hmchangw/chat/pkg/minioutil"
)

var errObjectLockRequired = errors.New("archive bucket is not in Object Lock compliance mode")

// lockConfigReader is the one minio-go call the startup check needs; the
// production value is a *minio.Client built for this check alone.
type lockConfigReader interface {
	GetObjectLockConfig(ctx context.Context, bucket string) (string, *minio.RetentionMode, *uint, *minio.ValidityUnit, error)
}

func checkObjectLock(ctx context.Context, r lockConfigReader, bucket string, require bool) error {
	lock, mode, _, _, err := r.GetObjectLockConfig(ctx, bucket)
	if err != nil && minio.ToErrorResponse(err).Code == "ObjectLockConfigurationNotFoundError" {
		// A bucket created without Object Lock answers 404 here; that is "lock
		// absent", not a failed lookup.
		lock, mode, err = "", nil, nil
	}
	if err != nil {
		return fmt.Errorf("read object lock config for %q: %w", bucket, err)
	}
	ok := lock == "Enabled" && mode != nil && *mode == minio.Compliance
	if ok {
		return nil
	}
	if !require {
		slog.Warn("archive bucket is not Object Lock compliance mode; continuing because ARCHIVE_REQUIRE_OBJECT_LOCK=false", "bucket", bucket, "lock", lock)
		return nil
	}
	return fmt.Errorf("%w: bucket %q lock=%q", errObjectLockRequired, bucket, lock)
}

// bucketSink must satisfy the worker's objectStore seam.
var _ objectStore = (*bucketSink)(nil)

type bucketSink struct {
	client minioutil.ObjectStore
	bucket string
}

func newBucketSink(client minioutil.ObjectStore, bucket string) *bucketSink {
	return &bucketSink{client: client, bucket: bucket}
}

// Put writes one object. Retention comes from the bucket's default rule, so
// no per-object lock options are set.
func (s *bucketSink) Put(ctx context.Context, key string, body io.Reader, size int64, contentType string) error {
	if _, err := s.client.PutObject(ctx, s.bucket, key, body, size, minio.PutObjectOptions{ContentType: contentType}); err != nil {
		return fmt.Errorf("put %s/%s: %w", s.bucket, key, err)
	}
	return nil
}
