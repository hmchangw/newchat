package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/minio/minio-go/v7"
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

// bucketClient is the minio-go surface the sink uses. Both *minio.Client and
// the traced *o11yminio.Client that minioutil.Connect returns satisfy it;
// minioutil.ObjectStore does not, because it has no StatObject.
type bucketClient interface {
	PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error)
	GetObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (*minio.Object, error)
	StatObject(ctx context.Context, bucketName, objectName string, opts minio.StatObjectOptions) (minio.ObjectInfo, error)
}

type bucketSink struct {
	client bucketClient
	bucket string
}

func newBucketSink(client bucketClient, bucket string) *bucketSink {
	return &bucketSink{client: client, bucket: bucket}
}

// Stat reports whether key exists. NoSuchKey is "absent", not a failure.
func (s *bucketSink) Stat(ctx context.Context, key string) (bool, error) {
	_, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}
	if minio.ToErrorResponse(err).Code == "NoSuchKey" {
		return false, nil
	}
	return false, fmt.Errorf("stat %s/%s: %w", s.bucket, key, err)
}

// Get opens key for reading. minio-go fetches lazily, so a missing object
// surfaces on the first Read rather than here.
func (s *bucketSink) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get %s/%s: %w", s.bucket, key, err)
	}
	return obj, nil
}

// Put writes one object. Retention comes from the bucket's default rule, so
// no per-object lock options are set.
func (s *bucketSink) Put(ctx context.Context, key string, body io.Reader, size int64, contentType string) error {
	if _, err := s.client.PutObject(ctx, s.bucket, key, body, size, minio.PutObjectOptions{ContentType: contentType}); err != nil {
		return fmt.Errorf("put %s/%s: %w", s.bucket, key, err)
	}
	return nil
}
