package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/hmchangw/chat/pkg/minioutil"
)

// minioVersionStore streams version artifacts in and out of a single MinIO bucket.
type minioVersionStore struct {
	client          minioutil.ObjectStore
	bucket          string
	downloadTimeout time.Duration
}

// newMinioVersionStore binds a minio client to a bucket. downloadTimeout bounds a
// single Open (Stat probe + streamed body) so a hung backend cannot hang forever.
func newMinioVersionStore(client minioutil.ObjectStore, bucket string, downloadTimeout time.Duration) *minioVersionStore {
	return &minioVersionStore{client: client, bucket: bucket, downloadTimeout: downloadTimeout}
}

func (s *minioVersionStore) Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error {
	if _, err := s.client.PutObject(ctx, s.bucket, key, r, size, minio.PutObjectOptions{ContentType: contentType}); err != nil {
		return fmt.Errorf("put object %s/%s: %w", s.bucket, key, err)
	}
	return nil
}

// Open Stat-probes first so a missing object or dead backend surfaces before any body.
// Reads are tied to the GetObject context, so cancelReadCloser releases it on Close.
func (s *minioVersionStore) Open(ctx context.Context, key string) (io.ReadCloser, blobInfo, error) {
	tctx, cancel := context.WithTimeout(ctx, s.downloadTimeout)
	obj, err := s.client.GetObject(tctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		cancel()
		return nil, blobInfo{}, fmt.Errorf("get object %s/%s: %w", s.bucket, key, err)
	}
	info, err := obj.Stat()
	if err != nil {
		_ = obj.Close()
		cancel()
		if isNotFound(err) {
			return nil, blobInfo{}, fmt.Errorf("stat object %s/%s: %w", s.bucket, key, ErrObjectNotFound)
		}
		return nil, blobInfo{}, fmt.Errorf("stat object %s/%s: %w", s.bucket, key, err)
	}
	return &cancelReadCloser{ReadCloser: obj, cancel: cancel}, blobInfo{Size: info.Size, ContentType: info.ContentType}, nil
}

// isNotFound reports whether err is MinIO's NoSuchKey (missing object).
func isNotFound(err error) bool {
	return minio.ToErrorResponse(err).Code == "NoSuchKey"
}

// bucketClient is the bucket-management surface not exposed by minioutil.ObjectStore.
// Both *minio.Client and *o11yminio.Client satisfy it.
type bucketClient interface {
	BucketExists(ctx context.Context, name string) (bool, error)
	MakeBucket(ctx context.Context, name string, opts minio.MakeBucketOptions) error
}

// ensureBucket creates the bucket when absent; idempotent and race-safe (a concurrent
// create surfacing as BucketAlreadyOwnedByYou/BucketAlreadyExists is treated as success).
func ensureBucket(ctx context.Context, client bucketClient, name string) error {
	exists, err := client.BucketExists(ctx, name)
	if err != nil {
		return fmt.Errorf("bucket exists check %q: %w", name, err)
	}
	if exists {
		return nil
	}
	if err := client.MakeBucket(ctx, name, minio.MakeBucketOptions{}); err != nil {
		switch minio.ToErrorResponse(err).Code {
		case "BucketAlreadyOwnedByYou", "BucketAlreadyExists":
			return nil
		}
		return fmt.Errorf("make bucket %q: %w", name, err)
	}
	return nil
}

// cancelReadCloser cancels the download's timeout context when the reader is closed.
type cancelReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelReadCloser) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}
