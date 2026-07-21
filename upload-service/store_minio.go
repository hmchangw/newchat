package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/hmchangw/chat/pkg/minioutil"
)

// minioObjectStore streams objects out of a single MinIO/S3 bucket.
type minioObjectStore struct {
	client          minioutil.ObjectStore
	bucket          string
	downloadTimeout time.Duration
}

// newMinioObjectStore binds a minio client to a bucket. downloadTimeout bounds a
// single download (Stat probe + streamed body) so a hung backend can't hang the request.
func newMinioObjectStore(client minioutil.ObjectStore, bucket string, downloadTimeout time.Duration) *minioObjectStore {
	return &minioObjectStore{client: client, bucket: bucket, downloadTimeout: downloadTimeout}
}

// Open returns a streaming reader for the object at key, Stat-probing first so a missing
// object or dead backend surfaces before any body is written. minio-go ties Reads to the
// GetObject context, so cancel must outlive Open — the reader releases it on Close.
func (s *minioObjectStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	tctx, cancel := context.WithTimeout(ctx, s.downloadTimeout)
	obj, err := s.client.GetObject(tctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("get object %s/%s: %w", s.bucket, key, err)
	}
	// GetObject is lazy; the request only fires on Stat/Read, so probe now.
	if _, err := obj.Stat(); err != nil {
		_ = obj.Close()
		cancel()
		return nil, fmt.Errorf("stat object %s/%s: %w", s.bucket, key, err)
	}
	return &cancelReadCloser{ReadCloser: obj, cancel: cancel}, nil
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
