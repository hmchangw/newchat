package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
)

// errBlobNotFound is returned by blobStore.Get when the object does not exist.
var errBlobNotFound = errors.New("blob not found")

type blobInfo struct {
	Size        int64
	ContentType string
	ETag        string
}

type blobStore interface {
	Get(ctx context.Context, key string) (io.ReadCloser, blobInfo, error)
	Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) (etag string, err error)
	Delete(ctx context.Context, key string) error
}

type minioBlobStore struct {
	client *minio.Client
	bucket string
}

func newMinioBlobStore(client *minio.Client, bucket string) *minioBlobStore {
	return &minioBlobStore{client: client, bucket: bucket}
}

func (m *minioBlobStore) Get(ctx context.Context, key string) (io.ReadCloser, blobInfo, error) {
	obj, err := m.client.GetObject(ctx, m.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, blobInfo{}, fmt.Errorf("get object: %w", err)
	}
	st, err := obj.Stat()
	if err != nil {
		_ = obj.Close()
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return nil, blobInfo{}, errBlobNotFound
		}
		return nil, blobInfo{}, fmt.Errorf("stat object: %w", err)
	}
	return obj, blobInfo{Size: st.Size, ContentType: st.ContentType, ETag: st.ETag}, nil
}

func (m *minioBlobStore) Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) (string, error) {
	info, err := m.client.PutObject(ctx, m.bucket, key, r, size, minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		return "", fmt.Errorf("put object: %w", err)
	}
	return info.ETag, nil
}

func (m *minioBlobStore) Delete(ctx context.Context, key string) error {
	if err := m.client.RemoveObject(ctx, m.bucket, key, minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("remove object: %w", err)
	}
	return nil
}
