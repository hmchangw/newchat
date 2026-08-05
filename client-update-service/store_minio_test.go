package main

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeBucketClient stands in for a MinIO client's bucket-management surface.
type fakeBucketClient struct {
	exists     bool
	existsErr  error
	makeErr    error
	madeBucket string
}

func (f *fakeBucketClient) BucketExists(_ context.Context, _ string) (bool, error) {
	return f.exists, f.existsErr
}

func (f *fakeBucketClient) MakeBucket(_ context.Context, name string, _ minio.MakeBucketOptions) error {
	f.madeBucket = name
	return f.makeErr
}

func TestEnsureBucket_ExistsNoCreate(t *testing.T) {
	f := &fakeBucketClient{exists: true}
	require.NoError(t, ensureBucket(context.Background(), f, "b"))
	assert.Equal(t, "", f.madeBucket, "must not create an existing bucket")
}

func TestEnsureBucket_AbsentCreates(t *testing.T) {
	f := &fakeBucketClient{exists: false}
	require.NoError(t, ensureBucket(context.Background(), f, "b"))
	assert.Equal(t, "b", f.madeBucket)
}

func TestEnsureBucket_RacyCreateTreatedSuccess(t *testing.T) {
	f := &fakeBucketClient{exists: false, makeErr: minio.ErrorResponse{Code: "BucketAlreadyOwnedByYou"}}
	require.NoError(t, ensureBucket(context.Background(), f, "b"))
}

func TestEnsureBucket_ExistsCheckError(t *testing.T) {
	f := &fakeBucketClient{existsErr: errors.New("net")}
	assert.Error(t, ensureBucket(context.Background(), f, "b"))
}

func TestEnsureBucket_MakeError(t *testing.T) {
	f := &fakeBucketClient{exists: false, makeErr: minio.ErrorResponse{Code: "AccessDenied"}}
	assert.Error(t, ensureBucket(context.Background(), f, "b"))
}

// fakeObjectStore is a minimal minioutil.ObjectStore for unit-testing Put.
type fakeObjectStore struct {
	putBucket string
	putKey    string
	putSize   int64
	putCT     string
	putErr    error
	getErr    error
}

func (f *fakeObjectStore) BucketExists(context.Context, string) (bool, error) { return true, nil }

func (f *fakeObjectStore) PutObject(_ context.Context, bucket, key string, _ io.Reader, size int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) { //nolint:gocritic // hugeParam: opts is passed by value to satisfy the minioutil.ObjectStore interface
	f.putBucket, f.putKey, f.putSize, f.putCT = bucket, key, size, opts.ContentType
	return minio.UploadInfo{}, f.putErr
}

func (f *fakeObjectStore) GetObject(context.Context, string, string, minio.GetObjectOptions) (*minio.Object, error) {
	return nil, f.getErr
}

func (f *fakeObjectStore) ListObjects(context.Context, string, minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	ch := make(chan minio.ObjectInfo)
	close(ch)
	return ch
}

func (f *fakeObjectStore) RemoveObject(context.Context, string, string, minio.RemoveObjectOptions) error {
	return nil
}

func TestMinioVersionStore_Put(t *testing.T) {
	f := &fakeObjectStore{}
	s := newMinioVersionStore(f, "bkt", time.Second)
	err := s.Put(context.Background(), "chat.go/chat-versions/app.yaml", strings.NewReader("cfg"), 3, "application/x-yaml")
	require.NoError(t, err)
	assert.Equal(t, "bkt", f.putBucket)
	assert.Equal(t, "chat.go/chat-versions/app.yaml", f.putKey)
	assert.Equal(t, int64(3), f.putSize)
	assert.Equal(t, "application/x-yaml", f.putCT)
}

func TestMinioVersionStore_Put_WrapsError(t *testing.T) {
	f := &fakeObjectStore{putErr: errors.New("net")}
	s := newMinioVersionStore(f, "bkt", time.Second)
	err := s.Put(context.Background(), "k", strings.NewReader("x"), 1, "application/octet-stream")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "put object bkt/k")
}

// TestMinioVersionStore_Open_GetObjectError covers the branch where GetObject
// fails outright (e.g. invalid bucket/object args); the Stat-based branches
// (success and NoSuchKey->ErrObjectNotFound) need a real *minio.Object and are
// covered by the integration tests.
func TestMinioVersionStore_Open_GetObjectError(t *testing.T) {
	f := &fakeObjectStore{getErr: errors.New("invalid object name")}
	s := newMinioVersionStore(f, "bkt", time.Second)
	rc, info, err := s.Open(context.Background(), "k")
	require.Error(t, err)
	assert.Nil(t, rc)
	assert.Equal(t, blobInfo{}, info)
	assert.Contains(t, err.Error(), "get object bkt/k")
}

func TestIsNotFound(t *testing.T) {
	assert.True(t, isNotFound(minio.ErrorResponse{Code: "NoSuchKey"}))
	assert.False(t, isNotFound(minio.ErrorResponse{Code: "AccessDenied"}))
	assert.False(t, isNotFound(errors.New("plain")))
}
