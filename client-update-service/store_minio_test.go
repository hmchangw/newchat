package main

import (
	"context"
	"errors"
	"testing"

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

func TestIsNotFound(t *testing.T) {
	assert.True(t, isNotFound(minio.ErrorResponse{Code: "NoSuchKey"}))
	assert.False(t, isNotFound(minio.ErrorResponse{Code: "AccessDenied"}))
	assert.False(t, isNotFound(errors.New("plain")))
}
