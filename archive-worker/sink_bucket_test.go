package main

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeLock struct {
	lock string
	mode *minio.RetentionMode
	err  error
}

func (f fakeLock) GetObjectLockConfig(context.Context, string) (string, *minio.RetentionMode, *uint, *minio.ValidityUnit, error) {
	return f.lock, f.mode, nil, nil, f.err
}

func TestCheckObjectLock(t *testing.T) {
	compliance, governance := minio.Compliance, minio.Governance
	notFound := minio.ErrorResponse{Code: "ObjectLockConfigurationNotFoundError", StatusCode: 404}
	cases := []struct {
		name    string
		lock    fakeLock
		require bool
		wantErr error
	}{
		{"compliance passes", fakeLock{lock: "Enabled", mode: &compliance}, true, nil},
		{"governance fails", fakeLock{lock: "Enabled", mode: &governance}, true, errObjectLockRequired},
		{"no lock fails", fakeLock{lock: "", mode: nil}, true, errObjectLockRequired},
		{"no lock allowed when not required", fakeLock{lock: ""}, false, nil},
		{"lock config not found is absent and fails when required", fakeLock{err: notFound}, true, errObjectLockRequired},
		{"lock config not found is absent and warns when not required", fakeLock{err: notFound}, false, nil},
		{"lookup error fails even when not required", fakeLock{err: errors.New("403")}, false, errors.New("403")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkObjectLock(context.Background(), tc.lock, "b", tc.require)
			if tc.wantErr == nil {
				assert.NoError(t, err)
				return
			}
			assert.Error(t, err)
			if errors.Is(tc.wantErr, errObjectLockRequired) {
				assert.True(t, errors.Is(err, errObjectLockRequired))
			}
		})
	}
}

// fakeObjectStore implements the bucketClient calls bucketSink makes.
type fakeObjectStore struct {
	bucket, key, contentType string
	size                     int64
	body                     string
	err                      error
	statErr                  error
	getErr                   error
}

func (f *fakeObjectStore) StatObject(_ context.Context, bucket, key string, _ minio.StatObjectOptions) (minio.ObjectInfo, error) { //nolint:gocritic // hugeParam: signature fixed by minio-go
	f.bucket, f.key = bucket, key
	return minio.ObjectInfo{Key: key}, f.statErr
}

func (f *fakeObjectStore) GetObject(_ context.Context, bucket, key string, _ minio.GetObjectOptions) (*minio.Object, error) { //nolint:gocritic // hugeParam: signature fixed by minio-go
	f.bucket, f.key = bucket, key
	if f.getErr != nil {
		return nil, f.getErr
	}
	return &minio.Object{}, nil
}

func (f *fakeObjectStore) PutObject(_ context.Context, bucket, key string, r io.Reader, size int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) { //nolint:gocritic // hugeParam: signature fixed by minioutil.ObjectStore
	b, _ := io.ReadAll(r)
	f.bucket, f.key, f.size, f.contentType, f.body = bucket, key, size, opts.ContentType, string(b)
	return minio.UploadInfo{}, f.err
}

func TestBucketSink_Put(t *testing.T) {
	t.Run("forwards bucket, key, size and content type", func(t *testing.T) {
		fake := &fakeObjectStore{}
		sink := newBucketSink(fake, "archive-site-a")
		require.NoError(t, sink.Put(context.Background(), "site-a/seg-1", strings.NewReader("payload"), 7, "application/octet-stream"))
		assert.Equal(t, "archive-site-a", fake.bucket)
		assert.Equal(t, "site-a/seg-1", fake.key)
		assert.Equal(t, int64(7), fake.size)
		assert.Equal(t, "application/octet-stream", fake.contentType)
		assert.Equal(t, "payload", fake.body)
	})
	t.Run("wraps a failure with bucket and key", func(t *testing.T) {
		cause := errors.New("connection refused")
		sink := newBucketSink(&fakeObjectStore{err: cause}, "archive-site-a")
		err := sink.Put(context.Background(), "site-a/seg-1", strings.NewReader("x"), 1, "application/octet-stream")
		require.Error(t, err)
		assert.ErrorIs(t, err, cause)
		assert.Contains(t, err.Error(), "archive-site-a/site-a/seg-1")
	})
}

func TestBucketSink_Stat(t *testing.T) {
	tests := []struct {
		name    string
		statErr error
		want    bool
		wantErr bool
	}{
		{"present", nil, true, false},
		{"absent is not an error", minio.ErrorResponse{Code: "NoSuchKey", StatusCode: 404}, false, false},
		{"lookup failure is an error", errors.New("connection refused"), false, true},
		{"access denied is an error", minio.ErrorResponse{Code: "AccessDenied", StatusCode: 403}, false, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeObjectStore{statErr: tc.statErr}
			got, err := newBucketSink(fake, "archive-site-a").Stat(context.Background(), "site-a/keys/current.json")
			assert.Equal(t, tc.want, got)
			assert.Equal(t, "archive-site-a", fake.bucket)
			assert.Equal(t, "site-a/keys/current.json", fake.key)
			if tc.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tc.statErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestBucketSink_Get(t *testing.T) {
	t.Run("opens the object", func(t *testing.T) {
		fake := &fakeObjectStore{}
		rc, err := newBucketSink(fake, "archive-site-a").Get(context.Background(), "site-a/keys/current.json")
		require.NoError(t, err)
		assert.NotNil(t, rc)
		assert.Equal(t, "site-a/keys/current.json", fake.key)
	})
	t.Run("wraps a failure with bucket and key", func(t *testing.T) {
		cause := errors.New("connection refused")
		_, err := newBucketSink(&fakeObjectStore{getErr: cause}, "archive-site-a").Get(context.Background(), "k")
		require.Error(t, err)
		assert.ErrorIs(t, err, cause)
		assert.Contains(t, err.Error(), "archive-site-a/k")
	})
}
