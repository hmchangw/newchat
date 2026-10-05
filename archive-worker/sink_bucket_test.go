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

	"github.com/hmchangw/chat/pkg/minioutil"
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

// fakeObjectStore implements only the PutObject call bucketSink makes; the
// embedded nil interface panics if anything else is used.
type fakeObjectStore struct {
	minioutil.ObjectStore
	bucket, key, contentType string
	size                     int64
	body                     string
	err                      error
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
