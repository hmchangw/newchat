//go:build integration

package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// countingStore wraps a versionStore to count Open calls (proves cache reuse).
type countingStore struct {
	versionStore
	opens int
}

func (c *countingStore) Open(ctx context.Context, key string) (io.ReadCloser, blobInfo, error) {
	c.opens++
	return c.versionStore.Open(ctx, key)
}

func TestIntegration_StoreRoundTrip(t *testing.T) {
	client, bucket := testutil.MinIO(t, "clientupdate")
	store := newMinioVersionStore(client, bucket, 30*time.Second)
	ctx := context.Background()

	require.NoError(t, store.Put(ctx, objectKey("app.yaml"), strings.NewReader("version: 1"), 10, "application/x-yaml"))

	rc, info, err := store.Open(ctx, objectKey("app.yaml"))
	require.NoError(t, err)
	defer rc.Close()
	body, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, "version: 1", string(body))
	assert.Equal(t, int64(10), info.Size)
	assert.Equal(t, "application/x-yaml", info.ContentType)
}

func TestIntegration_OpenMissing_NotFound(t *testing.T) {
	client, bucket := testutil.MinIO(t, "clientupdate")
	store := newMinioVersionStore(client, bucket, 30*time.Second)
	_, _, err := store.Open(context.Background(), objectKey("nope.exe"))
	assert.ErrorIs(t, err, ErrObjectNotFound)
}

func TestIntegration_EnsureBucketCreatesAbsent(t *testing.T) {
	client, _ := testutil.MinIO(t, "clientupdate")
	name := "cus-fresh-" + strings.ToLower(idgen.GenerateID()) // unique, lowercase (bucket rules)
	ctx := context.Background()

	require.NoError(t, ensureBucket(ctx, client, name))
	exists, err := client.BucketExists(ctx, name)
	require.NoError(t, err)
	assert.True(t, exists)
	// Idempotent second call.
	require.NoError(t, ensureBucket(ctx, client, name))
	_ = client.RemoveBucket(ctx, name)
}

func TestIntegration_DownloadServesFromCacheOnSecondHit(t *testing.T) {
	client, bucket := testutil.MinIO(t, "clientupdate")
	base := newMinioVersionStore(client, bucket, 30*time.Second)
	require.NoError(t, base.Put(context.Background(), objectKey("app.exe"), strings.NewReader("BINARY"), 6, "application/octet-stream"))

	cs := &countingStore{versionStore: base}
	h := NewHandler(cs, newBlobCache(4, time.Hour, 1024))
	r := gin.New()
	registerRoutes(r, h)

	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/version/app.exe", nil))
		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "BINARY", w.Body.String())
	}
	assert.Equal(t, 1, cs.opens, "second download must be served from cache, not re-opened")
}
