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
	// Read exactly info.Size bytes (bounded-read contract), not io.ReadAll.
	body := make([]byte, info.Size)
	_, err = io.ReadFull(rc, body)
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
	t.Cleanup(func() {
		assert.NoError(t, client.RemoveBucket(context.Background(), name))
	})
	exists, err := client.BucketExists(ctx, name)
	require.NoError(t, err)
	assert.True(t, exists)
	// Idempotent second call.
	require.NoError(t, ensureBucket(ctx, client, name))
}

func TestIntegration_DownloadServesFromCacheOnSecondHit(t *testing.T) {
	client, bucket := testutil.MinIO(t, "clientupdate")
	base := newMinioVersionStore(client, bucket, 30*time.Second)
	require.NoError(t, base.Put(context.Background(), objectKey("app.exe"), strings.NewReader("BINARY"), 6, "application/octet-stream"))

	cs := &countingStore{versionStore: base}
	h := NewHandler(cs, newBlobCache(4, time.Hour, 1024))
	r := gin.New()
	registerRoutes(r, h, testTokens())

	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/version/app.exe", nil))
		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "BINARY", w.Body.String())
	}
	assert.Equal(t, 1, cs.opens, "second download must be served from cache, not re-opened")
}

// A full round-trip through the real router and a real MinIO: the credential
// gates the write, and the artifact that lands is byte-identical.
func TestIntegration_UploadRequiresServiceAccountThenRoundTrips(t *testing.T) {
	client, bucket := testutil.MinIO(t, "clientupdate")
	store := newMinioVersionStore(client, bucket, 30*time.Second)
	h := NewHandler(store, newBlobCache(4, time.Hour, 1<<20))

	tokens := map[string]string{"admin-service": "0123456789abcdef"}
	r := gin.New()
	registerRoutes(r, h, tokens)

	newUpload := func() (*http.Request, error) {
		body, ct := multipartBody(t, map[string]fileSpec{
			"configFile":  {name: "itest.yaml", content: "version: 1"},
			"executeFile": {name: "itest.bin", content: "MZbinarypayload"},
		})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/version", body)
		req.Header.Set("Content-Type", ct)
		return req, nil
	}

	// Unauthenticated: rejected, and nothing is stored.
	req, err := newUpload()
	require.NoError(t, err)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusUnauthorized, w.Code)

	getW := httptest.NewRecorder()
	r.ServeHTTP(getW, httptest.NewRequest(http.MethodGet, "/api/v1/version/itest.yaml", nil))
	require.Equal(t, http.StatusNotFound, getW.Code,
		"a rejected upload must not have written anything to MinIO")

	// Authenticated: stored, and downloadable without a credential.
	req, err = newUpload()
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer 0123456789abcdef")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	getW = httptest.NewRecorder()
	r.ServeHTTP(getW, httptest.NewRequest(http.MethodGet, "/api/v1/version/itest.bin", nil))
	require.Equal(t, http.StatusOK, getW.Code)
	assert.Equal(t, "MZbinarypayload", getW.Body.String())
}
