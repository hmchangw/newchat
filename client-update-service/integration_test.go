//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/svcjwt"
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
	registerRoutes(r, h, func(c *gin.Context) { c.Next() })

	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/version/app.exe", nil))
		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "BINARY", w.Body.String())
	}
	assert.Equal(t, 1, cs.opens, "second download must be served from cache, not re-opened")
}

// authedRouter builds the real router with real auth against a real MinIO
// container, and returns it with a signer that mints tokens it will accept.
func authedRouter(t *testing.T) (*gin.Engine, *svcjwt.Signer) {
	t.Helper()
	client, bucket := testutil.MinIO(t, "clientupdateauth")
	store := newMinioVersionStore(client, bucket, 30*time.Second)
	h := NewHandler(store, newBlobCache(4, time.Hour, 1<<20))

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	enc := base64.StdEncoding
	signer, err := svcjwt.NewSigner(enc.EncodeToString(priv.Seed()), "admin-service")
	require.NoError(t, err)
	verifier, err := svcjwt.NewVerifier(enc.EncodeToString(pub), "admin-service", "client-update-service")
	require.NoError(t, err)

	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerRoutes(r, h, requireServiceAccount(verifier, []string{"svc-updater"}))
	return r, signer
}

// uploadForm builds the configFile + executeFile multipart body the upload expects.
func uploadForm(t *testing.T, cfgName, cfgBody, exeName, exeBody string) (*bytes.Buffer, string) {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	for _, f := range []struct{ field, name, body string }{
		{"configFile", cfgName, cfgBody},
		{"executeFile", exeName, exeBody},
	} {
		part, err := w.CreateFormFile(f.field, f.name)
		require.NoError(t, err)
		_, err = part.Write([]byte(f.body))
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	return &buf, w.FormDataContentType()
}

func TestIntegration_AuthedUploadThenOpenDownload(t *testing.T) {
	r, signer := authedRouter(t)
	token, _, err := signer.Sign("svc-updater", "client-update-service", time.Hour)
	require.NoError(t, err)

	body, contentType := uploadForm(t, "app.yaml", "version: 2", "app.exe", "MZbinary")
	req := httptest.NewRequest(http.MethodPost, "/api/v1/version", body)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())

	// The download must still work with NO credential — the whole point of
	// gating only the upload.
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/version/app.exe", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "MZbinary", w.Body.String())
}

func TestIntegration_UnauthenticatedUploadIsRefused(t *testing.T) {
	r, _ := authedRouter(t)

	body, contentType := uploadForm(t, "app.yaml", "version: 2", "app.exe", "MZbinary")
	req := httptest.NewRequest(http.MethodPost, "/api/v1/version", body)
	req.Header.Set("Content-Type", contentType)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusUnauthorized, w.Code)

	// And nothing was written: the artifact must not exist.
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/version/app.exe", nil))
	assert.Equal(t, http.StatusNotFound, w.Code,
		"a refused upload must not have reached MinIO")
}

func TestIntegration_UnallowlistedAccountIsRefused(t *testing.T) {
	r, signer := authedRouter(t)
	token, _, err := signer.Sign("svc-intruder", "client-update-service", time.Hour)
	require.NoError(t, err)

	body, contentType := uploadForm(t, "app.yaml", "version: 2", "app.exe", "MZbinary")
	req := httptest.NewRequest(http.MethodPost, "/api/v1/version", body)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
}
