package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestHandleUpload_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	store.EXPECT().Put(gomock.Any(), objectKey("app.yaml"), gomock.Any(), int64(6), "application/x-yaml").Return(nil)
	store.EXPECT().Put(gomock.Any(), objectKey("app.exe"), gomock.Any(), int64(4), "application/octet-stream").Return(nil)
	h := NewHandler(store, testCache(1024))

	body, ct := multipartBody(t, map[string]fileSpec{
		"configFile":  {name: "app.yaml", content: "config"},
		"executeFile": {name: "app.exe", content: "bin!"},
	})
	c, w := uploadCtx(t, body, ct)
	h.HandleUpload(c)

	require.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, `{"result":"success"}`, w.Body.String())
}

func TestHandleUpload_UsesPartContentType(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	// The part's declared Content-Type must win over the fallback.
	store.EXPECT().Put(gomock.Any(), objectKey("app.yaml"), gomock.Any(), int64(6), "text/yaml").Return(nil)
	store.EXPECT().Put(gomock.Any(), objectKey("app.exe"), gomock.Any(), int64(4), "application/x-msdownload").Return(nil)
	h := NewHandler(store, testCache(1024))

	body, ct := multipartBody(t, map[string]fileSpec{
		"configFile":  {name: "app.yaml", content: "config", contentType: "text/yaml"},
		"executeFile": {name: "app.exe", content: "bin!", contentType: "application/x-msdownload"},
	})
	c, w := uploadCtx(t, body, ct)
	h.HandleUpload(c)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHandleUpload_FallbackContentTypeWhenPartHeaderAbsent(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	// No Content-Type on either part -> fallbacks apply.
	store.EXPECT().Put(gomock.Any(), objectKey("app.yaml"), gomock.Any(), int64(1), "application/x-yaml").Return(nil)
	store.EXPECT().Put(gomock.Any(), objectKey("app.exe"), gomock.Any(), int64(3), "application/octet-stream").Return(nil)
	h := NewHandler(store, testCache(1024))

	body, ct := multipartBody(t, map[string]fileSpec{
		"configFile":  {name: "app.yaml", content: "c"},
		"executeFile": {name: "app.exe", content: "bin"},
	})
	c, w := uploadCtx(t, body, ct)
	h.HandleUpload(c)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHandleUpload_InvalidName_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := NewHandler(NewMockversionStore(ctrl), testCache(1024))
	// "e..vil.yaml" survives multipart's filepath.Base but fails validFileName ("..").
	body, ct := multipartBody(t, map[string]fileSpec{
		"configFile":  {name: "e..vil.yaml", content: "c"},
		"executeFile": {name: "app.exe", content: "bin"},
	})
	c, w := uploadCtx(t, body, ct)
	h.HandleUpload(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleUpload_EqualNames_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := NewHandler(NewMockversionStore(ctrl), testCache(1024))
	body, ct := multipartBody(t, map[string]fileSpec{
		"configFile":  {name: "same.yaml", content: "c"},
		"executeFile": {name: "same.yaml", content: "bin"},
	})
	c, w := uploadCtx(t, body, ct)
	h.HandleUpload(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleUpload_MissingConfigFile_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := NewHandler(NewMockversionStore(ctrl), testCache(1024))
	body, ct := multipartBody(t, map[string]fileSpec{"executeFile": {name: "app.exe", content: "bin"}})
	c, w := uploadCtx(t, body, ct)
	h.HandleUpload(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleUpload_MissingExecuteFile_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := NewHandler(NewMockversionStore(ctrl), testCache(1024))
	body, ct := multipartBody(t, map[string]fileSpec{"configFile": {name: "app.yaml", content: "c"}})
	c, w := uploadCtx(t, body, ct)
	h.HandleUpload(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleUpload_EmptyFile_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := NewHandler(NewMockversionStore(ctrl), testCache(1024))
	body, ct := multipartBody(t, map[string]fileSpec{
		"configFile":  {name: "app.yaml", content: ""},
		"executeFile": {name: "app.exe", content: "bin"},
	})
	c, w := uploadCtx(t, body, ct)
	h.HandleUpload(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleUpload_WrongConfigExt_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := NewHandler(NewMockversionStore(ctrl), testCache(1024))
	body, ct := multipartBody(t, map[string]fileSpec{
		"configFile":  {name: "app.txt", content: "c"},
		"executeFile": {name: "app.exe", content: "bin"},
	})
	c, w := uploadCtx(t, body, ct)
	h.HandleUpload(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleUpload_MalformedMultipart_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := NewHandler(NewMockversionStore(ctrl), testCache(1024))
	c, w := uploadCtx(t, bytesBufferString("not multipart"), "multipart/form-data; boundary=nope")
	h.HandleUpload(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleUpload_StoreError_500(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	store.EXPECT().Put(gomock.Any(), objectKey("app.yaml"), gomock.Any(), int64(1), "application/x-yaml").
		Return(errors.New("minio down"))
	h := NewHandler(store, testCache(1024))
	body, ct := multipartBody(t, map[string]fileSpec{
		"configFile":  {name: "app.yaml", content: "c"},
		"executeFile": {name: "app.exe", content: "bin"},
	})
	c, w := uploadCtx(t, body, ct)
	h.HandleUpload(c)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleUpload_OverwriteEvictsCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	store.EXPECT().Put(gomock.Any(), objectKey("app.yaml"), gomock.Any(), gomock.Any(), "application/x-yaml").Return(nil)
	store.EXPECT().Put(gomock.Any(), objectKey("app.exe"), gomock.Any(), gomock.Any(), "application/octet-stream").Return(nil)
	cache := testCache(1024)
	cache.add(objectKey("app.yaml"), cachedBlob{body: []byte("stale"), contentType: "application/x-yaml"})
	h := NewHandler(store, cache)

	body, ct := multipartBody(t, map[string]fileSpec{
		"configFile":  {name: "app.yaml", content: "new"},
		"executeFile": {name: "app.exe", content: "bin"},
	})
	c, w := uploadCtx(t, body, ct)
	h.HandleUpload(c)
	require.Equal(t, http.StatusOK, w.Code)
	_, ok := cache.get(objectKey("app.yaml"))
	assert.False(t, ok, "overwrite must evict the stale cached copy")
}

func TestHandleDownload_CacheHit_NoStoreCall(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl) // no EXPECT: any store call fails the test
	cache := testCache(1024)
	cache.add(objectKey("app.exe"), cachedBlob{body: []byte("BIN"), contentType: "application/octet-stream"})
	h := NewHandler(store, cache)

	c, w := downloadCtx(t, "app.exe")
	h.HandleDownload(c)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "BIN", w.Body.String())
	assert.Equal(t, "application/octet-stream", w.Header().Get("Content-Type"))
	assert.Equal(t, "3", w.Header().Get("Content-Length"), "cache path must set Content-Length")
	assert.Contains(t, w.Header().Get("Content-Disposition"), `filename="app.exe"`)
}

func TestHandleDownload_MissCacheable_CachesAndServes(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	store.EXPECT().Open(gomock.Any(), objectKey("app.yaml")).
		Return(rc("hello"), blobInfo{Size: 5, ContentType: "application/x-yaml"}, nil).Times(1)
	cache := testCache(1024)
	h := NewHandler(store, cache)

	c, w := downloadCtx(t, "app.yaml")
	h.HandleDownload(c)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "hello", w.Body.String())
	assert.Equal(t, "application/x-yaml", w.Header().Get("Content-Type"))
	assert.Equal(t, "5", w.Header().Get("Content-Length"))

	// Second request must be served from cache — no further Open (Times(1) above enforces it).
	c2, w2 := downloadCtx(t, "app.yaml")
	h.HandleDownload(c2)
	require.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, "hello", w2.Body.String())
}

func TestHandleDownload_MissTooLarge_StreamsUncached(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	// cap = 3; object is 7 bytes -> non-cacheable -> loader Open + stream Open = 2 opens.
	store.EXPECT().Open(gomock.Any(), objectKey("big.exe")).
		DoAndReturn(func(_ context.Context, _ string) (io.ReadCloser, blobInfo, error) {
			return rc("BIGDATA"), blobInfo{Size: 7, ContentType: "application/octet-stream"}, nil
		}).Times(2)
	cache := testCache(3)
	h := NewHandler(store, cache)

	c, w := downloadCtx(t, "big.exe")
	h.HandleDownload(c)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "BIGDATA", w.Body.String())
	assert.Equal(t, "application/octet-stream", w.Header().Get("Content-Type"), "stream path must set Content-Type")
	assert.Equal(t, "7", w.Header().Get("Content-Length"), "stream path must set Content-Length")
	assert.Contains(t, w.Header().Get("Content-Disposition"), `filename="big.exe"`)
	_, ok := cache.get(objectKey("big.exe"))
	assert.False(t, ok, "oversized object must not be cached")
}

func TestHandleDownload_DisabledCache_StreamsSingleOpen(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	// Disabled cache must stream with exactly one Open — no probe+re-open, no buffering.
	store.EXPECT().Open(gomock.Any(), objectKey("app.yaml")).
		Return(rc("hello"), blobInfo{Size: 5, ContentType: "application/x-yaml"}, nil).Times(1)
	h := NewHandler(store, newBlobCache(0, time.Hour, 1024)) // disabled
	c, w := downloadCtx(t, "app.yaml")
	h.HandleDownload(c)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "hello", w.Body.String())
	assert.Equal(t, "application/x-yaml", w.Header().Get("Content-Type"))
	assert.Equal(t, "5", w.Header().Get("Content-Length"))
}

func TestHandleDownload_NotFound_404(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	store.EXPECT().Open(gomock.Any(), objectKey("missing.exe")).
		Return(nil, blobInfo{}, fmt.Errorf("stat object: %w", ErrObjectNotFound))
	h := NewHandler(store, testCache(1024))
	c, w := downloadCtx(t, "missing.exe")
	h.HandleDownload(c)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleDownload_StoreError_500(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	store.EXPECT().Open(gomock.Any(), objectKey("app.exe")).Return(nil, blobInfo{}, errors.New("minio down"))
	h := NewHandler(store, testCache(1024))
	c, w := downloadCtx(t, "app.exe")
	h.HandleDownload(c)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleDownload_TooLarge_StreamOpenNotFound_404(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	gomock.InOrder(
		// loader Open: oversized (7 > cap 3) -> non-cacheable
		store.EXPECT().Open(gomock.Any(), objectKey("big.exe")).
			Return(rc("BIGDATA"), blobInfo{Size: 7, ContentType: "application/octet-stream"}, nil),
		// stream Open: object vanished between the size probe and the stream
		store.EXPECT().Open(gomock.Any(), objectKey("big.exe")).
			Return(nil, blobInfo{}, fmt.Errorf("stat object: %w", ErrObjectNotFound)),
	)
	h := NewHandler(store, testCache(3))
	c, w := downloadCtx(t, "big.exe")
	h.HandleDownload(c)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleDownload_TooLarge_StreamOpenError_500(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	gomock.InOrder(
		store.EXPECT().Open(gomock.Any(), objectKey("big.exe")).
			Return(rc("BIGDATA"), blobInfo{Size: 7, ContentType: "application/octet-stream"}, nil),
		store.EXPECT().Open(gomock.Any(), objectKey("big.exe")).
			Return(nil, blobInfo{}, errors.New("minio down")),
	)
	h := NewHandler(store, testCache(3))
	c, w := downloadCtx(t, "big.exe")
	h.HandleDownload(c)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleDownload_CacheableReadError_500(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	// Size claims 5 bytes but the reader yields fewer -> io.ReadFull errors.
	store.EXPECT().Open(gomock.Any(), objectKey("short.yaml")).
		Return(rc("hi"), blobInfo{Size: 5, ContentType: "application/x-yaml"}, nil)
	h := NewHandler(store, testCache(1024))
	c, w := downloadCtx(t, "short.yaml")
	h.HandleDownload(c)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleDownload_InvalidName_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := NewHandler(NewMockversionStore(ctrl), testCache(1024))
	for _, name := range []string{"", "..", "a/b", "a..b"} {
		c, w := downloadCtx(t, name)
		h.HandleDownload(c)
		assert.Equal(t, http.StatusBadRequest, w.Code, "name %q must be rejected", name)
	}
}

// zeroReader yields an endless run of NUL bytes without allocating, so a large
// request body can be generated on the fly instead of held on the test's heap.
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

// streamingUploadBody builds a two-part upload whose executable is generated as
// it is read, so constructing it costs no heap.
func streamingUploadBody(t *testing.T, exeSize int64) (io.Reader, string) {
	t.Helper()
	const boundary = "clientupdatestreamboundary"
	var head bytes.Buffer
	mw := multipart.NewWriter(&head)
	require.NoError(t, mw.SetBoundary(boundary))

	fw, err := mw.CreateFormFile("configFile", "app.yaml")
	require.NoError(t, err)
	_, err = fw.Write([]byte("version: 1"))
	require.NoError(t, err)

	_, err = mw.CreateFormFile("executeFile", "app.exe")
	require.NoError(t, err)
	// The executable's bytes are appended by the reader chain below, not here.

	var tail bytes.Buffer
	tailWriter := multipart.NewWriter(&tail)
	require.NoError(t, tailWriter.SetBoundary(boundary))
	require.NoError(t, tailWriter.Close())

	return io.MultiReader(
		bytes.NewReader(head.Bytes()),
		io.LimitReader(zeroReader{}, exeSize),
		bytes.NewReader([]byte("\r\n")),
		bytes.NewReader(tail.Bytes()),
	), mw.FormDataContentType()
}

// A large artifact must cost disk, not heap. Gin's default MaxMultipartMemory is
// 32 MiB, which this service would otherwise retain per concurrent upload for as
// long as the MinIO Put runs.
func TestHandleUpload_DoesNotBufferTheArtifactInMemory(t *testing.T) {
	const artifactSize = 48 << 20
	const maxPeak = 24 << 20

	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	var stored int64
	store.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, r io.Reader, _ int64, _ string) error {
			n, err := io.Copy(io.Discard, r)
			stored += n
			return err
		}).Times(2)

	r := gin.New()
	r.MaxMultipartMemory = maxMultipartMemory
	registerRoutes(r, NewHandler(store, testCache(1024)), testTokens(), 1<<30)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)

	body, ct := streamingUploadBody(t, artifactSize)
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/version", body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", ct)
	req.Header.Set("Authorization", "Bearer 0123456789abcdef")

	var status int
	peak := testutil.PeakHeapDuring(func() {
		resp, doErr := srv.Client().Do(req)
		require.NoError(t, doErr)
		defer func() { _ = resp.Body.Close() }()
		_, _ = io.Copy(io.Discard, resp.Body)
		status = resp.StatusCode
	})

	require.Equal(t, http.StatusOK, status)
	require.Greater(t, stored, int64(artifactSize), "the artifact must reach the store whole")
	require.LessOrEqualf(t, peak, uint64(maxPeak),
		"peak heap %d MiB while storing a %d MiB artifact: the body is being buffered",
		peak>>20, artifactSize>>20)
	t.Logf("stored %d MiB with a %d MiB peak heap", artifactSize>>20, peak>>20)
}

// An oversize body must be refused before it is spooled, and the refusal must
// name the limit — otherwise the generic "configFile is required" sends an
// operator hunting a malformed form when the real cause was the cap.
func TestHandleUpload_RejectsAnOversizeBody(t *testing.T) {
	const cap = 512

	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	// No EXPECT(): gomock fails if Put runs, which is the assertion — an
	// over-cap upload must never reach MinIO.

	r := gin.New()
	r.MaxMultipartMemory = maxMultipartMemory
	registerRoutes(r, NewHandler(store, testCache(1024)), testTokens(), cap)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)

	body, ct := multipartBody(t, map[string]fileSpec{
		"configFile":  {name: "app.yaml", content: "version: 1"},
		"executeFile": {name: "app.exe", content: strings.Repeat("x", cap)},
	})
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/version", body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", ct)
	req.Header.Set("Authorization", "Bearer 0123456789abcdef")

	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, string(got), "too large")
	assert.Contains(t, string(got), strconv.Itoa(cap), "the message must name the limit")
}

// A body under the cap is unaffected: the cap is a guard rail, not a policy.
func TestHandleUpload_AcceptsABodyUnderTheCap(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	store.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).Times(2)

	r := gin.New()
	r.MaxMultipartMemory = maxMultipartMemory
	registerRoutes(r, NewHandler(store, testCache(1024)), testTokens(), 1<<20)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)

	body, ct := multipartBody(t, map[string]fileSpec{
		"configFile":  {name: "app.yaml", content: "version: 1"},
		"executeFile": {name: "app.exe", content: "bin!"},
	})
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/version", body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", ct)
	req.Header.Set("Authorization", "Bearer 0123456789abcdef")

	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
