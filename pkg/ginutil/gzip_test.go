package ginutil

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/gzip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func gzipEngine(t *testing.T, minSize int, h gin.HandlerFunc) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Gzip(minSize))
	r.GET("/x", h)
	return r
}

func writeBody(body string) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Content-Type", "application/json")
		_, _ = c.Writer.WriteString(body)
	}
}

func doGet(r *gin.Engine, acceptEncoding string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	if acceptEncoding != "" {
		req.Header.Set("Accept-Encoding", acceptEncoding)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func gunzip(t *testing.T, b []byte) string {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(b))
	require.NoError(t, err)
	out, err := io.ReadAll(zr)
	require.NoError(t, err)
	return string(out)
}

func TestGzip_CompressesAboveThreshold(t *testing.T) {
	body := strings.Repeat("a", 4096)
	w := doGet(gzipEngine(t, 1024, writeBody(body)), "gzip")

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "gzip", w.Header().Get("Content-Encoding"))
	assert.Equal(t, "Accept-Encoding", w.Header().Get("Vary"))
	assert.Empty(t, w.Header().Get("Content-Length"), "a stale Content-Length would truncate the body")
	assert.Less(t, w.Body.Len(), len(body))
	assert.Equal(t, body, gunzip(t, w.Body.Bytes()), "round-trip must be lossless")
}

func TestGzip_PassesThroughBelowThreshold(t *testing.T) {
	body := strings.Repeat("a", 100)
	w := doGet(gzipEngine(t, 1024, writeBody(body)), "gzip")

	assert.Empty(t, w.Header().Get("Content-Encoding"))
	assert.Equal(t, body, w.Body.String())
}

func TestGzip_PassesThroughWithoutAcceptEncoding(t *testing.T) {
	body := strings.Repeat("a", 4096)
	w := doGet(gzipEngine(t, 1024, writeBody(body)), "")

	assert.Empty(t, w.Header().Get("Content-Encoding"))
	assert.Empty(t, w.Header().Get("Vary"))
	assert.Equal(t, body, w.Body.String())
}

func TestGzip_AcceptEncodingParsing(t *testing.T) {
	body := strings.Repeat("a", 4096)
	tests := []struct {
		header string
		want   bool
	}{
		{"gzip", true},
		{"GZIP", true},
		{"deflate, gzip", true},
		{"gzip;q=1.0, deflate", true},
		{" gzip ", true},
		{"deflate", false},
		{"br", false},
		{"gzipped", false},
	}
	for _, tc := range tests {
		t.Run(tc.header, func(t *testing.T) {
			w := doGet(gzipEngine(t, 1024, writeBody(body)), tc.header)
			if tc.want {
				assert.Equal(t, "gzip", w.Header().Get("Content-Encoding"))
				return
			}
			assert.Empty(t, w.Header().Get("Content-Encoding"))
		})
	}
}

func TestGzip_ExactThresholdBoundary(t *testing.T) {
	// Exactly minSize must compress: the check is >=, not >.
	w := doGet(gzipEngine(t, 1024, writeBody(strings.Repeat("a", 1024))), "gzip")
	assert.Equal(t, "gzip", w.Header().Get("Content-Encoding"))

	w = doGet(gzipEngine(t, 1024, writeBody(strings.Repeat("a", 1023))), "gzip")
	assert.Empty(t, w.Header().Get("Content-Encoding"))
}

func TestGzip_ManySmallWritesAccumulateToThreshold(t *testing.T) {
	want := strings.Repeat("b", 16*200)
	r := gzipEngine(t, 1024, func(c *gin.Context) {
		for i := 0; i < 200; i++ {
			_, _ = c.Writer.WriteString(strings.Repeat("b", 16))
		}
	})
	w := doGet(r, "gzip")

	assert.Equal(t, "gzip", w.Header().Get("Content-Encoding"))
	assert.Equal(t, want, gunzip(t, w.Body.Bytes()))
}

func TestGzip_PreservesStatusCode(t *testing.T) {
	body := strings.Repeat("c", 4096)
	r := gzipEngine(t, 1024, func(c *gin.Context) { c.String(http.StatusTeapot, body) })
	w := doGet(r, "gzip")

	assert.Equal(t, http.StatusTeapot, w.Code)
	assert.Equal(t, "gzip", w.Header().Get("Content-Encoding"))
	assert.Equal(t, body, gunzip(t, w.Body.Bytes()))
}

func TestGzip_PreservesStatusCodeBelowThreshold(t *testing.T) {
	r := gzipEngine(t, 1024, func(c *gin.Context) { c.String(http.StatusNotFound, "nope") })
	w := doGet(r, "gzip")

	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Equal(t, "nope", w.Body.String())
}

func TestGzip_EmptyBodyIsSafe(t *testing.T) {
	r := gzipEngine(t, 1024, func(c *gin.Context) { c.Status(http.StatusNoContent) })
	w := doGet(r, "gzip")

	assert.Equal(t, http.StatusNoContent, w.Code)
	assert.Empty(t, w.Body.String())
	assert.Empty(t, w.Header().Get("Content-Encoding"))
}

func TestGzip_JSONRenderRoundTrips(t *testing.T) {
	payload := map[string]string{"k": strings.Repeat("v", 4096)}
	r := gzipEngine(t, 1024, func(c *gin.Context) { c.JSON(http.StatusOK, payload) })
	w := doGet(r, "gzip")

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "gzip", w.Header().Get("Content-Encoding"))
	assert.Contains(t, w.Header().Get("Content-Type"), "application/json")
	assert.JSONEq(t, `{"k":"`+strings.Repeat("v", 4096)+`"}`, gunzip(t, w.Body.Bytes()))
}

func TestGzip_DoesNotDoubleEncode(t *testing.T) {
	body := strings.Repeat("a", 4096)
	r := gzipEngine(t, 1024, func(c *gin.Context) {
		c.Header("Content-Encoding", "br")
		_, _ = c.Writer.WriteString(body)
	})
	w := doGet(r, "gzip")

	assert.Equal(t, "br", w.Header().Get("Content-Encoding"), "an already-encoded body must pass through")
	assert.Equal(t, body, w.Body.String())
}

func TestGzip_ReusesPooledWritersAcrossRequests(t *testing.T) {
	body := strings.Repeat("a", 4096)
	r := gzipEngine(t, 1024, writeBody(body))
	for i := 0; i < 10; i++ {
		w := doGet(r, "gzip")
		require.Equal(t, "gzip", w.Header().Get("Content-Encoding"), "request %d", i)
		require.Equal(t, body, gunzip(t, w.Body.Bytes()), "pooled writer must be fully reset, request %d", i)
	}
}

// A panicking handler unwinds through the writer's deferred close. Recovery must
// still be able to report the failure rather than the process dying.
func TestGzip_PanicIsRecoverable(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(gin.Recovery(), Gzip(1024))
	r.GET("/x", func(c *gin.Context) { panic("boom") })

	assert.NotPanics(t, func() {
		assert.Equal(t, http.StatusInternalServerError, doGet(r, "gzip").Code)
	})
}

// The writer must be usable again after a panic left one mid-flight, or a pooled
// writer could carry a half-finished stream into the next request.
func TestGzip_PoolSurvivesPanicMidBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := strings.Repeat("a", 4096)
	r := gin.New()
	r.Use(gin.Recovery(), Gzip(1024))
	r.GET("/boom", func(c *gin.Context) {
		_, _ = c.Writer.WriteString(body)
		panic("boom")
	})
	r.GET("/x", writeBody(body))

	doGet(r, "gzip")

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, body, gunzip(t, w.Body.Bytes()), "a later request must get a clean writer")
}
